"""Relational invariants, transactions, migrations, and durable source registration."""

import json
import time
from contextlib import contextmanager
from pathlib import Path
from uuid import UUID

from sqlalchemy import (
    JSON,
    BigInteger,
    CheckConstraint,
    Column,
    Float,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
    create_engine,
    event,
    select,
    text,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from recap.config import Settings
from recap.ingestion import read_events
from recap.provenance import sha256_file, stable_hash
from recap.resources import resource_directory

metadata = MetaData()
STATES = "'INITIATED','PROCESSING','SUCCESS','UNRESOLVED','FAILED'"
runs = Table(
    "runs",
    metadata,
    Column("run_id", String(36), primary_key=True),
    Column("manifest", JSON, nullable=False),
    Column("started_at", Float, nullable=False),
    Column("finished_at", Float),
    Column("status", String(20), nullable=False, server_default="registered"),
    Column("execution", JSON),
)
references = Table(
    "source_references",
    metadata,
    Column("run_id", String(36), ForeignKey("runs.run_id"), primary_key=True),
    Column("transaction_id", String(36), primary_key=True),
    Column("idempotency_key", String(64), nullable=False),
    Column("event", JSON, nullable=False),
    Column("submitted_at", Float),
    UniqueConstraint("run_id", "idempotency_key", name="uq_reference_key"),
)
transactions = Table(
    "transactions",
    metadata,
    Column("run_id", String(36), ForeignKey("runs.run_id"), primary_key=True),
    Column("transaction_id", String(36), primary_key=True),
    Column("idempotency_key", String(64), nullable=False),
    Column("payload_hash", String(64), nullable=False),
    Column("originator", String(128), nullable=False),
    Column("destination", String(128), nullable=False),
    Column("amount_minor", BigInteger, nullable=False),
    Column("status", String(16), nullable=False),
    Column("created_at", Float, nullable=False),
    Column("updated_at", Float, nullable=False),
    Column("first_unresolved_at", Float),
    Column("resolved_at", Float),
    Column("attempts", Integer, nullable=False, server_default="0"),
    Column("duplicate_submissions", Integer, nullable=False, server_default="0"),
    Column("last_error", Text),
    UniqueConstraint("run_id", "idempotency_key", name="uq_transaction_key"),
    CheckConstraint(f"status IN ({STATES})", name="ck_transaction_state"),
    CheckConstraint(
        "amount_minor >= 0 AND attempts >= 0 AND duplicate_submissions >= 0",
        name="ck_transaction_numbers",
    ),
)
history = Table(
    "transaction_events",
    metadata,
    Column("event_id", String(36), primary_key=True),
    Column("run_id", String(36), nullable=False),
    Column("transaction_id", String(36), nullable=False),
    Column("event_type", String(40), nullable=False),
    Column("previous_state", String(16)),
    Column("next_state", String(16), nullable=False),
    Column("attempt_number", Integer, nullable=False),
    Column("timestamp", Float, nullable=False),
    Column("details", JSON, nullable=False),
    ForeignKeyConstraint(
        ["run_id", "transaction_id"], ["transactions.run_id", "transactions.transaction_id"]
    ),
    CheckConstraint(f"next_state IN ({STATES})", name="ck_event_next_state"),
    CheckConstraint("attempt_number >= 0", name="ck_event_attempt"),
)
inbox = Table(
    "inbox",
    metadata,
    Column("run_id", String(36), primary_key=True),
    Column("event_id", String(36), primary_key=True),
    Column("transaction_id", String(36), nullable=False),
    Column("event_hash", String(64), nullable=False),
    Column("accepted_at", Float, nullable=False),
    Column("metadata", JSON, nullable=False),
    ForeignKeyConstraint(
        ["run_id", "transaction_id"], ["transactions.run_id", "transactions.transaction_id"]
    ),
)
jobs = Table(
    "recovery_jobs",
    metadata,
    Column("run_id", String(36), primary_key=True),
    Column("transaction_id", String(36), primary_key=True),
    Column("status", String(16), nullable=False),
    Column("next_attempt_at", Float, nullable=False),
    Column("lease_token", String(36)),
    Column("lease_until", Float),
    ForeignKeyConstraint(
        ["run_id", "transaction_id"], ["transactions.run_id", "transactions.transaction_id"]
    ),
    CheckConstraint("status IN ('READY','LEASED','DONE','EXHAUSTED')", name="ck_job_state"),
    CheckConstraint(
        "status != 'LEASED' OR (lease_token IS NOT NULL AND lease_until IS NOT NULL)",
        name="ck_job_lease",
    ),
)
provider_payments = Table(
    "provider_payments",
    metadata,
    Column("run_id", String(36), ForeignKey("runs.run_id"), primary_key=True),
    Column("idempotency_key", String(64), primary_key=True),
    Column("transaction_id", String(36), nullable=False),
    Column("payload_hash", String(64), nullable=False),
    Column("outcome", String(16), nullable=False),
    Column("scenario", String(40), nullable=False),
    Column("submit_calls", Integer, nullable=False),
    Column("check_calls", Integer, nullable=False),
    Column("effect_count", Integer, nullable=False),
    Column("first_call_at", Float, nullable=False),
    Column("visible_after", Float, nullable=False),
    CheckConstraint(
        "outcome IN ('SUCCESS','FAILED','UNKNOWN','NOT_FOUND')", name="ck_provider_outcome"
    ),
    CheckConstraint(
        "effect_count IN (0,1) AND submit_calls >= 0 AND check_calls >= 0",
        name="ck_provider_counts",
    ),
)
rejections = Table(
    "rejected_events",
    metadata,
    Column("rejection_id", String(64), primary_key=True),
    Column("record", JSON, nullable=False),
    Column("created_at", Float, nullable=False),
    Column("published_at", Float),
)
reconciliations = Table(
    "reconciliation_results",
    metadata,
    Column("run_id", String(36), ForeignKey("runs.run_id"), primary_key=True),
    Column("transaction_id", String(36), primary_key=True),
    Column("internal_outcome", String(16)),
    Column("reference_outcome", String(16)),
    Column("classification", String(40), nullable=False),
    Column("details", JSON, nullable=False),
    Column("checked_at", Float, nullable=False),
)
Index(
    "ix_transaction_run_status",
    transactions.c.run_id,
    transactions.c.status,
    transactions.c.updated_at,
)
Index(
    "ix_history_transaction_time", history.c.run_id, history.c.transaction_id, history.c.timestamp
)
Index("ix_jobs_due", jobs.c.status, jobs.c.next_attempt_at)
Index("ix_jobs_expired", jobs.c.status, jobs.c.lease_until)
Index("ix_rejections_pending", rejections.c.published_at)


def insert_ignore(connection, table, values, keys):
    factory = pg_insert if connection.dialect.name == "postgresql" else sqlite_insert
    result = connection.execute(
        factory(table)
        .values(**values)
        .on_conflict_do_nothing(index_elements=keys)
        .returning(*table.primary_key.columns)
    )
    return result.first() is not None


def key_filter(table, run_id, transaction_id):
    return (table.c.run_id == run_id) & (table.c.transaction_id == transaction_id)


class Database:
    def __init__(self, url):
        kwargs = {"pool_pre_ping": True}
        if url.startswith("postgresql"):
            kwargs["connect_args"] = {"connect_timeout": 10, "application_name": "recap"}
        if url.startswith("sqlite"):
            kwargs["connect_args"] = {"timeout": 30, "check_same_thread": False}
            if url.startswith("sqlite:///") and not url.endswith(":memory:"):
                Path(url.removeprefix("sqlite:///")).parent.mkdir(parents=True, exist_ok=True)
        self.engine = create_engine(url, **kwargs)
        if self.engine.dialect.name == "sqlite":

            @event.listens_for(self.engine, "connect")
            def sqlite_setup(connection, record):
                connection.execute("PRAGMA foreign_keys=ON")
                connection.execute("PRAGMA journal_mode=WAL")
        elif self.engine.dialect.name != "postgresql":
            raise ValueError("Only PostgreSQL deployment and SQLite test databases are supported")

    @contextmanager
    def transaction(self):
        with self.engine.connect() as connection:
            if self.engine.dialect.name == "sqlite":
                connection.exec_driver_sql("BEGIN IMMEDIATE")
            else:
                connection.begin()
            try:
                yield connection
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    def migrate(self):
        if self.engine.dialect.name == "sqlite":
            metadata.create_all(self.engine)
            return
        folder = resource_directory("migrations")
        with self.transaction() as connection:
            connection.execute(text("SELECT pg_advisory_xact_lock(252526)"))
            connection.exec_driver_sql(
                "CREATE TABLE IF NOT EXISTS schema_migrations (version TEXT PRIMARY KEY, checksum TEXT NOT NULL)"
            )
            for path in sorted(folder.glob("*.sql")):
                checksum = sha256_file(path)
                existing = connection.execute(
                    text("SELECT checksum FROM schema_migrations WHERE version=:version"),
                    {"version": path.name},
                ).scalar_one_or_none()
                if existing:
                    if checksum != existing:
                        raise ValueError(f"Applied migration changed: {path.name}")
                    continue
                # Psycopg's simple protocol executes the script in this same transaction.
                # Never split SQL on semicolons: comments/literals/functions can contain them.
                with connection.connection.driver_connection.cursor() as cursor:
                    cursor.execute(path.read_text(encoding="utf-8"), prepare=False)
                connection.execute(
                    text(
                        "INSERT INTO schema_migrations (version, checksum) VALUES (:version, :checksum)"
                    ),
                    {"version": path.name, "checksum": checksum},
                )

    def dispose(self):
        self.engine.dispose()


def register_prepared(db: Database, prepared_dir, clock=time.time):
    folder = Path(prepared_dir)
    manifest = json.loads((folder / "manifest.json").read_text(encoding="utf-8"))
    Settings(**manifest["config"])
    UUID(manifest["run_id"])
    counts = manifest["counts"]
    if set(counts) != {"read", "accepted", "rejected", "published_events"} or any(
        type(v) is not int or v < 0 for v in counts.values()
    ):
        raise ValueError("Invalid manifest counts")
    if counts["read"] != counts["accepted"] + counts["rejected"]:
        raise ValueError("Source accounting does not balance")
    if set(manifest["artifacts"]) != {"events.jsonl", "rejected_rows.jsonl"}:
        raise ValueError("Manifest must checksum both prepared artifacts")
    for name, checksum in manifest["artifacts"].items():
        if name not in ("events.jsonl", "rejected_rows.jsonl"):
            raise ValueError("Unexpected prepared artifact")
        if sha256_file(folder / name) != checksum:
            raise ValueError(f"Prepared artifact checksum mismatch: {name}")
    run_id = manifest["run_id"]
    with db.transaction() as connection:
        insert_ignore(
            connection,
            runs,
            {"run_id": run_id, "manifest": manifest, "started_at": clock(), "status": "registered"},
            ["run_id"],
        )
        existing = connection.execute(
            select(runs.c.manifest).where(runs.c.run_id == run_id)
        ).scalar_one()
        if stable_hash(existing) != stable_hash(manifest):
            raise ValueError("run_id already exists with a different manifest")
    # Bound memory and transactions to 500 records; partial registration is resumable.
    batch, wire_count = [], 0
    for event_value in read_events(folder / "events.jsonl"):
        if event_value["run_id"] != run_id:
            raise ValueError("Event run_id differs from manifest")
        if event_value["source"]["dataset_sha256"] != manifest["dataset"]["sha256"]:
            raise ValueError("Event dataset checksum differs from manifest")
        wire_count += 1
        batch.append(event_value)
        if len(batch) == 500:
            _register_batch(db, batch)
            batch.clear()
    if batch:
        _register_batch(db, batch)
    from sqlalchemy import func

    with db.engine.connect() as connection:
        reference_count = connection.execute(
            select(func.count()).select_from(references).where(references.c.run_id == run_id)
        ).scalar_one()
    with (folder / "rejected_rows.jsonl").open(encoding="utf-8") as handle:
        rejected_count = sum(1 for line in handle if line.strip())
    if (
        wire_count != counts["published_events"]
        or reference_count != counts["accepted"]
        or rejected_count != counts["rejected"]
    ):
        raise ValueError("Prepared artifacts disagree with manifest counts")
    return manifest


def _register_batch(db, events):
    with db.transaction() as connection:
        for value in events:
            insert_ignore(
                connection,
                references,
                {
                    "run_id": value["run_id"],
                    "transaction_id": value["transaction_id"],
                    "idempotency_key": value["idempotency_key"],
                    "event": value,
                },
                ["run_id", "transaction_id"],
            )
