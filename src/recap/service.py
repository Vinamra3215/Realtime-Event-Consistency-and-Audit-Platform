"""M2: atomically accept requests, deduplicate delivery, and schedule processing."""

import json
import time
from uuid import UUID

from sqlalchemy import insert, select, update

from recap.database import (
    history,
    inbox,
    insert_ignore,
    jobs,
    key_filter,
    references,
    rejections,
    runs,
    transactions,
)
from recap.provenance import stable_hash
from recap.state_machine import audit
from recap.validation import InvalidEvent, dead_letter, decode_event, validate_event


class TransactionService:
    def __init__(self, db, clock=time.time):
        self.db, self.clock = db, clock

    def accept(self, value, metadata=None, after_write=None):
        validate_event(value)
        run_id, transaction_id = value["run_id"], value["transaction_id"]
        now = self.clock()
        with self.db.transaction() as connection:
            if not connection.execute(select(runs.c.run_id).where(runs.c.run_id == run_id)).first():
                raise InvalidEvent("Unknown run; register prepared manifest before publishing")
            reference = connection.execute(
                select(references.c.event).where(
                    (references.c.run_id == run_id)
                    & (references.c.idempotency_key == value["idempotency_key"])
                )
            ).scalar_one_or_none()
            if (
                reference is None
                or reference["payload_hash"] != value["payload_hash"]
                or reference["scenario"] != value["scenario"]
            ):
                raise InvalidEvent("Request conflicts with or lacks registered source reference")
            # Serialize the identity via unique constraint, then lock its canonical row.
            created = insert_ignore(
                connection,
                transactions,
                {
                    "run_id": run_id,
                    "transaction_id": transaction_id,
                    "idempotency_key": value["idempotency_key"],
                    "payload_hash": value["payload_hash"],
                    "originator": value["originator"],
                    "destination": value["destination"],
                    "amount_minor": value["amount_minor"],
                    "status": "INITIATED",
                    "attempts": 0,
                    "duplicate_submissions": 0,
                    "created_at": now,
                    "updated_at": now,
                },
                ["run_id", "idempotency_key"],
            )
            transaction = dict(
                connection.execute(
                    select(transactions)
                    .where(
                        (transactions.c.run_id == run_id)
                        & (transactions.c.idempotency_key == value["idempotency_key"])
                    )
                    .with_for_update()
                )
                .mappings()
                .one()
            )
            if transaction["payload_hash"] != value["payload_hash"]:
                raise InvalidEvent("Idempotency key reused for different payment payload")
            previous = connection.execute(
                select(inbox.c.event_hash).where(
                    (inbox.c.run_id == run_id) & (inbox.c.event_id == value["event_id"])
                )
            ).scalar_one_or_none()
            if previous:
                if previous != stable_hash(value):
                    raise InvalidEvent("event_id reused with changed contents")
                return "redelivery"
            connection.execute(
                insert(inbox).values(
                    run_id=run_id,
                    event_id=value["event_id"],
                    transaction_id=transaction["transaction_id"],
                    event_hash=stable_hash(value),
                    accepted_at=now,
                    metadata=metadata or {},
                )
            )
            if created:
                audit(connection, transaction, "request", now, {"event_id": value["event_id"]})
                connection.execute(
                    insert(jobs).values(
                        run_id=run_id,
                        transaction_id=transaction["transaction_id"],
                        status="READY",
                        next_attempt_at=now,
                    )
                )
            else:
                connection.execute(
                    update(transactions)
                    .where(key_filter(transactions, run_id, transaction["transaction_id"]))
                    .values(duplicate_submissions=transactions.c.duplicate_submissions + 1)
                )
                audit(
                    connection,
                    transaction,
                    "duplicate_submission",
                    now,
                    {"event_id": value["event_id"]},
                )
            if after_write:
                after_write(connection)  # Fault injection proves rollback of the entire acceptance.
            return "accepted" if created else "duplicate"

    def reject(self, raw, reason, metadata):
        record = dead_letter(raw, reason, metadata)
        with self.db.transaction() as connection:
            insert_ignore(
                connection,
                rejections,
                {
                    "rejection_id": record["rejection_id"],
                    "record": record,
                    "created_at": self.clock(),
                },
                ["rejection_id"],
            )
        return record

    def process_raw(self, raw, metadata=None):
        metadata = dict(metadata or {})
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                metadata["run_id"] = str(UUID(parsed.get("run_id", "")))
        except (ValueError, TypeError, UnicodeError, AttributeError):
            pass
        try:
            value = decode_event(raw)
            if "key" in metadata and metadata["key"] != value["transaction_id"]:
                raise InvalidEvent("Kafka key must equal transaction_id")
            return self.accept(value, metadata)
        except InvalidEvent as exc:
            self.reject(raw, str(exc), metadata)
            return "rejected"


def transaction_history(db, run_id, transaction_id):
    with db.engine.connect() as connection:
        return [
            dict(row)
            for row in connection.execute(
                select(history)
                .where(key_filter(history, run_id, transaction_id))
                .order_by(history.c.timestamp, history.c.event_id)
            ).mappings()
        ]
