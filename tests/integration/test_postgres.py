from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import func, inspect, select, update
from sqlalchemy.exc import IntegrityError

from recap.config import Settings
from recap.database import inbox, jobs, metadata, provider_payments, register_prepared, transactions
from recap.ingestion import prepare, read_events
from recap.recovery import RecoveryWorker
from recap.service import TransactionService
from tests.conftest import Clock

pytestmark = pytest.mark.integration


def setup_run(db, tmp_path, limit=2):
    folder = tmp_path / "prepared"
    manifest = prepare(
        Path(__file__).parents[1] / "fixtures" / "paysim_sample.csv",
        folder,
        Settings(failure_mix={"clean": 1}),
        limit=limit,
    )
    register_prepared(db, folder)
    return manifest, list(read_events(folder / "events.jsonl"))


def test_postgres_migrations_match_models(postgres_db):
    postgres_db.migrate()
    inspector = inspect(postgres_db.engine)
    assert set(metadata.tables) <= set(inspector.get_table_names())
    for name, table in metadata.tables.items():
        assert {c.name for c in table.columns} == {c["name"] for c in inspector.get_columns(name)}
        assert {i.name for i in table.indexes} <= {i["name"] for i in inspector.get_indexes(name)}
    assert inspector.get_foreign_keys("transaction_events")
    assert {"uq_transaction_key"} <= {
        c["name"] for c in inspector.get_unique_constraints("transactions")
    }


def test_postgres_concurrent_duplicate_submissions(postgres_db, tmp_path):
    _, events = setup_run(postgres_db, tmp_path)
    service = TransactionService(postgres_db)
    values = [{**events[0], "event_id": str(uuid4())} for _ in range(16)]
    with ThreadPoolExecutor(max_workers=8) as pool:
        outcomes = list(pool.map(service.accept, values))
    assert outcomes.count("accepted") == 1
    assert outcomes.count("duplicate") == 15
    with postgres_db.engine.connect() as connection:
        assert connection.execute(select(func.count()).select_from(transactions)).scalar_one() == 1
        assert connection.execute(select(transactions.c.duplicate_submissions)).scalar_one() == 15


def test_postgres_rollback(postgres_db, tmp_path):
    _, events = setup_run(postgres_db, tmp_path)

    def fail(connection):
        raise RuntimeError("rollback injection")

    with pytest.raises(RuntimeError):
        TransactionService(postgres_db).accept(events[0], after_write=fail)
    with postgres_db.engine.connect() as connection:
        for table in (transactions, inbox, jobs):
            assert connection.execute(select(func.count()).select_from(table)).scalar_one() == 0


def test_postgres_workers_claim_distinct_jobs(postgres_db, tmp_path):
    manifest, events = setup_run(postgres_db, tmp_path)
    for value in events:
        TransactionService(postgres_db).accept(value)
    workers = [RecoveryWorker(postgres_db), RecoveryWorker(postgres_db)]
    with ThreadPoolExecutor(max_workers=2) as pool:
        claims = list(pool.map(lambda w: w.claim(manifest["run_id"]), workers))
    assert len({c["event"]["transaction_id"] for c in claims}) == 2
    for worker, claim in zip(workers, claims):
        observed, actions = worker.execute(claim)
        worker.finalize(claim, observed, actions)
    with postgres_db.engine.connect() as connection:
        assert (
            connection.execute(select(func.sum(provider_payments.c.effect_count))).scalar_one() == 2
        )


def test_postgres_constraints_and_crash_restart(postgres_db, tmp_path):
    manifest, events = setup_run(postgres_db, tmp_path, limit=1)
    clock = Clock()
    TransactionService(postgres_db, clock).accept(events[0])
    with pytest.raises(IntegrityError), postgres_db.transaction() as connection:
        connection.execute(update(transactions).values(status="INVENTED"))
    with pytest.raises(IntegrityError), postgres_db.transaction() as connection:
        connection.execute(update(transactions).values(amount_minor=-1))
    worker = RecoveryWorker(postgres_db, clock, clock.sleep)

    def crash(*args):
        raise RuntimeError("after independent provider commit")

    with pytest.raises(RuntimeError):
        worker.step(manifest["run_id"], after_provider=crash)
    clock.sleep(31)
    RecoveryWorker(postgres_db, clock, clock.sleep).drain(manifest["run_id"])
    with postgres_db.engine.connect() as connection:
        assert connection.execute(select(transactions.c.status)).scalar_one() == "SUCCESS"
        assert connection.execute(select(provider_payments.c.effect_count)).scalar_one() == 1
