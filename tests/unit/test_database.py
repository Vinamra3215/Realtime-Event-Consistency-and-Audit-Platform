from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import pytest
from sqlalchemy import func, insert, select
from sqlalchemy.exc import IntegrityError

from recap.database import history, inbox, jobs, transactions
from recap.service import TransactionService
from recap.state_machine import require_transition


def count(db, table):
    with db.engine.connect() as connection:
        return connection.execute(select(func.count()).select_from(table)).scalar_one()


def test_atomic_acceptance_and_redelivery(db, prepared, clock):
    value = prepared[1][0]
    service = TransactionService(db, clock)
    assert service.accept(value) == "accepted"
    assert service.accept(value) == "redelivery"
    duplicate = {**value, "event_id": str(uuid4()), "event_type": "duplicate_submission"}
    assert service.accept(duplicate) == "duplicate"
    assert count(db, transactions) == count(db, jobs) == 1
    assert count(db, inbox) == count(db, history) == 2


def test_rollback(db, prepared, clock):
    def fail(connection):
        raise RuntimeError("injected crash")

    with pytest.raises(RuntimeError):
        TransactionService(db, clock).accept(prepared[1][0], after_write=fail)
    for table in (transactions, jobs, inbox, history):
        assert count(db, table) == 0


def test_concurrent_duplicates(db, prepared):
    service = TransactionService(db)
    value = prepared[1][0]
    with ThreadPoolExecutor(max_workers=4) as pool:
        outcomes = list(pool.map(lambda _: service.accept(value), range(12)))
    assert outcomes.count("accepted") == 1
    assert outcomes.count("redelivery") == 11
    assert count(db, transactions) == 1


def test_foreign_key_and_state_constraints(db):
    with pytest.raises(IntegrityError), db.transaction() as connection:
        connection.execute(
            insert(jobs).values(
                run_id=str(uuid4()), transaction_id=str(uuid4()), status="READY", next_attempt_at=0
            )
        )


@pytest.mark.parametrize(
    "previous,next_state",
    [
        ("INITIATED", "PROCESSING"),
        ("PROCESSING", "UNRESOLVED"),
        ("UNRESOLVED", "PROCESSING"),
        ("PROCESSING", "SUCCESS"),
        ("PROCESSING", "FAILED"),
    ],
)
def test_valid_transitions(previous, next_state):
    require_transition(previous, next_state)


@pytest.mark.parametrize(
    "previous,next_state",
    [
        ("SUCCESS", "PROCESSING"),
        ("FAILED", "SUCCESS"),
        ("INITIATED", "SUCCESS"),
        ("UNRESOLVED", "UNRESOLVED"),
    ],
)
def test_invalid_transitions(previous, next_state):
    with pytest.raises(ValueError):
        require_transition(previous, next_state)
