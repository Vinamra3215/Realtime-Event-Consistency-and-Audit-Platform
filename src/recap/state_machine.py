from uuid import uuid4

from sqlalchemy import insert, update

from recap.database import history, key_filter, transactions

TRANSITIONS = {
    "INITIATED": {"PROCESSING"},
    "PROCESSING": {"SUCCESS", "FAILED", "UNRESOLVED"},
    "UNRESOLVED": {"PROCESSING", "SUCCESS", "FAILED"},
    "SUCCESS": set(),
    "FAILED": set(),
}


def require_transition(previous, next_state):
    if next_state not in TRANSITIONS.get(previous, set()):
        raise ValueError(f"Invalid transition {previous} -> {next_state}")


def audit(connection, transaction, event_type, now, details=None, previous=None):
    connection.execute(
        insert(history).values(
            event_id=str(uuid4()),
            run_id=transaction["run_id"],
            transaction_id=transaction["transaction_id"],
            event_type=event_type,
            previous_state=previous,
            next_state=transaction["status"],
            attempt_number=transaction["attempts"],
            timestamp=now,
            details=details or {},
        )
    )


def transition(connection, transaction, next_state, now, details=None):
    previous = transaction["status"]
    require_transition(previous, next_state)
    values = {"status": next_state, "updated_at": now}
    if next_state == "UNRESOLVED" and transaction["first_unresolved_at"] is None:
        values["first_unresolved_at"] = now
    if next_state in ("SUCCESS", "FAILED"):
        values["resolved_at"] = now
    connection.execute(
        update(transactions)
        .where(key_filter(transactions, transaction["run_id"], transaction["transaction_id"]))
        .values(**values)
    )
    transaction.update(values)
    audit(connection, transaction, "transition", now, details, previous)
