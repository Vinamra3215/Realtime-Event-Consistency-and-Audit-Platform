"""Authoritative simulated provider: commits separately from application state."""

import time
from dataclasses import dataclass

from sqlalchemy import select, update

from recap.database import insert_ignore, provider_payments


@dataclass(frozen=True)
class Observation:
    outcome: str
    reason: str


class ProviderSimulator:
    def __init__(self, db, clock=time.time):
        self.db, self.clock = db, clock

    def request(self, event, settings, check=False):
        now = self.clock()
        where = (provider_payments.c.run_id == event["run_id"]) & (
            provider_payments.c.idempotency_key == event["idempotency_key"]
        )
        with self.db.transaction() as connection:
            insert_ignore(
                connection,
                provider_payments,
                {
                    "run_id": event["run_id"],
                    "idempotency_key": event["idempotency_key"],
                    "transaction_id": event["transaction_id"],
                    "payload_hash": event["payload_hash"],
                    "outcome": "NOT_FOUND",
                    "scenario": event["scenario"],
                    "submit_calls": 0,
                    "check_calls": 0,
                    "effect_count": 0,
                    "first_call_at": now,
                    "visible_after": now + settings.delayed_seconds,
                },
                ["run_id", "idempotency_key"],
            )
            row = dict(
                connection.execute(select(provider_payments).where(where).with_for_update())
                .mappings()
                .one()
            )
            if row["payload_hash"] != event["payload_hash"]:
                raise ValueError("Provider idempotency key payload conflict")
            if check:
                connection.execute(
                    update(provider_payments)
                    .where(where)
                    .values(check_calls=row["check_calls"] + 1)
                )
                return Observation(row["outcome"], "authoritative_status_check")
            call = row["submit_calls"] + 1
            scenario = row["scenario"]
            outcome, effect = row["outcome"], row["effect_count"]
            if outcome not in ("SUCCESS", "FAILED"):
                if scenario == "permanent_failure":
                    outcome = "FAILED"
                elif scenario == "always_unknown":
                    outcome = "UNKNOWN"
                elif scenario == "provider_unavailable" and call <= settings.unavailable_attempts:
                    outcome = "NOT_FOUND"
                else:
                    outcome, effect = "SUCCESS", 1
            connection.execute(
                update(provider_payments)
                .where(where)
                .values(outcome=outcome, effect_count=effect, submit_calls=call)
            )
            # Actual provider outcome is persisted even when no reply is observable.
            if scenario in ("timeout_after_completion", "dropped_response") and call == 1:
                return Observation("UNKNOWN", scenario)
            if scenario == "delayed_response" and now < row["visible_after"]:
                return Observation("UNKNOWN", "response_not_yet_visible")
            if outcome == "NOT_FOUND":
                return Observation("RETRYABLE", "provider_unavailable")
            return Observation(outcome, "idempotent_provider_response")
