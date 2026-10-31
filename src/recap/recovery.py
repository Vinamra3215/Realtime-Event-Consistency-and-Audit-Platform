"""M3 durable lease scheduler. A stale owner cannot finalize a reclaimed job."""

import time
from uuid import uuid4

from sqlalchemy import and_, func, or_, select, update

from recap.config import Settings
from recap.database import jobs, key_filter, references, runs, transactions
from recap.policies import get_policy
from recap.simulator import ProviderSimulator
from recap.state_machine import audit, transition


class RecoveryWorker:
    def __init__(self, db, clock=time.time, sleep=time.sleep, provider=None):
        self.db, self.clock, self.sleep = db, clock, sleep
        self.provider = provider or ProviderSimulator(db, clock)

    def claim(self, run_id=None):
        now = self.clock()
        with self.db.transaction() as connection:
            query = select(jobs).where(
                or_(
                    and_(jobs.c.status == "READY", jobs.c.next_attempt_at <= now),
                    and_(jobs.c.status == "LEASED", jobs.c.lease_until <= now),
                )
            )
            if run_id:
                query = query.where(jobs.c.run_id == run_id)
            job = (
                connection.execute(
                    query.order_by(jobs.c.next_attempt_at, jobs.c.transaction_id)
                    .limit(1)
                    .with_for_update(skip_locked=True)
                )
                .mappings()
                .first()
            )
            if job is None:
                return None
            run_id, tid = job["run_id"], job["transaction_id"]
            where = key_filter(transactions, run_id, tid)
            transaction = dict(
                connection.execute(select(transactions).where(where).with_for_update())
                .mappings()
                .one()
            )
            manifest = connection.execute(
                select(runs.c.manifest).where(runs.c.run_id == run_id)
            ).scalar_one()
            settings = Settings(**manifest["config"])
            if transaction["status"] in ("SUCCESS", "FAILED"):
                connection.execute(
                    update(jobs)
                    .where(key_filter(jobs, run_id, tid))
                    .values(status="DONE", lease_token=None, lease_until=None)
                )
                return {"finished": True}
            if job["status"] == "LEASED":
                if transaction["status"] == "PROCESSING":
                    transition(
                        connection,
                        transaction,
                        "UNRESOLVED",
                        now,
                        {"reason": "worker_lease_expired"},
                    )
                audit(connection, transaction, "lease_reclaimed", now)
            if transaction["attempts"] >= settings.max_attempts:
                self._exhaust(connection, transaction, now)
                return {"finished": True}
            transaction["attempts"] += 1
            connection.execute(
                update(transactions).where(where).values(attempts=transaction["attempts"])
            )
            transition(connection, transaction, "PROCESSING", now)
            token = str(uuid4())
            connection.execute(
                update(jobs)
                .where(key_filter(jobs, run_id, tid))
                .values(
                    status="LEASED", lease_token=token, lease_until=now + settings.lease_seconds
                )
            )
            value = connection.execute(
                select(references.c.event).where(key_filter(references, run_id, tid))
            ).scalar_one()
            return {
                "finished": False,
                "event": value,
                "settings": settings,
                "token": token,
                "attempt": transaction["attempts"],
            }

    def _exhaust(self, connection, transaction, now):
        if transaction["status"] == "PROCESSING":
            transition(connection, transaction, "UNRESOLVED", now)
        connection.execute(
            update(jobs)
            .where(key_filter(jobs, transaction["run_id"], transaction["transaction_id"]))
            .values(status="EXHAUSTED", lease_token=None, lease_until=None)
        )
        connection.execute(
            update(transactions)
            .where(key_filter(transactions, transaction["run_id"], transaction["transaction_id"]))
            .values(last_error="recovery_exhausted")
        )
        audit(
            connection,
            transaction,
            "exhaustion",
            now,
            {"reason": "attempt_budget_exhausted; outcome remains uncertain"},
        )

    def execute(self, claim):
        value, settings = claim["event"], claim["settings"]
        policy = get_policy(settings.policy)
        actions = []
        if policy.check_first and claim["attempt"] > 1:
            observed = self.provider.request(value, settings, check=True)
            actions.append(
                {"action": "status_check", "outcome": observed.outcome, "reason": observed.reason}
            )
            if observed.outcome != "NOT_FOUND":
                return observed, actions
        observed = self.provider.request(value, settings)
        actions.append(
            {
                "action": "submit" if claim["attempt"] == 1 else "retry",
                "outcome": observed.outcome,
                "reason": observed.reason,
            }
        )
        return observed, actions

    def finalize(self, claim, observed, actions):
        value, settings, now = claim["event"], claim["settings"], self.clock()
        run_id, tid = value["run_id"], value["transaction_id"]
        with self.db.transaction() as connection:
            job = (
                connection.execute(
                    select(jobs).where(key_filter(jobs, run_id, tid)).with_for_update()
                )
                .mappings()
                .one()
            )
            if job["lease_token"] != claim["token"] or job["status"] != "LEASED":
                return False
            transaction = dict(
                connection.execute(
                    select(transactions)
                    .where(key_filter(transactions, run_id, tid))
                    .with_for_update()
                )
                .mappings()
                .one()
            )
            for action in actions:
                audit(connection, transaction, action["action"], now, action)
            next_state = (
                observed.outcome if observed.outcome in ("SUCCESS", "FAILED") else "UNRESOLVED"
            )
            transition(
                connection,
                transaction,
                next_state,
                now,
                {"observation": observed.outcome, "reason": observed.reason},
            )
            connection.execute(
                update(transactions)
                .where(key_filter(transactions, run_id, tid))
                .values(last_error=None if next_state == "SUCCESS" else observed.reason)
            )
            if next_state in ("SUCCESS", "FAILED"):
                connection.execute(
                    update(jobs)
                    .where(key_filter(jobs, run_id, tid))
                    .values(status="DONE", lease_token=None, lease_until=None)
                )
            elif transaction["attempts"] >= settings.max_attempts:
                self._exhaust(connection, transaction, now)
            else:
                delay = get_policy(settings.policy).delay(
                    transaction["attempts"], settings, value["idempotency_key"]
                )
                connection.execute(
                    update(jobs)
                    .where(key_filter(jobs, run_id, tid))
                    .values(
                        status="READY",
                        lease_token=None,
                        lease_until=None,
                        next_attempt_at=now + delay,
                    )
                )
            return True

    def step(self, run_id=None, after_provider=None):
        claim = self.claim(run_id)
        if claim is None:
            return False
        if claim["finished"]:
            return True
        observed, actions = self.execute(claim)
        if after_provider:
            after_provider(claim, observed)  # Deliberate crash window used by restart tests.
        self.finalize(claim, observed, actions)
        return True

    def drain(self, run_id=None, timeout=120, stop=None):
        deadline, count = self.clock() + timeout, 0
        while not stop or not stop():
            if self.clock() > deadline:
                raise TimeoutError("Recovery timeout; durable jobs are retained for a later worker")
            if self.step(run_id):
                count += 1
                continue
            with self.db.engine.connect() as connection:
                query = (
                    select(func.count())
                    .select_from(jobs)
                    .where(jobs.c.status.in_(["READY", "LEASED"]))
                )
                if run_id:
                    query = query.where(jobs.c.run_id == run_id)
                remaining = connection.execute(query).scalar_one()
            if not remaining:
                return count
            self.sleep(0.01)
        return count
