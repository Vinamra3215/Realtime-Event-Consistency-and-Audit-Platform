from dataclasses import replace
from pathlib import Path

import pytest
from sqlalchemy import select

from recap.config import POLICIES, SCENARIOS, Settings
from recap.database import Database, provider_payments, register_prepared, transactions
from recap.ingestion import prepare, read_events
from recap.policies import get_policy
from recap.recovery import RecoveryWorker
from recap.service import TransactionService


def setup_scenario(db, tmp_path, clock, scenario, policy="status_check_first", **overrides):
    settings = Settings(policy=policy, failure_mix={scenario: 1}, **overrides)
    directory = tmp_path / "prepared"
    manifest = prepare(
        Path(__file__).parents[1] / "fixtures" / "paysim_sample.csv", directory, settings, limit=1
    )
    register_prepared(db, directory, clock)
    events = list(read_events(directory / "events.jsonl"))
    for value in events:
        TransactionService(db, clock).accept(value)
    return manifest, events[0]


@pytest.mark.parametrize("policy", POLICIES)
@pytest.mark.parametrize("scenario", SCENARIOS)
def test_every_policy_executes_every_scenario(db, tmp_path, clock, scenario, policy):
    manifest, value = setup_scenario(db, tmp_path, clock, scenario, policy)
    RecoveryWorker(db, clock, clock.sleep).drain(manifest["run_id"])
    with db.engine.connect() as connection:
        transaction = connection.execute(select(transactions)).mappings().one()
        provider = connection.execute(select(provider_payments)).mappings().one()
    expected = (
        "FAILED"
        if scenario == "permanent_failure"
        else "UNRESOLVED"
        if scenario == "always_unknown"
        else "SUCCESS"
    )
    assert transaction["status"] == expected
    assert provider["effect_count"] == (1 if expected == "SUCCESS" else 0)
    assert transaction["attempts"] <= 5
    if scenario == "permanent_failure":
        assert provider["submit_calls"] == 1
    if scenario == "timeout_after_completion" and policy == "status_check_first":
        assert provider["submit_calls"] == provider["check_calls"] == 1


def test_crash_after_provider_commit_and_restart(db, tmp_path, clock):
    manifest, _ = setup_scenario(db, tmp_path, clock, "clean", lease_seconds=1)
    worker = RecoveryWorker(db, clock, clock.sleep)

    def crash(*args):
        raise RuntimeError("process died after provider commit")

    with pytest.raises(RuntimeError):
        worker.step(manifest["run_id"], after_provider=crash)
    clock.sleep(2)
    restarted = Database(str(db.engine.url))
    try:
        RecoveryWorker(restarted, clock, clock.sleep).drain(manifest["run_id"])
        with restarted.engine.connect() as connection:
            assert connection.execute(select(transactions.c.status)).scalar_one() == "SUCCESS"
            assert connection.execute(select(provider_payments.c.effect_count)).scalar_one() == 1
    finally:
        restarted.dispose()


def test_stale_lease_cannot_finalize(db, tmp_path, clock):
    manifest, _ = setup_scenario(db, tmp_path, clock, "clean", lease_seconds=1)
    worker = RecoveryWorker(db, clock, clock.sleep)
    old = worker.claim(manifest["run_id"])
    observation, actions = worker.execute(old)
    clock.sleep(2)
    current = worker.claim(manifest["run_id"])
    assert not worker.finalize(old, observation, actions)
    observation2, actions2 = worker.execute(current)
    assert worker.finalize(current, observation2, actions2)


def test_exhaustion_does_not_mean_failed(db, tmp_path, clock):
    manifest, _ = setup_scenario(db, tmp_path, clock, "timeout_after_completion", max_attempts=1)
    RecoveryWorker(db, clock, clock.sleep).drain(manifest["run_id"])
    with db.engine.connect() as connection:
        assert connection.execute(select(transactions.c.status)).scalar_one() == "UNRESOLVED"
        assert connection.execute(select(provider_payments.c.outcome)).scalar_one() == "SUCCESS"


def test_policy_schedules():
    settings = replace(Settings(), base_delay=1, max_delay=4)
    assert [get_policy("fixed").delay(n, settings, "x") for n in range(1, 6)] == [1] * 5
    assert [get_policy("exponential").delay(n, settings, "x") for n in range(1, 6)] == [
        1,
        2,
        4,
        4,
        4,
    ]
    delays = [get_policy("jitter").delay(3, settings, str(i)) for i in range(10)]
    assert len(set(delays)) == 10
    assert all(0 <= d <= 4 for d in delays)
    assert delays[0] == get_policy("jitter").delay(3, settings, "0")
