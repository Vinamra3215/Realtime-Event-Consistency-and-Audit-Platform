from pathlib import Path

from recap.config import Settings
from recap.runner import simulate


def test_portable_whole_workflow_and_repeatability(db, tmp_path, clock):
    source = Path(__file__).parents[1] / "fixtures" / "paysim_sample.csv"
    settings = Settings(failure_mix={"timeout_after_completion": 1})
    a = simulate(db, source, tmp_path / "a", settings, clock=clock, sleep=clock.sleep)
    b = simulate(db, source, tmp_path / "b", settings, clock=clock, sleep=clock.sleep)
    assert a["logical_transactions"] == b["logical_transactions"] == 11
    assert a["provider_submit_calls"] == b["provider_submit_calls"] == 11
    assert a["provider_status_checks"] == b["provider_status_checks"] == 11
    assert a["recovery_success_rate"] == b["recovery_success_rate"] == 1
    assert a["duplicate_provider_effects"] == 0
    assert a["wire_events_unaccounted"] == 0
    assert a["execution"] == {"transport": "direct", "database": "sqlite"}
