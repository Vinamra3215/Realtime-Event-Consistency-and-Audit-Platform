import json
from pathlib import Path

import pytest

from recap.config import Settings
from recap.runner import benchmark

pytestmark = pytest.mark.performance


def test_repeatable_benchmark_artifacts(db, tmp_path, request):
    if not request.config.getoption("--performance"):
        pytest.skip("Use --performance to execute benchmark measurements")
    output = tmp_path / "benchmark"
    result = benchmark(
        db,
        Path(__file__).parents[1] / "fixtures" / "paysim_sample.csv",
        output,
        Settings(base_delay=0.001, max_delay=0.01, delayed_seconds=0.001),
        limit=100,
    )
    assert len(result["measurements"]) == 4
    assert all(m["metrics"]["logical_transactions"] == 11 for m in result["measurements"])
    assert all(m["peak_python_allocated_bytes"] > 0 for m in result["measurements"])
    assert json.loads((output / "benchmark.json").read_text())["transport"] == "direct"
    assert len((output / "comparison.csv").read_text().splitlines()) == 5
