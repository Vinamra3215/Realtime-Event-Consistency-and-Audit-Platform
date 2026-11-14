import csv
import json
from pathlib import Path

import pytest

from recap.cli import main
from recap.config import SCENARIOS, Settings, load_settings
from recap.demo_data import generate_fixture
from recap.ingestion import prepare, read_events
from recap.reconciliation import balance_findings


def test_large_fixture_is_repeatable_and_covers_configured_scenarios(tmp_path):
    first, second = tmp_path / "first.csv", tmp_path / "second.csv"
    catalog = generate_fixture(first, rows=1000, seed=25, edge_cases=True)
    generate_fixture(second, rows=1000, seed=25, edge_cases=True)
    assert first.read_bytes() == second.read_bytes()
    assert catalog["total_rows"] == 1017
    assert catalog["expected_accepted"] == 1007
    assert catalog["expected_rejected"] == 10
    settings = load_settings(Path(__file__).parents[2] / "config/edge_cases.yaml")
    manifest = prepare(first, tmp_path / "prepared", settings, limit=1017)
    assert manifest["counts"]["accepted"] == 1007
    assert manifest["counts"]["rejected"] == 10
    assert set(manifest["observed_scenarios"]) == set(SCENARIOS)
    assert manifest["counts"]["published_events"] == (
        1007 + manifest["observed_scenarios"]["duplicate_submission"]
    )
    with pytest.raises(ValueError, match="already exists"):
        generate_fixture(first)


def test_input_edges_are_identified_from_values_and_reconciled(tmp_path):
    source, output = tmp_path / "edges.csv", tmp_path / "prepared"
    catalog = generate_fixture(source, rows=0, edge_cases=True)
    prepare(source, output, Settings(failure_mix={"clean": 1}))
    accepted = {e["source"]["row"]: e for e in read_events(output / "events.jsonl")}
    rejected = {
        r["row"]: r
        for r in map(json.loads, (output / "rejected_rows.jsonl").read_text().splitlines())
    }
    named = {}
    for case in catalog["cases"]:
        row = case["row"]
        if case["expected_validation"] == "rejected":
            assert row in rejected and row not in accepted
            assert case["explanation"] in rejected[row]["reason"]
        else:
            assert row in accepted and row not in rejected
            named[case["case"]] = accepted[row]
    assert named["zero_amount"]["amount_minor"] == 0
    assert named["fractional_cent"]["amount_minor"] == 235
    assert balance_findings(named["fractional_cent"]) == []
    assert balance_findings(named["cash_in_direction"]) == []
    assert balance_findings(named["balance_mismatch"]) == ["origin_balance_inconsistent"]
    assert balance_findings(named["merchant_balance_omitted"]) == [
        "destination_balance_unavailable"
    ]
    assert (
        named["identical_source_row_a"]["idempotency_key"]
        != named["identical_source_row_b"]["idempotency_key"]
    )


@pytest.mark.parametrize("difference", [-1, 1])
def test_wrong_csv_field_count_is_rejected_without_losing_next_row(tmp_path, difference):
    source = tmp_path / "rows.csv"
    generate_fixture(source, rows=2)
    with source.open(newline="") as handle:
        rows = list(csv.reader(handle))
    rows[1] = rows[1][:-1] if difference == -1 else rows[1] + ["extra"]
    with source.open("w", newline="") as handle:
        csv.writer(handle).writerows(rows)
    output = tmp_path / "prepared"
    manifest = prepare(source, output, Settings(failure_mix={"clean": 1}))
    assert manifest["counts"] == {"read": 2, "accepted": 1, "rejected": 1, "published_events": 1}
    rejection = json.loads((output / "rejected_rows.jsonl").read_text())
    assert rejection["row"] == 1
    assert rejection["reason"] == "CSV row has incorrect field count"


def test_generation_cli_works_without_services_or_config(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    assert main(["generate-fixture", "--output", "small.csv", "--rows", "5"]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["total_rows"] == result["expected_accepted"] == 5
    assert main(["generate-fixture", "--output", "small.csv"]) == 1
    assert main(["generate-fixture", "--output", "negative.csv", "--rows", "-1"]) == 1
    assert not (tmp_path / "negative.csv").exists()
