from copy import deepcopy

import pytest
from sqlalchemy import select

from recap.analytics import percentiles, summarize, write_report
from recap.database import transactions
from recap.reconciliation import balance_findings, classify, reconcile
from recap.recovery import RecoveryWorker
from recap.service import TransactionService


def test_transaction_specific_balances(prepared):
    events = prepared[1]
    assert balance_findings(events[0]) == []
    assert balance_findings(events[1]) == []  # CASH_IN credits originator, debits destination.
    assert balance_findings(events[2]) == ["destination_balance_unavailable"]
    assert "origin_balance_inconsistent" in balance_findings(events[6])


@pytest.mark.parametrize(
    "internal,external,expected",
    [
        ({"status": "SUCCESS"}, {"outcome": "SUCCESS", "effect_count": 1}, "match"),
        (None, {"outcome": "SUCCESS", "effect_count": 1}, "internal_missing"),
        ({"status": "SUCCESS"}, None, "external_missing"),
        ({"status": "SUCCESS"}, {"outcome": "FAILED", "effect_count": 0}, "outcome_mismatch"),
        ({"status": "UNRESOLVED"}, {"outcome": "SUCCESS", "effect_count": 1}, "unresolved"),
        ({"status": "SUCCESS"}, {"outcome": "SUCCESS", "effect_count": 2}, "duplicate_processing"),
    ],
)
def test_discrepancy_classes(prepared, internal, external, expected):
    assert classify(internal, external, prepared[1][0])["classification"] == expected


def test_balance_findings_do_not_change_outcome_match(prepared):
    reference = deepcopy(prepared[1][0])
    reference["balances"]["origin_after"] = 0
    result = classify({"status": "SUCCESS"}, {"outcome": "SUCCESS", "effect_count": 1}, reference)
    assert result["classification"] == "match"
    assert result["details"]["balance_inconsistent"]


def test_reconciliation_report_agrees_with_database(db, prepared, clock, tmp_path):
    manifest, events = prepared
    # Before ingestion, references detect absent internal records.
    assert reconcile(db, manifest["run_id"], clock)["classifications"] == {"internal_missing": 11}
    service = TransactionService(db, clock)
    for event in events:
        service.accept(event)
    RecoveryWorker(db, clock, clock.sleep).drain(manifest["run_id"])
    result = reconcile(db, manifest["run_id"], clock)
    assert result["classifications"] == {"match": 11}
    assert result["balance_inconsistent"] == 1
    metrics = write_report(db, manifest["run_id"], tmp_path / "report")
    assert metrics["logical_transactions"] == 11
    assert metrics["source_rows_rejected"] == 3
    assert metrics["provider_effects"] == 11
    assert metrics["recovery_success_rate"] is None
    assert metrics["reconciliation_match_rate"] == 1
    assert (tmp_path / "report" / "provenance.json").exists()
    with db.engine.connect() as connection:
        assert (
            len(connection.execute(select(transactions)).all()) == metrics["logical_transactions"]
        )
    assert summarize(db, manifest["run_id"]) == metrics


def test_percentile_definition():
    assert percentiles([])["p95"] is None
    assert percentiles([1])["p99"] == 1
    assert percentiles([1, 2, 3, 4, 5])["p95"] == pytest.approx(4.8)
