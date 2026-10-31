import json
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import pytest

from recap.config import Settings
from recap.ingestion import make_event, minor_units, prepare, read_events
from recap.validation import InvalidEvent, validate_event

FIXTURE = Path(__file__).parents[1] / "fixtures" / "paysim_sample.csv"
ROW = {
    "step": "1",
    "type": "TRANSFER",
    "amount": "10",
    "nameOrig": "C1",
    "nameDest": "C2",
    "oldbalanceOrg": "20",
    "newbalanceOrig": "10",
    "oldbalanceDest": "0",
    "newbalanceDest": "10",
}


def test_money():
    assert minor_units("2.345") == 235
    assert minor_units("0.1") == 10
    for bad in ("NaN", "Infinity", "-1", "abc", "1e100"):
        with pytest.raises(ValueError):
            minor_units(bad)


def test_identity_and_timestamp():
    run = str(uuid4())
    event = make_event(ROW, 1, "a" * 64, run, Settings())
    assert event == make_event(ROW, 1, "a" * 64, run, Settings())
    other = make_event(ROW, 2, "a" * 64, run, Settings())
    assert other["idempotency_key"] != event["idempotency_key"]
    another_run = make_event(ROW, 1, "a" * 64, str(uuid4()), Settings())
    assert another_run["idempotency_key"] == event["idempotency_key"]
    assert another_run["event_id"] != event["event_id"]


def test_attribute_collision_does_not_merge_source_rows():
    run = str(uuid4())
    settings = Settings(time_bucket_seconds=3600)
    a = make_event(ROW, 1, "a" * 64, run, settings)
    b = make_event(ROW, 2, "a" * 64, run, settings)
    assert a["attribute_hash"] == b["attribute_hash"]
    assert a["idempotency_key"] != b["idempotency_key"]


def test_forged_transaction_identity_is_rejected():
    value = make_event(ROW, 1, "a" * 64, str(uuid4()), Settings())
    value["transaction_id"] = str(uuid4())
    with pytest.raises(InvalidEvent, match="source-row identity"):
        validate_event(value)


@pytest.mark.parametrize(
    "field,value",
    [
        ("amount_minor", -1),
        ("amount_minor", True),
        ("timestamp", "bad"),
        ("timestamp", "2026-02-30T00:00:00Z"),
        ("schema_version", "2"),
        ("attempt_number", 2),
        ("currency", "abc"),
    ],
)
def test_rejects_bad_event(field, value):
    event = make_event(ROW, 1, "a" * 64, str(uuid4()), Settings())
    event[field] = value
    with pytest.raises(InvalidEvent):
        validate_event(event)


def test_missing_and_tampered_fields():
    event = make_event(ROW, 1, "a" * 64, str(uuid4()), Settings())
    del event["originator"]
    with pytest.raises(InvalidEvent):
        validate_event(event)
    event["originator"] = "tampered"
    with pytest.raises(InvalidEvent, match="payload_hash"):
        validate_event(event)


def test_preparation_accounting_and_replay(tmp_path):
    settings = replace(Settings(), failure_mix={"duplicate_submission": 1})
    run = str(uuid4())
    a = prepare(FIXTURE, tmp_path / "a", settings, run_id=run)
    b = prepare(FIXTURE, tmp_path / "b", settings, run_id=run)
    assert a["counts"] == {"read": 14, "accepted": 11, "rejected": 3, "published_events": 22}
    assert a["artifacts"] == b["artifacts"]
    assert len(list(read_events(tmp_path / "a" / "events.jsonl"))) == 22
    rejects = [
        json.loads(line)
        for line in (tmp_path / "a" / "rejected_rows.jsonl").read_text().splitlines()
    ]
    assert all(r["reason"] for r in rejects)


def test_missing_csv_columns(tmp_path):
    source = tmp_path / "bad.csv"
    source.write_text("amount\n10\n")
    with pytest.raises(ValueError, match="Missing CSV columns"):
        prepare(source, tmp_path / "out", Settings())


@pytest.mark.parametrize(
    "values",
    [
        {"failure_mix": {"clean": 0.5}},
        {"base_delay": -1},
        {"max_attempts": 0},
        {"currency": "xyz"},
        {"epoch": "2026-01-01T00:00:00"},
        {"max_delay": float("nan")},
    ],
)
def test_bad_config(values):
    with pytest.raises(ValueError):
        Settings(**values)
