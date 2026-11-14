"""Bounded-memory CSV normalization and deterministic event preparation (M1)."""

import csv
import json
import random
from collections import Counter
from datetime import datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from pathlib import Path
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from recap.config import Settings
from recap.provenance import environment, sha256_file, stable_hash, utc_now, write_json
from recap.validation import business_payload, validate_event

REQUIRED_COLUMNS = {
    "step",
    "type",
    "amount",
    "nameOrig",
    "nameDest",
    "oldbalanceOrg",
    "newbalanceOrig",
    "oldbalanceDest",
    "newbalanceDest",
}
TYPES = {"CASH_IN", "CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER"}


def minor_units(value):
    try:
        number = Decimal(str(value))
        if not number.is_finite() or number < 0 or number > Decimal("92233720368547758.07"):
            raise ValueError("Money must be finite, nonnegative and fit signed 64-bit minor units")
        return int((number * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
    except (InvalidOperation, TypeError) as exc:
        raise ValueError("Malformed monetary value") from exc


def make_event(row, row_number, dataset_sha256, run_id, settings: Settings):
    if row.get("type") not in TYPES:
        raise ValueError("Unsupported transaction type")
    step = int(row["step"])
    if step < 0 or step > 1_000_000 or str(step) != str(row["step"]).strip():
        raise ValueError("step must be a nonnegative integer <= 1000000")
    parties = {k: str(row[k] or "").strip() for k in ("nameOrig", "nameDest")}
    if not all(parties.values()):
        raise ValueError("Missing originator/destination")
    identity = stable_hash(["paysim-row-v1", dataset_sha256, row_number])
    rng = random.Random(stable_hash([settings.seed, identity, "timestamp"]))
    timestamp = datetime.fromisoformat(settings.epoch.replace("Z", "+00:00")) + timedelta(
        seconds=step * 3600 + rng.randrange(3600)
    )
    scenario_rng = random.Random(stable_hash([settings.seed, identity, "scenario"]))
    scenarios = sorted(settings.failure_mix)
    scenario = scenario_rng.choices(
        scenarios, weights=[settings.failure_mix[s] for s in scenarios], k=1
    )[0]
    amount = minor_units(row["amount"])
    event = {
        "schema_version": "1.0",
        "run_id": str(UUID(run_id)),
        "event_id": str(uuid5(NAMESPACE_URL, run_id + identity + "request")),
        "event_type": "request",
        "transaction_id": str(uuid5(NAMESPACE_URL, identity)),
        "idempotency_key": identity,
        "attribute_hash": stable_hash(
            [
                parties["nameOrig"],
                parties["nameDest"],
                amount,
                int(timestamp.timestamp()) // settings.time_bucket_seconds,
            ]
        ),
        "source": {"dataset_sha256": dataset_sha256, "row": row_number, "step": step},
        "transaction_type": row["type"],
        "originator": parties["nameOrig"],
        "destination": parties["nameDest"],
        "amount_minor": amount,
        "currency": settings.currency,
        "balances": {
            name: minor_units(row[column])
            for name, column in {
                "origin_before": "oldbalanceOrg",
                "origin_after": "newbalanceOrig",
                "destination_before": "oldbalanceDest",
                "destination_after": "newbalanceDest",
            }.items()
        },
        "timestamp": timestamp.isoformat().replace("+00:00", "Z"),
        "attempt_number": 1,
        "scenario": scenario,
    }
    event["payload_hash"] = stable_hash(business_payload(event))
    return validate_event(event)


def prepare(csv_path, output_dir, settings: Settings, limit=1000, start_row=1, run_id=None):
    if limit <= 0 or start_row < 1:
        raise ValueError("limit and start_row must be positive")
    run_id = str(UUID(run_id)) if run_id else str(uuid4())
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    if any(
        (output / name).exists()
        for name in ("manifest.json", "events.jsonl", "rejected_rows.jsonl")
    ):
        raise ValueError("Preparation output already exists; use a new directory")
    checksum = sha256_file(csv_path)
    catalog_path = Path(csv_path).with_suffix(".cases.json")
    catalog = None
    if catalog_path.is_file():
        catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
        if catalog.get("sha256") != checksum:
            raise ValueError("Input case catalog checksum does not match the CSV")
    counts = Counter(read=0, accepted=0, rejected=0, published_events=0)
    scenarios = Counter()
    manifest = {
        "run_id": run_id,
        "started_at": utc_now(),
        "dataset": {"name": Path(csv_path).name, "sha256": checksum},
        "slice": {"start_row": start_row, "limit": limit},
        "config": settings.public_dict(),
        "environment": environment(),
    }
    if catalog is not None:
        # Labels are provenance only. Raw values still determine acceptance.
        manifest["input_catalog"] = catalog
    with (
        open(csv_path, newline="", encoding="utf-8-sig") as source,
        (output / "events.jsonl").open("w", encoding="utf-8") as events,
        (output / "rejected_rows.jsonl").open("w", encoding="utf-8") as rejects,
    ):
        reader = csv.DictReader(source)
        missing = REQUIRED_COLUMNS - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"Missing CSV columns: {sorted(missing)}")
        for index, row in enumerate(reader, 1):
            if index < start_row:
                continue
            if counts["read"] >= limit:
                break
            counts["read"] += 1
            try:
                if None in row or any(value is None for value in row.values()):
                    raise ValueError("CSV row has incorrect field count")
                event = make_event(row, index, checksum, run_id, settings)
            except (ValueError, TypeError, OverflowError) as exc:
                rejects.write(json.dumps({"row": index, "raw": row, "reason": str(exc)}) + "\n")
                counts["rejected"] += 1
                continue
            counts["accepted"] += 1
            scenarios[event["scenario"]] += 1
            events.write(json.dumps(event, sort_keys=True) + "\n")
            counts["published_events"] += 1
            if event["scenario"] == "duplicate_submission":
                duplicate = {
                    **event,
                    "event_type": "duplicate_submission",
                    "event_id": str(uuid5(UUID(event["event_id"]), "duplicate")),
                }
                events.write(json.dumps(duplicate, sort_keys=True) + "\n")
                counts["published_events"] += 1
    manifest.update(counts=dict(counts), observed_scenarios=dict(scenarios), prepared_at=utc_now())
    manifest["artifacts"] = {
        name: sha256_file(output / name) for name in ("events.jsonl", "rejected_rows.jsonl")
    }
    write_json(output / "manifest.json", manifest)
    return manifest


def read_events(path):
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            yield validate_event(json.loads(line))
