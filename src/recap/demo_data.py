"""Deterministic synthetic PaySim-format data; never an extract of real PaySim."""

import csv
import random
from pathlib import Path

from recap.provenance import sha256_file, write_json

FIELDS = (
    "step",
    "type",
    "amount",
    "nameOrig",
    "oldbalanceOrg",
    "newbalanceOrig",
    "nameDest",
    "oldbalanceDest",
    "newbalanceDest",
    "isFraud",
    "isFlaggedFraud",
)
BASE = dict(
    zip(
        FIELDS,
        (
            "1",
            "TRANSFER",
            "10.00",
            "C_EDGE_FROM",
            "100.00",
            "90.00",
            "C_EDGE_TO",
            "50.00",
            "60.00",
            "0",
            "0",
        ),
    )
)
# Labels document expectations only; ingestion must discover issues from raw field values.
EDGE_CASES = (
    ("negative_amount", {"amount": "-1"}, "rejected", "Money must be finite"),
    ("nonnumeric_amount", {"amount": "bad"}, "rejected", "Malformed monetary value"),
    ("nan_amount", {"amount": "NaN"}, "rejected", "Money must be finite"),
    ("infinite_amount", {"amount": "Infinity"}, "rejected", "Money must be finite"),
    ("amount_overflow", {"amount": "92233720368547758.08"}, "rejected", "Money must be finite"),
    ("missing_sender", {"nameOrig": "   "}, "rejected", "Missing originator/destination"),
    ("missing_receiver", {"nameDest": ""}, "rejected", "Missing originator/destination"),
    ("unsupported_type", {"type": "REFUND"}, "rejected", "Unsupported transaction type"),
    ("negative_step", {"step": "-1"}, "rejected", "step must be"),
    ("negative_balance", {"oldbalanceOrg": "-1"}, "rejected", "Money must be finite"),
    (
        "zero_amount",
        {"amount": "0", "newbalanceOrig": "100", "newbalanceDest": "50"},
        "accepted",
        "Zero is allowed by the current contract",
    ),
    (
        "fractional_cent",
        {"amount": "2.345", "newbalanceOrig": "97.65", "newbalanceDest": "52.35"},
        "accepted",
        "Rounded half-up to 235 minor units",
    ),
    (
        "balance_mismatch",
        {"newbalanceOrig": "95"},
        "accepted",
        "Reconciliation flags origin_balance_inconsistent",
    ),
    (
        "merchant_balance_omitted",
        {"type": "PAYMENT", "nameDest": "M_EDGE", "oldbalanceDest": "0", "newbalanceDest": "0"},
        "accepted",
        "Reconciliation marks destination_balance_unavailable",
    ),
    (
        "cash_in_direction",
        {"type": "CASH_IN", "newbalanceOrig": "110", "newbalanceDest": "40"},
        "accepted",
        "CASH_IN credits sender and debits destination",
    ),
    ("identical_source_row_a", {}, "accepted", "Separate source row has its own identity"),
    ("identical_source_row_b", {}, "accepted", "Same fields do not prove the same request"),
)


def money(cents):
    return f"{cents // 100}.{cents % 100:02d}"


def normal_row(index, rng):
    kind = ("TRANSFER", "CASH_OUT", "CASH_IN", "PAYMENT", "DEBIT")[index % 5]
    amount = rng.randint(1, 500_000)
    origin = amount + rng.randint(0, 1_000_000)
    destination = amount + rng.randint(0, 1_000_000)
    cash_in, merchant = kind == "CASH_IN", kind == "PAYMENT"
    return {
        "step": str(index % 744 + 1),
        "type": kind,
        "amount": money(amount),
        "nameOrig": f"C_GEN_{index:08d}",
        "oldbalanceOrg": money(origin),
        "newbalanceOrig": money(origin + amount if cash_in else origin - amount),
        "nameDest": f"{'M' if merchant else 'C'}_DEST_{index:08d}",
        "oldbalanceDest": "0.00" if merchant else money(destination),
        "newbalanceDest": "0.00"
        if merchant
        else money(destination - amount if cash_in else destination + amount),
        "isFraud": "0",
        "isFlaggedFraud": "0",
    }


def generate_fixture(output, rows=1000, seed=25, edge_cases=False):
    if type(rows) is not int or rows < 0 or (rows == 0 and not edge_cases):
        raise ValueError("rows must be positive, or zero with --edge-cases")
    if type(seed) is not int or seed < 0:
        raise ValueError("seed must be a nonnegative integer")
    output = Path(output)
    catalog = output.with_suffix(".cases.json")
    if output.suffix.lower() != ".csv":
        raise ValueError("Output must have a .csv extension")
    if output.exists() or catalog.exists():
        raise ValueError("Generated output already exists; choose a new filename")
    output.parent.mkdir(parents=True, exist_ok=True)
    rng, cases = random.Random(seed), []
    with output.open("x", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS, lineterminator="\n")
        writer.writeheader()
        for index in range(rows):
            writer.writerow(normal_row(index, rng))
        if edge_cases:
            for offset, (name, overrides, expectation, explanation) in enumerate(EDGE_CASES, 1):
                writer.writerow({**BASE, **overrides})
                cases.append(
                    {
                        "row": rows + offset,
                        "case": name,
                        "expected_validation": expectation,
                        "explanation": explanation,
                    }
                )
    rejected = sum(case["expected_validation"] == "rejected" for case in cases)
    result = {
        "source": "synthetic demonstration data; not real PaySim",
        "csv": str(output),
        "cases_file": str(catalog),
        "seed": seed,
        "normal_rows": rows,
        "edge_rows": len(cases),
        "total_rows": rows + len(cases),
        "expected_accepted": rows + len(cases) - rejected,
        "expected_rejected": rejected,
        "sha256": sha256_file(output),
        "cases": cases,
    }
    write_json(catalog, result)
    return result
