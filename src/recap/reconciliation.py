"""M4: compare provider truth and internal state; retain source quality separately."""

import time
from collections import Counter

from sqlalchemy import delete, select

from recap.database import provider_payments, reconciliations, references, runs, transactions


def balance_findings(event):
    balances, amount, kind = event["balances"], event["amount_minor"], event["transaction_type"]
    # CASH_IN credits the originator; other PaySim types debit the originator.
    expected_origin = (
        balances["origin_before"] + amount
        if kind == "CASH_IN"
        else balances["origin_before"] - amount
    )
    findings = []
    if abs(expected_origin - balances["origin_after"]) > 1:
        findings.append("origin_balance_inconsistent")
    # Merchant balances are not recorded by PaySim. Do not treat zeros as real balances.
    if event["destination"].startswith("M"):
        findings.append("destination_balance_unavailable")
    else:
        expected_destination = (
            balances["destination_before"] - amount
            if kind == "CASH_IN"
            else balances["destination_before"] + amount
        )
        if abs(expected_destination - balances["destination_after"]) > 1:
            findings.append("destination_balance_inconsistent")
    return findings


def classify(internal, external, reference):
    findings = balance_findings(reference) if reference else ["source_reference_missing"]
    if external and external["effect_count"] > 1:
        classification = "duplicate_processing"
    elif internal is None:
        classification = "internal_missing"
    elif external is None or external["outcome"] == "NOT_FOUND":
        classification = "external_missing"
    elif internal["status"] in ("INITIATED", "PROCESSING", "UNRESOLVED"):
        classification = "unresolved"
    elif internal["status"] != external["outcome"]:
        classification = "outcome_mismatch"
    else:
        classification = "match"
    return {
        "classification": classification,
        "internal_outcome": internal["status"] if internal else None,
        "reference_outcome": external["outcome"] if external else None,
        "details": {
            "data_quality": findings,
            "balance_inconsistent": any("inconsistent" in f for f in findings),
            "explanation": "Provider outcome compared independently; balance findings describe source quality, not fraud or processing failure.",
        },
    }


def reconcile(db, run_id, clock=time.time):
    # One snapshot prevents a mixed-time comparison while workers are updating.
    with db.transaction() as connection:
        if connection.dialect.name == "postgresql":
            connection.exec_driver_sql("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        if not connection.execute(select(runs.c.run_id).where(runs.c.run_id == run_id)).first():
            raise ValueError("Unknown run_id")
        refs = {
            r["transaction_id"]: r["event"]
            for r in connection.execute(
                select(references).where(references.c.run_id == run_id)
            ).mappings()
        }
        internal = {
            r["transaction_id"]: dict(r)
            for r in connection.execute(
                select(transactions).where(transactions.c.run_id == run_id)
            ).mappings()
        }
        external = {
            r["transaction_id"]: dict(r)
            for r in connection.execute(
                select(provider_payments).where(provider_payments.c.run_id == run_id)
            ).mappings()
        }
        results = []
        for transaction_id in sorted(refs.keys() | internal.keys() | external.keys()):
            result = classify(
                internal.get(transaction_id), external.get(transaction_id), refs.get(transaction_id)
            )
            results.append(
                {
                    "run_id": run_id,
                    "transaction_id": transaction_id,
                    "checked_at": clock(),
                    **result,
                }
            )
        connection.execute(delete(reconciliations).where(reconciliations.c.run_id == run_id))
        for start in range(0, len(results), 500):
            connection.execute(reconciliations.insert(), results[start : start + 500])
    return {
        "total": len(results),
        "classifications": dict(Counter(r["classification"] for r in results)),
        "balance_inconsistent": sum(r["details"]["balance_inconsistent"] for r in results),
    }
