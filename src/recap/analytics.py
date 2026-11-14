"""Precisely defined per-run metrics and portable JSON/CSV/Markdown reports."""

import csv
import json
import math
from collections import Counter
from pathlib import Path

from sqlalchemy import select

from recap.database import (
    history,
    inbox,
    provider_payments,
    reconciliations,
    references,
    rejections,
    runs,
    transactions,
)
from recap.provenance import environment, sha256_file, utc_now, write_json


def percentiles(values):
    values = sorted(values)

    def quantile(q):
        if not values:
            return None
        position = (len(values) - 1) * q
        low, high = math.floor(position), math.ceil(position)
        return values[low] + (values[high] - values[low]) * (position - low)

    return {
        "count": len(values),
        "mean": sum(values) / len(values) if values else None,
        "p50": quantile(0.50),
        "p95": quantile(0.95),
        "p99": quantile(0.99),
    }


def summarize(db, run_id):
    with db.transaction() as connection:
        if connection.dialect.name == "postgresql":
            connection.exec_driver_sql("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        run = connection.execute(select(runs).where(runs.c.run_id == run_id)).mappings().one()

        def rows(table):
            return [
                dict(r)
                for r in connection.execute(
                    select(table).where(table.c.run_id == run_id)
                ).mappings()
            ]

        txs, providers, checks, accepted, refs, events = [
            rows(t)
            for t in (transactions, provider_payments, reconciliations, inbox, references, history)
        ]
        rejected_count = sum(
            r["record"]["metadata"].get("run_id") == run_id
            for r in connection.execute(select(rejections)).mappings()
        )
    manifest = run["manifest"]
    recovered_population = [t for t in txs if t["first_unresolved_at"] is not None]
    recovered = [t for t in recovered_population if t["status"] == "SUCCESS"]
    resolved = [t for t in txs if t["resolved_at"] is not None]
    finished = run["finished_at"]
    elapsed = finished - run["started_at"] if finished is not None else None
    duplicate_effects = sum(max(0, p["effect_count"] - 1) for p in providers)
    classifications = Counter(r["classification"] for r in checks)
    attribute_counts = Counter(r["event"]["attribute_hash"] for r in refs)
    expected = manifest["counts"]["published_events"]
    submitted = {r["transaction_id"]: r["submitted_at"] for r in refs}
    return {
        "run_id": run_id,
        "policy": manifest["config"]["policy"],
        "run_status": run["status"],
        "execution": run["execution"],
        "wire_events_rejected": rejected_count,
        "source_rows_read": manifest["counts"]["read"],
        "source_rows_accepted": manifest["counts"]["accepted"],
        "source_rows_rejected": manifest["counts"]["rejected"],
        "expected_wire_events": expected,
        "processed_unique_wire_events": len(accepted),
        "wire_events_unaccounted": expected - len(accepted),
        "logical_transactions": len(txs),
        "state_counts": dict(Counter(t["status"] for t in txs)),
        "duplicate_submissions_detected": sum(t["duplicate_submissions"] for t in txs),
        "duplicate_provider_effects": duplicate_effects,
        "provider_submit_calls": sum(p["submit_calls"] for p in providers),
        "provider_status_checks": sum(p["check_calls"] for p in providers),
        "provider_effects": sum(p["effect_count"] for p in providers),
        "provider_requests_per_transaction": sum(
            p["submit_calls"] + p["check_calls"] for p in providers
        )
        / len(txs)
        if txs
        else None,
        "recovery_attempts": sum(max(0, t["attempts"] - 1) for t in txs),
        "ever_unresolved": len(recovered_population),
        "recovered_successfully": len(recovered),
        "recovery_success_rate": len(recovered) / len(recovered_population)
        if recovered_population
        else None,
        "unresolved_transactions": sum(t["status"] == "UNRESOLVED" for t in txs),
        "resolution_latency_seconds": percentiles(
            [t["resolved_at"] - t["created_at"] for t in resolved]
        ),
        "pipeline_latency_seconds": percentiles(
            [
                t["resolved_at"] - submitted[t["transaction_id"]]
                for t in resolved
                if submitted.get(t["transaction_id"]) is not None
            ]
        ),
        "recovery_time_seconds": percentiles(
            [
                t["resolved_at"] - t["first_unresolved_at"]
                for t in recovered_population
                if t["resolved_at"] is not None
            ]
        ),
        "unresolved_dwell_seconds": percentiles(
            [
                (t["resolved_at"] or finished or t["updated_at"]) - t["first_unresolved_at"]
                for t in recovered_population
            ]
        ),
        "run_elapsed_seconds": elapsed,
        "accepted_events_per_second": len(accepted) / elapsed if elapsed and elapsed > 0 else None,
        "reconciliation_counts": dict(classifications),
        "reconciliation_match_rate": classifications["match"] / len(checks) if checks else None,
        "balance_inconsistent_rows": sum(r["details"]["balance_inconsistent"] for r in checks),
        "attribute_key_collision_groups": sum(n > 1 for n in attribute_counts.values()),
        "audit_records": len(events),
        "observed_scenarios": manifest["observed_scenarios"],
    }


def write_report(db, run_id, output_dir):
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    metrics = summarize(db, run_id)
    write_json(output / "metrics.json", metrics)
    with (output / "metrics.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["metric", "value"])
        for key, value in metrics.items():
            writer.writerow([key, json.dumps(value) if isinstance(value, (dict, list)) else value])
    with db.engine.connect() as connection:
        run_manifest = connection.execute(
            select(runs.c.manifest).where(runs.c.run_id == run_id)
        ).scalar_one()
        with (output / "reconciliation.jsonl").open("w", encoding="utf-8") as handle:
            for row in connection.execute(
                select(reconciliations)
                .where(reconciliations.c.run_id == run_id)
                .order_by(reconciliations.c.transaction_id)
            ).mappings():
                handle.write(json.dumps(dict(row), sort_keys=True) + "\n")
        query = (
            select(transactions, references.c.event)
            .join(
                references,
                (references.c.run_id == transactions.c.run_id)
                & (references.c.transaction_id == transactions.c.transaction_id),
            )
            .where(transactions.c.run_id == run_id)
            .order_by(transactions.c.transaction_id)
        )
        with (output / "transactions.jsonl").open("w", encoding="utf-8") as handle:
            for row in connection.execute(query).mappings():
                handle.write(json.dumps(dict(row), sort_keys=True) + "\n")
        with (output / "audit.jsonl").open("w", encoding="utf-8") as handle:
            for row in connection.execute(
                select(history)
                .where(history.c.run_id == run_id)
                .order_by(history.c.timestamp, history.c.event_id)
            ).mappings():
                handle.write(json.dumps(dict(row), sort_keys=True) + "\n")
    lines = [
        "# RECAP run report",
        "",
        f"Run: `{run_id}` | Policy: `{metrics['policy']}`",
        "",
        "Academic simulation. Source quality findings are separate from provider outcome agreement.",
        "",
        "| Metric | Value |",
        "|---|---|",
    ]
    for key, value in metrics.items():
        lines.append(f"| {key} | {json.dumps(value, sort_keys=True)} |")
    lines += [
        "",
        "Latency starts at durable consumer acceptance; source timestamps are synthetic.",
        "Throughput covers run registration through worker completion; see README.md for benchmark scope.",
        "A null metric has no defined denominator or no measured completion interval.",
    ]
    (output / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    artifact_names = (
        "metrics.json",
        "metrics.csv",
        "reconciliation.jsonl",
        "report.md",
        "transactions.jsonl",
        "audit.jsonl",
    )
    provenance = {
        "prepared_manifest": run_manifest,
        "reported_at": utc_now(),
        "report_environment": environment(),
        "artifacts": {name: sha256_file(output / name) for name in artifact_names},
        "reconciliation_summary": metrics["reconciliation_counts"],
    }
    write_json(output / "provenance.json", provenance)
    return metrics
