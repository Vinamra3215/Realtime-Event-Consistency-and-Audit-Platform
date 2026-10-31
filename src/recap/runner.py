"""Bounded demonstrations and policy comparison; no invented performance numbers."""

import csv
import time
import tracemalloc
from dataclasses import replace
from pathlib import Path

from sqlalchemy import func, select, text, update

from recap.analytics import write_report
from recap.config import POLICIES
from recap.database import inbox, jobs, key_filter, references, register_prepared, runs
from recap.ingestion import prepare, read_events
from recap.provenance import environment, write_json
from recap.reconciliation import reconcile
from recap.recovery import RecoveryWorker
from recap.service import TransactionService


def finish_run(db, run_id, clock=time.time):
    with db.transaction() as connection:
        run = (
            connection.execute(select(runs).where(runs.c.run_id == run_id).with_for_update())
            .mappings()
            .one()
        )
        actual = connection.execute(
            select(func.count()).select_from(inbox).where(inbox.c.run_id == run_id)
        ).scalar_one()
        active = connection.execute(
            select(func.count())
            .select_from(jobs)
            .where((jobs.c.run_id == run_id) & jobs.c.status.in_(["READY", "LEASED"]))
        ).scalar_one()
        if actual != run["manifest"]["counts"]["published_events"] or active:
            raise ValueError(
                f"Run not complete: accepted events={actual}, expected={run['manifest']['counts']['published_events']}, active jobs={active}"
            )
        if run["finished_at"] is None:
            connection.execute(
                update(runs)
                .where(runs.c.run_id == run_id)
                .values(status="complete", finished_at=clock())
            )


def simulate(
    db,
    csv_path,
    output,
    settings,
    limit=1000,
    start_row=1,
    transport="direct",
    servers="localhost:9092",
    timeout=120,
    clock=time.time,
    sleep=time.sleep,
):
    output = Path(output)
    prepared = output / "prepared"
    manifest = prepare(csv_path, prepared, settings, limit=limit, start_row=start_row)
    run_id = manifest["run_id"]
    db.migrate()
    register_prepared(db, prepared, clock)
    with db.transaction() as connection:
        connection.execute(
            update(runs)
            .where(runs.c.run_id == run_id)
            .values(
                status="processing",
                execution={"transport": transport, "database": db.engine.dialect.name},
            )
        )
    if transport == "kafka":
        from recap.streaming import consume, create_topics, end_offsets, publish_prepared

        isolated = replace(settings, consumer_group=f"recap-run-{run_id}")
        create_topics(servers, settings)
        starting_offsets = end_offsets(servers, settings.kafka_topic)
        publish_prepared(db, prepared, settings, servers)
        # Initial assignment skips prior runs; rebalance resumes committed offsets.
        deadline = time.monotonic() + timeout
        while True:
            with db.engine.connect() as connection:
                accepted = connection.execute(
                    select(func.count()).select_from(inbox).where(inbox.c.run_id == run_id)
                ).scalar_one()
            if accepted >= manifest["counts"]["published_events"]:
                break
            if time.monotonic() > deadline:
                raise TimeoutError("Kafka ingestion did not complete within timeout")
            consume(
                db,
                isolated,
                servers,
                max_messages=min(1000, manifest["counts"]["published_events"] - accepted),
                idle_timeout=min(5, timeout),
                start_offsets=starting_offsets,
            )
    elif transport == "direct":
        service = TransactionService(db, clock)
        beginning = clock()
        for index, value in enumerate(read_events(prepared / "events.jsonl")):
            if settings.input_rate:
                sleep(max(0, beginning + index / settings.input_rate - clock()))
            with db.transaction() as connection:
                connection.execute(
                    update(references)
                    .where(key_filter(references, run_id, value["transaction_id"]))
                    .values(submitted_at=func.coalesce(references.c.submitted_at, clock()))
                )
            service.accept(value)
    else:
        raise ValueError("Transport must be direct or kafka")
    RecoveryWorker(db, clock, sleep).drain(run_id, timeout)
    finish_run(db, run_id, clock)
    reconcile(db, run_id, clock)
    return write_report(db, run_id, output)


def benchmark(
    db,
    csv_path,
    output,
    settings,
    limit=1000,
    transport="direct",
    servers="localhost:9092",
    policies=POLICIES,
    timeout=300,
):
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    if (output / "benchmark.json").exists():
        raise ValueError("Benchmark output exists; choose a new directory")
    measurements = []
    for policy in policies:
        tracemalloc.start()
        start = time.perf_counter()
        try:
            metrics = simulate(
                db,
                csv_path,
                output / policy,
                replace(settings, policy=policy),
                limit=limit,
                transport=transport,
                servers=servers,
                timeout=timeout,
            )
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        measurements.append(
            {
                "policy": policy,
                "elapsed_wall_seconds_including_preparation_and_reporting": time.perf_counter()
                - start,
                "peak_python_allocated_bytes": peak,
                "metrics": metrics,
            }
        )
    with db.engine.connect() as connection:
        query = "SELECT transaction_id FROM transactions WHERE run_id = :run AND status = 'UNRESOLVED' ORDER BY updated_at LIMIT 100"
        prefix = (
            "EXPLAIN (ANALYZE, BUFFERS) "
            if connection.dialect.name == "postgresql"
            else "EXPLAIN QUERY PLAN "
        )
        plan = [
            list(row)
            for row in connection.execute(
                text(prefix + query), {"run": measurements[-1]["metrics"]["run_id"]}
            )
        ]
    result = {
        "environment": environment(),
        "transport": transport,
        "database": db.engine.dialect.name,
        "config": settings.public_dict(),
        "limit": limit,
        "query_plan": plan,
        "measurements": measurements,
        "scope": "Sequential bounded replay. Python allocations exclude broker/database and native allocations. This is not a production capacity estimate.",
    }
    write_json(output / "benchmark.json", result)
    with (output / "comparison.csv").open("w", newline="", encoding="utf-8") as handle:
        fields = [
            "policy",
            "logical_transactions",
            "provider_submit_calls",
            "provider_status_checks",
            "recovery_success_rate",
            "unresolved_transactions",
            "duplicate_provider_effects",
            "accepted_events_per_second",
        ]
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for measurement in measurements:
            writer.writerow({k: measurement["metrics"][k] for k in fields})
    return result
