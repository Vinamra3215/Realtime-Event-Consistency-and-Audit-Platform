import csv
import json
import os
import time
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import pytest
from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient
from sqlalchemy import func, select

from recap.analytics import summarize
from recap.config import POLICIES, SCENARIOS, Settings, kafka_servers
from recap.database import inbox, rejections, transactions
from recap.runner import simulate
from recap.streaming import KafkaPublisher, consume, create_topics

pytestmark = pytest.mark.end_to_end


@pytest.fixture
def kafka_settings():
    if os.environ.get("RECAP_INTEGRATION") != "1":
        pytest.skip("Set RECAP_INTEGRATION=1 for live Kafka/PostgreSQL tests")
    suffix = uuid4().hex
    settings = Settings(
        kafka_topic="recap.test." + suffix,
        dead_letter_topic="recap.dlq." + suffix,
        consumer_group="recap.test." + suffix,
        base_delay=0.002,
        max_delay=0.01,
        delayed_seconds=0.005,
        failure_mix={s: 1 / len(SCENARIOS) for s in SCENARIOS},
    )
    yield settings
    admin = AdminClient({"bootstrap.servers": kafka_servers()})
    futures = admin.delete_topics(
        [settings.kafka_topic, settings.dead_letter_topic], operation_timeout=10
    )
    for future in futures.values():
        future.result(15)


def generate_fixture(path):
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "step",
                "type",
                "amount",
                "nameOrig",
                "oldbalanceOrg",
                "newbalanceOrig",
                "nameDest",
                "oldbalanceDest",
                "newbalanceDest",
            ]
        )
        for i in range(128):
            writer.writerow([1 + i // 20, "TRANSFER", 10, f"C{i}", 20, 10, f"D{i}", 0, 10])
        writer.writerow([1, "TRANSFER", -1, "bad", 20, 10, "D", 0, 10])


@pytest.mark.parametrize("policy", POLICIES)
def test_live_pipeline_all_policies(postgres_db, kafka_settings, tmp_path, policy):
    source = tmp_path / "workload.csv"
    generate_fixture(source)
    settings = replace(kafka_settings, policy=policy)
    metrics = simulate(
        postgres_db,
        source,
        tmp_path / "run",
        settings,
        limit=200,
        transport="kafka",
        servers=kafka_servers(),
        timeout=120,
    )
    assert metrics["logical_transactions"] == 128
    assert metrics["source_rows_rejected"] == 1
    assert metrics["wire_events_unaccounted"] == 0
    assert metrics["duplicate_provider_effects"] == 0
    assert set(metrics["observed_scenarios"]) == set(SCENARIOS)
    assert metrics["unresolved_transactions"] == metrics["observed_scenarios"]["always_unknown"]
    assert metrics["state_counts"]["FAILED"] == metrics["observed_scenarios"]["permanent_failure"]
    assert (
        metrics["duplicate_submissions_detected"]
        == metrics["observed_scenarios"]["duplicate_submission"]
    )
    assert metrics["pipeline_latency_seconds"]["count"] > 0
    # Replay the real topic with a new group; inbox suppresses already accepted events.
    replay = replace(settings, consumer_group="replay." + uuid4().hex)
    consume(
        postgres_db,
        replay,
        kafka_servers(),
        max_messages=metrics["expected_wire_events"],
        idle_timeout=20,
    )
    after = summarize(postgres_db, metrics["run_id"])
    assert after == metrics
    with postgres_db.engine.connect() as connection:
        assert (
            connection.execute(select(func.count()).select_from(transactions)).scalar_one() == 128
        )
        assert (
            connection.execute(select(func.count()).select_from(inbox)).scalar_one()
            == metrics["expected_wire_events"]
        )
    # Broker ordering is per-key: original request precedes its duplicate.
    probe = Consumer(
        {
            "bootstrap.servers": kafka_servers(),
            "group.id": "probe." + uuid4().hex,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    probe.subscribe([settings.kafka_topic])
    seen, count, deadline = {}, 0, time.monotonic() + 30
    try:
        while count < metrics["expected_wire_events"] and time.monotonic() < deadline:
            message = probe.poll(0.25)
            if message is None:
                continue
            assert message.error() is None
            value = json.loads(message.value())
            assert message.key().decode() == value["transaction_id"]
            prior = seen.setdefault(value["transaction_id"], [])
            if value["event_type"] == "duplicate_submission":
                assert prior == ["request"]
            prior.append(value["event_type"])
            count += 1
    finally:
        probe.close()
    assert count == metrics["expected_wire_events"]
    # Actual invalid wire event -> durable rejection -> actual Kafka DLQ.
    publisher = KafkaPublisher(kafka_servers())
    publisher.send(settings.kafka_topic, "bad", {"run_id": metrics["run_id"], "amount_minor": -1})
    publisher.flush()
    consume(postgres_db, replay, kafka_servers(), max_messages=1, idle_timeout=20)
    with postgres_db.engine.connect() as connection:
        assert connection.execute(select(func.count()).select_from(rejections)).scalar_one() == 1
    dlq = Consumer(
        {
            "bootstrap.servers": kafka_servers(),
            "group.id": "dlq." + uuid4().hex,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    dlq.subscribe([settings.dead_letter_topic])
    received, deadline = None, time.monotonic() + 30
    try:
        while received is None and time.monotonic() < deadline:
            message = dlq.poll(0.25)
            if message is not None:
                assert message.error() is None
                received = json.loads(message.value())
    finally:
        dlq.close()
    assert received and received["reason"] and received["raw_base64"]


def test_bounded_run_skips_old_topic_records(postgres_db, kafka_settings, tmp_path):
    settings = replace(kafka_settings, failure_mix={"clean": 1})
    create_topics(kafka_servers(), settings)
    publisher = KafkaPublisher(kafka_servers())
    publisher.send(settings.kafka_topic, "old", {"old_invalid_record": True})
    publisher.flush()
    metrics = simulate(
        postgres_db,
        Path(__file__).parents[1] / "fixtures" / "paysim_sample.csv",
        tmp_path / "bounded",
        settings,
        limit=1,
        transport="kafka",
        servers=kafka_servers(),
    )
    assert metrics["logical_transactions"] == 1
    assert metrics["state_counts"] == {"SUCCESS": 1}
    with postgres_db.engine.connect() as connection:
        assert connection.execute(select(func.count()).select_from(rejections)).scalar_one() == 0
