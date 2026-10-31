"""Kafka delivery is at-least-once; durable inbox acceptance precedes commits."""

import json
import logging
import time
from uuid import uuid4

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from sqlalchemy import func, select, update

from recap.database import key_filter, references, register_prepared, rejections, runs
from recap.ingestion import read_events
from recap.service import TransactionService
from recap.validation import validate_event

log = logging.getLogger(__name__)


class KafkaPublisher:
    def __init__(self, servers, client=None):
        self.client = client or Producer(
            {
                "bootstrap.servers": servers,
                "enable.idempotence": True,
                "acks": "all",
                "delivery.timeout.ms": 15000,
                "queue.buffering.max.messages": 2000,
            }
        )
        self.errors = []
        self.pending = 0

    def _delivered(self, error, message):
        if error:
            self.errors.append(str(error))

    def send(self, topic, key, value):
        encoded = json.dumps(value, sort_keys=True, allow_nan=False).encode()
        deadline = time.monotonic() + 20
        while True:
            try:
                self.client.produce(
                    topic, key=key.encode(), value=encoded, on_delivery=self._delivered
                )
                break
            except BufferError:
                if time.monotonic() >= deadline:
                    raise TimeoutError("Kafka producer queue remained full")
                self.client.poll(0.1)
        self.client.poll(0)
        self.pending += 1
        if self.pending >= 500:
            self.flush()

    def flush(self):
        outstanding = self.client.flush(20)
        self.pending = 0
        if outstanding or self.errors:
            errors, self.errors = self.errors, []
            raise RuntimeError(
                f"Kafka publication failed: outstanding={outstanding}, errors={errors[:5]}"
            )


def create_topics(servers, settings):
    admin = AdminClient({"bootstrap.servers": servers})
    futures = admin.create_topics(
        [
            NewTopic(name, num_partitions=3, replication_factor=1)
            for name in (settings.kafka_topic, settings.dead_letter_topic)
        ]
    )
    for name, future in futures.items():
        try:
            future.result(20)
        except KafkaException as exc:
            from confluent_kafka import KafkaError

            if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise
        log.info("topic_ready name=%s", name)


def end_offsets(servers, topic):
    """Snapshot partition ends so a bounded run does not replay older experiments."""
    client = Consumer(
        {
            "bootstrap.servers": servers,
            "group.id": "recap-snapshot-" + uuid4().hex,
            "enable.auto.commit": False,
        }
    )
    try:
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            try:
                metadata = client.list_topics(topic, timeout=10).topics[topic]
                if metadata.error:
                    raise KafkaException(metadata.error)
                return {
                    partition: client.get_watermark_offsets(
                        TopicPartition(topic, partition), timeout=10
                    )[1]
                    for partition in metadata.partitions
                }
            except KafkaException as exc:
                if not exc.args[0].retriable() and exc.args[0].code() not in (
                    KafkaError.UNKNOWN_TOPIC_OR_PART,
                    KafkaError.LEADER_NOT_AVAILABLE,
                    KafkaError.NOT_LEADER_FOR_PARTITION,
                ):
                    raise
                # Controller acknowledgement can precede broker metadata propagation.
                time.sleep(0.1)
        raise TimeoutError("Kafka partition metadata did not become ready")
    finally:
        client.close()


def publish_prepared(db, directory, settings, servers, publisher=None):
    from pathlib import Path

    manifest = register_prepared(db, directory)
    publisher = publisher or KafkaPublisher(servers)
    with db.transaction() as connection:
        connection.execute(
            update(runs)
            .where(runs.c.run_id == manifest["run_id"])
            .values(execution={"transport": "kafka", "database": db.engine.dialect.name})
        )
    start, count = time.monotonic(), 0
    for value in read_events(Path(directory) / "events.jsonl"):
        validate_event(value)
        if settings.input_rate:
            time.sleep(max(0, start + count / settings.input_rate - time.monotonic()))
        with db.transaction() as connection:
            connection.execute(
                update(references)
                .where(key_filter(references, value["run_id"], value["transaction_id"]))
                .values(submitted_at=func.coalesce(references.c.submitted_at, time.time()))
            )
        publisher.send(settings.kafka_topic, value["transaction_id"], value)
        count += 1
    publisher.flush()
    log.info("published run_id=%s events=%d", manifest["run_id"], count)
    return count


def flush_dead_letters(db, publisher, topic, limit=500):
    with db.engine.connect() as connection:
        pending = list(
            connection.execute(
                select(rejections)
                .where(rejections.c.published_at.is_(None))
                .order_by(rejections.c.created_at)
                .limit(limit)
            ).mappings()
        )
    for row in pending:
        publisher.send(topic, row["rejection_id"], row["record"])
    if pending:
        publisher.flush()
        # A crash before this mark can republish; rejection_id is stable.
        with db.transaction() as connection:
            connection.execute(
                update(rejections)
                .where(rejections.c.rejection_id.in_([r["rejection_id"] for r in pending]))
                .values(published_at=time.time())
            )
    return len(pending)


def consume(
    db,
    settings,
    servers,
    max_messages=0,
    idle_timeout=10,
    client=None,
    publisher=None,
    stop=None,
    start_offsets=None,
):
    if max_messages < 0 or idle_timeout <= 0:
        raise ValueError("max_messages must be nonnegative and idle_timeout positive")
    client = client or Consumer(
        {
            "bootstrap.servers": servers,
            "group.id": settings.consumer_group,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": "earliest",
            "max.poll.interval.ms": 300000,
        }
    )
    publisher = publisher or KafkaPublisher(servers)
    service = TransactionService(db)
    count, last_message = 0, time.monotonic()
    if start_offsets is None:
        client.subscribe([settings.kafka_topic])
    else:

        def assign_from_snapshot(consumer, partitions):
            committed = consumer.committed(partitions, timeout=15)
            for partition, previous in zip(partitions, committed):
                partition.offset = (
                    previous.offset if previous.offset >= 0 else start_offsets[partition.partition]
                )
            consumer.assign(partitions)

        client.subscribe([settings.kafka_topic], on_assign=assign_from_snapshot)
    try:
        flush_dead_letters(db, publisher, settings.dead_letter_topic)
        while not stop or not stop():
            message = client.poll(0.25)
            if message is None:
                if time.monotonic() - last_message >= idle_timeout:
                    break
                continue
            if message.error():
                raise KafkaException(message.error())
            metadata = {
                "topic": message.topic(),
                "partition": message.partition(),
                "offset": message.offset(),
                "key": message.key().decode("utf-8", errors="replace") if message.key() else None,
            }
            outcome = service.process_raw(message.value() or b"", metadata)
            # PostgreSQL has already committed an inbox record or a durable rejection.
            client.commit(message=message, asynchronous=False)
            count += 1
            last_message = time.monotonic()
            if outcome == "rejected":
                flush_dead_letters(db, publisher, settings.dead_letter_topic)
            if max_messages and count >= max_messages:
                break
    finally:
        client.close()
    log.info("consumed messages=%d", count)
    return count
