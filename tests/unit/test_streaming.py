import json

import pytest
from sqlalchemy import func, select

from recap.config import Settings
from recap.database import rejections
from recap.streaming import KafkaPublisher, consume, flush_dead_letters


class Message:
    def __init__(self, value):
        self.raw = value

    def value(self):
        return self.raw

    def key(self):
        return (
            json.loads(self.raw).get("transaction_id", "bad").encode()
            if self.raw.startswith(b"{")
            else b"bad"
        )

    def topic(self):
        return "requests"

    def partition(self):
        return 0

    def offset(self):
        return 1

    def error(self):
        return None


class FakeConsumer:
    def __init__(self, values):
        self.values = iter(values)
        self.commits, self.closed = [], False

    def subscribe(self, topics):
        pass

    def poll(self, timeout):
        return next(self.values, None)

    def commit(self, **kwargs):
        self.commits.append(kwargs)

    def close(self):
        self.closed = True


class FakePublisher:
    def __init__(self, fail=False):
        self.sent, self.fail = [], fail

    def send(self, *args):
        self.sent.append(args)

    def flush(self):
        if self.fail:
            raise ConnectionError("injected Kafka failure")


def test_consumer_commit_after_accept(db, prepared):
    value = prepared[1][0]
    client = FakeConsumer([Message(json.dumps(value).encode())])
    assert consume(db, Settings(), "unused", 1, client=client, publisher=FakePublisher()) == 1
    assert len(client.commits) == 1
    assert client.commits[0]["asynchronous"] is False
    assert client.closed


def test_db_failure_does_not_commit(db, prepared, monkeypatch):
    def fail(*args, **kwargs):
        raise ConnectionError("database unavailable")

    monkeypatch.setattr("recap.streaming.TransactionService.process_raw", fail)
    client = FakeConsumer([Message(json.dumps(prepared[1][0]).encode())])
    with pytest.raises(ConnectionError):
        consume(db, Settings(), "unused", 1, client=client, publisher=FakePublisher())
    assert not client.commits
    assert client.closed


def test_reject_outbox_survives_kafka_failure(db):
    client = FakeConsumer([Message(b"not json")])
    with pytest.raises(ConnectionError):
        consume(db, Settings(), "unused", 1, client=client, publisher=FakePublisher(fail=True))
    assert len(client.commits) == 1  # Rejection is durable, publication is retryable.
    with db.engine.connect() as connection:
        assert (
            connection.execute(
                select(func.count())
                .select_from(rejections)
                .where(rejections.c.published_at.is_(None))
            ).scalar_one()
            == 1
        )
    publisher = FakePublisher()
    assert flush_dead_letters(db, publisher, "dlq") == 1
    assert publisher.sent[0][2]["raw_base64"]
    assert flush_dead_letters(db, publisher, "dlq") == 0


def test_producer_delivery_failure():
    class Client:
        def produce(self, *args, **kwargs):
            kwargs["on_delivery"]("broker error", None)

        def poll(self, timeout):
            pass

        def flush(self, timeout):
            return 0

    publisher = KafkaPublisher("unused", client=Client())
    publisher.send("topic", "key", {"hello": "world"})
    with pytest.raises(RuntimeError, match="broker error"):
        publisher.flush()


def test_offset_snapshot_retries_new_topic_metadata(monkeypatch):
    from types import SimpleNamespace

    from confluent_kafka import KafkaError, KafkaException

    from recap.streaming import end_offsets

    class Client:
        def __init__(self):
            self.calls = 0

        def list_topics(self, topic, timeout):
            self.calls += 1
            error = KafkaError(KafkaError.UNKNOWN_TOPIC_OR_PART) if self.calls == 1 else None
            return SimpleNamespace(
                topics={topic: SimpleNamespace(error=error, partitions={0: None})}
            )

        def get_watermark_offsets(self, partition, timeout):
            if self.calls == 2:
                raise KafkaException(KafkaError(KafkaError.NOT_LEADER_FOR_PARTITION))
            return 0, 42

        def close(self):
            pass

    client = Client()
    monkeypatch.setattr("recap.streaming.Consumer", lambda config: client)
    monkeypatch.setattr("recap.streaming.time.sleep", lambda seconds: None)
    assert end_offsets("unused", "topic") == {0: 42}
    assert client.calls == 3
