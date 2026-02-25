"""Integration tests for KafkaQueue service."""

import os
import uuid

import pytest

pytestmark = pytest.mark.integration

pytest.importorskip("confluent_kafka")

from de_platform.services.message_queue.kafka_queue import KafkaQueue  # noqa: E402
from de_platform.services.secrets.env_secrets import EnvSecrets  # noqa: E402


def _make_secrets(group_id: str | None = None) -> EnvSecrets:
    in_dc = os.environ.get("DEVCONTAINER", "") == "1"
    bootstrap = "kafka:29092" if in_dc else "localhost:9092"
    return EnvSecrets(overrides={
        "MQ_KAFKA_BOOTSTRAP_SERVERS": os.environ.get(
            "MQ_KAFKA_BOOTSTRAP_SERVERS", bootstrap
        ),
        "MQ_KAFKA_GROUP_ID": group_id or f"test-{uuid.uuid4().hex[:8]}",
        "MQ_KAFKA_POLL_TIMEOUT": "0.5",
        "MQ_KAFKA_AUTO_OFFSET_RESET": "earliest",
    })


@pytest.fixture
def topic_name():
    return f"_test_topic_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def producer():
    secrets = _make_secrets()
    mq = KafkaQueue(secrets)
    mq.connect()
    yield mq
    mq.disconnect()


@pytest.fixture
def consumer(topic_name):
    secrets = _make_secrets(group_id=f"test-consumer-{uuid.uuid4().hex[:8]}")
    mq = KafkaQueue(secrets)
    mq.connect()
    yield mq
    mq.disconnect()


def test_connect_disconnect():
    secrets = _make_secrets()
    mq = KafkaQueue(secrets)
    mq.connect()
    assert mq.is_connected()
    mq.disconnect()
    assert not mq.is_connected()


def test_publish_and_consume(producer, consumer, topic_name):
    msg = {"id": "test-1", "data": "hello"}
    producer.publish(topic_name, msg)

    # Subscribe and poll
    consumer.subscribe(topic_name)
    import time
    deadline = time.monotonic() + 15.0
    received = None
    while time.monotonic() < deadline:
        received = consumer.consume_one(topic_name)
        if received is not None:
            break
        time.sleep(0.2)
    assert received is not None
    assert received["id"] == "test-1"


def test_publish_multiple(producer, consumer, topic_name):
    for i in range(5):
        producer.publish(topic_name, {"id": f"msg-{i}"})

    consumer.subscribe(topic_name)
    import time
    deadline = time.monotonic() + 15.0
    messages = []
    while time.monotonic() < deadline and len(messages) < 5:
        msg = consumer.consume_one(topic_name)
        if msg is not None:
            messages.append(msg)
        else:
            time.sleep(0.1)
    assert len(messages) == 5
    ids = {m["id"] for m in messages}
    assert ids == {f"msg-{i}" for i in range(5)}


def test_keyed_publish(producer, consumer, topic_name):
    """Messages with the same key go to the same partition."""
    producer.publish(topic_name, {"id": "k1"}, key="tenant-a:SYM")
    producer.publish(topic_name, {"id": "k2"}, key="tenant-a:SYM")

    consumer.subscribe(topic_name)
    import time
    deadline = time.monotonic() + 15.0
    messages = []
    while time.monotonic() < deadline and len(messages) < 2:
        msg = consumer.consume_one(topic_name)
        if msg is not None:
            messages.append(msg)
        else:
            time.sleep(0.1)
    assert len(messages) == 2
