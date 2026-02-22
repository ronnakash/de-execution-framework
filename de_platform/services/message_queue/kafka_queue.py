"""Kafka-backed message queue implementation using confluent-kafka."""

from __future__ import annotations

import json
from typing import Any, Callable

from de_platform.services.message_queue.interface import MessageQueueInterface
from de_platform.services.secrets.interface import SecretsInterface


class KafkaQueue(MessageQueueInterface):
    def __init__(self, secrets: SecretsInterface) -> None:
        self._bootstrap = secrets.get_or_default(
            "MQ_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        )
        self._group_id = secrets.get_or_default("MQ_KAFKA_GROUP_ID", "de-platform")
        self._poll_timeout = float(
            secrets.get_or_default("MQ_KAFKA_POLL_TIMEOUT", "1.0")
        )
        self._offset_reset = secrets.get_or_default(
            "MQ_KAFKA_AUTO_OFFSET_RESET", "earliest"
        )
        self._initial_topics = secrets.get_or_default(
            "MQ_KAFKA_SUBSCRIBE_TOPICS", ""
        )
        self._producer: Any = None
        self._consumer: Any = None
        self._handlers: dict[str, list[Callable[[Any], None]]] = {}
        self._subscribed_topics: set[str] = set()
        self._topic_buffer: dict[str, list[Any]] = {}

    def connect(self) -> None:
        from confluent_kafka import Consumer, Producer

        self._producer = Producer({"bootstrap.servers": self._bootstrap})
        self._consumer = Consumer({
            "bootstrap.servers": self._bootstrap,
            "group.id": self._group_id,
            "auto.offset.reset": self._offset_reset,
        })
        # Pre-subscribe to all topics at once to avoid incremental rebalances
        # (each subscribe() call triggers a group rebalance; batching them into
        # a single subscribe avoids the rebalance storm).
        if self._initial_topics:
            topics = [t.strip() for t in self._initial_topics.split(",") if t.strip()]
            self._subscribed_topics.update(topics)
            self._consumer.subscribe(list(self._subscribed_topics))

    def disconnect(self) -> None:
        if self._producer:
            self._producer.flush(timeout=10)
            self._producer = None
        if self._consumer:
            self._consumer.close()
            self._consumer = None

    def publish(self, topic: str, message: Any, key: str | None = None) -> None:
        if self._producer is None:
            self.connect()
        payload = json.dumps(message).encode("utf-8")
        key_bytes = key.encode("utf-8") if key else None
        self._producer.produce(topic, value=payload, key=key_bytes)
        self._producer.flush()

    def subscribe(self, topic: str, handler: Callable[[Any], None]) -> None:
        self._handlers.setdefault(topic, []).append(handler)
        if self._consumer is None:
            self.connect()
        self._subscribed_topics.add(topic)
        self._consumer.subscribe(list(self._subscribed_topics))

    def consume_one(self, topic: str) -> Any | None:
        # 1. Check buffer first
        if self._topic_buffer.get(topic):
            value = self._topic_buffer[topic].pop(0)
            for handler in self._handlers.get(topic, []):
                handler(value)
            return value

        if self._consumer is None:
            self.connect()

        # 2. Auto-subscribe if needed
        if topic not in self._subscribed_topics:
            self._subscribed_topics.add(topic)
            self._consumer.subscribe(list(self._subscribed_topics))

        # 3. Poll and filter by topic
        msg = self._consumer.poll(timeout=self._poll_timeout)
        if msg is None or msg.error():
            return None

        value = json.loads(msg.value().decode("utf-8"))
        msg_topic = msg.topic()

        if msg_topic == topic:
            for handler in self._handlers.get(topic, []):
                handler(value)
            return value
        else:
            self._topic_buffer.setdefault(msg_topic, []).append(value)
            return None

    def health_check(self) -> bool:
        if self._producer is None:
            return False
        try:
            metadata = self._producer.list_topics(timeout=5)
            return metadata is not None
        except Exception:
            return False
