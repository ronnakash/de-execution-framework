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
        self._producer: Any = None
        self._consumer: Any = None
        self._handlers: dict[str, list[Callable[[Any], None]]] = {}

    def connect(self) -> None:
        from confluent_kafka import Consumer, Producer

        self._producer = Producer({"bootstrap.servers": self._bootstrap})
        self._consumer = Consumer({
            "bootstrap.servers": self._bootstrap,
            "group.id": self._group_id,
            "auto.offset.reset": "earliest",
        })

    def disconnect(self) -> None:
        if self._producer:
            self._producer.flush(timeout=10)
            self._producer = None
        if self._consumer:
            self._consumer.close()
            self._consumer = None

    def publish(self, topic: str, message: Any) -> None:
        if self._producer is None:
            self.connect()
        payload = json.dumps(message).encode("utf-8")
        self._producer.produce(topic, value=payload)
        self._producer.flush()

    def subscribe(self, topic: str, handler: Callable[[Any], None]) -> None:
        self._handlers.setdefault(topic, []).append(handler)
        if self._consumer is None:
            self.connect()
        topics = list(self._handlers.keys())
        self._consumer.subscribe(topics)

    def consume_one(self, topic: str) -> Any | None:
        if self._consumer is None:
            return None
        msg = self._consumer.poll(timeout=1.0)
        if msg is None or msg.error():
            return None
        value = json.loads(msg.value().decode("utf-8"))
        for handler in self._handlers.get(msg.topic(), []):
            handler(value)
        return value

    def health_check(self) -> bool:
        if self._producer is None:
            return False
        try:
            metadata = self._producer.list_topics(timeout=5)
            return metadata is not None
        except Exception:
            return False
