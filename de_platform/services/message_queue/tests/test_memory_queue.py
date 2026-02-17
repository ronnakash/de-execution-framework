from de_platform.services.message_queue.memory_queue import MemoryQueue


def test_publish_and_consume():
    mq = MemoryQueue()
    mq.publish("events", {"type": "click"})
    msg = mq.consume_one("events")
    assert msg == {"type": "click"}


def test_consume_returns_none_on_empty():
    mq = MemoryQueue()
    assert mq.consume_one("events") is None


def test_fifo_order():
    mq = MemoryQueue()
    mq.publish("events", "first")
    mq.publish("events", "second")
    assert mq.consume_one("events") == "first"
    assert mq.consume_one("events") == "second"


def test_subscribe_handler_called():
    mq = MemoryQueue()
    received: list[str] = []
    mq.subscribe("events", lambda msg: received.append(msg))
    mq.publish("events", "hello")
    assert received == ["hello"]


def test_multiple_topics_isolated():
    mq = MemoryQueue()
    mq.publish("a", "msg_a")
    mq.publish("b", "msg_b")
    assert mq.consume_one("a") == "msg_a"
    assert mq.consume_one("b") == "msg_b"
    assert mq.consume_one("a") is None


def test_health_check():
    mq = MemoryQueue()
    assert mq.health_check() is True
