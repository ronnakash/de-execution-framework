from de_platform.modules.batch_etl.validator import validate_events, validate_single_event
from de_platform.services.logger.memory_logger import MemoryLogger


def test_validate_valid_event():
    event = {"event_type": "click", "payload": {"url": "/home"}, "source": "web"}
    is_valid, reason = validate_single_event(event)
    assert is_valid is True
    assert reason is None


def test_validate_missing_event_type():
    event = {"payload": {"url": "/home"}, "source": "web"}
    is_valid, reason = validate_single_event(event)
    assert is_valid is False
    assert reason == "missing_or_empty_event_type"


def test_validate_empty_event_type():
    event = {"event_type": "", "payload": {"url": "/home"}, "source": "web"}
    is_valid, reason = validate_single_event(event)
    assert is_valid is False
    assert reason == "missing_or_empty_event_type"


def test_validate_missing_payload():
    event = {"event_type": "click", "source": "web"}
    is_valid, reason = validate_single_event(event)
    assert is_valid is False
    assert reason == "payload_not_dict"


def test_validate_payload_not_dict():
    event = {"event_type": "click", "payload": "not_a_dict", "source": "web"}
    is_valid, reason = validate_single_event(event)
    assert is_valid is False
    assert reason == "payload_not_dict"


def test_validate_missing_source():
    event = {"event_type": "click", "payload": {"url": "/home"}}
    is_valid, reason = validate_single_event(event)
    assert is_valid is False
    assert reason == "missing_or_empty_source"


def test_validate_events_mixed():
    log = MemoryLogger()
    events = [
        {"event_type": "click", "payload": {"url": "/home"}, "source": "web"},
        {"event_type": "", "payload": {"url": "/home"}, "source": "web"},
        {"event_type": "view", "payload": {"url": "/about"}, "source": "mobile"},
        {"payload": {"url": "/home"}, "source": "web"},
        {"event_type": "buy", "payload": {"id": 1}, "source": "api"},
    ]
    valid, invalid_count = validate_events(events, log)
    assert len(valid) == 3
    assert invalid_count == 2
