from de_platform.modules.batch_etl.transformer import (
    compute_dedup_key,
    ensure_payload_dict,
    normalize_event_type,
    transform_events,
)


def test_normalize_event_type():
    assert normalize_event_type("  Page_View  ") == "page_view"
    assert normalize_event_type("CLICK") == "click"


def test_ensure_payload_dict_from_string():
    result = ensure_payload_dict('{"key": "value"}')
    assert result == {"key": "value"}


def test_ensure_payload_dict_passthrough():
    d = {"key": "value"}
    result = ensure_payload_dict(d)
    assert result is d


def test_compute_dedup_key_deterministic():
    event = {"event_type": "click", "payload": {"url": "/home"}, "event_date": "2026-01-15"}
    key1 = compute_dedup_key(event)
    key2 = compute_dedup_key(event)
    assert key1 == key2


def test_compute_dedup_key_different():
    e1 = {"event_type": "click", "payload": {"url": "/home"}, "event_date": "2026-01-15"}
    e2 = {"event_type": "click", "payload": {"url": "/about"}, "event_date": "2026-01-15"}
    assert compute_dedup_key(e1) != compute_dedup_key(e2)


def test_transform_events_adds_fields():
    events = [
        {"event_type": " Page_View ", "payload": {"url": "/home"}, "source": "web"},
    ]
    result = transform_events(events, "2026-01-15")
    assert len(result) == 1
    r = result[0]
    assert r["event_type"] == "page_view"
    assert r["payload"] == {"url": "/home"}
    assert r["source"] == "web"
    assert r["event_date"] == "2026-01-15"
    assert "created_at" in r
