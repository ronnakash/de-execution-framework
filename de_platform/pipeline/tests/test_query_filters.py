"""Tests for the extended query framework filter operators."""

from de_platform.pipeline.query_framework import apply_filters, handle_query


# -- Backward compatibility with plain string values --------------------------


def test_backward_compat_exact_match():
    rows = [{"name": "alice"}, {"name": "bob"}]
    result = apply_filters(rows, {"name": "alice"})
    assert len(result) == 1
    assert result[0]["name"] == "alice"


def test_backward_compat_skips_none():
    rows = [{"a": 1}, {"a": 2}]
    result = apply_filters(rows, {"a": None})
    assert len(result) == 2


def test_backward_compat_skips_empty():
    rows = [{"a": 1}, {"a": 2}]
    result = apply_filters(rows, {"a": ""})
    assert len(result) == 2


# -- eq operator --------------------------------------------------------------


def test_eq_operator():
    rows = [{"status": "open"}, {"status": "closed"}]
    result = apply_filters(rows, {"status": {"eq": "open"}})
    assert len(result) == 1
    assert result[0]["status"] == "open"


# -- neq operator -------------------------------------------------------------


def test_neq_operator():
    rows = [{"status": "open"}, {"status": "closed"}, {"status": "open"}]
    result = apply_filters(rows, {"status": {"neq": "open"}})
    assert len(result) == 1
    assert result[0]["status"] == "closed"


# -- gt / gte operators -------------------------------------------------------


def test_gt_numeric():
    rows = [{"amount": 10}, {"amount": 20}, {"amount": 30}]
    result = apply_filters(rows, {"amount": {"gt": 15}})
    assert len(result) == 2
    assert all(r["amount"] > 15 for r in result)


def test_gte_numeric():
    rows = [{"amount": 10}, {"amount": 20}, {"amount": 30}]
    result = apply_filters(rows, {"amount": {"gte": 20}})
    assert len(result) == 2
    assert all(r["amount"] >= 20 for r in result)


def test_gt_string_comparison():
    rows = [
        {"date": "2026-01-01"},
        {"date": "2026-01-15"},
        {"date": "2026-02-01"},
    ]
    result = apply_filters(rows, {"date": {"gt": "2026-01-10"}})
    assert len(result) == 2


# -- lt / lte operators -------------------------------------------------------


def test_lt_numeric():
    rows = [{"amount": 10}, {"amount": 20}, {"amount": 30}]
    result = apply_filters(rows, {"amount": {"lt": 25}})
    assert len(result) == 2
    assert all(r["amount"] < 25 for r in result)


def test_lte_numeric():
    rows = [{"amount": 10}, {"amount": 20}, {"amount": 30}]
    result = apply_filters(rows, {"amount": {"lte": 20}})
    assert len(result) == 2
    assert all(r["amount"] <= 20 for r in result)


# -- Range (combined gte + lte) -----------------------------------------------


def test_numeric_range():
    rows = [{"amount": 10}, {"amount": 20}, {"amount": 30}, {"amount": 40}]
    result = apply_filters(rows, {"amount": {"gte": 15, "lte": 35}})
    assert len(result) == 2
    assert {r["amount"] for r in result} == {20, 30}


def test_date_range():
    rows = [
        {"date": "2026-01-01"},
        {"date": "2026-01-15"},
        {"date": "2026-01-31"},
        {"date": "2026-02-15"},
    ]
    result = apply_filters(rows, {"date": {"gte": "2026-01-10", "lte": "2026-01-31"}})
    assert len(result) == 2


# -- contains operator --------------------------------------------------------


def test_contains_case_insensitive():
    rows = [
        {"symbol": "AAPL"},
        {"symbol": "GOOG"},
        {"symbol": "MSFT"},
    ]
    result = apply_filters(rows, {"symbol": {"contains": "aapl"}})
    assert len(result) == 1
    assert result[0]["symbol"] == "AAPL"


def test_contains_partial():
    rows = [
        {"desc": "Large notional detected"},
        {"desc": "Velocity alert"},
        {"desc": "Suspicious counterparty"},
    ]
    result = apply_filters(rows, {"desc": {"contains": "notional"}})
    assert len(result) == 1


# -- in operator --------------------------------------------------------------


def test_in_operator():
    rows = [
        {"severity": "low"},
        {"severity": "medium"},
        {"severity": "high"},
        {"severity": "critical"},
    ]
    result = apply_filters(rows, {"severity": {"in": ["high", "critical"]}})
    assert len(result) == 2
    assert {r["severity"] for r in result} == {"high", "critical"}


def test_in_single_value():
    rows = [{"status": "open"}, {"status": "closed"}]
    result = apply_filters(rows, {"status": {"in": ["open"]}})
    assert len(result) == 1


# -- is_null / is_not_null operators ------------------------------------------


def test_is_null():
    rows = [{"cp": None}, {"cp": "acme"}, {"cp": None}]
    result = apply_filters(rows, {"cp": {"is_null": True}})
    assert len(result) == 2
    assert all(r["cp"] is None for r in result)


def test_is_not_null():
    rows = [{"cp": None}, {"cp": "acme"}, {"cp": None}]
    result = apply_filters(rows, {"cp": {"is_not_null": True}})
    assert len(result) == 1
    assert result[0]["cp"] == "acme"


# -- handle_query with operator filters ---------------------------------------


def test_handle_query_with_operator_filters():
    rows = [
        {"tenant_id": "acme", "amount": 100, "severity": "low"},
        {"tenant_id": "acme", "amount": 500, "severity": "high"},
        {"tenant_id": "acme", "amount": 1000, "severity": "critical"},
        {"tenant_id": "beta", "amount": 200, "severity": "medium"},
    ]
    body = {
        "filters": {
            "tenant_id": "acme",  # backward compat exact match
            "amount": {"gte": 200},
        },
        "sort_by": "amount",
        "sort_order": "asc",
    }
    result = handle_query(rows, body)
    assert result["total"] == 2
    assert result["data"][0]["amount"] == 500
    assert result["data"][1]["amount"] == 1000


def test_handle_query_mixed_filters():
    rows = [
        {"severity": "high", "amount": 100},
        {"severity": "high", "amount": 500},
        {"severity": "low", "amount": 300},
        {"severity": "critical", "amount": 200},
    ]
    body = {
        "filters": {
            "severity": {"in": ["high", "critical"]},
            "amount": {"gt": 150},
        },
    }
    result = handle_query(rows, body)
    assert result["total"] == 2
    amounts = {r["amount"] for r in result["data"]}
    assert amounts == {500, 200}
