"""Tests for the shared query framework."""

from de_platform.pipeline.query_framework import (
    apply_filters,
    apply_pagination,
    apply_sort,
    build_query_response,
    handle_query,
    parse_query_request,
)


def test_parse_query_request_defaults():
    req = parse_query_request({})
    assert req.filters == {}
    assert req.sort_by is None
    assert req.sort_order == "desc"
    assert req.page == 1
    assert req.page_size == 50


def test_parse_query_request_max_page_size():
    req = parse_query_request({"page_size": 9999})
    assert req.page_size == 500


def test_parse_query_request_min_page():
    req = parse_query_request({"page": -5})
    assert req.page == 1


def test_parse_query_request_invalid_sort_order():
    req = parse_query_request({"sort_order": "random"})
    assert req.sort_order == "desc"


def test_parse_query_request_full():
    body = {
        "filters": {"tenant_id": "acme"},
        "sort_by": "created_at",
        "sort_order": "asc",
        "page": 3,
        "page_size": 25,
    }
    req = parse_query_request(body)
    assert req.filters == {"tenant_id": "acme"}
    assert req.sort_by == "created_at"
    assert req.sort_order == "asc"
    assert req.page == 3
    assert req.page_size == 25


def test_apply_filters():
    rows = [
        {"tenant_id": "acme", "severity": "high"},
        {"tenant_id": "acme", "severity": "low"},
        {"tenant_id": "beta", "severity": "high"},
        {"tenant_id": "beta", "severity": "medium"},
        {"tenant_id": "acme", "severity": "critical"},
    ]
    result = apply_filters(rows, {"tenant_id": "acme"})
    assert len(result) == 3
    assert all(r["tenant_id"] == "acme" for r in result)


def test_apply_filters_empty():
    rows = [{"a": 1}, {"a": 2}]
    result = apply_filters(rows, {})
    assert len(result) == 2


def test_apply_filters_skips_none_and_empty():
    rows = [
        {"tenant_id": "acme", "severity": "high"},
        {"tenant_id": "beta", "severity": "low"},
    ]
    result = apply_filters(rows, {"tenant_id": None, "severity": ""})
    assert len(result) == 2


def test_apply_sort_asc():
    rows = [{"amount": 30}, {"amount": 10}, {"amount": 20}]
    result = apply_sort(rows, "amount", "asc")
    assert [r["amount"] for r in result] == [10, 20, 30]


def test_apply_sort_desc():
    rows = [{"amount": 30}, {"amount": 10}, {"amount": 20}]
    result = apply_sort(rows, "amount", "desc")
    assert [r["amount"] for r in result] == [30, 20, 10]


def test_apply_sort_with_none_values():
    rows = [
        {"amount": 20},
        {"amount": None},
        {"amount": 10},
        {"amount": None},
    ]
    # Ascending: non-None first, None last
    result_asc = apply_sort(rows, "amount", "asc")
    assert result_asc[0]["amount"] == 10
    assert result_asc[1]["amount"] == 20
    assert result_asc[2]["amount"] is None
    assert result_asc[3]["amount"] is None

    # Descending: non-None first (in desc order), None last
    result_desc = apply_sort(rows, "amount", "desc")
    assert result_desc[-1]["amount"] is None
    assert result_desc[-2]["amount"] is None


def test_apply_sort_no_sort_by():
    rows = [{"a": 3}, {"a": 1}, {"a": 2}]
    result = apply_sort(rows, None, "asc")
    assert [r["a"] for r in result] == [3, 1, 2]


def test_apply_pagination():
    rows = [{"i": i} for i in range(10)]
    page_rows, total = apply_pagination(rows, 2, 3)
    assert total == 10
    assert [r["i"] for r in page_rows] == [3, 4, 5]


def test_apply_pagination_last_page():
    rows = [{"i": i} for i in range(10)]
    page_rows, total = apply_pagination(rows, 4, 3)
    assert total == 10
    assert len(page_rows) == 1
    assert page_rows[0]["i"] == 9


def test_apply_pagination_beyond_last_page():
    rows = [{"i": i} for i in range(10)]
    page_rows, total = apply_pagination(rows, 100, 3)
    assert total == 10
    assert page_rows == []


def test_build_query_response_structure():
    result = build_query_response([{"a": 1}], total=10, page=2, page_size=3)
    assert result["data"] == [{"a": 1}]
    assert result["total"] == 10
    assert result["page"] == 2
    assert result["page_size"] == 3
    assert result["total_pages"] == 4  # ceil(10/3)


def test_handle_query_integration():
    rows = []
    for i in range(20):
        rows.append({
            "tenant_id": "acme" if i % 2 == 0 else "beta",
            "severity": "high" if i % 3 == 0 else "low",
            "created_at": f"2026-01-{i + 1:02d}",
        })

    body = {
        "filters": {"tenant_id": "acme"},
        "sort_by": "created_at",
        "sort_order": "desc",
        "page": 1,
        "page_size": 5,
    }
    result = handle_query(rows, body)

    assert result["total"] == 10  # 10 acme rows
    assert len(result["data"]) == 5
    assert result["page"] == 1
    assert result["total_pages"] == 2
    # Verify descending sort
    dates = [r["created_at"] for r in result["data"]]
    assert dates == sorted(dates, reverse=True)


def test_handle_query_defaults():
    rows = [{"a": 1}, {"a": 2}, {"a": 3}]
    result = handle_query(rows, {})
    assert result["total"] == 3
    assert len(result["data"]) == 3
    assert result["page"] == 1
    assert result["total_pages"] == 1
