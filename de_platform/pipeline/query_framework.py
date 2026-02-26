"""Shared query protocol for POST-based paginated endpoints.

Provides parse, filter, sort, and paginate utilities that operate on
in-memory ``list[dict]`` rows — compatible with MemoryDatabase in tests
and the existing ``fetch_all_async`` pattern used by all services.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any


@dataclass
class QueryRequest:
    """Standard query request parsed from POST body."""

    filters: dict[str, Any] = field(default_factory=dict)
    sort_by: str | None = None
    sort_order: str = "desc"  # "asc" | "desc"
    page: int = 1  # 1-based
    page_size: int = 50  # default 50, max 500


@dataclass
class QueryResponse:
    """Standard paginated query response."""

    data: list[dict]
    total: int
    page: int
    page_size: int
    total_pages: int


def parse_query_request(body: dict) -> QueryRequest:
    """Parse a POST JSON body into a QueryRequest."""
    filters = body.get("filters", {})
    if not isinstance(filters, dict):
        filters = {}
    sort_by = body.get("sort_by")
    sort_order = body.get("sort_order", "desc")
    if sort_order not in ("asc", "desc"):
        sort_order = "desc"
    page = max(int(body.get("page", 1)), 1)
    page_size = min(int(body.get("page_size", 50)), 500)
    page_size = max(page_size, 1)
    return QueryRequest(
        filters=filters,
        sort_by=sort_by,
        sort_order=sort_order,
        page=page,
        page_size=page_size,
    )


def _match_filter(row_val: Any, spec: Any) -> bool:
    """Check if a single row value matches a filter spec.

    Supported operators (when spec is a dict):
        eq, neq, gt, gte, lt, lte, contains, in, is_null, is_not_null

    When spec is a plain string/number, it is treated as ``eq`` (backward compat).
    """
    if not isinstance(spec, dict):
        # Backward compat: plain value -> exact string match
        return str(row_val or "") == str(spec)

    for op, operand in spec.items():
        if op == "eq":
            if str(row_val or "") != str(operand):
                return False
        elif op == "neq":
            if str(row_val or "") == str(operand):
                return False
        elif op == "gt":
            try:
                if not (float(row_val) > float(operand)):
                    return False
            except (TypeError, ValueError):
                if not (str(row_val or "") > str(operand)):
                    return False
        elif op == "gte":
            try:
                if not (float(row_val) >= float(operand)):
                    return False
            except (TypeError, ValueError):
                if not (str(row_val or "") >= str(operand)):
                    return False
        elif op == "lt":
            try:
                if not (float(row_val) < float(operand)):
                    return False
            except (TypeError, ValueError):
                if not (str(row_val or "") < str(operand)):
                    return False
        elif op == "lte":
            try:
                if not (float(row_val) <= float(operand)):
                    return False
            except (TypeError, ValueError):
                if not (str(row_val or "") <= str(operand)):
                    return False
        elif op == "contains":
            if str(operand).lower() not in str(row_val or "").lower():
                return False
        elif op == "in":
            if not isinstance(operand, list):
                operand = [operand]
            if str(row_val or "") not in [str(v) for v in operand]:
                return False
        elif op == "is_null":
            if operand and row_val is not None:
                return False
        elif op == "is_not_null":
            if operand and row_val is None:
                return False
    return True


def apply_filters(rows: list[dict], filters: dict[str, Any]) -> list[dict]:
    """Filter rows using exact match or operator-based filters.

    Filter values can be:
      - Plain string/number: exact string match (backward compat)
      - Dict with operators: ``{"gte": 100, "lte": 500}``

    Supported operators: eq, neq, gt, gte, lt, lte, contains, in, is_null, is_not_null.
    Skips None and empty-string plain values.
    """
    for key, value in filters.items():
        if value is None or value == "":
            continue
        rows = [r for r in rows if _match_filter(r.get(key), value)]
    return rows


def apply_sort(
    rows: list[dict], sort_by: str | None, sort_order: str,
) -> list[dict]:
    """Sort rows by column name. None values sort last. No-op if sort_by is None."""
    if sort_by is None:
        return rows
    desc = sort_order == "desc"
    none_rows = [r for r in rows if r.get(sort_by) is None]
    non_none = [r for r in rows if r.get(sort_by) is not None]
    non_none.sort(key=lambda r: r[sort_by], reverse=desc)
    return non_none + none_rows


def apply_pagination(
    rows: list[dict], page: int, page_size: int,
) -> tuple[list[dict], int]:
    """Slice rows for the requested page. Returns (page_rows, total_count)."""
    total = len(rows)
    start = (page - 1) * page_size
    end = start + page_size
    return rows[start:end], total


def build_query_response(
    rows: list[dict], total: int, page: int, page_size: int,
) -> dict:
    """Build the standard response dict."""
    total_pages = math.ceil(total / page_size) if page_size > 0 else 0
    return {
        "data": rows,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
    }


def handle_query(all_rows: list[dict], body: dict) -> dict:
    """All-in-one: parse, filter, sort, paginate, return response dict."""
    req = parse_query_request(body)
    filtered = apply_filters(all_rows, req.filters)
    sorted_rows = apply_sort(filtered, req.sort_by, req.sort_order)
    page_rows, total = apply_pagination(sorted_rows, req.page, req.page_size)
    return build_query_response(page_rows, total, req.page, req.page_size)
