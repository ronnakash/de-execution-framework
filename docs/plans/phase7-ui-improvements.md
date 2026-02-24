# Phase 7: UI Improvements

## Overview

Enhance UI tables with advanced filtering, verify events explorer displays all required fields, and address remaining UI-related issues.

**Source:** bugs.md #4, #6; changes.md #1, #3, #15

## Current State

The UI already has:
- **DataTable component** (`ui/src/components/DataTable.tsx`) — generic table with click-to-sort on all columns, pagination
- **EventsPage** — shows orders, executions, transactions with `order_id`, `execution_id`, `transaction_id` fields
- **AlertsPage** — alerts tab + cases tab with severity/tenant filters
- **Query framework** (`de_platform/pipeline/query_framework.py`) — server-side filter (exact match only), sort, paginate

## What's Missing

### 7.1 Advanced filtering on all UI tables

**Why:** bugs.md #6 — "all UI tables should support sorting by clicking on the field we want to sort by. I also want to look into filtering by each field too, should be robust and support data ranges, constants filtering etc"

Sorting is done. Filtering needs work:

**Current:** Query framework only supports exact string match (`filters: {field: value}`).

**Needed:**
- **Date range filters** — `transact_time >= X AND transact_time <= Y`
- **Numeric range filters** — `notional_usd >= 500000`
- **Contains/partial match** — `symbol LIKE '%AAPL%'`
- **Multi-value select** — `severity IN ('high', 'critical')`
- **Null/not-null** — `counterparty IS NOT NULL`

#### Backend: Extend query framework

Add filter operators to `handle_query()`:

```python
# Current filter format:
{"filters": {"tenant_id": "acme"}}  # exact match only

# New filter format:
{"filters": {
    "tenant_id": {"eq": "acme"},
    "transact_time": {"gte": "2026-01-01", "lte": "2026-01-31"},
    "notional_usd": {"gt": 500000},
    "symbol": {"contains": "AAPL"},
    "severity": {"in": ["high", "critical"]},
}}
```

Supported operators: `eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `contains`, `in`, `is_null`, `is_not_null`

Backward compat: plain string values (`{"tenant_id": "acme"}`) still treated as exact match.

#### Frontend: Add filter UI to DataTable

Add a filter row below the header row in DataTable:
- **Text columns**: text input with debounced `contains` filter
- **Date columns**: date range picker (from/to)
- **Numeric columns**: min/max range inputs
- **Enum columns** (severity, status, event_type): dropdown multi-select
- Filter state stored in component, sent as query params to `queryApi()`

### 7.2 Events explorer missing IDs

**Why:** bugs.md #4 — "transaction/order/execution id missing in events explorer"

Investigation shows EventsPage already displays `order_id`, `execution_id`, `transaction_id` in the column definitions. However, these fields may not exist in ClickHouse results because the column names in the schema are `id` (generic), not `order_id`/`execution_id`/`transaction_id`.

**Check needed:** Verify that ClickHouse `orders` table returns rows with an `order_id` field vs just `id`. If the column is `id`, the UI needs to map `id` → `order_id` for display, or the schema needs aliasing.

**Fix options:**
1. Add computed columns / aliases in ClickHouse queries
2. Map `id` to the type-specific name in the query framework
3. Add `order_id`/`execution_id`/`transaction_id` as explicit columns (aliases of `id`)

### 7.3 UI test coverage for sorting and filtering

**Why:** changes.md #3 — "We should also have good UI tests for the sorting and filtering"

**New test file:** `tests/e2e_ui/test_table_features.py`

```
test_events_table_sort_by_column       — click column header, verify order changes
test_events_table_sort_toggle          — click same column twice toggles asc/desc
test_events_table_filter_by_text       — type in filter input, verify rows reduce
test_events_table_filter_by_date_range — select date range, verify rows scoped
test_events_table_pagination           — navigate pages, verify page content changes
test_alerts_table_filter_by_severity   — select severity filter, verify correct alerts
test_cases_table_filter_by_status      — filter by status, verify correct cases
```

## Files to Modify

| File | Changes |
|------|---------|
| `de_platform/pipeline/query_framework.py` | Add filter operators (gte, lte, contains, in, etc.) |
| `ui/src/components/DataTable.tsx` | Add filter row with per-column filter inputs |
| `ui/src/api/client.ts` | Update `queryApi()` to send structured filter objects |
| `ui/src/pages/EventsPage.tsx` | Wire up filter state to DataTable |
| `ui/src/pages/AlertsPage.tsx` | Wire up filter state |
| `ui/src/pages/DataAuditPage.tsx` | Wire up filter state |

## Files to Create

| File | Purpose |
|------|---------|
| `tests/e2e_ui/test_table_features.py` | UI tests for sorting, filtering, pagination |

## Acceptance Criteria

1. Query framework supports range, contains, in, and null operators
2. Backward compat: existing exact-match filters still work
3. DataTable shows filter inputs below column headers
4. Date columns have date range pickers
5. Numeric columns have min/max inputs
6. Enum columns have dropdown multi-select
7. Events explorer shows order_id/execution_id/transaction_id correctly
8. UI tests verify sorting and filtering work end-to-end
