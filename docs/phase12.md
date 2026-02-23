# Phase 12: Query Framework & Pagination

**Goal:** Introduce a POST-based query framework with shared filter/sort/pagination logic across all HTTP services. Update the UI DataTable component to support server-side sorting and pagination.

---

## Table of Contents

1. [Overview](#overview)
2. [Backend: Shared Query Protocol](#backend-shared-query-protocol)
3. [Backend: Service-by-Service Changes](#backend-service-by-service-changes)
4. [Frontend: DataTable Enhancement](#frontend-datatable-enhancement)
5. [Frontend: API Client Extension](#frontend-api-client-extension)
6. [Frontend: Page Updates](#frontend-page-updates)
7. [Unit Tests](#unit-tests)
8. [Files Changed Summary](#files-changed-summary)
9. [Verification](#verification)

---

## Overview

Currently, all list endpoints use GET requests with query-string parameters for simple filtering and a hardcoded `limit`. There is no server-side sorting, no pagination metadata (total count, page number, total pages), and no consistent contract across services.

Phase 12 introduces:

- A shared `query_framework.py` module in `de_platform/pipeline/` with dataclasses and utility functions for parsing, filtering, sorting, and paginating query requests.
- New `POST /api/v1/query/{resource}` endpoints on every HTTP service that accept a JSON body and return a standardized paginated response.
- An enhanced `DataTable` React component with clickable sortable column headers, sort-direction indicators, and a pagination bar.
- A new `queryApi()` function in the frontend API client.
- Updated pages that wire sort/pagination state through `queryApi` and react-query.

Existing GET endpoints remain for backward compatibility.

---

## Backend: Shared Query Protocol

### New file: `de_platform/pipeline/query_framework.py`

This module defines the standard request/response schema and pure-function utilities that every service can import. It operates on in-memory `list[dict]` rows, which is compatible with both `MemoryDatabase` in unit tests and the existing `fetch_all_async` pattern used by all services.

#### Dataclasses

```python
@dataclass
class QueryRequest:
    """Standard query request parsed from POST body."""
    filters: dict[str, Any] = field(default_factory=dict)
    sort_by: str | None = None
    sort_order: str = "desc"          # "asc" | "desc"
    page: int = 1                     # 1-based
    page_size: int = 50               # default 50, max 500

@dataclass
class QueryResponse:
    """Standard paginated query response."""
    data: list[dict]
    total: int
    page: int
    page_size: int
    total_pages: int
```

#### Functions

| Function | Signature | Description |
|----------|-----------|-------------|
| `parse_query_request` | `(body: dict) -> QueryRequest` | Parse a POST JSON body. Clamps `page_size` to max 500, `page` to min 1, validates `sort_order`. |
| `apply_filters` | `(rows: list[dict], filters: dict[str, Any]) -> list[dict]` | Filter rows by exact string match on each key. Skips `None` and empty-string values. |
| `apply_sort` | `(rows: list[dict], sort_by: str \| None, sort_order: str) -> list[dict]` | Sort rows by column name. `None` values sort last. No-op if `sort_by` is `None`. |
| `apply_pagination` | `(rows: list[dict], page: int, page_size: int) -> tuple[list[dict], int]` | Slice rows for the requested page. Returns `(page_rows, total_count)`. |
| `build_query_response` | `(rows: list[dict], total: int, page: int, page_size: int) -> dict` | Build the standard response dict with `data`, `total`, `page`, `page_size`, `total_pages`. |
| `handle_query` | `(all_rows: list[dict], body: dict) -> dict` | All-in-one convenience: parse, filter, sort, paginate, return response dict. |

#### Request body format (POST JSON)

```json
{
    "filters": {"tenant_id": "acme", "severity": "high"},
    "sort_by": "created_at",
    "sort_order": "desc",
    "page": 1,
    "page_size": 50
}
```

All fields are optional. Missing fields use the defaults from `QueryRequest`.

#### Response format

```json
{
    "data": [ ... ],
    "total": 237,
    "page": 2,
    "page_size": 50,
    "total_pages": 5
}
```

#### Implementation details

- `parse_query_request`: Use `min(int(body.get("page_size", 50)), 500)` for page_size clamping. Use `max(int(body.get("page", 1)), 1)` for page floor. Validate `sort_order` falls back to `"desc"` if not `"asc"` or `"desc"`.
- `apply_sort`: Use `sorted()` with a key of `(r.get(sort_by) is None, r.get(sort_by, ""))` so `None` values always sort after non-None values regardless of direction. Pass `reverse = (sort_order == "desc")`.
- `apply_pagination`: Compute `start = (page - 1) * page_size`, `end = start + page_size`, return `rows[start:end]` and `len(rows)` (total pre-pagination count).
- `build_query_response`: Compute `total_pages = math.ceil(total / page_size) if page_size > 0 else 0`.
- `handle_query`: Chain `parse_query_request` -> `apply_filters` -> `apply_sort` -> `apply_pagination` -> `build_query_response`.

---

## Backend: Service-by-Service Changes

Each service adds POST query endpoints using the shared `handle_query()`. The pattern for every handler is:

1. Read and validate the resource path (if parameterized).
2. Parse the POST JSON body via `await request.json()`.
3. Apply tenant scoping from JWT (if auth is active) by injecting `tenant_id` into the body's `filters`.
4. Fetch all rows from the database via the existing `fetch_all_async` pattern.
5. Call `handle_query(rows, body)` and return the result as JSON.

Existing GET endpoints remain unchanged for backward compatibility.

### data_api (`de_platform/modules/data_api/main.py`, port 8002)

**New route:**
```
POST /api/v1/query/events/{table}
```

**Handler: `_query_events`**

- Validate `table` from `request.match_info["table"]` is one of `("orders", "executions", "transactions")`. Return 400 if invalid.
- Parse JSON body from `await request.json()`.
- Resolve `tenant_id` via existing `_resolve_tenant_id(request)`. If non-None, inject into `body.setdefault("filters", {})["tenant_id"]`.
- Fetch rows: `await self.events_db.fetch_all_async(f"SELECT * FROM {table}")`.
- Return `web.json_response(dumps=_dumps, data=handle_query(rows, body))`.
- Emit `http_requests_total` metric with `method: "POST"` and the query endpoint.

**Registration in `_create_app`:**
```python
app.router.add_post("/api/v1/query/events/{table}", self._query_events)
```

### alert_manager (`de_platform/modules/alert_manager/main.py`, port 8007)

**New routes:**
```
POST /api/v1/query/alerts
POST /api/v1/query/cases
```

**Handler: `_query_alerts`**

- Parse JSON body.
- Resolve `tenant_id` via `_resolve_tenant_id(request)`. If non-None, inject into filters.
- Fetch rows: `await self.db.fetch_all_async("SELECT * FROM alerts")`.
- Return `handle_query(rows, body)`.

**Handler: `_query_cases`**

- Same pattern as `_query_alerts` but fetching from the `cases` table.

**Registration in `_create_app`:**
```python
app.router.add_post("/api/v1/query/alerts", self._query_alerts)
app.router.add_post("/api/v1/query/cases", self._query_cases)
```

### data_audit (`de_platform/modules/data_audit/main.py`, port 8005)

**New routes:**
```
POST /api/v1/query/audit/daily
POST /api/v1/query/audit/files
```

**Handler: `_query_daily`**

- Parse JSON body.
- Fetch rows: `await self.db.fetch_all_async("SELECT * FROM daily_audit")`.
- Return `handle_query(rows, body)`.

Note: The existing GET endpoint supports `start_date`/`end_date` range filtering via custom date comparison functions (`_date_gte`, `_date_lte`). The POST query endpoint uses exact-match filters only (via `apply_filters`). For date range filtering in the POST endpoint, the caller should continue using the GET endpoint or the filters should be extended. For Phase 12, the POST endpoint provides basic exact-match filtering on all fields (including `date`, `tenant_id`, `event_type`). Date range support can be added in a follow-up if needed.

**Handler: `_query_files`**

- Parse JSON body.
- Fetch rows: `await self.db.fetch_all_async("SELECT * FROM file_audit")`.
- Return `handle_query(rows, body)`.

**Registration in `_create_app`:**
```python
app.router.add_post("/api/v1/query/audit/daily", self._query_daily)
app.router.add_post("/api/v1/query/audit/files", self._query_files)
```

### task_scheduler (`de_platform/modules/task_scheduler/main.py`, port 8006)

**New routes:**
```
POST /api/v1/query/tasks
POST /api/v1/query/runs
```

**Handler: `_query_tasks`**

- Parse JSON body.
- Fetch rows: `await self.db.fetch_all_async("SELECT * FROM task_definitions")`.
- Parse JSON fields on each row (call `_parse_json_field` on `default_args`).
- Return `handle_query(rows, body)`.

**Handler: `_query_runs`**

- Parse JSON body.
- Fetch rows: `await self.db.fetch_all_async("SELECT * FROM task_runs")`.
- Parse JSON fields on each row (call `_parse_json_field` on `args`).
- Return `handle_query(rows, body)`.

**Registration in `_create_app`:**
```python
app.router.add_post("/api/v1/query/tasks", self._query_tasks)
app.router.add_post("/api/v1/query/runs", self._query_runs)
```

### client_config (`de_platform/modules/client_config/main.py`, port 8003)

**New route:**
```
POST /api/v1/query/clients
```

**Handler: `_query_clients`**

- Parse JSON body.
- Fetch rows: `await self.db.fetch_all_async("SELECT * FROM clients")`.
- Return `handle_query(rows, body)`.

**Registration in `_create_app`:**
```python
app.router.add_post("/api/v1/query/clients", self._query_clients)
```

---

## Frontend: DataTable Enhancement

### File: `ui/src/components/DataTable.tsx`

#### Updated Column interface

Add an optional `sortable` boolean to the existing `Column` interface:

```tsx
interface Column<T> {
  key: string;
  header: string;
  sortable?: boolean;            // NEW
  render?: (row: T) => React.ReactNode;
}
```

#### Updated Props interface

Add pagination and sort props:

```tsx
interface Props<T> {
  columns: Column<T>[];
  data: T[];
  emptyMessage?: string;
  // Pagination props (all optional — no pagination bar if omitted)
  total?: number;
  page?: number;
  pageSize?: number;
  totalPages?: number;
  onPageChange?: (page: number) => void;
  onPageSizeChange?: (size: number) => void;
  // Sort props (all optional — no sort indicators if omitted)
  sortBy?: string | null;
  sortOrder?: "asc" | "desc";
  onSortChange?: (column: string) => void;
}
```

#### Sortable column headers

When a column has `sortable: true` and `onSortChange` is provided:

- The `<th>` element gets `cursor-pointer` and a hover style.
- Clicking the header calls `onSortChange(col.key)`.
- If `sortBy === col.key`, show a sort direction indicator (up arrow for asc, down arrow for desc) next to the header text. Use simple Unicode characters: `\u25B2` (up triangle) for asc, `\u25BC` (down triangle) for desc.
- The parent page is responsible for toggling the sort direction (if the same column is clicked again, flip asc/desc; if a new column is clicked, default to desc).

#### Pagination bar

Rendered below the table when `total` is provided and greater than 0:

- Left: "Showing X-Y of Z" text (where X = `(page-1)*pageSize + 1`, Y = `min(page*pageSize, total)`, Z = `total`).
- Center: Page size selector dropdown with options `[25, 50, 100]`. Calls `onPageSizeChange` on change.
- Right: Previous/Next buttons. Previous is disabled when `page <= 1`. Next is disabled when `page >= totalPages`. Calls `onPageChange(page - 1)` or `onPageChange(page + 1)`.
- Also show current page: "Page X of Y".

#### Styling

- Pagination bar: `flex items-center justify-between px-4 py-3 border-t border-gray-200 bg-gray-50 text-sm text-gray-600`.
- Page size select: same styling as existing filter inputs (`px-2 py-1 border border-gray-300 rounded text-sm`).
- Prev/Next buttons: `px-3 py-1 rounded border border-gray-300 text-sm hover:bg-gray-100 disabled:opacity-50 disabled:cursor-not-allowed`.
- Sort indicator: `ml-1 text-xs text-gray-400` (or `text-primary` when active).

#### Backward compatibility

All new props are optional. Existing pages that do not pass pagination/sort props see no change in behavior -- the table renders exactly as before.

---

## Frontend: API Client Extension

### File: `ui/src/api/client.ts`

Add a `queryApi` function and supporting types:

```tsx
export interface QueryParams {
  filters?: Record<string, string>;
  sort_by?: string | null;
  sort_order?: "asc" | "desc";
  page?: number;
  page_size?: number;
}

export interface QueryResponse<T> {
  data: T[];
  total: number;
  page: number;
  page_size: number;
  total_pages: number;
}

export function queryApi<T>(
  resource: string,
  params: QueryParams = {},
): Promise<QueryResponse<T>> {
  return fetchApi<QueryResponse<T>>(`/api/v1/query/${resource}`, {
    method: "POST",
    body: JSON.stringify({
      filters: params.filters || {},
      sort_by: params.sort_by || null,
      sort_order: params.sort_order || "desc",
      page: params.page || 1,
      page_size: params.page_size || 50,
    }),
  });
}
```

This reuses the existing `fetchApi` function which already handles auth token injection and 401 refresh logic.

---

## Frontend: Page Updates

Each page is updated to:
1. Add state for `sortBy`, `sortOrder`, `page`, `pageSize`.
2. Replace the existing GET-based `fetchX` call with `queryApi`.
3. Include sort/page state in the react-query `queryKey` for proper cache keying.
4. Pass sort/page props and callbacks to `DataTable`.
5. Reset `page` to 1 when filters change (via `useEffect`).
6. Implement sort toggle logic: if clicking the already-sorted column, flip order; if clicking a new column, default to `"desc"`.

### EventsPage (`ui/src/pages/EventsPage.tsx`)

**State additions:**
```tsx
const [sortBy, setSortBy] = useState<string | null>(null);
const [sortOrder, setSortOrder] = useState<"asc" | "desc">("desc");
const [page, setPage] = useState(1);
const [pageSize, setPageSize] = useState(50);
```

**Query change:**
```tsx
const query = useQuery({
  queryKey: ["events", eventType, tenantId, date, sortBy, sortOrder, page, pageSize],
  queryFn: () =>
    queryApi<Record<string, unknown>>(`events/${eventType}`, {
      filters: {
        ...(tenantId ? { tenant_id: tenantId } : {}),
        ...(date ? { date } : {}),
      },
      sort_by: sortBy,
      sort_order: sortOrder,
      page,
      page_size: pageSize,
    }),
});
```

**Reset page on filter change:**
```tsx
useEffect(() => setPage(1), [eventType, tenantId, date]);
```

**Sort toggle handler:**
```tsx
const handleSortChange = (column: string) => {
  if (sortBy === column) {
    setSortOrder(prev => prev === "desc" ? "asc" : "desc");
  } else {
    setSortBy(column);
    setSortOrder("desc");
  }
  setPage(1);
};
```

**DataTable usage update:**
```tsx
<DataTable
  columns={COLUMN_DEFS[eventType].map(c => ({ ...c, sortable: true }))}
  data={query.data?.data || []}
  emptyMessage={`No ${eventType} found.`}
  total={query.data?.total}
  page={query.data?.page}
  pageSize={query.data?.page_size}
  totalPages={query.data?.total_pages}
  onPageChange={setPage}
  onPageSizeChange={(size) => { setPageSize(size); setPage(1); }}
  sortBy={sortBy}
  sortOrder={sortOrder}
  onSortChange={handleSortChange}
/>
```

Note: `query.data` is now a `QueryResponse` object, so data rows are at `query.data?.data`.

### AlertsPage (`ui/src/pages/AlertsPage.tsx`)

Same pattern as EventsPage. Two separate sets of sort/page state for the alerts tab and cases tab:

- `alertSortBy`, `alertSortOrder`, `alertPage`, `alertPageSize`
- `caseSortBy`, `caseSortOrder`, `casePage`, `casePageSize`

Both `alertsQuery` and `casesQuery` switch from `fetchAlerts`/`fetchCases` to `queryApi<Alert>("alerts", ...)` and `queryApi<Case>("cases", ...)`.

Mark appropriate columns as `sortable: true` (severity, tenant_id, algorithm, created_at for alerts; case_id, tenant_id, status, alert_count, created_at for cases). The `actions` column should NOT be sortable.

Reset alert page when `tenantId` or `severity` filter changes. Reset case page when `tenantId` changes.

### DataAuditPage (`ui/src/pages/DataAuditPage.tsx`)

Switch `dailyQuery` from `fetchDailyAudit` to `queryApi<DailyAudit>("audit/daily", ...)`.

Filters: `tenant_id`, `event_type`. Note: The POST query endpoint uses exact-match filters, so `start_date`/`end_date` range filtering is not available via `queryApi`. For Phase 12, pass `tenant_id` as a filter. The `start_date`/`end_date` inputs can be removed or kept as informational -- if kept, they should be passed as additional filter keys and the backend `apply_filters` will do exact string match on the `date` field. Alternatively, keep the summary query using the existing GET endpoint (since the summary endpoint does not need pagination) and only switch the daily table to queryApi.

Mark columns as sortable: `date`, `tenant_id`, `event_type`, `received_count`, `processed_count`, `error_count`, `duplicate_count`.

The summary cards section (`fetchAuditSummary`) remains unchanged -- it uses a separate GET endpoint that returns aggregate data, not a list.

### TasksPage (`ui/src/pages/TasksPage.tsx`)

Switch `tasksQuery` from `fetchTasks` to `queryApi<TaskDefinition>("tasks", ...)`.

For run history (the `runsQuery`), switch from `fetchRuns(selectedTask)` to `queryApi<TaskRun>("runs", { filters: { task_id: selectedTask } })`.

Mark task columns as sortable: `task_id`, `name`, `module_name`, `schedule_cron`, `enabled`. The `actions` column is NOT sortable.

Mark run columns as sortable: `run_id`, `status`, `exit_code`, `started_at`, `completed_at`. The `error_message` column can be sortable too.

### ClientConfigPage (`ui/src/pages/ClientConfigPage.tsx`)

Switch `clientsQuery` from `fetchClients` to `queryApi<ClientConfig>("clients", ...)`.

Mark columns as sortable: `tenant_id`, `display_name`, `mode`, `algo_run_hour`. The `actions` column is NOT sortable.

The algo config panel remains unchanged -- it uses a detail GET endpoint per tenant, not a paginated list.

---

## Unit Tests

### New file: `de_platform/pipeline/tests/test_query_framework.py`

All tests use plain Python data -- no infrastructure, no async. These are pure-function unit tests.

#### Test cases

1. **`test_parse_query_request_defaults`**
   - Input: empty dict `{}`.
   - Assert: `filters == {}`, `sort_by is None`, `sort_order == "desc"`, `page == 1`, `page_size == 50`.

2. **`test_parse_query_request_max_page_size`**
   - Input: `{"page_size": 9999}`.
   - Assert: `page_size == 500` (clamped).

3. **`test_parse_query_request_min_page`**
   - Input: `{"page": -5}`.
   - Assert: `page == 1` (floored).

4. **`test_parse_query_request_invalid_sort_order`**
   - Input: `{"sort_order": "random"}`.
   - Assert: `sort_order == "desc"` (fallback).

5. **`test_parse_query_request_full`**
   - Input: full body with all fields.
   - Assert: all fields match.

6. **`test_apply_filters`**
   - Rows: 5 dicts with varying `tenant_id` and `severity`.
   - Filters: `{"tenant_id": "acme"}`.
   - Assert: only rows with `tenant_id == "acme"` returned.

7. **`test_apply_filters_empty`**
   - Filters: `{}`.
   - Assert: all rows returned.

8. **`test_apply_filters_skips_none_and_empty`**
   - Filters: `{"tenant_id": None, "severity": ""}`.
   - Assert: all rows returned (None and empty string values are skipped).

9. **`test_apply_sort_asc`**
   - Rows with numeric `amount` field.
   - Sort by `amount`, order `asc`.
   - Assert: ascending order.

10. **`test_apply_sort_desc`**
    - Same rows, order `desc`.
    - Assert: descending order.

11. **`test_apply_sort_with_none_values`**
    - Rows where some have `amount: None`.
    - Assert: `None` values sort last regardless of asc/desc.

12. **`test_apply_sort_no_sort_by`**
    - `sort_by=None`.
    - Assert: rows unchanged (original order preserved).

13. **`test_apply_pagination`**
    - 10 rows, page 2, page_size 3.
    - Assert: returns rows [3, 4, 5] (0-indexed), total == 10.

14. **`test_apply_pagination_last_page`**
    - 10 rows, page 4, page_size 3.
    - Assert: returns 1 row (index 9), total == 10.

15. **`test_apply_pagination_beyond_last_page`**
    - 10 rows, page 100, page_size 3.
    - Assert: returns empty list, total == 10.

16. **`test_build_query_response_structure`**
    - Assert: returned dict has keys `data`, `total`, `page`, `page_size`, `total_pages`.
    - Assert: `total_pages` computed correctly (e.g., 10 total, page_size 3 -> 4 pages).

17. **`test_handle_query_integration`**
    - 20 rows with varying `tenant_id`, `severity`, and `created_at`.
    - Body: filter by `tenant_id`, sort by `created_at` desc, page 1, page_size 5.
    - Assert: correct filtered count in `total`, correct page of data, correct sort order.

18. **`test_handle_query_defaults`**
    - 3 rows, empty body `{}`.
    - Assert: all 3 rows returned, `total == 3`, `page == 1`, `total_pages == 1`.

### Existing module tests

No changes required to existing unit tests in `de_platform/modules/*/tests/`. The existing GET endpoint handlers are not modified, so their tests remain valid. New POST endpoint handlers follow the same pattern and can be tested in the same test files if desired, but the core logic is tested via `test_query_framework.py`.

---

## Files Changed Summary

### New files
| File | Description |
|------|-------------|
| `de_platform/pipeline/query_framework.py` | Shared query protocol: dataclasses + utility functions |
| `de_platform/pipeline/tests/test_query_framework.py` | Unit tests for query framework |

### Modified files
| File | Changes |
|------|---------|
| `de_platform/modules/data_api/main.py` | Add `POST /api/v1/query/events/{table}` route and `_query_events` handler |
| `de_platform/modules/alert_manager/main.py` | Add `POST /api/v1/query/alerts` and `POST /api/v1/query/cases` routes and handlers |
| `de_platform/modules/data_audit/main.py` | Add `POST /api/v1/query/audit/daily` and `POST /api/v1/query/audit/files` routes and handlers |
| `de_platform/modules/task_scheduler/main.py` | Add `POST /api/v1/query/tasks` and `POST /api/v1/query/runs` routes and handlers |
| `de_platform/modules/client_config/main.py` | Add `POST /api/v1/query/clients` route and handler |
| `ui/src/components/DataTable.tsx` | Add `sortable` to Column interface, add sort/pagination props, render sort indicators and pagination bar |
| `ui/src/api/client.ts` | Add `QueryParams`, `QueryResponse` types and `queryApi` function |
| `ui/src/pages/EventsPage.tsx` | Use `queryApi` with sort/pagination state, pass props to DataTable |
| `ui/src/pages/AlertsPage.tsx` | Use `queryApi` for both alerts and cases tabs with sort/pagination state |
| `ui/src/pages/DataAuditPage.tsx` | Use `queryApi` for daily audit table with sort/pagination state |
| `ui/src/pages/TasksPage.tsx` | Use `queryApi` for tasks and runs with sort/pagination state |
| `ui/src/pages/ClientConfigPage.tsx` | Use `queryApi` for clients list with sort/pagination state |

---

## Verification

1. **Unit tests:** `make test` -- all existing + new `test_query_framework.py` tests pass.
2. **Lint:** `make lint` -- no lint errors in new or modified files.
3. **Manual testing:**
   - Start services (`make infra-up`, then run `data_api`, `alert_manager`, `data_audit`, `task_scheduler`, `client_config`).
   - Open UI at `/ui/`.
   - Verify: clicking a sortable column header toggles asc/desc sorting, sort indicator arrows appear.
   - Verify: pagination bar shows "Showing X-Y of Z", prev/next buttons work, page size dropdown changes page size and resets to page 1.
   - Verify: changing a filter (e.g., tenant ID) resets pagination to page 1.
   - Verify: existing GET endpoints still work (backward compatibility).
   - Verify: `curl -X POST http://localhost:8002/api/v1/query/events/orders -H 'Content-Type: application/json' -d '{"page_size": 5, "sort_by": "transact_time"}'` returns a properly structured paginated response.
