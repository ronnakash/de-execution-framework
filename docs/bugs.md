1. tests/e2e_ui/test_pipeline_data.py::test_audit_page_shows_counts
  received count is 0, why?
2. I see way to many velocity algo alerts, this is probably not good
3. existing e2e tests mostly look at the admin ui instead of viewer UI, we need coverage for both and most importatnly the viewer one
4. transaction/order/execution id missing in events explorer
5. I see screenshots in UI tests when the screenshot is unrelated to the step because it doesn't interact with the ui
6. all UI tables should support sorting by clicking on the field we want to sort by. I also want to look into filtering by each field too, should be robust and support data ranges, constatns filtering ect
7. we need to add an additional_fields field to all event types (orders/executions/transactions) that contains a json with values that can be used in future algos.
8. data audit sums make no sense


bugs from e2e run (2026-02-24, 74/75 passed):

### FAILED: alert_manager case aggregation race condition
`test_multiple_alerts_aggregate_into_case` — ingests 2 large orders expecting 2 alerts aggregated into 1 case. Instead got 2 separate cases each with `alert_count: 1`. Timestamps 8ms apart (`...088469` and `...096663`). Root cause: `_aggregate_into_case()` does SELECT-then-INSERT without a database transaction. Two alerts arrive nearly simultaneously, both find no existing case, both create a new one. The `db.transaction()` context manager exists in PostgresDatabase but is never used in alert_manager. The DELETE+INSERT update pattern in `_add_alert_to_case()` is also non-atomic (brief window where the case disappears).

### Report observability is mostly blind in E2E
- **Kafka deltas always empty** — `TestDiagnostics` only reads Kafka state from `MemoryQueue` (unit tests). For real Kafka in E2E, it's intentionally skipped ("cannot be tenant-scoped"). This means if Kafka drops messages or a topic is stuck, the report won't show it.
- **Metrics deltas always empty** — no `prometheus_endpoints` are configured when `TestDiagnostics` is constructed in `RealInfraHarness`, so `snap.metrics` is always `{}`. Pipeline metrics like `events_processed_total`, `alerts_generated_total` are emitted but never captured in the report.
- **postgres.alerts always -1** — `diagnostics.py:168` calls the sync `fetch_all()` on `PostgresDatabase`, which internally does `asyncio.get_event_loop().run_until_complete()`. This fails from within an async test (event loop already running), caught by the bare `except`, sets count to -1. The warnings confirm: `RuntimeWarning: coroutine 'PostgresDatabase.fetch_all_async' was never awaited`. So we have zero Postgres alert visibility in E2E reports.

### ClickHouse delta row counts don't add up
Several passing tests show fewer rows in the step deltas than were actually ingested. The tests pass because `wait_for_rows()` polls until the expected count is reached, but the step-boundary snapshots miss rows due to ClickHouse's eventual consistency (merge-tree inserts not instantly queryable). Examples:
- `test_valid_events[files-order]`: delta sum = 98, expected 100
- `test_valid_events[rest-execution]`: delta sum = 98, expected 100
- `test_duplicate_events[kafka-execution]`: duplicate delta sum = 97, expected 99
- `test_invalid_events[rest-transaction]`: delta sum = 98, expected 100
- `test_velocity_algorithm`: delta sum = 53 orders, 151 were ingested (persistence far behind burst ingestion)
- `test_multi_error_consolidation[rest-execution]`, `[files-execution]`, `[files-transaction]`, `[kafka-transaction]`, `[kafka-execution]`: all show 9 errors in deltas, 10 expected

### Zero DB deltas in alert/algo tests
Multiple alert tests show `db: {}` in ALL steps even though they successfully ingest orders and create alerts:
- `test_alert_creates_case`: both steps show zero DB activity — 0.98s total, suggests it may be reading data created by a prior test's tenant isolation or the pipeline is fast enough that the order+alert are created between snapshot boundaries
- `test_alert_generation[rest]`, `[kafka]`: both steps show `db: {}` — alerts confirmed via API but no DB delta recorded
- `test_large_notional_algorithm`, `test_suspicious_counterparty_algorithm`: same pattern — zero deltas despite confirmed alerts. This is partly explained by the postgres.alerts=-1 bug above (Postgres alerts can never show up in deltas), but ClickHouse orders should still appear and sometimes don't