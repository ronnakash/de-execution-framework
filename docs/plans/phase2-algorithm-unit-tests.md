# Phase 2: Algorithm Unit Tests

## Overview

The `algos_rework.md` design doc called for dedicated unit tests at `de_platform/pipeline/tests/test_algorithms.py` but they were never created. Algorithms are tested indirectly through window engine tests and e2e scenarios, but there are no isolated unit tests covering edge cases, threshold overrides, or the `evaluate()` backward-compat wrapper.

**Source:** algos_rework.md section 8.1, changes.md #3 ("really strong unit testing for all edge cases")

## What Exists Today

- `de_platform/pipeline/tests/test_window_engine.py` — tests the window engine, exercises algos indirectly
- `de_platform/modules/batch_algos/tests/test_main.py` — 4 unit tests for batch module
- `de_platform/modules/alert_manager/tests/test_main.py` — tests alert manager, not algo logic
- E2E scenarios (`scenario_large_notional`, `scenario_velocity`, `scenario_suspicious_counterparty`) — happy path only

## File to Create

`de_platform/pipeline/tests/test_algorithms.py`

## Test Cases

### LargeNotionalAlgo

```
test_single_event_above_threshold         — notional > $1M → 1 alert
test_single_event_below_threshold         — notional < $1M → 0 alerts
test_single_event_at_threshold            — notional == $1M → 0 alerts (> not >=)
test_window_multiple_above_threshold      — 3 events above → 3 alerts
test_window_mixed_above_below             — 5 events, 2 above → 2 alerts
test_custom_threshold_override            — thresholds={"threshold_usd": 500_000} overrides default
test_notional_usd_field                   — uses notional_usd field
test_amount_usd_fallback                  — falls back to amount_usd when notional_usd missing
test_non_numeric_notional_skipped         — notional_usd="abc" → no alert, no crash
test_none_notional_skipped                — notional_usd=None → no alert
test_zero_notional                        — notional_usd=0 → no alert
test_negative_notional                    — notional_usd=-500000 → no alert
test_alert_fields_correct                 — verify alert_id, tenant_id, event_type, event_id, algorithm, severity, description, details
test_evaluate_single_event_convenience    — evaluate() wrapper returns Alert or None
```

### VelocityAlgo

```
test_under_threshold_no_alert             — 50 events, max_events=100 → 0 alerts
test_at_threshold_no_alert                — 100 events, max_events=100 → 0 alerts
test_over_threshold_one_alert             — 101 events, max_events=100 → 1 alert
test_trigger_event_is_threshold_breaker   — alert's event_id = events[max_events]
test_returns_at_most_one_alert            — 200 events → still only 1 alert
test_custom_max_events_override           — thresholds={"max_events": 10}
test_custom_window_seconds_override       — thresholds={"window_seconds": 30}
test_empty_window                         — 0 events → 0 alerts
test_single_event                         — 1 event, max_events=100 → 0 alerts
test_alert_severity_is_medium             — verify severity="medium"
test_alert_details_contain_event_count    — details has event_count, window_seconds
test_evaluate_single_event_convenience    — evaluate() with single event
```

### SuspiciousCounterpartyAlgo

```
test_suspicious_counterparty_detected     — event with blocklisted counterparty → 1 alert
test_clean_counterparty_no_alert          — event with non-blocklisted counterparty → 0 alerts
test_empty_blocklist_no_alerts            — default empty set → never alerts
test_multiple_suspicious_in_window        — 3 events with different suspicious IDs → 3 alerts
test_mixed_suspicious_and_clean           — 5 events, 2 suspicious → 2 alerts
test_custom_suspicious_ids_override       — thresholds={"suspicious_ids": ["X"]} overrides constructor
test_missing_counterparty_field           — no counterparty_id key → no alert
test_empty_counterparty_field             — counterparty_id="" → no alert (unless "" in blocklist)
test_alert_severity_is_critical           — verify severity="critical"
test_alert_details_contain_counterparty   — details has counterparty_id
test_evaluate_single_event_convenience    — evaluate() wrapper
```

### Alert Dataclass

```
test_alert_to_dict                        — all fields serialized, details becomes JSON string
test_alert_to_dict_preserves_all_fields   — round-trip: no data loss
```

## Implementation Notes

- Each algo is instantiated directly — no DI container, no external deps
- Events are plain dicts with the fields each algo reads
- Use `datetime.min` / `datetime.max` for window boundaries where irrelevant
- Use fixed UUIDs or mock `uuid.uuid4` for deterministic alert_id assertions where needed
- No infrastructure required — pure unit tests

## Acceptance Criteria

1. All tests pass with `pytest de_platform/pipeline/tests/test_algorithms.py -v`
2. Covers all three algo implementations + Alert dataclass
3. Tests both happy path and edge cases (boundary values, missing fields, type errors)
4. Tests threshold override mechanism for each algo
5. Tests `evaluate()` backward-compat convenience method
