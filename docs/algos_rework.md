# Algorithms Rework Design

Phase 5 design doc. Shared sliding-window framework for realtime and batch fraud detection, plus a dedicated alert management service with case aggregation.

---

## Design Principles

1. **In-memory algorithms** — All algorithm implementations must be pure in-memory computations. No external service calls (Redis, DB) during `evaluate_window()`. This maximizes processing speed and makes algos trivially testable.
2. **Shared framework** — Realtime (streaming) and batch (historical) modes share the same sliding window engine and algorithm implementations. The only difference is the event source.
3. **Per-tenant configuration** — Window size, slide interval, algo thresholds, and enabled algos are per-tenant via `ClientConfigCache`. Unconfigured tenants get sensible defaults.
4. **Single responsibility for alert persistence** — A dedicated alert management service owns the entire alert lifecycle: dedup, persistence, case aggregation, and query API. Algo modules only publish to Kafka.

---

## Architecture Overview

```
                    ┌──────────────────────────────┐
                    │      FraudAlgorithm ABC       │
                    │  evaluate_window(events, ...) │
                    └──────────┬───────────────────┘
                               │ implements
              ┌────────────────┼────────────────┐
              │                │                │
      LargeNotionalAlgo  VelocityAlgo  SuspiciousCounterpartyAlgo
       (pure in-memory)   (pure in-memory)    (pure in-memory)

                    ┌──────────────────────────────┐
                    │     SlidingWindowEngine       │
                    │  ingest(event) / run(events)  │
                    │  manages per-tenant buffers   │
                    │  calls evaluate_window()      │
                    └──────────┬───────────────────┘
                               │ used by
                    ┌──────────┴───────────┐
                    │                      │
            AlgosModule             BatchAlgosModule
          (realtime/streaming)     (historical/batch)
          events from Kafka        events from ClickHouse
                    │                      │
                    └──────────┬───────────┘
                               │ publish to Kafka
                    ┌──────────▼───────────────────┐
                    │   Alert Management Service    │
                    │  Kafka consume → dedup →      │
                    │  Postgres insert → aggregate  │
                    │  into cases → REST API        │
                    └──────────────────────────────┘
```

---

## Alert Pipeline Flow

### Current flow (being replaced)

```
algos → Kafka (alerts topic)
algos → Postgres (direct write)          ← double-write, no dedup
data_api → Kafka (consume) → Postgres    ← second writer, catches dup errors
data_api → REST API (serves alerts)      ← mixed responsibility
```

### New flow

```
algos/batch_algos → Kafka (alerts topic) only     ← single publish, no DB write
alert_manager → Kafka (consume) → dedup → Postgres → aggregate into cases
alert_manager → REST API (alerts + cases)          ← dedicated service
data_api → REST API (events only)                  ← no alert responsibility
```

**What changes:**
- `AlgosModule` and `BatchAlgosModule` only publish alerts to Kafka — no direct DB writes
- `data_api` loses alert consumption and alert REST endpoints
- New `alert_manager` service owns the entire alert lifecycle
- UI queries alerts and cases from `alert_manager` (proxied through `data_api` gateway or direct)

---

## 1. Algorithm Interface Redesign

### File: `de_platform/pipeline/algorithms.py`

### 1.1 New `FraudAlgorithm` base class

```python
class FraudAlgorithm(ABC):
    @abstractmethod
    def name(self) -> str:
        """Short identifier for this algorithm."""
        ...

    @abstractmethod
    def evaluate_window(
        self,
        events: list[dict[str, Any]],
        tenant_id: str,
        window_start: datetime,
        window_end: datetime,
        thresholds: dict[str, Any] | None = None,
    ) -> list[Alert]:
        """Evaluate a time-ordered batch of events within a window.

        Must be a pure in-memory computation — no external service calls.
        Returns zero or more Alerts.
        """
        ...

    def evaluate(
        self, event: dict[str, Any], thresholds: dict[str, Any] | None = None,
    ) -> Alert | None:
        """Legacy single-event convenience. Delegates to evaluate_window()."""
        alerts = self.evaluate_window(
            [event],
            event.get("tenant_id", ""),
            datetime.min,
            datetime.max,
            thresholds=thresholds,
        )
        return alerts[0] if alerts else None
```

**Key changes:**
- `evaluate_window()` is the primary abstract method, receives a list of events
- `evaluate()` becomes a concrete convenience method (backward-compatible)
- `thresholds` parameter moves into `evaluate_window()` signature
- No `CacheInterface` in any algo constructor — all state comes from the events list

### 1.2 Rewritten algorithms

**LargeNotionalAlgo** — Scan all events in window, alert on any exceeding threshold:

```python
class LargeNotionalAlgo(FraudAlgorithm):
    def __init__(self, threshold_usd: float = 1_000_000) -> None:
        self.threshold_usd = threshold_usd

    def name(self) -> str:
        return "large_notional"

    def evaluate_window(
        self,
        events: list[dict[str, Any]],
        tenant_id: str,
        window_start: datetime,
        window_end: datetime,
        thresholds: dict[str, Any] | None = None,
    ) -> list[Alert]:
        threshold = (thresholds or {}).get("threshold_usd", self.threshold_usd)
        alerts = []
        for event in events:
            notional_usd = event.get("notional_usd") or event.get("amount_usd", 0)
            try:
                if float(notional_usd) > threshold:
                    alerts.append(self._make_alert(event, threshold))
            except (TypeError, ValueError):
                pass
        return alerts
```

**VelocityAlgo** — Count events in the window (pure in-memory, no Redis):

```python
class VelocityAlgo(FraudAlgorithm):
    def __init__(self, max_events: int = 100, window_seconds: int = 60) -> None:
        self.max_events = max_events
        self.window_seconds = window_seconds

    def name(self) -> str:
        return "velocity"

    def evaluate_window(
        self,
        events: list[dict[str, Any]],
        tenant_id: str,
        window_start: datetime,
        window_end: datetime,
        thresholds: dict[str, Any] | None = None,
    ) -> list[Alert]:
        max_events = (thresholds or {}).get("max_events", self.max_events)
        window_seconds = (thresholds or {}).get("window_seconds", self.window_seconds)

        if len(events) > max_events:
            # Alert on the event that crossed the threshold
            trigger_event = events[max_events]  # 0-indexed, so this is event #max_events+1
            return [Alert(
                alert_id=uuid.uuid4().hex,
                tenant_id=tenant_id,
                event_type=trigger_event.get("event_type", ""),
                event_id=trigger_event.get("id", ""),
                message_id=trigger_event.get("message_id", ""),
                algorithm=self.name(),
                severity="medium",
                description=(
                    f"Tenant {tenant_id!r} exceeded {max_events} events"
                    f" in {window_seconds}s window"
                ),
                details={
                    "event_count": len(events),
                    "window_seconds": window_seconds,
                    "window_start": window_start.isoformat(),
                    "window_end": window_end.isoformat(),
                },
                created_at=_now_iso(),
            )]
        return []
```

**Note:** VelocityAlgo no longer depends on `CacheInterface`. The constructor drops the `cache` parameter. The event count comes directly from the window contents.

**SuspiciousCounterpartyAlgo** — Check each event's counterparty against blocklist:

```python
class SuspiciousCounterpartyAlgo(FraudAlgorithm):
    def __init__(self, suspicious_ids: set[str] | None = None) -> None:
        self.suspicious_ids = suspicious_ids or set()

    def name(self) -> str:
        return "suspicious_counterparty"

    def evaluate_window(
        self,
        events: list[dict[str, Any]],
        tenant_id: str,
        window_start: datetime,
        window_end: datetime,
        thresholds: dict[str, Any] | None = None,
    ) -> list[Alert]:
        suspicious_ids = self.suspicious_ids
        if thresholds and "suspicious_ids" in thresholds:
            suspicious_ids = set(thresholds["suspicious_ids"])

        alerts = []
        for event in events:
            counterparty_id = event.get("counterparty_id", "")
            if counterparty_id in suspicious_ids:
                alerts.append(self._make_alert(event, counterparty_id))
        return alerts
```

### 1.3 Impact on existing callers

The `evaluate()` convenience method means all existing test code and the single-event path in the realtime module continue to work without changes during migration. The algos module will transition to using the window engine which calls `evaluate_window()` directly.

---

## 2. Sliding Window Engine

### File: `de_platform/pipeline/window_engine.py` (new)

The `SlidingWindowEngine` is the shared core that both realtime and batch algos use. It manages per-tenant event buffers, tracks window positions, and dispatches evaluation.

### 2.1 WindowConfig

```python
@dataclass
class WindowConfig:
    """Sliding window parameters for a tenant."""
    window_size_minutes: int = 5
    window_slide_minutes: int = 1

    @property
    def window_size(self) -> timedelta:
        return timedelta(minutes=self.window_size_minutes)

    @property
    def window_slide(self) -> timedelta:
        return timedelta(minutes=self.window_slide_minutes)
```

Where does this config come from?
- **Realtime:** `ClientConfigCache` (from client_config service, real-time pub-sub updates). Window config is stored at the client level in the `clients` table (see section 7).
  If not set, defaults apply (5 min / 1 min).
- **Batch:** Snapshot from `ClientConfigCache` at job start. Does not change during the run.

### 2.2 TenantWindowState

```python
@dataclass
class TenantWindowState:
    """Per-tenant state maintained by the engine."""
    events: list[dict[str, Any]]  # time-ordered buffer
    last_window_end: datetime | None  # end of last evaluated window
```

### 2.3 SlidingWindowEngine

```python
class SlidingWindowEngine:
    """Manages per-tenant sliding windows and dispatches algorithm evaluation.

    Shared by both realtime (AlgosModule) and batch (BatchAlgosModule).
    """

    def __init__(
        self,
        algorithms: list[FraudAlgorithm],
        get_window_config: Callable[[str], WindowConfig],
        get_algo_config: Callable[[str, str], tuple[bool, dict]],
    ) -> None:
        """
        Args:
            algorithms: List of all registered algorithm instances.
            get_window_config: Callable that returns WindowConfig for a tenant_id.
                              Allows the engine to be decoupled from ClientConfigCache.
            get_algo_config: Callable(tenant_id, algo_name) -> (enabled, thresholds).
                            Returns whether the algo is enabled and its threshold overrides.
        """
        self.algorithms = algorithms
        self.get_window_config = get_window_config
        self.get_algo_config = get_algo_config
        self._tenants: dict[str, TenantWindowState] = defaultdict(
            lambda: TenantWindowState(events=[], last_window_end=None)
        )

    # ── Realtime path ─────────────────────────────────────────────────────

    def ingest(self, event: dict[str, Any]) -> list[Alert]:
        """Add a single event (from Kafka) and evaluate if window is ready.

        Returns any alerts produced. Used by the realtime AlgosModule.
        """
        tenant_id = event.get("tenant_id", "")
        state = self._tenants[tenant_id]

        # Insert in time-sorted order
        state.events.append(event)
        state.events.sort(key=lambda e: self._parse_time(e.get("transact_time", "")))

        # Check if buffer spans enough time for a window
        config = self.get_window_config(tenant_id)
        alerts = self._try_evaluate(tenant_id, state, config)

        return alerts

    # ── Batch path ────────────────────────────────────────────────────────

    def run_batch(
        self,
        events: list[dict[str, Any]],
        tenant_id: str,
        start_time: datetime,
        end_time: datetime,
    ) -> list[Alert]:
        """Evaluate sliding windows across a pre-loaded batch of events.

        Events must be time-ordered. Used by the batch BatchAlgosModule.
        Returns all alerts for the entire date range.
        """
        config = self.get_window_config(tenant_id)
        all_alerts: list[Alert] = []

        window_start = start_time
        while window_start + config.window_size <= end_time:
            window_end = window_start + config.window_size

            # Slice events within this window
            window_events = [
                e for e in events
                if window_start <= self._parse_time(e.get("transact_time", "")) < window_end
            ]

            if window_events:
                alerts = self._evaluate_window(
                    tenant_id, window_events, window_start, window_end,
                )
                all_alerts.extend(alerts)

            window_start += config.window_slide

        return all_alerts

    # ── Shared evaluation logic ───────────────────────────────────────────

    def _try_evaluate(
        self, tenant_id: str, state: TenantWindowState, config: WindowConfig,
    ) -> list[Alert]:
        """Check if the buffer spans a full window; if so, evaluate and slide."""
        if not state.events:
            return []

        # window_size=0 means evaluate immediately on every event
        if config.window_size == timedelta(0):
            latest = state.events[-1]
            now = self._parse_time(latest.get("transact_time", ""))
            alerts = self._evaluate_window(tenant_id, [latest], now, now)
            # Don't buffer — clear after immediate evaluation
            state.events.clear()
            return alerts

        earliest = self._parse_time(state.events[0].get("transact_time", ""))
        latest = self._parse_time(state.events[-1].get("transact_time", ""))
        span = latest - earliest

        if span < config.window_size:
            return []  # not enough data yet

        # Determine window boundaries
        if state.last_window_end is None:
            window_start = earliest
        else:
            window_start = state.last_window_end  # slide forward
        window_end = window_start + config.window_size

        # Slice events in window
        window_events = [
            e for e in state.events
            if window_start <= self._parse_time(e.get("transact_time", "")) < window_end
        ]

        alerts = self._evaluate_window(tenant_id, window_events, window_start, window_end)

        # Slide: advance and prune old events
        state.last_window_end = window_start + config.window_slide
        cutoff = state.last_window_end - config.window_size
        state.events = [
            e for e in state.events
            if self._parse_time(e.get("transact_time", "")) >= cutoff
        ]

        return alerts

    def _evaluate_window(
        self,
        tenant_id: str,
        events: list[dict[str, Any]],
        window_start: datetime,
        window_end: datetime,
    ) -> list[Alert]:
        """Run all enabled algorithms on a window of events."""
        alerts: list[Alert] = []
        for algo in self.algorithms:
            enabled, thresholds = self.get_algo_config(tenant_id, algo.name())
            if not enabled:
                continue
            algo_alerts = algo.evaluate_window(
                events, tenant_id, window_start, window_end,
                thresholds=thresholds or None,
            )
            alerts.extend(algo_alerts)
        return alerts

    @staticmethod
    def _parse_time(value: str | datetime) -> datetime:
        """Parse ISO timestamp string to datetime. Pass-through for datetime."""
        if isinstance(value, datetime):
            return value
        try:
            return datetime.fromisoformat(value)
        except (ValueError, TypeError):
            return datetime.min
```

### 2.4 Key design decisions

**Why a callable for config instead of direct `ClientConfigCache` reference?**
- Decouples the engine from the cache implementation
- In unit tests, pass a lambda that returns hardcoded configs
- In realtime, pass `config_cache.get_window_config`
- In batch, pass a snapshot function

**Why sort on ingest?**
- Kafka doesn't guarantee cross-partition ordering
- Events from different ingestion paths may arrive out of order
- Sorting ensures the window always sees time-ordered data

**Immediate evaluation (window_size=0):**
- Tenants that want per-event alerting (e.g., `LargeNotionalAlgo` doesn't inherently need a window) set `window_size_minutes=0`
- The engine evaluates immediately on every event, no buffering
- This keeps the interface uniform — no special "single-event mode" flag

**Memory management (realtime):**
- Events older than `last_window_end - window_size` are pruned after each evaluation
- For a 5-min window with 1-min slide, at most ~6 minutes of events are held per tenant
- With high-volume tenants (1000 events/sec), that's ~360K events max per tenant
- If this becomes a concern, we can add a max buffer size that forces evaluation

---

## 3. Realtime Algos Module (modified)

### File: `de_platform/modules/algos/main.py`

### 3.1 Changes

The existing `AlgosModule` is modified to:
- Use `SlidingWindowEngine` instead of directly calling `algo.evaluate()` per event
- **Only publish to Kafka** — remove the direct `db.insert_one_async("alerts", ...)` call
- Drop `DatabaseInterface` dependency (no longer writes to Postgres)

```python
class AlgosModule(Module):
    def __init__(
        self,
        config: ModuleConfig,
        logger: LoggerFactory,
        mq: MessageQueueInterface,
        cache: CacheInterface,
        lifecycle: LifecycleManager,
        metrics: MetricsInterface,
    ) -> None:
        # NOTE: no `db: DatabaseInterface` — algos no longer write to Postgres
        self.config = config
        self.logger = logger
        self.mq = mq
        self.cache = cache
        self.lifecycle = lifecycle
        self.metrics = metrics

    async def initialize(self) -> None:
        self.log = self.logger.create()

        suspicious_ids_str = self.config.get("suspicious-counterparty-ids", "")
        suspicious_ids = {x.strip() for x in suspicious_ids_str.split(",") if x.strip()}

        # Algorithms: all pure in-memory, no external deps
        self.algorithms = [
            LargeNotionalAlgo(threshold_usd=1_000_000),
            VelocityAlgo(max_events=100, window_seconds=60),
            SuspiciousCounterpartyAlgo(suspicious_ids=suspicious_ids),
        ]

        self.config_cache = ClientConfigCache(self.cache)
        self.config_cache.start()
        self.lifecycle.on_shutdown(lambda: self.config_cache.stop())

        # Initialize the window engine
        self.engine = SlidingWindowEngine(
            algorithms=self.algorithms,
            get_window_config=self._get_window_config,
            get_algo_config=self._get_algo_config,
        )

    def _get_window_config(self, tenant_id: str) -> WindowConfig:
        """Read window config from ClientConfigCache. Falls back to defaults."""
        config = self.config_cache._get_client(tenant_id)
        if config:
            return WindowConfig(
                window_size_minutes=config.get("window_size_minutes", 5),
                window_slide_minutes=config.get("window_slide_minutes", 1),
            )
        return WindowConfig()

    def _get_algo_config(self, tenant_id: str, algo_name: str) -> tuple[bool, dict]:
        """Read per-algo enabled/thresholds from ClientConfigCache."""
        enabled = self.config_cache.is_algo_enabled(tenant_id, algo_name)
        thresholds = self.config_cache.get_algo_thresholds(tenant_id, algo_name)
        return enabled, thresholds

    async def _evaluate(self, event: dict[str, Any]) -> None:
        """Ingest one event into the window engine; publish any resulting alerts."""
        tenant_id = event.get("tenant_id", "")
        self.metrics.counter("events_received_total", tags={
            "service": "algos", "tenant_id": tenant_id,
        })

        alerts = self.engine.ingest(event)

        for alert in alerts:
            alert_dict = alert.to_dict()
            msg_key = f"{tenant_id}:{alert.event_id}" if tenant_id else None
            self.mq.publish(ALERTS, alert_dict, key=msg_key)
            self.metrics.counter("alerts_generated_total", tags={
                "algorithm": alert.algorithm,
                "severity": alert.severity,
                "tenant_id": tenant_id,
            })
            self.log.info("Alert published", algorithm=alert.algorithm,
                          tenant_id=tenant_id, event_id=alert.event_id)
```

---

## 4. Batch Algos Module (new)

### File: `de_platform/modules/batch_algos/main.py`

### 4.1 Module structure

```
de_platform/modules/batch_algos/
    __init__.py
    main.py          # BatchAlgosModule(Module) — job type
    module.json      # type: "job"
    tests/
        __init__.py
        test_main.py
```

### 4.2 module.json

```json
{
  "name": "batch_algos",
  "display_name": "Batch Fraud Algos",
  "description": "Run fraud detection algorithms on historical ClickHouse data using sliding windows",
  "type": "job",
  "args": [
    {
      "name": "tenant-id",
      "description": "Tenant to process",
      "required": true,
      "type": "string"
    },
    {
      "name": "start-date",
      "description": "Start date (YYYY-MM-DD)",
      "required": true,
      "type": "string"
    },
    {
      "name": "end-date",
      "description": "End date (YYYY-MM-DD)",
      "required": true,
      "type": "string"
    },
    {
      "name": "suspicious-counterparty-ids",
      "description": "Comma-separated suspicious counterparty IDs",
      "required": false,
      "type": "string",
      "default": ""
    }
  ]
}
```

### 4.3 CLI usage

```bash
python -m de_platform run batch_algos \
    --db events=clickhouse --cache redis --mq kafka \
    --tenant-id acme --start-date 2026-01-01 --end-date 2026-01-31
```

### 4.4 Module implementation

```python
class BatchAlgosModule(Module):
    def __init__(
        self,
        config: ModuleConfig,
        logger: LoggerFactory,
        db_factory: DatabaseFactory,
        cache: CacheInterface,
        mq: MessageQueueInterface,
        metrics: MetricsInterface,
    ) -> None:
        self.config = config
        self.logger = logger
        self.db_factory = db_factory
        self.cache = cache
        self.mq = mq
        self.metrics = metrics

    async def initialize(self) -> None:
        self.log = self.logger.create()
        self.events_db = self.db_factory.get("events")   # ClickHouse
        await self.events_db.connect_async()

        suspicious_ids_str = self.config.get("suspicious-counterparty-ids", "")
        suspicious_ids = {x.strip() for x in suspicious_ids_str.split(",") if x.strip()}

        self.algorithms = [
            LargeNotionalAlgo(),
            VelocityAlgo(),
            SuspiciousCounterpartyAlgo(suspicious_ids=suspicious_ids),
        ]

        # Snapshot config at job start
        self.config_cache = ClientConfigCache(self.cache)

        self.engine = SlidingWindowEngine(
            algorithms=self.algorithms,
            get_window_config=self._get_window_config,
            get_algo_config=self._get_algo_config,
        )

    async def execute(self) -> int:
        tenant_id = self.config.get("tenant-id")
        start_date = self.config.get("start-date")
        end_date = self.config.get("end-date")

        start_dt = datetime.fromisoformat(f"{start_date}T00:00:00+00:00")
        end_dt = datetime.fromisoformat(f"{end_date}T23:59:59+00:00")

        self.log.info("Batch algos starting", tenant_id=tenant_id,
                      start_date=start_date, end_date=end_date)

        # 1. Load all events from ClickHouse
        events = await self._load_events(tenant_id, start_date, end_date)
        self.log.info("Events loaded", tenant_id=tenant_id, count=len(events))

        if not events:
            self.log.info("No events in range, exiting")
            return 0

        # 2. Sort by transact_time
        events.sort(key=lambda e: e.get("transact_time", ""))

        # 3. Run sliding window engine
        alerts = self.engine.run_batch(events, tenant_id, start_dt, end_dt)
        self.log.info("Window evaluation complete", alert_count=len(alerts))

        # 4. In-run dedup (overlapping windows produce duplicate alerts)
        alerts = self._dedup_in_run(alerts)
        self.log.info("After in-run dedup", alert_count=len(alerts))

        # 5. Publish to Kafka — alert_manager handles DB dedup and persistence
        for alert in alerts:
            alert_dict = alert.to_dict()
            msg_key = f"{tenant_id}:{alert.event_id}" if tenant_id else None
            self.mq.publish(ALERTS, alert_dict, key=msg_key)

        self.log.info("Batch algos complete", tenant_id=tenant_id,
                      alerts_published=len(alerts))
        return 0

    async def _load_events(self, tenant_id: str, start: str, end: str) -> list[dict]:
        """Query ClickHouse for all event types in the date range."""
        all_events = []
        for table in ["orders", "executions", "transactions"]:
            rows = await self.events_db.fetch_all_async(
                f"SELECT * FROM {table} WHERE tenant_id = $1 "
                f"AND transact_time >= $2 AND transact_time <= $3",
                [tenant_id, f"{start}T00:00:00", f"{end}T23:59:59"],
            )
            all_events.extend(rows)
        return all_events

    def _dedup_in_run(self, alerts: list[Alert]) -> list[Alert]:
        """Remove duplicate alerts within the same batch run.

        An event may appear in overlapping windows and trigger the same algo
        multiple times. Deduplicate by (algorithm, event_id).
        """
        seen: set[tuple[str, str]] = set()
        unique: list[Alert] = []
        for alert in alerts:
            key = (alert.algorithm, alert.event_id)
            if key not in seen:
                seen.add(key)
                unique.append(alert)
        return unique
```

**Note:** Batch algos does NOT write to Postgres or dedup against the DB. It publishes to Kafka only. The alert management service (section 5) handles cross-run dedup and persistence. This keeps batch_algos simple and the alert lifecycle in one place.

### 4.5 Performance characteristics

For a full day of data with 5-min windows and 1-min slide:
- 24h = 1440 minutes, sliding by 1 min = **1436 window positions**
- Each window slices from the already-loaded in-memory list (no DB round-trips)
- For 100K events/day, each window contains ~350 events on average
- Total evaluations: 1436 windows x N algorithms — pure CPU, should complete in seconds

---

## 5. Alert Management Service (new)

### Overview

The alert management service is the single owner of alert persistence, deduplication, and case aggregation. It replaces the alert-related responsibilities currently split between `algos` (direct DB write) and `data_api` (Kafka consume + DB write + REST API for alerts).

### 5.1 Module structure

```
de_platform/modules/alert_manager/
    __init__.py
    main.py          # AlertManagerModule(Module) — service type
    module.json      # type: "service"
    tests/
        __init__.py
        test_main.py
```

Port: **8007** (configurable via `--port`)

### 5.2 Responsibilities

1. **Kafka consumer** — Reads from the `alerts` topic
2. **Deduplication** — Checks `(algorithm, event_id)` against existing alerts before inserting
3. **Persistence** — Inserts deduplicated alerts into the `alerts` Postgres table
4. **Case aggregation** — Groups related alerts into cases (see section 6)
5. **REST API** — Serves alert and case query endpoints

### 5.3 Database schema

Reuses the existing `alerts` Postgres table. Adds a new `cases` table and a join table.

New migration namespace: `alert_manager`

```sql
-- de_platform/migrations/alert_manager/001_create_cases.up.sql

CREATE TABLE IF NOT EXISTS cases (
    case_id         TEXT PRIMARY KEY,
    tenant_id       TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'open',
    severity        TEXT NOT NULL DEFAULT 'medium',
    title           TEXT NOT NULL,
    description     TEXT,
    alert_count     INTEGER NOT NULL DEFAULT 0,
    first_alert_at  TIMESTAMP NOT NULL,
    last_alert_at   TIMESTAMP NOT NULL,
    algorithms      TEXT[] NOT NULL DEFAULT '{}',
    created_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_cases_tenant_id ON cases(tenant_id);
CREATE INDEX idx_cases_status ON cases(status);

-- Join table: which alerts belong to which case
CREATE TABLE IF NOT EXISTS case_alerts (
    case_id         TEXT NOT NULL REFERENCES cases(case_id) ON DELETE CASCADE,
    alert_id        TEXT NOT NULL REFERENCES alerts(alert_id) ON DELETE CASCADE,
    PRIMARY KEY (case_id, alert_id)
);
```

**Case statuses:** `open` → `investigating` → `resolved` | `dismissed`

**Case severity:** The highest severity among its constituent alerts. Updated when new alerts are added.

### 5.4 REST API endpoints

```
# Alerts
GET    /api/v1/alerts                            — list alerts (tenant-scoped)
GET    /api/v1/alerts/{alert_id}                 — get alert detail
GET    /api/v1/alerts/{alert_id}/case            — get the case this alert belongs to

# Cases
GET    /api/v1/cases                             — list cases (tenant-scoped)
GET    /api/v1/cases/{case_id}                   — get case detail + its alerts
PUT    /api/v1/cases/{case_id}/status            — update case status (investigating/resolved/dismissed)
GET    /api/v1/cases/summary                     — aggregate stats: open cases, by severity, by algo
```

**Query parameters for list endpoints:**
- `tenant_id` — filter by tenant (from JWT if auth active, or query param)
- `severity` — filter by severity
- `algorithm` — filter by algorithm
- `status` — filter by case status
- `start_date` / `end_date` — filter by time range
- `limit` / `offset` — pagination

### 5.5 Module implementation

```python
class AlertManagerModule(Module):
    def __init__(
        self,
        config: ModuleConfig,
        logger: LoggerFactory,
        mq: MessageQueueInterface,
        db: DatabaseInterface,       # Postgres (alerts + cases tables)
        lifecycle: LifecycleManager,
        metrics: MetricsInterface,
        secrets: SecretsInterface,
    ) -> None:
        self.config = config
        self.logger = logger
        self.mq = mq
        self.db = db
        self.lifecycle = lifecycle
        self.metrics = metrics
        self.secrets = secrets

    async def initialize(self) -> None:
        self.log = self.logger.create()
        self.port = self.config.get("port", 8007)
        await self.db.connect_async()
        self.lifecycle.on_shutdown(self._stop_server)
        self.lifecycle.on_shutdown(self.db.disconnect_async)

        # Load existing alert keys for dedup (in-memory set)
        await self._load_dedup_set()

        self.log.info("Alert Manager initialized", port=self.port)

    async def execute(self) -> int:
        app = self._create_app()
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "0.0.0.0", int(self.port))
        await site.start()
        self.log.info("Alert Manager listening", port=self.port)

        while not self.lifecycle.is_shutting_down:
            try:
                await self._consume_and_process()
            except Exception as exc:
                self.log.error("Processing error", error=str(exc))
            await asyncio.sleep(0.01)

        return 0

    async def _consume_and_process(self) -> None:
        """Consume one alert from Kafka, dedup, persist, and aggregate."""
        msg = self.mq.consume_one(ALERTS)
        if not msg:
            return

        alert_id = msg.get("alert_id", "")
        algorithm = msg.get("algorithm", "")
        event_id = msg.get("event_id", "")
        tenant_id = msg.get("tenant_id", "")
        dedup_key = (algorithm, event_id)

        # Layer 1: Dedup — skip if (algorithm, event_id) already exists
        if dedup_key in self._known_alerts:
            self.log.debug("Alert deduplicated", alert_id=alert_id,
                           algorithm=algorithm, event_id=event_id)
            self.metrics.counter("alerts_deduplicated_total", tags={
                "service": "alert_manager", "tenant_id": tenant_id,
            })
            return

        # Layer 2: Persist alert
        db_row = self._prepare_db_row(msg)
        await self.db.insert_one_async("alerts", db_row)
        self._known_alerts.add(dedup_key)

        self.metrics.counter("alerts_persisted_total", tags={
            "service": "alert_manager", "tenant_id": tenant_id,
            "algorithm": algorithm, "severity": msg.get("severity", ""),
        })

        # Layer 3: Aggregate into case
        await self._aggregate_into_case(msg)

        self.log.info("Alert processed", alert_id=alert_id,
                      tenant_id=tenant_id, algorithm=algorithm)

    async def _load_dedup_set(self) -> None:
        """Load existing (algorithm, event_id) pairs from DB into memory for fast dedup."""
        self._known_alerts: set[tuple[str, str]] = set()
        rows = await self.db.fetch_all_async("SELECT * FROM alerts")
        for row in rows:
            self._known_alerts.add((row.get("algorithm", ""), row.get("event_id", "")))
        self.log.info("Dedup set loaded", count=len(self._known_alerts))
```

### 5.6 Changes to data_api

The `data_api` module is simplified:
- **Remove** `_consume_alerts()` — no longer consumes from the alerts topic
- **Remove** alert REST endpoints (`/api/v1/alerts`, `/api/v1/alerts/{alert_id}`)
- **Keep** event REST endpoints (`/api/v1/events/*`) and static UI serving
- Alert/case endpoints are served by `alert_manager` and proxied through `data_api` gateway (if using the gateway pattern from Phase 8)

---

## 6. Case Aggregation Logic

### 6.1 What is a Case?

A **case** is a group of related alerts that together describe a potential fraud pattern for an analyst to review. Instead of triaging hundreds of individual alerts, analysts work with cases that provide context about the scope and nature of suspicious activity.

### 6.2 Aggregation rules

When a new alert arrives, the alert manager determines which case it belongs to using the following rules evaluated in order:

**Rule 1 — Same tenant, same algorithm, within aggregation window:**

```
IF there exists an open case for this tenant_id
   AND the case contains alerts from the same algorithm
   AND the case's last_alert_at is within the aggregation window (default: 1 hour)
THEN add this alert to that case
```

This groups bursts of the same type of alert together. For example, 50 velocity alerts for tenant "acme" within an hour become one case titled "Velocity threshold exceeded (50 alerts)".

**Rule 2 — Same tenant, different algorithms, overlapping events:**

```
IF there exists an open case for this tenant_id
   AND the case contains an alert referencing the same event_id
THEN add this alert to that case
```

This groups alerts about the same event. For example, one large trade triggers both `large_notional` and `velocity` — they belong to the same case because they concern the same underlying activity.

**Rule 3 — No matching case:**

```
Create a new case with this alert as the first entry.
```

### 6.3 Aggregation window

The aggregation window is configurable per-tenant in the `clients` table:

```sql
-- Added to migration 003
ALTER TABLE clients
    ADD COLUMN case_aggregation_minutes INTEGER NOT NULL DEFAULT 60;
```

Default: 60 minutes. After this window expires without new matching alerts, a new case is created for subsequent alerts. This prevents cases from growing indefinitely.

### 6.4 Case properties (computed)

When adding an alert to a case, the following are updated:

| Property | Computation |
|----------|-------------|
| `severity` | `max(case.severity, alert.severity)` using order: low < medium < high < critical |
| `alert_count` | Incremented by 1 |
| `last_alert_at` | Updated to alert's `created_at` |
| `algorithms` | Union of existing + new alert's algorithm |
| `title` | Auto-generated (see below) |
| `updated_at` | Current timestamp |

### 6.5 Auto-generated case titles

Cases get a human-readable title based on their contents:

- Single algorithm: `"Large notional detected — {tenant_id} ({alert_count} alerts)"`
- Multiple algorithms: `"Multiple fraud signals — {tenant_id} ({alert_count} alerts, {len(algorithms)} algorithms)"`
- Single event, multiple algos: `"Multi-signal alert on event {event_id} — {tenant_id}"`

Title is regenerated when alerts are added.

### 6.6 Implementation

```python
# In AlertManagerModule

_SEVERITY_ORDER = {"low": 0, "medium": 1, "high": 2, "critical": 3}

async def _aggregate_into_case(self, alert: dict) -> None:
    """Find or create a case for this alert."""
    tenant_id = alert.get("tenant_id", "")
    algorithm = alert.get("algorithm", "")
    event_id = alert.get("event_id", "")
    alert_id = alert.get("alert_id", "")
    severity = alert.get("severity", "medium")
    created_at = alert.get("created_at")

    # Try to find an existing open case to attach to
    case = await self._find_matching_case(tenant_id, algorithm, event_id)

    if case:
        # Add alert to existing case
        await self._add_alert_to_case(case, alert_id, severity, algorithm, created_at)
    else:
        # Create new case
        await self._create_case(tenant_id, alert_id, severity, algorithm, created_at)

async def _find_matching_case(
    self, tenant_id: str, algorithm: str, event_id: str,
) -> dict | None:
    """Find an open case matching the aggregation rules."""
    open_cases = await self.db.fetch_all_async("SELECT * FROM cases")
    open_cases = [
        c for c in open_cases
        if c.get("tenant_id") == tenant_id and c.get("status") == "open"
    ]

    if not open_cases:
        return None

    # Rule 2: same event_id in case alerts (cross-algorithm grouping)
    for case in open_cases:
        case_alert_rows = await self.db.fetch_all_async("SELECT * FROM case_alerts")
        case_alert_ids = {
            r["alert_id"] for r in case_alert_rows
            if r.get("case_id") == case["case_id"]
        }
        # Check if any alert in this case references the same event
        all_alerts = await self.db.fetch_all_async("SELECT * FROM alerts")
        for a in all_alerts:
            if a["alert_id"] in case_alert_ids and a.get("event_id") == event_id:
                return case

    # Rule 1: same algorithm, within aggregation window
    # TODO: get case_aggregation_minutes from client config (default 60)
    aggregation_minutes = 60
    for case in open_cases:
        case_algos = case.get("algorithms", [])
        if algorithm in case_algos:
            last_alert = case.get("last_alert_at")
            if last_alert:
                from datetime import datetime, timedelta
                if isinstance(last_alert, str):
                    last_alert = datetime.fromisoformat(last_alert)
                cutoff = datetime.utcnow() - timedelta(minutes=aggregation_minutes)
                if last_alert.replace(tzinfo=None) >= cutoff:
                    return case

    return None

async def _create_case(
    self, tenant_id: str, alert_id: str,
    severity: str, algorithm: str, alert_time: str,
) -> None:
    """Create a new case with one alert."""
    case_id = uuid.uuid4().hex
    now = datetime.utcnow()
    if isinstance(alert_time, str):
        alert_dt = datetime.fromisoformat(alert_time).replace(tzinfo=None)
    else:
        alert_dt = alert_time

    case = {
        "case_id": case_id,
        "tenant_id": tenant_id,
        "status": "open",
        "severity": severity,
        "title": self._generate_title(algorithm, tenant_id, 1, [algorithm]),
        "description": None,
        "alert_count": 1,
        "first_alert_at": alert_dt,
        "last_alert_at": alert_dt,
        "algorithms": [algorithm],
        "created_at": now,
        "updated_at": now,
    }
    await self.db.insert_one_async("cases", case)
    await self.db.insert_one_async("case_alerts", {
        "case_id": case_id, "alert_id": alert_id,
    })

async def _add_alert_to_case(
    self, case: dict, alert_id: str,
    severity: str, algorithm: str, alert_time: str,
) -> None:
    """Add an alert to an existing case, updating computed fields."""
    case_id = case["case_id"]
    new_count = case.get("alert_count", 0) + 1
    algorithms = list(set(case.get("algorithms", []) + [algorithm]))
    new_severity = max(
        case.get("severity", "medium"), severity,
        key=lambda s: _SEVERITY_ORDER.get(s, 0),
    )

    if isinstance(alert_time, str):
        alert_dt = datetime.fromisoformat(alert_time).replace(tzinfo=None)
    else:
        alert_dt = alert_time

    # Update case (delete + re-insert for MemoryDatabase compat)
    updated = dict(case)
    updated["alert_count"] = new_count
    updated["algorithms"] = algorithms
    updated["severity"] = new_severity
    updated["last_alert_at"] = alert_dt
    updated["title"] = self._generate_title(
        algorithms[0] if len(algorithms) == 1 else "multiple",
        case["tenant_id"], new_count, algorithms,
    )
    updated["updated_at"] = datetime.utcnow()

    await self.db.execute_async(
        "DELETE FROM cases WHERE case_id = $1", [case_id],
    )
    await self.db.insert_one_async("cases", updated)
    await self.db.insert_one_async("case_alerts", {
        "case_id": case_id, "alert_id": alert_id,
    })

def _generate_title(
    self, primary_algo: str, tenant_id: str,
    alert_count: int, algorithms: list[str],
) -> str:
    """Generate a human-readable case title."""
    algo_display = {
        "large_notional": "Large notional detected",
        "velocity": "Velocity threshold exceeded",
        "suspicious_counterparty": "Suspicious counterparty activity",
    }
    if len(algorithms) == 1:
        label = algo_display.get(algorithms[0], algorithms[0])
        return f"{label} — {tenant_id} ({alert_count} alerts)"
    else:
        return (
            f"Multiple fraud signals — {tenant_id} "
            f"({alert_count} alerts, {len(algorithms)} algorithms)"
        )
```

### 6.7 Case lifecycle

```
        New alert arrives
              │
              ▼
    ┌─────────────────┐
    │  Dedup check     │──── duplicate ──► discard
    └────────┬────────┘
             │ new
             ▼
    ┌─────────────────┐       ┌──────────────────┐
    │ Find matching   │──yes─►│ Add to existing  │
    │ open case       │       │ case, update     │
    └────────┬────────┘       │ severity/count   │
             │ no             └──────────────────┘
             ▼
    ┌─────────────────┐
    │ Create new case │
    │ status = open   │
    └─────────────────┘

    Analyst workflow:
    open ──► investigating ──► resolved
                           └─► dismissed
```

### 6.8 Future: smarter aggregation

The rule-based aggregation above is a starting point. Future enhancements:
- **Entity-based grouping:** Alerts involving the same counterparty_id, account_id, or symbol across tenants
- **Time-series correlation:** Detect alert patterns that are temporally correlated (e.g., large notional followed by rapid velocity)
- **Severity escalation:** Auto-escalate case severity if alert volume exceeds a threshold within the aggregation window
- **Auto-resolve:** Close cases if no new alerts arrive within a configurable timeout

---

## 7. Client Config Schema Changes

### 7.1 New fields in `clients` table

```sql
-- de_platform/migrations/client_config/003_add_window_config.up.sql
ALTER TABLE clients
    ADD COLUMN window_size_minutes INTEGER NOT NULL DEFAULT 5,
    ADD COLUMN window_slide_minutes INTEGER NOT NULL DEFAULT 1,
    ADD COLUMN case_aggregation_minutes INTEGER NOT NULL DEFAULT 60;
```

```sql
-- de_platform/migrations/client_config/003_add_window_config.down.sql
ALTER TABLE clients
    DROP COLUMN window_size_minutes,
    DROP COLUMN window_slide_minutes,
    DROP COLUMN case_aggregation_minutes;
```

These are tenant-level settings:
- `window_size_minutes` / `window_slide_minutes` — used by the sliding window engine
- `case_aggregation_minutes` — used by the alert manager for case grouping

### 7.2 Updated `ClientConfigCache`

Add method:

```python
def get_window_config(self, tenant_id: str) -> dict:
    """Return {"window_size_minutes": 5, "window_slide_minutes": 1}."""
    config = self._get_client(tenant_id)
    return {
        "window_size_minutes": (config or {}).get("window_size_minutes", 5),
        "window_slide_minutes": (config or {}).get("window_slide_minutes", 1),
    }
```

### 7.3 Updated client_config service

- `_sync_client_to_cache()` includes the new fields
- POST/PUT endpoints accept `window_size_minutes`, `window_slide_minutes`, `case_aggregation_minutes`

---

## 8. Testing Strategy

### 8.1 Algorithm unit tests (`de_platform/pipeline/tests/test_algorithms.py`)

```python
# Test evaluate_window() with multi-event batches
def test_large_notional_window_multiple_alerts():
    """Two events above threshold in same window → two alerts."""

def test_large_notional_window_mixed():
    """Mix of above/below threshold → only above-threshold events alert."""

def test_velocity_window_under_threshold():
    """Window with fewer events than max → no alert."""

def test_velocity_window_over_threshold():
    """Window with more events than max → one alert."""

def test_suspicious_counterparty_window():
    """Multiple suspicious events → one alert per event."""

def test_evaluate_backwards_compat():
    """Single-event evaluate() still works via delegation."""

def test_custom_thresholds_override_defaults():
    """Per-tenant thresholds passed to evaluate_window() override constructor defaults."""
```

### 8.2 Window engine unit tests (`de_platform/pipeline/tests/test_window_engine.py`)

```python
# Realtime path
def test_engine_ingest_no_alert_before_window_full():
    """Events don't span a full window → no alerts."""

def test_engine_ingest_alert_when_window_full():
    """Events spanning window_size → evaluation triggers."""

def test_engine_window_slides():
    """After evaluation, old events are pruned and window advances."""

def test_engine_multiple_tenants_independent():
    """Each tenant has its own buffer and window state."""

def test_engine_immediate_eval_zero_window():
    """window_size=0 evaluates on every event, no buffering."""

# Batch path
def test_engine_run_batch_full_day():
    """24h of events with 5-min window, 1-min slide → correct number of windows."""

def test_engine_run_batch_empty():
    """No events → no alerts."""

def test_engine_run_batch_single_window():
    """Events spanning exactly one window → one evaluation."""

# Config integration
def test_engine_disabled_algo_skipped():
    """Disabled algo returns no alerts even with matching events."""

def test_engine_per_tenant_window_config():
    """Different tenants can have different window sizes."""
```

### 8.3 Alert manager unit tests (`de_platform/modules/alert_manager/tests/test_main.py`)

```python
# Dedup
def test_duplicate_alert_not_persisted():
    """Same (algorithm, event_id) → second alert is discarded."""

def test_different_algo_same_event_both_persisted():
    """Different algorithms on same event_id → both alerts persisted."""

# Case aggregation
def test_first_alert_creates_new_case():
    """First alert for a tenant creates a new case."""

def test_same_algo_within_window_groups_into_case():
    """Second alert from same algo within aggregation window → same case."""

def test_same_algo_outside_window_creates_new_case():
    """Alert after aggregation window expires → new case."""

def test_same_event_id_cross_algo_groups_into_case():
    """Different algos alerting on same event_id → same case."""

def test_case_severity_escalates():
    """Adding a critical alert to a medium case → case becomes critical."""

def test_case_title_updates():
    """Title reflects current alert count and algorithms."""

# REST API
def test_list_alerts_tenant_scoped():
    """GET /api/v1/alerts returns only alerts for the authenticated tenant."""

def test_list_cases_with_filters():
    """GET /api/v1/cases?status=open&severity=high filters correctly."""

def test_get_case_includes_alerts():
    """GET /api/v1/cases/{case_id} returns case + its alert list."""

def test_update_case_status():
    """PUT /api/v1/cases/{case_id}/status transitions from open to resolved."""
```

### 8.4 Algos module tests (updated)

```python
# Existing tests updated to use evaluate_window() path
# Verify algos no longer write to DB
def test_algos_module_publishes_to_kafka_only():
    """Alerts are published to Kafka, not written directly to DB."""

def test_algos_module_buffers_events():
    """Events are buffered until window is full."""

def test_algos_module_produces_alerts_on_window():
    """After enough events, window evaluation produces alerts."""
```

### 8.5 Batch algos module tests

```python
def test_batch_algos_loads_from_clickhouse():
    """Events are loaded from ClickHouse for the date range."""

def test_batch_algos_dedup_in_run():
    """Same event in overlapping windows → single alert published."""

def test_batch_algos_empty_range():
    """No events in range → exit 0, no alerts."""

def test_batch_algos_publishes_to_kafka():
    """Alerts are published to the alerts topic (not written to DB)."""
```

### 8.6 Shared scenario updates

The existing shared scenarios (`scenario_large_notional`, `scenario_velocity`, etc.) need updating:
- Wait for alerts via the alert manager instead of direct DB queries
- Add case assertions: after alerts are generated, verify a case was created
- The `MemoryHarness` needs to include an in-process alert manager

---

## 9. Migration & Implementation Order

### Step 1: Algorithm interface
- Add `evaluate_window()` to `FraudAlgorithm` ABC
- Rewrite all three algos (remove `CacheInterface` from VelocityAlgo)
- Make `evaluate()` a concrete convenience method
- Update algorithm unit tests

### Step 2: Window engine
- Create `de_platform/pipeline/window_engine.py`
- Write unit tests for both realtime and batch paths
- No module changes yet — engine is standalone

### Step 3: Alert management service
- Create `de_platform/modules/alert_manager/`
- Implement Kafka consumer, dedup, Postgres persistence
- Implement case aggregation logic
- Implement REST API for alerts and cases
- Write unit tests
- Add database migration for cases table

### Step 4: Realtime algos module update
- Modify `AlgosModule` to use `SlidingWindowEngine`
- Remove direct DB write — publish to Kafka only
- Drop `DatabaseInterface` dependency
- Update client config integration
- Update module tests

### Step 5: Batch algos module
- Create `de_platform/modules/batch_algos/`
- Implement `BatchAlgosModule` using `SlidingWindowEngine`
- Publish to Kafka only (alert_manager handles the rest)
- Write unit tests

### Step 6: data_api cleanup + client config
- Remove alert consumption and alert REST endpoints from `data_api`
- Add proxy route for `/api/v1/alerts/*` and `/api/v1/cases/*` to alert_manager (gateway pattern)
- Add client_config schema migration for window + aggregation fields
- Update `ClientConfigCache` with `get_window_config()` method

### Step 7: Integration & E2E tests
- Update shared scenarios for new alert flow
- Add alert manager and case-specific integration tests
- Update `MemoryHarness` to include in-process alert manager
- Add batch-specific integration tests

---

## 10. Files Summary

### New files

| File | Purpose |
|------|---------|
| `de_platform/pipeline/window_engine.py` | `SlidingWindowEngine`, `WindowConfig`, `TenantWindowState` |
| `de_platform/pipeline/tests/test_window_engine.py` | Window engine unit tests |
| `de_platform/modules/alert_manager/__init__.py` | Package marker |
| `de_platform/modules/alert_manager/main.py` | `AlertManagerModule` — Kafka consume, dedup, cases, REST |
| `de_platform/modules/alert_manager/module.json` | Module descriptor (service, port 8007) |
| `de_platform/modules/alert_manager/tests/__init__.py` | Package marker |
| `de_platform/modules/alert_manager/tests/test_main.py` | Alert manager unit tests |
| `de_platform/modules/batch_algos/__init__.py` | Package marker |
| `de_platform/modules/batch_algos/main.py` | `BatchAlgosModule` |
| `de_platform/modules/batch_algos/module.json` | Module descriptor (job) |
| `de_platform/modules/batch_algos/tests/__init__.py` | Package marker |
| `de_platform/modules/batch_algos/tests/test_main.py` | Batch algos unit tests |
| `de_platform/migrations/alert_manager/001_create_cases.up.sql` | Cases + case_alerts tables |
| `de_platform/migrations/alert_manager/001_create_cases.down.sql` | Drop cases tables |
| `de_platform/migrations/client_config/003_add_window_config.up.sql` | Window + aggregation columns |
| `de_platform/migrations/client_config/003_add_window_config.down.sql` | Drop window columns |

### Modified files

| File | Change |
|------|--------|
| `de_platform/pipeline/algorithms.py` | Rewrite to `evaluate_window()` interface, remove VelocityAlgo cache dep |
| `de_platform/modules/algos/main.py` | Use `SlidingWindowEngine`, Kafka-only publish, drop DB dep |
| `de_platform/modules/algos/tests/test_main.py` | Update for new interface, verify no DB writes |
| `de_platform/modules/data_api/main.py` | Remove alert consumption + alert REST endpoints |
| `de_platform/pipeline/client_config_cache.py` | Add `get_window_config()` method |
| `de_platform/modules/client_config/main.py` | Accept window + aggregation config fields |
| `tests/helpers/scenarios.py` | Update algo scenarios for new alert flow via alert_manager |
| `tests/helpers/harness.py` | Include alert_manager in harness setup |
