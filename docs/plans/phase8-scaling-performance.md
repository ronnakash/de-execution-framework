# Phase 8: Scaling & Performance

## Overview

Add Kubernetes deployment configuration for horizontal scaling, Kafka partition tuning with autoscaling awareness, and a stress testing framework for benchmarking.

**Source:** changes.md #6, #7, #16

## 8.1 Kubernetes Deployment

**Why:** changes.md #6 — "do we have a way to scale the app up? We are creating docker images for each service, so maybe we can orchestrate the system with terraform and kubernetes"

Currently: single `docker-compose.yml` with one container per service, no scaling.

**Design:**

```
deploy/
    k8s/
        base/                          — shared resources
            namespace.yaml
            configmap.yaml             — shared env vars
            secrets.yaml               — template for secrets
        services/
            normalizer.yaml            — Deployment + HPA
            persistence.yaml
            algos.yaml
            alert-manager.yaml
            data-api.yaml
            kafka-starter.yaml
            rest-starter.yaml
            data-audit.yaml
            client-config.yaml
            auth.yaml
            task-scheduler.yaml
        infra/
            postgres.yaml              — StatefulSet (or managed service ref)
            redis.yaml
            clickhouse.yaml
            kafka.yaml                 — StatefulSet with partition awareness
        jobs/
            batch-algos.yaml           — Job template (triggered by scheduler)
            audit-calculator.yaml      — CronJob (every 15 min)
            currency-loader.yaml       — CronJob (daily)
    terraform/
        main.tf                        — EKS/GKE cluster provisioning
        variables.tf
        outputs.tf
```

**Per-service Deployment template:**
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: normalizer
spec:
  replicas: 2
  selector:
    matchLabels:
      app: normalizer
  template:
    spec:
      containers:
        - name: normalizer
          image: de-platform:latest
          command: ["python", "-m", "de_platform", "run", "normalizer", ...]
          resources:
            requests: { cpu: "100m", memory: "128Mi" }
            limits: { cpu: "500m", memory: "512Mi" }
---
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: normalizer-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: normalizer
  minReplicas: 1
  maxReplicas: 10
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 70
```

**Scaling considerations by service:**

| Service | Scalable? | Notes |
|---------|-----------|-------|
| normalizer | Yes | Stateless, Kafka consumer group handles partition assignment |
| persistence | Yes | Stateless, Kafka consumer group |
| algos | Yes | Per-tenant state in SlidingWindowEngine — need sticky routing by tenant_id (Kafka key partitioning) |
| alert_manager | Careful | Case aggregation has race condition (Phase 1 fix helps). Multiple instances need DB-level locking |
| kafka_starter | Yes | Stateless |
| rest_starter | Yes | Stateless HTTP server |
| data_api | Yes | Stateless proxy/query server |
| data_audit | Yes | Stateless reader (after Phase 5 redesign) |
| client_config | Yes | Stateless, all state in Postgres |
| auth | Yes | Stateless, JWT verification |
| task_scheduler | No (singleton) | APScheduler needs single leader. Use leader election or external scheduler |

## 8.2 Kafka Partition Tuning & Autoscaling Awareness

**Why:** changes.md #7 — "we need to have autoscaling for kafka as well. Having more pods than partitions they read from makes no sense"

Currently: `KAFKA_NUM_PARTITIONS=3` for all topics. If we scale normalizer to 5 replicas, 2 replicas sit idle (Kafka assigns at most 1 consumer per partition per group).

**Design:**

1. **Partition count per topic** — set based on expected throughput:
   - High-throughput topics (normalization, persistence): 12 partitions
   - Medium topics (algos, alerts): 6 partitions
   - Low topics (audit_counts, client_config_updates): 3 partitions

2. **HPA max = partition count** — configure HPA `maxReplicas` to match the topic's partition count:
   ```yaml
   # normalizer reads from trade_normalization (12 partitions)
   maxReplicas: 12
   ```

3. **KEDA-based scaling** (optional, advanced) — scale based on Kafka consumer group lag instead of CPU:
   ```yaml
   apiVersion: keda.sh/v1alpha1
   kind: ScaledObject
   metadata:
     name: normalizer-scaler
   spec:
     scaleTargetRef:
       name: normalizer
     triggers:
       - type: kafka
         metadata:
           bootstrapServers: kafka:9092
           consumerGroup: normalizer
           topic: trade_normalization
           lagThreshold: "100"
   ```

4. **kafka_starter topic creation** — update to create topics with correct partition counts based on client config. Currently uses Kafka's auto-create with default partition count.

## 8.3 Stress Testing & Benchmarking Framework

**Why:** changes.md #16 — "stress testing and benchmarking framework on real infra. supports one or multiple tenants sending data at the same time with varying data scales"

**New module:** `tests/stress/` (not a de_platform module — a standalone test framework)

```
tests/stress/
    __init__.py
    runner.py                — StressTestRunner orchestrator
    generators.py            — Event generators (orders, executions, transactions)
    scenarios.py             — Predefined scenarios (single tenant, multi-tenant, burst)
    metrics.py               — Latency/throughput collectors
    report.py                — HTML/JSON benchmark report
    conftest.py              — pytest fixtures
    test_single_tenant.py    — Single tenant stress tests
    test_multi_tenant.py     — Multi-tenant concurrent stress tests
```

**StressTestRunner:**
```python
class StressTestRunner:
    def __init__(self, pipeline: SharedPipeline):
        self.pipeline = pipeline

    async def run_scenario(
        self,
        tenants: list[str],
        events_per_tenant: int,
        event_types: list[str],
        rate_per_second: int,         # throttle to simulate realistic load
        ingestion_method: str = "rest",
    ) -> StressTestResult:
        """Run a stress test scenario and collect metrics."""
        # 1. Generate events for each tenant
        # 2. Ingest at specified rate (concurrent per-tenant)
        # 3. Wait for pipeline to process all events
        # 4. Collect latency metrics (ingestion → ClickHouse appearance)
        # 5. Collect throughput metrics (events/sec at each stage)
        # 6. Return results
```

**Predefined scenarios:**
- `single_tenant_1k` — 1 tenant, 1000 events, all types
- `single_tenant_10k` — 1 tenant, 10000 events, sustained rate
- `multi_tenant_10x1k` — 10 tenants, 1000 events each, concurrent
- `burst_5k` — 1 tenant, 5000 events as fast as possible (no throttle)
- `sustained_1hr` — 1 tenant, steady 100 events/sec for 1 hour

**Metrics collected:**
- End-to-end latency (ingestion → ClickHouse row visible)
- Per-stage latency (ingestion → normalization → persistence)
- Throughput (events/sec processed)
- Error rate
- Resource utilization (if Prometheus available)

**Report output:** HTML report with charts (latency distribution, throughput over time) saved to `reports/stress/`.

## Files to Create

| File | Purpose |
|------|---------|
| `deploy/k8s/base/*.yaml` | Namespace, configmap, secrets |
| `deploy/k8s/services/*.yaml` | Per-service Deployment + HPA |
| `deploy/k8s/infra/*.yaml` | StatefulSets for infra services |
| `deploy/k8s/jobs/*.yaml` | Job/CronJob templates |
| `deploy/terraform/*.tf` | Cluster provisioning |
| `tests/stress/runner.py` | StressTestRunner |
| `tests/stress/generators.py` | Event generators |
| `tests/stress/scenarios.py` | Predefined stress scenarios |
| `tests/stress/metrics.py` | Latency/throughput collection |
| `tests/stress/report.py` | Benchmark report generation |
| `tests/stress/test_*.py` | Stress test cases |

## Files to Modify

| File | Changes |
|------|---------|
| `docker-compose.yml` | Update KAFKA_NUM_PARTITIONS per topic |
| `de_platform/modules/kafka_starter/main.py` | Create topics with configured partition counts |
| `de_platform/pipeline/topics.py` | Add partition count metadata per topic |
| `Makefile` | Add `make test-stress` target |

## Acceptance Criteria

1. K8s manifests deploy the full platform to a cluster
2. HPA scales services based on CPU (or Kafka lag with KEDA)
3. No service has maxReplicas > its input topic partition count
4. Stress test framework can generate and ingest events at configurable rates
5. Multi-tenant concurrent stress test completes without errors
6. Benchmark report shows latency distribution and throughput metrics
7. Terraform provisions a basic EKS/GKE cluster (placeholder, refined per cloud provider)
