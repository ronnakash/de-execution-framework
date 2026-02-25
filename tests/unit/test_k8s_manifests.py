"""Tests to validate K8s manifests structure and HPA alignment with topic partitions."""

from __future__ import annotations

import re
from pathlib import Path

from de_platform.pipeline.topics import get_partitions

K8S_DIR = Path(__file__).resolve().parents[2] / "deploy" / "k8s"

# Map service names to the primary topic they consume from
SERVICE_PRIMARY_TOPIC: dict[str, str] = {
    "normalizer": "trade_normalization",
    "persistence": "orders_persistence",
    "algos": "trades_algos",
    "kafka-starter": "trade_normalization",
    "data-audit": "audit_counts",
}


def _read(path: Path) -> str:
    return path.read_text()


def _extract_max_replicas(content: str) -> int | None:
    """Extract maxReplicas from an HPA section in YAML text."""
    match = re.search(r"maxReplicas:\s*(\d+)", content)
    return int(match.group(1)) if match else None


def _extract_replicas(content: str) -> int | None:
    """Extract the first replicas value from Deployment spec."""
    match = re.search(r"replicas:\s*(\d+)", content)
    return int(match.group(1)) if match else None


# -- Structural tests ----------------------------------------------------------


def test_base_files_exist():
    assert (K8S_DIR / "base" / "namespace.yaml").exists()
    assert (K8S_DIR / "base" / "configmap.yaml").exists()
    assert (K8S_DIR / "base" / "secrets.yaml").exists()


def test_service_files_exist():
    expected = [
        "normalizer", "persistence", "algos", "alert-manager",
        "data-api", "kafka-starter", "rest-starter", "data-audit",
        "client-config", "auth", "task-scheduler",
    ]
    for name in expected:
        assert (K8S_DIR / "services" / f"{name}.yaml").exists(), f"Missing {name}.yaml"


def test_infra_files_exist():
    for name in ["postgres", "redis", "clickhouse", "kafka"]:
        assert (K8S_DIR / "infra" / f"{name}.yaml").exists(), f"Missing {name}.yaml"


def test_job_files_exist():
    for name in ["batch-algos", "audit-calculator", "currency-loader"]:
        assert (K8S_DIR / "jobs" / f"{name}.yaml").exists(), f"Missing {name}.yaml"


# -- HPA alignment tests -------------------------------------------------------


def test_hpa_max_replicas_does_not_exceed_partition_count():
    """No service HPA maxReplicas should exceed its input topic partition count."""
    for service_name, topic in SERVICE_PRIMARY_TOPIC.items():
        content = _read(K8S_DIR / "services" / f"{service_name}.yaml")
        max_replicas = _extract_max_replicas(content)
        assert max_replicas is not None, f"No maxReplicas found for {service_name}"

        partitions = get_partitions(topic)
        assert max_replicas <= partitions, (
            f"{service_name}: maxReplicas ({max_replicas}) > "
            f"partition count for {topic} ({partitions})"
        )


def test_all_deployments_have_resource_limits():
    """Every service Deployment should specify resource limits."""
    for yaml_file in sorted((K8S_DIR / "services").glob("*.yaml")):
        content = _read(yaml_file)
        assert "resources:" in content, f"{yaml_file.name}: missing resources"
        assert "requests:" in content, f"{yaml_file.name}: missing requests"
        assert "limits:" in content, f"{yaml_file.name}: missing limits"


def test_alert_manager_is_singleton():
    """Alert manager should have replicas=1 due to race conditions."""
    content = _read(K8S_DIR / "services" / "alert-manager.yaml")
    replicas = _extract_replicas(content)
    assert replicas == 1


def test_task_scheduler_is_singleton():
    """Task scheduler should have replicas=1 (leader election needed for multi)."""
    content = _read(K8S_DIR / "services" / "task-scheduler.yaml")
    replicas = _extract_replicas(content)
    assert replicas == 1


def test_namespace_is_de_platform():
    content = _read(K8S_DIR / "base" / "namespace.yaml")
    assert "name: de-platform" in content


def test_all_services_have_health_probes():
    """Every service Deployment should have readiness/liveness probes."""
    for yaml_file in sorted((K8S_DIR / "services").glob("*.yaml")):
        content = _read(yaml_file)
        assert "readinessProbe:" in content, f"{yaml_file.name}: missing readinessProbe"


def test_terraform_files_exist():
    tf_dir = K8S_DIR.parent / "terraform"
    assert (tf_dir / "main.tf").exists()
    assert (tf_dir / "variables.tf").exists()
    assert (tf_dir / "outputs.tf").exists()
