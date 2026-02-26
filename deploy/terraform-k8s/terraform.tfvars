# Dev defaults for local kind cluster.
# Production overrides go in a separate .tfvars file or environment variables.

kubeconfig_context = "kind-de-platform"
namespace          = "de-platform"
image              = "de-platform:latest"
image_pull_policy  = "IfNotPresent"

# Single-node Kafka for local dev
kafka_replicas            = 1
kafka_replication_factor  = 1
kafka_min_insync_replicas = 1

# Low-latency flush for E2E tests
persistence_flush_threshold = 1
persistence_flush_interval  = 0
data_audit_flush_threshold  = 1
data_audit_flush_interval   = 1

seed_dev_user = true
