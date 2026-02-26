variable "kubeconfig_path" {
  description = "Path to kubeconfig file"
  type        = string
  default     = "~/.kube/config"
}

variable "kubeconfig_context" {
  description = "Kubernetes context to use"
  type        = string
  default     = "kind-de-platform"
}

variable "namespace" {
  description = "Kubernetes namespace for all resources"
  type        = string
  default     = "de-platform"
}

variable "image" {
  description = "Docker image for de-platform modules"
  type        = string
  default     = "de-platform:latest"
}

variable "image_pull_policy" {
  description = "Image pull policy for de-platform containers"
  type        = string
  default     = "IfNotPresent"
}

# ── Infrastructure tunables ─────────────────────────────────────────

variable "kafka_replicas" {
  description = "Number of Kafka broker replicas"
  type        = number
  default     = 1
}

variable "kafka_replication_factor" {
  description = "Kafka topic replication factor"
  type        = number
  default     = 1
}

variable "kafka_min_insync_replicas" {
  description = "Kafka minimum in-sync replicas"
  type        = number
  default     = 1
}

variable "kafka_num_partitions" {
  description = "Default number of Kafka partitions"
  type        = number
  default     = 6
}

# ── Service tunables ────────────────────────────────────────────────

variable "persistence_flush_threshold" {
  description = "Persistence module flush threshold (rows)"
  type        = number
  default     = 1
}

variable "persistence_flush_interval" {
  description = "Persistence module flush interval (seconds)"
  type        = number
  default     = 0
}

variable "data_audit_flush_threshold" {
  description = "Data-audit module flush threshold (rows)"
  type        = number
  default     = 1
}

variable "data_audit_flush_interval" {
  description = "Data-audit module flush interval (seconds)"
  type        = number
  default     = 1
}

# ── Database credentials ────────────────────────────────────────────

variable "postgres_user" {
  description = "PostgreSQL user"
  type        = string
  default     = "platform"
}

variable "postgres_password" {
  description = "PostgreSQL password"
  type        = string
  default     = "platform"
  sensitive   = true
}

variable "postgres_database" {
  description = "PostgreSQL database name"
  type        = string
  default     = "platform"
}

variable "clickhouse_database" {
  description = "ClickHouse database name"
  type        = string
  default     = "fraud_pipeline"
}

variable "clickhouse_user" {
  description = "ClickHouse user"
  type        = string
  default     = "default"
}

variable "clickhouse_password" {
  description = "ClickHouse password"
  type        = string
  default     = "clickhouse"
  sensitive   = true
}

variable "minio_access_key" {
  description = "MinIO access key"
  type        = string
  default     = "minioadmin"
  sensitive   = true
}

variable "minio_secret_key" {
  description = "MinIO secret key"
  type        = string
  default     = "minioadmin"
  sensitive   = true
}

variable "jwt_secret" {
  description = "JWT signing secret (min 32 bytes)"
  type        = string
  default     = "dev-k8s-jwt-secret-minimum-32-bytes!!"
  sensitive   = true
}

# ── Dev options ─────────────────────────────────────────────────────

variable "seed_dev_user" {
  description = "Seed a dev user (admin@dev.local) after migrations"
  type        = bool
  default     = true
}

variable "clickhouse_init_sql" {
  description = "Path to ClickHouse init SQL file"
  type        = string
  default     = "../../scripts/clickhouse_init.sql"
}
