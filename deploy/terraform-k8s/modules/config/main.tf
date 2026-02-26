locals {
  pg_url = "postgresql://${var.postgres_user}:${var.postgres_password}@postgres:5432/${var.postgres_database}"
}

resource "kubernetes_config_map_v1" "platform_config" {
  metadata {
    name      = "de-platform-config"
    namespace = var.namespace
  }

  data = {
    # Core Postgres connection
    DB_POSTGRES_HOST     = "postgres"
    DB_POSTGRES_PORT     = "5432"
    DB_POSTGRES_USER     = var.postgres_user
    DB_POSTGRES_DATABASE = var.postgres_database

    # Named-instance DB URLs (eliminates kubectl patch hack)
    DB_WAREHOUSE_URL       = local.pg_url
    DB_ALERTS_URL          = local.pg_url
    DB_CLIENT_CONFIG_URL   = local.pg_url
    DB_AUTH_URL            = local.pg_url
    DB_ALERT_MANAGER_URL   = local.pg_url
    DB_DATA_AUDIT_URL      = local.pg_url
    DB_TASK_SCHEDULER_URL  = local.pg_url

    # ClickHouse
    DB_CLICKHOUSE_HOST     = "clickhouse"
    DB_CLICKHOUSE_PORT     = "8123"
    DB_CLICKHOUSE_DATABASE = var.clickhouse_database
    DB_CLICKHOUSE_USER     = var.clickhouse_user

    # ClickHouse named-instance (events)
    DB_EVENTS_HOST     = "clickhouse"
    DB_EVENTS_PORT     = "8123"
    DB_EVENTS_DATABASE = var.clickhouse_database
    DB_EVENTS_USER     = var.clickhouse_user

    # Kafka
    MQ_KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
    MQ_KAFKA_POLL_TIMEOUT      = "0.1"

    # Redis
    CACHE_REDIS_URL = "redis://redis:6379/0"

    # MinIO
    FS_MINIO_ENDPOINT = "minio:9000"
    FS_MINIO_BUCKET   = "de-platform"
    FS_MINIO_SECURE   = "false"
  }
}

resource "kubernetes_secret_v1" "platform_secrets" {
  metadata {
    name      = "de-platform-secrets"
    namespace = var.namespace
  }

  type = "Opaque"

  # Terraform handles base64 encoding automatically for `data` in kubernetes_secret_v1
  data = {
    DB_POSTGRES_PASSWORD   = var.postgres_password
    DB_CLICKHOUSE_PASSWORD = var.clickhouse_password
    DB_EVENTS_PASSWORD     = var.clickhouse_password
    FS_MINIO_ACCESS_KEY    = var.minio_access_key
    FS_MINIO_SECRET_KEY    = var.minio_secret_key
    JWT_SECRET             = var.jwt_secret
  }
}
