provider "kubernetes" {
  config_path    = var.kubeconfig_path
  config_context = var.kubeconfig_context
}

# ── Namespace ───────────────────────────────────────────────────────

module "namespace" {
  source    = "./modules/namespace"
  namespace = var.namespace
}

# ── ConfigMap & Secrets ─────────────────────────────────────────────

module "config" {
  source     = "./modules/config"
  namespace  = module.namespace.name
  depends_on = [module.namespace]

  postgres_user        = var.postgres_user
  postgres_password    = var.postgres_password
  postgres_database    = var.postgres_database
  clickhouse_database  = var.clickhouse_database
  clickhouse_user      = var.clickhouse_user
  clickhouse_password  = var.clickhouse_password
  minio_access_key     = var.minio_access_key
  minio_secret_key     = var.minio_secret_key
  jwt_secret           = var.jwt_secret
}

# ── Infrastructure (postgres, redis, kafka, clickhouse, minio) ─────

module "infra" {
  source     = "./modules/infra"
  namespace  = module.namespace.name
  depends_on = [module.config]

  configmap_name = module.config.configmap_name
  secret_name    = module.config.secret_name

  postgres_user     = var.postgres_user
  postgres_password = var.postgres_password
  postgres_database = var.postgres_database

  clickhouse_database = var.clickhouse_database
  clickhouse_password = var.clickhouse_password

  minio_access_key = var.minio_access_key
  minio_secret_key = var.minio_secret_key

  kafka_replicas            = var.kafka_replicas
  kafka_replication_factor  = var.kafka_replication_factor
  kafka_min_insync_replicas = var.kafka_min_insync_replicas
  kafka_num_partitions      = var.kafka_num_partitions

  clickhouse_init_sql = var.clickhouse_init_sql
}

# ── Jobs (migrations, currency-loader, audit-calculator) ────────────

module "jobs" {
  source     = "./modules/jobs"
  namespace  = module.namespace.name
  depends_on = [module.infra]

  image             = var.image
  image_pull_policy = var.image_pull_policy
  configmap_name    = module.config.configmap_name
  secret_name       = module.config.secret_name
  seed_dev_user     = var.seed_dev_user
  postgres_user     = var.postgres_user
  postgres_password = var.postgres_password
}

# ── Application services ────────────────────────────────────────────

module "services" {
  source     = "./modules/services"
  namespace  = module.namespace.name
  depends_on = [module.jobs]

  image             = var.image
  image_pull_policy = var.image_pull_policy
  configmap_name    = module.config.configmap_name
  secret_name       = module.config.secret_name

  persistence_flush_threshold = var.persistence_flush_threshold
  persistence_flush_interval  = var.persistence_flush_interval
  data_audit_flush_threshold  = var.data_audit_flush_threshold
  data_audit_flush_interval   = var.data_audit_flush_interval
}
