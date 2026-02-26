output "postgres_ready" {
  value = kubernetes_stateful_set_v1.postgres.metadata[0].name
}

output "clickhouse_ready" {
  value = kubernetes_stateful_set_v1.clickhouse.metadata[0].name
}

output "kafka_ready" {
  value = kubernetes_stateful_set_v1.kafka.metadata[0].name
}

output "redis_ready" {
  value = kubernetes_stateful_set_v1.redis.metadata[0].name
}
