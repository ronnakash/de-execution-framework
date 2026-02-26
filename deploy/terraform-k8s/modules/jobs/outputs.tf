output "migrations_complete" {
  value = kubernetes_job_v1.migrations.metadata[0].name
}
