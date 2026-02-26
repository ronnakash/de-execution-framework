output "configmap_name" {
  value = kubernetes_config_map_v1.platform_config.metadata[0].name
}

output "secret_name" {
  value = kubernetes_secret_v1.platform_secrets.metadata[0].name
}
