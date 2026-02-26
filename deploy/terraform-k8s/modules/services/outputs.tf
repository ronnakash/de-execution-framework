output "deployment_names" {
  value = [for k, v in kubernetes_deployment_v1.service : v.metadata[0].name]
}

output "service_names" {
  value = [for k, v in kubernetes_service_v1.service : v.metadata[0].name]
}
