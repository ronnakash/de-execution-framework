output "namespace" {
  value = module.namespace.name
}

output "configmap_name" {
  value = module.config.configmap_name
}

output "secret_name" {
  value = module.config.secret_name
}
