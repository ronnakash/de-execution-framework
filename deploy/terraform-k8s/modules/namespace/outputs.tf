output "name" {
  value = kubernetes_namespace_v1.de_platform.metadata[0].name
}
