resource "kubernetes_namespace_v1" "de_platform" {
  metadata {
    name = var.namespace
    labels = {
      "app.kubernetes.io/part-of" = "de-platform"
    }
  }
}
