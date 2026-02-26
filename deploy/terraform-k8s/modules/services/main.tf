locals {
  # ── Service definitions ─────────────────────────────────────────────
  # Each entry defines a Deployment + optional Service + optional HPA.
  services = {
    rest-starter = {
      command = ["python", "-m", "de_platform", "run", "rest_starter",
        "--mq", "kafka", "--port", "8001", "--health-port", "9100", "--log", "pretty"]
      replicas    = 2
      http_port   = 8001
      health_port = 9100
      env         = {}
      expose      = true
      hpa         = { min = 1, max = 4 }
      cpu_limit   = "500m"
      mem_limit   = "512Mi"
    }

    kafka-starter = {
      command = ["python", "-m", "de_platform", "run", "kafka_starter",
        "--mq", "kafka", "--cache", "redis", "--health-port", "9110", "--log", "pretty"]
      replicas    = 2
      http_port   = null
      health_port = 9110
      env = {
        MQ_KAFKA_GROUP_ID          = "kafka-starter"
        MQ_KAFKA_SUBSCRIBE_TOPICS  = "client_orders,client_executions,client_transactions"
      }
      expose    = false
      hpa       = { min = 1, max = 12 }
      cpu_limit = "500m"
      mem_limit = "512Mi"
    }

    normalizer = {
      command = ["python", "-m", "de_platform", "run", "normalizer",
        "--db", "warehouse=postgres", "--cache", "redis", "--mq", "kafka",
        "--health-port", "9101", "--log", "pretty"]
      replicas    = 2
      http_port   = null
      health_port = 9101
      env = {
        MQ_KAFKA_GROUP_ID          = "normalizer"
        MQ_KAFKA_SUBSCRIBE_TOPICS  = "trade_normalization,tx_normalization"
      }
      expose    = false
      hpa       = { min = 1, max = 12 }
      cpu_limit = "500m"
      mem_limit = "512Mi"
    }

    persistence = {
      command = ["python", "-m", "de_platform", "run", "persistence",
        "--db", "clickhouse=clickhouse", "--fs", "minio", "--mq", "kafka",
        "--health-port", "9102", "--log", "pretty",
        "--flush-threshold", tostring(var.persistence_flush_threshold),
        "--flush-interval", tostring(var.persistence_flush_interval)]
      replicas    = 2
      http_port   = null
      health_port = 9102
      env = {
        MQ_KAFKA_GROUP_ID          = "persistence"
        MQ_KAFKA_SUBSCRIBE_TOPICS  = "orders_persistence,executions_persistence,transactions_persistence,duplicates,normalization_errors"
      }
      expose    = false
      hpa       = { min = 1, max = 12 }
      cpu_limit = "500m"
      mem_limit = "512Mi"
    }

    algos = {
      command = ["python", "-m", "de_platform", "run", "algos",
        "--cache", "redis", "--mq", "kafka", "--health-port", "9103",
        "--log", "pretty", "--suspicious-counterparty-ids", "bad-cp-1"]
      replicas    = 2
      http_port   = null
      health_port = 9103
      env = {
        MQ_KAFKA_GROUP_ID          = "algos"
        MQ_KAFKA_SUBSCRIBE_TOPICS  = "trades_algos,transactions_algos"
      }
      expose    = false
      hpa       = { min = 1, max = 6 }
      cpu_limit = "500m"
      mem_limit = "512Mi"
    }

    alert-manager = {
      command = ["python", "-m", "de_platform", "run", "alert_manager",
        "--db", "alerts=postgres", "--mq", "kafka", "--cache", "redis",
        "--port", "8007", "--health-port", "9104", "--log", "pretty"]
      replicas    = 1
      http_port   = 8007
      health_port = 9104
      env = {
        MQ_KAFKA_GROUP_ID          = "alert-manager"
        MQ_KAFKA_SUBSCRIBE_TOPICS  = "alerts"
      }
      expose    = true
      hpa       = null
      cpu_limit = "500m"
      mem_limit = "512Mi"
    }

    data-api = {
      command = ["python", "-m", "de_platform", "run", "data_api",
        "--db", "events=clickhouse", "--port", "8002", "--health-port", "9105",
        "--log", "pretty"]
      replicas    = 2
      http_port   = 8002
      health_port = 9105
      env = {
        ALERT_MANAGER_URL  = "http://alert-manager:8007"
        CLIENT_CONFIG_URL  = "http://client-config:8003"
        DATA_AUDIT_URL     = "http://data-audit:8005"
        TASK_SCHEDULER_URL = "http://task-scheduler:8006"
        AUTH_URL           = "http://auth:8004"
      }
      expose    = true
      hpa       = { min = 1, max = 4 }
      cpu_limit = "500m"
      mem_limit = "512Mi"
    }

    client-config = {
      command = ["python", "-m", "de_platform", "run", "client_config",
        "--db", "client_config=postgres", "--cache", "redis", "--port", "8003",
        "--health-port", "9106", "--log", "pretty"]
      replicas    = 2
      http_port   = 8003
      health_port = 9106
      env         = {}
      expose      = true
      hpa         = { min = 1, max = 4 }
      cpu_limit   = "500m"
      mem_limit   = "256Mi"
    }

    auth = {
      command = ["python", "-m", "de_platform", "run", "auth",
        "--db", "auth=postgres", "--port", "8004", "--health-port", "9107",
        "--log", "pretty"]
      replicas    = 2
      http_port   = 8004
      health_port = 9107
      env         = {}
      expose      = true
      hpa         = { min = 1, max = 4 }
      cpu_limit   = "500m"
      mem_limit   = "256Mi"
    }

    data-audit = {
      command = ["python", "-m", "de_platform", "run", "data_audit",
        "--db", "data_audit=postgres", "--mq", "kafka",
        "--port", "8005", "--health-port", "9108", "--log", "pretty",
        "--flush-threshold", tostring(var.data_audit_flush_threshold),
        "--flush-interval", tostring(var.data_audit_flush_interval)]
      replicas    = 1
      http_port   = 8005
      health_port = 9108
      env = {
        MQ_KAFKA_GROUP_ID          = "data-audit"
        MQ_KAFKA_SUBSCRIBE_TOPICS  = "audit_counts,orders_persistence,executions_persistence,transactions_persistence,normalization_errors,duplicates"
      }
      expose    = true
      hpa       = { min = 1, max = 3 }
      cpu_limit = "500m"
      mem_limit = "256Mi"
    }

    task-scheduler = {
      command = ["python", "-m", "de_platform", "run", "task_scheduler",
        "--db", "task_scheduler=postgres", "--port", "8006",
        "--health-port", "9109", "--log", "pretty"]
      replicas    = 1
      http_port   = 8006
      health_port = 9109
      env         = {}
      expose      = true
      hpa         = null
      cpu_limit   = "250m"
      mem_limit   = "256Mi"
    }
  }

  # Filter maps for conditional resources
  services_with_expose = { for k, v in local.services : k => v if v.expose }
  services_with_hpa    = { for k, v in local.services : k => v if v.hpa != null }
}

# ── Deployments ─────────────────────────────────────────────────────

resource "kubernetes_deployment_v1" "service" {
  for_each = local.services

  metadata {
    name      = each.key
    namespace = var.namespace
  }

  spec {
    replicas = each.value.replicas

    selector {
      match_labels = { app = each.key }
    }

    template {
      metadata {
        labels = { app = each.key }
      }

      spec {
        termination_grace_period_seconds = each.key == "persistence" ? 30 : null

        container {
          name              = each.key
          image             = var.image
          image_pull_policy = var.image_pull_policy
          command           = each.value.command

          # HTTP port (if service has one)
          dynamic "port" {
            for_each = each.value.http_port != null ? [each.value.http_port] : []
            content {
              container_port = port.value
              name           = "http"
            }
          }

          # Health port
          port {
            container_port = each.value.health_port
            name           = "health"
          }

          # Service-specific env vars
          dynamic "env" {
            for_each = each.value.env
            content {
              name  = env.key
              value = env.value
            }
          }

          # ConfigMap env vars
          env_from {
            config_map_ref {
              name = var.configmap_name
            }
          }

          # Secret env vars
          env_from {
            secret_ref {
              name = var.secret_name
            }
          }

          resources {
            requests = { cpu = "100m", memory = "128Mi" }
            limits   = { cpu = each.value.cpu_limit, memory = each.value.mem_limit }
          }

          readiness_probe {
            http_get {
              path = "/health/startup"
              port = each.value.health_port
            }
            initial_delay_seconds = 10
            period_seconds        = 10
          }

          liveness_probe {
            http_get {
              path = "/health/startup"
              port = each.value.health_port
            }
            initial_delay_seconds = 15
            period_seconds        = 20
          }
        }
      }
    }
  }
}

# ── Services (only for deployments with expose=true) ────────────────

resource "kubernetes_service_v1" "service" {
  for_each = local.services_with_expose

  # kind has no cloud LB controller — don't block on external IP assignment
  wait_for_load_balancer = false

  metadata {
    name      = each.key
    namespace = var.namespace
  }

  spec {
    type     = each.key == "data-api" ? "LoadBalancer" : "ClusterIP"
    selector = { app = each.key }

    port {
      port        = each.value.http_port
      target_port = each.value.http_port
    }
  }
}

# ── HPAs ────────────────────────────────────────────────────────────

resource "kubernetes_horizontal_pod_autoscaler_v2" "service" {
  for_each = local.services_with_hpa

  metadata {
    name      = "${each.key}-hpa"
    namespace = var.namespace
  }

  spec {
    scale_target_ref {
      api_version = "apps/v1"
      kind        = "Deployment"
      name        = each.key
    }

    min_replicas = each.value.hpa.min
    max_replicas = each.value.hpa.max

    metric {
      type = "Resource"
      resource {
        name = "cpu"
        target {
          type                = "Utilization"
          average_utilization = 70
        }
      }
    }
  }
}
