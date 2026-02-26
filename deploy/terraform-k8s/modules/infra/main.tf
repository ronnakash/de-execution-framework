# ── PostgreSQL ──────────────────────────────────────────────────────

resource "kubernetes_stateful_set_v1" "postgres" {
  metadata {
    name      = "postgres"
    namespace = var.namespace
  }

  spec {
    service_name = "postgres"
    replicas     = 1

    selector {
      match_labels = { app = "postgres" }
    }

    template {
      metadata {
        labels = { app = "postgres" }
      }

      spec {
        container {
          name  = "postgres"
          image = "postgres:16"
          args  = ["-c", "max_connections=500"]

          port {
            container_port = 5432
          }

          env {
            name  = "POSTGRES_USER"
            value = var.postgres_user
          }
          env {
            name  = "POSTGRES_PASSWORD"
            value = var.postgres_password
          }
          env {
            name  = "POSTGRES_DB"
            value = var.postgres_database
          }

          resources {
            requests = { cpu = "250m", memory = "256Mi" }
            limits   = { cpu = "1", memory = "1Gi" }
          }

          readiness_probe {
            exec {
              command = ["pg_isready", "-U", var.postgres_user]
            }
            initial_delay_seconds = 5
            period_seconds        = 5
          }

          volume_mount {
            name       = "pgdata"
            mount_path = "/var/lib/postgresql/data"
          }
        }
      }
    }

    volume_claim_template {
      metadata {
        name = "pgdata"
      }
      spec {
        access_modes = ["ReadWriteOnce"]
        resources {
          requests = { storage = "10Gi" }
        }
      }
    }
  }
}

resource "kubernetes_service_v1" "postgres" {
  metadata {
    name      = "postgres"
    namespace = var.namespace
  }

  spec {
    cluster_ip = "None"
    selector   = { app = "postgres" }

    port {
      port = 5432
    }
  }
}

# ── Redis ───────────────────────────────────────────────────────────

resource "kubernetes_stateful_set_v1" "redis" {
  metadata {
    name      = "redis"
    namespace = var.namespace
  }

  spec {
    service_name = "redis"
    replicas     = 1

    selector {
      match_labels = { app = "redis" }
    }

    template {
      metadata {
        labels = { app = "redis" }
      }

      spec {
        container {
          name    = "redis"
          image   = "redis:7-alpine"
          command = ["redis-server", "--appendonly", "yes"]

          port {
            container_port = 6379
          }

          resources {
            requests = { cpu = "100m", memory = "128Mi" }
            limits   = { cpu = "500m", memory = "512Mi" }
          }

          readiness_probe {
            exec {
              command = ["redis-cli", "ping"]
            }
            initial_delay_seconds = 5
            period_seconds        = 5
          }

          volume_mount {
            name       = "redis-data"
            mount_path = "/data"
          }
        }
      }
    }

    volume_claim_template {
      metadata {
        name = "redis-data"
      }
      spec {
        access_modes = ["ReadWriteOnce"]
        resources {
          requests = { storage = "1Gi" }
        }
      }
    }
  }
}

resource "kubernetes_service_v1" "redis" {
  metadata {
    name      = "redis"
    namespace = var.namespace
  }

  spec {
    cluster_ip = "None"
    selector   = { app = "redis" }

    port {
      port = 6379
    }
  }
}

# ── Zookeeper ───────────────────────────────────────────────────────

resource "kubernetes_stateful_set_v1" "zookeeper" {
  metadata {
    name      = "zookeeper"
    namespace = var.namespace
  }

  spec {
    service_name = "zookeeper"
    replicas     = 1

    selector {
      match_labels = { app = "zookeeper" }
    }

    template {
      metadata {
        labels = { app = "zookeeper" }
      }

      spec {
        container {
          name  = "zookeeper"
          image = "confluentinc/cp-zookeeper:7.6.0"

          port {
            container_port = 2181
          }

          env {
            name  = "ZOOKEEPER_CLIENT_PORT"
            value = "2181"
          }

          resources {
            requests = { cpu = "100m", memory = "256Mi" }
            limits   = { cpu = "500m", memory = "512Mi" }
          }

          volume_mount {
            name       = "zk-data"
            mount_path = "/var/lib/zookeeper/data"
          }
        }
      }
    }

    volume_claim_template {
      metadata {
        name = "zk-data"
      }
      spec {
        access_modes = ["ReadWriteOnce"]
        resources {
          requests = { storage = "2Gi" }
        }
      }
    }
  }
}

resource "kubernetes_service_v1" "zookeeper" {
  metadata {
    name      = "zookeeper"
    namespace = var.namespace
  }

  spec {
    cluster_ip = "None"
    selector   = { app = "zookeeper" }

    port {
      port = 2181
    }
  }
}

# ── Kafka ───────────────────────────────────────────────────────────

resource "kubernetes_stateful_set_v1" "kafka" {
  metadata {
    name      = "kafka"
    namespace = var.namespace
  }

  spec {
    service_name = "kafka"
    replicas     = var.kafka_replicas

    selector {
      match_labels = { app = "kafka" }
    }

    template {
      metadata {
        labels = { app = "kafka" }
      }

      spec {
        container {
          name  = "kafka"
          image = "confluentinc/cp-kafka:7.6.0"

          port {
            container_port = 9092
          }

          env {
            name  = "KAFKA_ZOOKEEPER_CONNECT"
            value = "zookeeper:2181"
          }
          env {
            name  = "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP"
            value = "PLAINTEXT:PLAINTEXT"
          }
          env {
            name  = "KAFKA_INTER_BROKER_LISTENER_NAME"
            value = "PLAINTEXT"
          }
          env {
            name  = "KAFKA_ADVERTISED_LISTENERS"
            value = "PLAINTEXT://kafka-0.kafka.${var.namespace}.svc.cluster.local:9092"
          }
          env {
            name  = "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR"
            value = tostring(var.kafka_replication_factor)
          }
          env {
            name  = "KAFKA_DEFAULT_REPLICATION_FACTOR"
            value = tostring(var.kafka_replication_factor)
          }
          env {
            name  = "KAFKA_MIN_INSYNC_REPLICAS"
            value = tostring(var.kafka_min_insync_replicas)
          }
          env {
            name  = "KAFKA_NUM_PARTITIONS"
            value = tostring(var.kafka_num_partitions)
          }
          env {
            name  = "KAFKA_LOG_RETENTION_HOURS"
            value = "168"
          }
          env {
            name = "POD_NAME"
            value_from {
              field_ref {
                field_path = "metadata.name"
              }
            }
          }

          resources {
            requests = { cpu = "250m", memory = "512Mi" }
            limits   = { cpu = "1", memory = "2Gi" }
          }

          readiness_probe {
            exec {
              command = ["kafka-topics", "--bootstrap-server", "localhost:9092", "--list"]
            }
            initial_delay_seconds = 30
            period_seconds        = 10
            timeout_seconds       = 10
          }

          volume_mount {
            name       = "kafka-data"
            mount_path = "/var/lib/kafka/data"
          }
        }
      }
    }

    volume_claim_template {
      metadata {
        name = "kafka-data"
      }
      spec {
        access_modes = ["ReadWriteOnce"]
        resources {
          requests = { storage = "20Gi" }
        }
      }
    }
  }
}

resource "kubernetes_service_v1" "kafka" {
  metadata {
    name      = "kafka"
    namespace = var.namespace
  }

  spec {
    cluster_ip = "None"
    selector   = { app = "kafka" }

    port {
      port = 9092
    }
  }
}

# ── ClickHouse ──────────────────────────────────────────────────────

resource "kubernetes_stateful_set_v1" "clickhouse" {
  metadata {
    name      = "clickhouse"
    namespace = var.namespace
  }

  spec {
    service_name = "clickhouse"
    replicas     = 1

    selector {
      match_labels = { app = "clickhouse" }
    }

    template {
      metadata {
        labels = { app = "clickhouse" }
      }

      spec {
        container {
          name  = "clickhouse"
          image = "clickhouse/clickhouse-server:latest"

          port {
            container_port = 8123
            name           = "http"
          }
          port {
            container_port = 9000
            name           = "native"
          }

          env {
            name  = "CLICKHOUSE_DB"
            value = var.clickhouse_database
          }
          env {
            name  = "CLICKHOUSE_USER"
            value = "default"
          }
          env {
            name  = "CLICKHOUSE_PASSWORD"
            value = var.clickhouse_password
          }

          resources {
            requests = { cpu = "250m", memory = "512Mi" }
            limits   = { cpu = "2", memory = "2Gi" }
          }

          readiness_probe {
            http_get {
              path = "/ping"
              port = 8123
            }
            initial_delay_seconds = 10
            period_seconds        = 10
          }

          volume_mount {
            name       = "chdata"
            mount_path = "/var/lib/clickhouse"
          }
        }
      }
    }

    volume_claim_template {
      metadata {
        name = "chdata"
      }
      spec {
        access_modes = ["ReadWriteOnce"]
        resources {
          requests = { storage = "20Gi" }
        }
      }
    }
  }
}

resource "kubernetes_service_v1" "clickhouse" {
  metadata {
    name      = "clickhouse"
    namespace = var.namespace
  }

  spec {
    cluster_ip = "None"
    selector   = { app = "clickhouse" }

    port {
      port = 8123
      name = "http"
    }
    port {
      port = 9000
      name = "native"
    }
  }
}

# ── MinIO ───────────────────────────────────────────────────────────

resource "kubernetes_stateful_set_v1" "minio" {
  metadata {
    name      = "minio"
    namespace = var.namespace
  }

  spec {
    service_name = "minio"
    replicas     = 1

    selector {
      match_labels = { app = "minio" }
    }

    template {
      metadata {
        labels = { app = "minio" }
      }

      spec {
        container {
          name    = "minio"
          image   = "minio/minio:latest"
          command = ["minio", "server", "/data", "--console-address", ":9001"]

          port {
            container_port = 9000
            name           = "api"
          }
          port {
            container_port = 9001
            name           = "console"
          }

          env {
            name  = "MINIO_ROOT_USER"
            value = var.minio_access_key
          }
          env {
            name  = "MINIO_ROOT_PASSWORD"
            value = var.minio_secret_key
          }

          resources {
            requests = { cpu = "100m", memory = "256Mi" }
            limits   = { cpu = "500m", memory = "512Mi" }
          }

          readiness_probe {
            http_get {
              path = "/minio/health/live"
              port = 9000
            }
            initial_delay_seconds = 10
            period_seconds        = 10
          }

          volume_mount {
            name       = "minio-data"
            mount_path = "/data"
          }
        }
      }
    }

    volume_claim_template {
      metadata {
        name = "minio-data"
      }
      spec {
        access_modes = ["ReadWriteOnce"]
        resources {
          requests = { storage = "5Gi" }
        }
      }
    }
  }
}

resource "kubernetes_service_v1" "minio" {
  metadata {
    name      = "minio"
    namespace = var.namespace
  }

  spec {
    cluster_ip = "None"
    selector   = { app = "minio" }

    port {
      port = 9000
      name = "api"
    }
    port {
      port = 9001
      name = "console"
    }
  }
}

# ── ClickHouse init (run clickhouse_init.sql after ClickHouse is ready) ──

resource "null_resource" "clickhouse_init" {
  depends_on = [kubernetes_stateful_set_v1.clickhouse]

  triggers = {
    sql_hash = filesha256(var.clickhouse_init_sql)
  }

  provisioner "local-exec" {
    command = <<-EOT
      echo "Waiting for ClickHouse to be ready..."
      kubectl rollout status statefulset/clickhouse -n ${var.namespace} --timeout=180s
      sleep 5
      echo "Initializing ClickHouse tables..."
      kubectl exec -i -n ${var.namespace} statefulset/clickhouse -- \
        clickhouse-client --password ${var.clickhouse_password} \
        --database ${var.clickhouse_database} \
        --multiquery < ${var.clickhouse_init_sql}
    EOT
  }
}
