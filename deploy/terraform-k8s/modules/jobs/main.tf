# ── Migrations Job ──────────────────────────────────────────────────

resource "kubernetes_job_v1" "migrations" {
  metadata {
    # generate_name avoids name collision on re-apply
    generate_name = "migrations-"
    namespace     = var.namespace
  }

  spec {
    backoff_limit = 3

    template {
      metadata {
        labels = { app = "migrations" }
      }

      spec {
        restart_policy = "Never"

        container {
          name              = "migrations"
          image             = var.image
          image_pull_policy = var.image_pull_policy

          command = [
            "sh", "-c",
            join(" && ", [
              "python -m de_platform migrate up warehouse --db postgres",
              "python -m de_platform migrate up alerts --db postgres",
              "python -m de_platform migrate up client_config --db postgres",
              "python -m de_platform migrate up auth --db postgres",
              "python -m de_platform migrate up alert_manager --db postgres",
              "python -m de_platform migrate up data_audit --db postgres",
              "python -m de_platform migrate up task_scheduler --db postgres",
            ])
          ]

          env_from {
            config_map_ref {
              name = var.configmap_name
            }
          }

          env_from {
            secret_ref {
              name = var.secret_name
            }
          }

          resources {
            requests = { cpu = "100m", memory = "128Mi" }
            limits   = { cpu = "500m", memory = "256Mi" }
          }
        }
      }
    }
  }

  wait_for_completion = true

  timeouts {
    create = "5m"
  }
}

# ── Currency Loader CronJob ────────────────────────────────────────

resource "kubernetes_cron_job_v1" "currency_loader" {
  metadata {
    name      = "currency-loader"
    namespace = var.namespace
  }

  spec {
    schedule                      = "0 6 * * *"
    concurrency_policy            = "Forbid"
    successful_jobs_history_limit = 3
    failed_jobs_history_limit     = 3

    job_template {
      metadata {}

      spec {
        backoff_limit = 2

        template {
          metadata {
            labels = { app = "currency-loader" }
          }

          spec {
            restart_policy = "OnFailure"

            container {
              name              = "currency-loader"
              image             = var.image
              image_pull_policy = var.image_pull_policy

              command = [
                "python", "-m", "de_platform", "run", "currency_loader",
                "--db", "warehouse=postgres", "--cache", "redis", "--log", "pretty"
              ]

              env_from {
                config_map_ref {
                  name = var.configmap_name
                }
              }

              env_from {
                secret_ref {
                  name = var.secret_name
                }
              }

              resources {
                requests = { cpu = "100m", memory = "128Mi" }
                limits   = { cpu = "250m", memory = "256Mi" }
              }
            }
          }
        }
      }
    }
  }
}

# ── Audit Calculator CronJob ──────────────────────────────────────

resource "kubernetes_cron_job_v1" "audit_calculator" {
  metadata {
    name      = "audit-calculator"
    namespace = var.namespace
  }

  spec {
    schedule                      = "*/15 * * * *"
    concurrency_policy            = "Forbid"
    successful_jobs_history_limit = 3
    failed_jobs_history_limit     = 3

    job_template {
      metadata {}

      spec {
        backoff_limit = 2

        template {
          metadata {
            labels = { app = "audit-calculator" }
          }

          spec {
            restart_policy = "OnFailure"

            container {
              name              = "audit-calculator"
              image             = var.image
              image_pull_policy = var.image_pull_policy

              command = [
                "python", "-m", "de_platform", "run", "audit_calculator",
                "--db", "data_audit=postgres", "--mq", "kafka", "--log", "pretty"
              ]

              env_from {
                config_map_ref {
                  name = var.configmap_name
                }
              }

              env_from {
                secret_ref {
                  name = var.secret_name
                }
              }

              resources {
                requests = { cpu = "100m", memory = "128Mi" }
                limits   = { cpu = "500m", memory = "512Mi" }
              }
            }
          }
        }
      }
    }
  }
}

# ── Dev User Seed ──────────────────────────────────────────────────

resource "null_resource" "dev_user_seed" {
  count      = var.seed_dev_user ? 1 : 0
  depends_on = [kubernetes_job_v1.migrations]

  triggers = {
    always_run = timestamp()
  }

  provisioner "local-exec" {
    # Pre-computed bcrypt hash of "admin123"
    command = <<-EOT
      kubectl exec -n ${var.namespace} statefulset/postgres -- \
        psql -U ${var.postgres_user} -d ${var.postgres_user} -q -c "
      INSERT INTO tenants (tenant_id, name)
      VALUES ('dev_tenant', 'Development Tenant')
      ON CONFLICT (tenant_id) DO NOTHING;
      " -c "
      DELETE FROM users WHERE user_id = 'dev_admin';
      INSERT INTO users (user_id, tenant_id, email, password_hash, role)
      VALUES ('dev_admin', 'dev_tenant', 'admin@dev.local',
              '\$2b\$12\$S1kkBNM8hYsUXk4F6P2SluHKyyrw49sI/TmBDKmYcfID.ckfU5YAC',
              'admin');
      "
    EOT
  }
}
