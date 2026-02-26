variable "namespace" {
  type = string
}

variable "configmap_name" {
  type = string
}

variable "secret_name" {
  type = string
}

variable "postgres_user" {
  type = string
}

variable "postgres_password" {
  type      = string
  sensitive = true
}

variable "postgres_database" {
  type = string
}

variable "clickhouse_database" {
  type = string
}

variable "clickhouse_password" {
  type      = string
  sensitive = true
}

variable "minio_access_key" {
  type      = string
  sensitive = true
}

variable "minio_secret_key" {
  type      = string
  sensitive = true
}

variable "kafka_replicas" {
  type    = number
  default = 1
}

variable "kafka_replication_factor" {
  type    = number
  default = 1
}

variable "kafka_min_insync_replicas" {
  type    = number
  default = 1
}

variable "kafka_num_partitions" {
  type    = number
  default = 6
}

variable "clickhouse_init_sql" {
  type = string
}
