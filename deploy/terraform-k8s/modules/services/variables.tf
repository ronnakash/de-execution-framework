variable "namespace" {
  type = string
}

variable "image" {
  type = string
}

variable "image_pull_policy" {
  type    = string
  default = "IfNotPresent"
}

variable "configmap_name" {
  type = string
}

variable "secret_name" {
  type = string
}

variable "persistence_flush_threshold" {
  type    = number
  default = 1
}

variable "persistence_flush_interval" {
  type    = number
  default = 0
}

variable "data_audit_flush_threshold" {
  type    = number
  default = 1
}

variable "data_audit_flush_interval" {
  type    = number
  default = 1
}
