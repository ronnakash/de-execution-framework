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

variable "seed_dev_user" {
  type    = bool
  default = true
}

variable "postgres_user" {
  type = string
}

variable "postgres_password" {
  type      = string
  sensitive = true
}
