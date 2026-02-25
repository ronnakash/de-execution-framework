variable "cluster_name" {
  description = "Name of the Kubernetes cluster"
  type        = string
  default     = "de-platform"
}

variable "region" {
  description = "Cloud region for the cluster"
  type        = string
  default     = "us-east-1"
}

variable "node_count" {
  description = "Number of worker nodes"
  type        = number
  default     = 3
}

variable "node_instance_type" {
  description = "Instance type for worker nodes"
  type        = string
  default     = "t3.large"
}

variable "kubernetes_version" {
  description = "Kubernetes version"
  type        = string
  default     = "1.29"
}

variable "vpc_cidr" {
  description = "VPC CIDR block"
  type        = string
  default     = "10.0.0.0/16"
}

variable "tags" {
  description = "Tags to apply to all resources"
  type        = map(string)
  default = {
    Project     = "de-platform"
    Environment = "production"
    ManagedBy   = "terraform"
  }
}
