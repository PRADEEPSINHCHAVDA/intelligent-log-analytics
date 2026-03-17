# =============================================================================
# Variables - Log Analytics Platform
# =============================================================================

variable "aws_region" {
  description = "AWS region to deploy into"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be dev, staging, or prod."
  }
}

variable "project_name" {
  description = "Project name (used as prefix for all resources)"
  type        = string
  default     = "log-analytics"
}

variable "vpc_cidr" {
  description = "CIDR block for the VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "key_pair_name" {
  description = "EC2 key pair name for SSH access to Kafka brokers"
  type        = string
  default     = ""
}

variable "kafka_version" {
  description = "Apache Kafka version to install on EC2"
  type        = string
  default     = "3.6.0"
}

variable "opensearch_instance_type" {
  description = "OpenSearch instance type"
  type        = string
  default     = "t3.small.search"   # $0.036/hr = ~$26/month
}

variable "opensearch_instance_count" {
  description = "Number of OpenSearch data nodes"
  type        = number
  default     = 1   # Dev: 1 node. Prod: 3+ for HA
}

variable "opensearch_ebs_size" {
  description = "EBS volume size in GB per OpenSearch node"
  type        = number
  default     = 20   # 20GB dev. Scale to 100GB+ for 2GB/day × 90 days
}

variable "alert_email" {
  description = "Email for CloudWatch alerts"
  type        = string
  default     = ""
}

variable "spot_max_price" {
  description = "Maximum Spot Instance bid price (USD/hour)"
  type        = string
  default     = "0.05"   # Set to on-demand price as ceiling
}
