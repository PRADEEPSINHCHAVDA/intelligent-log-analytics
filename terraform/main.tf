# =============================================================================
# Log Analytics Platform - Terraform Root Module
# =============================================================================
# Architecture: Kafka (EC2 Spot Fleet) → Lambda → OpenSearch
#
# Cost optimization:
#   - EC2 Spot Fleet with Fleet diversification (3 types × 2 AZs)
#   - OpenSearch with hot/warm/cold storage tiers
#   - Lambda auto-scales based on Kafka consumer lag
#   - Target: $30-50/month (vs $450 on-demand equivalent)
# =============================================================================

terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Uncomment to use S3 backend (recommended for teams)
  # backend "s3" {
  #   bucket = "your-terraform-state-bucket"
  #   key    = "log-analytics/terraform.tfstate"
  #   region = "us-east-1"
  # }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "log-analytics-platform"
      Environment = var.environment
      ManagedBy   = "terraform"
      CostCenter  = "engineering"
    }
  }
}

# ---------------------------------------------------------------------------
# Local values
# ---------------------------------------------------------------------------
locals {
  name_prefix = "${var.project_name}-${var.environment}"

  common_tags = {
    Project     = var.project_name
    Environment = var.environment
  }

  # Kafka topics for all 8 microservices
  kafka_topics = [
    "logs.user-service",
    "logs.payment-service",
    "logs.order-service",
    "logs.notification-service",
    "logs.inventory-service",
    "logs.auth-service",
    "logs.api-gateway-service",
    "logs.reporting-service",
  ]
}

# ---------------------------------------------------------------------------
# Data sources
# ---------------------------------------------------------------------------
data "aws_availability_zones" "available" {
  state = "available"
}

data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

# ---------------------------------------------------------------------------
# Module: VPC (networking foundation)
# ---------------------------------------------------------------------------
module "vpc" {
  source = "./modules/vpc"

  name_prefix = local.name_prefix
  vpc_cidr    = var.vpc_cidr
  azs         = slice(data.aws_availability_zones.available.names, 0, 2)
  tags        = local.common_tags
}

# ---------------------------------------------------------------------------
# Module: EC2 Spot Fleet (Kafka brokers)
# ---------------------------------------------------------------------------
module "ec2_spot" {
  source = "./modules/ec2_spot"

  name_prefix        = local.name_prefix
  vpc_id             = module.vpc.vpc_id
  private_subnet_ids = module.vpc.private_subnet_ids
  key_pair_name      = var.key_pair_name
  kafka_version      = var.kafka_version
  tags               = local.common_tags

  depends_on = [module.vpc]
}

# ---------------------------------------------------------------------------
# Module: Lambda (log consumer)
# ---------------------------------------------------------------------------
module "lambda" {
  source = "./modules/lambda"

  name_prefix             = local.name_prefix
  vpc_id                  = module.vpc.vpc_id
  private_subnet_ids      = module.vpc.private_subnet_ids
  opensearch_endpoint     = module.opensearch.endpoint
  kafka_bootstrap_servers = module.ec2_spot.kafka_bootstrap_servers
  kafka_topics            = local.kafka_topics
  dlq_arn                 = aws_sqs_queue.dlq.arn
  tags                    = local.common_tags

  depends_on = [module.ec2_spot, module.opensearch]
}

# ---------------------------------------------------------------------------
# Module: OpenSearch (log storage)
# ---------------------------------------------------------------------------
module "opensearch" {
  source = "./modules/opensearch"

  name_prefix        = local.name_prefix
  vpc_id             = module.vpc.vpc_id
  private_subnet_ids = module.vpc.private_subnet_ids
  instance_type      = var.opensearch_instance_type
  instance_count     = var.opensearch_instance_count
  ebs_volume_size    = var.opensearch_ebs_size
  tags               = local.common_tags

  depends_on = [module.vpc]
}

# ---------------------------------------------------------------------------
# Module: CloudWatch dashboards + alarms
# ---------------------------------------------------------------------------
module "cloudwatch" {
  source = "./modules/cloudwatch"

  name_prefix         = local.name_prefix
  lambda_function_name = module.lambda.function_name
  opensearch_domain   = module.opensearch.domain_name
  kafka_asg_name      = module.ec2_spot.asg_name
  alert_email         = var.alert_email
  tags                = local.common_tags
}

# ---------------------------------------------------------------------------
# Dead Letter Queue (SQS) for failed log records
# ---------------------------------------------------------------------------
resource "aws_sqs_queue" "dlq" {
  name                      = "${local.name_prefix}-log-dlq"
  message_retention_seconds = 1209600  # 14 days
  visibility_timeout_seconds = 300

  tags = local.common_tags
}

resource "aws_sqs_queue_policy" "dlq" {
  queue_url = aws_sqs_queue.dlq.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sqs:SendMessage"
      Resource  = aws_sqs_queue.dlq.arn
    }]
  })
}

# ---------------------------------------------------------------------------
# S3 bucket for Lambda deployment packages + log archives
# ---------------------------------------------------------------------------
resource "aws_s3_bucket" "artifacts" {
  bucket = "${local.name_prefix}-artifacts-${data.aws_caller_identity.current.account_id}"
  tags   = local.common_tags
}

resource "aws_s3_bucket_versioning" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "artifacts" {
  bucket                  = aws_s3_bucket.artifacts.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ---------------------------------------------------------------------------
# S3 lifecycle: cold log archive (90d+)
# ---------------------------------------------------------------------------
resource "aws_s3_bucket_lifecycle_configuration" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id
  rule {
    id     = "log-archive-lifecycle"
    status = "Enabled"
    filter { prefix = "logs/" }
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    transition {
      days          = 90
      storage_class = "GLACIER"
    }
    expiration {
      days = 365
    }
  }
}
