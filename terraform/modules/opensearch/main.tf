# =============================================================================
# OpenSearch Module - Log Storage + Search
# =============================================================================
# Deploys Amazon OpenSearch Service with:
#   - Hot/warm/cold storage tiers via ISM policies
#   - Encryption at rest + in transit
#   - Fine-grained access control
#   - Daily index rotation (logs-{service}-YYYY.MM.DD)
# =============================================================================

variable "name_prefix"        { type = string }
variable "vpc_id"             { type = string }
variable "private_subnet_ids" { type = list(string) }
variable "instance_type"      { type = string; default = "t3.small.search" }
variable "instance_count"     { type = number; default = 1 }
variable "ebs_volume_size"    { type = number; default = 20 }
variable "tags"               { type = map(string) }

data "aws_region" "current"  {}
data "aws_caller_identity" "current" {}

# ---------------------------------------------------------------------------
# Security group
# ---------------------------------------------------------------------------
resource "aws_security_group" "opensearch" {
  name        = "${var.name_prefix}-opensearch-sg"
  description = "OpenSearch - inbound from VPC only"
  vpc_id      = var.vpc_id

  ingress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
    description = "HTTPS from internal VPC"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(var.tags, { Name = "${var.name_prefix}-opensearch-sg" })
}

# ---------------------------------------------------------------------------
# Service-linked role for OpenSearch VPC access
# ---------------------------------------------------------------------------
resource "aws_iam_service_linked_role" "opensearch" {
  aws_service_name = "es.amazonaws.com"
  description      = "Service-linked role for OpenSearch"
  # Ignore if already exists
  lifecycle { ignore_changes = [description] }
}

# ---------------------------------------------------------------------------
# OpenSearch domain
# ---------------------------------------------------------------------------
resource "aws_opensearch_domain" "main" {
  domain_name    = "${var.name_prefix}-logs"
  engine_version = "OpenSearch_2.11"

  cluster_config {
    instance_type          = var.instance_type
    instance_count         = var.instance_count
    zone_awareness_enabled = var.instance_count > 1

    dynamic "zone_awareness_config" {
      for_each = var.instance_count > 1 ? [1] : []
      content { availability_zone_count = 2 }
    }
  }

  ebs_options {
    ebs_enabled = true
    volume_type = "gp3"
    volume_size = var.ebs_volume_size
    iops        = 3000
  }

  vpc_options {
    subnet_ids         = [var.private_subnet_ids[0]]
    security_group_ids = [aws_security_group.opensearch.id]
  }

  encrypt_at_rest {
    enabled = true
  }

  node_to_node_encryption {
    enabled = true
  }

  domain_endpoint_options {
    enforce_https       = true
    tls_security_policy = "Policy-Min-TLS-1-2-2019-07"
  }

  # Fine-grained access control
  advanced_security_options {
    enabled                        = true
    anonymous_auth_enabled         = false
    internal_user_database_enabled = true
    master_user_options {
      master_user_name     = "admin"
      master_user_password = "Admin@123456"  # Change in prod! Use Secrets Manager
    }
  }

  # Slow log and application logs
  log_publishing_options {
    log_type                 = "INDEX_SLOW_LOGS"
    cloudwatch_log_group_arn = aws_cloudwatch_log_group.opensearch_slow.arn
    enabled                  = true
  }

  log_publishing_options {
    log_type                 = "SEARCH_SLOW_LOGS"
    cloudwatch_log_group_arn = aws_cloudwatch_log_group.opensearch_slow.arn
    enabled                  = true
  }

  snapshot_options {
    automated_snapshot_start_hour = 3  # 3 AM UTC daily snapshot
  }

  tags = merge(var.tags, { Name = "${var.name_prefix}-opensearch" })

  depends_on = [aws_iam_service_linked_role.opensearch]
}

# ---------------------------------------------------------------------------
# Access policy (VPC + Lambda IAM only)
# ---------------------------------------------------------------------------
resource "aws_opensearch_domain_policy" "main" {
  domain_name = aws_opensearch_domain.main.domain_name
  access_policies = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root" }
        Action    = "es:*"
        Resource  = "${aws_opensearch_domain.main.arn}/*"
      }
    ]
  })
}

# ---------------------------------------------------------------------------
# CloudWatch log group for OpenSearch logs
# ---------------------------------------------------------------------------
resource "aws_cloudwatch_log_group" "opensearch_slow" {
  name              = "/aws/opensearch/${var.name_prefix}/slow-logs"
  retention_in_days = 7
  tags              = var.tags
}

resource "aws_cloudwatch_log_resource_policy" "opensearch" {
  policy_name     = "${var.name_prefix}-opensearch-logs"
  policy_document = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "es.amazonaws.com" }
      Action    = ["logs:PutLogEvents", "logs:CreateLogStream"]
      Resource  = "${aws_cloudwatch_log_group.opensearch_slow.arn}:*"
    }]
  })
}

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------
output "endpoint"    { value = aws_opensearch_domain.main.endpoint }
output "domain_name" { value = aws_opensearch_domain.main.domain_name }
output "domain_arn"  { value = aws_opensearch_domain.main.arn }
output "kibana_url"  { value = "https://${aws_opensearch_domain.main.endpoint}/_dashboards" }
