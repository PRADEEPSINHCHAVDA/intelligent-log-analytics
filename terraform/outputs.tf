# =============================================================================
# Outputs - use these after `terraform apply`
# =============================================================================

output "vpc_id" {
  description = "VPC ID"
  value       = module.vpc.vpc_id
}

output "kafka_bootstrap_servers" {
  description = "Kafka broker endpoints (internal)"
  value       = module.ec2_spot.kafka_bootstrap_servers
  sensitive   = false
}

output "opensearch_endpoint" {
  description = "OpenSearch domain endpoint"
  value       = module.opensearch.endpoint
}

output "opensearch_dashboard_url" {
  description = "Kibana/OpenSearch Dashboards URL"
  value       = "https://${module.opensearch.endpoint}/_dashboards"
}

output "lambda_function_name" {
  description = "Lambda function name"
  value       = module.lambda.function_name
}

output "lambda_function_arn" {
  description = "Lambda function ARN"
  value       = module.lambda.function_arn
}

output "dlq_url" {
  description = "Dead Letter Queue URL"
  value       = aws_sqs_queue.dlq.url
}

output "artifacts_bucket" {
  description = "S3 bucket for artifacts and archived logs"
  value       = aws_s3_bucket.artifacts.bucket
}

output "cloudwatch_dashboard_url" {
  description = "CloudWatch dashboard URL"
  value       = "https://${var.aws_region}.console.aws.amazon.com/cloudwatch/home?region=${var.aws_region}#dashboards:name=${module.cloudwatch.dashboard_name}"
}

output "monthly_cost_estimate" {
  description = "Rough monthly cost estimate breakdown"
  value = {
    kafka_spot_ec2  = "~$8-15  (t3.small Spot Fleet, 3 instances)"
    opensearch      = "~$26    (t3.small.search, 1 node)"
    lambda          = "~$0-2   (within free tier for dev traffic)"
    data_transfer   = "~$1-3"
    s3              = "~$1-2"
    total_estimate  = "~$36-48/month"
    vs_ondemand     = "~$450/month (EC2 on-demand + MSK)"
    savings         = "~73-92%"
  }
}
