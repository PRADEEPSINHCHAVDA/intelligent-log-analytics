# =============================================================================
# CloudWatch Module - Monitoring + Alerting
# =============================================================================

variable "name_prefix"          { type = string }
variable "lambda_function_name" { type = string }
variable "opensearch_domain"    { type = string }
variable "kafka_asg_name"       { type = string }
variable "alert_email"          { type = string; default = "" }
variable "tags"                 { type = map(string) }

# ---------------------------------------------------------------------------
# SNS topic for alerts
# ---------------------------------------------------------------------------
resource "aws_sns_topic" "alerts" {
  name = "${var.name_prefix}-alerts"
  tags = var.tags
}

resource "aws_sns_topic_subscription" "email" {
  count     = var.alert_email != "" ? 1 : 0
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# ---------------------------------------------------------------------------
# CloudWatch Alarms
# ---------------------------------------------------------------------------

# Lambda error rate > 5%
resource "aws_cloudwatch_metric_alarm" "lambda_errors" {
  alarm_name          = "${var.name_prefix}-lambda-error-rate"
  alarm_description   = "Lambda log consumer error rate above 5%"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "Sum"
  threshold           = 10
  alarm_actions       = [aws_sns_topic.alerts.arn]
  dimensions          = { FunctionName = var.lambda_function_name }
  tags                = var.tags
}

# Lambda duration > 10 minutes
resource "aws_cloudwatch_metric_alarm" "lambda_duration" {
  alarm_name          = "${var.name_prefix}-lambda-duration"
  alarm_description   = "Lambda execution approaching timeout"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Duration"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "Maximum"
  threshold           = 600000   # 10 minutes in ms
  alarm_actions       = [aws_sns_topic.alerts.arn]
  dimensions          = { FunctionName = var.lambda_function_name }
  tags                = var.tags
}

# OpenSearch cluster health red
resource "aws_cloudwatch_metric_alarm" "opensearch_health" {
  alarm_name          = "${var.name_prefix}-opensearch-health-red"
  alarm_description   = "OpenSearch cluster health is RED"
  comparison_operator = "LessThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ClusterStatus.green"
  namespace           = "AWS/ES"
  period              = 60
  statistic           = "Minimum"
  threshold           = 1
  alarm_actions       = [aws_sns_topic.alerts.arn]
  dimensions          = { DomainName = var.opensearch_domain, ClientId = data.aws_caller_identity.current.account_id }
  tags                = var.tags
}

# OpenSearch storage > 80%
resource "aws_cloudwatch_metric_alarm" "opensearch_storage" {
  alarm_name          = "${var.name_prefix}-opensearch-storage-high"
  alarm_description   = "OpenSearch free storage below 20%"
  comparison_operator = "LessThanThreshold"
  evaluation_periods  = 2
  metric_name         = "FreeStorageSpace"
  namespace           = "AWS/ES"
  period              = 300
  statistic           = "Minimum"
  threshold           = 4096   # 4GB free (20% of 20GB)
  alarm_actions       = [aws_sns_topic.alerts.arn]
  dimensions          = { DomainName = var.opensearch_domain, ClientId = data.aws_caller_identity.current.account_id }
  tags                = var.tags
}

data "aws_caller_identity" "current" {}

# ---------------------------------------------------------------------------
# CloudWatch Dashboard
# ---------------------------------------------------------------------------
resource "aws_cloudwatch_dashboard" "main" {
  dashboard_name = "${var.name_prefix}-overview"
  dashboard_body = jsonencode({
    widgets = [
      {
        type       = "metric"
        properties = {
          title  = "Lambda: Records Processed"
          period = 300
          metrics = [
            ["LogAnalyticsPlatform", "RecordsProcessed", "Service", "all"]
          ]
          view  = "timeSeries"
          stat  = "Sum"
        }
      },
      {
        type       = "metric"
        properties = {
          title  = "Lambda: Error Rate"
          period = 300
          metrics = [
            ["AWS/Lambda", "Errors", "FunctionName", var.lambda_function_name],
            ["AWS/Lambda", "Invocations", "FunctionName", var.lambda_function_name],
          ]
          view = "timeSeries"
        }
      },
      {
        type       = "metric"
        properties = {
          title  = "OpenSearch: Indexing Rate"
          period = 60
          metrics = [
            ["AWS/ES", "IndexingRate", "DomainName", var.opensearch_domain, "ClientId", data.aws_caller_identity.current.account_id]
          ]
          view = "timeSeries"
          stat = "Average"
        }
      },
      {
        type       = "metric"
        properties = {
          title  = "OpenSearch: Free Storage"
          period = 300
          metrics = [
            ["AWS/ES", "FreeStorageSpace", "DomainName", var.opensearch_domain, "ClientId", data.aws_caller_identity.current.account_id]
          ]
          view = "timeSeries"
          stat = "Minimum"
        }
      },
      {
        type       = "metric"
        properties = {
          title  = "Throughput (MB/s)"
          period = 60
          metrics = [
            ["LogAnalyticsPlatform", "ThroughputMBps"]
          ]
          view = "timeSeries"
          stat = "Average"
        }
      },
      {
        type       = "alarm"
        properties = {
          title  = "Active Alarms"
          alarms = [
            aws_cloudwatch_metric_alarm.lambda_errors.arn,
            aws_cloudwatch_metric_alarm.lambda_duration.arn,
            aws_cloudwatch_metric_alarm.opensearch_health.arn,
            aws_cloudwatch_metric_alarm.opensearch_storage.arn,
          ]
        }
      }
    ]
  })
}

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------
output "dashboard_name" { value = aws_cloudwatch_dashboard.main.dashboard_name }
output "alerts_topic_arn" { value = aws_sns_topic.alerts.arn }
