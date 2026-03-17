# =============================================================================
# Lambda Module - Log Consumer
# =============================================================================

variable "name_prefix"             { type = string }
variable "vpc_id"                  { type = string }
variable "private_subnet_ids"      { type = list(string) }
variable "opensearch_endpoint"     { type = string }
variable "kafka_bootstrap_servers" { type = string }
variable "kafka_topics"            { type = list(string) }
variable "dlq_arn"                 { type = string }
variable "tags"                    { type = map(string) }

data "aws_region" "current" {}
data "aws_caller_identity" "current" {}

# ---------------------------------------------------------------------------
# Security group
# ---------------------------------------------------------------------------
resource "aws_security_group" "lambda" {
  name        = "${var.name_prefix}-lambda-sg"
  description = "Lambda log consumer"
  vpc_id      = var.vpc_id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(var.tags, { Name = "${var.name_prefix}-lambda-sg" })
}

# ---------------------------------------------------------------------------
# IAM role
# ---------------------------------------------------------------------------
resource "aws_iam_role" "lambda" {
  name = "${var.name_prefix}-lambda-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
  tags = var.tags
}

resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

resource "aws_iam_role_policy" "lambda_custom" {
  name = "${var.name_prefix}-lambda-policy"
  role = aws_iam_role.lambda.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["es:ESHttpPost", "es:ESHttpPut", "es:ESHttpGet"]
        Resource = "arn:aws:es:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:domain/*"
      },
      {
        Effect   = "Allow"
        Action   = ["sqs:SendMessage"]
        Resource = var.dlq_arn
      },
      {
        Effect   = "Allow"
        Action   = ["cloudwatch:PutMetricData"]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["kafka:DescribeCluster", "kafka:GetBootstrapBrokers"]
        Resource = "*"
      }
    ]
  })
}

# ---------------------------------------------------------------------------
# CloudWatch Log Group for Lambda
# ---------------------------------------------------------------------------
resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/aws/lambda/${var.name_prefix}-log-consumer"
  retention_in_days = 7
  tags              = var.tags
}

# ---------------------------------------------------------------------------
# Lambda function
# ---------------------------------------------------------------------------
resource "aws_lambda_function" "log_consumer" {
  function_name = "${var.name_prefix}-log-consumer"
  role          = aws_iam_role.lambda.arn
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.11"
  timeout       = 900    # 15 minutes max
  memory_size   = 512    # 512MB for bulk indexing

  # Package lambda code (zip before apply: see scripts/package-lambda.sh)
  filename         = "${path.module}/../../../lambda/log_consumer/function.zip"
  source_code_hash = filebase64sha256("${path.module}/../../../lambda/log_consumer/function.zip")

  vpc_config {
    subnet_ids         = var.private_subnet_ids
    security_group_ids = [aws_security_group.lambda.id]
  }

  environment {
    variables = {
      OPENSEARCH_ENDPOINT     = "https://${var.opensearch_endpoint}"
      KAFKA_BOOTSTRAP_SERVERS = var.kafka_bootstrap_servers
      KAFKA_TOPIC_PREFIX      = "logs"
      CONSUMER_GROUP          = "log-analytics-lambda"
      BATCH_SIZE              = "500"
      DLQ_URL                 = var.dlq_arn
      AWS_DEFAULT_REGION      = data.aws_region.current.name
    }
  }

  dead_letter_config {
    target_arn = var.dlq_arn
  }

  tracing_config {
    mode = "Active"   # AWS X-Ray tracing
  }

  tags = var.tags

  depends_on = [
    aws_cloudwatch_log_group.lambda,
    aws_iam_role_policy_attachment.lambda_basic,
  ]
}

# ---------------------------------------------------------------------------
# EventBridge: trigger Lambda every 5 minutes to poll Kafka
# (In prod, use MSK trigger for event-driven consumption)
# ---------------------------------------------------------------------------
resource "aws_cloudwatch_event_rule" "lambda_schedule" {
  name                = "${var.name_prefix}-lambda-schedule"
  description         = "Trigger log consumer Lambda every 5 minutes"
  schedule_expression = "rate(5 minutes)"
  tags                = var.tags
}

resource "aws_cloudwatch_event_target" "lambda" {
  rule = aws_cloudwatch_event_rule.lambda_schedule.name
  arn  = aws_lambda_function.log_consumer.arn
}

resource "aws_lambda_permission" "eventbridge" {
  statement_id  = "AllowEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.log_consumer.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.lambda_schedule.arn
}

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------
output "function_name" { value = aws_lambda_function.log_consumer.function_name }
output "function_arn"  { value = aws_lambda_function.log_consumer.arn }
