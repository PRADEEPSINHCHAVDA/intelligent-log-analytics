# =============================================================================
# EC2 Spot Fleet Module - Kafka Brokers
# =============================================================================
# Deploys Kafka on EC2 Spot Instances with Fleet diversification:
#   - 3 instance types (t3.small, t3a.small, t2.small)
#   - 2 AZs for high availability
#   - Auto Spot interruption handling (graceful shutdown)
#   - User data installs and configures Kafka automatically
#
# Cost: ~$8-15/month vs ~$80/month on-demand
# =============================================================================

variable "name_prefix"        { type = string }
variable "vpc_id"             { type = string }
variable "private_subnet_ids" { type = list(string) }
variable "key_pair_name"      { type = string; default = "" }
variable "kafka_version"      { type = string; default = "3.6.0" }
variable "tags"               { type = map(string) }

# ---------------------------------------------------------------------------
# Data: Latest Amazon Linux 2 AMI
# ---------------------------------------------------------------------------
data "aws_ami" "amazon_linux2" {
  most_recent = true
  owners      = ["amazon"]
  filter {
    name   = "name"
    values = ["amzn2-ami-hvm-*-x86_64-gp2"]
  }
  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

# ---------------------------------------------------------------------------
# IAM role for EC2 (Kafka brokers)
# ---------------------------------------------------------------------------
resource "aws_iam_role" "kafka" {
  name = "${var.name_prefix}-kafka-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
  tags = var.tags
}

resource "aws_iam_role_policy_attachment" "kafka_ssm" {
  role       = aws_iam_role.kafka.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_role_policy" "kafka_cloudwatch" {
  name = "${var.name_prefix}-kafka-cw"
  role = aws_iam_role.kafka.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "cloudwatch:PutMetricData",
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents",
        "ec2:DescribeInstances",     # For Spot interruption detection
        "ec2:DescribeSpotInstanceRequests",
      ]
      Resource = "*"
    }]
  })
}

resource "aws_iam_instance_profile" "kafka" {
  name = "${var.name_prefix}-kafka-profile"
  role = aws_iam_role.kafka.name
}

# ---------------------------------------------------------------------------
# Security group (Kafka ports - internal VPC only)
# ---------------------------------------------------------------------------
resource "aws_security_group" "kafka" {
  name        = "${var.name_prefix}-kafka-sg"
  description = "Kafka brokers - internal VPC only"
  vpc_id      = var.vpc_id

  ingress {
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
    description = "Kafka plaintext"
  }

  ingress {
    from_port   = 2181
    to_port     = 2181
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
    description = "Zookeeper"
  }

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
    description = "SSH via bastion/SSM only"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(var.tags, { Name = "${var.name_prefix}-kafka-sg" })
}

# ---------------------------------------------------------------------------
# Launch template (used by Spot Fleet and Auto Scaling)
# ---------------------------------------------------------------------------
resource "aws_launch_template" "kafka" {
  name_prefix   = "${var.name_prefix}-kafka-lt-"
  image_id      = data.aws_ami.amazon_linux2.id
  instance_type = "t3.small"   # Default; Spot Fleet will override with diversification

  iam_instance_profile {
    name = aws_iam_instance_profile.kafka.name
  }

  network_interfaces {
    associate_public_ip_address = false
    security_groups             = [aws_security_group.kafka.id]
  }

  key_name = var.key_pair_name != "" ? var.key_pair_name : null

  # EBS root volume (encrypted)
  block_device_mappings {
    device_name = "/dev/xvda"
    ebs {
      volume_size           = 30     # 30GB for Kafka logs
      volume_type           = "gp3"
      iops                  = 3000
      encrypted             = true
      delete_on_termination = true
    }
  }

  # User data: install and configure Kafka + Spot interruption handler
  user_data = base64encode(templatefile("${path.module}/userdata.sh.tpl", {
    kafka_version = var.kafka_version
  }))

  metadata_options {
    http_tokens                 = "required"   # IMDSv2 (security best practice)
    http_put_response_hop_limit = 1
  }

  tag_specifications {
    resource_type = "instance"
    tags = merge(var.tags, { Name = "${var.name_prefix}-kafka-broker" })
  }

  lifecycle { create_before_destroy = true }
}

# ---------------------------------------------------------------------------
# Auto Scaling Group (with Spot Fleet diversification)
# 3 instance types × 2 AZs = 6 possible placement combinations
# ---------------------------------------------------------------------------
resource "aws_autoscaling_group" "kafka" {
  name                = "${var.name_prefix}-kafka-asg"
  desired_capacity    = 1          # Dev: 1 broker. Prod: 3 for fault tolerance
  min_size            = 1
  max_size            = 3
  vpc_zone_identifier = var.private_subnet_ids

  # Spot Fleet diversification: 3 instance types
  mixed_instances_policy {
    instances_distribution {
      on_demand_base_capacity                  = 0      # 100% Spot
      on_demand_percentage_above_base_capacity = 0
      spot_allocation_strategy                 = "capacity-optimized"  # Best availability
      spot_max_price                           = "0.05"  # Max bid (safety cap)
    }

    launch_template {
      launch_template_specification {
        launch_template_id = aws_launch_template.kafka.id
        version            = "$Latest"
      }

      # Instance type diversification (3 types as required)
      override {
        instance_type     = "t3.small"
        weighted_capacity = "1"
      }
      override {
        instance_type     = "t3a.small"   # AMD variant (often cheaper)
        weighted_capacity = "1"
      }
      override {
        instance_type     = "t2.small"    # Fallback option
        weighted_capacity = "1"
      }
    }
  }

  # Health checks
  health_check_type         = "EC2"
  health_check_grace_period = 300

  # Instance refresh (zero-downtime Kafka rolling updates)
  instance_refresh {
    strategy = "Rolling"
    preferences {
      min_healthy_percentage = 50
      instance_warmup        = 300
    }
  }

  tag {
    key                 = "Name"
    value               = "${var.name_prefix}-kafka-broker"
    propagate_at_launch = true
  }

  lifecycle { create_before_destroy = true }
}

# ---------------------------------------------------------------------------
# EventBridge rule: catch Spot interruption warnings (2-minute advance notice)
# ---------------------------------------------------------------------------
resource "aws_cloudwatch_event_rule" "spot_interruption" {
  name        = "${var.name_prefix}-spot-interruption"
  description = "Detect EC2 Spot interruption (2-min warning)"
  event_pattern = jsonencode({
    source      = ["aws.ec2"]
    detail-type = ["EC2 Spot Instance Interruption Warning"]
  })
  tags = var.tags
}

resource "aws_cloudwatch_event_target" "spot_interruption_sns" {
  rule = aws_cloudwatch_event_rule.spot_interruption.name
  arn  = aws_sns_topic.spot_interruption.arn
}

resource "aws_sns_topic" "spot_interruption" {
  name = "${var.name_prefix}-spot-interruption"
  tags = var.tags
}

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------
output "asg_name" {
  value = aws_autoscaling_group.kafka.name
}

output "kafka_bootstrap_servers" {
  description = "Kafka broker endpoints (resolved at runtime via DNS)"
  value       = "kafka.internal:9092"  # Use Route53 private zone in prod
}

output "kafka_sg_id" {
  value = aws_security_group.kafka.id
}
