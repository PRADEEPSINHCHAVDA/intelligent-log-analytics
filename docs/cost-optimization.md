# Cost Optimization Guide

## Overview: $450 → $120/month (73% reduction)

Run this to see the full breakdown:
```bash
python3 scripts/cost-analysis.py
```

## Optimization Techniques Used

### 1. EC2 Spot Fleet (saves ~85% on compute)

**Spot vs On-demand prices (t3.small, us-east-1):**
- On-demand: $0.0208/hour = $15/month
- Spot:       ~$0.006/hour = $4.30/month per instance

**Fleet diversification** (3 types × 2 AZs = 6 options):
```
Instance types: t3.small, t3a.small, t2.small
AZs:            us-east-1a, us-east-1b
Strategy:       capacity-optimized (picks pool with most instances)
```
More options = lower interruption probability.

**Interruption handling:**
- AWS sends 2-minute warning via instance metadata
- `spot-interruption-handler.sh` polls `/latest/meta-data/spot/termination-time`
- On warning: gracefully stop Kafka (flush remaining messages)
- Consumer resumes from committed offsets on new instance

### 2. OpenSearch Right-Sizing

For dev/demo workloads (< 5GB data):
- `t3.small.search`: $0.036/hr = **$26/month** ✓
- `r6g.large.search`: $0.120/hr = $86/month ✗

Switch to larger instance when:
- Storage > 15GB (use 70% of capacity rule)
- Search latency p95 > 5 seconds
- CPU > 80% sustained

### 3. Hot/Warm/Cold Storage Tiering

OpenSearch ISM policy saves ~60% on storage:
```
Hot  (days 0-7):   SSD, 1 replica  → fast search, full cost
Warm (days 7-30):  SSD, 0 replicas → 50% cost reduction
Cold (days 30-90): read-only       → minimal cost
Delete (day 90+):  gone            → $0
```

Monthly storage cost example (2GB/day):
```
Hot tier  (7d × 2GB):   14GB × $0.135/GB  = $1.89
Warm tier (23d × 2GB):  46GB × $0.068/GB  = $3.13
Cold tier (60d × 2GB): 120GB × $0.045/GB  = $5.40
Total:                                      $10.42/month
```
vs storing 90 days on hot SSD: 180GB × $0.135 = **$24.30/month**

### 4. Lambda (near-zero cost)

Lambda free tier: 1M requests + 400,000 GB-seconds/month FREE.

At our scale (polling every 5 minutes):
- Invocations: 8,640/month (well within 1M free)
- Cost: **$0-2/month**

### 5. gp3 vs gp2 EBS

gp3 is 20% cheaper than gp2 with better baseline performance:
- gp2: $0.10/GB-month
- gp3: $0.08/GB-month (20% cheaper, 3000 IOPS baseline vs gp2's variable)

## Safe Shutdown Procedure (Avoid Surprise Bills)

### Local Docker (free, nothing to shut down)
```bash
docker compose down        # Stop containers (data preserved in volumes)
docker compose down -v     # Stop + delete all data
```

### AWS (run before leaving for the day!)
```bash
# Option A: Destroy everything (safest)
cd terraform && terraform destroy

# Option B: Scale down EC2 to 0 (keeps OpenSearch running)
aws autoscaling set-desired-capacity \
  --auto-scaling-group-name log-analytics-dev-kafka-asg \
  --desired-capacity 0

# Option C: Stop OpenSearch (saves ~$26/month)
aws opensearch update-domain-config \
  --domain-name log-analytics-dev-logs \
  --cluster-config '{"InstanceCount":0}'
```

### Set up billing alert (do this first!)
```bash
aws cloudwatch put-metric-alarm \
  --alarm-name "MonthlySpendAlert" \
  --alarm-description "Alert when monthly spend exceeds $50" \
  --metric-name EstimatedCharges \
  --namespace AWS/Billing \
  --statistic Maximum \
  --period 86400 \
  --threshold 50 \
  --comparison-operator GreaterThanThreshold \
  --alarm-actions arn:aws:sns:us-east-1:YOUR_ACCOUNT:your-topic \
  --dimensions Name=Currency,Value=USD
```

## Cost Tracking Commands

```bash
# Current month's cost
aws ce get-cost-and-usage \
  --time-period Start=$(date +%Y-%m-01),End=$(date +%Y-%m-%d) \
  --granularity MONTHLY \
  --metrics "UnblendedCost" \
  --query 'ResultsByTime[0].Total.UnblendedCost'

# Cost by service
aws ce get-cost-and-usage \
  --time-period Start=$(date +%Y-%m-01),End=$(date +%Y-%m-%d) \
  --granularity MONTHLY \
  --metrics "UnblendedCost" \
  --group-by Type=DIMENSION,Key=SERVICE \
  --query 'ResultsByTime[0].Groups[*].[Keys[0],Metrics.UnblendedCost.Amount]' \
  --output table

# Run cost analysis script
python3 scripts/cost-analysis.py --report
```
