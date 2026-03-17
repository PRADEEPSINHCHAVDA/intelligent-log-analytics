# Architecture - Log Analytics Platform

## System Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                   LOG ANALYTICS PLATFORM                        │
│                                                                 │
│  8 Microservices          Kafka (EC2 Spot Fleet)                │
│  ┌─────────────┐          ┌─────────────────────┐              │
│  │user-service │──────────▶ Topic: logs.user-svc │              │
│  │payment-svc  │──────────▶ Topic: logs.payment  │              │
│  │order-service│──────────▶ Topic: logs.order     │──┐          │
│  │notification │──────────▶ Topic: logs.notif     │  │          │
│  │inventory    │──────────▶ Topic: logs.inventory │  │ Lambda   │
│  │auth-service │──────────▶ Topic: logs.auth      │  │ Consumer │
│  │api-gateway  │──────────▶ Topic: logs.api-gw    │  │          │
│  │reporting    │──────────▶ Topic: logs.reporting │  │          │
│  └─────────────┘          └─────────────────────┘  │          │
│                                                      │          │
│                           ┌──────────────────────┐  │          │
│                           │   AWS Lambda          │◀─┘          │
│                           │   log_consumer.py     │             │
│                           │   - Enrich logs       │             │
│                           │   - Correlation ID    │             │
│                           │   - Bulk index        │             │
│                           └──────────┬───────────┘             │
│                                      │                          │
│                           ┌──────────▼───────────┐             │
│                           │   Amazon OpenSearch   │             │
│                           │   logs-{svc}-YYYY.MM.DD│            │
│                           │                       │             │
│                           │   HOT  (0-7d):  SSD   │             │
│                           │   WARM (7-30d): reduced│            │
│                           │   COLD (30-90d): r/o  │             │
│                           │   DELETE (>90d)       │             │
│                           └──────────┬───────────┘             │
│                                      │                          │
│                           ┌──────────▼───────────┐             │
│                           │  OpenSearch Dashboards│             │
│                           │  (Kibana)             │             │
│                           │  - Log search         │             │
│                           │  - Error dashboards   │             │
│                           │  - Latency analysis   │             │
│                           └───────────────────────┘             │
└─────────────────────────────────────────────────────────────────┘
```

## EC2 Spot Fleet Architecture

```
                    EC2 Spot Fleet (kafka_asg)
                    ┌────────────────────────────┐
  AZ: us-east-1a   │  t3.small   (primary)      │
                    │  t3a.small  (AMD, cheaper) │
                    │  t2.small   (fallback)     │
                    ├────────────────────────────┤
  AZ: us-east-1b   │  t3.small   (primary)      │
                    │  t3a.small  (AMD, cheaper) │
                    │  t2.small   (fallback)     │
                    └────────────────────────────┘

  Strategy: capacity-optimized (best Spot availability)
  Interruption: 2-min warning → graceful Kafka shutdown
```

## Data Flow with Correlation IDs

```
Browser Request
    │
    ▼
API Gateway Service ── generates correlation_id: "abc-123"
    │
    ├──▶ User Service      logs: {correlation_id: "abc-123", ...}
    │
    ├──▶ Auth Service      logs: {correlation_id: "abc-123", ...}
    │
    ├──▶ Order Service     logs: {correlation_id: "abc-123", ...}
    │
    └──▶ Payment Service   logs: {correlation_id: "abc-123", ...}

OpenSearch query: GET /logs-*/_search?q=correlation_id:abc-123
→ Returns all 4 log entries, ordered by timestamp
→ Complete request trace across microservices in one query
```

## Cost Architecture

```
BEFORE (naive approach):         AFTER (this project):
┌───────────────────────┐        ┌───────────────────────┐
│ MSK (Kafka managed)   │        │ Kafka on EC2 Spot      │
│ 3× kafka.m5.large     │        │ 3× t3.small (Spot)     │
│ $183/month            │        │ $12/month              │
├───────────────────────┤        ├───────────────────────┤
│ OpenSearch r6g.large  │        │ OpenSearch t3.small    │
│ $87/month             │        │ $26/month              │
├───────────────────────┤        ├───────────────────────┤
│ EC2 on-demand compute │        │ EC2 Spot Fleet         │
│ $108/month            │        │ $11/month              │
├───────────────────────┤        ├───────────────────────┤
│ gp2 storage           │        │ gp3 + S3 Glacier tiers │
│ $42/month             │        │ $8/month               │
├───────────────────────┤        ├───────────────────────┤
│ TOTAL: ~$450/month    │        │ TOTAL: ~$57-67/month   │
└───────────────────────┘        └───────────────────────┘
                                          ▲
                                   73% reduction
```

## Index Naming and Rotation

```
Daily index rotation:
logs-user-service-2024.01.15
logs-payment-service-2024.01.15
...

ISM Policy transitions:
Day 0-7   (HOT):  number_of_replicas=1, full SSD access
Day 7-30  (WARM): number_of_replicas=0, lower priority
Day 30-90 (COLD): read_only=true, no replicas
Day 90+   (DEL):  index deleted automatically
```

## Security Architecture

- All Kafka brokers in **private subnets** (no public IP)
- OpenSearch in **private subnets** (no public endpoint)
- Lambda in VPC with **minimal egress rules**
- EC2 accessed via **AWS Systems Manager** (no SSH port open externally)
- **IMDSv2** required on all EC2 instances
- OpenSearch: **encryption at rest + in transit** (TLS 1.2+)
- IAM: **least privilege** - Lambda can only write to OpenSearch, not read
- **SQS DLQ** for failed records (no data loss)
