"""
Log Consumer Lambda Function
=============================
Processes log records from Kafka and indexes them into OpenSearch.

Production flow:  Kafka (EC2 Spot) → Lambda → OpenSearch
Local dev flow:   Kafka (Docker)   → This script → OpenSearch (Docker)

Features:
- Correlation ID tracking across microservices
- Structured log parsing + enrichment
- Bulk indexing to OpenSearch (hot/warm/cold tiers)
- Error handling with dead-letter queue fallback
- CloudWatch metrics emission
- Auto-scaling trigger based on consumer lag
"""

import json
import os
import re
import time
import logging
import hashlib
from datetime import datetime, timezone
from typing import Any
import boto3
from kafka import KafkaConsumer
from opensearchpy import OpenSearch, helpers, RequestsHttpConnection
from opensearchpy.exceptions import OpenSearchException

# ---------------------------------------------------------------------------
# Configuration (from environment variables)
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_PREFIX = os.environ.get("KAFKA_TOPIC_PREFIX", "logs")
KAFKA_TOPICS = [
    f"{KAFKA_TOPIC_PREFIX}.user-service",
    f"{KAFKA_TOPIC_PREFIX}.payment-service",
    f"{KAFKA_TOPIC_PREFIX}.order-service",
    f"{KAFKA_TOPIC_PREFIX}.notification-service",
    f"{KAFKA_TOPIC_PREFIX}.inventory-service",
    f"{KAFKA_TOPIC_PREFIX}.auth-service",
    f"{KAFKA_TOPIC_PREFIX}.api-gateway-service",
    f"{KAFKA_TOPIC_PREFIX}.reporting-service",
]
CONSUMER_GROUP = os.environ.get("CONSUMER_GROUP", "log-analytics-consumer")
OPENSEARCH_ENDPOINT = os.environ.get("OPENSEARCH_ENDPOINT", "http://localhost:9200")
AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "100"))
POLL_TIMEOUT_MS = int(os.environ.get("POLL_TIMEOUT_MS", "1000"))
DLQ_URL = os.environ.get("DLQ_URL", "")  # SQS Dead Letter Queue URL

# OpenSearch index tiers (hot→warm→cold)
INDEX_HOT_DAYS = 7
INDEX_WARM_DAYS = 30
INDEX_COLD_DAYS = 90

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("log-consumer")

# ---------------------------------------------------------------------------
# OpenSearch client
# ---------------------------------------------------------------------------
def get_opensearch_client() -> OpenSearch:
    """Create OpenSearch client. Handles both local (HTTP) and AWS (HTTPS+SigV4)."""
    endpoint = OPENSEARCH_ENDPOINT.replace("http://", "").replace("https://", "")
    use_ssl = OPENSEARCH_ENDPOINT.startswith("https")

    if use_ssl:
        # Production: AWS OpenSearch with SigV4 signing
        from requests_aws4auth import AWS4Auth
        credentials = boto3.Session().get_credentials()
        auth = AWS4Auth(
            credentials.access_key,
            credentials.secret_key,
            AWS_REGION,
            "es",
            session_token=credentials.token,
        )
        return OpenSearch(
            hosts=[{"host": endpoint, "port": 443}],
            http_auth=auth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection,
        )
    else:
        # Local Docker: plain HTTP
        host, port = (endpoint.split(":") + ["9200"])[:2]
        return OpenSearch(
            hosts=[{"host": host, "port": int(port)}],
            use_ssl=False,
            verify_certs=False,
        )


# ---------------------------------------------------------------------------
# Log enrichment + parsing
# ---------------------------------------------------------------------------
def enrich_log_record(raw_record: dict) -> dict:
    """
    Enrich raw log record with additional metadata for OpenSearch indexing.

    Adds:
      - @timestamp normalized to ISO8601 UTC
      - correlation_id (generated if missing)
      - log_level normalized to uppercase
      - service metadata (name, version, environment)
      - geo/request data if HTTP log
      - error classification if error log
      - index routing hints (hot/warm/cold tier)
    """
    enriched = dict(raw_record)

    # --- Timestamp normalization ---
    ts = enriched.get("timestamp") or enriched.get("@timestamp")
    if ts:
        try:
            if isinstance(ts, (int, float)):
                dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
            else:
                dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
            enriched["@timestamp"] = dt.isoformat()
        except (ValueError, OSError):
            enriched["@timestamp"] = datetime.now(timezone.utc).isoformat()
    else:
        enriched["@timestamp"] = datetime.now(timezone.utc).isoformat()

    # --- Correlation ID ---
    if not enriched.get("correlation_id"):
        # Generate deterministic ID from content hash
        content = json.dumps(enriched, sort_keys=True, default=str)
        enriched["correlation_id"] = hashlib.md5(
            content.encode(), usedforsecurity=False
        ).hexdigest()[:16]

    # --- Log level normalization ---
    level = str(enriched.get("level") or enriched.get("severity") or "INFO")
    enriched["level"] = level.upper()
    enriched["level_numeric"] = {
        "TRACE": 10, "DEBUG": 20, "INFO": 30,
        "WARN": 40, "WARNING": 40, "ERROR": 50, "FATAL": 60,
    }.get(enriched["level"], 30)

    # --- Service metadata ---
    enriched.setdefault("service", "unknown-service")
    enriched.setdefault("version", "1.0.0")
    enriched.setdefault("environment", "production")
    enriched.setdefault("region", AWS_REGION)

    # --- HTTP log enrichment ---
    if enriched.get("http_method") or enriched.get("status_code"):
        sc = int(enriched.get("status_code", 0))
        enriched["http_status_class"] = f"{sc // 100}xx" if sc else "unknown"
        enriched["is_error"] = sc >= 400

    # --- Error classification ---
    if enriched["level"] in ("ERROR", "FATAL"):
        error_msg = str(enriched.get("message", "") + enriched.get("error", ""))
        enriched["error_category"] = _classify_error(error_msg)
        enriched["requires_alert"] = enriched["level"] == "FATAL"

    # --- Duration bucketing (for latency analysis) ---
    duration_ms = enriched.get("duration_ms")
    if duration_ms is not None:
        ms = float(duration_ms)
        if ms < 100:
            enriched["latency_bucket"] = "fast"
        elif ms < 500:
            enriched["latency_bucket"] = "normal"
        elif ms < 2000:
            enriched["latency_bucket"] = "slow"
        else:
            enriched["latency_bucket"] = "very_slow"

    # --- Ingestion metadata ---
    enriched["_ingested_at"] = datetime.now(timezone.utc).isoformat()
    enriched["_pipeline_version"] = "1.0.0"

    return enriched


def _classify_error(message: str) -> str:
    """Classify error type from message text."""
    msg_lower = message.lower()
    patterns = {
        "database": r"(sql|mysql|postgres|mongodb|db|database|query|connection pool)",
        "network": r"(timeout|connection refused|dns|socket|network|unreachable)",
        "authentication": r"(auth|unauthorized|forbidden|401|403|token|jwt|oauth)",
        "validation": r"(invalid|validation|required|missing|schema|format)",
        "payment": r"(payment|stripe|paypal|charge|billing|card)",
        "rate_limit": r"(rate limit|throttl|429|too many requests)",
        "not_found": r"(not found|404|does not exist|missing)",
        "server": r"(500|internal server|exception|panic|crash)",
    }
    for category, pattern in patterns.items():
        if re.search(pattern, msg_lower):
            return category
    return "unknown"


# ---------------------------------------------------------------------------
# OpenSearch index management
# ---------------------------------------------------------------------------
def get_index_name(service: str, timestamp_str: str) -> str:
    """
    Generate daily index name for log rotation.
    Pattern: logs-{service}-YYYY.MM.DD
    ISM policy will move this from hot → warm → cold automatically.
    """
    try:
        dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        date_str = dt.strftime("%Y.%m.%d")
    except (ValueError, AttributeError):
        date_str = datetime.now(timezone.utc).strftime("%Y.%m.%d")
    safe_service = re.sub(r"[^a-z0-9-]", "-", service.lower())
    return f"logs-{safe_service}-{date_str}"


def ensure_index_template(client: OpenSearch) -> None:
    """Create OpenSearch index template if it doesn't exist."""
    template_name = "logs-template"
    if client.indices.exists_template(name=template_name):
        return

    template = {
        "index_patterns": ["logs-*"],
        "settings": {
            "number_of_shards": 2,
            "number_of_replicas": 1,
            "refresh_interval": "30s",
            "index.codec": "best_compression",
        },
        "mappings": {
            "dynamic_templates": [
                {
                    "strings_as_keywords": {
                        "match_mapping_type": "string",
                        "mapping": {"type": "keyword", "ignore_above": 256},
                    }
                },
                {
                    "message_as_text": {
                        "match": "message",
                        "mapping": {
                            "type": "text",
                            "fields": {"keyword": {"type": "keyword", "ignore_above": 512}},
                        },
                    }
                },
            ],
            "properties": {
                "@timestamp": {"type": "date"},
                "_ingested_at": {"type": "date"},
                "correlation_id": {"type": "keyword"},
                "service": {"type": "keyword"},
                "version": {"type": "keyword"},
                "environment": {"type": "keyword"},
                "region": {"type": "keyword"},
                "level": {"type": "keyword"},
                "level_numeric": {"type": "integer"},
                "message": {
                    "type": "text",
                    "fields": {"keyword": {"type": "keyword", "ignore_above": 512}},
                },
                "trace_id": {"type": "keyword"},
                "span_id": {"type": "keyword"},
                "user_id": {"type": "keyword"},
                "session_id": {"type": "keyword"},
                "request_id": {"type": "keyword"},
                "http_method": {"type": "keyword"},
                "http_path": {"type": "keyword"},
                "http_status_class": {"type": "keyword"},
                "status_code": {"type": "integer"},
                "duration_ms": {"type": "float"},
                "latency_bucket": {"type": "keyword"},
                "error_category": {"type": "keyword"},
                "requires_alert": {"type": "boolean"},
                "is_error": {"type": "boolean"},
                "ip_address": {"type": "ip"},
            },
        },
    }
    client.indices.put_template(name=template_name, body=template)
    logger.info("Created OpenSearch index template: %s", template_name)


# ---------------------------------------------------------------------------
# Bulk indexing to OpenSearch
# ---------------------------------------------------------------------------
def index_batch(client: OpenSearch, records: list[dict]) -> tuple[int, int]:
    """
    Bulk index a batch of log records into OpenSearch.
    Returns (success_count, failure_count).
    """
    actions = []
    for record in records:
        index_name = get_index_name(
            record.get("service", "unknown"),
            record.get("@timestamp", ""),
        )
        actions.append({
            "_index": index_name,
            "_source": record,
        })

    success, errors = 0, 0
    try:
        ok, failed = helpers.bulk(
            client,
            actions,
            chunk_size=500,
            request_timeout=30,
            raise_on_error=False,
        )
        success = ok
        errors = len(failed)
        if failed:
            logger.warning("Bulk index failures: %d records", errors)
            for err in failed[:3]:   # Log first 3 errors
                logger.error("Index error: %s", json.dumps(err, default=str))
    except OpenSearchException as exc:
        logger.error("OpenSearch bulk error: %s", exc)
        errors = len(records)
    return success, errors


# ---------------------------------------------------------------------------
# Dead Letter Queue (for failed records)
# ---------------------------------------------------------------------------
def send_to_dlq(records: list[dict], reason: str) -> None:
    """Send failed records to SQS Dead Letter Queue."""
    if not DLQ_URL:
        logger.warning("No DLQ configured. Dropping %d failed records.", len(records))
        return
    try:
        sqs = boto3.client("sqs", region_name=AWS_REGION)
        for record in records:
            sqs.send_message(
                QueueUrl=DLQ_URL,
                MessageBody=json.dumps(record, default=str),
                MessageAttributes={
                    "FailureReason": {"DataType": "String", "StringValue": reason},
                    "Timestamp": {
                        "DataType": "String",
                        "StringValue": datetime.now(timezone.utc).isoformat(),
                    },
                },
            )
        logger.info("Sent %d records to DLQ", len(records))
    except Exception as exc:
        logger.error("Failed to send to DLQ: %s", exc)


# ---------------------------------------------------------------------------
# CloudWatch metrics
# ---------------------------------------------------------------------------
class MetricsEmitter:
    """Emit custom CloudWatch metrics for pipeline monitoring."""

    def __init__(self):
        try:
            self.cw = boto3.client("cloudwatch", region_name=AWS_REGION)
            self.namespace = "LogAnalyticsPlatform"
            self._enabled = True
        except Exception:
            self._enabled = False

    def emit(self, metric_name: str, value: float, unit: str = "Count", **dims) -> None:
        if not self._enabled:
            return
        dimensions = [{"Name": k, "Value": str(v)} for k, v in dims.items()]
        try:
            self.cw.put_metric_data(
                Namespace=self.namespace,
                MetricData=[{
                    "MetricName": metric_name,
                    "Value": value,
                    "Unit": unit,
                    "Dimensions": dimensions,
                    "Timestamp": datetime.now(timezone.utc),
                }],
            )
        except Exception as exc:
            logger.debug("CloudWatch emit failed (non-critical): %s", exc)


# ---------------------------------------------------------------------------
# Main consumer loop (runs as long-lived process or Lambda handler)
# ---------------------------------------------------------------------------
def run_consumer():
    """
    Main Kafka consumer loop.
    In production: triggered by EventBridge/MSK trigger on Lambda.
    Locally: runs as a continuous process.
    """
    logger.info("Starting log consumer...")
    logger.info("Kafka: %s | Topics: %s", KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPICS)
    logger.info("OpenSearch: %s | Batch size: %d", OPENSEARCH_ENDPOINT, BATCH_SIZE)

    # Initialize clients
    os_client = get_opensearch_client()
    metrics = MetricsEmitter()

    # Ensure index template exists
    try:
        ensure_index_template(os_client)
    except Exception as exc:
        logger.warning("Could not create index template: %s", exc)

    # Create Kafka consumer
    consumer = KafkaConsumer(
        *KAFKA_TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        auto_commit_interval_ms=5000,
        max_poll_records=BATCH_SIZE,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=-1,    # Poll indefinitely
        session_timeout_ms=30000,
        heartbeat_interval_ms=10000,
    )

    logger.info("Consumer connected. Waiting for messages...")

    # Metrics accumulators
    total_processed = 0
    total_errors = 0
    batch_start = time.time()
    batch_bytes = 0

    try:
        while True:
            batch: list[dict] = []
            raw_records = consumer.poll(timeout_ms=POLL_TIMEOUT_MS)

            for tp, messages in raw_records.items():
                for msg in messages:
                    try:
                        record = msg.value
                        if not isinstance(record, dict):
                            record = {"message": str(record), "service": "unknown"}
                        enriched = enrich_log_record(record)
                        batch.append(enriched)
                        batch_bytes += len(json.dumps(enriched, default=str))
                    except (json.JSONDecodeError, ValueError) as exc:
                        logger.warning("Parse error on msg offset %d: %s", msg.offset, exc)

            if batch:
                ok, err = index_batch(os_client, batch)
                total_processed += ok
                total_errors += err

                elapsed = time.time() - batch_start
                throughput_mbps = (batch_bytes / 1024 / 1024) / elapsed if elapsed > 0 else 0

                logger.info(
                    "Batch: %d indexed, %d errors | Total: %d | "
                    "Throughput: %.2f MB/s",
                    ok, err, total_processed, throughput_mbps,
                )

                # Emit CloudWatch metrics
                metrics.emit("RecordsProcessed", ok, "Count", Service="all")
                metrics.emit("RecordErrors", err, "Count", Service="all")
                metrics.emit("ThroughputMBps", throughput_mbps, "None")

                if err > 0:
                    # Send failed records to DLQ
                    failed_batch = batch[-err:] if err <= len(batch) else batch
                    send_to_dlq(failed_batch, "OpenSearch indexing failure")

                # Reset batch counters
                batch_bytes = 0
                batch_start = time.time()

    except KeyboardInterrupt:
        logger.info("Shutting down gracefully...")
    finally:
        consumer.close()
        logger.info("Consumer closed. Total processed: %d", total_processed)


# ---------------------------------------------------------------------------
# AWS Lambda handler (used when deployed to real Lambda)
# ---------------------------------------------------------------------------
def lambda_handler(event: dict[str, Any], context: Any) -> dict:
    """
    AWS Lambda entry point.
    Triggered by EventBridge schedule or MSK trigger.

    For MSK (Managed Kafka) trigger, event contains:
    {
        "eventSource": "aws:kafka",
        "records": {
            "topic-0": [
                {"value": "<base64-encoded-json>", "offset": 0, ...}
            ]
        }
    }
    """
    import base64

    os_client = get_opensearch_client()
    metrics = MetricsEmitter()

    try:
        ensure_index_template(os_client)
    except Exception:
        pass

    records_to_index: list[dict] = []

    # Parse MSK trigger event
    if event.get("eventSource") == "aws:kafka":
        for topic, messages in event.get("records", {}).items():
            for msg in messages:
                try:
                    raw = base64.b64decode(msg["value"]).decode("utf-8")
                    record = json.loads(raw)
                    enriched = enrich_log_record(record)
                    records_to_index.append(enriched)
                except Exception as exc:
                    logger.error("Failed to parse MSK record: %s", exc)
    else:
        # Direct invocation (testing)
        for record in event.get("records", []):
            try:
                enriched = enrich_log_record(record)
                records_to_index.append(enriched)
            except Exception as exc:
                logger.error("Failed to enrich record: %s", exc)

    ok, err = 0, 0
    if records_to_index:
        ok, err = index_batch(os_client, records_to_index)
        metrics.emit("RecordsProcessed", ok, "Count", Service="lambda")
        metrics.emit("RecordErrors", err, "Count", Service="lambda")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "processed": ok,
            "errors": err,
            "total": len(records_to_index),
        }),
    }


# ---------------------------------------------------------------------------
# Entry point (local Docker execution)
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    run_consumer()
