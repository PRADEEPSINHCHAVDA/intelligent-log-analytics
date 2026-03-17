#!/usr/bin/env python3
"""
Log Generator - Simulate high-volume log production
====================================================
Use this to:
  1. Prove 2GB+ daily logs (burst mode)
  2. Create realistic load patterns for demos
  3. Test pipeline throughput
  4. Generate metrics for your resume claims

Usage:
  python3 generate-logs.py                    # Default: all services, normal rate
  python3 generate-logs.py --burst            # High volume to hit 2GB/day target
  python3 generate-logs.py --service payment  # Single service only
  python3 generate-logs.py --duration 60      # Run for 60 seconds then report
  python3 generate-logs.py --report           # Show current stats and exit

HOW TO PROVE 2GB/DAY:
  Run with --burst for 5 minutes, then extrapolate:
  $ python3 generate-logs.py --burst --duration 300
  > "Produced 12.5 MB in 300s → 3.6 GB/day throughput"
"""

import argparse
import json
import os
import random
import signal
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from kafka import KafkaProducer
from kafka.errors import KafkaError


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

SERVICES = {
    "user-service":         {"rate": 10, "error_rate": 0.05, "avg_msg_bytes": 450},
    "payment-service":      {"rate": 5,  "error_rate": 0.02, "avg_msg_bytes": 600},
    "order-service":        {"rate": 8,  "error_rate": 0.04, "avg_msg_bytes": 500},
    "notification-service": {"rate": 15, "error_rate": 0.08, "avg_msg_bytes": 350},
    "inventory-service":    {"rate": 6,  "error_rate": 0.03, "avg_msg_bytes": 400},
    "auth-service":         {"rate": 12, "error_rate": 0.06, "avg_msg_bytes": 380},
    "api-gateway-service":  {"rate": 20, "error_rate": 0.03, "avg_msg_bytes": 550},
    "reporting-service":    {"rate": 3,  "error_rate": 0.04, "avg_msg_bytes": 800},
}

BURST_MULTIPLIER = 10   # 10x rate in burst mode


# ---------------------------------------------------------------------------
# Stats tracking
# ---------------------------------------------------------------------------
@dataclass
class Stats:
    messages_sent: int = 0
    bytes_sent: int = 0
    errors: int = 0
    start_time: float = field(default_factory=time.time)
    lock: threading.Lock = field(default_factory=threading.Lock)

    def add(self, msg_bytes: int, is_error: bool = False):
        with self.lock:
            self.messages_sent += 1
            self.bytes_sent += msg_bytes
            if is_error:
                self.errors += 1

    def report(self) -> dict:
        elapsed = time.time() - self.start_time
        mb_sent = self.bytes_sent / 1024 / 1024
        msgs_per_sec = self.messages_sent / elapsed if elapsed > 0 else 0
        mb_per_sec = mb_sent / elapsed if elapsed > 0 else 0
        # Extrapolate to daily
        daily_gb = mb_per_sec * 3600 * 24 / 1024
        return {
            "elapsed_seconds":    round(elapsed, 1),
            "messages_sent":      self.messages_sent,
            "bytes_sent_mb":      round(mb_sent, 2),
            "errors":             self.errors,
            "messages_per_sec":   round(msgs_per_sec, 1),
            "throughput_mbps":    round(mb_per_sec, 3),
            "projected_daily_gb": round(daily_gb, 2),
        }


stats = Stats()


# ---------------------------------------------------------------------------
# Log record generation
# ---------------------------------------------------------------------------
SAMPLE_MESSAGES = {
    "INFO": [
        "Request processed successfully",
        "Database query executed in {ms}ms",
        "Cache hit for key {key}",
        "User authenticated via JWT",
        "Order status updated to {status}",
        "Payment authorized: ${amount}",
        "Email notification sent to {email}",
        "Inventory updated: SKU {sku} = {qty} units",
        "API rate limit check passed for user {uid}",
        "Report generation completed in {ms}ms",
    ],
    "WARN": [
        "Slow database query: {ms}ms (threshold: 500ms)",
        "Cache miss rate above 30% for past 5 minutes",
        "Retry attempt {n}/3 for external API call",
        "JWT token expiring in {mins} minutes",
        "High memory usage: {pct}% of limit",
        "Connection pool at {pct}% capacity",
        "Rate limit approaching: {remaining} requests remaining",
    ],
    "ERROR": [
        "Database connection timeout after 30s",
        "Payment gateway returned error: {code}",
        "Failed to send notification: {reason}",
        "Order validation failed: {reason}",
        "Authentication failed: invalid token",
        "External API returned 503: {service}",
        "Failed to acquire distributed lock: {resource}",
    ],
}

HTTP_PATHS = [
    "/api/v1/users/{id}", "/api/v1/orders/{id}", "/api/v1/payments/charge",
    "/api/v1/auth/login", "/api/v1/auth/refresh", "/api/v1/products/{id}",
    "/api/v1/cart/{id}/checkout", "/api/v1/notifications/send",
    "/health", "/metrics",
]

HTTP_METHODS = ["GET", "POST", "PUT", "PATCH", "DELETE"]
STATUS_CODES_OK = [200, 200, 200, 201, 204]
STATUS_CODES_ERR = [400, 401, 403, 404, 422, 429, 500, 502, 503]


def _rand_fill(template: str) -> str:
    return (template
            .replace("{ms}", str(random.randint(5, 2000)))
            .replace("{key}", f"user:{random.randint(1,9999)}")
            .replace("{status}", random.choice(["pending", "confirmed", "shipped"]))
            .replace("{amount}", f"{random.uniform(5, 500):.2f}")
            .replace("{email}", f"user{random.randint(1,999)}@example.com")
            .replace("{sku}", f"SKU-{random.randint(1000,9999)}")
            .replace("{qty}", str(random.randint(0, 100)))
            .replace("{uid}", f"usr_{random.randint(1000,9999)}")
            .replace("{code}", random.choice(["card_declined", "insufficient_funds", "expired"]))
            .replace("{reason}", random.choice(["validation_failed", "timeout", "not_found"]))
            .replace("{service}", random.choice(["stripe", "sendgrid", "twilio", "fedex"]))
            .replace("{resource}", f"lock:{random.randint(1,99)}")
            .replace("{pct}", str(random.randint(50, 95)))
            .replace("{n}", str(random.randint(1, 3)))
            .replace("{mins}", str(random.randint(1, 30)))
            .replace("{remaining}", str(random.randint(5, 100)))
            .replace("{id}", str(random.randint(1000, 9999)))
            )


def make_log_record(service: str, is_error: bool) -> dict:
    """Generate a realistic log record."""
    cid = str(uuid.uuid4())
    level = "ERROR" if is_error else random.choices(
        ["DEBUG", "INFO", "INFO", "INFO", "WARN"],
        weights=[5, 60, 60, 60, 15]
    )[0]

    msg_template = random.choice(SAMPLE_MESSAGES.get(level, SAMPLE_MESSAGES["INFO"]))
    message = _rand_fill(msg_template)

    record: dict = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "level": level,
        "message": message,
        "service": service,
        "version": f"1.{random.randint(0,9)}.{random.randint(0,20)}",
        "environment": "production",
        "region": "us-east-1",
        "correlation_id": cid,
        "trace_id": uuid.uuid4().hex,
        "span_id": uuid.uuid4().hex[:16],
    }

    # HTTP context (80% of logs)
    if random.random() < 0.8:
        sc = random.choice(STATUS_CODES_ERR if is_error else STATUS_CODES_OK)
        record.update({
            "request_id": str(uuid.uuid4()),
            "http_method": random.choice(HTTP_METHODS),
            "http_path": _rand_fill(random.choice(HTTP_PATHS)),
            "status_code": sc,
            "duration_ms": round(random.lognormvariate(4.5, 0.8), 2),
            "user_id": f"usr_{random.randint(1000, 9999)}",
        })

    # Error details
    if is_error:
        record["error"] = message
        record["error_category"] = random.choice([
            "database", "network", "authentication", "validation",
            "payment", "rate_limit", "not_found", "server",
        ])

    return record


# ---------------------------------------------------------------------------
# Producer thread per service
# ---------------------------------------------------------------------------
def produce_for_service(
    service: str,
    config: dict,
    producer: KafkaProducer,
    burst: bool,
    stop_event: threading.Event,
):
    topic = f"logs.{service}"
    rate = config["rate"] * (BURST_MULTIPLIER if burst else 1)
    error_rate = config["error_rate"]
    interval = 1.0 / rate

    while not stop_event.is_set():
        t0 = time.time()
        is_error = random.random() < error_rate
        record = make_log_record(service, is_error)
        payload = json.dumps(record, default=str).encode("utf-8")

        try:
            producer.send(topic, value=payload)
            stats.add(len(payload), is_error)
        except KafkaError as exc:
            stats.add(0, True)

        elapsed = time.time() - t0
        sleep = max(0.0, interval - elapsed)
        if sleep > 0:
            time.sleep(sleep)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Log Analytics - Log Generator")
    parser.add_argument("--burst", action="store_true",
                        help=f"Burst mode: {BURST_MULTIPLIER}x normal rate")
    parser.add_argument("--service", help="Only generate for this service")
    parser.add_argument("--duration", type=int, default=0,
                        help="Stop after N seconds (0 = run forever)")
    parser.add_argument("--report", action="store_true",
                        help="Query and display current stats then exit")
    parser.add_argument("--kafka", default=KAFKA_BOOTSTRAP,
                        help=f"Kafka bootstrap servers (default: {KAFKA_BOOTSTRAP})")
    args = parser.parse_args()

    if args.report:
        # Just show current OpenSearch stats
        import subprocess, urllib.request
        try:
            resp = urllib.request.urlopen("http://localhost:9200/logs-*/_count", timeout=5)
            data = json.loads(resp.read())
            print(f"\nOpenSearch records indexed: {data.get('count', 0):,}")
        except Exception as exc:
            print(f"Cannot reach OpenSearch: {exc}")
        return

    services = SERVICES.copy()
    if args.service:
        if args.service not in services:
            print(f"Unknown service. Available: {', '.join(services.keys())}")
            sys.exit(1)
        services = {args.service: services[args.service]}

    print("\n" + "═" * 60)
    print(f"  Log Analytics Platform - Log Generator")
    print(f"  Mode: {'BURST' if args.burst else 'NORMAL'}")
    print(f"  Services: {len(services)}")
    if args.duration:
        print(f"  Duration: {args.duration}s")
    print("═" * 60 + "\n")

    # Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=args.kafka.split(","),
        value_serializer=lambda v: v if isinstance(v, bytes) else json.dumps(v).encode(),
        acks=1,           # Don't wait for all replicas (faster for load testing)
        batch_size=65536,
        linger_ms=50,
        compression_type="gzip",
    )

    stop_event = threading.Event()
    threads = []

    # Start one thread per service
    for svc, cfg in services.items():
        t = threading.Thread(
            target=produce_for_service,
            args=(svc, cfg, producer, args.burst, stop_event),
            daemon=True,
            name=svc,
        )
        t.start()
        threads.append(t)

    # Handle Ctrl+C
    def shutdown(sig, frame):
        print("\n\nShutting down...")
        stop_event.set()
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # Stats printing loop
    report_interval = 10
    last_report = time.time()
    start = time.time()

    while not stop_event.is_set():
        time.sleep(1)

        if time.time() - last_report >= report_interval:
            r = stats.report()
            print(f"[{r['elapsed_seconds']:6.0f}s] "
                  f"Messages: {r['messages_sent']:>8,} | "
                  f"MB sent: {r['bytes_sent_mb']:>8.1f} | "
                  f"Rate: {r['messages_per_sec']:>6.0f} msg/s | "
                  f"Throughput: {r['throughput_mbps']:.2f} MB/s | "
                  f"→ {r['projected_daily_gb']:.1f} GB/day")
            last_report = time.time()

        if args.duration and (time.time() - start) >= args.duration:
            stop_event.set()

    producer.flush(timeout=15)
    producer.close()

    # Final report
    r = stats.report()
    print("\n" + "═" * 60)
    print("  FINAL REPORT")
    print("═" * 60)
    print(f"  Duration:           {r['elapsed_seconds']}s")
    print(f"  Messages sent:      {r['messages_sent']:,}")
    print(f"  Data produced:      {r['bytes_sent_mb']:.2f} MB")
    print(f"  Avg throughput:     {r['throughput_mbps']:.3f} MB/s")
    print(f"  Projected daily:    {r['projected_daily_gb']:.2f} GB/day")
    print(f"  Error records:      {r['errors']:,} ({r['errors']/max(r['messages_sent'],1)*100:.1f}%)")
    print("═" * 60)
    print("\n✓ Use --burst mode to demonstrate 2GB+ daily throughput")
    print("✓ Screenshot this output for your resume/GitHub README")


if __name__ == "__main__":
    main()
