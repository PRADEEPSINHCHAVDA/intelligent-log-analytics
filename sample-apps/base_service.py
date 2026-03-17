"""
Base Microservice - Shared log production framework
====================================================
All 8 sample microservices inherit from this base.
Produces realistic, structured JSON logs to Kafka with:
  - Correlation ID threading across services
  - Realistic operation simulation (with configurable error rates)
  - Proper log levels (DEBUG/INFO/WARN/ERROR/FATAL)
  - HTTP request/response patterns
  - Business event patterns
  - Performance metrics (latency, throughput)
"""

import json
import os
import random
import time
import uuid
import logging
import threading
from datetime import datetime, timezone
from dataclasses import dataclass, asdict, field
from typing import Optional
from kafka import KafkaProducer
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Correlation ID context (thread-local for request tracking)
# ---------------------------------------------------------------------------
_local = threading.local()


def get_correlation_id() -> str:
    """Get the current request's correlation ID."""
    return getattr(_local, "correlation_id", str(uuid.uuid4()))


def set_correlation_id(cid: str) -> None:
    """Set correlation ID for current thread (simulates middleware injection)."""
    _local.correlation_id = cid


def new_correlation_id() -> str:
    """Generate and set a new correlation ID."""
    cid = str(uuid.uuid4())
    set_correlation_id(cid)
    return cid


# ---------------------------------------------------------------------------
# Log record schema
# ---------------------------------------------------------------------------
@dataclass
class LogRecord:
    """Structured log record matching OpenSearch mapping."""
    # Core fields
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    level: str = "INFO"
    message: str = ""
    service: str = "unknown-service"
    version: str = "1.0.0"
    environment: str = "production"
    region: str = "us-east-1"

    # Tracing
    correlation_id: str = field(default_factory=get_correlation_id)
    trace_id: str = field(default_factory=lambda: str(uuid.uuid4()).replace("-", ""))
    span_id: str = field(default_factory=lambda: str(uuid.uuid4())[:16].replace("-", ""))

    # User context
    user_id: Optional[str] = None
    session_id: Optional[str] = None

    # HTTP context
    request_id: Optional[str] = None
    http_method: Optional[str] = None
    http_path: Optional[str] = None
    status_code: Optional[int] = None

    # Performance
    duration_ms: Optional[float] = None

    # Business/error context
    error: Optional[str] = None
    stack_trace: Optional[str] = None
    metadata: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        d = asdict(self)
        # Remove None fields to save space
        return {k: v for k, v in d.items() if v is not None}


# ---------------------------------------------------------------------------
# Base Microservice
# ---------------------------------------------------------------------------
class BaseMicroservice:
    """
    Base class for all sample microservices.
    Handles Kafka connection, log emission, and realistic simulation.
    """

    SERVICE_NAME: str = "base-service"
    SERVICE_VERSION: str = "1.0.0"

    # Operations this service performs (subclasses override)
    OPERATIONS: list[dict] = []

    # Shared correlation IDs (simulates cross-service requests)
    _shared_correlation_ids: list[str] = []
    _correlation_lock = threading.Lock()

    def __init__(self):
        self.service_name = os.environ.get("SERVICE_NAME", self.SERVICE_NAME)
        self.kafka_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.log_rate = float(os.environ.get("LOG_RATE", "5"))      # logs/second
        self.error_rate = float(os.environ.get("ERROR_RATE", "0.05"))
        self.topic = f"logs.{self.service_name}"
        self.environment = os.environ.get("ENVIRONMENT", "production")
        self.region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

        self.producer = self._create_producer()
        logging.basicConfig(level=logging.INFO)
        logger.info("[%s] Initialized. Rate: %s logs/s, Error rate: %s%%",
                    self.service_name, self.log_rate, self.error_rate * 100)

    def _create_producer(self) -> KafkaProducer:
        """Create Kafka producer with retry logic."""
        retries = 0
        while retries < 10:
            try:
                producer = KafkaProducer(
                    bootstrap_servers=self.kafka_servers.split(","),
                    value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                    acks="all",
                    retries=3,
                    batch_size=16384,
                    linger_ms=100,       # Wait up to 100ms to batch messages
                    compression_type="gzip",
                    max_request_size=5242880,
                )
                logger.info("[%s] Kafka producer connected to %s",
                            self.service_name, self.kafka_servers)
                return producer
            except KafkaError as e:
                retries += 1
                logger.warning("[%s] Kafka connection failed (%d/10): %s",
                               self.service_name, retries, e)
                time.sleep(5)
        raise RuntimeError(f"Cannot connect to Kafka after 10 retries: {self.kafka_servers}")

    def emit_log(self, record: LogRecord) -> None:
        """Send a log record to Kafka."""
        try:
            self.producer.send(self.topic, value=record.to_dict())
        except KafkaError as exc:
            logger.error("[%s] Failed to send log: %s", self.service_name, exc)

    def log_info(self, message: str, **kwargs) -> LogRecord:
        record = LogRecord(
            level="INFO", message=message,
            service=self.service_name, version=self.SERVICE_VERSION,
            environment=self.environment, region=self.region, **kwargs
        )
        self.emit_log(record)
        return record

    def log_debug(self, message: str, **kwargs) -> LogRecord:
        record = LogRecord(
            level="DEBUG", message=message,
            service=self.service_name, version=self.SERVICE_VERSION,
            environment=self.environment, region=self.region, **kwargs
        )
        self.emit_log(record)
        return record

    def log_warn(self, message: str, **kwargs) -> LogRecord:
        record = LogRecord(
            level="WARN", message=message,
            service=self.service_name, version=self.SERVICE_VERSION,
            environment=self.environment, region=self.region, **kwargs
        )
        self.emit_log(record)
        return record

    def log_error(self, message: str, error: str = "", **kwargs) -> LogRecord:
        record = LogRecord(
            level="ERROR", message=message, error=error,
            service=self.service_name, version=self.SERVICE_VERSION,
            environment=self.environment, region=self.region, **kwargs
        )
        self.emit_log(record)
        return record

    def log_fatal(self, message: str, error: str = "", **kwargs) -> LogRecord:
        record = LogRecord(
            level="FATAL", message=message, error=error,
            service=self.service_name, version=self.SERVICE_VERSION,
            environment=self.environment, region=self.region, **kwargs
        )
        self.emit_log(record)
        return record

    @classmethod
    def register_correlation_id(cls, cid: str) -> None:
        """Register a correlation ID for cross-service reuse."""
        with cls._correlation_lock:
            cls._shared_correlation_ids.append(cid)
            if len(cls._shared_correlation_ids) > 100:
                cls._shared_correlation_ids.pop(0)

    @classmethod
    def get_shared_correlation_id(cls) -> Optional[str]:
        """Get a random existing correlation ID (simulates receiving a request)."""
        with cls._correlation_lock:
            if cls._shared_correlation_ids:
                return random.choice(cls._shared_correlation_ids)
        return None

    def simulate_request(self) -> None:
        """
        Simulate a single request lifecycle.
        Subclasses override this to implement realistic business logic.
        """
        # Either start a new request or continue an existing one
        if random.random() < 0.3:
            # Receive a cross-service request (use existing correlation ID)
            existing_cid = self.get_shared_correlation_id()
            if existing_cid:
                cid = existing_cid
            else:
                cid = new_correlation_id()
        else:
            cid = new_correlation_id()

        set_correlation_id(cid)
        self.register_correlation_id(cid)
        self._run_operation()

    def _run_operation(self) -> None:
        """Run a random operation from OPERATIONS list."""
        if not self.OPERATIONS:
            self.log_info(f"{self.service_name} processed request")
            return

        op = random.choice(self.OPERATIONS)
        is_error = random.random() < self.error_rate

        start = time.time()
        duration = self._simulate_latency(op.get("avg_ms", 100), is_error)
        status = self._pick_status_code(is_error, op.get("critical", False))

        request_id = str(uuid.uuid4())
        user_id = f"usr_{random.randint(1000, 9999)}" if random.random() < 0.8 else None

        base_kwargs = dict(
            request_id=request_id,
            user_id=user_id,
            http_method=op.get("method", "GET"),
            http_path=op.get("path", "/"),
            status_code=status,
            duration_ms=round(duration, 2),
            metadata=op.get("metadata", {}),
        )

        if is_error:
            err = random.choice(op.get("errors", ["Internal server error"]))
            self.log_error(
                f"{op['name']} failed: {err}",
                error=err,
                **base_kwargs,
            )
            if random.random() < 0.1:  # 10% chance of fatal
                self.log_fatal(
                    f"Critical failure in {op['name']}: service may be degraded",
                    error=f"Cascading failure after: {err}",
                    **base_kwargs,
                )
        else:
            self.log_info(f"{op['name']} completed successfully", **base_kwargs)
            if random.random() < 0.1:  # Debug logs 10% of the time
                self.log_debug(
                    f"{op['name']} debug: cache_hit={random.choice([True,False])}, "
                    f"db_queries={random.randint(1,5)}",
                    **base_kwargs,
                )
            if duration > op.get("avg_ms", 100) * 3:
                self.log_warn(
                    f"{op['name']} slow response: {duration:.0f}ms "
                    f"(threshold: {op.get('avg_ms',100)*3:.0f}ms)",
                    **base_kwargs,
                )

    @staticmethod
    def _simulate_latency(avg_ms: float, is_error: bool) -> float:
        """Simulate realistic latency with log-normal distribution."""
        if is_error:
            return random.uniform(avg_ms * 2, avg_ms * 10)
        # Log-normal gives realistic right-skewed latency
        sigma = 0.5
        return max(1.0, random.lognormvariate(
            __import__("math").log(avg_ms) - sigma**2 / 2, sigma
        ))

    @staticmethod
    def _pick_status_code(is_error: bool, is_critical: bool) -> int:
        if not is_error:
            return random.choice([200, 200, 200, 201, 204])
        if is_critical:
            return random.choice([500, 503, 504])
        return random.choice([400, 401, 403, 404, 422, 429, 500, 502])

    def run(self) -> None:
        """Main service loop - generates logs at configured rate."""
        interval = 1.0 / self.log_rate
        logger.info("[%s] Starting log generation at %.1f logs/s",
                    self.service_name, self.log_rate)

        # Emit startup log
        self.log_info(
            f"{self.service_name} started successfully",
            metadata={
                "version": self.SERVICE_VERSION,
                "kafka_topic": self.topic,
                "log_rate": self.log_rate,
            }
        )

        try:
            while True:
                loop_start = time.time()
                self.simulate_request()
                elapsed = time.time() - loop_start
                sleep_time = max(0, interval - elapsed)
                time.sleep(sleep_time)
        except KeyboardInterrupt:
            logger.info("[%s] Shutting down gracefully...", self.service_name)
            self.log_info(f"{self.service_name} shutting down")
            self.producer.flush(timeout=10)
            self.producer.close()
