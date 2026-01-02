"""
Observability utilities for Batch Feature Pipeline.

Provides:
- Prometheus metrics (via prometheus_client)
- OpenTelemetry tracing (via OTLP exporter)
- Structured logging helpers
"""

import os
from functools import lru_cache
from typing import Any

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from prometheus_client import Counter, Gauge, Histogram

from src.utils.logger import get_logger

logger = get_logger(__name__)

# =============================================================================
# Prometheus Metrics
# =============================================================================

# Processing metrics
BATCH_RECORDS_READ = Counter(
    "batch_records_read_total",
    "Total records read from source",
    ["source"],
)

BATCH_RECORDS_PROCESSED = Counter(
    "batch_records_processed_total",
    "Total records processed",
    ["stage", "entity_type"],
)

BATCH_FEATURES_WRITTEN = Counter(
    "batch_features_written_total",
    "Total features written to store",
    ["entity_type", "store"],
)

# Job metrics
BATCH_JOB_DURATION = Histogram(
    "batch_job_duration_seconds",
    "Batch job duration in seconds",
    ["stage"],
    buckets=[10, 30, 60, 120, 300, 600, 1200, 1800, 3600],
)

BATCH_JOB_STATUS = Gauge(
    "batch_job_status",
    "Current batch job status (1=running, 0=idle, -1=failed)",
    ["job_name"],
)

# Data quality metrics
BATCH_VALIDATION_FAILURES = Counter(
    "batch_validation_failures_total",
    "Total validation failures",
    ["validation_type", "entity_type"],
)

BATCH_DLQ_RECORDS = Counter(
    "batch_dlq_records_total",
    "Total records sent to DLQ",
    ["reason"],
)


def record_metric(
    metric_name: str,
    value: float,
    labels: dict[str, str] | None = None,
) -> None:
    """
    Record a Prometheus metric by name.

    Args:
        metric_name: Name of the metric (without prefix)
        value: Metric value
        labels: Optional label dict
    """
    labels = labels or {}

    metric_map = {
        "batch_records_read_total": BATCH_RECORDS_READ,
        "batch_records_processed_total": BATCH_RECORDS_PROCESSED,
        "batch_features_written_total": BATCH_FEATURES_WRITTEN,
        "batch_validation_failures_total": BATCH_VALIDATION_FAILURES,
        "batch_dlq_records_total": BATCH_DLQ_RECORDS,
    }

    if metric_name in metric_map:
        metric = metric_map[metric_name]
        if labels:
            metric.labels(**labels).inc(value)
        else:
            metric.inc(value)


def observe_duration(stage: str, duration_seconds: float) -> None:
    """Record job stage duration."""
    BATCH_JOB_DURATION.labels(stage=stage).observe(duration_seconds)


def set_job_status(job_name: str, status: int) -> None:
    """Set job status gauge (1=running, 0=idle, -1=failed)."""
    BATCH_JOB_STATUS.labels(job_name=job_name).set(status)


# =============================================================================
# OpenTelemetry Tracing
# =============================================================================


@lru_cache(maxsize=1)
def _setup_tracing() -> TracerProvider:
    """
    Initialize OpenTelemetry tracing with OTLP exporter.

    Sends traces to Tempo via Alloy.
    """
    # Get OTLP endpoint from environment
    otlp_endpoint = os.getenv(
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "http://alloy.observability.svc.cluster.local:4317",
    )

    # Create resource
    resource = Resource.create(
        {
            "service.name": os.getenv("OTEL_SERVICE_NAME", "batch-feature-pipeline"),
            "service.version": "1.0.0",
            "deployment.environment": os.getenv("APP_ENV", "development"),
        }
    )

    # Create provider
    provider = TracerProvider(resource=resource)

    # Add OTLP exporter
    try:
        otlp_exporter = OTLPSpanExporter(endpoint=otlp_endpoint, insecure=True)
        provider.add_span_processor(BatchSpanProcessor(otlp_exporter))
        logger.info("OpenTelemetry tracing initialized", endpoint=otlp_endpoint)
    except Exception as e:
        logger.warning(f"Failed to initialize OTLP exporter: {e}")

    # Set global provider
    trace.set_tracer_provider(provider)

    return provider


def get_tracer(name: str = "batch-feature-pipeline") -> trace.Tracer:
    """
    Get OpenTelemetry tracer instance.

    Args:
        name: Tracer name (usually module name)

    Returns:
        Tracer instance
    """
    _setup_tracing()
    return trace.get_tracer(name)


# =============================================================================
# Context Manager for Job Stages
# =============================================================================


class JobStageContext:
    """
    Context manager for tracking job stage metrics and traces.

    Usage:
        with JobStageContext("aggregate_users") as ctx:
            # Do work
            ctx.set_attribute("user_count", 1000)
    """

    def __init__(self, stage_name: str, job_name: str = "batch_feature_job"):
        self.stage_name = stage_name
        self.job_name = job_name
        # Disable tracing if DISABLE_OTEL env var is set
        self.tracing_enabled = not os.getenv("DISABLE_OTEL", "true").lower() == "true"
        self.tracer = get_tracer() if self.tracing_enabled else None
        self.span: Any = None
        self.start_time: float = 0

    def __enter__(self) -> "JobStageContext":
        import time

        self.start_time = time.time()

        if self.tracer:
            self.span = self.tracer.start_span(self.stage_name)
            self.span.__enter__()

        set_job_status(self.job_name, 1)
        logger.info(f"Starting stage: {self.stage_name}")

        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        import time

        duration = time.time() - self.start_time
        observe_duration(self.stage_name, duration)

        if exc_type:
            if self.span:
                self.span.set_attribute("error", True)
                self.span.set_attribute("error.message", str(exc_val))
            set_job_status(self.job_name, -1)
            logger.error(
                f"Stage failed: {self.stage_name}",
                error=str(exc_val),
                duration_s=round(duration, 2),
            )
        else:
            set_job_status(self.job_name, 0)
            logger.info(
                f"Stage completed: {self.stage_name}",
                duration_s=round(duration, 2),
            )

        if self.span:
            self.span.__exit__(exc_type, exc_val, exc_tb)

    def set_attribute(self, key: str, value: Any) -> None:
        """Set span attribute."""
        if self.span:
            self.span.set_attribute(key, value)
