"""
Spark Session Configuration for Batch Feature Pipeline.

Configures Spark with S3A (MinIO) and Redis connectivity,
plus observability settings (Prometheus metrics, event logging).
"""

import os
from dataclasses import dataclass, field

from pyspark.sql import SparkSession

from src.utils.config import settings
from src.utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class SparkConfig:
    """Configuration for Spark session with MinIO/S3 and Redis."""

    # Application settings
    app_name: str = "batch-feature-pipeline"
    master: str = "local[*]"  # Override in K8s: k8s://https://kubernetes.default.svc

    # MinIO/S3 settings (used for both raw events and offline feature store)
    s3_endpoint: str = field(default_factory=lambda: settings.minio.endpoint_url)
    # Credentials from env vars (injected via K8s secrets)
    s3_access_key: str | None = field(
        default_factory=lambda: (os.getenv("AWS_ACCESS_KEY_ID") or os.getenv("MINIO_ACCESS_KEY"))
    )
    s3_secret_key: str | None = field(
        default_factory=lambda: (
            os.getenv("AWS_SECRET_ACCESS_KEY") or os.getenv("MINIO_SECRET_KEY")
        )
    )
    s3_path_style_access: bool = True

    # Redis settings (for Feast online store)
    redis_host: str = field(default_factory=lambda: settings.redis.host)
    redis_port: int = field(default_factory=lambda: settings.redis.port)
    redis_password: str | None = field(default_factory=lambda: settings.redis.password)

    # Performance settings
    executor_memory: str = "2g"
    executor_cores: int = 2
    driver_memory: str = "1g"
    shuffle_partitions: int = 200

    # Observability
    enable_metrics: bool = True
    enable_event_log: bool = False  # Disabled: hadoop-aws S3Guard issue
    event_log_dir: str = "s3a://spark-events/"


def create_spark_session(config: SparkConfig | None = None) -> SparkSession:
    """
    Create and configure Spark session for batch feature pipeline.

    Args:
        config: Optional SparkConfig, uses defaults if not provided

    Returns:
        Configured SparkSession
    """
    if config is None:
        config = SparkConfig()

    logger.info(
        "Creating Spark session",
        app_name=config.app_name,
        master=config.master,
        s3_endpoint=config.s3_endpoint,
    )

    builder = SparkSession.builder.appName(config.app_name).master(config.master)

    # S3A Endpoint Configuration Logic
    # 1. If MINIO_ENDPOINT_URL env var is set, config.s3_endpoint is correct -> use it.
    # 2. If it is NOT set, config.s3_endpoint defaults to "localhost:9000".
    # 3. If "localhost:9000" and we are in K8s, force internal K8s service DNS.
    # 4. Otherwise use the config value.

    val_from_env = os.getenv("MINIO_ENDPOINT_URL")

    if val_from_env:
        builder = builder.config("spark.hadoop.fs.s3a.endpoint", f"http://{config.s3_endpoint}")
    elif config.s3_endpoint == "localhost:9000" and os.getenv("KUBERNETES_SERVICE_HOST"):
        logger.info(
            "Detected K8s environment with default localhost endpoint. Switching to internal service DNS."
        )
        builder = builder.config(
            "spark.hadoop.fs.s3a.endpoint",
            "http://minio.platform.svc.cluster.local:9000",
        )
    else:
        builder = builder.config("spark.hadoop.fs.s3a.endpoint", f"http://{config.s3_endpoint}")

    # Configure S3A impl and credentials
    builder = builder.config(
        "spark.hadoop.fs.s3a.path.style.access",
        str(config.s3_path_style_access).lower(),
    ).config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    # Set credentials from config if available, otherwise use EnvironmentVariableCredentialsProvider
    if config.s3_access_key and config.s3_secret_key:
        builder = (
            builder.config("spark.hadoop.fs.s3a.access.key", config.s3_access_key)
            .config("spark.hadoop.fs.s3a.secret.key", config.s3_secret_key)
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            )
        )
    else:
        # Use environment variables provider - reads AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY
        builder = builder.config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.EnvironmentVariableCredentialsProvider",
        )

    builder = (
        builder
        # Parquet optimization (mergeSchema=false to avoid schema conflicts across different writes)
        .config("spark.sql.parquet.mergeSchema", "false")
        .config("spark.sql.parquet.filterPushdown", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        # Hive partition discovery
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        # Performance
        .config("spark.executor.memory", config.executor_memory)
        .config("spark.executor.cores", str(config.executor_cores))
        .config("spark.driver.memory", config.driver_memory)
        .config("spark.sql.shuffle.partitions", str(config.shuffle_partitions))
        # Adaptive query execution
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    )

    # Observability: Prometheus metrics
    if config.enable_metrics:
        builder = (
            builder.config("spark.ui.prometheus.enabled", "true")
            .config("spark.metrics.namespace", "batch_feature")
            .config(
                "spark.metrics.conf.*.sink.prometheusServlet.class",
                "org.apache.spark.metrics.sink.PrometheusServlet",
            )
            .config(
                "spark.metrics.conf.*.sink.prometheusServlet.path",
                "/metrics/prometheus",
            )
        )

    # Observability: Event logging for Spark History Server
    if config.enable_event_log:
        builder = (
            builder.config("spark.eventLog.enabled", "true")
            .config("spark.eventLog.dir", config.event_log_dir)
            .config("spark.eventLog.compress", "true")
        )

    spark = builder.getOrCreate()

    # Set log level
    spark.sparkContext.setLogLevel("WARN")

    logger.info(
        "Spark session created",
        version=spark.version,
        app_id=spark.sparkContext.applicationId,
    )

    return spark


def get_spark_session() -> SparkSession:
    """Get or create the active Spark session."""
    return SparkSession.getActiveSession() or create_spark_session()
