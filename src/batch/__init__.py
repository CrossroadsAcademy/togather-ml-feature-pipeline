"""
Batch Feature Pipeline Module.

Provides Spark-based batch feature engineering for the ToGather ML platform.

Components:
- spark_config: Spark session configuration
- minio_reader: Read Hive-partitioned events from MinIO
- feature_aggregations: User, experience, session aggregations
- data_quality: Great Expectations-style validation
- feast_writer: Write to MinIO offline + Redis online stores
- feast_features: Feast entity and feature view definitions
- observability: Prometheus metrics + OpenTelemetry tracing
- batch_feature_job: Main entry point

Usage:
    # Programmatic (requires pyspark)
    from src.batch import run_batch_job
    result = run_batch_job(start_date, end_date, event_types)

    # CLI
    spark-submit src/batch/batch_feature_job.py --target-date 2024-12-15

Note: Requires pyspark to be installed (poetry install --with jvm)
"""

# Observability is always available (no pyspark dependency)
from src.batch.observability import (
    JobStageContext,
    get_tracer,
    observe_duration,
    record_metric,
    set_job_status,
)

# PySpark-dependent modules - import only when available
try:
    from src.batch.batch_feature_job import run_batch_job
    from src.batch.data_quality import DataQualityValidator, ValidationReport
    from src.batch.feast_writer import FeastFeatureWriter, FeastWriterConfig
    from src.batch.feature_aggregations import (
        aggregate_experience_features,
        aggregate_session_features,
        aggregate_user_features,
        compute_all_features,
    )
    from src.batch.minio_reader import MinIOReader
    from src.batch.spark_config import SparkConfig, create_spark_session, get_spark_session

    _PYSPARK_AVAILABLE = True
except ImportError:
    _PYSPARK_AVAILABLE = False
    # Provide stubs for type hints
    SparkConfig = None  # type: ignore
    create_spark_session = None  # type: ignore
    get_spark_session = None  # type: ignore
    MinIOReader = None  # type: ignore
    aggregate_experience_features = None  # type: ignore
    aggregate_session_features = None  # type: ignore
    aggregate_user_features = None  # type: ignore
    compute_all_features = None  # type: ignore
    DataQualityValidator = None  # type: ignore
    ValidationReport = None  # type: ignore
    FeastFeatureWriter = None  # type: ignore
    FeastWriterConfig = None  # type: ignore
    run_batch_job = None  # type: ignore

__all__ = [
    # Main entry
    "run_batch_job",
    # Spark
    "SparkConfig",
    "create_spark_session",
    "get_spark_session",
    # Reader
    "MinIOReader",
    # Aggregations
    "aggregate_user_features",
    "aggregate_experience_features",
    "aggregate_session_features",
    "compute_all_features",
    # Data Quality
    "DataQualityValidator",
    "ValidationReport",
    # Writer
    "FeastFeatureWriter",
    "FeastWriterConfig",
    # Observability
    "JobStageContext",
    "get_tracer",
    "record_metric",
    "observe_duration",
    "set_job_status",
]
