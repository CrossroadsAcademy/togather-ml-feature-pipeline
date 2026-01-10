"""
Data Validation Utilities.

Provides validation functions and quality metrics for feature pipelines.
Follows the "validate early, fail fast" principle.
"""

from dataclasses import dataclass, field
from typing import Any

import structlog
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.batch.observability import record_metric

logger = structlog.get_logger(__name__)


# Data Quality Metrics


@dataclass
class DataQualityMetrics:
    """Tracks data quality metrics during pipeline execution."""

    total_records: int = 0
    valid_records: int = 0
    invalid_records: int = 0
    null_counts: dict[str, int] = field(default_factory=dict)
    validation_failures: dict[str, int] = field(default_factory=dict)

    @property
    def validity_rate(self) -> float:
        """Calculate the percentage of valid records."""
        if self.total_records == 0:
            return 0.0
        return self.valid_records / self.total_records

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for logging/metrics."""
        return {
            "total_records": self.total_records,
            "valid_records": self.valid_records,
            "invalid_records": self.invalid_records,
            "validity_rate": round(self.validity_rate, 4),
            "null_counts": self.null_counts,
            "validation_failures": self.validation_failures,
        }

    def log(self, entity_type: str) -> None:
        """Log quality metrics."""
        logger.info(
            "Data quality metrics",
            entity_type=entity_type,
            **self.to_dict(),
        )

        # Record to Prometheus
        record_metric(
            "data_quality_validity_rate",
            self.validity_rate,
            {"entity_type": entity_type},
        )
        record_metric(
            "data_quality_invalid_records",
            self.invalid_records,
            {"entity_type": entity_type},
        )


# Validation Functions


def validate_not_null(
    df: DataFrame,
    columns: list[str],
    metrics: DataQualityMetrics | None = None,
) -> DataFrame:
    """
    Add validation flag for null values in specified columns.

    Args:
        df: Input DataFrame
        columns: Columns to check for nulls
        metrics: Optional metrics tracker

    Returns:
        DataFrame with `_null_check_passed` column
    """
    null_checks = [F.col(c).isNotNull() for c in columns]
    combined_check = null_checks[0]
    for check in null_checks[1:]:
        combined_check = combined_check & check

    df = df.withColumn("_null_check_passed", combined_check)

    if metrics:
        for col in columns:
            null_count = df.filter(F.col(col).isNull()).count()
            if null_count > 0:
                metrics.null_counts[col] = null_count

    return df


def validate_range(
    df: DataFrame,
    column: str,
    min_value: float | None = None,
    max_value: float | None = None,
    allow_null: bool = True,
) -> DataFrame:
    """
    Add validation flag for values within specified range.

    Args:
        df: Input DataFrame
        column: Column to validate
        min_value: Minimum allowed value (inclusive)
        max_value: Maximum allowed value (inclusive)
        allow_null: Whether null values pass validation

    Returns:
        DataFrame with `_range_check_{column}` column
    """
    col_ref = F.col(column)

    # Build range check
    if min_value is not None and max_value is not None:
        range_check = (col_ref >= min_value) & (col_ref <= max_value)
    elif min_value is not None:
        range_check = col_ref >= min_value
    elif max_value is not None:
        range_check = col_ref <= max_value
    else:
        range_check = F.lit(True)

    # Handle nulls
    if allow_null:
        range_check = F.when(col_ref.isNull(), F.lit(True)).otherwise(range_check)

    df = df.withColumn(f"_range_check_{column}", range_check)

    return df


def validate_string_length(
    df: DataFrame,
    column: str,
    min_length: int = 1,
    max_length: int = 1000,
) -> DataFrame:
    """
    Add validation flag for string length.

    Args:
        df: Input DataFrame
        column: Column to validate
        min_length: Minimum string length
        max_length: Maximum string length

    Returns:
        DataFrame with `_length_check_{column}` column
    """
    col_ref = F.col(column)
    length_check = (F.length(col_ref) >= min_length) & (F.length(col_ref) <= max_length)

    # Null strings fail validation
    length_check = F.when(col_ref.isNull(), F.lit(False)).otherwise(length_check)

    df = df.withColumn(f"_length_check_{column}", length_check)

    return df


def validate_enum(
    df: DataFrame,
    column: str,
    valid_values: list[Any],
    allow_null: bool = True,
) -> DataFrame:
    """
    Add validation flag for enum/categorical values.

    Args:
        df: Input DataFrame
        column: Column to validate
        valid_values: List of allowed values
        allow_null: Whether null values pass validation

    Returns:
        DataFrame with `_enum_check_{column}` column
    """
    col_ref = F.col(column)
    enum_check = col_ref.isin(valid_values)

    if allow_null:
        enum_check = F.when(col_ref.isNull(), F.lit(True)).otherwise(enum_check)

    df = df.withColumn(f"_enum_check_{column}", enum_check)

    return df


# Quality Flags


def add_quality_flags(
    df: DataFrame,
    check_columns: list[str] | None = None,
) -> DataFrame:
    """
    Combine individual validation checks into overall quality flag.

    Args:
        df: DataFrame with validation check columns (_*_check_*)
        check_columns: Specific check columns to combine (default: all)

    Returns:
        DataFrame with `is_valid` column and check columns dropped
    """
    # Find all check columns
    if check_columns is None:
        check_columns = [c for c in df.columns if c.startswith("_") and "_check" in c]

    if not check_columns:
        # No checks to combine
        return df.withColumn("is_valid", F.lit(True))

    # Combine all checks with AND
    combined = F.col(check_columns[0])
    for col in check_columns[1:]:
        combined = combined & F.col(col)

    df = df.withColumn("is_valid", combined)

    # Drop helper columns
    df = df.drop(*check_columns)

    return df


def compute_quality_metrics(
    df: DataFrame,
    entity_type: str,
) -> DataQualityMetrics:
    """
    Compute and log quality metrics for a DataFrame.

    Args:
        df: DataFrame with `is_valid` column
        entity_type: Entity type for logging

    Returns:
        DataQualityMetrics instance
    """
    if "is_valid" not in df.columns:
        raise ValueError("DataFrame must have 'is_valid' column")

    metrics = DataQualityMetrics()
    metrics.total_records = df.count()
    metrics.valid_records = df.filter(F.col("is_valid")).count()
    metrics.invalid_records = metrics.total_records - metrics.valid_records

    metrics.log(entity_type)

    return metrics


# Anti-Pattern Checks


def check_data_skew(
    df: DataFrame,
    key_column: str,
    max_skew_ratio: float = 100.0,
) -> bool:
    """
    Check for data skew that could cause Spark performance issues.

    Args:
        df: Input DataFrame
        key_column: Column to check for skew
        max_skew_ratio: Maximum allowed ratio of max/min count

    Returns:
        True if skew is acceptable, False if skewed
    """
    key_counts = df.groupBy(key_column).count()
    stats = key_counts.agg(
        F.max("count").alias("max_count"),
        F.min("count").alias("min_count"),
        F.avg("count").alias("avg_count"),
    ).collect()[0]

    if stats["min_count"] == 0:
        logger.warning("Data skew detected: some keys have zero records", column=key_column)
        return False

    skew_ratio = stats["max_count"] / stats["min_count"]

    if skew_ratio > max_skew_ratio:
        logger.warning(
            "Data skew detected",
            column=key_column,
            skew_ratio=skew_ratio,
            max_count=stats["max_count"],
            min_count=stats["min_count"],
        )
        return False

    return True


def check_freshness(
    df: DataFrame,
    timestamp_column: str,
    max_staleness_hours: int = 24,
) -> bool:
    """
    Check that data is fresh enough for processing.

    Args:
        df: Input DataFrame
        timestamp_column: Timestamp column to check
        max_staleness_hours: Maximum allowed staleness in hours

    Returns:
        True if data is fresh, False if stale
    """
    from pyspark.sql import functions as F

    max_ts = df.agg(F.max(timestamp_column)).collect()[0][0]

    if max_ts is None:
        logger.warning("No timestamp data found for freshness check")
        return False

    # Convert to Python datetime for comparison
    from datetime import datetime, timedelta

    now = datetime.utcnow()
    staleness = now - max_ts

    if staleness > timedelta(hours=max_staleness_hours):
        logger.warning(
            "Data is stale",
            max_timestamp=str(max_ts),
            staleness_hours=staleness.total_seconds() / 3600,
            threshold_hours=max_staleness_hours,
        )
        return False

    return True
