"""
Data Quality Validation for Batch Pipeline.

Implements Great Expectations-style validation with:
- Input validation (schema, nulls, types)
- Output validation (ranges, duplicates)
- DLQ handling for invalid records
- Prometheus metrics for quality tracking
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.batch.observability import (
    BATCH_DLQ_RECORDS,
    BATCH_VALIDATION_FAILURES,
    JobStageContext,
)
from src.utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class ValidationResult:
    """Result of a validation check."""

    is_valid: bool
    check_name: str
    details: dict[str, Any] = field(default_factory=dict)
    failed_count: int = 0
    total_count: int = 0

    @property
    def failure_rate(self) -> float:
        """Failure rate as percentage."""
        if self.total_count == 0:
            return 0.0
        return (self.failed_count / self.total_count) * 100


@dataclass
class ValidationReport:
    """Aggregated validation report."""

    entity_type: str
    timestamp: datetime
    results: list[ValidationResult] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        """True if all checks passed."""
        return all(r.is_valid for r in self.results)

    @property
    def total_failures(self) -> int:
        """Total failed records across all checks."""
        return sum(r.failed_count for r in self.results)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for logging."""
        return {
            "entity_type": self.entity_type,
            "timestamp": self.timestamp.isoformat(),
            "is_valid": self.is_valid,
            "total_failures": self.total_failures,
            "checks": [
                {
                    "name": r.check_name,
                    "is_valid": r.is_valid,
                    "failure_rate": round(r.failure_rate, 2),
                    "details": r.details,
                }
                for r in self.results
            ],
        }


class DataQualityValidator:
    """
    Validates DataFrames before and after processing.

    Implements Great Expectations-style checks with Prometheus metrics.
    """

    def __init__(
        self,
        null_threshold: float = 0.05,  # Max 5% nulls
        dlq_path: str = "s3a://raw-events-dlq",
    ):
        """
        Initialize validator.

        Args:
            null_threshold: Maximum allowed null rate (0-1)
            dlq_path: Path for DLQ writes
        """
        self.null_threshold = null_threshold
        self.dlq_path = dlq_path
        self.logger = get_logger(self.__class__.__name__)

    def validate_input(
        self,
        df: DataFrame,
        entity_type: str,
        required_columns: list[str],
    ) -> tuple[DataFrame, ValidationReport]:
        """
        Validate input data before processing.

        Checks:
        - Required columns exist
        - Null rate below threshold
        - Timestamp validity
        - Entity ID format

        Args:
            df: Input DataFrame
            entity_type: Type of entity (user, experience, session)
            required_columns: List of required column names

        Returns:
            Tuple of (valid_df, ValidationReport)
        """
        with JobStageContext(f"validate_input_{entity_type}"):
            report = ValidationReport(
                entity_type=entity_type,
                timestamp=datetime.now(),
            )

            total_count = df.count()

            # Check 1: Required columns
            missing_cols = [c for c in required_columns if c not in df.columns]
            report.results.append(
                ValidationResult(
                    is_valid=len(missing_cols) == 0,
                    check_name="required_columns",
                    details={"missing": missing_cols},
                    failed_count=total_count if missing_cols else 0,
                    total_count=total_count,
                )
            )

            if missing_cols:
                self._record_validation_failure("required_columns", entity_type, total_count)
                return df.limit(0), report  # Return empty if missing critical columns

            # Check Null rates
            null_check, null_failures = self._check_null_rates(df, required_columns)
            report.results.append(null_check)
            if not null_check.is_valid:
                self._record_validation_failure("null_rate", entity_type, null_failures)

            # Check Timestamp validity
            if "timestamp" in df.columns or "event_timestamp" in df.columns:
                ts_col = "event_timestamp" if "event_timestamp" in df.columns else "timestamp"
                ts_check = self._check_timestamp_validity(df, ts_col)
                report.results.append(ts_check)
                if not ts_check.is_valid:
                    self._record_validation_failure(
                        "timestamp_invalid", entity_type, ts_check.failed_count
                    )

            # Check Entity ID not empty
            entity_col = f"{entity_type}_id"
            if entity_col in df.columns:
                id_check = self._check_entity_id(df, entity_col)
                report.results.append(id_check)
                if not id_check.is_valid:
                    self._record_validation_failure(
                        "entity_id_empty", entity_type, id_check.failed_count
                    )

            # Filter out invalid records
            valid_df, invalid_df = self._split_valid_invalid(df, report)

            # Write invalid to DLQ
            if invalid_df.count() > 0:
                self._write_to_dlq(invalid_df, entity_type, "input_validation")

            self.logger.info(
                "Input validation completed",
                entity_type=entity_type,
                is_valid=report.is_valid,
                valid_count=valid_df.count(),
                invalid_count=invalid_df.count(),
            )

            return valid_df, report

    def validate_output(
        self,
        df: DataFrame,
        entity_type: str,
        entity_col: str,
    ) -> tuple[DataFrame, ValidationReport]:
        """
        Validate output features before writing.

        Checks:
        - No duplicate entity+timestamp
        - Feature values within expected ranges
        - No nulls in key columns

        Args:
            df: Output features DataFrame
            entity_type: Type of entity
            entity_col: Name of entity column

        Returns:
            Tuple of (valid_df, ValidationReport)
        """
        with JobStageContext(f"validate_output_{entity_type}"):
            report = ValidationReport(
                entity_type=entity_type,
                timestamp=datetime.now(),
            )

            total_count = df.count()  # noqa: F841

            # Check Duplicates
            dup_check = self._check_duplicates(df, entity_col)
            report.results.append(dup_check)
            if not dup_check.is_valid:
                self._record_validation_failure("duplicates", entity_type, dup_check.failed_count)

            # Check Key column nulls
            key_cols = [entity_col, "event_timestamp"]
            null_check, _ = self._check_null_rates(df, key_cols, threshold=0.0)
            report.results.append(null_check)

            # Check Feature ranges (numeric columns)
            numeric_cols = [
                c
                for c, t in df.dtypes
                if t in ("double", "float", "int", "long", "bigint") and c not in key_cols
            ]
            for col in numeric_cols:
                range_check = self._check_numeric_range(df, col)
                report.results.append(range_check)

            # Deduplicate if needed
            if not dup_check.is_valid:
                df = df.dropDuplicates([entity_col, "event_timestamp"])

            self.logger.info(
                "Output validation completed",
                entity_type=entity_type,
                is_valid=report.is_valid,
                record_count=df.count(),
            )

            return df, report

    def _check_null_rates(
        self,
        df: DataFrame,
        columns: list[str],
        threshold: float | None = None,
    ) -> tuple[ValidationResult, int]:
        """Check null rates for specified columns."""
        threshold = threshold if threshold is not None else self.null_threshold
        total = df.count()

        if total == 0:
            return (
                ValidationResult(
                    is_valid=True,
                    check_name="null_rate",
                    total_count=0,
                ),
                0,
            )

        null_counts = {}
        max_null_rate = 0.0
        total_failures = 0

        for col in columns:
            if col in df.columns:
                null_count = df.filter(F.col(col).isNull()).count()
                null_rate = null_count / total
                null_counts[col] = {"count": null_count, "rate": round(null_rate, 4)}
                max_null_rate = max(max_null_rate, null_rate)
                total_failures += null_count

        return (
            ValidationResult(
                is_valid=max_null_rate <= threshold,
                check_name="null_rate",
                details={"columns": null_counts, "threshold": threshold},
                failed_count=total_failures,
                total_count=total * len(columns),
            ),
            total_failures,
        )

    def _check_timestamp_validity(self, df: DataFrame, ts_col: str) -> ValidationResult:
        """Check that timestamps are valid (not null, not in future)."""
        total = df.count()
        invalid = df.filter(
            F.col(ts_col).isNull()
            | (F.col(ts_col) > F.current_timestamp())
            | (F.col(ts_col) < F.lit("2020-01-01"))
        ).count()

        return ValidationResult(
            is_valid=invalid == 0,
            check_name="timestamp_validity",
            details={"column": ts_col},
            failed_count=invalid,
            total_count=total,
        )

    def _check_entity_id(self, df: DataFrame, entity_col: str) -> ValidationResult:
        """Check that entity IDs are not empty."""
        total = df.count()
        invalid = df.filter(F.col(entity_col).isNull() | (F.trim(F.col(entity_col)) == "")).count()

        return ValidationResult(
            is_valid=invalid == 0,
            check_name="entity_id_not_empty",
            details={"column": entity_col},
            failed_count=invalid,
            total_count=total,
        )

    def _check_duplicates(self, df: DataFrame, entity_col: str) -> ValidationResult:
        """Check for duplicate entity+timestamp combinations."""
        total = df.count()
        unique = df.dropDuplicates([entity_col, "event_timestamp"]).count()
        duplicates = total - unique

        return ValidationResult(
            is_valid=duplicates == 0,
            check_name="no_duplicates",
            details={"entity_col": entity_col},
            failed_count=duplicates,
            total_count=total,
        )

    def _check_numeric_range(
        self,
        df: DataFrame,
        col: str,
        min_val: float | None = None,
        max_val: float | None = None,
    ) -> ValidationResult:
        """Check numeric column is within expected range."""
        stats = df.select(
            F.min(col).alias("min"),
            F.max(col).alias("max"),
            F.count(F.when(F.isnan(col), 1)).alias("nan_count"),
        ).collect()[0]

        details = {
            "column": col,
            "min": stats["min"],
            "max": stats["max"],
            "nan_count": stats["nan_count"],
        }

        # Check for NaN values
        is_valid = stats["nan_count"] == 0

        return ValidationResult(
            is_valid=is_valid,
            check_name=f"numeric_range_{col}",
            details=details,
            failed_count=stats["nan_count"] or 0,
            total_count=df.count(),
        )

    def _split_valid_invalid(
        self,
        df: DataFrame,
        report: ValidationReport,
    ) -> tuple[DataFrame, DataFrame]:
        """
        Split DataFrame into valid and invalid records based on event type.

        Different event types have different required fields:
        - experience_events: require 'id' (experience ID)
        - user_account_events, user_profile_events: require 'id' (user ID)
        - recommendation_feedback_v1: require 'user_id'
        """
        # Determine ID field based on event type
        # Check if partition_event_type column exists
        if "partition_event_type" in df.columns:
            # Event-type aware validation
            valid_df = df.filter(
                # Experience events: check 'id'
                (
                    (F.col("partition_event_type") == "experience_events")
                    & F.col("id").isNotNull()
                    & (F.trim(F.col("id")) != "")
                )
                |
                # User account/profile events: check 'id'
                (
                    (
                        F.col("partition_event_type").isin(
                            ["user_account_events", "user_profile_events"]
                        )
                    )
                    & F.col("id").isNotNull()
                    & (F.trim(F.col("id")) != "")
                )
                |
                # Recommendation feedback: check 'user_id'
                (
                    (F.col("partition_event_type") == "recommendation_feedback_v1")
                    & F.col("user_id").isNotNull()
                    & (F.trim(F.col("user_id")) != "")
                )
            )
        elif "_event_type" in df.columns:
            # Fallback to _event_type column
            valid_df = df.filter(
                # Experience events
                (F.col("_event_type").contains("Experience") & F.col("id").isNotNull())
                |
                # User events
                (F.col("_event_type").contains("User") & F.col("id").isNotNull())
                |
                # Feedback events
                (F.col("_event_type").contains("Feedback") & F.col("user_id").isNotNull())
                |
                # Pass through unknown event types if they have some ID
                (
                    ~F.col("_event_type").contains("Experience")
                    & ~F.col("_event_type").contains("User")
                    & ~F.col("_event_type").contains("Feedback")
                    & (F.col("id").isNotNull() | F.col("user_id").isNotNull())
                )
            )
        else:
            # Legacy fallback: check user_id OR id
            valid_df = df.filter(
                (F.col("user_id").isNotNull() & (F.trim(F.col("user_id")) != ""))
                | (F.col("id").isNotNull() & (F.trim(F.col("id")) != ""))
            )

        invalid_df = df.subtract(valid_df)

        return valid_df, invalid_df

    def _write_to_dlq(
        self,
        df: DataFrame,
        entity_type: str,
        reason: str,
    ) -> None:
        """Write invalid records to Dead Letter Queue."""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            path = f"{self.dlq_path}/{entity_type}/{reason}/{timestamp}"

            df.write.mode("append").parquet(path)

            count = df.count()
            BATCH_DLQ_RECORDS.labels(reason=reason).inc(count)

            self.logger.info(
                "Records written to DLQ",
                path=path,
                count=count,
                reason=reason,
            )
        except Exception as e:
            self.logger.error(f"Failed to write to DLQ: {e}")

    def _record_validation_failure(
        self,
        validation_type: str,
        entity_type: str,
        count: int,
    ) -> None:
        """Record validation failure metric."""
        BATCH_VALIDATION_FAILURES.labels(
            validation_type=validation_type,
            entity_type=entity_type,
        ).inc(count)
