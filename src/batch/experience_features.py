"""
Experience Features Extractor.

Extracts features from ExperienceCreated events for the Two-Tower and Ranking models.

Features extracted:
    - exp_category_id: Category ID
    - exp_category_name: Category name
    - exp_tag_ids: List of tag IDs
    - exp_tag_count: Number of tags
    - exp_location_lat: Venue latitude
    - exp_location_lng: Venue longitude
    - exp_city: Venue city
    - exp_country: Venue country
    - exp_price_amount: Price
    - exp_price_currency: Currency code
    - exp_days_until_start: Days until event starts
    - exp_duration_hours: Event duration
    - exp_capacity: Total capacity (bucket_size * total_buckets)
    - exp_creator_type: Individual or Partner
"""

from dataclasses import dataclass
from datetime import date
from typing import TYPE_CHECKING

import structlog
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from src.batch.observability import JobStageContext, record_metric
from src.batch.validation import (
    add_quality_flags,
    compute_quality_metrics,
    validate_range,
)

if TYPE_CHECKING:
    pass

logger = structlog.get_logger(__name__)


# Configuration


@dataclass
class ExperienceFeaturesConfig:
    """Configuration for experience feature extraction."""

    # Input/Output paths
    experience_catalog_bucket: str = "experience-catalog"
    output_bucket: str = "feast-offline-store"
    output_table: str = "experience_features"

    # Feature engineering params
    max_tags: int = 10  # Cap tags to prevent explosion
    default_price: float = 0.0
    default_capacity: int = 0

    # Validation thresholds
    min_lat: float = -90.0
    max_lat: float = 90.0
    min_lng: float = -180.0
    max_lng: float = 180.0
    min_price: float = 0.0
    max_price: float = 1000000.0  # 1M cap for price


# Feature Extraction


def extract_experience_features(
    experiences_df: DataFrame,
    target_date: date,
    config: ExperienceFeaturesConfig | None = None,
) -> DataFrame:
    """
    Extract experience features from ExperienceCreated events.

    Args:
        experiences_df: DataFrame with experience data (from ExperienceCreated)
        target_date: Target date for feature computation (for days_until_start)
        config: Optional configuration override

    Returns:
        DataFrame with experience features

    Raises:
        ValueError: If input DataFrame is missing required columns
    """
    config = config or ExperienceFeaturesConfig()

    with JobStageContext("extract_experience_features") as ctx:
        # Validate input schema
        required_columns = ["id", "name"]
        _validate_required_columns(experiences_df, required_columns)

        logger.info(
            "Extracting experience features",
            target_date=str(target_date),
            input_count=experiences_df.count(),
        )

        # Extract base features
        # Helper to safely get column or null
        def _get_col_or_null(col_name: str) -> F.Column:
            if col_name in experiences_df.columns:
                return F.col(col_name)
            else:
                logger.warning(f"Column {col_name} missing from input schema, using NULL")
                return F.lit(None)

        # Helper to get coordinate column - tries nested struct first, then flattened
        def _get_coordinate_col(base: str, coord: str) -> F.Column:
            """Get coordinate from either nested struct or flattened column.

            Old storage_sink flattening: event_location_coordinate_latitude
            New lean extraction: event_location_coordinate_latitude
            Nested struct access: event_location_coordinate.latitude
            """
            nested_path = f"{base}.{coord}"  # e.g., event_location_coordinate.latitude
            flat_path = f"{base}_{coord}"  # e.g., event_location_coordinate_latitude

            # Try flattened column first (new extraction), then nested struct
            if flat_path in experiences_df.columns:
                return F.col(flat_path)
            else:
                # Nested struct access - will return null if column doesn't exist
                return F.col(nested_path)

        df = experiences_df.select(
            F.col("id").alias("experience_id"),
            F.col("name").alias("exp_name"),
            F.col("description").alias("exp_description"),
            # Category (Flattened)
            _get_col_or_null("category_id").alias("exp_category_id"),
            _get_col_or_null("category_name").alias("exp_category_name"),
            # Location - handle both nested struct and flattened field names
            _get_coordinate_col("event_location_coordinate", "latitude").alias("exp_location_lat"),
            _get_coordinate_col("event_location_coordinate", "longitude").alias("exp_location_lng"),
            _get_col_or_null("event_location_city").alias("exp_city"),
            _get_col_or_null("event_location_country").alias("exp_country"),
            # Price
            F.coalesce(F.col("price_amount"), F.lit(config.default_price)).alias(
                "exp_price_amount"
            ),
            F.col("price_currency").alias("exp_price_currency"),
            # Timing
            F.col("start_time").alias("exp_start_time"),
            F.col("end_time").alias("exp_end_time"),
            # Capacity
            F.col("bucket_size").alias("_bucket_size"),
            F.col("total_buckets").alias("_total_buckets"),
            # Creator
            F.col("creator_type").alias("exp_creator_type"),
            F.col("creator_id").alias("exp_creator_id"),
            # Tags (array)
            F.col("tags").alias("_tags_raw"),
            # Timestamps
            F.col("created_at").alias("exp_created_at"),
            F.col("updated_at").alias("exp_updated_at"),
        )

        # Extract tag features
        df = _extract_tag_features(df, config.max_tags)

        # Compute derived features
        df = _compute_derived_features(df, target_date, config)

        # Apply validation
        df = _apply_validation(df, config)

        # Add event_timestamp for Feast
        df = df.withColumn(
            "event_timestamp",
            F.to_timestamp(F.lit(target_date.isoformat())),
        )

        # Drop intermediate columns
        df = df.drop(
            "_tags_raw",
            "_bucket_size",
            "_total_buckets",
            "exp_start_time",
            "exp_end_time",
            "exp_created_at",
            "exp_updated_at",
        )

        # Compute quality metrics
        metrics = compute_quality_metrics(df, "experience")

        # Record metrics
        output_count = df.count()
        ctx.set_attribute("output_count", output_count)
        record_metric(
            "feature_extraction_records",
            output_count,
            {"entity_type": "experience", "stage": "extract"},
        )

        logger.info(
            "Experience features extracted",
            output_count=output_count,
            validity_rate=metrics.validity_rate,
        )

        return df


# Tag Feature Engineering


def _extract_tag_features(df: DataFrame, max_tags: int) -> DataFrame:
    """Extract and encode tag features."""
    # Extract tag IDs (limit to max_tags)
    df = df.withColumn(
        "exp_tag_ids",
        F.when(
            F.col("_tags_raw").isNotNull(),
            F.slice(
                F.transform(F.col("_tags_raw"), lambda x: x.getField("id")),
                1,
                max_tags,
            ),
        ).otherwise(F.array()),
    )

    # Count of tags
    df = df.withColumn(
        "exp_tag_count",
        F.size(F.col("exp_tag_ids")),
    )

    # Extract tag names for analysis
    df = df.withColumn(
        "exp_tag_names",
        F.when(
            F.col("_tags_raw").isNotNull(),
            F.slice(
                F.transform(F.col("_tags_raw"), lambda x: x.getField("name")),
                1,
                max_tags,
            ),
        ).otherwise(F.array()),
    )

    return df


# Derived Features


def _compute_derived_features(
    df: DataFrame,
    target_date: date,
    config: ExperienceFeaturesConfig,
) -> DataFrame:
    """Compute derived features from raw experience data."""
    target_ts = F.to_timestamp(F.lit(target_date.isoformat()))

    # Days until event starts
    df = df.withColumn(
        "exp_days_until_start",
        F.when(
            F.col("exp_start_time").isNotNull(),
            F.datediff(
                F.from_unixtime(F.col("exp_start_time") / 1000),  # Proto uses millis
                target_ts,
            ),
        ).otherwise(F.lit(0)),
    )

    # Is upcoming (starts within 7 days)
    df = df.withColumn(
        "exp_is_upcoming",
        F.when(
            (F.col("exp_days_until_start") >= 0) & (F.col("exp_days_until_start") <= 7),
            F.lit(True),
        ).otherwise(F.lit(False)),
    )

    # Is past event
    df = df.withColumn(
        "exp_is_past",
        F.when(F.col("exp_days_until_start") < 0, F.lit(True)).otherwise(F.lit(False)),
    )

    # Duration in hours
    df = df.withColumn(
        "exp_duration_hours",
        F.when(
            F.col("exp_start_time").isNotNull() & F.col("exp_end_time").isNotNull(),
            (F.col("exp_end_time") - F.col("exp_start_time")) / (1000 * 60 * 60),
        ).otherwise(F.lit(0.0)),
    )

    # Total capacity
    df = df.withColumn(
        "exp_capacity",
        F.coalesce(
            F.col("_bucket_size") * F.col("_total_buckets"),
            F.lit(config.default_capacity),
        ),
    )

    # Has capacity (not unlimited)
    df = df.withColumn(
        "exp_has_capacity",
        F.when(F.col("exp_capacity") > 0, F.lit(True)).otherwise(F.lit(False)),
    )

    # Is free event
    df = df.withColumn(
        "exp_is_free",
        F.when(F.col("exp_price_amount") == 0, F.lit(True)).otherwise(F.lit(False)),
    )

    # Price tier (for bucketing)
    df = df.withColumn(
        "exp_price_tier",
        F.when(F.col("exp_price_amount") == 0, F.lit("free"))
        .when(F.col("exp_price_amount") < 500, F.lit("budget"))
        .when(F.col("exp_price_amount") < 2000, F.lit("standard"))
        .when(F.col("exp_price_amount") < 5000, F.lit("premium"))
        .otherwise(F.lit("luxury")),
    )

    # Has location
    df = df.withColumn(
        "exp_has_location",
        F.when(
            F.col("exp_location_lat").isNotNull() & F.col("exp_location_lng").isNotNull(),
            F.lit(True),
        ).otherwise(F.lit(False)),
    )

    # Age in days (since creation)
    df = df.withColumn(
        "exp_age_days",
        F.when(
            F.col("exp_created_at").isNotNull(),
            F.datediff(
                target_ts,
                F.from_unixtime(F.col("exp_created_at") / 1000),
            ),
        ).otherwise(F.lit(0)),
    )

    # Description length
    df = df.withColumn(
        "exp_description_length",
        F.coalesce(F.length(F.col("exp_description")), F.lit(0)),
    )

    return df


# Validation


def _validate_required_columns(df: DataFrame, required: list[str]) -> None:
    """Validate that required columns exist."""
    missing = set(required) - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def _apply_validation(df: DataFrame, config: ExperienceFeaturesConfig) -> DataFrame:
    """Apply validation rules and add quality flags."""
    # Validate latitude range
    df = validate_range(
        df,
        "exp_location_lat",
        min_value=config.min_lat,
        max_value=config.max_lat,
        allow_null=True,
    )

    # Validate longitude range
    df = validate_range(
        df,
        "exp_location_lng",
        min_value=config.min_lng,
        max_value=config.max_lng,
        allow_null=True,
    )

    # Validate price range
    df = validate_range(
        df,
        "exp_price_amount",
        min_value=config.min_price,
        max_value=config.max_price,
        allow_null=False,
    )

    # Combine all checks
    df = add_quality_flags(df)

    return df


# Schema Definition


def get_experience_features_schema() -> T.StructType:
    """Return the schema for experience features."""
    return T.StructType(
        [
            T.StructField("experience_id", T.StringType(), nullable=False),
            T.StructField("exp_name", T.StringType(), nullable=True),
            T.StructField("exp_description", T.StringType(), nullable=True),
            T.StructField("exp_category_id", T.StringType(), nullable=True),
            T.StructField("exp_category_name", T.StringType(), nullable=True),
            T.StructField("exp_location_lat", T.DoubleType(), nullable=True),
            T.StructField("exp_location_lng", T.DoubleType(), nullable=True),
            T.StructField("exp_city", T.StringType(), nullable=True),
            T.StructField("exp_country", T.StringType(), nullable=True),
            T.StructField("exp_price_amount", T.DoubleType(), nullable=False),
            T.StructField("exp_price_currency", T.IntegerType(), nullable=True),
            T.StructField("exp_creator_type", T.StringType(), nullable=True),
            T.StructField("exp_creator_id", T.StringType(), nullable=True),
            T.StructField("exp_tag_ids", T.ArrayType(T.StringType()), nullable=False),
            T.StructField("exp_tag_count", T.IntegerType(), nullable=False),
            T.StructField("exp_tag_names", T.ArrayType(T.StringType()), nullable=False),
            T.StructField("exp_days_until_start", T.IntegerType(), nullable=False),
            T.StructField("exp_is_upcoming", T.BooleanType(), nullable=False),
            T.StructField("exp_is_past", T.BooleanType(), nullable=False),
            T.StructField("exp_duration_hours", T.DoubleType(), nullable=False),
            T.StructField("exp_capacity", T.IntegerType(), nullable=False),
            T.StructField("exp_has_capacity", T.BooleanType(), nullable=False),
            T.StructField("exp_is_free", T.BooleanType(), nullable=False),
            T.StructField("exp_price_tier", T.StringType(), nullable=False),
            T.StructField("exp_has_location", T.BooleanType(), nullable=False),
            T.StructField("exp_age_days", T.IntegerType(), nullable=False),
            T.StructField("exp_description_length", T.IntegerType(), nullable=False),
            T.StructField("is_valid", T.BooleanType(), nullable=False),
            T.StructField("event_timestamp", T.TimestampType(), nullable=False),
        ]
    )
