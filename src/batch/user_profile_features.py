"""
User Profile Features Extractor.

Extracts features from UserProfileCreated events for the Two-Tower and Ranking models.

Features extracted:
    - user_interest_ids: List of interest IDs
    - user_interest_count: Number of interests
    - user_location_lat: Current latitude
    - user_location_lng: Current longitude
    - user_city: Current city
    - user_country: Current country
    - user_gender: Gender enum value
    - user_social_score: Platform engagement score
    - user_account_age_days: Days since account creation
    - user_onboarding_status: Onboarding completion status
"""

from dataclasses import dataclass
from datetime import date
from typing import TYPE_CHECKING

import structlog
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from src.batch.observability import JobStageContext, record_metric

if TYPE_CHECKING:
    pass

logger = structlog.get_logger(__name__)


# Configuration


@dataclass
class UserProfileFeaturesConfig:
    """Configuration for user profile feature extraction."""

    # Input/Output paths
    user_profiles_bucket: str = "user-profiles"
    output_bucket: str = "feast-offline-store"
    output_table: str = "user_profile_features"

    # Feature engineering params
    max_interests: int = 20  # Cap interests
    default_social_score: int = 0
    default_gender: int = 0  # GENDER_UNSPECIFIED

    # Validation thresholds
    min_lat: float = -90.0
    max_lat: float = 90.0
    min_lng: float = -180.0
    max_lng: float = 180.0


# Feature Extraction


def extract_user_profile_features(
    user_profiles_df: DataFrame,
    target_date: date,
    config: UserProfileFeaturesConfig | None = None,
) -> DataFrame:
    """
    Extract user profile features from UserProfileCreated events.

    Args:
        user_profiles_df: DataFrame with user profile data (from UserProfileCreated)
        target_date: Target date for feature computation
        config: Optional configuration override

    Returns:
        DataFrame with user profile features

    Raises:
        ValueError: If input DataFrame is missing required columns
    """
    config = config or UserProfileFeaturesConfig()

    with JobStageContext("extract_user_profile_features") as ctx:
        # Validate input schema
        required_columns = ["id", "created_at"]
        _validate_required_columns(user_profiles_df, required_columns)

        logger.info(
            "Extracting user profile features",
            target_date=str(target_date),
            input_count=user_profiles_df.count(),
        )

        # Helper to safely get column or null
        def _get_col_or_null(col_name: str) -> F.Column:
            if col_name in user_profiles_df.columns:
                return F.col(col_name)
            else:
                logger.warning(f"Column {col_name} missing from input schema, using NULL")
                return F.lit(None)

        # Start with user ID
        df = user_profiles_df.select(
            F.col("id").alias("user_id"),
            # Location features (coordinate structs seem to be consistently present)
            F.col("current_address_coordinate.latitude").alias("user_location_lat"),
            F.col("current_address_coordinate.longitude").alias("user_location_lng"),
            # Flattened fields might be missing if source data didn't have them
            _get_col_or_null("current_address_city").alias("user_city"),
            _get_col_or_null("current_address_country").alias("user_country"),
            # Profile features
            F.col("gender").alias("user_gender"),
            F.coalesce(F.col("social_score"), F.lit(config.default_social_score)).alias(
                "user_social_score"
            ),
            F.col("on_boarding_status").alias("user_onboarding_status"),
            # Interests (array)
            F.col("interests").alias("_interests_raw"),
            # Timestamps
            F.col("created_at").alias("user_created_at"),
            F.col("updated_at").alias("user_updated_at"),
        )

        # Extract interest features
        df = _extract_interest_features(df, config.max_interests)

        # Compute derived features
        df = _compute_derived_features(df, target_date)

        # Apply validation and quality flags
        df = _apply_validation(df, config)

        # Add event_timestamp for Feast
        df = df.withColumn(
            "event_timestamp",
            F.to_timestamp(F.lit(target_date.isoformat())),
        )

        # Drop intermediate columns
        df = df.drop("_interests_raw", "user_created_at", "user_updated_at")

        # Record metrics
        output_count = df.count()
        ctx.set_attribute("output_count", output_count)
        record_metric(
            "feature_extraction_records",
            output_count,
            {"entity_type": "user_profile", "stage": "extract"},
        )

        logger.info(
            "User profile features extracted",
            output_count=output_count,
        )

        return df


# Interest Feature Engineering


def _extract_interest_features(df: DataFrame, max_interests: int) -> DataFrame:
    """Extract and encode interest features."""
    # Extract interest IDs (limit to max_interests)
    df = df.withColumn(
        "user_interest_ids",
        F.when(
            F.col("_interests_raw").isNotNull(),
            F.slice(
                F.transform(F.col("_interests_raw"), lambda x: x.getField("id")),
                1,
                max_interests,
            ),
        ).otherwise(F.array()),
    )

    # Count of interests (useful for cold-start detection)
    df = df.withColumn(
        "user_interest_count",
        F.size(F.col("user_interest_ids")),
    )

    # Extract interest names for debugging/analysis
    df = df.withColumn(
        "user_interest_names",
        F.when(
            F.col("_interests_raw").isNotNull(),
            F.slice(
                F.transform(F.col("_interests_raw"), lambda x: x.getField("name")),
                1,
                max_interests,
            ),
        ).otherwise(F.array()),
    )

    return df


# Derived Features


def _compute_derived_features(df: DataFrame, target_date: date) -> DataFrame:
    """Compute derived features from raw profile data."""
    target_ts = F.to_timestamp(F.lit(target_date.isoformat()))

    # Account age in days
    df = df.withColumn(
        "user_account_age_days",
        F.when(
            F.col("user_created_at").isNotNull(),
            F.datediff(
                target_ts,
                F.from_unixtime(F.col("user_created_at") / 1000),  # Proto uses millis
            ),
        ).otherwise(F.lit(0)),
    )

    # Is new user (< 7 days)
    df = df.withColumn(
        "user_is_new",
        F.when(F.col("user_account_age_days") < 7, F.lit(True)).otherwise(F.lit(False)),
    )

    # Has completed onboarding
    df = df.withColumn(
        "user_onboarding_completed",
        F.when(
            F.col("user_onboarding_status") == 2,  # ON_BOARDING_STATUS_COMPLETED
            F.lit(True),
        ).otherwise(F.lit(False)),
    )

    # Has location (important for geo-based recommendations)
    df = df.withColumn(
        "user_has_location",
        F.when(
            F.col("user_location_lat").isNotNull() & F.col("user_location_lng").isNotNull(),
            F.lit(True),
        ).otherwise(F.lit(False)),
    )

    return df


# Validation


def _validate_required_columns(df: DataFrame, required: list[str]) -> None:
    """Validate that required columns exist."""
    missing = set(required) - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def _apply_validation(df: DataFrame, config: UserProfileFeaturesConfig) -> DataFrame:
    """Apply validation rules and add quality flags."""
    # Validate latitude range
    df = df.withColumn(
        "_valid_lat",
        F.when(
            F.col("user_location_lat").isNull(),
            F.lit(True),  # Null is valid (no location)
        ).otherwise(
            (F.col("user_location_lat") >= config.min_lat)
            & (F.col("user_location_lat") <= config.max_lat)
        ),
    )

    # Validate longitude range
    df = df.withColumn(
        "_valid_lng",
        F.when(
            F.col("user_location_lng").isNull(),
            F.lit(True),
        ).otherwise(
            (F.col("user_location_lng") >= config.min_lng)
            & (F.col("user_location_lng") <= config.max_lng)
        ),
    )

    # Overall quality flag
    df = df.withColumn(
        "is_valid",
        F.col("_valid_lat") & F.col("_valid_lng"),
    )

    # Log invalid records
    invalid_count = df.filter(~F.col("is_valid")).count()
    if invalid_count > 0:
        logger.warning(
            "Found invalid user profile records",
            invalid_count=invalid_count,
        )
        record_metric(
            "feature_validation_failures",
            invalid_count,
            {"entity_type": "user_profile"},
        )

    # Drop validation helper columns
    df = df.drop("_valid_lat", "_valid_lng")

    return df


# Schema Definition (for downstream consumers)


def get_user_profile_features_schema() -> T.StructType:
    """Return the schema for user profile features."""
    return T.StructType(
        [
            T.StructField("user_id", T.StringType(), nullable=False),
            T.StructField("user_location_lat", T.DoubleType(), nullable=True),
            T.StructField("user_location_lng", T.DoubleType(), nullable=True),
            T.StructField("user_city", T.StringType(), nullable=True),
            T.StructField("user_country", T.StringType(), nullable=True),
            T.StructField("user_gender", T.IntegerType(), nullable=True),
            T.StructField("user_social_score", T.IntegerType(), nullable=False),
            T.StructField("user_onboarding_status", T.IntegerType(), nullable=True),
            T.StructField("user_interest_ids", T.ArrayType(T.StringType()), nullable=False),
            T.StructField("user_interest_count", T.IntegerType(), nullable=False),
            T.StructField("user_interest_names", T.ArrayType(T.StringType()), nullable=False),
            T.StructField("user_account_age_days", T.IntegerType(), nullable=False),
            T.StructField("user_is_new", T.BooleanType(), nullable=False),
            T.StructField("user_onboarding_completed", T.BooleanType(), nullable=False),
            T.StructField("user_has_location", T.BooleanType(), nullable=False),
            T.StructField("is_valid", T.BooleanType(), nullable=False),
            T.StructField("event_timestamp", T.TimestampType(), nullable=False),
        ]
    )
