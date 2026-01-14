"""
Training Data Generator for Ranking Model.

Generates training samples for the ranking model that re-ranks retrieved candidates.
Uses graded relevance labels based on engagement level.

Label scheme:
    - 0.0: Impression only (shown but no interaction)
    - 0.1: View (short dwell time < 3s)
    - 0.3: View (dwell time 3-10s)
    - 0.5: Click
    - 0.7: View (dwell time > 10s)
    - 1.0: Action (book)

Output includes context features (device, time, trigger) for contextual ranking.
"""

from dataclasses import dataclass
from datetime import date
from typing import TYPE_CHECKING

import structlog
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.batch.engagement_features import EventType
from src.batch.observability import JobStageContext, record_metric

if TYPE_CHECKING:
    pass

logger = structlog.get_logger(__name__)


# Configuration


@dataclass
class RankingTrainingConfig:
    """Configuration for ranking training data generation."""

    # Label thresholds
    short_view_threshold_ms: int = 3000  # 3 seconds
    long_view_threshold_ms: int = 10000  # 10 seconds

    # Label values (graded relevance)
    label_impression: float = 0.0
    label_view_short: float = 0.1
    label_view_medium: float = 0.3
    label_click: float = 0.5
    label_view_long: float = 0.7
    label_action: float = 1.0

    # Output path
    output_bucket: str = "training-data"
    output_table: str = "ranking"

    # Position bias correction
    include_position_features: bool = True


# Label Assignment


def assign_graded_labels(
    served_df: DataFrame,
    feedback_df: DataFrame,
    config: RankingTrainingConfig | None = None,
) -> DataFrame:
    """
    Join served recommendations with feedback and assign graded labels.

    Args:
        served_df: RecommendationServed events (what was shown)
        feedback_df: RecommendationFeedback events (user interactions)
        config: Optional configuration

    Returns:
        DataFrame with (request_id, user_id, experience_id, position, label)
    """
    config = config or RankingTrainingConfig()

    with JobStageContext("assign_graded_labels") as ctx:
        from pyspark.sql.types import (
            ArrayType,
            FloatType,
            IntegerType,
            StringType,
            StructField,
            StructType,
        )

        # Recommendations might be stored as native array OR JSON string (due to StorageSink changes)
        if "recommendations" in served_df.columns:
            # Check if it's a string and parse it
            if dict(served_df.dtypes)["recommendations"] == "string":
                rec_array_schema = ArrayType(
                    StructType(
                        [
                            StructField("event_id", StringType()),
                            StructField("ranking_score", FloatType()),
                        ]
                    )
                )
                served_df = served_df.withColumn(
                    "recommendations", F.from_json("recommendations", rec_array_schema)
                )

            served_df = served_df.filter(F.col("recommendations").isNotNull())
        else:
            logger.warning("No 'recommendations' column in served_df")
            # Return empty DataFrame with expected schema
            empty_schema = StructType(
                [
                    StructField("request_id", StringType()),
                    StructField("user_id", StringType()),
                    StructField("experience_id", StringType()),
                    StructField("position", IntegerType()),
                    StructField("label", IntegerType()),
                ]
            )
            return served_df.sparkSession.createDataFrame([], empty_schema)

        # Use timestamp as fallback for served_at if not present
        if "served_at" not in served_df.columns:
            served_df = served_df.withColumn("served_at", F.col("timestamp"))

        # Use default trigger if not present
        if "trigger" not in served_df.columns:
            served_df = served_df.withColumn("trigger", F.lit("UNKNOWN"))

        # Explode served recommendations
        # recommendations is array of structs: [{event_id, ranking_score}, ...]
        # Also extract session_id for fallback matching
        served_exploded = served_df.select(
            F.col("request_id"),
            F.col("user_id"),
            F.col("session_id"),
            F.col("served_at"),
            F.col("trigger"),
            F.col("timestamp").alias("served_timestamp"),
            F.posexplode(F.col("recommendations")).alias("position", "rec"),
        ).select(
            F.col("request_id"),
            F.col("user_id"),
            F.col("session_id"),
            F.col("served_at"),
            F.col("served_timestamp"),
            F.col("trigger"),
            F.col("position"),
            F.col("rec.event_id").alias("experience_id"),
            F.col("rec.ranking_score").alias("ranking_score"),
        )

        # Normalize experience_id: in RecommendationFeedback, event_id IS the experience_id
        if "experience_id" not in feedback_df.columns and "event_id" in feedback_df.columns:
            feedback_df = feedback_df.withColumn("experience_id", F.col("event_id"))
            logger.info("Normalized event_id -> experience_id for feedback events")

        # Dwell time column (flattened from view_data.dwell_time_ms -> view_data_dwell_time_ms)
        dwell_time_col = F.coalesce(
            F.col("view_data_dwell_time_ms"),
            F.lit(0),
        )

        # Two-stage join strategy for feedback matching:
        # 1. Primary: Join on (request_id, user_id, experience_id) - for ML "For You"
        # 2. Fallback: Join on (session_id, user_id, experience_id) - for Explore/detail views

        # Aggregate feedback - group by both request_id AND session_id for flexibility
        feedback_agg = feedback_df.groupBy(
            "request_id", "session_id", "user_id", "experience_id"
        ).agg(
            # Did user click?
            F.max(
                F.when(F.col("event_type") == EventType.CLICK, F.lit(1)).otherwise(F.lit(0))
            ).alias("clicked"),
            # Did user take action?
            F.max(
                F.when(F.col("event_type") == EventType.ACTION, F.lit(1)).otherwise(F.lit(0))
            ).alias("actioned"),
            # Max dwell time (for views)
            F.max(
                F.when(
                    F.col("event_type") == EventType.VIEW,
                    dwell_time_col,
                )
            ).alias("max_dwell_time_ms"),
            # Track feedback timestamp for time-window filtering
            F.min("timestamp").alias("first_feedback_ts"),
        )

        # Also create session-level aggregation for fallback
        feedback_by_session = feedback_df.groupBy("session_id", "user_id", "experience_id").agg(
            F.max(
                F.when(F.col("event_type") == EventType.CLICK, F.lit(1)).otherwise(F.lit(0))
            ).alias("sess_clicked"),
            F.max(
                F.when(F.col("event_type") == EventType.ACTION, F.lit(1)).otherwise(F.lit(0))
            ).alias("sess_actioned"),
            F.max(F.when(F.col("event_type") == EventType.VIEW, dwell_time_col)).alias(
                "sess_max_dwell_time_ms"
            ),
        )

        # Stage 1: Try joining on request_id (works for ML "For You" section)
        labeled = served_exploded.join(
            feedback_agg.select(
                "request_id",
                "user_id",
                "experience_id",
                "clicked",
                "actioned",
                "max_dwell_time_ms",
            ),
            on=["request_id", "user_id", "experience_id"],
            how="left",
        )

        # Stage 2: For rows that didn't match, try session_id fallback
        # This handles Explore section and detail view clicks
        labeled = labeled.join(
            feedback_by_session,
            on=["session_id", "user_id", "experience_id"],
            how="left",
        )

        # Coalesce: prefer request_id match, fall back to session_id match
        labeled = (
            labeled.withColumn(
                "clicked",
                F.coalesce(F.col("clicked"), F.col("sess_clicked"), F.lit(0)),
            )
            .withColumn(
                "actioned",
                F.coalesce(F.col("actioned"), F.col("sess_actioned"), F.lit(0)),
            )
            .withColumn(
                "max_dwell_time_ms",
                F.coalesce(
                    F.col("max_dwell_time_ms"),
                    F.col("sess_max_dwell_time_ms"),
                    F.lit(0),
                ),
            )
            .drop("sess_clicked", "sess_actioned", "sess_max_dwell_time_ms")
        )

        # Assign graded labels
        labeled = labeled.withColumn(
            "label",
            F.when(
                F.col("actioned") == 1,
                F.lit(config.label_action),
            )
            .when(
                F.col("max_dwell_time_ms") >= config.long_view_threshold_ms,
                F.lit(config.label_view_long),
            )
            .when(
                F.col("clicked") == 1,
                F.lit(config.label_click),
            )
            .when(
                F.col("max_dwell_time_ms") >= config.short_view_threshold_ms,
                F.lit(config.label_view_medium),
            )
            .when(
                F.col("max_dwell_time_ms").isNotNull(),
                F.lit(config.label_view_short),
            )
            .otherwise(F.lit(config.label_impression)),  # No interaction
        )

        # Fill nulls in intermediate columns
        labeled = labeled.fillna(
            {
                "clicked": 0,
                "actioned": 0,
                "max_dwell_time_ms": 0,
            }
        )

        sample_count = labeled.count()
        ctx.set_attribute("sample_count", sample_count)

        # Log label distribution
        label_dist = labeled.groupBy("label").count().orderBy("label").collect()
        for row in label_dist:
            record_metric(
                "training_labels_distribution",
                row["count"],
                {"model": "ranking", "label": str(row["label"])},
            )

        logger.info(
            "Graded labels assigned",
            sample_count=sample_count,
            label_distribution={row["label"]: row["count"] for row in label_dist},
        )

        return labeled


# Context Features


def extract_context_features(
    labeled_df: DataFrame,
    feedback_df: DataFrame,
) -> DataFrame:
    """
    Extract context features from feedback events.

    Context features:
    - Device: platform (iOS, Android, Web)
    - Time: hour of day, day of week
    - Trigger: what caused the recommendation request

    Args:
        labeled_df: Labeled samples
        feedback_df: Feedback events with device_context

    Returns:
        DataFrame with context features added
    """
    with JobStageContext("extract_context_features") as ctx:  # noqa: F841
        # Get device context from any feedback event per request
        # Columns are flattened: device_context.platform -> device_context_platform
        context = feedback_df.groupBy("request_id").agg(
            F.first("device_context_platform").alias("ctx_platform"),
            F.first("device_context_app_version").alias("ctx_app_version"),
            F.first("device_context_screen_width").alias("ctx_screen_width"),
            F.first("device_context_screen_height").alias("ctx_screen_height"),
            F.first("client_timestamp").alias("ctx_client_timestamp"),
        )

        # Join context
        df = labeled_df.join(context, on="request_id", how="left")

        # Extract time features
        df = df.withColumn(
            "ctx_hour_of_day",
            F.when(
                F.col("ctx_client_timestamp").isNotNull(),
                F.hour(F.from_unixtime(F.col("ctx_client_timestamp") / 1000)),
            ).otherwise(F.lit(12)),  # Default to noon
        )

        df = df.withColumn(
            "ctx_day_of_week",
            F.when(
                F.col("ctx_client_timestamp").isNotNull(),
                F.dayofweek(F.from_unixtime(F.col("ctx_client_timestamp") / 1000)),
            ).otherwise(F.lit(1)),  # Default to Sunday
        )

        # Is weekend
        df = df.withColumn(
            "ctx_is_weekend",
            F.when(
                F.col("ctx_day_of_week").isin([1, 7]),  # Sunday=1, Saturday=7
                F.lit(True),
            ).otherwise(F.lit(False)),
        )

        # Time of day bucket
        df = df.withColumn(
            "ctx_time_bucket",
            F.when(F.col("ctx_hour_of_day") < 6, F.lit("night"))
            .when(F.col("ctx_hour_of_day") < 12, F.lit("morning"))
            .when(F.col("ctx_hour_of_day") < 17, F.lit("afternoon"))
            .when(F.col("ctx_hour_of_day") < 21, F.lit("evening"))
            .otherwise(F.lit("night")),
        )

        return df


# Complete Training Data


def create_ranking_training_data(
    served_df: DataFrame,
    feedback_df: DataFrame,
    user_features_df: DataFrame | None,
    experience_features_df: DataFrame | None,
    target_date: date,
    config: RankingTrainingConfig | None = None,
) -> DataFrame:
    """
    Create complete training dataset for Ranking model.

    Args:
        served_df: RecommendationServed events
        feedback_df: RecommendationFeedback events
        user_features_df: User features
        experience_features_df: Experience features
        target_date: Target date
        config: Optional configuration

    Returns:
        Complete training DataFrame
    """
    config = config or RankingTrainingConfig()

    with JobStageContext("create_ranking_training_data") as ctx:
        # Assign graded labels
        labeled = assign_graded_labels(served_df, feedback_df, config)

        # Add context features
        labeled = extract_context_features(labeled, feedback_df)

        # Join user features
        if user_features_df is not None:
            labeled = labeled.join(
                user_features_df.drop("event_timestamp", "is_valid"),
                on="user_id",
                how="left",
            )

        # Join experience features
        if experience_features_df is not None:
            labeled = labeled.join(
                experience_features_df.drop("event_timestamp", "is_valid"),
                on="experience_id",
                how="left",
            )

        # Add position bias features
        if config.include_position_features:
            labeled = _add_position_features(labeled)

        # Add cross features
        labeled = _add_cross_features(labeled)

        # Add metadata
        labeled = labeled.withColumn(
            "training_date",
            F.lit(target_date.isoformat()),
        )

        # Record metrics
        total_count = labeled.count()
        positive_count = labeled.filter(F.col("label") > 0).count()

        ctx.set_attribute("total_samples", total_count)
        ctx.set_attribute("positive_samples", positive_count)

        record_metric(
            "training_data_samples",
            total_count,
            {"model": "ranking", "label": "total"},
        )

        logger.info(
            "Ranking training data created",
            total=total_count,
            with_engagement=positive_count,
            impression_only=total_count - positive_count,
        )

        return labeled


def _add_position_features(df: DataFrame) -> DataFrame:
    """Add position-related features for bias correction."""
    # Position normalized (0-1 scale)
    max_pos = 50  # Assume max 50 items in a request
    df = df.withColumn(
        "position_normalized",
        F.col("position") / F.lit(max_pos),
    )

    # Position buckets
    df = df.withColumn(
        "position_bucket",
        F.when(F.col("position") <= 3, F.lit("top_3"))
        .when(F.col("position") <= 10, F.lit("top_10"))
        .when(F.col("position") <= 20, F.lit("top_20"))
        .otherwise(F.lit("below_20")),
    )

    # Is first page (assuming 10 items per page)
    df = df.withColumn(
        "is_first_page",
        F.when(F.col("position") <= 10, F.lit(True)).otherwise(F.lit(False)),
    )

    return df


def _add_cross_features(df: DataFrame) -> DataFrame:
    """Add cross features between user and experience.

    Gracefully handles missing columns by checking column existence first.
    """
    columns = df.columns

    # Distance features (only if location columns exist)
    has_location_cols = all(
        col in columns
        for col in [
            "user_has_location",
            "exp_has_location",
            "user_location_lat",
            "user_location_lng",
            "exp_location_lat",
            "exp_location_lng",
        ]
    )

    if has_location_cols:
        df = df.withColumn(
            "cross_distance_km",
            F.when(
                F.col("user_has_location") & F.col("exp_has_location"),
                _haversine_distance(
                    F.col("user_location_lat"),
                    F.col("user_location_lng"),
                    F.col("exp_location_lat"),
                    F.col("exp_location_lng"),
                ),
            ).otherwise(F.lit(None)),
        )

        # Distance bucket
        df = df.withColumn(
            "cross_distance_bucket",
            F.when(F.col("cross_distance_km").isNull(), F.lit("unknown"))
            .when(F.col("cross_distance_km") <= 5, F.lit("nearby"))
            .when(F.col("cross_distance_km") <= 20, F.lit("local"))
            .when(F.col("cross_distance_km") <= 50, F.lit("regional"))
            .otherwise(F.lit("distant")),
        )
    else:
        # Add placeholder columns when location data unavailable
        df = df.withColumn("cross_distance_km", F.lit(None).cast("double"))
        df = df.withColumn("cross_distance_bucket", F.lit("unknown"))
        logger.info("Skipping distance features: location columns not available")

    # Same city feature (only if city columns exist)
    has_city_cols = "user_city" in columns and "exp_city" in columns

    if has_city_cols:
        df = df.withColumn(
            "cross_same_city",
            F.when(
                (F.col("user_city").isNotNull())
                & (F.col("exp_city").isNotNull())
                & (F.lower(F.col("user_city")) == F.lower(F.col("exp_city"))),
                F.lit(True),
            ).otherwise(F.lit(False)),
        )
    else:
        df = df.withColumn("cross_same_city", F.lit(False))
        logger.info("Skipping same_city feature: city columns not available")

    return df


def _haversine_distance(lat1, lng1, lat2, lng2):
    """Calculate Haversine distance in kilometers."""
    R = 6371

    lat1_rad = F.radians(lat1)
    lat2_rad = F.radians(lat2)
    dlat = F.radians(lat2 - lat1)
    dlng = F.radians(lng2 - lng1)

    a = F.pow(F.sin(dlat / 2), 2) + F.cos(lat1_rad) * F.cos(lat2_rad) * F.pow(F.sin(dlng / 2), 2)

    return F.lit(R) * F.lit(2) * F.asin(F.sqrt(a))
