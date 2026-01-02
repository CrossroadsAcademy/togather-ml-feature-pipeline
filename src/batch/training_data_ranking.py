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
    - 1.0: Action (book, RSVP)

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
        # Explode served recommendations
        served_exploded = served_df.select(
            F.col("request_id"),
            F.col("user_id"),
            F.col("served_at"),
            F.col("trigger"),
            F.posexplode(F.col("recommendations")).alias("position", "rec"),
        ).select(
            F.col("request_id"),
            F.col("user_id"),
            F.col("served_at"),
            F.col("trigger"),
            F.col("position"),
            F.col("rec.event_id").alias("experience_id"),
            F.col("rec.ranking_score").alias("ranking_score"),
        )

        # Aggregate feedback per (request_id, experience_id)
        # Get the highest engagement level
        feedback_agg = feedback_df.groupBy("request_id", "user_id").agg(
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
                    F.col("view_data_dwell_time_ms"),
                )
            ).alias("max_dwell_time_ms"),
        )

        # Join served with feedback
        labeled = served_exploded.join(
            feedback_agg,
            on=["request_id", "user_id"],
            how="left",
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
        context = feedback_df.groupBy("request_id").agg(
            F.first("device_context.platform").alias("ctx_platform"),
            F.first("device_context.app_version").alias("ctx_app_version"),
            F.first("device_context.screen_width").alias("ctx_screen_width"),
            F.first("device_context.screen_height").alias("ctx_screen_height"),
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
    user_features_df: DataFrame,
    experience_features_df: DataFrame,
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
        labeled = labeled.join(
            user_features_df.drop("event_timestamp", "is_valid"),
            on="user_id",
            how="left",
        )

        # Join experience features
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
    """Add cross features between user and experience."""
    # Distance (reuse from two_tower)
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

    # Same city
    df = df.withColumn(
        "cross_same_city",
        F.when(
            (F.col("user_city").isNotNull())
            & (F.col("exp_city").isNotNull())
            & (F.lower(F.col("user_city")) == F.lower(F.col("exp_city"))),
            F.lit(True),
        ).otherwise(F.lit(False)),
    )

    # Price match (user's typical vs experience price)
    # This would require historical spend data

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
