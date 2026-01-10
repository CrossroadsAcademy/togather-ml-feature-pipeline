"""
Engagement Features Aggregator.

Aggregates engagement features from RecommendationFeedback events for users and experiences.

User Engagement Features:
    - user_impressions_7d: Total impressions received
    - user_clicks_7d: Total clicks
    - user_views_7d: Total views (with dwell time)
    - user_actions_7d: Total actions (bookings)
    - user_ctr_7d: Click-through rate
    - user_action_rate_7d: Actions per click
    - user_avg_dwell_time_ms_7d: Average view duration

Experience Engagement Features:
    - exp_impressions_7d: Total impressions
    - exp_clicks_7d: Total clicks
    - exp_unique_viewers_7d: Distinct users who viewed
    - exp_ctr_7d: Click-through rate
    - exp_avg_position_7d: Average position shown
    - exp_booking_rate_7d: Actions per click
"""

from dataclasses import dataclass
from datetime import date
from typing import TYPE_CHECKING

import structlog
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from src.batch.observability import JobStageContext

if TYPE_CHECKING:
    pass

logger = structlog.get_logger(__name__)


# Constants (from feed.v1.EventType proto)


class EventType:
    """Event type enum values from RecommendationFeedback proto.

    NOTE: Protobuf enums are serialized as strings when using MessageToDict.
    These values match the proto enum names: EVENT_TYPE_IMPRESSION, etc.
    """

    UNSPECIFIED = "EVENT_TYPE_UNSPECIFIED"
    IMPRESSION = "EVENT_TYPE_IMPRESSION"
    CLICK = "EVENT_TYPE_CLICK"
    VIEW = "EVENT_TYPE_VIEW"
    ACTION = "EVENT_TYPE_ACTION"
    NEGATIVE = "EVENT_TYPE_NEGATIVE"


# Configuration


@dataclass
class EngagementFeaturesConfig:
    """Configuration for engagement feature aggregation."""

    # Time windows
    window_days_short: int = 7
    window_days_long: int = 30

    # Quality thresholds
    min_dwell_time_ms: int = 1000  # 1 second minimum for valid view
    max_dwell_time_ms: int = 600000  # 10 minutes cap for outliers

    # Smoothing for rates (Laplace smoothing)
    ctr_smoothing: int = 10
    action_rate_smoothing: int = 5


# User Engagement Features


def aggregate_user_engagement_features(
    feedback_df: DataFrame,
    target_date: date,
    config: EngagementFeaturesConfig | None = None,
) -> DataFrame:
    """
    Aggregate user-level engagement features from RecommendationFeedback.

    Args:
        feedback_df: DataFrame with RecommendationFeedback events
        target_date: Target date for aggregation window
        config: Optional configuration override

    Returns:
        DataFrame with user engagement features
    """
    config = config or EngagementFeaturesConfig()

    with JobStageContext("aggregate_user_engagement_features") as ctx:
        # Define time boundaries
        target_ts = F.to_timestamp(F.lit(target_date.isoformat()))
        window_start_7d = F.date_sub(target_ts, config.window_days_short)
        window_start_30d = F.date_sub(target_ts, config.window_days_long)
        target_end = F.date_add(target_ts, 1)  # Include full target date

        # Filter to 30-day window (we'll compute both 7d and 30d)
        df = feedback_df.filter(
            (F.col("timestamp") >= window_start_30d) & (F.col("timestamp") < target_end)
        )

        # Add time window flags
        df = df.withColumn(
            "is_7d",
            F.when(F.col("timestamp") >= window_start_7d, F.lit(1)).otherwise(F.lit(0)),
        )

        # Extract event type counts
        df = df.withColumn(
            "is_impression",
            F.when(F.col("event_type") == EventType.IMPRESSION, F.lit(1)).otherwise(F.lit(0)),
        )
        df = df.withColumn(
            "is_click",
            F.when(F.col("event_type") == EventType.CLICK, F.lit(1)).otherwise(F.lit(0)),
        )
        df = df.withColumn(
            "is_view",
            F.when(F.col("event_type") == EventType.VIEW, F.lit(1)).otherwise(F.lit(0)),
        )
        df = df.withColumn(
            "is_action",
            F.when(F.col("event_type") == EventType.ACTION, F.lit(1)).otherwise(F.lit(0)),
        )
        df = df.withColumn(
            "is_negative",
            F.when(F.col("event_type") == EventType.NEGATIVE, F.lit(1)).otherwise(F.lit(0)),
        )

        # Extract view data (dwell time)
        df = df.withColumn(
            "dwell_time_ms",
            F.when(
                (F.col("event_type") == EventType.VIEW)
                & (F.col("view_data.dwell_time_ms").isNotNull()),
                F.least(
                    F.greatest(
                        F.col("view_data.dwell_time_ms"),
                        F.lit(config.min_dwell_time_ms),
                    ),
                    F.lit(config.max_dwell_time_ms),
                ),
            ).otherwise(F.lit(0)),
        )

        # Aggregate by user
        user_features = df.groupBy("user_id").agg(
            # 7-day metrics
            F.sum(F.col("is_impression") * F.col("is_7d")).alias("user_impressions_7d"),
            F.sum(F.col("is_click") * F.col("is_7d")).alias("user_clicks_7d"),
            F.sum(F.col("is_view") * F.col("is_7d")).alias("user_views_7d"),
            F.sum(F.col("is_action") * F.col("is_7d")).alias("user_actions_7d"),
            F.sum(F.col("is_negative") * F.col("is_7d")).alias("user_negatives_7d"),
            # 30-day metrics
            F.sum("is_impression").alias("user_impressions_30d"),
            F.sum("is_click").alias("user_clicks_30d"),
            F.sum("is_view").alias("user_views_30d"),
            F.sum("is_action").alias("user_actions_30d"),
            # Dwell time (7d only)
            F.avg(
                F.when(
                    (F.col("is_view") == 1) & (F.col("is_7d") == 1),
                    F.col("dwell_time_ms"),
                )
            ).alias("user_avg_dwell_time_ms_7d"),
            # Unique experiences engaged with
            F.countDistinct(
                F.when(
                    (F.col("is_click") == 1) & (F.col("is_7d") == 1),
                    F.col("experience_id"),  # Assuming this field exists
                )
            ).alias("user_unique_clicks_7d"),
        )

        # Compute derived rates with Laplace smoothing
        user_features = _compute_user_rates(user_features, config)

        # Add cold-start indicator
        user_features = user_features.withColumn(
            "user_is_cold_start",
            F.when(F.col("user_impressions_7d") < 10, F.lit(True)).otherwise(F.lit(False)),
        )

        # Fill nulls with defaults
        user_features = _fill_user_defaults(user_features)

        # Add event_timestamp
        user_features = user_features.withColumn(
            "event_timestamp",
            F.to_timestamp(F.lit(target_date.isoformat())),
        )

        # Add validity flag
        user_features = user_features.withColumn("is_valid", F.lit(True))

        output_count = user_features.count()
        ctx.set_attribute("output_count", output_count)

        logger.info(
            "User engagement features aggregated",
            output_count=output_count,
        )

        return user_features


def _compute_user_rates(df: DataFrame, config: EngagementFeaturesConfig) -> DataFrame:
    """Compute CTR and action rates with Laplace smoothing."""
    # CTR = clicks / (impressions + smoothing)
    df = df.withColumn(
        "user_ctr_7d",
        F.col("user_clicks_7d") / (F.col("user_impressions_7d") + config.ctr_smoothing),
    )
    df = df.withColumn(
        "user_ctr_30d",
        F.col("user_clicks_30d") / (F.col("user_impressions_30d") + config.ctr_smoothing),
    )

    # Action rate = actions / (clicks + smoothing)
    df = df.withColumn(
        "user_action_rate_7d",
        F.col("user_actions_7d") / (F.col("user_clicks_7d") + config.action_rate_smoothing),
    )

    # View rate = views / clicks (how often clicked leads to view)
    df = df.withColumn(
        "user_view_rate_7d",
        F.col("user_views_7d") / (F.col("user_clicks_7d") + config.action_rate_smoothing),
    )

    return df


def _fill_user_defaults(df: DataFrame) -> DataFrame:
    """Fill null values with defaults for user features."""
    defaults = {
        "user_impressions_7d": 0,
        "user_clicks_7d": 0,
        "user_views_7d": 0,
        "user_actions_7d": 0,
        "user_negatives_7d": 0,
        "user_impressions_30d": 0,
        "user_clicks_30d": 0,
        "user_views_30d": 0,
        "user_actions_30d": 0,
        "user_avg_dwell_time_ms_7d": 0.0,
        "user_unique_clicks_7d": 0,
        "user_ctr_7d": 0.0,
        "user_ctr_30d": 0.0,
        "user_action_rate_7d": 0.0,
        "user_view_rate_7d": 0.0,
    }

    for col, default in defaults.items():
        if col in df.columns:
            df = df.withColumn(col, F.coalesce(F.col(col), F.lit(default)))

    return df


# Experience Engagement Features


def aggregate_experience_engagement_features(
    feedback_df: DataFrame,
    target_date: date,
    config: EngagementFeaturesConfig | None = None,
) -> DataFrame:
    """
    Aggregate experience-level engagement features from RecommendationFeedback.

    Args:
        feedback_df: DataFrame with RecommendationFeedback events
        target_date: Target date for aggregation window
        config: Optional configuration override

    Returns:
        DataFrame with experience engagement features
    """
    config = config or EngagementFeaturesConfig()

    with JobStageContext("aggregate_experience_engagement_features") as ctx:
        # Define time boundaries
        target_ts = F.to_timestamp(F.lit(target_date.isoformat()))
        window_start_7d = F.date_sub(target_ts, config.window_days_short)
        target_end = F.date_add(target_ts, 1)

        # Filter to 7-day window
        df = feedback_df.filter(
            (F.col("timestamp") >= window_start_7d) & (F.col("timestamp") < target_end)
        )

        # Add event type flags
        df = df.withColumn(
            "is_impression",
            F.when(F.col("event_type") == EventType.IMPRESSION, F.lit(1)).otherwise(F.lit(0)),
        )
        df = df.withColumn(
            "is_click",
            F.when(F.col("event_type") == EventType.CLICK, F.lit(1)).otherwise(F.lit(0)),
        )
        df = df.withColumn(
            "is_view",
            F.when(F.col("event_type") == EventType.VIEW, F.lit(1)).otherwise(F.lit(0)),
        )
        df = df.withColumn(
            "is_action",
            F.when(F.col("event_type") == EventType.ACTION, F.lit(1)).otherwise(F.lit(0)),
        )

        # Aggregate by experience
        exp_features = df.groupBy("experience_id").agg(
            # Count metrics
            F.sum("is_impression").alias("exp_impressions_7d"),
            F.sum("is_click").alias("exp_clicks_7d"),
            F.sum("is_view").alias("exp_views_7d"),
            F.sum("is_action").alias("exp_actions_7d"),
            # Unique users
            F.countDistinct("user_id").alias("exp_unique_viewers_7d"),
            F.countDistinct(F.when(F.col("is_click") == 1, F.col("user_id"))).alias(
                "exp_unique_clickers_7d"
            ),
            # Position analysis
            F.avg("position").alias("exp_avg_position_7d"),
            F.min("position").alias("exp_best_position_7d"),
        )

        # Compute derived rates
        exp_features = _compute_experience_rates(exp_features, config)

        # Add popularity tier
        exp_features = _compute_popularity_tier(exp_features)

        # Fill defaults
        exp_features = _fill_experience_defaults(exp_features)

        # Add event_timestamp
        exp_features = exp_features.withColumn(
            "event_timestamp",
            F.to_timestamp(F.lit(target_date.isoformat())),
        )

        # Add validity flag
        exp_features = exp_features.withColumn("is_valid", F.lit(True))

        output_count = exp_features.count()
        ctx.set_attribute("output_count", output_count)

        logger.info(
            "Experience engagement features aggregated",
            output_count=output_count,
        )

        return exp_features


def _compute_experience_rates(df: DataFrame, config: EngagementFeaturesConfig) -> DataFrame:
    """Compute CTR and booking rates for experiences."""
    # CTR
    df = df.withColumn(
        "exp_ctr_7d",
        F.col("exp_clicks_7d") / (F.col("exp_impressions_7d") + config.ctr_smoothing),
    )

    # Booking rate
    df = df.withColumn(
        "exp_booking_rate_7d",
        F.col("exp_actions_7d") / (F.col("exp_clicks_7d") + config.action_rate_smoothing),
    )

    # View rate (clicks that lead to views)
    df = df.withColumn(
        "exp_view_rate_7d",
        F.col("exp_views_7d") / (F.col("exp_clicks_7d") + config.action_rate_smoothing),
    )

    return df


def _compute_popularity_tier(df: DataFrame) -> DataFrame:
    """Compute popularity tier based on impressions."""
    # Use window to compute percentiles
    window_spec = Window.orderBy(F.col("exp_impressions_7d").desc())

    df = df.withColumn("_rank", F.row_number().over(window_spec))
    total = df.count()

    df = df.withColumn(
        "exp_popularity_tier",
        F.when(F.col("_rank") <= total * 0.1, F.lit("hot"))  # Top 10%
        .when(F.col("_rank") <= total * 0.3, F.lit("popular"))  # Top 30%
        .when(F.col("_rank") <= total * 0.7, F.lit("normal"))  # Middle
        .otherwise(F.lit("cold")),
    )

    df = df.drop("_rank")

    return df


def _fill_experience_defaults(df: DataFrame) -> DataFrame:
    """Fill null values with defaults for experience features."""
    defaults = {
        "exp_impressions_7d": 0,
        "exp_clicks_7d": 0,
        "exp_views_7d": 0,
        "exp_actions_7d": 0,
        "exp_unique_viewers_7d": 0,
        "exp_unique_clickers_7d": 0,
        "exp_avg_position_7d": 0.0,
        "exp_best_position_7d": 0,
        "exp_ctr_7d": 0.0,
        "exp_booking_rate_7d": 0.0,
        "exp_view_rate_7d": 0.0,
    }

    for col, default in defaults.items():
        if col in df.columns:
            df = df.withColumn(col, F.coalesce(F.col(col), F.lit(default)))

    return df
