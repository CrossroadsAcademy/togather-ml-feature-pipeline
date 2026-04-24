"""
Training Data Generator for Two-Tower Model.

Generates training samples for the retrieval model (Two-Tower architecture).
- Positives: User-experience pairs where user engaged (clicked, viewed, booked)
- Negatives: Random experiences user didn't interact with

Output schema:
    - user_id, experience_id, label (1.0 positive, 0.0 negative)
    - user_* features from user profile + engagement
    - exp_* features from experience profile + engagement
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
class TwoTowerTrainingConfig:
    """Configuration for Two-Tower training data generation."""

    # Negative sampling
    negatives_per_positive: int = 4  # 1:4 positive:negative ratio
    hard_negative_ratio: float = 0.5  # 50% of negatives are hard negatives

    # Positive engagement threshold
    # Click, View (dwell > 3s), or Action counts as positive
    min_dwell_time_ms: int = 3000

    # Output path
    output_bucket: str = "training-data"
    output_table: str = "two-tower"

    # Filtering
    min_user_impressions: int = 5  # Skip users with too few impressions
    min_experience_impressions: int = 10  # Skip cold experiences


# Positive Sample Generation


def generate_positive_samples(
    feedback_df: DataFrame,
    target_date: date,
    config: TwoTowerTrainingConfig | None = None,
) -> DataFrame:
    """
    Generate positive samples from user engagement events.

    A positive sample is created when a user:
    - Clicks on an experience
    - Views an experience for > 3 seconds
    - Takes an action (book)

    Args:
        feedback_df: RecommendationFeedback events
        target_date: Target date for training data
        config: Optional configuration

    Returns:
        DataFrame with (user_id, experience_id, label=1.0, engagement_type)
    """
    config = config or TwoTowerTrainingConfig()

    with JobStageContext("generate_positive_samples") as ctx:
        # Filter to engagement events
        positives = feedback_df.filter(
            # Clicks
            (F.col("event_type") == EventType.CLICK)
            # Views with sufficient dwell time
            | (
                (F.col("event_type") == EventType.VIEW)
                & (F.col("view_data_dwell_time_ms") >= config.min_dwell_time_ms)
            )
            # Actions (bookings)
            | (F.col("event_type") == EventType.ACTION)
        )

        # Deduplicate: one positive per (user, experience) pair
        # Keep the strongest engagement signal
        positives = positives.withColumn(
            "engagement_strength",
            F.when(F.col("event_type") == EventType.ACTION, F.lit(3))
            .when(F.col("event_type") == EventType.VIEW, F.lit(2))
            .otherwise(F.lit(1)),  # CLICK
        )

        # Window to get best engagement
        from pyspark.sql import Window

        window_spec = Window.partitionBy("user_id", "experience_id").orderBy(
            F.col("engagement_strength").desc()
        )

        positives = positives.withColumn(
            "_rank",
            F.row_number().over(window_spec),
        ).filter(F.col("_rank") == 1)

        # Create positive samples
        positives = positives.select(
            F.col("user_id"),
            F.col("experience_id"),
            F.lit(1.0).alias("label"),
            F.col("event_type").alias("engagement_type"),
            F.col("request_id"),
            F.col("timestamp").alias("interaction_timestamp"),
        )

        positive_count = positives.count()
        ctx.set_attribute("positive_count", positive_count)

        logger.info("Generated positive samples", count=positive_count)

        return positives


# Negative Sample Generation


def generate_negative_samples(
    positive_df: DataFrame,
    all_experiences: DataFrame,
    config: TwoTowerTrainingConfig | None = None,
) -> DataFrame:
    """
    Generate negative samples using random and hard negative sampling.

    Strategies:
    1. Random negatives: Random experiences user didn't interact with
    2. Hard negatives: Experiences shown but not clicked (impressions only)

    Args:
        positive_df: Positive samples (user_id, experience_id pairs)
        all_experiences: All valid experience IDs
        config: Optional configuration

    Returns:
        DataFrame with (user_id, experience_id, label=0.0, negative_type)
    """
    config = config or TwoTowerTrainingConfig()

    with JobStageContext("generate_negative_samples") as ctx:
        # Get distinct users
        users = positive_df.select("user_id").distinct()

        # Get experience pool
        experience_pool = all_experiences.select(
            F.col("experience_id"),
        ).distinct()

        # Cross join users x experiences (expensive with large datasets!)
        # TODO: use sampling or approximate methods
        user_exp_cross = users.crossJoin(
            experience_pool.sample(fraction=0.1)  # Sample 10% for efficiency
        )

        # Remove positive pairs
        positive_pairs = positive_df.select("user_id", "experience_id").distinct()
        negatives = user_exp_cross.join(
            positive_pairs,
            on=["user_id", "experience_id"],
            how="left_anti",  # Keep only non-matching pairs
        )

        # Sample negatives per positive count
        positive_count = positive_df.count()
        target_negatives = positive_count * config.negatives_per_positive

        negatives = negatives.limit(target_negatives)

        # Add negative label
        negatives = negatives.select(
            F.col("user_id"),
            F.col("experience_id"),
            F.lit(0.0).alias("label"),
            F.lit("random").alias("negative_type"),
        )

        negative_count = negatives.count()
        ctx.set_attribute("negative_count", negative_count)

        logger.info("Generated negative samples", count=negative_count)

        return negatives


# Feature Joining


def create_two_tower_training_data(
    feedback_df: DataFrame,
    user_features_df: DataFrame,
    experience_features_df: DataFrame,
    target_date: date,
    config: TwoTowerTrainingConfig | None = None,
) -> DataFrame:
    """
    Create complete training dataset for Two-Tower model.

    Joins positive/negative samples with user and experience features.

    Args:
        feedback_df: RecommendationFeedback events
        user_features_df: User profile + engagement features
        experience_features_df: Experience profile + engagement features
        target_date: Target date
        config: Optional configuration

    Returns:
        Complete training DataFrame
    """
    config = config or TwoTowerTrainingConfig()

    with JobStageContext("create_two_tower_training_data") as ctx:
        # Normalize: in RecommendationFeedback, event_id IS the experience_id
        # (the ID of the experience being recommended/interacted with)
        if "experience_id" not in feedback_df.columns and "event_id" in feedback_df.columns:
            feedback_df = feedback_df.withColumn("experience_id", F.col("event_id"))
            logger.info("Normalized event_id -> experience_id for feedback events")

        # Generate positives
        positives = generate_positive_samples(feedback_df, target_date, config)

        # Generate negatives
        negatives = generate_negative_samples(positives, experience_features_df, config)

        # Combine positives and negatives
        samples = positives.select("user_id", "experience_id", "label").unionByName(
            negatives.select("user_id", "experience_id", "label")
        )

        # Join user features
        samples = samples.join(
            user_features_df.drop("event_timestamp", "is_valid"),
            on="user_id",
            how="left",
        )

        # Join experience features
        samples = samples.join(
            experience_features_df.drop("event_timestamp", "is_valid"),
            on="experience_id",
            how="left",
        )

        # Add metadata
        samples = samples.withColumn(
            "training_date",
            F.lit(target_date.isoformat()),
        )

        # Add cross-features (user-experience interactions)
        samples = _add_cross_features(samples)

        # Record metrics
        total_count = samples.count()
        positive_count = samples.filter(F.col("label") == 1.0).count()

        ctx.set_attribute("total_samples", total_count)
        ctx.set_attribute("positive_samples", positive_count)
        ctx.set_attribute("negative_samples", total_count - positive_count)

        record_metric(
            "training_data_samples",
            total_count,
            {"model": "two_tower", "label": "total"},
        )
        record_metric(
            "training_data_samples",
            positive_count,
            {"model": "two_tower", "label": "positive"},
        )

        logger.info(
            "Two-Tower training data created",
            total=total_count,
            positives=positive_count,
            negatives=total_count - positive_count,
        )

        return samples


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
    else:
        # Add placeholder column when location data unavailable
        df = df.withColumn("cross_distance_km", F.lit(None).cast("double"))
        logger.info("Skipping distance feature: location columns not available")

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
    """
    Calculate Haversine distance between two points.

    Returns distance in kilometers.
    """
    R = 6371  # Earth radius in km

    lat1_rad = F.radians(lat1)
    lat2_rad = F.radians(lat2)
    dlat = F.radians(lat2 - lat1)
    dlng = F.radians(lng2 - lng1)

    a = F.pow(F.sin(dlat / 2), 2) + F.cos(lat1_rad) * F.cos(lat2_rad) * F.pow(F.sin(dlng / 2), 2)

    return F.lit(R) * F.lit(2) * F.asin(F.sqrt(a))
