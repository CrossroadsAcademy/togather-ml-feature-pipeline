"""
Real Feature Pipeline Job.

Enhanced batch job that computes real features from protobuf events
and generates training data for Two-Tower and Ranking models.

Usage:
    spark-submit real_feature_pipeline.py \
        --target-date 2024-12-29 \
        --generate-training-data

Environment Variables:
    MINIO_ENDPOINT_URL, MINIO_ACCESS_KEY, MINIO_SECRET_KEY
    REDIS_HOST, REDIS_PORT, REDIS_PASSWORD
"""

import argparse
import sys
from datetime import date, datetime, timedelta
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from src.batch.engagement_features import (
    aggregate_experience_engagement_features,
    aggregate_user_engagement_features,
)
from src.batch.experience_features import (
    extract_experience_features,
)
from src.batch.feast_writer import FeastFeatureWriter
from src.batch.minio_reader import MinIOReader
from src.batch.observability import JobStageContext, set_job_status
from src.batch.spark_config import SparkConfig, create_spark_session
from src.batch.training_data_ranking import (
    create_ranking_training_data,
)
from src.batch.training_data_two_tower import (
    create_two_tower_training_data,
)
from src.batch.user_profile_features import (
    extract_user_profile_features,
)
from src.utils.logger import get_logger

logger = get_logger(__name__)


# =============================================================================
# Data Readers
# =============================================================================


def read_user_profiles(spark: SparkSession, bucket: str = "user-profiles") -> DataFrame:
    """
    Read user profiles from MinIO.

    Assumes Flink writes UserProfileCreated events to this bucket.
    """
    with JobStageContext("read_user_profiles") as ctx:
        try:
            df = spark.read.parquet(f"s3a://{bucket}/")
            count = df.count()
            ctx.set_attribute("count", count)
            logger.info(f"Read {count} user profiles")
            return df
        except Exception as e:
            logger.warning(f"No user profiles found: {e}. Creating empty DataFrame.")
            # Return empty DataFrame with expected schema
            return spark.createDataFrame([], schema="id STRING, created_at LONG")


def read_experience_catalog(spark: SparkSession, bucket: str = "experience-catalog") -> DataFrame:
    """
    Read experience catalog from MinIO.

    Assumes Flink writes ExperienceCreated events to this bucket.
    """
    with JobStageContext("read_experience_catalog") as ctx:
        try:
            df = spark.read.parquet(f"s3a://{bucket}/")
            count = df.count()
            ctx.set_attribute("count", count)
            logger.info(f"Read {count} experiences")
            return df
        except Exception as e:
            logger.warning(f"No experience catalog found: {e}. Creating empty DataFrame.")
            return spark.createDataFrame([], schema="id STRING, name STRING")


def read_recommendation_feedback(
    reader: MinIOReader,
    start_date: date,
    end_date: date,
) -> DataFrame:
    """Read RecommendationFeedback events."""
    with JobStageContext("read_recommendation_feedback") as ctx:
        df = reader.read_events(
            start_date=start_date,
            end_date=end_date,
            event_types=["recommendation_feedback"],
        )
        count = df.count()
        ctx.set_attribute("count", count)
        logger.info(f"Read {count} feedback events")
        return df


def read_recommendation_served(
    reader: MinIOReader,
    start_date: date,
    end_date: date,
) -> DataFrame:
    """Read RecommendationServed events."""
    with JobStageContext("read_recommendation_served") as ctx:
        df = reader.read_events(
            start_date=start_date,
            end_date=end_date,
            event_types=["recommendation_served"],
        )
        count = df.count()
        ctx.set_attribute("count", count)
        logger.info(f"Read {count} served events")
        return df


# =============================================================================
# Main Pipeline
# =============================================================================


def run_real_feature_pipeline(
    target_date: date,
    lookback_days: int = 7,
    generate_training_data: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    """
    Run the real feature pipeline.

    Args:
        target_date: Target date for features
        lookback_days: Days of history for engagement features
        generate_training_data: Also generate training data
        dry_run: Don't write to stores

    Returns:
        Job result summary
    """
    import time

    job_start = time.time()
    set_job_status("real_feature_pipeline", 1)

    result = {
        "status": "success",
        "target_date": target_date.isoformat(),
        "features": {},
        "training_data": {},
        "duration_seconds": 0,
    }

    start_date = target_date - timedelta(days=lookback_days)

    try:
        # Initialize Spark
        with JobStageContext("initialize_spark"):
            spark = create_spark_session(SparkConfig())

        reader = MinIOReader(spark)
        writer = FeastFeatureWriter()

        # ===== PHASE 1: Entity Features =====
        logger.info("Phase 1: Extracting entity features")

        # User profile features
        user_profiles = read_user_profiles(spark)
        if user_profiles.count() > 0:
            user_profile_features = extract_user_profile_features(user_profiles, target_date)
            result["features"]["user_profile"] = user_profile_features.count()
        else:
            user_profile_features = None
            logger.warning("Skipping user profile features - no data")

        # Experience features
        experiences = read_experience_catalog(spark)
        if experiences.count() > 0:
            experience_features = extract_experience_features(experiences, target_date)
            result["features"]["experience_profile"] = experience_features.count()
        else:
            experience_features = None
            logger.warning("Skipping experience features - no data")

        # ===== PHASE 2: Engagement Features =====
        logger.info("Phase 2: Aggregating engagement features")

        feedback_df = read_recommendation_feedback(reader, start_date, target_date)

        if feedback_df.count() > 0:
            # User engagement
            user_engagement = aggregate_user_engagement_features(feedback_df, target_date)
            result["features"]["user_engagement"] = user_engagement.count()

            # Experience engagement
            exp_engagement = aggregate_experience_engagement_features(feedback_df, target_date)
            result["features"]["exp_engagement"] = exp_engagement.count()
        else:
            user_engagement = None
            exp_engagement = None
            logger.warning("Skipping engagement features - no feedback data")

        # ===== PHASE 3: Merge Features =====
        logger.info("Phase 3: Merging feature tables")

        # Merge user features
        if user_profile_features and user_engagement:
            user_features = user_profile_features.join(
                user_engagement.drop("event_timestamp", "is_valid"),
                on="user_id",
                how="left",
            )
        elif user_profile_features:
            user_features = user_profile_features
        elif user_engagement:
            user_features = user_engagement
        else:
            user_features = None

        # Merge experience features
        if experience_features and exp_engagement:
            exp_features = experience_features.join(
                exp_engagement.drop("event_timestamp", "is_valid"),
                on="experience_id",
                how="left",
            )
        elif experience_features:
            exp_features = experience_features
        elif exp_engagement:
            exp_features = exp_engagement
        else:
            exp_features = None

        # ===== PHASE 4: Write to Stores =====
        if not dry_run:
            logger.info("Phase 4: Writing to feature stores")

            if user_features:
                with JobStageContext("write_user_features"):
                    writer.write_offline_features(
                        df=user_features,
                        table_name="user_features",
                        entity_col="user_id",
                        target_date=target_date,
                        mode="overwrite",
                    )
                    writer.write_online_features(
                        df=user_features,
                        entity_col="user_id",
                        feature_prefix="user",
                    )

            if exp_features:
                with JobStageContext("write_experience_features"):
                    writer.write_offline_features(
                        df=exp_features,
                        table_name="experience_features",
                        entity_col="experience_id",
                        target_date=target_date,
                        mode="overwrite",
                    )
                    writer.write_online_features(
                        df=exp_features,
                        entity_col="experience_id",
                        feature_prefix="experience",
                    )

        # ===== PHASE 5: Training Data Generation =====
        if generate_training_data and user_features and exp_features:
            logger.info("Phase 5: Generating training data")

            served_df = read_recommendation_served(reader, start_date, target_date)

            if served_df.count() > 0 and feedback_df.count() > 0:
                # Two-Tower training data
                with JobStageContext("generate_two_tower_data"):
                    two_tower_data = create_two_tower_training_data(
                        feedback_df=feedback_df,
                        user_features_df=user_features,
                        experience_features_df=exp_features,
                        target_date=target_date,
                    )
                    result["training_data"]["two_tower"] = two_tower_data.count()

                    if not dry_run:
                        two_tower_data.write.mode("overwrite").parquet(
                            f"s3a://training-data/two-tower/date={target_date.isoformat()}"
                        )

                # Ranking training data
                with JobStageContext("generate_ranking_data"):
                    ranking_data = create_ranking_training_data(
                        served_df=served_df,
                        feedback_df=feedback_df,
                        user_features_df=user_features,
                        experience_features_df=exp_features,
                        target_date=target_date,
                    )
                    result["training_data"]["ranking"] = ranking_data.count()

                    if not dry_run:
                        ranking_data.write.mode("overwrite").parquet(
                            f"s3a://training-data/ranking/date={target_date.isoformat()}"
                        )
            else:
                logger.warning("Skipping training data - no served/feedback events")

        spark.stop()

    except Exception as e:
        result["status"] = "failed"
        result["error"] = str(e)
        logger.error(f"Pipeline failed: {e}")
        set_job_status("real_feature_pipeline", -1)
        raise

    finally:
        duration = time.time() - job_start
        result["duration_seconds"] = round(duration, 2)
        set_job_status("real_feature_pipeline", 0)

    logger.info(
        "Real feature pipeline completed",
        status=result["status"],
        duration=result["duration_seconds"],
        features=result["features"],
        training_data=result["training_data"],
    )

    return result


# =============================================================================
# CLI
# =============================================================================


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Real Feature Pipeline - Extract features from protobuf events"
    )

    parser.add_argument(
        "--target-date",
        type=str,
        help="Target date (YYYY-MM-DD). Defaults to yesterday.",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=7,
        help="Days of history for engagement features",
    )
    parser.add_argument(
        "--generate-training-data",
        action="store_true",
        help="Also generate training data for Two-Tower and Ranking models",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute features but don't write to stores",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.target_date:
        target_date = datetime.strptime(args.target_date, "%Y-%m-%d").date()
    else:
        target_date = date.today() - timedelta(days=1)

    try:
        result = run_real_feature_pipeline(
            target_date=target_date,
            lookback_days=args.lookback_days,
            generate_training_data=args.generate_training_data,
            dry_run=args.dry_run,
        )

        if result["status"] == "failed":
            sys.exit(1)

    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
