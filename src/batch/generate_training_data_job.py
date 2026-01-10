import argparse
from datetime import datetime, timedelta

import pyspark.sql.functions as F
import structlog
from pyspark.sql import SparkSession

from src.batch.minio_reader import MinIOReader
from src.batch.training_data_ranking import create_ranking_training_data
from src.batch.training_data_two_tower import create_two_tower_training_data

# Configure structured logging
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ],
    logger_factory=structlog.stdlib.LoggerFactory(),
)
logger = structlog.get_logger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Generate Training Data Batch Job")
    parser.add_argument("--target-date", type=str, required=True, help="Target date (YYYY-MM-DD)")
    parser.add_argument("--output-bucket", type=str, default="dvc-data", help="Output S3 bucket")
    parser.add_argument(
        "--feature-bucket", type=str, default="raw-events", help="Feature store bucket"
    )

    args = parser.parse_args()
    target_date = datetime.strptime(args.target_date, "%Y-%m-%d").date()

    logger.info("Starting training data generation job", target_date=str(target_date))

    # Initialize Spark
    spark = SparkSession.builder.appName("TrainingDataGeneration").getOrCreate()

    try:
        # 1. Read Raw Events (Feedback & Served)
        reader = MinIOReader(spark, args.feature_bucket)

        # Read window: target_date - 7 days to target_date
        start_date = target_date - timedelta(days=7)
        end_date = target_date

        logger.info("Reading events", start_date=str(start_date), end_date=str(end_date))

        # Read Feedback and Served events together
        events_df = reader.read_events(
            start_date=start_date,
            end_date=end_date,
            event_types=["recommendation_feedback_v1", "recommendation_served_v1"],
        )

        # Filter for specific event types
        # Note: Event types in the data might be normalized or just the partition value.
        # MinIOReader likely sets 'event_type' column.
        feedback_df = events_df.filter(F.col("event_type") == "recommendation_feedback_v1")
        served_df = events_df.filter(F.col("event_type") == "recommendation_served_v1")

        # Read Features (User & Experience) from where batch pipeline wrote them
        # Assuming batch pipeline writes to minio://feature-store/offline/...
        # For now, let's re-compute them or read from raw events if features aren't persisted offline
        # Industrial practice: Read from Feature Store (Offline).
        # Since we don't have a formal offline feature store reader yet, we'll fast-track by passing None
        # (the generator functions currently expect features, let's verify if they are mandatory)

        # Checking create_two_tower_training_data signature:
        # def create_two_tower_training_data(feedback_df, user_features_df, experience_features_df, ...)
        # It requires user_features_df and experience_features_df.

        feature_base_path = "s3a://feast-offline-store"

        # Try to read features from today's batch run
        # Path structure: {table_name}/date={date}
        try:
            user_features_path = f"{feature_base_path}/user_features/date={args.target_date}"
            experience_features_path = (
                f"{feature_base_path}/experience_features/date={args.target_date}"
            )

            user_features_df = spark.read.parquet(user_features_path)
            experience_features_df = spark.read.parquet(experience_features_path)
            logger.info("Loaded features from offline store", path=feature_base_path)
        except Exception as e:
            logger.warning("Could not load offline features, using empty DFs", error=str(e))

            user_features_df = None
            experience_features_df = None

        # 2. Generate Two-Tower Data

        # 2. Generate Two-Tower Data
        if user_features_df is None or experience_features_df is None:
            logger.error("Cannot generate training data without user and experience features")
            raise ValueError("user_features_df and experience_features_df are required")

        logger.info("Generating Two-Tower training data")
        two_tower_df = create_two_tower_training_data(
            feedback_df=feedback_df,
            user_features_df=user_features_df,
            experience_features_df=experience_features_df,
            target_date=target_date,
        )

        # Write to S3
        tt_output_path = f"s3a://{args.output_bucket}/training/two_tower/date={args.target_date}"
        two_tower_df.write.mode("overwrite").parquet(tt_output_path)
        logger.info("Wrote Two-Tower data", path=tt_output_path, count=two_tower_df.count())

        # 3. Generate Ranking Data
        logger.info("Generating Ranking training data")
        ranking_df = create_ranking_training_data(
            served_df=served_df,
            feedback_df=feedback_df,
            user_features_df=user_features_df,
            experience_features_df=experience_features_df,
            target_date=target_date,
        )

        # Write to S3
        rank_output_path = f"s3a://{args.output_bucket}/training/ranking/date={args.target_date}"
        ranking_df.write.mode("overwrite").parquet(rank_output_path)
        logger.info("Wrote Ranking data", path=rank_output_path, count=ranking_df.count())

    except Exception as e:
        logger.error("Training data generation failed", error=str(e))
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
