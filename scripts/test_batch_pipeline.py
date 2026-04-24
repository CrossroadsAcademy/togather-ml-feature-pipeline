#!/usr/bin/env python3
"""
Test Batch Feature Pipeline Locally.

Runs the batch feature pipeline against local MinIO data.

Usage:
    # From feature-pipeline directory
    poetry run python scripts/test_batch_pipeline.py

    # With specific date
    poetry run python scripts/test_batch_pipeline.py --target-date 2024-12-15
"""

import argparse
import os
import sys
from datetime import date, timedelta
from pathlib import Path

# Load .env file
from dotenv import load_dotenv

env_path = Path(__file__).parent.parent / ".env"
load_dotenv(env_path)
print(f"Loaded .env from: {env_path}")


def get_required_env(key: str) -> str:
    """Get required environment variable or exit."""
    value = os.getenv(key)
    if not value:
        print(f"Missing required environment variable: {key}")
        sys.exit(1)
    return value


def main():
    parser = argparse.ArgumentParser(description="Test Batch Feature Pipeline")
    parser.add_argument(
        "--target-date",
        type=str,
        default=None,
        help="Target date (YYYY-MM-DD). Defaults to yesterday.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute features without writing to stores",
    )
    parser.add_argument(
        "--show-sample",
        action="store_true",
        default=True,
        help="Show sample output",
    )
    args = parser.parse_args()

    # Parse date
    if args.target_date:
        from datetime import datetime

        target = datetime.strptime(args.target_date, "%Y-%m-%d").date()
    else:
        target = date.today() - timedelta(days=1)

    start_date = target - timedelta(days=7)
    end_date = target

    print("=" * 60)
    print("Batch Feature Pipeline - Local Test")
    print("=" * 60)
    print(f"Target Date: {target}")
    print(f"Date Range: {start_date} to {end_date}")
    print(f"Dry Run: {args.dry_run}")
    print()

    # Import batch modules (after env is loaded)
    try:
        from src.batch.data_quality import DataQualityValidator
        from src.batch.feature_aggregations import compute_all_features
        from src.batch.minio_reader import MinIOReader
        from src.batch.spark_config import SparkConfig, create_spark_session
    except ImportError as e:
        print(f"Import error: {e}")
        print("Make sure you're running from the feature-pipeline directory with poetry")
        sys.exit(1)

    # Step 1: Create Spark session
    print("\n" + "-" * 40)
    print("Step 1: Initialize Spark")
    print("-" * 40)

    config = SparkConfig(
        app_name="batch-feature-test",
        master="local[*]",
        s3_endpoint=get_required_env("MINIO_ENDPOINT_URL"),
        s3_access_key=get_required_env("MINIO_ACCESS_KEY"),
        s3_secret_key=get_required_env("MINIO_SECRET_KEY"),
        enable_event_log=False,  # Disable for local testing
    )

    spark = create_spark_session(config)
    print(f"Spark version: {spark.version}")
    print(f"App ID: {spark.sparkContext.applicationId}")

    # Step 2: Read events from MinIO
    print("\n" + "-" * 40)
    print("Step 2: Read Events from MinIO")
    print("-" * 40)

    reader = MinIOReader(spark, bucket="raw-events")

    try:
        events_df = reader.read_events(
            start_date=start_date,
            end_date=end_date,
            event_types=["experience", "engagement", "location_streams"],
        )
        event_count = events_df.count()
        print(f"Events read: {event_count}")

        if event_count == 0:
            print("\nNo events found in MinIO. Run test_feature_pipeline.py first.")
            print("Checking available partitions...")
            partitions = reader.list_partitions()
            if partitions:
                print(f"Found partitions: {partitions[:5]}")
            spark.stop()
            return

    except Exception as e:
        print(f"Error reading from MinIO: {e}")
        print("\nMake sure MinIO is running and has data.")
        spark.stop()
        return

    # Step 3: Show sample events
    print("\n" + "-" * 40)
    print("Step 3: Sample Events")
    print("-" * 40)

    print("\nSchema:")
    events_df.printSchema()

    print("\nSample records:")
    events_df.select("user_id", "event_type", "experience_id", "event_timestamp").show(
        5, truncate=False
    )

    # Step 4: Validate input
    print("\n" + "-" * 40)
    print("Step 4: Validate Input")
    print("-" * 40)

    validator = DataQualityValidator()
    valid_df, report = validator.validate_input(
        events_df,
        entity_type="events",
        required_columns=["user_id", "event_type"],
    )
    print(f"Input validation: {'PASSED' if report.is_valid else 'FAILED'}")
    print(f"Valid records: {valid_df.count()}")
    if report.total_failures > 0:
        print(f"Failures: {report.total_failures}")

    # Step 5: Compute features
    print("\n" + "-" * 40)
    print("Step 5: Compute Features")
    print("-" * 40)

    features = compute_all_features(valid_df, target)

    for name, df in features.items():
        count = df.count()
        print(f"\n{name}: {count} records")
        if args.show_sample and count > 0:
            df.show(3, truncate=False)

    # Step 6: Validate output
    print("\n" + "-" * 40)
    print("Step 6: Validate Output")
    print("-" * 40)

    for name, df in features.items():
        entity_col = name.replace("_features", "_id")
        validated_df, out_report = validator.validate_output(df, name, entity_col)
        status = "PASSED" if out_report.is_valid else "FAILED"
        print(f"{name}: {status}")

    # Step 7: Summary
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"Events processed: {event_count}")
    for name, df in features.items():
        print(f"  {name}: {df.count()} records")

    if args.dry_run:
        print("\nDry run complete - no data written to stores")
    else:
        print("\nTo write to stores, implement writer integration or remove --dry-run")

    # Cleanup
    spark.stop()
    print("\nTest completed successfully!")


if __name__ == "__main__":
    main()
