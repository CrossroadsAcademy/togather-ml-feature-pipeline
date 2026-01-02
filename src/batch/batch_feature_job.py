"""
Batch Feature Job - Main Entry Point.

Spark job for computing batch features from raw events.

Usage:
    spark-submit batch_feature_job.py \
        --start-date 2024-12-01 \
        --end-date 2024-12-15 \
        --mode incremental

Environment Variables:
    MINIO_ENDPOINT_URL, MINIO_ACCESS_KEY, MINIO_SECRET_KEY
    REDIS_HOST, REDIS_PORT, REDIS_PASSWORD
    OTEL_EXPORTER_OTLP_ENDPOINT
"""

import argparse
import sys
from datetime import date, datetime, timedelta
from typing import Any

from src.batch.data_quality import DataQualityValidator
from src.batch.feast_writer import FeastFeatureWriter
from src.batch.feature_aggregations import compute_all_features
from src.batch.minio_reader import MinIOReader
from src.batch.observability import JobStageContext, observe_duration, set_job_status
from src.batch.spark_config import SparkConfig, create_spark_session
from src.utils.logger import get_logger

logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Batch Feature Pipeline - Compute features from raw events"
    )

    parser.add_argument(
        "--start-date",
        type=str,
        required=False,
        help="Start date (YYYY-MM-DD). Defaults to 7 days ago.",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        required=False,
        help="End date (YYYY-MM-DD). Defaults to yesterday.",
    )
    parser.add_argument(
        "--target-date",
        type=str,
        required=False,
        help="Single target date for features. Overrides start/end.",
    )
    parser.add_argument(
        "--mode",
        type=str,
        choices=["full", "incremental"],
        default="incremental",
        help="Processing mode: full (recompute all) or incremental (append)",
    )
    parser.add_argument(
        "--event-types",
        type=str,
        default="",  # Empty = read all event types for testing
        help="Comma-separated event types to process (empty = all)",
    )
    parser.add_argument(
        "--skip-validation",
        action="store_true",
        help="Skip data quality validation",
    )
    parser.add_argument(
        "--skip-online-write",
        action="store_true",
        help="Skip writing to Redis online store",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute features but don't write to stores",
    )

    return parser.parse_args()


def run_batch_job(
    start_date: date,
    end_date: date,
    event_types: list[str] | None,
    mode: str = "incremental",
    skip_validation: bool = False,
    skip_online_write: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    """
    Run the batch feature pipeline.

    Args:
        start_date: Start date for reading events
        end_date: End date for reading events
        event_types: List of event types to process
        mode: 'full' or 'incremental'
        skip_validation: Skip data quality checks
        skip_online_write: Skip Redis writes
        dry_run: Don't write to stores

    Returns:
        Job result summary
    """
    import time

    job_start = time.time()
    set_job_status("batch_feature_job", 1)

    logger.info(
        "Starting batch feature job",
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        event_types=event_types,
        mode=mode,
    )

    result = {
        "status": "success",
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "features_computed": {},
        "validation_reports": {},
        "duration_seconds": 0,
    }

    try:
        # Initialize Spark
        with JobStageContext("initialize_spark"):
            spark = create_spark_session(SparkConfig())

        # Initialize components
        reader = MinIOReader(spark)
        validator = DataQualityValidator()
        writer = FeastFeatureWriter()

        # Read raw events
        with JobStageContext("read_events") as ctx:
            events_df = reader.read_events(
                start_date=start_date,
                end_date=end_date,
                event_types=event_types,
            )
            event_count = events_df.count()
            ctx.set_attribute("event_count", event_count)
            logger.info(f"Read {event_count} events from MinIO")

        # Validate input
        if not skip_validation:
            events_df, input_report = validator.validate_input(
                df=events_df,
                entity_type="events",
                required_columns=["user_id", "event_type", "timestamp"],
            )
            result["validation_reports"]["input"] = input_report.to_dict()

            if not input_report.is_valid:
                logger.warning(
                    "Input validation had failures",
                    failures=input_report.total_failures,
                )

        # Compute features for target date (end_date)
        target_date = end_date
        features = compute_all_features(events_df, target_date)

        # Validate outputs
        if not skip_validation:
            for feature_name, df in features.items():
                entity_col = feature_name.replace("_features", "_id")
                validated_df, output_report = validator.validate_output(
                    df=df,
                    entity_type=feature_name,
                    entity_col=entity_col,
                )
                features[feature_name] = validated_df
                result["validation_reports"][feature_name] = output_report.to_dict()

        # Record feature counts
        for name, df in features.items():
            result["features_computed"][name] = df.count()

        # Write to stores
        if not dry_run:
            write_mode = "overwrite" if mode == "full" else "append"

            for feature_name, df in features.items():
                entity_col = feature_name.replace("_features", "_id")

                # Write to offline store (MinIO Parquet)
                with JobStageContext(f"write_offline_{feature_name}"):
                    writer.write_offline_features(
                        df=df,
                        table_name=feature_name,
                        entity_col=entity_col,
                        target_date=target_date,
                        mode=write_mode,
                    )

                # Write to online store (Redis)
                if not skip_online_write:
                    with JobStageContext(f"write_online_{feature_name}"):
                        prefix = feature_name.replace("_features", "")
                        writer.write_online_features(
                            df=df,
                            entity_col=entity_col,
                            feature_prefix=prefix,
                        )

        # Stop Spark
        spark.stop()

    except Exception as e:
        result["status"] = "failed"
        result["error"] = str(e)
        logger.error(f"Batch job failed: {e}")
        set_job_status("batch_feature_job", -1)
        raise

    finally:
        duration = time.time() - job_start
        result["duration_seconds"] = round(duration, 2)
        observe_duration("total_job", duration)
        set_job_status("batch_feature_job", 0)

    logger.info(
        "Batch feature job completed",
        status=result["status"],
        duration_seconds=result["duration_seconds"],
        features_computed=result["features_computed"],
    )

    return result


def main() -> None:
    """Main entry point."""
    args = parse_args()

    # Determine date range
    if args.target_date:
        target = datetime.strptime(args.target_date, "%Y-%m-%d").date()
        start_date = target - timedelta(days=7)
        end_date = target
    elif args.start_date and args.end_date:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
    else:
        # Default: last 7 days ending yesterday
        end_date = date.today() - timedelta(days=1)
        start_date = end_date - timedelta(days=7)

    # Parse event types (empty = all)
    event_types = [t.strip() for t in args.event_types.split(",") if t.strip()] or None

    try:
        result = run_batch_job(
            start_date=start_date,
            end_date=end_date,
            event_types=event_types,
            mode=args.mode,
            skip_validation=args.skip_validation,
            skip_online_write=args.skip_online_write,
            dry_run=args.dry_run,
        )

        if result["status"] == "failed":
            sys.exit(1)

    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
