"""
Feast Feature Writer for Batch Pipeline.

Writes aggregated features to:
- Offline store (MinIO Parquet) for Feast training dataset generation
- Online store (Redis) for inference

Handles incremental updates with upsert logic.
"""

import os
from dataclasses import dataclass
from datetime import date

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from src.batch.observability import JobStageContext, record_metric
from src.utils.config import settings
from src.utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class FeastWriterConfig:
    """Configuration for Feast feature writer."""

    # MinIO (offline store for Feast) - credentials from env vars
    minio_endpoint: str = "minio.platform.svc.cluster.local:9000"
    minio_access_key: str | None = None
    minio_secret_key: str | None = None
    offline_store_bucket: str = "feast-offline-store"

    # Redis (online store)
    redis_host: str = "redis.platform.svc.cluster.local"
    redis_port: int = 6379
    redis_password: str | None = None

    @classmethod
    def from_env(cls) -> "FeastWriterConfig":
        """Create config from environment variables."""
        # Get Redis config with K8s fallback
        redis_host = os.getenv("REDIS_HOST", settings.redis.host)
        redis_password = os.getenv("REDIS_PASSWORD", settings.redis.password)

        # Detect K8s environment and switch from localhost to internal service DNS
        is_k8s = os.getenv("KUBERNETES_SERVICE_HOST") is not None
        if is_k8s and redis_host == "localhost":
            redis_host = "redis.platform.svc.cluster.local"
            logger.info("Detected K8s environment. Switching Redis host to internal service DNS.")

        return cls(
            minio_endpoint=os.getenv("MINIO_ENDPOINT_URL", settings.minio.endpoint_url),
            minio_access_key=os.getenv("MINIO_ACCESS_KEY", settings.minio.access_key_id),
            minio_secret_key=os.getenv("MINIO_SECRET_KEY", settings.minio.secret_access_key),
            offline_store_bucket=os.getenv("FEAST_OFFLINE_BUCKET", cls.offline_store_bucket),
            redis_host=redis_host,
            redis_port=int(os.getenv("REDIS_PORT", str(settings.redis.port))),
            redis_password=redis_password,
        )

    @property
    def offline_store_base_path(self) -> str:
        """S3A path for offline store."""
        return f"s3a://{self.offline_store_bucket}"


class FeastFeatureWriter:
    """
    Writes features to Feast offline and online stores.

    Offline: MinIO Parquet (for get_historical_features / training datasets)
    Online: Redis (for get_online_features / inference)
    """

    def __init__(self, config: FeastWriterConfig | None = None):
        """
        Initialize feature writer.

        Args:
            config: Optional config, uses environment if not provided
        """
        self.config = config or FeastWriterConfig.from_env()
        self.logger = get_logger(self.__class__.__name__)

    def write_offline_features(
        self,
        df: DataFrame,
        table_name: str,
        entity_col: str,
        target_date: date | None = None,
        mode: str = "overwrite",
    ) -> int:
        """
        Write features to MinIO offline store for Feast.

        Features are written as Parquet files with Hive-style partitioning
        by event_timestamp so Feast can efficiently query by time range.

        Output path structure:
            s3a://feast-offline-store/{table_name}/event_timestamp=YYYY-MM-DD/*.parquet

        Args:
            df: Features DataFrame
            table_name: Feature table name (e.g., user_features)
            entity_col: Entity column name
            target_date: Date for partitioning (uses event_timestamp if None)
            mode: Write mode (overwrite, append)

        Returns:
            Number of records written
        """
        with JobStageContext(f"write_offline_{table_name}") as ctx:
            # Prepare DataFrame for Parquet
            df = self._prepare_for_parquet(df)

            record_count = df.count()
            ctx.set_attribute("record_count", record_count)

            try:
                # Build output path with date partitioning (industry standard)
                base_path = f"{self.config.offline_store_base_path}/{table_name}"

                if target_date:
                    # Write to specific date partition
                    path = f"{base_path}/date={target_date.isoformat()}"
                    df.write.mode(mode).parquet(path)
                else:
                    # Partition by date extracted from event_timestamp
                    df = df.withColumn("date", F.to_date(F.col("event_timestamp")))
                    df.write.mode(mode).partitionBy("date").parquet(base_path)

                record_metric(
                    "batch_features_written_total",
                    record_count,
                    {"entity_type": entity_col, "store": "minio"},
                )

                self.logger.info(
                    "Features written to MinIO offline store",
                    table=table_name,
                    path=base_path,
                    record_count=record_count,
                )

            except Exception as e:
                self.logger.error(f"Failed to write to MinIO: {e}")
                raise

            return record_count

    def write_online_features(
        self,
        df: DataFrame,
        entity_col: str,
        feature_prefix: str,
        ttl_seconds: int = 86400 * 7,  # 7 days
    ) -> int:
        """
        Write features to Redis online store.

        Key format: feast:{feature_prefix}:{entity_id}
        Value: Hash with feature name -> value

        Args:
            df: Features DataFrame (should be latest per entity)
            entity_col: Entity column name
            feature_prefix: Prefix for Redis keys
            ttl_seconds: TTL for Redis keys

        Returns:
            Number of records written
        """
        with JobStageContext(f"write_online_{feature_prefix}") as ctx:
            import redis

            # Get only latest features per entity
            window = F.row_number().over(
                Window.partitionBy(entity_col).orderBy(F.col("event_timestamp").desc())
            )
            latest_df = df.withColumn("_rn", window).filter(F.col("_rn") == 1).drop("_rn")

            # Get feature columns (exclude entity and timestamp)
            feature_cols = [
                c
                for c in latest_df.columns
                if c not in [entity_col, "event_timestamp", "_partition_date"]
            ]

            # Collect to driver (for Redis writes)
            records = latest_df.select(entity_col, *feature_cols).collect()

            # Connect to Redis
            redis_client = redis.Redis(
                host=self.config.redis_host,
                port=self.config.redis_port,
                password=self.config.redis_password,
                decode_responses=True,
            )

            # Write each record
            pipeline = redis_client.pipeline()
            for row in records:
                entity_id = row[entity_col]
                key = f"feast:{feature_prefix}:{entity_id}"

                # Build feature hash
                features = {col: str(row[col]) for col in feature_cols if row[col] is not None}

                pipeline.hset(key, mapping=features)
                pipeline.expire(key, ttl_seconds)

            pipeline.execute()
            redis_client.close()

            record_count = len(records)
            ctx.set_attribute("record_count", record_count)

            record_metric(
                "batch_features_written_total",
                record_count,
                {"entity_type": entity_col, "store": "redis"},
            )

            self.logger.info(
                "Features written to Redis",
                feature_prefix=feature_prefix,
                record_count=record_count,
            )

            return record_count

    def write_all_features(
        self,
        features: dict[str, DataFrame],
        target_date: date,
    ) -> dict[str, dict[str, int]]:
        """
        Write all feature DataFrames to both stores.

        Args:
            features: Dict with keys like 'user_features', 'experience_features'
            target_date: Target date for features

        Returns:
            Dict with write counts per entity type
        """
        results = {}

        entity_configs = {
            "user_features": ("user_id", "user"),
            "experience_features": ("experience_id", "experience"),
            "session_features": ("session_id", "session"),
        }

        for feature_name, df in features.items():
            if feature_name not in entity_configs:
                self.logger.warning(f"Unknown feature type: {feature_name}")
                continue

            entity_col, prefix = entity_configs[feature_name]

            # Write to offline store (MinIO Parquet)
            offline_count = self.write_offline_features(
                df=df,
                table_name=feature_name,
                entity_col=entity_col,
                target_date=target_date,
            )

            # Write to online store (Redis)
            online_count = self.write_online_features(
                df=df,
                entity_col=entity_col,
                feature_prefix=prefix,
            )

            results[feature_name] = {
                "offline": offline_count,
                "online": online_count,
            }

        return results

    def _prepare_for_parquet(self, df: DataFrame) -> DataFrame:
        """Prepare DataFrame for Parquet write."""
        # Ensure timestamp is proper timestamp type
        if "event_timestamp" in df.columns:
            df = df.withColumn(
                "event_timestamp",
                F.to_date(F.col("event_timestamp")),  # Use date for partitioning
            )

        # Convert array columns to string (for compatibility)
        for col_name, col_type in df.dtypes:
            if "array" in col_type.lower():
                df = df.withColumn(
                    col_name,
                    F.concat_ws(",", F.col(col_name)),
                )

        return df
