"""
Unified Storage Sink for S3-compatible storage (MinIO/AWS S3).

Writes Parquet files with Hive-style partitioning for Spark/Flink compatibility.

Features:
- Works with both MinIO (local) and AWS S3 (production)
- Parquet format with configurable compression
- Hive-style partitioning (event_type=X/year=Y/month=M/day=D/hour=H)
- Batch writing with buffering
- Prometheus metrics
- Retry logic for reliability
"""

from datetime import datetime, timezone
from io import BytesIO
from typing import Any, Literal

import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio
from minio.error import S3Error
from prometheus_client import Counter, Histogram
from pydantic import BaseModel, Field
from tenacity import retry, stop_after_attempt, wait_exponential

from src.utils.config import settings
from src.utils.logger import get_logger

logger = get_logger(__name__)

# Prometheus Metrics

storage_writes_total = Counter(
    "storage_writes_total",
    "Total files written to storage",
    ["bucket", "format", "status"],
)
storage_write_duration = Histogram(
    "storage_write_duration_seconds", "Storage write duration", ["format"]
)
storage_records_written = Counter(
    "storage_records_written_total", "Total records written to storage", ["format"]
)


# Configuration


class StorageSinkConfig(BaseModel):
    """Configuration for unified storage sink."""

    # Connection settings
    endpoint: str = Field(..., description="S3-compatible endpoint (MinIO or AWS S3)")
    access_key: str = Field(..., description="Access key")
    secret_key: str = Field(..., description="Secret key")
    secure: bool = Field(default=False, description="Use HTTPS")
    region: str = Field(default="us-east-1", description="AWS region (for S3)")

    # Storage settings
    bucket_name: str = Field(default="raw-events", description="Target bucket")
    compression: Literal["snappy", "gzip", "zstd", "none"] = Field(
        default="snappy", description="Parquet compression"
    )

    # Batching settings
    batch_size: int = Field(default=10000, description="Records per Parquet file")
    flush_interval_seconds: int = Field(default=60, description="Max time between flushes")


# Unified Storage Sink


class StorageSink:
    """
    Unified S3-compatible storage sink with Parquet + Hive partitioning.

    Works with both MinIO (local development) and AWS S3 (production).
    Uses Hive-style partitioning for Spark/Flink compatibility.

    Folder structure:
        s3://raw-events/
        ├── event_type=user_click/
        │   └── year=2024/month=12/day=15/hour=10/
        │       └── part-00000-uuid.snappy.parquet
        ├── event_type=db_change/
        │   └── table=users/
        │       └── year=2024/month=12/day=15/hour=10/
        │           └── part-00000-uuid.snappy.parquet

    Usage (recommended - credentials from settings/.env):
        from src.streaming import StorageSink

        sink = StorageSink()  # Uses credentials from .env/settings
        sink.write_batch(records, event_type="user_click")

    """

    def __init__(self, config: StorageSinkConfig | None = None):
        self.logger = get_logger(self.__class__.__name__)

        if config:
            self.config = config
        else:
            # Use settings from config
            self.config = StorageSinkConfig(
                endpoint=settings.minio.endpoint_url,
                access_key=settings.minio.access_key_id,
                secret_key=settings.minio.secret_access_key,
                secure=settings.minio.secure,
                bucket_name=getattr(settings.minio, "bucket_name", "raw-events"),
            )

        self._client: Minio | None = None
        self._buffer: dict[str, list[dict[str, Any]]] = {}  # Buffer per event type
        self._last_flush: dict[str, datetime] = {}

        self._initialize_client()

    def _initialize_client(self) -> None:
        """Initialize S3-compatible client."""
        try:
            self._client = Minio(
                self.config.endpoint,
                access_key=self.config.access_key,
                secret_key=self.config.secret_key,
                secure=self.config.secure,
                region=self.config.region,
            )

            # Ensure bucket exists
            if not self._client.bucket_exists(self.config.bucket_name):
                self._client.make_bucket(self.config.bucket_name)
                self.logger.info(f"Created bucket: {self.config.bucket_name}")

            self.logger.info(
                "Storage sink initialized",
                endpoint=self.config.endpoint,
                bucket=self.config.bucket_name,
            )

        except Exception as e:
            self.logger.error(f"Failed to initialize storage client: {e}")
            raise

    def is_healthy(self) -> tuple[bool, str]:
        """
        Check if MinIO/S3 connection is healthy. Used by health check server.

        Returns:
            Tuple of (is_healthy, message)
        """
        try:
            if not self._client:
                return False, "Client not initialized"

            # Check if bucket exists and is accessible
            if not self._client.bucket_exists(self.config.bucket_name):
                return False, f"Bucket '{self.config.bucket_name}' not found"

            # List objects to verify read access (limit to 1)
            objects = list(self._client.list_objects(self.config.bucket_name, max_keys=1))  # noqa: F841

            # Get buffer stats
            total_buffered = sum(len(buf) for buf in self._buffer.values())
            buffer_info = f", buffered: {total_buffered} records" if total_buffered > 0 else ""

            return True, f"Bucket '{self.config.bucket_name}' accessible{buffer_info}"

        except S3Error as e:
            return False, f"S3 error: {e.code} - {e.message}"
        except Exception as e:
            return False, str(e)

    def _get_partition_path(
        self,
        event_type: str,
        timestamp: datetime,
        table_name: str | None = None,
    ) -> str:
        """
        Generate Hive-style partition path.

        Args:
            event_type: Type of event (user_click, db_change, etc.)
            timestamp: Event timestamp

        Returns:
            Hive-style partition path (e.g., event_type=user_click/year=2024/month=12/day=15/hour=10)
        """
        parts = [f"event_type={event_type}"]

        # Add table partition for CDC events
        if table_name:
            parts.append(f"table={table_name}")

        # Time partitions
        parts.extend(
            [
                f"year={timestamp.year}",
                f"month={timestamp.month:02d}",
                f"day={timestamp.day:02d}",
                f"hour={timestamp.hour:02d}",
            ]
        )

        return "/".join(parts)

    def _generate_filename(self) -> str:
        """Generate unique Parquet filename."""
        import uuid

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        compression_suffix = (
            f".{self.config.compression}" if self.config.compression != "none" else ""
        )
        return f"part-{timestamp}-{unique_id}{compression_suffix}.parquet"

    def _convert_to_arrow_table(self, records: list[dict[str, Any]]) -> pa.Table:
        """Convert list of records to Arrow table.

        Handles nested structures:
        - Dicts are flattened with prefix (device_context.platform -> device_context_platform)
        - Lists/arrays are serialized as JSON strings (recommendations -> JSON array string)
        """
        import json

        if not records:
            return pa.table({})

        # Flatten nested dicts and serialize arrays for Parquet compatibility
        flat_records = []
        for record in records:
            flat = {}
            for key, value in record.items():
                if isinstance(value, dict):
                    # Flatten nested dict with prefix
                    for nested_key, nested_value in value.items():
                        flat[f"{key}_{nested_key}"] = nested_value
                elif isinstance(value, list):
                    # Serialize arrays as JSON strings to preserve them
                    flat[key] = json.dumps(value)
                else:
                    flat[key] = value
            flat_records.append(flat)

        # Build columns from flattened records
        all_keys: set[str] = set()
        for record in flat_records:
            all_keys.update(record.keys())

        columns = {key: [r.get(key) for r in flat_records] for key in all_keys}

        return pa.table(columns)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def write_batch(
        self,
        records: list[dict[str, Any]],
        event_type: str,
        table_name: str | None = None,
        timestamp: datetime | None = None,
    ) -> str | None:
        """
        Write batch of records to Parquet file in S3-compatible storage.

        Args:
            records: List of event records
            event_type: Event type for partitioning
            table_name: Optional table name for CDC events
            timestamp: Timestamp for partitioning (defaults to now)

        Returns:
            Object key if successful, None otherwise
        """
        if not records:
            self.logger.warning("No records to write")
            return None

        if not self._client:
            self.logger.error("Storage client not initialized")
            return None

        timestamp = timestamp or datetime.now(timezone.utc)

        try:
            import time

            start_time = time.time()

            # Generate partition path and filename
            partition_path = self._get_partition_path(event_type, timestamp, table_name)
            filename = self._generate_filename()
            object_key = f"{partition_path}/{filename}"

            # Convert to Arrow table
            table = self._convert_to_arrow_table(records)

            # Write to Parquet buffer
            buffer = BytesIO()
            compression = None if self.config.compression == "none" else self.config.compression
            pq.write_table(table, buffer, compression=compression)  # type: ignore
            buffer.seek(0)
            file_size = buffer.getbuffer().nbytes

            # Upload to storage
            self._client.put_object(
                self.config.bucket_name,
                object_key,
                buffer,
                file_size,
                content_type="application/octet-stream",
            )

            # Record metrics
            duration = time.time() - start_time
            storage_writes_total.labels(
                bucket=self.config.bucket_name, format="parquet", status="success"
            ).inc()
            storage_write_duration.labels(format="parquet").observe(duration)
            storage_records_written.labels(format="parquet").inc(len(records))

            self.logger.info(
                "Parquet file written",
                bucket=self.config.bucket_name,
                key=object_key,
                records=len(records),
                size_bytes=file_size,
                duration_ms=round(duration * 1000),
            )

            return object_key

        except S3Error as e:
            self.logger.error(f"Storage upload failed: {e}")
            storage_writes_total.labels(
                bucket=self.config.bucket_name, format="parquet", status="error"
            ).inc()
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error writing Parquet: {e}")
            storage_writes_total.labels(
                bucket=self.config.bucket_name, format="parquet", status="error"
            ).inc()
            raise

    def add_record(self, record: dict[str, Any], event_type: str) -> str | None:
        """
        Add record to buffer, flush when batch size or time threshold reached.

        Args:
            record: Event record
            event_type: Event type for partitioning

        Returns:
            Object key if flushed, None otherwise
        """
        if event_type not in self._buffer:
            self._buffer[event_type] = []
            self._last_flush[event_type] = datetime.now(timezone.utc)

        self._buffer[event_type].append(record)

        # Check if it should be flushed
        time_since_flush = (datetime.now(timezone.utc) - self._last_flush[event_type]).seconds
        should_flush = (
            len(self._buffer[event_type]) >= self.config.batch_size
            or time_since_flush >= self.config.flush_interval_seconds
        )

        if should_flush:
            return self.flush(event_type)

        return None

    def flush(self, event_type: str, table_name: str | None = None) -> str | None:
        """Flush buffer for specific event type to Parquet file."""
        if event_type not in self._buffer or not self._buffer[event_type]:
            return None

        records = self._buffer[event_type].copy()
        self._buffer[event_type].clear()
        self._last_flush[event_type] = datetime.now(timezone.utc)

        return self.write_batch(records, event_type, table_name)

    def flush_all(self) -> list[str]:
        """Flush all buffered events."""
        keys = []
        for event_type in list(self._buffer.keys()):
            key = self.flush(event_type)
            if key:
                keys.append(key)
        return keys

    def list_files(self, prefix: str | None = None) -> list[dict[str, Any]]:
        """
        List files in bucket.

        Args:
            prefix: Optional prefix to filter by

        Returns:
            List of file metadata
        """
        if not self._client:
            return []

        try:
            objects = self._client.list_objects(
                self.config.bucket_name,
                prefix=prefix,
                recursive=True,
            )

            return [
                {
                    "key": obj.object_name,
                    "size": obj.size,
                    "last_modified": obj.last_modified,
                }
                for obj in objects
            ]
        except Exception as e:
            self.logger.error(f"Failed to list files: {e}")
            return []


# Data Archiver


class DataArchiver:
    """
    Data archiver that batches and writes events to storage.

    Replaces the duplicate DataArchiver classes from minio_sink.py and s3_sink.py.
    """

    def __init__(self, sink: StorageSink, batch_size: int = 1000):
        self.sink = sink
        self.batch_size = batch_size
        self.logger = get_logger(self.__class__.__name__)

    def archive_messages(
        self,
        messages: list[dict[str, Any]],
        topic: str,
        event_type: str | None = None,
    ) -> list[str]:
        """
        Archive messages to storage.

        Args:
            messages: Messages to archive
            topic: Kafka topic name
            event_type: Event type for partitioning (defaults to topic name)

        Returns:
            List of uploaded object keys
        """
        if not messages:
            return []

        event_type = event_type or topic.replace(".", "_")
        keys = []

        # Process in batches
        for i in range(0, len(messages), self.batch_size):
            batch = messages[i : i + self.batch_size]

            # Extract event timestamp from first message for partitioning
            # Use event time (not processing time) so historical replays go to correct partitions
            event_timestamp = self._extract_event_timestamp(batch[0]) if batch else None

            key = self.sink.write_batch(batch, event_type, timestamp=event_timestamp)
            if key:
                keys.append(key)

        self.logger.info(
            f"Archived {len(messages)} messages to {len(keys)} files",
            topic=topic,
            event_type=event_type,
        )

        return keys

    def _extract_event_timestamp(self, message: dict[str, Any]) -> datetime | None:
        """Extract event timestamp from message for partitioning by event time."""
        from datetime import datetime, timezone

        # Try SDK envelope timestamp first (milliseconds)
        ts = message.get("_timestamp")
        if ts:
            try:
                return datetime.fromtimestamp(int(ts) / 1000, tz=timezone.utc)
            except (ValueError, TypeError, OSError):
                pass

        # Try common timestamp fields (camelCase from SDK)
        for field in ["clientTimestamp", "createdAt", "servedAt", "timestamp"]:
            ts = message.get(field)
            if ts:
                try:
                    # Timestamps are typically in milliseconds
                    return datetime.fromtimestamp(int(ts) / 1000, tz=timezone.utc)
                except (ValueError, TypeError, OSError):
                    pass

        # Try processed_at (ISO string from app-events-processor)
        processed_at = message.get("processed_at")
        if processed_at and isinstance(processed_at, str):
            try:
                return datetime.fromisoformat(processed_at.replace("Z", "+00:00"))
            except ValueError:
                pass

        # Fallback to now (shouldn't happen with proper events)
        return None


# Backward Compatibility Aliases

# Alias for ParquetSink
ParquetSink = StorageSink
ParquetSinkConfig = StorageSinkConfig


# Example Usage

if __name__ == "__main__":
    import os
    import sys

    def get_required_env(key: str) -> str:
        """Get required environment variable or exit with error."""
        value = os.getenv(key)
        if not value:
            print(f"Missing required environment variable: {key}")
            print(f"Set it in .env file or export {key}=<value>")
            sys.exit(1)
        return value

    # Get credentials from environment
    config = StorageSinkConfig(
        endpoint=get_required_env("MINIO_ENDPOINT_URL"),
        access_key=get_required_env("MINIO_ACCESS_KEY"),
        secret_key=get_required_env("MINIO_SECRET_KEY"),
        bucket_name=os.getenv("MINIO_BUCKET_NAME", "raw-events"),
    )

    sink = StorageSink(config)
