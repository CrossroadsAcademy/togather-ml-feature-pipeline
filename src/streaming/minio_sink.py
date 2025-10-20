"""MinIO S3-compatible sink for data persistence with enhanced features."""

import json
from datetime import datetime, timezone
from typing import Any

from minio import Minio
from minio.error import S3Error
from prometheus_client import Counter, Histogram
from tenacity import retry, stop_after_attempt, wait_exponential

from src.utils.config import settings
from src.utils.logger import get_logger

logger = get_logger(__name__)

# Prometheus Metrics
minio_operations = Counter(
    "minio_operations_total", "Total MinIO operations", ["operation", "status"]
)

minio_operation_duration = Histogram(
    "minio_operation_duration_seconds", "MinIO operation duration", ["operation"]
)

minio_file_size = Histogram("minio_file_size_bytes", "MinIO file size in bytes", ["bucket"])


class MinIOSink:
    """MinIO S3-compatible sink for persisting data with enhanced features."""

    def __init__(self, config: dict[str, Any] | None = None):
        if config is None:
            # Use settings configuration
            self.config = {
                "endpoint_url": settings.minio.endpoint_url,
                "access_key_id": settings.minio.access_key_id,
                "secret_access_key": settings.minio.secret_access_key,
                "bucket_name": settings.minio.bucket_name,
                "secure": settings.minio.secure,
                "region": settings.minio.region,
            }
        else:
            self.config = config
        self.logger = get_logger(self.__class__.__name__)

        # Initialize MinIO client
        self._minio_client: Minio | None = None
        self._initialize_client()

    def _initialize_client(self) -> None:
        """Initialize MinIO client with enhanced configuration."""
        try:
            endpoint = str(self.config.get("endpoint_url", "localhost:9000"))
            access_key = str(self.config.get("access_key_id", "minioadmin"))
            secret_key = str(self.config.get("secret_access_key", "minioadmin"))
            secure = bool(self.config.get("secure", False))
            region = str(self.config.get("region")) if self.config.get("region") else None

            self._minio_client = Minio(
                endpoint=endpoint,
                access_key=access_key,
                secret_key=secret_key,
                secure=secure,
                region=region,
                http_client=None,
                credentials=None,
            )

            self._test_connection_and_setup_bucket()
            self.logger.info("MinIO client initialized successfully")

        except Exception as e:
            self.logger.error(f"Failed to initialize MinIO client: {e}")
            raise

    def _test_connection_and_setup_bucket(self) -> None:
        """Test MinIO connection and setup bucket."""
        assert self._minio_client is not None

        try:
            bucket_name = str(self.config.get("bucket_name", "togather-ml-features"))

            # Check if bucket exists
            if not self._minio_client.bucket_exists(bucket_name):
                self.logger.info(f"Creating MinIO bucket: {bucket_name}")
                self._minio_client.make_bucket(bucket_name)
                self.logger.info(f"MinIO bucket '{bucket_name}' created successfully")
            else:
                self.logger.info(f"MinIO bucket '{bucket_name}' already exists")

            # Test connection by listing objects
            objects = list(self._minio_client.list_objects(bucket_name, recursive=False))  # type: ignore # noqa: F841
            self.logger.info(f"MinIO connection test successful for bucket: {bucket_name}")

        except S3Error as e:
            if e.code == "NoSuchBucket":
                self.logger.warning(f"Bucket not found: {bucket_name}")
            else:
                self.logger.error(f"MinIO connection test failed: {e}")
                raise
        except Exception as e:
            self.logger.error(f"MinIO connection test failed: {e}")
            raise

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def upload_file(
        self,
        data: list[dict[str, Any]],
        topic: str,
        partition: int | None = None,
        timestamp: datetime | None = None,
    ) -> str | None:
        """
        Upload data to MinIO.

        Args:
            data: List of messages to upload
            topic: Kafka topic name
            partition: Kafka partition (optional)
            timestamp: Message timestamp (optional)

        Returns:
            MinIO object key if successful, None otherwise
        """
        if not self._minio_client:
            self.logger.error("MinIO client not initialized")
            return None

        try:
            # Generate object key with better organization
            timestamp = timestamp or datetime.now(timezone.utc)
            date_path = timestamp.strftime("%Y/%m/%d/%H")

            if partition is not None:
                object_key = f"kafka-data/{topic}/partition={partition}/{date_path}/{timestamp.isoformat()}.json"
            else:
                object_key = f"kafka-data/{topic}/{date_path}/{timestamp.isoformat()}.json"

            # Prepare data for upload
            file_content = json.dumps(data, indent=2)
            file_size = len(file_content.encode("utf-8"))

            # Upload to MinIO using put_object
            bucket_name = str(self.config.get("bucket_name", "togather-ml-features"))

            from io import BytesIO

            data_stream = BytesIO(file_content.encode("utf-8"))

            self._minio_client.put_object(
                bucket_name, object_key, data_stream, file_size, content_type="application/json"
            )

            # Record metrics
            minio_operations.labels(operation="upload", status="success").inc()
            minio_file_size.labels(bucket=bucket_name).observe(file_size)

            self.logger.info(
                "File uploaded to MinIO successfully",
                bucket=bucket_name,
                key=object_key,
                size=file_size,
                record_count=len(data),
            )

            return object_key

        except S3Error as e:
            self.logger.error(f"MinIO upload failed: {e}")
            minio_operations.labels(operation="upload", status="error").inc()
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error during MinIO upload: {e}")
            minio_operations.labels(operation="upload", status="error").inc()
            raise

    def upload_batch(
        self, messages: list[dict[str, Any]], batch_size: int = 1000
    ) -> list[str | None]:
        """
        Upload messages in batches.

        Args:
            messages: List of messages to upload
            batch_size: Size of each batch

        Returns:
            List of MinIO object keys
        """
        uploaded_keys: list[str | None] = []

        # Group messages by topic and partition
        grouped_messages: dict[tuple[str, int | None], list[dict[str, Any]]] = {}
        for message in messages:
            topic = message.get("topic", "unknown")
            partition = message.get("partition")
            group_key = (topic, partition)

            if group_key not in grouped_messages:
                grouped_messages[group_key] = []
            grouped_messages[group_key].append(message)

        # Upload each group
        for (topic, partition), group_messages in grouped_messages.items():
            # Split into batches
            for i in range(0, len(group_messages), batch_size):
                batch = group_messages[i : i + batch_size]

                try:
                    object_key = self.upload_file(batch, topic, partition)
                    uploaded_keys.append(object_key)
                except Exception as e:
                    self.logger.error(f"Failed to upload batch: {e}")
                    uploaded_keys.append(None)

        return uploaded_keys

    def list_files(
        self, topic: str | None = None, prefix: str | None = None
    ) -> list[dict[str, Any]]:
        """
        List files in MinIO bucket.

        Args:
            topic: Filter by topic
            prefix: Filter by prefix

        Returns:
            List of file metadata
        """
        if not self._minio_client:
            self.logger.error("MinIO client not initialized")
            return []

        try:
            bucket_name = str(self.config.get("bucket_name", "togather-ml-features"))

            # Build prefix
            if topic:
                search_prefix = f"kafka-data/{topic}/"
            elif prefix:
                search_prefix = prefix
            else:
                search_prefix = "kafka-data/"

            objects = self._minio_client.list_objects(
                bucket_name, prefix=search_prefix, recursive=True
            )

            files = []
            for obj in objects:
                files.append(
                    {
                        "key": obj.object_name,
                        "size": obj.size,
                        "last_modified": obj.last_modified,
                        "etag": obj.etag,
                    }
                )

            minio_operations.labels(operation="list", status="success").inc()

            self.logger.debug(f"Listed {len(files)} files from MinIO")

            return files

        except S3Error as e:
            self.logger.error(f"MinIO list operation failed: {e}")
            minio_operations.labels(operation="list", status="error").inc()
            return []

    def download_file(self, object_key: str) -> str | None:
        """
        Download file from MinIO.

        Args:
            object_key: MinIO object key

        Returns:
            File content as string
        """
        if not self._minio_client:
            self.logger.error("MinIO client not initialized")
            return None

        try:
            bucket_name = str(self.config.get("bucket_name", "togather-ml-features"))

            response = self._minio_client.get_object(bucket_name, object_key)
            content = response.read().decode("utf-8")
            response.close()
            response.release_conn()

            minio_operations.labels(operation="download", status="success").inc()

            self.logger.debug(f"Downloaded file from MinIO: {object_key}")

            return content

        except S3Error as e:
            self.logger.error(f"MinIO download failed for {object_key}: {e}")
            minio_operations.labels(operation="download", status="error").inc()
            return None

    def delete_file(self, object_key: str) -> bool:
        """
        Delete file from MinIO.

        Args:
            object_key: MinIO object key

        Returns:
            True if successful, False otherwise
        """
        if not self._minio_client:
            self.logger.error("MinIO client not initialized")
            return False

        try:
            bucket_name = str(self.config.get("bucket_name", "togather-ml-features"))

            self._minio_client.remove_object(bucket_name, object_key)

            minio_operations.labels(operation="delete", status="success").inc()

            self.logger.info(f"Deleted file from MinIO: {object_key}")

            return True

        except S3Error as e:
            self.logger.error(f"MinIO delete failed for {object_key}: {e}")
            minio_operations.labels(operation="delete", status="error").inc()
            return False

    def get_bucket_info(self) -> dict[str, Any]:
        """Get bucket information and statistics."""
        if not self._minio_client:
            return {}

        try:
            bucket_name = str(self.config.get("bucket_name", "togather-ml-features"))

            # List objects to get statistics
            objects = list(self._minio_client.list_objects(bucket_name, recursive=True))

            total_size = sum(obj.size for obj in objects)
            total_files = len(objects)

            return {
                "bucket_name": bucket_name,
                "total_files": total_files,
                "total_size_bytes": total_size,
                "total_size_mb": round(total_size / (1024 * 1024), 2),
            }

        except Exception as e:
            self.logger.error(f"Failed to get bucket info: {e}")
            return {}


class DataArchiver:
    """Enhanced data archiver that periodically uploads processed data to MinIO."""

    def __init__(self, minio_sink: MinIOSink, batch_size: int = 1000):
        self.minio_sink = minio_sink
        self.batch_size = batch_size
        self.logger = get_logger(self.__class__.__name__)

    def archive_messages(self, messages: list[dict[str, Any]], topic: str) -> list[str | None]:
        """
        Archive messages to MinIO.

        Args:
            messages: Messages to archive
            topic: Topic name

        Returns:
            List of uploaded MinIO keys
        """
        if not messages:
            return []

        self.logger.info(f"Archiving {len(messages)} messages for topic: {topic}")

        # Add metadata to messages
        enriched_messages = []
        for message in messages:
            enriched_message = {
                **message,
                "archive_timestamp": datetime.now(timezone.utc).isoformat(),
                "topic": topic,
                "archive_source": "kafka-consumer",
            }
            enriched_messages.append(enriched_message)

        # Upload in batches
        return self.minio_sink.upload_batch(enriched_messages, self.batch_size)
