"""S3/MinIO file sink for data persistence."""

import json
from datetime import datetime, timezone
from typing import Any

import boto3
from botocore.client import BaseClient
from botocore.exceptions import ClientError
from prometheus_client import Counter, Histogram
from tenacity import retry, stop_after_attempt, wait_exponential

from src.utils.logger import get_logger
from src.utils.secrets import get_secrets_manager

logger = get_logger(__name__)

# Prometheus Metrics
s3_operations = Counter("s3_operations_total", "Total S3 operations", ["operation", "status"])

s3_operation_duration = Histogram(
    "s3_operation_duration_seconds", "S3 operation duration", ["operation"]
)

s3_file_size = Histogram("s3_file_size_bytes", "S3 file size in bytes", ["bucket"])


class S3Sink:
    """S3/MinIO sink for persisting data."""

    def __init__(self, config: dict[str, Any] | None = None):
        self.config = config or get_secrets_manager().get_s3_config()
        self.logger = get_logger(self.__class__.__name__)

        # Initialize S3 client
        self._s3_client: BaseClient | None = None
        self._initialize_client()

    def _initialize_client(self) -> None:
        """Initialize S3 client."""
        try:
            # Create S3 client configuration
            s3_config = {
                "region_name": self.config.get("region", "us-east-1"),
            }

            # Add endpoint URL for MinIO
            if self.config.get("endpoint_url"):
                s3_config["endpoint_url"] = self.config["endpoint_url"]

            # Add credentials if provided
            if self.config.get("access_key_id") and self.config.get("secret_access_key"):
                s3_config["aws_access_key_id"] = self.config["access_key_id"]
                s3_config["aws_secret_access_key"] = self.config["secret_access_key"]

            self._s3_client = boto3.client("s3", **s3_config)

            # Test connection
            self._test_connection()

            self.logger.info("S3 client initialized successfully")

        except Exception as e:
            self.logger.error(f"Failed to initialize S3 client: {e}")
            raise

    def _test_connection(self) -> None:
        """Test S3 connection."""
        assert self._s3_client is not None
        try:
            bucket_name = self.config.get("bucket_name", "togather-ml-features")
            self._s3_client.head_bucket(Bucket=bucket_name)
            self.logger.info(f"S3 connection test successful for bucket: {bucket_name}")
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "404":
                self.logger.warning(f"Bucket not found: {bucket_name}")
            else:
                self.logger.error(f"S3 connection test failed: {e}")
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
        Upload data to S3.

        Args:
            data: List of messages to upload
            topic: Kafka topic name
            partition: Kafka partition (optional)
            timestamp: Message timestamp (optional)

        Returns:
            S3 object key if successful, None otherwise
        """
        if not self._s3_client:
            self.logger.error("S3 client not initialized")
            return None

        try:
            # Generate object key
            timestamp = timestamp or datetime.now(timezone.utc)
            date_path = timestamp.strftime("%Y/%m/%d/%H")

            if partition is not None:
                object_key = f"kafka-data/{topic}/partition={partition}/{date_path}/{timestamp.isoformat()}.json"
            else:
                object_key = f"kafka-data/{topic}/{date_path}/{timestamp.isoformat()}.json"

            # Prepare data for upload
            file_content = json.dumps(data, indent=2)
            file_size = len(file_content.encode("utf-8"))

            # Upload to S3
            bucket_name = self.config.get("bucket_name", "togather-ml-features")

            self._s3_client.put_object(
                Bucket=bucket_name,
                Key=object_key,
                Body=file_content,
                ContentType="application/json",
            )

            # Record metrics
            s3_operations.labels(operation="upload", status="success").inc()
            s3_file_size.labels(bucket=bucket_name).observe(file_size)

            self.logger.info(
                "File uploaded to S3 successfully",
                bucket=bucket_name,
                key=object_key,
                size=file_size,
                record_count=len(data),
            )

            return object_key

        except ClientError as e:
            self.logger.error(f"S3 upload failed: {e}")
            s3_operations.labels(operation="upload", status="error").inc()
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error during S3 upload: {e}")
            s3_operations.labels(operation="upload", status="error").inc()
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
            List of S3 object keys
        """
        # Group messages by topic and partition
        uploaded_keys: list[str | None] = []
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
        List files in S3 bucket.

        Args:
            topic: Filter by topic
            prefix: Filter by prefix

        Returns:
            List of file metadata
        """
        if not self._s3_client:
            self.logger.error("S3 client not initialized")
            return []

        try:
            bucket_name = self.config.get("bucket_name", "togather-ml-features")

            # Build prefix
            if topic:
                search_prefix = f"kafka-data/{topic}/"
            elif prefix:
                search_prefix = prefix
            else:
                search_prefix = "kafka-data/"

            response = self._s3_client.list_objects_v2(Bucket=bucket_name, Prefix=search_prefix)

            files = []
            for obj in response.get("Contents", []):
                files.append(
                    {
                        "key": obj["Key"],
                        "size": obj["Size"],
                        "last_modified": obj["LastModified"],
                        "etag": obj["ETag"],
                    }
                )

            s3_operations.labels(operation="list", status="success").inc()

            self.logger.debug(f"Listed {len(files)} files from S3")

            return files

        except ClientError as e:
            self.logger.error(f"S3 list operation failed: {e}")
            s3_operations.labels(operation="list", status="error").inc()
            return []

    def download_file(self, object_key: str) -> str | None:
        """
        Download file from S3.

        Args:
            object_key: S3 object key

        Returns:
            File content as string
        """
        if not self._s3_client:
            self.logger.error("S3 client not initialized")
            return None

        try:
            bucket_name = self.config.get("bucket_name", "togather-ml-features")

            response = self._s3_client.get_object(Bucket=bucket_name, Key=object_key)

            content: str = response["Body"].read().decode("utf-8")

            s3_operations.labels(operation="download", status="success").inc()

            self.logger.debug(f"Downloaded file from S3: {object_key}")

            return content

        except ClientError as e:
            self.logger.error(f"S3 download failed for {object_key}: {e}")
            s3_operations.labels(operation="download", status="error").inc()
            return None

    def delete_file(self, object_key: str) -> bool:
        """
        Delete file from S3.

        Args:
            object_key: S3 object key

        Returns:
            True if successful, False otherwise
        """
        if not self._s3_client:
            self.logger.error("S3 client not initialized")
            return False

        try:
            bucket_name = self.config.get("bucket_name", "togather-ml-features")

            self._s3_client.delete_object(Bucket=bucket_name, Key=object_key)

            s3_operations.labels(operation="delete", status="success").inc()

            self.logger.info(f"Deleted file from S3: {object_key}")

            return True

        except ClientError as e:
            self.logger.error(f"S3 delete failed for {object_key}: {e}")
            s3_operations.labels(operation="delete", status="error").inc()
            return False


class DataArchiver:
    """Data archiver that periodically uploads processed data to S3."""

    def __init__(self, s3_sink: S3Sink, batch_size: int = 1000):
        self.s3_sink = s3_sink
        self.batch_size = batch_size
        self.logger = get_logger(self.__class__.__name__)

    def archive_messages(self, messages: list[dict[str, Any]], topic: str) -> list[str | None]:
        """
        Archive messages to S3.

        Args:
            messages: Messages to archive
            topic: Topic name

        Returns:
            List of uploaded S3 keys
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
            }
            enriched_messages.append(enriched_message)

        # Upload in batches
        return self.s3_sink.upload_batch(enriched_messages, self.batch_size)
