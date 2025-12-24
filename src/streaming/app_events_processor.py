import asyncio
from datetime import datetime, timezone
from typing import Any

from src.streaming.kafka_consumer import EventProcessor, NonRetryableException
from src.streaming.schema_registry import (  # type: ignore[attr-defined]
    MessageValidator,
    ProtobufDeserializer,
    SchemaRegistryClient,
)
from src.streaming.storage_sink import DataArchiver, StorageSink
from src.utils.config import settings
from src.utils.logger import get_logger

logger = get_logger(__name__)


class AppEventsProcessor(EventProcessor):
    """Enhanced processor for app events with full observability and validation."""

    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)

        # Initialize components
        self._initialize_components()

        # Message buffer for batch processing
        self.message_buffer: list[dict[str, Any]] = []
        self.buffer_size = 100  # Process in batches of 100

        # Archive processed messages periodically
        self.archive_interval = 60  # seconds
        self.last_archive_time = datetime.now(timezone.utc)

    def _initialize_components(self) -> None:
        """Initialize all required components."""
        try:
            # Schema Registry
            schema_registry_config = {
                "url": settings.schema_registry.url,
                "username": settings.schema_registry.username,
                "password": settings.schema_registry.password,
                "ssl_ca_location": settings.schema_registry.ssl_ca_location,
            }

            # Remove None values
            schema_registry_config = {
                k: v for k, v in schema_registry_config.items() if v is not None
            }

            base_url = str(schema_registry_config.get("url", ""))
            auth: tuple[str, str] | None = None
            if schema_registry_config.get("username") and schema_registry_config.get("password"):
                auth = (
                    str(schema_registry_config["username"]),
                    str(schema_registry_config["password"]),
                )

            self.schema_registry_client = SchemaRegistryClient(base_url=base_url, auth=auth)

            # Protobuf deserializer
            self.deserializer = ProtobufDeserializer(self.schema_registry_client)

            # Message validator
            self.validator = MessageValidator(self.schema_registry_client)

            # Unified storage sink
            self.storage_sink: StorageSink | None = None
            self.archiver: DataArchiver | None = None
            self.storage_enabled = False

            try:
                self.storage_sink = StorageSink()
                self.archiver = DataArchiver(self.storage_sink, batch_size=1000)
                self.storage_enabled = True
                self.logger.info(
                    "Storage sink initialized",
                    endpoint=self.storage_sink.config.endpoint,
                    bucket=self.storage_sink.config.bucket_name,
                )
            except Exception as storage_error:
                self.logger.warning(f"Storage sink initialization failed: {storage_error}")
                self.logger.warning(
                    "Continuing without storage archiving - messages will be processed but not archived"
                )
                self.storage_sink = None
                self.archiver = None
                self.storage_enabled = False

            # Message deduplication for idempotent processing
            self.processed_messages: set[str] = set()  # in-memory deduplication
            self.max_dedup_cache_size = 10000  # Prevent memory leaks

            self.logger.info("App events processor components initialized successfully")

        except Exception as e:
            self.logger.error(f"Failed to initialize processor components: {e}")
            raise

    async def process(self, message: dict[str, Any], topic: str) -> None:
        """
        Process a single event message with full validation, idempotent processing, and archiving.

        Args:
            message: Message data
            topic: Kafka topic name
        """
        try:
            # Create message ID for deduplication for idempotent processing
            message_id = self._create_message_id(message, topic)

            # Check for duplicate messages for idempotent processing
            if self._is_duplicate_message(message_id):
                self.logger.info(
                    "Duplicate message detected, skipping processing",
                    topic=topic,
                    message_id=message_id,
                    user_id=message.get("user_id"),
                )
                return

            # Add processing metadata
            enriched_message = {
                **message,
                "processed_at": datetime.now(timezone.utc).isoformat(),
                "topic": topic,
                "processor_version": "1.0.0",
                "message_id": message_id,
            }

            # Validate message
            is_valid, error_msg = self.validator.validate_message(enriched_message, topic)
            if not is_valid:
                raise NonRetryableException(f"Message validation failed: {error_msg}")

            # Process based on event type
            await self._process_by_event_type(enriched_message, topic)

            # Mark message as processed (idempotent processing)
            self._mark_message_processed(message_id)

            # Add to buffer for batch archiving
            if self.storage_enabled:
                self.message_buffer.append(enriched_message)
                # Check if should archive
                await self._check_and_archive()

            self.logger.info(
                "Event processed successfully",
                topic=topic,
                event_type=message.get("event_type"),
                user_id=message.get("user_id"),
                message_id=message_id,
            )

        except Exception as e:
            self.logger.error("Error processing event", topic=topic, error=str(e), exc_info=True)
            raise

    def _create_message_id(self, message: dict[str, Any], topic: str) -> str:
        """Create a unique message ID for deduplication."""
        # Use topic, partition, offset, and timestamp for unique ID
        import hashlib

        message_key = f"{topic}:{message.get('partition', 'unknown')}:{message.get('offset', 'unknown')}:{message.get('timestamp', datetime.now(timezone.utc).timestamp())}"
        return hashlib.md5(message_key.encode()).hexdigest()

    def _is_duplicate_message(self, message_id: str) -> bool:
        """Check if message has already been processed (idempotent processing)."""
        return message_id in self.processed_messages

    def _mark_message_processed(self, message_id: str) -> None:
        """Mark message as processed and manage cache size."""
        self.processed_messages.add(message_id)

        # Prevent memory leaks by limiting cache size
        if len(self.processed_messages) > self.max_dedup_cache_size:
            # Remove oldest entries
            oldest_messages = list(self.processed_messages)[:1000]  # Remove 1000 oldest
            self.processed_messages = self.processed_messages - set(oldest_messages)

    async def _process_by_event_type(self, message: dict[str, Any], topic: str) -> None:
        """Process message based on event type and topic."""
        event_type = message.get("event_type", "unknown")  # noqa: F841

        # Route to specific processors based on topic
        if topic == "user.account":
            await self._process_user_account_event(message)
        elif topic == "user.profile":
            await self._process_user_profile_event(message)
        elif topic == "experience":
            await self._process_experience_event(message)
        elif topic == "engagement":
            await self._process_engagement_event(message)
        elif topic == "location.streams":
            await self._process_location_event(message)
        else:
            self.logger.warning(f"Unknown topic: {topic}")

    async def _process_user_account_event(self, message: dict[str, Any]) -> None:
        """Process user account events."""
        user_id = message.get("user_id")
        if not user_id:
            raise NonRetryableException("Missing user_id in user account event")

        # Add logic here
        self.logger.debug(f"Processing user account event for user: {user_id}")

        # Simulate processing time
        await asyncio.sleep(0.01)

    async def _process_user_profile_event(self, message: dict[str, Any]) -> None:
        """Process user profile events."""
        user_id = message.get("user_id")
        if not user_id:
            raise NonRetryableException("Missing user_id in user profile event")

        # Add logic here
        # calculate preferences, etc.
        self.logger.debug(f"Processing user profile event for user: {user_id}")

        # Simulate processing time
        await asyncio.sleep(0.01)

    async def _process_experience_event(self, message: dict[str, Any]) -> None:
        """Process experience events."""
        user_id = message.get("user_id")
        experience_id = message.get("experience_id")

        if not user_id or not experience_id:
            raise NonRetryableException("Missing user_id or experience_id in experience event")

        # Add logic here
        # Example: Update experience metrics etc.
        self.logger.debug(
            f"Processing experience event for user: {user_id}, experience: {experience_id}"
        )

        # Simulate processing time
        await asyncio.sleep(0.02)

    async def _process_engagement_event(self, message: dict[str, Any]) -> None:
        """Process engagement events."""
        user_id = message.get("user_id")
        if not user_id:
            raise NonRetryableException("Missing user_id in engagement event")

        # Add logic here
        # Example: Update engagement metrics
        self.logger.debug(f"Processing engagement event for user: {user_id}")

        # Simulate processing time
        await asyncio.sleep(0.01)

    async def _process_location_event(self, message: dict[str, Any]) -> None:
        """Process location events."""
        user_id = message.get("user_id")
        latitude = message.get("latitude")
        longitude = message.get("longitude")

        if not user_id or latitude is None or longitude is None:
            raise NonRetryableException("Missing required fields in location event")

        # Validate coordinates
        if not (-90 <= latitude <= 90) or not (-180 <= longitude <= 180):
            raise NonRetryableException("Invalid latitude or longitude values")

        # Add logic here
        self.logger.debug(
            f"Processing location event for user: {user_id} at ({latitude}, {longitude})"
        )

        # Simulate processing time
        await asyncio.sleep(0.02)

    async def _check_and_archive(self) -> None:
        """Check if we should archive messages and do so if needed."""
        # Skip archiving if storage is not enabled
        if not self.storage_enabled or not self.archiver:
            return

        current_time = datetime.now(timezone.utc)

        # Archive if buffer is full or time interval has passed
        should_archive = (
            len(self.message_buffer) >= self.buffer_size
            or (current_time - self.last_archive_time).seconds >= self.archive_interval
        )

        if should_archive and self.message_buffer:
            try:
                # Group messages by topic for archiving
                messages_by_topic: dict[str, list[dict[str, Any]]] = {}
                for message in self.message_buffer:
                    topic = message.get("topic", "unknown")
                    if topic not in messages_by_topic:
                        messages_by_topic[topic] = []
                    messages_by_topic[topic].append(message)

                # Archive each topic's messages
                for topic, messages in messages_by_topic.items():
                    archived_keys = self.archiver.archive_messages(messages, topic)
                    self.logger.info(
                        f"Archived {len(messages)} messages for topic {topic}",
                        archived_keys=len([k for k in archived_keys if k is not None]),
                    )

                # Clear buffer and update timestamp
                self.message_buffer.clear()
                self.last_archive_time = current_time

            except Exception as e:
                self.logger.error(f"Failed to archive messages: {e}", exc_info=True)
                # Don't raise the exception to avoid stopping message processing

    async def shutdown(self) -> None:
        """Shutdown processor and archive remaining messages."""
        self.logger.info("Shutting down app events processor...")

        # Archive any remaining messages (only if storage is enabled)
        if self.storage_enabled and self.message_buffer and self.archiver:
            try:
                # Group messages by topic
                messages_by_topic: dict[str, list[dict[str, Any]]] = {}
                for message in self.message_buffer:
                    topic = message.get("topic", "unknown")
                    if topic not in messages_by_topic:
                        messages_by_topic[topic] = []
                    messages_by_topic[topic].append(message)

                # Archive each topic's messages
                for topic, messages in messages_by_topic.items():
                    archived_keys = self.archiver.archive_messages(messages, topic)
                    self.logger.info(
                        f"Archived {len(messages)} remaining messages for topic {topic}",
                        archived_keys=len([k for k in archived_keys if k is not None]),
                    )

                self.message_buffer.clear()

            except Exception as e:
                self.logger.error(f"Failed to archive remaining messages: {e}", exc_info=True)
        elif self.message_buffer and not self.storage_enabled:
            self.logger.info(
                f"Clearing {len(self.message_buffer)} messages from buffer (storage archiving disabled)"
            )
            self.message_buffer.clear()

        self.logger.info("App events processor shutdown complete")
