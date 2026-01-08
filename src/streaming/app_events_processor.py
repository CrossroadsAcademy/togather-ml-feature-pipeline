"""App events processor with EventEnvelope parsing and storage archiving.

Note: This processor uses togather-event-sdk for EventEnvelope parsing.
No Schema Registry is required - messages are self-describing protobufs.
"""

import asyncio
from datetime import datetime, timezone
from typing import Any

from src.streaming.kafka_consumer import EventProcessor, NonRetryableException
from src.streaming.storage_sink import DataArchiver, StorageSink
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
        """Initialize all required components.

        Note: No Schema Registry needed - we use EventEnvelope (self-describing protobuf)
        parsed by togather-event-sdk.
        """
        try:
            # Storage sink for archiving to MinIO (works with both MinIO and S3)
            self.storage_sink: StorageSink | None = None
            self.archiver: DataArchiver | None = None
            self.storage_enabled = False

            try:
                self.storage_sink = StorageSink()  # Uses settings from config
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
            self.processed_messages: set[str] = set()  # Simple in-memory deduplication
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
            # Create message ID for deduplication
            message_id = self._create_message_id(message, topic)

            # Check for duplicate messages (idempotent processing)
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

            # Basic validation - check for event_type from EventEnvelope
            event_type = message.get("_event_type", "")
            if not event_type and not message.get("user_id") and not message.get("id"):
                self.logger.warning(
                    "Message missing _event_type and identifiers, archiving anyway",
                    topic=topic,
                    keys=list(message.keys())[:10],
                )

            # Process based on event type
            await self._process_by_event_type(enriched_message, topic)

            # Mark message as processed (idempotent processing)
            self._mark_message_processed(message_id)

            # Add to buffer for batch archiving (only if storage is enabled)
            if self.storage_enabled:
                self.message_buffer.append(enriched_message)
                # Check if we should archive
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
            # Remove oldest entries (simple FIFO)
            oldest_messages = list(self.processed_messages)[:1000]  # Remove 1000 oldest
            self.processed_messages = self.processed_messages - set(oldest_messages)

    async def _process_by_event_type(self, message: dict[str, Any], topic: str) -> None:
        """
        Process message based on event type.

        Supports two routing modes:
        1. By _event_type from EventEnvelope (cloud Kafka with togather-event-sdk)
        2. By topic name (local testing with JSON messages)
        """
        # Prefer _event_type from EventEnvelope if present
        event_type = message.get("_event_type", "")

        # Route by event_type (from EventEnvelope)
        if event_type:
            if event_type == "user.v1.UserAccountCreated":
                await self._process_user_account_event(message)
            elif event_type == "user.v1.UserProfileCreated":
                await self._process_user_profile_event(message)
            elif event_type == "experience.v1.ExperienceCreated":
                await self._process_experience_event(message)
            elif event_type == "partner.v1.PartnerProfileCreated":
                await self._process_partner_profile_event(message)
            elif event_type == "feed.v1.RecommendationServedEvent":
                await self._process_recommendation_served_event(message)
            elif event_type == "feed.v1.RecommendationFeedback":
                await self._process_recommendation_feedback_event(message)
            else:
                # Unknown event type - still archive it
                self.logger.info(f"Unknown event_type: {event_type}, archiving without processing")
            return

        # Fallback: Route by topic name (local testing / JSON messages)
        # Support both cloud topics (*.events) and local topics
        if topic in ("user.account", "user.account.events"):
            await self._process_user_account_event(message)
        elif topic in ("user.profile", "user.profile.events"):
            await self._process_user_profile_event(message)
        elif topic in ("experience", "experience.events"):
            await self._process_experience_event(message)
        elif topic in ("partner.profile.events", "partner.account.events"):
            await self._process_partner_profile_event(message)
        elif topic == "engagement":
            await self._process_engagement_event(message)
        elif topic == "location.streams":
            await self._process_location_event(message)
        elif topic == "feed.recommendation_served":
            await self._process_recommendation_served_event(message)
        elif topic == "feed.recommendation_feedback":
            await self._process_recommendation_feedback_event(message)
        else:
            # Log but don't fail - allows adding new topics without code changes
            self.logger.warning(f"Unknown topic: {topic}, archiving without processing")

    async def _process_partner_profile_event(self, message: dict[str, Any]) -> None:
        """Process partner profile events (PartnerProfileCreated proto)."""
        partner_id = message.get("id")
        if not partner_id:
            raise NonRetryableException("Missing id in partner profile event")

        self.logger.debug(f"Processing partner profile event for partner: {partner_id}")
        await asyncio.sleep(0.01)

    async def _process_user_account_event(self, message: dict[str, Any]) -> None:
        """Process user account events (UserAccountCreated proto)."""
        # Proto uses 'id' field, not 'user_id'
        user_id = message.get("user_id") or message.get("id")
        if not user_id:
            raise NonRetryableException("Missing user_id/id in user account event")

        self.logger.debug(f"Processing user account event for user: {user_id}")
        await asyncio.sleep(0.01)

    async def _process_user_profile_event(self, message: dict[str, Any]) -> None:
        """Process user profile events (UserProfileCreated proto)."""
        # Proto uses 'id' field, not 'user_id'
        user_id = message.get("user_id") or message.get("id")
        if not user_id:
            raise NonRetryableException("Missing user_id/id in user profile event")

        # Extract interests for logging
        interests = message.get("interests", [])
        interest_names = [i.get("name", "") for i in interests] if interests else []

        self.logger.debug(
            f"Processing user profile event for user: {user_id}",
            extra={"interests": interest_names[:3]},  # Log first 3 interests
        )
        await asyncio.sleep(0.01)

    async def _process_experience_event(self, message: dict[str, Any]) -> None:
        """Process experience events (ExperienceCreated proto)."""
        # Proto uses 'id' for experience, 'creator_id' for creator
        experience_id = message.get("experience_id") or message.get("id")
        creator_id = message.get("creator_id")

        if not experience_id:
            raise NonRetryableException("Missing experience_id/id in experience event")

        category = message.get("category", {})
        category_name = category.get("name", "unknown") if isinstance(category, dict) else category

        self.logger.debug(
            f"Processing experience event: {experience_id}",
            extra={"creator_id": creator_id, "category": category_name},
        )
        await asyncio.sleep(0.02)

    async def _process_engagement_event(self, message: dict[str, Any]) -> None:
        """Process engagement events."""
        user_id = message.get("user_id")
        if not user_id:
            raise NonRetryableException("Missing user_id in engagement event")

        self.logger.debug(f"Processing engagement event for user: {user_id}")
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

        self.logger.debug(
            f"Processing location event for user: {user_id} at ({latitude}, {longitude})"
        )
        await asyncio.sleep(0.02)

    async def _process_recommendation_served_event(self, message: dict[str, Any]) -> None:
        """
        Process RecommendationServed events (feed.v1.RecommendationServedEvent proto).

        Records which recommendations were shown to a user.
        """
        request_id = message.get("request_id")
        user_id = message.get("user_id")
        recommendations = message.get("recommendations", [])

        if not request_id:
            raise NonRetryableException("Missing request_id in recommendation served event")
        if not user_id:
            raise NonRetryableException("Missing user_id in recommendation served event")

        self.logger.debug(
            "Processing recommendation served event",
            extra={
                "request_id": request_id,
                "user_id": user_id,
                "num_recommendations": len(recommendations),
                "trigger": message.get("trigger"),
                "retrieval_model": message.get("retrieval_model"),
                "ranking_model": message.get("ranking_model"),
            },
        )
        await asyncio.sleep(0.01)

    async def _process_recommendation_feedback_event(self, message: dict[str, Any]) -> None:
        """
        Process RecommendationFeedback events (feed.v1.RecommendationFeedback proto).

        Records user engagement signals: impressions, clicks, views, actions.
        """
        user_id = message.get("user_id")
        event_id = message.get("event_id")  # The experience ID
        event_type = message.get(
            "event_type"
        )  # 1=impression, 2=click, 3=view, 4=action, 5=negative

        if not user_id:
            raise NonRetryableException("Missing user_id in recommendation feedback event")
        if not event_id:
            raise NonRetryableException("Missing event_id in recommendation feedback event")

        # Map event type to name for logging - supports both int and string enum values
        event_type_names: dict[int | str, str] = {
            # Integer values (legacy/numeric)
            1: "impression",
            2: "click",
            3: "view",
            4: "action",
            5: "negative",
            # String enum values from protobuf MessageToDict
            "EVENT_TYPE_IMPRESSION": "impression",
            "EVENT_TYPE_CLICK": "click",
            "EVENT_TYPE_VIEW": "view",
            "EVENT_TYPE_ACTION": "action",
            "EVENT_TYPE_NEGATIVE": "negative",
        }
        event_type_name = event_type_names.get(event_type, f"unknown({event_type})")  # type: ignore

        # Extract engagement details based on event type
        extra_data = {
            "user_id": user_id,
            "event_id": event_id,
            "event_type": event_type_name,
            "request_id": message.get("request_id"),
            "position": message.get("position"),
        }

        # Add event-specific data (check both int and string types)
        is_view = event_type in (3, "EVENT_TYPE_VIEW")
        is_action = event_type in (4, "EVENT_TYPE_ACTION")

        if is_view:
            view_data = message.get("view_data", {})
            extra_data["dwell_time_ms"] = view_data.get("dwell_time_ms")
        elif is_action:
            action_data = message.get("action_data", {})
            extra_data["action_type"] = action_data.get("action_type")

        self.logger.debug(
            f"Processing recommendation feedback: {event_type_name}", extra=extra_data
        )
        await asyncio.sleep(0.01)

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
