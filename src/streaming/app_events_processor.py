"""App events processor with EventEnvelope parsing and storage archiving.

Note: This processor uses togather-event-sdk for EventEnvelope parsing.
No Schema Registry is required - messages are self-describing protobufs.

ML-Lean Schema:
- Extracts only ML-relevant fields to keep Parquet files lean
- Skips user.account.events (user.profile.events has all needed data)
- Experience: id, name, description, location (city + coords), price, times, capacity, tags, category
- User Profile: id, location (city + coords), interests, demographics, status
- Recommendation events: ALL fields (needed for training data)
"""

import asyncio
from datetime import datetime, timezone
from typing import Any

from src.streaming.kafka_consumer import EventProcessor, NonRetryableException
from src.streaming.storage_sink import DataArchiver, StorageSink
from src.utils.logger import get_logger

logger = get_logger(__name__)


# ML FIELD SCHEMAS
# Only extract fields needed for ML pipeline (ranking, training, aggregations)

# Fields to extract from experience.events
EXPERIENCE_FIELDS = {
    "id",
    "name",
    "description",
    "creatorId",
    "priceAmount",
    "priceCurrency",
    "startTime",
    "endTime",
    "bucketSize",
    "totalBuckets",
    "createdAt",
    "updatedAt",
}

# Nested fields from experience.events that need special handling
EXPERIENCE_LOCATION_FIELDS = {"city", "latitude", "longitude"}
EXPERIENCE_NESTED_FIELDS = {"tags", "category", "eventLocation"}

# Fields to extract from user.profile.events
USER_PROFILE_FIELDS = {
    "id",
    "dob",
    "gender",
    "avatarKey",
    "socialScore",
    "status",
    "createdAt",
    "updatedAt",
    "onBoardingStatus",
    "onBoardedAt",
}

# Nested fields from user.profile.events
USER_PROFILE_NESTED_FIELDS = {"interests", "currentAddress"}


class AppEventsProcessor(EventProcessor):
    """Enhanced processor for app events with full observability and validation.

    Implements ML- field extraction to only archive fields needed for:
    - Two-Tower model training (user/item embeddings)
    - Ranking model training (engagement signals)
    - Real-time ranking features
    - Batch aggregations (per-user, per-item)
    """

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

    # FIELD EXTRACTION METHODS

    def _extract_experience_fields(self, message: dict[str, Any]) -> dict[str, Any]:
        """Extract only ML-relevant fields from ExperienceCreated event.

        Extracts per contract:
        - Core: id, name, description, creator_id, price, times, buckets, timestamps
        - Location: city, latitude, longitude
        - Tags: array of {id, name} objects (as JSON string)
        - Category: id, name

        Note: SDK MessageToDict outputs camelCase field names.
        """
        import json

        extracted: dict[str, Any] = {}

        # Direct field mappings (SDK camelCase -> output snake_case)
        field_mappings = {
            "id": "id",
            "name": "name",
            "description": "description",
            "creatorId": "creator_id",
            "creatorType": "creator_type",
            "thumbnailKey": "thumbnail_key",
            "priceAmount": "price_amount",
            "priceCurrency": "price_currency",
            "startTime": "start_time",
            "endTime": "end_time",
            "bucketSize": "bucket_size",
            "totalBuckets": "total_buckets",
            "createdAt": "created_at",
            "updatedAt": "updated_at",
        }

        for sdk_field, output_field in field_mappings.items():
            if sdk_field in message and message[sdk_field] is not None:
                extracted[output_field] = message[sdk_field]

        # Extract location from eventLocation (camelCase from SDK)
        location = message.get("eventLocation") or {}
        if location:
            extracted["event_location_city"] = location.get("city")
            coordinate = location.get("coordinate") or {}
            if coordinate:
                extracted["event_location_latitude"] = coordinate.get("latitude")
                extracted["event_location_longitude"] = coordinate.get("longitude")

        # Extract tags - SDK returns array of objects, store as JSON string
        tags = message.get("tags") or []
        if tags and isinstance(tags, list):
            # Keep only id and name per contract
            tag_list = [
                {"id": t.get("id"), "name": t.get("name")} for t in tags if isinstance(t, dict)
            ]
            extracted["tags"] = json.dumps(tag_list)

        # Extract category (id and name)
        category = message.get("category") or {}
        if category and isinstance(category, dict):
            extracted["category_id"] = category.get("id")
            extracted["category_name"] = category.get("name")

        return extracted

    def _extract_user_profile_fields(self, message: dict[str, Any]) -> dict[str, Any]:
        """Extract only ML-relevant fields from UserProfileCreated event.

        Extracts:
        - Core: id, dob, gender, avatar_key, social_score, status, timestamps, onboarding
        - Address: city, latitude, longitude
        - Interests: array of {id, name} objects (as JSON string)

        Note: SDK MessageToDict outputs camelCase field names.
        """
        import json

        extracted: dict[str, Any] = {}

        # Direct field mappings (SDK camelCase -> output snake_case)
        field_mappings = {
            "id": "id",
            "dob": "dob",
            "gender": "gender",
            "avatarKey": "avatar_key",
            "socialScore": "social_score",
            "status": "status",
            "createdAt": "created_at",
            "updatedAt": "updated_at",
            "onBoardingStatus": "on_boarding_status",
            "onBoardedAt": "on_boarded_at",
        }

        for sdk_field, output_field in field_mappings.items():
            if sdk_field in message and message[sdk_field] is not None:
                extracted[output_field] = message[sdk_field]

        # Extract address from currentAddress (camelCase from SDK)
        address = message.get("currentAddress") or {}
        if address:
            extracted["current_address_city"] = address.get("city")
            coordinate = address.get("coordinate") or {}
            if coordinate:
                extracted["current_address_latitude"] = coordinate.get("latitude")
                extracted["current_address_longitude"] = coordinate.get("longitude")

        # Extract interests - SDK returns array of objects, store as JSON string
        interests = message.get("interests") or []
        if interests and isinstance(interests, list):
            # Keep only id and name per contract
            interest_list = [
                {"id": i.get("id"), "name": i.get("name")} for i in interests if isinstance(i, dict)
            ]
            extracted["interests"] = json.dumps(interest_list)

        return extracted

    def _extract_all_fields(self, message: dict[str, Any]) -> dict[str, Any]:
        """Extract all fields (for recommendation events that need full data)."""
        # Filter out internal fields but keep everything else
        return {
            k: v
            for k, v in message.items()
            if not k.startswith("_") or k in ("_event_type", "_timestamp")
        }

    # MESSAGE PROCESSING

    async def process(self, message: dict[str, Any], topic: str) -> None:
        """
        Process a single event message with full validation, idempotent processing, and archiving.

        Args:
            message: Message data
            topic: Kafka topic name
        """
        try:
            # Skip user.account.events - user.profile.events
            event_type = message.get("_event_type", "")
            if event_type == "user.v1.UserAccountCreated" or topic == "user.account.events":
                self.logger.debug(
                    "Skipping user.account.events (user.profile.events has all needed data)",
                    topic=topic,
                )
                return

            # Create message ID for deduplication
            message_id = self._create_message_id(message, topic)

            # Check for duplicate messages (idempotent processing)
            if self._is_duplicate_message(message_id):
                self.logger.debug(
                    "Duplicate message detected, skipping processing",
                    topic=topic,
                    message_id=message_id,
                )
                return

            # Extract ML-relevant fields based on event type
            extracted_message = self._extract_ml_fields(message, topic)

            # Add processing metadata
            enriched_message = {
                **extracted_message,
                "processed_at": datetime.now(timezone.utc).isoformat(),
                "topic": topic,
                "processor_version": "2.0.0",
                "message_id": message_id,
                "_event_type": event_type,  # Keep event type for routing
            }

            # Process based on event type (validation + logging)
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
                event_type=event_type,
                fields_extracted=len(extracted_message),
                message_id=message_id,
            )

        except Exception as e:
            self.logger.error("Error processing event", topic=topic, error=str(e), exc_info=True)
            raise

    def _extract_ml_fields(self, message: dict[str, Any], topic: str) -> dict[str, Any]:
        """Extract only ML-relevant fields based on event type.

        Routes to appropriate extraction method:
        - experience.events -> lean experience schema
        - user.profile.events -> lean user profile schema
        - recommendation.* -> all fields (needed for training)
        """
        event_type = message.get("_event_type", "")

        # Experience events - extraction
        if event_type == "experience.v1.ExperienceCreated" or topic == "experience.events":
            return self._extract_experience_fields(message)

        # User profile events - extraction
        if (
            event_type in ("user.v1.UserProfileCreated", "user.v1.UserProfileUpdated")
            or topic == "user.profile.events"
        ):
            return self._extract_user_profile_fields(message)

        # Recommendation events - keep all fields for training data
        if event_type in (
            "feed.v1.RecommendationServedEvent",
            "feed.v1.RecommendationFeedback",
        ):
            return self._extract_all_fields(message)
        if topic in ("recommendation.served", "recommendation.feedback.v1"):
            return self._extract_all_fields(message)

        # Partner events - minimal extraction (future use)
        if event_type == "partner.v1.PartnerProfileCreated" or "partner" in topic:
            return {"id": message.get("id"), "_event_type": event_type}

        # Unknown events - extract all but log warning
        self.logger.warning(f"Unknown event type: {event_type}, extracting all fields")
        return self._extract_all_fields(message)

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

        # Extract interests for logging - may be JSON string or list
        import json

        interests = message.get("interests", [])
        interest_names: list[str] = []
        if interests:
            # Handle JSON string from extraction (or list from raw message)
            if isinstance(interests, str):
                try:
                    interests = json.loads(interests)
                except json.JSONDecodeError:
                    interests = []
            if isinstance(interests, list):
                interest_names = [i.get("name", "") for i in interests if isinstance(i, dict)]

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
