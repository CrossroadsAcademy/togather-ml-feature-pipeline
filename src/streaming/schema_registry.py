"""Schema Registry integration for protobuf deserialization with schema ID extraction."""

from dataclasses import dataclass
from typing import Any

import requests
from prometheus_client import Counter, Histogram
from tenacity import retry, stop_after_attempt, wait_exponential

from src.utils.logger import get_logger

logger = get_logger(__name__)

# Prometheus Metrics
schema_registry_requests = Counter(
    "schema_registry_requests_total", "Total schema registry requests", ["method", "status"]
)

schema_registry_duration = Histogram(
    "schema_registry_request_duration_seconds", "Schema registry request duration"
)


@dataclass
class SchemaMetadata:
    """Schema metadata from registry."""

    schema_id: int
    version: int
    schema_type: str
    subject: str
    schema_content: str


class SchemaRegistryClient:
    """Client for interacting with Schema Registry."""

    def __init__(self, base_url: str, auth: tuple[str, str] | None = None):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        if auth:
            self.session.auth = auth

        self.logger = get_logger(self.__class__.__name__)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def get_schema_by_id(self, schema_id: int) -> SchemaMetadata | None:
        """Get schema by ID from registry."""
        try:
            response = self.session.get(f"{self.base_url}/schemas/ids/{schema_id}", timeout=10)

            schema_registry_requests.labels(
                method="get_schema", status=str(response.status_code)
            ).inc()

            if response.status_code == 200:
                data = response.json()
                return SchemaMetadata(
                    schema_id=schema_id,
                    version=data.get("version", 0),
                    schema_type=data.get("schemaType", "PROTOBUF"),
                    subject=data.get("subject", ""),
                    schema_content=data.get("schema", ""),
                )
            elif response.status_code == 404:
                self.logger.warning(f"Schema not found for ID: {schema_id}")
                return None
            else:
                response.raise_for_status()

        except requests.RequestException as e:
            self.logger.error(f"Error fetching schema {schema_id}: {e}")
            schema_registry_requests.labels(method="get_schema", status="error").inc()
            raise
        return None

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def get_latest_schema(self, subject: str) -> SchemaMetadata | None:
        """Get latest schema for subject."""
        try:
            response = self.session.get(
                f"{self.base_url}/subjects/{subject}/versions/latest", timeout=10
            )

            schema_registry_requests.labels(
                method="get_latest_schema", status=str(response.status_code)
            ).inc()

            if response.status_code == 200:
                data = response.json()
                return SchemaMetadata(
                    schema_id=data.get("id", 0),
                    version=data.get("version", 0),
                    schema_type=data.get("schemaType", "PROTOBUF"),
                    subject=subject,
                    schema_content=data.get("schema", ""),
                )
            elif response.status_code == 404:
                self.logger.warning(f"No schema found for subject: {subject}")
                return None
            else:
                response.raise_for_status()

        except requests.RequestException as e:
            self.logger.error(f"Error fetching latest schema for subject {subject}: {e}")
            schema_registry_requests.labels(method="get_latest_schema", status="error").inc()
            raise
        return None


class ProtobufDeserializer:
    """Protobuf deserializer with schema registry integration."""

    def __init__(self, schema_registry_client: SchemaRegistryClient):
        self.schema_registry = schema_registry_client
        self.schema_cache: dict[int, SchemaMetadata] = {}
        self.logger = get_logger(self.__class__.__name__)

    def extract_schema_id(self, message_bytes: bytes) -> int | None:
        """Extract schema ID from message bytes (Confluent format)."""
        if len(message_bytes) < 5:
            return None

        # Confluent format: 1 byte magic + 4 bytes schema ID
        if message_bytes[0] == 0:  # Magic byte
            schema_id = int.from_bytes(message_bytes[1:5], byteorder="big")
            return schema_id

        return None

    def get_schema(self, schema_id: int) -> SchemaMetadata | None:
        """Get schema from cache or registry."""
        if schema_id in self.schema_cache:
            return self.schema_cache[schema_id]

        schema = self.schema_registry.get_schema_by_id(schema_id)
        if schema:
            self.schema_cache[schema_id] = schema

        return schema

    def deserialize_message(
        self, message_bytes: bytes, topic: str
    ) -> tuple[dict[str, Any] | None, str | None]:
        """
        Deserialize protobuf message using schema registry.

        Returns:
            Tuple of (deserialized_data, error_message)
        """
        try:
            # Extract schema ID
            schema_id = self.extract_schema_id(message_bytes)
            if not schema_id:
                return None, "Could not extract schema ID from message"

            # Get schema metadata
            schema = self.get_schema(schema_id)
            if not schema:
                return None, f"Schema not found for ID: {schema_id}"

            # Skip magic byte and schema ID (5 bytes total)
            message_data = message_bytes[5:]

            # For now, return raw data with schema metadata
            # In a real implementation, use the protobuf schema
            # to deserialize the message_data
            deserialized = {
                "schema_id": schema_id,
                "schema_version": schema.version,
                "schema_subject": schema.subject,
                "raw_data": message_data.hex(),  # Hex representation for now
                "topic": topic,
            }

            self.logger.debug(
                "Message deserialized successfully",
                topic=topic,
                schema_id=schema_id,
                schema_subject=schema.subject,
            )

            return deserialized, None

        except Exception as e:
            error_msg = f"Failed to deserialize message: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            return None, error_msg


class MessageValidator:
    """Message validator using schema registry."""

    def __init__(self, schema_registry_client: SchemaRegistryClient):
        self.schema_registry = schema_registry_client
        self.logger = get_logger(self.__class__.__name__)

    def validate_message(self, message_data: dict[str, Any], topic: str) -> tuple[bool, str | None]:
        """
        Validate message against schema.

        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            # Get latest schema for topic
            schema = self.schema_registry.get_latest_schema(topic)
            if not schema:
                return False, f"No schema found for topic: {topic}"

            # Basic validation - check required fields
            # In a real implementation, validate against the actual schema
            required_fields = self._get_required_fields_for_topic(topic)

            for field in required_fields:
                if field not in message_data:
                    return False, f"Missing required field: {field}"

            self.logger.debug(
                "Message validation successful",
                topic=topic,
                schema_id=schema.schema_id,
                schema_version=schema.version,
            )

            return True, None

        except Exception as e:
            error_msg = f"Validation error: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            return False, error_msg

    def _get_required_fields_for_topic(self, topic: str) -> list[str]:
        """Get required fields for a topic."""
        topic_requirements = {
            "user.account": ["user_id", "event_type", "timestamp"],
            "user.profile": ["user_id", "event_type", "timestamp"],
            "experience": ["user_id", "experience_id", "event_type", "timestamp"],
            "engagement": ["user_id", "event_type", "timestamp"],
            "location.streams": ["user_id", "latitude", "longitude", "timestamp"],
        }

        return topic_requirements.get(topic, ["user_id", "event_type", "timestamp"])
