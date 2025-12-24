"""Schema Registry integration for Protobuf serialization/deserialization.

Uses Python-generated Protobuf stubs from the shared package.
"""

import struct
from dataclasses import dataclass
from typing import Any, TypeVar

import requests
from google.protobuf.message import Message as ProtobufMessage
from prometheus_client import Counter, Histogram
from tenacity import retry, stop_after_attempt, wait_exponential

from src.utils.logger import get_logger

logger = get_logger(__name__)

# Type variable for generic protobuf messages
T = TypeVar("T", bound=ProtobufMessage)

# Prometheus Metrics
schema_registry_requests = Counter(
    "schema_registry_requests_total",
    "Total schema registry requests",
    ["method", "status"],
)

schema_registry_duration = Histogram(
    "schema_registry_request_duration_seconds", "Schema registry request duration"
)

protobuf_serialization = Counter(
    "protobuf_serialization_total",
    "Protobuf serialization operations",
    ["operation", "status"],
)


# Confluent wire format constants
MAGIC_BYTE = 0
HEADER_SIZE = 5  # 1 byte magic + 4 bytes schema ID


@dataclass
class SchemaMetadata:
    """Schema metadata from registry."""

    schema_id: int
    version: int
    schema_type: str
    subject: str
    schema_content: str


class SchemaRegistryClient:
    """Client for interacting with Confluent Schema Registry."""

    def __init__(
        self,
        base_url: str,
        auth: tuple[str, str] | None = None,
        ssl_verify: bool = True,
    ):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.verify = ssl_verify
        if auth:
            self.session.auth = auth

        self.logger = get_logger(self.__class__.__name__)
        self._schema_cache: dict[int, SchemaMetadata] = {}
        self._subject_cache: dict[str, SchemaMetadata] = {}

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def get_schema_by_id(self, schema_id: int) -> SchemaMetadata | None:
        """Get schema by ID from registry."""
        # Check cache first
        if schema_id in self._schema_cache:
            return self._schema_cache[schema_id]

        try:
            response = self.session.get(
                f"{self.base_url}/schemas/ids/{schema_id}",
                timeout=10,
            )

            schema_registry_requests.labels(
                method="get_schema", status=str(response.status_code)
            ).inc()

            if response.status_code == 200:
                data = response.json()
                schema = SchemaMetadata(
                    schema_id=schema_id,
                    version=data.get("version", 0),
                    schema_type=data.get("schemaType", "PROTOBUF"),
                    subject=data.get("subject", ""),
                    schema_content=data.get("schema", ""),
                )
                self._schema_cache[schema_id] = schema
                return schema
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
                f"{self.base_url}/subjects/{subject}/versions/latest",
                timeout=10,
            )

            schema_registry_requests.labels(
                method="get_latest_schema", status=str(response.status_code)
            ).inc()

            if response.status_code == 200:
                data = response.json()
                schema = SchemaMetadata(
                    schema_id=data.get("id", 0),
                    version=data.get("version", 0),
                    schema_type=data.get("schemaType", "PROTOBUF"),
                    subject=subject,
                    schema_content=data.get("schema", ""),
                )
                self._subject_cache[subject] = schema
                return schema
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

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def register_schema(self, subject: str, schema_str: str) -> int | None:
        """Register a new schema version for a subject."""
        try:
            response = self.session.post(
                f"{self.base_url}/subjects/{subject}/versions",
                json={"schemaType": "PROTOBUF", "schema": schema_str},
                headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
                timeout=10,
            )

            schema_registry_requests.labels(
                method="register_schema", status=str(response.status_code)
            ).inc()

            if response.status_code == 200:
                result = response.json().get("id")
                if result is not None:
                    return int(result)
                return None
            else:
                response.raise_for_status()

        except requests.RequestException as e:
            self.logger.error(f"Error registering schema for subject {subject}: {e}")
            schema_registry_requests.labels(method="register_schema", status="error").inc()
            raise
        return None


class ProtobufSerializer:
    """Serializer for Protobuf messages with Schema Registry integration."""

    def __init__(self, schema_registry: SchemaRegistryClient):
        self.schema_registry = schema_registry
        self.logger = get_logger(self.__class__.__name__)
        self._schema_id_cache: dict[str, int] = {}

    def serialize(
        self,
        message: ProtobufMessage,
        subject: str,
    ) -> bytes:
        """
        Serialize Protobuf message with Confluent wire format.

        Format: [magic byte (1)] [schema ID (4)] [protobuf data]

        Args:
            message: Protobuf message instance
            subject: Schema Registry subject name

        Returns:
            Serialized bytes with Confluent header
        """
        try:
            # Get or cache schema ID
            if subject not in self._schema_id_cache:
                schema = self.schema_registry.get_latest_schema(subject)
                if schema:
                    self._schema_id_cache[subject] = schema.schema_id
                else:
                    raise ValueError(f"No schema found for subject: {subject}")

            schema_id = self._schema_id_cache[subject]

            # Serialize protobuf message
            message_bytes = message.SerializeToString()

            # Create Confluent wire format: magic byte + schema ID + message
            header = struct.pack(">bI", MAGIC_BYTE, schema_id)
            result: bytes = header + message_bytes

            protobuf_serialization.labels(operation="serialize", status="success").inc()

            self.logger.debug(
                "Message serialized",
                subject=subject,
                schema_id=schema_id,
                size=len(result),
            )

            return result

        except Exception as e:
            protobuf_serialization.labels(operation="serialize", status="error").inc()
            self.logger.error(f"Serialization error: {e}", exc_info=True)
            raise


class ProtobufDeserializer:
    """Deserializer for Protobuf messages with Schema Registry integration."""

    def __init__(self, schema_registry: SchemaRegistryClient):
        self.schema_registry = schema_registry
        self.logger = get_logger(self.__class__.__name__)
        # Map topic/subject to protobuf message class
        self._message_type_registry: dict[str, type[ProtobufMessage]] = {}

    def register_message_type(self, subject: str, message_class: type[ProtobufMessage]) -> None:
        """
        Register a Protobuf message class for a subject.

        Args:
            subject: Schema Registry subject (usually topic-value)
            message_class: Generated Protobuf message class from stub package
        """
        self._message_type_registry[subject] = message_class
        self.logger.info(f"Registered message type {message_class.__name__} for subject {subject}")

    def extract_schema_id(self, data: bytes) -> int | None:
        """Extract schema ID from Confluent wire format."""
        if len(data) < HEADER_SIZE:
            return None

        if data[0] != MAGIC_BYTE:
            self.logger.warning(f"Invalid magic byte: {data[0]}, expected {MAGIC_BYTE}")
            return None

        schema_id: int = struct.unpack(">I", data[1:5])[0]
        return schema_id

    def deserialize(
        self,
        data: bytes,
        subject: str,
    ) -> tuple[ProtobufMessage | dict[str, Any] | None, str | None]:
        """
        Deserialize Protobuf message from Confluent wire format.

        Args:
            data: Raw bytes with Confluent header
            subject: Schema Registry subject for message type lookup

        Returns:
            Tuple of (deserialized message, error message)
        """
        try:
            if len(data) < HEADER_SIZE:
                return None, "Message too short for Confluent wire format"

            # Extract schema ID
            schema_id = self.extract_schema_id(data)
            if schema_id is None:
                return None, "Could not extract schema ID"

            # Get message payload (skip header)
            message_bytes = data[HEADER_SIZE:]

            # Look up registered message type
            if subject in self._message_type_registry:
                message_class = self._message_type_registry[subject]
                message = message_class()
                message.ParseFromString(message_bytes)

                protobuf_serialization.labels(operation="deserialize", status="success").inc()

                self.logger.debug(
                    "Message deserialized",
                    subject=subject,
                    schema_id=schema_id,
                    message_type=message_class.__name__,
                )

                return message, None
            else:
                # No registered type, return dict with metadata
                self.logger.warning(f"No message type registered for subject: {subject}")

                # Verify schema exists
                schema = self.schema_registry.get_schema_by_id(schema_id)

                return {
                    "schema_id": schema_id,
                    "schema_subject": schema.subject if schema else None,
                    "schema_version": schema.version if schema else None,
                    "raw_payload": message_bytes,
                }, None

        except Exception as e:
            protobuf_serialization.labels(operation="deserialize", status="error").inc()
            error_msg = f"Deserialization error: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            return None, error_msg

    def deserialize_to_dict(
        self,
        data: bytes,
        subject: str,
    ) -> tuple[dict[str, Any] | None, str | None]:
        """
        Deserialize Protobuf message and convert to dictionary.

        Args:
            data: Raw bytes with Confluent header
            subject: Schema Registry subject

        Returns:
            Tuple of (dict representation, error message)
        """
        from google.protobuf.json_format import MessageToDict

        message, error = self.deserialize(data, subject)

        if error:
            return None, error

        if isinstance(message, dict):
            return message, None

        if isinstance(message, ProtobufMessage):
            return MessageToDict(message, preserving_proto_field_name=True), None

        return None, "Unknown message type"


# Topic to subject naming conventions
def get_value_subject(topic: str) -> str:
    """Get Schema Registry subject name for topic value."""
    return f"{topic}-value"


def get_key_subject(topic: str) -> str:
    """Get Schema Registry subject name for topic key."""
    return f"{topic}-key"
