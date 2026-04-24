"""Secrets management with Infisical integration and environment variable fallbacks."""

import logging
import os
from functools import lru_cache
from typing import Any

import requests
from prometheus_client import Counter, Histogram
from tenacity import retry, stop_after_attempt, wait_exponential

from src.utils.logger import get_logger

logger = logging.getLogger(__name__)

# Prometheus Metrics
secrets_requests = Counter("secrets_requests_total", "Total secrets requests", ["source", "status"])

secrets_duration = Histogram("secrets_request_duration_seconds", "Secrets request duration")


class InfisicalClient:
    """Client for Infisical secrets management."""

    def __init__(
        self,
        base_url: str,
        service_token: str | None = None,
        project_id: str | None = None,
        environment: str = "development",
    ):
        self.base_url = base_url.rstrip("/")
        self.service_token = service_token
        self.project_id = project_id
        self.environment = environment
        self.logger = get_logger(self.__class__.__name__)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def get_secret(self, key: str, path: str = "/") -> str | None:
        """Get secret from Infisical."""
        if not self.service_token or not self.project_id:
            self.logger.warning("Infisical client not properly configured")
            return None

        try:
            headers = {
                "Authorization": f"Bearer {self.service_token}",
                "Content-Type": "application/json",
            }

            params = {"environment": self.environment, "path": path, "secretPath": f"{path}{key}"}

            response = requests.get(
                f"{self.base_url}/api/v3/secrets/{key}", headers=headers, params=params, timeout=10
            )

            secrets_requests.labels(source="infisical", status=str(response.status_code)).inc()

            if response.status_code == 200:
                data: dict[str, Any] = response.json()
                secret: str | None = data.get("secret", {}).get("secretValue")
                self.logger.debug(f"Secret retrieved from Infisical: {key}")
                return secret
            elif response.status_code == 404:
                self.logger.warning(f"Secret not found in Infisical: {key}")
                return None
            else:
                response.raise_for_status()

        except requests.RequestException as e:
            self.logger.error(f"Error fetching secret from Infisical {key}: {e}")
            secrets_requests.labels(source="infisical", status="error").inc()
            raise
        return None


class SecretsManager:
    """Unified secrets management with Infisical and environment variable fallbacks."""

    def __init__(
        self, infisical_client: InfisicalClient | None = None, enable_infisical: bool = True
    ):
        self.infisical = infisical_client
        self.enable_infisical = enable_infisical
        self.logger = get_logger(self.__class__.__name__)

        # Cache for secrets to avoid repeated API calls
        self._cache: dict[str, str] = {}

    @lru_cache(maxsize=100)  # noqa: B019
    def get_secret(self, key: str, path: str = "/", default: str | None = None) -> str | None:
        """
        Get secret with fallback priority:
        1. Cache
        2. Environment variable
        3. Infisical
        4. Default value
        """
        # Check cache first
        cache_key = f"{path}:{key}"
        if cache_key in self._cache:
            self.logger.debug(f"Secret retrieved from cache: {key}")
            return self._cache[cache_key]

        # Try environment variable
        env_value = os.getenv(key)
        if env_value is not None:
            self.logger.debug(f"Secret retrieved from environment: {key}")
            self._cache[cache_key] = env_value
            secrets_requests.labels(source="env", status="success").inc()
            return env_value

        # Try Infisical if enabled
        if self.enable_infisical and self.infisical:
            try:
                infisical_value = self.infisical.get_secret(key, path)
                if infisical_value is not None:
                    self.logger.debug(f"Secret retrieved from Infisical: {key}")
                    self._cache[cache_key] = infisical_value
                    secrets_requests.labels(source="infisical", status="success").inc()
                    return infisical_value
            except Exception as e:
                self.logger.warning(f"Failed to get secret from Infisical {key}: {e}")
                secrets_requests.labels(source="infisical", status="error").inc()

        # Return default if provided
        if default is not None:
            self.logger.debug(f"Using default value for secret: {key}")
            return default

        self.logger.warning(f"Secret not found: {key}")
        secrets_requests.labels(source="none", status="not_found").inc()
        return None

    def get_required_secret(self, key: str, path: str = "/") -> str:
        """Get required secret, raise exception if not found."""
        value = self.get_secret(key, path)
        if value is None:
            raise ValueError(f"Required secret not found: {key}")
        return value

    def get_kafka_config(self) -> dict[str, Any]:
        """Get Kafka configuration from secrets."""
        return {
            "bootstrap_servers": self.get_secret(
                "KAFKA_BOOTSTRAP_SERVERS", default="localhost:9092"
            ),
            "security_protocol": self.get_secret("KAFKA_SECURITY_PROTOCOL", default="PLAINTEXT"),
            "sasl_mechanism": self.get_secret("KAFKA_SASL_MECHANISM"),
            "sasl_username": self.get_secret("KAFKA_SASL_USERNAME"),
            "sasl_password": self.get_secret("KAFKA_SASL_PASSWORD"),
            "ssl_ca_location": self.get_secret("KAFKA_SSL_CA_LOCATION"),
            "ssl_certificate_location": self.get_secret("KAFKA_SSL_CERTIFICATE_LOCATION"),
            "ssl_key_location": self.get_secret("KAFKA_SSL_KEY_LOCATION"),
        }

    def get_schema_registry_config(self) -> dict[str, Any]:
        """Get Schema Registry configuration from secrets."""
        return {
            "url": self.get_secret("SCHEMA_REGISTRY_URL", default="http://localhost:8081"),
            "username": self.get_secret("SCHEMA_REGISTRY_USERNAME"),
            "password": self.get_secret("SCHEMA_REGISTRY_PASSWORD"),
            "ssl_ca_location": self.get_secret("SCHEMA_REGISTRY_SSL_CA_LOCATION"),
        }

    def get_redis_config(self) -> dict[str, Any]:
        """Get Redis configuration from secrets."""
        return {
            "host": self.get_secret("REDIS_HOST", default="localhost"),
            "port": int(self.get_secret("REDIS_PORT", default="6379") or "6379"),
            "password": self.get_secret("REDIS_PASSWORD"),
            "db": int(self.get_secret("REDIS_DB", default="0") or "0"),
            "ssl": (self.get_secret("REDIS_SSL", default="false") or "false").lower() == "true",
        }

    def get_s3_config(self) -> dict[str, Any]:
        """Get S3/MinIO configuration from secrets."""
        return {
            "endpoint_url": self.get_secret("S3_ENDPOINT_URL"),
            "access_key_id": self.get_secret("AWS_ACCESS_KEY_ID"),
            "secret_access_key": self.get_secret("AWS_SECRET_ACCESS_KEY"),
            "bucket_name": self.get_secret("S3_BUCKET_NAME", default="togather-ml-features"),
            "region": self.get_secret("AWS_REGION", default="us-east-1"),
        }

    def get_minio_config(self) -> dict[str, Any]:
        """Get MinIO-specific configuration from secrets."""
        return {
            "endpoint_url": self.get_secret("MINIO_ENDPOINT_URL", default="localhost:9000"),
            "access_key_id": self.get_secret("MINIO_ACCESS_KEY", default="minioadmin"),
            "secret_access_key": self.get_secret("MINIO_SECRET_KEY", default="minioadmin"),
            "bucket_name": self.get_secret("MINIO_BUCKET_NAME", default="togather-ml-features"),
            "secure": (self.get_secret("MINIO_SECURE", default="false") or "false").lower()
            == "true",
            "region": self.get_secret("MINIO_REGION", default="us-east-1"),
        }

    def clear_cache(self) -> None:
        """Clear secrets cache."""
        self._cache.clear()
        self.get_secret.cache_clear()
        self.logger.info("Secrets cache cleared")


# Global secrets manager instance
_secrets_manager: SecretsManager | None = None


def get_secrets_manager() -> SecretsManager:
    """Get global secrets manager instance."""
    global _secrets_manager

    if _secrets_manager is None:
        # Initialize Infisical client if configured
        infisical_client = None
        if os.getenv("INFISICAL_SERVICE_TOKEN") and os.getenv("INFISICAL_PROJECT_ID"):
            infisical_client = InfisicalClient(
                base_url=os.getenv("INFISICAL_URL", "https://app.infisical.com"),
                service_token=os.getenv("INFISICAL_SERVICE_TOKEN"),
                project_id=os.getenv("INFISICAL_PROJECT_ID"),
                environment=os.getenv("INFISICAL_ENVIRONMENT", "development"),
            )

        _secrets_manager = SecretsManager(
            infisical_client=infisical_client,
            enable_infisical=bool(os.getenv("ENABLE_INFISICAL", "true").lower() == "true"),
        )

    return _secrets_manager


# Convenience functions
def get_secret(key: str, path: str = "/", default: str | None = None) -> str | None:
    """Get secret using global secrets manager."""
    return get_secrets_manager().get_secret(key, path, default)


def get_required_secret(key: str, path: str = "/") -> str:
    """Get required secret using global secrets manager."""
    return get_secrets_manager().get_required_secret(key, path)
