"""
Redis Sink for Flink Feature Pipeline.

Writes session features to Redis for real-time inference.
"""

import hashlib
import json
import os
from typing import Any, cast

import redis
from pydantic import BaseModel, Field

# Note: Using print() for logging in Flink to avoid structlog compatibility issues


class RedisSinkConfig(BaseModel):
    """Configuration for Redis sink."""

    host: str = Field(default="localhost", description="Redis host")
    port: int = Field(default=6379, description="Redis port")
    db: int = Field(default=0, description="Redis database number")
    password: str | None = Field(default=None, description="Redis password")
    key_prefix: str = Field(default="session", description="Key prefix for features")
    ttl_seconds: int = Field(default=3600, description="TTL for stored features (1 hour)")
    connection_timeout: int = Field(default=5, description="Connection timeout in seconds")

    @classmethod
    def from_env(cls) -> "RedisSinkConfig":
        """Create config from environment variables."""
        return cls(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", "6379")),
            db=int(os.getenv("REDIS_DB", "0")),
            password=os.getenv("REDIS_PASSWORD"),
            key_prefix=os.getenv("REDIS_KEY_PREFIX", "session"),
            ttl_seconds=int(os.getenv("REDIS_TTL_SECONDS", "3600")),
        )


class RedisSink:
    """
    Redis sink for storing session features.

    Uses connection pooling for efficient connection management.
    Writes features as Redis hashes with TTL for automatic expiration.

    Key pattern: {prefix}:{user_id}:{session_id}
    Example: session:user_0001:abc123

    Usage:
        config = RedisSinkConfig.from_env()
        sink = RedisSink(config)

        features = SessionFeatures(...)
        sink.write_session_features(features)
    """

    def __init__(self, config: RedisSinkConfig | None = None):
        self.config = config or RedisSinkConfig.from_env()
        self._pool: redis.ConnectionPool | None = None
        self._client: redis.Redis | None = None
        self._connected = False
        # Lazy initialization - don't connect in constructor

    def _ensure_connected(self) -> bool:
        """Ensure Redis client is connected using connection pool. Returns True if connected."""
        if self._connected and self._client:
            return True

        try:
            # Create connection pool if not exists
            if self._pool is None:
                self._pool = redis.ConnectionPool(
                    host=self.config.host,
                    port=self.config.port,
                    db=self.config.db,
                    password=self.config.password,
                    max_connections=20,  # Limit max connections
                    socket_timeout=self.config.connection_timeout,
                    socket_connect_timeout=self.config.connection_timeout,
                    retry_on_timeout=True,
                    decode_responses=True,
                )

            # Create client from pool
            self._client = redis.Redis(connection_pool=self._pool)

            # Test connection
            self._client.ping()
            self._connected = True
            print(f"Redis sink connected (pooled): {self.config.host}:{self.config.port}")
            return True
        except redis.ConnectionError as e:
            print(f"Redis connection failed (will retry): {e}")
            self._client = None
            self._connected = False
            return False

    def is_healthy(self) -> tuple[bool, str]:
        """Check if Redis connection is healthy. Used by health check server."""
        try:
            if not self._ensure_connected() or self._client is None:
                return False, "Not connected"

            # Ping to verify connection is alive
            self._client.ping()

            # Get memory info for diagnostics
            info = self._client.info("memory")
            used_memory = info.get("used_memory") if isinstance(info, dict) else 0
            used_memory_mb = (used_memory if used_memory else 0) / 1024 / 1024

            # Check pool stats if available
            pool_info = ""
            if self._pool:
                in_use = len(self._pool._in_use_connections)
                available = len(self._pool._available_connections)
                pool_info = f", pool: {in_use} in-use, {available} available"

            return True, f"Connected, memory: {used_memory_mb:.1f}MB{pool_info}"
        except Exception as e:
            return False, str(e)

    def _compute_hash(self, features: dict[str, Any]) -> str:
        """Compute hash of features for change detection."""
        # Exclude metadata fields from hash
        exclude_keys = {"_hash", "updated_at", "created_at", "timestamp"}
        feature_data = {k: v for k, v in features.items() if k not in exclude_keys}
        return hashlib.md5(
            json.dumps(feature_data, sort_keys=True, default=str).encode()
        ).hexdigest()

    def write_session_features(
        self,
        user_id: str,
        session_id: str,
        features: dict[str, Any],
        ttl_seconds: int | None = None,
    ) -> str | None:
        """
        Write session features to Redis (only if changed).

        Uses hash-based change detection to avoid redundant writes.

        Args:
            user_id: User identifier
            session_id: Session identifier
            features: Feature dictionary
            ttl_seconds: Optional TTL override

        Returns:
            Redis key where features were stored, or None if unchanged/unavailable
        """
        if not self._ensure_connected():
            print(f"Redis unavailable, skipping write for {user_id}")
            return None

        if self._client is None:
            return None

        key = f"{self.config.key_prefix}:{user_id}:{session_id}"
        ttl = ttl_seconds or self.config.ttl_seconds

        try:
            # Compute hash of new features
            new_hash = self._compute_hash(features)

            # Check existing hash in Redis
            existing_hash = self._client.hget(key, "_hash")

            # Skip write if content unchanged
            if existing_hash == new_hash:
                return None  # No change, skip write

            # Content changed - write features with hash
            features_with_hash = {**features, "_hash": new_hash}

            # Use pipeline for atomic operation
            pipe = self._client.pipeline()
            pipe.hset(key, mapping=features_with_hash)
            pipe.expire(key, ttl)
            pipe.execute()

            print(f"Written changed features for {user_id}:{session_id}")
            return key

        except redis.RedisError as e:
            print(f"Failed to write to Redis: {e}")
            self._connected = False  # Mark for reconnection
            return None

    def write_user_embedding(
        self,
        user_id: str,
        embedding: list[float],
        ttl_seconds: int | None = None,
    ) -> str | None:
        """
        Write user embedding to Redis.

        Stores embedding as a comma-separated string for compatibility.

        Args:
            user_id: User identifier
            embedding: Embedding vector
            ttl_seconds: Optional TTL override

        Returns:
            Redis key where embedding was stored, or None if unavailable
        """
        if not self._ensure_connected():
            return None

        key = f"embedding:user:{user_id}"
        ttl = ttl_seconds or self.config.ttl_seconds

        try:
            # Store as comma-separated string for simplicity
            embedding_str = ",".join(str(v) for v in embedding)
            if self._client is None:
                return None
            pipe = self._client.pipeline()
            pipe.set(key, embedding_str)
            pipe.expire(key, ttl)
            pipe.execute()

            return key

        except redis.RedisError as e:
            print(f"Failed to write embedding to Redis: {e}")
            self._connected = False
            return None

    def get_session_features(self, user_id: str, session_id: str) -> dict[str, str] | None:
        """
        Get session features from Redis.

        Args:
            user_id: User identifier
            session_id: Session identifier

        Returns:
            Feature dictionary or None if not found
        """
        if not self._ensure_connected():
            return None

        key = f"{self.config.key_prefix}:{user_id}:{session_id}"

        try:
            if self._client is None:
                return None
            features = self._client.hgetall(key)
            return cast(dict[str, str], features) if features else None
        except redis.RedisError as e:
            print(f"Failed to read from Redis: {e}")
            return None

    def get_user_embedding(self, user_id: str) -> list[float] | None:
        """
        Get user embedding from Redis.

        Args:
            user_id: User identifier

        Returns:
            Embedding vector or None if not found
        """
        if not self._ensure_connected():
            return None

        key = f"embedding:user:{user_id}"

        try:
            if self._client is None:
                return None
            embedding_str = self._client.get(key)
            if embedding_str and isinstance(embedding_str, str):
                return [float(v) for v in embedding_str.split(",")]
            return None
        except redis.RedisError as e:
            print(f"Failed to read embedding from Redis: {e}")
            return None

    def delete_session(self, user_id: str, session_id: str) -> bool:
        """Delete session features from Redis."""
        if not self._client:
            return False

        key = f"{self.config.key_prefix}:{user_id}:{session_id}"
        result = self._client.delete(key)
        return bool(result)

    def close(self) -> None:
        """Close Redis connection and pool."""
        if self._client:
            self._client.close()
            self._client = None

        if self._pool:
            self._pool.disconnect()
            self._pool = None

        self._connected = False
        print("Redis connection and pool closed")
