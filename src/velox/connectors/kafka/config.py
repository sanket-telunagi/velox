"""
Kafka configuration models.

Provides type-safe configuration for Kafka connections with
support for multiple authentication methods and environments.
"""

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, SecretStr, field_validator


class SecurityProtocol(str, Enum):
    """Kafka security protocols."""

    PLAINTEXT = "PLAINTEXT"
    SSL = "SSL"
    SASL_PLAINTEXT = "SASL_PLAINTEXT"
    SASL_SSL = "SASL_SSL"


class SASLMechanism(str, Enum):
    """SASL authentication mechanisms."""

    PLAIN = "PLAIN"
    SCRAM_SHA_256 = "SCRAM-SHA-256"
    SCRAM_SHA_512 = "SCRAM-SHA-512"
    GSSAPI = "GSSAPI"
    OAUTHBEARER = "OAUTHBEARER"


class SSLConfig(BaseModel):
    """SSL/TLS configuration."""

    cafile: str | None = Field(default=None, description="Path to CA certificate file")
    certfile: str | None = Field(default=None, description="Path to client certificate file")
    keyfile: str | None = Field(default=None, description="Path to client private key file")
    password: SecretStr | None = Field(default=None, description="Password for private key")
    check_hostname: bool = Field(default=True, description="Verify server hostname")
    ciphers: str | None = Field(default=None, description="SSL cipher suites")

    def to_aiokafka_config(self) -> dict[str, Any]:
        """Convert to aiokafka SSL configuration."""
        config: dict[str, Any] = {}
        
        if self.cafile:
            config["ssl_cafile"] = self.cafile
        if self.certfile:
            config["ssl_certfile"] = self.certfile
        if self.keyfile:
            config["ssl_keyfile"] = self.keyfile
        if self.password:
            config["ssl_password"] = self.password.get_secret_value()
        if self.ciphers:
            config["ssl_ciphers"] = self.ciphers
        
        return config


class SASLConfig(BaseModel):
    """SASL authentication configuration."""

    mechanism: SASLMechanism = Field(
        default=SASLMechanism.PLAIN,
        description="SASL mechanism"
    )
    username: str | None = Field(default=None, description="SASL username")
    password: SecretStr | None = Field(default=None, description="SASL password")
    
    # For GSSAPI/Kerberos
    kerberos_service_name: str = Field(
        default="kafka",
        description="Kerberos service name"
    )
    kerberos_domain_name: str | None = Field(
        default=None,
        description="Kerberos domain name"
    )
    
    # For OAUTHBEARER
    oauth_token: SecretStr | None = Field(
        default=None,
        description="OAuth bearer token"
    )

    def to_aiokafka_config(self) -> dict[str, Any]:
        """Convert to aiokafka SASL configuration."""
        config: dict[str, Any] = {
            "sasl_mechanism": self.mechanism.value,
        }
        
        if self.username:
            config["sasl_plain_username"] = self.username
        if self.password:
            config["sasl_plain_password"] = self.password.get_secret_value()
        if self.kerberos_service_name:
            config["sasl_kerberos_service_name"] = self.kerberos_service_name
        if self.kerberos_domain_name:
            config["sasl_kerberos_domain_name"] = self.kerberos_domain_name
        
        return config


class KafkaConfig(BaseModel):
    """
    Complete Kafka configuration.
    
    Supports all authentication methods and common producer/consumer settings.
    """

    # Connection settings
    bootstrap_servers: str = Field(
        description="Comma-separated list of bootstrap servers"
    )
    security_protocol: SecurityProtocol = Field(
        default=SecurityProtocol.PLAINTEXT,
        description="Security protocol to use"
    )
    
    # Authentication
    ssl: SSLConfig | None = Field(default=None, description="SSL configuration")
    sasl: SASLConfig | None = Field(default=None, description="SASL configuration")
    
    # Client settings
    client_id: str | None = Field(
        default="etl-connectors",
        description="Client identifier"
    )
    
    # Timeouts (in milliseconds)
    request_timeout_ms: int = Field(
        default=30000,
        description="Request timeout in milliseconds"
    )
    connections_max_idle_ms: int = Field(
        default=540000,
        description="Max idle time for connections"
    )
    
    # Metadata settings
    metadata_max_age_ms: int = Field(
        default=300000,
        description="Max age of metadata before refresh"
    )

    @field_validator("bootstrap_servers")
    @classmethod
    def validate_bootstrap_servers(cls, v: str) -> str:
        """Validate bootstrap servers format."""
        if not v or not v.strip():
            raise ValueError("bootstrap_servers cannot be empty")
        return v.strip()

    def to_aiokafka_config(self) -> dict[str, Any]:
        """
        Convert to aiokafka configuration dictionary.
        
        Returns:
            Dictionary of configuration options for aiokafka.
        """
        config: dict[str, Any] = {
            "bootstrap_servers": self.bootstrap_servers,
            "security_protocol": self.security_protocol.value,
            "client_id": self.client_id,
            "request_timeout_ms": self.request_timeout_ms,
            "connections_max_idle_ms": self.connections_max_idle_ms,
            "metadata_max_age_ms": self.metadata_max_age_ms,
        }
        
        # Add SSL config if needed
        if self.security_protocol in (SecurityProtocol.SSL, SecurityProtocol.SASL_SSL):
            if self.ssl:
                config.update(self.ssl.to_aiokafka_config())
        
        # Add SASL config if needed
        if self.security_protocol in (SecurityProtocol.SASL_PLAINTEXT, SecurityProtocol.SASL_SSL):
            if self.sasl:
                config.update(self.sasl.to_aiokafka_config())
        
        return config


class ProducerConfig(BaseModel):
    """Kafka producer-specific configuration."""

    # Batching
    batch_size: int = Field(
        default=16384,
        description="Batch size in bytes"
    )
    linger_ms: int = Field(
        default=5,
        description="Time to wait for more messages before sending batch"
    )
    
    # Buffering
    buffer_memory: int = Field(
        default=33554432,
        description="Total memory for buffering"
    )
    max_request_size: int = Field(
        default=1048576,
        description="Maximum size of a request"
    )
    
    # Reliability
    acks: str = Field(
        default="all",
        description="Acknowledgment mode: 0, 1, or 'all'"
    )
    retries: int = Field(
        default=3,
        description="Number of retries"
    )
    retry_backoff_ms: int = Field(
        default=100,
        description="Backoff time between retries"
    )
    
    # Compression
    compression_type: str = Field(
        default="none",
        description="Compression type: none, gzip, snappy, lz4, zstd"
    )
    
    # Idempotence
    enable_idempotence: bool = Field(
        default=False,
        description="Enable idempotent producer"
    )

    def to_aiokafka_config(self) -> dict[str, Any]:
        """Convert to aiokafka producer configuration."""
        return {
            "batch_size": self.batch_size,
            "linger_ms": self.linger_ms,
            "max_request_size": self.max_request_size,
            "acks": self.acks,
            "retries": self.retries,
            "retry_backoff_ms": self.retry_backoff_ms,
            "compression_type": self.compression_type,
            "enable_idempotence": self.enable_idempotence,
        }


class ConsumerConfig(BaseModel):
    """Kafka consumer-specific configuration."""

    # Group settings
    group_id: str | None = Field(
        default=None,
        description="Consumer group ID"
    )
    
    # Offset management
    auto_offset_reset: str = Field(
        default="earliest",
        description="Where to start reading: earliest, latest, none"
    )
    enable_auto_commit: bool = Field(
        default=True,
        description="Auto-commit offsets"
    )
    auto_commit_interval_ms: int = Field(
        default=5000,
        description="Auto-commit interval"
    )
    
    # Fetching
    fetch_min_bytes: int = Field(
        default=1,
        description="Minimum bytes to fetch"
    )
    fetch_max_bytes: int = Field(
        default=52428800,
        description="Maximum bytes to fetch"
    )
    fetch_max_wait_ms: int = Field(
        default=500,
        description="Max wait time for fetch"
    )
    max_poll_records: int = Field(
        default=500,
        description="Max records per poll"
    )
    
    # Session
    session_timeout_ms: int = Field(
        default=10000,
        description="Session timeout"
    )
    heartbeat_interval_ms: int = Field(
        default=3000,
        description="Heartbeat interval"
    )
    max_poll_interval_ms: int = Field(
        default=300000,
        description="Max interval between polls"
    )

    def to_aiokafka_config(self) -> dict[str, Any]:
        """Convert to aiokafka consumer configuration."""
        config: dict[str, Any] = {
            "auto_offset_reset": self.auto_offset_reset,
            "enable_auto_commit": self.enable_auto_commit,
            "auto_commit_interval_ms": self.auto_commit_interval_ms,
            "fetch_min_bytes": self.fetch_min_bytes,
            "fetch_max_bytes": self.fetch_max_bytes,
            "fetch_max_wait_ms": self.fetch_max_wait_ms,
            "max_poll_records": self.max_poll_records,
            "session_timeout_ms": self.session_timeout_ms,
            "heartbeat_interval_ms": self.heartbeat_interval_ms,
            "max_poll_interval_ms": self.max_poll_interval_ms,
        }
        
        if self.group_id:
            config["group_id"] = self.group_id
        
        return config


class KafkaEnvironmentConfig(BaseModel):
    """
    Complete Kafka environment configuration.
    
    Combines connection, producer, and consumer settings for an environment.
    """

    environment: str = Field(description="Environment name")
    kafka: KafkaConfig = Field(description="Kafka connection configuration")
    producer: ProducerConfig = Field(
        default_factory=ProducerConfig,
        description="Producer configuration"
    )
    consumer: ConsumerConfig = Field(
        default_factory=ConsumerConfig,
        description="Consumer configuration"
    )
    
    # Optional overrides per environment
    extra: dict[str, Any] = Field(
        default_factory=dict,
        description="Extra configuration options"
    )

    def get_producer_config(self) -> dict[str, Any]:
        """Get combined configuration for producer."""
        config = self.kafka.to_aiokafka_config()
        config.update(self.producer.to_aiokafka_config())
        config.update(self.extra)
        return config

    def get_consumer_config(self) -> dict[str, Any]:
        """Get combined configuration for consumer."""
        config = self.kafka.to_aiokafka_config()
        config.update(self.consumer.to_aiokafka_config())
        config.update(self.extra)
        return config

