"""
Kafka authentication handlers.

Supports:
- SASL_SSL (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)
- SSL (mutual TLS)
- PLAINTEXT (no authentication)
"""

import ssl
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import structlog

from velox.core.exceptions import AuthenticationError
from velox.connectors.kafka.config import (
    KafkaConfig,
    SASLMechanism,
    SecurityProtocol,
)

logger = structlog.get_logger()


@dataclass
class AuthConfig:
    """Base authentication configuration."""

    security_protocol: SecurityProtocol = SecurityProtocol.PLAINTEXT
    
    # SSL settings
    ssl_cafile: str | None = None
    ssl_certfile: str | None = None
    ssl_keyfile: str | None = None
    ssl_password: str | None = None
    ssl_check_hostname: bool = True
    
    # SASL settings
    sasl_mechanism: SASLMechanism | None = None
    sasl_username: str | None = None
    sasl_password: str | None = None


class AuthHandler(ABC):
    """
    Abstract base class for authentication handlers.
    
    Each handler is responsible for:
    - Validating configuration
    - Building SSL context if needed
    - Providing aiokafka-compatible configuration
    """

    def __init__(self, config: AuthConfig) -> None:
        self._config = config
        self._ssl_context: ssl.SSLContext | None = None

    @property
    def config(self) -> AuthConfig:
        """Get the authentication configuration."""
        return self._config

    @property
    def ssl_context(self) -> ssl.SSLContext | None:
        """Get the SSL context if configured."""
        return self._ssl_context

    @abstractmethod
    def validate(self) -> None:
        """
        Validate the authentication configuration.
        
        Raises:
            AuthenticationError: If configuration is invalid.
        """
        ...

    @abstractmethod
    def to_aiokafka_config(self) -> dict[str, Any]:
        """
        Convert to aiokafka configuration.
        
        Returns:
            Dictionary of configuration options.
        """
        ...

    def _build_ssl_context(self) -> ssl.SSLContext | None:
        """
        Build SSL context from configuration.
        
        Returns:
            SSLContext or None if SSL not configured.
        """
        if self._config.security_protocol not in (
            SecurityProtocol.SSL,
            SecurityProtocol.SASL_SSL,
        ):
            return None

        try:
            context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
            
            # Load CA certificate
            if self._config.ssl_cafile:
                ca_path = Path(self._config.ssl_cafile)
                if not ca_path.exists():
                    raise AuthenticationError(
                        f"CA certificate file not found: {ca_path}",
                        auth_type="ssl",
                    )
                context.load_verify_locations(cafile=str(ca_path))
            
            # Load client certificate and key for mutual TLS
            if self._config.ssl_certfile and self._config.ssl_keyfile:
                cert_path = Path(self._config.ssl_certfile)
                key_path = Path(self._config.ssl_keyfile)
                
                if not cert_path.exists():
                    raise AuthenticationError(
                        f"Client certificate file not found: {cert_path}",
                        auth_type="ssl",
                    )
                if not key_path.exists():
                    raise AuthenticationError(
                        f"Client key file not found: {key_path}",
                        auth_type="ssl",
                    )
                
                context.load_cert_chain(
                    certfile=str(cert_path),
                    keyfile=str(key_path),
                    password=self._config.ssl_password,
                )
            
            # Configure hostname checking
            context.check_hostname = self._config.ssl_check_hostname
            
            logger.debug(
                "ssl_context_created",
                cafile=self._config.ssl_cafile,
                certfile=self._config.ssl_certfile,
                check_hostname=self._config.ssl_check_hostname,
            )
            
            return context
            
        except ssl.SSLError as e:
            raise AuthenticationError(
                f"Failed to create SSL context: {e}",
                auth_type="ssl",
            ) from e

    @classmethod
    def from_kafka_config(cls, kafka_config: KafkaConfig) -> "AuthHandler":
        """
        Create appropriate handler from KafkaConfig.
        
        Args:
            kafka_config: Kafka configuration.
            
        Returns:
            Appropriate AuthHandler subclass instance.
        """
        auth_config = AuthConfig(
            security_protocol=kafka_config.security_protocol,
        )
        
        # Add SSL settings if present
        if kafka_config.ssl:
            auth_config.ssl_cafile = kafka_config.ssl.cafile
            auth_config.ssl_certfile = kafka_config.ssl.certfile
            auth_config.ssl_keyfile = kafka_config.ssl.keyfile
            if kafka_config.ssl.password:
                auth_config.ssl_password = kafka_config.ssl.password.get_secret_value()
            auth_config.ssl_check_hostname = kafka_config.ssl.check_hostname
        
        # Add SASL settings if present
        if kafka_config.sasl:
            auth_config.sasl_mechanism = kafka_config.sasl.mechanism
            auth_config.sasl_username = kafka_config.sasl.username
            if kafka_config.sasl.password:
                auth_config.sasl_password = kafka_config.sasl.password.get_secret_value()
        
        # Return appropriate handler
        if kafka_config.security_protocol == SecurityProtocol.SASL_SSL:
            return SASLSSLAuth(auth_config)
        elif kafka_config.security_protocol == SecurityProtocol.SSL:
            return SSLAuth(auth_config)
        elif kafka_config.security_protocol == SecurityProtocol.SASL_PLAINTEXT:
            return SASLPlaintextAuth(auth_config)
        else:
            return PlaintextAuth(auth_config)


class PlaintextAuth(AuthHandler):
    """Handler for PLAINTEXT connections (no authentication)."""

    def validate(self) -> None:
        """Plaintext requires no validation."""
        logger.warning(
            "plaintext_auth",
            message="Using PLAINTEXT protocol - no encryption or authentication",
        )

    def to_aiokafka_config(self) -> dict[str, Any]:
        """Return minimal config for plaintext."""
        return {
            "security_protocol": SecurityProtocol.PLAINTEXT.value,
        }


class SSLAuth(AuthHandler):
    """
    Handler for SSL/TLS connections (mutual TLS).
    
    Requires:
    - CA certificate for server verification (ssl_cafile is mandatory)
    - Optional: client certificate and key for mutual TLS authentication
    """

    def validate(self) -> None:
        """Validate SSL configuration."""
        # SSL context is mandatory - requires CA certificate
        if not self._config.ssl_cafile:
            raise AuthenticationError(
                "SSL authentication requires ssl_cafile for SSL context",
                auth_type="ssl",
            )
        
        # For mutual TLS, both cert and key are required
        if self._config.ssl_certfile and not self._config.ssl_keyfile:
            raise AuthenticationError(
                "SSL client certificate requires ssl_keyfile",
                auth_type="ssl",
            )
        if self._config.ssl_keyfile and not self._config.ssl_certfile:
            raise AuthenticationError(
                "SSL client key requires ssl_certfile",
                auth_type="ssl",
            )
        
        # Build and validate SSL context (mandatory)
        self._ssl_context = self._build_ssl_context()
        
        if not self._ssl_context:
            raise AuthenticationError(
                "Failed to create SSL context for SSL authentication",
                auth_type="ssl",
            )
        
        logger.info(
            "ssl_auth_configured",
            cafile=self._config.ssl_cafile,
            mutual_tls=bool(self._config.ssl_certfile),
        )

    def to_aiokafka_config(self) -> dict[str, Any]:
        """Return SSL configuration."""
        return {
            "security_protocol": SecurityProtocol.SSL.value,
            "ssl_context": self._ssl_context,  # Always include SSL context
        }


class SASLPlaintextAuth(AuthHandler):
    """
    Handler for SASL_PLAINTEXT connections.
    
    Warning: Credentials are sent in plaintext - use only for testing.
    """

    def validate(self) -> None:
        """Validate SASL configuration."""
        if not self._config.sasl_mechanism:
            raise AuthenticationError(
                "SASL authentication requires sasl_mechanism",
                auth_type="sasl",
            )
        
        # PLAIN and SCRAM require username/password
        if self._config.sasl_mechanism in (
            SASLMechanism.PLAIN,
            SASLMechanism.SCRAM_SHA_256,
            SASLMechanism.SCRAM_SHA_512,
        ):
            if not self._config.sasl_username:
                raise AuthenticationError(
                    f"{self._config.sasl_mechanism.value} requires sasl_username",
                    auth_type="sasl",
                )
            if not self._config.sasl_password:
                raise AuthenticationError(
                    f"{self._config.sasl_mechanism.value} requires sasl_password",
                    auth_type="sasl",
                )
        
        logger.warning(
            "sasl_plaintext_auth",
            message="Using SASL_PLAINTEXT - credentials sent unencrypted",
            mechanism=self._config.sasl_mechanism.value if self._config.sasl_mechanism else None,
        )

    def to_aiokafka_config(self) -> dict[str, Any]:
        """Return SASL configuration."""
        config: dict[str, Any] = {
            "security_protocol": SecurityProtocol.SASL_PLAINTEXT.value,
            "sasl_mechanism": self._config.sasl_mechanism.value if self._config.sasl_mechanism else None,
        }
        
        if self._config.sasl_username:
            config["sasl_plain_username"] = self._config.sasl_username
        if self._config.sasl_password:
            config["sasl_plain_password"] = self._config.sasl_password
        
        return config


class SASLSSLAuth(AuthHandler):
    """
    Handler for SASL_SSL connections.
    
    Combines SASL authentication with SSL encryption.
    This is the recommended method for production.
    
    Requires:
    - CA certificate for server verification (ssl_cafile is mandatory)
    - SASL mechanism and credentials
    - Optional: client certificate and key for mutual TLS
    """

    def validate(self) -> None:
        """Validate SASL_SSL configuration."""
        # SSL context is mandatory for SASL_SSL
        if not self._config.ssl_cafile:
            raise AuthenticationError(
                "SASL_SSL authentication requires ssl_cafile for SSL context",
                auth_type="sasl_ssl",
            )
        
        # Validate SASL settings
        if not self._config.sasl_mechanism:
            raise AuthenticationError(
                "SASL_SSL authentication requires sasl_mechanism",
                auth_type="sasl_ssl",
            )
        
        # PLAIN and SCRAM require username/password
        if self._config.sasl_mechanism in (
            SASLMechanism.PLAIN,
            SASLMechanism.SCRAM_SHA_256,
            SASLMechanism.SCRAM_SHA_512,
        ):
            if not self._config.sasl_username:
                raise AuthenticationError(
                    f"{self._config.sasl_mechanism.value} requires sasl_username",
                    auth_type="sasl_ssl",
                )
            if not self._config.sasl_password:
                raise AuthenticationError(
                    f"{self._config.sasl_mechanism.value} requires sasl_password",
                    auth_type="sasl_ssl",
                )
        
        # For mutual TLS, both cert and key are required
        if self._config.ssl_certfile and not self._config.ssl_keyfile:
            raise AuthenticationError(
                "SSL client certificate requires ssl_keyfile",
                auth_type="sasl_ssl",
            )
        if self._config.ssl_keyfile and not self._config.ssl_certfile:
            raise AuthenticationError(
                "SSL client key requires ssl_certfile",
                auth_type="sasl_ssl",
            )
        
        # Build SSL context (mandatory for SASL_SSL)
        self._ssl_context = self._build_ssl_context()
        
        if not self._ssl_context:
            raise AuthenticationError(
                "Failed to create SSL context for SASL_SSL",
                auth_type="sasl_ssl",
            )
        
        logger.info(
            "sasl_ssl_auth_configured",
            mechanism=self._config.sasl_mechanism.value if self._config.sasl_mechanism else None,
            cafile=self._config.ssl_cafile,
            mutual_tls=bool(self._config.ssl_certfile),
        )

    def to_aiokafka_config(self) -> dict[str, Any]:
        """Return SASL_SSL configuration."""
        config: dict[str, Any] = {
            "security_protocol": SecurityProtocol.SASL_SSL.value,
            "sasl_mechanism": self._config.sasl_mechanism.value if self._config.sasl_mechanism else None,
            "ssl_context": self._ssl_context,  # Always include SSL context
        }
        
        # SASL credentials
        if self._config.sasl_username:
            config["sasl_plain_username"] = self._config.sasl_username
        if self._config.sasl_password:
            config["sasl_plain_password"] = self._config.sasl_password
        
        return config


@dataclass 
class AuthHandlerFactory:
    """
    Factory for creating authentication handlers.
    
    Provides a convenient way to create handlers based on
    security protocol.
    """
    
    _handlers: dict[SecurityProtocol, type[AuthHandler]] = field(
        default_factory=lambda: {
            SecurityProtocol.PLAINTEXT: PlaintextAuth,
            SecurityProtocol.SSL: SSLAuth,
            SecurityProtocol.SASL_PLAINTEXT: SASLPlaintextAuth,
            SecurityProtocol.SASL_SSL: SASLSSLAuth,
        }
    )
    
    def create(self, auth_config: AuthConfig) -> AuthHandler:
        """
        Create an authentication handler.
        
        Args:
            auth_config: Authentication configuration.
            
        Returns:
            Appropriate AuthHandler instance.
        """
        handler_class = self._handlers.get(auth_config.security_protocol, PlaintextAuth)
        handler = handler_class(auth_config)
        handler.validate()
        return handler
    
    def register(
        self,
        protocol: SecurityProtocol,
        handler_class: type[AuthHandler],
    ) -> None:
        """
        Register a custom handler for a protocol.
        
        Args:
            protocol: Security protocol.
            handler_class: Handler class to use.
        """
        self._handlers[protocol] = handler_class


# Global factory instance
auth_factory = AuthHandlerFactory()

