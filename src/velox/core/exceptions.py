"""
Custom exceptions for the Velox library.

All exceptions inherit from VeloxError for easy catching.
"""

from typing import Any


class VeloxError(Exception):
    """Base exception for all Velox errors."""

    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.details = details or {}

    def __str__(self) -> str:
        if self.details:
            return f"{self.message} | Details: {self.details}"
        return self.message


class ConfigurationError(VeloxError):
    """Raised when there's a configuration error."""

    def __init__(
        self,
        message: str,
        config_path: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message, details)
        self.config_path = config_path


class TemplateError(VeloxError):
    """Raised when template rendering fails."""

    def __init__(
        self,
        message: str,
        template_name: str | None = None,
        context: dict[str, Any] | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message, details)
        self.template_name = template_name
        self.context = context


class ConnectorError(VeloxError):
    """Raised when a connector operation fails."""

    def __init__(
        self,
        message: str,
        connector_type: str | None = None,
        operation: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message, details)
        self.connector_type = connector_type
        self.operation = operation


class KafkaError(ConnectorError):
    """Raised when a Kafka operation fails."""

    def __init__(
        self,
        message: str,
        topic: str | None = None,
        partition: int | None = None,
        operation: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message, "kafka", operation, details)
        self.topic = topic
        self.partition = partition


class AuthenticationError(ConnectorError):
    """Raised when authentication fails."""

    def __init__(
        self,
        message: str,
        auth_type: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message, None, "authenticate", details)
        self.auth_type = auth_type


class ETLExecutionError(VeloxError):
    """Raised when ETL execution fails."""

    def __init__(
        self,
        message: str,
        etl_name: str | None = None,
        environment: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message, details)
        self.etl_name = etl_name
        self.environment = environment


class ValidationError(VeloxError):
    """Raised when validation fails."""

    def __init__(
        self,
        message: str,
        field: str | None = None,
        value: Any = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message, details)
        self.field = field
        self.value = value

