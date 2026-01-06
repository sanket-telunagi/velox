"""
Abstract base classes and protocols for connectors.

Provides the foundation for all connector implementations with
consistent interfaces for connection management, health checks,
and async context manager support.
"""

from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, AsyncIterator, Protocol, Self, runtime_checkable


class ConnectorState(Enum):
    """Represents the state of a connector."""

    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    DISCONNECTING = "disconnecting"
    ERROR = "error"


@dataclass
class ConnectorMetadata:
    """Metadata about a connector instance."""

    connector_type: str
    environment: str
    created_at: datetime = field(default_factory=datetime.now)
    connected_at: datetime | None = None
    last_activity: datetime | None = None
    extra: dict[str, Any] = field(default_factory=dict)


@runtime_checkable
class Connectable(Protocol):
    """Protocol for objects that can connect and disconnect."""

    async def connect(self) -> None:
        """Establish connection."""
        ...

    async def disconnect(self) -> None:
        """Close connection."""
        ...


@runtime_checkable
class HealthCheckable(Protocol):
    """Protocol for objects that support health checks."""

    async def health_check(self) -> bool:
        """
        Check if the connection is healthy.
        
        Returns:
            True if healthy, False otherwise.
        """
        ...


class AbstractConnector(ABC):
    """
    Abstract base class for all connectors.
    
    Provides common functionality for:
    - Connection lifecycle management
    - State tracking
    - Async context manager support
    - Health checking
    
    Subclasses must implement:
    - _do_connect(): Actual connection logic
    - _do_disconnect(): Actual disconnection logic
    - _do_health_check(): Actual health check logic
    """

    def __init__(
        self,
        connector_type: str,
        environment: str,
        **kwargs: Any,
    ) -> None:
        self._state = ConnectorState.DISCONNECTED
        self._metadata = ConnectorMetadata(
            connector_type=connector_type,
            environment=environment,
            extra=kwargs,
        )
        self._error: Exception | None = None

    @property
    def state(self) -> ConnectorState:
        """Current state of the connector."""
        return self._state

    @property
    def metadata(self) -> ConnectorMetadata:
        """Metadata about this connector."""
        return self._metadata

    @property
    def is_connected(self) -> bool:
        """Check if connector is currently connected."""
        return self._state == ConnectorState.CONNECTED

    @property
    def last_error(self) -> Exception | None:
        """Last error that occurred, if any."""
        return self._error

    async def connect(self) -> None:
        """
        Establish connection to the target system.
        
        Raises:
            ConnectorError: If connection fails.
        """
        if self._state == ConnectorState.CONNECTED:
            return

        self._state = ConnectorState.CONNECTING
        self._error = None

        try:
            await self._do_connect()
            self._state = ConnectorState.CONNECTED
            self._metadata.connected_at = datetime.now()
        except Exception as e:
            self._state = ConnectorState.ERROR
            self._error = e
            raise

    async def disconnect(self) -> None:
        """
        Close connection to the target system.
        
        Raises:
            ConnectorError: If disconnection fails.
        """
        if self._state == ConnectorState.DISCONNECTED:
            return

        self._state = ConnectorState.DISCONNECTING

        try:
            await self._do_disconnect()
            self._state = ConnectorState.DISCONNECTED
        except Exception as e:
            self._state = ConnectorState.ERROR
            self._error = e
            raise

    async def health_check(self) -> bool:
        """
        Check if the connection is healthy.
        
        Returns:
            True if healthy, False otherwise.
        """
        if self._state != ConnectorState.CONNECTED:
            return False

        try:
            result = await self._do_health_check()
            self._metadata.last_activity = datetime.now()
            return result
        except Exception:
            return False

    async def reconnect(self) -> None:
        """Disconnect and reconnect."""
        await self.disconnect()
        await self.connect()

    async def __aenter__(self) -> Self:
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        """Async context manager exit."""
        await self.disconnect()

    @abstractmethod
    async def _do_connect(self) -> None:
        """
        Implement actual connection logic.
        
        Raises:
            ConnectorError: If connection fails.
        """
        ...

    @abstractmethod
    async def _do_disconnect(self) -> None:
        """
        Implement actual disconnection logic.
        
        Raises:
            ConnectorError: If disconnection fails.
        """
        ...

    @abstractmethod
    async def _do_health_check(self) -> bool:
        """
        Implement actual health check logic.
        
        Returns:
            True if healthy, False otherwise.
        """
        ...


@asynccontextmanager
async def connector_session(connector: AbstractConnector) -> AsyncIterator[AbstractConnector]:
    """
    Context manager for connector sessions with automatic cleanup.
    
    Usage:
        async with connector_session(my_connector) as conn:
            await conn.do_something()
    """
    try:
        await connector.connect()
        yield connector
    finally:
        await connector.disconnect()

