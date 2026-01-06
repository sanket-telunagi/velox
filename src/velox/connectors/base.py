"""
Base connector classes - re-exports from core for convenience.
"""

from velox.core.base import (
    AbstractConnector,
    Connectable,
    ConnectorMetadata,
    ConnectorState,
    HealthCheckable,
    connector_session,
)

__all__ = [
    "AbstractConnector",
    "ConnectorState",
    "ConnectorMetadata",
    "Connectable",
    "HealthCheckable",
    "connector_session",
]

