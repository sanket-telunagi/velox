"""
Connectors module - Various technology connectors (Kafka, future: MSSQL, PostgreSQL, etc.)
"""

from velox.connectors.base import AbstractConnector, ConnectorState

__all__ = [
    "AbstractConnector",
    "ConnectorState",
]

