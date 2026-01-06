"""
Core module - Base abstractions, configuration, templating, and metrics.
"""

from velox.core.base import AbstractConnector, ConnectorState
from velox.core.config import ConfigManager
from velox.core.exceptions import (
    ConfigurationError,
    ConnectorError,
    VeloxError,
    TemplateError,
)
from velox.core.metrics import ExecutionStats, MetricsCollector, StepTiming
from velox.core.templating import TemplateEngine

__all__ = [
    "AbstractConnector",
    "ConnectorState",
    "ConfigManager",
    "TemplateEngine",
    "MetricsCollector",
    "ExecutionStats",
    "StepTiming",
    "VeloxError",
    "ConfigurationError",
    "ConnectorError",
    "TemplateError",
]

