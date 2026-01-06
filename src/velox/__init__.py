"""
Velox - High-performance, async-first ETL connector library.

This library provides a modular framework for building ETL pipelines with:
- Multi-environment configuration support
- Async-first design for high concurrency
- Jinja2 templating for dynamic payloads
- Comprehensive metrics and timing
- Support for Kafka, and future connectors (MSSQL, PostgreSQL, Couchbase, REST APIs)
"""

from velox.core.config import ConfigManager
from velox.core.metrics import ExecutionStats, MetricsCollector
from velox.core.templating import TemplateEngine

__version__ = "0.1.0"
__all__ = [
    "ConfigManager",
    "TemplateEngine",
    "MetricsCollector",
    "ExecutionStats",
    "__version__",
]

