"""
Kafka Connector module - Async Kafka producer and consumer with multi-environment support.
"""

from velox.connectors.kafka.auth import (
    AuthConfig,
    AuthHandler,
    SASLSSLAuth,
    SSLAuth,
)
from velox.connectors.kafka.batch import BatchProcessor, BatchResult
from velox.connectors.kafka.config import KafkaConfig, KafkaEnvironmentConfig
from velox.connectors.kafka.consumer import AsyncKafkaConsumer
from velox.connectors.kafka.producer import AsyncKafkaProducer, SendResult

__all__ = [
    "AsyncKafkaProducer",
    "AsyncKafkaConsumer",
    "SendResult",
    "BatchResult",
    "BatchProcessor",
    "KafkaConfig",
    "KafkaEnvironmentConfig",
    "AuthHandler",
    "AuthConfig",
    "SASLSSLAuth",
    "SSLAuth",
]

