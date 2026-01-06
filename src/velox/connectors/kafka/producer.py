"""
Async Kafka producer with batch and single message support.

Provides:
- Async message sending
- Batch processing with configurable size
- Automatic retry with backoff
- Comprehensive metrics
"""

import asyncio
from datetime import datetime
from typing import Any, Self

import structlog
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError as AIOKafkaError

from velox.connectors.base import AbstractConnector, ConnectorState
from velox.connectors.kafka.auth import AuthHandler
from velox.connectors.kafka.batch import (
    BatchProcessor,
    BatchResult,
    Message,
    MessageBatcher,
    SendResult,
)
from velox.connectors.kafka.config import KafkaConfig, ProducerConfig
from velox.core.exceptions import KafkaError

logger = structlog.get_logger()


class AsyncKafkaProducer(AbstractConnector):
    """
    Async Kafka producer with batch support.
    
    Features:
    - Single and batch message sending
    - Automatic batching with configurable size and timeout
    - Retry logic with exponential backoff
    - Connection pooling
    - Comprehensive error handling
    
    Usage:
        config = KafkaConfig(bootstrap_servers="localhost:9092")
        
        async with AsyncKafkaProducer(config, "dev") as producer:
            # Single message
            result = await producer.send(
                topic="my-topic",
                value=b'{"key": "value"}',
            )
            
            # Batch messages
            messages = [Message(topic="my-topic", value=b"msg1"), ...]
            batch_result = await producer.send_batch(messages)
    """

    def __init__(
        self,
        kafka_config: KafkaConfig,
        environment: str,
        producer_config: ProducerConfig | None = None,
        auth_handler: AuthHandler | None = None,
        batch_size: int = 100,
        batch_timeout_ms: int = 5000,
    ) -> None:
        """
        Initialize the Kafka producer.
        
        Args:
            kafka_config: Kafka connection configuration.
            environment: Environment name.
            producer_config: Producer-specific configuration.
            auth_handler: Authentication handler (created from config if not provided).
            batch_size: Default batch size for batch operations.
            batch_timeout_ms: Timeout for batch operations.
        """
        super().__init__("kafka_producer", environment)
        
        self._kafka_config = kafka_config
        self._producer_config = producer_config or ProducerConfig()
        self._auth_handler = auth_handler or AuthHandler.from_kafka_config(kafka_config)
        self._batch_size = batch_size
        self._batch_timeout_ms = batch_timeout_ms
        
        self._producer: AIOKafkaProducer | None = None
        self._batch_processor: BatchProcessor[Message] | None = None
        
        # Statistics
        self._messages_sent = 0
        self._messages_failed = 0
        self._bytes_sent = 0

    @property
    def messages_sent(self) -> int:
        """Total messages sent successfully."""
        return self._messages_sent

    @property
    def messages_failed(self) -> int:
        """Total messages that failed to send."""
        return self._messages_failed

    @property
    def bytes_sent(self) -> int:
        """Total bytes sent."""
        return self._bytes_sent

    async def _do_connect(self) -> None:
        """Connect to Kafka."""
        # Validate authentication
        self._auth_handler.validate()
        
        # Build configuration
        config = self._kafka_config.to_aiokafka_config()
        config.update(self._producer_config.to_aiokafka_config())
        config.update(self._auth_handler.to_aiokafka_config())
        
        logger.info(
            "kafka_producer_connecting",
            environment=self._metadata.environment,
            bootstrap_servers=self._kafka_config.bootstrap_servers,
        )
        
        try:
            self._producer = AIOKafkaProducer(**config)
            await self._producer.start()
            
            # Initialize batch processor
            self._batch_processor = BatchProcessor(
                batch_size=self._batch_size,
                batch_timeout_ms=self._batch_timeout_ms,
                process_func=self._process_batch,
            )
            
            logger.info(
                "kafka_producer_connected",
                environment=self._metadata.environment,
            )
            
        except AIOKafkaError as e:
            raise KafkaError(
                f"Failed to connect to Kafka: {e}",
                operation="connect",
                details={"bootstrap_servers": self._kafka_config.bootstrap_servers},
            ) from e

    async def _do_disconnect(self) -> None:
        """Disconnect from Kafka."""
        if self._batch_processor:
            # Flush any pending messages
            await self._batch_processor.flush()
        
        if self._producer:
            logger.info(
                "kafka_producer_disconnecting",
                environment=self._metadata.environment,
                messages_sent=self._messages_sent,
            )
            
            try:
                await self._producer.stop()
            except Exception as e:
                logger.warning(
                    "kafka_producer_disconnect_error",
                    error=str(e),
                )
            finally:
                self._producer = None

    async def _do_health_check(self) -> bool:
        """Check if producer is healthy."""
        if not self._producer:
            return False
        
        try:
            # Check if we can get cluster metadata
            await asyncio.wait_for(
                self._producer.client.fetch_all_metadata(),
                timeout=5.0,
            )
            return True
        except Exception:
            return False

    async def send(
        self,
        topic: str,
        value: bytes,
        key: bytes | None = None,
        headers: list[tuple[str, bytes]] | None = None,
        partition: int | None = None,
        timestamp_ms: int | None = None,
    ) -> SendResult:
        """
        Send a single message.
        
        Args:
            topic: Topic to send to.
            value: Message value.
            key: Optional message key.
            headers: Optional message headers.
            partition: Optional target partition.
            timestamp_ms: Optional timestamp.
            
        Returns:
            SendResult with delivery information.
            
        Raises:
            KafkaError: If send fails.
        """
        if not self._producer or self._state != ConnectorState.CONNECTED:
            raise KafkaError(
                "Producer not connected",
                topic=topic,
                operation="send",
            )
        
        start_time = datetime.now()
        
        try:
            result = await self._producer.send_and_wait(
                topic=topic,
                value=value,
                key=key,
                headers=headers,
                partition=partition,
                timestamp_ms=timestamp_ms,
            )
            
            duration_ms = (datetime.now() - start_time).total_seconds() * 1000
            
            self._messages_sent += 1
            self._bytes_sent += len(value) + (len(key) if key else 0)
            
            send_result = SendResult(
                topic=result.topic,
                partition=result.partition,
                offset=result.offset,
                timestamp_ms=result.timestamp,
                success=True,
                duration_ms=duration_ms,
            )
            
            logger.debug(
                "message_sent",
                topic=topic,
                partition=result.partition,
                offset=result.offset,
                duration_ms=duration_ms,
            )
            
            return send_result
            
        except AIOKafkaError as e:
            self._messages_failed += 1
            duration_ms = (datetime.now() - start_time).total_seconds() * 1000
            
            logger.error(
                "message_send_failed",
                topic=topic,
                error=str(e),
                duration_ms=duration_ms,
            )
            
            return SendResult(
                topic=topic,
                partition=-1,
                offset=-1,
                timestamp_ms=int(datetime.now().timestamp() * 1000),
                success=False,
                error=str(e),
                duration_ms=duration_ms,
            )

    async def send_message(self, message: Message) -> SendResult:
        """
        Send a Message object.
        
        Args:
            message: Message to send.
            
        Returns:
            SendResult with delivery information.
        """
        result = await self.send(
            topic=message.topic,
            value=message.value,
            key=message.key,
            headers=message.headers,
            partition=message.partition,
            timestamp_ms=message.timestamp_ms,
        )
        result.message = message
        return result

    async def send_batch(
        self,
        messages: list[Message],
        parallel: bool = True,
        max_concurrency: int = 10,
    ) -> BatchResult:
        """
        Send a batch of messages.
        
        Args:
            messages: List of messages to send.
            parallel: Whether to send in parallel.
            max_concurrency: Maximum concurrent sends.
            
        Returns:
            BatchResult with aggregated results.
        """
        if not messages:
            return BatchResult()
        
        batch_result = BatchResult(start_time=datetime.now())
        
        if parallel:
            # Send in parallel with semaphore for concurrency control
            semaphore = asyncio.Semaphore(max_concurrency)
            
            async def send_with_semaphore(msg: Message) -> SendResult:
                async with semaphore:
                    return await self.send_message(msg)
            
            tasks = [send_with_semaphore(msg) for msg in messages]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for result in results:
                if isinstance(result, Exception):
                    batch_result.add_result(SendResult(
                        topic="unknown",
                        partition=-1,
                        offset=-1,
                        timestamp_ms=int(datetime.now().timestamp() * 1000),
                        success=False,
                        error=str(result),
                    ))
                else:
                    batch_result.add_result(result)
        else:
            # Send sequentially
            for msg in messages:
                result = await self.send_message(msg)
                batch_result.add_result(result)
        
        batch_result.finalize()
        
        logger.info(
            "batch_sent",
            total=batch_result.total,
            successful=batch_result.successful,
            failed=batch_result.failed,
            duration_ms=batch_result.duration_ms,
            messages_per_second=batch_result.messages_per_second,
        )
        
        return batch_result

    async def send_batch_by_topic(
        self,
        messages: list[Message],
        parallel: bool = True,
    ) -> dict[str, BatchResult]:
        """
        Send messages grouped by topic.
        
        Args:
            messages: List of messages.
            parallel: Whether to send in parallel.
            
        Returns:
            Dictionary mapping topic to BatchResult.
        """
        grouped = MessageBatcher.by_topic(messages)
        results: dict[str, BatchResult] = {}
        
        if parallel:
            tasks = {
                topic: self.send_batch(msgs, parallel=True)
                for topic, msgs in grouped.items()
            }
            
            for topic, task in tasks.items():
                results[topic] = await task
        else:
            for topic, msgs in grouped.items():
                results[topic] = await self.send_batch(msgs, parallel=False)
        
        return results

    async def _process_batch(self, messages: list[Message]) -> BatchResult:
        """Process a batch from the batch processor."""
        return await self.send_batch(messages, parallel=True)

    async def queue_message(self, message: Message) -> BatchResult | None:
        """
        Queue a message for batch sending.
        
        Message will be sent when batch is full or flushed.
        
        Args:
            message: Message to queue.
            
        Returns:
            BatchResult if batch was flushed, None otherwise.
        """
        if not self._batch_processor:
            raise KafkaError(
                "Producer not connected",
                operation="queue_message",
            )
        
        return await self._batch_processor.add(message)

    async def flush_queue(self) -> BatchResult | None:
        """
        Flush all queued messages.
        
        Returns:
            BatchResult from flush.
        """
        if not self._batch_processor:
            return None
        
        return await self._batch_processor.flush()

    def get_stats(self) -> dict[str, Any]:
        """
        Get producer statistics.
        
        Returns:
            Dictionary of statistics.
        """
        return {
            "environment": self._metadata.environment,
            "state": self._state.value,
            "messages_sent": self._messages_sent,
            "messages_failed": self._messages_failed,
            "bytes_sent": self._bytes_sent,
            "connected_at": self._metadata.connected_at.isoformat() if self._metadata.connected_at else None,
            "queued_messages": self._batch_processor.buffer_size if self._batch_processor else 0,
        }

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
        # Flush any pending messages before disconnect
        if self._batch_processor:
            await self._batch_processor.flush()
        await self.disconnect()

