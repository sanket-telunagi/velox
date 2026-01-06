"""
Async Kafka consumer with parallel processing support.

Provides:
- Async message consumption
- Parallel message processing
- Manual and automatic offset management
- Consumer group support
"""

import asyncio
from collections.abc import AsyncIterator, Callable, Coroutine
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Self

import structlog
from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.errors import KafkaError as AIOKafkaError
from aiokafka.structs import ConsumerRecord

from velox.connectors.base import AbstractConnector, ConnectorState
from velox.connectors.kafka.auth import AuthHandler
from velox.connectors.kafka.config import ConsumerConfig, KafkaConfig
from velox.core.exceptions import KafkaError

logger = structlog.get_logger()


@dataclass
class ConsumedMessage:
    """Represents a consumed Kafka message."""

    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes
    headers: list[tuple[str, bytes | None]]
    timestamp: int
    timestamp_type: int
    
    # Processing metadata
    received_at: datetime = field(default_factory=datetime.now)
    processed: bool = False
    processing_time_ms: float = 0.0
    error: str | None = None

    @classmethod
    def from_record(cls, record: ConsumerRecord) -> "ConsumedMessage":
        """Create from aiokafka ConsumerRecord."""
        return cls(
            topic=record.topic,
            partition=record.partition,
            offset=record.offset,
            key=record.key,
            value=record.value,
            headers=list(record.headers) if record.headers else [],
            timestamp=record.timestamp,
            timestamp_type=record.timestamp_type,
        )


@dataclass
class ConsumerStats:
    """Statistics for consumer operations."""

    messages_received: int = 0
    messages_processed: int = 0
    messages_failed: int = 0
    bytes_received: int = 0
    start_time: datetime = field(default_factory=datetime.now)
    last_message_time: datetime | None = None
    processing_times_ms: list[float] = field(default_factory=list)

    @property
    def average_processing_time_ms(self) -> float:
        """Average message processing time."""
        if not self.processing_times_ms:
            return 0.0
        return sum(self.processing_times_ms) / len(self.processing_times_ms)

    @property
    def messages_per_second(self) -> float:
        """Messages processed per second."""
        elapsed = (datetime.now() - self.start_time).total_seconds()
        if elapsed == 0:
            return 0.0
        return self.messages_processed / elapsed


# Type alias for message handlers
MessageHandler = Callable[[ConsumedMessage], Coroutine[Any, Any, None]]


class AsyncKafkaConsumer(AbstractConnector):
    """
    Async Kafka consumer with parallel processing.
    
    Features:
    - Subscribe to multiple topics
    - Parallel message processing with configurable concurrency
    - Manual and automatic offset commit
    - Consumer group support
    - Graceful shutdown
    
    Usage:
        config = KafkaConfig(bootstrap_servers="localhost:9092")
        consumer_config = ConsumerConfig(group_id="my-group")
        
        async with AsyncKafkaConsumer(config, consumer_config, "dev") as consumer:
            await consumer.subscribe(["topic1", "topic2"])
            
            async for message in consumer.consume():
                print(f"Received: {message.value}")
                await consumer.commit(message)
    """

    def __init__(
        self,
        kafka_config: KafkaConfig,
        consumer_config: ConsumerConfig,
        environment: str,
        auth_handler: AuthHandler | None = None,
    ) -> None:
        """
        Initialize the Kafka consumer.
        
        Args:
            kafka_config: Kafka connection configuration.
            consumer_config: Consumer-specific configuration.
            environment: Environment name.
            auth_handler: Authentication handler.
        """
        super().__init__("kafka_consumer", environment)
        
        self._kafka_config = kafka_config
        self._consumer_config = consumer_config
        self._auth_handler = auth_handler or AuthHandler.from_kafka_config(kafka_config)
        
        self._consumer: AIOKafkaConsumer | None = None
        self._subscribed_topics: list[str] = []
        self._running = False
        self._stats = ConsumerStats()

    @property
    def stats(self) -> ConsumerStats:
        """Get consumer statistics."""
        return self._stats

    @property
    def subscribed_topics(self) -> list[str]:
        """Get list of subscribed topics."""
        return self._subscribed_topics.copy()

    async def _do_connect(self) -> None:
        """Connect to Kafka."""
        self._auth_handler.validate()
        
        # Build configuration
        config = self._kafka_config.to_aiokafka_config()
        config.update(self._consumer_config.to_aiokafka_config())
        config.update(self._auth_handler.to_aiokafka_config())
        
        logger.info(
            "kafka_consumer_connecting",
            environment=self._metadata.environment,
            bootstrap_servers=self._kafka_config.bootstrap_servers,
            group_id=self._consumer_config.group_id,
        )
        
        try:
            self._consumer = AIOKafkaConsumer(**config)
            await self._consumer.start()
            self._stats = ConsumerStats()
            
            logger.info(
                "kafka_consumer_connected",
                environment=self._metadata.environment,
            )
            
        except AIOKafkaError as e:
            raise KafkaError(
                f"Failed to connect consumer: {e}",
                operation="connect",
            ) from e

    async def _do_disconnect(self) -> None:
        """Disconnect from Kafka."""
        self._running = False
        
        if self._consumer:
            logger.info(
                "kafka_consumer_disconnecting",
                environment=self._metadata.environment,
                messages_processed=self._stats.messages_processed,
            )
            
            try:
                await self._consumer.stop()
            except Exception as e:
                logger.warning("kafka_consumer_disconnect_error", error=str(e))
            finally:
                self._consumer = None

    async def _do_health_check(self) -> bool:
        """Check if consumer is healthy."""
        if not self._consumer:
            return False
        
        try:
            # Check subscription
            return len(self._consumer.subscription()) > 0 or len(self._subscribed_topics) == 0
        except Exception:
            return False

    async def subscribe(self, topics: list[str]) -> None:
        """
        Subscribe to topics.
        
        Args:
            topics: List of topic names.
        """
        if not self._consumer:
            raise KafkaError("Consumer not connected", operation="subscribe")
        
        self._consumer.subscribe(topics)
        self._subscribed_topics = topics
        
        logger.info(
            "consumer_subscribed",
            topics=topics,
            environment=self._metadata.environment,
        )

    async def unsubscribe(self) -> None:
        """Unsubscribe from all topics."""
        if self._consumer:
            self._consumer.unsubscribe()
            self._subscribed_topics = []

    async def consume(
        self,
        timeout_ms: int = 1000,
        max_records: int | None = None,
    ) -> AsyncIterator[ConsumedMessage]:
        """
        Consume messages as an async iterator.
        
        Args:
            timeout_ms: Timeout for each fetch.
            max_records: Maximum records to return (None for unlimited).
            
        Yields:
            ConsumedMessage for each received message.
        """
        if not self._consumer or self._state != ConnectorState.CONNECTED:
            raise KafkaError("Consumer not connected", operation="consume")
        
        self._running = True
        records_yielded = 0
        
        try:
            while self._running:
                try:
                    record = await asyncio.wait_for(
                        self._consumer.getone(),
                        timeout=timeout_ms / 1000,
                    )
                    
                    message = ConsumedMessage.from_record(record)
                    self._stats.messages_received += 1
                    self._stats.bytes_received += len(record.value) if record.value else 0
                    self._stats.last_message_time = datetime.now()
                    
                    yield message
                    
                    records_yielded += 1
                    if max_records and records_yielded >= max_records:
                        break
                        
                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    logger.error("consume_error", error=str(e))
                    raise
                    
        finally:
            self._running = False

    async def consume_batch(
        self,
        timeout_ms: int = 1000,
        max_records: int = 500,
    ) -> list[ConsumedMessage]:
        """
        Consume a batch of messages.
        
        Args:
            timeout_ms: Timeout for fetch.
            max_records: Maximum records to return.
            
        Returns:
            List of consumed messages.
        """
        if not self._consumer:
            raise KafkaError("Consumer not connected", operation="consume_batch")
        
        messages: list[ConsumedMessage] = []
        
        try:
            data = await asyncio.wait_for(
                self._consumer.getmany(timeout_ms=timeout_ms, max_records=max_records),
                timeout=(timeout_ms / 1000) + 5,
            )
            
            for tp, records in data.items():
                for record in records:
                    message = ConsumedMessage.from_record(record)
                    messages.append(message)
                    self._stats.messages_received += 1
                    self._stats.bytes_received += len(record.value) if record.value else 0
            
            if messages:
                self._stats.last_message_time = datetime.now()
                
        except asyncio.TimeoutError:
            pass
        
        return messages

    async def process_messages(
        self,
        handler: MessageHandler,
        parallel: bool = True,
        max_concurrency: int = 10,
        timeout_ms: int = 1000,
        max_records: int | None = None,
    ) -> ConsumerStats:
        """
        Consume and process messages with a handler.
        
        Args:
            handler: Async function to process each message.
            parallel: Whether to process in parallel.
            max_concurrency: Maximum concurrent processing tasks.
            timeout_ms: Timeout for each fetch.
            max_records: Maximum total records to process.
            
        Returns:
            Consumer statistics.
        """
        semaphore = asyncio.Semaphore(max_concurrency) if parallel else None
        
        async def process_one(msg: ConsumedMessage) -> None:
            start = datetime.now()
            try:
                if semaphore:
                    async with semaphore:
                        await handler(msg)
                else:
                    await handler(msg)
                
                msg.processed = True
                msg.processing_time_ms = (datetime.now() - start).total_seconds() * 1000
                self._stats.messages_processed += 1
                self._stats.processing_times_ms.append(msg.processing_time_ms)
                
            except Exception as e:
                msg.error = str(e)
                self._stats.messages_failed += 1
                logger.error(
                    "message_processing_failed",
                    topic=msg.topic,
                    partition=msg.partition,
                    offset=msg.offset,
                    error=str(e),
                )
        
        if parallel:
            tasks: list[asyncio.Task[None]] = []
            
            async for message in self.consume(timeout_ms, max_records):
                task = asyncio.create_task(process_one(message))
                tasks.append(task)
            
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
        else:
            async for message in self.consume(timeout_ms, max_records):
                await process_one(message)
        
        return self._stats

    async def commit(
        self,
        message: ConsumedMessage | None = None,
    ) -> None:
        """
        Commit offsets.
        
        Args:
            message: Specific message to commit (commits all if None).
        """
        if not self._consumer:
            raise KafkaError("Consumer not connected", operation="commit")
        
        if message:
            tp = TopicPartition(message.topic, message.partition)
            await self._consumer.commit({tp: message.offset + 1})
        else:
            await self._consumer.commit()

    async def seek(self, topic: str, partition: int, offset: int) -> None:
        """
        Seek to a specific offset.
        
        Args:
            topic: Topic name.
            partition: Partition number.
            offset: Offset to seek to.
        """
        if not self._consumer:
            raise KafkaError("Consumer not connected", operation="seek")
        
        tp = TopicPartition(topic, partition)
        self._consumer.seek(tp, offset)

    async def seek_to_beginning(self, topics: list[str] | None = None) -> None:
        """Seek to beginning of topics."""
        if not self._consumer:
            raise KafkaError("Consumer not connected", operation="seek_to_beginning")
        
        if topics:
            partitions = [
                TopicPartition(topic, p)
                for topic in topics
                for p in await self._get_partitions(topic)
            ]
        else:
            partitions = self._consumer.assignment()
        
        await self._consumer.seek_to_beginning(*partitions)

    async def seek_to_end(self, topics: list[str] | None = None) -> None:
        """Seek to end of topics."""
        if not self._consumer:
            raise KafkaError("Consumer not connected", operation="seek_to_end")
        
        if topics:
            partitions = [
                TopicPartition(topic, p)
                for topic in topics
                for p in await self._get_partitions(topic)
            ]
        else:
            partitions = self._consumer.assignment()
        
        await self._consumer.seek_to_end(*partitions)

    async def _get_partitions(self, topic: str) -> list[int]:
        """Get partition IDs for a topic."""
        if not self._consumer:
            return []
        
        partitions = self._consumer.partitions_for_topic(topic)
        return list(partitions) if partitions else []

    def stop(self) -> None:
        """Signal the consumer to stop processing."""
        self._running = False

    def get_stats(self) -> dict[str, Any]:
        """Get consumer statistics as dictionary."""
        return {
            "environment": self._metadata.environment,
            "state": self._state.value,
            "subscribed_topics": self._subscribed_topics,
            "messages_received": self._stats.messages_received,
            "messages_processed": self._stats.messages_processed,
            "messages_failed": self._stats.messages_failed,
            "bytes_received": self._stats.bytes_received,
            "average_processing_time_ms": self._stats.average_processing_time_ms,
            "messages_per_second": self._stats.messages_per_second,
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
        await self.disconnect()

