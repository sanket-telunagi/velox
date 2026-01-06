"""
Batch processing utilities for Kafka.

Provides efficient batch message handling with:
- Configurable batch size and timeout
- Automatic batching of messages
- Result aggregation
"""

import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Generic, TypeVar

import structlog

logger = structlog.get_logger()

T = TypeVar("T")


@dataclass
class Message:
    """Represents a single Kafka message."""

    topic: str
    value: bytes
    key: bytes | None = None
    headers: list[tuple[str, bytes]] | None = None
    partition: int | None = None
    timestamp_ms: int | None = None
    
    # Metadata
    correlation_id: str | None = None
    etl_name: str | None = None
    environment: str | None = None

    def __post_init__(self) -> None:
        """Set default timestamp if not provided."""
        if self.timestamp_ms is None:
            self.timestamp_ms = int(datetime.now().timestamp() * 1000)


@dataclass
class SendResult:
    """Result of sending a single message."""

    topic: str
    partition: int
    offset: int
    timestamp_ms: int
    success: bool = True
    error: str | None = None
    message: Message | None = None
    duration_ms: float = 0.0


@dataclass
class BatchResult:
    """Result of sending a batch of messages."""

    total: int = 0
    successful: int = 0
    failed: int = 0
    results: list[SendResult] = field(default_factory=list)
    start_time: datetime = field(default_factory=datetime.now)
    end_time: datetime | None = None
    duration_ms: float = 0.0
    errors: list[str] = field(default_factory=list)

    def add_result(self, result: SendResult) -> None:
        """Add a send result to the batch."""
        self.total += 1
        if result.success:
            self.successful += 1
        else:
            self.failed += 1
            if result.error:
                self.errors.append(result.error)
        self.results.append(result)

    def finalize(self) -> None:
        """Finalize the batch result."""
        self.end_time = datetime.now()
        self.duration_ms = (self.end_time - self.start_time).total_seconds() * 1000

    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage."""
        if self.total == 0:
            return 0.0
        return (self.successful / self.total) * 100

    @property
    def messages_per_second(self) -> float:
        """Calculate throughput."""
        if self.duration_ms == 0:
            return 0.0
        return (self.successful / self.duration_ms) * 1000


@dataclass
class BatchConfig:
    """Configuration for batch processing."""

    batch_size: int = 100
    batch_timeout_ms: int = 5000
    max_retries: int = 3
    retry_backoff_ms: int = 100
    fail_fast: bool = False  # Stop on first error


class BatchProcessor(Generic[T]):
    """
    Generic batch processor for messages.
    
    Collects messages and processes them in batches either when:
    - Batch size is reached
    - Timeout expires
    - flush() is called
    
    Usage:
        async def send_batch(messages):
            # Send messages to Kafka
            return BatchResult(...)
        
        processor = BatchProcessor(
            batch_size=100,
            process_func=send_batch,
        )
        
        for msg in messages:
            await processor.add(msg)
        
        results = await processor.flush()
    """

    def __init__(
        self,
        batch_size: int = 100,
        batch_timeout_ms: int = 5000,
        process_func: Callable[[list[T]], Any] | None = None,
    ) -> None:
        """
        Initialize the batch processor.
        
        Args:
            batch_size: Maximum messages per batch.
            batch_timeout_ms: Timeout to wait for batch to fill.
            process_func: Async function to process batches.
        """
        self._batch_size = batch_size
        self._batch_timeout_ms = batch_timeout_ms
        self._process_func = process_func
        self._buffer: list[T] = []
        self._results: list[BatchResult] = []
        self._lock = asyncio.Lock()
        self._last_flush = datetime.now()

    @property
    def buffer_size(self) -> int:
        """Current number of messages in buffer."""
        return len(self._buffer)

    @property
    def is_full(self) -> bool:
        """Check if buffer has reached batch size."""
        return len(self._buffer) >= self._batch_size

    async def add(self, item: T) -> BatchResult | None:
        """
        Add an item to the batch.
        
        Automatically flushes if batch size is reached.
        
        Args:
            item: Item to add.
            
        Returns:
            BatchResult if flush occurred, None otherwise.
        """
        async with self._lock:
            self._buffer.append(item)
            
            if self.is_full:
                return await self._flush_internal()
            
            return None

    async def add_many(self, items: list[T]) -> list[BatchResult]:
        """
        Add multiple items to the batch.
        
        Args:
            items: Items to add.
            
        Returns:
            List of BatchResults from any flushes that occurred.
        """
        results: list[BatchResult] = []
        
        for item in items:
            result = await self.add(item)
            if result:
                results.append(result)
        
        return results

    async def flush(self) -> BatchResult | None:
        """
        Flush the current batch.
        
        Returns:
            BatchResult or None if buffer was empty.
        """
        async with self._lock:
            return await self._flush_internal()

    async def _flush_internal(self) -> BatchResult | None:
        """Internal flush without lock."""
        if not self._buffer:
            return None
        
        batch = self._buffer.copy()
        self._buffer.clear()
        self._last_flush = datetime.now()
        
        result = BatchResult(start_time=datetime.now())
        
        if self._process_func:
            try:
                process_result = await self._process_func(batch)
                if isinstance(process_result, BatchResult):
                    result = process_result
                else:
                    # Assume success if no BatchResult returned
                    result.total = len(batch)
                    result.successful = len(batch)
            except Exception as e:
                result.total = len(batch)
                result.failed = len(batch)
                result.errors.append(str(e))
                logger.error("batch_process_error", error=str(e), batch_size=len(batch))
        else:
            # No process function, just mark as processed
            result.total = len(batch)
            result.successful = len(batch)
        
        result.finalize()
        self._results.append(result)
        
        logger.debug(
            "batch_flushed",
            total=result.total,
            successful=result.successful,
            failed=result.failed,
            duration_ms=result.duration_ms,
        )
        
        return result

    async def flush_all(self) -> list[BatchResult]:
        """
        Flush all pending items and return all results.
        
        Returns:
            List of all BatchResults.
        """
        await self.flush()
        return self._results.copy()

    def get_aggregate_result(self) -> BatchResult:
        """
        Get aggregated result across all batches.
        
        Returns:
            Aggregated BatchResult.
        """
        aggregate = BatchResult()
        
        if not self._results:
            return aggregate
        
        aggregate.start_time = min(r.start_time for r in self._results)
        aggregate.end_time = max(r.end_time for r in self._results if r.end_time)
        
        for result in self._results:
            aggregate.total += result.total
            aggregate.successful += result.successful
            aggregate.failed += result.failed
            aggregate.errors.extend(result.errors)
            aggregate.duration_ms += result.duration_ms
        
        return aggregate

    def clear_results(self) -> None:
        """Clear stored results."""
        self._results.clear()


class MessageBatcher:
    """
    Utility for creating batches from messages.
    
    Provides static methods for batching messages by various criteria.
    """

    @staticmethod
    def by_size(messages: list[Message], batch_size: int) -> list[list[Message]]:
        """
        Split messages into batches by count.
        
        Args:
            messages: List of messages.
            batch_size: Maximum messages per batch.
            
        Returns:
            List of message batches.
        """
        return [
            messages[i:i + batch_size]
            for i in range(0, len(messages), batch_size)
        ]

    @staticmethod
    def by_topic(messages: list[Message]) -> dict[str, list[Message]]:
        """
        Group messages by topic.
        
        Args:
            messages: List of messages.
            
        Returns:
            Dictionary mapping topic to messages.
        """
        grouped: dict[str, list[Message]] = {}
        
        for msg in messages:
            if msg.topic not in grouped:
                grouped[msg.topic] = []
            grouped[msg.topic].append(msg)
        
        return grouped

    @staticmethod
    def by_partition(messages: list[Message]) -> dict[int | None, list[Message]]:
        """
        Group messages by partition.
        
        Args:
            messages: List of messages.
            
        Returns:
            Dictionary mapping partition to messages.
        """
        grouped: dict[int | None, list[Message]] = {}
        
        for msg in messages:
            if msg.partition not in grouped:
                grouped[msg.partition] = []
            grouped[msg.partition].append(msg)
        
        return grouped

    @staticmethod
    def by_bytes(
        messages: list[Message],
        max_bytes: int,
    ) -> list[list[Message]]:
        """
        Split messages into batches by total byte size.
        
        Args:
            messages: List of messages.
            max_bytes: Maximum bytes per batch.
            
        Returns:
            List of message batches.
        """
        batches: list[list[Message]] = []
        current_batch: list[Message] = []
        current_size = 0
        
        for msg in messages:
            msg_size = len(msg.value)
            if msg.key:
                msg_size += len(msg.key)
            
            if current_size + msg_size > max_bytes and current_batch:
                batches.append(current_batch)
                current_batch = []
                current_size = 0
            
            current_batch.append(msg)
            current_size += msg_size
        
        if current_batch:
            batches.append(current_batch)
        
        return batches

