"""
Performance metrics collection and reporting.

Provides:
- Step-by-step timing for operations
- Aggregated statistics
- Per-environment and per-ETL breakdowns
- Rich output formatting
"""

import asyncio
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, AsyncIterator, Iterator

import structlog
from rich.console import Console
from rich.table import Table

logger = structlog.get_logger()


@dataclass
class StepTiming:
    """Timing information for a single step."""

    name: str
    start_time: datetime
    end_time: datetime | None = None
    duration_ms: float = 0.0
    success: bool = True
    error: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def complete(self, success: bool = True, error: str | None = None) -> None:
        """Mark the step as complete."""
        self.end_time = datetime.now()
        self.duration_ms = (self.end_time - self.start_time).total_seconds() * 1000
        self.success = success
        self.error = error


@dataclass
class ExecutionStats:
    """
    Comprehensive execution statistics.
    
    Contains detailed timing and success/failure information
    for an ETL execution run.
    """

    etl_name: str
    environment: str
    total_events: int = 0
    successful: int = 0
    failed: int = 0
    start_time: datetime = field(default_factory=datetime.now)
    end_time: datetime | None = None
    duration_ms: float = 0.0
    events_per_second: float = 0.0
    step_timings: dict[str, StepTiming] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def finalize(self) -> None:
        """Finalize the stats after execution completes."""
        self.end_time = datetime.now()
        self.duration_ms = (self.end_time - self.start_time).total_seconds() * 1000
        
        if self.duration_ms > 0:
            self.events_per_second = (self.successful / self.duration_ms) * 1000

    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage."""
        if self.total_events == 0:
            return 0.0
        return (self.successful / self.total_events) * 100

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "etl_name": self.etl_name,
            "environment": self.environment,
            "total_events": self.total_events,
            "successful": self.successful,
            "failed": self.failed,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_ms": self.duration_ms,
            "events_per_second": self.events_per_second,
            "success_rate": self.success_rate,
            "step_timings": {
                name: {
                    "duration_ms": timing.duration_ms,
                    "success": timing.success,
                    "error": timing.error,
                }
                for name, timing in self.step_timings.items()
            },
            "errors": self.errors,
            "metadata": self.metadata,
        }


@dataclass
class AggregatedStats:
    """Aggregated statistics across multiple executions."""

    total_executions: int = 0
    total_events: int = 0
    total_successful: int = 0
    total_failed: int = 0
    total_duration_ms: float = 0.0
    start_time: datetime = field(default_factory=datetime.now)
    end_time: datetime | None = None
    by_environment: dict[str, ExecutionStats] = field(default_factory=dict)
    by_etl: dict[str, ExecutionStats] = field(default_factory=dict)
    all_stats: list[ExecutionStats] = field(default_factory=list)

    @property
    def average_duration_ms(self) -> float:
        """Average execution duration."""
        if self.total_executions == 0:
            return 0.0
        return self.total_duration_ms / self.total_executions

    @property
    def overall_events_per_second(self) -> float:
        """Overall throughput."""
        if self.total_duration_ms == 0:
            return 0.0
        return (self.total_successful / self.total_duration_ms) * 1000

    @property
    def success_rate(self) -> float:
        """Overall success rate."""
        if self.total_events == 0:
            return 0.0
        return (self.total_successful / self.total_events) * 100

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "total_executions": self.total_executions,
            "total_events": self.total_events,
            "total_successful": self.total_successful,
            "total_failed": self.total_failed,
            "total_duration_ms": self.total_duration_ms,
            "average_duration_ms": self.average_duration_ms,
            "overall_events_per_second": self.overall_events_per_second,
            "success_rate": self.success_rate,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "by_environment": {
                env: stats.to_dict() for env, stats in self.by_environment.items()
            },
            "by_etl": {
                etl: stats.to_dict() for etl, stats in self.by_etl.items()
            },
        }


class MetricsCollector:
    """
    Collects and aggregates execution metrics.
    
    Features:
    - Step-by-step timing with context managers
    - Per-ETL and per-environment tracking
    - Aggregated statistics
    - Rich console output
    
    Usage:
        collector = MetricsCollector()
        
        with collector.track_execution("my_etl", "dev") as stats:
            with collector.track_step(stats, "connect"):
                await connect()
            
            with collector.track_step(stats, "send"):
                await send_events()
                stats.successful = 100
        
        collector.print_summary()
    """

    def __init__(self) -> None:
        self._stats: list[ExecutionStats] = []
        self._current_stats: ExecutionStats | None = None
        self._console = Console()
        self._lock = asyncio.Lock()

    @property
    def all_stats(self) -> list[ExecutionStats]:
        """Get all collected stats."""
        return self._stats.copy()

    @contextmanager
    def track_execution(
        self,
        etl_name: str,
        environment: str,
        metadata: dict[str, Any] | None = None,
    ) -> Iterator[ExecutionStats]:
        """
        Track an entire ETL execution.
        
        Args:
            etl_name: Name of the ETL.
            environment: Environment name.
            metadata: Additional metadata.
            
        Yields:
            ExecutionStats object to track.
        """
        stats = ExecutionStats(
            etl_name=etl_name,
            environment=environment,
            start_time=datetime.now(),
            metadata=metadata or {},
        )
        self._current_stats = stats
        
        try:
            yield stats
        except Exception as e:
            stats.errors.append(str(e))
            raise
        finally:
            stats.finalize()
            self._stats.append(stats)
            self._current_stats = None
            
            logger.info(
                "execution_complete",
                etl_name=etl_name,
                environment=environment,
                duration_ms=stats.duration_ms,
                successful=stats.successful,
                failed=stats.failed,
            )

    @asynccontextmanager
    async def track_execution_async(
        self,
        etl_name: str,
        environment: str,
        metadata: dict[str, Any] | None = None,
    ) -> AsyncIterator[ExecutionStats]:
        """
        Track an ETL execution asynchronously.
        
        Args:
            etl_name: Name of the ETL.
            environment: Environment name.
            metadata: Additional metadata.
            
        Yields:
            ExecutionStats object to track.
        """
        async with self._lock:
            stats = ExecutionStats(
                etl_name=etl_name,
                environment=environment,
                start_time=datetime.now(),
                metadata=metadata or {},
            )
        
        try:
            yield stats
        except Exception as e:
            stats.errors.append(str(e))
            raise
        finally:
            stats.finalize()
            async with self._lock:
                self._stats.append(stats)
            
            logger.info(
                "execution_complete",
                etl_name=etl_name,
                environment=environment,
                duration_ms=stats.duration_ms,
                successful=stats.successful,
                failed=stats.failed,
            )

    @contextmanager
    def track_step(
        self,
        stats: ExecutionStats,
        step_name: str,
        metadata: dict[str, Any] | None = None,
    ) -> Iterator[StepTiming]:
        """
        Track a single step within an execution.
        
        Args:
            stats: Parent ExecutionStats.
            step_name: Name of the step.
            metadata: Additional metadata.
            
        Yields:
            StepTiming object.
        """
        timing = StepTiming(
            name=step_name,
            start_time=datetime.now(),
            metadata=metadata or {},
        )
        
        try:
            yield timing
            timing.complete(success=True)
        except Exception as e:
            timing.complete(success=False, error=str(e))
            raise
        finally:
            stats.step_timings[step_name] = timing
            
            logger.debug(
                "step_complete",
                step=step_name,
                duration_ms=timing.duration_ms,
                success=timing.success,
            )

    @asynccontextmanager
    async def track_step_async(
        self,
        stats: ExecutionStats,
        step_name: str,
        metadata: dict[str, Any] | None = None,
    ) -> AsyncIterator[StepTiming]:
        """
        Track a single step asynchronously.
        
        Args:
            stats: Parent ExecutionStats.
            step_name: Name of the step.
            metadata: Additional metadata.
            
        Yields:
            StepTiming object.
        """
        timing = StepTiming(
            name=step_name,
            start_time=datetime.now(),
            metadata=metadata or {},
        )
        
        try:
            yield timing
            timing.complete(success=True)
        except Exception as e:
            timing.complete(success=False, error=str(e))
            raise
        finally:
            stats.step_timings[step_name] = timing
            
            logger.debug(
                "step_complete",
                step=step_name,
                duration_ms=timing.duration_ms,
                success=timing.success,
            )

    def aggregate(self) -> AggregatedStats:
        """
        Aggregate all collected statistics.
        
        Returns:
            AggregatedStats with totals and breakdowns.
        """
        agg = AggregatedStats(
            start_time=min((s.start_time for s in self._stats), default=datetime.now()),
            end_time=max(
                (s.end_time for s in self._stats if s.end_time),
                default=None,
            ),
        )
        
        for stats in self._stats:
            agg.total_executions += 1
            agg.total_events += stats.total_events
            agg.total_successful += stats.successful
            agg.total_failed += stats.failed
            agg.total_duration_ms += stats.duration_ms
            agg.all_stats.append(stats)
            
            # Aggregate by environment
            if stats.environment not in agg.by_environment:
                agg.by_environment[stats.environment] = ExecutionStats(
                    etl_name="all",
                    environment=stats.environment,
                )
            env_stats = agg.by_environment[stats.environment]
            env_stats.total_events += stats.total_events
            env_stats.successful += stats.successful
            env_stats.failed += stats.failed
            env_stats.duration_ms += stats.duration_ms
            
            # Aggregate by ETL
            if stats.etl_name not in agg.by_etl:
                agg.by_etl[stats.etl_name] = ExecutionStats(
                    etl_name=stats.etl_name,
                    environment="all",
                )
            etl_stats = agg.by_etl[stats.etl_name]
            etl_stats.total_events += stats.total_events
            etl_stats.successful += stats.successful
            etl_stats.failed += stats.failed
            etl_stats.duration_ms += stats.duration_ms
        
        return agg

    def print_summary(self) -> None:
        """Print a summary table to console."""
        agg = self.aggregate()
        
        # Overall summary
        self._console.print("\n[bold cyan]Execution Summary[/bold cyan]")
        self._console.print(f"Total Executions: {agg.total_executions}")
        self._console.print(f"Total Events: {agg.total_events}")
        self._console.print(f"Successful: {agg.total_successful}")
        self._console.print(f"Failed: {agg.total_failed}")
        self._console.print(f"Success Rate: {agg.success_rate:.2f}%")
        self._console.print(f"Total Duration: {agg.total_duration_ms:.2f}ms")
        self._console.print(f"Throughput: {agg.overall_events_per_second:.2f} events/sec")
        
        # By environment table
        if agg.by_environment:
            self._console.print("\n[bold cyan]By Environment[/bold cyan]")
            env_table = Table(show_header=True)
            env_table.add_column("Environment")
            env_table.add_column("Events", justify="right")
            env_table.add_column("Success", justify="right")
            env_table.add_column("Failed", justify="right")
            env_table.add_column("Duration (ms)", justify="right")
            
            for env, stats in sorted(agg.by_environment.items()):
                env_table.add_row(
                    env,
                    str(stats.total_events),
                    str(stats.successful),
                    str(stats.failed),
                    f"{stats.duration_ms:.2f}",
                )
            
            self._console.print(env_table)
        
        # By ETL table
        if agg.by_etl:
            self._console.print("\n[bold cyan]By ETL[/bold cyan]")
            etl_table = Table(show_header=True)
            etl_table.add_column("ETL")
            etl_table.add_column("Events", justify="right")
            etl_table.add_column("Success", justify="right")
            etl_table.add_column("Failed", justify="right")
            etl_table.add_column("Duration (ms)", justify="right")
            
            for etl, stats in sorted(agg.by_etl.items()):
                etl_table.add_row(
                    etl,
                    str(stats.total_events),
                    str(stats.successful),
                    str(stats.failed),
                    f"{stats.duration_ms:.2f}",
                )
            
            self._console.print(etl_table)

    def print_detailed(self) -> None:
        """Print detailed statistics for each execution."""
        self._console.print("\n[bold cyan]Detailed Execution Stats[/bold cyan]")
        
        for stats in self._stats:
            self._console.print(f"\n[bold]{stats.etl_name}[/bold] @ {stats.environment}")
            self._console.print(f"  Events: {stats.successful}/{stats.total_events}")
            self._console.print(f"  Duration: {stats.duration_ms:.2f}ms")
            self._console.print(f"  Throughput: {stats.events_per_second:.2f} events/sec")
            
            if stats.step_timings:
                self._console.print("  Steps:")
                for name, timing in stats.step_timings.items():
                    status = "[green]OK[/green]" if timing.success else "[red]FAIL[/red]"
                    self._console.print(f"    {name}: {timing.duration_ms:.2f}ms {status}")
            
            if stats.errors:
                self._console.print("  [red]Errors:[/red]")
                for error in stats.errors:
                    self._console.print(f"    - {error}")

    def clear(self) -> None:
        """Clear all collected statistics."""
        self._stats.clear()

