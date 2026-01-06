"""
ETL Runner for orchestrating ETL execution.

Provides:
- Parallel execution across environments
- Configurable filtering and limits
- Comprehensive metrics collection
- Template rendering with runtime context
"""

import asyncio
import uuid
from datetime import datetime
from typing import Any

import orjson
import structlog

from velox.connectors.kafka.batch import BatchResult, Message
from velox.connectors.kafka.config import KafkaEnvironmentConfig
from velox.connectors.kafka.producer import AsyncKafkaProducer
from velox.core.config import ConfigManager
from velox.core.exceptions import ETLExecutionError
from velox.core.metrics import AggregatedStats, ExecutionStats, MetricsCollector
from velox.core.templating import TemplateEngine
from velox.etl.config import ETLConfig, EventConfig, RunnerConfig
from velox.etl.registry import ETLRegistry

logger = structlog.get_logger()


class ETLRunner:
    """
    Orchestrates ETL execution with parallel processing.
    
    Features:
    - Multi-environment execution
    - Parallel ETL processing
    - Configurable filters and limits
    - Dynamic template rendering
    - Comprehensive metrics
    
    Usage:
        config_manager = ConfigManager(config_dir=Path("config"))
        registry = ETLRegistry(config_manager)
        registry.load_all()
        
        runner = ETLRunner(config_manager, registry)
        
        # Run with filters
        stats = await runner.run(
            runner_config=RunnerConfig(
                filters=RunnerFilters(environments=["dev", "qa"]),
                limits=RunnerLimits(max_events_per_etl=100),
            )
        )
        
        runner.print_stats()
    """

    def __init__(
        self,
        config_manager: ConfigManager,
        registry: ETLRegistry,
        template_engine: TemplateEngine | None = None,
    ) -> None:
        """
        Initialize the ETL runner.
        
        Args:
            config_manager: Configuration manager.
            registry: ETL registry.
            template_engine: Optional custom template engine.
        """
        self._config_manager = config_manager
        self._registry = registry
        self._template_engine = template_engine or config_manager.template_engine
        self._metrics = MetricsCollector()
        
        # Connection pools per environment
        self._producers: dict[str, AsyncKafkaProducer] = {}
        
        # Execution state
        self._running = False
        self._total_events_sent = 0

    @property
    def metrics(self) -> MetricsCollector:
        """Get the metrics collector."""
        return self._metrics

    @property
    def registry(self) -> ETLRegistry:
        """Get the ETL registry."""
        return self._registry

    async def run(
        self,
        runner_config: RunnerConfig | None = None,
        event_config: EventConfig | None = None,
        context: dict[str, Any] | None = None,
    ) -> AggregatedStats:
        """
        Run ETLs according to configuration.
        
        Args:
            runner_config: Runner configuration.
            event_config: Event generation configuration.
            context: Additional context for template rendering.
            
        Returns:
            Aggregated execution statistics.
        """
        runner_config = runner_config or RunnerConfig()
        event_config = event_config or EventConfig()
        
        # Merge contexts
        full_context = {
            **runner_config.context,
            **(context or {}),
        }
        
        self._running = True
        self._total_events_sent = 0
        self._metrics.clear()
        
        logger.info(
            "etl_run_starting",
            filters=runner_config.filters.model_dump(),
            limits=runner_config.limits.model_dump(),
            dry_run=runner_config.dry_run,
        )
        
        try:
            # Get filtered ETLs
            etls = list(self._registry.filter(
                enabled_only=True,
                filters=runner_config.filters,
            ))
            
            if runner_config.limits.max_etls:
                etls = etls[:runner_config.limits.max_etls]
            
            logger.info("etl_run_filtered", etl_count=len(etls))
            
            if not etls:
                logger.warning("etl_run_no_etls", message="No ETLs match filters")
                return self._metrics.aggregate()
            
            # Get environments
            environments = self._get_target_environments(etls, runner_config)
            
            if not environments:
                logger.warning("etl_run_no_environments")
                return self._metrics.aggregate()
            
            # Initialize producers for each environment
            await self._init_producers(environments, runner_config)
            
            # Execute ETLs
            if runner_config.execution.parallel_workers > 1:
                await self._run_parallel(
                    etls, environments, runner_config, event_config, full_context
                )
            else:
                await self._run_sequential(
                    etls, environments, runner_config, event_config, full_context
                )
            
        except Exception as e:
            logger.error("etl_run_failed", error=str(e))
            raise ETLExecutionError(f"ETL run failed: {e}") from e
            
        finally:
            self._running = False
            await self._cleanup_producers()
        
        # Get aggregated stats
        stats = self._metrics.aggregate()
        
        logger.info(
            "etl_run_complete",
            total_events=stats.total_events,
            successful=stats.total_successful,
            failed=stats.total_failed,
            duration_ms=stats.total_duration_ms,
        )
        
        return stats

    async def run_single_etl(
        self,
        etl_name: str,
        environments: list[str] | None = None,
        event_config: EventConfig | None = None,
        context: dict[str, Any] | None = None,
        dry_run: bool = False,
    ) -> list[ExecutionStats]:
        """
        Run a single ETL across specified environments.
        
        Args:
            etl_name: Name of the ETL to run.
            environments: Environments to run in (None = all configured).
            event_config: Event configuration.
            context: Template context.
            dry_run: If true, don't actually send.
            
        Returns:
            List of execution stats per environment.
        """
        etl = self._registry.get_or_raise(etl_name)
        event_config = event_config or EventConfig()
        
        # Get environments
        target_envs = environments or list(etl.topics.keys())
        
        # Initialize producers
        runner_config = RunnerConfig(dry_run=dry_run)
        await self._init_producers(target_envs, runner_config)
        
        results: list[ExecutionStats] = []
        
        try:
            for env in target_envs:
                if env not in etl.topics:
                    continue
                
                async with self._metrics.track_execution_async(etl.name, env) as stats:
                    await self._execute_etl(
                        etl, env, event_config, context or {}, runner_config, stats
                    )
                    results.append(stats)
                    
        finally:
            await self._cleanup_producers()
        
        return results

    async def _run_parallel(
        self,
        etls: list[ETLConfig],
        environments: list[str],
        runner_config: RunnerConfig,
        event_config: EventConfig,
        context: dict[str, Any],
    ) -> None:
        """Run ETLs in parallel."""
        semaphore = asyncio.Semaphore(runner_config.execution.parallel_workers)
        
        async def run_etl_env(etl: ETLConfig, env: str) -> None:
            async with semaphore:
                if not self._running:
                    return
                
                if self._check_limits_exceeded(runner_config):
                    return
                
                async with self._metrics.track_execution_async(etl.name, env) as stats:
                    try:
                        await self._execute_etl(
                            etl, env, event_config, context, runner_config, stats
                        )
                    except Exception as e:
                        stats.errors.append(str(e))
                        if not runner_config.execution.continue_on_error:
                            self._running = False
                            raise
        
        # Create tasks for all ETL x environment combinations
        tasks = []
        for etl in etls:
            for env in environments:
                if env in etl.topics and runner_config.filters.matches(etl, env):
                    tasks.append(run_etl_env(etl, env))
        
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _run_sequential(
        self,
        etls: list[ETLConfig],
        environments: list[str],
        runner_config: RunnerConfig,
        event_config: EventConfig,
        context: dict[str, Any],
    ) -> None:
        """Run ETLs sequentially."""
        for etl in etls:
            if not self._running:
                break
            
            for env in environments:
                if not self._running:
                    break
                
                if env not in etl.topics:
                    continue
                
                if not runner_config.filters.matches(etl, env):
                    continue
                
                if self._check_limits_exceeded(runner_config):
                    break
                
                async with self._metrics.track_execution_async(etl.name, env) as stats:
                    try:
                        await self._execute_etl(
                            etl, env, event_config, context, runner_config, stats
                        )
                    except Exception as e:
                        stats.errors.append(str(e))
                        if not runner_config.execution.continue_on_error:
                            raise

    async def _execute_etl(
        self,
        etl: ETLConfig,
        environment: str,
        event_config: EventConfig,
        context: dict[str, Any],
        runner_config: RunnerConfig,
        stats: ExecutionStats,
    ) -> None:
        """Execute a single ETL for an environment."""
        topic = etl.get_topic(environment)
        if not topic:
            raise ETLExecutionError(
                f"No topic configured for {etl.name} in {environment}",
                etl_name=etl.name,
                environment=environment,
            )
        
        producer = self._producers.get(environment)
        if not producer and not runner_config.dry_run:
            raise ETLExecutionError(
                f"No producer for environment: {environment}",
                etl_name=etl.name,
                environment=environment,
            )
        
        # Determine event count
        event_count = event_config.count
        if runner_config.limits.max_events_per_etl:
            event_count = min(event_count, runner_config.limits.max_events_per_etl)
        
        stats.total_events = event_count
        
        logger.info(
            "etl_executing",
            etl=etl.name,
            environment=environment,
            topic=topic,
            event_count=event_count,
            dry_run=runner_config.dry_run,
        )
        
        # Generate and send messages
        messages: list[Message] = []
        
        async with self._metrics.track_step_async(stats, "render_templates"):
            for i in range(event_count):
                # Build context for this event
                event_context = {
                    **context,
                    **event_config.event_context,
                    "etl_name": etl.name,
                    "environment": environment,
                    "topic": topic,
                    "event_index": i,
                    "event_count": event_count,
                    "correlation_id": str(uuid.uuid4()),
                    "timestamp": datetime.now().isoformat(),
                    "timestamp_ms": int(datetime.now().timestamp() * 1000),
                }
                
                # Render payload
                payload = await self._render_payload(etl, event_context)
                
                # Render headers
                headers = await self._render_headers(etl, event_context)
                
                # Render key if configured
                key = None
                if etl.key_template:
                    key = self._template_engine.render_string(
                        etl.key_template, event_context
                    ).encode("utf-8")
                
                message = Message(
                    topic=topic,
                    value=payload,
                    key=key,
                    headers=headers,
                    partition=etl.partition,
                    correlation_id=event_context["correlation_id"],
                    etl_name=etl.name,
                    environment=environment,
                )
                messages.append(message)
                
                if runner_config.log_payloads:
                    logger.debug(
                        "payload_rendered",
                        etl=etl.name,
                        payload=payload.decode("utf-8")[:500],
                    )
        
        # Send messages
        if runner_config.dry_run:
            stats.successful = len(messages)
            logger.info(
                "etl_dry_run",
                etl=etl.name,
                environment=environment,
                would_send=len(messages),
            )
        else:
            async with self._metrics.track_step_async(stats, "send_messages"):
                batch_result = await producer.send_batch(
                    messages,
                    parallel=etl.parallel,
                    max_concurrency=runner_config.execution.parallel_workers,
                )
                
                stats.successful = batch_result.successful
                stats.failed = batch_result.failed
                stats.errors.extend(batch_result.errors)
                
                self._total_events_sent += batch_result.successful
        
        logger.info(
            "etl_executed",
            etl=etl.name,
            environment=environment,
            successful=stats.successful,
            failed=stats.failed,
            duration_ms=stats.duration_ms,
        )

    async def _render_payload(
        self,
        etl: ETLConfig,
        context: dict[str, Any],
    ) -> bytes:
        """Render payload template."""
        template_path = f"payloads/{etl.payload_template}"
        rendered = await self._template_engine.render_async(template_path, context)
        return rendered.encode("utf-8")

    async def _render_headers(
        self,
        etl: ETLConfig,
        context: dict[str, Any],
    ) -> list[tuple[str, bytes]]:
        """Render message headers."""
        headers: list[tuple[str, bytes]] = []
        
        for key, value_template in etl.headers.items():
            rendered = self._template_engine.render_string(value_template, context)
            headers.append((key, rendered.encode("utf-8")))
        
        return headers

    def _get_target_environments(
        self,
        etls: list[ETLConfig],
        runner_config: RunnerConfig,
    ) -> list[str]:
        """Get target environments based on ETLs and filters."""
        all_envs: set[str] = set()
        
        for etl in etls:
            all_envs.update(etl.topics.keys())
        
        # Apply filters
        filtered = [
            env for env in all_envs
            if runner_config.filters.matches_environment(env)
        ]
        
        # Apply limits
        if runner_config.limits.max_environments:
            filtered = filtered[:runner_config.limits.max_environments]
        
        return sorted(filtered)

    async def _init_producers(
        self,
        environments: list[str],
        runner_config: RunnerConfig,
    ) -> None:
        """Initialize Kafka producers for environments."""
        if runner_config.dry_run:
            return
        
        for env in environments:
            if env in self._producers:
                continue
            
            try:
                # Load environment config
                env_config_data = self._config_manager.load_environment_config(env)
                env_config = KafkaEnvironmentConfig.model_validate({
                    "environment": env,
                    **env_config_data,
                })
                
                # Create producer
                producer = AsyncKafkaProducer(
                    kafka_config=env_config.kafka,
                    environment=env,
                    producer_config=env_config.producer,
                    batch_size=runner_config.execution.batch_size,
                    batch_timeout_ms=runner_config.execution.batch_timeout_ms,
                )
                
                await producer.connect()
                self._producers[env] = producer
                
                logger.info("producer_initialized", environment=env)
                
            except Exception as e:
                logger.error(
                    "producer_init_failed",
                    environment=env,
                    error=str(e),
                )
                if not runner_config.execution.continue_on_error:
                    raise

    async def _cleanup_producers(self) -> None:
        """Cleanup all producers."""
        for env, producer in self._producers.items():
            try:
                await producer.disconnect()
            except Exception as e:
                logger.warning(
                    "producer_cleanup_error",
                    environment=env,
                    error=str(e),
                )
        
        self._producers.clear()

    def _check_limits_exceeded(self, runner_config: RunnerConfig) -> bool:
        """Check if any limits have been exceeded."""
        limits = runner_config.limits
        
        if limits.max_total_events and self._total_events_sent >= limits.max_total_events:
            logger.info("limit_reached", limit="max_total_events")
            return True
        
        return False

    def stop(self) -> None:
        """Signal the runner to stop."""
        self._running = False

    def print_stats(self) -> None:
        """Print execution statistics."""
        self._metrics.print_summary()

    def print_detailed_stats(self) -> None:
        """Print detailed execution statistics."""
        self._metrics.print_detailed()

    def get_stats(self) -> AggregatedStats:
        """Get aggregated statistics."""
        return self._metrics.aggregate()

