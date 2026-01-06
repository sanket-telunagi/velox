"""
ETL configuration models.

Provides type-safe configuration for:
- Individual ETL definitions
- Runner execution settings
- Filtering and limits
"""

from typing import Any

from pydantic import BaseModel, Field, field_validator


class ETLTopicConfig(BaseModel):
    """Topic configuration per environment."""
    
    # Maps environment name to topic name
    topics: dict[str, str] = Field(
        default_factory=dict,
        description="Mapping of environment to topic name"
    )
    
    def get_topic(self, environment: str) -> str | None:
        """Get topic for an environment."""
        return self.topics.get(environment)


class ETLHeaderConfig(BaseModel):
    """Header configuration for ETL messages."""
    
    headers: dict[str, str] = Field(
        default_factory=dict,
        description="Header templates (supports Jinja)"
    )
    
    def get_headers(self) -> dict[str, str]:
        """Get all headers."""
        return self.headers.copy()


class ETLConfig(BaseModel):
    """
    Configuration for a single ETL.
    
    Defines:
    - Basic metadata (name, description, category)
    - Topic mappings per environment
    - Payload template
    - Header configuration
    - Authentication overrides
    """
    
    name: str = Field(description="Unique ETL name")
    description: str = Field(default="", description="ETL description")
    category: str = Field(default="default", description="ETL category for filtering")
    enabled: bool = Field(default=True, description="Whether ETL is enabled")
    
    # Topic configuration
    topics: dict[str, str] = Field(
        default_factory=dict,
        description="Environment to topic mapping"
    )
    
    # Payload configuration
    payload_template: str = Field(
        description="Name of the Jinja template file for payload"
    )
    
    # Header configuration (supports Jinja templating)
    headers: dict[str, str] = Field(
        default_factory=dict,
        description="Message headers (templates)"
    )
    
    # Optional authentication override
    auth_override: str | None = Field(
        default=None,
        description="Override authentication method for this ETL"
    )
    
    # Message configuration
    key_template: str | None = Field(
        default=None,
        description="Optional message key template"
    )
    partition: int | None = Field(
        default=None,
        description="Target partition (None for default partitioning)"
    )
    
    # Execution settings
    batch_size: int | None = Field(
        default=None,
        description="Override batch size for this ETL"
    )
    parallel: bool = Field(
        default=True,
        description="Whether to send messages in parallel"
    )
    
    # Retry settings
    max_retries: int = Field(default=3, description="Maximum retry attempts")
    retry_backoff_ms: int = Field(default=100, description="Backoff between retries")
    
    # Extra configuration
    extra: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional configuration options"
    )

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        """Validate ETL name."""
        if not v or not v.strip():
            raise ValueError("ETL name cannot be empty")
        return v.strip().lower().replace(" ", "_")

    def get_topic(self, environment: str) -> str | None:
        """Get topic for a specific environment."""
        return self.topics.get(environment)

    def get_all_environments(self) -> list[str]:
        """Get all configured environments."""
        return list(self.topics.keys())


class RunnerFilters(BaseModel):
    """
    Filters for controlling which ETLs and environments to run.
    
    All filters are optional - empty lists mean "all".
    """
    
    environments: list[str] = Field(
        default_factory=list,
        description="Environments to run (empty = all)"
    )
    categories: list[str] = Field(
        default_factory=list,
        description="ETL categories to run (empty = all)"
    )
    etls: list[str] = Field(
        default_factory=list,
        description="Specific ETL names to run (empty = all)"
    )
    
    # Exclusion filters
    exclude_environments: list[str] = Field(
        default_factory=list,
        description="Environments to exclude"
    )
    exclude_categories: list[str] = Field(
        default_factory=list,
        description="Categories to exclude"
    )
    exclude_etls: list[str] = Field(
        default_factory=list,
        description="ETLs to exclude"
    )

    def matches_environment(self, environment: str) -> bool:
        """Check if environment matches filters."""
        if environment in self.exclude_environments:
            return False
        if self.environments and environment not in self.environments:
            return False
        return True

    def matches_category(self, category: str) -> bool:
        """Check if category matches filters."""
        if category in self.exclude_categories:
            return False
        if self.categories and category not in self.categories:
            return False
        return True

    def matches_etl(self, etl_name: str) -> bool:
        """Check if ETL matches filters."""
        if etl_name in self.exclude_etls:
            return False
        if self.etls and etl_name not in self.etls:
            return False
        return True

    def matches(self, etl: ETLConfig, environment: str) -> bool:
        """Check if ETL and environment combination matches all filters."""
        return (
            self.matches_environment(environment)
            and self.matches_category(etl.category)
            and self.matches_etl(etl.name)
        )


class RunnerLimits(BaseModel):
    """
    Limits for controlling execution volume.
    
    Provides micro-level control over how much is processed.
    """
    
    max_events_per_etl: int | None = Field(
        default=None,
        description="Maximum events to send per ETL"
    )
    max_events_per_environment: int | None = Field(
        default=None,
        description="Maximum events per environment"
    )
    max_total_events: int | None = Field(
        default=None,
        description="Maximum total events across all"
    )
    max_etls: int | None = Field(
        default=None,
        description="Maximum number of ETLs to run"
    )
    max_environments: int | None = Field(
        default=None,
        description="Maximum environments per ETL"
    )
    
    # Time limits
    timeout_seconds: int | None = Field(
        default=None,
        description="Overall timeout in seconds"
    )
    timeout_per_etl_seconds: int | None = Field(
        default=None,
        description="Timeout per ETL in seconds"
    )


class ExecutionConfig(BaseModel):
    """Configuration for execution behavior."""
    
    parallel_workers: int = Field(
        default=10,
        description="Number of parallel workers"
    )
    batch_size: int = Field(
        default=100,
        description="Default batch size"
    )
    batch_timeout_ms: int = Field(
        default=5000,
        description="Batch timeout in milliseconds"
    )
    
    # Retry configuration
    max_retries: int = Field(default=3, description="Maximum retries")
    retry_backoff_ms: int = Field(default=100, description="Retry backoff")
    
    # Connection pool
    connection_pool_size: int = Field(
        default=5,
        description="Kafka connection pool size per environment"
    )
    
    # Error handling
    fail_fast: bool = Field(
        default=False,
        description="Stop on first error"
    )
    continue_on_error: bool = Field(
        default=True,
        description="Continue to next ETL on error"
    )


class RunnerConfig(BaseModel):
    """
    Complete runner configuration.
    
    Combines execution settings, filters, and limits for
    fine-grained control over ETL execution.
    """
    
    execution: ExecutionConfig = Field(
        default_factory=ExecutionConfig,
        description="Execution settings"
    )
    filters: RunnerFilters = Field(
        default_factory=RunnerFilters,
        description="Filtering criteria"
    )
    limits: RunnerLimits = Field(
        default_factory=RunnerLimits,
        description="Execution limits"
    )
    
    # Context for template rendering
    context: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional context for templates"
    )
    
    # Dry run mode
    dry_run: bool = Field(
        default=False,
        description="If true, don't actually send messages"
    )
    
    # Logging
    verbose: bool = Field(
        default=False,
        description="Enable verbose logging"
    )
    log_payloads: bool = Field(
        default=False,
        description="Log rendered payloads (careful with sensitive data)"
    )

    @classmethod
    def from_yaml(cls, data: dict[str, Any]) -> "RunnerConfig":
        """Create from YAML data."""
        return cls.model_validate(data)


class EventConfig(BaseModel):
    """
    Configuration for generating ETL events.
    
    Defines how many events to generate and with what parameters.
    """
    
    count: int = Field(
        default=1,
        description="Number of events to generate"
    )
    
    # Context per event (supports templates)
    event_context: dict[str, Any] = Field(
        default_factory=dict,
        description="Context to pass to each event"
    )
    
    # Variable iteration
    iterate_over: str | None = Field(
        default=None,
        description="Context key to iterate over for multiple events"
    )
    
    # Delay between events
    delay_ms: int = Field(
        default=0,
        description="Delay between events in milliseconds"
    )

