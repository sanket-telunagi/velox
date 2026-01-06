# Velox

A high-performance, async-first Python library for ETL connectors with multi-environment support, Jinja templating, and comprehensive metrics.

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Usage Examples](#usage-examples)
- [API Reference](#api-reference)
- [Extending the Library](#extending-the-library)
- [Performance Tuning](#performance-tuning)
- [Troubleshooting](#troubleshooting)

## Features

- **Async-First Design**: Built on `asyncio` for high concurrency and throughput
- **Multi-Environment Support**: Manage 6+ environments (dev, qa, staging, uat, preprod, prod)
- **Jinja2 Templating**: Dynamic payload and configuration rendering with runtime context
- **Multiple Authentication Methods**: SASL_SSL, SSL (mutual TLS), SASL_PLAINTEXT, PLAINTEXT
- **Batch Processing**: Configurable batch sizes with parallel execution
- **Comprehensive Metrics**: Step-by-step timing, throughput, and success rates
- **Highly Configurable**: External YAML configuration for everything
- **Type Safe**: Full Pydantic model validation and type hints
- **Modular Architecture**: Easy to extend with new connectors

## Architecture

### High-Level Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              Velox Library                               │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐                 │
│  │   .env       │   │ environments │   │  etl/*.yaml  │                 │
│  │   secrets    │   │   /*.yaml    │   │  definitions │                 │
│  └──────┬───────┘   └──────┬───────┘   └──────┬───────┘                 │
│         │                  │                  │                          │
│         └────────────┬─────┴─────────────────┘                          │
│                      ▼                                                   │
│         ┌────────────────────────┐                                      │
│         │    ConfigManager       │                                      │
│         │  (Jinja + YAML + .env) │                                      │
│         └───────────┬────────────┘                                      │
│                     │                                                    │
│         ┌───────────┴────────────┐                                      │
│         ▼                        ▼                                      │
│  ┌─────────────────┐    ┌─────────────────┐                            │
│  │  ETL Registry   │    │ Template Engine │                            │
│  │  (ETL configs)  │    │   (payloads)    │                            │
│  └────────┬────────┘    └────────┬────────┘                            │
│           │                      │                                      │
│           └──────────┬───────────┘                                      │
│                      ▼                                                   │
│         ┌────────────────────────┐                                      │
│         │      ETL Runner        │                                      │
│         │ (orchestration layer)  │                                      │
│         └───────────┬────────────┘                                      │
│                     │                                                    │
│    ┌────────────────┼────────────────┐                                  │
│    ▼                ▼                ▼                                  │
│ ┌──────┐       ┌──────┐        ┌──────────┐                            │
│ │ Kafka│       │ Kafka│        │ Metrics  │                            │
│ │Producer     │Consumer│        │Collector │                            │
│ └──────┘       └──────┘        └──────────┘                            │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### Data Flow

```
User Request
     │
     ▼
┌─────────────────┐
│   ETL Runner    │
│                 │
│ 1. Load configs │
│ 2. Filter ETLs  │
│ 3. Apply limits │
└────────┬────────┘
         │
         ▼
┌─────────────────┐     ┌─────────────────┐
│  For each ETL   │────▶│ Template Engine │
│  x Environment  │     │                 │
└────────┬────────┘     │ Render payload  │
         │              │ Render headers  │
         │              └────────┬────────┘
         │                       │
         ▼                       ▼
┌─────────────────┐     ┌─────────────────┐
│ Kafka Producer  │◀────│    Messages     │
│                 │     │  (batch/single) │
│ Send to topic   │     └─────────────────┘
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Metrics Collector│
│                  │
│ - Duration       │
│ - Success/Fail   │
│ - Throughput     │
└──────────────────┘
```

### Project Structure

```
velox/
├── pyproject.toml              # Dependencies and project config
├── .gitignore
├── .env.example                # Environment variables template
├── .env                        # Actual secrets (git-ignored)
├── README.md
│
├── config/
│   ├── environments/           # Environment configurations
│   │   ├── dev.yaml
│   │   ├── qa.yaml
│   │   ├── staging.yaml
│   │   ├── uat.yaml
│   │   ├── preprod.yaml
│   │   └── prod.yaml
│   ├── etl/                    # ETL definitions
│   │   ├── sample_data_sync.yaml
│   │   ├── customer_update.yaml
│   │   └── inventory_refresh.yaml
│   └── runner.yaml             # Runner configuration
│
├── templates/
│   └── payloads/               # Jinja2 payload templates
│       ├── sample_data_sync.json.j2
│       ├── customer_update.json.j2
│       └── inventory_refresh.json.j2
│
└── src/
    └── velox/
        ├── __init__.py
        ├── py.typed
        │
        ├── core/               # Core abstractions
        │   ├── base.py         # Abstract connector class
        │   ├── config.py       # ConfigManager
        │   ├── templating.py   # Jinja2 engine
        │   ├── metrics.py      # Metrics collection
        │   └── exceptions.py   # Custom exceptions
        │
        ├── connectors/
        │   ├── base.py
        │   └── kafka/          # Kafka connector
        │       ├── producer.py # AsyncKafkaProducer
        │       ├── consumer.py # AsyncKafkaConsumer
        │       ├── auth.py     # Authentication handlers
        │       ├── config.py   # Kafka config models
        │       └── batch.py    # Batch processing
        │
        └── etl/
            ├── config.py       # ETL config models
            ├── registry.py     # ETL registry
            └── runner.py       # ETL runner
```

## Installation

### Requirements

- Python 3.11+
- pip or pipx

### Install from Source

```bash
# Clone the repository
cd C:\Users\sankette\Documents\Examples\Python\velox

# Create virtual environment
python -m venv .venv
.venv\Scripts\activate  # Windows
# source .venv/bin/activate  # Linux/macOS

# Install in development mode
pip install -e ".[dev]"

# Or install with all optional dependencies
pip install -e ".[all]"
```

### Dependencies

| Package | Purpose |
|---------|---------|
| `aiokafka` | Async Kafka client |
| `orjson` | Fast JSON serialization (Rust-based) |
| `pydantic` | Config validation (Rust core) |
| `jinja2` | Template rendering |
| `python-dotenv` | .env file loading |
| `pyyaml` | YAML parsing |
| `structlog` | Structured logging |
| `rich` | CLI output and progress |
| `uvloop` | Fast event loop (Unix only) |

## Quick Start

### 1. Copy Environment Template

```bash
copy .env.example .env
# Edit .env with your actual values
```

### 2. Basic Usage

```python
import asyncio
from pathlib import Path

from velox.core.config import ConfigManager
from velox.etl.registry import ETLRegistry
from velox.etl.runner import ETLRunner
from velox.etl.config import RunnerConfig, RunnerFilters, EventConfig

async def main():
    # Initialize configuration
    config_manager = ConfigManager(
        config_dir=Path("config"),
        template_dir=Path("templates"),
        env_file=Path(".env"),
    )
    
    # Load ETL registry
    registry = ETLRegistry(config_manager)
    registry.load_all()
    
    # Create runner
    runner = ETLRunner(config_manager, registry)
    
    # Run ETLs
    stats = await runner.run(
        runner_config=RunnerConfig(
            filters=RunnerFilters(
                environments=["dev", "qa"],
                categories=["data_sync"],
            ),
        ),
        event_config=EventConfig(count=10),
        context={
            "source_system": "my-app",
            "requested_by": "admin",
        },
    )
    
    # Print results
    runner.print_stats()

if __name__ == "__main__":
    asyncio.run(main())
```

### 3. Direct Kafka Producer Usage

```python
import asyncio
from velox.connectors.kafka import (
    AsyncKafkaProducer,
    KafkaConfig,
    Message,
)

async def send_messages():
    config = KafkaConfig(
        bootstrap_servers="localhost:9092",
        security_protocol="PLAINTEXT",
    )
    
    async with AsyncKafkaProducer(config, "dev") as producer:
        # Single message
        result = await producer.send(
            topic="my-topic",
            value=b'{"key": "value"}',
            headers=[("Content-Type", b"application/json")],
        )
        print(f"Sent to partition {result.partition}, offset {result.offset}")
        
        # Batch messages
        messages = [
            Message(topic="my-topic", value=f'{{"id": {i}}}'.encode())
            for i in range(100)
        ]
        batch_result = await producer.send_batch(messages, parallel=True)
        print(f"Batch: {batch_result.successful}/{batch_result.total} successful")

asyncio.run(send_messages())
```

## Configuration

### Environment Variables (.env)

```bash
# Environment selection
ACTIVE_ENVIRONMENT=dev

# Kafka - Dev
KAFKA_DEV_BOOTSTRAP=localhost:9092
KAFKA_DEV_SECURITY_PROTOCOL=PLAINTEXT

# Kafka - QA (SASL_SSL)
KAFKA_QA_BOOTSTRAP=kafka-qa.example.com:9093
KAFKA_QA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_QA_SASL_MECHANISM=PLAIN
KAFKA_QA_USERNAME=qa_user
KAFKA_QA_PASSWORD=secret
KAFKA_QA_SSL_CA_PATH=/path/to/ca.pem

# Kafka - Production (mutual TLS)
KAFKA_PROD_BOOTSTRAP=kafka-prod.example.com:9093
KAFKA_PROD_SECURITY_PROTOCOL=SSL
KAFKA_PROD_SSL_CA_PATH=/path/to/ca.pem
KAFKA_PROD_SSL_CERT_PATH=/path/to/client.pem
KAFKA_PROD_SSL_KEY_PATH=/path/to/client.key
```

### Environment Configuration (YAML)

Environment configs support Jinja2 templating with access to environment variables:

```yaml
# config/environments/qa.yaml
environment: "{{ ENV_NAME | default('qa') }}"

kafka:
  bootstrap_servers: "{{ KAFKA_QA_BOOTSTRAP }}"
  security_protocol: "{{ KAFKA_QA_SECURITY_PROTOCOL | default('SASL_SSL') }}"
  
  sasl:
    mechanism: "{{ KAFKA_QA_SASL_MECHANISM | default('PLAIN') }}"
    username: "{{ KAFKA_QA_USERNAME }}"
    password: "{{ KAFKA_QA_PASSWORD }}"
  
  ssl:
    cafile: "{{ KAFKA_QA_SSL_CA_PATH }}"
    check_hostname: true

producer:
  batch_size: 16384
  acks: "all"
  compression_type: "gzip"
```

### ETL Configuration

```yaml
# config/etl/my_etl.yaml
name: "my_etl"
description: "My ETL process"
category: "data_sync"
enabled: true

# Topic per environment
topics:
  dev: "dev.my-etl.events"
  qa: "qa.my-etl.events"
  staging: "staging.my-etl.events"
  prod: "prod.my-etl.events"

# Payload template (Jinja2)
payload_template: "my_etl.json.j2"

# Headers (support Jinja2)
headers:
  X-ETL-Name: "{{ etl_name }}"
  X-Correlation-ID: "{{ correlation_id }}"
  X-Timestamp: "{{ timestamp }}"

# Execution settings
batch_size: 100
parallel: true
max_retries: 3
```

### Payload Templates

```json
// templates/payloads/my_etl.json.j2
{
  "eventType": "MY_ETL_EVENT",
  "eventId": "{{ correlation_id }}",
  "timestamp": "{{ timestamp }}",
  "timestampMs": {{ timestamp_ms }},
  "source": {
    "system": "{{ source_system | default('velox') }}",
    "environment": "{{ environment }}"
  },
  "payload": {
    "action": "{{ action | default('PROCESS') }}",
    "data": {{ data | default({}) | to_json }}
  }
}
```

### Runner Configuration

```yaml
# config/runner.yaml
execution:
  parallel_workers: 10
  batch_size: 100
  batch_timeout_ms: 5000
  fail_fast: false
  continue_on_error: true

filters:
  environments: ["dev", "qa"]
  categories: ["data_sync"]
  etls: []  # Empty = all
  exclude_environments: ["prod"]

limits:
  max_events_per_etl: 1000
  max_total_events: 10000
  timeout_seconds: 3600

dry_run: false
verbose: false
```

## Usage Examples

### Running Specific ETLs

```python
# Run a single ETL
stats = await runner.run_single_etl(
    etl_name="customer_update",
    environments=["dev", "qa"],
    event_config=EventConfig(count=50),
    context={"customer_id": "CUST-123"},
)

# Run with filters
stats = await runner.run(
    runner_config=RunnerConfig(
        filters=RunnerFilters(
            categories=["customer", "inventory"],
            exclude_environments=["prod"],
        ),
        limits=RunnerLimits(
            max_events_per_etl=100,
            max_total_events=500,
        ),
    ),
)
```

### Custom Template Context

```python
# Dynamic context for template rendering
context = {
    "customer_id": "CUST-456",
    "action": "UPDATE",
    "fields": {
        "email": "new@example.com",
        "name": "John Doe",
    },
    "requested_by": "api_user",
    "trace_id": "trace-abc-123",
}

stats = await runner.run_single_etl(
    etl_name="customer_update",
    environments=["dev"],
    context=context,
)
```

### Kafka Consumer Example

```python
from velox.connectors.kafka import (
    AsyncKafkaConsumer,
    KafkaConfig,
    ConsumerConfig,
    ConsumedMessage,
)

async def process_message(message: ConsumedMessage) -> None:
    print(f"Processing: {message.value.decode()}")
    # Your processing logic here

async def consume_events():
    kafka_config = KafkaConfig(
        bootstrap_servers="localhost:9092",
        security_protocol="PLAINTEXT",
    )
    consumer_config = ConsumerConfig(
        group_id="my-consumer-group",
        auto_offset_reset="earliest",
    )
    
    async with AsyncKafkaConsumer(kafka_config, consumer_config, "dev") as consumer:
        await consumer.subscribe(["my-topic"])
        
        # Process messages in parallel
        stats = await consumer.process_messages(
            handler=process_message,
            parallel=True,
            max_concurrency=10,
            max_records=1000,
        )
        
        print(f"Processed {stats.messages_processed} messages")

asyncio.run(consume_events())
```

### Dry Run Mode

```python
# Test without actually sending messages
stats = await runner.run(
    runner_config=RunnerConfig(
        dry_run=True,
        log_payloads=True,  # See rendered payloads
    ),
)
```

## API Reference

### Core Classes

#### ConfigManager

```python
ConfigManager(
    config_dir: Path = Path("config"),
    template_dir: Path = Path("templates"),
    env_file: Path | None = None,
    load_system_env: bool = True,
)

# Methods
config_manager.load_yaml(file_path, render_template=True)
config_manager.load_environment_config(environment)
config_manager.load_etl_config(etl_name)
config_manager.render_template(template_name, extra_context)
config_manager.update_context(updates)
config_manager.get_env(key, default=None)
config_manager.require_env(key)  # Raises if missing
```

#### ETLRegistry

```python
ETLRegistry(config_manager: ConfigManager)

# Methods
registry.load_all(reload=False) -> int
registry.load(etl_name) -> ETLConfig
registry.get(etl_name) -> ETLConfig | None
registry.filter(category=None, enabled_only=True, has_environment=None)
registry.all() -> list[ETLConfig]
registry.names() -> list[str]
registry.get_categories() -> set[str]
registry.get_environments() -> set[str]
```

#### ETLRunner

```python
ETLRunner(
    config_manager: ConfigManager,
    registry: ETLRegistry,
    template_engine: TemplateEngine | None = None,
)

# Methods
await runner.run(runner_config, event_config, context) -> AggregatedStats
await runner.run_single_etl(etl_name, environments, event_config, context)
runner.print_stats()
runner.print_detailed_stats()
runner.get_stats() -> AggregatedStats
runner.stop()
```

#### AsyncKafkaProducer

```python
AsyncKafkaProducer(
    kafka_config: KafkaConfig,
    environment: str,
    producer_config: ProducerConfig | None = None,
    auth_handler: AuthHandler | None = None,
    batch_size: int = 100,
    batch_timeout_ms: int = 5000,
)

# Methods
await producer.connect()
await producer.disconnect()
await producer.send(topic, value, key=None, headers=None) -> SendResult
await producer.send_batch(messages, parallel=True) -> BatchResult
await producer.queue_message(message) -> BatchResult | None
await producer.flush_queue() -> BatchResult | None
producer.get_stats() -> dict
```

### Configuration Models

#### RunnerConfig

```python
RunnerConfig(
    execution: ExecutionConfig,  # parallel_workers, batch_size, etc.
    filters: RunnerFilters,      # environments, categories, etls
    limits: RunnerLimits,        # max_events_per_etl, max_total_events
    context: dict,               # Additional template context
    dry_run: bool = False,
    verbose: bool = False,
)
```

#### EventConfig

```python
EventConfig(
    count: int = 1,              # Number of events to generate
    event_context: dict = {},    # Context per event
    delay_ms: int = 0,           # Delay between events
)
```

## Extending the Library

### Adding a New Connector

1. Create connector directory: `src/velox/connectors/myconnector/`

2. Implement the connector:

```python
# connectors/myconnector/client.py
from velox.connectors.base import AbstractConnector

class MyConnector(AbstractConnector):
    def __init__(self, config: MyConfig, environment: str):
        super().__init__("myconnector", environment)
        self._config = config
    
    async def _do_connect(self) -> None:
        # Connection logic
        pass
    
    async def _do_disconnect(self) -> None:
        # Disconnection logic
        pass
    
    async def _do_health_check(self) -> bool:
        # Health check logic
        return True
```

3. Add configuration model:

```python
# connectors/myconnector/config.py
from pydantic import BaseModel

class MyConfig(BaseModel):
    host: str
    port: int
    # ... other settings
```

### Adding Custom Template Filters

```python
from velox.core.templating import TemplateEngine

engine = TemplateEngine(template_dirs=[Path("templates")])

# Add custom filter
engine._env.filters["my_filter"] = lambda x: x.upper()

# Use in templates: {{ value | my_filter }}
```

## Performance Tuning

### Batch Size Optimization

```yaml
# For high throughput
producer:
  batch_size: 65536    # Larger batches
  linger_ms: 10        # Wait for more messages
  compression_type: lz4 # Fast compression

# For low latency
producer:
  batch_size: 1024     # Smaller batches
  linger_ms: 0         # No waiting
  compression_type: none
```

### Parallel Workers

```yaml
execution:
  parallel_workers: 20   # Increase for more parallelism
  connection_pool_size: 10
```

### Memory Optimization

```python
# Use streaming for large datasets
async for message in consumer.consume(max_records=100):
    await process(message)
    await consumer.commit(message)  # Commit per message
```

## Troubleshooting

### Common Issues

#### Connection Refused

```
KafkaError: Failed to connect to Kafka
```

- Check `bootstrap_servers` in environment config
- Verify network connectivity
- Check firewall rules

#### Authentication Failed

```
AuthenticationError: SASL authentication failed
```

- Verify username/password in `.env`
- Check SASL mechanism matches broker config
- Verify SSL certificates are valid

#### Template Not Found

```
TemplateError: Template not found: my_template.json.j2
```

- Check template exists in `templates/payloads/`
- Verify `payload_template` in ETL config
- Check template_dir in ConfigManager

### Debug Mode

```python
# Enable verbose logging
runner_config = RunnerConfig(
    verbose=True,
    log_payloads=True,
)

# Or via environment
import structlog
structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(logging.DEBUG),
)
```

### Dry Run Testing

Always test with dry_run before production:

```python
stats = await runner.run(
    runner_config=RunnerConfig(
        dry_run=True,
        filters=RunnerFilters(environments=["prod"]),
    ),
)
print(f"Would send {stats.total_events} events")
```

## License

MIT License - See LICENSE file for details.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests: `pytest`
5. Submit a pull request

## Future Roadmap

- [ ] MSSQL Connector
- [ ] PostgreSQL Connector
- [ ] Couchbase Connector
- [ ] REST API Connector
- [ ] CLI Tool
- [ ] Web Dashboard
- [ ] Prometheus Metrics Export

