# Velox

**A high-performance, async-first Python library for ETL connectors with multi-environment support, Jinja templating, and comprehensive metrics.**

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)
[![AsyncIO](https://img.shields.io/badge/Framework-AsyncIO-purple.svg)](https://docs.python.org/3/library/asyncio.html)
[![Type Hints](https://img.shields.io/badge/Typing-Typed-yellow.svg)](https://www.python.org/dev/peps/pep-0484/)

---

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
  - [High-Level Overview](#high-level-overview)
  - [Data Flow](#data-flow)
  - [Class Hierarchy](#class-hierarchy)
- [Project Structure](#project-structure)
- [Installation](#installation)
  - [From Git Repository](#from-git-repository)
  - [From Source](#from-source)
  - [Dependencies](#dependencies)
- [Quick Start](#quick-start)
- [Implementation Deep-Dive](#implementation-deep-dive)
  - [Core Module](#core-module)
  - [Kafka Connector Module](#kafka-connector-module)
  - [ETL Module](#etl-module)
- [Configuration System](#configuration-system)
  - [Configuration Resolution Flow](#configuration-resolution-flow)
  - [Environment Variables](#environment-variables)
  - [Environment Configuration YAML](#environment-configuration-yaml)
  - [ETL Configuration](#etl-configuration)
  - [Payload Templates](#payload-templates)
  - [Runner Configuration](#runner-configuration)
- [Authentication Implementation](#authentication-implementation)
  - [Supported Protocols](#supported-protocols)
  - [SASL_SSL Flow](#saslssl-flow)
  - [SSL/mTLS Certificate Handling](#sslmtls-certificate-handling)
  - [Auth Handler Factory Pattern](#auth-handler-factory-pattern)
- [Usage Examples](#usage-examples)
  - [Basic Usage](#basic-usage)
  - [Direct Kafka Producer](#direct-kafka-producer)
  - [Kafka Consumer](#kafka-consumer)
  - [Running Specific ETLs](#running-specific-etls)
  - [Custom Template Context](#custom-template-context)
  - [Dry Run Mode](#dry-run-mode)
- [API Reference](#api-reference)
  - [Core Classes](#core-classes)
  - [Configuration Models](#configuration-models)
  - [Exception Hierarchy](#exception-hierarchy)
- [Extending the Library](#extending-the-library)
  - [Adding a New Connector](#adding-a-new-connector)
  - [Adding Custom Template Filters](#adding-custom-template-filters)
  - [Custom Authentication Handlers](#custom-authentication-handlers)
- [Development Guide](#development-guide)
  - [Project Setup](#project-setup)
  - [Running Tests](#running-tests)
  - [Code Style and Linting](#code-style-and-linting)
  - [Type Checking](#type-checking)
  - [Pre-commit Hooks](#pre-commit-hooks)
  - [Contributing Guidelines](#contributing-guidelines)
- [Performance Tuning](#performance-tuning)
- [Troubleshooting](#troubleshooting)
- [Roadmap](#roadmap)
- [License](#license)

---

## Features

| Feature | Description |
|---------|-------------|
| **Async-First Design** | Built entirely on `asyncio` for maximum concurrency and throughput |
| **Multi-Environment Support** | Manage 6+ environments (dev, qa, staging, uat, preprod, prod) with per-environment configs |
| **Jinja2 Templating** | Dynamic payload and configuration rendering with runtime-modifiable context |
| **Multiple Auth Methods** | SASL_SSL, SSL (mutual TLS), SASL_PLAINTEXT, PLAINTEXT |
| **Batch Processing** | Configurable batch sizes with parallel execution and semaphore-based concurrency control |
| **Comprehensive Metrics** | Step-by-step timing, throughput, success rates, per-ETL and per-environment breakdowns |
| **Highly Configurable** | External YAML configuration for everything with Jinja variable substitution |
| **Type Safe** | Full Pydantic model validation with runtime type checking |
| **Modular Architecture** | Protocol-based abstractions for easy extension with new connectors |
| **High Performance** | Rust-based libraries (orjson, pydantic core) for critical paths |

---

## Architecture

### High-Level Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Velox Library                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐                     │
│  │   .env       │   │ environments │   │  etl/*.yaml  │                     │
│  │   secrets    │   │   /*.yaml    │   │  definitions │                     │
│  └──────┬───────┘   └──────┬───────┘   └──────┬───────┘                     │
│         │                  │                  │                              │
│         └────────────┬─────┴─────────────────┘                              │
│                      ▼                                                       │
│         ┌────────────────────────┐                                          │
│         │    ConfigManager       │  ◄── Loads .env, merges with YAML        │
│         │  (Jinja + YAML + .env) │      Renders templates at load time      │
│         └───────────┬────────────┘                                          │
│                     │                                                        │
│         ┌───────────┴────────────┐                                          │
│         ▼                        ▼                                          │
│  ┌─────────────────┐    ┌─────────────────┐                                 │
│  │  ETL Registry   │    │ Template Engine │  ◄── Runtime context updates    │
│  │  (ETL configs)  │    │   (payloads)    │      Custom filters & globals   │
│  └────────┬────────┘    └────────┬────────┘                                 │
│           │                      │                                          │
│           └──────────┬───────────┘                                          │
│                      ▼                                                       │
│         ┌────────────────────────┐                                          │
│         │      ETL Runner        │  ◄── Parallel execution orchestration    │
│         │ (orchestration layer)  │      Filters, limits, metrics tracking   │
│         └───────────┬────────────┘                                          │
│                     │                                                        │
│    ┌────────────────┼────────────────┐                                      │
│    ▼                ▼                ▼                                      │
│ ┌──────┐       ┌──────┐        ┌──────────┐                                 │
│ │Kafka │       │Kafka │        │ Metrics  │                                 │
│ │Producer      │Consumer       │Collector │                                 │
│ └──────┘       └──────┘        └──────────┘                                 │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Data Flow

```
┌──────────────┐
│ User Request │
│ (run ETLs)   │
└──────┬───────┘
       │
       ▼
┌─────────────────┐
│   ETL Runner    │
│                 │
│ 1. Load configs │ ◄── ConfigManager loads YAML + renders Jinja
│ 2. Filter ETLs  │ ◄── RunnerFilters.matches(etl, environment)
│ 3. Apply limits │ ◄── RunnerLimits controls volume
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  For each ETL   │
│  x Environment  │
│                 │
│ ┌─────────────┐ │
│ │  Semaphore  │ │ ◄── Controls parallel workers
│ │  (asyncio)  │ │
│ └──────┬──────┘ │
│        │        │
│        ▼        │
│ ┌─────────────┐ │
│ │  Render     │ │ ◄── TemplateEngine.render_async()
│ │  Payload    │ │     Merges context + event_context
│ └──────┬──────┘ │
│        │        │
│        ▼        │
│ ┌─────────────┐ │
│ │  Render     │ │ ◄── Headers support Jinja too
│ │  Headers    │ │
│ └──────┬──────┘ │
└────────┼────────┘
         │
         ▼
┌─────────────────┐
│ Message Batch   │
│                 │
│ topic, value,   │
│ key, headers,   │
│ correlation_id  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ AsyncKafkaProducer
│                 │
│ send_batch()    │ ◄── asyncio.gather() with semaphore
│  └─► aiokafka   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│MetricsCollector │
│                 │
│ - Duration      │ ◄── StepTiming per operation
│ - Success/Fail  │ ◄── Aggregated by env/ETL
│ - Throughput    │ ◄── events_per_second
└─────────────────┘
```

### Class Hierarchy

```
AbstractConnector (ABC)                    ◄── Connection lifecycle, state machine
├── implements: Connectable (Protocol)
├── implements: HealthCheckable (Protocol)
│
└── AsyncKafkaProducer                     ◄── Kafka-specific implementation
    ├── _do_connect() → aiokafka setup
    ├── _do_disconnect() → flush + stop
    └── _do_health_check() → metadata fetch

AuthHandler (ABC)                          ◄── Authentication abstraction
├── PlaintextAuth                          ◄── No auth (development)
├── SSLAuth                                ◄── Mutual TLS
├── SASLPlaintextAuth                      ◄── SASL without encryption
└── SASLSSLAuth                            ◄── SASL + SSL (production)

VeloxError (Exception)                     ◄── Base exception
├── ConfigurationError                     ◄── Config loading/validation
├── TemplateError                          ◄── Jinja rendering
├── ConnectorError                         ◄── Connection issues
│   ├── KafkaError                         ◄── Kafka-specific
│   └── AuthenticationError                ◄── Auth failures
├── ETLExecutionError                      ◄── Runtime execution
└── ValidationError                        ◄── Data validation
```

---

## Project Structure

```
velox/
├── pyproject.toml              # Dependencies, build config, tool settings
├── .gitignore                  # Git ignore patterns
├── .env.example                # Environment variables template
├── .env                        # Actual secrets (git-ignored)
├── README.md                   # This documentation
│
├── config/                     # All YAML configurations
│   ├── environments/           # Per-environment Kafka configs
│   │   ├── dev.yaml            #   Development (PLAINTEXT)
│   │   ├── qa.yaml             #   QA (SASL_SSL)
│   │   ├── staging.yaml        #   Staging
│   │   ├── uat.yaml            #   UAT
│   │   ├── preprod.yaml        #   Pre-production
│   │   └── prod.yaml           #   Production (SSL/mTLS)
│   │
│   ├── etl/                    # ETL process definitions
│   │   ├── sample_data_sync.yaml
│   │   └── customer_update.yaml
│   │
│   └── runner.yaml             # Default runner configuration
│
├── templates/                  # Jinja2 templates
│   └── payloads/               # JSON payload templates
│       ├── sample_data_sync.json.j2
│       └── customer_update.json.j2
│
├── src/                        # Source code
│   └── velox/                  # Main package
│       ├── __init__.py         # Package exports
│       ├── py.typed            # PEP 561 marker
│       │
│       ├── core/               # Core abstractions
│       │   ├── __init__.py
│       │   ├── base.py         # AbstractConnector, Protocols
│       │   ├── config.py       # ConfigManager
│       │   ├── templating.py   # TemplateEngine
│       │   ├── metrics.py      # MetricsCollector, ExecutionStats
│       │   └── exceptions.py   # Custom exception classes
│       │
│       ├── connectors/         # Technology connectors
│       │   ├── __init__.py
│       │   ├── base.py         # Re-exports for convenience
│       │   └── kafka/          # Kafka connector
│       │       ├── __init__.py
│       │       ├── config.py   # KafkaConfig, ProducerConfig, etc.
│       │       ├── auth.py     # Auth handlers (SASL, SSL)
│       │       ├── batch.py    # BatchProcessor, Message, BatchResult
│       │       ├── producer.py # AsyncKafkaProducer
│       │       └── consumer.py # AsyncKafkaConsumer
│       │
│       └── etl/                # ETL orchestration
│           ├── __init__.py
│           ├── config.py       # ETLConfig, RunnerConfig, etc.
│           ├── registry.py     # ETLRegistry
│           └── runner.py       # ETLRunner
│
└── tests/                      # Test suite
    ├── __init__.py
    ├── conftest.py             # Pytest fixtures
    ├── test_config.py
    ├── test_templating.py
    ├── test_kafka_producer.py
    └── test_runner.py
```

---

## Installation

### From Git Repository

Install directly from Git in your Python projects:

```bash
# Install latest from main branch
pip install git+https://github.com/your-org/velox.git

# Install specific version/tag
pip install git+https://github.com/your-org/velox.git@v0.1.0

# Install specific branch
pip install git+https://github.com/your-org/velox.git@develop

# Install with optional dependencies
pip install "velox[uvloop] @ git+https://github.com/your-org/velox.git"
```

In `requirements.txt`:

```txt
git+https://github.com/your-org/velox.git@main
```

In `pyproject.toml`:

```toml
[project]
dependencies = [
    "velox @ git+https://github.com/your-org/velox.git@main",
]
```

### From Source

```bash
# Clone the repository
git clone https://github.com/your-org/velox.git
cd velox

# Create virtual environment
python -m venv .venv

# Activate (Windows)
.venv\Scripts\activate

# Activate (Linux/macOS)
source .venv/bin/activate

# Install in development mode
pip install -e ".[dev]"

# Or install with all optional dependencies
pip install -e ".[all]"
```

### Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| `aiokafka` | ≥0.10.0 | Async Kafka client built on asyncio |
| `orjson` | ≥3.9.0 | Fast JSON serialization (Rust-based, ~10x faster) |
| `pydantic` | ≥2.5.0 | Config validation with Rust core |
| `pydantic-settings` | ≥2.1.0 | Environment-aware settings |
| `jinja2` | ≥3.1.0 | Template rendering |
| `python-dotenv` | ≥1.0.0 | .env file loading |
| `pyyaml` | ≥6.0.0 | YAML parsing |
| `structlog` | ≥24.1.0 | Structured logging |
| `rich` | ≥13.7.0 | CLI output and progress bars |
| `anyio` | ≥4.2.0 | Async utilities |
| `uvloop` | ≥0.19.0 | Fast event loop (Unix only, optional) |

---

## Quick Start

### 1. Copy Environment Template

```bash
copy .env.example .env
# Edit .env with your actual Kafka credentials and servers
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
    # Initialize configuration manager
    # Loads .env automatically and makes variables available in templates
    config_manager = ConfigManager(
        config_dir=Path("config"),
        template_dir=Path("templates"),
        env_file=Path(".env"),
    )
    
    # Load ETL registry
    # Scans config/etl/*.yaml and loads all ETL definitions
    registry = ETLRegistry(config_manager)
    count = registry.load_all()
    print(f"Loaded {count} ETLs")
    
    # Create runner
    runner = ETLRunner(config_manager, registry)
    
    # Run ETLs with filters and limits
    stats = await runner.run(
        runner_config=RunnerConfig(
            filters=RunnerFilters(
                environments=["dev", "qa"],    # Only these environments
                categories=["data_sync"],       # Only this category
            ),
        ),
        event_config=EventConfig(count=10),     # Generate 10 events per ETL
        context={
            "source_system": "my-app",
            "requested_by": "admin",
        },
    )
    
    # Print results
    runner.print_stats()
    print(f"Total: {stats.total_events} events, "
          f"{stats.total_successful} successful, "
          f"{stats.overall_events_per_second:.2f} events/sec")

if __name__ == "__main__":
    asyncio.run(main())
```

---

## Implementation Deep-Dive

### Core Module

The core module (`src/velox/core/`) provides foundational abstractions used across all connectors.

#### AbstractConnector (`base.py`)

The connector base class implements a **state machine pattern** for connection lifecycle management:

```python
class ConnectorState(Enum):
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    DISCONNECTING = "disconnecting"
    ERROR = "error"
```

**Key Design Decisions:**

1. **Template Method Pattern**: Subclasses implement `_do_connect()`, `_do_disconnect()`, `_do_health_check()` while the base class handles state transitions and error handling.

2. **Async Context Manager**: Built-in `__aenter__` / `__aexit__` for clean resource management:
   ```python
   async with AsyncKafkaProducer(config, "dev") as producer:
       await producer.send(...)
   # Automatically disconnected and cleaned up
   ```

3. **Metadata Tracking**: Each connector tracks creation time, connection time, and last activity.

#### ConfigManager (`config.py`)

Orchestrates configuration loading with Jinja templating:

```python
# Load order:
# 1. .env file (if specified)
# 2. System environment variables (if load_system_env=True)
# 3. YAML file content
# 4. Jinja rendering with merged context
```

**Key Methods:**

| Method | Purpose |
|--------|---------|
| `load_yaml(path, render_template=True)` | Load YAML with optional Jinja rendering |
| `load_config(path, model)` | Load and validate into Pydantic model |
| `load_environment_config(env)` | Load `config/environments/{env}.yaml` |
| `load_etl_config(etl_name)` | Load `config/etl/{etl_name}.yaml` |
| `update_context(dict)` | Add values to template context at runtime |
| `require_env(key)` | Get env var or raise ConfigurationError |

#### TemplateEngine (`templating.py`)

High-performance Jinja2 wrapper with:

**Built-in Filters:**

| Filter | Example | Result |
|--------|---------|--------|
| `to_json` | `{{ data \| to_json }}` | JSON string (orjson) |
| `from_json` | `{{ json_str \| from_json }}` | Parsed object |
| `uuid` | `{{ \| uuid }}` | New UUID4 |
| `hash_md5` | `{{ val \| hash_md5 }}` | MD5 hash |
| `hash_sha256` | `{{ val \| hash_sha256 }}` | SHA256 hash |
| `timestamp` | `{{ now() \| timestamp }}` | Unix timestamp |
| `iso_date` | `{{ now() \| iso_date }}` | ISO 8601 string |

**Built-in Globals:**

| Global | Usage | Result |
|--------|-------|--------|
| `now()` | `{{ now() }}` | Current datetime |
| `utcnow()` | `{{ utcnow() }}` | Current UTC datetime |
| `uuid4()` | `{{ uuid4() }}` | New UUID4 string |
| `timestamp_ms()` | `{{ timestamp_ms() }}` | Milliseconds since epoch |

#### MetricsCollector (`metrics.py`)

Tracks execution with nested context managers:

```python
collector = MetricsCollector()

async with collector.track_execution_async("my_etl", "dev") as stats:
    async with collector.track_step_async(stats, "connect"):
        await producer.connect()
    
    async with collector.track_step_async(stats, "send"):
        result = await producer.send_batch(messages)
        stats.successful = result.successful
        stats.failed = result.failed

# Get aggregated stats
agg = collector.aggregate()
print(f"Total: {agg.total_events}, Rate: {agg.success_rate}%")

# Rich console output
collector.print_summary()
collector.print_detailed()
```

### Kafka Connector Module

The Kafka connector (`src/velox/connectors/kafka/`) provides async producer and consumer implementations.

#### Configuration Models (`config.py`)

Pydantic models with validation and aiokafka conversion:

```python
class KafkaConfig(BaseModel):
    bootstrap_servers: str          # Required, validated non-empty
    security_protocol: SecurityProtocol
    ssl: SSLConfig | None
    sasl: SASLConfig | None
    client_id: str | None
    request_timeout_ms: int = 30000
    
    def to_aiokafka_config(self) -> dict[str, Any]:
        # Converts to aiokafka-compatible dict
```

#### Auth Handlers (`auth.py`)

Factory pattern for authentication:

```python
# Factory creates appropriate handler from config
handler = AuthHandler.from_kafka_config(kafka_config)
handler.validate()  # Raises AuthenticationError if invalid
config = handler.to_aiokafka_config()  # Get aiokafka params
```

Each handler builds SSL context and validates requirements:

| Handler | Requirements | Notes |
|---------|--------------|-------|
| `PlaintextAuth` | None | Development only, logs warning |
| `SSLAuth` | **CA cert (mandatory)**, optional client cert+key | SSL context always created |
| `SASLPlaintextAuth` | SASL mechanism, username, password | Logs warning (unencrypted) |
| `SASLSSLAuth` | **CA cert (mandatory)**, SASL mechanism, username, password, optional client cert+key | SSL context always created, recommended for production |

#### AsyncKafkaProducer (`producer.py`)

Extends `AbstractConnector` with Kafka-specific functionality:

```python
class AsyncKafkaProducer(AbstractConnector):
    async def send(self, topic, value, key=None, headers=None) -> SendResult:
        """Send single message, await delivery confirmation."""
    
    async def send_batch(self, messages, parallel=True, max_concurrency=10) -> BatchResult:
        """Send batch with semaphore-controlled parallelism."""
    
    async def queue_message(self, message) -> BatchResult | None:
        """Queue for batch processor, returns result if batch flushed."""
```

**Batch Processing:**

```python
# Parallel sending with concurrency control
semaphore = asyncio.Semaphore(max_concurrency)

async def send_with_semaphore(msg):
    async with semaphore:
        return await self.send_message(msg)

results = await asyncio.gather(*[send_with_semaphore(m) for m in messages])
```

### ETL Module

The ETL module (`src/velox/etl/`) orchestrates execution across environments.

#### ETLRegistry (`registry.py`)

Central repository for ETL configurations:

```python
registry = ETLRegistry(config_manager)
registry.load_all()  # Loads all config/etl/*.yaml

# Query
etl = registry.get("customer_update")
etls = list(registry.filter(category="data_sync", enabled_only=True))

# Programmatic registration
registry.register(ETLConfig(name="custom", ...))
```

#### ETLRunner (`runner.py`)

Orchestrates parallel execution:

**Execution Flow:**

1. **Filter ETLs** using `RunnerFilters`
2. **Apply limits** from `RunnerLimits`
3. **Initialize producers** per environment (pooled)
4. **Execute in parallel** with semaphore control
5. **Track metrics** per step
6. **Cleanup producers** on completion

**Parallel vs Sequential:**

```python
if runner_config.execution.parallel_workers > 1:
    # Create semaphore, asyncio.gather all ETL x environment combos
    await self._run_parallel(...)
else:
    # Simple nested for loops
    await self._run_sequential(...)
```

---

## Configuration System

### Configuration Resolution Flow

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   .env      │     │   System    │     │   YAML      │
│   file      │     │   environ   │     │   config    │
└──────┬──────┘     └──────┬──────┘     └──────┬──────┘
       │                   │                   │
       └─────────┬─────────┘                   │
                 ▼                             │
        ┌────────────────┐                     │
        │  Merged dict   │                     │
        │  (env_vars)    │                     │
        └────────┬───────┘                     │
                 │                             │
                 ▼                             ▼
        ┌────────────────────────────────────────┐
        │            Jinja2 Rendering            │
        │                                        │
        │  YAML content + env_vars as context    │
        │  {{ KAFKA_QA_BOOTSTRAP }} → value      │
        └────────────────┬───────────────────────┘
                         │
                         ▼
                ┌────────────────┐
                │  Parsed YAML   │
                │  (rendered)    │
                └────────┬───────┘
                         │
                         ▼
                ┌────────────────┐
                │  Pydantic      │
                │  Validation    │
                └────────────────┘
```

### Environment Variables

Create `.env` from `.env.example`:

```bash
# Environment selection
ACTIVE_ENVIRONMENT=dev

# ============ DEV ENVIRONMENT ============
KAFKA_DEV_BOOTSTRAP=localhost:9092
KAFKA_DEV_SECURITY_PROTOCOL=PLAINTEXT

# ============ QA ENVIRONMENT (SASL_SSL) ============
KAFKA_QA_BOOTSTRAP=kafka-qa.example.com:9093
KAFKA_QA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_QA_SASL_MECHANISM=PLAIN
KAFKA_QA_USERNAME=qa_service_account
KAFKA_QA_PASSWORD=secret_password_here
KAFKA_QA_SSL_CA_PATH=/path/to/qa-ca.pem

# ============ PROD ENVIRONMENT (SSL/mTLS) ============
KAFKA_PROD_BOOTSTRAP=kafka-prod.example.com:9093
KAFKA_PROD_SECURITY_PROTOCOL=SSL
KAFKA_PROD_SSL_CA_PATH=/path/to/prod-ca.pem
KAFKA_PROD_SSL_CERT_PATH=/path/to/client.pem
KAFKA_PROD_SSL_KEY_PATH=/path/to/client.key
KAFKA_PROD_SSL_KEY_PASSWORD=optional_key_password
```

### Environment Configuration YAML

Environment configs in `config/environments/*.yaml` support Jinja2:

```yaml
# config/environments/qa.yaml
environment: "{{ ENV_NAME | default('qa') }}"

kafka:
  bootstrap_servers: "{{ KAFKA_QA_BOOTSTRAP }}"
  security_protocol: "{{ KAFKA_QA_SECURITY_PROTOCOL | default('SASL_SSL') }}"
  client_id: "velox-{{ ENV_NAME | default('qa') }}"
  
  sasl:
    mechanism: "{{ KAFKA_QA_SASL_MECHANISM | default('PLAIN') }}"
    username: "{{ KAFKA_QA_USERNAME }}"
    password: "{{ KAFKA_QA_PASSWORD }}"
  
  ssl:
    cafile: "{{ KAFKA_QA_SSL_CA_PATH | default('') }}"
    check_hostname: true

producer:
  batch_size: 16384
  linger_ms: 5
  acks: "all"
  compression_type: "gzip"
  retries: 3
  retry_backoff_ms: 100
  enable_idempotence: false

consumer:
  group_id: "velox-qa-group"
  auto_offset_reset: "earliest"
  enable_auto_commit: true
```

### ETL Configuration

ETL definitions in `config/etl/*.yaml`:

```yaml
# config/etl/customer_update.yaml
name: "customer_update"
description: "Sends customer update events to downstream systems"
category: "customer"
enabled: true

# Topic per environment - allows different naming conventions
topics:
  dev: "dev.customer.updates.v1"
  qa: "qa.customer.updates.v1"
  staging: "staging.customer.updates.v1"
  uat: "uat.customer.updates.v1"
  preprod: "preprod.customer.updates.v1"
  prod: "prod.customer.updates.v1"

# Payload template file (in templates/payloads/)
payload_template: "customer_update.json.j2"

# Message key template (optional, for partition routing)
key_template: "{{ customer_id }}"

# Headers (all values support Jinja templating)
headers:
  X-ETL-Name: "{{ etl_name }}"
  X-Correlation-ID: "{{ correlation_id }}"
  X-Timestamp: "{{ timestamp }}"
  X-Environment: "{{ environment }}"
  Content-Type: "application/json"

# Execution settings
batch_size: 100
parallel: true
max_retries: 3
retry_backoff_ms: 100

# Optional: force specific partition (null = key-based partitioning)
partition: null

# Optional: override auth for this specific ETL
auth_override: null

# Extra config (passed to producer)
extra:
  compression: "lz4"
```

### Payload Templates

Templates in `templates/payloads/*.json.j2`:

```json
{
  "eventType": "CUSTOMER_UPDATE",
  "eventId": "{{ correlation_id }}",
  "eventVersion": "1.0",
  "timestamp": "{{ timestamp }}",
  "timestampMs": {{ timestamp_ms }},
  "source": {
    "system": "{{ source_system | default('velox') }}",
    "environment": "{{ environment }}",
    "etl": "{{ etl_name }}"
  },
  "metadata": {
    "correlationId": "{{ correlation_id }}",
    "requestedBy": "{{ requested_by | default('system') }}",
    "eventIndex": {{ event_index }},
    "totalEvents": {{ event_count }}
  },
  "payload": {
    "customerId": "{{ customer_id | default('CUST-' ~ uuid4()[:8]) }}",
    "action": "{{ action | default('UPDATE') }}",
    "fields": {{ fields | default({}) | to_json }},
    "processedAt": "{{ now() | iso_date }}"
  }
}
```

### Runner Configuration

Default runner settings in `config/runner.yaml`:

```yaml
# Execution behavior
execution:
  parallel_workers: 10          # Concurrent ETL x environment executions
  batch_size: 100               # Default messages per batch
  batch_timeout_ms: 5000        # Flush batch after this time
  max_retries: 3
  retry_backoff_ms: 100
  connection_pool_size: 5       # Producers per environment
  fail_fast: false              # Continue on individual failures
  continue_on_error: true       # Don't stop on ETL errors

# Filtering criteria (empty lists = all)
filters:
  environments: []              # All environments
  categories: []                # All categories
  etls: []                      # All ETLs
  exclude_environments: []
  exclude_categories: []
  exclude_etls: []

# Volume limits
limits:
  max_events_per_etl: null      # No limit
  max_events_per_environment: null
  max_total_events: null
  max_etls: null
  max_environments: null
  timeout_seconds: null
  timeout_per_etl_seconds: null

# Debugging
dry_run: false
verbose: false
log_payloads: false
```

---

## Authentication Implementation

### Supported Protocols

| Protocol | Security | SSL Context | Use Case |
|----------|----------|-------------|----------|
| `PLAINTEXT` | None | Not required | Local development only |
| `SSL` | TLS encryption + mTLS auth | **Mandatory** | Production with certificate auth |
| `SASL_PLAINTEXT` | SASL auth, no encryption | Not required | **Not recommended** |
| `SASL_SSL` | SASL auth + TLS encryption | **Mandatory** | Production with password auth |

> **Note**: SSL context (via `ssl_cafile`) is mandatory for both `SSL` and `SASL_SSL` protocols. This ensures proper server certificate verification and secure connections.

### SASL_SSL Flow

```
┌────────────────┐     ┌────────────────┐     ┌────────────────┐
│     Client     │     │     Kafka      │     │   Auth Server  │
│   (Velox)      │     │    Broker      │     │   (optional)   │
└───────┬────────┘     └───────┬────────┘     └───────┬────────┘
        │                      │                      │
        │  1. TCP Connect      │                      │
        │─────────────────────►│                      │
        │                      │                      │
        │  2. TLS Handshake    │                      │
        │◄────────────────────►│                      │
        │  (verify CA cert)    │                      │
        │                      │                      │
        │  3. SASL Auth        │                      │
        │─────────────────────►│                      │
        │  mechanism: PLAIN    │                      │
        │  username + password │                      │
        │                      │                      │
        │                      │  4. Validate         │
        │                      │  (if external)       │
        │                      │─────────────────────►│
        │                      │◄─────────────────────│
        │                      │                      │
        │  5. Auth Success     │                      │
        │◄─────────────────────│                      │
        │                      │                      │
        │  6. Kafka API Calls  │                      │
        │◄────────────────────►│                      │
        │  (encrypted)         │                      │
```

### SSL/mTLS Certificate Handling

Both `SSLAuth` and `SASLSSLAuth` handlers **always** build an `ssl.SSLContext`. The CA certificate (`ssl_cafile`) is mandatory:

```python
def _build_ssl_context(self) -> ssl.SSLContext:
    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    
    # 1. Load CA certificate (MANDATORY - verifies server)
    # Raises AuthenticationError if ssl_cafile is not provided
    context.load_verify_locations(cafile=self._config.ssl_cafile)
    
    # 2. Load client certificate + key (optional, for mTLS)
    if self._config.ssl_certfile and self._config.ssl_keyfile:
        context.load_cert_chain(
            certfile=self._config.ssl_certfile,
            keyfile=self._config.ssl_keyfile,
            password=self._config.ssl_password,  # Optional key password
        )
    
    # 3. Configure hostname verification
    context.check_hostname = self._config.ssl_check_hostname
    
    return context
```

**Validation ensures:**
- `ssl_cafile` is always required for `SSL` and `SASL_SSL`
- SSL context is always created (never returns `None`)
- If `ssl_certfile` is provided, `ssl_keyfile` must also be provided (and vice versa)

### Auth Handler Factory Pattern

```python
# Global factory instance
auth_factory = AuthHandlerFactory()

# Usage
auth_config = AuthConfig(
    security_protocol=SecurityProtocol.SASL_SSL,
    sasl_mechanism=SASLMechanism.PLAIN,
    sasl_username="my_user",
    sasl_password="my_pass",
    ssl_cafile="/path/to/ca.pem",
)

handler = auth_factory.create(auth_config)  # Returns SASLSSLAuth
# Validation happens in create()

# Or from KafkaConfig directly
handler = AuthHandler.from_kafka_config(kafka_config)
```

**Extending with custom handlers:**

```python
class MyCustomAuth(AuthHandler):
    def validate(self) -> None:
        # Custom validation logic
        pass
    
    def to_aiokafka_config(self) -> dict[str, Any]:
        return {"custom_param": "value"}

# Register
auth_factory.register(SecurityProtocol.SASL_SSL, MyCustomAuth)
```

---

## Usage Examples

### Basic Usage

```python
import asyncio
from pathlib import Path
from velox.core.config import ConfigManager
from velox.etl.registry import ETLRegistry
from velox.etl.runner import ETLRunner
from velox.etl.config import RunnerConfig, RunnerFilters, RunnerLimits, EventConfig

async def main():
    config_manager = ConfigManager(
        config_dir=Path("config"),
        template_dir=Path("templates"),
        env_file=Path(".env"),
    )
    
    registry = ETLRegistry(config_manager)
    registry.load_all()
    
    runner = ETLRunner(config_manager, registry)
    
    stats = await runner.run(
        runner_config=RunnerConfig(
            filters=RunnerFilters(
                environments=["dev", "qa"],
                categories=["customer"],
            ),
            limits=RunnerLimits(
                max_events_per_etl=50,
                max_total_events=200,
            ),
        ),
        event_config=EventConfig(count=10),
        context={"source_system": "order-service"},
    )
    
    runner.print_stats()

asyncio.run(main())
```

### Direct Kafka Producer

```python
import asyncio
from velox.connectors.kafka import AsyncKafkaProducer, KafkaConfig, Message

async def send_messages():
    config = KafkaConfig(
        bootstrap_servers="localhost:9092",
        security_protocol="PLAINTEXT",
    )
    
    async with AsyncKafkaProducer(config, "dev") as producer:
        # Single message with await
        result = await producer.send(
            topic="my-topic",
            value=b'{"event": "test"}',
            key=b"user-123",
            headers=[("Content-Type", b"application/json")],
        )
        print(f"Sent to partition {result.partition}, offset {result.offset}")
        
        # Batch of 100 messages
        messages = [
            Message(
                topic="my-topic",
                value=f'{{"id": {i}, "data": "test"}}'.encode(),
                key=f"key-{i}".encode(),
            )
            for i in range(100)
        ]
        
        batch_result = await producer.send_batch(
            messages,
            parallel=True,
            max_concurrency=10,
        )
        
        print(f"Batch: {batch_result.successful}/{batch_result.total} successful")
        print(f"Throughput: {batch_result.messages_per_second:.2f} msg/sec")
        print(f"Duration: {batch_result.duration_ms:.2f}ms")

asyncio.run(send_messages())
```

### Kafka Consumer

```python
import asyncio
from velox.connectors.kafka import (
    AsyncKafkaConsumer,
    KafkaConfig,
    ConsumerConfig,
    ConsumedMessage,
)

async def process_message(message: ConsumedMessage) -> None:
    data = message.value.decode("utf-8")
    print(f"Received from {message.topic}[{message.partition}]: {data}")

async def consume_events():
    kafka_config = KafkaConfig(
        bootstrap_servers="localhost:9092",
        security_protocol="PLAINTEXT",
    )
    consumer_config = ConsumerConfig(
        group_id="my-consumer-group",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )
    
    async with AsyncKafkaConsumer(kafka_config, consumer_config, "dev") as consumer:
        await consumer.subscribe(["my-topic", "other-topic"])
        
        # Process with parallelism
        stats = await consumer.process_messages(
            handler=process_message,
            parallel=True,
            max_concurrency=10,
            max_records=1000,
            timeout_seconds=60,
        )
        
        print(f"Processed: {stats.messages_processed}")
        print(f"Failed: {stats.messages_failed}")

asyncio.run(consume_events())
```

### Running Specific ETLs

```python
# Run single ETL across environments
stats = await runner.run_single_etl(
    etl_name="customer_update",
    environments=["dev", "qa"],  # None = all configured
    event_config=EventConfig(count=50),
    context={"customer_id": "CUST-12345"},
    dry_run=False,
)

# Run with exclusions
stats = await runner.run(
    runner_config=RunnerConfig(
        filters=RunnerFilters(
            categories=["customer", "inventory"],
            exclude_environments=["prod"],  # Never touch prod
            exclude_etls=["legacy_sync"],   # Skip this one
        ),
        limits=RunnerLimits(
            max_events_per_etl=100,
            max_total_events=1000,
            timeout_seconds=300,
        ),
    ),
)
```

### Custom Template Context

```python
# Context flows through to all templates
context = {
    # Business context
    "customer_id": "CUST-456",
    "order_id": "ORD-789",
    "action": "REFUND",
    
    # Complex data structures
    "line_items": [
        {"sku": "PROD-1", "qty": 2, "price": 19.99},
        {"sku": "PROD-2", "qty": 1, "price": 49.99},
    ],
    
    # Metadata
    "requested_by": "refund-service",
    "trace_id": "trace-abc-123",
    "priority": "high",
}

stats = await runner.run_single_etl(
    etl_name="order_event",
    environments=["qa"],
    event_config=EventConfig(count=1),
    context=context,
)
```

### Dry Run Mode

```python
# Test without sending - renders payloads and shows what would happen
stats = await runner.run(
    runner_config=RunnerConfig(
        dry_run=True,           # Don't actually send
        log_payloads=True,      # Print rendered payloads
        verbose=True,           # Extra logging
        filters=RunnerFilters(
            environments=["prod"],  # Safe to test prod config
        ),
    ),
)

print(f"Would send {stats.total_events} events to production")
```

---

## API Reference

### Core Classes

#### ConfigManager

```python
class ConfigManager:
    def __init__(
        self,
        config_dir: Path | None = None,        # Default: Path("config")
        template_dir: Path | None = None,       # Default: Path("templates")
        env_file: Path | None = None,           # .env file path
        load_system_env: bool = True,           # Include os.environ
    ) -> None
    
    # Properties
    config_dir: Path                            # Config directory path
    template_dir: Path                          # Template directory path
    env_vars: dict[str, str]                    # Loaded environment variables (copy)
    template_engine: TemplateEngine             # Direct access to template engine
    
    # Environment variable access
    def get_env(key: str, default: str | None = None) -> str | None
    def require_env(key: str) -> str            # Raises ConfigurationError if missing
    
    # Context management
    def update_context(updates: dict[str, Any]) -> None
    def set_context(context: dict[str, Any]) -> None
    def reset_context() -> None
    
    # YAML loading
    def load_yaml(
        file_path: Path | str,
        render_template: bool = True,
        extra_context: dict[str, Any] | None = None,
    ) -> dict[str, Any]
    
    def load_config(
        file_path: Path | str,
        model: type[T],                         # Pydantic model class
        extra_context: dict[str, Any] | None = None,
    ) -> T                                      # Returns validated model
    
    # Convenience loaders
    def load_environment_config(environment: str) -> dict[str, Any]
    def load_etl_config(etl_name: str) -> dict[str, Any]
    
    # Discovery
    def list_environments() -> list[str]
    def list_etls() -> list[str]
    
    # Template rendering
    def render_template(template_name: str, extra_context: dict | None) -> str
    async def render_template_async(template_name: str, extra_context: dict | None) -> str
    
    # Cache
    def clear_cache() -> None
```

#### ETLRegistry

```python
class ETLRegistry:
    def __init__(self, config_manager: ConfigManager) -> None
    
    # Properties
    config_manager: ConfigManager
    is_loaded: bool
    
    # Loading
    def load_all(reload: bool = False) -> int   # Returns count
    def load(etl_name: str) -> ETLConfig
    
    # CRUD
    def register(etl: ETLConfig) -> None        # Programmatic registration
    def unregister(etl_name: str) -> bool
    def get(etl_name: str) -> ETLConfig | None
    def get_or_raise(etl_name: str) -> ETLConfig  # Raises ConfigurationError
    
    # Querying
    def all() -> list[ETLConfig]
    def names() -> list[str]
    def count() -> int
    def filter(
        category: str | None = None,
        enabled_only: bool = True,
        has_environment: str | None = None,
        filters: RunnerFilters | None = None,
    ) -> Iterator[ETLConfig]
    def filter_for_environment(environment: str, filters: RunnerFilters | None) -> list[ETLConfig]
    
    # Introspection
    def get_categories() -> set[str]
    def get_environments() -> set[str]
    def validate_all() -> list[str]             # Returns error messages
    def to_dict() -> dict[str, dict[str, Any]]
    
    # Protocols
    def __len__() -> int
    def __contains__(etl_name: str) -> bool
    def __iter__() -> Iterator[ETLConfig]
```

#### ETLRunner

```python
class ETLRunner:
    def __init__(
        self,
        config_manager: ConfigManager,
        registry: ETLRegistry,
        template_engine: TemplateEngine | None = None,
    ) -> None
    
    # Properties
    metrics: MetricsCollector
    registry: ETLRegistry
    
    # Execution
    async def run(
        runner_config: RunnerConfig | None = None,
        event_config: EventConfig | None = None,
        context: dict[str, Any] | None = None,
    ) -> AggregatedStats
    
    async def run_single_etl(
        etl_name: str,
        environments: list[str] | None = None,
        event_config: EventConfig | None = None,
        context: dict[str, Any] | None = None,
        dry_run: bool = False,
    ) -> list[ExecutionStats]
    
    # Control
    def stop() -> None                          # Signal to stop running
    
    # Statistics
    def print_stats() -> None                   # Rich summary table
    def print_detailed_stats() -> None          # Per-execution breakdown
    def get_stats() -> AggregatedStats
```

#### AsyncKafkaProducer

```python
class AsyncKafkaProducer(AbstractConnector):
    def __init__(
        self,
        kafka_config: KafkaConfig,
        environment: str,
        producer_config: ProducerConfig | None = None,
        auth_handler: AuthHandler | None = None,
        batch_size: int = 100,
        batch_timeout_ms: int = 5000,
    ) -> None
    
    # Properties
    messages_sent: int
    messages_failed: int
    bytes_sent: int
    
    # Connection (inherited from AbstractConnector)
    async def connect() -> None
    async def disconnect() -> None
    async def health_check() -> bool
    async def reconnect() -> None
    
    # Sending
    async def send(
        topic: str,
        value: bytes,
        key: bytes | None = None,
        headers: list[tuple[str, bytes]] | None = None,
        partition: int | None = None,
        timestamp_ms: int | None = None,
    ) -> SendResult
    
    async def send_message(message: Message) -> SendResult
    
    async def send_batch(
        messages: list[Message],
        parallel: bool = True,
        max_concurrency: int = 10,
    ) -> BatchResult
    
    async def send_batch_by_topic(
        messages: list[Message],
        parallel: bool = True,
    ) -> dict[str, BatchResult]
    
    # Queuing
    async def queue_message(message: Message) -> BatchResult | None
    async def flush_queue() -> BatchResult | None
    
    # Stats
    def get_stats() -> dict[str, Any]
    
    # Context manager
    async def __aenter__() -> Self
    async def __aexit__(...) -> None
```

### Configuration Models

#### RunnerConfig

```python
class RunnerConfig(BaseModel):
    execution: ExecutionConfig       # Parallel workers, batch settings
    filters: RunnerFilters           # What to run
    limits: RunnerLimits             # How much to run
    context: dict[str, Any] = {}     # Template context
    dry_run: bool = False
    verbose: bool = False
    log_payloads: bool = False
```

#### ExecutionConfig

```python
class ExecutionConfig(BaseModel):
    parallel_workers: int = 10
    batch_size: int = 100
    batch_timeout_ms: int = 5000
    max_retries: int = 3
    retry_backoff_ms: int = 100
    connection_pool_size: int = 5
    fail_fast: bool = False
    continue_on_error: bool = True
```

#### RunnerFilters

```python
class RunnerFilters(BaseModel):
    environments: list[str] = []     # Empty = all
    categories: list[str] = []
    etls: list[str] = []
    exclude_environments: list[str] = []
    exclude_categories: list[str] = []
    exclude_etls: list[str] = []
    
    def matches(etl: ETLConfig, environment: str) -> bool
    def matches_environment(environment: str) -> bool
    def matches_category(category: str) -> bool
    def matches_etl(etl_name: str) -> bool
```

#### RunnerLimits

```python
class RunnerLimits(BaseModel):
    max_events_per_etl: int | None = None
    max_events_per_environment: int | None = None
    max_total_events: int | None = None
    max_etls: int | None = None
    max_environments: int | None = None
    timeout_seconds: int | None = None
    timeout_per_etl_seconds: int | None = None
```

#### EventConfig

```python
class EventConfig(BaseModel):
    count: int = 1                   # Events to generate
    event_context: dict[str, Any] = {}  # Per-event context
    iterate_over: str | None = None  # Context key to iterate
    delay_ms: int = 0                # Delay between events
```

### Exception Hierarchy

```python
class VeloxError(Exception):
    """Base for all Velox exceptions."""
    message: str
    details: dict[str, Any]

class ConfigurationError(VeloxError):
    """Config loading/validation failed."""
    config_path: str | None

class TemplateError(VeloxError):
    """Jinja rendering failed."""
    template_name: str | None
    context: dict[str, Any] | None

class ConnectorError(VeloxError):
    """Connector operation failed."""
    connector_type: str | None
    operation: str | None

class KafkaError(ConnectorError):
    """Kafka-specific error."""
    topic: str | None
    partition: int | None

class AuthenticationError(ConnectorError):
    """Authentication failed."""
    auth_type: str | None

class ETLExecutionError(VeloxError):
    """ETL execution failed."""
    etl_name: str | None
    environment: str | None

class ValidationError(VeloxError):
    """Data validation failed."""
    field: str | None
    value: Any
```

---

## Extending the Library

### Adding a New Connector

1. **Create connector directory:**

```
src/velox/connectors/myconnector/
├── __init__.py
├── config.py      # Pydantic models
├── client.py      # Main connector class
└── auth.py        # If auth is needed
```

2. **Define configuration models:**

```python
# connectors/myconnector/config.py
from pydantic import BaseModel, Field

class MyConnectorConfig(BaseModel):
    host: str = Field(description="Server host")
    port: int = Field(default=5432, description="Server port")
    database: str = Field(description="Database name")
    pool_size: int = Field(default=5, description="Connection pool size")
    
    def to_connection_string(self) -> str:
        return f"{self.host}:{self.port}/{self.database}"
```

3. **Implement the connector:**

```python
# connectors/myconnector/client.py
from velox.connectors.base import AbstractConnector
from .config import MyConnectorConfig

class MyConnector(AbstractConnector):
    def __init__(
        self,
        config: MyConnectorConfig,
        environment: str,
    ) -> None:
        super().__init__("myconnector", environment)
        self._config = config
        self._client = None
    
    async def _do_connect(self) -> None:
        """Implement actual connection logic."""
        # self._client = await some_library.connect(self._config.to_connection_string())
        pass
    
    async def _do_disconnect(self) -> None:
        """Implement disconnection logic."""
        if self._client:
            await self._client.close()
            self._client = None
    
    async def _do_health_check(self) -> bool:
        """Implement health check."""
        if not self._client:
            return False
        try:
            await self._client.ping()
            return True
        except Exception:
            return False
    
    # Add connector-specific methods
    async def execute(self, query: str) -> list[dict]:
        """Execute a query."""
        return await self._client.query(query)
```

4. **Export from package:**

```python
# connectors/myconnector/__init__.py
from .client import MyConnector
from .config import MyConnectorConfig

__all__ = ["MyConnector", "MyConnectorConfig"]
```

### Adding Custom Template Filters

```python
from velox.core.templating import TemplateEngine
from pathlib import Path

# Create engine
engine = TemplateEngine(template_dirs=[Path("templates")])

# Add custom filter
def mask_pii(value: str, visible_chars: int = 4) -> str:
    """Mask PII data, showing only last N characters."""
    if len(value) <= visible_chars:
        return "*" * len(value)
    return "*" * (len(value) - visible_chars) + value[-visible_chars:]

engine._env.filters["mask_pii"] = mask_pii

# Add custom global
engine._env.globals["app_version"] = "1.2.3"

# Use in templates:
# {{ customer_ssn | mask_pii(4) }}  →  *****1234
# {{ app_version }}  →  1.2.3
```

### Custom Authentication Handlers

```python
from velox.connectors.kafka.auth import AuthHandler, AuthConfig, auth_factory
from velox.connectors.kafka.config import SecurityProtocol
from typing import Any

class OAuth2Auth(AuthHandler):
    """OAuth2 bearer token authentication."""
    
    def __init__(self, config: AuthConfig) -> None:
        super().__init__(config)
        self._token = None
    
    def validate(self) -> None:
        if not self._config.oauth_token:
            raise AuthenticationError(
                "OAuth2 requires oauth_token",
                auth_type="oauth2",
            )
        self._token = self._config.oauth_token
    
    def to_aiokafka_config(self) -> dict[str, Any]:
        return {
            "security_protocol": "SASL_SSL",
            "sasl_mechanism": "OAUTHBEARER",
            "sasl_oauth_token_provider": self._get_token_provider(),
        }
    
    def _get_token_provider(self):
        # Return token provider callback
        token = self._token
        async def provider():
            return token, 3600  # token, expiry_seconds
        return provider

# Register globally
auth_factory.register(SecurityProtocol.SASL_SSL, OAuth2Auth)
```

---

## Development Guide

### Project Setup

```bash
# Clone repository
git clone https://github.com/your-org/velox.git
cd velox

# Create virtual environment
python -m venv .venv

# Activate (Windows PowerShell)
.venv\Scripts\Activate.ps1

# Activate (Windows cmd)
.venv\Scripts\activate.bat

# Activate (Linux/macOS)
source .venv/bin/activate

# Install with dev dependencies
pip install -e ".[dev]"

# Copy environment template
copy .env.example .env
# Edit .env with your test Kafka settings
```

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=velox --cov-report=html

# Run specific test file
pytest tests/test_kafka_producer.py

# Run specific test
pytest tests/test_config.py::test_load_yaml_with_jinja

# Verbose output
pytest -v --tb=long

# Run async tests only
pytest -k async
```

### Code Style and Linting

```bash
# Format with Black
black src tests

# Sort imports with Ruff
ruff check --fix src tests

# Lint with Ruff
ruff check src tests

# All linting (CI command)
black --check src tests && ruff check src tests
```

### Type Checking

```bash
# Run mypy
mypy src

# Strict mode (as configured)
mypy src --strict

# Check specific file
mypy src/velox/core/config.py
```

### Pre-commit Hooks

```bash
# Install hooks
pre-commit install

# Run manually
pre-commit run --all-files

# Update hooks
pre-commit autoupdate
```

### Contributing Guidelines

1. **Fork and Clone**: Fork the repo, clone locally
2. **Branch**: Create feature branch from `main`
3. **Develop**: Make changes, add tests
4. **Test**: Run `pytest` and ensure 100% pass
5. **Lint**: Run `black`, `ruff`, `mypy`
6. **Commit**: Use conventional commits
7. **PR**: Submit pull request with description

**Commit Message Format:**

```
type(scope): description

[optional body]

[optional footer]
```

Types: `feat`, `fix`, `docs`, `style`, `refactor`, `test`, `chore`

---

## Performance Tuning

### Batch Size Optimization

```yaml
# High throughput (latency-tolerant)
producer:
  batch_size: 65536        # 64KB batches
  linger_ms: 50            # Wait up to 50ms for more messages
  compression_type: lz4    # Fast compression
  acks: "1"                # Leader ack only

# Low latency (throughput-tolerant)
producer:
  batch_size: 1024         # 1KB batches
  linger_ms: 0             # No waiting
  compression_type: none   # No compression overhead
  acks: "all"              # Full replication
```

### Parallel Workers

```yaml
execution:
  parallel_workers: 20       # More parallelism
  connection_pool_size: 10   # More connections per env
  batch_size: 200            # Larger batches per send
```

### Memory Optimization

```python
# For large datasets, use streaming
async for batch in batched(messages, size=100):
    result = await producer.send_batch(batch)
    # Process result before next batch
    
    if result.failed > 0:
        # Handle failures
        pass
```

### uvloop (Unix only)

```bash
pip install "velox[uvloop]"
```

```python
import uvloop
uvloop.install()  # Before any async code

asyncio.run(main())
```

---

## Troubleshooting

### Connection Refused

```
KafkaError: Failed to connect to Kafka
```

- Verify `bootstrap_servers` in environment YAML
- Check network connectivity: `telnet host port`
- Verify Kafka is running: `kafka-broker-api-versions.sh --bootstrap-server host:port`

### Authentication Failed

```
AuthenticationError: SASL authentication failed
```

- Verify username/password in `.env`
- Check SASL mechanism matches broker config
- For SCRAM: verify user exists in Kafka

### SSL Certificate Errors

```
ssl.SSLCertVerificationError: certificate verify failed
```

- Verify CA certificate path is correct
- Check certificate is not expired: `openssl x509 -in ca.pem -text -noout`
- For self-signed: ensure `ssl_check_hostname: false`

### Template Not Found

```
TemplateError: Template not found: my_template.json.j2
```

- Check file exists in `templates/payloads/`
- Verify `payload_template` in ETL config matches filename
- Check `template_dir` in ConfigManager

### Debug Mode

```python
import structlog
import logging

# Enable debug logging
structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(logging.DEBUG),
)

# Or per-module
logging.getLogger("velox.connectors.kafka").setLevel(logging.DEBUG)
```

---

## Roadmap

- [ ] **MSSQL Connector** - SQL Server with async support
- [ ] **PostgreSQL Connector** - asyncpg-based connector
- [ ] **Couchbase Connector** - N1QL and KV operations
- [ ] **REST API Connector** - Generic HTTP client with retry
- [ ] **CLI Tool** - `velox-run` command with full options
- [ ] **Web Dashboard** - Real-time execution monitoring
- [ ] **Prometheus Metrics** - Export metrics for monitoring
- [ ] **Schema Registry** - Avro/Protobuf schema support
- [ ] **Dead Letter Queue** - Failed message handling
- [ ] **Rate Limiting** - Per-environment rate limits

---

## License

MIT License - See LICENSE file for details.

---

## Support

- **Issues**: [GitHub Issues](https://github.com/your-org/velox/issues)
- **Discussions**: [GitHub Discussions](https://github.com/your-org/velox/discussions)
- **Documentation**: This README and inline docstrings

---

*Built with ❤️ for high-performance ETL operations*
