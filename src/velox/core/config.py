"""
Configuration management with Jinja templating and .env integration.

Provides:
- Loading YAML configs with Jinja variable substitution
- Environment variable injection from .env
- Multi-environment configuration support
- Type-safe configuration models
"""

import os
from pathlib import Path
from typing import Any, TypeVar

import yaml
from dotenv import dotenv_values, load_dotenv
from pydantic import BaseModel

from velox.core.exceptions import ConfigurationError
from velox.core.templating import TemplateEngine

T = TypeVar("T", bound=BaseModel)


class ConfigManager:
    """
    Configuration manager with Jinja templating and environment variable support.
    
    Features:
    - Load YAML configs with Jinja variable substitution
    - Automatic .env file loading
    - Environment-specific configuration loading
    - Type-safe config parsing with Pydantic models
    - Runtime context modification
    
    Usage:
        manager = ConfigManager(
            config_dir=Path("config"),
            env_file=Path(".env"),
        )
        
        # Load environment config
        env_config = manager.load_environment_config("dev")
        
        # Load with Pydantic model
        kafka_config = manager.load_config("kafka.yaml", KafkaConfig)
    """

    def __init__(
        self,
        config_dir: Path | None = None,
        template_dir: Path | None = None,
        env_file: Path | None = None,
        load_system_env: bool = True,
    ) -> None:
        """
        Initialize the configuration manager.
        
        Args:
            config_dir: Directory containing configuration files.
            template_dir: Directory containing Jinja templates.
            env_file: Path to .env file.
            load_system_env: Whether to load system environment variables.
        """
        self._config_dir = config_dir or Path("config")
        self._template_dir = template_dir or Path("templates")
        self._env_file = env_file
        
        # Load environment variables
        self._env_vars: dict[str, str] = {}
        self._load_environment_variables(load_system_env)
        
        # Initialize template engine
        self._template_engine = TemplateEngine(
            template_dirs=[self._template_dir] if self._template_dir.exists() else None,
        )
        
        # Set environment variables as context
        self._template_engine.set_context(self._env_vars)
        
        # Cache for loaded configs
        self._config_cache: dict[str, Any] = {}

    def _load_environment_variables(self, load_system_env: bool) -> None:
        """Load environment variables from .env and system."""
        # Load from .env file if specified
        if self._env_file and self._env_file.exists():
            load_dotenv(self._env_file)
            self._env_vars.update(dotenv_values(self._env_file))
        
        # Load system environment variables
        if load_system_env:
            self._env_vars.update(os.environ)

    @property
    def config_dir(self) -> Path:
        """Get the configuration directory."""
        return self._config_dir

    @property
    def template_dir(self) -> Path:
        """Get the template directory."""
        return self._template_dir

    @property
    def env_vars(self) -> dict[str, str]:
        """Get loaded environment variables (copy)."""
        return self._env_vars.copy()

    @property
    def template_engine(self) -> TemplateEngine:
        """Get the template engine for direct access."""
        return self._template_engine

    def get_env(self, key: str, default: str | None = None) -> str | None:
        """
        Get an environment variable.
        
        Args:
            key: Environment variable name.
            default: Default value if not found.
            
        Returns:
            The environment variable value or default.
        """
        return self._env_vars.get(key, default)

    def require_env(self, key: str) -> str:
        """
        Get a required environment variable.
        
        Args:
            key: Environment variable name.
            
        Returns:
            The environment variable value.
            
        Raises:
            ConfigurationError: If variable not found.
        """
        value = self._env_vars.get(key)
        if value is None:
            raise ConfigurationError(
                f"Required environment variable not found: {key}",
                details={"variable": key},
            )
        return value

    def update_context(self, updates: dict[str, Any]) -> None:
        """
        Update the template context with new values.
        
        Args:
            updates: Dictionary of values to add/update.
        """
        self._template_engine.update_context(updates)

    def set_context(self, context: dict[str, Any]) -> None:
        """
        Replace the template context.
        
        Note: This will remove access to environment variables unless
        they are included in the new context.
        
        Args:
            context: New context dictionary.
        """
        self._template_engine.set_context(context)

    def reset_context(self) -> None:
        """Reset context to only environment variables."""
        self._template_engine.set_context(self._env_vars)

    def load_yaml(
        self,
        file_path: Path | str,
        render_template: bool = True,
        extra_context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Load a YAML file with optional Jinja rendering.
        
        Args:
            file_path: Path to the YAML file (relative to config_dir or absolute).
            render_template: Whether to render Jinja variables.
            extra_context: Additional context for rendering.
            
        Returns:
            Parsed YAML content as dictionary.
            
        Raises:
            ConfigurationError: If file not found or parsing fails.
        """
        path = self._resolve_path(file_path)
        
        try:
            content = path.read_text(encoding="utf-8")
            
            if render_template:
                content = self._template_engine.render_string(content, extra_context)
            
            data = yaml.safe_load(content)
            return data if data else {}
            
        except FileNotFoundError as e:
            raise ConfigurationError(
                f"Configuration file not found: {path}",
                config_path=str(path),
            ) from e
        except yaml.YAMLError as e:
            raise ConfigurationError(
                f"Failed to parse YAML: {e}",
                config_path=str(path),
            ) from e
        except Exception as e:
            raise ConfigurationError(
                f"Failed to load configuration: {e}",
                config_path=str(path),
            ) from e

    def load_config(
        self,
        file_path: Path | str,
        model: type[T],
        extra_context: dict[str, Any] | None = None,
    ) -> T:
        """
        Load a YAML config file and parse into a Pydantic model.
        
        Args:
            file_path: Path to the YAML file.
            model: Pydantic model class to parse into.
            extra_context: Additional context for rendering.
            
        Returns:
            Parsed and validated Pydantic model instance.
            
        Raises:
            ConfigurationError: If file not found, parsing, or validation fails.
        """
        data = self.load_yaml(file_path, render_template=True, extra_context=extra_context)
        
        try:
            return model.model_validate(data)
        except Exception as e:
            raise ConfigurationError(
                f"Failed to validate configuration: {e}",
                config_path=str(file_path),
                details={"model": model.__name__, "data": data},
            ) from e

    def load_environment_config(
        self,
        environment: str,
        extra_context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Load an environment-specific configuration.
        
        Args:
            environment: Environment name (dev, qa, staging, etc.).
            extra_context: Additional context for rendering.
            
        Returns:
            Environment configuration dictionary.
        """
        env_file = self._config_dir / "environments" / f"{environment}.yaml"
        
        # Add environment name to context
        context = {"ENV_NAME": environment, **(extra_context or {})}
        
        return self.load_yaml(env_file, render_template=True, extra_context=context)

    def load_etl_config(
        self,
        etl_name: str,
        extra_context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Load an ETL-specific configuration.
        
        Args:
            etl_name: Name of the ETL.
            extra_context: Additional context for rendering.
            
        Returns:
            ETL configuration dictionary.
        """
        etl_file = self._config_dir / "etl" / f"{etl_name}.yaml"
        
        # Add ETL name to context
        context = {"ETL_NAME": etl_name, **(extra_context or {})}
        
        return self.load_yaml(etl_file, render_template=True, extra_context=context)

    def list_environments(self) -> list[str]:
        """
        List all available environment configurations.
        
        Returns:
            List of environment names.
        """
        env_dir = self._config_dir / "environments"
        if not env_dir.exists():
            return []
        
        return [
            f.stem for f in env_dir.glob("*.yaml")
            if f.is_file() and not f.name.startswith("_")
        ]

    def list_etls(self) -> list[str]:
        """
        List all available ETL configurations.
        
        Returns:
            List of ETL names.
        """
        etl_dir = self._config_dir / "etl"
        if not etl_dir.exists():
            return []
        
        return [
            f.stem for f in etl_dir.glob("*.yaml")
            if f.is_file() and not f.name.startswith("_")
        ]

    def render_template(
        self,
        template_name: str,
        extra_context: dict[str, Any] | None = None,
    ) -> str:
        """
        Render a template file.
        
        Args:
            template_name: Name of the template file.
            extra_context: Additional context for rendering.
            
        Returns:
            Rendered template string.
        """
        return self._template_engine.render(template_name, extra_context)

    async def render_template_async(
        self,
        template_name: str,
        extra_context: dict[str, Any] | None = None,
    ) -> str:
        """
        Render a template file asynchronously.
        
        Args:
            template_name: Name of the template file.
            extra_context: Additional context for rendering.
            
        Returns:
            Rendered template string.
        """
        return await self._template_engine.render_async(template_name, extra_context)

    def clear_cache(self) -> None:
        """Clear all caches."""
        self._config_cache.clear()
        self._template_engine.clear_cache()

    def _resolve_path(self, file_path: Path | str) -> Path:
        """
        Resolve a file path.
        
        Args:
            file_path: Relative or absolute path.
            
        Returns:
            Resolved absolute path.
        """
        path = Path(file_path)
        
        if path.is_absolute():
            return path
        
        # Try relative to config directory
        config_path = self._config_dir / path
        if config_path.exists():
            return config_path
        
        # Return as-is
        return path

