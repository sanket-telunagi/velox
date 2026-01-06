"""
ETL registry for managing ETL configurations.

Provides:
- Loading ETL configs from files
- Querying and filtering ETLs
- Validation and caching
"""

from pathlib import Path
from typing import Any, Iterator

import structlog

from velox.core.config import ConfigManager
from velox.core.exceptions import ConfigurationError
from velox.etl.config import ETLConfig, RunnerFilters

logger = structlog.get_logger()


class ETLRegistry:
    """
    Registry for managing ETL configurations.
    
    Provides a central place to load, store, and query ETL definitions.
    
    Usage:
        registry = ETLRegistry(config_manager)
        registry.load_all()
        
        # Get specific ETL
        etl = registry.get("my_etl")
        
        # Query ETLs
        for etl in registry.filter(category="data_sync"):
            print(etl.name)
    """

    def __init__(self, config_manager: ConfigManager) -> None:
        """
        Initialize the registry.
        
        Args:
            config_manager: Configuration manager instance.
        """
        self._config_manager = config_manager
        self._etls: dict[str, ETLConfig] = {}
        self._loaded = False

    @property
    def config_manager(self) -> ConfigManager:
        """Get the configuration manager."""
        return self._config_manager

    @property
    def is_loaded(self) -> bool:
        """Check if registry has been loaded."""
        return self._loaded

    def load_all(self, reload: bool = False) -> int:
        """
        Load all ETL configurations from the config directory.
        
        Args:
            reload: Force reload even if already loaded.
            
        Returns:
            Number of ETLs loaded.
        """
        if self._loaded and not reload:
            return len(self._etls)
        
        self._etls.clear()
        etl_names = self._config_manager.list_etls()
        
        for etl_name in etl_names:
            try:
                self.load(etl_name)
            except Exception as e:
                logger.error(
                    "etl_load_failed",
                    etl_name=etl_name,
                    error=str(e),
                )
        
        self._loaded = True
        
        logger.info(
            "etl_registry_loaded",
            total=len(self._etls),
            etls=list(self._etls.keys()),
        )
        
        return len(self._etls)

    def load(self, etl_name: str) -> ETLConfig:
        """
        Load a specific ETL configuration.
        
        Args:
            etl_name: Name of the ETL to load.
            
        Returns:
            Loaded ETLConfig.
            
        Raises:
            ConfigurationError: If loading fails.
        """
        try:
            data = self._config_manager.load_etl_config(etl_name)
            
            # Ensure name is set
            if "name" not in data:
                data["name"] = etl_name
            
            etl = ETLConfig.model_validate(data)
            self._etls[etl.name] = etl
            
            logger.debug(
                "etl_loaded",
                name=etl.name,
                category=etl.category,
                environments=list(etl.topics.keys()),
            )
            
            return etl
            
        except Exception as e:
            raise ConfigurationError(
                f"Failed to load ETL config: {etl_name}",
                config_path=f"etl/{etl_name}.yaml",
                details={"error": str(e)},
            ) from e

    def register(self, etl: ETLConfig) -> None:
        """
        Register an ETL configuration programmatically.
        
        Args:
            etl: ETL configuration to register.
        """
        self._etls[etl.name] = etl
        logger.debug("etl_registered", name=etl.name)

    def unregister(self, etl_name: str) -> bool:
        """
        Remove an ETL from the registry.
        
        Args:
            etl_name: Name of the ETL to remove.
            
        Returns:
            True if removed, False if not found.
        """
        if etl_name in self._etls:
            del self._etls[etl_name]
            return True
        return False

    def get(self, etl_name: str) -> ETLConfig | None:
        """
        Get an ETL by name.
        
        Args:
            etl_name: Name of the ETL.
            
        Returns:
            ETLConfig or None if not found.
        """
        return self._etls.get(etl_name)

    def get_or_raise(self, etl_name: str) -> ETLConfig:
        """
        Get an ETL by name, raising if not found.
        
        Args:
            etl_name: Name of the ETL.
            
        Returns:
            ETLConfig.
            
        Raises:
            ConfigurationError: If ETL not found.
        """
        etl = self.get(etl_name)
        if etl is None:
            raise ConfigurationError(
                f"ETL not found: {etl_name}",
                details={"available": list(self._etls.keys())},
            )
        return etl

    def all(self) -> list[ETLConfig]:
        """
        Get all registered ETLs.
        
        Returns:
            List of all ETL configurations.
        """
        return list(self._etls.values())

    def names(self) -> list[str]:
        """
        Get all ETL names.
        
        Returns:
            List of ETL names.
        """
        return list(self._etls.keys())

    def count(self) -> int:
        """
        Get number of registered ETLs.
        
        Returns:
            Count of ETLs.
        """
        return len(self._etls)

    def filter(
        self,
        category: str | None = None,
        enabled_only: bool = True,
        has_environment: str | None = None,
        filters: RunnerFilters | None = None,
    ) -> Iterator[ETLConfig]:
        """
        Filter ETLs by criteria.
        
        Args:
            category: Filter by category.
            enabled_only: Only return enabled ETLs.
            has_environment: Only return ETLs with this environment configured.
            filters: RunnerFilters for complex filtering.
            
        Yields:
            Matching ETL configurations.
        """
        for etl in self._etls.values():
            # Check enabled
            if enabled_only and not etl.enabled:
                continue
            
            # Check category
            if category and etl.category != category:
                continue
            
            # Check environment
            if has_environment and has_environment not in etl.topics:
                continue
            
            # Check runner filters
            if filters:
                if not filters.matches_category(etl.category):
                    continue
                if not filters.matches_etl(etl.name):
                    continue
            
            yield etl

    def filter_for_environment(
        self,
        environment: str,
        filters: RunnerFilters | None = None,
    ) -> list[ETLConfig]:
        """
        Get all ETLs configured for an environment.
        
        Args:
            environment: Environment name.
            filters: Optional additional filters.
            
        Returns:
            List of matching ETL configurations.
        """
        return [
            etl for etl in self.filter(
                enabled_only=True,
                has_environment=environment,
                filters=filters,
            )
            if not filters or filters.matches(etl, environment)
        ]

    def get_categories(self) -> set[str]:
        """
        Get all unique categories.
        
        Returns:
            Set of category names.
        """
        return {etl.category for etl in self._etls.values()}

    def get_environments(self) -> set[str]:
        """
        Get all unique environments across all ETLs.
        
        Returns:
            Set of environment names.
        """
        environments: set[str] = set()
        for etl in self._etls.values():
            environments.update(etl.topics.keys())
        return environments

    def validate_all(self) -> list[str]:
        """
        Validate all registered ETLs.
        
        Returns:
            List of validation errors (empty if all valid).
        """
        errors: list[str] = []
        
        for etl in self._etls.values():
            # Check required fields
            if not etl.payload_template:
                errors.append(f"{etl.name}: missing payload_template")
            
            # Check topics
            if not etl.topics:
                errors.append(f"{etl.name}: no topics configured")
            
            # Validate template exists
            template_path = (
                self._config_manager.template_dir 
                / "payloads" 
                / etl.payload_template
            )
            if not template_path.exists():
                errors.append(
                    f"{etl.name}: template not found: {etl.payload_template}"
                )
        
        return errors

    def to_dict(self) -> dict[str, dict[str, Any]]:
        """
        Export all ETLs as a dictionary.
        
        Returns:
            Dictionary of ETL name to config dict.
        """
        return {
            name: etl.model_dump()
            for name, etl in self._etls.items()
        }

    def __len__(self) -> int:
        """Get number of registered ETLs."""
        return len(self._etls)

    def __contains__(self, etl_name: str) -> bool:
        """Check if ETL is registered."""
        return etl_name in self._etls

    def __iter__(self) -> Iterator[ETLConfig]:
        """Iterate over all ETLs."""
        return iter(self._etls.values())

