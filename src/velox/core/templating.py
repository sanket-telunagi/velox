"""
Jinja2 templating engine with runtime-modifiable context.

Provides:
- Template rendering for config files and payloads
- Custom filters and globals
- Context management with runtime updates
- Caching for performance
"""

import hashlib
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import orjson
from jinja2 import (
    BaseLoader,
    Environment,
    FileSystemLoader,
    StrictUndefined,
    Template,
    TemplateNotFound,
    select_autoescape,
)

from velox.core.exceptions import TemplateError


class DictLoader(BaseLoader):
    """Loader that loads templates from a dictionary."""

    def __init__(self, mapping: dict[str, str]) -> None:
        self.mapping = mapping

    def get_source(
        self, environment: Environment, template: str
    ) -> tuple[str, str | None, Callable[[], bool] | None]:
        if template in self.mapping:
            source = self.mapping[template]
            return source, None, lambda: True
        raise TemplateNotFound(template)


class TemplateEngine:
    """
    High-performance Jinja2 template engine with runtime context support.
    
    Features:
    - File-based and string-based template loading
    - Runtime-modifiable context
    - Custom filters for common operations
    - Template caching
    - Strict undefined variable handling
    
    Usage:
        engine = TemplateEngine(template_dirs=[Path("templates")])
        engine.set_context({"user": "john", "timestamp": datetime.now()})
        result = engine.render("payload.json.j2")
    """

    def __init__(
        self,
        template_dirs: list[Path] | None = None,
        string_templates: dict[str, str] | None = None,
        auto_reload: bool = True,
    ) -> None:
        """
        Initialize the template engine.
        
        Args:
            template_dirs: List of directories to load templates from.
            string_templates: Dictionary of template name -> template string.
            auto_reload: Whether to auto-reload templates when they change.
        """
        self._context: dict[str, Any] = {}
        self._template_cache: dict[str, Template] = {}
        
        # Build list of loaders
        loaders: list[BaseLoader] = []
        
        if template_dirs:
            loaders.append(
                FileSystemLoader(
                    [str(d) for d in template_dirs],
                    followlinks=True,
                )
            )
        
        if string_templates:
            loaders.append(DictLoader(string_templates))
        
        # If no loaders provided, use empty dict loader
        if not loaders:
            loaders.append(DictLoader({}))
        
        # Create environment with the first loader (we'll handle multiple manually)
        self._env = Environment(
            loader=loaders[0] if len(loaders) == 1 else FileSystemLoader([]),
            autoescape=select_autoescape(["html", "xml"]),
            undefined=StrictUndefined,
            auto_reload=auto_reload,
            enable_async=True,
        )
        
        # Store additional loaders
        self._loaders = loaders
        self._string_templates = string_templates or {}
        
        # Register custom filters
        self._register_filters()
        
        # Register custom globals
        self._register_globals()

    def _register_filters(self) -> None:
        """Register custom Jinja2 filters."""
        self._env.filters.update({
            "to_json": self._filter_to_json,
            "from_json": self._filter_from_json,
            "uuid": self._filter_uuid,
            "hash_md5": self._filter_hash_md5,
            "hash_sha256": self._filter_hash_sha256,
            "timestamp": self._filter_timestamp,
            "iso_date": self._filter_iso_date,
            "env_upper": lambda s: s.upper().replace("-", "_"),
            "default_empty": lambda v, d="": v if v else d,
        })

    def _register_globals(self) -> None:
        """Register custom Jinja2 globals."""
        self._env.globals.update({
            "now": datetime.now,
            "utcnow": lambda: datetime.now(timezone.utc),
            "uuid4": lambda: str(uuid.uuid4()),
            "timestamp_ms": lambda: int(datetime.now().timestamp() * 1000),
        })

    @staticmethod
    def _filter_to_json(value: Any, indent: int | None = None) -> str:
        """Convert value to JSON string using orjson for performance."""
        opts = orjson.OPT_INDENT_2 if indent else 0
        return orjson.dumps(value, option=opts).decode("utf-8")

    @staticmethod
    def _filter_from_json(value: str) -> Any:
        """Parse JSON string using orjson for performance."""
        return orjson.loads(value)

    @staticmethod
    def _filter_uuid(value: Any = None) -> str:
        """Generate or convert to UUID."""
        if value is None:
            return str(uuid.uuid4())
        return str(uuid.UUID(str(value)))

    @staticmethod
    def _filter_hash_md5(value: str) -> str:
        """Generate MD5 hash of value."""
        return hashlib.md5(value.encode()).hexdigest()

    @staticmethod
    def _filter_hash_sha256(value: str) -> str:
        """Generate SHA256 hash of value."""
        return hashlib.sha256(value.encode()).hexdigest()

    @staticmethod
    def _filter_timestamp(value: datetime | None = None) -> int:
        """Convert datetime to Unix timestamp."""
        dt = value or datetime.now()
        return int(dt.timestamp())

    @staticmethod
    def _filter_iso_date(value: datetime | None = None) -> str:
        """Convert datetime to ISO format."""
        dt = value or datetime.now()
        return dt.isoformat()

    @property
    def context(self) -> dict[str, Any]:
        """Get current context (copy)."""
        return self._context.copy()

    def set_context(self, context: dict[str, Any]) -> None:
        """
        Replace the entire context.
        
        Args:
            context: New context dictionary.
        """
        self._context = context.copy()

    def update_context(self, updates: dict[str, Any]) -> None:
        """
        Update context with new values.
        
        Args:
            updates: Dictionary of values to add/update.
        """
        self._context.update(updates)

    def clear_context(self) -> None:
        """Clear all context values."""
        self._context.clear()

    def get_context_value(self, key: str, default: Any = None) -> Any:
        """
        Get a single context value.
        
        Args:
            key: Context key.
            default: Default value if key not found.
            
        Returns:
            The context value or default.
        """
        return self._context.get(key, default)

    def set_context_value(self, key: str, value: Any) -> None:
        """
        Set a single context value.
        
        Args:
            key: Context key.
            value: Value to set.
        """
        self._context[key] = value

    def render(
        self,
        template_name: str,
        extra_context: dict[str, Any] | None = None,
    ) -> str:
        """
        Render a template by name.
        
        Args:
            template_name: Name of the template to render.
            extra_context: Additional context to merge (doesn't modify base context).
            
        Returns:
            Rendered template string.
            
        Raises:
            TemplateError: If template not found or rendering fails.
        """
        try:
            template = self._get_template(template_name)
            context = {**self._context, **(extra_context or {})}
            return template.render(**context)
        except TemplateNotFound as e:
            raise TemplateError(
                f"Template not found: {template_name}",
                template_name=template_name,
            ) from e
        except Exception as e:
            raise TemplateError(
                f"Failed to render template: {e}",
                template_name=template_name,
                context=self._context,
            ) from e

    async def render_async(
        self,
        template_name: str,
        extra_context: dict[str, Any] | None = None,
    ) -> str:
        """
        Render a template asynchronously.
        
        Args:
            template_name: Name of the template to render.
            extra_context: Additional context to merge.
            
        Returns:
            Rendered template string.
        """
        try:
            template = self._get_template(template_name)
            context = {**self._context, **(extra_context or {})}
            return await template.render_async(**context)
        except TemplateNotFound as e:
            raise TemplateError(
                f"Template not found: {template_name}",
                template_name=template_name,
            ) from e
        except Exception as e:
            raise TemplateError(
                f"Failed to render template: {e}",
                template_name=template_name,
                context=self._context,
            ) from e

    def render_string(
        self,
        template_string: str,
        extra_context: dict[str, Any] | None = None,
    ) -> str:
        """
        Render a template from a string.
        
        Args:
            template_string: Template content as string.
            extra_context: Additional context to merge.
            
        Returns:
            Rendered string.
        """
        try:
            template = self._env.from_string(template_string)
            context = {**self._context, **(extra_context or {})}
            return template.render(**context)
        except Exception as e:
            raise TemplateError(
                f"Failed to render string template: {e}",
                context=self._context,
            ) from e

    async def render_string_async(
        self,
        template_string: str,
        extra_context: dict[str, Any] | None = None,
    ) -> str:
        """
        Render a template string asynchronously.
        
        Args:
            template_string: Template content as string.
            extra_context: Additional context to merge.
            
        Returns:
            Rendered string.
        """
        try:
            template = self._env.from_string(template_string)
            context = {**self._context, **(extra_context or {})}
            return await template.render_async(**context)
        except Exception as e:
            raise TemplateError(
                f"Failed to render string template: {e}",
                context=self._context,
            ) from e

    def _get_template(self, name: str) -> Template:
        """
        Get template by name, with caching.
        
        Args:
            name: Template name.
            
        Returns:
            Jinja2 Template object.
        """
        # Check string templates first
        if name in self._string_templates:
            if name not in self._template_cache:
                self._template_cache[name] = self._env.from_string(
                    self._string_templates[name]
                )
            return self._template_cache[name]
        
        # Load from environment
        return self._env.get_template(name)

    def add_string_template(self, name: str, template_string: str) -> None:
        """
        Add a string template at runtime.
        
        Args:
            name: Template name.
            template_string: Template content.
        """
        self._string_templates[name] = template_string
        # Invalidate cache
        if name in self._template_cache:
            del self._template_cache[name]

    def clear_cache(self) -> None:
        """Clear the template cache."""
        self._template_cache.clear()

