"""
Scraper registry for dynamic discovery and instantiation.

Allows adapters to register themselves and be discovered/instantiated
by the orchestrator without hardcoding imports.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, TypeVar

if TYPE_CHECKING:
    from ..schemas.metadata import SourceConfig
    from .base_scraper import BaseScraper

logger = logging.getLogger(__name__)

T = TypeVar("T", bound="BaseScraper")


class ScraperRegistry:
    """
    Central registry for all scraper adapters.

    Scrapers register themselves with a unique name and can be
    instantiated by name with their configuration.
    """

    _registry: dict[str, type[BaseScraper]] = {}
    _configs: dict[str, SourceConfig] = {}

    @classmethod
    def register(cls, name: str, scraper_class: type[BaseScraper]) -> None:
        """
        Register a scraper class.

        Args:
            name: Unique identifier for this scraper
            scraper_class: The scraper class to register
        """
        if name in cls._registry:
            logger.warning(f"Overwriting existing scraper registration: {name}")
        cls._registry[name] = scraper_class
        logger.debug(f"Registered scraper: {name} -> {scraper_class.__name__}")

    @classmethod
    def register_config(cls, config: SourceConfig) -> None:
        """
        Register a source configuration.

        Args:
            config: SourceConfig to register
        """
        cls._configs[config.name] = config
        logger.debug(f"Registered config: {config.name}")

    @classmethod
    def get(cls, name: str) -> type[BaseScraper] | None:
        """
        Get a registered scraper class by name.

        Args:
            name: Scraper identifier

        Returns:
            Scraper class or None if not found
        """
        return cls._registry.get(name)

    @classmethod
    def get_config(cls, name: str) -> SourceConfig | None:
        """
        Get a registered configuration by name.

        Args:
            name: Source identifier

        Returns:
            SourceConfig or None if not found
        """
        return cls._configs.get(name)

    @classmethod
    def create(cls, name: str, config: SourceConfig | None = None, **kwargs) -> BaseScraper:
        """
        Create a scraper instance by name.

        Args:
            name: Scraper identifier
            config: Optional config override (uses registered config if not provided)
            **kwargs: Additional arguments for scraper constructor

        Returns:
            Instantiated scraper

        Raises:
            ValueError: If scraper not found
        """
        scraper_class = cls.get(name)
        if scraper_class is None:
            raise ValueError(f"Unknown scraper: {name}. Available: {list(cls._registry.keys())}")

        if config is None:
            config = cls.get_config(name)
            if config is None:
                raise ValueError(f"No config registered for: {name}")

        return scraper_class(config=config, **kwargs)

    @classmethod
    def list_scrapers(cls) -> list[str]:
        """List all registered scraper names."""
        return list(cls._registry.keys())

    @classmethod
    def list_enabled(cls) -> list[str]:
        """List scrapers with enabled configs."""
        return [
            name for name, config in cls._configs.items()
            if config.enabled and name in cls._registry
        ]

    @classmethod
    def clear(cls) -> None:
        """Clear all registrations (useful for testing)."""
        cls._registry.clear()
        cls._configs.clear()


def register_scraper(name: str) -> Callable[[type[T]], type[T]]:
    """
    Decorator to register a scraper class.

    Usage:
        @register_scraper("my_source")
        class MySourceAdapter(BaseScraper):
            ...

    Args:
        name: Unique identifier for this scraper

    Returns:
        Decorator function
    """
    def decorator(cls: type[T]) -> type[T]:
        ScraperRegistry.register(name, cls)
        return cls
    return decorator
