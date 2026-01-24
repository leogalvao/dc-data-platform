"""Core framework components."""

from .base_scraper import BaseScraper, ScraperResult
from .rate_limiter import RateLimitConfig, RateLimiter
from .registry import ScraperRegistry, register_scraper

__all__ = [
    "BaseScraper",
    "ScraperResult",
    "ScraperRegistry",
    "register_scraper",
    "RateLimiter",
    "RateLimitConfig",
]
