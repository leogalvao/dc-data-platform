"""
Per-source rate limiting with token bucket algorithm.

Supports both sync and async usage, with automatic backoff
on rate limit responses (HTTP 429).
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from threading import Lock
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class RateLimitConfig:
    """Configuration for rate limiting."""

    requests_per_second: float = 1.0
    burst_size: int = 1
    retry_after_default: float = 60.0
    max_retries: int = 3
    backoff_factor: float = 2.0


@dataclass
class RateLimitStats:
    """Statistics for rate limiting."""

    requests_made: int = 0
    requests_delayed: int = 0
    total_delay_seconds: float = 0.0
    rate_limit_hits: int = 0
    last_request_time: float = 0.0


class RateLimiter:
    """
    Token bucket rate limiter with automatic retry handling.

    Supports per-source rate limiting to avoid overwhelming
    any single data source while allowing parallel scraping
    of multiple sources.
    """

    def __init__(
        self,
        source_name: str,
        config: RateLimitConfig | None = None,
    ):
        """
        Initialize rate limiter.

        Args:
            source_name: Identifier for the source being limited
            config: Rate limit configuration
        """
        self.source_name = source_name
        self.config = config or RateLimitConfig()

        # Token bucket state
        self._tokens = float(self.config.burst_size)
        self._last_refill = time.monotonic()
        self._lock = Lock()
        self._async_lock: asyncio.Lock | None = None

        # Stats
        self.stats = RateLimitStats()

        self._logger = logging.getLogger(f"{__name__}.{source_name}")

    @property
    def async_lock(self) -> asyncio.Lock:
        """Lazy-init async lock (must be created in async context)."""
        if self._async_lock is None:
            self._async_lock = asyncio.Lock()
        return self._async_lock

    def _refill_tokens(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.monotonic()
        elapsed = now - self._last_refill
        new_tokens = elapsed * self.config.requests_per_second
        self._tokens = min(self.config.burst_size, self._tokens + new_tokens)
        self._last_refill = now

    def _calculate_wait_time(self) -> float:
        """Calculate how long to wait for a token."""
        if self._tokens >= 1.0:
            return 0.0
        tokens_needed = 1.0 - self._tokens
        return tokens_needed / self.config.requests_per_second

    def acquire(self) -> float:
        """
        Acquire a rate limit token (blocking).

        Returns:
            Time spent waiting in seconds
        """
        with self._lock:
            self._refill_tokens()
            wait_time = self._calculate_wait_time()

            if wait_time > 0:
                self._logger.debug(f"Rate limiting: waiting {wait_time:.2f}s")
                self.stats.requests_delayed += 1
                self.stats.total_delay_seconds += wait_time
                time.sleep(wait_time)
                self._refill_tokens()

            self._tokens -= 1.0
            self.stats.requests_made += 1
            self.stats.last_request_time = time.monotonic()

            return wait_time

    async def acquire_async(self) -> float:
        """
        Acquire a rate limit token (async).

        Returns:
            Time spent waiting in seconds
        """
        async with self.async_lock:
            self._refill_tokens()
            wait_time = self._calculate_wait_time()

            if wait_time > 0:
                self._logger.debug(f"Rate limiting: waiting {wait_time:.2f}s")
                self.stats.requests_delayed += 1
                self.stats.total_delay_seconds += wait_time
                await asyncio.sleep(wait_time)
                self._refill_tokens()

            self._tokens -= 1.0
            self.stats.requests_made += 1
            self.stats.last_request_time = time.monotonic()

            return wait_time

    def handle_rate_limit_response(
        self,
        retry_after: float | None = None,
    ) -> float:
        """
        Handle a rate limit response (HTTP 429).

        Drains tokens and returns recommended wait time.

        Args:
            retry_after: Server-provided retry delay (from Retry-After header)

        Returns:
            Recommended wait time in seconds
        """
        self.stats.rate_limit_hits += 1

        # Drain all tokens
        with self._lock:
            self._tokens = 0.0

        wait_time = retry_after or self.config.retry_after_default
        self._logger.warning(
            f"Rate limited by {self.source_name}. "
            f"Hit #{self.stats.rate_limit_hits}. "
            f"Waiting {wait_time:.1f}s"
        )

        return wait_time

    def get_backoff_delay(self, attempt: int) -> float:
        """
        Calculate exponential backoff delay.

        Args:
            attempt: Current attempt number (0-indexed)

        Returns:
            Delay in seconds
        """
        delay = self.config.retry_after_default * (self.config.backoff_factor ** attempt)
        return min(delay, 300.0)  # Cap at 5 minutes

    def get_stats(self) -> dict[str, Any]:
        """Get current rate limit statistics."""
        return {
            "source_name": self.source_name,
            "requests_made": self.stats.requests_made,
            "requests_delayed": self.stats.requests_delayed,
            "total_delay_seconds": round(self.stats.total_delay_seconds, 2),
            "rate_limit_hits": self.stats.rate_limit_hits,
            "current_tokens": round(self._tokens, 2),
        }


class RateLimiterPool:
    """
    Pool of rate limiters for multiple sources.

    Provides centralized management and monitoring of
    per-source rate limits.
    """

    def __init__(self, default_config: RateLimitConfig | None = None):
        """
        Initialize the pool.

        Args:
            default_config: Default config for new limiters
        """
        self._limiters: dict[str, RateLimiter] = {}
        self._default_config = default_config or RateLimitConfig()
        self._lock = Lock()

    def get(
        self,
        source_name: str,
        config: RateLimitConfig | None = None,
    ) -> RateLimiter:
        """
        Get or create a rate limiter for a source.

        Args:
            source_name: Source identifier
            config: Optional custom config

        Returns:
            RateLimiter for the source
        """
        with self._lock:
            if source_name not in self._limiters:
                self._limiters[source_name] = RateLimiter(
                    source_name=source_name,
                    config=config or self._default_config,
                )
            return self._limiters[source_name]

    def get_all_stats(self) -> dict[str, dict[str, Any]]:
        """Get stats for all rate limiters."""
        return {
            name: limiter.get_stats()
            for name, limiter in self._limiters.items()
        }

    def reset(self, source_name: str | None = None) -> None:
        """
        Reset rate limiter(s).

        Args:
            source_name: Specific source to reset, or None for all
        """
        with self._lock:
            if source_name:
                if source_name in self._limiters:
                    del self._limiters[source_name]
            else:
                self._limiters.clear()
