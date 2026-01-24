"""
Unified HTTP client wrapper for sync and async requests.

Provides:
- Consistent interface for requests and aiohttp
- Automatic retries with exponential backoff
- Rate limit handling
- Request timing
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any

import requests

logger = logging.getLogger(__name__)


@dataclass
class HTTPResponse:
    """Standardized HTTP response container."""

    status_code: int
    content: bytes
    headers: dict[str, str]
    url: str
    elapsed_ms: float

    @property
    def ok(self) -> bool:
        """Check if response was successful (2xx)."""
        return 200 <= self.status_code < 300

    @property
    def text(self) -> str:
        """Decode content as UTF-8 text."""
        return self.content.decode("utf-8", errors="replace")

    def json(self) -> Any:
        """Parse content as JSON."""
        import json
        return json.loads(self.content)


class HTTPClient:
    """
    Synchronous HTTP client with retry and rate limiting.

    Wraps the requests library with:
    - Automatic retries on failure
    - Exponential backoff
    - Rate limit detection and handling
    - Request timing
    """

    def __init__(
        self,
        *,
        timeout: int = 30,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
        rate_limit_wait: float = 60.0,
        user_agent: str = "UniversalScraper/1.0",
    ):
        """
        Initialize the HTTP client.

        Args:
            timeout: Request timeout in seconds
            max_retries: Maximum retry attempts
            backoff_factor: Exponential backoff multiplier
            rate_limit_wait: Seconds to wait on rate limit (429)
            user_agent: User-Agent header value
        """
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.rate_limit_wait = rate_limit_wait

        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": user_agent,
            "Accept": "application/json, text/html, */*",
        })

    def get(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> HTTPResponse:
        """
        Make a GET request with retries.

        Args:
            url: URL to fetch
            params: Query parameters
            headers: Additional headers

        Returns:
            HTTPResponse object

        Raises:
            requests.RequestException: If all retries fail
        """
        return self._request("GET", url, params=params, headers=headers)

    def post(
        self,
        url: str,
        *,
        data: dict[str, Any] | None = None,
        json_data: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> HTTPResponse:
        """
        Make a POST request with retries.

        Args:
            url: URL to post to
            data: Form data
            json_data: JSON data
            headers: Additional headers

        Returns:
            HTTPResponse object
        """
        return self._request(
            "POST", url, data=data, json=json_data, headers=headers
        )

    def _request(
        self,
        method: str,
        url: str,
        **kwargs: Any,
    ) -> HTTPResponse:
        """Internal request method with retry logic."""
        last_exception = None

        for attempt in range(self.max_retries):
            try:
                start_time = time.perf_counter()

                response = self._session.request(
                    method,
                    url,
                    timeout=self.timeout,
                    **kwargs,
                )

                elapsed_ms = (time.perf_counter() - start_time) * 1000

                # Handle rate limiting
                if response.status_code == 429:
                    retry_after = response.headers.get("Retry-After")
                    wait_time = float(retry_after) if retry_after else self.rate_limit_wait
                    logger.warning(f"Rate limited, waiting {wait_time}s")
                    time.sleep(wait_time)
                    continue

                return HTTPResponse(
                    status_code=response.status_code,
                    content=response.content,
                    headers=dict(response.headers),
                    url=str(response.url),
                    elapsed_ms=elapsed_ms,
                )

            except requests.RequestException as e:
                last_exception = e
                logger.warning(f"Request failed (attempt {attempt + 1}): {e}")

                if attempt < self.max_retries - 1:
                    wait_time = self.backoff_factor ** attempt
                    time.sleep(wait_time)

        raise last_exception or requests.RequestException("Request failed")

    def close(self) -> None:
        """Close the session."""
        self._session.close()

    def __enter__(self) -> HTTPClient:
        return self

    def __exit__(self, *args) -> None:
        self.close()


class AsyncHTTPClient:
    """
    Asynchronous HTTP client with retry and rate limiting.

    Wraps aiohttp with the same interface as HTTPClient.
    """

    def __init__(
        self,
        *,
        timeout: int = 30,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
        rate_limit_wait: float = 60.0,
        user_agent: str = "UniversalScraper/1.0",
        max_connections: int = 10,
    ):
        """
        Initialize the async HTTP client.

        Args:
            timeout: Request timeout in seconds
            max_retries: Maximum retry attempts
            backoff_factor: Exponential backoff multiplier
            rate_limit_wait: Seconds to wait on rate limit
            user_agent: User-Agent header value
            max_connections: Maximum concurrent connections
        """
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.rate_limit_wait = rate_limit_wait
        self.user_agent = user_agent
        self.max_connections = max_connections

        self._session = None

    async def _get_session(self):
        """Lazy initialization of aiohttp session."""
        if self._session is None:
            import aiohttp
            connector = aiohttp.TCPConnector(limit=self.max_connections)
            timeout = aiohttp.ClientTimeout(total=self.timeout)
            self._session = aiohttp.ClientSession(
                connector=connector,
                timeout=timeout,
                headers={"User-Agent": self.user_agent},
            )
        return self._session

    async def get(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> HTTPResponse:
        """Make an async GET request with retries."""
        return await self._request("GET", url, params=params, headers=headers)

    async def post(
        self,
        url: str,
        *,
        data: dict[str, Any] | None = None,
        json_data: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> HTTPResponse:
        """Make an async POST request with retries."""
        return await self._request(
            "POST", url, data=data, json=json_data, headers=headers
        )

    async def _request(
        self,
        method: str,
        url: str,
        **kwargs: Any,
    ) -> HTTPResponse:
        """Internal async request method with retry logic."""
        import aiohttp

        session = await self._get_session()
        last_exception = None

        for attempt in range(self.max_retries):
            try:
                start_time = time.perf_counter()

                async with session.request(method, url, **kwargs) as response:
                    content = await response.read()
                    elapsed_ms = (time.perf_counter() - start_time) * 1000

                    # Handle rate limiting
                    if response.status == 429:
                        retry_after = response.headers.get("Retry-After")
                        wait_time = float(retry_after) if retry_after else self.rate_limit_wait
                        logger.warning(f"Rate limited, waiting {wait_time}s")
                        await asyncio.sleep(wait_time)
                        continue

                    return HTTPResponse(
                        status_code=response.status,
                        content=content,
                        headers=dict(response.headers),
                        url=str(response.url),
                        elapsed_ms=elapsed_ms,
                    )

            except aiohttp.ClientError as e:
                last_exception = e
                logger.warning(f"Request failed (attempt {attempt + 1}): {e}")

                if attempt < self.max_retries - 1:
                    wait_time = self.backoff_factor ** attempt
                    await asyncio.sleep(wait_time)

        raise last_exception or Exception("Request failed")

    async def close(self) -> None:
        """Close the session."""
        if self._session:
            await self._session.close()
            self._session = None

    async def __aenter__(self) -> AsyncHTTPClient:
        return self

    async def __aexit__(self, *args) -> None:
        await self.close()
