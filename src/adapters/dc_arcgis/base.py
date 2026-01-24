"""
Base adapter for DC ArcGIS REST API scrapers.

Provides shared functionality for all DC government data scrapers
that use the ArcGIS REST API (purchase_orders, contracts, payments,
dc_opendata, affordable_housing).

This adapter extracts and reuses the common async batch fetching
pattern from the existing scrapers.

Performance optimizations:
- Reuses event loop instead of creating new one per call
- Maintains aiohttp session across requests for connection pooling
- Uses thread-local storage for sync-to-async bridge
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
from abc import abstractmethod
from collections.abc import AsyncIterator, Iterator
from datetime import datetime, timezone
from typing import Any

import aiohttp

from ...core.base_scraper import BaseScraper, ScraperResult
from ...schemas.base import BaseRecord, RawPayload
from ...schemas.metadata import SourceConfig

logger = logging.getLogger(__name__)

# Thread-local storage for event loop reuse in sync mode
_thread_local = threading.local()


class DCArcGISBaseAdapter(BaseScraper):
    """
    Base adapter for DC ArcGIS REST API sources.

    Reuses the async batch fetching pattern from existing scrapers:
    - Offset-based pagination
    - Semaphore-controlled concurrency
    - Rate limiting based on requests_per_second config
    - ESRI date conversion
    - Retry with exponential backoff

    Subclasses override:
    - MAPSERVER_ID: The MapServer endpoint number
    - FIELDS: Fields to request from the API
    - validate(): Source-specific validation
    - normalize(): Source-specific normalization
    """

    # Override in subclasses
    MAPSERVER_ID: int = 0
    FIELDS: str = "*"
    BASE_URL: str = "https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Government_Operations/MapServer"

    SOURCE_NAME = "dc_arcgis"
    SOURCE_VERSION = "1.0.0"

    def __init__(self, config: SourceConfig, **kwargs):
        super().__init__(config, **kwargs)
        self._session: aiohttp.ClientSession | None = None
        self._semaphore: asyncio.Semaphore | None = None
        self._total_records: int | None = None
        self._last_request_time: float = 0.0
        self._rate_lock: asyncio.Lock | None = None

    # =========================================================================
    # Lifecycle
    # =========================================================================

    def setup(self) -> None:
        """Initialize async resources."""
        super().setup()
        # Note: aiohttp session must be created in async context
        # We'll create it lazily in fetch_async

    def teardown(self) -> None:
        """Clean up async resources including session and event loop."""
        # Clean up aiohttp session if it exists
        if self._session is not None:
            loop = self._get_or_create_event_loop()
            if not loop.is_closed():
                try:
                    loop.run_until_complete(self._session.close())
                except Exception:
                    pass  # Best effort cleanup
            self._session = None
        super().teardown()

    # =========================================================================
    # Sync Interface (wraps async)
    # =========================================================================

    def _get_or_create_event_loop(self) -> asyncio.AbstractEventLoop:
        """
        Get or create a reusable event loop for sync-to-async bridging.

        Uses thread-local storage to maintain one loop per thread,
        avoiding the overhead of creating/destroying loops per call.
        """
        if not hasattr(_thread_local, 'loop') or _thread_local.loop is None or _thread_local.loop.is_closed():
            _thread_local.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(_thread_local.loop)
        return _thread_local.loop

    def discover(self) -> Iterator[dict[str, Any]]:
        """
        Sync discover - runs async version with reused event loop.

        Performance: Reuses event loop across calls instead of creating
        new loop per invocation. Session is kept alive for connection pooling.
        """
        loop = self._get_or_create_event_loop()

        async def _collect():
            items = []
            async for item in self.discover_async():
                items.append(item)
            # Note: Session is NOT closed here to enable connection reuse
            return items

        items = loop.run_until_complete(_collect())
        yield from items

    def fetch(self, item: dict[str, Any]) -> RawPayload | None:
        """
        Sync fetch - runs async version with reused event loop.

        Performance: Reuses event loop and aiohttp session across calls
        to benefit from connection pooling and avoid per-call overhead.
        """
        loop = self._get_or_create_event_loop()
        # Note: Session is NOT closed after fetch to enable connection reuse
        return loop.run_until_complete(self.fetch_async(item))

    def parse(self, payload: RawPayload) -> Iterator[dict[str, Any]]:
        """Parse ESRI JSON response."""
        try:
            content = payload.content.decode("utf-8")
            data = json.loads(content)
            features = data.get("features", [])

            for feature in features:
                attrs = feature.get("attributes", {})
                # Convert ESRI dates
                attrs = self._convert_esri_dates(attrs)
                # Add geometry if present
                geometry = feature.get("geometry")
                if geometry:
                    attrs["_longitude"] = geometry.get("x")
                    attrs["_latitude"] = geometry.get("y")
                yield attrs

        except Exception as e:
            self._logger.error(f"Parse error: {e}")

    # =========================================================================
    # Async Interface
    # =========================================================================

    async def discover_async(self) -> AsyncIterator[dict[str, Any]]:
        """
        Discover batches to fetch via offset pagination.

        First queries for total record count, then yields
        batch items for each offset.
        """
        # Get total record count
        self._total_records = await self._fetch_total_records()

        if self._total_records == 0:
            self._logger.warning("No records found")
            return

        self._logger.info(f"Discovered {self._total_records} total records")

        batch_size = self.config.batch_size
        for offset in range(0, self._total_records, batch_size):
            yield {
                "offset": offset,
                "limit": batch_size,
                "url": self._build_query_url(offset, batch_size),
            }

    async def fetch_async(self, item: dict[str, Any]) -> RawPayload | None:
        """
        Fetch a batch of records.

        Uses semaphore for concurrency control, rate limiting based on
        requests_per_second config, and retry with exponential backoff.
        """
        import time

        # Lazy init session, semaphore, and rate lock
        if self._session is None:
            timeout = aiohttp.ClientTimeout(total=self.config.timeout_seconds)
            self._session = aiohttp.ClientSession(timeout=timeout)
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(self.config.max_workers)
        if self._rate_lock is None:
            self._rate_lock = asyncio.Lock()

        url = item["url"]
        offset = item["offset"]

        async with self._semaphore:
            # Rate limiting: respect requests_per_second
            if self.config.requests_per_second > 0:
                min_interval = 1.0 / self.config.requests_per_second
                async with self._rate_lock:
                    now = time.monotonic()
                    elapsed = now - self._last_request_time
                    if elapsed < min_interval:
                        await asyncio.sleep(min_interval - elapsed)
                    self._last_request_time = time.monotonic()
            for attempt in range(self.config.max_retries):
                try:
                    start_time = datetime.now(timezone.utc)
                    async with self._session.get(url) as response:
                        if response.status == 200:
                            content = await response.read()
                            duration_ms = int(
                                (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
                            )
                            return RawPayload(
                                content_type="application/json",
                                content=content,
                                lineage=self._make_lineage(url),
                                fetch_status_code=response.status,
                                fetch_duration_ms=duration_ms,
                            )
                        elif response.status == 429:
                            wait_time = self.config.extra.get("rate_limit_wait", 60)
                            self._logger.warning(f"Rate limited, waiting {wait_time}s")
                            await asyncio.sleep(wait_time)
                        else:
                            self._logger.warning(
                                f"HTTP {response.status} for offset {offset}"
                            )

                except asyncio.TimeoutError:
                    self._logger.warning(f"Timeout for offset {offset}, attempt {attempt + 1}")
                except Exception as e:
                    self._logger.error(f"Fetch error for offset {offset}: {e}")

                # Exponential backoff
                if attempt < self.config.max_retries - 1:
                    wait = 2 ** attempt
                    await asyncio.sleep(wait)

        self._logger.error(f"Failed to fetch offset {offset} after {self.config.max_retries} attempts")
        return None

    # =========================================================================
    # Override in subclasses
    # =========================================================================

    @abstractmethod
    def validate(self, data: dict[str, Any]) -> tuple[bool, list[str]]:
        """Validate a parsed record. Override in subclass."""
        ...

    @abstractmethod
    def normalize(self, data: dict[str, Any]) -> BaseRecord:
        """Normalize to canonical schema. Override in subclass."""
        ...

    # =========================================================================
    # Helpers
    # =========================================================================

    def _build_query_url(self, offset: int, limit: int) -> str:
        """Build the ArcGIS REST query URL."""
        base = f"{self.BASE_URL}/{self.MAPSERVER_ID}/query"
        params = (
            f"where=1%3D1"
            f"&outFields={self.FIELDS}"
            f"&returnGeometry=true"
            f"&outSR=4326"
            f"&f=json"
            f"&resultOffset={offset}"
            f"&resultRecordCount={limit}"
        )
        return f"{base}?{params}"

    async def _fetch_total_records(self) -> int:
        """Query the API for total record count."""
        url = (
            f"{self.BASE_URL}/{self.MAPSERVER_ID}/query"
            f"?where=1%3D1&returnCountOnly=true&f=json"
        )

        if self._session is None:
            timeout = aiohttp.ClientTimeout(total=self.config.timeout_seconds)
            self._session = aiohttp.ClientSession(timeout=timeout)

        try:
            async with self._session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get("count", 0)
        except Exception as e:
            self._logger.error(f"Failed to get total count: {e}")

        return 0

    @staticmethod
    def _convert_esri_dates(attrs: dict[str, Any]) -> dict[str, Any]:
        """
        Convert ESRI timestamp fields to ISO date strings.

        ESRI dates are milliseconds since epoch.
        """
        for key, value in attrs.items():
            if isinstance(value, int) and value > 10_000_000_000:
                try:
                    dt = datetime.fromtimestamp(value / 1000, tz=timezone.utc)
                    attrs[key] = dt.strftime("%Y-%m-%d")
                except (ValueError, OSError):
                    pass
        return attrs

    def run(self, *, max_records: int | None = None) -> ScraperResult:
        """
        Execute the full scraping pipeline with concurrent batch fetching.

        Overrides base class to enable parallel fetching via asyncio.gather(),
        using the configured max_workers for concurrency control.
        """
        loop = self._get_or_create_event_loop()
        return loop.run_until_complete(self.run_async(max_records=max_records))

    async def run_async(self, *, max_records: int | None = None) -> ScraperResult:
        """
        Execute the full scraping pipeline with concurrent batch fetching.

        This implementation fetches batches in parallel chunks using asyncio.gather()
        with semaphore-controlled concurrency, significantly improving performance
        for sources with many batches (e.g., dc_payments with 1500+ batches).

        Items are processed in chunks to avoid memory issues and allow early termination.
        """
        result = ScraperResult()
        self.metadata.start()
        done = False

        try:
            self.setup()

            # Phase 1: Discover all items
            items = []
            async for item in self.discover_async():
                items.append(item)
                result.discovered_count += 1

            if not items:
                self._logger.warning("No items discovered")
                from ...schemas.metadata import RunStatus
                self.metadata.complete(RunStatus.COMPLETED)
                return result

            # Process items in chunks to avoid memory issues and allow progress logging
            # Use smaller chunks for better concurrency control
            chunk_size = self.config.max_workers * 2  # 2 rounds per chunk
            total_chunks = (len(items) + chunk_size - 1) // chunk_size
            self._logger.info(
                f"Fetching {len(items)} batches in {total_chunks} chunks "
                f"with {self.config.max_workers} workers"
            )

            for chunk_idx in range(0, len(items), chunk_size):
                if done:
                    break

                chunk = items[chunk_idx:chunk_idx + chunk_size]
                chunk_num = chunk_idx // chunk_size + 1

                # Phase 2: Fetch chunk concurrently
                payloads = await asyncio.gather(
                    *[self.fetch_async(item) for item in chunk],
                    return_exceptions=True,
                )

                # Phase 3: Process fetched payloads
                for item, payload in zip(chunk, payloads):
                    if isinstance(payload, Exception):
                        self._logger.error(
                            f"Fetch exception for offset {item.get('offset', '?')}: {payload}"
                        )
                        continue
                    if payload is None:
                        continue

                    result.add_raw(payload)

                    for data in self.parse(payload):
                        result.parsed_count += 1

                        is_valid, errors = self.validate(data)
                        if not is_valid:
                            result.quarantine(
                                data=data,
                                error_type="validation_error",
                                error_message="; ".join(errors),
                                lineage=self._make_lineage(payload.lineage.source_url),
                            )
                            continue

                        try:
                            record = self.normalize(data)
                        except Exception as e:
                            result.quarantine(
                                data=data,
                                error_type="normalization_error",
                                error_message=str(e),
                                lineage=self._make_lineage(payload.lineage.source_url),
                            )
                            continue

                        if not self.deduplicate(record):
                            result.deduplicated_count += 1
                            continue

                        record = self.transform(record)
                        result.add_record(record)

                        if max_records and result.validated_count >= max_records:
                            done = True
                            break

                    if done:
                        break

                # Flush to storage periodically to avoid OOM for large scrapers
                self._flush_to_storage(result)

                # Log progress
                if chunk_num % 5 == 0 or chunk_num == total_chunks:
                    self._logger.info(
                        f"Progress: {chunk_num}/{total_chunks} chunks, "
                        f"{result.validated_count} records processed"
                    )

            # Force flush any remaining records
            self._flush_to_storage(result, force=True)

            from ...schemas.metadata import RunStatus
            self.metadata.complete(RunStatus.COMPLETED)

        except Exception as e:
            self._logger.exception(f"Scraper failed: {e}")
            from ...schemas.metadata import RunStatus
            self.metadata.complete(RunStatus.FAILED)
            raise

        finally:
            # Clean up aiohttp session
            if self._session:
                await self._session.close()
                self._session = None
            self.teardown()

        return result
