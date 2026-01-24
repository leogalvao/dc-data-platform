"""
Base scraper abstract class defining the universal interface.

All source-specific adapters implement this interface, enabling
unified orchestration, storage, and observability.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, TypeVar
from uuid import UUID, uuid4

from ..schemas.base import BaseRecord, LineageInfo, QuarantineRecord, RawPayload
from ..schemas.metadata import Checkpoint, RunMetadata, RunReport, RunStatus, SourceConfig

if TYPE_CHECKING:
    from ..storage.writer import UnifiedStorageWriter

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseRecord)


@dataclass
class ScraperResult:
    """
    Result container for a scraping operation.

    Collects successful records, quarantined records, and metrics.
    """

    records: list[BaseRecord] = field(default_factory=list)
    raw_payloads: list[RawPayload] = field(default_factory=list)
    quarantined: list[QuarantineRecord] = field(default_factory=list)

    # Counters
    discovered_count: int = 0
    fetched_count: int = 0
    parsed_count: int = 0
    validated_count: int = 0
    deduplicated_count: int = 0

    # Errors
    fetch_errors: list[dict[str, Any]] = field(default_factory=list)
    parse_errors: list[dict[str, Any]] = field(default_factory=list)
    validation_errors: list[dict[str, Any]] = field(default_factory=list)

    def add_record(self, record: BaseRecord) -> None:
        """Add a successfully processed record."""
        self.records.append(record)
        self.validated_count += 1

    def add_raw(self, payload: RawPayload) -> None:
        """Add a raw payload for bronze storage."""
        self.raw_payloads.append(payload)
        self.fetched_count += 1

    def quarantine(
        self,
        data: dict[str, Any],
        error_type: str,
        error_message: str,
        lineage: LineageInfo,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Send a record to quarantine."""
        self.quarantined.append(
            QuarantineRecord(
                original_data=data,
                error_type=error_type,
                error_message=error_message,
                error_details=details or {},
                lineage=lineage,
            )
        )

    def merge(self, other: ScraperResult) -> None:
        """Merge another result into this one."""
        self.records.extend(other.records)
        self.raw_payloads.extend(other.raw_payloads)
        self.quarantined.extend(other.quarantined)
        self.discovered_count += other.discovered_count
        self.fetched_count += other.fetched_count
        self.parsed_count += other.parsed_count
        self.validated_count += other.validated_count
        self.deduplicated_count += other.deduplicated_count
        self.fetch_errors.extend(other.fetch_errors)
        self.parse_errors.extend(other.parse_errors)
        self.validation_errors.extend(other.validation_errors)


class BaseScraper(ABC):
    """
    Abstract base class for all scrapers.

    Defines the standard pipeline:
        discover() -> fetch() -> parse() -> validate() -> normalize() -> write()

    Adapters wrap existing scraper logic and map it to this interface
    with minimal changes to the underlying implementation.

    Lifecycle:
        1. __init__ with SourceConfig
        2. setup() for any initialization
        3. run() executes the full pipeline
        4. teardown() for cleanup
    """

    # Class-level metadata (override in subclasses)
    SOURCE_NAME: str = "base"
    SOURCE_VERSION: str = "1.0.0"
    SUPPORTED_RECORD_TYPES: list[type[BaseRecord]] = []

    def __init__(
        self,
        config: SourceConfig,
        run_id: UUID | None = None,
        checkpoint: Checkpoint | None = None,
    ):
        """
        Initialize the scraper.

        Args:
            config: Source configuration
            run_id: Optional run ID (generated if not provided)
            checkpoint: Optional checkpoint for incremental runs
        """
        self.config = config
        self.run_id = run_id or uuid4()
        self.checkpoint = checkpoint
        self.metadata = RunMetadata(
            run_id=self.run_id,
            source_name=config.name,
            config_snapshot=config.model_dump(),
        )
        self._logger = logging.getLogger(f"{__name__}.{config.name}")

        # Incremental flush support for memory-efficient large scrapes
        self._storage: UnifiedStorageWriter | None = None
        self._flush_batch_size: int = 50_000
        self._flushed_counts: dict[str, int] = {"bronze": 0, "silver": 0, "quarantine": 0}

    # =========================================================================
    # Lifecycle Methods
    # =========================================================================

    def setup(self) -> None:
        """
        Initialize resources before scraping.

        Override to set up HTTP clients, authenticate, etc.
        Called once before run().
        """
        self._logger.info(f"Setting up scraper for {self.config.name}")

    def teardown(self) -> None:
        """
        Clean up resources after scraping.

        Override to close connections, flush buffers, etc.
        Called after run() completes (success or failure).
        """
        self._logger.info(f"Tearing down scraper for {self.config.name}")

    # =========================================================================
    # Pipeline Methods (implement in adapters)
    # =========================================================================

    @abstractmethod
    def discover(self) -> Iterator[dict[str, Any]]:
        """
        Discover items to scrape.

        Yields discovery items (URLs, IDs, parameters) that will be
        passed to fetch(). For paginated APIs, yield one item per page
        or record batch.

        Yields:
            Dict with information needed to fetch the item.
            Typically includes: url, params, offset, page, etc.

        Example:
            yield {"url": "https://api.example.com/data", "page": 1}
            yield {"url": "https://api.example.com/data", "page": 2}
        """
        ...

    @abstractmethod
    def fetch(self, item: dict[str, Any]) -> RawPayload | None:
        """
        Fetch raw data for a discovered item.

        Args:
            item: Discovery item from discover()

        Returns:
            RawPayload with the fetched content, or None if fetch failed.
            Failed fetches should be logged/tracked, not raised.
        """
        ...

    @abstractmethod
    def parse(self, payload: RawPayload) -> Iterator[dict[str, Any]]:
        """
        Parse raw payload into record dictionaries.

        Args:
            payload: Raw payload from fetch()

        Yields:
            Dict for each record found in the payload.
            These will be passed to validate().
        """
        ...

    @abstractmethod
    def validate(self, data: dict[str, Any]) -> tuple[bool, list[str]]:
        """
        Validate a parsed record.

        Args:
            data: Parsed record dict from parse()

        Returns:
            Tuple of (is_valid, list_of_errors).
            Records with is_valid=False are quarantined.
        """
        ...

    @abstractmethod
    def normalize(self, data: dict[str, Any]) -> BaseRecord:
        """
        Normalize validated data into a canonical record.

        Args:
            data: Validated record dict

        Returns:
            Pydantic model instance with lineage attached.
        """
        ...

    # =========================================================================
    # Optional Override Methods
    # =========================================================================

    def deduplicate(self, record: BaseRecord) -> bool:
        """
        Check if record is a duplicate.

        Override to implement custom deduplication logic.
        Default uses checkpoint seen_hashes if available.

        Args:
            record: Normalized record

        Returns:
            True if record is new, False if duplicate.
        """
        if self.checkpoint is None:
            return True

        record_hash = record.lineage.raw_hash
        source_id = getattr(record, "source_record_id", None)
        return self.checkpoint.mark_seen(record_hash, source_id)

    def transform(self, record: BaseRecord) -> BaseRecord:
        """
        Apply additional transformations to normalized record.

        Override to add computed fields, enrichments, etc.
        Default returns record unchanged.

        Args:
            record: Normalized record

        Returns:
            Transformed record
        """
        return record

    # =========================================================================
    # Async Variants (override if source uses async)
    # =========================================================================

    async def discover_async(self) -> AsyncIterator[dict[str, Any]]:
        """Async version of discover(). Override for async sources."""
        for item in self.discover():
            yield item

    async def fetch_async(self, item: dict[str, Any]) -> RawPayload | None:
        """Async version of fetch(). Override for async sources."""
        return self.fetch(item)

    async def parse_async(self, payload: RawPayload) -> AsyncIterator[dict[str, Any]]:
        """Async version of parse()."""
        for data in self.parse(payload):
            yield data

    # =========================================================================
    # Main Entry Points
    # =========================================================================

    def run(self, *, max_records: int | None = None) -> ScraperResult:
        """
        Execute the full scraping pipeline (synchronous).

        Args:
            max_records: Optional limit on records to process

        Returns:
            ScraperResult with all records and metrics
        """
        result = ScraperResult()
        self.metadata.start()

        try:
            self.setup()

            for item in self.discover():
                result.discovered_count += 1

                # Fetch
                payload = self.fetch(item)
                if payload is None:
                    continue
                result.add_raw(payload)

                # Parse each record in payload
                for data in self.parse(payload):
                    result.parsed_count += 1

                    # Validate
                    is_valid, errors = self.validate(data)
                    if not is_valid:
                        result.quarantine(
                            data=data,
                            error_type="validation_error",
                            error_message="; ".join(errors),
                            lineage=self._make_lineage(payload.lineage.source_url),
                        )
                        result.validation_errors.append(
                            {"data_sample": str(data)[:200], "errors": errors}
                        )
                        continue

                    # Normalize
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

                    # Deduplicate
                    if not self.deduplicate(record):
                        result.deduplicated_count += 1
                        continue

                    # Transform
                    record = self.transform(record)

                    # Add to result
                    result.add_record(record)

                    # Check limit
                    if max_records and result.validated_count >= max_records:
                        self._logger.info(f"Reached max_records limit: {max_records}")
                        break

                if max_records and result.validated_count >= max_records:
                    break

            self.metadata.complete(RunStatus.COMPLETED)

        except Exception as e:
            self._logger.exception(f"Scraper failed: {e}")
            self.metadata.complete(RunStatus.FAILED)
            raise

        finally:
            self.teardown()

        return result

    async def run_async(self, *, max_records: int | None = None) -> ScraperResult:
        """
        Execute the full scraping pipeline (asynchronous).

        Args:
            max_records: Optional limit on records to process

        Returns:
            ScraperResult with all records and metrics
        """
        result = ScraperResult()
        self.metadata.start()

        try:
            self.setup()

            async for item in self.discover_async():
                result.discovered_count += 1

                # Fetch
                payload = await self.fetch_async(item)
                if payload is None:
                    continue
                result.add_raw(payload)

                # Parse each record in payload
                async for data in self.parse_async(payload):
                    result.parsed_count += 1

                    # Validate
                    is_valid, errors = self.validate(data)
                    if not is_valid:
                        result.quarantine(
                            data=data,
                            error_type="validation_error",
                            error_message="; ".join(errors),
                            lineage=self._make_lineage(payload.lineage.source_url),
                        )
                        continue

                    # Normalize
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

                    # Deduplicate
                    if not self.deduplicate(record):
                        result.deduplicated_count += 1
                        continue

                    # Transform
                    record = self.transform(record)

                    # Add to result
                    result.add_record(record)

                    if max_records and result.validated_count >= max_records:
                        break

                if max_records and result.validated_count >= max_records:
                    break

            self.metadata.complete(RunStatus.COMPLETED)

        except Exception as e:
            self._logger.exception(f"Scraper failed: {e}")
            self.metadata.complete(RunStatus.FAILED)
            raise

        finally:
            self.teardown()

        return result

    # =========================================================================
    # Helper Methods
    # =========================================================================

    def _flush_to_storage(self, result: ScraperResult, *, force: bool = False) -> None:
        """
        Flush accumulated records to storage and clear memory.

        For large scrapers (e.g., dc_payments with 1.5M+ records), this prevents
        OOM by writing records incrementally instead of holding all in memory.

        Args:
            result: ScraperResult with records to flush
            force: If True, flush even if below batch threshold
        """
        if self._storage is None:
            return

        should_flush = force or len(result.records) >= self._flush_batch_size
        if not should_flush:
            return

        # Skip if nothing to flush
        if not result.raw_payloads and not result.records and not result.quarantined:
            return

        self._logger.info(
            f"Flushing batch: {len(result.records)} records, "
            f"{len(result.raw_payloads)} raw payloads, "
            f"{len(result.quarantined)} quarantined"
        )

        counts = self._storage.write_run_results(
            run_id=self.metadata.run_id,
            source_name=self.config.name,
            raw_payloads=result.raw_payloads,
            records=result.records,
            quarantined=result.quarantined,
        )

        # Track flushed counts
        self._flushed_counts["bronze"] += counts["bronze"]
        self._flushed_counts["silver"] += counts["silver"]
        self._flushed_counts["quarantine"] += counts["quarantine"]

        # Clear memory
        result.raw_payloads.clear()
        result.records.clear()
        result.quarantined.clear()

    def _make_lineage(self, source_url: str | None = None) -> LineageInfo:
        """Create a LineageInfo with current run context."""
        return LineageInfo(
            source_name=self.config.name,
            source_url=source_url or self.config.base_url,
            run_id=self.run_id,
            schema_version=self.SOURCE_VERSION,
            adapter_version=self.SOURCE_VERSION,
        )

    def generate_report(self, result: ScraperResult) -> RunReport:
        """Generate a run report from results."""
        return RunReport(
            run_id=self.run_id,
            source_name=self.config.name,
            metadata=self.metadata,
            records_discovered=result.discovered_count,
            records_fetched=result.fetched_count,
            records_parsed=result.parsed_count,
            records_validated=result.validated_count,
            records_quarantined=len(result.quarantined),
            records_deduplicated=result.deduplicated_count,
            errors_fetch=len(result.fetch_errors),
            errors_parse=len(result.parse_errors),
            errors_validation=len(result.validation_errors),
            error_samples=(
                result.fetch_errors[:3]
                + result.parse_errors[:3]
                + result.validation_errors[:4]
            ),
        )
