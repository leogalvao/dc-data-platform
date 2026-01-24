"""
Orchestrator for running multiple scrapers with worker pool.

Provides:
- Parallel execution of multiple scrapers
- Per-source rate limiting
- Centralized error handling
- Run scheduling and management
"""

from __future__ import annotations

import asyncio
import logging
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

from ..schemas.metadata import RunReport, RunStatus
from ..storage.bronze import BronzeWriter
from ..storage.checkpoint import CheckpointManager
from ..storage.quarantine import QuarantineWriter
from ..storage.silver import SilverWriter
from ..storage.writer import StorageConfig, UnifiedStorageWriter
from .rate_limiter import RateLimitConfig, RateLimiterPool
from .registry import ScraperRegistry

logger = logging.getLogger(__name__)


@dataclass
class OrchestratorConfig:
    """Configuration for the orchestrator."""

    # Parallelism
    max_concurrent_sources: int = 3
    use_processes: bool = False  # Use ProcessPool vs ThreadPool

    # Execution
    fail_fast: bool = False  # Stop all on first failure
    max_records_per_source: int | None = None

    # Storage
    storage_config: StorageConfig = field(default_factory=StorageConfig)

    # Rate limiting
    default_rate_limit: RateLimitConfig = field(default_factory=RateLimitConfig)

    # Timeouts
    source_timeout_seconds: int = 3600  # 1 hour per source

    # Retry
    retry_failed_sources: bool = True
    max_source_retries: int = 2


@dataclass
class SourceRunResult:
    """Result of running a single source."""

    source_name: str
    run_id: UUID
    status: RunStatus
    report: RunReport | None = None
    error: str | None = None
    duration_seconds: float = 0.0


class Orchestrator:
    """
    Orchestrates execution of multiple scrapers.

    Features:
    - Parallel execution with configurable worker pool
    - Per-source rate limiting via RateLimiterPool
    - Unified storage writing
    - Checkpoint management for resume
    - Comprehensive run reporting
    """

    def __init__(self, config: OrchestratorConfig | None = None):
        """
        Initialize the orchestrator.

        Args:
            config: Orchestrator configuration
        """
        self.config = config or OrchestratorConfig()
        self._logger = logging.getLogger(__name__)

        # Initialize components
        self._rate_limiters = RateLimiterPool(self.config.default_rate_limit)
        self._checkpoint_manager = CheckpointManager(
            self.config.storage_config.checkpoint_path
        )

        # Initialize storage
        self._storage = UnifiedStorageWriter(
            config=self.config.storage_config,
            bronze_writer=BronzeWriter(self.config.storage_config),
            silver_writer=SilverWriter(self.config.storage_config),
            quarantine_writer=QuarantineWriter(self.config.storage_config),
        )

        # Track active runs
        self._active_runs: dict[str, UUID] = {}
        self._results: list[SourceRunResult] = []

    def run_all(
        self,
        sources: list[str] | None = None,
        *,
        incremental: bool = False,
    ) -> list[SourceRunResult]:
        """
        Run all registered (or specified) scrapers.

        Args:
            sources: Specific sources to run (None = all enabled)
            incremental: Use checkpoints for incremental load

        Returns:
            List of run results for each source
        """
        # Get sources to run
        if sources is None:
            sources = ScraperRegistry.list_enabled()

        if not sources:
            self._logger.warning("No sources to run")
            return []

        self._logger.info(f"Starting orchestrated run for {len(sources)} sources: {sources}")

        # Run sources
        if self.config.max_concurrent_sources == 1:
            # Sequential execution
            results = [self._run_source(s, incremental) for s in sources]
        else:
            # Parallel execution
            results = self._run_parallel(sources, incremental)

        self._results = results

        # Summary logging
        succeeded = sum(1 for r in results if r.status == RunStatus.COMPLETED)
        failed = sum(1 for r in results if r.status == RunStatus.FAILED)
        self._logger.info(
            f"Orchestrated run complete: {succeeded} succeeded, {failed} failed"
        )

        return results

    async def run_all_async(
        self,
        sources: list[str] | None = None,
        *,
        incremental: bool = False,
    ) -> list[SourceRunResult]:
        """
        Run all scrapers asynchronously.

        Args:
            sources: Specific sources to run
            incremental: Use checkpoints

        Returns:
            List of run results
        """
        if sources is None:
            sources = ScraperRegistry.list_enabled()

        if not sources:
            return []

        self._logger.info(f"Starting async run for {len(sources)} sources")

        # Create semaphore for concurrency control
        semaphore = asyncio.Semaphore(self.config.max_concurrent_sources)

        async def run_with_semaphore(source: str) -> SourceRunResult:
            async with semaphore:
                return await self._run_source_async(source, incremental)

        # Run all sources concurrently (up to semaphore limit)
        tasks = [run_with_semaphore(s) for s in sources]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Handle any exceptions
        final_results = []
        for source, result in zip(sources, results):
            if isinstance(result, Exception):
                final_results.append(
                    SourceRunResult(
                        source_name=source,
                        run_id=uuid4(),
                        status=RunStatus.FAILED,
                        error=str(result),
                    )
                )
            else:
                final_results.append(result)

        self._results = final_results
        return final_results

    def _run_parallel(
        self,
        sources: list[str],
        incremental: bool,
    ) -> list[SourceRunResult]:
        """Run sources in parallel using thread/process pool."""
        executor_class = (
            ProcessPoolExecutor if self.config.use_processes else ThreadPoolExecutor
        )

        with executor_class(max_workers=self.config.max_concurrent_sources) as executor:
            futures = {
                executor.submit(self._run_source, s, incremental): s
                for s in sources
            }

            results = []
            for future in futures:
                source = futures[future]
                try:
                    result = future.result(timeout=self.config.source_timeout_seconds)
                    results.append(result)
                except Exception as e:
                    self._logger.exception(f"Source {source} failed: {e}")
                    results.append(
                        SourceRunResult(
                            source_name=source,
                            run_id=uuid4(),
                            status=RunStatus.FAILED,
                            error=str(e),
                        )
                    )

                    if self.config.fail_fast:
                        # Cancel remaining futures
                        for f in futures:
                            f.cancel()
                        break

            return results

    def _run_source(self, source_name: str, incremental: bool) -> SourceRunResult:
        """
        Run a single source scraper.

        Args:
            source_name: Source to run
            incremental: Use checkpoint

        Returns:
            SourceRunResult
        """
        run_id = uuid4()
        self._active_runs[source_name] = run_id
        start_time = datetime.now(timezone.utc)

        self._logger.info(f"Starting {source_name} (run_id={run_id})")

        try:
            # Get checkpoint if incremental
            checkpoint = None
            if incremental:
                checkpoint = self._checkpoint_manager.load(source_name)

            # Create scraper instance
            scraper = ScraperRegistry.create(
                source_name,
                run_id=run_id,
                checkpoint=checkpoint,
            )

            # Pass storage reference for incremental flushing (reduces memory for large scrapers)
            scraper._storage = self._storage

            # Run scraper
            result = scraper.run(max_records=self.config.max_records_per_source)

            # Start with counts from incremental flushes during scraping
            write_counts = {
                "bronze": scraper._flushed_counts["bronze"],
                "silver": scraper._flushed_counts["silver"],
                "quarantine": scraper._flushed_counts["quarantine"],
            }

            # Write any remaining unflushed records
            if result.raw_payloads or result.records or result.quarantined:
                remaining = self._storage.write_run_results(
                    run_id=run_id,
                    source_name=source_name,
                    raw_payloads=result.raw_payloads,
                    records=result.records,
                    quarantined=result.quarantined,
                )
                write_counts["bronze"] += remaining["bronze"]
                write_counts["silver"] += remaining["silver"]
                write_counts["quarantine"] += remaining["quarantine"]

            # Generate report
            report = scraper.generate_report(result)
            report.records_written_bronze = write_counts["bronze"]
            report.records_written_silver = write_counts["silver"]

            # Save report
            self._storage.save_run_report(report)

            # Update checkpoint
            if result.records:
                latest_scraped = max(
                    r.lineage.scraped_at for r in result.records
                )
                self._checkpoint_manager.update_after_run(
                    source_name,
                    run_id,
                    high_watermark=latest_scraped,
                )

            duration = (datetime.now(timezone.utc) - start_time).total_seconds()

            return SourceRunResult(
                source_name=source_name,
                run_id=run_id,
                status=RunStatus.COMPLETED,
                report=report,
                duration_seconds=duration,
            )

        except Exception as e:
            self._logger.exception(f"Source {source_name} failed: {e}")
            duration = (datetime.now(timezone.utc) - start_time).total_seconds()

            return SourceRunResult(
                source_name=source_name,
                run_id=run_id,
                status=RunStatus.FAILED,
                error=str(e),
                duration_seconds=duration,
            )

        finally:
            del self._active_runs[source_name]

    async def _run_source_async(
        self,
        source_name: str,
        incremental: bool,
    ) -> SourceRunResult:
        """
        Run a single source scraper asynchronously.

        Args:
            source_name: Source to run
            incremental: Use checkpoint

        Returns:
            SourceRunResult
        """
        run_id = uuid4()
        self._active_runs[source_name] = run_id
        start_time = datetime.now(timezone.utc)

        self._logger.info(f"Starting async {source_name} (run_id={run_id})")

        try:
            checkpoint = None
            if incremental:
                checkpoint = self._checkpoint_manager.load(source_name)

            scraper = ScraperRegistry.create(
                source_name,
                run_id=run_id,
                checkpoint=checkpoint,
            )

            # Pass storage reference for incremental flushing (reduces memory for large scrapers)
            scraper._storage = self._storage

            # Run async if supported
            if hasattr(scraper, "run_async"):
                result = await scraper.run_async(
                    max_records=self.config.max_records_per_source
                )
            else:
                # Fallback to sync in thread
                loop = asyncio.get_event_loop()
                result = await loop.run_in_executor(
                    None,
                    lambda: scraper.run(max_records=self.config.max_records_per_source),
                )

            # Start with counts from incremental flushes during scraping
            write_counts = {
                "bronze": scraper._flushed_counts["bronze"],
                "silver": scraper._flushed_counts["silver"],
                "quarantine": scraper._flushed_counts["quarantine"],
            }

            # Write any remaining unflushed records
            if result.raw_payloads or result.records or result.quarantined:
                remaining = self._storage.write_run_results(
                    run_id=run_id,
                    source_name=source_name,
                    raw_payloads=result.raw_payloads,
                    records=result.records,
                    quarantined=result.quarantined,
                )
                write_counts["bronze"] += remaining["bronze"]
                write_counts["silver"] += remaining["silver"]
                write_counts["quarantine"] += remaining["quarantine"]

            report = scraper.generate_report(result)
            report.records_written_bronze = write_counts["bronze"]
            report.records_written_silver = write_counts["silver"]

            self._storage.save_run_report(report)

            if result.records:
                latest_scraped = max(r.lineage.scraped_at for r in result.records)
                self._checkpoint_manager.update_after_run(
                    source_name, run_id, high_watermark=latest_scraped
                )

            duration = (datetime.now(timezone.utc) - start_time).total_seconds()

            return SourceRunResult(
                source_name=source_name,
                run_id=run_id,
                status=RunStatus.COMPLETED,
                report=report,
                duration_seconds=duration,
            )

        except Exception as e:
            self._logger.exception(f"Async source {source_name} failed: {e}")
            duration = (datetime.now(timezone.utc) - start_time).total_seconds()

            return SourceRunResult(
                source_name=source_name,
                run_id=run_id,
                status=RunStatus.FAILED,
                error=str(e),
                duration_seconds=duration,
            )

        finally:
            if source_name in self._active_runs:
                del self._active_runs[source_name]

    def get_run_summary(self) -> dict[str, Any]:
        """Get summary of last orchestrated run."""
        if not self._results:
            return {"status": "no_runs"}

        return {
            "total_sources": len(self._results),
            "completed": sum(1 for r in self._results if r.status == RunStatus.COMPLETED),
            "failed": sum(1 for r in self._results if r.status == RunStatus.FAILED),
            "total_duration": sum(r.duration_seconds for r in self._results),
            "results": [
                {
                    "source": r.source_name,
                    "status": r.status.value,
                    "duration": r.duration_seconds,
                    "records": r.report.records_written_silver if r.report else 0,
                    "error": r.error,
                }
                for r in self._results
            ],
        }
