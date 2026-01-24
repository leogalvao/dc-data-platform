"""
Storage writer interface and configuration.

Provides a unified interface for writing data to the lakehouse
with support for Bronze (raw) and Silver (normalized) layers.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

from ..schemas.base import BaseRecord, QuarantineRecord, RawPayload
from ..schemas.metadata import RunReport

logger = logging.getLogger(__name__)


@dataclass
class StorageConfig:
    """Configuration for lakehouse storage."""

    # Root paths
    lakehouse_root: Path = field(default_factory=lambda: Path("data"))
    bronze_path: Path | None = None  # Defaults to {root}/bronze
    silver_path: Path | None = None  # Defaults to {root}/silver
    quarantine_path: Path | None = None  # Defaults to {root}/quarantine
    checkpoint_path: Path | None = None  # Defaults to {root}/checkpoints

    # Format settings
    file_format: str = "parquet"  # parquet, delta, json
    compression: str = "snappy"  # snappy, gzip, zstd, none

    # Partitioning
    partition_by_source: bool = True
    partition_by_date: bool = True
    date_partition_format: str = "%Y-%m-%d"  # YYYY-MM-DD

    # Write behavior
    write_mode: str = "append"  # append, overwrite, merge
    batch_size: int = 10000  # Records per file
    enable_dedup: bool = True  # Check for duplicates before writing

    # Delta Lake specific (if using delta format)
    enable_delta: bool = False
    delta_optimize_interval: int = 10  # Runs between optimize
    delta_vacuum_hours: int = 168  # 7 days

    def __post_init__(self):
        """Set default paths based on root."""
        if self.bronze_path is None:
            self.bronze_path = self.lakehouse_root / "bronze"
        if self.silver_path is None:
            self.silver_path = self.lakehouse_root / "silver"
        if self.quarantine_path is None:
            self.quarantine_path = self.lakehouse_root / "quarantine"
        if self.checkpoint_path is None:
            self.checkpoint_path = self.lakehouse_root / "checkpoints"

        # Ensure directories exist
        for path in [self.bronze_path, self.silver_path, self.quarantine_path, self.checkpoint_path]:
            path.mkdir(parents=True, exist_ok=True)


class StorageWriter(ABC):
    """
    Abstract base class for storage writers.

    Defines the interface for writing records to storage.
    Implementations handle specific formats (Parquet, Delta, etc.).
    """

    def __init__(self, config: StorageConfig):
        """
        Initialize the storage writer.

        Args:
            config: Storage configuration
        """
        self.config = config
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @abstractmethod
    def write(
        self,
        records: Sequence[BaseRecord | RawPayload | QuarantineRecord],
        *,
        run_id: UUID,
        source_name: str,
    ) -> int:
        """
        Write records to storage.

        Args:
            records: Records to write
            run_id: Run identifier for tracing
            source_name: Source identifier for partitioning

        Returns:
            Number of records written
        """
        ...

    @abstractmethod
    def read(
        self,
        *,
        source_name: str | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """
        Read records from storage.

        Args:
            source_name: Filter by source
            start_date: Filter by start date
            end_date: Filter by end date
            limit: Maximum records to return

        Returns:
            List of record dictionaries
        """
        ...

    def build_partition_path(
        self,
        base_path: Path,
        source_name: str,
        date: datetime | None = None,
    ) -> Path:
        """
        Build the partition path based on configuration.

        Args:
            base_path: Base storage path
            source_name: Source identifier
            date: Date for partitioning (uses now if None)

        Returns:
            Full partition path
        """
        path = base_path

        if self.config.partition_by_source:
            path = path / f"source={source_name}"

        if self.config.partition_by_date:
            if date is None:
                date = datetime.now(timezone.utc)
            date_str = date.strftime(self.config.date_partition_format)
            path = path / f"date={date_str}"

        return path

    def generate_filename(self, run_id: UUID, batch_num: int = 0) -> str:
        """
        Generate a unique filename for a batch.

        Args:
            run_id: Run identifier
            batch_num: Batch number within the run

        Returns:
            Filename with appropriate extension
        """
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        ext = "parquet" if self.config.file_format in ("parquet", "delta") else self.config.file_format
        return f"{timestamp}_{run_id}_{batch_num:04d}.{ext}"


class UnifiedStorageWriter:
    """
    Unified writer that coordinates Bronze, Silver, and Quarantine layers.

    Provides a single interface for the orchestrator to write
    all types of records to appropriate storage locations.
    """

    def __init__(
        self,
        config: StorageConfig,
        bronze_writer: StorageWriter,
        silver_writer: StorageWriter,
        quarantine_writer: StorageWriter,
    ):
        """
        Initialize the unified writer.

        Args:
            config: Storage configuration
            bronze_writer: Writer for raw payloads
            silver_writer: Writer for normalized records
            quarantine_writer: Writer for bad records
        """
        self.config = config
        self.bronze = bronze_writer
        self.silver = silver_writer
        self.quarantine = quarantine_writer
        self._logger = logging.getLogger(__name__)

    def write_run_results(
        self,
        *,
        run_id: UUID,
        source_name: str,
        raw_payloads: Sequence[RawPayload],
        records: Sequence[BaseRecord],
        quarantined: Sequence[QuarantineRecord],
    ) -> dict[str, int]:
        """
        Write all results from a scraping run.

        Args:
            run_id: Run identifier
            source_name: Source identifier
            raw_payloads: Raw payloads for bronze layer
            records: Normalized records for silver layer
            quarantined: Bad records for quarantine

        Returns:
            Dict with counts: {bronze: N, silver: N, quarantine: N}
        """
        results = {"bronze": 0, "silver": 0, "quarantine": 0}

        # Write bronze layer
        if raw_payloads:
            try:
                results["bronze"] = self.bronze.write(
                    raw_payloads, run_id=run_id, source_name=source_name
                )
                self._logger.info(f"Wrote {results['bronze']} records to bronze layer")
            except Exception as e:
                self._logger.error(f"Bronze write failed: {e}")

        # Write silver layer
        if records:
            try:
                results["silver"] = self.silver.write(
                    records, run_id=run_id, source_name=source_name
                )
                self._logger.info(f"Wrote {results['silver']} records to silver layer")
            except Exception as e:
                self._logger.error(f"Silver write failed: {e}")

        # Write quarantine
        if quarantined:
            try:
                results["quarantine"] = self.quarantine.write(
                    quarantined, run_id=run_id, source_name=source_name
                )
                self._logger.warning(f"Quarantined {results['quarantine']} bad records")
            except Exception as e:
                self._logger.error(f"Quarantine write failed: {e}")

        return results

    def save_run_report(self, report: RunReport) -> Path:
        """
        Save a run report to the reports directory.

        Args:
            report: Run report to save

        Returns:
            Path to saved report
        """
        reports_dir = self.config.lakehouse_root / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)

        filename = f"{report.source_name}_{report.run_id}.json"
        path = reports_dir / filename

        import json
        with open(path, "w") as f:
            json.dump(report.model_dump(mode="json"), f, indent=2, default=str)

        self._logger.info(f"Saved run report to {path}")
        return path
