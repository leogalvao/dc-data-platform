"""
Quarantine storage for bad records.

Stores records that failed validation or parsing with
full error details for debugging and potential recovery.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

from ..schemas.base import QuarantineRecord

logger = logging.getLogger(__name__)

try:
    import pyarrow as pa  # noqa: F401 # Used for availability check
    import pyarrow.parquet as pq  # noqa: F401 # Used for availability check
    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False

from .writer import StorageConfig, StorageWriter  # noqa: E402


class QuarantineWriter(StorageWriter):
    """
    Writer for quarantine storage (bad records).

    Stores failed records with:
    - Original data that caused the failure
    - Error type and message
    - Full stack trace (if available)
    - Lineage for tracing back to source

    Records can be reviewed, fixed, and reprocessed.
    """

    def __init__(self, config: StorageConfig):
        super().__init__(config)
        self._base_path = config.quarantine_path

    def write(
        self,
        records: Sequence[QuarantineRecord],
        *,
        run_id: UUID,
        source_name: str,
    ) -> int:
        """
        Write quarantined records to storage.

        Args:
            records: QuarantineRecord objects to write
            run_id: Run identifier
            source_name: Source identifier

        Returns:
            Number of records written
        """
        if not records:
            return 0

        # Build partition path
        partition_path = self.build_partition_path(
            self._base_path, source_name
        )
        partition_path.mkdir(parents=True, exist_ok=True)

        written = 0

        # Always write as JSON for human readability
        # (quarantine records need to be inspected)
        for batch_num, batch_start in enumerate(
            range(0, len(records), self.config.batch_size)
        ):
            batch = records[batch_start : batch_start + self.config.batch_size]
            written += self._write_json(batch, partition_path, run_id, batch_num)

        return written

    def _write_json(
        self,
        records: Sequence[QuarantineRecord],
        path: Path,
        run_id: UUID,
        batch_num: int,
    ) -> int:
        """Write batch as JSON file with readable formatting."""
        data = []
        for r in records:
            data.append({
                "quarantine_id": str(r.quarantine_id),
                "error_type": r.error_type,
                "error_message": r.error_message,
                "error_details": r.error_details,
                "original_data": r.original_data,
                "lineage": r.lineage.model_dump(mode="json"),
                "quarantined_at": r.quarantined_at.isoformat(),
            })

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        filename = f"quarantine_{timestamp}_{run_id}_{batch_num:04d}.json"
        filepath = path / filename

        with open(filepath, "w") as f:
            json.dump(data, f, indent=2, default=str)

        self._logger.debug(f"Quarantined {len(records)} records to {filepath}")
        return len(records)

    def read(
        self,
        *,
        source_name: str | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        limit: int | None = None,
        error_type: str | None = None,
    ) -> list[dict[str, Any]]:
        """
        Read quarantined records.

        Args:
            source_name: Filter by source
            start_date: Filter by start date
            end_date: Filter by end date
            limit: Maximum records to return
            error_type: Filter by error type

        Returns:
            List of quarantine record dictionaries
        """
        results = []

        search_path = self._base_path
        if source_name and self.config.partition_by_source:
            search_path = search_path / f"source={source_name}"

        if not search_path.exists():
            return results

        for filepath in sorted(search_path.rglob("*.json")):
            with open(filepath) as f:
                batch_data = json.load(f)

            # Filter by error type if specified
            if error_type:
                batch_data = [r for r in batch_data if r.get("error_type") == error_type]

            results.extend(batch_data)

            if limit and len(results) >= limit:
                results = results[:limit]
                break

        return results

    def get_error_summary(self, source_name: str | None = None) -> dict[str, int]:
        """
        Get summary of errors by type.

        Args:
            source_name: Filter by source (or all sources)

        Returns:
            Dict of error_type -> count
        """
        records = self.read(source_name=source_name)
        summary = {}
        for r in records:
            error_type = r.get("error_type", "unknown")
            summary[error_type] = summary.get(error_type, 0) + 1
        return summary

    def requeue_records(
        self,
        quarantine_ids: list[str],
    ) -> list[dict[str, Any]]:
        """
        Extract records from quarantine for reprocessing.

        Args:
            quarantine_ids: IDs of records to requeue

        Returns:
            List of original_data dicts ready for reprocessing
        """
        all_records = self.read()
        id_set = set(quarantine_ids)
        return [
            r["original_data"]
            for r in all_records
            if r.get("quarantine_id") in id_set
        ]
