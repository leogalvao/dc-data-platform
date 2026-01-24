"""
Bronze layer storage for raw payloads.

Stores raw HTTP responses (HTML, JSON, CSV, etc.) with full
metadata for reproducibility and debugging.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import UUID

from ..schemas.base import RawPayload

logger = logging.getLogger(__name__)


try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False
    logger.warning("PyArrow not installed. Bronze layer will use JSON fallback.")


from .writer import StorageConfig, StorageWriter  # noqa: E402


class BronzeWriter(StorageWriter):
    """
    Writer for the Bronze layer (raw payloads).

    Stores raw content with metadata for:
    - Debugging parse/validation issues
    - Reprocessing with updated schemas
    - Audit trail and data lineage
    """

    def __init__(self, config: StorageConfig):
        super().__init__(config)
        self._base_path = config.bronze_path

    def write(
        self,
        records: Sequence[RawPayload],
        *,
        run_id: UUID,
        source_name: str,
    ) -> int:
        """
        Write raw payloads to bronze storage.

        Args:
            records: RawPayload objects to write
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

        # Batch records
        for batch_num, batch_start in enumerate(
            range(0, len(records), self.config.batch_size)
        ):
            batch = records[batch_start : batch_start + self.config.batch_size]

            if PYARROW_AVAILABLE and self.config.file_format == "parquet":
                written += self._write_parquet(batch, partition_path, run_id, batch_num)
            else:
                written += self._write_json(batch, partition_path, run_id, batch_num)

        return written

    def _write_parquet(
        self,
        records: Sequence[RawPayload],
        path: Path,
        run_id: UUID,
        batch_num: int,
    ) -> int:
        """Write batch as Parquet file."""
        # Convert to Arrow-friendly format
        data = []
        for r in records:
            data.append({
                "payload_id": str(r.payload_id),
                "content_type": r.content_type,
                "content": r.content,  # bytes
                "content_hash": r.content_hash,
                "source_name": r.lineage.source_name,
                "source_url": r.lineage.source_url,
                "scraped_at": r.lineage.scraped_at.isoformat(),
                "run_id": str(r.lineage.run_id),
                "fetch_status_code": r.fetch_status_code,
                "fetch_duration_ms": r.fetch_duration_ms,
            })

        # Create Arrow table
        table = pa.Table.from_pylist(data)

        # Write to Parquet
        filename = self.generate_filename(run_id, batch_num)
        filepath = path / filename

        pq.write_table(
            table,
            filepath,
            compression=self.config.compression,
        )

        self._logger.debug(f"Wrote {len(records)} records to {filepath}")
        return len(records)

    def _write_json(
        self,
        records: Sequence[RawPayload],
        path: Path,
        run_id: UUID,
        batch_num: int,
    ) -> int:
        """Write batch as JSON file (fallback)."""
        import base64

        data = []
        for r in records:
            data.append({
                "payload_id": str(r.payload_id),
                "content_type": r.content_type,
                "content_base64": base64.b64encode(r.content).decode("ascii"),
                "content_hash": r.content_hash,
                "lineage": r.lineage.model_dump(mode="json"),
                "fetch_status_code": r.fetch_status_code,
                "fetch_duration_ms": r.fetch_duration_ms,
            })

        filename = f"{self.generate_filename(run_id, batch_num).rsplit('.', 1)[0]}.json"
        filepath = path / filename

        with open(filepath, "w") as f:
            json.dump(data, f)

        self._logger.debug(f"Wrote {len(records)} records to {filepath}")
        return len(records)

    def read(
        self,
        *,
        source_name: str | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """
        Read raw payloads from bronze storage.

        Args:
            source_name: Filter by source
            start_date: Filter by start date
            end_date: Filter by end date
            limit: Maximum records to return

        Returns:
            List of payload dictionaries
        """
        results = []

        # Find matching partition paths
        search_path = self._base_path
        if source_name and self.config.partition_by_source:
            search_path = search_path / f"source={source_name}"

        if not search_path.exists():
            return results

        # Read files
        pattern = "*.parquet" if PYARROW_AVAILABLE else "*.json"
        for filepath in sorted(search_path.rglob(pattern)):
            # Filter by date partition if needed
            if start_date or end_date:
                # Extract date from partition path
                date_match = None
                for part in filepath.parts:
                    if part.startswith("date="):
                        date_match = part.split("=")[1]
                        break

                if date_match:
                    file_date = datetime.strptime(date_match, self.config.date_partition_format)
                    if start_date and file_date < start_date:
                        continue
                    if end_date and file_date > end_date:
                        continue

            # Read file
            if filepath.suffix == ".parquet" and PYARROW_AVAILABLE:
                table = pq.read_table(filepath)
                batch_data = table.to_pylist()
            else:
                with open(filepath) as f:
                    batch_data = json.load(f)

            results.extend(batch_data)

            if limit and len(results) >= limit:
                results = results[:limit]
                break

        return results
