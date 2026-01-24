"""
Silver layer storage for normalized records.

Stores validated, normalized records in analysis-ready format
with support for incremental loads and deduplication.

Performance optimizations:
- Bounded dedup hash cache with configurable max size
- Automatic eviction of oldest hashes when limit exceeded
- Explicit PyArrow schemas to prevent null type inference
"""

from __future__ import annotations

import json
import logging
from collections import deque
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import UUID

from ..schemas.base import BaseRecord
from ..utils.arrow_schemas import get_schema_for_records

logger = logging.getLogger(__name__)

# Default maximum number of hashes to keep in memory for deduplication
DEFAULT_MAX_DEDUP_CACHE_SIZE = 100_000

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False
    logger.warning("PyArrow not installed. Silver layer will use JSON fallback.")

try:
    from deltalake import DeltaTable, write_deltalake
    DELTA_AVAILABLE = True
except ImportError:
    DELTA_AVAILABLE = False
    logger.info("Delta Lake not installed. Using Parquet for silver layer.")

from .writer import StorageConfig, StorageWriter  # noqa: E402


class SilverWriter(StorageWriter):
    """
    Writer for the Silver layer (normalized records).

    Features:
    - Schema enforcement via Pydantic models
    - Partitioning by source and date
    - Deduplication via raw_hash
    - Optional Delta Lake support for ACID transactions

    Performance: Uses bounded dedup cache with configurable max size
    to prevent unbounded memory growth during large scraping runs.
    """

    def __init__(self, config: StorageConfig, max_dedup_cache_size: int = DEFAULT_MAX_DEDUP_CACHE_SIZE):
        super().__init__(config)
        self._base_path = config.silver_path
        self._max_dedup_cache_size = max_dedup_cache_size
        # Use set for O(1) lookup and deque to track insertion order for eviction
        self._seen_hashes: set[str] = set()
        self._hash_order: deque[str] = deque()  # Track insertion order for FIFO eviction

    def _add_to_dedup_cache(self, hash_key: str) -> None:
        """Add hash to dedup cache with automatic eviction if limit exceeded."""
        if hash_key in self._seen_hashes:
            return

        # Evict oldest hashes if we're at capacity
        while len(self._seen_hashes) >= self._max_dedup_cache_size:
            oldest = self._hash_order.popleft()
            self._seen_hashes.discard(oldest)

        self._seen_hashes.add(hash_key)
        self._hash_order.append(hash_key)

    def clear_dedup_cache(self) -> None:
        """Clear the deduplication cache (useful between runs or for memory management)."""
        self._seen_hashes.clear()
        self._hash_order.clear()
        self._logger.debug("Cleared deduplication cache")

    def write(
        self,
        records: Sequence[BaseRecord],
        *,
        run_id: UUID,
        source_name: str,
    ) -> int:
        """
        Write normalized records to silver storage.

        Args:
            records: BaseRecord objects to write
            run_id: Run identifier
            source_name: Source identifier

        Returns:
            Number of records written (after dedup)
        """
        if not records:
            return 0

        # Deduplicate within batch using bounded cache
        unique_records = []
        for record in records:
            hash_key = record.lineage.raw_hash
            if hash_key and hash_key in self._seen_hashes:
                continue
            if hash_key:
                self._add_to_dedup_cache(hash_key)
            unique_records.append(record)

        if not unique_records:
            self._logger.info("All records were duplicates, nothing to write")
            return 0

        # Build partition path
        partition_path = self.build_partition_path(
            self._base_path, source_name
        )
        partition_path.mkdir(parents=True, exist_ok=True)

        written = 0

        # Use Delta if enabled and available
        if self.config.enable_delta and DELTA_AVAILABLE:
            written = self._write_delta(unique_records, source_name, run_id)
        elif PYARROW_AVAILABLE:
            for batch_num, batch_start in enumerate(
                range(0, len(unique_records), self.config.batch_size)
            ):
                batch = unique_records[batch_start : batch_start + self.config.batch_size]
                written += self._write_parquet(batch, partition_path, run_id, batch_num)
        else:
            for batch_num, batch_start in enumerate(
                range(0, len(unique_records), self.config.batch_size)
            ):
                batch = unique_records[batch_start : batch_start + self.config.batch_size]
                written += self._write_json(batch, partition_path, run_id, batch_num)

        return written

    def _write_delta(
        self,
        records: Sequence[BaseRecord],
        source_name: str,
        run_id: UUID,
    ) -> int:
        """Write records to Delta Lake table."""
        # Convert records to dicts suitable for Arrow
        data = [r.to_silver_dict() for r in records]

        # Get explicit schema to prevent null type inference
        schema = get_schema_for_records(records)

        # Create Arrow table with explicit schema if available
        if schema is not None:
            try:
                table = pa.Table.from_pylist(data, schema=schema)
            except (pa.ArrowInvalid, pa.ArrowTypeError) as e:
                # Fall back to inferred schema if explicit fails
                self._logger.warning(f"Explicit schema failed, using inference: {e}")
                table = pa.Table.from_pylist(data)
        else:
            table = pa.Table.from_pylist(data)

        # Delta table path (one per source)
        delta_path = str(self._base_path / source_name)

        # Write with merge if table exists
        if Path(delta_path).exists():
            write_deltalake(
                delta_path,
                table,
                mode="append",
                schema_mode="merge",
            )
        else:
            write_deltalake(
                delta_path,
                table,
                mode="overwrite",
                partition_by=["partition_source", "partition_date"] if self.config.partition_by_date else None,
            )

        self._logger.debug(f"Wrote {len(records)} records to Delta table {delta_path}")
        return len(records)

    def _write_parquet(
        self,
        records: Sequence[BaseRecord],
        path: Path,
        run_id: UUID,
        batch_num: int,
    ) -> int:
        """Write batch as Parquet file."""
        # Convert to flat dicts for Arrow
        data = [r.to_silver_dict() for r in records]

        # Get explicit schema to prevent null type inference
        schema = get_schema_for_records(records)

        # Create Arrow table with explicit schema if available
        if schema is not None:
            try:
                table = pa.Table.from_pylist(data, schema=schema)
            except (pa.ArrowInvalid, pa.ArrowTypeError) as e:
                # Fall back to inferred schema if explicit fails
                self._logger.warning(f"Explicit schema failed, using inference: {e}")
                table = pa.Table.from_pylist(data)
        else:
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
        records: Sequence[BaseRecord],
        path: Path,
        run_id: UUID,
        batch_num: int,
    ) -> int:
        """Write batch as JSON file (fallback)."""
        data = [r.to_silver_dict() for r in records]

        filename = f"{self.generate_filename(run_id, batch_num).rsplit('.', 1)[0]}.json"
        filepath = path / filename

        with open(filepath, "w") as f:
            json.dump(data, f, default=str)

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
        Read normalized records from silver storage.

        Args:
            source_name: Filter by source
            start_date: Filter by start date
            end_date: Filter by end date
            limit: Maximum records to return

        Returns:
            List of record dictionaries
        """
        results = []

        # Try Delta first if available
        if self.config.enable_delta and DELTA_AVAILABLE and source_name:
            delta_path = self._base_path / source_name
            if delta_path.exists():
                dt = DeltaTable(str(delta_path))
                df = dt.to_pandas()

                if start_date:
                    df = df[df["lineage_scraped_at"] >= start_date.isoformat()]
                if end_date:
                    df = df[df["lineage_scraped_at"] <= end_date.isoformat()]
                if limit:
                    df = df.head(limit)

                return df.to_dict("records")

        # Fallback to reading individual files
        search_path = self._base_path
        if source_name and self.config.partition_by_source:
            search_path = search_path / f"source={source_name}"

        if not search_path.exists():
            return results

        pattern = "*.parquet" if PYARROW_AVAILABLE else "*.json"
        for filepath in sorted(search_path.rglob(pattern)):
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

    def get_existing_hashes(self, source_name: str) -> set[str]:
        """
        Load existing record hashes for deduplication.

        Args:
            source_name: Source to check

        Returns:
            Set of raw_hash values already in storage
        """
        hashes = set()

        try:
            records = self.read(source_name=source_name)
            for r in records:
                if "lineage_raw_hash" in r and r["lineage_raw_hash"]:
                    hashes.add(r["lineage_raw_hash"])
        except Exception as e:
            self._logger.warning(f"Could not load existing hashes: {e}")

        return hashes
