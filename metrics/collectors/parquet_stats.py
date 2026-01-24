"""Collect metrics from Parquet files."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

try:
    import pyarrow.parquet as pq
    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False


@dataclass
class ParquetMetrics:
    """Metrics for a Parquet dataset."""

    dataset_name: str
    path: str
    row_count: int
    file_count: int
    total_bytes: int
    column_null_rates: dict[str, float] = field(default_factory=dict)
    last_modified: datetime = field(default_factory=datetime.now)
    schema_fields: list[str] = field(default_factory=list)
    partition_count: int = 0
    compression: str | None = None

    @property
    def size_mb(self) -> float:
        """Size in megabytes."""
        return self.total_bytes / (1024 * 1024)

    @property
    def max_null_rate(self) -> float:
        """Maximum null rate across all columns."""
        if not self.column_null_rates:
            return 0.0
        return max(self.column_null_rates.values())

    @property
    def avg_null_rate(self) -> float:
        """Average null rate across all columns."""
        if not self.column_null_rates:
            return 0.0
        return sum(self.column_null_rates.values()) / len(self.column_null_rates)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "dataset_name": self.dataset_name,
            "path": self.path,
            "row_count": self.row_count,
            "file_count": self.file_count,
            "total_bytes": self.total_bytes,
            "size_mb": round(self.size_mb, 2),
            "max_null_rate": round(self.max_null_rate, 4),
            "avg_null_rate": round(self.avg_null_rate, 4),
            "column_null_rates": {
                k: round(v, 4) for k, v in self.column_null_rates.items()
            },
            "last_modified": self.last_modified.isoformat(),
            "schema_fields": self.schema_fields,
            "partition_count": self.partition_count,
            "compression": self.compression,
        }


def collect_parquet_metrics(
    dataset_path: Path,
    dataset_name: str | None = None,
    sample_for_nulls: bool = True,
) -> ParquetMetrics | None:
    """
    Collect metrics for a Parquet dataset.

    Args:
        dataset_path: Path to Parquet file or directory
        dataset_name: Name for the dataset (defaults to path name)
        sample_for_nulls: Whether to read data for null rate calculation

    Returns:
        ParquetMetrics or None if not available
    """
    if not PYARROW_AVAILABLE:
        logger.warning("PyArrow not installed, skipping metrics")
        return None

    if not dataset_path.exists():
        logger.warning(f"Dataset path not found: {dataset_path}")
        return None

    dataset_name = dataset_name or dataset_path.name

    # Find all parquet files
    if dataset_path.is_file():
        parquet_files = [dataset_path]
    else:
        parquet_files = list(dataset_path.rglob("*.parquet"))

    if not parquet_files:
        logger.warning(f"No parquet files found in {dataset_path}")
        return None

    # Count partitions (directories with = in name)
    partition_dirs = set()
    for pf in parquet_files:
        for parent in pf.parents:
            if "=" in parent.name:
                partition_dirs.add(parent)

    # Aggregate metrics
    total_rows = 0
    total_bytes = 0
    null_counts: dict[str, int] = {}
    total_counts: dict[str, int] = {}
    schema_fields: list[str] = []
    latest_mtime = datetime.min
    compression = None

    for pf in parquet_files:
        try:
            # Get file stats
            stat = pf.stat()
            total_bytes += stat.st_size
            mtime = datetime.fromtimestamp(stat.st_mtime)
            if mtime > latest_mtime:
                latest_mtime = mtime

            # Read metadata
            metadata = pq.read_metadata(pf)
            total_rows += metadata.num_rows

            # Get compression from first row group
            if compression is None and metadata.num_row_groups > 0:
                rg = metadata.row_group(0)
                if rg.num_columns > 0:
                    compression = rg.column(0).compression

            # Read schema from first file
            if not schema_fields:
                schema = pq.read_schema(pf)
                schema_fields = [f.name for f in schema]

            # Sample for null rates if requested
            if sample_for_nulls:
                try:
                    table = pq.read_table(pf, columns=schema_fields)
                    for col in schema_fields:
                        null_count = table.column(col).null_count
                        null_counts[col] = null_counts.get(col, 0) + null_count
                        total_counts[col] = total_counts.get(col, 0) + len(table)
                except Exception as e:
                    logger.debug(f"Error reading table for null rates: {e}")

        except Exception as e:
            logger.warning(f"Error reading {pf}: {e}")
            continue

    # Calculate null rates
    null_rates = {}
    for col in schema_fields:
        total = total_counts.get(col, 0)
        nulls = null_counts.get(col, 0)
        null_rates[col] = nulls / total if total > 0 else 0.0

    return ParquetMetrics(
        dataset_name=dataset_name,
        path=str(dataset_path),
        row_count=total_rows,
        file_count=len(parquet_files),
        total_bytes=total_bytes,
        column_null_rates=null_rates,
        last_modified=latest_mtime if latest_mtime != datetime.min else datetime.now(),
        schema_fields=schema_fields,
        partition_count=len(partition_dirs),
        compression=compression,
    )


def collect_layer_metrics(
    layer_path: Path,
    layer_name: str,
) -> list[ParquetMetrics]:
    """
    Collect metrics for all datasets in a data layer.

    Args:
        layer_path: Path to layer directory (e.g., data/silver)
        layer_name: Name of the layer

    Returns:
        List of ParquetMetrics for each dataset
    """
    if not layer_path.exists():
        logger.warning(f"Layer path not found: {layer_path}")
        return []

    metrics = []
    datasets = [d for d in layer_path.iterdir() if d.is_dir()]

    for ds_path in sorted(datasets):
        ds_metrics = collect_parquet_metrics(ds_path, ds_path.name)
        if ds_metrics:
            metrics.append(ds_metrics)

    logger.info(f"Collected metrics for {len(metrics)} datasets in {layer_name}")
    return metrics
