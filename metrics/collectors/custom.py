"""Custom metric collectors for business-specific metrics."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from metrics.collectors.parquet_stats import collect_parquet_metrics

logger = logging.getLogger(__name__)


@dataclass
class FreshnessMetrics:
    """Freshness metrics for a dataset."""

    dataset_name: str
    layer: str
    last_modified: datetime
    age_hours: float
    sla_hours: float
    within_sla: bool

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "dataset_name": self.dataset_name,
            "layer": self.layer,
            "last_modified": self.last_modified.isoformat(),
            "age_hours": round(self.age_hours, 2),
            "sla_hours": self.sla_hours,
            "within_sla": self.within_sla,
        }


@dataclass
class QualityMetrics:
    """Quality metrics for a dataset."""

    dataset_name: str
    layer: str
    row_count: int
    null_rate_avg: float
    null_rate_max: float
    high_null_columns: list[str] = field(default_factory=list)
    quality_score: float = 1.0  # 0-1 score

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "dataset_name": self.dataset_name,
            "layer": self.layer,
            "row_count": self.row_count,
            "null_rate_avg": round(self.null_rate_avg, 4),
            "null_rate_max": round(self.null_rate_max, 4),
            "high_null_columns": self.high_null_columns,
            "quality_score": round(self.quality_score, 4),
        }


def check_freshness(
    dataset_path: Path,
    dataset_name: str,
    layer: str,
    sla_hours: float = 24.0,
) -> FreshnessMetrics | None:
    """
    Check freshness of a dataset against SLA.

    Args:
        dataset_path: Path to dataset
        dataset_name: Name of dataset
        layer: Layer name (silver, gold, etc.)
        sla_hours: Maximum allowed age in hours

    Returns:
        FreshnessMetrics or None if not available
    """
    metrics = collect_parquet_metrics(dataset_path, dataset_name, sample_for_nulls=False)
    if metrics is None:
        return None

    age = datetime.now() - metrics.last_modified
    age_hours = age.total_seconds() / 3600

    return FreshnessMetrics(
        dataset_name=dataset_name,
        layer=layer,
        last_modified=metrics.last_modified,
        age_hours=age_hours,
        sla_hours=sla_hours,
        within_sla=age_hours <= sla_hours,
    )


def calculate_quality_score(
    dataset_path: Path,
    dataset_name: str,
    layer: str,
    null_threshold: float = 0.3,
) -> QualityMetrics | None:
    """
    Calculate quality score for a dataset.

    Quality score is based on:
    - Null rates (lower is better)
    - Row count (non-empty is required)

    Args:
        dataset_path: Path to dataset
        dataset_name: Name of dataset
        layer: Layer name
        null_threshold: Threshold above which a column is considered "high null"

    Returns:
        QualityMetrics or None if not available
    """
    metrics = collect_parquet_metrics(dataset_path, dataset_name, sample_for_nulls=True)
    if metrics is None:
        return None

    # Find high null columns
    high_null_cols = [
        col
        for col, rate in metrics.column_null_rates.items()
        if rate > null_threshold
    ]

    # Calculate quality score (1.0 is perfect)
    # Penalize for high null rates and high null columns
    null_penalty = metrics.avg_null_rate * 0.5  # Up to 50% penalty
    column_penalty = len(high_null_cols) / max(len(metrics.schema_fields), 1) * 0.3  # Up to 30%
    empty_penalty = 0.2 if metrics.row_count == 0 else 0

    quality_score = max(0, 1.0 - null_penalty - column_penalty - empty_penalty)

    return QualityMetrics(
        dataset_name=dataset_name,
        layer=layer,
        row_count=metrics.row_count,
        null_rate_avg=metrics.avg_null_rate,
        null_rate_max=metrics.max_null_rate,
        high_null_columns=high_null_cols,
        quality_score=quality_score,
    )


def generate_layer_report(
    layer_path: Path,
    layer_name: str,
    sla_hours: float = 24.0,
) -> dict[str, Any]:
    """
    Generate comprehensive report for a data layer.

    Args:
        layer_path: Path to layer directory
        layer_name: Name of layer
        sla_hours: Default SLA for freshness

    Returns:
        Report dictionary
    """
    if not layer_path.exists():
        return {"error": f"Layer path not found: {layer_path}"}

    datasets = [d for d in layer_path.iterdir() if d.is_dir()]

    report = {
        "layer": layer_name,
        "generated_at": datetime.now().isoformat(),
        "dataset_count": len(datasets),
        "datasets": [],
        "summary": {
            "total_rows": 0,
            "total_bytes": 0,
            "datasets_within_sla": 0,
            "avg_quality_score": 0,
        },
    }

    quality_scores = []

    for ds_path in sorted(datasets):
        ds_name = ds_path.name

        # Collect metrics
        parquet = collect_parquet_metrics(ds_path, ds_name)
        freshness = check_freshness(ds_path, ds_name, layer_name, sla_hours)
        quality = calculate_quality_score(ds_path, ds_name, layer_name)

        ds_report = {
            "name": ds_name,
            "metrics": parquet.to_dict() if parquet else None,
            "freshness": freshness.to_dict() if freshness else None,
            "quality": quality.to_dict() if quality else None,
        }

        report["datasets"].append(ds_report)

        # Update summary
        if parquet:
            report["summary"]["total_rows"] += parquet.row_count
            report["summary"]["total_bytes"] += parquet.total_bytes

        if freshness and freshness.within_sla:
            report["summary"]["datasets_within_sla"] += 1

        if quality:
            quality_scores.append(quality.quality_score)

    # Calculate average quality score
    if quality_scores:
        report["summary"]["avg_quality_score"] = sum(quality_scores) / len(quality_scores)

    return report
