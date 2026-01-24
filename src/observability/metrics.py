"""
Metrics collection for scraper performance monitoring.

Tracks:
- Request counts and latencies
- Record processing rates
- Error rates by type
- Data quality metrics
"""

from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass
class SourceMetrics:
    """Metrics for a single source."""

    source_name: str
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    # Counters
    requests_total: int = 0
    requests_success: int = 0
    requests_failed: int = 0
    records_discovered: int = 0
    records_processed: int = 0
    records_written: int = 0
    records_quarantined: int = 0

    # Latencies (milliseconds)
    fetch_latencies: list[float] = field(default_factory=list)
    parse_latencies: list[float] = field(default_factory=list)

    # Errors by type
    errors_by_type: dict[str, int] = field(default_factory=lambda: defaultdict(int))

    # Data quality
    null_counts: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    field_counts: dict[str, int] = field(default_factory=lambda: defaultdict(int))

    def record_request(self, success: bool, latency_ms: float) -> None:
        """Record a request metric."""
        self.requests_total += 1
        if success:
            self.requests_success += 1
        else:
            self.requests_failed += 1
        self.fetch_latencies.append(latency_ms)

    def record_parse(self, latency_ms: float) -> None:
        """Record parse latency."""
        self.parse_latencies.append(latency_ms)

    def record_error(self, error_type: str) -> None:
        """Record an error by type."""
        self.errors_by_type[error_type] += 1

    def record_field_stats(self, record: dict[str, Any]) -> None:
        """Track field presence and null rates."""
        for key, value in record.items():
            self.field_counts[key] += 1
            if value is None:
                self.null_counts[key] += 1

    @property
    def avg_fetch_latency(self) -> float:
        """Average fetch latency in ms."""
        if not self.fetch_latencies:
            return 0.0
        return sum(self.fetch_latencies) / len(self.fetch_latencies)

    @property
    def avg_parse_latency(self) -> float:
        """Average parse latency in ms."""
        if not self.parse_latencies:
            return 0.0
        return sum(self.parse_latencies) / len(self.parse_latencies)

    @property
    def success_rate(self) -> float:
        """Request success rate (0-100)."""
        if self.requests_total == 0:
            return 0.0
        return (self.requests_success / self.requests_total) * 100

    def get_null_rates(self) -> dict[str, float]:
        """Get null rate per field (0-100)."""
        return {
            field: (self.null_counts[field] / self.field_counts[field] * 100)
            if self.field_counts[field] > 0 else 0.0
            for field in self.field_counts
        }

    def to_dict(self) -> dict[str, Any]:
        """Export metrics as dictionary."""
        return {
            "source_name": self.source_name,
            "started_at": self.started_at.isoformat(),
            "requests": {
                "total": self.requests_total,
                "success": self.requests_success,
                "failed": self.requests_failed,
                "success_rate": self.success_rate,
            },
            "records": {
                "discovered": self.records_discovered,
                "processed": self.records_processed,
                "written": self.records_written,
                "quarantined": self.records_quarantined,
            },
            "latency_ms": {
                "fetch_avg": self.avg_fetch_latency,
                "parse_avg": self.avg_parse_latency,
                "fetch_p95": self._percentile(self.fetch_latencies, 95),
                "parse_p95": self._percentile(self.parse_latencies, 95),
            },
            "errors_by_type": dict(self.errors_by_type),
            "null_rates": self.get_null_rates(),
        }

    @staticmethod
    def _percentile(values: list[float], p: int) -> float:
        """Calculate percentile."""
        if not values:
            return 0.0
        sorted_values = sorted(values)
        index = int(len(sorted_values) * p / 100)
        return sorted_values[min(index, len(sorted_values) - 1)]


class MetricsCollector:
    """
    Collects and aggregates metrics across sources.

    Thread-safe for use in concurrent scraping.
    """

    def __init__(self):
        self._metrics: dict[str, SourceMetrics] = {}
        self._global_start = datetime.now(timezone.utc)

    def get_or_create(self, source_name: str) -> SourceMetrics:
        """Get or create metrics for a source."""
        if source_name not in self._metrics:
            self._metrics[source_name] = SourceMetrics(source_name=source_name)
        return self._metrics[source_name]

    def record_request(
        self,
        source_name: str,
        success: bool,
        latency_ms: float,
    ) -> None:
        """Record a request metric."""
        self.get_or_create(source_name).record_request(success, latency_ms)

    def record_records(
        self,
        source_name: str,
        *,
        discovered: int = 0,
        processed: int = 0,
        written: int = 0,
        quarantined: int = 0,
    ) -> None:
        """Record record counts."""
        metrics = self.get_or_create(source_name)
        metrics.records_discovered += discovered
        metrics.records_processed += processed
        metrics.records_written += written
        metrics.records_quarantined += quarantined

    def record_error(self, source_name: str, error_type: str) -> None:
        """Record an error."""
        self.get_or_create(source_name).record_error(error_type)

    def get_all_metrics(self) -> dict[str, Any]:
        """Get metrics for all sources."""
        return {
            "collection_started": self._global_start.isoformat(),
            "sources": {
                name: metrics.to_dict()
                for name, metrics in self._metrics.items()
            },
            "totals": self._calculate_totals(),
        }

    def _calculate_totals(self) -> dict[str, Any]:
        """Calculate aggregate totals."""
        if not self._metrics:
            return {}

        return {
            "requests_total": sum(m.requests_total for m in self._metrics.values()),
            "requests_success": sum(m.requests_success for m in self._metrics.values()),
            "records_written": sum(m.records_written for m in self._metrics.values()),
            "records_quarantined": sum(m.records_quarantined for m in self._metrics.values()),
        }

    def reset(self) -> None:
        """Reset all metrics."""
        self._metrics.clear()
        self._global_start = datetime.now(timezone.utc)


class Timer:
    """Context manager for timing operations."""

    def __init__(self):
        self.start_time: float = 0
        self.elapsed_ms: float = 0

    def __enter__(self) -> Timer:
        self.start_time = time.perf_counter()
        return self

    def __exit__(self, *args) -> None:
        self.elapsed_ms = (time.perf_counter() - self.start_time) * 1000


# =============================================================================
# Pipeline Health Summary
# =============================================================================


@dataclass
class PipelineHealthSummary:
    """Overall pipeline health summary for monitoring."""

    run_id: str
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    total_sources_run: int = 0
    total_sources_succeeded: int = 0
    total_sources_failed: int = 0
    total_records_processed: int = 0
    total_records_quarantined: int = 0
    total_duration_seconds: float = 0.0
    source_summaries: dict[str, dict[str, Any]] = field(default_factory=dict)

    @property
    def overall_success_rate(self) -> float:
        """Overall success rate across all sources."""
        if self.total_records_processed == 0:
            return 0.0
        written = self.total_records_processed - self.total_records_quarantined
        return (written / self.total_records_processed) * 100

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "run_id": self.run_id,
            "timestamp": self.timestamp.isoformat(),
            "total_sources_run": self.total_sources_run,
            "total_sources_succeeded": self.total_sources_succeeded,
            "total_sources_failed": self.total_sources_failed,
            "total_records_processed": self.total_records_processed,
            "total_records_quarantined": self.total_records_quarantined,
            "total_duration_seconds": round(self.total_duration_seconds, 2),
            "overall_success_rate_percent": round(self.overall_success_rate, 2),
            "source_summaries": self.source_summaries,
        }


# =============================================================================
# Enhanced Metrics Writer
# =============================================================================


class MetricsWriter:
    """
    Writes metrics in multiple formats for monitoring dashboards.

    Supports:
    - JSON (detailed metrics)
    - Prometheus (scrape-ready format)
    - CSV (field-level null rates)
    """

    def __init__(self, collector: MetricsCollector, run_id: str):
        self.collector = collector
        self.run_id = run_id

    def write_json(self, path: str) -> None:
        """Write detailed metrics as JSON."""
        import json
        from pathlib import Path

        output_path = Path(path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        metrics = self.collector.get_all_metrics()
        metrics["run_id"] = self.run_id

        with open(output_path, "w") as f:
            json.dump(metrics, f, indent=2, default=str)

    def write_prometheus(self, path: str) -> None:
        """Write metrics in Prometheus exposition format."""
        from pathlib import Path

        output_path = Path(path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        lines = []
        metrics = self.collector.get_all_metrics()

        # Pipeline-level metrics
        totals = metrics.get("totals", {})
        lines.append("# HELP pipeline_records_total Total records processed")
        lines.append("# TYPE pipeline_records_total gauge")
        lines.append(f'pipeline_records_total{{run_id="{self.run_id}"}} {totals.get("requests_total", 0)}')

        lines.append("# HELP pipeline_records_quarantined Total records quarantined")
        lines.append("# TYPE pipeline_records_quarantined gauge")
        lines.append(f'pipeline_records_quarantined{{run_id="{self.run_id}"}} {totals.get("records_quarantined", 0)}')

        # Source-level metrics
        lines.append("# HELP source_records_written Records written per source")
        lines.append("# TYPE source_records_written gauge")
        for source_name, source_data in metrics.get("sources", {}).items():
            records = source_data.get("records", {})
            lines.append(f'source_records_written{{source="{source_name}"}} {records.get("written", 0)}')

        lines.append("# HELP source_quarantine_rate Quarantine rate per source")
        lines.append("# TYPE source_quarantine_rate gauge")
        for source_name, source_data in metrics.get("sources", {}).items():
            records = source_data.get("records", {})
            total = records.get("processed", 0)
            quarantined = records.get("quarantined", 0)
            rate = (quarantined / total * 100) if total > 0 else 0
            lines.append(f'source_quarantine_rate{{source="{source_name}"}} {rate:.2f}')

        # Field null rates
        lines.append("# HELP field_null_rate Null rate per field (percent)")
        lines.append("# TYPE field_null_rate gauge")
        for source_name, source_data in metrics.get("sources", {}).items():
            for field_name, null_rate in source_data.get("null_rates", {}).items():
                lines.append(f'field_null_rate{{source="{source_name}",field="{field_name}"}} {null_rate:.2f}')

        with open(output_path, "w") as f:
            f.write("\n".join(lines))

    def write_csv(self, path: str) -> None:
        """Write field-level null rates as CSV."""
        import csv
        from pathlib import Path

        output_path = Path(path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        metrics = self.collector.get_all_metrics()

        with open(output_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "timestamp", "run_id", "source", "field",
                "total_records", "null_count", "null_rate_percent"
            ])

            timestamp = metrics.get("collection_started", datetime.now(timezone.utc).isoformat())

            for source_name, source_data in metrics.get("sources", {}).items():
                null_rates = source_data.get("null_rates", {})
                for field_name, null_rate in null_rates.items():
                    # Estimate counts from rate
                    total = source_data.get("records", {}).get("processed", 0)
                    null_count = int(total * null_rate / 100) if total > 0 else 0
                    writer.writerow([
                        timestamp,
                        self.run_id,
                        source_name,
                        field_name,
                        total,
                        null_count,
                        round(null_rate, 2),
                    ])

    def build_health_summary(self) -> PipelineHealthSummary:
        """Build pipeline health summary from collected metrics."""
        metrics = self.collector.get_all_metrics()
        totals = metrics.get("totals", {})

        summary = PipelineHealthSummary(
            run_id=self.run_id,
            total_records_processed=totals.get("records_written", 0) + totals.get("records_quarantined", 0),
            total_records_quarantined=totals.get("records_quarantined", 0),
        )

        for source_name, source_data in metrics.get("sources", {}).items():
            summary.total_sources_run += 1
            records = source_data.get("records", {})
            if records.get("written", 0) > 0:
                summary.total_sources_succeeded += 1
            elif records.get("quarantined", 0) > 0:
                summary.total_sources_failed += 1

            summary.source_summaries[source_name] = {
                "records_written": records.get("written", 0),
                "records_quarantined": records.get("quarantined", 0),
                "success_rate": source_data.get("requests", {}).get("success_rate", 0),
                "high_null_fields": [
                    f for f, r in source_data.get("null_rates", {}).items()
                    if r > 90
                ],
            }

        return summary


# =============================================================================
# Null Rate Analysis Functions
# =============================================================================


def analyze_null_rates(
    metrics_path: str,
    threshold: float = 90.0,
) -> dict[str, Any]:
    """
    Analyze null rates from metrics file.

    Args:
        metrics_path: Path to metrics JSON file
        threshold: Null rate threshold for alerts (percent)

    Returns:
        Analysis results with alerts
    """
    import json

    with open(metrics_path) as f:
        metrics = json.load(f)

    alerts = []

    for source_name, source_data in metrics.get("sources", {}).items():
        for field_name, null_rate in source_data.get("null_rates", {}).items():
            if null_rate >= threshold:
                alerts.append({
                    "type": "high_null_rate",
                    "source": source_name,
                    "field": field_name,
                    "null_rate": null_rate,
                    "threshold": threshold,
                })

    return {
        "run_id": metrics.get("run_id"),
        "alerts": alerts,
        "summary": {
            "total_alerts": len(alerts),
            "fields_above_threshold": len(set(a["field"] for a in alerts)),
        },
    }


def compare_null_rates(
    current_path: str,
    previous_path: str,
    threshold_change: float = 10.0,
) -> dict[str, Any]:
    """
    Compare null rates between two runs.

    Args:
        current_path: Path to current metrics
        previous_path: Path to previous metrics
        threshold_change: Alert if null rate changed by more than this

    Returns:
        Comparison results with changes
    """
    import json

    with open(current_path) as f:
        current = json.load(f)
    with open(previous_path) as f:
        previous = json.load(f)

    changes = []

    for source_name, source_data in current.get("sources", {}).items():
        prev_source = previous.get("sources", {}).get(source_name, {})
        prev_null_rates = prev_source.get("null_rates", {})

        for field_name, current_rate in source_data.get("null_rates", {}).items():
            prev_rate = prev_null_rates.get(field_name, 0)
            change = current_rate - prev_rate

            if abs(change) >= threshold_change:
                changes.append({
                    "source": source_name,
                    "field": field_name,
                    "previous_rate": prev_rate,
                    "current_rate": current_rate,
                    "change": round(change, 2),
                    "direction": "increased" if change > 0 else "decreased",
                })

    return {
        "current_run_id": current.get("run_id"),
        "previous_run_id": previous.get("run_id"),
        "changes": changes,
        "summary": {
            "total_changes": len(changes),
            "increases": len([c for c in changes if c["change"] > 0]),
            "decreases": len([c for c in changes if c["change"] < 0]),
        },
    }
