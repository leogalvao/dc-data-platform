"""
Null rate tracking for data quality monitoring.

Tracks and reports null rates per field per source
to identify data quality issues early.
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import pyarrow.parquet as pq
    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False

logger = logging.getLogger(__name__)


class NullRateTracker:
    """Track null rates across silver layer data."""

    def __init__(self, silver_path: Path | str, metrics_path: Path | str):
        self.silver_path = Path(silver_path)
        self.metrics_path = Path(metrics_path)
        self.metrics_path.mkdir(parents=True, exist_ok=True)

    def compute_null_rates(self) -> dict[str, dict[str, float]]:
        """
        Compute null rates for all sources and fields.

        Returns:
            Dict of source -> field -> null_rate (0.0 to 1.0)
        """
        if not PYARROW_AVAILABLE:
            raise ImportError("PyArrow required for null rate tracking")

        results: dict[str, dict[str, dict[str, int]]] = defaultdict(
            lambda: defaultdict(lambda: {"nulls": 0, "total": 0})
        )

        for source_dir in self.silver_path.iterdir():
            if not source_dir.is_dir() or not source_dir.name.startswith("source="):
                continue

            source_name = source_dir.name.replace("source=", "")

            for f in source_dir.rglob("*.parquet"):
                try:
                    table = pq.read_table(f)
                    for col_name in table.column_names:
                        col = table.column(col_name)
                        results[source_name][col_name]["nulls"] += col.null_count
                        results[source_name][col_name]["total"] += len(table)
                except Exception as e:
                    logger.warning(f"Error reading {f}: {e}")

        # Convert to rates
        null_rates: dict[str, dict[str, float]] = {}
        for source, fields in results.items():
            null_rates[source] = {}
            for field, counts in fields.items():
                if counts["total"] > 0:
                    null_rates[source][field] = counts["nulls"] / counts["total"]
                else:
                    null_rates[source][field] = 0.0

        return null_rates

    def find_high_null_fields(
        self,
        null_rates: dict[str, dict[str, float]],
        threshold: float = 0.9
    ) -> list[dict[str, Any]]:
        """
        Find fields with null rates above threshold.

        Args:
            null_rates: Output from compute_null_rates()
            threshold: Null rate threshold (0.0 to 1.0)

        Returns:
            List of {source, field, null_rate} dicts
        """
        high_null = []
        for source, fields in null_rates.items():
            for field, rate in fields.items():
                if rate >= threshold:
                    high_null.append({
                        "source": source,
                        "field": field,
                        "null_rate": round(rate, 4),
                        "null_rate_percent": round(rate * 100, 2),
                    })

        return sorted(high_null, key=lambda x: x["null_rate"], reverse=True)

    def save_metrics(
        self,
        null_rates: dict[str, dict[str, float]],
        filename: str = "null_rates.json"
    ) -> Path:
        """
        Save null rates to metrics file.

        Args:
            null_rates: Output from compute_null_rates()
            filename: Output filename

        Returns:
            Path to saved file
        """
        metrics = {
            "computed_at": datetime.now(timezone.utc).isoformat(),
            "null_rates_by_source": null_rates,
            "high_null_fields": self.find_high_null_fields(null_rates),
            "summary": self._compute_summary(null_rates),
        }

        filepath = self.metrics_path / filename
        with open(filepath, "w") as f:
            json.dump(metrics, f, indent=2)

        logger.info(f"Saved null rate metrics to {filepath}")
        return filepath

    def _compute_summary(self, null_rates: dict[str, dict[str, float]]) -> dict[str, Any]:
        """Compute summary statistics."""
        all_rates = []
        for source, fields in null_rates.items():
            all_rates.extend(fields.values())

        if not all_rates:
            return {
                "total_sources": 0,
                "total_fields": 0,
                "avg_null_rate": 0.0,
                "fields_above_50_percent": 0,
                "fields_above_90_percent": 0,
            }

        return {
            "total_sources": len(null_rates),
            "total_fields": len(all_rates),
            "avg_null_rate": round(sum(all_rates) / len(all_rates), 4),
            "fields_above_50_percent": len([r for r in all_rates if r >= 0.5]),
            "fields_above_90_percent": len([r for r in all_rates if r >= 0.9]),
        }

    def compare_with_previous(
        self,
        current_rates: dict[str, dict[str, float]],
        previous_file: Path | str,
        threshold_change: float = 0.1
    ) -> dict[str, Any]:
        """
        Compare current null rates with previous run.

        Args:
            current_rates: Current null rates
            previous_file: Path to previous null_rates.json
            threshold_change: Alert threshold for change (0.0 to 1.0)

        Returns:
            Comparison results with significant changes
        """
        previous_file = Path(previous_file)
        if not previous_file.exists():
            return {
                "status": "no_previous",
                "message": "No previous metrics file found",
            }

        with open(previous_file) as f:
            previous = json.load(f)

        previous_rates = previous.get("null_rates_by_source", {})
        changes = []

        for source, fields in current_rates.items():
            prev_fields = previous_rates.get(source, {})
            for field, current_rate in fields.items():
                prev_rate = prev_fields.get(field, 0.0)
                change = current_rate - prev_rate

                if abs(change) >= threshold_change:
                    changes.append({
                        "source": source,
                        "field": field,
                        "previous_rate": round(prev_rate, 4),
                        "current_rate": round(current_rate, 4),
                        "change": round(change, 4),
                        "direction": "increased" if change > 0 else "decreased",
                    })

        return {
            "status": "compared",
            "previous_computed_at": previous.get("computed_at"),
            "significant_changes": changes,
            "summary": {
                "total_changes": len(changes),
                "increases": len([c for c in changes if c["change"] > 0]),
                "decreases": len([c for c in changes if c["change"] < 0]),
            }
        }


def track_null_rates(
    silver_path: str = "data/silver",
    metrics_path: str = "data/reports",
    compare_previous: bool = True,
) -> dict[str, Any]:
    """
    Track null rates and save metrics.

    Convenience function for CLI/script usage.

    Args:
        silver_path: Path to silver layer data
        metrics_path: Path to save metrics
        compare_previous: Whether to compare with previous run

    Returns:
        Metrics dict
    """
    tracker = NullRateTracker(silver_path, metrics_path)

    # Compute current rates
    null_rates = tracker.compute_null_rates()

    # Compare with previous if requested
    comparison = None
    previous_file = Path(metrics_path) / "null_rates.json"
    if compare_previous and previous_file.exists():
        comparison = tracker.compare_with_previous(null_rates, previous_file)

    # Save metrics
    filepath = tracker.save_metrics(null_rates)

    result = {
        "filepath": str(filepath),
        "null_rates": null_rates,
        "high_null_fields": tracker.find_high_null_fields(null_rates),
        "summary": tracker._compute_summary(null_rates),
    }

    if comparison:
        result["comparison"] = comparison

    return result


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Track null rates in silver layer")
    parser.add_argument("--silver-path", default="data/silver")
    parser.add_argument("--metrics-path", default="data/reports")
    parser.add_argument("--threshold", type=float, default=0.9,
                        help="Threshold for high null rate alerts")
    parser.add_argument("--no-compare", action="store_true",
                        help="Skip comparison with previous run")

    args = parser.parse_args()

    result = track_null_rates(
        silver_path=args.silver_path,
        metrics_path=args.metrics_path,
        compare_previous=not args.no_compare,
    )

    print(f"Metrics saved to: {result['filepath']}")
    print(f"\nSummary: {json.dumps(result['summary'], indent=2)}")

    if result.get("high_null_fields"):
        print(f"\nHigh null fields (>{args.threshold * 100}%):")
        for field in result["high_null_fields"][:10]:
            print(f"  {field['source']}.{field['field']}: {field['null_rate_percent']}%")

    if result.get("comparison", {}).get("significant_changes"):
        print(f"\nSignificant changes from previous run:")
        for change in result["comparison"]["significant_changes"][:5]:
            print(f"  {change['source']}.{change['field']}: "
                  f"{change['previous_rate']:.1%} -> {change['current_rate']:.1%} "
                  f"({change['direction']})")
