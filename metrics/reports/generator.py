"""Report generation for metrics."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from factory.config.settings import Settings
from factory.metrics.collectors.custom import generate_layer_report
from factory.metrics.collectors.parquet_stats import collect_layer_metrics

logger = logging.getLogger(__name__)


def generate_quality_report(settings: Settings) -> dict[str, Any]:
    """
    Generate quality report for all data layers.

    Args:
        settings: Configuration settings

    Returns:
        Quality report dictionary
    """
    report = {
        "report_type": "quality",
        "generated_at": datetime.now().isoformat(),
        "layers": {},
        "summary": {
            "total_datasets": 0,
            "datasets_with_issues": 0,
            "avg_quality_score": 0,
        },
    }

    layers = [
        ("bronze", settings.bronze_path),
        ("silver", settings.silver_path),
        ("gold", settings.gold_path),
        ("diamond", settings.diamond_path),
    ]

    quality_scores = []

    for layer_name, layer_path in layers:
        if not layer_path.exists():
            continue

        layer_report = generate_layer_report(layer_path, layer_name)
        report["layers"][layer_name] = layer_report

        report["summary"]["total_datasets"] += layer_report.get("dataset_count", 0)

        # Count issues
        for ds in layer_report.get("datasets", []):
            quality = ds.get("quality")
            if quality:
                quality_scores.append(quality.get("quality_score", 1.0))
                if quality.get("quality_score", 1.0) < 0.8:
                    report["summary"]["datasets_with_issues"] += 1

    if quality_scores:
        report["summary"]["avg_quality_score"] = sum(quality_scores) / len(quality_scores)

    return report


def generate_freshness_report(settings: Settings) -> dict[str, Any]:
    """
    Generate freshness report for all data layers.

    Args:
        settings: Configuration settings

    Returns:
        Freshness report dictionary
    """
    report = {
        "report_type": "freshness",
        "generated_at": datetime.now().isoformat(),
        "layers": {},
        "summary": {
            "total_datasets": 0,
            "datasets_within_sla": 0,
            "datasets_stale": 0,
        },
    }

    layers = [
        ("silver", settings.silver_path, 24),
        ("gold", settings.gold_path, 24),
        ("diamond", settings.diamond_path, 168),
    ]

    for layer_name, layer_path, default_sla in layers:
        if not layer_path.exists():
            continue

        layer_report = generate_layer_report(layer_path, layer_name, default_sla)
        report["layers"][layer_name] = layer_report

        report["summary"]["total_datasets"] += layer_report.get("dataset_count", 0)
        report["summary"]["datasets_within_sla"] += layer_report.get("summary", {}).get(
            "datasets_within_sla", 0
        )

    report["summary"]["datasets_stale"] = (
        report["summary"]["total_datasets"] - report["summary"]["datasets_within_sla"]
    )

    return report


def generate_full_report(settings: Settings) -> dict[str, Any]:
    """
    Generate comprehensive report including quality, freshness, and metrics.

    Args:
        settings: Configuration settings

    Returns:
        Full report dictionary
    """
    report = {
        "report_type": "full",
        "generated_at": datetime.now().isoformat(),
        "quality": generate_quality_report(settings),
        "freshness": generate_freshness_report(settings),
        "infrastructure": {
            "scrapers_unified_available": settings.scrapers_unified_path.exists(),
            "warehouse_available": settings.warehouse_path.exists(),
            "silver_path": str(settings.silver_path),
            "gold_path": str(settings.gold_path),
            "diamond_path": str(settings.diamond_path),
        },
    }

    return report


def export_report_json(report: dict[str, Any], output_path: Path) -> None:
    """
    Export report to JSON file.

    Args:
        report: Report dictionary
        output_path: Path to output file
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    logger.info(f"Report exported to {output_path}")


def export_report_html(report: dict[str, Any], output_path: Path) -> None:
    """
    Export report to HTML file.

    Args:
        report: Report dictionary
        output_path: Path to output file
    """
    html = _generate_html_report(report)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        f.write(html)
    logger.info(f"HTML report exported to {output_path}")


def _generate_html_report(report: dict[str, Any]) -> str:
    """Generate HTML from report dictionary."""
    report_type = report.get("report_type", "unknown")
    generated_at = report.get("generated_at", "")

    html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>DC Data Platform - {report_type.title()} Report</title>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 40px; }}
        h1 {{ color: #1a1a2e; }}
        h2 {{ color: #16213e; border-bottom: 2px solid #0f3460; padding-bottom: 8px; }}
        table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
        th, td {{ border: 1px solid #ddd; padding: 12px; text-align: left; }}
        th {{ background-color: #0f3460; color: white; }}
        tr:nth-child(even) {{ background-color: #f9f9f9; }}
        .metric {{ display: inline-block; background: #e8e8e8; padding: 8px 16px; margin: 4px; border-radius: 4px; }}
        .good {{ color: #28a745; }}
        .warning {{ color: #ffc107; }}
        .bad {{ color: #dc3545; }}
        .summary {{ background: #f5f5f5; padding: 20px; border-radius: 8px; margin: 20px 0; }}
    </style>
</head>
<body>
    <h1>DC Data Platform - {report_type.title()} Report</h1>
    <p><em>Generated: {generated_at}</em></p>
"""

    # Add summary section
    if "summary" in report:
        html += '<div class="summary"><h2>Summary</h2><div>'
        for key, value in report["summary"].items():
            html += f'<span class="metric"><strong>{key.replace("_", " ").title()}:</strong> {value}</span>'
        html += "</div></div>"

    # Add layers section
    if "layers" in report:
        for layer_name, layer_data in report["layers"].items():
            html += f"<h2>{layer_name.title()} Layer</h2>"

            if "datasets" in layer_data:
                html += """
                <table>
                    <tr>
                        <th>Dataset</th>
                        <th>Rows</th>
                        <th>Size</th>
                        <th>Max Null Rate</th>
                        <th>Quality Score</th>
                        <th>Within SLA</th>
                    </tr>
                """
                for ds in layer_data["datasets"]:
                    metrics = ds.get("metrics") or {}
                    quality = ds.get("quality") or {}
                    freshness = ds.get("freshness") or {}

                    row_count = metrics.get("row_count", "-")
                    size_mb = metrics.get("size_mb", "-")
                    max_null = metrics.get("max_null_rate", 0) * 100
                    q_score = quality.get("quality_score", "-")
                    within_sla = freshness.get("within_sla", "-")

                    null_class = "good" if max_null < 10 else "warning" if max_null < 30 else "bad"
                    sla_class = "good" if within_sla else "bad"

                    html += f"""
                    <tr>
                        <td>{ds['name']}</td>
                        <td>{row_count:,}</td>
                        <td>{size_mb} MB</td>
                        <td class="{null_class}">{max_null:.1f}%</td>
                        <td>{q_score:.2f if isinstance(q_score, float) else q_score}</td>
                        <td class="{sla_class}">{within_sla}</td>
                    </tr>
                    """
                html += "</table>"

    html += """
</body>
</html>
"""
    return html
