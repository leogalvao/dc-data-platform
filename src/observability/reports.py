"""
Run report generation for scraper runs.

Generates JSON and optional HTML reports with:
- Execution summary
- Record counts and statistics
- Error analysis
- Data quality metrics
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..schemas.metadata import RunReport


class ReportGenerator:
    """
    Generates run reports in various formats.

    Supports:
    - JSON for machine consumption
    - Console summary for quick review
    - HTML for detailed analysis (optional)
    """

    def __init__(self, output_dir: Path):
        """
        Initialize the report generator.

        Args:
            output_dir: Directory to save reports
        """
        self._output_dir = output_dir
        self._output_dir.mkdir(parents=True, exist_ok=True)

    def generate_json(self, report: RunReport) -> Path:
        """
        Generate JSON report file.

        Args:
            report: Run report data

        Returns:
            Path to generated report
        """
        filename = f"{report.source_name}_{report.run_id}.json"
        filepath = self._output_dir / filename

        with open(filepath, "w") as f:
            json.dump(report.model_dump(mode="json"), f, indent=2, default=str)

        return filepath

    def generate_console_summary(self, report: RunReport) -> str:
        """
        Generate console-friendly summary.

        Args:
            report: Run report data

        Returns:
            Formatted summary string
        """
        lines = [
            "",
            "=" * 60,
            f"  Run Report: {report.source_name}",
            "=" * 60,
            f"  Run ID:      {report.run_id}",
            f"  Status:      {report.metadata.status.value}",
            f"  Duration:    {report.metadata.duration_seconds:.1f}s" if report.metadata.duration_seconds else "",
            "",
            "  Records:",
            f"    Discovered:   {report.records_discovered:,}",
            f"    Fetched:      {report.records_fetched:,}",
            f"    Parsed:       {report.records_parsed:,}",
            f"    Validated:    {report.records_validated:,}",
            f"    Bronze:       {report.records_written_bronze:,}",
            f"    Silver:       {report.records_written_silver:,}",
            f"    Quarantined:  {report.records_quarantined:,}",
            f"    Deduplicated: {report.records_deduplicated:,}",
            "",
            f"  Success Rate:   {report.success_rate:.1f}%",
            f"  Error Rate:     {report.error_rate:.1f}%",
            "",
        ]

        if report.error_samples:
            lines.extend([
                "  Sample Errors:",
                *[f"    - {e.get('errors', e)}" for e in report.error_samples[:3]],
                "",
            ])

        if report.new_fields_detected:
            lines.extend([
                "  Schema Drift Detected:",
                *[f"    + {f}" for f in report.new_fields_detected[:5]],
                "",
            ])

        lines.append("=" * 60)
        return "\n".join(lines)

    def generate_multi_source_summary(
        self,
        reports: list[RunReport],
    ) -> dict[str, Any]:
        """
        Generate summary across multiple source runs.

        Args:
            reports: List of run reports

        Returns:
            Summary dictionary
        """
        total_records = sum(r.records_written_silver for r in reports)
        total_quarantined = sum(r.records_quarantined for r in reports)
        total_duration = sum(r.metadata.duration_seconds or 0 for r in reports)

        successful = [r for r in reports if r.metadata.status.value == "completed"]
        failed = [r for r in reports if r.metadata.status.value == "failed"]

        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "summary": {
                "total_sources": len(reports),
                "successful": len(successful),
                "failed": len(failed),
                "total_records": total_records,
                "total_quarantined": total_quarantined,
                "total_duration_seconds": total_duration,
            },
            "by_source": [
                {
                    "source": r.source_name,
                    "status": r.metadata.status.value,
                    "records": r.records_written_silver,
                    "quarantined": r.records_quarantined,
                    "duration": r.metadata.duration_seconds,
                    "success_rate": r.success_rate,
                }
                for r in reports
            ],
            "errors": {
                r.source_name: r.error_samples[:2]
                for r in reports
                if r.error_samples
            },
        }

    def save_multi_source_report(
        self,
        reports: list[RunReport],
        filename: str = "run_summary.json",
    ) -> Path:
        """
        Save multi-source summary report.

        Args:
            reports: List of run reports
            filename: Output filename

        Returns:
            Path to saved report
        """
        summary = self.generate_multi_source_summary(reports)
        filepath = self._output_dir / filename

        with open(filepath, "w") as f:
            json.dump(summary, f, indent=2, default=str)

        return filepath
