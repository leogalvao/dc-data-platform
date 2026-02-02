"""Local execution adapter for pipelines."""

from __future__ import annotations

import logging
from pathlib import Path

from config.settings import Settings
from pipelines.base import Pipeline, PipelineResult
from pipelines.runner import PipelineRunner

logger = logging.getLogger(__name__)


class LocalAdapter:
    """
    Local execution adapter.

    Runs pipelines using local subprocess calls to scrapers_unified
    and warehouse repositories.
    """

    def __init__(self, settings: Settings | None = None):
        """Initialize adapter with settings."""
        self.settings = settings or Settings()
        self.runner = PipelineRunner(self.settings)

    def run_pipeline(
        self,
        pipeline_name: str,
        dry_run: bool = False,
    ) -> PipelineResult:
        """
        Run a pipeline by name.

        Args:
            pipeline_name: Name of pipeline definition file (without .yaml)
            dry_run: If True, only log what would be done

        Returns:
            PipelineResult with execution details
        """
        # Find pipeline definition
        fabric_base = Path(__file__).parent.parent.parent
        definitions_path = fabric_base / "pipelines" / "definitions"

        pipeline_file = definitions_path / f"{pipeline_name}.yaml"
        if not pipeline_file.exists():
            raise FileNotFoundError(f"Pipeline not found: {pipeline_file}")

        pipeline = Pipeline.from_yaml(pipeline_file)
        return self.runner.run(pipeline, dry_run=dry_run)

    def list_pipelines(self) -> list[str]:
        """List available pipeline names."""
        fabric_base = Path(__file__).parent.parent.parent
        definitions_path = fabric_base / "pipelines" / "definitions"

        return [
            f.stem
            for f in definitions_path.glob("*.yaml")
            if f.is_file()
        ]

    def validate_environment(self) -> dict[str, bool]:
        """
        Validate that required paths and dependencies exist.

        Returns:
            Dictionary mapping component names to availability status
        """
        checks = {}

        # Check paths
        checks["scrapers_unified"] = self.settings.scrapers_unified_path.exists()
        checks["warehouse"] = self.settings.warehouse_path.exists()
        checks["silver_data"] = self.settings.silver_path.exists()
        checks["gold_data"] = self.settings.gold_path.exists()

        # Check Python modules
        try:
            import pyarrow
            checks["pyarrow"] = True
        except ImportError:
            checks["pyarrow"] = False

        try:
            import psycopg2
            checks["psycopg2"] = True
        except ImportError:
            checks["psycopg2"] = False

        return checks
