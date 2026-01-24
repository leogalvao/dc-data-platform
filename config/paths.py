"""Path constants and utilities for Factory layer."""

from __future__ import annotations

from pathlib import Path


class Paths:
    """Centralized path management for the Factory layer."""

    def __init__(self, base_path: Path | None = None):
        """
        Initialize paths.

        Args:
            base_path: Base path for factory repo. Defaults to parent of this file.
        """
        if base_path is None:
            base_path = Path(__file__).parent.parent
        self.base = base_path

    @property
    def config(self) -> Path:
        """Path to config directory."""
        return self.base / "config"

    @property
    def contracts(self) -> Path:
        """Path to contracts directory."""
        return self.base / "contracts"

    @property
    def contract_schemas(self) -> Path:
        """Path to contract schema definitions."""
        return self.contracts / "schemas"

    @property
    def contract_slas(self) -> Path:
        """Path to SLA definitions."""
        return self.contracts / "slas"

    @property
    def pipelines(self) -> Path:
        """Path to pipelines directory."""
        return self.base / "pipelines"

    @property
    def pipeline_definitions(self) -> Path:
        """Path to pipeline definitions."""
        return self.pipelines / "definitions"

    @property
    def metrics(self) -> Path:
        """Path to metrics directory."""
        return self.base / "metrics"

    @property
    def metric_definitions(self) -> Path:
        """Path to metric definitions."""
        return self.metrics / "definitions"

    @property
    def semantic(self) -> Path:
        """Path to semantic layer directory."""
        return self.base / "semantic"

    @property
    def lineage(self) -> Path:
        """Path to lineage directory."""
        return self.base / "lineage"

    @property
    def serving(self) -> Path:
        """Path to serving layer directory."""
        return self.base / "serving"

    @property
    def tests(self) -> Path:
        """Path to tests directory."""
        return self.base / "tests"

    @property
    def test_fixtures(self) -> Path:
        """Path to test fixtures."""
        return self.tests / "fixtures"
