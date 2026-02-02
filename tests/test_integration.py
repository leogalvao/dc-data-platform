"""Integration tests for Factory layer."""

from __future__ import annotations

from pathlib import Path

import pytest


class TestEndToEnd:
    """End-to-end integration tests."""

    def test_contract_validation_workflow(self, temp_dir, sample_contract_yaml, mock_parquet_data):
        """Test complete contract validation workflow."""
        from contracts.base import DataContract, validate_data_against_contract

        # Create contract that matches mock data
        contract_yaml = """
name: test_dataset
version: "1.0"
description: Test dataset
owner: test
layer: silver

columns:
  id:
    type: string
    nullable: false
  name:
    type: string
    nullable: true
    max_null_rate: 0.5
  value:
    type: decimal
    nullable: true

sla:
  freshness_hours: 24
  min_row_count: 1
  max_null_rate: 0.5
"""
        contract_file = temp_dir / "contract.yaml"
        contract_file.write_text(contract_yaml)

        contract = DataContract.from_yaml(contract_file)
        result = validate_data_against_contract(mock_parquet_data, contract)

        assert result.valid is True
        assert result.contract_name == "test_dataset"
        assert "row_count" in result.metrics

    def test_quality_report_generation(self, settings, mock_parquet_data):
        """Test quality report generation."""
        from metrics.collectors.custom import generate_layer_report

        # Move mock data to silver layer
        import shutil

        silver_ds = settings.silver_path / "test_dataset"
        shutil.copytree(mock_parquet_data, silver_ds)

        report = generate_layer_report(
            settings.silver_path,
            "silver",
            sla_hours=24,
        )

        assert report["layer"] == "silver"
        assert report["dataset_count"] >= 1
        assert "summary" in report
        assert len(report["datasets"]) >= 1

    def test_pipeline_dry_run(self, temp_dir, sample_pipeline_yaml, settings):
        """Test pipeline dry run."""
        from pipelines.adapters.local import LocalAdapter
        from pipelines.base import Pipeline

        # Create pipeline file
        pipeline_file = temp_dir / "test.yaml"
        pipeline_file.write_text(sample_pipeline_yaml)

        pipeline = Pipeline.from_yaml(pipeline_file)
        adapter = LocalAdapter(settings)

        result = adapter.runner.run(pipeline, dry_run=True)

        assert result.status.value in ("success", "running")
        assert len(result.task_results) == 2

    def test_lineage_graph(self, temp_dir):
        """Test lineage graph construction."""
        from lineage.graph import LineageGraph, LineageNode, NodeType, LineageEdge

        graph = LineageGraph()

        # Add nodes
        source = LineageNode("api", NodeType.SOURCE, "API source")
        silver = LineageNode("contracts", NodeType.SILVER, "Contracts data")
        gold = LineageNode("spend_by_vendor", NodeType.GOLD, "Aggregated spend")

        graph.add_node(source)
        graph.add_node(silver)
        graph.add_node(gold)

        # Add edges
        graph.add_edge(LineageEdge(source, silver, "normalize"))
        graph.add_edge(LineageEdge(silver, gold, "aggregate"))

        # Test upstream/downstream
        gold_key = f"{NodeType.GOLD.value}:spend_by_vendor"
        upstream = graph.get_upstream(gold_key)

        assert len(upstream) == 2  # silver and source
        assert any(n.name == "contracts" for n in upstream)

    def test_semantic_model(self, temp_dir):
        """Test semantic model loading and export."""
        from semantic.models import SemanticModel

        model_yaml = """
name: test_model
description: Test semantic model
source_tables:
  - fact_spend
dimensions:
  vendor:
    description: Vendor name
    source_column: vendor_name
    source_table: dim_supplier
measures:
  total_spend:
    description: Total spending
    expression: SUM(amount)
    aggregation: sum
"""
        model_file = temp_dir / "model.yaml"
        model_file.write_text(model_yaml)

        model = SemanticModel.from_yaml(model_file)

        assert model.name == "test_model"
        assert len(model.dimensions) == 1
        assert len(model.measures) == 1

        # Test LookML export
        lookml = model.to_looker_view()
        assert "view: test_model" in lookml
        assert "dimension: vendor" in lookml
        assert "measure: total_spend" in lookml


class TestCLI:
    """Tests for CLI commands."""

    def test_cli_help(self):
        """Test CLI help output."""
        from typer.testing import CliRunner

        from cli import app

        runner = CliRunner()
        result = runner.invoke(app, ["--help"])

        assert result.exit_code == 0
        assert "DC Data Platform" in result.output

    def test_cli_version(self):
        """Test CLI version command."""
        from typer.testing import CliRunner

        from cli import app

        runner = CliRunner()
        result = runner.invoke(app, ["version"])

        assert result.exit_code == 0
        assert "0.1.0" in result.output

    def test_cli_status(self, settings, monkeypatch):
        """Test CLI status command."""
        from typer.testing import CliRunner

        from cli import app

        # Mock settings
        def mock_get_settings():
            return settings

        import factory.cli

        monkeypatch.setattr(factory.cli, "get_settings", mock_get_settings)

        runner = CliRunner()
        result = runner.invoke(app, ["status"])

        assert result.exit_code == 0
        assert "Status" in result.output
