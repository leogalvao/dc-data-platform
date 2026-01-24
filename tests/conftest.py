"""Pytest fixtures for Factory layer tests."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest


@pytest.fixture
def temp_dir():
    """Create a temporary directory for tests."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def sample_contract_yaml():
    """Sample contract YAML content."""
    return """
name: test_dataset
version: "1.0"
description: Test dataset for unit tests
owner: test-team
layer: silver

columns:
  id:
    type: string
    nullable: false
    primary_key: true
  name:
    type: string
    nullable: false
    max_null_rate: 0.01
  value:
    type: decimal
    nullable: true

sla:
  freshness_hours: 24
  min_row_count: 100
  max_null_rate: 0.3

primary_key:
  - id

tags:
  - test
"""


@pytest.fixture
def sample_pipeline_yaml():
    """Sample pipeline YAML content."""
    return """
name: test_pipeline
description: Test pipeline for unit tests
enabled: true
fail_fast: true

tasks:
  task_one:
    type: build_gold
    config:
      datasets: []
    timeout_seconds: 60

  task_two:
    type: quality_checks
    depends_on:
      - task_one
    config:
      layers:
        - gold
"""


@pytest.fixture
def mock_parquet_data(temp_dir):
    """Create mock Parquet files for testing."""
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq

        # Create sample data
        table = pa.table({
            "id": ["1", "2", "3", "4", "5"],
            "name": ["Alice", "Bob", None, "Diana", "Eve"],
            "value": [100.0, 200.0, 300.0, None, 500.0],
        })

        # Create dataset directory
        dataset_dir = temp_dir / "test_dataset"
        dataset_dir.mkdir()

        # Write Parquet file
        pq.write_table(table, dataset_dir / "data.parquet")

        return dataset_dir

    except ImportError:
        pytest.skip("PyArrow not installed")


@pytest.fixture
def settings(temp_dir):
    """Create test settings."""
    from factory.config.settings import Settings

    # Create mock directories
    scrapers_path = temp_dir / "scrapers_unified"
    warehouse_path = temp_dir / "warehouse"
    scrapers_path.mkdir()
    warehouse_path.mkdir()

    # Create data directories
    for layer in ["bronze", "silver", "gold", "diamond"]:
        (scrapers_path / "data" / layer).mkdir(parents=True)

    return Settings(
        environment="local",
        scrapers_unified_path=scrapers_path,
        warehouse_path=warehouse_path,
        log_level="DEBUG",
    )
