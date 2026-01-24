"""Tests for metrics collection module."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest


class TestParquetMetrics:
    """Tests for ParquetMetrics class."""

    def test_size_mb(self):
        """Test size_mb calculation."""
        from factory.metrics.collectors.parquet_stats import ParquetMetrics

        metrics = ParquetMetrics(
            dataset_name="test",
            path="/test/path",
            row_count=1000,
            file_count=1,
            total_bytes=1024 * 1024 * 5,  # 5 MB
            column_null_rates={},
            last_modified=datetime.now(),
            schema_fields=[],
        )

        assert metrics.size_mb == 5.0

    def test_max_null_rate(self):
        """Test max_null_rate calculation."""
        from factory.metrics.collectors.parquet_stats import ParquetMetrics

        metrics = ParquetMetrics(
            dataset_name="test",
            path="/test/path",
            row_count=1000,
            file_count=1,
            total_bytes=1000,
            column_null_rates={"a": 0.1, "b": 0.3, "c": 0.05},
            last_modified=datetime.now(),
            schema_fields=["a", "b", "c"],
        )

        assert metrics.max_null_rate == 0.3

    def test_avg_null_rate(self):
        """Test avg_null_rate calculation."""
        from factory.metrics.collectors.parquet_stats import ParquetMetrics

        metrics = ParquetMetrics(
            dataset_name="test",
            path="/test/path",
            row_count=1000,
            file_count=1,
            total_bytes=1000,
            column_null_rates={"a": 0.1, "b": 0.2, "c": 0.3},
            last_modified=datetime.now(),
            schema_fields=["a", "b", "c"],
        )

        assert abs(metrics.avg_null_rate - 0.2) < 0.0001

    def test_to_dict(self):
        """Test converting to dict."""
        from factory.metrics.collectors.parquet_stats import ParquetMetrics

        metrics = ParquetMetrics(
            dataset_name="test",
            path="/test/path",
            row_count=1000,
            file_count=1,
            total_bytes=1024,
            column_null_rates={"a": 0.1},
            last_modified=datetime.now(),
            schema_fields=["a"],
        )

        d = metrics.to_dict()
        assert d["dataset_name"] == "test"
        assert d["row_count"] == 1000
        assert "size_mb" in d
        assert "max_null_rate" in d


class TestCollectParquetMetrics:
    """Tests for collect_parquet_metrics function."""

    def test_nonexistent_path(self, temp_dir):
        """Test with nonexistent path."""
        from factory.metrics.collectors.parquet_stats import collect_parquet_metrics

        result = collect_parquet_metrics(temp_dir / "nonexistent", "test")
        assert result is None

    def test_empty_directory(self, temp_dir):
        """Test with directory containing no parquet files."""
        from factory.metrics.collectors.parquet_stats import collect_parquet_metrics

        empty_dir = temp_dir / "empty"
        empty_dir.mkdir()

        result = collect_parquet_metrics(empty_dir, "test")
        assert result is None

    def test_with_parquet_data(self, mock_parquet_data):
        """Test with actual parquet data."""
        from factory.metrics.collectors.parquet_stats import collect_parquet_metrics

        result = collect_parquet_metrics(mock_parquet_data, "test")

        assert result is not None
        assert result.row_count == 5
        assert result.file_count == 1
        assert "id" in result.schema_fields
        assert "name" in result.schema_fields
        assert "value" in result.schema_fields

        # Check null rates
        assert result.column_null_rates.get("id", 0) == 0.0
        assert result.column_null_rates.get("name", 0) == 0.2  # 1 null out of 5
        assert result.column_null_rates.get("value", 0) == 0.2  # 1 null out of 5


class TestCollectLayerMetrics:
    """Tests for collect_layer_metrics function."""

    def test_nonexistent_layer(self, temp_dir):
        """Test with nonexistent layer path."""
        from factory.metrics.collectors.parquet_stats import collect_layer_metrics

        result = collect_layer_metrics(temp_dir / "nonexistent", "test")
        assert result == []

    def test_with_datasets(self, mock_parquet_data, temp_dir):
        """Test with multiple datasets."""
        from factory.metrics.collectors.parquet_stats import collect_layer_metrics

        # mock_parquet_data is a single dataset, use its parent as layer
        layer_path = mock_parquet_data.parent

        result = collect_layer_metrics(layer_path, "test_layer")

        assert len(result) == 1
        assert result[0].dataset_name == "test_dataset"


class TestCustomMetrics:
    """Tests for custom metric collectors."""

    def test_check_freshness(self, mock_parquet_data):
        """Test freshness check."""
        from factory.metrics.collectors.custom import check_freshness

        result = check_freshness(
            mock_parquet_data,
            "test_dataset",
            "silver",
            sla_hours=24,
        )

        assert result is not None
        assert result.dataset_name == "test_dataset"
        assert result.layer == "silver"
        assert result.sla_hours == 24
        # Data was just created, should be within SLA
        assert result.within_sla is True
        assert result.age_hours < 1

    def test_calculate_quality_score(self, mock_parquet_data):
        """Test quality score calculation."""
        from factory.metrics.collectors.custom import calculate_quality_score

        result = calculate_quality_score(
            mock_parquet_data,
            "test_dataset",
            "silver",
            null_threshold=0.3,
        )

        assert result is not None
        assert result.dataset_name == "test_dataset"
        assert result.row_count == 5
        assert 0 <= result.quality_score <= 1
