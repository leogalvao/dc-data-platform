"""
Phase 4: Monitoring and documentation tests.

Tests for:
1. Metrics collection and tracking
2. Null rate analysis
3. Pipeline health summary
4. Metrics output formats (JSON, Prometheus, CSV)
"""

from __future__ import annotations

import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pytest

from src.observability.metrics import (
    SourceMetrics,
    MetricsCollector,
    MetricsWriter,
    PipelineHealthSummary,
    Timer,
    analyze_null_rates,
    compare_null_rates,
)


class TestSourceMetrics:
    """Test SourceMetrics data collection."""

    def test_record_request_success(self):
        """Test recording successful request."""
        metrics = SourceMetrics(source_name="test")
        metrics.record_request(success=True, latency_ms=100.0)

        assert metrics.requests_total == 1
        assert metrics.requests_success == 1
        assert metrics.requests_failed == 0
        assert 100.0 in metrics.fetch_latencies

    def test_record_request_failure(self):
        """Test recording failed request."""
        metrics = SourceMetrics(source_name="test")
        metrics.record_request(success=False, latency_ms=50.0)

        assert metrics.requests_total == 1
        assert metrics.requests_success == 0
        assert metrics.requests_failed == 1

    def test_success_rate_calculation(self):
        """Test success rate calculation."""
        metrics = SourceMetrics(source_name="test")

        # 8 success, 2 failure = 80%
        for _ in range(8):
            metrics.record_request(success=True, latency_ms=100.0)
        for _ in range(2):
            metrics.record_request(success=False, latency_ms=100.0)

        assert metrics.success_rate == 80.0

    def test_success_rate_zero_requests(self):
        """Test success rate with no requests."""
        metrics = SourceMetrics(source_name="test")
        assert metrics.success_rate == 0.0

    def test_record_field_stats(self):
        """Test field statistics tracking."""
        metrics = SourceMetrics(source_name="test")

        # Record some data
        metrics.record_field_stats({"name": "Alice", "age": 30, "email": None})
        metrics.record_field_stats({"name": "Bob", "age": None, "email": None})
        metrics.record_field_stats({"name": None, "age": 25, "email": "test@example.com"})

        # Check null rates
        null_rates = metrics.get_null_rates()

        # name: 1 null out of 3 = 33.33%
        assert abs(null_rates["name"] - 33.33) < 1

        # age: 1 null out of 3 = 33.33%
        assert abs(null_rates["age"] - 33.33) < 1

        # email: 2 null out of 3 = 66.67%
        assert abs(null_rates["email"] - 66.67) < 1

    def test_to_dict_structure(self):
        """Test dictionary export structure."""
        metrics = SourceMetrics(source_name="test")
        metrics.record_request(success=True, latency_ms=100.0)
        metrics.records_written = 10

        result = metrics.to_dict()

        assert "source_name" in result
        assert "requests" in result
        assert "records" in result
        assert "latency_ms" in result
        assert "null_rates" in result


class TestMetricsCollector:
    """Test MetricsCollector aggregation."""

    def test_get_or_create(self):
        """Test get_or_create creates new metrics."""
        collector = MetricsCollector()

        metrics1 = collector.get_or_create("source1")
        metrics2 = collector.get_or_create("source1")
        metrics3 = collector.get_or_create("source2")

        assert metrics1 is metrics2  # Same instance
        assert metrics1 is not metrics3  # Different instance

    def test_record_request(self):
        """Test recording requests through collector."""
        collector = MetricsCollector()
        collector.record_request("test", success=True, latency_ms=100.0)

        metrics = collector.get_or_create("test")
        assert metrics.requests_total == 1

    def test_record_records(self):
        """Test recording record counts."""
        collector = MetricsCollector()
        collector.record_records(
            "test",
            discovered=100,
            processed=90,
            written=85,
            quarantined=5,
        )

        metrics = collector.get_or_create("test")
        assert metrics.records_discovered == 100
        assert metrics.records_processed == 90
        assert metrics.records_written == 85
        assert metrics.records_quarantined == 5

    def test_get_all_metrics(self):
        """Test getting all metrics."""
        collector = MetricsCollector()
        collector.record_request("source1", success=True, latency_ms=100.0)
        collector.record_request("source2", success=True, latency_ms=150.0)

        result = collector.get_all_metrics()

        assert "sources" in result
        assert "source1" in result["sources"]
        assert "source2" in result["sources"]
        assert "totals" in result

    def test_calculate_totals(self):
        """Test totals calculation."""
        collector = MetricsCollector()
        collector.record_records("s1", written=100, quarantined=10)
        collector.record_records("s2", written=200, quarantined=20)

        result = collector.get_all_metrics()
        totals = result["totals"]

        assert totals["records_written"] == 300
        assert totals["records_quarantined"] == 30


class TestPipelineHealthSummary:
    """Test PipelineHealthSummary."""

    def test_creation(self):
        """Test summary creation."""
        summary = PipelineHealthSummary(
            run_id="test-123",
            total_records_processed=1000,
            total_records_quarantined=50,
        )

        assert summary.run_id == "test-123"
        assert summary.overall_success_rate == 95.0

    def test_zero_records(self):
        """Test with zero records."""
        summary = PipelineHealthSummary(run_id="test")
        assert summary.overall_success_rate == 0.0

    def test_to_dict(self):
        """Test dictionary export."""
        summary = PipelineHealthSummary(
            run_id="test-123",
            total_sources_run=5,
            total_records_processed=1000,
        )

        result = summary.to_dict()

        assert result["run_id"] == "test-123"
        assert result["total_sources_run"] == 5
        assert "timestamp" in result


class TestMetricsWriter:
    """Test MetricsWriter output formats."""

    @pytest.fixture
    def sample_collector(self) -> MetricsCollector:
        """Create collector with sample data."""
        collector = MetricsCollector()

        # Source 1
        collector.record_request("source1", success=True, latency_ms=100.0)
        collector.record_records("source1", discovered=100, processed=90, written=85, quarantined=5)
        metrics1 = collector.get_or_create("source1")
        metrics1.record_field_stats({"field_a": "value", "field_b": None})
        metrics1.record_field_stats({"field_a": None, "field_b": "value"})

        # Source 2
        collector.record_request("source2", success=True, latency_ms=150.0)
        collector.record_records("source2", discovered=50, processed=48, written=45, quarantined=3)

        return collector

    def test_write_json(self, sample_collector):
        """Test JSON output."""
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            path = f.name

        try:
            writer = MetricsWriter(sample_collector, "test-run")
            writer.write_json(path)

            with open(path) as f:
                data = json.load(f)

            assert data["run_id"] == "test-run"
            assert "sources" in data
            assert "source1" in data["sources"]
        finally:
            Path(path).unlink(missing_ok=True)

    def test_write_prometheus(self, sample_collector):
        """Test Prometheus output."""
        with tempfile.NamedTemporaryFile(suffix=".prom", delete=False) as f:
            path = f.name

        try:
            writer = MetricsWriter(sample_collector, "test-run")
            writer.write_prometheus(path)

            with open(path) as f:
                content = f.read()

            # Check for expected metrics
            assert "# HELP pipeline_records_total" in content
            assert "# TYPE pipeline_records_total" in content
            assert "source_records_written" in content
            assert "field_null_rate" in content
        finally:
            Path(path).unlink(missing_ok=True)

    def test_write_csv(self, sample_collector):
        """Test CSV output."""
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
            path = f.name

        try:
            writer = MetricsWriter(sample_collector, "test-run")
            writer.write_csv(path)

            with open(path) as f:
                lines = f.readlines()

            # Check header
            assert "timestamp" in lines[0]
            assert "run_id" in lines[0]
            assert "source" in lines[0]
            assert "field" in lines[0]

            # Should have data rows
            assert len(lines) > 1
        finally:
            Path(path).unlink(missing_ok=True)

    def test_build_health_summary(self, sample_collector):
        """Test health summary building."""
        writer = MetricsWriter(sample_collector, "test-run")
        summary = writer.build_health_summary()

        assert summary.run_id == "test-run"
        assert summary.total_sources_run == 2
        assert "source1" in summary.source_summaries


class TestNullRateAnalysis:
    """Test null rate analysis functions."""

    @pytest.fixture
    def sample_metrics_file(self, tmp_path: Path) -> Path:
        """Create sample metrics file."""
        metrics = {
            "run_id": "test-run",
            "sources": {
                "source1": {
                    "null_rates": {
                        "field_a": 50.0,
                        "field_b": 95.0,  # Above threshold
                        "field_c": 10.0,
                    }
                },
                "source2": {
                    "null_rates": {
                        "field_a": 100.0,  # Above threshold
                        "field_d": 5.0,
                    }
                }
            }
        }

        path = tmp_path / "metrics.json"
        with open(path, "w") as f:
            json.dump(metrics, f)

        return path

    def test_analyze_null_rates(self, sample_metrics_file):
        """Test null rate analysis."""
        result = analyze_null_rates(str(sample_metrics_file), threshold=90.0)

        assert result["run_id"] == "test-run"
        assert len(result["alerts"]) == 2  # field_b and field_a

        # Check alert details
        alerts = result["alerts"]
        field_b_alert = next((a for a in alerts if a["field"] == "field_b"), None)
        assert field_b_alert is not None
        assert field_b_alert["null_rate"] == 95.0

    def test_analyze_null_rates_no_alerts(self, sample_metrics_file):
        """Test with high threshold (no alerts)."""
        result = analyze_null_rates(str(sample_metrics_file), threshold=101.0)

        assert len(result["alerts"]) == 0

    def test_compare_null_rates(self, tmp_path: Path):
        """Test null rate comparison."""
        # Previous metrics
        prev_metrics = {
            "run_id": "prev-run",
            "sources": {
                "source1": {
                    "null_rates": {
                        "field_a": 50.0,
                        "field_b": 20.0,
                    }
                }
            }
        }

        # Current metrics (field_b increased significantly)
        curr_metrics = {
            "run_id": "curr-run",
            "sources": {
                "source1": {
                    "null_rates": {
                        "field_a": 52.0,  # Small change
                        "field_b": 45.0,  # Large increase
                    }
                }
            }
        }

        prev_path = tmp_path / "prev.json"
        curr_path = tmp_path / "curr.json"

        with open(prev_path, "w") as f:
            json.dump(prev_metrics, f)
        with open(curr_path, "w") as f:
            json.dump(curr_metrics, f)

        result = compare_null_rates(str(curr_path), str(prev_path), threshold_change=10.0)

        assert result["current_run_id"] == "curr-run"
        assert result["previous_run_id"] == "prev-run"

        # Should detect field_b change (25 percentage points)
        assert len(result["changes"]) == 1
        assert result["changes"][0]["field"] == "field_b"
        assert result["changes"][0]["direction"] == "increased"


class TestTimer:
    """Test Timer context manager."""

    def test_timer_measures_time(self):
        """Test timer measures elapsed time."""
        import time

        with Timer() as timer:
            time.sleep(0.05)  # 50ms

        # Should be around 50ms
        assert timer.elapsed_ms >= 40
        assert timer.elapsed_ms < 200

    def test_timer_default_values(self):
        """Test timer default values."""
        timer = Timer()
        assert timer.start_time == 0
        assert timer.elapsed_ms == 0
