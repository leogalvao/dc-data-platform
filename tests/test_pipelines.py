"""Tests for pipeline orchestration module."""

from __future__ import annotations

from pathlib import Path

import pytest

from factory.pipelines.base import (
    Pipeline,
    PipelineResult,
    PipelineTask,
    TaskResult,
    TaskStatus,
)


class TestPipelineTask:
    """Tests for PipelineTask class."""

    def test_from_dict_basic(self):
        """Test creating task from dict."""
        task = PipelineTask.from_dict("build_gold", {
            "type": "build_gold",
            "config": {"datasets": ["spend_by_vendor"]},
        })

        assert task.name == "build_gold"
        assert task.task_type == "build_gold"
        assert task.config["datasets"] == ["spend_by_vendor"]
        assert task.enabled is True

    def test_from_dict_with_dependencies(self):
        """Test creating task with dependencies."""
        task = PipelineTask.from_dict("task_two", {
            "type": "quality_checks",
            "depends_on": ["task_one"],
            "timeout_seconds": 300,
        })

        assert task.depends_on == ["task_one"]
        assert task.timeout_seconds == 300

    def test_disabled_task(self):
        """Test creating disabled task."""
        task = PipelineTask.from_dict("disabled_task", {
            "type": "scrape",
            "enabled": False,
        })

        assert task.enabled is False


class TestPipeline:
    """Tests for Pipeline class."""

    def test_from_dict(self):
        """Test creating pipeline from dict."""
        pipeline = Pipeline.from_dict({
            "name": "test_pipeline",
            "description": "Test pipeline",
            "enabled": True,
            "fail_fast": True,
            "tasks": {
                "task_one": {"type": "build_gold"},
                "task_two": {"type": "quality_checks", "depends_on": ["task_one"]},
            },
        })

        assert pipeline.name == "test_pipeline"
        assert len(pipeline.tasks) == 2
        assert pipeline.fail_fast is True

    def test_get_task(self):
        """Test getting task by name."""
        pipeline = Pipeline.from_dict({
            "name": "test",
            "description": "",
            "tasks": {
                "task_one": {"type": "build_gold"},
                "task_two": {"type": "quality_checks"},
            },
        })

        task = pipeline.get_task("task_one")
        assert task is not None
        assert task.task_type == "build_gold"

        missing = pipeline.get_task("nonexistent")
        assert missing is None

    def test_execution_order_simple(self):
        """Test execution order with no dependencies."""
        pipeline = Pipeline.from_dict({
            "name": "test",
            "description": "",
            "tasks": {
                "task_one": {"type": "a"},
                "task_two": {"type": "b"},
            },
        })

        order = pipeline.get_execution_order()
        # Both tasks can run in first batch
        assert len(order) == 1
        assert set(order[0]) == {"task_one", "task_two"}

    def test_execution_order_with_dependencies(self):
        """Test execution order with dependencies."""
        pipeline = Pipeline.from_dict({
            "name": "test",
            "description": "",
            "tasks": {
                "task_one": {"type": "a"},
                "task_two": {"type": "b", "depends_on": ["task_one"]},
                "task_three": {"type": "c", "depends_on": ["task_two"]},
            },
        })

        order = pipeline.get_execution_order()
        assert len(order) == 3
        assert order[0] == ["task_one"]
        assert order[1] == ["task_two"]
        assert order[2] == ["task_three"]

    def test_execution_order_parallel(self):
        """Test execution order with parallel tasks."""
        pipeline = Pipeline.from_dict({
            "name": "test",
            "description": "",
            "tasks": {
                "scrape": {"type": "scrape"},
                "build_gold": {"type": "build_gold", "depends_on": ["scrape"]},
                "build_diamond": {"type": "build_diamond", "depends_on": ["scrape"]},
                "quality": {"type": "quality", "depends_on": ["build_gold", "build_diamond"]},
            },
        })

        order = pipeline.get_execution_order()
        assert len(order) == 3
        assert order[0] == ["scrape"]
        assert set(order[1]) == {"build_gold", "build_diamond"}
        assert order[2] == ["quality"]

    def test_from_yaml(self, temp_dir, sample_pipeline_yaml):
        """Test loading pipeline from YAML."""
        yaml_file = temp_dir / "pipeline.yaml"
        yaml_file.write_text(sample_pipeline_yaml)

        pipeline = Pipeline.from_yaml(yaml_file)

        assert pipeline.name == "test_pipeline"
        assert len(pipeline.tasks) == 2


class TestTaskResult:
    """Tests for TaskResult class."""

    def test_duration_calculation(self):
        """Test duration calculation."""
        from datetime import datetime, timedelta

        start = datetime.now()
        end = start + timedelta(seconds=30)

        result = TaskResult(
            task_name="test",
            status=TaskStatus.SUCCESS,
            started_at=start,
            completed_at=end,
        )

        assert result.duration_seconds == 30.0

    def test_duration_none_when_incomplete(self):
        """Test duration is None when task not completed."""
        from datetime import datetime

        result = TaskResult(
            task_name="test",
            status=TaskStatus.RUNNING,
            started_at=datetime.now(),
        )

        assert result.duration_seconds is None


class TestPipelineResult:
    """Tests for PipelineResult class."""

    def test_success_count(self):
        """Test counting successful tasks."""
        from datetime import datetime

        result = PipelineResult(
            pipeline_name="test",
            status=TaskStatus.SUCCESS,
            started_at=datetime.now(),
        )

        result.add_task_result(TaskResult("t1", TaskStatus.SUCCESS))
        result.add_task_result(TaskResult("t2", TaskStatus.SUCCESS))
        result.add_task_result(TaskResult("t3", TaskStatus.FAILED))
        result.add_task_result(TaskResult("t4", TaskStatus.SKIPPED))

        assert result.success_count == 2
        assert result.failed_count == 1

    def test_to_dict(self):
        """Test converting result to dict."""
        from datetime import datetime

        start = datetime.now()
        result = PipelineResult(
            pipeline_name="test",
            status=TaskStatus.SUCCESS,
            started_at=start,
            completed_at=start,
        )

        d = result.to_dict()
        assert d["pipeline_name"] == "test"
        assert d["status"] == "success"
        assert "started_at" in d
