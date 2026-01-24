"""Base classes for pipeline orchestration."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Callable

import yaml

logger = logging.getLogger(__name__)


class TaskStatus(str, Enum):
    """Status of a pipeline task."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class TaskResult:
    """Result of executing a pipeline task."""

    task_name: str
    status: TaskStatus
    started_at: datetime | None = None
    completed_at: datetime | None = None
    output: Any = None
    error: str | None = None
    metrics: dict[str, Any] = field(default_factory=dict)

    @property
    def duration_seconds(self) -> float | None:
        """Duration of task execution in seconds."""
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None


@dataclass
class PipelineTask:
    """Definition of a task within a pipeline."""

    name: str
    task_type: str  # e.g., "scrape", "build_gold", "load_warehouse"
    depends_on: list[str] = field(default_factory=list)
    config: dict[str, Any] = field(default_factory=dict)
    enabled: bool = True
    retry_count: int = 0
    timeout_seconds: int = 3600

    @classmethod
    def from_dict(cls, name: str, data: dict[str, Any]) -> PipelineTask:
        """Create PipelineTask from dictionary."""
        return cls(
            name=name,
            task_type=data.get("type", name),
            depends_on=data.get("depends_on", []),
            config=data.get("config", {}),
            enabled=data.get("enabled", True),
            retry_count=data.get("retry_count", 0),
            timeout_seconds=data.get("timeout_seconds", 3600),
        )


@dataclass
class Pipeline:
    """Pipeline definition with tasks and execution configuration."""

    name: str
    description: str
    tasks: list[PipelineTask] = field(default_factory=list)
    schedule: str | None = None  # cron expression
    tags: list[str] = field(default_factory=list)
    enabled: bool = True
    max_parallel: int = 1
    fail_fast: bool = True

    @classmethod
    def from_yaml(cls, path: Path) -> Pipeline:
        """Load Pipeline from YAML file."""
        with open(path) as f:
            data = yaml.safe_load(f)
        return cls.from_dict(data)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Pipeline:
        """Create Pipeline from dictionary."""
        tasks = []
        for task_name, task_data in data.get("tasks", {}).items():
            tasks.append(PipelineTask.from_dict(task_name, task_data))

        return cls(
            name=data.get("name", ""),
            description=data.get("description", ""),
            tasks=tasks,
            schedule=data.get("schedule"),
            tags=data.get("tags", []),
            enabled=data.get("enabled", True),
            max_parallel=data.get("max_parallel", 1),
            fail_fast=data.get("fail_fast", True),
        )

    def get_task(self, name: str) -> PipelineTask | None:
        """Get task by name."""
        for task in self.tasks:
            if task.name == name:
                return task
        return None

    def get_execution_order(self) -> list[list[str]]:
        """
        Get task execution order as list of parallel task groups.

        Returns:
            List of task name lists, where tasks in same inner list
            can run in parallel.
        """
        # Build dependency graph
        remaining = {t.name for t in self.tasks if t.enabled}
        completed: set[str] = set()
        order: list[list[str]] = []

        while remaining:
            # Find tasks with all dependencies satisfied
            ready = []
            for task_name in remaining:
                task = self.get_task(task_name)
                if task and all(dep in completed for dep in task.depends_on):
                    ready.append(task_name)

            if not ready:
                # Circular dependency or missing dependency
                logger.warning(f"Cannot resolve dependencies for: {remaining}")
                break

            order.append(ready)
            for task_name in ready:
                remaining.remove(task_name)
                completed.add(task_name)

        return order


@dataclass
class PipelineResult:
    """Result of executing a pipeline."""

    pipeline_name: str
    status: TaskStatus
    started_at: datetime
    completed_at: datetime | None = None
    task_results: list[TaskResult] = field(default_factory=list)
    error: str | None = None

    @property
    def duration_seconds(self) -> float | None:
        """Duration of pipeline execution in seconds."""
        if self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None

    @property
    def success_count(self) -> int:
        """Number of successful tasks."""
        return sum(1 for r in self.task_results if r.status == TaskStatus.SUCCESS)

    @property
    def failed_count(self) -> int:
        """Number of failed tasks."""
        return sum(1 for r in self.task_results if r.status == TaskStatus.FAILED)

    def add_task_result(self, result: TaskResult) -> None:
        """Add a task result."""
        self.task_results.append(result)
        if result.status == TaskStatus.FAILED:
            self.status = TaskStatus.FAILED

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "pipeline_name": self.pipeline_name,
            "status": self.status.value,
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "duration_seconds": self.duration_seconds,
            "success_count": self.success_count,
            "failed_count": self.failed_count,
            "task_results": [
                {
                    "task_name": r.task_name,
                    "status": r.status.value,
                    "duration_seconds": r.duration_seconds,
                    "error": r.error,
                }
                for r in self.task_results
            ],
            "error": self.error,
        }
