"""Pipeline runner for local execution."""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from factory.config.settings import Settings
from factory.pipelines.base import (
    Pipeline,
    PipelineResult,
    PipelineTask,
    TaskResult,
    TaskStatus,
)

logger = logging.getLogger(__name__)


# Task handler registry
TaskHandler = Callable[[PipelineTask, Settings], TaskResult]
_task_handlers: dict[str, TaskHandler] = {}


def register_task_handler(task_type: str) -> Callable[[TaskHandler], TaskHandler]:
    """Decorator to register a task handler."""

    def decorator(func: TaskHandler) -> TaskHandler:
        _task_handlers[task_type] = func
        return func

    return decorator


def get_task_handler(task_type: str) -> TaskHandler | None:
    """Get task handler for task type."""
    return _task_handlers.get(task_type)


class PipelineRunner:
    """Local pipeline runner."""

    def __init__(self, settings: Settings | None = None):
        """
        Initialize pipeline runner.

        Args:
            settings: Configuration settings
        """
        self.settings = settings or Settings()
        self._register_default_handlers()

    def _register_default_handlers(self) -> None:
        """Register default task handlers."""
        # Import task modules to register their handlers
        try:
            from factory.pipelines.tasks import scrape
            from factory.pipelines.tasks import load_warehouse
            from factory.pipelines.tasks import build_gold
            from factory.pipelines.tasks import build_diamond
            from factory.pipelines.tasks import build_silver
            from factory.pipelines.tasks import quality_checks
            from factory.pipelines.tasks import validate_layer
            from factory.pipelines.tasks import inject_bronze
        except ImportError as e:
            logger.warning(f"Failed to import task handlers: {e}")

    def run(self, pipeline: Pipeline, dry_run: bool = False) -> PipelineResult:
        """
        Execute a pipeline.

        Args:
            pipeline: Pipeline to execute
            dry_run: If True, only log what would be done

        Returns:
            PipelineResult with execution details
        """
        logger.info(f"Running pipeline: {pipeline.name}")

        result = PipelineResult(
            pipeline_name=pipeline.name,
            status=TaskStatus.RUNNING,
            started_at=datetime.now(),
        )

        if not pipeline.enabled:
            logger.warning(f"Pipeline {pipeline.name} is disabled")
            result.status = TaskStatus.SKIPPED
            result.completed_at = datetime.now()
            return result

        # Get execution order
        execution_order = pipeline.get_execution_order()
        logger.info(f"Execution order: {execution_order}")

        try:
            for task_group in execution_order:
                if pipeline.fail_fast and result.status == TaskStatus.FAILED:
                    # Skip remaining tasks on failure
                    for task_name in task_group:
                        result.add_task_result(
                            TaskResult(
                                task_name=task_name,
                                status=TaskStatus.SKIPPED,
                            )
                        )
                    continue

                # Execute tasks in group (sequentially for now)
                for task_name in task_group:
                    task = pipeline.get_task(task_name)
                    if task is None:
                        continue

                    if not task.enabled:
                        result.add_task_result(
                            TaskResult(
                                task_name=task_name,
                                status=TaskStatus.SKIPPED,
                            )
                        )
                        continue

                    task_result = self._execute_task(task, dry_run)
                    result.add_task_result(task_result)

            if result.status == TaskStatus.RUNNING:
                result.status = TaskStatus.SUCCESS

        except Exception as e:
            logger.exception(f"Pipeline failed: {e}")
            result.status = TaskStatus.FAILED
            result.error = str(e)

        result.completed_at = datetime.now()
        logger.info(
            f"Pipeline {pipeline.name} completed: {result.status.value} "
            f"({result.success_count} success, {result.failed_count} failed)"
        )

        return result

    def _execute_task(self, task: PipelineTask, dry_run: bool) -> TaskResult:
        """Execute a single task."""
        logger.info(f"Executing task: {task.name} (type: {task.task_type})")

        result = TaskResult(
            task_name=task.name,
            status=TaskStatus.RUNNING,
            started_at=datetime.now(),
        )

        if dry_run:
            logger.info(f"  [DRY RUN] Would execute: {task.task_type}")
            result.status = TaskStatus.SUCCESS
            result.completed_at = datetime.now()
            return result

        handler = get_task_handler(task.task_type)
        if handler is None:
            logger.warning(f"No handler registered for task type: {task.task_type}")
            result.status = TaskStatus.FAILED
            result.error = f"Unknown task type: {task.task_type}"
            result.completed_at = datetime.now()
            return result

        try:
            handler_result = handler(task, self.settings)
            result.status = handler_result.status
            result.output = handler_result.output
            result.error = handler_result.error
            result.metrics = handler_result.metrics
        except Exception as e:
            logger.exception(f"Task {task.name} failed: {e}")
            result.status = TaskStatus.FAILED
            result.error = str(e)

        result.completed_at = datetime.now()
        logger.info(
            f"Task {task.name} completed: {result.status.value} "
            f"({result.duration_seconds:.1f}s)"
        )

        return result

    def load_pipeline(self, path: Path) -> Pipeline:
        """Load pipeline from YAML file."""
        return Pipeline.from_yaml(path)

    def list_pipelines(self, definitions_path: Path) -> list[Pipeline]:
        """List all pipeline definitions."""
        pipelines = []
        for yaml_file in definitions_path.glob("*.yaml"):
            try:
                pipeline = self.load_pipeline(yaml_file)
                pipelines.append(pipeline)
            except Exception as e:
                logger.warning(f"Failed to load pipeline from {yaml_file}: {e}")
        return pipelines
