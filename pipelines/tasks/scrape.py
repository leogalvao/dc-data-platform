"""Scrape task - runs scrapers_unified ingestion."""

from __future__ import annotations

import logging
import subprocess
import sys
from datetime import datetime

from config.settings import Settings
from pipelines.base import PipelineTask, TaskResult, TaskStatus
from pipelines.runner import register_task_handler

logger = logging.getLogger(__name__)


@register_task_handler("scrape")
def run_scrape(task: PipelineTask, settings: Settings) -> TaskResult:
    """
    Run scrapers_unified to ingest data from sources.

    Task config options:
        sources: list[str] - Specific sources to scrape (default: all)
        skip_existing: bool - Skip if recent data exists (default: False)
    """
    result = TaskResult(
        task_name=task.name,
        status=TaskStatus.RUNNING,
        started_at=datetime.now(),
    )

    sources = task.config.get("sources", [])
    skip_existing = task.config.get("skip_existing", False)

    logger.info(f"Running scrape task with sources={sources}")

    scrapers_path = settings.scrapers_unified_path
    if not scrapers_path.exists():
        result.status = TaskStatus.FAILED
        result.error = f"Scrapers path not found: {scrapers_path}"
        result.completed_at = datetime.now()
        return result

    # Build command
    cmd = [
        sys.executable,
        str(scrapers_path / "scripts" / "run_all.py"),
    ]

    if sources:
        cmd.extend(["--sources", ",".join(sources)])
    if skip_existing:
        cmd.append("--skip-existing")

    try:
        proc = subprocess.run(
            cmd,
            cwd=str(scrapers_path),
            capture_output=True,
            text=True,
            timeout=task.timeout_seconds,
        )

        if proc.returncode == 0:
            result.status = TaskStatus.SUCCESS
            result.output = proc.stdout
            # Parse output for metrics
            lines = proc.stdout.split("\n")
            for line in lines:
                if "records" in line.lower():
                    result.metrics["output_line"] = line
        else:
            result.status = TaskStatus.FAILED
            result.error = proc.stderr or f"Exit code: {proc.returncode}"

    except subprocess.TimeoutExpired:
        result.status = TaskStatus.FAILED
        result.error = f"Task timed out after {task.timeout_seconds}s"
    except Exception as e:
        result.status = TaskStatus.FAILED
        result.error = str(e)

    result.completed_at = datetime.now()
    return result
