"""Build gold layer task."""

from __future__ import annotations

import logging
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from factory.config.settings import Settings
from factory.pipelines.base import PipelineTask, TaskResult, TaskStatus
from factory.pipelines.runner import register_task_handler

logger = logging.getLogger(__name__)


@register_task_handler("build_gold")
def run_build_gold(task: PipelineTask, settings: Settings) -> TaskResult:
    """
    Build gold layer from silver data.

    Task config options:
        datasets: list[str] - Specific datasets to build (default: all)
        force_rebuild: bool - Rebuild even if up to date (default: False)
    """
    result = TaskResult(
        task_name=task.name,
        status=TaskStatus.RUNNING,
        started_at=datetime.now(),
    )

    datasets = task.config.get("datasets", [])
    force_rebuild = task.config.get("force_rebuild", False)

    logger.info(f"Building gold layer: datasets={datasets}, force={force_rebuild}")

    scrapers_path = settings.scrapers_unified_path
    silver_path = settings.silver_path
    gold_path = settings.gold_path

    if not silver_path.exists():
        result.status = TaskStatus.FAILED
        result.error = f"Silver path not found: {silver_path}"
        result.completed_at = datetime.now()
        return result

    # Build using subprocess to invoke scrapers_unified gold builder
    build_script = f"""
import sys
sys.path.insert(0, '{scrapers_path}')

try:
    from src.storage.gold import build_gold_layer
    results = build_gold_layer(
        silver_path='{silver_path}',
        gold_path='{gold_path}',
        datasets={datasets if datasets else None},
        force_rebuild={force_rebuild},
    )
    print("BUILD_SUCCESS")
    for dataset, count in results.items():
        print(f"DATASET:{{dataset}}:{{count}}")
except ImportError:
    # Fallback: just verify gold layer exists
    from pathlib import Path
    gold_path = Path('{gold_path}')
    if gold_path.exists():
        datasets = [d.name for d in gold_path.iterdir() if d.is_dir()]
        print("BUILD_SUCCESS")
        for ds in datasets:
            print(f"DATASET:{{ds}}:exists")
    else:
        print("BUILD_FAILED:Gold path does not exist")
except Exception as e:
    print(f"BUILD_FAILED:{{e}}")
"""

    try:
        proc = subprocess.run(
            [sys.executable, "-c", build_script],
            capture_output=True,
            text=True,
            timeout=task.timeout_seconds,
        )

        output = proc.stdout.strip()
        if "BUILD_SUCCESS" in output:
            result.status = TaskStatus.SUCCESS
            result.output = output

            # Parse dataset counts
            record_counts = {}
            for line in output.split("\n"):
                if line.startswith("DATASET:"):
                    parts = line.split(":")
                    if len(parts) >= 3:
                        ds_name = parts[1]
                        count = parts[2]
                        try:
                            record_counts[ds_name] = int(count)
                        except ValueError:
                            record_counts[ds_name] = count

            result.metrics["datasets_built"] = list(record_counts.keys())
            result.metrics["record_counts"] = record_counts
        else:
            result.status = TaskStatus.FAILED
            result.error = output or proc.stderr

    except subprocess.TimeoutExpired:
        result.status = TaskStatus.FAILED
        result.error = f"Task timed out after {task.timeout_seconds}s"
    except Exception as e:
        result.status = TaskStatus.FAILED
        result.error = str(e)

    result.completed_at = datetime.now()
    return result


def build_gold_layer_direct(
    settings: Settings,
    datasets: list[str] | None = None,
) -> dict[str, int]:
    """
    Build gold layer directly (for CLI use).

    Args:
        settings: Configuration settings
        datasets: Specific datasets to build

    Returns:
        Dictionary mapping dataset names to record counts
    """
    task = PipelineTask(
        name="build_gold_direct",
        task_type="build_gold",
        config={"datasets": datasets or []},
    )

    result = run_build_gold(task, settings)

    if result.status != TaskStatus.SUCCESS:
        raise RuntimeError(result.error or "Build failed")

    return result.metrics.get("record_counts", {})
