"""Load warehouse task - loads Parquet data into PostgreSQL."""

from __future__ import annotations

import logging
import subprocess
import sys
from datetime import datetime

from factory.config.settings import Settings
from factory.pipelines.base import PipelineTask, TaskResult, TaskStatus
from factory.pipelines.runner import register_task_handler

logger = logging.getLogger(__name__)


@register_task_handler("load_warehouse")
def run_load_warehouse(task: PipelineTask, settings: Settings) -> TaskResult:
    """
    Load data into PostgreSQL warehouse.

    Task config options:
        load_type: str - "dimensions", "facts", "gold", "diamond", or "all" (default: "all")
        truncate: bool - Truncate tables before loading (default: False)
    """
    result = TaskResult(
        task_name=task.name,
        status=TaskStatus.RUNNING,
        started_at=datetime.now(),
    )

    load_type = task.config.get("load_type", "all")
    truncate = task.config.get("truncate", False)

    logger.info(f"Loading warehouse: type={load_type}, truncate={truncate}")

    warehouse_path = settings.warehouse_path
    if not warehouse_path.exists():
        result.status = TaskStatus.FAILED
        result.error = f"Warehouse path not found: {warehouse_path}"
        result.completed_at = datetime.now()
        return result

    # Build commands based on load type
    commands = []
    if load_type in ("dimensions", "all"):
        commands.append(["python", "-m", "warehouse", "load-dimensions"])
    if load_type in ("facts", "all"):
        commands.append(["python", "-m", "warehouse", "load-facts"])
    if load_type in ("gold", "all"):
        commands.append(["python", "-m", "warehouse", "load-gold"])
    if load_type in ("diamond", "all"):
        commands.append(["python", "-m", "warehouse", "load-diamond"])

    if truncate:
        for cmd in commands:
            cmd.append("--truncate")

    success_count = 0
    errors = []

    for cmd in commands:
        try:
            proc = subprocess.run(
                cmd,
                cwd=str(warehouse_path),
                capture_output=True,
                text=True,
                timeout=task.timeout_seconds,
                env={
                    **subprocess.os.environ,
                    "WAREHOUSE_DB_HOST": settings.warehouse_db_host,
                    "WAREHOUSE_DB_PORT": str(settings.warehouse_db_port),
                    "WAREHOUSE_DB_NAME": settings.warehouse_db_name,
                    "WAREHOUSE_DB_USER": settings.warehouse_db_user,
                    "WAREHOUSE_DB_PASSWORD": settings.warehouse_db_password,
                },
            )

            if proc.returncode == 0:
                success_count += 1
            else:
                errors.append(f"{' '.join(cmd)}: {proc.stderr}")

        except subprocess.TimeoutExpired:
            errors.append(f"{' '.join(cmd)}: Timed out")
        except Exception as e:
            errors.append(f"{' '.join(cmd)}: {e}")

    if errors:
        result.status = TaskStatus.FAILED
        result.error = "; ".join(errors)
    else:
        result.status = TaskStatus.SUCCESS

    result.metrics["commands_run"] = len(commands)
    result.metrics["success_count"] = success_count
    result.completed_at = datetime.now()
    return result
