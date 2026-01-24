"""Refresh materialized views task."""

from __future__ import annotations

import logging
import subprocess
from datetime import datetime

from factory.config.settings import Settings
from factory.pipelines.base import PipelineTask, TaskResult, TaskStatus
from factory.pipelines.runner import register_task_handler

logger = logging.getLogger(__name__)


@register_task_handler("refresh_views")
def run_refresh_views(task: PipelineTask, settings: Settings) -> TaskResult:
    """
    Refresh PostgreSQL materialized views.

    Task config options:
        views: list[str] - Specific views to refresh (default: all)
        concurrent: bool - Use CONCURRENTLY option (default: True)
    """
    result = TaskResult(
        task_name=task.name,
        status=TaskStatus.RUNNING,
        started_at=datetime.now(),
    )

    views = task.config.get("views", [])
    concurrent = task.config.get("concurrent", True)

    logger.info(f"Refreshing views: {views or 'all'}, concurrent={concurrent}")

    # Default materialized views to refresh
    default_views = [
        "analytics.mv_kpi_summary",
        "analytics.mv_spend_by_ward",
        "analytics.mv_monthly_spend_trend",
        "analytics.mv_spend_by_category",
    ]

    views_to_refresh = views if views else default_views

    # Build SQL command
    refresh_sql = []
    for view in views_to_refresh:
        if concurrent:
            refresh_sql.append(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view};")
        else:
            refresh_sql.append(f"REFRESH MATERIALIZED VIEW {view};")

    sql_command = "\n".join(refresh_sql)

    try:
        # Use psql to execute
        proc = subprocess.run(
            [
                "psql",
                "-h", settings.warehouse_db_host,
                "-p", str(settings.warehouse_db_port),
                "-U", settings.warehouse_db_user,
                "-d", settings.warehouse_db_name,
                "-c", sql_command,
            ],
            capture_output=True,
            text=True,
            timeout=task.timeout_seconds,
            env={
                **subprocess.os.environ,
                "PGPASSWORD": settings.warehouse_db_password,
            },
        )

        if proc.returncode == 0:
            result.status = TaskStatus.SUCCESS
            result.output = proc.stdout
            result.metrics["views_refreshed"] = len(views_to_refresh)
        else:
            result.status = TaskStatus.FAILED
            result.error = proc.stderr

    except FileNotFoundError:
        result.status = TaskStatus.FAILED
        result.error = "psql command not found - PostgreSQL client not installed"
    except subprocess.TimeoutExpired:
        result.status = TaskStatus.FAILED
        result.error = f"Task timed out after {task.timeout_seconds}s"
    except Exception as e:
        result.status = TaskStatus.FAILED
        result.error = str(e)

    result.completed_at = datetime.now()
    return result
