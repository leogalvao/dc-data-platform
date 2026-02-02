"""Inject bronze task - injects raw data into bronze layer with validation."""

from __future__ import annotations

import hashlib
import json
import logging
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from config.settings import Settings
from contracts.base import DataContract, validate_data_against_contract
from pipelines.base import PipelineTask, TaskResult, TaskStatus
from pipelines.runner import register_task_handler

logger = logging.getLogger(__name__)


@register_task_handler("inject_bronze")
def run_inject_bronze(task: PipelineTask, settings: Settings) -> TaskResult:
    """
    Inject raw data into bronze layer with validation.

    This task takes raw data from a source path and injects it into
    the bronze layer with proper metadata and optional contract validation.

    Task config options:
        source_path: str - Path to source data file or directory (required)
        source_name: str - Name of the source system (required)
        content_type: str - MIME type of content (default: application/json)
        validate_contract: bool - Validate against bronze contract (default: True)
        run_id: str - Override run ID (default: auto-generated UUID)
    """
    result = TaskResult(
        task_name=task.name,
        status=TaskStatus.RUNNING,
        started_at=datetime.now(),
    )

    source_path_str = task.config.get("source_path")
    source_name = task.config.get("source_name")
    content_type = task.config.get("content_type", "application/json")
    validate_contract = task.config.get("validate_contract", True)
    run_id = task.config.get("run_id", str(uuid.uuid4()))

    # Validate required config
    if not source_path_str:
        result.status = TaskStatus.FAILED
        result.error = "source_path config is required"
        result.completed_at = datetime.now()
        return result

    if not source_name:
        result.status = TaskStatus.FAILED
        result.error = "source_name config is required"
        result.completed_at = datetime.now()
        return result

    source_path = Path(source_path_str).expanduser()
    if not source_path.exists():
        result.status = TaskStatus.FAILED
        result.error = f"Source path not found: {source_path}"
        result.completed_at = datetime.now()
        return result

    logger.info(
        f"Injecting bronze data: source={source_name}, "
        f"path={source_path}, validate={validate_contract}"
    )

    bronze_path = settings.bronze_path
    today = datetime.now().strftime("%Y-%m-%d")
    target_path = bronze_path / f"source={source_name}" / f"date={today}"
    target_path.mkdir(parents=True, exist_ok=True)

    records_injected = 0
    errors: list[str] = []
    warnings: list[str] = []

    try:
        # Handle single file or directory
        source_files = []
        if source_path.is_file():
            source_files = [source_path]
        else:
            # Process all files in directory
            source_files = list(source_path.glob("*"))
            source_files = [f for f in source_files if f.is_file()]

        for src_file in source_files:
            try:
                # Read content
                if content_type == "application/json":
                    with open(src_file, "r") as f:
                        content = json.load(f)
                else:
                    with open(src_file, "rb") as f:
                        content = f.read()
                        if isinstance(content, bytes):
                            content = content.decode("utf-8", errors="replace")

                # Generate payload metadata
                content_str = json.dumps(content) if isinstance(content, (dict, list)) else str(content)
                content_hash = hashlib.sha256(content_str.encode()).hexdigest()
                payload_id = str(uuid.uuid4())

                # Build bronze record
                bronze_record = {
                    "payload_id": payload_id,
                    "content_type": content_type,
                    "content": content,
                    "content_hash": content_hash,
                    "source_name": source_name,
                    "source_url": str(src_file),
                    "scraped_at": datetime.now().isoformat(),
                    "run_id": run_id,
                    "byte_size": len(content_str),
                }

                # Estimate record count for JSON arrays
                if isinstance(content, dict):
                    # Check for common array patterns
                    for key in ["features", "results", "data", "records", "items"]:
                        if key in content and isinstance(content[key], list):
                            bronze_record["record_count"] = len(content[key])
                            break
                elif isinstance(content, list):
                    bronze_record["record_count"] = len(content)

                # Write bronze record
                output_file = target_path / f"{payload_id}.json"
                with open(output_file, "w") as f:
                    json.dump(bronze_record, f, indent=2, default=str)

                records_injected += 1
                logger.debug(f"Injected: {src_file.name} -> {output_file.name}")

            except Exception as e:
                errors.append(f"Failed to process {src_file.name}: {e}")
                logger.warning(f"Failed to process {src_file.name}: {e}")

        # Validate against contract if requested
        if validate_contract and records_injected > 0:
            fabric_base = Path(__file__).parent.parent.parent
            contract_path = (
                fabric_base / "contracts" / "schemas" / "bronze" / f"{source_name}.yaml"
            )

            # Fall back to base contract if source-specific doesn't exist
            if not contract_path.exists():
                contract_path = (
                    fabric_base / "contracts" / "schemas" / "bronze" / "raw_payload.yaml"
                )

            if contract_path.exists():
                try:
                    contract = DataContract.from_yaml(contract_path)
                    validation = validate_data_against_contract(target_path, contract)

                    if not validation.valid:
                        for err in validation.errors:
                            warnings.append(f"Contract validation: {err}")

                    for warn in validation.warnings:
                        warnings.append(f"Contract validation: {warn}")

                except Exception as e:
                    warnings.append(f"Contract validation failed: {e}")
            else:
                warnings.append(f"No bronze contract found for {source_name}")

    except Exception as e:
        result.status = TaskStatus.FAILED
        result.error = str(e)
        result.completed_at = datetime.now()
        return result

    result.metrics["records_injected"] = records_injected
    result.metrics["source_name"] = source_name
    result.metrics["target_path"] = str(target_path)
    result.metrics["run_id"] = run_id
    result.metrics["errors"] = errors
    result.metrics["warnings"] = warnings

    if errors and records_injected == 0:
        result.status = TaskStatus.FAILED
        result.error = f"All records failed: {'; '.join(errors[:3])}"
    elif errors:
        result.status = TaskStatus.SUCCESS
        result.output = f"Injected {records_injected} records with {len(errors)} errors"
    else:
        result.status = TaskStatus.SUCCESS
        result.output = f"Injected {records_injected} records into bronze layer"

    result.completed_at = datetime.now()
    return result


def inject_bronze_direct(
    settings: Settings,
    source_path: str | Path,
    source_name: str,
    content_type: str = "application/json",
    validate_contract: bool = True,
) -> dict[str, Any]:
    """
    Inject data into bronze layer directly (for CLI use).

    Args:
        settings: Configuration settings
        source_path: Path to source data
        source_name: Name of the source system
        content_type: MIME type of content
        validate_contract: Whether to validate against contract

    Returns:
        Injection results dictionary
    """
    task = PipelineTask(
        name="inject_bronze_direct",
        task_type="inject_bronze",
        config={
            "source_path": str(source_path),
            "source_name": source_name,
            "content_type": content_type,
            "validate_contract": validate_contract,
        },
    )

    result = run_inject_bronze(task, settings)

    if result.status == TaskStatus.FAILED:
        raise RuntimeError(result.error or "Bronze injection failed")

    return {
        "status": result.status.value,
        "records_injected": result.metrics.get("records_injected", 0),
        "target_path": result.metrics.get("target_path", ""),
        "errors": result.metrics.get("errors", []),
        "warnings": result.metrics.get("warnings", []),
    }
