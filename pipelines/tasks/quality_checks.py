"""Quality check task - validates data against contracts."""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

from config.settings import Settings
from contracts.base import DataContract, validate_data_against_contract
from pipelines.base import PipelineTask, TaskResult, TaskStatus
from pipelines.runner import register_task_handler

logger = logging.getLogger(__name__)


@register_task_handler("quality_checks")
def run_quality_checks(task: PipelineTask, settings: Settings) -> TaskResult:
    """
    Run quality checks on data layers.

    Task config options:
        layers: list[str] - Layers to check: silver, gold, diamond (default: all)
        datasets: list[str] - Specific datasets to check (default: all)
        fail_on_warning: bool - Fail if warnings exist (default: False)
    """
    result = TaskResult(
        task_name=task.name,
        status=TaskStatus.RUNNING,
        started_at=datetime.now(),
    )

    layers = task.config.get("layers", ["silver", "gold", "diamond"])
    datasets = task.config.get("datasets", [])
    fail_on_warning = task.config.get("fail_on_warning", False)

    logger.info(f"Running quality checks: layers={layers}, datasets={datasets}")

    errors = []
    warnings = []
    checks_passed = 0
    checks_failed = 0

    # Load contracts from fabric/contracts/schemas
    fabric_base = Path(__file__).parent.parent.parent
    contracts_base = fabric_base / "contracts" / "schemas"

    for layer in layers:
        layer_path = getattr(settings, f"{layer}_path", None)
        if layer_path is None or not layer_path.exists():
            logger.warning(f"Layer path not found: {layer}")
            continue

        contracts_path = contracts_base / layer
        if not contracts_path.exists():
            logger.warning(f"No contracts found for layer: {layer}")
            continue

        # Find datasets in layer
        layer_datasets = [d.name for d in layer_path.iterdir() if d.is_dir()]

        for ds_name in layer_datasets:
            if datasets and ds_name not in datasets:
                continue

            # Find matching contract
            contract_file = contracts_path / f"{ds_name}.yaml"
            if not contract_file.exists():
                warnings.append(f"No contract found for {layer}/{ds_name}")
                continue

            try:
                contract = DataContract.from_yaml(contract_file)
                data_path = layer_path / ds_name

                validation = validate_data_against_contract(data_path, contract)

                if validation.valid:
                    checks_passed += 1
                    logger.info(f"[PASS] {layer}/{ds_name}")
                else:
                    checks_failed += 1
                    for err in validation.errors:
                        errors.append(f"{layer}/{ds_name}: {err}")
                        logger.error(f"[FAIL] {layer}/{ds_name}: {err}")

                for warn in validation.warnings:
                    warnings.append(f"{layer}/{ds_name}: {warn}")
                    logger.warning(f"[WARN] {layer}/{ds_name}: {warn}")

            except Exception as e:
                errors.append(f"{layer}/{ds_name}: Failed to validate - {e}")
                checks_failed += 1

    result.metrics["checks_passed"] = checks_passed
    result.metrics["checks_failed"] = checks_failed
    result.metrics["warnings"] = len(warnings)
    result.metrics["errors"] = errors[:10]  # Limit stored errors

    if errors:
        result.status = TaskStatus.FAILED
        result.error = f"{len(errors)} validation errors"
    elif fail_on_warning and warnings:
        result.status = TaskStatus.FAILED
        result.error = f"{len(warnings)} warnings (fail_on_warning=True)"
    else:
        result.status = TaskStatus.SUCCESS

    result.completed_at = datetime.now()
    return result
