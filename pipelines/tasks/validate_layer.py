"""Validate layer task - validates a specific data layer against its contracts."""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from factory.config.settings import Settings
from factory.contracts.base import DataContract, validate_data_against_contract
from factory.pipelines.base import PipelineTask, TaskResult, TaskStatus
from factory.pipelines.runner import register_task_handler

logger = logging.getLogger(__name__)


@register_task_handler("validate_layer")
def run_validate_layer(task: PipelineTask, settings: Settings) -> TaskResult:
    """
    Validate a specific data layer against its contracts.

    This is a more focused version of quality_checks that validates
    a single layer with detailed reporting.

    Task config options:
        layer: str - Layer to validate: bronze, silver, gold, diamond (required)
        datasets: list[str] - Specific datasets to check (default: all)
        fail_on_warning: bool - Fail if warnings exist (default: False)
        strict: bool - Apply stricter validation rules (default: False)
    """
    result = TaskResult(
        task_name=task.name,
        status=TaskStatus.RUNNING,
        started_at=datetime.now(),
    )

    layer = task.config.get("layer")
    if not layer:
        result.status = TaskStatus.FAILED
        result.error = "Layer config is required (bronze, silver, gold, diamond)"
        result.completed_at = datetime.now()
        return result

    if layer not in ("bronze", "silver", "gold", "diamond"):
        result.status = TaskStatus.FAILED
        result.error = f"Invalid layer: {layer}. Must be bronze, silver, gold, or diamond"
        result.completed_at = datetime.now()
        return result

    datasets = task.config.get("datasets", [])
    fail_on_warning = task.config.get("fail_on_warning", False)
    strict = task.config.get("strict", False)

    logger.info(
        f"Validating layer: {layer}, datasets={datasets}, "
        f"fail_on_warning={fail_on_warning}, strict={strict}"
    )

    errors: list[str] = []
    warnings: list[str] = []
    validation_results: dict[str, Any] = {}
    checks_passed = 0
    checks_failed = 0

    # Get layer path
    layer_path = getattr(settings, f"{layer}_path", None)
    if layer_path is None or not layer_path.exists():
        result.status = TaskStatus.FAILED
        result.error = f"Layer path not found: {layer_path}"
        result.completed_at = datetime.now()
        return result

    # Load contracts from fabric/contracts/schemas
    fabric_base = Path(__file__).parent.parent.parent
    contracts_base = fabric_base / "contracts" / "schemas"
    contracts_path = contracts_base / layer

    if not contracts_path.exists():
        result.status = TaskStatus.FAILED
        result.error = f"No contracts directory found for layer: {layer}"
        result.completed_at = datetime.now()
        return result

    # Find datasets in layer (handle partitioned directories)
    layer_datasets: list[str] = []
    for item in layer_path.iterdir():
        if item.is_dir():
            # Handle partitioned paths like source=dc_arcgis
            name = item.name
            if "=" in name:
                name = name.split("=")[1]
            layer_datasets.append(name)

    if not layer_datasets:
        warnings.append(f"No datasets found in layer: {layer}")

    for ds_name in layer_datasets:
        if datasets and ds_name not in datasets:
            continue

        # Find matching contract
        contract_file = contracts_path / f"{ds_name}.yaml"
        if not contract_file.exists():
            # Try base contract for bronze
            if layer == "bronze":
                contract_file = contracts_path / "raw_payload.yaml"

        if not contract_file.exists():
            warnings.append(f"No contract found for {layer}/{ds_name}")
            continue

        try:
            contract = DataContract.from_yaml(contract_file)

            # Find data path (handle partitioned directories)
            data_path = layer_path / ds_name
            if not data_path.exists():
                # Try partitioned path
                for item in layer_path.iterdir():
                    if item.is_dir() and item.name.endswith(f"={ds_name}"):
                        data_path = item
                        break

            if not data_path.exists():
                warnings.append(f"Data path not found for {layer}/{ds_name}")
                continue

            validation = validate_data_against_contract(data_path, contract)

            ds_result = {
                "valid": validation.valid,
                "errors": validation.errors,
                "warnings": validation.warnings,
                "metrics": validation.metrics,
            }
            validation_results[ds_name] = ds_result

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

            # Apply strict mode checks
            if strict and validation.valid:
                metrics = validation.metrics
                # Check for high null rates even if within threshold
                null_rate = metrics.get("avg_null_rate", 0)
                if null_rate > 0.1:
                    warnings.append(
                        f"{layer}/{ds_name}: High null rate {null_rate:.2%} (strict mode)"
                    )

        except Exception as e:
            errors.append(f"{layer}/{ds_name}: Failed to validate - {e}")
            checks_failed += 1
            logger.exception(f"Validation error for {layer}/{ds_name}")

    result.metrics["layer"] = layer
    result.metrics["checks_passed"] = checks_passed
    result.metrics["checks_failed"] = checks_failed
    result.metrics["warnings_count"] = len(warnings)
    result.metrics["errors_count"] = len(errors)
    result.metrics["validation_results"] = validation_results
    result.metrics["errors"] = errors[:10]  # Limit stored errors
    result.metrics["warnings"] = warnings[:10]  # Limit stored warnings

    if errors:
        result.status = TaskStatus.FAILED
        result.error = f"{len(errors)} validation errors in {layer} layer"
    elif fail_on_warning and warnings:
        result.status = TaskStatus.FAILED
        result.error = f"{len(warnings)} warnings in {layer} layer (fail_on_warning=True)"
    else:
        result.status = TaskStatus.SUCCESS
        if warnings:
            result.output = f"Validation passed with {len(warnings)} warnings"
        else:
            result.output = f"Validation passed: {checks_passed} datasets validated"

    result.completed_at = datetime.now()
    return result


def validate_layer_direct(
    settings: Settings,
    layer: str,
    datasets: list[str] | None = None,
    fail_on_warning: bool = False,
    strict: bool = False,
) -> dict[str, Any]:
    """
    Validate a layer directly (for CLI use).

    Args:
        settings: Configuration settings
        layer: Layer to validate (bronze, silver, gold, diamond)
        datasets: Specific datasets to validate
        fail_on_warning: Fail on warnings
        strict: Apply strict validation

    Returns:
        Validation results dictionary
    """
    task = PipelineTask(
        name="validate_layer_direct",
        task_type="validate_layer",
        config={
            "layer": layer,
            "datasets": datasets or [],
            "fail_on_warning": fail_on_warning,
            "strict": strict,
        },
    )

    result = run_validate_layer(task, settings)

    return {
        "status": result.status.value,
        "layer": layer,
        "checks_passed": result.metrics.get("checks_passed", 0),
        "checks_failed": result.metrics.get("checks_failed", 0),
        "validation_results": result.metrics.get("validation_results", {}),
        "errors": result.metrics.get("errors", []),
        "warnings": result.metrics.get("warnings", []),
    }
