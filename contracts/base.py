"""Base classes for data contracts."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)


class DataType(str, Enum):
    """Supported data types for contract columns."""

    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    DECIMAL = "decimal"
    BOOLEAN = "boolean"
    DATE = "date"
    DATETIME = "datetime"
    TIMESTAMP = "timestamp"
    ARRAY = "array"
    OBJECT = "object"


@dataclass
class ColumnDefinition:
    """Definition of a column in a data contract."""

    name: str
    data_type: DataType
    nullable: bool = True
    description: str = ""
    primary_key: bool = False
    max_null_rate: float | None = None
    allowed_values: list[Any] | None = None
    min_value: float | None = None
    max_value: float | None = None
    pattern: str | None = None

    @classmethod
    def from_dict(cls, name: str, data: dict[str, Any]) -> ColumnDefinition:
        """Create ColumnDefinition from dictionary."""
        return cls(
            name=name,
            data_type=DataType(data.get("type", "string")),
            nullable=data.get("nullable", True),
            description=data.get("description", ""),
            primary_key=data.get("primary_key", False),
            max_null_rate=data.get("max_null_rate"),
            allowed_values=data.get("allowed_values"),
            min_value=data.get("min_value"),
            max_value=data.get("max_value"),
            pattern=data.get("pattern"),
        )


@dataclass
class SLADefinition:
    """Service Level Agreement definition for a dataset."""

    freshness_hours: int = 24
    min_row_count: int = 0
    max_null_rate: float = 0.5
    max_duplicate_rate: float = 0.0

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SLADefinition:
        """Create SLADefinition from dictionary."""
        return cls(
            freshness_hours=data.get("freshness_hours", 24),
            min_row_count=data.get("min_row_count", 0),
            max_null_rate=data.get("max_null_rate", 0.5),
            max_duplicate_rate=data.get("max_duplicate_rate", 0.0),
        )


@dataclass
class DataContract:
    """Data contract defining schema, SLAs, and ownership for a dataset."""

    name: str
    version: str
    description: str
    owner: str
    layer: str  # bronze, silver, gold, diamond
    columns: list[ColumnDefinition] = field(default_factory=list)
    sla: SLADefinition = field(default_factory=SLADefinition)
    primary_key: list[str] = field(default_factory=list)
    partition_columns: list[str] = field(default_factory=list)
    tags: list[str] = field(default_factory=list)
    created_at: datetime | None = None
    updated_at: datetime | None = None

    @classmethod
    def from_yaml(cls, path: Path) -> DataContract:
        """Load a DataContract from a YAML file."""
        with open(path) as f:
            data = yaml.safe_load(f)
        return cls.from_dict(data)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DataContract:
        """Create DataContract from dictionary."""
        columns = []
        for col_name, col_data in data.get("columns", {}).items():
            columns.append(ColumnDefinition.from_dict(col_name, col_data))

        sla_data = data.get("sla", {})
        sla = SLADefinition.from_dict(sla_data)

        return cls(
            name=data.get("name", ""),
            version=data.get("version", "1.0"),
            description=data.get("description", ""),
            owner=data.get("owner", ""),
            layer=data.get("layer", "silver"),
            columns=columns,
            sla=sla,
            primary_key=data.get("primary_key", []),
            partition_columns=data.get("partition_columns", []),
            tags=data.get("tags", []),
        )

    def get_column(self, name: str) -> ColumnDefinition | None:
        """Get column definition by name."""
        for col in self.columns:
            if col.name == name:
                return col
        return None

    @property
    def column_names(self) -> list[str]:
        """List of column names."""
        return [col.name for col in self.columns]

    @property
    def required_columns(self) -> list[str]:
        """List of non-nullable column names."""
        return [col.name for col in self.columns if not col.nullable]


@dataclass
class ContractValidationResult:
    """Result of validating data against a contract."""

    contract_name: str
    valid: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    metrics: dict[str, Any] = field(default_factory=dict)

    def add_error(self, message: str) -> None:
        """Add an error message."""
        self.errors.append(message)
        self.valid = False

    def add_warning(self, message: str) -> None:
        """Add a warning message."""
        self.warnings.append(message)


def load_contract(path: Path) -> DataContract:
    """
    Load a data contract from a YAML file.

    Args:
        path: Path to the YAML contract file

    Returns:
        DataContract instance
    """
    logger.info(f"Loading contract from {path}")
    return DataContract.from_yaml(path)


def validate_data_against_contract(
    data_path: Path,
    contract: DataContract,
) -> ContractValidationResult:
    """
    Validate data against a contract.

    Args:
        data_path: Path to Parquet data file or directory
        contract: DataContract to validate against

    Returns:
        ContractValidationResult with validation status
    """
    result = ContractValidationResult(
        contract_name=contract.name,
        valid=True,
    )

    try:
        import pyarrow.parquet as pq
    except ImportError:
        result.add_error("PyArrow not installed, cannot validate Parquet data")
        return result

    if not data_path.exists():
        result.add_error(f"Data path not found: {data_path}")
        return result

    # Find parquet files
    if data_path.is_file():
        parquet_files = [data_path]
    else:
        parquet_files = list(data_path.rglob("*.parquet"))

    if not parquet_files:
        result.add_error(f"No Parquet files found at {data_path}")
        return result

    # Read schema from first file
    try:
        schema = pq.read_schema(parquet_files[0])
        actual_columns = set(schema.names)
    except Exception as e:
        result.add_error(f"Failed to read Parquet schema: {e}")
        return result

    # Validate required columns exist
    expected_columns = set(contract.column_names)
    missing_columns = expected_columns - actual_columns
    extra_columns = actual_columns - expected_columns

    for col in missing_columns:
        col_def = contract.get_column(col)
        if col_def and not col_def.nullable:
            result.add_error(f"Required column missing: {col}")
        else:
            result.add_warning(f"Expected column missing: {col}")

    for col in extra_columns:
        result.add_warning(f"Unexpected column in data: {col}")

    # Count total rows and check SLAs
    total_rows = 0
    for pf in parquet_files:
        try:
            metadata = pq.read_metadata(pf)
            total_rows += metadata.num_rows
        except Exception as e:
            result.add_warning(f"Failed to read metadata from {pf}: {e}")

    result.metrics["row_count"] = total_rows

    if total_rows < contract.sla.min_row_count:
        result.add_error(
            f"Row count {total_rows} below SLA minimum {contract.sla.min_row_count}"
        )

    # Validate null rates for sampled data
    try:
        sample_table = pq.read_table(parquet_files[0])
        for col_def in contract.columns:
            if col_def.name in sample_table.column_names:
                col_data = sample_table.column(col_def.name)
                null_rate = col_data.null_count / len(col_data) if len(col_data) > 0 else 0

                max_null = col_def.max_null_rate or contract.sla.max_null_rate
                if null_rate > max_null:
                    result.add_error(
                        f"Column {col_def.name} null rate {null_rate:.1%} exceeds max {max_null:.1%}"
                    )

                result.metrics[f"null_rate_{col_def.name}"] = null_rate
    except Exception as e:
        result.add_warning(f"Failed to validate null rates: {e}")

    return result
