"""Tests for data contracts module."""

from __future__ import annotations

from pathlib import Path

import pytest

from factory.contracts.base import (
    ColumnDefinition,
    DataContract,
    DataType,
    SLADefinition,
)


class TestColumnDefinition:
    """Tests for ColumnDefinition class."""

    def test_from_dict_basic(self):
        """Test creating column from dict."""
        col = ColumnDefinition.from_dict("test_col", {
            "type": "string",
            "nullable": False,
            "description": "Test column",
        })

        assert col.name == "test_col"
        assert col.data_type == DataType.STRING
        assert col.nullable is False
        assert col.description == "Test column"

    def test_from_dict_with_constraints(self):
        """Test creating column with constraints."""
        col = ColumnDefinition.from_dict("amount", {
            "type": "decimal",
            "nullable": True,
            "min_value": 0,
            "max_value": 1000000,
            "max_null_rate": 0.1,
        })

        assert col.data_type == DataType.DECIMAL
        assert col.min_value == 0
        assert col.max_value == 1000000
        assert col.max_null_rate == 0.1


class TestSLADefinition:
    """Tests for SLADefinition class."""

    def test_defaults(self):
        """Test default SLA values."""
        sla = SLADefinition()

        assert sla.freshness_hours == 24
        assert sla.min_row_count == 0
        assert sla.max_null_rate == 0.5
        assert sla.max_duplicate_rate == 0.0

    def test_from_dict(self):
        """Test creating SLA from dict."""
        sla = SLADefinition.from_dict({
            "freshness_hours": 48,
            "min_row_count": 1000,
            "max_null_rate": 0.2,
        })

        assert sla.freshness_hours == 48
        assert sla.min_row_count == 1000
        assert sla.max_null_rate == 0.2


class TestDataContract:
    """Tests for DataContract class."""

    def test_from_dict(self):
        """Test creating contract from dict."""
        contract = DataContract.from_dict({
            "name": "test_dataset",
            "version": "1.0",
            "description": "Test dataset",
            "owner": "test-team",
            "layer": "silver",
            "columns": {
                "id": {"type": "string", "nullable": False},
                "name": {"type": "string", "nullable": True},
            },
            "sla": {"freshness_hours": 24},
            "primary_key": ["id"],
        })

        assert contract.name == "test_dataset"
        assert contract.version == "1.0"
        assert contract.layer == "silver"
        assert len(contract.columns) == 2
        assert contract.sla.freshness_hours == 24
        assert contract.primary_key == ["id"]

    def test_get_column(self):
        """Test getting column by name."""
        contract = DataContract.from_dict({
            "name": "test",
            "version": "1.0",
            "description": "",
            "owner": "",
            "layer": "silver",
            "columns": {
                "id": {"type": "string", "nullable": False},
                "name": {"type": "string", "nullable": True},
            },
        })

        col = contract.get_column("id")
        assert col is not None
        assert col.name == "id"
        assert col.nullable is False

        missing = contract.get_column("nonexistent")
        assert missing is None

    def test_column_names(self):
        """Test getting column names."""
        contract = DataContract.from_dict({
            "name": "test",
            "version": "1.0",
            "description": "",
            "owner": "",
            "layer": "silver",
            "columns": {
                "id": {"type": "string"},
                "name": {"type": "string"},
                "value": {"type": "decimal"},
            },
        })

        assert contract.column_names == ["id", "name", "value"]

    def test_required_columns(self):
        """Test getting required (non-nullable) columns."""
        contract = DataContract.from_dict({
            "name": "test",
            "version": "1.0",
            "description": "",
            "owner": "",
            "layer": "silver",
            "columns": {
                "id": {"type": "string", "nullable": False},
                "name": {"type": "string", "nullable": True},
                "status": {"type": "string", "nullable": False},
            },
        })

        assert set(contract.required_columns) == {"id", "status"}

    def test_from_yaml(self, temp_dir, sample_contract_yaml):
        """Test loading contract from YAML file."""
        yaml_file = temp_dir / "contract.yaml"
        yaml_file.write_text(sample_contract_yaml)

        contract = DataContract.from_yaml(yaml_file)

        assert contract.name == "test_dataset"
        assert contract.version == "1.0"
        assert contract.owner == "test-team"
        assert len(contract.columns) == 3
