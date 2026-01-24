"""
Phase 2: Quarantine investigation and reduction tests.

Tests for:
1. Adapter validation robustness
2. Type coercion handling edge cases
3. Error classification
4. Quarantine rate monitoring
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

import pytest

from src.utils.type_coercion import (
    safe_int,
    safe_float,
    safe_decimal,
    safe_date,
    safe_bool,
    safe_string,
)


class TestTypeCoercionRobustness:
    """Test type coercion functions handle edge cases that cause quarantine."""

    # -------------------------------------------------------------------------
    # safe_int tests
    # -------------------------------------------------------------------------

    @pytest.mark.parametrize("value,expected", [
        (None, None),
        ("", None),
        ("nan", None),
        ("NaN", None),
        ("null", None),
        ("N/A", None),
        ("-", None),
        ("   ", None),
        (float("nan"), None),
        # Valid conversions
        (42, 42),
        (42.7, 42),  # Truncates
        ("42", 42),
        ("  42  ", 42),
        ("$1,234", 1234),
        ("-100", -100),
    ])
    def test_safe_int_edge_cases(self, value: Any, expected: int | None):
        """Test safe_int handles various edge cases."""
        result = safe_int(value)
        assert result == expected, f"safe_int({value!r}) = {result}, expected {expected}"

    # -------------------------------------------------------------------------
    # safe_float tests
    # -------------------------------------------------------------------------

    @pytest.mark.parametrize("value,expected", [
        (None, None),
        ("", None),
        ("nan", None),
        ("NaN", None),
        ("null", None),
        ("none", None),
        ("N/A", None),
        ("-", None),
        (float("nan"), None),
        # Valid conversions
        (42.5, 42.5),
        ("42.5", 42.5),
        ("  42.5  ", 42.5),
        ("$1,234.56", 1234.56),
        ("-100.5", -100.5),
    ])
    def test_safe_float_edge_cases(self, value: Any, expected: float | None):
        """Test safe_float handles various edge cases."""
        result = safe_float(value)
        if expected is None:
            assert result is None, f"safe_float({value!r}) = {result}, expected None"
        else:
            assert abs(result - expected) < 0.001, f"safe_float({value!r}) = {result}, expected {expected}"

    # -------------------------------------------------------------------------
    # safe_decimal tests
    # -------------------------------------------------------------------------

    @pytest.mark.parametrize("value,expected", [
        (None, None),
        ("", None),
        ("nan", None),
        ("null", None),
        # Valid conversions
        (Decimal("123.45"), Decimal("123.45")),
        ("123.45", Decimal("123.45")),
        ("$1,234.56", Decimal("1234.56")),
    ])
    def test_safe_decimal_edge_cases(self, value: Any, expected: Decimal | None):
        """Test safe_decimal handles various edge cases."""
        result = safe_decimal(value)
        assert result == expected, f"safe_decimal({value!r}) = {result}, expected {expected}"

    # -------------------------------------------------------------------------
    # safe_date tests
    # -------------------------------------------------------------------------

    @pytest.mark.parametrize("value,expected", [
        (None, None),
        ("", None),
        ("null", None),
        ("none", None),
        ("N/A", None),
        ("-", None),
        # Valid conversions
        (date(2024, 1, 15), date(2024, 1, 15)),
        ("2024-01-15", date(2024, 1, 15)),
        # ESRI timestamp (milliseconds since epoch) - 2024-01-15
        (1705276800000, date(2024, 1, 15)),
    ])
    def test_safe_date_edge_cases(self, value: Any, expected: date | None):
        """Test safe_date handles various edge cases."""
        result = safe_date(value)
        assert result == expected, f"safe_date({value!r}) = {result}, expected {expected}"

    def test_safe_date_various_formats(self):
        """Test safe_date handles various date formats."""
        # ISO format
        assert safe_date("2024-01-15") == date(2024, 1, 15)

        # US format
        assert safe_date("01/15/2024") == date(2024, 1, 15)

        # Month name
        assert safe_date("January 15, 2024") == date(2024, 1, 15)

        # ESRI timestamp
        assert safe_date(1705276800000) == date(2024, 1, 15)


class TestContractAdapterValidation:
    """Test dc_contracts adapter validation and normalization."""

    def test_validate_valid_contract(self, sample_raw_contract_api_response):
        """Test validation passes for valid contract data."""
        from src.adapters.dc_arcgis.contracts import ContractsAdapter

        adapter = ContractsAdapter.__new__(ContractsAdapter)
        is_valid, errors = adapter.validate(sample_raw_contract_api_response)

        assert is_valid is True
        assert errors == []

    def test_validate_missing_agency(self):
        """Test validation fails for missing agency."""
        from src.adapters.dc_arcgis.contracts import ContractsAdapter

        adapter = ContractsAdapter.__new__(ContractsAdapter)
        data = {"CONTRACTAMOUNT": 100000}

        is_valid, errors = adapter.validate(data)

        assert is_valid is False
        assert any("AGENCY_NAME" in e for e in errors)

    def test_validate_invalid_amount(self):
        """Test validation handles invalid contract amount."""
        from src.adapters.dc_arcgis.contracts import ContractsAdapter

        adapter = ContractsAdapter.__new__(ContractsAdapter)
        data = {
            "AGENCY_NAME": "Test Agency",
            "CONTRACTAMOUNT": "not-a-number",
        }

        is_valid, errors = adapter.validate(data)

        assert is_valid is False
        assert any("amount" in e.lower() for e in errors)

    def test_validate_agency_fallback(self):
        """Test validation uses AGENCY as fallback for AGENCY_NAME."""
        from src.adapters.dc_arcgis.contracts import ContractsAdapter

        adapter = ContractsAdapter.__new__(ContractsAdapter)
        data = {
            "AGENCY": "Test Agency",  # Fallback field
            "CONTRACTAMOUNT": 100000,
        }

        is_valid, errors = adapter.validate(data)
        assert is_valid is True


class TestResidentialCAMAValidation:
    """Test dc_residential_cama adapter validation and normalization."""

    def test_validate_valid_cama(self, sample_raw_cama_api_response):
        """Test validation passes for valid CAMA data."""
        from src.adapters.dc_arcgis.residential_cama import ResidentialCAMAAdapter

        adapter = ResidentialCAMAAdapter.__new__(ResidentialCAMAAdapter)
        is_valid, errors = adapter.validate(sample_raw_cama_api_response)

        assert is_valid is True
        assert errors == []

    def test_validate_missing_identifier(self):
        """Test validation fails for missing identifier."""
        from src.adapters.dc_arcgis.residential_cama import ResidentialCAMAAdapter

        adapter = ResidentialCAMAAdapter.__new__(ResidentialCAMAAdapter)
        data = {"BATHRM": 2, "BEDRM": 3}  # Missing SSL and OBJECTID

        is_valid, errors = adapter.validate(data)

        assert is_valid is False
        assert any("SSL" in e or "OBJECTID" in e for e in errors)

    def test_validate_negative_values(self):
        """Test validation catches negative values."""
        from src.adapters.dc_arcgis.residential_cama import ResidentialCAMAAdapter

        adapter = ResidentialCAMAAdapter.__new__(ResidentialCAMAAdapter)
        data = {
            "OBJECTID": 123,
            "BATHRM": -1,  # Invalid
            "BEDRM": 3,
        }

        is_valid, errors = adapter.validate(data)

        assert is_valid is False
        assert any("BATHRM" in e for e in errors)

    def test_validate_invalid_year_built(self):
        """Test validation catches invalid year built."""
        from src.adapters.dc_arcgis.residential_cama import ResidentialCAMAAdapter

        adapter = ResidentialCAMAAdapter.__new__(ResidentialCAMAAdapter)
        data = {
            "OBJECTID": 123,
            "AYB": 1500,  # Too old
        }

        is_valid, errors = adapter.validate(data)

        assert is_valid is False
        assert any("year" in e.lower() for e in errors)


class TestQuarantineReduction:
    """Tests to verify quarantine reduction strategies work."""

    def test_normalize_handles_null_fields(self, sample_raw_cama_api_response):
        """Test normalization handles null fields gracefully."""
        from src.adapters.dc_arcgis.residential_cama import ResidentialCAMAAdapter
        from src.schemas.base import LineageInfo
        from datetime import datetime, timezone
        from uuid import uuid4

        # Create adapter with minimal setup
        adapter = ResidentialCAMAAdapter.__new__(ResidentialCAMAAdapter)

        # Mock the _make_lineage method
        def mock_make_lineage():
            return LineageInfo(
                source_name="test",
                source_url="http://test",
                scraped_at=datetime.now(timezone.utc),
                run_id=uuid4(),
                raw_hash="test",
            )
        adapter._make_lineage = mock_make_lineage

        # Test with fields set to None
        data = sample_raw_cama_api_response.copy()
        data["BATHRM"] = None
        data["BEDRM"] = None
        data["PRICE"] = None

        # Should not raise
        record = adapter.normalize(data)

        # Verify record is created with None values
        assert record.bedrooms is None
        assert record.bathrooms is None
        assert record.price is None

    def test_normalize_handles_string_numbers(self, sample_raw_cama_api_response):
        """Test normalization handles string numbers correctly."""
        from src.adapters.dc_arcgis.residential_cama import ResidentialCAMAAdapter
        from src.schemas.base import LineageInfo
        from datetime import datetime, timezone
        from uuid import uuid4

        adapter = ResidentialCAMAAdapter.__new__(ResidentialCAMAAdapter)

        def mock_make_lineage():
            return LineageInfo(
                source_name="test",
                source_url="http://test",
                scraped_at=datetime.now(timezone.utc),
                run_id=uuid4(),
                raw_hash="test",
            )
        adapter._make_lineage = mock_make_lineage

        # Test with string numbers (no half baths to get clean bathroom count)
        data = sample_raw_cama_api_response.copy()
        data["BATHRM"] = "2"
        data["HF_BATHRM"] = "0"  # No half baths
        data["BEDRM"] = "3"
        data["PRICE"] = "550000"

        record = adapter.normalize(data)

        assert record.bedrooms == 3
        assert record.bathrooms == 2.0  # 2 full + 0*0.5 half
        assert record.price == 550000.0
