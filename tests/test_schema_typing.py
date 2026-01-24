"""
Phase 1: Schema typing validation tests.

These tests ensure:
1. No schema fields are annotated as literal None
2. All fields have proper Optional[T] typing when nullable
3. Arrow schemas match Pydantic models
4. Records can store non-null values correctly
"""

from __future__ import annotations

import inspect
from datetime import date
from decimal import Decimal
from typing import Any, get_args, get_origin
from types import NoneType, UnionType

import pytest

from src.schemas import (
    BaseRecord,
    ContractRecord,
    PaymentRecord,
    PropertyRecord,
    PurchaseOrderRecord,
    RentalRecord,
    BusinessRecord,
)
from src.schemas.tabular import (
    MarketStatsRecord,
    DemographicRecord,
    TabularRecord,
)
from src.schemas.textual import (
    TextualRecord,
    DocumentRecord,
    ListingDescriptionRecord,
    ContractDescriptionRecord,
)


# All record classes to test
ALL_RECORD_CLASSES = [
    BaseRecord,
    TabularRecord,
    ContractRecord,
    PurchaseOrderRecord,
    PaymentRecord,
    PropertyRecord,
    RentalRecord,
    MarketStatsRecord,
    BusinessRecord,
    DemographicRecord,
    TextualRecord,
    DocumentRecord,
    ListingDescriptionRecord,
    ContractDescriptionRecord,
]


def get_field_type_info(cls: type, field_name: str) -> tuple[type | None, bool]:
    """
    Get the base type and nullability of a field.

    Returns:
        Tuple of (base_type, is_nullable)
        base_type is None if the field is typed as literal None
    """
    hints = cls.__annotations__
    if field_name not in hints:
        # Check parent classes
        for parent in cls.__mro__[1:]:
            if hasattr(parent, '__annotations__') and field_name in parent.__annotations__:
                hints = parent.__annotations__
                break

    if field_name not in hints:
        return (None, False)

    type_hint = hints[field_name]

    # Handle UnionType (X | Y syntax from Python 3.10+)
    if isinstance(type_hint, UnionType):
        args = get_args(type_hint)
        non_none_args = [a for a in args if a is not NoneType]
        is_nullable = NoneType in args
        if not non_none_args:
            # All types are None - this is the error case
            return (None, True)
        return (non_none_args[0], is_nullable)

    # Handle typing.Union
    origin = get_origin(type_hint)
    if origin is not None:
        args = get_args(type_hint)
        non_none_args = [a for a in args if a is not NoneType and a is not type(None)]
        is_nullable = type(None) in args or NoneType in args
        if not non_none_args:
            return (None, True)
        return (non_none_args[0], is_nullable)

    # Literal None type
    if type_hint is None or type_hint is NoneType:
        return (None, True)

    # Regular type
    return (type_hint, False)


class TestNoLiteralNoneTyping:
    """Test that no schema fields use literal None typing."""

    @pytest.mark.parametrize("record_class", ALL_RECORD_CLASSES)
    def test_no_literal_none_fields(self, record_class: type):
        """Verify no field is annotated as literal None."""
        fields_with_literal_none = []

        # Get all annotations including inherited ones
        all_annotations = {}
        for cls in reversed(record_class.__mro__):
            if hasattr(cls, '__annotations__'):
                all_annotations.update(cls.__annotations__)

        for field_name, type_hint in all_annotations.items():
            if field_name.startswith('_'):
                continue

            base_type, is_nullable = get_field_type_info(record_class, field_name)

            # Check if the field is typed as pure None
            if base_type is None and is_nullable:
                # This is acceptable if it's part of a Union
                # Check if the original type is literally just None
                if type_hint is None or type_hint is NoneType:
                    fields_with_literal_none.append(field_name)

        assert not fields_with_literal_none, (
            f"{record_class.__name__} has fields with literal None type: "
            f"{fields_with_literal_none}. Use Optional[T] instead."
        )

    @pytest.mark.parametrize("record_class", ALL_RECORD_CLASSES)
    def test_optional_fields_have_base_type(self, record_class: type):
        """Verify optional fields have a proper base type."""
        fields_without_base_type = []

        # Get Pydantic model fields
        model_fields = record_class.model_fields

        for field_name, field_info in model_fields.items():
            annotation = field_info.annotation
            if annotation is None:
                continue

            # Check Union types
            if isinstance(annotation, UnionType):
                args = get_args(annotation)
                non_none_args = [a for a in args if a is not NoneType]
                if not non_none_args:
                    fields_without_base_type.append(field_name)

        assert not fields_without_base_type, (
            f"{record_class.__name__} has optional fields without base type: "
            f"{fields_without_base_type}"
        )


class TestArrowSchemaCompleteness:
    """Test that arrow schemas cover all Pydantic fields."""

    def test_contract_record_arrow_schema(self):
        """Verify ContractRecord arrow schema covers all fields."""
        from src.utils.arrow_schemas import get_arrow_schema

        schema = get_arrow_schema("ContractRecord")
        if schema is None:
            pytest.skip("PyArrow not available")

        arrow_field_names = {f.name for f in schema}

        # Get fields from ContractRecord.to_silver_dict()
        from src.schemas.base import LineageInfo
        from datetime import datetime, timezone
        from uuid import uuid4

        lineage = LineageInfo(
            source_name="test",
            source_url="http://test",
            scraped_at=datetime.now(timezone.utc),
            run_id=uuid4(),
            raw_hash="test",
        )

        record = ContractRecord(
            lineage=lineage,
            agency_name="Test",
            contract_amount=Decimal("100.00"),
        )

        silver_dict = record.to_silver_dict()
        pydantic_field_names = set(silver_dict.keys())

        missing_in_arrow = pydantic_field_names - arrow_field_names

        # Allow some flexibility for dynamic fields
        allowed_missing = {'model_fields_set'}
        missing_in_arrow -= allowed_missing

        assert not missing_in_arrow, (
            f"ContractRecord has fields not in arrow schema: {missing_in_arrow}"
        )

    def test_property_record_arrow_schema(self):
        """Verify PropertyRecord arrow schema covers all fields."""
        from src.utils.arrow_schemas import get_arrow_schema

        schema = get_arrow_schema("PropertyRecord")
        if schema is None:
            pytest.skip("PyArrow not available")

        arrow_field_names = {f.name for f in schema}

        # Get fields from PropertyRecord.to_silver_dict()
        from src.schemas.base import LineageInfo
        from src.schemas.tabular import ListingType, PropertyType
        from datetime import datetime, timezone
        from uuid import uuid4

        lineage = LineageInfo(
            source_name="test",
            source_url="http://test",
            scraped_at=datetime.now(timezone.utc),
            run_id=uuid4(),
            raw_hash="test",
        )

        record = PropertyRecord(
            lineage=lineage,
            listing_type=ListingType.FOR_SALE,
            property_type=PropertyType.SINGLE_FAMILY,
        )

        silver_dict = record.to_silver_dict()
        pydantic_field_names = set(silver_dict.keys())

        missing_in_arrow = pydantic_field_names - arrow_field_names

        allowed_missing = {'model_fields_set'}
        missing_in_arrow -= allowed_missing

        assert not missing_in_arrow, (
            f"PropertyRecord has fields not in arrow schema: {missing_in_arrow}"
        )


class TestNonNullValueStorage:
    """Test that records can store non-null values correctly."""

    def test_contract_record_stores_values(self, sample_contract_record):
        """Verify ContractRecord stores non-null values."""
        assert sample_contract_record.agency_name == "DC Department of Testing"
        assert sample_contract_record.supplier_name == "Test Vendor LLC"
        assert sample_contract_record.contract_amount == Decimal("150000.00")
        assert sample_contract_record.fiscal_year == 2024
        assert sample_contract_record.start_date == date(2024, 1, 1)

    def test_property_record_stores_values(self, sample_property_record):
        """Verify PropertyRecord stores non-null values."""
        assert sample_property_record.bedrooms == 3
        assert sample_property_record.bathrooms == 2.5
        assert sample_property_record.sqft == 1800
        assert sample_property_record.price == Decimal("550000.00")
        assert sample_property_record.year_built == 1995

    def test_silver_dict_preserves_values(self, sample_contract_record):
        """Verify to_silver_dict preserves non-null values."""
        silver_dict = sample_contract_record.to_silver_dict()

        assert silver_dict['agency_name'] == "DC Department of Testing"
        assert silver_dict['supplier_name'] == "Test Vendor LLC"
        # Decimal may be serialized as string
        assert Decimal(str(silver_dict['contract_amount'])) == Decimal("150000.00")
        assert silver_dict['fiscal_year'] == 2024

    def test_silver_dict_flattens_lineage(self, sample_contract_record):
        """Verify lineage is flattened in silver dict."""
        silver_dict = sample_contract_record.to_silver_dict()

        assert 'lineage' not in silver_dict
        assert 'lineage_source_name' in silver_dict
        assert silver_dict['lineage_source_name'] == "test_source"


class TestArrowTableCreation:
    """Test that records can be converted to Arrow tables correctly."""

    def test_contract_records_to_arrow(self, sample_contract_record):
        """Test creating Arrow table from ContractRecords."""
        try:
            import pyarrow as pa
            from src.utils.arrow_schemas import get_arrow_schema
        except ImportError:
            pytest.skip("PyArrow not available")

        schema = get_arrow_schema("ContractRecord")
        data = [sample_contract_record.to_silver_dict()]

        # Should not raise
        table = pa.Table.from_pylist(data, schema=schema)

        assert len(table) == 1
        assert table.schema == schema

    def test_property_records_to_arrow(self, sample_property_record):
        """Test creating Arrow table from PropertyRecords."""
        try:
            import pyarrow as pa
            from src.utils.arrow_schemas import get_arrow_schema
        except ImportError:
            pytest.skip("PyArrow not available")

        schema = get_arrow_schema("PropertyRecord")
        data = [sample_property_record.to_silver_dict()]

        # Convert Decimal strings to float for Arrow compatibility
        # (Arrow schema uses float64 for price fields)
        for record in data:
            for key in ['price', 'price_per_sqft', 'original_price', 'hoa_fee', 'property_tax']:
                if key in record and record[key] is not None:
                    record[key] = float(record[key])

        # Should not raise
        table = pa.Table.from_pylist(data, schema=schema)

        assert len(table) == 1
        assert table.schema == schema

    def test_arrow_table_field_types_not_null(self, sample_property_record):
        """Verify Arrow schema fields are not null type."""
        try:
            import pyarrow as pa
            from src.utils.arrow_schemas import get_arrow_schema
        except ImportError:
            pytest.skip("PyArrow not available")

        schema = get_arrow_schema("PropertyRecord")

        null_type_fields = []
        for field in schema:
            if field.type == pa.null():
                null_type_fields.append(field.name)

        assert not null_type_fields, (
            f"Arrow schema has null-type fields: {null_type_fields}"
        )
