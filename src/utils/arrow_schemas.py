"""
Explicit PyArrow schema definitions for silver layer records.

These schemas prevent PyArrow from inferring null types when all values
in a column happen to be None. Instead, the proper nullable types are
enforced, ensuring downstream consumers can work with the data correctly.

Usage:
    from src.utils.arrow_schemas import get_arrow_schema

    schema = get_arrow_schema("ContractRecord")
    table = pa.Table.from_pylist(data, schema=schema)
"""

from __future__ import annotations

from typing import Any

try:
    import pyarrow as pa
    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False
    pa = None


# =============================================================================
# Common Field Definitions
# =============================================================================

def _lineage_fields() -> list[tuple[str, Any]]:
    """Common lineage fields (flattened for silver layer)."""
    if not PYARROW_AVAILABLE:
        return []
    return [
        ("lineage_source_name", pa.string()),
        ("lineage_source_url", pa.string()),
        ("lineage_scraped_at", pa.string()),  # ISO timestamp string
        ("lineage_run_id", pa.string()),
        ("lineage_raw_hash", pa.string()),
        ("lineage_schema_version", pa.string()),
        ("lineage_adapter_version", pa.string()),
    ]


def _base_record_fields() -> list[tuple[str, Any]]:
    """Common fields from BaseRecord."""
    if not PYARROW_AVAILABLE:
        return []
    return [
        ("record_id", pa.string()),
        ("validation_errors", pa.list_(pa.string())),
        ("is_valid", pa.bool_()),
        ("partition_date", pa.string()),
        ("partition_source", pa.string()),
    ]


def _tabular_record_fields() -> list[tuple[str, Any]]:
    """Common fields from TabularRecord."""
    if not PYARROW_AVAILABLE:
        return []
    return [
        ("category", pa.string()),
        ("entity_type", pa.string()),
        ("source_record_id", pa.string()),
    ]


# =============================================================================
# Record-Specific Schemas
# =============================================================================

def _contract_record_schema() -> Any:
    """Schema for ContractRecord."""
    if not PYARROW_AVAILABLE:
        return None

    fields = (
        _base_record_fields() +
        _lineage_fields() +
        _tabular_record_fields() +
        [
            # Core fields
            ("agency_name", pa.string()),
            ("supplier_name", pa.string()),
            ("contract_number", pa.string()),
            ("contract_amount", pa.string()),  # Decimal stored as string
            ("contract_status", pa.string()),
            ("contract_type", pa.string()),
            ("procurement_method", pa.string()),
            # Dates
            ("fiscal_year", pa.int32()),
            ("start_date", pa.string()),
            ("end_date", pa.string()),
            # URL
            ("contract_url", pa.string()),
            # Geo
            ("latitude", pa.float64()),
            ("longitude", pa.float64()),
        ]
    )
    return pa.schema(fields)


def _purchase_order_record_schema() -> Any:
    """Schema for PurchaseOrderRecord."""
    if not PYARROW_AVAILABLE:
        return None

    fields = (
        _base_record_fields() +
        _lineage_fields() +
        _tabular_record_fields() +
        [
            ("agency_name", pa.string()),
            ("commodity_code", pa.string()),
            ("commodity_name", pa.string()),
            ("project_title", pa.string()),
            ("procurement_type", pa.string()),
            ("contract_number", pa.string()),
            ("amount_range", pa.string()),
            ("amount_min", pa.string()),  # Decimal
            ("amount_max", pa.string()),  # Decimal
            ("status", pa.string()),
            ("description", pa.string()),
            ("fiscal_year", pa.int32()),
            ("need_by_date", pa.string()),
        ]
    )
    return pa.schema(fields)


def _payment_record_schema() -> Any:
    """Schema for PaymentRecord."""
    if not PYARROW_AVAILABLE:
        return None

    fields = (
        _base_record_fields() +
        _lineage_fields() +
        _tabular_record_fields() +
        [
            ("agency_name", pa.string()),
            ("supplier_name", pa.string()),
            ("payment_amount", pa.string()),  # Decimal
            ("payment_type", pa.string()),
            ("payment_status", pa.string()),
            ("invoice_number", pa.string()),
            ("check_number", pa.string()),
            ("fiscal_year", pa.int32()),
            ("payment_date", pa.string()),
        ]
    )
    return pa.schema(fields)


def _property_record_schema() -> Any:
    """Schema for PropertyRecord (real estate)."""
    if not PYARROW_AVAILABLE:
        return None

    fields = (
        _base_record_fields() +
        _lineage_fields() +
        _tabular_record_fields() +
        [
            # Listing info
            ("listing_type", pa.string()),
            ("property_type", pa.string()),
            ("listing_id", pa.string()),
            # Price - use float64 for proper numeric support
            ("price", pa.float64()),
            ("price_per_sqft", pa.float64()),
            ("original_price", pa.float64()),
            ("price_reduced", pa.bool_()),
            # Property details
            ("bedrooms", pa.int32()),
            ("bathrooms", pa.float32()),
            ("sqft", pa.int32()),
            ("lot_sqft", pa.int32()),
            ("year_built", pa.int16()),
            ("stories", pa.int32()),
            ("garage_spaces", pa.int32()),
            # Location
            ("address", pa.string()),
            ("city", pa.string()),
            ("state", pa.string()),
            ("zip_code", pa.string()),
            ("county", pa.string()),
            ("neighborhood", pa.string()),
            ("latitude", pa.float64()),
            ("longitude", pa.float64()),
            # Dates
            ("list_date", pa.string()),
            ("sold_date", pa.string()),
            ("days_on_market", pa.int32()),
            # Additional
            ("hoa_fee", pa.float64()),
            ("property_tax", pa.float64()),
            ("description", pa.string()),
            ("photo_count", pa.int32()),
            ("broker_name", pa.string()),
        ]
    )
    return pa.schema(fields)


def _rental_record_schema() -> Any:
    """Schema for RentalRecord."""
    if not PYARROW_AVAILABLE:
        return None

    fields = (
        _base_record_fields() +
        _lineage_fields() +
        _tabular_record_fields() +
        [
            # Property info
            ("property_name", pa.string()),
            ("unit_number", pa.string()),
            ("floor", pa.int32()),
            ("floor_plan_name", pa.string()),
            # Pricing - use float64 for proper numeric support
            ("rent_price", pa.float64()),
            ("deposit", pa.float64()),
            # Details
            ("bedrooms", pa.int32()),
            ("bathrooms", pa.float32()),
            ("sqft", pa.int32()),
            ("status", pa.string()),
            # Location
            ("address", pa.string()),
            ("city", pa.string()),
            ("state", pa.string()),
            ("zip_code", pa.string()),
            ("latitude", pa.float64()),
            ("longitude", pa.float64()),
            # Dates
            ("available_date", pa.string()),
            # Amenities
            ("amenities", pa.list_(pa.string())),
        ]
    )
    return pa.schema(fields)


def _market_stats_record_schema() -> Any:
    """Schema for MarketStatsRecord (Zillow data)."""
    if not PYARROW_AVAILABLE:
        return None

    fields = (
        _base_record_fields() +
        _lineage_fields() +
        _tabular_record_fields() +
        [
            # Location context
            ("region_name", pa.string()),
            ("region_type", pa.string()),
            ("state", pa.string()),
            ("zip_code", pa.string()),
            # Metric info
            ("metric_name", pa.string()),
            ("metric_value", pa.string()),  # Decimal stored as string
            ("metric_date", pa.string()),
            ("metric_period", pa.string()),
            # Property type context
            ("bedrooms", pa.int32()),
            ("property_type", pa.string()),
            # Historical values stored as JSON string
            ("historical_values", pa.string()),
        ]
    )
    return pa.schema(fields)


def _business_record_schema() -> Any:
    """Schema for BusinessRecord."""
    if not PYARROW_AVAILABLE:
        return None

    fields = (
        _base_record_fields() +
        _lineage_fields() +
        _tabular_record_fields() +
        [
            # Core fields
            ("business_name", pa.string()),
            ("trade_name", pa.string()),
            ("business_type", pa.string()),
            ("industry", pa.string()),
            ("naics_code", pa.string()),
            # License
            ("license_number", pa.string()),
            ("license_type", pa.string()),
            ("license_status", pa.string()),
            ("certification_type", pa.string()),
            # Contact
            ("owner_name", pa.string()),
            ("phone", pa.string()),
            ("email", pa.string()),
            ("website", pa.string()),
            # Location
            ("address", pa.string()),
            ("city", pa.string()),
            ("state", pa.string()),
            ("zip_code", pa.string()),
            ("ward", pa.string()),
            ("latitude", pa.float64()),
            ("longitude", pa.float64()),
            # Dates
            ("issue_date", pa.string()),
            ("expiration_date", pa.string()),
            ("registration_date", pa.string()),
        ]
    )
    return pa.schema(fields)


def _demographic_record_schema() -> Any:
    """Schema for DemographicRecord."""
    if not PYARROW_AVAILABLE:
        return None

    fields = (
        _base_record_fields() +
        _lineage_fields() +
        _tabular_record_fields() +
        [
            # Location context
            ("region_name", pa.string()),
            ("region_type", pa.string()),
            ("region_id", pa.string()),
            # Metric info
            ("metric_category", pa.string()),
            ("metric_name", pa.string()),
            ("metric_value", pa.string()),  # Decimal
            ("metric_unit", pa.string()),
            # Time context
            ("year", pa.int32()),
            ("period", pa.string()),
            # Comparison
            ("comparison_value", pa.string()),  # Decimal
            ("percentile", pa.float64()),
        ]
    )
    return pa.schema(fields)


# =============================================================================
# Schema Registry
# =============================================================================

_SCHEMA_REGISTRY: dict[str, Any] = {}


def _init_registry():
    """Initialize the schema registry."""
    if not PYARROW_AVAILABLE:
        return

    global _SCHEMA_REGISTRY
    _SCHEMA_REGISTRY = {
        "ContractRecord": _contract_record_schema(),
        "PurchaseOrderRecord": _purchase_order_record_schema(),
        "PaymentRecord": _payment_record_schema(),
        "PropertyRecord": _property_record_schema(),
        "RentalRecord": _rental_record_schema(),
        "MarketStatsRecord": _market_stats_record_schema(),
        "BusinessRecord": _business_record_schema(),
        "DemographicRecord": _demographic_record_schema(),
    }


def get_arrow_schema(record_type: str) -> Any | None:
    """
    Get the PyArrow schema for a record type.

    Args:
        record_type: Name of the record class (e.g., "ContractRecord")

    Returns:
        PyArrow schema or None if not found/PyArrow unavailable
    """
    if not PYARROW_AVAILABLE:
        return None

    if not _SCHEMA_REGISTRY:
        _init_registry()

    return _SCHEMA_REGISTRY.get(record_type)


def get_schema_for_records(records: list) -> Any | None:
    """
    Get the appropriate PyArrow schema for a list of records.

    Inspects the first record to determine the type.

    Args:
        records: List of Pydantic record objects

    Returns:
        PyArrow schema or None
    """
    if not PYARROW_AVAILABLE or not records:
        return None

    if not _SCHEMA_REGISTRY:
        _init_registry()

    # Get class name from first record
    first_record = records[0]
    class_name = first_record.__class__.__name__

    return _SCHEMA_REGISTRY.get(class_name)


def list_available_schemas() -> list[str]:
    """List all available schema names."""
    if not PYARROW_AVAILABLE:
        return []

    if not _SCHEMA_REGISTRY:
        _init_registry()

    return list(_SCHEMA_REGISTRY.keys())


# Initialize registry on module load
if PYARROW_AVAILABLE:
    _init_registry()


# =============================================================================
# Safe Schema Application
# =============================================================================


def apply_schema_safely(
    data: list[dict],
    schema: Any | None,
    logger: Any = None
) -> Any:
    """
    Apply schema to data with graceful fallback for mismatched fields.

    Instead of falling back to full inference when schema fails,
    this applies the schema field-by-field, preserving types for
    matching fields and using safe defaults for mismatches.

    Args:
        data: List of dicts to convert
        schema: PyArrow schema to apply
        logger: Optional logger for warnings

    Returns:
        PyArrow Table with best-effort schema application
    """
    if not PYARROW_AVAILABLE:
        raise ImportError("PyArrow required")

    if schema is None or not data:
        return pa.Table.from_pylist(data)

    # First, try direct application
    try:
        return pa.Table.from_pylist(data, schema=schema)
    except (pa.ArrowInvalid, pa.ArrowTypeError) as e:
        if logger:
            logger.warning(f"Direct schema application failed: {e}")

    # Build table column by column with schema types where possible
    columns = {}
    schema_fields = {f.name: f.type for f in schema}

    # Get all keys from data
    all_keys = set()
    for row in data:
        all_keys.update(row.keys())

    for key in all_keys:
        values = [row.get(key) for row in data]

        if key in schema_fields:
            target_type = schema_fields[key]
            try:
                # Try to cast to target type
                arr = pa.array(values, type=target_type)
                columns[key] = arr
                continue
            except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError):
                if logger:
                    logger.debug(f"Could not cast {key} to {target_type}, using inference")

        # Fallback: infer type but use safe defaults for all-null
        try:
            arr = pa.array(values)
            # If inferred as null type, force to string
            if pa.types.is_null(arr.type):
                arr = pa.array(values, type=pa.string())
            columns[key] = arr
        except Exception:
            # Last resort: convert everything to string
            columns[key] = pa.array([str(v) if v is not None else None for v in values])

    # Build table preserving schema field order where possible
    ordered_columns = []
    ordered_names = []

    # First add schema fields in order
    for field in schema:
        if field.name in columns:
            ordered_names.append(field.name)
            ordered_columns.append(columns.pop(field.name))

    # Then add any extra fields
    for name, col in columns.items():
        ordered_names.append(name)
        ordered_columns.append(col)

    return pa.Table.from_arrays(ordered_columns, names=ordered_names)


def prepare_record_for_arrow(data: dict) -> dict:
    """
    Prepare a record dict for Arrow serialization.

    Handles:
    - Decimal to float conversion
    - Complex objects to JSON strings
    - Empty list type preservation

    Args:
        data: Record dict from to_silver_dict()

    Returns:
        Cleaned dict ready for Arrow
    """
    from decimal import Decimal
    import json

    result = {}
    for key, value in data.items():
        if isinstance(value, Decimal):
            result[key] = float(value) if value is not None else None
        elif isinstance(value, dict) and key in ('historical_values', 'extra_fields'):
            # Serialize complex objects to JSON string
            result[key] = json.dumps(value) if value else None
        elif isinstance(value, list) and not value:
            # Preserve empty list
            result[key] = []
        else:
            result[key] = value

    return result
