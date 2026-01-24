"""
Purchase Orders adapter for DC ArcGIS MapServer/16.

Migrated from legacy/purchase_orders.py with:
- Amount range parsing (string to min/max values)
- Quarter field handling
- Full field mapping to PurchaseOrderRecord schema

Performance optimizations:
- Pre-compiled regex patterns at module level
"""

from __future__ import annotations

import re
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any

from ...core.registry import register_scraper
from ...schemas.base import LineageInfo
from ...schemas.metadata import SourceConfig, SourceType
from ...schemas.tabular import PurchaseOrderRecord
from .base import DCArcGISBaseAdapter

# Pre-compiled regex patterns for amount range parsing
# Pattern -> (min_amount, max_amount) mapping
_AMOUNT_PATTERNS_NO_DOLLAR = [
    (re.compile(r"0\s*[-–]\s*10000", re.IGNORECASE), (0, 10000)),
    (re.compile(r"10001\s*[-–]\s*100000", re.IGNORECASE), (10001, 100000)),
    (re.compile(r"100001\s*[-–]\s*250000", re.IGNORECASE), (100001, 250000)),
    (re.compile(r"250001\s*[-–]\s*500000", re.IGNORECASE), (250001, 500000)),
    (re.compile(r"500001\s*[-–]\s*1000000", re.IGNORECASE), (500001, 1000000)),
    (re.compile(r"1000001\s*[-–]\s*5000000", re.IGNORECASE), (1000001, 5000000)),
    (re.compile(r"5000001\s*[-–]\s*10000000", re.IGNORECASE), (5000001, 10000000)),
    (re.compile(r">\s*10000001", re.IGNORECASE), (10000001, 50000000)),
]

_AMOUNT_PATTERNS_WITH_DOLLAR = [
    (re.compile(r"\$0\s*[-–]\s*\$10,?000", re.IGNORECASE), (0, 10000)),
    (re.compile(r"\$10,?001\s*[-–]\s*\$100,?000", re.IGNORECASE), (10001, 100000)),
    (re.compile(r"\$100,?001\s*[-–]\s*\$250,?000", re.IGNORECASE), (100001, 250000)),
    (re.compile(r"\$250,?001\s*[-–]\s*\$500,?000", re.IGNORECASE), (250001, 500000)),
    (re.compile(r"\$500,?001\s*[-–]\s*\$1,?000,?000", re.IGNORECASE), (500001, 1000000)),
    (re.compile(r"\$1,?000,?001\s*[-–]\s*\$5,?000,?000", re.IGNORECASE), (1000001, 5000000)),
    (re.compile(r"\$5,?000,?001\s*[-–]\s*\$10,?000,?000", re.IGNORECASE), (5000001, 10000000)),
    (re.compile(r">\s*\$10,?000,?001", re.IGNORECASE), (10000001, 50000000)),
]

_RE_EXTRACT_NUMBERS = re.compile(r'[\d,]+')


# Field mappings - actual API field names from MapServer/16
# Discovered via API metadata query (differs from legacy scraper which used older field names)
FIELD_MAPPINGS = {
    "AGENCY_NAME": "agency_name",
    "AGENCY_ACRONYM": "agency_acronym",
    "AGENCYCODE": "agency_code",
    "COMMODITYCODE": "commodity_code",
    "COMMODITYNAME": "commodity_name",
    "POTITLE": "project_title",
    "CONTRACTNUMBER": "contract_number",
    "POTOTAL": "po_total",  # Numeric amount, not range string
    "STATUS": "status",
    "FISCALYEAR": "fiscal_year",
    "ORDEREDDATE": "ordered_date",
    "CREATEDATE": "create_date",
    "PONUMBER": "po_number",
    "REQUESTER": "requester",
    "REQUISTIONNUMBER": "requisition_number",
    "SUPPLIER": "supplier",
    "OBJECT_ID": "source_record_id",
}

# Fields to request from API
API_FIELDS = [
    "OBJECT_ID",
    "PONUMBER",
    "AGENCY_NAME",
    "AGENCY_ACRONYM",
    "AGENCYCODE",
    "COMMODITYCODE",
    "COMMODITYNAME",
    "POTITLE",
    "CONTRACTNUMBER",
    "POTOTAL",
    "STATUS",
    "FISCALYEAR",
    "ORDEREDDATE",
    "CREATEDATE",
    "REQUESTER",
    "REQUISTIONNUMBER",
    "SUPPLIER",
]


def parse_amount_range(amount_str: str | None) -> tuple[int, int]:
    """
    Parse amount range string and return (min, max) values.

    Examples:
        "$0 - $10,000" -> (0, 10000)
        "10001 - 100000" -> (10001, 100000)
        "> $10,000,001" -> (10000001, 50000000)

    Preserved from legacy/purchase_orders.py
    Uses pre-compiled regex patterns for better performance.
    """
    if not amount_str:
        return 0, 0

    amount_str = str(amount_str).strip()
    amount_str_no_comma = amount_str.replace(",", "")

    # Check patterns without dollar signs (using pre-compiled patterns)
    for pattern, values in _AMOUNT_PATTERNS_NO_DOLLAR:
        if pattern.search(amount_str_no_comma):
            return values

    # Check patterns with dollar signs (using pre-compiled patterns)
    for pattern, values in _AMOUNT_PATTERNS_WITH_DOLLAR:
        if pattern.search(amount_str):
            return values

    # Try to extract numbers directly using pre-compiled pattern
    numbers = _RE_EXTRACT_NUMBERS.findall(amount_str)
    if numbers:
        clean_numbers = []
        for n in numbers:
            try:
                num = int(n.replace(",", ""))
                if num >= 0:
                    clean_numbers.append(num)
            except ValueError:
                continue

        if len(clean_numbers) >= 2:
            return clean_numbers[0], clean_numbers[1]
        elif len(clean_numbers) == 1:
            return clean_numbers[0], clean_numbers[0]

    return 0, 0


def get_default_config() -> SourceConfig:
    """Get default configuration for DC Purchase Orders source."""
    return SourceConfig(
        name="dc_purchase_orders",
        display_name="DC Government Purchase Orders",
        description="District of Columbia government purchase orders from DCGIS",
        source_type=SourceType.REST_API,
        base_url="https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Government_Operations/MapServer",
        requires_auth=False,
        requests_per_second=2.0,
        max_workers=5,
        batch_size=1000,
        timeout_seconds=60,
        max_retries=3,
        adapter_class="src.adapters.dc_arcgis.purchase_orders.PurchaseOrdersAdapter",
        output_record_types=["PurchaseOrderRecord"],
        extra={
            "mapserver_id": 16,
            "rate_limit_wait": 30,
        },
    )


@register_scraper("dc_purchase_orders")
class PurchaseOrdersAdapter(DCArcGISBaseAdapter):
    """
    Adapter for DC Government Purchase Orders.

    MapServer endpoint: 16

    Features:
    - Amount range parsing from string buckets
    - Quarter field handling
    - Full mapping to PurchaseOrderRecord schema
    """

    SOURCE_NAME = "dc_purchase_orders"
    SOURCE_VERSION = "1.0.0"
    SUPPORTED_RECORD_TYPES = [PurchaseOrderRecord]

    # MapServer endpoint for purchase orders
    MAPSERVER_ID = 16

    # Fields to request from API
    FIELDS = ",".join(API_FIELDS)

    def validate(self, data: dict[str, Any]) -> tuple[bool, list[str]]:
        """
        Validate a purchase order record.

        Checks:
        - Agency name is present
        - Fiscal year is reasonable (if present)
        - PO Total is numeric (if present)
        """
        errors = []

        # Agency name is required
        agency = data.get("AGENCY_NAME")
        if not agency:
            errors.append("Missing required field: AGENCY_NAME")

        # Validate fiscal year
        fy = data.get("FISCALYEAR")
        if fy is not None:
            try:
                fy_int = int(fy)
                if fy_int < 1900 or fy_int > 2100:
                    errors.append(f"Fiscal year out of range: {fy}")
            except (ValueError, TypeError):
                pass  # Allow non-standard formats

        # Validate PO total is numeric
        po_total = data.get("POTOTAL")
        if po_total is not None:
            try:
                Decimal(str(po_total))
            except (InvalidOperation, ValueError, TypeError):
                errors.append(f"Invalid PO total: {po_total}")

        return len(errors) == 0, errors

    def normalize(self, data: dict[str, Any]) -> PurchaseOrderRecord:
        """
        Normalize raw API data to PurchaseOrderRecord.

        Maps API field names to canonical schema fields.
        Note: POTOTAL is a numeric amount, not an amount range string.
        """
        # Map fields using field mappings
        normalized: dict[str, Any] = {}
        for api_field, canonical_field in FIELD_MAPPINGS.items():
            if api_field in data and data[api_field] is not None:
                normalized[canonical_field] = data[api_field]

        # Convert POTOTAL (numeric) to amount_min/amount_max
        po_total = normalized.get("po_total")
        if po_total is not None:
            try:
                amount = Decimal(str(po_total))
                normalized["amount_min"] = amount
                normalized["amount_max"] = amount
                # Also format as range string for display
                normalized["amount_range"] = f"${amount:,.2f}"
            except (InvalidOperation, ValueError, TypeError):
                pass

        # Type conversion - fiscal year
        if "fiscal_year" in normalized:
            try:
                normalized["fiscal_year"] = int(normalized["fiscal_year"])
            except (ValueError, TypeError):
                normalized["fiscal_year"] = None

        # Date conversions (already converted by base class _convert_esri_dates)
        for date_field in ["ordered_date", "create_date"]:
            if date_field in normalized and normalized[date_field]:
                try:
                    if isinstance(normalized[date_field], str):
                        normalized[date_field] = date.fromisoformat(
                            normalized[date_field]
                        )
                except ValueError:
                    normalized[date_field] = None

        # Add geometry if available
        if "_latitude" in data and "_longitude" in data:
            normalized["latitude"] = data["_latitude"]
            normalized["longitude"] = data["_longitude"]

        # Capture extra fields not in PurchaseOrderRecord schema
        extra_fields = {}
        extra_field_names = [
            "agency_acronym", "agency_code", "po_number", "requester",
            "requisition_number", "supplier", "ordered_date", "create_date",
            "po_total",
        ]
        for field_name in extra_field_names:
            if field_name in normalized and normalized[field_name] is not None:
                extra_fields[field_name] = normalized.pop(field_name)

        # Also capture any unmapped API fields
        for key, value in data.items():
            if key not in FIELD_MAPPINGS and not key.startswith("_") and value is not None:
                extra_fields[key] = value

        # Create lineage with content hash
        raw_content = str(sorted(data.items())).encode()
        lineage = self._make_lineage()
        lineage = lineage.model_copy(
            update={"raw_hash": LineageInfo.compute_hash(raw_content)}
        )

        # Create the record
        record = PurchaseOrderRecord(
            lineage=lineage,
            extra_fields=extra_fields if extra_fields else {},
            source_record_id=str(normalized.pop("source_record_id", "")),
            agency_name=normalized.get("agency_name"),
            commodity_code=normalized.get("commodity_code"),
            commodity_name=normalized.get("commodity_name"),
            project_title=normalized.get("project_title"),
            procurement_type=None,  # Not available in API
            contract_number=normalized.get("contract_number"),
            amount_range=normalized.get("amount_range"),
            amount_min=normalized.get("amount_min"),
            amount_max=normalized.get("amount_max"),
            status=normalized.get("status"),
            description=None,  # Not available in API
            fiscal_year=normalized.get("fiscal_year"),
            need_by_date=None,  # Not available in API
        )

        return record
