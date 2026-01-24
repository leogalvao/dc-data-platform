"""
Contracts adapter wrapping legacy/contracts.py.

This adapter demonstrates how to wrap an existing scraper
with MINIMAL changes to the underlying implementation.

The key patterns shown:
1. Reuse existing field mappings from legacy code
2. Implement validate() and normalize() to map to canonical schema
3. Keep source-specific logic in the adapter, not the base class
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any

from ...core.registry import register_scraper
from ...schemas.metadata import SourceConfig, SourceType
from ...schemas.tabular import ContractRecord
from .base import DCArcGISBaseAdapter

# Field mappings - actual API field names from MapServer/37
# Discovered via API metadata query
FIELD_MAPPINGS = {
    "AGENCY_NAME": "agency_name",
    "AGENCY": "agency_name",  # Fallback alias
    "CONTRACTAMOUNT": "contract_amount",
    "FISCALYEAR": "fiscal_year",
    "CONTRACTSTATUS": "contract_status",
    "MARKETTYPE": "contract_type",
    "PROCUREMENTMETHODDESCRIPTION": "procurement_method",
    "CONTRACTNUMBER": "contract_number",
    "STARTDATE": "start_date",
    "ENDDATE": "end_date",
    "ROW_ID": "source_record_id",
    "TITLE": "project_title",  # Store in extra_fields
    "CONTRACTINGOFFICER": "contracting_officer",  # Store in extra_fields
}

# Required fields for validation
REQUIRED_FIELDS = ["AGENCY_NAME", "CONTRACTAMOUNT"]


def get_default_config() -> SourceConfig:
    """Get default configuration for DC Contracts source."""
    return SourceConfig(
        name="dc_contracts",
        display_name="DC Government Contracts",
        description="District of Columbia government contracts from DCGIS",
        source_type=SourceType.REST_API,
        base_url="https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Government_Operations/MapServer",
        requires_auth=False,
        requests_per_second=2.0,
        max_workers=5,
        batch_size=1000,
        timeout_seconds=60,
        max_retries=3,
        adapter_class="src.adapters.dc_arcgis.contracts.ContractsAdapter",
        output_record_types=["ContractRecord"],
        extra={
            "mapserver_id": 37,  # From legacy contracts.py
            "rate_limit_wait": 30,
        },
    )


@register_scraper("dc_contracts")
class ContractsAdapter(DCArcGISBaseAdapter):
    """
    Adapter for DC Government Contracts.

    Wraps the logic from legacy/contracts.py and maps it
    to the canonical ContractRecord schema.
    """

    SOURCE_NAME = "dc_contracts"
    SOURCE_VERSION = "1.0.0"
    SUPPORTED_RECORD_TYPES = [ContractRecord]

    # MapServer endpoint for contracts
    MAPSERVER_ID = 37

    # Fields to request from API (actual field names from MapServer metadata)
    FIELDS = ",".join([
        "ROW_ID", "AGENCY_NAME", "AGENCY", "CONTRACTAMOUNT", "FISCALYEAR",
        "CONTRACTSTATUS", "MARKETTYPE", "PROCUREMENTMETHODDESCRIPTION",
        "CONTRACTNUMBER", "STARTDATE", "ENDDATE", "TITLE", "CONTRACTINGOFFICER",
        "AWARDDATE"
    ])

    def validate(self, data: dict[str, Any]) -> tuple[bool, list[str]]:
        """
        Validate a contract record.

        Checks:
        - Required fields present
        - Contract amount is numeric (if present)
        - Fiscal year is reasonable (if present)
        """
        errors = []

        # Check required fields (allow AGENCY as fallback for AGENCY_NAME)
        agency = data.get("AGENCY_NAME") or data.get("AGENCY")
        if not agency:
            errors.append("Missing required field: AGENCY_NAME")

        # Validate contract amount
        amt = data.get("CONTRACTAMOUNT")
        if amt is not None:
            try:
                Decimal(str(amt))
            except (InvalidOperation, ValueError, TypeError):
                errors.append(f"Invalid contract amount: {amt}")

        # Validate fiscal year
        fy = data.get("FISCALYEAR")
        if fy is not None:
            try:
                fy_int = int(str(fy).replace("FY", "").strip())
                if fy_int < 1900 or fy_int > 2100:
                    errors.append(f"Fiscal year out of range: {fy}")
            except (ValueError, TypeError):
                pass  # Allow non-standard fiscal year formats

        return len(errors) == 0, errors

    def normalize(self, data: dict[str, Any]) -> ContractRecord:
        """
        Normalize raw API data to ContractRecord.

        Maps API field names to canonical schema fields.
        """
        # Map fields using field mappings
        normalized = {}
        for api_field, canonical_field in FIELD_MAPPINGS.items():
            if api_field in data and data[api_field] is not None:
                # Don't overwrite if we already have a value (handles aliases)
                if canonical_field not in normalized:
                    normalized[canonical_field] = data[api_field]

        # Type conversions - contract amount
        if "contract_amount" in normalized:
            try:
                normalized["contract_amount"] = Decimal(str(normalized["contract_amount"]))
            except (InvalidOperation, ValueError, TypeError):
                normalized["contract_amount"] = None

        # Type conversions - fiscal year (may have "FY" prefix)
        if "fiscal_year" in normalized:
            try:
                fy_str = str(normalized["fiscal_year"]).replace("FY", "").strip()
                normalized["fiscal_year"] = int(fy_str)
            except (ValueError, TypeError):
                normalized["fiscal_year"] = None

        # Date conversions (already converted by base class _convert_esri_dates)
        for date_field in ["start_date", "end_date"]:
            if date_field in normalized and normalized[date_field]:
                try:
                    if isinstance(normalized[date_field], str):
                        normalized[date_field] = date.fromisoformat(normalized[date_field])
                except ValueError:
                    normalized[date_field] = None

        # Add geometry if available
        if "_latitude" in data and "_longitude" in data:
            normalized["latitude"] = data["_latitude"]
            normalized["longitude"] = data["_longitude"]

        # Capture extra fields not in mapping
        extra_fields = {}
        for key, value in data.items():
            if key not in FIELD_MAPPINGS and not key.startswith("_"):
                extra_fields[key] = value
        # Also add mapped fields that aren't in ContractRecord
        if "project_title" in normalized:
            extra_fields["project_title"] = normalized.pop("project_title")
        if "contracting_officer" in normalized:
            extra_fields["contracting_officer"] = normalized.pop("contracting_officer")

        # Create the record
        record = ContractRecord(
            lineage=self._make_lineage(),
            extra_fields=extra_fields,
            source_record_id=str(normalized.get("source_record_id", "")),
            **{k: v for k, v in normalized.items() if k != "source_record_id"},
        )

        # Update lineage with content hash for dedup
        raw_content = str(sorted(data.items())).encode()
        record.lineage = record.lineage.model_copy(
            update={"raw_hash": record.lineage.compute_hash(raw_content)}
        )

        return record
