"""
Payments adapter for DC ArcGIS MapServer/17.

Migrated from legacy/payments.py with:
- Full field mapping to PaymentRecord schema
- Payment amount validation
- Date field conversions
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any

from ...core.registry import register_scraper
from ...schemas.base import LineageInfo
from ...schemas.metadata import SourceConfig, SourceType
from ...schemas.tabular import PaymentRecord
from .base import DCArcGISBaseAdapter

# Field mappings - actual API field names from MapServer/17
FIELD_MAPPINGS = {
    "AGENCY_NAME": "agency_name",
    "AGENCY_ACRONYM": "agency_acronym",
    "AGENCYCODE": "agency_code",
    "SUPPLIERNAME": "supplier_name",
    "PAYMENTAMOUNT": "payment_amount",
    "PAYMENTTYPE": "payment_type",
    "INVOICENUMBER": "invoice_number",
    "PAYMENTNUMBER": "payment_number",
    "VOUCHERNUMBER": "voucher_number",
    "CONTRACTNUMBER": "contract_number",
    "PONUMBER": "po_number",
    "PAYMENTDATE": "payment_date",
    "INVOICEDATE": "invoice_date",
    "ESTPAYMENTDATE": "est_payment_date",
    "FISCALYEAR": "fiscal_year",
    "TRANSACTION_CODE": "transaction_code",
    "RECORDUPDATEDDATE": "record_updated_date",
    "RECORDCREATED": "record_created_date",
    "OBJECTID": "source_record_id",
}

# Fields to request from API
API_FIELDS = [
    "OBJECTID",
    "AGENCY_NAME",
    "AGENCY_ACRONYM",
    "AGENCYCODE",
    "SUPPLIERNAME",
    "PAYMENTAMOUNT",
    "PAYMENTTYPE",
    "INVOICENUMBER",
    "PAYMENTNUMBER",
    "VOUCHERNUMBER",
    "CONTRACTNUMBER",
    "PONUMBER",
    "PAYMENTDATE",
    "INVOICEDATE",
    "ESTPAYMENTDATE",
    "FISCALYEAR",
    "TRANSACTION_CODE",
]


def get_default_config() -> SourceConfig:
    """Get default configuration for DC Payments source."""
    return SourceConfig(
        name="dc_payments",
        display_name="DC Government Payments",
        description="District of Columbia government payments from DCGIS",
        source_type=SourceType.REST_API,
        base_url="https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Government_Operations/MapServer",
        requires_auth=False,
        requests_per_second=2.0,
        max_workers=5,
        batch_size=1000,
        timeout_seconds=60,
        max_retries=3,
        adapter_class="src.adapters.dc_arcgis.payments.PaymentsAdapter",
        output_record_types=["PaymentRecord"],
        extra={
            "mapserver_id": 17,
            "rate_limit_wait": 30,
        },
    )


@register_scraper("dc_payments")
class PaymentsAdapter(DCArcGISBaseAdapter):
    """
    Adapter for DC Government Payments.

    MapServer endpoint: 17

    Features:
    - Payment amount validation
    - Multiple date field handling
    - Full mapping to PaymentRecord schema
    """

    SOURCE_NAME = "dc_payments"
    SOURCE_VERSION = "1.0.0"
    SUPPORTED_RECORD_TYPES = [PaymentRecord]

    # MapServer endpoint for payments
    MAPSERVER_ID = 17

    # Fields to request from API
    FIELDS = ",".join(API_FIELDS)

    def validate(self, data: dict[str, Any]) -> tuple[bool, list[str]]:
        """
        Validate a payment record.

        Checks:
        - Supplier name is present
        - Payment amount is numeric (if present)
        - Fiscal year is reasonable (if present)
        """
        errors = []

        # Supplier name should be present for meaningful payments
        supplier = data.get("SUPPLIERNAME")
        if not supplier:
            # Log but don't fail - some payments may have no supplier
            self._logger.debug("Payment record missing SUPPLIERNAME")

        # Validate payment amount
        amount = data.get("PAYMENTAMOUNT")
        if amount is not None:
            try:
                Decimal(str(amount))
            except (InvalidOperation, ValueError, TypeError):
                errors.append(f"Invalid payment amount: {amount}")

        # Validate fiscal year
        fy = data.get("FISCALYEAR")
        if fy is not None:
            try:
                fy_int = int(fy)
                if fy_int < 1900 or fy_int > 2100:
                    errors.append(f"Fiscal year out of range: {fy}")
            except (ValueError, TypeError):
                pass  # Allow non-standard formats

        return len(errors) == 0, errors

    def normalize(self, data: dict[str, Any]) -> PaymentRecord:
        """
        Normalize raw API data to PaymentRecord.

        Maps API field names to canonical schema fields.
        """
        # Map fields using field mappings
        normalized: dict[str, Any] = {}
        for api_field, canonical_field in FIELD_MAPPINGS.items():
            if api_field in data and data[api_field] is not None:
                normalized[canonical_field] = data[api_field]

        # Type conversion - payment amount
        if "payment_amount" in normalized:
            try:
                normalized["payment_amount"] = Decimal(str(normalized["payment_amount"]))
            except (InvalidOperation, ValueError, TypeError):
                normalized["payment_amount"] = None

        # Type conversion - fiscal year
        if "fiscal_year" in normalized:
            try:
                normalized["fiscal_year"] = int(normalized["fiscal_year"])
            except (ValueError, TypeError):
                normalized["fiscal_year"] = None

        # Date conversions (already converted by base class _convert_esri_dates)
        date_fields = ["payment_date", "invoice_date", "est_payment_date",
                       "record_updated_date", "record_created_date"]
        for date_field in date_fields:
            if date_field in normalized and normalized[date_field]:
                try:
                    if isinstance(normalized[date_field], str):
                        normalized[date_field] = date.fromisoformat(
                            normalized[date_field]
                        )
                except ValueError:
                    normalized[date_field] = None

        # Add geometry if available (payments table doesn't have geometry)
        if "_latitude" in data and "_longitude" in data:
            normalized["latitude"] = data["_latitude"]
            normalized["longitude"] = data["_longitude"]

        # Capture extra fields not in PaymentRecord schema
        extra_fields = {}
        extra_field_names = [
            "agency_acronym", "agency_code", "voucher_number", "contract_number",
            "po_number", "payment_number", "invoice_date", "est_payment_date",
            "transaction_code", "record_updated_date", "record_created_date",
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
        record = PaymentRecord(
            lineage=lineage,
            extra_fields=extra_fields if extra_fields else {},
            source_record_id=str(normalized.pop("source_record_id", "")),
            agency_name=normalized.get("agency_name"),
            supplier_name=normalized.get("supplier_name"),
            payment_amount=normalized.get("payment_amount"),
            payment_type=normalized.get("payment_type"),
            payment_status=None,  # Not available in API
            invoice_number=normalized.get("invoice_number"),
            check_number=None,  # Could use payment_number but keeping separate
            fiscal_year=normalized.get("fiscal_year"),
            payment_date=normalized.get("payment_date"),
        )

        return record
