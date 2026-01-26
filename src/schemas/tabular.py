"""Tabular record schemas for structured data (rows/records)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from enum import Enum
from typing import Any

from pydantic import Field

from .base import BaseRecord


class RecordCategory(str, Enum):
    """High-level category for tabular records."""

    GOVERNMENT = "government"
    REAL_ESTATE = "real_estate"
    BUSINESS = "business"
    DEMOGRAPHIC = "demographic"


class TabularRecord(BaseRecord):
    """
    Base class for all tabular (row-based) records.

    Extend this for specific entity types while keeping
    the common lineage and validation infrastructure.
    """

    category: RecordCategory = Field(
        ...,
        description="High-level category for this record type",
    )
    entity_type: str = Field(
        ...,
        description="Specific entity type (contract, property, payment, etc.)",
    )
    source_record_id: str | None = Field(
        default=None,
        description="Original ID from source system for deduplication",
    )


# =============================================================================
# DC Government Records
# =============================================================================


class ContractRecord(TabularRecord):
    """Normalized DC government contract record."""

    category: RecordCategory = RecordCategory.GOVERNMENT
    entity_type: str = "contract"

    # Core fields
    agency_name: str | None = Field(default=None, description="Contracting agency")
    supplier_name: str | None = Field(default=None, description="Vendor/supplier name")
    contract_number: str | None = Field(default=None, description="Contract identifier")
    contract_amount: Decimal | None = Field(default=None, description="Total contract value")
    contract_status: str | None = Field(default=None, description="Active, Completed, etc.")
    contract_type: str | None = Field(default=None, description="Type classification")
    procurement_method: str | None = Field(default=None, description="How it was procured")

    # Dates
    fiscal_year: int | None = Field(default=None, ge=1900, le=2100)
    start_date: date | None = Field(default=None)
    end_date: date | None = Field(default=None)

    # URL to contract detail page
    contract_url: str | None = Field(default=None, description="URL to contract detail page")

    # Geo (if available)
    latitude: float | None = Field(default=None, ge=-90, le=90)
    longitude: float | None = Field(default=None, ge=-180, le=180)


class PurchaseOrderRecord(TabularRecord):
    """Normalized DC government purchase order record."""

    category: RecordCategory = RecordCategory.GOVERNMENT
    entity_type: str = "purchase_order"

    # Core fields
    agency_name: str | None = Field(default=None)
    commodity_code: str | None = Field(default=None)
    commodity_name: str | None = Field(default=None)
    project_title: str | None = Field(default=None)
    procurement_type: str | None = Field(default=None)
    contract_number: str | None = Field(default=None)
    amount_range: str | None = Field(
        default=None,
        description="Amount bucket: $0-$10K, $10K-$25K, etc.",
    )
    amount_min: Decimal | None = Field(default=None, description="Lower bound of amount range")
    amount_max: Decimal | None = Field(default=None, description="Upper bound of amount range")
    status: str | None = Field(default=None)
    description: str | None = Field(default=None)

    # Dates
    fiscal_year: int | None = Field(default=None, ge=1900, le=2100)
    need_by_date: date | None = Field(default=None)


class PaymentRecord(TabularRecord):
    """Normalized DC government payment record."""

    category: RecordCategory = RecordCategory.GOVERNMENT
    entity_type: str = "payment"

    # Core fields
    agency_name: str | None = Field(default=None)
    supplier_name: str | None = Field(default=None)
    payment_amount: Decimal | None = Field(default=None)
    payment_type: str | None = Field(default=None)
    payment_status: str | None = Field(default=None)
    invoice_number: str | None = Field(default=None)
    check_number: str | None = Field(default=None)

    # Dates
    fiscal_year: int | None = Field(default=None, ge=1900, le=2100)
    payment_date: date | None = Field(default=None)


# =============================================================================
# Real Estate Records
# =============================================================================


class ListingType(str, Enum):
    """Type of real estate listing."""

    FOR_SALE = "for_sale"
    FOR_RENT = "for_rent"
    SOLD = "sold"
    OFF_MARKET = "off_market"


class PropertyType(str, Enum):
    """Type of property."""

    SINGLE_FAMILY = "single_family"
    CONDO = "condo"
    TOWNHOUSE = "townhouse"
    MULTI_FAMILY = "multi_family"
    APARTMENT = "apartment"
    LAND = "land"
    COMMERCIAL = "commercial"
    OTHER = "other"


class PropertyRecord(TabularRecord):
    """Normalized real estate property record."""

    category: RecordCategory = RecordCategory.REAL_ESTATE
    entity_type: str = "property"

    # Listing info
    listing_type: ListingType | None = Field(default=None)
    property_type: PropertyType | None = Field(default=None)
    listing_id: str | None = Field(default=None, description="MLS or source listing ID")

    # Price
    price: Decimal | None = Field(default=None, description="Listing or sale price")
    price_per_sqft: Decimal | None = Field(default=None)
    original_price: Decimal | None = Field(default=None)
    price_reduced: bool = Field(default=False)

    # Property details
    bedrooms: int | None = Field(default=None, ge=0)
    bathrooms: float | None = Field(default=None, ge=0)
    sqft: int | None = Field(default=None, ge=0)
    lot_sqft: int | None = Field(default=None, ge=0)
    year_built: int | None = Field(default=None, ge=1600, le=2100)
    stories: int | None = Field(default=None, ge=0)
    garage_spaces: int | None = Field(default=None, ge=0)

    # Location
    address: str | None = Field(default=None)
    city: str | None = Field(default=None)
    state: str | None = Field(default=None)
    zip_code: str | None = Field(default=None)
    county: str | None = Field(default=None)
    neighborhood: str | None = Field(default=None)
    ward: str | None = Field(default=None, description="Ward/district (e.g., DC Ward 1-8)")
    latitude: float | None = Field(default=None, ge=-90, le=90)
    longitude: float | None = Field(default=None, ge=-180, le=180)

    # Dates
    list_date: date | None = Field(default=None)
    sold_date: date | None = Field(default=None)
    days_on_market: int | None = Field(default=None, ge=0)

    # Additional
    hoa_fee: Decimal | None = Field(default=None)
    property_tax: Decimal | None = Field(default=None)
    description: str | None = Field(default=None)
    photo_count: int | None = Field(default=None, ge=0)
    broker_name: str | None = Field(default=None)


class RentalRecord(TabularRecord):
    """Normalized rental listing record (apartments, units)."""

    category: RecordCategory = RecordCategory.REAL_ESTATE
    entity_type: str = "rental"

    # Property info
    property_name: str | None = Field(default=None, description="Building/complex name")
    unit_number: str | None = Field(default=None)
    floor: int | None = Field(default=None)
    floor_plan_name: str | None = Field(default=None)

    # Pricing
    rent_price: Decimal | None = Field(default=None)
    deposit: Decimal | None = Field(default=None)

    # Details
    bedrooms: int | None = Field(default=None, ge=0)
    bathrooms: float | None = Field(default=None, ge=0)
    sqft: int | None = Field(default=None, ge=0)
    status: str | None = Field(default=None, description="Available, Leased, etc.")

    # Location
    address: str | None = Field(default=None)
    city: str | None = Field(default=None)
    state: str | None = Field(default=None)
    zip_code: str | None = Field(default=None)
    latitude: float | None = Field(default=None, ge=-90, le=90)
    longitude: float | None = Field(default=None, ge=-180, le=180)

    # Dates
    available_date: date | None = Field(default=None)

    # Amenities (stored as list in extra_fields if complex)
    amenities: list[str] = Field(default_factory=list)


class MarketStatsRecord(TabularRecord):
    """Market statistics record (Zillow ZHVI, ZORI, etc.)."""

    category: RecordCategory = RecordCategory.REAL_ESTATE
    entity_type: str = "market_stats"

    # Location context
    region_name: str | None = Field(default=None)
    region_type: str | None = Field(default=None, description="metro, county, zip, etc.")
    state: str | None = Field(default=None)
    zip_code: str | None = Field(default=None)

    # Metric info
    metric_name: str = Field(..., description="ZHVI, ZORI, etc.")
    metric_value: Decimal | None = Field(default=None)
    metric_date: date | None = Field(default=None)
    metric_period: str | None = Field(default=None, description="monthly, yearly, etc.")

    # Property type context
    bedrooms: int | None = Field(default=None, description="BR filter if applicable")
    property_type: str | None = Field(default=None)

    # Time series (stored in extra_fields if needed)
    historical_values: dict[str, Any] = Field(default_factory=dict)


# =============================================================================
# Business Records
# =============================================================================


class BusinessRecord(TabularRecord):
    """Normalized business entity record."""

    category: RecordCategory = RecordCategory.BUSINESS
    entity_type: str = "business"

    # Core fields
    business_name: str | None = Field(default=None)
    trade_name: str | None = Field(default=None, description="DBA name")
    business_type: str | None = Field(default=None)
    industry: str | None = Field(default=None)
    naics_code: str | None = Field(default=None)

    # License/certification
    license_number: str | None = Field(default=None)
    license_type: str | None = Field(default=None)
    license_status: str | None = Field(default=None)
    certification_type: str | None = Field(default=None, description="CBE, DBE, etc.")

    # Contact
    owner_name: str | None = Field(default=None)
    phone: str | None = Field(default=None)
    email: str | None = Field(default=None)
    website: str | None = Field(default=None)

    # Location
    address: str | None = Field(default=None)
    city: str | None = Field(default=None)
    state: str | None = Field(default=None)
    zip_code: str | None = Field(default=None)
    ward: str | None = Field(default=None)
    latitude: float | None = Field(default=None, ge=-90, le=90)
    longitude: float | None = Field(default=None, ge=-180, le=180)

    # Dates
    issue_date: date | None = Field(default=None)
    expiration_date: date | None = Field(default=None)
    registration_date: date | None = Field(default=None)


# =============================================================================
# Demographic Records
# =============================================================================


class DemographicRecord(TabularRecord):
    """Normalized demographic/economic data record."""

    category: RecordCategory = RecordCategory.DEMOGRAPHIC
    entity_type: str = "demographic"

    # Location context
    region_name: str = Field(..., description="DC, Maryland, Virginia, etc.")
    region_type: str = Field(
        default="state",
        description="state, county, city, tract, etc.",
    )
    region_id: str | None = Field(default=None, description="FIPS code or similar")

    # Metric info
    metric_category: str = Field(
        ...,
        description="population, economics, education, etc.",
    )
    metric_name: str = Field(..., description="Specific metric name")
    metric_value: Decimal | None = Field(default=None)
    metric_unit: str | None = Field(default=None, description="count, percent, dollars, etc.")

    # Time context
    year: int | None = Field(default=None, ge=1900, le=2100)
    period: str | None = Field(default=None, description="Q1, annual, etc.")

    # Comparison context
    comparison_value: Decimal | None = Field(
        default=None,
        description="National/regional comparison value",
    )
    percentile: float | None = Field(
        default=None,
        ge=0,
        le=100,
        description="Percentile rank if available",
    )
