"""
Gold layer storage for aggregated analytics tables.

The Gold layer provides business-ready aggregated views:
- Pre-computed aggregations for common reporting needs
- Denormalized for query performance
- Partitioned by business-relevant dimensions
- Schema-enforced for consistency

Key datasets:
1. spend_by_vendor - Government spending aggregated by vendor
2. property_values_by_ward - Property valuations aggregated by ward
3. agency_spend_summary - Spending patterns by DC government agency
4. market_trends - Time-series market statistics from Zillow and RentCast
"""

from __future__ import annotations

import hashlib
import json
import logging
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False
    logger.warning("PyArrow not installed. Gold layer will use JSON fallback.")


# =============================================================================
# Gold Layer Schemas
# =============================================================================


class SpendByVendorRecord(BaseModel):
    """Aggregated government spending by vendor."""

    record_id: UUID = Field(default_factory=uuid4)
    aggregation_date: datetime = Field(
        default_factory=lambda: datetime.utcnow(),
        description="When this aggregation was computed",
    )

    # Vendor identifiers
    vendor_id: str = Field(..., description="Normalized vendor identifier (hash of normalized name)")
    vendor_name: str = Field(..., description="Original vendor name (most common variant)")
    vendor_name_normalized: str = Field(
        ...,
        description="Uppercase, suffix-stripped vendor name for grouping",
    )

    # Spending aggregations
    total_contract_value: Decimal = Field(
        default=Decimal("0"),
        description="Sum of contract amounts",
    )
    total_po_value: Decimal = Field(
        default=Decimal("0"),
        description="Sum of PO amounts (midpoint of min/max)",
    )
    total_payments: Decimal = Field(
        default=Decimal("0"),
        description="Sum of actual payments",
    )

    # Counts
    contract_count: int = Field(default=0, description="Number of contracts")
    po_count: int = Field(default=0, description="Number of purchase orders")
    payment_count: int = Field(default=0, description="Number of payments")

    # Agency info
    agencies: list[str] = Field(
        default_factory=list,
        description="List of agencies vendor has worked with",
    )
    agency_count: int = Field(default=0, description="Number of distinct agencies")

    # Time range
    first_contract_date: str | None = Field(default=None, description="Earliest contract start date")
    last_payment_date: str | None = Field(default=None, description="Most recent payment date")
    fiscal_years: list[int] = Field(
        default_factory=list,
        description="Fiscal years with activity",
    )

    # CBE status
    is_cbe: bool = Field(default=False, description="Whether vendor is in CBE directory")

    # Metadata
    computed_at: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(),
        description="ISO timestamp of computation",
    )

    # Legacy fields for backwards compatibility
    supplier_name: str | None = Field(default=None, description="Alias for vendor_name")
    supplier_name_normalized: str | None = Field(default=None, description="Alias for vendor_name_normalized")
    total_payment_value: Decimal | None = Field(default=None, description="Alias for total_payments")
    earliest_contract_date: datetime | None = Field(default=None)
    latest_contract_date: datetime | None = Field(default=None)
    agencies_served: list[str] = Field(default_factory=list)
    top_agency: str | None = Field(default=None)
    top_agency_amount: Decimal | None = Field(default=None)
    avg_contract_value: Decimal | None = Field(default=None)
    spend_rank: int | None = Field(default=None, description="Rank by total spend (1 = highest)")
    purchase_order_count: int = Field(default=0)

    model_config = {"extra": "allow"}


class PropertyValuesByWardRecord(BaseModel):
    """Aggregated property valuations by ward."""

    record_id: UUID = Field(default_factory=uuid4)
    aggregation_date: datetime = Field(
        default_factory=lambda: datetime.utcnow(),
    )

    # Location
    ward: str = Field(..., description="DC Ward (1-8)")

    # Property counts
    property_count: int = Field(default=0, description="Number of properties")

    # Value aggregations
    total_assessed_value: float = Field(default=0.0, description="Sum of assessed values")
    avg_assessed_value: float | None = Field(default=None, description="Mean assessed value")
    median_assessed_value: float | None = Field(default=None, description="Median assessed value")
    min_assessed_value: float | None = Field(default=None, description="Minimum assessed value")
    max_assessed_value: float | None = Field(default=None, description="Maximum assessed value")

    # Property characteristics (averages)
    avg_sqft: float | None = Field(default=None, description="Average square footage")
    avg_price_per_sqft: float | None = Field(default=None, description="Average price per square foot")
    avg_year_built: float | None = Field(default=None, description="Average year built")

    # Distributions as JSON strings
    property_type_distribution: str | None = Field(default=None, description="Count by property type as JSON")
    bedroom_distribution: str | None = Field(default=None, description="Count by bedroom count as JSON")

    # Metadata
    computed_at: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(),
        description="ISO timestamp of computation",
    )

    # Legacy fields for backwards compatibility
    neighborhood: str | None = Field(default=None)
    total_properties: int = Field(default=0)
    residential_count: int = Field(default=0)
    commercial_count: int = Field(default=0)
    vacant_count: int = Field(default=0)
    total_sale_value: Decimal = Field(default=Decimal("0"))
    avg_sale_price: Decimal | None = Field(default=None)
    median_sale_price: Decimal | None = Field(default=None)
    avg_lot_sqft: float | None = Field(default=None)
    avg_bedrooms: float | None = Field(default=None)
    avg_bathrooms: float | None = Field(default=None)
    price_percentile_25: Decimal | None = Field(default=None)
    price_percentile_75: Decimal | None = Field(default=None)
    price_percentile_90: Decimal | None = Field(default=None)
    sales_last_30_days: int = Field(default=0)
    sales_last_90_days: int = Field(default=0)
    sales_last_year: int = Field(default=0)

    model_config = {"extra": "allow"}


class AgencySpendSummaryRecord(BaseModel):
    """Spending patterns by DC government agency."""

    record_id: UUID = Field(default_factory=uuid4)
    aggregation_date: datetime = Field(default_factory=lambda: datetime.utcnow())

    # Agency identifiers
    agency_name: str = Field(..., description="Original agency name")
    agency_name_normalized: str = Field(..., description="Normalized agency name")

    # Spending aggregations
    total_contract_value: float = Field(default=0.0, description="Sum of contract amounts")
    total_payments: float = Field(default=0.0, description="Sum of payment amounts")
    total_po_value: float = Field(default=0.0, description="Sum of PO amounts")

    # Counts
    contract_count: int = Field(default=0, description="Number of contracts")
    vendor_count: int = Field(default=0, description="Number of distinct vendors")

    # Top vendors as JSON list
    top_vendors: str | None = Field(default=None, description="Top 10 vendors by spend as JSON")

    # Fiscal year breakdown as JSON object
    fiscal_year_breakdown: str | None = Field(default=None, description="Spend by fiscal year as JSON")

    # Metadata
    computed_at: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(),
        description="ISO timestamp of computation",
    )

    model_config = {"extra": "allow"}


class MarketTrendsRecord(BaseModel):
    """Time-series market statistics from Zillow and RentCast."""

    record_id: UUID = Field(default_factory=uuid4)
    aggregation_date: datetime = Field(default_factory=lambda: datetime.utcnow())

    # Location
    region_name: str = Field(..., description="Region name")
    region_type: str = Field(..., description="metro, zip, county")

    # Metric info
    metric_name: str = Field(..., description="ZHVI, ZORI, etc.")
    metric_category: str = Field(default="", description="rent, sale, inventory")

    # Values
    latest_value: float | None = Field(default=None)
    latest_date: str | None = Field(default=None)

    # Change metrics
    yoy_change: float | None = Field(default=None, description="Year-over-year percent change")
    mom_change: float | None = Field(default=None, description="Month-over-month percent change")
    trend_direction: str | None = Field(default=None, description="up, down, stable")

    # Historical data as JSON
    historical_values: str | None = Field(default=None, description="Last 24 months of values as JSON")

    # Metadata
    computed_at: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(),
        description="ISO timestamp of computation",
    )

    model_config = {"extra": "allow"}


# =============================================================================
# Gold Layer Writer
# =============================================================================


class GoldWriter:
    """
    Writer for the Gold layer (aggregated analytics).

    Computes aggregations from Silver layer data and writes to
    analysis-ready formats.
    """

    def __init__(self, gold_path: str | Path, silver_path: str | Path):
        self._gold_path = Path(gold_path)
        self._silver_path = Path(silver_path)
        self._gold_path.mkdir(parents=True, exist_ok=True)
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def build_spend_by_vendor(
        self,
        run_id: UUID | None = None,
    ) -> list[SpendByVendorRecord]:
        """
        Build spend by vendor aggregation from contracts, payments, and PO data.

        Transformation logic:
        1. Read all contracts, extract supplier_name, contract_amount, agency_name, start_date, fiscal_year
        2. Read all payments, extract supplier_name, payment_amount, agency_name, payment_date, fiscal_year
        3. Read all purchase orders, extract amounts (use midpoint of min/max)
        4. Normalize vendor names: UPPER, strip LLC/INC/CORP suffixes, trim whitespace
        5. Group by normalized vendor name
        6. Aggregate sums, counts, and collect distinct values
        7. Join with dc_cbe_businesses on normalized name to set is_cbe flag
        8. Sort by total_payments + total_contract_value descending

        Returns:
            List of SpendByVendorRecord aggregations
        """
        run_id = run_id or uuid4()
        computed_at = datetime.now(timezone.utc).isoformat()

        vendor_data: dict[str, dict[str, Any]] = defaultdict(lambda: {
            "vendor_names": [],  # Track all name variants
            "total_contract_value": Decimal("0"),
            "total_po_value": Decimal("0"),
            "total_payments": Decimal("0"),
            "contract_count": 0,
            "po_count": 0,
            "payment_count": 0,
            "fiscal_years": set(),
            "agencies": set(),
            "first_contract_date": None,
            "last_payment_date": None,
        })

        # Load contract records from silver
        contracts = self._load_silver_records("dc_contracts")
        for record in contracts:
            vendor = record.get("supplier_name")
            if not vendor:
                continue

            vendor_key = self._normalize_vendor_name(vendor)
            data = vendor_data[vendor_key]
            data["vendor_names"].append(vendor)

            # Sum contract amounts
            amount = self._to_decimal(record.get("contract_amount"))
            if amount and amount > 0:
                data["total_contract_value"] += amount

            data["contract_count"] += 1

            # Track fiscal years
            fy = record.get("fiscal_year")
            if fy:
                try:
                    data["fiscal_years"].add(int(fy))
                except (ValueError, TypeError):
                    pass

            # Track agencies
            agency = record.get("agency_name")
            if agency:
                data["agencies"].add(agency)

            # Track first contract date
            start_date = record.get("start_date")
            if start_date:
                try:
                    date_str = str(start_date).split("T")[0] if "T" in str(start_date) else str(start_date)
                    if data["first_contract_date"] is None or date_str < data["first_contract_date"]:
                        data["first_contract_date"] = date_str
                except (ValueError, TypeError):
                    pass

        # Stream purchase order records (can be large)
        self._logger.info("Processing purchase orders...")
        for record in self._iter_silver_records("dc_purchase_orders"):
            # POs may use different vendor field names
            vendor = record.get("supplier_name") or record.get("vendor_name")
            if not vendor:
                continue

            vendor_key = self._normalize_vendor_name(vendor)
            data = vendor_data[vendor_key]
            if vendor not in data["vendor_names"]:
                data["vendor_names"].append(vendor)

            # Calculate PO amount as midpoint of min/max
            amount_min = self._to_decimal(record.get("amount_min"))
            amount_max = self._to_decimal(record.get("amount_max"))

            if amount_min is not None and amount_max is not None:
                po_amount = (amount_min + amount_max) / 2
            elif amount_min is not None:
                po_amount = amount_min
            elif amount_max is not None:
                po_amount = amount_max
            else:
                po_amount = Decimal("0")

            if po_amount > 0:
                data["total_po_value"] += po_amount

            data["po_count"] += 1

            # Track agencies
            agency = record.get("agency_name")
            if agency:
                data["agencies"].add(agency)

        # Stream payment records (largest dataset - 9M+ records)
        self._logger.info("Processing payments (streaming)...")
        for record in self._iter_silver_records("dc_payments"):
            vendor = record.get("supplier_name")
            if not vendor:
                continue

            vendor_key = self._normalize_vendor_name(vendor)
            data = vendor_data[vendor_key]
            if vendor not in data["vendor_names"]:
                data["vendor_names"].append(vendor)

            amount = self._to_decimal(record.get("payment_amount"))
            if amount and amount > 0:
                data["total_payments"] += amount

            data["payment_count"] += 1

            # Track last payment date
            payment_date = record.get("payment_date")
            if payment_date:
                try:
                    date_str = str(payment_date).split("T")[0] if "T" in str(payment_date) else str(payment_date)
                    if data["last_payment_date"] is None or date_str > data["last_payment_date"]:
                        data["last_payment_date"] = date_str
                except (ValueError, TypeError):
                    pass

            # Track agencies
            agency = record.get("agency_name")
            if agency:
                data["agencies"].add(agency)

        # Load CBE businesses for is_cbe flag
        cbe_normalized_names = set()
        cbe_businesses = self._load_silver_records("dc_cbe_businesses")
        for record in cbe_businesses:
            business_name = record.get("business_name")
            if business_name:
                cbe_normalized_names.add(self._normalize_vendor_name(business_name))

        # Build records
        records = []
        for vendor_key, data in vendor_data.items():
            # Get most common vendor name variant
            vendor_names = data["vendor_names"]
            if vendor_names:
                from collections import Counter
                vendor_name = Counter(vendor_names).most_common(1)[0][0]
            else:
                vendor_name = vendor_key

            # Generate vendor_id as hash of normalized name
            vendor_id = hashlib.md5(vendor_key.encode()).hexdigest()[:16]

            # Check CBE status
            is_cbe = vendor_key in cbe_normalized_names

            agencies_list = sorted(data["agencies"])

            # Calculate average contract value for legacy field
            avg_contract = None
            if data["contract_count"] > 0:
                avg_contract = data["total_contract_value"] / data["contract_count"]

            # Find top agency for legacy fields
            top_agency = agencies_list[0] if agencies_list else None

            record = SpendByVendorRecord(
                # New fields per spec
                vendor_id=vendor_id,
                vendor_name=vendor_name,
                vendor_name_normalized=vendor_key.upper(),
                total_contract_value=data["total_contract_value"],
                total_po_value=data["total_po_value"],
                total_payments=data["total_payments"],
                contract_count=data["contract_count"],
                po_count=data["po_count"],
                payment_count=data["payment_count"],
                agencies=agencies_list,
                agency_count=len(agencies_list),
                first_contract_date=data["first_contract_date"],
                last_payment_date=data["last_payment_date"],
                fiscal_years=sorted(data["fiscal_years"]),
                is_cbe=is_cbe,
                computed_at=computed_at,
                # Legacy fields
                supplier_name=vendor_name,
                supplier_name_normalized=vendor_key,
                total_payment_value=data["total_payments"],
                earliest_contract_date=None,
                latest_contract_date=None,
                agencies_served=agencies_list,
                top_agency=top_agency,
                top_agency_amount=None,
                avg_contract_value=avg_contract,
                purchase_order_count=data["po_count"],
            )
            records.append(record)

        # Sort by total spend descending and assign ranks
        records.sort(
            key=lambda r: float(r.total_payments) + float(r.total_contract_value),
            reverse=True,
        )
        for i, record in enumerate(records, 1):
            record.spend_rank = i

        # Write to storage
        self._write_records(records, "spend_by_vendor", run_id)

        self._logger.info(f"Built {len(records)} spend_by_vendor records")
        return records

    def build_property_values_by_ward(
        self,
        run_id: UUID | None = None,
    ) -> list[PropertyValuesByWardRecord]:
        """
        Build property values by ward aggregation from CAMA data.

        Transformation logic:
        1. Read dc_residential_cama silver data
        2. Extract ward from address or dedicated ward field
        3. Filter to valid wards (1-8)
        4. Group by ward
        5. Compute aggregations: count, sum, avg, median, min, max
        6. Compute distributions as JSON strings
        7. Sort by ward number

        Returns:
            List of PropertyValuesByWardRecord aggregations
        """
        run_id = run_id or uuid4()
        computed_at = datetime.now(timezone.utc).isoformat()

        ward_data: dict[str, dict[str, Any]] = defaultdict(lambda: {
            "property_count": 0,
            "prices": [],
            "sqft_values": [],
            "year_built_values": [],
            "property_types": defaultdict(int),
            "bedroom_counts": defaultdict(int),
        })

        # Stream property records from silver (large datasets)
        from itertools import chain
        self._logger.info("Processing property records (streaming)...")
        property_sources = chain(
            self._iter_silver_records("dc_residential_cama"),
            self._iter_silver_records("dc_itspe_property_land"),
        )

        for record in property_sources:
            # Extract ward
            ward = self._extract_ward(record)
            if not ward:
                continue

            # Filter to valid wards (1-8)
            try:
                ward_num = int(str(ward).replace("Ward ", "").strip())
                if not (1 <= ward_num <= 8):
                    continue
                ward = str(ward_num)
            except (ValueError, TypeError):
                continue

            data = ward_data[ward]
            data["property_count"] += 1

            # Collect assessed values for aggregation (prefer total_assessment over price)
            assessed_value = self._extract_assessed_value(record)
            if assessed_value and assessed_value > 0:
                data["prices"].append(assessed_value)

            # Collect sqft values
            sqft = record.get("sqft")
            if sqft and sqft > 0:
                data["sqft_values"].append(float(sqft))

            # Collect year built values
            year_built = record.get("year_built")
            if year_built and 1600 < year_built < 2100:
                data["year_built_values"].append(int(year_built))

            # Track property type distribution
            prop_type = record.get("property_type") or "unknown"
            data["property_types"][str(prop_type)] += 1

            # Track bedroom distribution
            bedrooms = record.get("bedrooms")
            if bedrooms is not None:
                try:
                    bedroom_key = str(int(bedrooms))
                    data["bedroom_counts"][bedroom_key] += 1
                except (ValueError, TypeError):
                    pass

        # Build records - one per ward
        records = []
        for ward in sorted(ward_data.keys(), key=lambda w: int(w)):
            data = ward_data[ward]
            prices = data["prices"]

            # Calculate statistics
            avg_assessed = self._safe_mean_float(prices)
            median_assessed = self._safe_median_float(prices)
            min_assessed = min(prices) if prices else None
            max_assessed = max(prices) if prices else None
            total_assessed = sum(prices) if prices else 0.0

            # Calculate price per sqft
            sqft_values = data["sqft_values"]
            avg_sqft = self._safe_mean_float(sqft_values)
            avg_price_per_sqft = None
            if avg_assessed and avg_sqft and avg_sqft > 0:
                avg_price_per_sqft = avg_assessed / avg_sqft

            # Serialize distributions to JSON
            property_type_dist = json.dumps(dict(data["property_types"]))
            bedroom_dist = json.dumps(dict(data["bedroom_counts"]))

            record = PropertyValuesByWardRecord(
                ward=ward,
                property_count=data["property_count"],
                total_assessed_value=total_assessed,
                avg_assessed_value=avg_assessed,
                median_assessed_value=median_assessed,
                min_assessed_value=min_assessed,
                max_assessed_value=max_assessed,
                avg_sqft=avg_sqft,
                avg_price_per_sqft=avg_price_per_sqft,
                avg_year_built=self._safe_mean_float(data["year_built_values"]),
                property_type_distribution=property_type_dist,
                bedroom_distribution=bedroom_dist,
                computed_at=computed_at,
                # Legacy fields
                total_properties=data["property_count"],
                total_sale_value=Decimal(str(total_assessed)),
                avg_sale_price=Decimal(str(avg_assessed)) if avg_assessed else None,
                median_sale_price=Decimal(str(median_assessed)) if median_assessed else None,
                price_percentile_25=self._safe_percentile_float(prices, 25),
                price_percentile_75=self._safe_percentile_float(prices, 75),
                price_percentile_90=self._safe_percentile_float(prices, 90),
            )
            records.append(record)

        # Write to storage
        self._write_records(records, "property_values_by_ward", run_id)

        self._logger.info(f"Built {len(records)} property_values_by_ward records")
        return records

    def build_agency_spend_summary(
        self,
        run_id: UUID | None = None,
    ) -> list[AgencySpendSummaryRecord]:
        """
        Build agency spending summary from contracts, payments, and PO data.

        Returns:
            List of AgencySpendSummaryRecord aggregations
        """
        run_id = run_id or uuid4()
        computed_at = datetime.now(timezone.utc).isoformat()

        agency_data: dict[str, dict[str, Any]] = defaultdict(lambda: {
            "agency_names": [],
            "total_contract_value": Decimal("0"),
            "total_payments": Decimal("0"),
            "total_po_value": Decimal("0"),
            "contract_count": 0,
            "vendors": set(),
            "vendor_spend": defaultdict(lambda: Decimal("0")),
            "fiscal_year_spend": defaultdict(lambda: Decimal("0")),
        })

        # Load contracts
        contracts = self._load_silver_records("dc_contracts")
        for record in contracts:
            agency = record.get("agency_name")
            if not agency:
                continue

            agency_key = self._normalize_agency_name(agency)
            data = agency_data[agency_key]
            if agency not in data["agency_names"]:
                data["agency_names"].append(agency)

            amount = self._to_decimal(record.get("contract_amount"))
            if amount and amount > 0:
                data["total_contract_value"] += amount

                # Track vendor spend
                vendor = record.get("supplier_name")
                if vendor:
                    normalized_vendor = self._normalize_vendor_name(vendor)
                    data["vendors"].add(normalized_vendor)
                    data["vendor_spend"][vendor] += amount

                # Track fiscal year spend
                fy = record.get("fiscal_year")
                if fy:
                    try:
                        data["fiscal_year_spend"][str(int(fy))] += amount
                    except (ValueError, TypeError):
                        pass

            data["contract_count"] += 1

        # Stream payments (large dataset)
        self._logger.info("Processing payments for agency summary (streaming)...")
        for record in self._iter_silver_records("dc_payments"):
            agency = record.get("agency_name")
            if not agency:
                continue

            agency_key = self._normalize_agency_name(agency)
            data = agency_data[agency_key]
            if agency not in data["agency_names"]:
                data["agency_names"].append(agency)

            amount = self._to_decimal(record.get("payment_amount"))
            if amount and amount > 0:
                data["total_payments"] += amount

                # Track vendor
                vendor = record.get("supplier_name")
                if vendor:
                    normalized_vendor = self._normalize_vendor_name(vendor)
                    data["vendors"].add(normalized_vendor)
                    data["vendor_spend"][vendor] += amount

        # Stream POs (can be large)
        self._logger.info("Processing POs for agency summary (streaming)...")
        for record in self._iter_silver_records("dc_purchase_orders"):
            agency = record.get("agency_name")
            if not agency:
                continue

            agency_key = self._normalize_agency_name(agency)
            data = agency_data[agency_key]
            if agency not in data["agency_names"]:
                data["agency_names"].append(agency)

            # Calculate PO amount as midpoint
            amount_min = self._to_decimal(record.get("amount_min"))
            amount_max = self._to_decimal(record.get("amount_max"))

            if amount_min is not None and amount_max is not None:
                po_amount = (amount_min + amount_max) / 2
            elif amount_min is not None:
                po_amount = amount_min
            elif amount_max is not None:
                po_amount = amount_max
            else:
                po_amount = Decimal("0")

            if po_amount > 0:
                data["total_po_value"] += po_amount

        # Build records
        records = []
        for agency_key, data in agency_data.items():
            # Get most common agency name
            agency_names = data["agency_names"]
            if agency_names:
                from collections import Counter
                agency_name = Counter(agency_names).most_common(1)[0][0]
            else:
                agency_name = agency_key

            # Get top 10 vendors by spend
            vendor_spend = data["vendor_spend"]
            top_vendors = sorted(
                [(v, float(s)) for v, s in vendor_spend.items()],
                key=lambda x: x[1],
                reverse=True
            )[:10]
            top_vendors_json = json.dumps([{"vendor": v, "spend": s} for v, s in top_vendors])

            # Format fiscal year breakdown
            fy_breakdown = {k: float(v) for k, v in data["fiscal_year_spend"].items()}
            fy_breakdown_json = json.dumps(fy_breakdown)

            record = AgencySpendSummaryRecord(
                agency_name=agency_name,
                agency_name_normalized=agency_key.upper(),
                total_contract_value=float(data["total_contract_value"]),
                total_payments=float(data["total_payments"]),
                total_po_value=float(data["total_po_value"]),
                contract_count=data["contract_count"],
                vendor_count=len(data["vendors"]),
                top_vendors=top_vendors_json,
                fiscal_year_breakdown=fy_breakdown_json,
                computed_at=computed_at,
            )
            records.append(record)

        # Write to storage
        self._write_records(records, "agency_spend_summary", run_id)

        self._logger.info(f"Built {len(records)} agency_spend_summary records")
        return records

    def build_market_trends(
        self,
        run_id: UUID | None = None,
    ) -> list[MarketTrendsRecord]:
        """
        Build market trends from Zillow and RentCast data.

        Returns:
            List of MarketTrendsRecord aggregations
        """
        run_id = run_id or uuid4()
        computed_at = datetime.now(timezone.utc).isoformat()

        # Dictionary to track metrics by (region, metric_name)
        metrics_data: dict[tuple[str, str, str], dict[str, Any]] = {}

        # Load market stats from silver (Zillow data)
        # Use actual source names from silver layer
        market_sources = [
            "zillow_zhvi_metro",
            "zillow_zhvi_zip",
            "zillow_zori_metro",
            "zillow_zori_zip",
            "zillow_market_heat",
            "zillow_inventory",
            "zillow_sales_count",
            "zillow_forecast",
            "zillow_research",
            "rentcast_market_stats",
            "rentcast_rent_estimates",
            "rentcast_value_estimates",
        ]

        for source in market_sources:
            records = self._load_silver_records(source)
            for record in records:
                region_name = record.get("region_name")
                metric_name = record.get("metric_name")
                if not region_name or not metric_name:
                    continue

                region_type = record.get("region_type") or "unknown"
                key = (region_name, region_type, metric_name)

                if key not in metrics_data:
                    metrics_data[key] = {
                        "values": [],  # List of (date, value) tuples
                        "region_name": region_name,
                        "region_type": region_type,
                        "metric_name": metric_name,
                    }

                # Extract value and date
                metric_value = record.get("metric_value")
                metric_date = record.get("metric_date")

                if metric_value is not None and metric_date:
                    try:
                        value = float(metric_value)
                        date_str = str(metric_date).split("T")[0] if "T" in str(metric_date) else str(metric_date)
                        metrics_data[key]["values"].append((date_str, value))
                    except (ValueError, TypeError):
                        pass

        # Build records
        records = []
        for key, data in metrics_data.items():
            values = data["values"]
            if not values:
                continue

            # Sort by date
            values.sort(key=lambda x: x[0])

            # Get latest value
            latest_date, latest_value = values[-1]

            # Calculate YoY change (compare to 12 months ago)
            yoy_change = None
            if len(values) >= 12:
                old_value = values[-12][1]
                if old_value and old_value != 0:
                    yoy_change = ((latest_value - old_value) / old_value) * 100

            # Calculate MoM change
            mom_change = None
            if len(values) >= 2:
                prev_value = values[-2][1]
                if prev_value and prev_value != 0:
                    mom_change = ((latest_value - prev_value) / prev_value) * 100

            # Determine trend direction
            trend_direction = "stable"
            if yoy_change is not None:
                if yoy_change > 2:
                    trend_direction = "up"
                elif yoy_change < -2:
                    trend_direction = "down"

            # Determine metric category
            metric_name = data["metric_name"]
            if "rent" in metric_name.lower() or "zori" in metric_name.lower():
                metric_category = "rent"
            elif "inventory" in metric_name.lower():
                metric_category = "inventory"
            else:
                metric_category = "sale"

            # Get last 24 months of historical values
            historical = values[-24:] if len(values) > 24 else values
            historical_json = json.dumps([{"date": d, "value": v} for d, v in historical])

            record = MarketTrendsRecord(
                region_name=data["region_name"],
                region_type=data["region_type"],
                metric_name=metric_name,
                metric_category=metric_category,
                latest_value=latest_value,
                latest_date=latest_date,
                yoy_change=round(yoy_change, 2) if yoy_change is not None else None,
                mom_change=round(mom_change, 2) if mom_change is not None else None,
                trend_direction=trend_direction,
                historical_values=historical_json,
                computed_at=computed_at,
            )
            records.append(record)

        # Write to storage
        self._write_records(records, "market_trends", run_id)

        self._logger.info(f"Built {len(records)} market_trends records")
        return records

    def _normalize_agency_name(self, name: str) -> str:
        """Normalize agency name for matching."""
        if not name:
            return ""
        return name.lower().strip()

    def _load_silver_records(self, source_name: str) -> list[dict[str, Any]]:
        """Load records from silver layer for a source (small datasets)."""
        return list(self._iter_silver_records(source_name))

    def _iter_silver_records(self, source_name: str):
        """Iterate over records from silver layer, yielding file by file to save memory."""
        source_path = self._silver_path / f"source={source_name}"

        if not source_path.exists():
            self._logger.debug(f"No silver data for {source_name}")
            return

        if PYARROW_AVAILABLE:
            for f in sorted(source_path.glob("**/*.parquet")):
                try:
                    table = pq.read_table(f)
                    for record in table.to_pylist():
                        yield record
                    del table  # Free memory
                except Exception as e:
                    self._logger.warning(f"Could not read {f}: {e}")
        else:
            for f in sorted(source_path.glob("**/*.json")):
                try:
                    with open(f) as fp:
                        data = json.load(fp)
                        if isinstance(data, list):
                            for record in data:
                                yield record
                except Exception as e:
                    self._logger.warning(f"Could not read {f}: {e}")

    def _write_records(
        self,
        records: list[BaseModel],
        dataset_name: str,
        run_id: UUID,
    ) -> None:
        """Write records to gold storage."""
        if not records:
            return

        output_path = self._gold_path / dataset_name
        output_path.mkdir(parents=True, exist_ok=True)

        data = [r.model_dump(mode="json") for r in records]
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

        if PYARROW_AVAILABLE:
            filepath = output_path / f"{timestamp}_{run_id}.parquet"
            table = pa.Table.from_pylist(data)
            pq.write_table(table, filepath, compression="snappy")
        else:
            filepath = output_path / f"{timestamp}_{run_id}.json"
            with open(filepath, "w") as f:
                json.dump(data, f, default=str, indent=2)

        self._logger.debug(f"Wrote {len(records)} records to {filepath}")

    def _normalize_vendor_name(self, name: str) -> str:
        """Normalize vendor name for matching."""
        if not name:
            return ""
        # Simple normalization: lowercase, remove common suffixes
        normalized = name.lower().strip()
        for suffix in [" llc", " inc", " corp", " ltd", " co", " company"]:
            if normalized.endswith(suffix):
                normalized = normalized[:-len(suffix)]
        return normalized.strip()

    def _extract_ward(self, record: dict[str, Any]) -> str | None:
        """Extract ward from a property record."""
        # Try direct ward field (top-level PropertyRecord field)
        ward = record.get("ward")
        if ward is not None and str(ward).strip():
            return str(ward).strip()

        # Try extra_fields (various possible field names) - for backwards compatibility
        extra = record.get("extra_fields", {})
        if isinstance(extra, dict):
            for field in ["ward", "WARD", "PRMS_WARD", "prms_ward"]:
                ward = extra.get(field)
                if ward is not None and str(ward).strip():
                    return str(ward).strip()

        # Try extra_fields as JSON string (some records serialize extra_fields)
        if isinstance(extra, str):
            try:
                extra_dict = json.loads(extra)
                for field in ["ward", "WARD", "PRMS_WARD"]:
                    ward = extra_dict.get(field)
                    if ward is not None and str(ward).strip():
                        return str(ward).strip()
            except (json.JSONDecodeError, TypeError):
                pass

        return None

    def _extract_assessed_value(self, record: dict[str, Any]) -> float | None:
        """Extract assessed value from a property record (prefer property_tax/total_assessment over price)."""
        # Try property_tax field first (contains total_assessment for DC ITSPE data)
        property_tax = record.get("property_tax")
        if property_tax is not None:
            try:
                return float(property_tax)
            except (ValueError, TypeError):
                pass

        # Try extra_fields for tax assessment values
        extra = record.get("extra_fields", {})
        if isinstance(extra, dict):
            # Prefer total_assessment (tax assessed value)
            for field in ["total_assessment", "ASSESSMENT", "assessment"]:
                value = extra.get(field)
                if value is not None:
                    try:
                        return float(value)
                    except (ValueError, TypeError):
                        pass

        # Fall back to price field
        price = record.get("price")
        if price is not None:
            try:
                return float(price)
            except (ValueError, TypeError):
                pass

        return None

    def _to_decimal(self, value: Any) -> Decimal | None:
        """Convert value to Decimal."""
        if value is None:
            return None
        try:
            return Decimal(str(value))
        except Exception:
            return None

    def _safe_mean(self, values: list[Decimal]) -> Decimal | None:
        """Calculate mean of Decimal values."""
        if not values:
            return None
        return sum(values) / len(values)

    def _safe_mean_float(self, values: list[float]) -> float | None:
        """Calculate mean of float values."""
        if not values:
            return None
        return sum(values) / len(values)

    def _safe_median(self, values: list[Decimal]) -> Decimal | None:
        """Calculate median of Decimal values."""
        if not values:
            return None
        sorted_values = sorted(values)
        n = len(sorted_values)
        mid = n // 2
        if n % 2 == 0:
            return (sorted_values[mid - 1] + sorted_values[mid]) / 2
        return sorted_values[mid]

    def _safe_percentile(self, values: list[Decimal], percentile: int) -> Decimal | None:
        """Calculate percentile of Decimal values."""
        if not values:
            return None
        sorted_values = sorted(values)
        idx = int(len(sorted_values) * percentile / 100)
        idx = min(idx, len(sorted_values) - 1)
        return sorted_values[idx]

    def _safe_median_float(self, values: list[float]) -> float | None:
        """Calculate median of float values."""
        if not values:
            return None
        sorted_values = sorted(values)
        n = len(sorted_values)
        mid = n // 2
        if n % 2 == 0:
            return (sorted_values[mid - 1] + sorted_values[mid]) / 2
        return sorted_values[mid]

    def _safe_percentile_float(self, values: list[float], percentile: int) -> Decimal | None:
        """Calculate percentile of float values, return as Decimal."""
        if not values:
            return None
        sorted_values = sorted(values)
        idx = int(len(sorted_values) * percentile / 100)
        idx = min(idx, len(sorted_values) - 1)
        return Decimal(str(sorted_values[idx]))


# =============================================================================
# CLI Entry Point
# =============================================================================


def build_gold_layer(
    silver_path: str = "data/silver",
    gold_path: str = "data/gold",
    datasets: list[str] | None = None,
) -> dict[str, int]:
    """
    Build gold layer datasets.

    Args:
        silver_path: Path to silver layer data
        gold_path: Path to write gold layer data
        datasets: List of datasets to build (default: all)

    Returns:
        Dict mapping dataset name to record count
    """
    writer = GoldWriter(gold_path, silver_path)
    results = {}

    all_datasets = ["spend_by_vendor", "property_values_by_ward", "agency_spend_summary", "market_trends"]
    datasets = datasets or all_datasets

    if "spend_by_vendor" in datasets:
        records = writer.build_spend_by_vendor()
        results["spend_by_vendor"] = len(records)

    if "property_values_by_ward" in datasets:
        records = writer.build_property_values_by_ward()
        results["property_values_by_ward"] = len(records)

    if "agency_spend_summary" in datasets:
        records = writer.build_agency_spend_summary()
        results["agency_spend_summary"] = len(records)

    if "market_trends" in datasets:
        records = writer.build_market_trends()
        results["market_trends"] = len(records)

    return results


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Build gold layer datasets")
    parser.add_argument("--silver-path", default="data/silver")
    parser.add_argument("--gold-path", default="data/gold")
    parser.add_argument("--dataset", dest="datasets", nargs="+", help="Specific datasets to build")

    args = parser.parse_args()

    results = build_gold_layer(
        silver_path=args.silver_path,
        gold_path=args.gold_path,
        datasets=args.datasets,
    )

    print("Gold layer build complete:")
    for dataset, count in results.items():
        print(f"  {dataset}: {count} records")
