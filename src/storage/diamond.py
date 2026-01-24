"""
Diamond layer storage for ML-ready feature sets.

The Diamond layer provides machine learning-optimized data:
- Cleaned numeric fields with currency normalization
- Outlier handling with documented rules
- Date-derived features (year, month, recency)
- Cross-source joins where keys exist
- Feature documentation and metadata

Key datasets:
1. vendor_features - ML features for vendor analysis
2. property_features - ML features for property valuation
"""

from __future__ import annotations

import json
import logging
import math
from datetime import datetime, timedelta
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


# =============================================================================
# Feature Documentation
# =============================================================================


FEATURE_DOCUMENTATION = {
    "vendor_features": {
        "description": "ML-ready features for vendor/supplier analysis",
        "join_keys": ["supplier_name_normalized"],
        "update_frequency": "daily",
        "features": {
            "total_spend_normalized": {
                "type": "float64",
                "description": "Total spend normalized to 0-1 scale across all vendors",
                "outlier_handling": "winsorized at 99th percentile",
            },
            "log_total_spend": {
                "type": "float64",
                "description": "Log-transformed total spend (log10)",
                "outlier_handling": "log transformation handles skew",
            },
            "contract_frequency": {
                "type": "float64",
                "description": "Average contracts per year",
                "outlier_handling": "capped at 99th percentile",
            },
            "agency_diversity_score": {
                "type": "float64",
                "description": "Normalized count of unique agencies (0-1)",
                "outlier_handling": "none (bounded by design)",
            },
            "tenure_years": {
                "type": "float64",
                "description": "Years since first contract",
                "outlier_handling": "none",
            },
            "recency_days": {
                "type": "int32",
                "description": "Days since most recent contract",
                "outlier_handling": "capped at 3650 (10 years)",
            },
            "is_active": {
                "type": "bool",
                "description": "Has contract in last 2 years",
                "outlier_handling": "n/a",
            },
            "fiscal_year_latest": {
                "type": "int32",
                "description": "Most recent fiscal year with activity",
                "outlier_handling": "none",
            },
        },
    },
    "property_features": {
        "description": "ML-ready features for property valuation models",
        "join_keys": ["source_record_id", "ssl"],
        "update_frequency": "daily",
        "features": {
            "price_normalized": {
                "type": "float64",
                "description": "Sale price normalized to 0-1 scale",
                "outlier_handling": "winsorized at 1st and 99th percentile",
            },
            "log_price": {
                "type": "float64",
                "description": "Log-transformed price (log10)",
                "outlier_handling": "log transformation handles skew",
            },
            "price_per_sqft_normalized": {
                "type": "float64",
                "description": "Price per sqft normalized to 0-1",
                "outlier_handling": "winsorized at 1st and 99th percentile",
            },
            "sqft_normalized": {
                "type": "float64",
                "description": "Square footage normalized to 0-1",
                "outlier_handling": "winsorized at 99th percentile",
            },
            "lot_sqft_normalized": {
                "type": "float64",
                "description": "Lot size normalized to 0-1",
                "outlier_handling": "winsorized at 99th percentile",
            },
            "building_age": {
                "type": "int32",
                "description": "Current year - year_built",
                "outlier_handling": "capped at 200 years",
            },
            "bedrooms_capped": {
                "type": "int32",
                "description": "Bedroom count capped at 10",
                "outlier_handling": "capped at 10",
            },
            "bathrooms_capped": {
                "type": "float32",
                "description": "Bathroom count capped at 10",
                "outlier_handling": "capped at 10",
            },
            "sale_month": {
                "type": "int32",
                "description": "Month of sale (1-12)",
                "outlier_handling": "n/a",
            },
            "sale_year": {
                "type": "int32",
                "description": "Year of sale",
                "outlier_handling": "none",
            },
            "sale_quarter": {
                "type": "int32",
                "description": "Quarter of sale (1-4)",
                "outlier_handling": "n/a",
            },
            "days_since_sale": {
                "type": "int32",
                "description": "Days since property sold",
                "outlier_handling": "capped at 3650 (10 years)",
            },
            "is_recent_sale": {
                "type": "bool",
                "description": "Sold in last 2 years",
                "outlier_handling": "n/a",
            },
            "ward_encoded": {
                "type": "int32",
                "description": "Ward as integer (1-8, 0 for unknown)",
                "outlier_handling": "n/a",
            },
        },
    },
}


# =============================================================================
# Diamond Layer Schemas
# =============================================================================


class VendorFeatures(BaseModel):
    """ML-ready vendor risk feature set."""

    record_id: UUID = Field(default_factory=uuid4)
    feature_timestamp: datetime = Field(default_factory=lambda: datetime.utcnow())

    # Identifiers (not for ML, for joining)
    vendor_id: str = Field(..., description="Normalized vendor identifier")
    supplier_name: str = Field(...)
    supplier_name_normalized: str = Field(...)

    # Numeric features (per spec)
    total_lifetime_spend: float = Field(default=0.0, description="Total contract + payment value")
    total_lifetime_spend_log: float = Field(default=0.0, description="log1p(total_lifetime_spend) for modeling")
    contract_count: int = Field(default=0)
    agency_diversity: float = Field(default=0.0, description="agencies / total contracts")
    avg_contract_size: float = Field(default=0.0)
    tenure_days: int = Field(default=0, description="Days between first contract and last payment")
    contracts_per_year: float = Field(default=0.0, description="contract_count / tenure_years")
    payment_to_contract_ratio: float = Field(default=0.0, description="total_payments / total_contract_value")

    # Categorical features (encoded)
    is_cbe: int = Field(default=0, description="1 if certified business enterprise, 0 otherwise")
    primary_agency_encoded: int = Field(default=0, description="Label-encoded most common agency")

    # Time-based features
    months_since_last_contract: int = Field(default=0)
    months_since_last_payment: int = Field(default=0)

    # Legacy/additional features
    total_spend_normalized: float = Field(default=0.0)
    log_total_spend: float = Field(default=0.0)
    contract_frequency: float = Field(default=0.0)
    agency_diversity_score: float = Field(default=0.0)
    tenure_years: float = Field(default=0.0)
    recency_days: int = Field(default=0)
    is_active: bool = Field(default=False)
    fiscal_year_latest: int | None = Field(default=None)

    # Raw values for reference
    total_contract_value: float = Field(default=0.0)
    total_payment_value: float = Field(default=0.0)
    agency_count: int = Field(default=0)

    model_config = {"extra": "allow"}


class PropertyFeatures(BaseModel):
    """ML-ready property valuation feature set."""

    record_id: UUID = Field(default_factory=uuid4)
    feature_timestamp: datetime = Field(default_factory=lambda: datetime.utcnow())

    # Identifiers (not for ML, for joining)
    property_id: str = Field(..., description="SSL or address hash")
    source_record_id: str = Field(...)
    ssl: str | None = Field(default=None)

    # Core property features (per spec)
    sqft: float | None = Field(default=None)
    sqft_log: float | None = Field(default=None, description="log(sqft) for linear models")
    bedrooms: int | None = Field(default=None)
    bathrooms: float | None = Field(default=None)
    year_built: int | None = Field(default=None)
    age_years: int | None = Field(default=None, description="current_year - year_built")
    lot_sqft: float | None = Field(default=None)

    # Location features (per spec)
    ward: int | None = Field(default=None, description="Ward number 1-8")
    ward_avg_value: float | None = Field(default=None, description="From gold/property_values_by_ward")
    ward_median_value: float | None = Field(default=None)
    latitude: float | None = Field(default=None)
    longitude: float | None = Field(default=None)

    # Market context features (per spec)
    zhvi_current: float | None = Field(default=None, description="Current ZHVI for region")
    zhvi_yoy_change: float | None = Field(default=None)

    # Derived features (per spec)
    price_per_sqft_vs_ward: float | None = Field(default=None, description="Property price/sqft vs ward average")
    rooms_per_sqft: float | None = Field(default=None, description="(bedrooms + bathrooms) / sqft * 1000")

    # Property type (one-hot encoded per spec)
    is_single_family: int = Field(default=0)
    is_condo: int = Field(default=0)
    is_townhouse: int = Field(default=0)

    # Target variable (per spec)
    assessed_value: float | None = Field(default=None, description="Target for prediction models")

    # Legacy/additional normalized features
    price_normalized: float | None = Field(default=None)
    log_price: float | None = Field(default=None)
    price_per_sqft_normalized: float | None = Field(default=None)
    sqft_normalized: float | None = Field(default=None)
    lot_sqft_normalized: float | None = Field(default=None)
    building_age: int | None = Field(default=None)
    bedrooms_capped: int | None = Field(default=None)
    bathrooms_capped: float | None = Field(default=None)

    # Date-derived features
    sale_month: int | None = Field(default=None)
    sale_year: int | None = Field(default=None)
    sale_quarter: int | None = Field(default=None)
    days_since_sale: int | None = Field(default=None)
    is_recent_sale: bool = Field(default=False)

    # Additional location features
    ward_encoded: int = Field(default=0)
    city: str | None = Field(default=None)
    state: str | None = Field(default=None)

    # Raw values for reference
    price: float | None = Field(default=None)

    model_config = {"extra": "allow"}


# =============================================================================
# Diamond Layer Writer
# =============================================================================


class DiamondWriter:
    """
    Writer for the Diamond layer (ML-ready features).

    Transforms gold and silver data into feature vectors suitable
    for machine learning models.
    """

    def __init__(
        self,
        diamond_path: str | Path,
        gold_path: str | Path,
        silver_path: str | Path,
    ):
        self._diamond_path = Path(diamond_path)
        self._gold_path = Path(gold_path)
        self._silver_path = Path(silver_path)
        self._diamond_path.mkdir(parents=True, exist_ok=True)
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def build_vendor_features(
        self,
        run_id: UUID | None = None,
    ) -> list[VendorFeatures]:
        """
        Build ML-ready vendor risk features from gold layer data.

        Features per spec:
        - vendor_id (identifier)
        - total_lifetime_spend (log1p for modeling)
        - contract_count, agency_diversity, avg_contract_size
        - tenure_days, contracts_per_year, payment_to_contract_ratio
        - is_cbe (encoded), primary_agency_encoded
        - months_since_last_contract, months_since_last_payment
        """
        run_id = run_id or uuid4()
        now = datetime.utcnow()

        # Load gold layer spend data
        gold_records = self._load_gold_records("spend_by_vendor")

        if not gold_records:
            self._logger.warning("No spend_by_vendor data found")
            return []

        # Build agency encoding map (label encoding for primary agency)
        all_agencies = set()
        for record in gold_records:
            agencies = record.get("agencies", []) or record.get("agencies_served", [])
            all_agencies.update(agencies)
        agency_to_id = {agency: idx for idx, agency in enumerate(sorted(all_agencies))}

        # Calculate normalization parameters
        all_spends = []
        all_contracts = []
        max_agencies = 0

        for record in gold_records:
            total_contract = float(record.get("total_contract_value", 0))
            total_payments = float(record.get("total_payments", 0) or record.get("total_payment_value", 0))
            all_spends.append(total_contract + total_payments)
            all_contracts.append(record.get("contract_count", 0))
            agencies = record.get("agencies", []) or record.get("agencies_served", [])
            max_agencies = max(max_agencies, len(agencies))

        # Calculate percentiles for winsorization
        spend_99 = self._percentile(all_spends, 99) if all_spends else 1
        contract_99 = self._percentile(all_contracts, 99) if all_contracts else 1

        # Build features
        features = []
        for record in gold_records:
            total_contract = float(record.get("total_contract_value", 0))
            total_payments = float(record.get("total_payments", 0) or record.get("total_payment_value", 0))
            total_spend = total_contract + total_payments
            contracts = record.get("contract_count", 0)
            agencies = record.get("agencies", []) or record.get("agencies_served", [])

            # Vendor ID (use from gold or generate)
            vendor_id = record.get("vendor_id", "")
            if not vendor_id:
                import hashlib
                normalized = record.get("vendor_name_normalized", "") or record.get("supplier_name_normalized", "")
                vendor_id = hashlib.md5(normalized.encode()).hexdigest()[:16]

            # Winsorize and normalize spend
            spend_winsorized = min(total_spend, spend_99)
            spend_normalized = spend_winsorized / spend_99 if spend_99 > 0 else 0

            # Log transforms
            log_spend = math.log10(total_spend + 1) if total_spend > 0 else 0
            total_lifetime_spend_log = math.log1p(total_spend)

            # Agency diversity (agencies / contracts)
            agency_diversity = len(agencies) / contracts if contracts > 0 else 0
            agency_diversity_score = len(agencies) / max_agencies if max_agencies > 0 else 0

            # Average contract size
            avg_contract_size = total_contract / contracts if contracts > 0 else 0

            # Tenure calculation (days between first contract and last payment)
            first_contract = record.get("first_contract_date") or record.get("earliest_contract_date")
            last_payment = record.get("last_payment_date") or record.get("latest_contract_date")
            tenure_days = 0
            tenure_years = 0.0
            months_since_last_contract = 0
            months_since_last_payment = 0
            recency = 3650  # Default to 10 years

            first_dt = None
            if first_contract:
                try:
                    first_str = str(first_contract).split("T")[0]
                    first_dt = datetime.fromisoformat(first_str)
                    months_since_last_contract = int((now - first_dt).days / 30)
                except (ValueError, TypeError):
                    pass

            last_dt = None
            if last_payment:
                try:
                    last_str = str(last_payment).split("T")[0]
                    last_dt = datetime.fromisoformat(last_str)
                    months_since_last_payment = int((now - last_dt).days / 30)
                    recency = min((now - last_dt).days, 3650)
                except (ValueError, TypeError):
                    pass

            if first_dt and last_dt:
                tenure_days = max(0, (last_dt - first_dt).days)
                tenure_years = tenure_days / 365.25
            elif first_dt:
                tenure_days = (now - first_dt).days
                tenure_years = tenure_days / 365.25

            # Contracts per year
            contracts_per_year = contracts / tenure_years if tenure_years > 0 else 0

            # Payment to contract ratio
            payment_to_contract_ratio = total_payments / total_contract if total_contract > 0 else 0

            # CBE status (from gold data)
            is_cbe_bool = record.get("is_cbe", False)
            is_cbe = 1 if is_cbe_bool else 0

            # Primary agency encoded
            primary_agency = agencies[0] if agencies else ""
            primary_agency_encoded = agency_to_id.get(primary_agency, 0)

            # Fiscal years
            fiscal_years = record.get("fiscal_years", [])

            # Contract frequency
            year_span = (max(fiscal_years) - min(fiscal_years) + 1) if fiscal_years else 1
            contract_freq = min(contracts / year_span, contract_99) / contract_99 if contract_99 > 0 else 0

            is_active = recency < 730  # 2 years

            feature = VendorFeatures(
                # New spec fields
                vendor_id=vendor_id,
                supplier_name=record.get("vendor_name", "") or record.get("supplier_name", ""),
                supplier_name_normalized=record.get("vendor_name_normalized", "") or record.get("supplier_name_normalized", ""),
                total_lifetime_spend=total_spend,
                total_lifetime_spend_log=total_lifetime_spend_log,
                contract_count=contracts,
                agency_diversity=agency_diversity,
                avg_contract_size=avg_contract_size,
                tenure_days=tenure_days,
                contracts_per_year=contracts_per_year,
                payment_to_contract_ratio=payment_to_contract_ratio,
                is_cbe=is_cbe,
                primary_agency_encoded=primary_agency_encoded,
                months_since_last_contract=months_since_last_contract,
                months_since_last_payment=months_since_last_payment,
                # Legacy fields
                total_spend_normalized=spend_normalized,
                log_total_spend=log_spend,
                contract_frequency=contract_freq,
                agency_diversity_score=agency_diversity_score,
                tenure_years=tenure_years,
                recency_days=recency,
                is_active=is_active,
                fiscal_year_latest=max(fiscal_years) if fiscal_years else None,
                total_contract_value=total_contract,
                total_payment_value=total_payments,
                agency_count=len(agencies),
            )
            features.append(feature)

        # Write to storage
        self._write_features(features, "vendor_features", run_id)
        self._write_documentation("vendor_features")

        self._logger.info(f"Built {len(features)} vendor features")
        return features

    def build_property_features(
        self,
        run_id: UUID | None = None,
    ) -> list[PropertyFeatures]:
        """
        Build ML-ready property valuation features from silver and gold layer data.

        Features per spec:
        - property_id (identifier), sqft, sqft_log, bedrooms, bathrooms
        - year_built, age_years, lot_sqft
        - ward, ward_avg_value, ward_median_value
        - latitude, longitude
        - zhvi_current, zhvi_yoy_change
        - price_per_sqft_vs_ward, rooms_per_sqft
        - is_single_family, is_condo, is_townhouse (one-hot)
        - assessed_value (target)
        """
        run_id = run_id or uuid4()
        now = datetime.utcnow()
        current_year = now.year

        # Load silver layer property data
        silver_records = []
        for source in ["dc_residential_cama", "dc_itspe_property_land"]:
            silver_records.extend(self._load_silver_records(source))

        if not silver_records:
            self._logger.warning("No property data found")
            return []

        # Load gold layer ward data for ward_avg_value and ward_median_value
        ward_stats = {}
        gold_ward_data = self._load_gold_records("property_values_by_ward")
        for ward_record in gold_ward_data:
            ward_key = str(ward_record.get("ward", ""))
            ward_stats[ward_key] = {
                "avg_value": self._safe_float(ward_record.get("avg_assessed_value") or ward_record.get("avg_sale_price")),
                "median_value": self._safe_float(ward_record.get("median_assessed_value") or ward_record.get("median_sale_price")),
                "avg_price_per_sqft": self._safe_float(ward_record.get("avg_price_per_sqft")),
            }

        # Load gold layer market trends for ZHVI data
        zhvi_data = {}
        market_trends = self._load_gold_records("market_trends")
        for trend_record in market_trends:
            metric_name = trend_record.get("metric_name", "")
            if "zhvi" in metric_name.lower():
                region = trend_record.get("region_name", "")
                zhvi_data[region] = {
                    "current": self._safe_float(trend_record.get("latest_value")),
                    "yoy_change": self._safe_float(trend_record.get("yoy_change")),
                }

        # Get DC-level ZHVI if available (use as default)
        dc_zhvi = zhvi_data.get("Washington") or zhvi_data.get("Washington, DC") or {}
        default_zhvi_current = dc_zhvi.get("current")
        default_zhvi_yoy = dc_zhvi.get("yoy_change")

        # Calculate normalization parameters
        all_prices = []
        all_price_per_sqft = []
        all_sqft = []
        all_lot_sqft = []

        for record in silver_records:
            price = self._safe_float(record.get("price"))
            sqft = self._safe_float(record.get("sqft"))

            if price and price > 0:
                all_prices.append(price)
            if sqft and sqft > 0:
                all_sqft.append(sqft)
            if price and sqft and sqft > 0:
                all_price_per_sqft.append(price / sqft)

            lot_sqft = self._safe_float(record.get("lot_sqft"))
            if lot_sqft and lot_sqft > 0:
                all_lot_sqft.append(lot_sqft)

        # Calculate percentiles for winsorization
        price_1 = self._percentile(all_prices, 1) if all_prices else 0
        price_99 = self._percentile(all_prices, 99) if all_prices else 1
        ppsf_1 = self._percentile(all_price_per_sqft, 1) if all_price_per_sqft else 0
        ppsf_99 = self._percentile(all_price_per_sqft, 99) if all_price_per_sqft else 1
        sqft_99 = self._percentile(all_sqft, 99) if all_sqft else 1
        lot_99 = self._percentile(all_lot_sqft, 99) if all_lot_sqft else 1

        # Build features
        features = []
        for record in silver_records:
            price = self._safe_float(record.get("price"))
            sqft_val = self._safe_float(record.get("sqft"))
            lot_sqft_val = self._safe_float(record.get("lot_sqft"))
            bedrooms = self._safe_int(record.get("bedrooms"))
            bathrooms = self._safe_float(record.get("bathrooms"))
            year_built = self._safe_int(record.get("year_built"))
            latitude = self._safe_float(record.get("latitude"))
            longitude = self._safe_float(record.get("longitude"))

            # Get SSL and generate property_id
            ssl = record.get("listing_id") or record.get("extra_fields", {}).get("ssl")
            source_record_id = str(record.get("source_record_id", ""))
            if ssl:
                import hashlib
                property_id = hashlib.md5(str(ssl).encode()).hexdigest()[:16]
            elif source_record_id:
                import hashlib
                property_id = hashlib.md5(source_record_id.encode()).hexdigest()[:16]
            else:
                import hashlib
                address = record.get("address", "")
                property_id = hashlib.md5(address.encode()).hexdigest()[:16]

            # sqft_log per spec
            sqft_log = math.log(sqft_val) if sqft_val and sqft_val > 0 else None

            # age_years per spec
            age_years = None
            if year_built and year_built > 1600:
                age_years = current_year - year_built

            # Ward extraction and encoding
            ward_str = record.get("ward") or ""
            if not ward_str:
                extra = record.get("extra_fields", {})
                if isinstance(extra, dict):
                    ward_str = extra.get("ward", "") or extra.get("WARD", "")

            ward_int = None
            ward_encoded = 0
            if ward_str:
                try:
                    ward_int = int(str(ward_str).replace("Ward ", "").strip())
                    if 1 <= ward_int <= 8:
                        ward_encoded = ward_int
                except (ValueError, TypeError):
                    pass

            # Get ward statistics
            ward_key = str(ward_int) if ward_int else ""
            ward_info = ward_stats.get(ward_key, {})
            ward_avg_value = ward_info.get("avg_value")
            ward_median_value = ward_info.get("median_value")
            ward_avg_ppsf = ward_info.get("avg_price_per_sqft")

            # ZHVI data (use DC-level as default)
            zhvi_current = default_zhvi_current
            zhvi_yoy_change = default_zhvi_yoy

            # price_per_sqft_vs_ward: Property price/sqft vs ward average
            price_per_sqft_vs_ward = None
            if price and sqft_val and sqft_val > 0 and ward_avg_ppsf and ward_avg_ppsf > 0:
                property_ppsf = price / sqft_val
                price_per_sqft_vs_ward = property_ppsf / ward_avg_ppsf

            # rooms_per_sqft: (bedrooms + bathrooms) / sqft * 1000
            rooms_per_sqft = None
            if bedrooms is not None and bathrooms is not None and sqft_val and sqft_val > 0:
                rooms_per_sqft = (bedrooms + bathrooms) / sqft_val * 1000

            # Property type one-hot encoding
            property_type = record.get("property_type", "").lower()
            is_single_family = 1 if "single" in property_type or "sfr" in property_type else 0
            is_condo = 1 if "condo" in property_type else 0
            is_townhouse = 1 if "town" in property_type else 0

            # Normalized price
            price_normalized = None
            log_price = None
            if price and price > 0:
                price_winsorized = max(price_1, min(price, price_99))
                price_normalized = (price_winsorized - price_1) / (price_99 - price_1) if price_99 > price_1 else 0
                log_price = math.log10(price + 1)

            # Price per sqft normalized
            ppsf_normalized = None
            if price and sqft_val and sqft_val > 0:
                ppsf = price / sqft_val
                ppsf_winsorized = max(ppsf_1, min(ppsf, ppsf_99))
                ppsf_normalized = (ppsf_winsorized - ppsf_1) / (ppsf_99 - ppsf_1) if ppsf_99 > ppsf_1 else 0

            # Size features normalized
            sqft_normalized = min(sqft_val, sqft_99) / sqft_99 if sqft_val and sqft_99 > 0 else None
            lot_normalized = min(lot_sqft_val, lot_99) / lot_99 if lot_sqft_val and lot_99 > 0 else None

            # Building age
            building_age = None
            if year_built and year_built > 1600:
                building_age = min(current_year - year_built, 200)

            # Capped features
            bedrooms_capped = min(bedrooms, 10) if bedrooms is not None else None
            bathrooms_capped = min(bathrooms, 10) if bathrooms is not None else None

            # Date features
            sale_date = record.get("sold_date") or record.get("sale_date")
            sale_month = None
            sale_year = None
            sale_quarter = None
            days_since_sale = None
            is_recent_sale = False

            if sale_date:
                try:
                    if isinstance(sale_date, str):
                        sale_dt = datetime.fromisoformat(sale_date.replace("Z", "+00:00"))
                    else:
                        sale_dt = sale_date
                    sale_dt = sale_dt.replace(tzinfo=None) if hasattr(sale_dt, 'tzinfo') else sale_dt
                    sale_month = sale_dt.month
                    sale_year = sale_dt.year
                    sale_quarter = (sale_dt.month - 1) // 3 + 1
                    days_since_sale = min((now - sale_dt).days, 3650)
                    is_recent_sale = days_since_sale < 730
                except (ValueError, TypeError, AttributeError):
                    pass

            feature = PropertyFeatures(
                # New spec fields
                property_id=property_id,
                source_record_id=source_record_id,
                ssl=ssl,
                sqft=sqft_val,
                sqft_log=sqft_log,
                bedrooms=bedrooms,
                bathrooms=bathrooms,
                year_built=year_built,
                age_years=age_years,
                lot_sqft=lot_sqft_val,
                ward=ward_int,
                ward_avg_value=ward_avg_value,
                ward_median_value=ward_median_value,
                latitude=latitude,
                longitude=longitude,
                zhvi_current=zhvi_current,
                zhvi_yoy_change=zhvi_yoy_change,
                price_per_sqft_vs_ward=price_per_sqft_vs_ward,
                rooms_per_sqft=rooms_per_sqft,
                is_single_family=is_single_family,
                is_condo=is_condo,
                is_townhouse=is_townhouse,
                assessed_value=price,  # Using price as assessed_value (target)
                # Legacy/additional fields
                price_normalized=price_normalized,
                log_price=log_price,
                price_per_sqft_normalized=ppsf_normalized,
                sqft_normalized=sqft_normalized,
                lot_sqft_normalized=lot_normalized,
                building_age=building_age,
                bedrooms_capped=bedrooms_capped,
                bathrooms_capped=bathrooms_capped,
                sale_month=sale_month,
                sale_year=sale_year,
                sale_quarter=sale_quarter,
                days_since_sale=days_since_sale,
                is_recent_sale=is_recent_sale,
                ward_encoded=ward_encoded,
                city=record.get("city"),
                state=record.get("state"),
                price=price,
            )
            features.append(feature)

        # Write to storage
        self._write_features(features, "property_features", run_id)
        self._write_documentation("property_features")

        self._logger.info(f"Built {len(features)} property features")
        return features

    def _load_gold_records(self, dataset: str) -> list[dict[str, Any]]:
        """Load records from gold layer."""
        records = []
        path = self._gold_path / dataset

        if not path.exists():
            return records

        if PYARROW_AVAILABLE:
            for f in path.glob("**/*.parquet"):
                try:
                    table = pq.read_table(f)
                    records.extend(table.to_pylist())
                except Exception as e:
                    self._logger.warning(f"Could not read {f}: {e}")
        else:
            for f in path.glob("**/*.json"):
                try:
                    with open(f) as fp:
                        data = json.load(fp)
                        if isinstance(data, list):
                            records.extend(data)
                except Exception as e:
                    self._logger.warning(f"Could not read {f}: {e}")

        return records

    def _load_silver_records(self, source: str) -> list[dict[str, Any]]:
        """Load records from silver layer."""
        records = []
        path = self._silver_path / f"source={source}"

        if not path.exists():
            return records

        if PYARROW_AVAILABLE:
            for f in path.glob("**/*.parquet"):
                try:
                    table = pq.read_table(f)
                    records.extend(table.to_pylist())
                except Exception as e:
                    self._logger.warning(f"Could not read {f}: {e}")
        else:
            for f in path.glob("**/*.json"):
                try:
                    with open(f) as fp:
                        data = json.load(fp)
                        if isinstance(data, list):
                            records.extend(data)
                except Exception as e:
                    self._logger.warning(f"Could not read {f}: {e}")

        return records

    def _write_features(
        self,
        features: list[BaseModel],
        dataset: str,
        run_id: UUID,
    ) -> None:
        """Write features to diamond storage."""
        if not features:
            return

        path = self._diamond_path / dataset
        path.mkdir(parents=True, exist_ok=True)

        data = [f.model_dump(mode="json") for f in features]
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

        if PYARROW_AVAILABLE:
            filepath = path / f"{timestamp}_{run_id}.parquet"
            table = pa.Table.from_pylist(data)
            pq.write_table(table, filepath, compression="snappy")
        else:
            filepath = path / f"{timestamp}_{run_id}.json"
            with open(filepath, "w") as f:
                json.dump(data, f, default=str, indent=2)

        self._logger.debug(f"Wrote {len(features)} features to {filepath}")

    def _write_documentation(self, dataset: str) -> None:
        """Write feature documentation to diamond storage."""
        if dataset not in FEATURE_DOCUMENTATION:
            return

        path = self._diamond_path / dataset / "_documentation.json"
        with open(path, "w") as f:
            json.dump(FEATURE_DOCUMENTATION[dataset], f, indent=2)

    def _percentile(self, values: list[float], p: int) -> float:
        """Calculate percentile."""
        if not values:
            return 0.0
        sorted_values = sorted(values)
        idx = int(len(sorted_values) * p / 100)
        idx = min(idx, len(sorted_values) - 1)
        return sorted_values[idx]

    def _safe_float(self, value: Any) -> float | None:
        """Safe float conversion."""
        if value is None:
            return None
        try:
            f = float(value)
            if math.isnan(f) or math.isinf(f):
                return None
            return f
        except (ValueError, TypeError):
            return None

    def _safe_int(self, value: Any) -> int | None:
        """Safe int conversion."""
        f = self._safe_float(value)
        if f is None:
            return None
        return int(f)


# =============================================================================
# CLI Entry Point
# =============================================================================


def build_diamond_layer(
    silver_path: str = "data/silver",
    gold_path: str = "data/gold",
    diamond_path: str = "data/diamond",
    datasets: list[str] | None = None,
) -> dict[str, int]:
    """
    Build diamond layer datasets.

    Args:
        silver_path: Path to silver layer data
        gold_path: Path to gold layer data
        diamond_path: Path to write diamond layer data
        datasets: List of datasets to build (default: all)

    Returns:
        Dict mapping dataset name to feature count
    """
    writer = DiamondWriter(diamond_path, gold_path, silver_path)
    results = {}

    all_datasets = ["vendor_features", "property_features"]
    datasets = datasets or all_datasets

    if "vendor_features" in datasets:
        features = writer.build_vendor_features()
        results["vendor_features"] = len(features)

    if "property_features" in datasets:
        features = writer.build_property_features()
        results["property_features"] = len(features)

    return results


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Build diamond layer features")
    parser.add_argument("--silver-path", default="data/silver")
    parser.add_argument("--gold-path", default="data/gold")
    parser.add_argument("--diamond-path", default="data/diamond")
    parser.add_argument("--datasets", nargs="+", help="Specific datasets to build")

    args = parser.parse_args()

    results = build_diamond_layer(
        silver_path=args.silver_path,
        gold_path=args.gold_path,
        diamond_path=args.diamond_path,
        datasets=args.datasets,
    )

    print("Diamond layer build complete:")
    for dataset, count in results.items():
        print(f"  {dataset}: {count} features")
