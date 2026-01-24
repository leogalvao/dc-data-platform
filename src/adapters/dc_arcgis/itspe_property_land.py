"""
ITSPE Property & Land adapter for DC GIS (Layer 53).

Endpoint: Property_and_Land_WebMercator/MapServer/53
Data: Integrated Tax System Public Extract (ITSPE) - Office of Tax and Revenue data

This layer contains tax assessment and sales data including:
- SSL: Square/Suffix/Lot identifier (primary key)
- PREMISEADDR: Premise address on record
- OWNERNAME: Owner name on record
- PRICE/SALEDATE: Last sale price and date
- LAND_VALUE/IMPR_VALUE/ASSESSMENT: Tax assessed values
- CLASS3/USECODE: Tax classification codes
- LANDAREA: Lot size in square feet
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from ...core.registry import register_scraper
from ...schemas.base import LineageInfo
from ...schemas.metadata import SourceConfig, SourceType
from ...schemas.tabular import ListingType, PropertyRecord, PropertyType
from .base import DCArcGISBaseAdapter

# =============================================================================
# Configuration
# =============================================================================

ITSPE_PROPERTY_LAND_CONFIG = {
    "name": "dc_itspe_property_land",
    "display_name": "DC ITSPE Property & Land (OTR)",
    "description": "DC Office of Tax and Revenue property tax assessment and sales data (Layer 53)",
    "base_url": "https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Property_and_Land_WebMercator/MapServer",
    "layer_id": 53,
    "field_mappings": {
        # Identification
        "SSL": "ssl",
        "PREMISEADDR": "premise_address",
        "OWNERNAME": "owner_name",
        # Sales
        "PRICE": "price",
        "SALEDATE": "sale_date",
        # Tax Assessment Values
        "LAND_VALUE": "land_value",
        "IMPR_VALUE": "improvement_value",
        "ASSESSMENT": "total_assessment",
        # Tax Classification
        "CLASS3": "tax_class",
        "USECODE": "use_code",
        # Property Details
        "LANDAREA": "land_area",
        # System fields
        "OBJECTID": "source_record_id",
    },
}

# Use code to property type mapping
# Reference: DC OTR Use Code definitions
USE_CODE_PROPERTY_TYPE = {
    # Residential
    "11": PropertyType.TOWNHOUSE,      # Residential Row Single Family
    "12": PropertyType.SINGLE_FAMILY,  # Residential Detached
    "13": PropertyType.TOWNHOUSE,      # Residential Semi-Detached
    "14": PropertyType.CONDO,          # Residential Condo Horizontal
    "15": PropertyType.CONDO,          # Residential Condo Vertical
    "16": PropertyType.MULTI_FAMILY,   # Residential Multi-Family
    "17": PropertyType.MULTI_FAMILY,   # Residential Flats
    "19": PropertyType.MULTI_FAMILY,   # Residential Cooperative
    # Commercial
    "21": PropertyType.COMMERCIAL,     # Commercial Office
    "22": PropertyType.COMMERCIAL,     # Commercial Hotel
    "23": PropertyType.COMMERCIAL,     # Commercial Retail
    "24": PropertyType.COMMERCIAL,     # Commercial Mixed Use
    "25": PropertyType.COMMERCIAL,     # Commercial Garage
    "26": PropertyType.COMMERCIAL,     # Commercial Industrial
    "29": PropertyType.COMMERCIAL,     # Commercial Other
    # Special/Exempt
    "31": PropertyType.OTHER,          # Government
    "32": PropertyType.OTHER,          # Religious
    "33": PropertyType.OTHER,          # Educational
    "34": PropertyType.OTHER,          # Charitable
    "35": PropertyType.OTHER,          # Hospital
    # Vacant
    "91": PropertyType.LAND,           # Vacant Residential
    "92": PropertyType.LAND,           # Vacant Commercial
    "93": PropertyType.LAND,           # Vacant Mixed
}

# Tax class to property type fallback
TAX_CLASS_PROPERTY_TYPE = {
    "001": PropertyType.SINGLE_FAMILY,  # Class 1 Residential
    "002": PropertyType.COMMERCIAL,     # Class 2 Commercial
    "003": PropertyType.LAND,           # Class 3 Vacant
    "004": PropertyType.OTHER,          # Class 4 Exempt
}


def get_default_config() -> SourceConfig:
    """Get default configuration for ITSPE Property & Land source."""
    return SourceConfig(
        name="dc_itspe_property_land",
        display_name=ITSPE_PROPERTY_LAND_CONFIG["display_name"],
        description=ITSPE_PROPERTY_LAND_CONFIG["description"],
        source_type=SourceType.REST_API,
        base_url=ITSPE_PROPERTY_LAND_CONFIG["base_url"],
        requires_auth=False,
        requests_per_second=2.0,
        max_workers=5,
        batch_size=1000,
        timeout_seconds=60,
        max_retries=3,
        adapter_class="src.adapters.dc_arcgis.itspe_property_land.ITSPEPropertyLandAdapter",
        output_record_types=["PropertyRecord"],
        extra={
            "layer_id": ITSPE_PROPERTY_LAND_CONFIG["layer_id"],
            "rate_limit_wait": 30,
        },
    )


# =============================================================================
# Adapter Implementation
# =============================================================================

@register_scraper("dc_itspe_property_land")
class ITSPEPropertyLandAdapter(DCArcGISBaseAdapter):
    """
    Adapter for DC ITSPE Property & Land data (Layer 53).

    Fetches tax assessment and sales data from DC GIS MapServer.
    Includes property identification, valuation, sales history,
    and tax classification information.
    """

    SOURCE_NAME = "dc_itspe_property_land"
    SOURCE_VERSION = "1.0.0"
    SUPPORTED_RECORD_TYPES = [PropertyRecord]

    BASE_URL = ITSPE_PROPERTY_LAND_CONFIG["base_url"]
    LAYER_ID = ITSPE_PROPERTY_LAND_CONFIG["layer_id"]
    FIELD_MAPPINGS = ITSPE_PROPERTY_LAND_CONFIG["field_mappings"]

    def _build_query_url(self, offset: int, limit: int) -> str:
        """Build the ArcGIS REST query URL for MapServer."""
        base = f"{self.BASE_URL}/{self.LAYER_ID}/query"
        params = (
            f"where=1%3D1"
            f"&outFields=*"
            f"&returnGeometry=true"
            f"&outSR=4326"
            f"&f=json"
            f"&resultOffset={offset}"
            f"&resultRecordCount={limit}"
        )
        return f"{base}?{params}"

    async def _fetch_total_records(self) -> int:
        """Query the API for total record count."""
        import aiohttp

        url = (
            f"{self.BASE_URL}/{self.LAYER_ID}/query"
            f"?where=1%3D1&returnCountOnly=true&f=json"
        )

        if self._session is None:
            timeout = aiohttp.ClientTimeout(total=self.config.timeout_seconds)
            self._session = aiohttp.ClientSession(timeout=timeout)

        try:
            async with self._session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get("count", 0)
        except Exception as e:
            self._logger.error(f"Failed to get total count: {e}")

        return 0

    def validate(self, data: dict[str, Any]) -> tuple[bool, list[str]]:
        """Validate an ITSPE record."""
        errors = []

        # Check for identifying field (SSL or OBJECTID)
        if not data.get("SSL") and not data.get("OBJECTID"):
            errors.append("Record missing SSL or OBJECTID")

        # Validate numeric fields
        numeric_fields = [
            "PRICE", "LAND_VALUE", "IMPR_VALUE", "ASSESSMENT", "LANDAREA"
        ]
        for field in numeric_fields:
            value = data.get(field)
            if value is not None:
                try:
                    v = float(value)
                    if v < 0:
                        errors.append(f"Invalid {field}: {value}")
                except (ValueError, TypeError):
                    errors.append(f"Non-numeric {field}: {value}")

        # Validate use code if present (should be numeric string)
        use_code = data.get("USECODE")
        if use_code is not None and use_code != "":
            try:
                int(use_code)
            except (ValueError, TypeError):
                # Use codes can be strings, this is fine
                pass

        return len(errors) == 0, errors

    def _convert_timestamp(self, ts: int | str | None) -> str | None:
        """Convert Unix timestamp (milliseconds) or date string to ISO date string."""
        if ts is None:
            return None

        # Already a date string
        if isinstance(ts, str):
            return ts if ts else None

        # Unix timestamp in milliseconds
        try:
            dt = datetime.fromtimestamp(ts / 1000)
            return dt.strftime("%Y-%m-%d")
        except (ValueError, TypeError, OSError):
            return None

    def _determine_property_type(
        self, use_code: str | None, tax_class: str | None
    ) -> PropertyType:
        """
        Determine property type from use code or tax class.

        Follows governance rules: prefer Use Code, fall back to Class.
        """
        # Try use code first
        if use_code:
            use_code_str = str(use_code).strip()
            if use_code_str in USE_CODE_PROPERTY_TYPE:
                return USE_CODE_PROPERTY_TYPE[use_code_str]

        # Fall back to tax class
        if tax_class:
            tax_class_str = str(tax_class).strip()
            if tax_class_str in TAX_CLASS_PROPERTY_TYPE:
                return TAX_CLASS_PROPERTY_TYPE[tax_class_str]

        # Default
        return PropertyType.OTHER

    def normalize(self, data: dict[str, Any]) -> PropertyRecord:
        """Normalize raw API data to PropertyRecord."""

        # Safe type conversion helpers
        def safe_int(val: Any) -> int | None:
            if val is None:
                return None
            try:
                return int(float(val))
            except (ValueError, TypeError):
                return None

        def safe_float(val: Any) -> float | None:
            if val is None:
                return None
            try:
                return float(val)
            except (ValueError, TypeError):
                return None

        def safe_decimal(val: Any) -> Decimal | None:
            if val is None:
                return None
            try:
                return Decimal(str(val))
            except (ValueError, TypeError):
                return None

        # Core property fields
        ssl = data.get("SSL", "").strip() if data.get("SSL") else None
        premise_address = data.get("PREMISEADDR", "").strip() if data.get("PREMISEADDR") else None
        owner_name = data.get("OWNERNAME", "").strip() if data.get("OWNERNAME") else None

        # Sale information
        price = safe_decimal(data.get("PRICE"))
        sale_date_str = self._convert_timestamp(data.get("SALEDATE"))

        # Tax assessment values
        land_value = safe_decimal(data.get("LAND_VALUE"))
        improvement_value = safe_decimal(data.get("IMPR_VALUE"))
        total_assessment = safe_decimal(data.get("ASSESSMENT"))

        # Tax classification
        tax_class = data.get("CLASS3")
        use_code = data.get("USECODE")

        # Property details
        lot_sqft = safe_int(data.get("LANDAREA"))

        # Coordinates from geometry
        latitude = safe_float(data.get("_latitude"))
        longitude = safe_float(data.get("_longitude"))

        # Determine property type
        property_type = self._determine_property_type(use_code, tax_class)

        # Determine listing type based on price
        listing_type = ListingType.SOLD if price else ListingType.OFF_MARKET

        # Extract ward from PRMS_WARD field
        ward = data.get("PRMS_WARD")
        if ward is not None:
            try:
                ward = str(int(ward))
            except (ValueError, TypeError):
                ward = None

        # Build extra fields for tax-specific data
        extra_fields = {}
        tax_fields = [
            ("ssl", ssl),
            ("owner_name", owner_name),
            ("land_value", float(land_value) if land_value else None),
            ("improvement_value", float(improvement_value) if improvement_value else None),
            ("total_assessment", float(total_assessment) if total_assessment else None),
            ("tax_class", tax_class),
            ("use_code", use_code),
            ("sale_date", sale_date_str),
            ("ward", ward),
            ("neighborhood", data.get("NBHDNAME")),
            ("neighborhood_code", data.get("NBHD")),
            ("quadrant", data.get("QDRNTNAME")),
        ]

        for key, value in tax_fields:
            if value is not None:
                extra_fields[key] = value

        # Create lineage with content hash
        raw_content = str(sorted(data.items())).encode()
        lineage = self._make_lineage()
        lineage = lineage.model_copy(
            update={"raw_hash": LineageInfo.compute_hash(raw_content)}
        )

        # Create the record
        record = PropertyRecord(
            lineage=lineage,
            extra_fields=extra_fields,
            source_record_id=str(data.get("OBJECTID", ssl or "")),
            listing_type=listing_type,
            property_type=property_type,
            listing_id=ssl,
            price=price,
            price_per_sqft=None,  # No sqft in this layer
            original_price=None,
            price_reduced=False,
            bedrooms=None,  # Not in ITSPE layer
            bathrooms=None,  # Not in ITSPE layer
            sqft=None,  # GBA not in ITSPE layer, use CAMA for this
            lot_sqft=lot_sqft,
            year_built=None,  # Not in ITSPE layer
            stories=None,
            garage_spaces=None,
            address=premise_address or ssl,
            city="Washington",
            state="DC",
            zip_code=None,
            ward=ward,  # DC Ward (1-8) from PRMS_WARD field
            neighborhood=data.get("NBHDNAME"),
            latitude=latitude,
            longitude=longitude,
            sold_date=sale_date_str,
            property_tax=total_assessment,
        )

        return record


# Register config on import
def _register_config():
    """Register ITSPE Property & Land config with the registry."""
    from ...core.registry import ScraperRegistry
    ScraperRegistry.register_config(get_default_config())

_register_config()
