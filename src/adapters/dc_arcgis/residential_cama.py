"""
Residential CAMA (Computer Assisted Mass Appraisal) adapter for DC GIS.

Endpoint: Property_and_Land_WebMercator/FeatureServer/25
Data: Residential property assessments with detailed characteristics.

Fields include:
- SSL: Square/Suffix/Lot identifier
- BATHRM, HF_BATHRM: Full and half bathrooms
- BEDRM, ROOMS: Bedrooms and total rooms
- AYB, YR_RMDL, EYB: Actual Year Built, Year Remodeled, Effective Year Built
- STORIES, GBA, LANDAREA: Building dimensions
- PRICE, SALEDATE, QUALIFIED: Sale information
- STYLE_D, STRUCT_D, GRADE_D, CNDTN_D: Property characteristics
- HEAT_D, AC, EXTWALL_D, ROOF_D: Building features
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from ...core.registry import register_scraper
from ...schemas.base import LineageInfo
from ...schemas.metadata import SourceConfig, SourceType
from ...schemas.tabular import ListingType, PropertyRecord, PropertyType
from .base import DCArcGISBaseAdapter

# =============================================================================
# Configuration
# =============================================================================

RESIDENTIAL_CAMA_CONFIG = {
    "name": "dc_residential_cama",
    "display_name": "DC Residential CAMA",
    "description": "DC residential property assessment data (Computer Assisted Mass Appraisal)",
    "base_url": "https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Property_and_Land_WebMercator/FeatureServer",
    "layer_id": 25,
    "field_mappings": {
        "SSL": "ssl",
        "BATHRM": "bathrooms",
        "HF_BATHRM": "half_bathrooms",
        "BEDRM": "bedrooms",
        "ROOMS": "rooms",
        "AYB": "year_built",
        "YR_RMDL": "year_remodeled",
        "EYB": "effective_year_built",
        "STORIES": "stories",
        "GBA": "sqft",
        "LANDAREA": "lot_sqft",
        "PRICE": "price",
        "SALEDATE": "sale_date",
        "QUALIFIED": "qualified_sale",
        "SALE_NUM": "sale_number",
        "NUM_UNITS": "num_units",
        "KITCHENS": "kitchens",
        "FIREPLACES": "fireplaces",
        "HEAT_D": "heating_type",
        "AC": "has_ac",
        "STYLE_D": "style",
        "STRUCT_D": "structure_type",
        "GRADE_D": "grade",
        "CNDTN_D": "condition",
        "EXTWALL_D": "exterior_wall",
        "ROOF_D": "roof_type",
        "INTWALL_D": "interior_wall",
        "USECODE": "use_code",
        "OBJECTID": "source_record_id",
    },
}


def get_default_config() -> SourceConfig:
    """Get default configuration for Residential CAMA source."""
    return SourceConfig(
        name="dc_residential_cama",
        display_name=RESIDENTIAL_CAMA_CONFIG["display_name"],
        description=RESIDENTIAL_CAMA_CONFIG["description"],
        source_type=SourceType.REST_API,
        base_url=RESIDENTIAL_CAMA_CONFIG["base_url"],
        requires_auth=False,
        requests_per_second=2.0,
        max_workers=5,
        batch_size=1000,
        timeout_seconds=60,
        max_retries=3,
        adapter_class="src.adapters.dc_arcgis.residential_cama.ResidentialCAMAAdapter",
        output_record_types=["PropertyRecord"],
        extra={
            "layer_id": RESIDENTIAL_CAMA_CONFIG["layer_id"],
            "rate_limit_wait": 30,
        },
    )


# =============================================================================
# Adapter Implementation
# =============================================================================

@register_scraper("dc_residential_cama")
class ResidentialCAMAAdapter(DCArcGISBaseAdapter):
    """
    Adapter for DC Residential CAMA data.

    Fetches residential property assessment data from DC GIS FeatureServer.
    Includes detailed property characteristics like bedrooms, bathrooms,
    year built, sale price, condition, and building features.
    """

    SOURCE_NAME = "dc_residential_cama"
    SOURCE_VERSION = "1.0.0"
    SUPPORTED_RECORD_TYPES = [PropertyRecord]

    BASE_URL = RESIDENTIAL_CAMA_CONFIG["base_url"]
    LAYER_ID = RESIDENTIAL_CAMA_CONFIG["layer_id"]
    FIELD_MAPPINGS = RESIDENTIAL_CAMA_CONFIG["field_mappings"]

    def _build_query_url(self, offset: int, limit: int) -> str:
        """Build the ArcGIS REST query URL for FeatureServer."""
        base = f"{self.BASE_URL}/{self.LAYER_ID}/query"
        params = (
            f"where=1%3D1"
            f"&outFields=*"
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
        """Validate a CAMA record."""
        errors = []

        # Check for identifying field (SSL or OBJECTID)
        if not data.get("SSL") and not data.get("OBJECTID"):
            errors.append("Record missing SSL or OBJECTID")

        # Validate numeric fields
        for field in ["BATHRM", "BEDRM", "ROOMS", "STORIES", "GBA", "PRICE"]:
            value = data.get(field)
            if value is not None:
                try:
                    v = float(value)
                    if v < 0:
                        errors.append(f"Invalid {field}: {value}")
                except (ValueError, TypeError):
                    errors.append(f"Non-numeric {field}: {value}")

        # Validate year built
        ayb = data.get("AYB")
        if ayb is not None:
            try:
                year = int(float(ayb))
                if year < 1600 or year > datetime.now().year + 1:
                    errors.append(f"Invalid year built: {ayb}")
            except (ValueError, TypeError):
                errors.append(f"Non-numeric year built: {ayb}")

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

    def normalize(self, data: dict[str, Any]) -> PropertyRecord:
        """Normalize raw API data to PropertyRecord."""

        # Extract and convert fields
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

        # Core property fields
        ssl = data.get("SSL", "").strip() if data.get("SSL") else None
        bedrooms = safe_int(data.get("BEDRM"))
        full_baths = safe_float(data.get("BATHRM"))
        half_baths = safe_float(data.get("HF_BATHRM"))
        bathrooms = full_baths
        if bathrooms is not None and half_baths is not None:
            bathrooms = full_baths + (half_baths * 0.5)

        sqft = safe_int(data.get("GBA"))
        lot_sqft = safe_int(data.get("LANDAREA"))
        stories_raw = safe_float(data.get("STORIES"))
        stories = int(stories_raw) if stories_raw is not None else None  # Round down fractional stories
        year_built = safe_int(data.get("AYB"))
        price = safe_float(data.get("PRICE"))

        # Sale date conversion
        sale_date_str = self._convert_timestamp(data.get("SALEDATE"))

        # Build extra fields for property characteristics
        extra_fields = {}
        characteristic_fields = [
            ("ssl", data.get("SSL")),
            ("rooms", safe_int(data.get("ROOMS"))),
            ("half_bathrooms", safe_float(data.get("HF_BATHRM"))),
            ("stories_raw", stories_raw),  # Preserve fractional stories (e.g., 3.25 for "3.5 Story")
            ("year_remodeled", safe_int(data.get("YR_RMDL"))),
            ("effective_year_built", safe_int(data.get("EYB"))),
            ("num_units", safe_int(data.get("NUM_UNITS"))),
            ("kitchens", safe_int(data.get("KITCHENS"))),
            ("fireplaces", safe_int(data.get("FIREPLACES"))),
            ("heating_type", data.get("HEAT_D")),
            ("has_ac", data.get("AC")),
            ("style", data.get("STYLE_D")),
            ("structure_type", data.get("STRUCT_D")),
            ("grade", data.get("GRADE_D")),
            ("condition", data.get("CNDTN_D")),
            ("exterior_wall", data.get("EXTWALL_D")),
            ("roof_type", data.get("ROOF_D")),
            ("interior_wall", data.get("INTWALL_D")),
            ("use_code", safe_int(data.get("USECODE"))),
            ("qualified_sale", data.get("QUALIFIED")),
            ("sale_number", safe_int(data.get("SALE_NUM"))),
            ("sale_date", sale_date_str),
        ]

        for key, value in characteristic_fields:
            if value is not None:
                extra_fields[key] = value

        # Create lineage with content hash
        raw_content = str(sorted(data.items())).encode()
        lineage = self._make_lineage()
        lineage = lineage.model_copy(
            update={"raw_hash": LineageInfo.compute_hash(raw_content)}
        )

        # Determine property type based on structure
        struct_type = data.get("STRUCT_D", "").lower() if data.get("STRUCT_D") else ""
        if "row" in struct_type:
            property_type = PropertyType.TOWNHOUSE
        elif "semi" in struct_type or "attach" in struct_type:
            property_type = PropertyType.TOWNHOUSE
        elif "condo" in struct_type:
            property_type = PropertyType.CONDO
        else:
            property_type = PropertyType.SINGLE_FAMILY

        # Create the record
        record = PropertyRecord(
            lineage=lineage,
            extra_fields=extra_fields,
            source_record_id=str(data.get("OBJECTID", ssl or "")),
            listing_type=ListingType.SOLD if price else ListingType.FOR_SALE,
            property_type=property_type,
            listing_id=ssl,
            price=price,
            price_per_sqft=round(price / sqft, 2) if price and sqft else None,
            original_price=None,
            price_reduced=False,
            bedrooms=bedrooms,
            bathrooms=bathrooms,
            sqft=sqft,
            lot_sqft=lot_sqft,
            year_built=year_built,
            stories=stories,
            garage_spaces=None,
            address=ssl,  # SSL is the property identifier in DC
            city="Washington",
            state="DC",
            zip_code=None,
            latitude=None,
            longitude=None,
            sold_date=sale_date_str,
        )

        return record


# Register config on import
def _register_config():
    """Register Residential CAMA config with the registry."""
    from ...core.registry import ScraperRegistry
    ScraperRegistry.register_config(get_default_config())

_register_config()
