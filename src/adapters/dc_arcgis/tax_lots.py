"""
Tax Lots adapter for DC GIS.

Endpoint: Property_and_Land_WebMercator/FeatureServer/39
Data: Tax lot boundaries and parcel information.

Fields include:
- SQUARE, SUFFIX, LOT: SSL components
- SSL: Square/Suffix/Lot identifier
- ACCOUNT_ID: Tax account identifier
- COMPUTED_AREA_SF, SURVEYED_AREA_SF, RECORD_AREA_SF: Area measurements
- BLDG_NUM, QUADRANT, ZIP5, ZIP4: Address components
- CREATION_DT, RECORDATION_DT, EXPIRATION_DT: Date fields
- STATUS, LOT_TYPE: Classification fields
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from ...core.registry import register_scraper
from ...schemas.base import LineageInfo
from ...schemas.metadata import SourceConfig, SourceType
from ...schemas.tabular import PropertyRecord, PropertyType, ListingType
from .base import DCArcGISBaseAdapter

# =============================================================================
# Configuration
# =============================================================================

TAX_LOTS_CONFIG = {
    "name": "dc_tax_lots",
    "display_name": "DC Tax Lots",
    "description": "DC tax lot boundaries and parcel information",
    "base_url": "https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Property_and_Land_WebMercator/FeatureServer",
    "layer_id": 39,
    "field_mappings": {
        "SSL": "ssl",
        "SQUARE": "square",
        "SUFFIX": "suffix",
        "LOT": "lot",
        "ACCOUNT_ID": "account_id",
        "COMPUTED_AREA_SF": "computed_area_sf",
        "SURVEYED_AREA_SF": "surveyed_area_sf",
        "RECORD_AREA_SF": "record_area_sf",
        "BLDG_NUM": "building_number",
        "QUADRANT": "quadrant",
        "ZIP5": "zip5",
        "ZIP4": "zip4",
        "STATUS": "status",
        "LOT_TYPE": "lot_type",
        "OBJECTID": "source_record_id",
    },
}


def get_default_config() -> SourceConfig:
    """Get default configuration for Tax Lots source."""
    return SourceConfig(
        name="dc_tax_lots",
        display_name=TAX_LOTS_CONFIG["display_name"],
        description=TAX_LOTS_CONFIG["description"],
        source_type=SourceType.REST_API,
        base_url=TAX_LOTS_CONFIG["base_url"],
        requires_auth=False,
        requests_per_second=2.0,
        max_workers=5,
        batch_size=1000,
        timeout_seconds=60,
        max_retries=3,
        adapter_class="src.adapters.dc_arcgis.tax_lots.TaxLotsAdapter",
        output_record_types=["PropertyRecord"],
        extra={
            "layer_id": TAX_LOTS_CONFIG["layer_id"],
            "rate_limit_wait": 30,
        },
    )


# =============================================================================
# Adapter Implementation
# =============================================================================

@register_scraper("dc_tax_lots")
class TaxLotsAdapter(DCArcGISBaseAdapter):
    """
    Adapter for DC Tax Lots data.

    Fetches tax lot boundaries and parcel information from DC GIS FeatureServer.
    Includes SSL identifiers, area measurements, and address components.
    """

    SOURCE_NAME = "dc_tax_lots"
    SOURCE_VERSION = "1.0.0"
    SUPPORTED_RECORD_TYPES = [PropertyRecord]

    BASE_URL = TAX_LOTS_CONFIG["base_url"]
    LAYER_ID = TAX_LOTS_CONFIG["layer_id"]
    FIELD_MAPPINGS = TAX_LOTS_CONFIG["field_mappings"]

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
        """Validate a Tax Lot record."""
        errors = []

        # Check for identifying field (SSL or OBJECTID)
        if not data.get("SSL") and not data.get("OBJECTID"):
            errors.append("Record missing SSL or OBJECTID")

        # Validate area fields
        for field in ["COMPUTED_AREA_SF", "SURVEYED_AREA_SF", "RECORD_AREA_SF"]:
            value = data.get(field)
            if value is not None:
                try:
                    v = float(value)
                    if v < 0:
                        errors.append(f"Invalid {field}: {value}")
                except (ValueError, TypeError):
                    errors.append(f"Non-numeric {field}: {value}")

        return len(errors) == 0, errors

    def _convert_timestamp(self, ts: int | str | None) -> str | None:
        """Convert Unix timestamp (milliseconds) or date string to ISO date string."""
        if ts is None:
            return None

        if isinstance(ts, str):
            return ts if ts else None

        try:
            dt = datetime.fromtimestamp(ts / 1000)
            return dt.strftime("%Y-%m-%d")
        except (ValueError, TypeError, OSError):
            return None

    def normalize(self, data: dict[str, Any]) -> PropertyRecord:
        """Normalize raw API data to PropertyRecord."""

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

        ssl = data.get("SSL", "").strip() if data.get("SSL") else None
        lot_sqft = safe_int(data.get("RECORD_AREA_SF") or data.get("COMPUTED_AREA_SF"))

        # Build extra fields
        extra_fields = {}
        lot_fields = [
            ("ssl", ssl),
            ("square", data.get("SQUARE")),
            ("suffix", data.get("SUFFIX")),
            ("lot", data.get("LOT")),
            ("account_id", data.get("ACCOUNT_ID")),
            ("col", data.get("COL")),
            ("computed_area_sf", safe_float(data.get("COMPUTED_AREA_SF"))),
            ("surveyed_area_sf", safe_float(data.get("SURVEYED_AREA_SF"))),
            ("record_area_sf", safe_float(data.get("RECORD_AREA_SF"))),
            ("building_number", safe_int(data.get("BLDG_NUM"))),
            ("building_number_ext", data.get("BLDG_NUM_EXT")),
            ("building_number_end_range", safe_int(data.get("BLDG_NUM_END_RANGE"))),
            ("quadrant", data.get("QUADRANT")),
            ("status", safe_int(data.get("STATUS"))),
            ("lot_type", safe_int(data.get("LOT_TYPE"))),
            ("is_rear_of", safe_int(data.get("IS_REAR_OF"))),
            ("is_theoretical_lot", safe_int(data.get("IS_THEORETICAL_LOT"))),
            ("underlies_condo", safe_int(data.get("UNDERLIES_CONDO"))),
            ("condo_regime_num", data.get("CONDO_REGIME_NUM")),
            ("condo_book_num", data.get("CONDO_BOOK_NUM")),
            ("condo_page_num", data.get("CONDO_PAGE_NUM")),
            ("book_num", data.get("BOOK_NUM")),
            ("page_num", data.get("PAGE_NUM")),
            ("mar_id", safe_int(data.get("MAR_ID"))),
            ("creation_date", self._convert_timestamp(data.get("CREATION_DT"))),
            ("recordation_date", self._convert_timestamp(data.get("RECORDATION_DT"))),
            ("expiration_date", self._convert_timestamp(data.get("EXPIRATION_DT"))),
            ("effective_tax_year", self._convert_timestamp(data.get("EFFECTIVETAXYEAR"))),
            ("kill_date", self._convert_timestamp(data.get("KILL_DT"))),
            ("narrative", data.get("NARRATIVE")),
        ]

        for key, value in lot_fields:
            if value is not None:
                extra_fields[key] = value

        # Create lineage with content hash
        raw_content = str(sorted(data.items())).encode()
        lineage = self._make_lineage()
        lineage = lineage.model_copy(
            update={"raw_hash": LineageInfo.compute_hash(raw_content)}
        )

        # Build address from components
        bldg_num = data.get("BLDG_NUM")
        quadrant = data.get("QUADRANT")
        address = None
        if bldg_num and quadrant:
            address = f"{bldg_num} {quadrant}"

        zip_code = data.get("ZIP5")
        if zip_code and data.get("ZIP4"):
            zip_code = f"{zip_code}-{data.get('ZIP4')}"

        record = PropertyRecord(
            lineage=lineage,
            extra_fields=extra_fields,
            source_record_id=str(data.get("OBJECTID", ssl or "")),
            listing_type=ListingType.OFF_MARKET,
            property_type=PropertyType.LAND,
            listing_id=ssl,
            price=None,
            lot_sqft=lot_sqft,
            address=address or ssl,
            city="Washington",
            state="DC",
            zip_code=zip_code,
        )

        return record


# Register config on import
def _register_config():
    """Register Tax Lots config with the registry."""
    from ...core.registry import ScraperRegistry
    ScraperRegistry.register_config(get_default_config())

_register_config()
