"""
Property Sales (CAMA) adapter for DC GIS.

Endpoint: Property_and_Land_WebMercator/FeatureServer/57
Data: Property sales records from the tax assessment roll (CAMA database).

Fields include:
- SSL: Square/Suffix/Lot identifier
- SALE_DATE: Date of sale
- SALE_PRICE: Sale amount
- QUALIFIED: Whether sale is qualified (arm's length transaction)
- SALE_CODE: Sale classification code
- SALE_CURR_OWNER: Current owner from sale
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from ...core.registry import register_scraper
from ...schemas.base import LineageInfo
from ...schemas.metadata import SourceConfig, SourceType
from ...schemas.tabular import PropertyRecord, PropertyType, ListingType
from .base import DCArcGISBaseAdapter

# =============================================================================
# Configuration
# =============================================================================

PROPERTY_SALES_CONFIG = {
    "name": "dc_property_sales_cama",
    "display_name": "DC Property Sales (CAMA)",
    "description": "DC property sales records from tax assessment roll (Computer Assisted Mass Appraisal)",
    "base_url": "https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Property_and_Land_WebMercator/FeatureServer",
    "layer_id": 57,
    "field_mappings": {
        "SSL": "ssl",
        "SALE_DATE": "sale_date",
        "SALE_PRICE": "price",
        "QUALIFIED": "qualified",
        "SALE_CODE": "sale_code",
        "SALE_CURR_OWNER": "owner",
        "ROW_NUMBER": "row_number",
        "OBJECTID": "source_record_id",
    },
}


def get_default_config() -> SourceConfig:
    """Get default configuration for Property Sales source."""
    return SourceConfig(
        name="dc_property_sales_cama",
        display_name=PROPERTY_SALES_CONFIG["display_name"],
        description=PROPERTY_SALES_CONFIG["description"],
        source_type=SourceType.REST_API,
        base_url=PROPERTY_SALES_CONFIG["base_url"],
        requires_auth=False,
        requests_per_second=2.0,
        max_workers=5,
        batch_size=1000,
        timeout_seconds=60,
        max_retries=3,
        adapter_class="src.adapters.dc_arcgis.property_sales_cama.PropertySalesCAMAAdapter",
        output_record_types=["PropertyRecord"],
        extra={
            "layer_id": PROPERTY_SALES_CONFIG["layer_id"],
            "rate_limit_wait": 30,
        },
    )


# =============================================================================
# Adapter Implementation
# =============================================================================

@register_scraper("dc_property_sales_cama")
class PropertySalesCAMAAdapter(DCArcGISBaseAdapter):
    """
    Adapter for DC Property Sales (CAMA) data.

    Fetches property sales records from DC GIS FeatureServer.
    This is a public extract from the tax assessment database (tax roll).
    """

    SOURCE_NAME = "dc_property_sales_cama"
    SOURCE_VERSION = "1.0.0"
    SUPPORTED_RECORD_TYPES = [PropertyRecord]

    BASE_URL = PROPERTY_SALES_CONFIG["base_url"]
    LAYER_ID = PROPERTY_SALES_CONFIG["layer_id"]
    FIELD_MAPPINGS = PROPERTY_SALES_CONFIG["field_mappings"]

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
        """Validate a Property Sales record."""
        errors = []

        # Check for identifying field (SSL or OBJECTID)
        if not data.get("SSL") and not data.get("OBJECTID"):
            errors.append("Record missing SSL or OBJECTID")

        # Validate sale price
        price = data.get("SALE_PRICE")
        if price is not None:
            try:
                p = float(price)
                if p < 0:
                    errors.append(f"Invalid SALE_PRICE: {price}")
            except (ValueError, TypeError):
                errors.append(f"Non-numeric SALE_PRICE: {price}")

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
        price = safe_float(data.get("SALE_PRICE"))
        sale_date_str = self._convert_timestamp(data.get("SALE_DATE"))

        # Build extra fields
        extra_fields = {}
        sale_fields = [
            ("ssl", ssl),
            ("sale_date", sale_date_str),
            ("qualified", data.get("QUALIFIED")),
            ("sale_code", data.get("SALE_CODE")),
            ("owner", data.get("SALE_CURR_OWNER")),
            ("row_number", safe_int(data.get("ROW_NUMBER"))),
            ("gis_last_mod_dttm", self._convert_timestamp(data.get("GIS_LAST_MOD_DTTM"))),
        ]

        for key, value in sale_fields:
            if value is not None:
                extra_fields[key] = value

        # Create lineage with content hash
        raw_content = str(sorted(data.items())).encode()
        lineage = self._make_lineage()
        lineage = lineage.model_copy(
            update={"raw_hash": LineageInfo.compute_hash(raw_content)}
        )

        # Determine if qualified sale (arm's length transaction)
        qualified = data.get("QUALIFIED", "").upper()
        is_qualified = qualified in ("Q", "Y", "YES", "TRUE", "1")

        # Create unique record ID combining SSL and sale date
        record_id = str(data.get("OBJECTID", ""))
        if not record_id and ssl and sale_date_str:
            record_id = f"{ssl}_{sale_date_str}"

        record = PropertyRecord(
            lineage=lineage,
            extra_fields=extra_fields,
            source_record_id=record_id,
            listing_type=ListingType.SOLD,
            property_type=PropertyType.OTHER,  # Type not specified in this dataset
            listing_id=ssl,
            price=Decimal(str(price)) if price else None,
            address=ssl,  # SSL is the property identifier in DC
            city="Washington",
            state="DC",
            sold_date=sale_date_str,
        )

        return record


# Register config on import
def _register_config():
    """Register Property Sales CAMA config with the registry."""
    from ...core.registry import ScraperRegistry
    ScraperRegistry.register_config(get_default_config())

_register_config()
