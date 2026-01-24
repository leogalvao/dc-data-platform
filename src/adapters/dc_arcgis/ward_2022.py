"""
Ward 2022 adapter for DC GIS.

Endpoint: Administrative_Other_Boundaries_WebMercator/MapServer/53
Data: 2022 election Ward boundaries with Census 2020 demographic data.

Fields include:
- WARD: Ward number (1-8)
- NAME: Area name with LSAD term
- REP_NAME, REP_PHONE, REP_EMAIL, REP_OFFICE: Representative info
- WARD_ID: Ward identifier
- GEOID, GEOCODE: Geographic identifiers
- STATE, STUSAB: State codes
"""

from __future__ import annotations

from typing import Any

from ...core.registry import register_scraper
from ...schemas.base import LineageInfo
from ...schemas.metadata import SourceConfig, SourceType
from ...schemas.tabular import DemographicRecord
from .base import DCArcGISBaseAdapter

# =============================================================================
# Configuration
# =============================================================================

WARD_2022_CONFIG = {
    "name": "dc_ward_2022",
    "display_name": "DC Ward Boundaries 2022",
    "description": "DC 2022 election Ward boundaries with Census 2020 demographic data",
    "base_url": "https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Administrative_Other_Boundaries_WebMercator/MapServer",
    "layer_id": 53,
    "field_mappings": {
        "WARD": "ward",
        "NAME": "name",
        "REP_NAME": "representative_name",
        "REP_PHONE": "representative_phone",
        "REP_EMAIL": "representative_email",
        "REP_OFFICE": "representative_office",
        "WEB_URL": "website",
        "WARD_ID": "ward_id",
        "LABEL": "label",
        "STUSAB": "state_abbr",
        "SUMLEV": "summary_level",
        "GEOID": "geoid",
        "GEOCODE": "geocode",
        "STATE": "state_fips",
        "OBJECTID": "source_record_id",
    },
}


def get_default_config() -> SourceConfig:
    """Get default configuration for Ward 2022 source."""
    return SourceConfig(
        name="dc_ward_2022",
        display_name=WARD_2022_CONFIG["display_name"],
        description=WARD_2022_CONFIG["description"],
        source_type=SourceType.REST_API,
        base_url=WARD_2022_CONFIG["base_url"],
        requires_auth=False,
        requests_per_second=2.0,
        max_workers=5,
        batch_size=1000,
        timeout_seconds=60,
        max_retries=3,
        adapter_class="src.adapters.dc_arcgis.ward_2022.Ward2022Adapter",
        output_record_types=["DemographicRecord"],
        extra={
            "layer_id": WARD_2022_CONFIG["layer_id"],
            "rate_limit_wait": 30,
        },
    )


# =============================================================================
# Adapter Implementation
# =============================================================================

@register_scraper("dc_ward_2022")
class Ward2022Adapter(DCArcGISBaseAdapter):
    """
    Adapter for DC Ward 2022 boundary data.

    Fetches ward boundaries with representative information and
    Census 2020 demographic data from DC GIS MapServer.
    """

    SOURCE_NAME = "dc_ward_2022"
    SOURCE_VERSION = "1.0.0"
    SUPPORTED_RECORD_TYPES = [DemographicRecord]

    BASE_URL = WARD_2022_CONFIG["base_url"]
    LAYER_ID = WARD_2022_CONFIG["layer_id"]
    FIELD_MAPPINGS = WARD_2022_CONFIG["field_mappings"]

    def _build_query_url(self, offset: int, limit: int) -> str:
        """Build the ArcGIS REST query URL for MapServer."""
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
        """Validate a Ward record."""
        errors = []

        # Check for identifying field
        if not data.get("WARD") and not data.get("OBJECTID"):
            errors.append("Record missing WARD or OBJECTID")

        # Validate ward number
        ward = data.get("WARD")
        if ward is not None:
            try:
                w = int(ward)
                if w < 1 or w > 8:
                    errors.append(f"Invalid ward number: {ward}")
            except (ValueError, TypeError):
                errors.append(f"Non-numeric ward: {ward}")

        return len(errors) == 0, errors

    def normalize(self, data: dict[str, Any]) -> DemographicRecord:
        """Normalize raw API data to DemographicRecord."""

        def safe_int(val: Any) -> int | None:
            if val is None:
                return None
            try:
                return int(float(val))
            except (ValueError, TypeError):
                return None

        ward = safe_int(data.get("WARD"))
        ward_name = f"Ward {ward}" if ward else data.get("NAME", "")

        # Build extra fields for ward-specific info
        extra_fields = {}
        ward_fields = [
            ("ward", ward),
            ("ward_id", data.get("WARD_ID")),
            ("name", data.get("NAME")),
            ("label", data.get("LABEL")),
            ("representative_name", data.get("REP_NAME")),
            ("representative_phone", data.get("REP_PHONE")),
            ("representative_email", data.get("REP_EMAIL")),
            ("representative_office", data.get("REP_OFFICE")),
            ("website", data.get("WEB_URL")),
            ("geoid", data.get("GEOID")),
            ("geocode", data.get("GEOCODE")),
            ("state_fips", data.get("STATE")),
            ("state_abbr", data.get("STUSAB")),
            ("summary_level", data.get("SUMLEV")),
            ("shape_area", data.get("SHAPE.AREA")),
            ("shape_length", data.get("SHAPE.LEN")),
        ]

        for key, value in ward_fields:
            if value is not None:
                extra_fields[key] = value

        # Create lineage with content hash
        raw_content = str(sorted(data.items())).encode()
        lineage = self._make_lineage()
        lineage = lineage.model_copy(
            update={"raw_hash": LineageInfo.compute_hash(raw_content)}
        )

        record = DemographicRecord(
            lineage=lineage,
            extra_fields=extra_fields,
            source_record_id=str(data.get("OBJECTID", ward or "")),
            region_name=ward_name,
            region_type="ward",
            region_id=data.get("WARD_ID") or data.get("GEOID"),
            metric_category="boundary",
            metric_name="ward_boundary",
            metric_value=None,
            metric_unit="polygon",
        )

        return record


# Register config on import
def _register_config():
    """Register Ward 2022 config with the registry."""
    from ...core.registry import ScraperRegistry
    ScraperRegistry.register_config(get_default_config())

_register_config()
