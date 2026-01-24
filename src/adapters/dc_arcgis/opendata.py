"""
DC Open Data adapter for multiple ArcGIS endpoints.

Migrated from legacy/dc_opendata.py which handles:
- CBE Certified Businesses
- Solicitations (Open Bids)
- Business Licenses
- CAMA Properties (residential/commercial)

This adapter uses a configurable dataset approach where
each dataset has its own URL, field mappings, and schema.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from ...core.registry import register_scraper
from ...schemas.base import LineageInfo
from ...schemas.metadata import SourceConfig, SourceType
from ...schemas.tabular import BusinessRecord
from .base import DCArcGISBaseAdapter

# =============================================================================
# Dataset Configurations
# =============================================================================

@dataclass
class DatasetConfig:
    """Configuration for a specific dataset."""
    name: str
    display_name: str
    description: str
    base_url: str
    record_type: str  # Schema class name
    field_mappings: dict[str, str]


# CBE Certified Businesses
CBE_BUSINESSES_CONFIG = DatasetConfig(
    name="cbe_businesses",
    display_name="CBE Certified Businesses",
    description="DC Certified Business Enterprise directory",
    base_url="https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Business_Licensing_and_Grants_WebMercator/MapServer/33/query",
    record_type="BusinessRecord",
    field_mappings={
        "BUSINESSNAME": "business_name",
        "BUSINESSDESC": "industry",
        "ORGANIZATIONTYPE": "business_type",
        "CERTIFICATIONNUMBER": "license_number",
        "CATEGORIES": "certification_type",
        "PRINCIPALOWNER": "owner_name",
        "CONTACTNAME": "contact_name",
        "ADDRESS": "address",
        "PHONE": "phone",
        "EMAIL": "email",
        "WEBSITE": "website",
        "WARD": "ward",
        "STARTDATE": "start_date",
        "EXPIRATIONDATE": "expiration_date",
        "DATEESTABLISHED": "date_established",
        "LATITUDE": "latitude",
        "LONGITUDE": "longitude",
        "OBJECTID": "source_record_id",
    },
)

# Solicitations (Open Bids)
SOLICITATIONS_CONFIG = DatasetConfig(
    name="solicitations",
    display_name="Solicitations (Open Bids)",
    description="DC open procurement opportunities",
    base_url="https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Government_Operations/MapServer/19/query",
    record_type="BusinessRecord",  # Reusing BusinessRecord for simplicity
    field_mappings={
        "SOLICITATIONTITLE": "business_name",  # Using as title
        "SOLICITATIONNUMBER": "license_number",
        "AGENCYCODE": "business_type",
        "NIGPCODEDESCRIPTION": "industry",
        "AGENCY_NAME": "owner_name",
        "TYPE": "license_type",
        "SOLICITATIONSTATUS": "license_status",
        "EMAIL": "email",
        "PHONE": "phone",
        "FISCALYEAR": "fiscal_year",
        "DUE_DATE": "expiration_date",
        "ISSUANCEDATE": "start_date",
        "OBJECT_ID": "source_record_id",
    },
)

# All available datasets
DATASETS = {
    "cbe_businesses": CBE_BUSINESSES_CONFIG,
    "solicitations": SOLICITATIONS_CONFIG,
}


# =============================================================================
# Adapter Implementation
# =============================================================================

def get_default_config(dataset_name: str = "cbe_businesses") -> SourceConfig:
    """Get default configuration for a DC Open Data source."""
    dataset = DATASETS.get(dataset_name, CBE_BUSINESSES_CONFIG)

    # Map dataset names to registered scraper names
    scraper_names = {
        "cbe_businesses": "dc_cbe_businesses",
        "solicitations": "dc_solicitations",
    }
    scraper_name = scraper_names.get(dataset_name, "dc_opendata")

    return SourceConfig(
        name=scraper_name,
        display_name=dataset.display_name,
        description=dataset.description,
        source_type=SourceType.REST_API,
        base_url=dataset.base_url.rsplit("/query", 1)[0],  # Remove /query suffix
        requires_auth=False,
        requests_per_second=2.0,
        max_workers=5,
        batch_size=1000,
        timeout_seconds=60,
        max_retries=3,
        adapter_class="src.adapters.dc_arcgis.opendata.DCOpenDataAdapter",
        output_record_types=[dataset.record_type],
        extra={
            "dataset_name": dataset_name,
            "query_url": dataset.base_url,
            "rate_limit_wait": 30,
        },
    )


class DCOpenDataBaseAdapter(DCArcGISBaseAdapter):
    """
    Base adapter for DC Open Data multi-dataset sources.

    Subclasses specify the dataset configuration.
    """

    SOURCE_NAME = "dc_opendata"
    SOURCE_VERSION = "1.0.0"
    SUPPORTED_RECORD_TYPES = [BusinessRecord]

    # Override in subclasses or set via config
    DATASET_CONFIG: DatasetConfig = CBE_BUSINESSES_CONFIG

    @property
    def _query_url(self) -> str:
        """Get the query URL for this dataset."""
        return self.config.extra.get("query_url", self.DATASET_CONFIG.base_url)

    def _build_query_url(self, offset: int, limit: int) -> str:
        """Build the ArcGIS REST query URL."""
        base = self._query_url
        # Remove trailing /query if present, then add it back
        if base.endswith("/query"):
            base = base[:-6]

        params = (
            f"where=1%3D1"
            f"&outFields=*"
            f"&returnGeometry=true"
            f"&outSR=4326"
            f"&f=json"
            f"&resultOffset={offset}"
            f"&resultRecordCount={limit}"
        )
        return f"{base}/query?{params}"

    async def _fetch_total_records(self) -> int:
        """Query the API for total record count."""
        import aiohttp

        base = self._query_url
        if base.endswith("/query"):
            base = base[:-6]

        url = f"{base}/query?where=1%3D1&returnCountOnly=true&f=json"

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
        """
        Validate a record.

        Basic validation - checks for primary identifier.
        """
        errors = []

        # Check for some identifying field
        id_fields = ["BUSINESSNAME", "SOLICITATIONTITLE", "OBJECTID", "OBJECT_ID"]

        has_id = any(data.get(f) for f in id_fields)
        if not has_id:
            errors.append("Record missing identifying field")

        return len(errors) == 0, errors

    def normalize(self, data: dict[str, Any]) -> BusinessRecord:
        """
        Normalize raw API data to BusinessRecord.
        """
        mappings = self.DATASET_CONFIG.field_mappings

        # Map fields using dataset-specific mappings
        normalized: dict[str, Any] = {}
        for api_field, canonical_field in mappings.items():
            if api_field in data and data[api_field] is not None:
                normalized[canonical_field] = data[api_field]

        # Handle geometry from parsed data
        if "_latitude" in data and "_longitude" in data:
            normalized["latitude"] = data["_latitude"]
            normalized["longitude"] = data["_longitude"]
        elif "latitude" not in normalized:
            # Try LATITUDE/LONGITUDE fields directly
            if data.get("LATITUDE"):
                normalized["latitude"] = data["LATITUDE"]
            if data.get("LONGITUDE"):
                normalized["longitude"] = data["LONGITUDE"]

        # Date conversions (already converted by base class)
        date_fields = ["start_date", "expiration_date", "date_established"]
        for date_field in date_fields:
            if date_field in normalized and normalized[date_field]:
                try:
                    if isinstance(normalized[date_field], str):
                        normalized[date_field] = date.fromisoformat(
                            normalized[date_field]
                        )
                except ValueError:
                    normalized[date_field] = None

        # Capture extra fields not in mappings
        extra_fields = {}
        for key, value in data.items():
            if key not in mappings and not key.startswith("_") and value is not None:
                extra_fields[key] = value

        # Create lineage with content hash
        raw_content = str(sorted(data.items())).encode()
        lineage = self._make_lineage()
        lineage = lineage.model_copy(
            update={"raw_hash": LineageInfo.compute_hash(raw_content)}
        )

        # Extract fields for BusinessRecord
        record = BusinessRecord(
            lineage=lineage,
            extra_fields=extra_fields if extra_fields else {},
            source_record_id=str(normalized.pop("source_record_id", "")),
            business_name=normalized.get("business_name"),
            trade_name=None,
            business_type=normalized.get("business_type"),
            industry=normalized.get("industry"),
            naics_code=None,
            license_number=normalized.get("license_number"),
            license_type=normalized.get("license_type"),
            license_status=normalized.get("license_status"),
            certification_type=normalized.get("certification_type"),
            owner_name=normalized.get("owner_name"),
            address=normalized.get("address"),
            city="Washington",
            state="DC",
            zip_code=None,
            phone=normalized.get("phone"),
            email=normalized.get("email"),
            website=normalized.get("website"),
            latitude=normalized.get("latitude"),
            longitude=normalized.get("longitude"),
        )

        return record


# =============================================================================
# Registered Adapters
# =============================================================================

@register_scraper("dc_opendata")
class DCOpenDataAdapter(DCOpenDataBaseAdapter):
    """
    Default DC Open Data adapter (CBE Businesses).

    This is the main entry point for generic DC Open Data scraping.
    """
    SOURCE_NAME = "dc_opendata"
    DATASET_CONFIG = CBE_BUSINESSES_CONFIG
    MAPSERVER_ID = 33  # CBE businesses
    BASE_URL = "https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Business_Licensing_and_Grants_WebMercator/MapServer"


@register_scraper("dc_cbe_businesses")
class CBEBusinessesAdapter(DCOpenDataBaseAdapter):
    """Adapter for CBE Certified Businesses."""
    SOURCE_NAME = "dc_cbe_businesses"
    DATASET_CONFIG = CBE_BUSINESSES_CONFIG
    MAPSERVER_ID = 33
    BASE_URL = "https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Business_Licensing_and_Grants_WebMercator/MapServer"


@register_scraper("dc_solicitations")
class SolicitationsAdapter(DCOpenDataBaseAdapter):
    """Adapter for DC Solicitations (Open Bids)."""
    SOURCE_NAME = "dc_solicitations"
    DATASET_CONFIG = SOLICITATIONS_CONFIG
    MAPSERVER_ID = 19
    BASE_URL = "https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Government_Operations/MapServer"


# Helper function to get config for specific datasets
def get_cbe_config() -> SourceConfig:
    """Get configuration for CBE Businesses."""
    return get_default_config("cbe_businesses")


def get_solicitations_config() -> SourceConfig:
    """Get configuration for Solicitations."""
    return get_default_config("solicitations")
