"""
Affordable Housing adapter for DC ArcGIS endpoints.

Migrated from legacy/affordable_housing.py with corrected endpoints:
- Affordable Housing Projects (Property_and_Land_WebMercator/MapServer/62)
- Public Housing Areas (Public_Service_WebMercator/MapServer/15)

Note: The original endpoints (MapServer/66, 57, 68, 65) have been
repurposed by DC GIS for different data. These are the current
correct endpoints for affordable housing data.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ...core.registry import register_scraper
from ...schemas.base import LineageInfo
from ...schemas.metadata import SourceConfig, SourceType
from ...schemas.tabular import PropertyRecord
from .base import DCArcGISBaseAdapter

# =============================================================================
# Dataset Configurations
# =============================================================================

@dataclass
class HousingDatasetConfig:
    """Configuration for a housing dataset."""
    name: str
    display_name: str
    description: str
    base_url: str
    mapserver_id: int
    field_mappings: dict[str, str]


# Affordable Housing Projects
AFFORDABLE_HOUSING_CONFIG = HousingDatasetConfig(
    name="affordable_housing",
    display_name="Affordable Housing Projects",
    description="DC affordable housing developments and units",
    base_url="https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Property_and_Land_WebMercator/MapServer",
    mapserver_id=62,
    field_mappings={
        "PROJECT_NAME": "project_name",
        "ADDRESS": "address",
        "FULLADDRESS": "full_address",
        "MAR_WARD": "ward",
        "STATUS_PUBLIC": "status",
        "AGENCY_CALCULATED": "agency",
        "TOTAL_AFFORDABLE_UNITS": "total_units",
        "AFFORDABLE_UNITS_AT_0_30_AMI": "units_0_30_ami",
        "AFFORDABLE_UNITS_AT_31_50_AMI": "units_31_50_ami",
        "AFFORDABLE_UNITS_AT_51_60_AMI": "units_51_60_ami",
        "AFFORDABLE_UNITS_AT_61_80_AMI": "units_61_80_ami",
        "AFFORDABLE_UNITS_AT_81_AMI": "units_81_ami",
        "LATITUDE": "latitude",
        "LONGITUDE": "longitude",
        "MAR_ID": "mar_id",
        "OBJECTID": "source_record_id",
    },
)

# Public Housing Areas
PUBLIC_HOUSING_CONFIG = HousingDatasetConfig(
    name="public_housing",
    display_name="Public Housing Areas",
    description="DC Housing Authority properties",
    base_url="https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Public_Service_WebMercator/MapServer",
    mapserver_id=15,
    field_mappings={
        "SITENAME": "project_name",
        "ADDRESS": "address",
        "WARD": "ward",
        "OWNERSHIP_TYPE": "ownership_type",
        "BUILDING_USE": "building_use",
        "USE_DESCRIPTION": "use_description",
        "AGENCY": "agency",
        "OWNERNAME": "owner_name",
        "UNITS": "total_units",
        "PROPTYPE": "property_type",
        "ZONING": "zoning",
        "ANC": "anc",
        "MAR_ID": "mar_id",
        "OBJECTID": "source_record_id",
    },
)

# All available housing datasets
HOUSING_DATASETS = {
    "affordable_housing": AFFORDABLE_HOUSING_CONFIG,
    "public_housing": PUBLIC_HOUSING_CONFIG,
}


# =============================================================================
# Adapter Implementation
# =============================================================================

def get_default_config(dataset_name: str = "affordable_housing") -> SourceConfig:
    """Get default configuration for an affordable housing source."""
    dataset = HOUSING_DATASETS.get(dataset_name, AFFORDABLE_HOUSING_CONFIG)

    # Map dataset names to registered scraper names
    scraper_names = {
        "affordable_housing": "dc_affordable_housing",
        "public_housing": "dc_public_housing",
    }
    scraper_name = scraper_names.get(dataset_name, "dc_affordable_housing")

    return SourceConfig(
        name=scraper_name,
        display_name=dataset.display_name,
        description=dataset.description,
        source_type=SourceType.REST_API,
        base_url=dataset.base_url,
        requires_auth=False,
        requests_per_second=2.0,
        max_workers=5,
        batch_size=1000,
        timeout_seconds=60,
        max_retries=3,
        adapter_class="src.adapters.dc_arcgis.affordable_housing.AffordableHousingAdapter",
        output_record_types=["PropertyRecord"],
        extra={
            "dataset_name": dataset_name,
            "mapserver_id": dataset.mapserver_id,
            "rate_limit_wait": 30,
        },
    )


class AffordableHousingBaseAdapter(DCArcGISBaseAdapter):
    """
    Base adapter for DC Affordable Housing datasets.

    Subclasses specify the dataset configuration.
    """

    SOURCE_NAME = "dc_affordable_housing"
    SOURCE_VERSION = "1.0.0"
    SUPPORTED_RECORD_TYPES = [PropertyRecord]

    # Override in subclasses
    DATASET_CONFIG: HousingDatasetConfig = AFFORDABLE_HOUSING_CONFIG

    @property
    def _mapserver_id(self) -> int:
        """Get the MapServer ID for this dataset."""
        return self.config.extra.get("mapserver_id", self.DATASET_CONFIG.mapserver_id)

    def _build_query_url(self, offset: int, limit: int) -> str:
        """Build the ArcGIS REST query URL."""
        base = f"{self.DATASET_CONFIG.base_url}/{self._mapserver_id}/query"
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
            f"{self.DATASET_CONFIG.base_url}/{self._mapserver_id}/query"
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
        """
        Validate a housing record.

        Basic validation - checks for address or project name.
        """
        errors = []

        # Check for identifying field
        has_id = (
            data.get("ADDRESS") or
            data.get("PROJECT_NAME") or
            data.get("SITENAME") or
            data.get("OBJECTID")
        )
        if not has_id:
            errors.append("Record missing address or project name")

        # Validate units if present
        units = data.get("TOTAL_AFFORDABLE_UNITS") or data.get("UNITS")
        if units is not None:
            try:
                u = float(units)
                if u < 0:
                    errors.append(f"Invalid unit count: {units}")
            except (ValueError, TypeError):
                errors.append(f"Non-numeric units: {units}")

        return len(errors) == 0, errors

    def normalize(self, data: dict[str, Any]) -> PropertyRecord:
        """
        Normalize raw API data to PropertyRecord.
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

        # Use full address if available, otherwise regular address
        address = normalized.get("full_address") or normalized.get("address")

        # Calculate total units from AMI breakdown if not directly available
        total_units = normalized.get("total_units")
        if total_units is not None:
            try:
                total_units = int(float(total_units))
            except (ValueError, TypeError):
                total_units = None

        # Capture extra fields not in PropertyRecord
        extra_fields = {}
        property_record_fields = {
            "address", "full_address", "latitude", "longitude", "source_record_id"
        }
        for canonical_field, value in normalized.items():
            if canonical_field not in property_record_fields and value is not None:
                extra_fields[canonical_field] = value

        # Also capture unmapped API fields
        for key, value in data.items():
            if key not in mappings and not key.startswith("_") and value is not None:
                extra_fields[key] = value

        # Create lineage with content hash
        raw_content = str(sorted(data.items())).encode()
        lineage = self._make_lineage()
        lineage = lineage.model_copy(
            update={"raw_hash": LineageInfo.compute_hash(raw_content)}
        )

        # Determine property type based on dataset
        from ...schemas.tabular import ListingType, PropertyType
        property_type = PropertyType.MULTI_FAMILY
        listing_type = ListingType.FOR_RENT

        # Create the record
        record = PropertyRecord(
            lineage=lineage,
            extra_fields=extra_fields if extra_fields else {},
            source_record_id=str(normalized.pop("source_record_id", "")),
            listing_type=listing_type,
            property_type=property_type,
            listing_id=None,
            price=None,
            price_per_sqft=None,
            original_price=None,
            price_reduced=False,
            bedrooms=None,
            bathrooms=None,
            sqft=None,
            lot_sqft=None,
            year_built=None,
            stories=None,
            garage_spaces=None,
            address=address,
            city="Washington",
            state="DC",
            zip_code=None,
            latitude=normalized.get("latitude"),
            longitude=normalized.get("longitude"),
        )

        return record


# =============================================================================
# Registered Adapters
# =============================================================================

@register_scraper("dc_affordable_housing")
class AffordableHousingAdapter(AffordableHousingBaseAdapter):
    """
    Adapter for DC Affordable Housing Projects.

    Endpoint: Property_and_Land_WebMercator/MapServer/62
    """
    SOURCE_NAME = "dc_affordable_housing"
    DATASET_CONFIG = AFFORDABLE_HOUSING_CONFIG
    MAPSERVER_ID = 62
    BASE_URL = "https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Property_and_Land_WebMercator/MapServer"


@register_scraper("dc_public_housing")
class PublicHousingAdapter(AffordableHousingBaseAdapter):
    """
    Adapter for DC Public Housing Areas.

    Endpoint: Public_Service_WebMercator/MapServer/15
    """
    SOURCE_NAME = "dc_public_housing"
    DATASET_CONFIG = PUBLIC_HOUSING_CONFIG
    MAPSERVER_ID = 15
    BASE_URL = "https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Public_Service_WebMercator/MapServer"


# Helper functions
def get_affordable_housing_config() -> SourceConfig:
    """Get configuration for Affordable Housing Projects."""
    return get_default_config("affordable_housing")


def get_public_housing_config() -> SourceConfig:
    """Get configuration for Public Housing Areas."""
    return get_default_config("public_housing")
