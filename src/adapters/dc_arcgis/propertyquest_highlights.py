"""
PropertyQuest Highlights adapter for DC GIS spatial queries.

Endpoint: https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_APPS/PropertyQuest/MapServer
Data: Spatial intersection queries returning zoning, ward, ANC, historic status, flood plain

This adapter performs spatial intersection queries using lat/lon coordinates to determine:
- Historic District (Layer 16)
- Historic Landmark (Layer 18)
- ANC / SMD (Layer 19)
- Flood Plain (Layer 23)
- Zoning (Layer 25)
- Ward (Layer 35)

Query Logic: If count > 0, property is within the district/zone.
Join Key: LATITUDE, LONGITUDE (from Location Verifier)
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator, Iterator
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote_plus

import aiohttp
from pydantic import Field

from ...core.base_scraper import BaseScraper, ScraperResult
from ...core.registry import register_scraper
from ...schemas.base import BaseRecord, LineageInfo, RawPayload
from ...schemas.metadata import SourceConfig, SourceType

logger = logging.getLogger(__name__)


# =============================================================================
# Custom Record Type for PropertyQuest Highlights
# =============================================================================

class PropertyQuestHighlightsRecord(BaseRecord):
    """
    Normalized spatial query results for a property location.

    Contains results from all PropertyQuest spatial layers.
    """

    # Input coordinates (join key)
    latitude: float = Field(..., ge=-90, le=90)
    longitude: float = Field(..., ge=-180, le=180)

    # Historic Status
    historic_district: str | None = Field(
        default=None,
        description="Name of historic district if property is within one"
    )
    historic_landmark: str | None = Field(
        default=None,
        description="Name of historic landmark if property is one"
    )
    in_historic_district: bool = Field(
        default=False,
        description="Whether property is in any historic district"
    )
    is_historic_landmark: bool = Field(
        default=False,
        description="Whether property is a historic landmark"
    )

    # Zoning
    zoning: str | None = Field(
        default=None,
        description="Zoning designation (e.g., R-1-B, C-2-A)"
    )
    zoning_label: str | None = Field(
        default=None,
        description="Full zoning label"
    )

    # Political/Administrative
    ward: str | None = Field(
        default=None,
        description="Ward number (1-8)"
    )
    anc: str | None = Field(
        default=None,
        description="Advisory Neighborhood Commission / SMD ID"
    )

    # Environmental
    flood_plain: str | None = Field(
        default=None,
        description="Flood plain zone subtype if in flood plain"
    )
    in_flood_plain: bool = Field(
        default=False,
        description="Whether property is in a flood plain"
    )


# =============================================================================
# Configuration
# =============================================================================

PROPERTYQUEST_CONFIG = {
    "name": "dc_propertyquest_highlights",
    "display_name": "DC PropertyQuest Spatial Highlights",
    "description": "DC GIS spatial queries for zoning, historic, ward, ANC, and flood plain",
    "base_url": "https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_APPS/PropertyQuest/MapServer",
    "layers": {
        "historic_district": {
            "id": 16,
            "return_field": "NAME",
            "description": "Historic Districts",
        },
        "historic_landmark": {
            "id": 18,
            "return_field": "NAME",
            "description": "Historic Landmarks",
        },
        "anc": {
            "id": 19,
            "return_field": "SMD_ID",
            "description": "Advisory Neighborhood Commissions",
        },
        "flood_plain": {
            "id": 23,
            "return_field": "ZONE_SUBTY",
            "description": "Flood Plains (FEMA)",
        },
        "zoning": {
            "id": 25,
            "return_field": "ZONING_LABEL",
            "description": "Zoning Districts",
        },
        "ward": {
            "id": 35,
            "return_field": "WARD",
            "description": "DC Wards",
        },
    },
}


def get_default_config() -> SourceConfig:
    """Get default configuration for PropertyQuest Highlights source."""
    return SourceConfig(
        name="dc_propertyquest_highlights",
        display_name=PROPERTYQUEST_CONFIG["display_name"],
        description=PROPERTYQUEST_CONFIG["description"],
        source_type=SourceType.REST_API,
        base_url=PROPERTYQUEST_CONFIG["base_url"],
        requires_auth=False,
        requests_per_second=2.0,
        max_workers=5,
        batch_size=50,  # Lower batch size since we make multiple queries per location
        timeout_seconds=30,
        max_retries=3,
        adapter_class="src.adapters.dc_arcgis.propertyquest_highlights.PropertyQuestHighlightsAdapter",
        output_record_types=["PropertyQuestHighlightsRecord"],
        extra={
            "layers": PROPERTYQUEST_CONFIG["layers"],
        },
    )


# =============================================================================
# Adapter Implementation
# =============================================================================

@register_scraper("dc_propertyquest_highlights")
class PropertyQuestHighlightsAdapter(BaseScraper):
    """
    Adapter for DC PropertyQuest spatial highlight queries.

    Performs spatial intersection queries on multiple GIS layers
    to determine property characteristics like zoning, historic status,
    ward, ANC, and flood plain location.

    Usage:
        # Provide coordinates via config.extra.locations or set_locations()
        adapter = PropertyQuestHighlightsAdapter(config)
        adapter.set_locations([(38.9072, -77.0369), (38.9, -77.0)])
        result = await adapter.run_async()
    """

    SOURCE_NAME = "dc_propertyquest_highlights"
    SOURCE_VERSION = "1.0.0"
    SUPPORTED_RECORD_TYPES = [PropertyQuestHighlightsRecord]

    BASE_URL = PROPERTYQUEST_CONFIG["base_url"]
    LAYERS = PROPERTYQUEST_CONFIG["layers"]

    def __init__(self, config: SourceConfig, **kwargs):
        super().__init__(config, **kwargs)
        self._session: aiohttp.ClientSession | None = None
        self._semaphore: asyncio.Semaphore | None = None
        # Locations to query (list of (lat, lon) tuples)
        self._locations: list[tuple[float, float]] = config.extra.get("locations", [])

    def set_locations(self, locations: list[tuple[float, float]]) -> None:
        """Set the list of (latitude, longitude) tuples to query."""
        self._locations = locations

    # =========================================================================
    # Sync Interface
    # =========================================================================

    def discover(self) -> Iterator[dict[str, Any]]:
        """Discover locations to query."""
        for lat, lon in self._locations:
            yield {"latitude": lat, "longitude": lon}

    def fetch(self, item: dict[str, Any]) -> RawPayload | None:
        """Sync fetch - runs async version."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            async def _fetch_and_cleanup():
                result = await self.fetch_async(item)
                if self._session:
                    await self._session.close()
                    self._session = None
                return result
            return loop.run_until_complete(_fetch_and_cleanup())
        finally:
            loop.close()
            asyncio.set_event_loop(None)

    def parse(self, payload: RawPayload) -> Iterator[dict[str, Any]]:
        """Parse combined layer query results."""
        try:
            content = payload.content.decode("utf-8")
            data = json.loads(content)
            yield data
        except Exception as e:
            self._logger.error(f"Parse error: {e}")

    def validate(self, data: dict[str, Any]) -> tuple[bool, list[str]]:
        """Validate spatial query results."""
        errors = []

        # Must have coordinates
        if not data.get("latitude") or not data.get("longitude"):
            errors.append("Missing latitude or longitude")

        # Validate coordinate ranges
        lat = data.get("latitude")
        lon = data.get("longitude")

        if lat is not None:
            try:
                if float(lat) < -90 or float(lat) > 90:
                    errors.append(f"Invalid latitude: {lat}")
            except (ValueError, TypeError):
                errors.append(f"Non-numeric latitude: {lat}")

        if lon is not None:
            try:
                if float(lon) < -180 or float(lon) > 180:
                    errors.append(f"Invalid longitude: {lon}")
            except (ValueError, TypeError):
                errors.append(f"Non-numeric longitude: {lon}")

        return len(errors) == 0, errors

    def normalize(self, data: dict[str, Any]) -> PropertyQuestHighlightsRecord:
        """Normalize to PropertyQuestHighlightsRecord."""

        # Create lineage
        raw_content = str(sorted(data.items())).encode()
        lineage = self._make_lineage()
        lineage = lineage.model_copy(
            update={"raw_hash": LineageInfo.compute_hash(raw_content)}
        )

        # Extract layer results
        historic_district = data.get("historic_district")
        historic_landmark = data.get("historic_landmark")
        zoning = data.get("zoning")
        ward = data.get("ward")
        anc = data.get("anc")
        flood_plain = data.get("flood_plain")

        return PropertyQuestHighlightsRecord(
            lineage=lineage,
            extra_fields={},
            latitude=float(data["latitude"]),
            longitude=float(data["longitude"]),
            historic_district=historic_district,
            historic_landmark=historic_landmark,
            in_historic_district=bool(historic_district),
            is_historic_landmark=bool(historic_landmark),
            zoning=zoning,
            zoning_label=zoning,
            ward=ward,
            anc=anc,
            flood_plain=flood_plain,
            in_flood_plain=bool(flood_plain),
        )

    # =========================================================================
    # Async Interface
    # =========================================================================

    async def discover_async(self) -> AsyncIterator[dict[str, Any]]:
        """Discover locations to query."""
        for lat, lon in self._locations:
            yield {"latitude": lat, "longitude": lon}

    async def _query_layer(
        self,
        layer_name: str,
        layer_config: dict[str, Any],
        lat: float,
        lon: float,
    ) -> tuple[str, str | None]:
        """
        Query a single spatial layer for intersection.

        Returns: (layer_name, value or None)
        """
        if self._session is None:
            return layer_name, None

        layer_id = layer_config["id"]
        return_field = layer_config["return_field"]

        # Build spatial query URL
        # Using geometry point and spatial relationship intersects
        geometry = f'{{"x":{lon},"y":{lat},"spatialReference":{{"wkid":4326}}}}'
        url = (
            f"{self.BASE_URL}/{layer_id}/query"
            f"?geometry={quote_plus(geometry)}"
            f"&geometryType=esriGeometryPoint"
            f"&spatialRel=esriSpatialRelIntersects"
            f"&outFields={return_field}"
            f"&returnGeometry=false"
            f"&f=json"
        )

        try:
            async with self._session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    features = data.get("features", [])
                    if features:
                        # Return the first matching feature's return field value
                        attrs = features[0].get("attributes", {})
                        value = attrs.get(return_field)
                        return layer_name, value
        except Exception as e:
            self._logger.warning(f"Error querying {layer_name} layer: {e}")

        return layer_name, None

    async def fetch_async(self, item: dict[str, Any]) -> RawPayload | None:
        """
        Fetch spatial highlights for a location.

        Queries all configured layers in parallel.
        """
        # Lazy init session and semaphore
        if self._session is None:
            timeout = aiohttp.ClientTimeout(total=self.config.timeout_seconds)
            self._session = aiohttp.ClientSession(timeout=timeout)
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(self.config.max_workers)

        lat = item["latitude"]
        lon = item["longitude"]

        async with self._semaphore:
            start_time = datetime.now(timezone.utc)

            # Query all layers in parallel
            tasks = [
                self._query_layer(layer_name, layer_config, lat, lon)
                for layer_name, layer_config in self.LAYERS.items()
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            duration_ms = int(
                (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
            )

            # Combine results
            combined = {
                "latitude": lat,
                "longitude": lon,
            }

            for result in results:
                if isinstance(result, tuple):
                    layer_name, value = result
                    combined[layer_name] = value
                elif isinstance(result, Exception):
                    self._logger.warning(f"Layer query failed: {result}")

            # Create raw payload with combined results
            content = json.dumps(combined).encode("utf-8")
            url = f"{self.BASE_URL}?lat={lat}&lon={lon}"

            return RawPayload(
                content_type="application/json",
                content=content,
                lineage=self._make_lineage(url),
                fetch_status_code=200,
                fetch_duration_ms=duration_ms,
            )

    async def run_async(self, *, max_records: int | None = None) -> ScraperResult:
        """Run the PropertyQuest spatial query pipeline."""
        result = ScraperResult()
        self.metadata.start()

        try:
            self.setup()

            async for item in self.discover_async():
                result.discovered_count += 1

                payload = await self.fetch_async(item)
                if payload is None:
                    continue
                result.add_raw(payload)

                for data in self.parse(payload):
                    result.parsed_count += 1

                    is_valid, errors = self.validate(data)
                    if not is_valid:
                        result.quarantine(
                            data=data,
                            error_type="validation_error",
                            error_message="; ".join(errors),
                            lineage=self._make_lineage(payload.lineage.source_url),
                        )
                        continue

                    try:
                        record = self.normalize(data)
                    except Exception as e:
                        result.quarantine(
                            data=data,
                            error_type="normalization_error",
                            error_message=str(e),
                            lineage=self._make_lineage(payload.lineage.source_url),
                        )
                        continue

                    result.add_record(record)

                    if max_records and result.validated_count >= max_records:
                        break

                if max_records and result.validated_count >= max_records:
                    break

            from ...schemas.metadata import RunStatus
            self.metadata.complete(RunStatus.COMPLETED)

        except Exception as e:
            self._logger.exception(f"Scraper failed: {e}")
            from ...schemas.metadata import RunStatus
            self.metadata.complete(RunStatus.FAILED)
            raise

        finally:
            if self._session:
                await self._session.close()
                self._session = None
            self.teardown()

        return result

    def _make_lineage(self, source_url: str | None = None) -> LineageInfo:
        """Create lineage info for a record."""
        return LineageInfo(
            source_name=self.SOURCE_NAME,
            source_url=source_url or self.BASE_URL,
            scraped_at=datetime.now(timezone.utc),
            run_id=self.run_id,
            raw_hash="",
            schema_version="1.0.0",
            adapter_version=self.SOURCE_VERSION,
        )


# Register config on import
def _register_config():
    """Register PropertyQuest Highlights config with the registry."""
    from ...core.registry import ScraperRegistry
    ScraperRegistry.register_config(get_default_config())

_register_config()
