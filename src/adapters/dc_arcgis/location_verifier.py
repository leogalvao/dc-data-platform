"""
DC Location Verifier adapter for the Citizen Atlas SOAP API.

Endpoint: https://citizenatlas.dc.gov/newwebservices/locationverifier.asmx/findLocation2
Data: Address lookup returning SSL (Square/Suffix/Lot), Latitude, and Longitude

This is the "key bootstrap" for DC property data - it resolves addresses to:
- SSL: The unique property identifier used to join with ITSPE and CAMA data
- LATITUDE/LONGITUDE: Coordinates used for PropertyQuest spatial queries

Note: This adapter uses a SOAP/XML API, not an ArcGIS REST API.
"""

from __future__ import annotations

import asyncio
import logging
import xml.etree.ElementTree as ET
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
# Custom Record Type for Location Verifier
# =============================================================================


class LocationVerifierRecord(BaseRecord):
    """
    Normalized location verification result.

    This record serves as the key for joining to other DC property datasets.
    """

    # Input
    input_address: str = Field(..., description="Original address string searched")

    # Output - Property Identification
    ssl: str | None = Field(default=None, description="Square, Suffix, Lot (Primary Key)")
    address_id: str | None = Field(default=None, description="MAR Address ID")
    full_address: str | None = Field(default=None, description="Standardized full address")

    # Output - Coordinates (for PropertyQuest spatial queries)
    latitude: float | None = Field(default=None, ge=-90, le=90)
    longitude: float | None = Field(default=None, ge=-180, le=180)

    # Output - Location Details
    quadrant: str | None = Field(default=None, description="NW, NE, SE, SW")
    ward: str | None = Field(default=None, description="Ward number")
    anc: str | None = Field(default=None, description="Advisory Neighborhood Commission")
    zip_code: str | None = Field(default=None)

    # Status
    status: str = Field(default="unknown", description="FOUND, NOT_FOUND, etc.")
    confidence_level: str | None = Field(default=None)


# =============================================================================
# Configuration
# =============================================================================

LOCATION_VERIFIER_CONFIG = {
    "name": "dc_location_verifier",
    "display_name": "DC Location Verifier (Citizen Atlas)",
    "description": "DC address lookup API returning SSL, coordinates, and location details",
    "base_url": "https://citizenatlas.dc.gov/newwebservices/locationverifier.asmx",
    "find_location_url": "https://citizenatlas.dc.gov/newwebservices/locationverifier.asmx/findLocation2",
}


def get_default_config() -> SourceConfig:
    """Get default configuration for Location Verifier source."""
    return SourceConfig(
        name="dc_location_verifier",
        display_name=LOCATION_VERIFIER_CONFIG["display_name"],
        description=LOCATION_VERIFIER_CONFIG["description"],
        source_type=SourceType.REST_API,
        base_url=LOCATION_VERIFIER_CONFIG["base_url"],
        requires_auth=False,
        requests_per_second=2.0,
        max_workers=5,
        batch_size=100,
        timeout_seconds=30,
        max_retries=3,
        adapter_class="src.adapters.dc_arcgis.location_verifier.LocationVerifierAdapter",
        output_record_types=["LocationVerifierRecord"],
        extra={
            "find_location_url": LOCATION_VERIFIER_CONFIG["find_location_url"],
        },
    )


# =============================================================================
# Adapter Implementation
# =============================================================================

@register_scraper("dc_location_verifier")
class LocationVerifierAdapter(BaseScraper):
    """
    Adapter for DC Location Verifier API.

    Resolves addresses to SSL (Square/Suffix/Lot), coordinates, and
    location details. This is the key bootstrap for DC property data,
    enabling joins to ITSPE, CAMA, and PropertyQuest datasets.

    Usage:
        # Provide addresses via config.extra.addresses or discover() override
        adapter = LocationVerifierAdapter(config)
        result = await adapter.run_async()
    """

    SOURCE_NAME = "dc_location_verifier"
    SOURCE_VERSION = "1.0.0"
    SUPPORTED_RECORD_TYPES = [LocationVerifierRecord]

    BASE_URL = LOCATION_VERIFIER_CONFIG["base_url"]
    FIND_LOCATION_URL = LOCATION_VERIFIER_CONFIG["find_location_url"]

    def __init__(self, config: SourceConfig, **kwargs):
        super().__init__(config, **kwargs)
        self._session: aiohttp.ClientSession | None = None
        self._semaphore: asyncio.Semaphore | None = None
        # Addresses to look up (can be set programmatically or via config)
        self._addresses: list[str] = config.extra.get("addresses", [])

    def set_addresses(self, addresses: list[str]) -> None:
        """Set the list of addresses to verify."""
        self._addresses = addresses

    # =========================================================================
    # Sync Interface (wraps async)
    # =========================================================================

    def discover(self) -> Iterator[dict[str, Any]]:
        """Discover addresses to look up."""
        for address in self._addresses:
            yield {"address": address}

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
        """Parse XML response from Location Verifier."""
        try:
            content = payload.content.decode("utf-8")

            # Parse XML response
            root = ET.fromstring(content)

            # Extract namespace if present
            ns = {}
            if root.tag.startswith("{"):
                ns_uri = root.tag.split("}")[0][1:]
                ns = {"ns": ns_uri}

            # The response structure varies; handle both formats
            result = self._extract_location_data(root, ns)
            if result:
                # Add the original address from payload metadata
                result["_input_address"] = payload.lineage.source_url.split("str=")[-1] if "str=" in payload.lineage.source_url else ""
                yield result

        except ET.ParseError as e:
            self._logger.error(f"XML parse error: {e}")
        except Exception as e:
            self._logger.error(f"Parse error: {e}")

    def _extract_location_data(
        self, root: ET.Element, ns: dict[str, str]
    ) -> dict[str, Any] | None:
        """Extract location data from XML response."""
        result = {}

        # Helper to get text from element
        def get_text(elem: ET.Element | None) -> str | None:
            if elem is not None and elem.text:
                return elem.text.strip()
            return None

        # Find the main data element (varies by response format)
        # Try different possible paths
        data_elem = root

        # Common fields in Location Verifier response
        field_mappings = {
            "SSL": "ssl",
            "ADDRESS_ID": "address_id",
            "FULLADDRESS": "full_address",
            "LATITUDE": "latitude",
            "LONGITUDE": "longitude",
            "XCOORD": "x_coord",
            "YCOORD": "y_coord",
            "QUADRANT": "quadrant",
            "WARD": "ward",
            "ANC": "anc",
            "ZIPCODE": "zip_code",
            "STATUS": "status",
            "CONFIDENCELEVEL": "confidence_level",
        }

        # Extract all known fields
        for child in data_elem.iter():
            tag_name = child.tag.split("}")[-1] if "}" in child.tag else child.tag
            if tag_name.upper() in field_mappings:
                value = get_text(child)
                if value:
                    result[field_mappings[tag_name.upper()]] = value

        # Also check for nested returnDataset structure
        for elem in data_elem.iter():
            tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
            if tag == "returnDataset":
                for table in elem:
                    for row in table:
                        for field in row:
                            field_name = field.tag.split("}")[-1] if "}" in field.tag else field.tag
                            if field_name.upper() in field_mappings:
                                value = get_text(field)
                                if value:
                                    result[field_mappings[field_name.upper()]] = value

        return result if result else None

    def validate(self, data: dict[str, Any]) -> tuple[bool, list[str]]:
        """Validate a location verifier result."""
        errors = []

        # Must have at least SSL or coordinates
        has_ssl = bool(data.get("ssl"))
        has_coords = data.get("latitude") and data.get("longitude")

        if not has_ssl and not has_coords:
            errors.append("No SSL or coordinates returned")

        # Validate coordinates if present
        if data.get("latitude"):
            try:
                lat = float(data["latitude"])
                if lat < -90 or lat > 90:
                    errors.append(f"Invalid latitude: {lat}")
            except (ValueError, TypeError):
                errors.append(f"Non-numeric latitude: {data['latitude']}")

        if data.get("longitude"):
            try:
                lon = float(data["longitude"])
                if lon < -180 or lon > 180:
                    errors.append(f"Invalid longitude: {lon}")
            except (ValueError, TypeError):
                errors.append(f"Non-numeric longitude: {data['longitude']}")

        return len(errors) == 0, errors

    def normalize(self, data: dict[str, Any]) -> LocationVerifierRecord:
        """Normalize to LocationVerifierRecord."""

        def safe_float(val: Any) -> float | None:
            if val is None:
                return None
            try:
                return float(val)
            except (ValueError, TypeError):
                return None

        input_address = data.get("_input_address", "")

        # Create lineage
        raw_content = str(sorted(data.items())).encode()
        lineage = self._make_lineage(f"{self.FIND_LOCATION_URL}?str={quote_plus(input_address)}")
        lineage = lineage.model_copy(
            update={"raw_hash": LineageInfo.compute_hash(raw_content)}
        )

        return LocationVerifierRecord(
            lineage=lineage,
            extra_fields={k: v for k, v in data.items() if not k.startswith("_")},
            input_address=input_address,
            ssl=data.get("ssl"),
            address_id=data.get("address_id"),
            full_address=data.get("full_address"),
            latitude=safe_float(data.get("latitude")),
            longitude=safe_float(data.get("longitude")),
            quadrant=data.get("quadrant"),
            ward=data.get("ward"),
            anc=data.get("anc"),
            zip_code=data.get("zip_code"),
            status=data.get("status", "unknown"),
            confidence_level=data.get("confidence_level"),
        )

    # =========================================================================
    # Async Interface
    # =========================================================================

    async def discover_async(self) -> AsyncIterator[dict[str, Any]]:
        """Discover addresses to look up."""
        for address in self._addresses:
            yield {"address": address}

    async def fetch_async(self, item: dict[str, Any]) -> RawPayload | None:
        """Fetch location data for an address."""
        # Lazy init session and semaphore
        if self._session is None:
            timeout = aiohttp.ClientTimeout(total=self.config.timeout_seconds)
            self._session = aiohttp.ClientSession(timeout=timeout)
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(self.config.max_workers)

        address = item["address"]
        url = f"{self.FIND_LOCATION_URL}?str={quote_plus(address)}"

        async with self._semaphore:
            for attempt in range(self.config.max_retries):
                try:
                    start_time = datetime.now(timezone.utc)
                    async with self._session.get(url) as response:
                        if response.status == 200:
                            content = await response.read()
                            duration_ms = int(
                                (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
                            )
                            return RawPayload(
                                content_type="text/xml",
                                content=content,
                                lineage=self._make_lineage(url),
                                fetch_status_code=response.status,
                                fetch_duration_ms=duration_ms,
                            )
                        else:
                            self._logger.warning(
                                f"HTTP {response.status} for address: {address}"
                            )

                except asyncio.TimeoutError:
                    self._logger.warning(f"Timeout for address: {address}, attempt {attempt + 1}")
                except Exception as e:
                    self._logger.error(f"Fetch error for address {address}: {e}")

                # Exponential backoff
                if attempt < self.config.max_retries - 1:
                    wait = 2 ** attempt
                    await asyncio.sleep(wait)

        self._logger.error(f"Failed to fetch address {address} after {self.config.max_retries} attempts")
        return None

    async def run_async(self, *, max_records: int | None = None) -> ScraperResult:
        """Run the location verification pipeline."""
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
            raw_hash="",  # Will be set during normalize
            schema_version="1.0.0",
            adapter_version=self.SOURCE_VERSION,
        )


# Register config on import
def _register_config():
    """Register Location Verifier config with the registry."""
    from ...core.registry import ScraperRegistry
    ScraperRegistry.register_config(get_default_config())

_register_config()
