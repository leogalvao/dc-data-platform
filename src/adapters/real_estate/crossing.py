"""
Crossing DC adapter wrapping legacy/crossing.py.

Demonstrates wrapping a simpler sync scraper that uses:
- requests library
- Regex + BeautifulSoup parsing
- Multiple fallback methods (SightMap API, Astro data)

The adapter preserves the existing extraction logic while
adding the standard pipeline interface.
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Iterator
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any

import requests

from ...core.base_scraper import BaseScraper
from ...core.registry import register_scraper
from ...schemas.base import LineageInfo, RawPayload
from ...schemas.metadata import SourceConfig, SourceType
from ...schemas.tabular import RentalRecord

logger = logging.getLogger(__name__)

# Pre-compiled regex patterns for better performance
# Avoids O(n) compilation overhead per record
_RE_SIGHTMAP_ID = re.compile(r'sightmap\.com/app/api/v1/sightmaps/(\w+)/units')
_RE_RENTCAFE_ASTRO = re.compile(
    r'"floorplanName":\[0,"([^"]+)"\].*?'
    r'"maximumRent":\[0,(\d+)\].*?'
    r'"minimumRent":\[0,(\d+)\].*?'
    r'"sqft":\[0,(\d+)\].*?'
    r'"unitStatus":\[0,"([^"]+)"',
    re.DOTALL
)
_RE_ASTRO_DATA = re.compile(
    r'<script[^>]*>.*?Astro\.props\s*=\s*(\{.*?\});.*?</script>',
    re.DOTALL
)


# =============================================================================
# Legacy code preserved from crossing.py
# These functions are copied/adapted from the original scraper
# =============================================================================

def _extract_sightmap_id(html: str) -> str | None:
    """
    Extract SightMap ID from HTML using pre-compiled regex.

    Preserved from legacy/crossing.py
    """
    match = _RE_SIGHTMAP_ID.search(html)
    return match.group(1) if match else None


def _parse_sightmap_units(units_data: list[dict]) -> list[dict[str, Any]]:
    """
    Parse SightMap API response into unit records.

    Preserved from legacy/crossing.py
    """
    results = []
    for unit in units_data:
        floor_plan = unit.get("floor_plan", {})
        results.append({
            "unit_number": unit.get("unit_number"),
            "price": unit.get("price"),
            "status": unit.get("available") and "Available" or "Leased",
            "bedrooms": floor_plan.get("bedrooms"),
            "bathrooms": floor_plan.get("bathrooms"),
            "sqft": unit.get("area"),
            "floor": unit.get("floor"),
            "floor_plan_name": floor_plan.get("name"),
            "available_date": unit.get("available_date"),
        })
    return results


def _parse_rentcafe_astro_data(html: str) -> list[dict[str, Any]]:
    """
    Parse RentCafe data embedded in Astro format.

    The site uses HTML-encoded JSON in the page with format:
    "floorplanName":[0,"S7"],"maximumRent":[0,3028],"minimumRent":[0,2178]
    """
    results = []

    # Decode HTML entities
    html = html.replace('&quot;', '"').replace('&amp;', '&')

    # Find all floor plan data blocks using pre-compiled pattern
    matches = _RE_RENTCAFE_ASTRO.findall(html)

    for i, match in enumerate(matches):
        name, max_rent, min_rent, sqft, status = match
        results.append({
            "unit_number": f"{name}-{i+1}",  # Generate unit number from floorplan
            "floor_plan_name": name,
            "price": int(min_rent),  # Use minimum rent as the base price
            "max_price": int(max_rent),
            "sqft": int(sqft),
            "status": status,
            "bedrooms": _infer_bedrooms_from_floorplan(name),
            "bathrooms": None,  # Not available in this format
        })

    return results


def _infer_bedrooms_from_floorplan(name: str) -> int | None:
    """Infer bedroom count from floor plan name."""
    name_upper = name.upper()
    if name_upper.startswith('S'):
        return 0  # Studio
    elif name_upper.startswith('A'):
        return 1  # 1 bedroom
    elif name_upper.startswith('B'):
        return 2  # 2 bedroom
    elif name_upper.startswith('C'):
        return 3  # 3 bedroom
    return None


def _parse_astro_data(html: str) -> list[dict[str, Any]]:
    """
    Fallback parser using Astro embedded data.

    Preserved from legacy/crossing.py
    """
    # Look for Astro data in script tags using pre-compiled pattern
    match = _RE_ASTRO_DATA.search(html)
    if not match:
        return []

    try:
        data = json.loads(match.group(1))
        units = data.get("units", [])
        return [
            {
                "unit_number": u.get("name"),
                "price": u.get("rent"),
                "status": u.get("status"),
                "bedrooms": u.get("beds"),
                "bathrooms": u.get("baths"),
                "sqft": u.get("sqft"),
            }
            for u in units
        ]
    except (json.JSONDecodeError, KeyError):
        return []


# =============================================================================
# Adapter Implementation
# =============================================================================


def get_default_config() -> SourceConfig:
    """Get default configuration for Crossing DC source."""
    return SourceConfig(
        name="crossing_dc",
        display_name="Crossing DC Apartments",
        description="Residential apartment listings from Crossing DC complex",
        source_type=SourceType.WEB_SCRAPE,
        base_url="https://crossingdc.com/",
        requires_auth=False,
        requests_per_second=0.5,  # Be gentle with the website
        max_workers=1,
        batch_size=100,
        timeout_seconds=30,
        max_retries=3,
        adapter_class="src.adapters.real_estate.crossing.CrossingAdapter",
        output_record_types=["RentalRecord"],
        extra={
            "sightmap_api_base": "https://sightmap.com/app/api/v1/sightmaps",
            "property_name": "Crossing DC",
            "city": "Washington",
            "state": "DC",
        },
    )


@register_scraper("crossing_dc")
class CrossingAdapter(BaseScraper):
    """
    Adapter for Crossing DC apartment listings.

    Wraps the dual-method extraction from legacy/crossing.py:
    1. Primary: SightMap API (if ID can be extracted)
    2. Fallback: Astro embedded data parsing
    """

    SOURCE_NAME = "crossing_dc"
    SOURCE_VERSION = "1.0.0"
    SUPPORTED_RECORD_TYPES = [RentalRecord]

    def __init__(self, config: SourceConfig, **kwargs):
        super().__init__(config, **kwargs)
        self._session: requests.Session | None = None
        self._sightmap_id: str | None = None

    def setup(self) -> None:
        """Initialize HTTP session."""
        super().setup()
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; UniversalScraper/1.0)"
        })

    def teardown(self) -> None:
        """Close HTTP session."""
        if self._session:
            self._session.close()
            self._session = None
        super().teardown()

    def discover(self) -> Iterator[dict[str, Any]]:
        """
        Discover the Crossing DC listing page.

        For this single-property scraper, we just yield one item.
        The actual unit discovery happens during fetch/parse.
        """
        # The floor-plans page contains the embedded RentCafe data
        floor_plans_url = self.config.base_url.rstrip('/') + '/floor-plans'
        yield {
            "url": floor_plans_url,
            "method": "rentcafe_astro",  # Primary method for current site structure
        }

    def fetch(self, item: dict[str, Any]) -> RawPayload | None:
        """
        Fetch the floor plans page with embedded RentCafe data.

        Returns raw payload containing the HTML page.
        """
        if self._session is None:
            self._session = requests.Session()
            self._session.headers.update({
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            })

        url = item["url"]

        try:
            response = self._session.get(url, timeout=self.config.timeout_seconds)
            response.raise_for_status()

            return RawPayload(
                content_type="text/html",
                content=response.content,
                lineage=self._make_lineage(url),
                fetch_status_code=response.status_code,
            )

        except requests.RequestException as e:
            self._logger.error(f"Fetch failed: {e}")
            return None

    def parse(self, payload: RawPayload) -> Iterator[dict[str, Any]]:
        """
        Parse the raw payload into unit records.

        Uses RentCafe/Astro parser for HTML content.
        """
        content = payload.content.decode("utf-8")

        if payload.content_type == "application/json":
            # SightMap API response (legacy, may not work anymore)
            try:
                data = json.loads(content)
                units = data if isinstance(data, list) else data.get("data", [])
                yield from _parse_sightmap_units(units)
            except json.JSONDecodeError as e:
                self._logger.error(f"JSON parse error: {e}")
        else:
            # Primary: RentCafe data embedded in Astro format
            units = _parse_rentcafe_astro_data(content)
            if units:
                self._logger.info(f"Parsed {len(units)} floor plans from RentCafe data")
                yield from units
            else:
                # Fallback: legacy Astro parser
                self._logger.info("RentCafe data not found, trying legacy Astro parser")
                yield from _parse_astro_data(content)

    def validate(self, data: dict[str, Any]) -> tuple[bool, list[str]]:
        """
        Validate a unit record.

        Checks:
        - Unit number is present
        - Price is numeric (if present)
        - Bedrooms/bathrooms are reasonable
        """
        errors = []

        # Unit number is the primary key
        if not data.get("unit_number"):
            errors.append("Missing unit number")

        # Validate price
        price = data.get("price")
        if price is not None:
            try:
                p = Decimal(str(price))
                if p < 0:
                    errors.append(f"Negative price: {price}")
            except (InvalidOperation, ValueError, TypeError):
                errors.append(f"Invalid price: {price}")

        # Validate bedrooms
        beds = data.get("bedrooms")
        if beds is not None:
            try:
                b = int(beds)
                if b < 0 or b > 20:
                    errors.append(f"Invalid bedroom count: {beds}")
            except (ValueError, TypeError):
                errors.append(f"Non-numeric bedrooms: {beds}")

        return len(errors) == 0, errors

    def normalize(self, data: dict[str, Any]) -> RentalRecord:
        """
        Normalize to canonical RentalRecord schema.
        """
        # Type conversions
        price = None
        if data.get("price"):
            try:
                price = Decimal(str(data["price"]))
            except (InvalidOperation, ValueError, TypeError):
                pass

        bedrooms = None
        if data.get("bedrooms") is not None:
            try:
                bedrooms = int(data["bedrooms"])
            except (ValueError, TypeError):
                pass

        bathrooms = None
        if data.get("bathrooms") is not None:
            try:
                bathrooms = float(data["bathrooms"])
            except (ValueError, TypeError):
                pass

        sqft = None
        if data.get("sqft") is not None:
            try:
                sqft = int(data["sqft"])
            except (ValueError, TypeError):
                pass

        floor = None
        if data.get("floor") is not None:
            try:
                floor = int(data["floor"])
            except (ValueError, TypeError):
                pass

        # Parse available date
        available_date = None
        if data.get("available_date"):
            try:
                available_date = date.fromisoformat(str(data["available_date"]))
            except ValueError:
                pass

        # Create lineage with content hash
        raw_content = str(sorted(data.items())).encode()
        lineage = self._make_lineage()
        lineage = lineage.model_copy(
            update={"raw_hash": LineageInfo.compute_hash(raw_content)}
        )

        return RentalRecord(
            lineage=lineage,
            property_name=self.config.extra.get("property_name"),
            unit_number=str(data.get("unit_number", "")),
            floor=floor,
            floor_plan_name=data.get("floor_plan_name"),
            rent_price=price,
            bedrooms=bedrooms,
            bathrooms=bathrooms,
            sqft=sqft,
            status=data.get("status"),
            city=self.config.extra.get("city"),
            state=self.config.extra.get("state"),
            available_date=available_date,
            source_record_id=str(data.get("unit_number", "")),
        )


# =============================================================================
# Auto-register config on module import
# =============================================================================

def _register_config():
    """Register Crossing DC config with the registry."""
    from ...core.registry import ScraperRegistry
    ScraperRegistry.register_config(get_default_config())


_register_config()
