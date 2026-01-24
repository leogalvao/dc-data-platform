"""
Realtor.com API adapter via RapidAPI.

Fetches property listings for DC area:
- For Sale Listings
- For Rent Listings
- Recently Sold

Requires RapidAPI key: Set RAPIDAPI_KEY environment variable.
Supports demo mode when no API key is available.
"""

from __future__ import annotations

import json
import logging
import os
import time
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

import requests

from ...core.base_scraper import BaseScraper
from ...core.registry import register_scraper
from ...schemas.base import LineageInfo, RawPayload
from ...schemas.metadata import SourceConfig, SourceType
from ...schemas.tabular import ListingType, PropertyRecord, PropertyType

logger = logging.getLogger(__name__)


# =============================================================================
# Configuration
# =============================================================================

RAPIDAPI_HOST = "realtor16.p.rapidapi.com"
RAPIDAPI_BASE = f"https://{RAPIDAPI_HOST}"

# Search areas for DC metro
DC_SEARCH_AREAS = [
    {"city": "Washington", "state_code": "DC"},
    {"city": "Arlington", "state_code": "VA"},
    {"city": "Alexandria", "state_code": "VA"},
    {"city": "Bethesda", "state_code": "MD"},
    {"city": "Silver Spring", "state_code": "MD"},
]


@dataclass
class DatasetConfig:
    """Configuration for a Realtor.com dataset."""
    name: str
    display_name: str
    description: str
    endpoint: str
    listing_type: ListingType


# Dataset configurations
DATASETS = {
    "for_sale": DatasetConfig(
        name="for_sale",
        display_name="For Sale Listings",
        description="Properties currently for sale",
        endpoint="/forsale",
        listing_type=ListingType.FOR_SALE,
    ),
    "for_rent": DatasetConfig(
        name="for_rent",
        display_name="For Rent Listings",
        description="Properties currently for rent",
        endpoint="/forrent",
        listing_type=ListingType.FOR_RENT,
    ),
    "sold": DatasetConfig(
        name="sold",
        display_name="Recently Sold",
        description="Recently sold properties",
        endpoint="/sold",
        listing_type=ListingType.SOLD,
    ),
}

# Field mappings from Realtor.com API to PropertyRecord
# API returns nested structure: location.address, description, etc.
FIELD_MAPPINGS = {
    # IDs
    "property_id": "listing_id",

    # Location - nested under location.address
    "line": "address",
    "city": "city",
    "state_code": "state",
    "postal_code": "zip_code",
    "county": "county",
    "neighborhood": "neighborhood",

    # Coordinates - nested under location.address.coordinate
    "lat": "latitude",
    "lon": "longitude",

    # Description - nested under description
    "beds": "bedrooms",
    "baths": "bathrooms",
    "sqft": "sqft",
    "lot_sqft": "lot_sqft",
    "year_built": "year_built",
    "stories": "stories",
    "garage": "garage_spaces",
    "type": "property_type_raw",

    # Pricing
    "list_price": "price",
    "sold_price": "sold_price",
    "price_per_sqft": "price_per_sqft",
    "original_price": "original_price",

    # Dates
    "list_date": "list_date",
    "sold_date": "sold_date",
    "last_update": "last_update",

    # Additional
    "status": "status",
    "photo_count": "photo_count",
    "days_on_market": "days_on_market",
}

# Property type mapping
PROPERTY_TYPE_MAP = {
    "single_family": PropertyType.SINGLE_FAMILY,
    "condo": PropertyType.CONDO,
    "condos": PropertyType.CONDO,
    "townhouse": PropertyType.TOWNHOUSE,
    "townhome": PropertyType.TOWNHOUSE,
    "multi_family": PropertyType.MULTI_FAMILY,
    "apartment": PropertyType.APARTMENT,
    "land": PropertyType.LAND,
    "commercial": PropertyType.COMMERCIAL,
}


# =============================================================================
# Default Configuration
# =============================================================================

def get_default_config(dataset_name: str = "for_sale") -> SourceConfig:
    """Get default configuration for Realtor.com API source."""
    dataset = DATASETS.get(dataset_name, DATASETS["for_sale"])

    # Map dataset names to registered scraper names
    scraper_names = {
        "for_sale": "realtor_for_sale",
        "for_rent": "realtor_for_rent",
        "sold": "realtor_sold",
    }
    scraper_name = scraper_names.get(dataset_name, "realtor_for_sale")

    return SourceConfig(
        name=scraper_name,
        display_name=f"Realtor.com {dataset.display_name}",
        description=f"DC area {dataset.description.lower()} from Realtor.com via RapidAPI",
        source_type=SourceType.REST_API,
        base_url=RAPIDAPI_BASE,
        requires_auth=True,
        requests_per_second=1.0,  # RapidAPI rate limits
        max_workers=1,  # Sequential to avoid rate limits
        batch_size=50,
        timeout_seconds=30,
        max_retries=3,
        adapter_class="src.adapters.real_estate.realtor.RealtorBaseAdapter",
        output_record_types=["PropertyRecord"],
        extra={
            "dataset_name": dataset_name,
            "rapidapi_host": RAPIDAPI_HOST,
            "search_areas": DC_SEARCH_AREAS,
            "limit_per_area": 50,
            "rate_limit_wait": 5,
        },
    )


# =============================================================================
# Adapter Implementation
# =============================================================================

class RealtorBaseAdapter(BaseScraper):
    """
    Base adapter for Realtor.com via RapidAPI.

    Supports multiple datasets (for_sale, for_rent, sold).
    Uses demo mode when no API key is available.
    """

    SOURCE_NAME = "realtor_api"
    SOURCE_VERSION = "1.0.0"
    SUPPORTED_RECORD_TYPES = [PropertyRecord]

    # Override in subclasses
    DATASET_CONFIG: DatasetConfig = DATASETS["for_sale"]

    def __init__(self, config: SourceConfig, **kwargs):
        super().__init__(config, **kwargs)
        self._session: requests.Session | None = None
        self._api_key: str | None = None
        self._demo_mode: bool = False
        self._rate_limit_wait: float = config.extra.get("rate_limit_wait", 5)

    @property
    def _dataset_name(self) -> str:
        """Get the dataset name from config."""
        return self.config.extra.get("dataset_name", self.DATASET_CONFIG.name)

    @property
    def _dataset(self) -> DatasetConfig:
        """Get the dataset configuration."""
        return DATASETS.get(self._dataset_name, self.DATASET_CONFIG)

    def setup(self) -> None:
        """Initialize HTTP session and check for API key."""
        super().setup()

        self._api_key = os.environ.get("RAPIDAPI_KEY")
        if not self._api_key:
            self._logger.warning("No RAPIDAPI_KEY found. Running in demo mode.")
            self._demo_mode = True

        self._session = requests.Session()
        if self._api_key:
            self._session.headers.update({
                "X-RapidAPI-Key": self._api_key,
                "X-RapidAPI-Host": RAPIDAPI_HOST,
            })

    def teardown(self) -> None:
        """Close HTTP session."""
        if self._session:
            self._session.close()
            self._session = None
        super().teardown()

    def discover(self) -> Iterator[dict[str, Any]]:
        """
        Discover search areas for the dataset.

        Yields one item per search area (city/state combination).
        """
        search_areas = self.config.extra.get("search_areas", DC_SEARCH_AREAS)
        limit_per_area = self.config.extra.get("limit_per_area", 50)

        for area in search_areas:
            yield {
                "city": area["city"],
                "state_code": area["state_code"],
                "limit": limit_per_area,
                "endpoint": self._dataset.endpoint,
                "dataset": self._dataset_name,
            }

    def fetch(self, item: dict[str, Any]) -> RawPayload | None:
        """
        Fetch listings from RapidAPI or generate demo data.
        """
        if self._demo_mode:
            return self._fetch_demo(item)

        return self._fetch_api(item)

    def _fetch_api(self, item: dict[str, Any]) -> RawPayload | None:
        """Fetch from the actual RapidAPI endpoint."""
        if self._session is None:
            return None

        url = f"{RAPIDAPI_BASE}{item['endpoint']}"
        params = {
            "city": item["city"],
            "state_code": item["state_code"],
            "limit": item["limit"],
            "offset": 0,
            "sort": "newest",
        }

        try:
            response = self._session.get(
                url,
                params=params,
                timeout=self.config.timeout_seconds,
            )

            if response.status_code == 401 or response.status_code == 403:
                self._logger.error("Invalid API key")
                return None
            elif response.status_code == 429:
                self._logger.warning(f"Rate limited, waiting {self._rate_limit_wait}s...")
                time.sleep(self._rate_limit_wait)
                return self._fetch_api(item)

            response.raise_for_status()

            # Rate limit between requests
            time.sleep(1.0 / self.config.requests_per_second)

            return RawPayload(
                content_type="application/json",
                content=response.content,
                lineage=self._make_lineage(url),
                fetch_status_code=response.status_code,
            )

        except requests.RequestException as e:
            self._logger.error(f"Fetch failed for {item['city']}, {item['state_code']}: {e}")
            return None

    def _fetch_demo(self, item: dict[str, Any]) -> RawPayload:
        """Generate demo data when no API key is available."""
        import random

        streets = [
            "Constitution Ave", "Pennsylvania Ave", "M St", "K St", "H St",
            "14th St", "U St", "Georgia Ave", "Connecticut Ave", "Wisconsin Ave",
        ]

        demo_listings = []
        for i in range(10):
            listing = {
                "property_id": f"demo_{item['city']}_{i:04d}",
                "_demo": True,
                "location": {
                    "address": {
                        "line": f"{random.randint(100, 9999)} {random.choice(streets)} NW",
                        "city": item["city"],
                        "state_code": item["state_code"],
                        "postal_code": f"200{random.randint(1, 20):02d}",
                        "coordinate": {
                            "lat": 38.9 + random.uniform(-0.1, 0.1),
                            "lon": -77.0 + random.uniform(-0.1, 0.1),
                        },
                    },
                },
                "description": {
                    "beds": random.randint(0, 4),
                    "baths": random.randint(1, 3),
                    "sqft": random.randint(500, 2500),
                    "year_built": random.randint(1920, 2024),
                    "type": random.choice(["single_family", "condo", "townhouse"]),
                },
                "list_price": random.randint(300000, 1500000) if item["dataset"] != "for_rent" else random.randint(1500, 4500),
                "status": item["dataset"],
                "_scraped_at": datetime.now(timezone.utc).isoformat(),
                "_search_city": item["city"],
                "_search_state": item["state_code"],
            }

            if item["dataset"] == "sold":
                listing["sold_price"] = listing["list_price"] - random.randint(-10000, 20000)
                listing["sold_date"] = "2024-12-15"

            demo_listings.append(listing)

        # Wrap in API response structure
        response_data = {"results": demo_listings}

        return RawPayload(
            content_type="application/json",
            content=json.dumps(response_data).encode(),
            lineage=self._make_lineage(f"{RAPIDAPI_BASE}{item['endpoint']}?demo=true"),
            fetch_status_code=200,
        )

    def parse(self, payload: RawPayload) -> Iterator[dict[str, Any]]:
        """
        Parse API response into listing records.
        """
        try:
            data = json.loads(payload.content.decode("utf-8"))
        except json.JSONDecodeError as e:
            self._logger.error(f"JSON parse error: {e}")
            return

        # Handle different response structures
        if "home_search" in data:
            results = data["home_search"].get("results", [])
        elif "results" in data:
            results = data["results"]
        else:
            results = data if isinstance(data, list) else []

        for listing in results:
            # Flatten nested structure
            flat = self._flatten_listing(listing)
            yield flat

    def _flatten_listing(self, listing: dict[str, Any]) -> dict[str, Any]:
        """Flatten nested API response into flat dict."""
        flat = {}

        # Top-level fields
        flat["property_id"] = listing.get("property_id")
        flat["list_price"] = listing.get("list_price")
        flat["sold_price"] = listing.get("sold_price")
        flat["status"] = listing.get("status")
        flat["_demo"] = listing.get("_demo", False)
        flat["_scraped_at"] = listing.get("_scraped_at")
        flat["_search_city"] = listing.get("_search_city")
        flat["_search_state"] = listing.get("_search_state")

        # Location fields
        location = listing.get("location", {})
        address = location.get("address", {})
        flat["line"] = address.get("line")
        flat["city"] = address.get("city")
        flat["state_code"] = address.get("state_code")
        flat["postal_code"] = address.get("postal_code")
        flat["county"] = address.get("county")
        flat["neighborhood"] = address.get("neighborhood")

        # Coordinates
        coord = address.get("coordinate", {})
        flat["lat"] = coord.get("lat")
        flat["lon"] = coord.get("lon")

        # Description fields
        desc = listing.get("description", {})
        flat["beds"] = desc.get("beds")
        flat["baths"] = desc.get("baths")
        flat["sqft"] = desc.get("sqft")
        flat["lot_sqft"] = desc.get("lot_sqft")
        flat["year_built"] = desc.get("year_built")
        flat["stories"] = desc.get("stories")
        flat["garage"] = desc.get("garage")
        flat["type"] = desc.get("type")

        # Price-related
        flat["price_per_sqft"] = listing.get("price_per_sqft")
        flat["original_price"] = listing.get("original_price")

        # Dates
        flat["list_date"] = listing.get("list_date")
        flat["sold_date"] = listing.get("sold_date")
        flat["last_update"] = listing.get("last_update")

        # Additional
        flat["photo_count"] = listing.get("photo_count")
        flat["days_on_market"] = listing.get("days_on_market")

        return flat

    def validate(self, data: dict[str, Any]) -> tuple[bool, list[str]]:
        """
        Validate a listing record.

        Checks:
        - Has property_id or address
        - Price is numeric (if present)
        - Coordinates are valid (if present)
        """
        errors = []

        # Must have identifying info
        if not data.get("property_id") and not data.get("line"):
            errors.append("Missing property_id and address")

        # Validate price
        price = data.get("list_price") or data.get("sold_price")
        if price is not None:
            try:
                p = Decimal(str(price))
                if p < 0:
                    errors.append(f"Negative price: {price}")
            except (InvalidOperation, ValueError, TypeError):
                errors.append(f"Invalid price: {price}")

        # Validate coordinates
        lat = data.get("lat")
        lon = data.get("lon")
        if lat is not None:
            try:
                lat_f = float(lat)
                if lat_f < -90 or lat_f > 90:
                    errors.append(f"Invalid latitude: {lat}")
            except (ValueError, TypeError):
                errors.append(f"Non-numeric latitude: {lat}")

        if lon is not None:
            try:
                lon_f = float(lon)
                if lon_f < -180 or lon_f > 180:
                    errors.append(f"Invalid longitude: {lon}")
            except (ValueError, TypeError):
                errors.append(f"Non-numeric longitude: {lon}")

        return len(errors) == 0, errors

    def normalize(self, data: dict[str, Any]) -> PropertyRecord:
        """
        Normalize to canonical PropertyRecord schema.
        """
        # Price - use list_price or sold_price depending on dataset
        price = None
        price_raw = data.get("list_price") or data.get("sold_price")
        if price_raw is not None:
            try:
                price = Decimal(str(price_raw))
            except (InvalidOperation, ValueError, TypeError):
                pass

        # Original price
        original_price = None
        if data.get("original_price") is not None:
            try:
                original_price = Decimal(str(data["original_price"]))
            except (InvalidOperation, ValueError, TypeError):
                pass

        # Price per sqft
        price_per_sqft = None
        if data.get("price_per_sqft") is not None:
            try:
                price_per_sqft = Decimal(str(data["price_per_sqft"]))
            except (InvalidOperation, ValueError, TypeError):
                pass

        # Integer fields
        bedrooms = self._safe_int(data.get("beds"))
        sqft = self._safe_int(data.get("sqft"))
        lot_sqft = self._safe_int(data.get("lot_sqft"))
        year_built = self._safe_int(data.get("year_built"))
        stories = self._safe_int(data.get("stories"))
        garage_spaces = self._safe_int(data.get("garage"))
        photo_count = self._safe_int(data.get("photo_count"))
        days_on_market = self._safe_int(data.get("days_on_market"))

        # Float fields
        bathrooms = self._safe_float(data.get("baths"))
        latitude = self._safe_float(data.get("lat"))
        longitude = self._safe_float(data.get("lon"))

        # Property type mapping
        property_type = None
        type_raw = data.get("type")
        if type_raw:
            type_lower = str(type_raw).lower().replace(" ", "_")
            property_type = PROPERTY_TYPE_MAP.get(type_lower, PropertyType.OTHER)

        # Dates
        list_date = self._parse_date(data.get("list_date"))
        sold_date = self._parse_date(data.get("sold_date"))

        # Determine listing type from dataset
        listing_type = self._dataset.listing_type

        # Check for price reduction
        price_reduced = False
        if original_price and price and original_price > price:
            price_reduced = True

        # Extra fields
        extra_fields = {}
        if data.get("_demo"):
            extra_fields["demo_data"] = True
        if data.get("_search_city"):
            extra_fields["search_city"] = data["_search_city"]
        if data.get("_search_state"):
            extra_fields["search_state"] = data["_search_state"]
        if data.get("_scraped_at"):
            extra_fields["scraped_at"] = data["_scraped_at"]

        # Create lineage with content hash
        raw_content = str(sorted(data.items())).encode()
        lineage = self._make_lineage()
        lineage = lineage.model_copy(
            update={"raw_hash": LineageInfo.compute_hash(raw_content)}
        )

        return PropertyRecord(
            lineage=lineage,
            extra_fields=extra_fields if extra_fields else {},
            source_record_id=str(data.get("property_id", "")),
            listing_type=listing_type,
            property_type=property_type,
            listing_id=str(data.get("property_id")) if data.get("property_id") else None,
            price=price,
            price_per_sqft=price_per_sqft,
            original_price=original_price,
            price_reduced=price_reduced,
            bedrooms=bedrooms,
            bathrooms=bathrooms,
            sqft=sqft,
            lot_sqft=lot_sqft,
            year_built=year_built,
            stories=stories,
            garage_spaces=garage_spaces,
            address=data.get("line"),
            city=data.get("city"),
            state=data.get("state_code"),
            zip_code=data.get("postal_code"),
            county=data.get("county"),
            neighborhood=data.get("neighborhood"),
            latitude=latitude,
            longitude=longitude,
            list_date=list_date,
            sold_date=sold_date,
            days_on_market=days_on_market,
            photo_count=photo_count,
        )

    def _safe_int(self, value: Any) -> int | None:
        """Safely convert to int."""
        if value is None:
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    def _safe_float(self, value: Any) -> float | None:
        """Safely convert to float."""
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def _parse_date(self, value: Any) -> date | None:
        """Parse date from various formats."""
        if value is None:
            return None
        try:
            if isinstance(value, date):
                return value
            return date.fromisoformat(str(value)[:10])
        except (ValueError, TypeError):
            return None


# =============================================================================
# Registered Adapters
# =============================================================================

@register_scraper("realtor_for_sale")
class RealtorForSaleAdapter(RealtorBaseAdapter):
    """Adapter for Realtor.com For Sale listings."""

    SOURCE_NAME = "realtor_for_sale"
    DATASET_CONFIG = DATASETS["for_sale"]


@register_scraper("realtor_for_rent")
class RealtorForRentAdapter(RealtorBaseAdapter):
    """Adapter for Realtor.com For Rent listings."""

    SOURCE_NAME = "realtor_for_rent"
    DATASET_CONFIG = DATASETS["for_rent"]


@register_scraper("realtor_sold")
class RealtorSoldAdapter(RealtorBaseAdapter):
    """Adapter for Realtor.com Recently Sold listings."""

    SOURCE_NAME = "realtor_sold"
    DATASET_CONFIG = DATASETS["sold"]


# Keep legacy name for backward compatibility
@register_scraper("realtor_api")
class RealtorAdapter(RealtorForSaleAdapter):
    """Legacy adapter name - defaults to For Sale listings."""

    SOURCE_NAME = "realtor_api"


# =============================================================================
# Helper Functions
# =============================================================================

def get_for_sale_config() -> SourceConfig:
    """Get configuration for For Sale listings."""
    return get_default_config("for_sale")


def get_for_rent_config() -> SourceConfig:
    """Get configuration for For Rent listings."""
    return get_default_config("for_rent")


def get_sold_config() -> SourceConfig:
    """Get configuration for Recently Sold listings."""
    return get_default_config("sold")


# =============================================================================
# Auto-register configs on module import
# =============================================================================

def _register_all_configs():
    """Register all Realtor.com configs with the registry."""
    from ...core.registry import ScraperRegistry

    for dataset_name in DATASETS:
        config = get_default_config(dataset_name)
        ScraperRegistry.register_config(config)

    # Also register legacy name
    legacy_config = get_default_config("for_sale")
    legacy_config = SourceConfig(
        **{**legacy_config.model_dump(), "name": "realtor_api"}
    )
    ScraperRegistry.register_config(legacy_config)


# Register configs when module is imported
_register_all_configs()
