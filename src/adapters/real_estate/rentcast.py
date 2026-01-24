"""
RentCast API adapter for rental market data.

Fetches rental market data for DC area ZIP codes:
- Market Statistics (avg rent, median rent, occupancy)
- Rent Estimates by property type and bedrooms
- Property Value Estimates

Requires API key: Set RENTCAST_API_KEY environment variable.
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
from ...core.registry import ScraperRegistry, register_scraper
from ...schemas.base import LineageInfo, RawPayload
from ...schemas.metadata import SourceConfig, SourceType
from ...schemas.tabular import MarketStatsRecord

logger = logging.getLogger(__name__)


# =============================================================================
# Configuration
# =============================================================================

RENTCAST_BASE_URL = "https://api.rentcast.io/v1"

# DC Area ZIP codes
DC_ZIP_CODES = [
    # DC proper
    "20001", "20002", "20003", "20004", "20005", "20006", "20007", "20008",
    "20009", "20010", "20011", "20012", "20015", "20016", "20017", "20018",
    "20019", "20020", "20024", "20032", "20036", "20037",
    # Arlington VA
    "22201", "22202", "22203", "22204", "22205", "22206", "22207", "22209",
    # Alexandria VA
    "22301", "22302", "22304", "22305", "22314",
    # Bethesda/Silver Spring MD
    "20814", "20815", "20816", "20817", "20910", "20901", "20902",
]


@dataclass
class DatasetConfig:
    """Configuration for a RentCast dataset."""
    name: str
    display_name: str
    description: str
    endpoint: str
    metric_prefix: str


# Dataset configurations
DATASETS = {
    "market_stats": DatasetConfig(
        name="market_stats",
        display_name="Market Statistics",
        description="Rental market statistics by ZIP code",
        endpoint="/markets",
        metric_prefix="market",
    ),
    "rent_estimates": DatasetConfig(
        name="rent_estimates",
        display_name="Rent Estimates",
        description="Rent estimates by property type and ZIP",
        endpoint="/avm/rent/zipcode",
        metric_prefix="rent",
    ),
    "value_estimates": DatasetConfig(
        name="value_estimates",
        display_name="Property Value Estimates",
        description="Property value estimates by ZIP",
        endpoint="/avm/value/zipcode",
        metric_prefix="value",
    ),
}

# Market stats metrics to extract
MARKET_STATS_METRICS = [
    ("averageRent", "average_rent", "dollars"),
    ("medianRent", "median_rent", "dollars"),
    ("minRent", "min_rent", "dollars"),
    ("maxRent", "max_rent", "dollars"),
    ("averageRentPerSqft", "average_rent_per_sqft", "dollars_per_sqft"),
    ("occupancyRate", "occupancy_rate", "percent"),
    ("daysOnMarket", "days_on_market", "days"),
    ("totalListings", "total_listings", "count"),
]


# =============================================================================
# Default Configuration
# =============================================================================

def get_default_config(dataset_name: str = "market_stats") -> SourceConfig:
    """Get default configuration for RentCast API source."""
    dataset = DATASETS.get(dataset_name, DATASETS["market_stats"])

    # Map dataset names to registered scraper names
    scraper_names = {
        "market_stats": "rentcast_market_stats",
        "rent_estimates": "rentcast_rent_estimates",
        "value_estimates": "rentcast_value_estimates",
    }
    scraper_name = scraper_names.get(dataset_name, "rentcast_market_stats")

    return SourceConfig(
        name=scraper_name,
        display_name=f"RentCast {dataset.display_name}",
        description=f"DC area {dataset.description.lower()} from RentCast API",
        source_type=SourceType.REST_API,
        base_url=RENTCAST_BASE_URL,
        requires_auth=True,
        requests_per_second=2.0,  # RentCast rate limits
        max_workers=1,  # Sequential to avoid rate limits
        batch_size=50,
        timeout_seconds=30,
        max_retries=3,
        adapter_class="src.adapters.real_estate.rentcast.RentCastBaseAdapter",
        output_record_types=["MarketStatsRecord"],
        extra={
            "dataset_name": dataset_name,
            "zip_codes": DC_ZIP_CODES,
            "rate_limit_wait": 5,
        },
    )


# =============================================================================
# Adapter Implementation
# =============================================================================

class RentCastBaseAdapter(BaseScraper):
    """
    Base adapter for RentCast API.

    Supports multiple datasets (market_stats, rent_estimates, value_estimates).
    Uses demo mode when no API key is available.
    """

    SOURCE_NAME = "rentcast_api"
    SOURCE_VERSION = "1.0.0"
    SUPPORTED_RECORD_TYPES = [MarketStatsRecord]

    # Override in subclasses
    DATASET_CONFIG: DatasetConfig = DATASETS["market_stats"]

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

        self._api_key = os.environ.get("RENTCAST_API_KEY")
        if not self._api_key:
            self._logger.warning("No RENTCAST_API_KEY found. Running in demo mode.")
            self._demo_mode = True

        self._session = requests.Session()
        if self._api_key:
            self._session.headers.update({
                "X-Api-Key": self._api_key,
                "Accept": "application/json",
            })

    def teardown(self) -> None:
        """Close HTTP session."""
        if self._session:
            self._session.close()
            self._session = None
        super().teardown()

    def discover(self) -> Iterator[dict[str, Any]]:
        """
        Discover ZIP codes to query.

        For market_stats: one request per ZIP code
        For rent_estimates: one request per ZIP code + bedroom combo
        For value_estimates: one request per ZIP code + property type
        """
        zip_codes = self.config.extra.get("zip_codes", DC_ZIP_CODES)

        if self._dataset_name == "market_stats":
            for zipcode in zip_codes:
                yield {
                    "zipcode": zipcode,
                    "endpoint": self._dataset.endpoint,
                    "dataset": self._dataset_name,
                }

        elif self._dataset_name == "rent_estimates":
            bedroom_configs = [0, 1, 2, 3]  # Studio, 1BR, 2BR, 3BR
            for zipcode in zip_codes:
                for bedrooms in bedroom_configs:
                    yield {
                        "zipcode": zipcode,
                        "bedrooms": bedrooms,
                        "endpoint": self._dataset.endpoint,
                        "dataset": self._dataset_name,
                    }

        elif self._dataset_name == "value_estimates":
            property_types = ["condo", "single_family"]
            for zipcode in zip_codes:
                for prop_type in property_types:
                    yield {
                        "zipcode": zipcode,
                        "property_type": prop_type,
                        "endpoint": self._dataset.endpoint,
                        "dataset": self._dataset_name,
                    }

    def fetch(self, item: dict[str, Any]) -> RawPayload | None:
        """
        Fetch data from RentCast API or generate demo data.
        """
        if self._demo_mode:
            return self._fetch_demo(item)

        return self._fetch_api(item)

    def _fetch_api(self, item: dict[str, Any]) -> RawPayload | None:
        """Fetch from the actual RentCast API."""
        if self._session is None:
            return None

        url = f"{RENTCAST_BASE_URL}{item['endpoint']}"
        params = {"zipCode": item["zipcode"]}

        # Add additional params based on dataset
        if item["dataset"] == "rent_estimates":
            params["bedrooms"] = item.get("bedrooms", 2)
            params["propertyType"] = "apartment"
        elif item["dataset"] == "value_estimates":
            params["propertyType"] = item.get("property_type", "condo")

        try:
            response = self._session.get(
                url,
                params=params,
                timeout=self.config.timeout_seconds,
            )

            if response.status_code == 401:
                self._logger.error("Invalid API key")
                return None
            elif response.status_code == 429:
                self._logger.warning(f"Rate limited, waiting {self._rate_limit_wait}s...")
                time.sleep(self._rate_limit_wait)
                return self._fetch_api(item)

            response.raise_for_status()

            # Rate limit between requests
            time.sleep(1.0 / self.config.requests_per_second)

            # Add metadata to response
            data = response.json()
            data["_zipcode"] = item["zipcode"]
            data["_dataset"] = item["dataset"]
            if "bedrooms" in item:
                data["_bedrooms"] = item["bedrooms"]
            if "property_type" in item:
                data["_property_type"] = item["property_type"]
            data["_scraped_at"] = datetime.now(timezone.utc).isoformat()

            return RawPayload(
                content_type="application/json",
                content=json.dumps(data).encode(),
                lineage=self._make_lineage(url),
                fetch_status_code=response.status_code,
            )

        except requests.RequestException as e:
            self._logger.error(f"Fetch failed for {item['zipcode']}: {e}")
            return None

    def _fetch_demo(self, item: dict[str, Any]) -> RawPayload:
        """Generate demo data when no API key is available."""
        import random

        zipcode = item["zipcode"]
        dataset = item["dataset"]

        if dataset == "market_stats":
            data = {
                "_zipcode": zipcode,
                "_dataset": dataset,
                "_demo": True,
                "averageRent": random.randint(1800, 3500),
                "medianRent": random.randint(1700, 3300),
                "minRent": random.randint(1200, 1600),
                "maxRent": random.randint(4000, 6000),
                "averageRentPerSqft": round(random.uniform(2.0, 4.5), 2),
                "occupancyRate": round(random.uniform(0.92, 0.98), 3),
                "daysOnMarket": random.randint(15, 45),
                "totalListings": random.randint(50, 300),
                "_scraped_at": datetime.now(timezone.utc).isoformat(),
            }

        elif dataset == "rent_estimates":
            bedrooms = item.get("bedrooms", 2)
            base_rent = 1500 + (bedrooms * 500) + random.randint(-200, 200)
            data = {
                "_zipcode": zipcode,
                "_dataset": dataset,
                "_bedrooms": bedrooms,
                "_demo": True,
                "rent": base_rent,
                "rentRangeLow": base_rent - 200,
                "rentRangeHigh": base_rent + 300,
                "confidence": round(random.uniform(0.7, 0.95), 2),
                "_scraped_at": datetime.now(timezone.utc).isoformat(),
            }

        elif dataset == "value_estimates":
            prop_type = item.get("property_type", "condo")
            base_value = 450000 if prop_type == "condo" else 650000
            value = base_value + random.randint(-50000, 100000)
            data = {
                "_zipcode": zipcode,
                "_dataset": dataset,
                "_property_type": prop_type,
                "_demo": True,
                "price": value,
                "priceRangeLow": value - 30000,
                "priceRangeHigh": value + 50000,
                "confidence": round(random.uniform(0.75, 0.92), 2),
                "_scraped_at": datetime.now(timezone.utc).isoformat(),
            }
        else:
            data = {"_zipcode": zipcode, "_dataset": dataset, "_demo": True}

        return RawPayload(
            content_type="application/json",
            content=json.dumps(data).encode(),
            lineage=self._make_lineage(f"{RENTCAST_BASE_URL}{item['endpoint']}?demo=true"),
            fetch_status_code=200,
        )

    def parse(self, payload: RawPayload) -> Iterator[dict[str, Any]]:
        """
        Parse API response into metric records.

        For market_stats: yields multiple records (one per metric)
        For rent/value estimates: yields one record per API response
        """
        try:
            data = json.loads(payload.content.decode("utf-8"))
        except json.JSONDecodeError as e:
            self._logger.error(f"JSON parse error: {e}")
            return

        dataset = data.get("_dataset", self._dataset_name)

        if dataset == "market_stats":
            # Extract each metric as a separate record
            for api_field, metric_name, unit in MARKET_STATS_METRICS:
                if api_field in data and data[api_field] is not None:
                    yield {
                        "zipcode": data.get("_zipcode"),
                        "metric_name": metric_name,
                        "metric_value": data[api_field],
                        "metric_unit": unit,
                        "dataset": dataset,
                        "_demo": data.get("_demo", False),
                        "_scraped_at": data.get("_scraped_at"),
                    }

        elif dataset == "rent_estimates":
            yield {
                "zipcode": data.get("_zipcode"),
                "metric_name": "rent_estimate",
                "metric_value": data.get("rent"),
                "metric_unit": "dollars",
                "bedrooms": data.get("_bedrooms"),
                "rent_range_low": data.get("rentRangeLow"),
                "rent_range_high": data.get("rentRangeHigh"),
                "confidence": data.get("confidence"),
                "dataset": dataset,
                "_demo": data.get("_demo", False),
                "_scraped_at": data.get("_scraped_at"),
            }

        elif dataset == "value_estimates":
            yield {
                "zipcode": data.get("_zipcode"),
                "metric_name": "value_estimate",
                "metric_value": data.get("price"),
                "metric_unit": "dollars",
                "property_type": data.get("_property_type"),
                "price_range_low": data.get("priceRangeLow"),
                "price_range_high": data.get("priceRangeHigh"),
                "confidence": data.get("confidence"),
                "dataset": dataset,
                "_demo": data.get("_demo", False),
                "_scraped_at": data.get("_scraped_at"),
            }

    def validate(self, data: dict[str, Any]) -> tuple[bool, list[str]]:
        """
        Validate a metric record.

        Checks:
        - Has zipcode
        - Has metric_name
        - metric_value is numeric (if present)
        """
        errors = []

        if not data.get("zipcode"):
            errors.append("Missing zipcode")

        if not data.get("metric_name"):
            errors.append("Missing metric_name")

        value = data.get("metric_value")
        if value is not None:
            try:
                Decimal(str(value))
            except (InvalidOperation, ValueError, TypeError):
                errors.append(f"Invalid metric_value: {value}")

        return len(errors) == 0, errors

    def normalize(self, data: dict[str, Any]) -> MarketStatsRecord:
        """
        Normalize to canonical MarketStatsRecord schema.
        """
        # Convert metric value
        metric_value = None
        if data.get("metric_value") is not None:
            try:
                metric_value = Decimal(str(data["metric_value"]))
            except (InvalidOperation, ValueError, TypeError):
                pass

        # Determine state from ZIP code
        zipcode = data.get("zipcode", "")
        state = self._get_state_from_zip(zipcode)

        # Build extra fields
        extra_fields = {}
        if data.get("_demo"):
            extra_fields["demo_data"] = True
        if data.get("rent_range_low") is not None:
            extra_fields["range_low"] = data["rent_range_low"]
        if data.get("rent_range_high") is not None:
            extra_fields["range_high"] = data["rent_range_high"]
        if data.get("price_range_low") is not None:
            extra_fields["range_low"] = data["price_range_low"]
        if data.get("price_range_high") is not None:
            extra_fields["range_high"] = data["price_range_high"]
        if data.get("confidence") is not None:
            extra_fields["confidence"] = data["confidence"]
        if data.get("_scraped_at"):
            extra_fields["scraped_at"] = data["_scraped_at"]

        # Create lineage with content hash
        raw_content = str(sorted(data.items())).encode()
        lineage = self._make_lineage()
        lineage = lineage.model_copy(
            update={"raw_hash": LineageInfo.compute_hash(raw_content)}
        )

        # Generate source record ID
        source_id_parts = [zipcode, data.get("metric_name", "")]
        if data.get("bedrooms") is not None:
            source_id_parts.append(f"br{data['bedrooms']}")
        if data.get("property_type"):
            source_id_parts.append(data["property_type"])
        source_record_id = "_".join(source_id_parts)

        return MarketStatsRecord(
            lineage=lineage,
            extra_fields=extra_fields if extra_fields else {},
            source_record_id=source_record_id,
            region_name=zipcode,
            region_type="zip",
            state=state,
            zip_code=zipcode,
            metric_name=data.get("metric_name", "unknown"),
            metric_value=metric_value,
            metric_date=date.today(),
            metric_period="snapshot",
            bedrooms=data.get("bedrooms"),
            property_type=data.get("property_type"),
            # Provide non-empty dict to avoid Parquet empty struct error
            historical_values={"_source": "rentcast"},
        )

    def _get_state_from_zip(self, zipcode: str) -> str | None:
        """Determine state from ZIP code prefix."""
        if not zipcode:
            return None
        prefix = zipcode[:3]
        if prefix.startswith("200") or prefix.startswith("201"):
            return "DC"
        elif prefix.startswith("222"):
            return "VA"
        elif prefix.startswith("223"):
            return "VA"
        elif prefix.startswith("208") or prefix.startswith("209"):
            return "MD"
        return None


# =============================================================================
# Registered Adapters
# =============================================================================

@register_scraper("rentcast_market_stats")
class RentCastMarketStatsAdapter(RentCastBaseAdapter):
    """Adapter for RentCast Market Statistics."""

    SOURCE_NAME = "rentcast_market_stats"
    DATASET_CONFIG = DATASETS["market_stats"]


@register_scraper("rentcast_rent_estimates")
class RentCastRentEstimatesAdapter(RentCastBaseAdapter):
    """Adapter for RentCast Rent Estimates."""

    SOURCE_NAME = "rentcast_rent_estimates"
    DATASET_CONFIG = DATASETS["rent_estimates"]


@register_scraper("rentcast_value_estimates")
class RentCastValueEstimatesAdapter(RentCastBaseAdapter):
    """Adapter for RentCast Property Value Estimates."""

    SOURCE_NAME = "rentcast_value_estimates"
    DATASET_CONFIG = DATASETS["value_estimates"]


# Keep legacy name for backward compatibility
@register_scraper("rentcast_api")
class RentCastAdapter(RentCastMarketStatsAdapter):
    """Legacy adapter name - defaults to Market Statistics."""

    SOURCE_NAME = "rentcast_api"


# =============================================================================
# Helper Functions
# =============================================================================

def get_market_stats_config() -> SourceConfig:
    """Get configuration for Market Statistics."""
    return get_default_config("market_stats")


def get_rent_estimates_config() -> SourceConfig:
    """Get configuration for Rent Estimates."""
    return get_default_config("rent_estimates")


def get_value_estimates_config() -> SourceConfig:
    """Get configuration for Value Estimates."""
    return get_default_config("value_estimates")


# =============================================================================
# Auto-register configs on module import
# =============================================================================

def _register_all_configs():
    """Register all RentCast configs with the registry."""
    for dataset_name in DATASETS:
        config = get_default_config(dataset_name)
        ScraperRegistry.register_config(config)

    # Also register legacy name
    legacy_config = get_default_config("market_stats")
    legacy_config = SourceConfig(
        **{**legacy_config.model_dump(), "name": "rentcast_api"}
    )
    ScraperRegistry.register_config(legacy_config)


# Register configs when module is imported
_register_all_configs()
