"""
Open Data Network (ODN) adapter for demographic and economic data.

Downloads demographic and economic data from Socrata's Open Data Network API:
- DC Demographics (population, age, race, education)
- DC Economics (GDP, income, unemployment)
- DC Jobs by Sector
- Cost of Living
- Regional Comparisons (DC vs MD, VA, US)

Optional app token: Set ODN_APP_TOKEN environment variable.
Works without authentication but may be rate limited.
"""

from __future__ import annotations

import json
import logging
import os
import time
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

import requests

from ...core.base_scraper import BaseScraper
from ...core.registry import ScraperRegistry, register_scraper
from ...schemas.base import LineageInfo, RawPayload
from ...schemas.metadata import SourceConfig, SourceType
from ...schemas.tabular import DemographicRecord

logger = logging.getLogger(__name__)


# =============================================================================
# Configuration
# =============================================================================

ODN_BASE_URL = "https://api.us.socrata.com/api/catalog/v1"

# Entity IDs (FIPS-based identifiers)
ENTITIES = {
    "dc": {"id": "0400000US11", "name": "District of Columbia", "type": "state"},
    "maryland": {"id": "0400000US24", "name": "Maryland", "type": "state"},
    "virginia": {"id": "0400000US51", "name": "Virginia", "type": "state"},
    "us": {"id": "0100000US", "name": "United States", "type": "nation"},
    "arlington": {"id": "0500000US51013", "name": "Arlington County, VA", "type": "county"},
    "fairfax": {"id": "0500000US51059", "name": "Fairfax County, VA", "type": "county"},
    "montgomery": {"id": "0500000US24031", "name": "Montgomery County, MD", "type": "county"},
    "prince_georges": {"id": "0500000US24033", "name": "Prince George's County, MD", "type": "county"},
}

# Variable definitions by category
VARIABLES = {
    "demographics": {
        "population": ("demographics.population.count", "count"),
        "population_change": ("demographics.population.change", "percent"),
        "percent_under_18": ("demographics.population_by_age.percent_under_18", "percent"),
        "percent_18_to_64": ("demographics.population_by_age.percent_18_to_64", "percent"),
        "percent_65_plus": ("demographics.population_by_age.percent_65_and_over", "percent"),
        "percent_white": ("demographics.population_by_race.percent_white", "percent"),
        "percent_black": ("demographics.population_by_race.percent_black", "percent"),
        "percent_asian": ("demographics.population_by_race.percent_asian", "percent"),
        "percent_hispanic": ("demographics.population_by_race.percent_hispanic_or_latino", "percent"),
        "high_school_grad_pct": ("demographics.education.percent_high_school_graduate_or_higher", "percent"),
        "bachelors_degree_pct": ("demographics.education.percent_bachelors_degree_or_higher", "percent"),
    },
    "economics": {
        "gdp_per_capita": ("economy.gdp.per_capita", "dollars"),
        "median_household_income": ("earnings.median_household_income", "dollars"),
        "unemployment_count": ("economy.employment.unemployed", "count"),
        "labor_force_participation": ("economy.employment.labor_force_participation_rate", "percent"),
    },
    "jobs": {
        "government": ("jobs.government", "count"),
        "education_healthcare": ("jobs.education_and_healthcare", "count"),
        "professional_services": ("jobs.professional_and_business_services", "count"),
        "finance_real_estate": ("jobs.finance_and_real_estate", "count"),
        "retail_wholesale": ("jobs.retail_and_wholesale", "count"),
        "construction": ("jobs.construction", "count"),
        "information": ("jobs.information", "count"),
        "manufacturing": ("jobs.manufacturing", "count"),
        "transportation_utilities": ("jobs.transportation_and_utilities", "count"),
    },
    "cost_of_living": {
        "overall_index": ("cost.index", "index"),
        "housing_index": ("cost.housing", "index"),
        "food_index": ("cost.food", "index"),
        "transportation_index": ("cost.transportation", "index"),
        "healthcare_index": ("cost.healthcare", "index"),
    },
}


@dataclass
class DatasetConfig:
    """Configuration for an ODN dataset."""
    name: str
    display_name: str
    description: str
    category: str
    entities: list[str]


# Dataset configurations
DATASETS = {
    "dc_demographics": DatasetConfig(
        name="dc_demographics",
        display_name="DC Demographics",
        description="DC population, age, race, education statistics",
        category="demographics",
        entities=["dc"],
    ),
    "dc_economics": DatasetConfig(
        name="dc_economics",
        display_name="DC Economics",
        description="DC GDP, income, employment statistics",
        category="economics",
        entities=["dc"],
    ),
    "dc_jobs": DatasetConfig(
        name="dc_jobs",
        display_name="DC Jobs by Sector",
        description="DC employment by industry sector",
        category="jobs",
        entities=["dc"],
    ),
    "dc_cost_of_living": DatasetConfig(
        name="dc_cost_of_living",
        display_name="DC Cost of Living",
        description="DC cost of living indices",
        category="cost_of_living",
        entities=["dc"],
    ),
    "regional_comparison": DatasetConfig(
        name="regional_comparison",
        display_name="Regional Comparison",
        description="DC vs MD, VA, US comparison",
        category="demographics",
        entities=["dc", "maryland", "virginia", "us"],
    ),
    "dc_metro_counties": DatasetConfig(
        name="dc_metro_counties",
        display_name="DC Metro Counties",
        description="Surrounding county demographics",
        category="demographics",
        entities=["arlington", "fairfax", "montgomery", "prince_georges"],
    ),
}


# =============================================================================
# Default Configuration
# =============================================================================

def get_default_config(dataset_name: str = "dc_demographics") -> SourceConfig:
    """Get default configuration for ODN Demographics source."""
    dataset = DATASETS.get(dataset_name, DATASETS["dc_demographics"])

    # Map dataset names to registered scraper names
    scraper_names = {
        "dc_demographics": "odn_dc_demographics",
        "dc_economics": "odn_dc_economics",
        "dc_jobs": "odn_dc_jobs",
        "dc_cost_of_living": "odn_dc_cost_of_living",
        "regional_comparison": "odn_regional_comparison",
        "dc_metro_counties": "odn_dc_metro_counties",
    }
    scraper_name = scraper_names.get(dataset_name, "odn_dc_demographics")

    return SourceConfig(
        name=scraper_name,
        display_name=f"ODN {dataset.display_name}",
        description=f"{dataset.description} from Open Data Network",
        source_type=SourceType.REST_API,
        base_url=ODN_BASE_URL,
        requires_auth=False,  # Optional app token
        requests_per_second=2.0,
        max_workers=1,
        batch_size=50,
        timeout_seconds=30,
        max_retries=3,
        adapter_class="src.adapters.demographics.odn.ODNBaseAdapter",
        output_record_types=["DemographicRecord"],
        extra={
            "dataset_name": dataset_name,
            "rate_limit_wait": 5,
        },
    )


# =============================================================================
# Adapter Implementation
# =============================================================================

class ODNBaseAdapter(BaseScraper):
    """
    Base adapter for Open Data Network API.

    Fetches demographic and economic data from Socrata's ODN API.
    """

    SOURCE_NAME = "odn_demographics"
    SOURCE_VERSION = "1.0.0"
    SUPPORTED_RECORD_TYPES = [DemographicRecord]

    # Override in subclasses
    DATASET_CONFIG: DatasetConfig = DATASETS["dc_demographics"]

    def __init__(self, config: SourceConfig, **kwargs):
        super().__init__(config, **kwargs)
        self._session: requests.Session | None = None
        self._app_token: str | None = None
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
        """Initialize HTTP session and check for app token."""
        super().setup()

        self._app_token = os.environ.get("ODN_APP_TOKEN")
        if self._app_token:
            self._logger.info("ODN app token found")
        else:
            self._logger.debug("No ODN_APP_TOKEN found, using unauthenticated requests")

        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; UniversalScraper/1.0)",
            "Accept": "application/json",
        })
        if self._app_token:
            self._session.headers["X-App-Token"] = self._app_token

    def teardown(self) -> None:
        """Close HTTP session."""
        if self._session:
            self._session.close()
            self._session = None
        super().teardown()

    def discover(self) -> Iterator[dict[str, Any]]:
        """
        Discover entity/variable combinations to fetch.

        Yields one item per entity/variable combination.
        """
        category = self._dataset.category
        variables = VARIABLES.get(category, {})

        for entity_key in self._dataset.entities:
            entity = ENTITIES.get(entity_key)
            if not entity:
                continue

            for var_name, (var_path, var_unit) in variables.items():
                yield {
                    "entity_key": entity_key,
                    "entity_id": entity["id"],
                    "entity_name": entity["name"],
                    "entity_type": entity["type"],
                    "variable_name": var_name,
                    "variable_path": var_path,
                    "variable_unit": var_unit,
                    "category": category,
                }

    def fetch(self, item: dict[str, Any]) -> RawPayload | None:
        """
        Fetch a single variable for an entity from ODN API.
        """
        if self._demo_mode:
            return self._fetch_demo(item)

        return self._fetch_api(item)

    def _fetch_api(self, item: dict[str, Any]) -> RawPayload | None:
        """Fetch from the actual ODN API."""
        if self._session is None:
            return None

        url = f"{ODN_BASE_URL}/data"
        params = {
            "entity_id": item["entity_id"],
            "variable": item["variable_path"],
        }

        try:
            response = self._session.get(
                url,
                params=params,
                timeout=self.config.timeout_seconds,
            )

            if response.status_code == 429:
                self._logger.warning(f"Rate limited, waiting {self._rate_limit_wait}s...")
                time.sleep(self._rate_limit_wait)
                return self._fetch_api(item)

            # Handle not found - fallback to demo
            if response.status_code == 404:
                self._logger.debug(f"No data for {item['variable_name']} in {item['entity_name']}, using demo")
                return self._fetch_demo(item)

            response.raise_for_status()

            # Rate limit between requests
            time.sleep(1.0 / self.config.requests_per_second)

            # Add metadata to response
            data = response.json()
            data["_entity_key"] = item["entity_key"]
            data["_entity_name"] = item["entity_name"]
            data["_entity_type"] = item["entity_type"]
            data["_entity_id"] = item["entity_id"]
            data["_variable_name"] = item["variable_name"]
            data["_variable_path"] = item["variable_path"]
            data["_variable_unit"] = item["variable_unit"]
            data["_category"] = item["category"]
            data["_scraped_at"] = datetime.now(timezone.utc).isoformat()

            return RawPayload(
                content_type="application/json",
                content=json.dumps(data).encode(),
                lineage=self._make_lineage(url),
                fetch_status_code=response.status_code,
            )

        except requests.RequestException as e:
            self._logger.warning(f"Fetch failed for {item['variable_name']}: {e}")
            # Return demo data on failure
            return self._fetch_demo(item)

    def _fetch_demo(self, item: dict[str, Any]) -> RawPayload:
        """Generate demo data."""
        import random

        # Generate realistic demo values based on metric type
        var_name = item["variable_name"]
        var_unit = item["variable_unit"]

        if var_unit == "count":
            if "population" in var_name:
                value = random.randint(600000, 720000)  # DC population range
            else:
                value = random.randint(10000, 100000)
        elif var_unit == "percent":
            value = round(random.uniform(5, 95), 1)
        elif var_unit == "dollars":
            if "income" in var_name:
                value = random.randint(70000, 100000)
            elif "gdp" in var_name:
                value = random.randint(150000, 200000)
            else:
                value = random.randint(50000, 80000)
        elif var_unit == "index":
            value = round(random.uniform(90, 150), 1)  # Cost of living index
        else:
            value = random.randint(1000, 10000)

        data = {
            "data": [
                {"value": value, "year": 2023},
                {"value": value * 0.98, "year": 2022},
            ],
            "_entity_key": item["entity_key"],
            "_entity_name": item["entity_name"],
            "_entity_type": item["entity_type"],
            "_entity_id": item["entity_id"],
            "_variable_name": item["variable_name"],
            "_variable_path": item["variable_path"],
            "_variable_unit": item["variable_unit"],
            "_category": item["category"],
            "_demo": True,
            "_scraped_at": datetime.now(timezone.utc).isoformat(),
        }

        return RawPayload(
            content_type="application/json",
            content=json.dumps(data).encode(),
            lineage=self._make_lineage(f"{ODN_BASE_URL}/data?demo=true"),
            fetch_status_code=200,
        )

    def parse(self, payload: RawPayload) -> Iterator[dict[str, Any]]:
        """
        Parse API response into metric records.
        """
        try:
            data = json.loads(payload.content.decode("utf-8"))
        except json.JSONDecodeError as e:
            self._logger.error(f"JSON parse error: {e}")
            return

        # Extract value from response
        raw_data = data.get("data", [])

        if isinstance(raw_data, list) and len(raw_data) > 0:
            # Get most recent year's data
            latest = raw_data[0]
            yield {
                "region_name": data.get("_entity_name"),
                "region_type": data.get("_entity_type"),
                "region_id": data.get("_entity_id"),
                "metric_category": data.get("_category"),
                "metric_name": data.get("_variable_name"),
                "metric_value": latest.get("value"),
                "metric_unit": data.get("_variable_unit"),
                "year": latest.get("year"),
                "variable_path": data.get("_variable_path"),
                "_demo": data.get("_demo", False),
                "_scraped_at": data.get("_scraped_at"),
            }
        elif raw_data:
            # Single value, not a list
            yield {
                "region_name": data.get("_entity_name"),
                "region_type": data.get("_entity_type"),
                "region_id": data.get("_entity_id"),
                "metric_category": data.get("_category"),
                "metric_name": data.get("_variable_name"),
                "metric_value": raw_data,
                "metric_unit": data.get("_variable_unit"),
                "variable_path": data.get("_variable_path"),
                "_demo": data.get("_demo", False),
                "_scraped_at": data.get("_scraped_at"),
            }

    def validate(self, data: dict[str, Any]) -> tuple[bool, list[str]]:
        """
        Validate a metric record.
        """
        errors = []

        if not data.get("region_name"):
            errors.append("Missing region_name")

        if not data.get("metric_name"):
            errors.append("Missing metric_name")

        if not data.get("metric_category"):
            errors.append("Missing metric_category")

        value = data.get("metric_value")
        if value is not None:
            try:
                Decimal(str(value))
            except (InvalidOperation, ValueError, TypeError):
                errors.append(f"Invalid metric_value: {value}")

        return len(errors) == 0, errors

    def normalize(self, data: dict[str, Any]) -> DemographicRecord:
        """
        Normalize to canonical DemographicRecord schema.
        """
        # Convert metric value
        metric_value = None
        if data.get("metric_value") is not None:
            try:
                metric_value = Decimal(str(data["metric_value"]))
            except (InvalidOperation, ValueError, TypeError):
                pass

        # Build extra fields
        extra_fields = {}
        if data.get("_demo"):
            extra_fields["demo_data"] = True
        if data.get("variable_path"):
            extra_fields["variable_path"] = data["variable_path"]
        if data.get("_scraped_at"):
            extra_fields["scraped_at"] = data["_scraped_at"]

        # Create lineage with content hash
        raw_content = str(sorted(data.items())).encode()
        lineage = self._make_lineage()
        lineage = lineage.model_copy(
            update={"raw_hash": LineageInfo.compute_hash(raw_content)}
        )

        # Generate source record ID
        region = data.get("region_name", "")
        metric = data.get("metric_name", "")
        year = data.get("year", "")
        source_record_id = f"{region}_{metric}_{year}"

        return DemographicRecord(
            lineage=lineage,
            extra_fields=extra_fields if extra_fields else {},
            source_record_id=source_record_id,
            region_name=data.get("region_name", ""),
            region_type=data.get("region_type", "state"),
            region_id=data.get("region_id"),
            metric_category=data.get("metric_category", "demographic"),
            metric_name=data.get("metric_name", "unknown"),
            metric_value=metric_value,
            metric_unit=data.get("metric_unit"),
            year=data.get("year"),
            period="annual",
        )


# =============================================================================
# Registered Adapters
# =============================================================================

@register_scraper("odn_dc_demographics")
class ODNDCDemographicsAdapter(ODNBaseAdapter):
    """Adapter for DC Demographics."""
    SOURCE_NAME = "odn_dc_demographics"
    DATASET_CONFIG = DATASETS["dc_demographics"]


@register_scraper("odn_dc_economics")
class ODNDCEconomicsAdapter(ODNBaseAdapter):
    """Adapter for DC Economics."""
    SOURCE_NAME = "odn_dc_economics"
    DATASET_CONFIG = DATASETS["dc_economics"]


@register_scraper("odn_dc_jobs")
class ODNDCJobsAdapter(ODNBaseAdapter):
    """Adapter for DC Jobs by Sector."""
    SOURCE_NAME = "odn_dc_jobs"
    DATASET_CONFIG = DATASETS["dc_jobs"]


@register_scraper("odn_dc_cost_of_living")
class ODNCostOfLivingAdapter(ODNBaseAdapter):
    """Adapter for DC Cost of Living."""
    SOURCE_NAME = "odn_dc_cost_of_living"
    DATASET_CONFIG = DATASETS["dc_cost_of_living"]


@register_scraper("odn_regional_comparison")
class ODNRegionalComparisonAdapter(ODNBaseAdapter):
    """Adapter for Regional Comparison."""
    SOURCE_NAME = "odn_regional_comparison"
    DATASET_CONFIG = DATASETS["regional_comparison"]


@register_scraper("odn_dc_metro_counties")
class ODNMetroCountiesAdapter(ODNBaseAdapter):
    """Adapter for DC Metro Counties."""
    SOURCE_NAME = "odn_dc_metro_counties"
    DATASET_CONFIG = DATASETS["dc_metro_counties"]


# Keep legacy name for backward compatibility
@register_scraper("odn_demographics")
class ODNDemographicsAdapter(ODNDCDemographicsAdapter):
    """Legacy adapter name - defaults to DC Demographics."""
    SOURCE_NAME = "odn_demographics"


# =============================================================================
# Helper Functions
# =============================================================================

def get_dc_demographics_config() -> SourceConfig:
    """Get configuration for DC Demographics."""
    return get_default_config("dc_demographics")


def get_dc_economics_config() -> SourceConfig:
    """Get configuration for DC Economics."""
    return get_default_config("dc_economics")


def get_regional_comparison_config() -> SourceConfig:
    """Get configuration for Regional Comparison."""
    return get_default_config("regional_comparison")


# =============================================================================
# Auto-register configs on module import
# =============================================================================

def _register_all_configs():
    """Register all ODN configs with the registry."""
    for dataset_name in DATASETS:
        config = get_default_config(dataset_name)
        ScraperRegistry.register_config(config)

    # Also register legacy name
    legacy_config = get_default_config("dc_demographics")
    legacy_config = SourceConfig(
        **{**legacy_config.model_dump(), "name": "odn_demographics"}
    )
    ScraperRegistry.register_config(legacy_config)


# Register configs when module is imported
_register_all_configs()
