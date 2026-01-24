"""
Zillow Research adapter for public CSV datasets.

Downloads public CSV datasets from Zillow Research and filters for DC metro area:
- ZORI (Zillow Observed Rent Index)
- ZHVI (Zillow Home Value Index)
- Market Heat Index
- For-Sale Inventory
- Sales Count
- Home Value Forecasts

No authentication required - uses public Zillow Research CSV files.
"""

from __future__ import annotations

import io
import json
import logging
import random
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

# Check for pandas
try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    pd = None


# =============================================================================
# Configuration
# =============================================================================

ZILLOW_BASE_URL = "https://files.zillowstatic.com/research/public_csvs"

# DC Metro Area filters
DC_METRO_NAMES = ["Washington", "Arlington", "Alexandria", "Baltimore"]
DC_ZIP_PREFIXES = ["200", "201", "202", "207", "208", "209", "220", "221", "222", "223"]


@dataclass
class DatasetConfig:
    """Configuration for a Zillow dataset."""
    name: str
    display_name: str
    description: str
    url: str
    level: str  # "metro" or "zip"
    metric_name: str


# Dataset configurations
DATASETS = {
    "zori_metro": DatasetConfig(
        name="zori_metro",
        display_name="ZORI Metro Rent Index",
        description="Zillow Observed Rent Index by Metro Area",
        url=f"{ZILLOW_BASE_URL}/zori/Metro_zori_uc_sfrcondomfr_sm_month.csv",
        level="metro",
        metric_name="zori",
    ),
    "zori_zip": DatasetConfig(
        name="zori_zip",
        display_name="ZORI ZIP Rent Index",
        description="Zillow Observed Rent Index by ZIP Code",
        url=f"{ZILLOW_BASE_URL}/zori/Zip_zori_uc_sfrcondomfr_sm_month.csv",
        level="zip",
        metric_name="zori",
    ),
    "zhvi_metro": DatasetConfig(
        name="zhvi_metro",
        display_name="ZHVI Metro Home Values",
        description="Zillow Home Value Index by Metro Area",
        url=f"{ZILLOW_BASE_URL}/zhvi/Metro_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv",
        level="metro",
        metric_name="zhvi",
    ),
    "zhvi_zip": DatasetConfig(
        name="zhvi_zip",
        display_name="ZHVI ZIP Home Values",
        description="Zillow Home Value Index by ZIP Code",
        url=f"{ZILLOW_BASE_URL}/zhvi/Zip_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv",
        level="zip",
        metric_name="zhvi",
    ),
    "market_heat": DatasetConfig(
        name="market_heat",
        display_name="Market Heat Index",
        description="Market Temperature Index (0-100, higher = seller's market)",
        url=f"{ZILLOW_BASE_URL}/market_temp_index/Metro_market_temp_index_uc_sfrcondo_month.csv",
        level="metro",
        metric_name="market_heat",
    ),
    "inventory": DatasetConfig(
        name="inventory",
        display_name="For-Sale Inventory",
        description="For-Sale Listing Inventory by Metro",
        url=f"{ZILLOW_BASE_URL}/invt_fs/Metro_invt_fs_uc_sfrcondo_sm_month.csv",
        level="metro",
        metric_name="inventory",
    ),
    "sales_count": DatasetConfig(
        name="sales_count",
        display_name="Sales Count",
        description="Estimated Monthly Sales Count by Metro",
        url=f"{ZILLOW_BASE_URL}/sales_count_now/Metro_sales_count_now_uc_sfrcondo_month.csv",
        level="metro",
        metric_name="sales_count",
    ),
    "forecast": DatasetConfig(
        name="forecast",
        display_name="Home Value Forecast",
        description="Zillow Home Value Forecast (% growth)",
        url=f"{ZILLOW_BASE_URL}/zhvf_growth/Metro_zhvf_growth_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv",
        level="metro",
        metric_name="zhvf_growth",
    ),
}


# =============================================================================
# Default Configuration
# =============================================================================

def get_default_config(dataset_name: str = "zori_metro") -> SourceConfig:
    """Get default configuration for Zillow Research source."""
    dataset = DATASETS.get(dataset_name, DATASETS["zori_metro"])

    # Map dataset names to registered scraper names
    scraper_names = {
        "zori_metro": "zillow_zori_metro",
        "zori_zip": "zillow_zori_zip",
        "zhvi_metro": "zillow_zhvi_metro",
        "zhvi_zip": "zillow_zhvi_zip",
        "market_heat": "zillow_market_heat",
        "inventory": "zillow_inventory",
        "sales_count": "zillow_sales_count",
        "forecast": "zillow_forecast",
    }
    scraper_name = scraper_names.get(dataset_name, "zillow_zori_metro")

    return SourceConfig(
        name=scraper_name,
        display_name=f"Zillow {dataset.display_name}",
        description=f"DC area {dataset.description.lower()} from Zillow Research",
        source_type=SourceType.CSV_DOWNLOAD,
        base_url=ZILLOW_BASE_URL,
        requires_auth=False,
        requests_per_second=1.0,
        max_workers=1,
        batch_size=100,
        timeout_seconds=60,
        max_retries=3,
        adapter_class="src.adapters.real_estate.zillow.ZillowBaseAdapter",
        output_record_types=["MarketStatsRecord"],
        extra={
            "dataset_name": dataset_name,
            "filter_dc": True,
            "months_to_extract": 12,  # Extract last 12 months of data
        },
    )


# =============================================================================
# Adapter Implementation
# =============================================================================

class ZillowBaseAdapter(BaseScraper):
    """
    Base adapter for Zillow Research public datasets.

    Downloads CSV files, filters for DC area, and converts to MarketStatsRecord.
    """

    SOURCE_NAME = "zillow_research"
    SOURCE_VERSION = "1.0.0"
    SUPPORTED_RECORD_TYPES = [MarketStatsRecord]

    # Override in subclasses
    DATASET_CONFIG: DatasetConfig = DATASETS["zori_metro"]

    def __init__(self, config: SourceConfig, **kwargs):
        super().__init__(config, **kwargs)
        self._session: requests.Session | None = None
        self._demo_mode: bool = False

    @property
    def _dataset_name(self) -> str:
        """Get the dataset name from config."""
        return self.config.extra.get("dataset_name", self.DATASET_CONFIG.name)

    @property
    def _dataset(self) -> DatasetConfig:
        """Get the dataset configuration."""
        return DATASETS.get(self._dataset_name, self.DATASET_CONFIG)

    def setup(self) -> None:
        """Initialize HTTP session."""
        super().setup()

        if not PANDAS_AVAILABLE:
            self._logger.warning("pandas not available. Running in demo mode.")
            self._demo_mode = True

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
        Discover the dataset to download.

        Yields one item for the CSV download.
        """
        yield {
            "url": self._dataset.url,
            "dataset": self._dataset_name,
            "level": self._dataset.level,
            "metric_name": self._dataset.metric_name,
        }

    def fetch(self, item: dict[str, Any]) -> RawPayload | None:
        """
        Fetch CSV from Zillow Research or generate demo data.
        """
        if self._demo_mode:
            return self._fetch_demo(item)

        return self._fetch_csv(item)

    def _fetch_csv(self, item: dict[str, Any]) -> RawPayload | None:
        """Download CSV from Zillow Research."""
        if self._session is None:
            return None

        url = item["url"]

        try:
            self._logger.info(f"Downloading from {url}")
            response = self._session.get(url, timeout=self.config.timeout_seconds)
            response.raise_for_status()

            return RawPayload(
                content_type="text/csv",
                content=response.content,
                lineage=self._make_lineage(url),
                fetch_status_code=response.status_code,
            )

        except requests.RequestException as e:
            self._logger.error(f"Fetch failed: {e}")
            return None

    def _fetch_demo(self, item: dict[str, Any]) -> RawPayload:
        """Generate demo data when pandas is not available."""
        level = item["level"]
        metric_name = item["metric_name"]

        # Generate demo CSV-like data
        demo_records = []

        if level == "metro":
            regions = [
                ("Washington-Arlington-Alexandria, DC-VA-MD-WV", "DC"),
                ("Baltimore-Columbia-Towson, MD", "MD"),
            ]
        else:
            regions = [
                ("20001", "DC"), ("20002", "DC"), ("20003", "DC"),
                ("22201", "VA"), ("22202", "VA"),
                ("20814", "MD"), ("20815", "MD"),
            ]

        # Generate values for last 12 months
        for region_name, state in regions:
            base_value = 2000 if metric_name in ["zori"] else 500000
            if metric_name == "market_heat":
                base_value = 60
            elif metric_name in ["inventory", "sales_count"]:
                base_value = 1000

            record = {
                "RegionName": region_name,
                "State": state,
                "RegionType": level,
                "_demo": True,
            }

            # Add monthly values
            for i in range(12):
                month = 12 - i
                year = 2024 if month <= 12 else 2023
                date_col = f"{year}-{month:02d}-01"
                variation = random.uniform(-0.05, 0.05)
                record[date_col] = round(base_value * (1 + variation))

            demo_records.append(record)

        # Convert to JSON for demo
        data = {
            "records": demo_records,
            "_dataset": item["dataset"],
            "_level": level,
            "_metric_name": metric_name,
            "_demo": True,
            "_scraped_at": datetime.now(timezone.utc).isoformat(),
        }

        return RawPayload(
            content_type="application/json",
            content=json.dumps(data).encode(),
            lineage=self._make_lineage(f"{item['url']}?demo=true"),
            fetch_status_code=200,
        )

    def parse(self, payload: RawPayload) -> Iterator[dict[str, Any]]:
        """
        Parse CSV or demo data into metric records.

        Converts wide format (date columns) to long format (one record per date).
        """
        if payload.content_type == "application/json":
            # Demo mode
            yield from self._parse_demo(payload)
        else:
            # Real CSV
            yield from self._parse_csv(payload)

    def _parse_csv(self, payload: RawPayload) -> Iterator[dict[str, Any]]:
        """Parse real CSV data from Zillow."""
        if not PANDAS_AVAILABLE:
            return

        try:
            df = pd.read_csv(io.BytesIO(payload.content))
        except Exception as e:
            self._logger.error(f"CSV parse error: {e}")
            return

        original_count = len(df)
        self._logger.info(f"Downloaded {original_count} rows")

        # Filter for DC area
        filter_dc = self.config.extra.get("filter_dc", True)
        if filter_dc:
            df = self._filter_dc_area(df)
            self._logger.info(f"Filtered to {len(df)} DC-area rows")

        if df.empty:
            return

        # Find date columns (format: YYYY-MM-DD or YYYY-MM)
        date_cols = [col for col in df.columns if col[:4].isdigit()]
        if not date_cols:
            self._logger.warning("No date columns found in CSV")
            return

        # Sort and limit to recent months
        date_cols = sorted(date_cols, reverse=True)
        months_to_extract = self.config.extra.get("months_to_extract", 12)
        date_cols = date_cols[:months_to_extract]

        # Convert wide to long format
        for _, row in df.iterrows():
            region_name = str(row.get("RegionName", ""))
            state = row.get("State") or row.get("StateName")
            region_type = self._dataset.level

            for date_col in date_cols:
                value = row.get(date_col)
                if pd.isna(value):
                    continue

                yield {
                    "region_name": region_name,
                    "state": state,
                    "region_type": region_type,
                    "metric_name": self._dataset.metric_name,
                    "metric_value": value,
                    "metric_date": date_col,
                    "zip_code": region_name if region_type == "zip" else None,
                    "_demo": False,
                }

    def _parse_demo(self, payload: RawPayload) -> Iterator[dict[str, Any]]:
        """Parse demo data."""
        try:
            data = json.loads(payload.content.decode("utf-8"))
        except json.JSONDecodeError:
            return

        records = data.get("records", [])
        metric_name = data.get("_metric_name", self._dataset.metric_name)
        level = data.get("_level", self._dataset.level)

        for record in records:
            region_name = record.get("RegionName", "")
            state = record.get("State")

            # Find date columns
            date_cols = [k for k in record.keys() if k[:4].isdigit()]
            date_cols = sorted(date_cols, reverse=True)

            months_to_extract = self.config.extra.get("months_to_extract", 12)
            for date_col in date_cols[:months_to_extract]:
                value = record.get(date_col)
                if value is None:
                    continue

                yield {
                    "region_name": region_name,
                    "state": state,
                    "region_type": level,
                    "metric_name": metric_name,
                    "metric_value": value,
                    "metric_date": date_col,
                    "zip_code": region_name if level == "zip" else None,
                    "_demo": True,
                }

    def _filter_dc_area(self, df) -> Any:
        """Filter DataFrame for DC metro area."""
        if not PANDAS_AVAILABLE:
            return df

        level = self._dataset.level
        original_count = len(df)

        if level == "metro":
            if "RegionName" in df.columns:
                mask = df["RegionName"].str.contains(
                    "|".join(DC_METRO_NAMES),
                    case=False,
                    na=False
                )
                df = df[mask]

        elif level == "zip":
            if "RegionName" in df.columns:
                df["RegionName"] = df["RegionName"].astype(str).str.zfill(5)
                mask = df["RegionName"].str[:3].isin(DC_ZIP_PREFIXES)
                df = df[mask]

        self._logger.debug(f"Filtered {original_count} -> {len(df)} records")
        return df

    def validate(self, data: dict[str, Any]) -> tuple[bool, list[str]]:
        """
        Validate a metric record.
        """
        errors = []

        if not data.get("region_name"):
            errors.append("Missing region_name")

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

        # Parse metric date
        metric_date = None
        date_str = data.get("metric_date")
        if date_str:
            try:
                # Handle both YYYY-MM-DD and YYYY-MM formats
                if len(date_str) == 7:
                    date_str = f"{date_str}-01"
                metric_date = date.fromisoformat(date_str[:10])
            except ValueError:
                pass

        # Build extra fields
        extra_fields = {}
        if data.get("_demo"):
            extra_fields["demo_data"] = True

        # Create lineage with content hash
        raw_content = str(sorted(data.items())).encode()
        lineage = self._make_lineage()
        lineage = lineage.model_copy(
            update={"raw_hash": LineageInfo.compute_hash(raw_content)}
        )

        # Generate source record ID
        region = data.get("region_name", "")
        metric = data.get("metric_name", "")
        date_part = data.get("metric_date", "")[:7] if data.get("metric_date") else ""
        source_record_id = f"{region}_{metric}_{date_part}"

        return MarketStatsRecord(
            lineage=lineage,
            extra_fields=extra_fields if extra_fields else {},
            source_record_id=source_record_id,
            region_name=data.get("region_name"),
            region_type=data.get("region_type"),
            state=data.get("state"),
            zip_code=data.get("zip_code"),
            metric_name=data.get("metric_name", "unknown"),
            metric_value=metric_value,
            metric_date=metric_date,
            metric_period="monthly",
            # Provide non-empty dict to avoid Parquet empty struct error
            historical_values={"_source": "zillow"},
        )


# =============================================================================
# Registered Adapters
# =============================================================================

@register_scraper("zillow_zori_metro")
class ZillowZoriMetroAdapter(ZillowBaseAdapter):
    """Adapter for Zillow ZORI Metro Rent Index."""
    SOURCE_NAME = "zillow_zori_metro"
    DATASET_CONFIG = DATASETS["zori_metro"]


@register_scraper("zillow_zori_zip")
class ZillowZoriZipAdapter(ZillowBaseAdapter):
    """Adapter for Zillow ZORI ZIP Rent Index."""
    SOURCE_NAME = "zillow_zori_zip"
    DATASET_CONFIG = DATASETS["zori_zip"]


@register_scraper("zillow_zhvi_metro")
class ZillowZhviMetroAdapter(ZillowBaseAdapter):
    """Adapter for Zillow ZHVI Metro Home Values."""
    SOURCE_NAME = "zillow_zhvi_metro"
    DATASET_CONFIG = DATASETS["zhvi_metro"]


@register_scraper("zillow_zhvi_zip")
class ZillowZhviZipAdapter(ZillowBaseAdapter):
    """Adapter for Zillow ZHVI ZIP Home Values."""
    SOURCE_NAME = "zillow_zhvi_zip"
    DATASET_CONFIG = DATASETS["zhvi_zip"]


@register_scraper("zillow_market_heat")
class ZillowMarketHeatAdapter(ZillowBaseAdapter):
    """Adapter for Zillow Market Heat Index."""
    SOURCE_NAME = "zillow_market_heat"
    DATASET_CONFIG = DATASETS["market_heat"]


@register_scraper("zillow_inventory")
class ZillowInventoryAdapter(ZillowBaseAdapter):
    """Adapter for Zillow For-Sale Inventory."""
    SOURCE_NAME = "zillow_inventory"
    DATASET_CONFIG = DATASETS["inventory"]


@register_scraper("zillow_sales_count")
class ZillowSalesCountAdapter(ZillowBaseAdapter):
    """Adapter for Zillow Sales Count."""
    SOURCE_NAME = "zillow_sales_count"
    DATASET_CONFIG = DATASETS["sales_count"]


@register_scraper("zillow_forecast")
class ZillowForecastAdapter(ZillowBaseAdapter):
    """Adapter for Zillow Home Value Forecast."""
    SOURCE_NAME = "zillow_forecast"
    DATASET_CONFIG = DATASETS["forecast"]


# Keep legacy name for backward compatibility
@register_scraper("zillow_research")
class ZillowResearchAdapter(ZillowZoriMetroAdapter):
    """Legacy adapter name - defaults to ZORI Metro."""
    SOURCE_NAME = "zillow_research"


# =============================================================================
# Helper Functions
# =============================================================================

def get_zori_metro_config() -> SourceConfig:
    """Get configuration for ZORI Metro."""
    return get_default_config("zori_metro")


def get_zori_zip_config() -> SourceConfig:
    """Get configuration for ZORI ZIP."""
    return get_default_config("zori_zip")


def get_zhvi_metro_config() -> SourceConfig:
    """Get configuration for ZHVI Metro."""
    return get_default_config("zhvi_metro")


def get_zhvi_zip_config() -> SourceConfig:
    """Get configuration for ZHVI ZIP."""
    return get_default_config("zhvi_zip")


# =============================================================================
# Auto-register configs on module import
# =============================================================================

def _register_all_configs():
    """Register all Zillow configs with the registry."""
    for dataset_name in DATASETS:
        config = get_default_config(dataset_name)
        ScraperRegistry.register_config(config)

    # Also register legacy name
    legacy_config = get_default_config("zori_metro")
    legacy_config = SourceConfig(
        **{**legacy_config.model_dump(), "name": "zillow_research"}
    )
    ScraperRegistry.register_config(legacy_config)


# Register configs when module is imported
_register_all_configs()
