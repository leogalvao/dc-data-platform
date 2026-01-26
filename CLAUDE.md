# CLAUDE.md - DC Data Platform

This file provides guidance for AI assistants working with this codebase.

## Project Overview

DC Data Platform is a unified data platform for DC government procurement, property, and demographic data. It consolidates scraping, transformation, and warehouse loading into a single cohesive project following a lakehouse architecture pattern.

**Tech Stack:**
- Python 3.11+ (required)
- Pydantic v2 for data validation and settings
- PyArrow/Polars for data processing
- PostgreSQL for the analytics warehouse
- Typer/Rich for CLI interfaces

## Architecture

```
Sources → Bronze → Silver → Gold → Diamond → Warehouse
   ↓        ↓        ↓        ↓        ↓          ↓
  APIs    Raw     Clean   Aggregated  ML-ready  PostgreSQL
         JSON    Parquet  Parquet    Features   Analytics
```

### Directory Structure

```
dc-data-platform/
├── src/                      # Core scraping and utilities
│   ├── adapters/             # Source-specific scrapers
│   │   ├── dc_arcgis/        # DC government ArcGIS APIs
│   │   ├── dc_contracts/     # Contract attachment scraper
│   │   ├── demographics/     # Census and ODN data
│   │   └── real_estate/      # Zillow, Realtor, RentCast
│   ├── core/                 # Orchestrator, rate limiter, registry
│   │   ├── base_scraper.py   # BaseScraper abstract class
│   │   ├── orchestrator.py   # Multi-source orchestration
│   │   ├── rate_limiter.py   # Rate limiting utilities
│   │   └── registry.py       # Scraper discovery/registration
│   ├── schemas/              # Pydantic data models
│   │   ├── base.py           # BaseRecord, LineageInfo, RawPayload
│   │   ├── tabular.py        # ContractRecord, PaymentRecord, etc.
│   │   ├── textual.py        # OCR/text processing schemas
│   │   └── metadata.py       # SourceConfig, RunMetadata, etc.
│   ├── storage/              # Lakehouse writers
│   │   ├── writer.py         # UnifiedStorageWriter
│   │   ├── bronze.py         # Raw data storage
│   │   ├── silver.py         # Cleaned data storage
│   │   ├── gold.py           # Aggregated data storage
│   │   └── diamond.py        # ML features storage
│   ├── transformers/         # Data transformation logic
│   ├── observability/        # Metrics, logging, reports
│   └── utils/                # HTTP client, hashing, type coercion
├── warehouse/                # PostgreSQL warehouse layer
│   ├── loaders/              # Dimension and fact table loaders
│   ├── api.py                # FastAPI dashboard API
│   ├── cli.py                # Warehouse CLI commands
│   └── config.py             # Database configuration
├── pipelines/                # Pipeline orchestration
│   ├── definitions/          # YAML pipeline definitions
│   ├── tasks/                # Task implementations
│   ├── adapters/             # Pipeline adapters (local, etc.)
│   ├── base.py               # Pipeline base classes
│   └── runner.py             # Pipeline execution engine
├── config/                   # Configuration files
│   ├── settings.py           # Pydantic settings class
│   ├── sources.yaml          # Source configurations
│   └── storage.yaml          # Storage configurations
├── contracts/                # Data contracts and SLAs
├── metrics/                  # Data quality metrics
├── tests/                    # Test suite
├── scripts/                  # CLI scripts
│   └── unified_pipeline.py   # Main unified pipeline script
├── cli.py                    # Factory CLI entry point
└── pyproject.toml            # Project configuration
```

## Key Patterns and Conventions

### 1. Scraper Adapter Pattern

All scrapers implement `BaseScraper` from `src/core/base_scraper.py`:

```python
from src.core.base_scraper import BaseScraper
from src.core.registry import register_scraper
from src.schemas.tabular import ContractRecord

@register_scraper("my_source")
class MySourceAdapter(BaseScraper):
    SOURCE_NAME = "my_source"
    SOURCE_VERSION = "1.0.0"
    SUPPORTED_RECORD_TYPES = [ContractRecord]

    def discover(self) -> Iterator[dict[str, Any]]:
        """Yield items to scrape (URLs, IDs, etc.)"""
        ...

    def fetch(self, item: dict[str, Any]) -> RawPayload | None:
        """Fetch raw data for a discovered item"""
        ...

    def parse(self, payload: RawPayload) -> Iterator[dict[str, Any]]:
        """Parse raw payload into record dicts"""
        ...

    def validate(self, data: dict[str, Any]) -> tuple[bool, list[str]]:
        """Validate a parsed record. Return (is_valid, errors)"""
        ...

    def normalize(self, data: dict[str, Any]) -> BaseRecord:
        """Normalize to canonical Pydantic model"""
        ...
```

### 2. Registry Pattern

Scrapers self-register using the `@register_scraper` decorator. The orchestrator discovers them via `ScraperRegistry`:

```python
from src.core.registry import ScraperRegistry

# List available scrapers
ScraperRegistry.list_scrapers()

# Create a scraper instance
scraper = ScraperRegistry.create("dc_contracts")
```

### 3. Lineage Tracking

Every record has a `LineageInfo` object for full traceability:

```python
lineage = LineageInfo(
    source_name="dc_contracts",
    source_url="https://api.example.com/data",
    run_id=uuid4(),
    raw_hash=LineageInfo.compute_hash(content),
    schema_version="1.0.0",
    adapter_version="1.0.0",
)
```

### 4. Pydantic Models

All data models use Pydantic v2 with strict validation:

- `BaseRecord` - Base for all normalized records
- `RawPayload` - Bronze layer raw data container
- `QuarantineRecord` - Failed validation records
- `SourceConfig` - Source configuration
- `RunMetadata` - Execution metadata

### 5. Settings Pattern

Configuration uses Pydantic Settings with environment variables:

```python
from config.settings import Settings

settings = Settings()  # Loads from .env with DC_ prefix
print(settings.silver_path)  # data/silver
print(settings.database_url)  # postgresql://...
```

Environment variables use `DC_` prefix (e.g., `DC_WAREHOUSE_DB_HOST`).

## Development Workflow

### Running the Pipeline

```bash
# Full pipeline (scrape → transform → load)
python scripts/unified_pipeline.py

# Dry run
python scripts/unified_pipeline.py --dry-run

# Individual stages
python scripts/unified_pipeline.py --stage scrape
python scripts/unified_pipeline.py --stage transform
python scripts/unified_pipeline.py --stage load

# Specific sources
python scripts/unified_pipeline.py --sources dc_contracts,dc_payments

# With record limit (for testing)
python scripts/unified_pipeline.py --max-records 100
```

### CLI Commands

```bash
# Factory CLI
python cli.py status          # Show platform status
python cli.py build-gold      # Build gold layer
python cli.py check-quality   # Run quality checks
python cli.py run-pipeline full_refresh

# Warehouse CLI
python -m warehouse.cli status    # Show warehouse status
python -m warehouse.cli load-all  # Load all tables
```

### Testing

```bash
# Run all tests
pytest

# Run specific test file
pytest tests/test_pipelines.py

# Run with coverage
pytest --cov=src --cov=warehouse

# Run with verbose output
pytest -v
```

### Code Quality

```bash
# Format code
ruff format .

# Lint
ruff check .

# Type checking
mypy src warehouse
```

## Code Style Guidelines

1. **Type Hints**: Use type hints everywhere. Leverage `from __future__ import annotations` for forward references.

2. **Docstrings**: Use Google-style docstrings for public functions and classes.

3. **Imports**: Use `from __future__ import annotations` at top. Group imports as: stdlib, third-party, local.

4. **Line Length**: 100 characters max (configured in pyproject.toml).

5. **Naming**:
   - Classes: `PascalCase`
   - Functions/variables: `snake_case`
   - Constants: `UPPER_SNAKE_CASE`
   - Private: prefix with `_`

6. **Error Handling**: Use specific exceptions. Quarantine bad records instead of crashing.

## Data Layers

| Layer | Purpose | Format | Location |
|-------|---------|--------|----------|
| Bronze | Raw API responses | JSON/Parquet | `data/bronze/` |
| Silver | Cleaned, validated | Parquet | `data/silver/` |
| Gold | Business aggregations | Parquet | `data/gold/` |
| Diamond | ML features | Parquet | `data/diamond/` |
| Quarantine | Failed records | Parquet | `data/quarantine/` |

## Adding a New Data Source

1. **Create adapter file** in appropriate `src/adapters/` subdirectory
2. **Implement BaseScraper** with all required methods
3. **Register with decorator**: `@register_scraper("source_name")`
4. **Add configuration** in `config/sources.yaml`
5. **Create schema** if needed in `src/schemas/tabular.py`
6. **Write tests** in `tests/`

Example skeleton:

```python
# src/adapters/my_category/my_source.py
from src.core.base_scraper import BaseScraper
from src.core.registry import register_scraper
from src.schemas.metadata import SourceConfig, SourceType

def get_default_config() -> SourceConfig:
    return SourceConfig(
        name="my_source",
        display_name="My Source",
        source_type=SourceType.REST_API,
        base_url="https://api.example.com",
        requests_per_second=2.0,
        batch_size=1000,
    )

@register_scraper("my_source")
class MySourceAdapter(BaseScraper):
    SOURCE_NAME = "my_source"
    # ... implement required methods
```

## Important Files

- `src/core/base_scraper.py` - Base class for all scrapers (understand this first)
- `src/schemas/base.py` - Core Pydantic models (BaseRecord, LineageInfo)
- `src/storage/writer.py` - Storage layer interface
- `config/settings.py` - Settings and path configuration
- `scripts/unified_pipeline.py` - Main execution entry point
- `pyproject.toml` - Dependencies and tool configuration

## Common Gotchas

1. **Import order matters**: Adapters must be imported to register themselves. The unified pipeline does this in `_run_scrape_stage()`.

2. **Date handling**: ESRI/ArcGIS returns dates as milliseconds since epoch. Use `src/utils/esri_utils.py` for conversion.

3. **Memory management**: For large scrapers (1M+ records), use `_flush_to_storage()` to write incrementally.

4. **Environment variables**: Must use `DC_` prefix (e.g., `DC_WAREHOUSE_DB_HOST`).

5. **Parquet partitioning**: Data is partitioned by source and date. Use `source=X/date=YYYY-MM-DD` format.

6. **Quarantine**: Invalid records go to quarantine, not exceptions. Check `data/quarantine/` for debugging.

## Database Schema

The warehouse uses a star schema:
- **Dimensions**: `dim_supplier`, `dim_contract`, `dim_geography`
- **Facts**: `fact_spend`
- **Gold tables**: Loaded from gold layer aggregations

## Quick Reference

```bash
# Install
pip install -e ".[dev]"

# Run tests
pytest

# Format
ruff format . && ruff check .

# Full pipeline
python scripts/unified_pipeline.py

# Check status
python cli.py status
```

## External Dependencies

- DC ArcGIS APIs (no auth required)
- RapidAPI (for Realtor.com - requires API key)
- RentCast API (requires API key)
- PostgreSQL (for warehouse)
