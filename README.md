# DC Data Platform

A unified data platform for DC government procurement, property, and demographic data. This repository consolidates scraping, transformation, and warehouse loading into a single cohesive project.

## Architecture

```
dc-data-platform/
├── src/                    # Data scrapers and core utilities
│   ├── adapters/           # Source-specific scrapers
│   │   ├── dc_arcgis/      # DC government ArcGIS APIs
│   │   ├── demographics/   # Census and ODN data
│   │   └── real_estate/    # Zillow, Realtor, RentCast
│   ├── core/               # Orchestrator, rate limiter, registry
│   ├── schemas/            # Data schemas and validation
│   ├── storage/            # Bronze/Silver/Gold/Diamond writers
│   └── transformers/       # Data transformation logic
├── warehouse/              # PostgreSQL warehouse loaders
│   ├── loaders/            # Dimension and fact table loaders
│   ├── config.py           # Database configuration
│   └── cli.py              # Warehouse CLI
├── pipelines/              # Pipeline orchestration
│   ├── definitions/        # YAML pipeline definitions
│   ├── tasks/              # Task implementations
│   └── runner.py           # Pipeline runner
├── config/                 # Configuration files
├── contracts/              # Data contracts and SLAs
├── data/                   # Data lake (bronze/silver/gold/diamond)
├── scripts/                # CLI scripts
│   └── unified_pipeline.py # Main unified pipeline script
└── tests/                  # Test suite
```

## Data Flow

```
Sources → Bronze → Silver → Gold → Diamond → Warehouse
   ↓        ↓        ↓        ↓        ↓          ↓
  APIs    Raw     Clean   Aggregated  ML-ready  PostgreSQL
         JSON    Parquet  Parquet    Features   Analytics
```

## Quick Start

### Installation

```bash
# Clone the repository
git clone https://github.com/leogalvao/dc-data-platform.git
cd dc-data-platform

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # or .venv\Scripts\activate on Windows

# Install dependencies
pip install -e .
```

### Configuration

Copy the example environment file and configure:

```bash
cp .env.example .env
# Edit .env with your database credentials
```

### Running the Pipeline

```bash
# Run the full pipeline (scrape → transform → load)
python scripts/unified_pipeline.py

# Dry run to see what would happen
python scripts/unified_pipeline.py --dry-run

# Run individual stages
python scripts/unified_pipeline.py --stage scrape
python scripts/unified_pipeline.py --stage transform
python scripts/unified_pipeline.py --stage load

# Run specific data sources
python scripts/unified_pipeline.py --sources dc_contracts,dc_payments

# Incremental mode (use checkpoints)
python scripts/unified_pipeline.py --incremental

# Limit records per source (for testing)
python scripts/unified_pipeline.py --max-records 100
```

### CLI Commands

```bash
# Factory CLI - orchestration commands
python cli.py status              # Show platform status
python cli.py build-gold          # Build gold layer
python cli.py check-quality       # Run quality checks
python cli.py run-pipeline full_refresh

# Warehouse CLI
python -m warehouse.cli status    # Show warehouse status
python -m warehouse.cli load-all  # Load all tables
python -m warehouse.cli serve     # Start dashboard API
```

## Data Sources

### DC Government (ArcGIS)
- **dc_contracts** - Government contracts
- **dc_payments** - Payment records
- **dc_purchase_orders** - Purchase orders
- **dc_residential_cama** - Property assessments
- **dc_tax_lots** - Tax lot boundaries
- **dc_ward_2022** - Ward demographics

### Real Estate
- **zillow_zhvi** - Zillow Home Value Index
- **zillow_zori** - Zillow Observed Rent Index
- **crossing_dc** - Apartment listings
- **realtor_api** - Realtor.com listings (requires API key)
- **rentcast_api** - Rental market stats (requires API key)

### Demographics
- **odn_demographics** - Open Data Network census data

## Data Layers

| Layer | Purpose | Format | Example |
|-------|---------|--------|---------|
| **Bronze** | Raw data as received | JSON/Parquet | API responses |
| **Silver** | Cleaned, deduplicated | Parquet | Standardized records |
| **Gold** | Business aggregations | Parquet | spend_by_vendor, property_by_ward |
| **Diamond** | ML-ready features | Parquet | vendor_features, property_features |
| **Warehouse** | Analytics-ready | PostgreSQL | Dimension/Fact tables |

## Development

```bash
# Run tests
pytest

# Run specific test file
pytest tests/test_pipelines.py

# Format code
ruff format .

# Lint
ruff check .
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DC_ENVIRONMENT` | Environment (local/dev/prod) | local |
| `DC_WAREHOUSE_DB_HOST` | PostgreSQL host | localhost |
| `DC_WAREHOUSE_DB_PORT` | PostgreSQL port | 5432 |
| `DC_WAREHOUSE_DB_NAME` | Database name | dc_analytics |
| `DC_WAREHOUSE_DB_USER` | Database user | postgres |
| `DC_WAREHOUSE_DB_PASSWORD` | Database password | |
| `RAPIDAPI_KEY` | RapidAPI key (for Realtor) | |
| `RENTCAST_API_KEY` | RentCast API key | |

## License

MIT
