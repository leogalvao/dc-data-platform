"""Load Gold Parquet data directly into warehouse tables."""

from __future__ import annotations

import json
import logging
from io import StringIO
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from warehouse.config import config

from .base import BaseParquetLoader

logger = logging.getLogger(__name__)


class GoldTableLoader:
    """
    Load Gold layer Parquet files directly into warehouse tables.

    These are pre-aggregated datasets from scrapers_unified that can be
    loaded as-is without complex transformations.
    """

    GOLD_MAPPINGS: dict[str, dict[str, Any]] = {
        "spend_by_vendor": {
            "table": "analytics.gold_spend_by_vendor",
            "truncate": True,
            "columns": [
                "vendor_id", "vendor_name", "vendor_name_normalized",
                "total_contract_value", "total_po_value", "total_payments",
                "contract_count", "po_count", "payment_count",
                "agencies", "agency_count",
                "first_contract_date", "last_payment_date",
                "fiscal_years", "is_cbe", "computed_at",
            ],
        },
        "property_values_by_ward": {
            "table": "analytics.gold_property_by_ward",
            "truncate": True,
            "columns": [
                "ward", "property_count",
                "total_assessed_value", "avg_assessed_value",
                "median_assessed_value", "min_assessed_value", "max_assessed_value",
                "avg_sqft", "avg_price_per_sqft", "avg_year_built",
                "property_type_distribution", "bedroom_distribution",
                "computed_at",
            ],
        },
        "agency_spend_summary": {
            "table": "analytics.gold_agency_spend",
            "truncate": True,
            "columns": [
                "agency_name_normalized", "agency_name",
                "total_contract_value", "total_payments", "total_po_value",
                "contract_count", "vendor_count",
                "top_vendors", "fiscal_year_breakdown",
                "computed_at",
            ],
        },
        "market_trends": {
            "table": "analytics.gold_market_trends",
            "truncate": True,
            "columns": [
                "region_name", "region_type",
                "metric_name", "metric_category",
                "latest_value", "latest_date",
                "yoy_change", "mom_change", "trend_direction",
                "historical_values", "computed_at",
            ],
        },
    }

    def __init__(self, gold_path: Path | None = None) -> None:
        """Initialize Gold table loader."""
        self.gold_path = Path(gold_path) if gold_path else config.gold_path
        self.logger = logging.getLogger(self.__class__.__name__)
        self._connection_string = config.warehouse_db.connection_string

    def load_dataset(self, dataset_name: str) -> dict[str, Any]:
        """Load a single Gold dataset into warehouse."""
        import psycopg

        if dataset_name not in self.GOLD_MAPPINGS:
            return {"status": "error", "reason": f"Unknown dataset: {dataset_name}"}

        cfg = self.GOLD_MAPPINGS[dataset_name]
        source_path = self.gold_path / dataset_name

        if not source_path.exists():
            self.logger.warning(f"Source path does not exist: {source_path}")
            return {"status": "skipped", "reason": "no source data", "dataset": dataset_name}

        # Read parquet files
        try:
            if source_path.is_dir():
                parquet_files = list(source_path.rglob("*.parquet"))
                if not parquet_files:
                    return {"status": "skipped", "reason": "no parquet files", "dataset": dataset_name}
                tables = [pq.read_table(f) for f in parquet_files]
                combined = pa.concat_tables(tables)
                df = combined.to_pandas()
            else:
                df = pq.read_table(source_path).to_pandas()
        except Exception as e:
            self.logger.error(f"Error reading parquet for {dataset_name}: {e}")
            return {"status": "error", "reason": str(e), "dataset": dataset_name}

        if df.empty:
            return {"status": "skipped", "reason": "empty dataset", "dataset": dataset_name}

        self.logger.info(f"Loading {len(df)} rows from {dataset_name} -> {cfg['table']}")

        # Filter to configured columns
        available_cols = [c for c in cfg.get("columns", []) if c in df.columns]
        if available_cols:
            df = df[available_cols]

        # Handle array and JSONB columns
        # Columns that should be PostgreSQL arrays
        array_columns = {"agencies", "fiscal_years"}
        import numpy as np

        def to_pg_array(x):
            """Convert Python/numpy list to PostgreSQL array format: {val1,val2,...}"""
            if x is None:
                return "{}"
            # Handle numpy arrays and lists
            if hasattr(x, 'tolist'):
                x = x.tolist()
            if not isinstance(x, list):
                return x
            if len(x) == 0:
                return "{}"
            # Escape and quote strings containing special chars
            items = []
            for item in x:
                s = str(item)
                # Escape quotes and backslashes, wrap in quotes if needed
                if '"' in s or ',' in s or '\\' in s or '{' in s or '}' in s or ' ' in s or "'" in s:
                    s = '"' + s.replace('\\', '\\\\').replace('"', '\\"') + '"'
                items.append(s)
            return "{" + ",".join(items) + "}"

        for col in df.columns:
            if col in array_columns:
                # Always process array columns
                df[col] = df[col].apply(to_pg_array)
            elif df[col].dtype == "object":
                sample = df[col].dropna().head(1)
                if len(sample) > 0:
                    val = sample.iloc[0]
                    # Check for dict/list/numpy array
                    if isinstance(val, dict) or (hasattr(val, 'tolist') and not isinstance(val, str)):
                        # Regular JSONB column
                        df[col] = df[col].apply(
                            lambda x: json.dumps(x.tolist() if hasattr(x, 'tolist') else x)
                            if isinstance(x, (dict, list)) or (hasattr(x, 'tolist') and not isinstance(x, str))
                            else x
                        )

        with psycopg.connect(self._connection_string) as conn:
            with conn.cursor() as cursor:
                if cfg.get("truncate", False):
                    cursor.execute(f"TRUNCATE TABLE {cfg['table']}")
                    self.logger.info(f"Truncated {cfg['table']}")

                # Use COPY for bulk insert
                buffer = StringIO()
                df.to_csv(buffer, index=False, header=False, sep="\t", na_rep="\\N")
                buffer.seek(0)

                # Use copy_from with the cursor
                with cursor.copy(
                    f"COPY {cfg['table']} ({', '.join(df.columns)}) FROM STDIN WITH (FORMAT CSV, DELIMITER '\t', NULL '\\N')"
                ) as copy:
                    copy.write(buffer.getvalue())

            conn.commit()

        return {
            "status": "success",
            "dataset": dataset_name,
            "table": cfg["table"],
            "rows_loaded": len(df),
        }

    def load_all(self) -> dict[str, Any]:
        """Load all Gold datasets."""
        results = {}
        for dataset_name in self.GOLD_MAPPINGS.keys():
            try:
                results[dataset_name] = self.load_dataset(dataset_name)
            except Exception as e:
                self.logger.error(f"Error loading {dataset_name}: {e}")
                results[dataset_name] = {"status": "error", "reason": str(e)}
        return results
