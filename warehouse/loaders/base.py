"""Base loader class for Parquet to Postgres ETL."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Generator

import psycopg
import pyarrow.parquet as pq

from warehouse.config import config

logger = logging.getLogger(__name__)


class BaseParquetLoader(ABC):
    """
    Base class for loading Parquet data into warehouse tables.

    Subclasses implement source_pattern, target_table, transform(), and load()
    to define the ETL logic for each entity type.
    """

    def __init__(self, parquet_path: Path | None = None) -> None:
        """
        Initialize loader with paths.

        Args:
            parquet_path: Path to the Silver or Gold layer directory.
                         Defaults to config.silver_path.
        """
        self.parquet_path = Path(parquet_path) if parquet_path else config.silver_path
        self.logger = logging.getLogger(self.__class__.__name__)
        self._connection_string = config.warehouse_db.connection_string

    @property
    @abstractmethod
    def source_pattern(self) -> str:
        """
        Glob pattern for source parquet files.

        Example: "source=dc_contracts" or "spend_by_vendor"
        """
        pass

    @property
    @abstractmethod
    def target_table(self) -> str:
        """
        Target warehouse table name.

        Example: "analytics.dim_supplier"
        """
        pass

    @contextmanager
    def get_connection(self) -> Generator[psycopg.Connection, None, None]:
        """Get a database connection."""
        conn = psycopg.connect(self._connection_string)
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    @contextmanager
    def get_cursor(self) -> Generator[psycopg.Cursor, None, None]:
        """Get a database cursor with auto-commit connection."""
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                yield cursor

    def read_source_data(self) -> list[dict[str, Any]]:
        """
        Read all parquet files matching the source pattern.

        Returns:
            List of records as dictionaries
        """
        source_path = self.parquet_path / self.source_pattern
        all_records = []

        # Handle both directory and file patterns
        if source_path.is_dir():
            parquet_files = list(source_path.rglob("*.parquet"))
        else:
            parquet_files = list(self.parquet_path.glob(f"{self.source_pattern}/**/*.parquet"))

        if not parquet_files:
            self.logger.warning(f"No parquet files found for pattern: {self.source_pattern}")
            return []

        for pq_file in parquet_files:
            try:
                table = pq.read_table(pq_file)
                records = table.to_pylist()
                all_records.extend(records)
                self.logger.debug(f"Read {len(records)} records from {pq_file.name}")
            except Exception as e:
                self.logger.warning(f"Error reading {pq_file}: {e}")

        self.logger.info(f"Read {len(all_records)} total records from {self.source_pattern}")
        return all_records

    @abstractmethod
    def transform(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Transform raw records for the target table.

        Args:
            records: Raw records from parquet files

        Returns:
            Transformed records ready for loading
        """
        pass

    @abstractmethod
    def load(self, records: list[dict[str, Any]]) -> dict[str, Any]:
        """
        Load transformed records into the target table.

        Args:
            records: Transformed records

        Returns:
            Load statistics (inserted, updated, etc.)
        """
        pass

    def run(self) -> dict[str, Any]:
        """
        Execute the full ETL pipeline: extract, transform, load.

        Returns:
            Dictionary with load statistics and status
        """
        self.logger.info(f"Starting load: {self.source_pattern} -> {self.target_table}")

        # Extract
        raw_records = self.read_source_data()
        if not raw_records:
            return {
                "status": "skipped",
                "reason": "no source data",
                "table": self.target_table,
            }

        # Transform
        transformed = self.transform(raw_records)
        if not transformed:
            return {
                "status": "skipped",
                "reason": "no records after transform",
                "table": self.target_table,
                "raw_count": len(raw_records),
            }

        # Load
        result = self.load(transformed)
        result["status"] = "success"
        result["raw_count"] = len(raw_records)
        result["transformed_count"] = len(transformed)

        self.logger.info(f"Load complete: {result}")
        return result
