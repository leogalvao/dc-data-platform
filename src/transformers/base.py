"""
Base transformer classes for Gold and Diamond layers.

Provides abstract interfaces and utility methods for data transformations.
"""

from __future__ import annotations

import json
import logging
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TypeVar

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False
    pa = None
    pq = None

logger = logging.getLogger(__name__)

T = TypeVar("T")


class BaseTransformer(ABC):
    """
    Abstract base class for data transformers.

    Provides common functionality for:
    - Source table reading from Silver/Gold layers
    - Output writing with schema enforcement
    - Logging and timing utilities
    - Validation hooks
    """

    def __init__(
        self,
        silver_path: str | Path = "data/silver",
        gold_path: str | Path = "data/gold",
        diamond_path: str | Path = "data/diamond",
    ):
        """
        Initialize the transformer.

        Args:
            silver_path: Path to Silver layer data
            gold_path: Path to Gold layer data
            diamond_path: Path to Diamond layer data
        """
        self._silver_path = Path(silver_path)
        self._gold_path = Path(gold_path)
        self._diamond_path = Path(diamond_path)
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @abstractmethod
    def transform(self) -> list[dict[str, Any]]:
        """
        Execute the transformation logic.

        Returns:
            List of transformed records
        """
        ...

    @abstractmethod
    def get_schema(self) -> pa.Schema | None:
        """
        Get the PyArrow schema for the output.

        Returns:
            PyArrow schema or None if schema not defined
        """
        ...

    @abstractmethod
    def validate(self, records: list[dict[str, Any]]) -> list[str]:
        """
        Validate transformed records.

        Args:
            records: Records to validate

        Returns:
            List of validation error messages (empty if valid)
        """
        ...

    def run(self) -> tuple[list[dict[str, Any]], float]:
        """
        Execute the full transform pipeline with timing.

        Returns:
            Tuple of (records, elapsed_time_seconds)
        """
        self._logger.info(f"Starting {self.__class__.__name__}")
        start_time = time.time()

        try:
            records = self.transform()

            # Validate
            errors = self.validate(records)
            if errors:
                for error in errors:
                    self._logger.warning(f"Validation warning: {error}")

            elapsed = time.time() - start_time
            self._logger.info(
                f"Completed {self.__class__.__name__}: "
                f"{len(records)} records in {elapsed:.2f}s"
            )

            return records, elapsed

        except Exception as e:
            elapsed = time.time() - start_time
            self._logger.error(
                f"Failed {self.__class__.__name__} after {elapsed:.2f}s: {e}"
            )
            raise

    def load_silver_records(self, source_name: str) -> list[dict[str, Any]]:
        """
        Load records from Silver layer for a source.

        Args:
            source_name: Source identifier (e.g., 'dc_contracts')

        Returns:
            List of record dictionaries
        """
        records = []
        source_path = self._silver_path / f"source={source_name}"

        if not source_path.exists():
            self._logger.debug(f"No silver data for {source_name}")
            return records

        if PYARROW_AVAILABLE:
            for f in source_path.glob("**/*.parquet"):
                try:
                    table = pq.read_table(f)
                    records.extend(table.to_pylist())
                except Exception as e:
                    self._logger.warning(f"Could not read {f}: {e}")
        else:
            for f in source_path.glob("**/*.json"):
                try:
                    with open(f) as fp:
                        data = json.load(fp)
                        if isinstance(data, list):
                            records.extend(data)
                except Exception as e:
                    self._logger.warning(f"Could not read {f}: {e}")

        self._logger.debug(f"Loaded {len(records)} records from silver/{source_name}")
        return records

    def load_gold_records(self, dataset_name: str) -> list[dict[str, Any]]:
        """
        Load records from Gold layer.

        Args:
            dataset_name: Dataset name (e.g., 'spend_by_vendor')

        Returns:
            List of record dictionaries
        """
        records = []
        dataset_path = self._gold_path / dataset_name

        if not dataset_path.exists():
            self._logger.debug(f"No gold data for {dataset_name}")
            return records

        if PYARROW_AVAILABLE:
            for f in dataset_path.glob("**/*.parquet"):
                try:
                    table = pq.read_table(f)
                    records.extend(table.to_pylist())
                except Exception as e:
                    self._logger.warning(f"Could not read {f}: {e}")
        else:
            for f in dataset_path.glob("**/*.json"):
                try:
                    with open(f) as fp:
                        data = json.load(fp)
                        if isinstance(data, list):
                            records.extend(data)
                except Exception as e:
                    self._logger.warning(f"Could not read {f}: {e}")

        self._logger.debug(f"Loaded {len(records)} records from gold/{dataset_name}")
        return records

    def write_output(
        self,
        records: list[dict[str, Any]],
        output_path: Path,
        schema: pa.Schema | None = None,
    ) -> Path:
        """
        Write transformed records to storage.

        Args:
            records: Records to write
            output_path: Directory path for output
            schema: Optional PyArrow schema

        Returns:
            Path to written file
        """
        if not records:
            self._logger.warning("No records to write")
            return output_path

        output_path.mkdir(parents=True, exist_ok=True)

        # Add computed_at if not present
        computed_at = datetime.now(timezone.utc).isoformat()
        for record in records:
            if "computed_at" not in record:
                record["computed_at"] = computed_at

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

        if PYARROW_AVAILABLE:
            filepath = output_path / f"{timestamp}.parquet"

            if schema:
                try:
                    table = pa.Table.from_pylist(records, schema=schema)
                except (pa.ArrowInvalid, pa.ArrowTypeError):
                    self._logger.warning("Schema enforcement failed, using inference")
                    table = pa.Table.from_pylist(records)
            else:
                table = pa.Table.from_pylist(records)

            pq.write_table(table, filepath, compression="snappy")
        else:
            filepath = output_path / f"{timestamp}.json"
            with open(filepath, "w") as f:
                json.dump(records, f, default=str, indent=2)

        self._logger.debug(f"Wrote {len(records)} records to {filepath}")
        return filepath

    def write_metadata(
        self,
        output_path: Path,
        row_count: int,
        schema_fields: list[str],
        extra_metadata: dict[str, Any] | None = None,
    ) -> None:
        """
        Write metadata JSON alongside the output.

        Args:
            output_path: Directory containing the output
            row_count: Number of records written
            schema_fields: List of field names
            extra_metadata: Additional metadata to include
        """
        metadata = {
            "transformer": self.__class__.__name__,
            "row_count": row_count,
            "computed_at": datetime.now(timezone.utc).isoformat(),
            "schema_fields": schema_fields,
        }

        if extra_metadata:
            metadata.update(extra_metadata)

        metadata_path = output_path / "metadata.json"
        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=2, default=str)

    @staticmethod
    def normalize_vendor_name(name: str) -> str:
        """Normalize vendor name for matching."""
        if not name:
            return ""
        normalized = name.lower().strip()
        for suffix in [" llc", " inc", " corp", " ltd", " co", " company"]:
            if normalized.endswith(suffix):
                normalized = normalized[: -len(suffix)]
        return normalized.strip()

    @staticmethod
    def normalize_agency_name(name: str) -> str:
        """Normalize agency name for matching."""
        if not name:
            return ""
        return name.lower().strip()
