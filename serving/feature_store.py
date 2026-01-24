"""Feature store interface for serving ML features."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

try:
    import pyarrow.parquet as pq
    import pyarrow as pa
    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False


@dataclass
class FeatureVector:
    """Feature vector for a single entity."""

    entity_id: str
    features: dict[str, Any]
    computed_at: datetime


class FeatureStore:
    """
    Simple feature store interface for serving diamond layer features.

    This provides a unified interface for accessing ML features
    from the diamond layer, with support for batch and point lookups.
    """

    def __init__(self, diamond_path: Path):
        """
        Initialize feature store.

        Args:
            diamond_path: Path to diamond layer data
        """
        self.diamond_path = diamond_path
        self._cache: dict[str, Any] = {}

    def list_feature_sets(self) -> list[str]:
        """List available feature sets."""
        if not self.diamond_path.exists():
            return []
        return [d.name for d in self.diamond_path.iterdir() if d.is_dir()]

    def get_feature_set_schema(self, feature_set: str) -> list[str]:
        """
        Get schema (column names) for a feature set.

        Args:
            feature_set: Name of the feature set

        Returns:
            List of column names
        """
        if not PYARROW_AVAILABLE:
            logger.warning("PyArrow not available")
            return []

        feature_path = self.diamond_path / feature_set
        parquet_files = list(feature_path.glob("*.parquet"))

        if not parquet_files:
            return []

        schema = pq.read_schema(parquet_files[0])
        return [f.name for f in schema]

    def get_features(
        self,
        feature_set: str,
        entity_ids: list[str],
        feature_columns: list[str] | None = None,
    ) -> list[FeatureVector]:
        """
        Get features for specific entities.

        Args:
            feature_set: Name of the feature set (e.g., "vendor_features")
            entity_ids: List of entity IDs to fetch
            feature_columns: Specific columns to return (default: all)

        Returns:
            List of FeatureVector objects
        """
        if not PYARROW_AVAILABLE:
            logger.warning("PyArrow not available")
            return []

        feature_path = self.diamond_path / feature_set
        if not feature_path.exists():
            logger.warning(f"Feature set not found: {feature_set}")
            return []

        # Determine primary key column
        pk_column = self._infer_pk_column(feature_set)

        # Read data
        parquet_files = list(feature_path.glob("*.parquet"))
        if not parquet_files:
            return []

        try:
            table = pq.read_table(feature_path, columns=feature_columns)
            df_dict = table.to_pydict()

            # Filter to requested entity IDs
            results = []
            entity_id_set = set(entity_ids)

            pk_values = df_dict.get(pk_column, [])
            for i, pk_val in enumerate(pk_values):
                if str(pk_val) in entity_id_set:
                    features = {
                        col: df_dict[col][i]
                        for col in df_dict
                        if col != pk_column
                    }
                    results.append(
                        FeatureVector(
                            entity_id=str(pk_val),
                            features=features,
                            computed_at=datetime.now(),
                        )
                    )

            return results

        except Exception as e:
            logger.error(f"Error fetching features: {e}")
            return []

    def get_all_features(
        self,
        feature_set: str,
        feature_columns: list[str] | None = None,
    ) -> list[FeatureVector]:
        """
        Get all features from a feature set.

        Args:
            feature_set: Name of the feature set
            feature_columns: Specific columns to return

        Returns:
            List of all FeatureVector objects
        """
        if not PYARROW_AVAILABLE:
            logger.warning("PyArrow not available")
            return []

        feature_path = self.diamond_path / feature_set
        if not feature_path.exists():
            return []

        pk_column = self._infer_pk_column(feature_set)

        try:
            table = pq.read_table(feature_path, columns=feature_columns)
            df_dict = table.to_pydict()

            results = []
            pk_values = df_dict.get(pk_column, [])

            for i, pk_val in enumerate(pk_values):
                features = {
                    col: df_dict[col][i]
                    for col in df_dict
                    if col != pk_column
                }
                results.append(
                    FeatureVector(
                        entity_id=str(pk_val),
                        features=features,
                        computed_at=datetime.now(),
                    )
                )

            return results

        except Exception as e:
            logger.error(f"Error fetching all features: {e}")
            return []

    def _infer_pk_column(self, feature_set: str) -> str:
        """Infer primary key column from feature set name."""
        pk_mapping = {
            "vendor_features": "vendor_id",
            "property_features": "property_id",
        }
        return pk_mapping.get(feature_set, "id")
