"""
Parquet to Warehouse loaders for DC Procurement data.

This module provides loaders to move data from scrapers_unified Parquet files
into the warehouse schema (analytics.dim_*, analytics.fact_*, analytics.gold_*).

Usage:
    from warehouse.loaders import DimSupplierLoader, FactSpendLoader
    from warehouse.config import config

    # Load dimensions
    loader = DimSupplierLoader(config.silver_path)
    result = loader.run()

    # Load facts (after dimensions)
    loader = FactSpendLoader(config.silver_path)
    result = loader.run()

    # Load Gold data
    loader = GoldTableLoader(config.gold_path)
    results = loader.load_all()
"""

from .base import BaseParquetLoader
from .dim_contract import DimContractLoader
from .dim_geography import DimGeographyLoader
from .dim_supplier import DimSupplierLoader
from .fact_spend import FactSpendLoader
from .gold_loader import GoldTableLoader

__all__ = [
    "BaseParquetLoader",
    "DimSupplierLoader",
    "DimContractLoader",
    "DimGeographyLoader",
    "FactSpendLoader",
    "GoldTableLoader",
]
