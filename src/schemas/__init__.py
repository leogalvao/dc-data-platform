"""Pydantic schemas for canonical data models."""

from .base import BaseRecord, LineageInfo, RawPayload
from .metadata import RunMetadata, RunReport, SourceConfig
from .tabular import (
    BusinessRecord,
    ContractRecord,
    PaymentRecord,
    PropertyRecord,
    PurchaseOrderRecord,
    RentalRecord,
    TabularRecord,
)
from .textual import DocumentRecord, TextualRecord

__all__ = [
    # Base
    "BaseRecord",
    "LineageInfo",
    "RawPayload",
    # Tabular
    "TabularRecord",
    "ContractRecord",
    "PurchaseOrderRecord",
    "PaymentRecord",
    "PropertyRecord",
    "RentalRecord",
    "BusinessRecord",
    # Textual
    "TextualRecord",
    "DocumentRecord",
    # Metadata
    "RunMetadata",
    "SourceConfig",
    "RunReport",
]
