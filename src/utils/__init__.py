"""Shared utility modules extracted from existing scrapers."""

from .arrow_schemas import (
    get_arrow_schema,
    get_schema_for_records,
    list_available_schemas,
)
from .console import Console, create_progress_bar
from .esri_utils import convert_esri_date, convert_esri_geometry
from .hash_utils import compute_hash, compute_record_hash
from .http_client import AsyncHTTPClient, HTTPClient
from .type_coercion import (
    clean_phone,
    clean_zip_code,
    safe_bool,
    safe_date,
    safe_datetime,
    safe_decimal,
    safe_float,
    safe_int,
    safe_string,
)

__all__ = [
    # ESRI utilities
    "convert_esri_date",
    "convert_esri_geometry",
    # Hash utilities
    "compute_hash",
    "compute_record_hash",
    # HTTP clients
    "HTTPClient",
    "AsyncHTTPClient",
    # Console utilities
    "Console",
    "create_progress_bar",
    # Type coercion helpers
    "safe_float",
    "safe_int",
    "safe_decimal",
    "safe_date",
    "safe_datetime",
    "safe_bool",
    "safe_string",
    "clean_phone",
    "clean_zip_code",
    # PyArrow schemas
    "get_arrow_schema",
    "get_schema_for_records",
    "list_available_schemas",
]
