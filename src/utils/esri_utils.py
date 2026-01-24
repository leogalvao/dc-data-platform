"""
ESRI/ArcGIS utility functions extracted from DC scrapers.

Provides:
- Date conversion from ESRI timestamp format
- Geometry extraction from ESRI responses
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def convert_esri_date(value: Any) -> str | None:
    """
    Convert ESRI timestamp to ISO date string.

    ESRI dates are stored as milliseconds since Unix epoch.
    Values > 10 billion are assumed to be timestamps.

    Args:
        value: Potential timestamp value

    Returns:
        ISO date string (YYYY-MM-DD) or None if not a valid timestamp

    Example:
        >>> convert_esri_date(1704067200000)
        '2024-01-01'
    """
    if not isinstance(value, (int, float)):
        return None

    # Check if value looks like a timestamp (ms since epoch)
    if value <= 10_000_000_000:
        return None

    try:
        dt = datetime.fromtimestamp(value / 1000, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d")
    except (ValueError, OSError, OverflowError):
        return None


def convert_esri_datetime(value: Any) -> datetime | None:
    """
    Convert ESRI timestamp to datetime object.

    Args:
        value: Potential timestamp value

    Returns:
        datetime object or None if not valid

    Example:
        >>> convert_esri_datetime(1704067200000)
        datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    """
    if not isinstance(value, (int, float)):
        return None

    if value <= 10_000_000_000:
        return None

    try:
        return datetime.fromtimestamp(value / 1000, tz=timezone.utc)
    except (ValueError, OSError, OverflowError):
        return None


def convert_esri_dates_in_dict(data: dict[str, Any]) -> dict[str, Any]:
    """
    Convert all ESRI date fields in a dictionary.

    Modifies the dict in-place and returns it.

    Args:
        data: Dictionary potentially containing ESRI timestamps

    Returns:
        Same dictionary with timestamps converted to ISO dates
    """
    for key, value in data.items():
        converted = convert_esri_date(value)
        if converted is not None:
            data[key] = converted
    return data


def convert_esri_geometry(
    geometry: dict[str, Any] | None,
) -> tuple[float | None, float | None]:
    """
    Extract latitude and longitude from ESRI geometry object.

    ESRI geometry format:
        {"x": -77.0369, "y": 38.9072, "spatialReference": {...}}

    Args:
        geometry: ESRI geometry object

    Returns:
        Tuple of (latitude, longitude) or (None, None) if not available
    """
    if not geometry:
        return None, None

    try:
        x = geometry.get("x")  # longitude
        y = geometry.get("y")  # latitude

        if x is not None and y is not None:
            return float(y), float(x)
    except (ValueError, TypeError):
        pass

    return None, None


def parse_esri_feature(feature: dict[str, Any]) -> dict[str, Any]:
    """
    Parse a complete ESRI feature (attributes + geometry).

    Combines attribute extraction with date conversion and
    geometry extraction.

    Args:
        feature: ESRI feature object with "attributes" and "geometry"

    Returns:
        Flat dictionary with converted dates and lat/long added
    """
    attrs = feature.get("attributes", {}).copy()

    # Convert dates
    convert_esri_dates_in_dict(attrs)

    # Extract geometry
    geometry = feature.get("geometry")
    lat, lng = convert_esri_geometry(geometry)
    if lat is not None:
        attrs["_latitude"] = lat
    if lng is not None:
        attrs["_longitude"] = lng

    return attrs
