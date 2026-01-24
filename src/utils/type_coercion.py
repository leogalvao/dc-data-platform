"""
Type coercion helpers for safe data conversion.

Provides robust conversion functions that handle edge cases like:
- Empty strings
- NaN values
- Currency formatting
- Various date formats
"""

from __future__ import annotations

import math
import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

# Try to import dateutil for flexible date parsing
try:
    from dateutil import parser as date_parser
    DATEUTIL_AVAILABLE = True
except ImportError:
    DATEUTIL_AVAILABLE = False


def safe_float(value: Any, default: float | None = None) -> float | None:
    """
    Safely convert value to float.

    Handles:
    - None and empty strings
    - NaN values
    - Currency strings ($1,234.56)
    - Whitespace

    Args:
        value: Value to convert
        default: Default to return on failure

    Returns:
        Float value or default
    """
    if value is None or value == "":
        return default

    # Handle NaN
    if isinstance(value, float) and math.isnan(value):
        return default

    try:
        # Already a numeric type
        if isinstance(value, (int, float)):
            return float(value)

        # Handle strings
        if isinstance(value, str):
            # Remove currency symbols, commas, whitespace
            cleaned = value.replace("$", "").replace(",", "").strip()
            if not cleaned or cleaned.lower() in ("nan", "null", "none", "n/a", "-"):
                return default
            return float(cleaned)

        # Try direct conversion
        return float(value)

    except (ValueError, TypeError):
        return default


def safe_int(value: Any, default: int | None = None) -> int | None:
    """
    Safely convert value to int.

    Handles:
    - None and empty strings
    - NaN values
    - Decimal values (truncated)
    - Currency strings

    Args:
        value: Value to convert
        default: Default to return on failure

    Returns:
        Int value or default
    """
    f = safe_float(value)
    if f is None:
        return default
    try:
        return int(f)
    except (ValueError, TypeError, OverflowError):
        return default


def safe_decimal(value: Any, default: Decimal | None = None) -> Decimal | None:
    """
    Safely convert value to Decimal.

    Handles:
    - None and empty strings
    - NaN values
    - Currency strings

    Args:
        value: Value to convert
        default: Default to return on failure

    Returns:
        Decimal value or default
    """
    if value is None or value == "":
        return default

    # Handle NaN
    if isinstance(value, float) and math.isnan(value):
        return default

    try:
        # Clean string values
        if isinstance(value, str):
            cleaned = value.replace("$", "").replace(",", "").strip()
            if not cleaned or cleaned.lower() in ("nan", "null", "none", "n/a", "-"):
                return default
            return Decimal(cleaned)

        return Decimal(str(value))

    except (InvalidOperation, ValueError, TypeError):
        return default


def safe_date(
    value: Any,
    formats: list[str] | None = None,
    default: date | None = None,
) -> date | None:
    """
    Parse date from various formats.

    Handles:
    - None and empty strings
    - date and datetime objects
    - ISO format strings
    - Common US/EU formats
    - ESRI timestamps (milliseconds since epoch)

    Args:
        value: Value to convert
        formats: List of strptime format strings to try
        default: Default to return on failure

    Returns:
        date value or default
    """
    if value is None or value == "":
        return default

    # Already a date
    if isinstance(value, date):
        return value if not isinstance(value, datetime) else value.date()

    if isinstance(value, datetime):
        return value.date()

    # Handle ESRI timestamps (milliseconds since epoch)
    if isinstance(value, int) and value > 10_000_000_000:
        try:
            return datetime.utcfromtimestamp(value / 1000).date()
        except (ValueError, OSError, OverflowError):
            pass

    # String parsing
    if isinstance(value, str):
        value = value.strip()
        if not value or value.lower() in ("null", "none", "n/a", "-"):
            return default

        # Try ISO format first
        try:
            # Handle YYYY-MM format
            if len(value) == 7 and value[4] == "-":
                return date.fromisoformat(f"{value}-01")
            return date.fromisoformat(value[:10])
        except ValueError:
            pass

        # Try custom formats
        if formats:
            for fmt in formats:
                try:
                    return datetime.strptime(value, fmt).date()
                except ValueError:
                    continue

        # Use dateutil if available for flexible parsing
        if DATEUTIL_AVAILABLE:
            try:
                return date_parser.parse(value).date()
            except (ValueError, TypeError):
                pass

        # Common fallback formats
        default_formats = [
            "%m/%d/%Y",
            "%m-%d-%Y",
            "%d/%m/%Y",
            "%d-%m-%Y",
            "%Y/%m/%d",
            "%B %d, %Y",
            "%b %d, %Y",
        ]
        for fmt in default_formats:
            try:
                return datetime.strptime(value, fmt).date()
            except ValueError:
                continue

    return default


def safe_datetime(
    value: Any,
    formats: list[str] | None = None,
    default: datetime | None = None,
) -> datetime | None:
    """
    Parse datetime from various formats.

    Handles:
    - None and empty strings
    - datetime objects
    - ISO format strings
    - ESRI timestamps

    Args:
        value: Value to convert
        formats: List of strptime format strings to try
        default: Default to return on failure

    Returns:
        datetime value or default
    """
    if value is None or value == "":
        return default

    if isinstance(value, datetime):
        return value

    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())

    # Handle ESRI timestamps
    if isinstance(value, int) and value > 10_000_000_000:
        try:
            return datetime.utcfromtimestamp(value / 1000)
        except (ValueError, OSError, OverflowError):
            pass

    if isinstance(value, str):
        value = value.strip()
        if not value or value.lower() in ("null", "none", "n/a", "-"):
            return default

        # Try ISO format
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            pass

        # Use dateutil if available
        if DATEUTIL_AVAILABLE:
            try:
                return date_parser.parse(value)
            except (ValueError, TypeError):
                pass

        # Try custom formats
        if formats:
            for fmt in formats:
                try:
                    return datetime.strptime(value, fmt)
                except ValueError:
                    continue

    return default


def safe_bool(value: Any, default: bool | None = None) -> bool | None:
    """
    Safely convert value to boolean.

    Handles:
    - None and empty strings
    - String representations (yes/no, true/false, 1/0)

    Args:
        value: Value to convert
        default: Default to return on failure

    Returns:
        Boolean value or default
    """
    if value is None or value == "":
        return default

    if isinstance(value, bool):
        return value

    if isinstance(value, (int, float)):
        return bool(value)

    if isinstance(value, str):
        lower = value.strip().lower()
        if lower in ("true", "yes", "y", "1", "on"):
            return True
        if lower in ("false", "no", "n", "0", "off"):
            return False

    return default


def safe_string(value: Any, default: str | None = None, max_length: int | None = None) -> str | None:
    """
    Safely convert value to string.

    Handles:
    - None values
    - Whitespace trimming
    - Length limiting

    Args:
        value: Value to convert
        default: Default to return for None/empty
        max_length: Maximum string length (truncates if exceeded)

    Returns:
        String value or default
    """
    if value is None:
        return default

    result = str(value).strip()

    if not result:
        return default

    if max_length and len(result) > max_length:
        result = result[:max_length]

    return result


def clean_phone(value: Any) -> str | None:
    """
    Clean and normalize phone number.

    Removes common formatting characters.

    Args:
        value: Phone number string

    Returns:
        Cleaned phone number or None
    """
    if value is None:
        return None

    # Remove common formatting
    cleaned = re.sub(r"[^\d+]", "", str(value))

    if not cleaned or len(cleaned) < 7:
        return None

    return cleaned


def clean_zip_code(value: Any) -> str | None:
    """
    Clean and normalize ZIP code.

    Handles 5-digit and ZIP+4 formats.

    Args:
        value: ZIP code value

    Returns:
        Cleaned ZIP code or None
    """
    if value is None:
        return None

    cleaned = str(value).strip()

    # Remove non-alphanumeric except hyphen
    cleaned = re.sub(r"[^0-9-]", "", cleaned)

    if not cleaned:
        return None

    # Pad to 5 digits if numeric
    if cleaned.isdigit() and len(cleaned) < 5:
        cleaned = cleaned.zfill(5)

    return cleaned
