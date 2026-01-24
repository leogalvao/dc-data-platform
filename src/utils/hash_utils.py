"""
Hashing utilities for content deduplication.

Provides consistent hashing for:
- Raw content (bytes/strings)
- Record dictionaries
"""

from __future__ import annotations

import hashlib
import json
from typing import Any


def compute_hash(content: str | bytes) -> str:
    """
    Compute SHA-256 hash of content.

    Args:
        content: String or bytes to hash

    Returns:
        Hex-encoded SHA-256 hash

    Example:
        >>> compute_hash("hello world")
        'b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9'
    """
    if isinstance(content, str):
        content = content.encode("utf-8")
    return hashlib.sha256(content).hexdigest()


def compute_record_hash(
    record: dict[str, Any],
    *,
    exclude_fields: set[str] | None = None,
) -> str:
    """
    Compute hash of a record dictionary for deduplication.

    Normalizes the record to ensure consistent hashing:
    - Sorts keys alphabetically
    - Converts to JSON with sorted keys
    - Excludes specified fields (e.g., timestamps)

    Args:
        record: Record dictionary to hash
        exclude_fields: Fields to exclude from hash computation

    Returns:
        Hex-encoded SHA-256 hash

    Example:
        >>> compute_record_hash({"b": 2, "a": 1})
        '...'  # Same hash regardless of key order
    """
    if exclude_fields is None:
        exclude_fields = {"scraped_at", "run_id", "record_id", "_timestamp"}

    # Filter and sort record
    filtered = {
        k: v for k, v in sorted(record.items())
        if k not in exclude_fields
    }

    # Serialize to JSON with sorted keys for consistency
    json_str = json.dumps(filtered, sort_keys=True, default=str)
    return compute_hash(json_str)


def compute_content_signature(
    content_type: str,
    content: bytes,
    source_url: str,
) -> str:
    """
    Compute a signature for raw content including metadata.

    Useful for detecting duplicate fetches of the same content
    from the same URL.

    Args:
        content_type: MIME type
        content: Raw content bytes
        source_url: URL content was fetched from

    Returns:
        Hex-encoded hash
    """
    # Combine metadata with content hash
    content_hash = compute_hash(content)
    signature_input = f"{content_type}|{source_url}|{content_hash}"
    return compute_hash(signature_input)
