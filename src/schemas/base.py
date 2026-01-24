"""Base Pydantic models with lineage tracking for all records."""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, computed_field


class LineageInfo(BaseModel):
    """Lineage metadata attached to every record for traceability."""

    source_name: str = Field(
        ...,
        description="Identifier for the data source (e.g., 'dc_contracts', 'realtor_api')",
    )
    source_url: str | None = Field(
        default=None,
        description="Original URL or endpoint the data was fetched from",
    )
    scraped_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="UTC timestamp when record was scraped",
    )
    run_id: UUID = Field(
        default_factory=uuid4,
        description="Unique identifier for this scraping run",
    )
    raw_hash: str = Field(
        default="",
        description="SHA-256 hash of the raw payload for deduplication",
    )
    schema_version: str = Field(
        default="1.0.0",
        description="Version of the schema used to parse this record",
    )
    adapter_version: str = Field(
        default="1.0.0",
        description="Version of the adapter that produced this record",
    )

    model_config = {"frozen": True}

    @classmethod
    def compute_hash(cls, content: str | bytes) -> str:
        """Compute SHA-256 hash of content for deduplication."""
        if isinstance(content, str):
            content = content.encode("utf-8")
        return hashlib.sha256(content).hexdigest()


class RawPayload(BaseModel):
    """Raw payload stored in Bronze layer before normalization."""

    payload_id: UUID = Field(default_factory=uuid4)
    content_type: str = Field(
        ...,
        description="MIME type: application/json, text/html, text/csv, etc.",
    )
    content: bytes = Field(
        ...,
        description="Raw bytes of the fetched content",
    )
    content_hash: str = Field(
        default="",
        description="SHA-256 of content for deduplication",
    )
    lineage: LineageInfo
    fetch_status_code: int | None = Field(
        default=None,
        description="HTTP status code from fetch",
    )
    fetch_headers: dict[str, str] = Field(
        default_factory=dict,
        description="Response headers from fetch",
    )
    fetch_duration_ms: int | None = Field(
        default=None,
        description="Time taken to fetch in milliseconds",
    )

    model_config = {"arbitrary_types_allowed": True}

    def model_post_init(self, __context: Any) -> None:
        """Compute content hash after initialization if not set."""
        if not self.content_hash:
            object.__setattr__(
                self,
                "content_hash",
                LineageInfo.compute_hash(self.content),
            )


class BaseRecord(BaseModel):
    """
    Base model for all normalized records.

    All domain-specific records (tabular, textual) inherit from this.
    Provides common fields and validation.
    """

    record_id: UUID = Field(
        default_factory=uuid4,
        description="Unique identifier for this normalized record",
    )
    lineage: LineageInfo = Field(
        ...,
        description="Provenance and lineage information",
    )
    extra_fields: dict[str, Any] = Field(
        default_factory=dict,
        description="Catch-all for source-specific fields not in schema",
    )
    validation_errors: list[str] = Field(
        default_factory=list,
        description="Non-fatal validation warnings",
    )
    is_valid: bool = Field(
        default=True,
        description="Whether record passed all validations",
    )

    model_config = {
        "extra": "allow",
        "validate_assignment": True,
    }

    @computed_field
    @property
    def partition_date(self) -> str:
        """Date partition key derived from scraped_at (YYYY-MM-DD)."""
        return self.lineage.scraped_at.strftime("%Y-%m-%d")

    @computed_field
    @property
    def partition_source(self) -> str:
        """Source partition key."""
        return self.lineage.source_name

    def mark_invalid(self, reason: str) -> None:
        """Mark record as invalid with reason."""
        self.is_valid = False
        self.validation_errors.append(reason)

    def to_bronze_dict(self) -> dict[str, Any]:
        """Export for Bronze layer (includes all fields)."""
        return self.model_dump(mode="json")

    def to_silver_dict(self) -> dict[str, Any]:
        """Export for Silver layer (excludes extra_fields, flattens lineage)."""
        data = self.model_dump(mode="json", exclude={"extra_fields"})
        # Flatten lineage for easier querying
        lineage = data.pop("lineage")
        for key, value in lineage.items():
            data[f"lineage_{key}"] = value
        return data


class QuarantineRecord(BaseModel):
    """Record that failed validation, stored in quarantine for review."""

    quarantine_id: UUID = Field(default_factory=uuid4)
    original_data: dict[str, Any] = Field(
        ...,
        description="Original data that failed validation",
    )
    error_type: str = Field(
        ...,
        description="Classification of the error (parse_error, validation_error, etc.)",
    )
    error_message: str = Field(
        ...,
        description="Human-readable error description",
    )
    error_details: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional error context (stack trace, field info, etc.)",
    )
    lineage: LineageInfo
    quarantined_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    model_config = {"extra": "allow"}
