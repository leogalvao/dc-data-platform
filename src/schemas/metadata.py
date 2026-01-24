"""Metadata schemas for run configuration and reporting."""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class RunStatus(str, Enum):
    """Status of a scraping run."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    PARTIAL = "partial"  # Some records succeeded, some failed
    CANCELLED = "cancelled"


class SourceType(str, Enum):
    """Type of data source."""

    REST_API = "rest_api"
    GRAPHQL = "graphql"
    WEB_SCRAPE = "web_scrape"
    CSV_DOWNLOAD = "csv_download"
    DATABASE = "database"


class SourceConfig(BaseModel):
    """Configuration for a data source."""

    name: str = Field(..., description="Unique source identifier")
    display_name: str = Field(..., description="Human-readable name")
    description: str = Field(default="", description="What this source provides")
    source_type: SourceType = Field(..., description="Type of source")
    enabled: bool = Field(default=True)

    # Connection
    base_url: str = Field(..., description="Base URL or endpoint")
    api_key_env: str | None = Field(
        default=None,
        description="Environment variable name for API key",
    )
    requires_auth: bool = Field(default=False)

    # Rate limiting
    requests_per_second: float = Field(
        default=1.0,
        gt=0,
        description="Max requests per second",
    )
    rate_limit_wait_seconds: int = Field(
        default=60,
        description="Seconds to wait when rate limited (429)",
    )
    max_retries: int = Field(default=3, ge=0)
    retry_backoff_factor: float = Field(
        default=2.0,
        description="Exponential backoff multiplier",
    )

    # Concurrency
    max_workers: int = Field(
        default=1,
        ge=1,
        description="Max concurrent requests/workers",
    )
    batch_size: int = Field(
        default=100,
        ge=1,
        description="Records per batch/page",
    )
    timeout_seconds: int = Field(default=60, ge=1)

    # Schema info
    schema_version: str = Field(default="1.0.0")
    adapter_class: str = Field(
        ...,
        description="Fully qualified adapter class name",
    )
    output_record_types: list[str] = Field(
        default_factory=list,
        description="Record types this source produces",
    )

    # Storage
    bronze_partition_by: list[str] = Field(
        default_factory=lambda: ["source", "date"],
        description="Partition columns for bronze layer",
    )
    silver_partition_by: list[str] = Field(
        default_factory=lambda: ["category", "date"],
        description="Partition columns for silver layer",
    )

    # Extra config
    extra: dict[str, Any] = Field(
        default_factory=dict,
        description="Source-specific configuration",
    )

    model_config = {"extra": "allow"}


class RunMetadata(BaseModel):
    """Metadata for a single scraping run."""

    run_id: UUID = Field(default_factory=uuid4)
    source_name: str = Field(..., description="Source being scraped")
    status: RunStatus = Field(default=RunStatus.PENDING)

    # Timing
    started_at: datetime | None = Field(default=None)
    completed_at: datetime | None = Field(default=None)
    duration_seconds: float | None = Field(default=None)

    # Config snapshot
    config_snapshot: dict[str, Any] = Field(
        default_factory=dict,
        description="Copy of SourceConfig used for this run",
    )

    # Scope
    incremental: bool = Field(
        default=False,
        description="Whether this was an incremental (vs full) run",
    )
    watermark_start: datetime | None = Field(
        default=None,
        description="Start of time window for incremental",
    )
    watermark_end: datetime | None = Field(
        default=None,
        description="End of time window for incremental",
    )

    # Worker info
    worker_id: str | None = Field(default=None)
    host_name: str | None = Field(default=None)
    python_version: str | None = Field(default=None)

    def start(self) -> None:
        """Mark run as started."""
        self.status = RunStatus.RUNNING
        self.started_at = datetime.now(timezone.utc)

    def complete(self, status: RunStatus = RunStatus.COMPLETED) -> None:
        """Mark run as completed."""
        self.status = status
        self.completed_at = datetime.now(timezone.utc)
        if self.started_at:
            delta = self.completed_at - self.started_at
            self.duration_seconds = delta.total_seconds()


class RunReport(BaseModel):
    """Report generated after a scraping run completes."""

    run_id: UUID = Field(...)
    source_name: str = Field(...)
    metadata: RunMetadata = Field(...)

    # Record counts
    records_discovered: int = Field(default=0, description="Total records found")
    records_fetched: int = Field(default=0, description="Records successfully fetched")
    records_parsed: int = Field(default=0, description="Records successfully parsed")
    records_validated: int = Field(default=0, description="Records that passed validation")
    records_written_bronze: int = Field(default=0, description="Written to bronze layer")
    records_written_silver: int = Field(default=0, description="Written to silver layer")
    records_quarantined: int = Field(default=0, description="Sent to quarantine")
    records_deduplicated: int = Field(default=0, description="Duplicates skipped")

    # Error summary
    errors_fetch: int = Field(default=0, description="Fetch errors (HTTP, timeout)")
    errors_parse: int = Field(default=0, description="Parse errors")
    errors_validation: int = Field(default=0, description="Validation failures")
    errors_storage: int = Field(default=0, description="Storage write errors")
    error_samples: list[dict[str, Any]] = Field(
        default_factory=list,
        max_length=10,
        description="Sample of errors for debugging",
    )

    # Performance
    avg_fetch_time_ms: float | None = Field(default=None)
    avg_parse_time_ms: float | None = Field(default=None)
    total_bytes_fetched: int = Field(default=0)
    requests_made: int = Field(default=0)
    rate_limit_hits: int = Field(default=0)

    # Data quality
    null_rate_by_field: dict[str, float] = Field(
        default_factory=dict,
        description="Percentage of nulls per field",
    )
    schema_drift_detected: bool = Field(
        default=False,
        description="Whether new fields appeared",
    )
    new_fields_detected: list[str] = Field(
        default_factory=list,
        description="Fields not in schema",
    )

    # Output locations
    bronze_path: str | None = Field(default=None)
    silver_path: str | None = Field(default=None)
    quarantine_path: str | None = Field(default=None)
    report_path: str | None = Field(default=None)

    @property
    def success_rate(self) -> float:
        """Percentage of records successfully processed."""
        if self.records_discovered == 0:
            return 0.0
        return (self.records_written_silver / self.records_discovered) * 100

    @property
    def error_rate(self) -> float:
        """Percentage of records with errors."""
        if self.records_discovered == 0:
            return 0.0
        total_errors = (
            self.errors_fetch
            + self.errors_parse
            + self.errors_validation
            + self.errors_storage
        )
        return (total_errors / self.records_discovered) * 100

    def to_summary_dict(self) -> dict[str, Any]:
        """Generate summary dict for logging/display."""
        return {
            "run_id": str(self.run_id),
            "source": self.source_name,
            "status": self.metadata.status.value,
            "duration_seconds": self.metadata.duration_seconds,
            "records_discovered": self.records_discovered,
            "records_written": self.records_written_silver,
            "records_quarantined": self.records_quarantined,
            "success_rate": f"{self.success_rate:.1f}%",
            "error_rate": f"{self.error_rate:.1f}%",
        }


class Checkpoint(BaseModel):
    """Checkpoint for incremental loading and resume capability."""

    source_name: str = Field(...)
    checkpoint_id: UUID = Field(default_factory=uuid4)

    # Watermarks
    last_successful_run_id: UUID | None = Field(default=None)
    last_run_completed_at: datetime | None = Field(default=None)
    high_watermark: datetime | None = Field(
        default=None,
        description="Latest timestamp successfully processed",
    )

    # Position tracking (for offset-based pagination)
    last_offset: int = Field(default=0)
    last_page: int = Field(default=0)
    last_cursor: str | None = Field(default=None)

    # Dedup state
    seen_hashes: set[str] = Field(
        default_factory=set,
        description="Hashes of records already processed",
    )
    seen_ids: set[str] = Field(
        default_factory=set,
        description="Source IDs already processed",
    )

    # Updated timestamp
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    model_config = {"arbitrary_types_allowed": True}

    def update_watermark(self, timestamp: datetime) -> None:
        """Update high watermark if newer."""
        if self.high_watermark is None or timestamp > self.high_watermark:
            self.high_watermark = timestamp
        self.updated_at = datetime.now(timezone.utc)

    def mark_seen(self, record_hash: str, record_id: str | None = None) -> bool:
        """
        Mark a record as seen. Returns True if new, False if duplicate.
        """
        if record_hash in self.seen_hashes:
            return False
        self.seen_hashes.add(record_hash)
        if record_id:
            self.seen_ids.add(record_id)
        return True
