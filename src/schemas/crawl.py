"""Raw crawl record schema for web crawler output (JSONL storage)."""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class PageType(str, Enum):
    """Type of crawled content."""

    HTML = "html"
    PDF = "pdf"
    JSON = "json"
    OTHER = "other"


class CrawlRecord(BaseModel):
    """
    Raw crawl record for JSONL storage.

    Stores only raw extracted data + metadata, no tokenization/chunking/embedding.
    Another downstream script handles transformation.
    """

    # Source tracking
    source_seed: str = Field(
        ...,
        description="Original seed URL that initiated this crawl path",
    )
    crawl_depth: int = Field(
        ...,
        ge=0,
        le=3,
        description="Depth level from seed (0=seed itself, max=3)",
    )

    # URL info
    url_requested: str = Field(
        ...,
        description="URL that was requested",
    )
    final_url: str = Field(
        ...,
        description="Final URL after redirects (canonicalized)",
    )

    # Fetch metadata
    fetched_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="ISO-8601 UTC timestamp of fetch",
    )
    http_status: int | None = Field(
        default=None,
        description="HTTP status code (200, 403, 404, etc.)",
    )
    content_type: str | None = Field(
        default=None,
        description="MIME content type from response headers",
    )
    page_type: PageType = Field(
        default=PageType.OTHER,
        description="Classified page type: html/pdf/json/other",
    )

    # Raw content
    raw_content: str | None = Field(
        default=None,
        description=(
            "For HTML: visible text extracted; "
            "For JSON: raw JSON string; "
            "For PDF: extracted_text (full, no chunking)"
        ),
    )
    raw_html_snapshot: str | None = Field(
        default=None,
        description="Optional: raw HTML source if captured",
    )
    raw_bytes_path: str | None = Field(
        default=None,
        description="Path to binary file if stored separately (e.g., PDF)",
    )

    # HTML-specific metadata
    title: str | None = Field(default=None, description="Page title if available")
    meta_description: str | None = Field(
        default=None,
        description="Meta description tag content",
    )
    headings: list[str] = Field(
        default_factory=list,
        description="H1-H3 heading text (in order)",
    )

    # Link extraction
    out_links_internal: list[str] = Field(
        default_factory=list,
        description="Internal links found (within allowlist)",
    )
    out_links_external: list[str] = Field(
        default_factory=list,
        description="External links found (outside allowlist, not followed)",
    )
    pdf_links: list[str] = Field(
        default_factory=list,
        description="PDF links discovered on this page",
    )

    # PDF-specific metadata
    ocr_performed: bool = Field(
        default=False,
        description="Whether OCR was used for text extraction",
    )
    pdf_page_count: int | None = Field(
        default=None,
        description="Number of pages in PDF",
    )
    pdf_pages_native: int | None = Field(
        default=None,
        description="Pages extracted via native text",
    )
    pdf_pages_ocr: int | None = Field(
        default=None,
        description="Pages requiring OCR",
    )
    ocr_confidence: float | None = Field(
        default=None,
        ge=0.0,
        le=1.0,
        description="Average OCR confidence if OCR was performed",
    )

    # PDF parsed content (for future tokenization)
    pdf_pages: list[dict[str, Any]] | None = Field(
        default=None,
        description=(
            "Page-by-page content for granular processing. "
            "Each dict: {page_num, text, char_count, method}"
        ),
    )
    pdf_sections: list[dict[str, str]] | None = Field(
        default=None,
        description=(
            "Extracted sections/headings from PDF. "
            "Each dict: {heading, content, page_num}"
        ),
    )
    pdf_metadata: dict[str, Any] | None = Field(
        default=None,
        description="PDF document metadata (title, author, creation_date, etc.)",
    )
    pdf_tables: list[dict[str, Any]] | None = Field(
        default=None,
        description="Extracted tables from PDF (list of dicts with headers and rows)",
    )

    # ArcGIS REST-specific
    arcgis_service_name: str | None = Field(
        default=None,
        description="ArcGIS service name if REST endpoint",
    )
    arcgis_layer_id: int | None = Field(
        default=None,
        description="ArcGIS layer ID",
    )
    arcgis_geometry_type: str | None = Field(
        default=None,
        description="Geometry type (point, polygon, etc.)",
    )
    arcgis_fields: list[str] | None = Field(
        default=None,
        description="Field names from layer metadata",
    )
    arcgis_record_count: int | None = Field(
        default=None,
        description="Total record count if discoverable",
    )
    arcgis_page_index: int | None = Field(
        default=None,
        description="Page index for paginated results",
    )

    # Error handling
    extraction_errors: list[str] = Field(
        default_factory=list,
        description="Errors encountered during extraction",
    )
    notes: str | None = Field(
        default=None,
        description="Freeform notes (e.g., auth required, robots blocked)",
    )

    model_config = {
        "extra": "allow",
        "json_encoders": {
            datetime: lambda v: v.isoformat(),
        },
    }

    def to_jsonl_dict(self) -> dict[str, Any]:
        """Export as dict suitable for JSONL serialization."""
        return self.model_dump(mode="json", exclude_none=False)
