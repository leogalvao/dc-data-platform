"""Textual record schemas for document/snippet data."""

from __future__ import annotations

from datetime import date
from enum import Enum
from typing import Any

from pydantic import Field

from .base import BaseRecord


class DocumentType(str, Enum):
    """Classification of document types."""

    DESCRIPTION = "description"
    LISTING = "listing"
    CONTRACT = "contract"
    REPORT = "report"
    WEBPAGE = "webpage"
    PDF = "pdf"
    OTHER = "other"


class TextualRecord(BaseRecord):
    """
    Base class for text-heavy records (documents, descriptions, etc.).

    Use this for content that is primarily unstructured text,
    potentially with some structured metadata.
    """

    document_type: DocumentType = Field(
        ...,
        description="Classification of the document",
    )
    title: str | None = Field(default=None, description="Document title if available")
    content: str = Field(
        ...,
        min_length=1,
        description="Main text content",
    )
    content_length: int = Field(
        default=0,
        description="Character count of content",
    )
    word_count: int = Field(
        default=0,
        description="Word count of content",
    )
    language: str = Field(
        default="en",
        description="ISO 639-1 language code",
    )

    def model_post_init(self, __context: Any) -> None:
        """Compute content metrics after initialization."""
        if self.content and not self.content_length:
            object.__setattr__(self, "content_length", len(self.content))
        if self.content and not self.word_count:
            object.__setattr__(self, "word_count", len(self.content.split()))


class DocumentRecord(TextualRecord):
    """Full document record with additional metadata."""

    # Source info
    source_document_id: str | None = Field(
        default=None,
        description="Original document ID from source",
    )
    source_url: str | None = Field(default=None, description="URL where document was found")

    # Classification
    category: str | None = Field(default=None, description="Domain category")
    subcategory: str | None = Field(default=None)
    tags: list[str] = Field(default_factory=list)

    # Dates
    publish_date: date | None = Field(default=None)
    effective_date: date | None = Field(default=None)
    expiration_date: date | None = Field(default=None)

    # Related entities (for linking to tabular records)
    related_entity_type: str | None = Field(
        default=None,
        description="Type of related entity (contract, property, etc.)",
    )
    related_entity_id: str | None = Field(
        default=None,
        description="ID of related entity",
    )

    # Content sections (for structured documents)
    sections: list[dict[str, str]] = Field(
        default_factory=list,
        description="List of {heading, content} dicts for sectioned documents",
    )

    # Extracted entities (optional NER results)
    extracted_entities: dict[str, list[str]] = Field(
        default_factory=dict,
        description="Named entities extracted: {type: [values]}",
    )


class ListingDescriptionRecord(TextualRecord):
    """Property listing description (extracted from real estate listings)."""

    document_type: DocumentType = DocumentType.LISTING

    # Link to property
    property_id: str | None = Field(default=None, description="ID of related PropertyRecord")
    listing_id: str | None = Field(default=None, description="MLS or source listing ID")

    # Property context
    address: str | None = Field(default=None)
    city: str | None = Field(default=None)
    state: str | None = Field(default=None)
    zip_code: str | None = Field(default=None)

    # Extracted features (from NLP if available)
    mentioned_features: list[str] = Field(
        default_factory=list,
        description="Features mentioned in description",
    )
    mentioned_amenities: list[str] = Field(
        default_factory=list,
        description="Amenities mentioned",
    )
    sentiment_score: float | None = Field(
        default=None,
        ge=-1,
        le=1,
        description="Sentiment analysis score if computed",
    )


class ContractDescriptionRecord(TextualRecord):
    """Contract description/scope of work text."""

    document_type: DocumentType = DocumentType.CONTRACT

    # Link to contract
    contract_id: str | None = Field(
        default=None,
        description="ID of related ContractRecord",
    )
    contract_number: str | None = Field(default=None)

    # Contract context
    agency_name: str | None = Field(default=None)
    supplier_name: str | None = Field(default=None)
    fiscal_year: int | None = Field(default=None)

    # Extracted info
    deliverables: list[str] = Field(
        default_factory=list,
        description="Extracted deliverables from scope",
    )
    requirements: list[str] = Field(
        default_factory=list,
        description="Extracted requirements",
    )


class ExtractionMethod(str, Enum):
    """Method used to extract text from PDF."""

    NATIVE = "native"  # Text extracted directly from PDF
    OCR = "ocr"  # Text extracted via OCR
    MIXED = "mixed"  # Some pages native, some OCR


class ContractAttachmentRecord(TextualRecord):
    """Contract attachment (PDF) with extracted and tokenized text."""

    document_type: DocumentType = DocumentType.PDF

    # Parent contract linkage
    parent_contract_id: str | None = Field(
        default=None,
        description="ID of the parent ContractRecord",
    )
    parent_contract_number: str | None = Field(
        default=None,
        description="Contract number from parent record",
    )

    # Attachment metadata
    attachment_url: str | None = Field(
        default=None,
        description="URL where attachment was downloaded from",
    )
    original_filename: str | None = Field(
        default=None,
        description="Original filename of the PDF",
    )
    page_count: int = Field(
        default=0,
        ge=0,
        description="Number of pages in the PDF",
    )

    # Extraction info
    extraction_method: ExtractionMethod = Field(
        default=ExtractionMethod.NATIVE,
        description="How text was extracted (native/ocr/mixed)",
    )
    ocr_confidence: float | None = Field(
        default=None,
        ge=0.0,
        le=1.0,
        description="Average OCR confidence score if OCR was used",
    )
    pages_native: int = Field(
        default=0,
        ge=0,
        description="Number of pages with native text extraction",
    )
    pages_ocr: int = Field(
        default=0,
        ge=0,
        description="Number of pages requiring OCR",
    )

    # Tokenization results
    tokens: list[str] = Field(
        default_factory=list,
        description="Word tokens extracted from text",
    )
    token_count: int = Field(
        default=0,
        ge=0,
        description="Number of tokens",
    )
    sentences: list[str] = Field(
        default_factory=list,
        description="Sentences extracted from text",
    )
    sentence_count: int = Field(
        default=0,
        ge=0,
        description="Number of sentences",
    )

    # N-gram features (optional)
    bigrams: list[str] = Field(
        default_factory=list,
        description="Common bigrams (word pairs)",
    )
    trigrams: list[str] = Field(
        default_factory=list,
        description="Common trigrams (word triples)",
    )
