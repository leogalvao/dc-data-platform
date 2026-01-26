"""
PDF text extraction with OCR fallback for scanned documents.

Uses pdfplumber for native text extraction and pytesseract for OCR
on pages that appear to be scanned (low text content).
"""

from __future__ import annotations

import io
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from PIL import Image

logger = logging.getLogger(__name__)


@dataclass
class PageExtractionResult:
    """Result of extracting text from a single page."""

    page_number: int
    text: str
    method: str  # "native" or "ocr"
    char_count: int = 0
    ocr_confidence: float | None = None

    def __post_init__(self):
        self.char_count = len(self.text)


@dataclass
class PDFExtractionResult:
    """Result of extracting text from an entire PDF."""

    pages: list[PageExtractionResult] = field(default_factory=list)
    total_pages: int = 0
    pages_native: int = 0
    pages_ocr: int = 0
    extraction_method: str = "native"  # "native", "ocr", or "mixed"
    average_ocr_confidence: float | None = None

    @property
    def full_text(self) -> str:
        """Concatenate all page texts."""
        return "\n\n".join(page.text for page in self.pages if page.text)

    @property
    def total_chars(self) -> int:
        """Total character count across all pages."""
        return sum(page.char_count for page in self.pages)

    def compute_stats(self) -> None:
        """Compute aggregate statistics after extraction."""
        self.total_pages = len(self.pages)
        self.pages_native = sum(1 for p in self.pages if p.method == "native")
        self.pages_ocr = sum(1 for p in self.pages if p.method == "ocr")

        # Determine overall extraction method
        if self.pages_ocr == 0:
            self.extraction_method = "native"
        elif self.pages_native == 0:
            self.extraction_method = "ocr"
        else:
            self.extraction_method = "mixed"

        # Compute average OCR confidence
        ocr_confidences = [
            p.ocr_confidence for p in self.pages
            if p.method == "ocr" and p.ocr_confidence is not None
        ]
        if ocr_confidences:
            self.average_ocr_confidence = sum(ocr_confidences) / len(ocr_confidences)


class PDFProcessor:
    """
    Extract text from PDFs with OCR fallback for scanned pages.

    Uses pdfplumber for native text extraction. If a page has fewer than
    MIN_CHARS_THRESHOLD characters, it's assumed to be scanned and OCR
    is attempted using pytesseract.

    Usage:
        processor = PDFProcessor()
        result = processor.extract(pdf_bytes)
        print(result.full_text)
        print(f"Pages: {result.total_pages}, OCR pages: {result.pages_ocr}")
    """

    MIN_CHARS_THRESHOLD = 50  # Pages with fewer chars are assumed scanned
    DPI = 300  # Resolution for PDF to image conversion

    def __init__(
        self,
        min_chars_threshold: int = MIN_CHARS_THRESHOLD,
        dpi: int = DPI,
        ocr_lang: str = "eng",
    ):
        """
        Initialize the PDF processor.

        Args:
            min_chars_threshold: Minimum characters for a page to be considered
                                 as having native text. Below this, OCR is used.
            dpi: DPI for converting PDF pages to images for OCR.
            ocr_lang: Language code for Tesseract OCR.
        """
        self.min_chars_threshold = min_chars_threshold
        self.dpi = dpi
        self.ocr_lang = ocr_lang
        self._pdfplumber = None
        self._pytesseract = None
        self._pdf2image = None

    def _lazy_import_pdfplumber(self):
        """Lazy import pdfplumber to avoid import errors if not installed."""
        if self._pdfplumber is None:
            try:
                import pdfplumber
                self._pdfplumber = pdfplumber
            except ImportError as e:
                raise ImportError(
                    "pdfplumber is required for PDF processing. "
                    "Install with: pip install pdfplumber"
                ) from e
        return self._pdfplumber

    def _lazy_import_ocr(self):
        """Lazy import OCR dependencies."""
        if self._pytesseract is None:
            try:
                import pytesseract
                self._pytesseract = pytesseract
            except ImportError as e:
                raise ImportError(
                    "pytesseract is required for OCR. "
                    "Install with: pip install pytesseract"
                ) from e

        if self._pdf2image is None:
            try:
                from pdf2image import convert_from_bytes
                self._pdf2image = convert_from_bytes
            except ImportError as e:
                raise ImportError(
                    "pdf2image is required for OCR. "
                    "Install with: pip install pdf2image"
                ) from e

        return self._pytesseract, self._pdf2image

    def extract(self, pdf_bytes: bytes) -> PDFExtractionResult:
        """
        Extract text from a PDF with OCR fallback.

        Args:
            pdf_bytes: Raw PDF file content as bytes.

        Returns:
            PDFExtractionResult with extracted text and metadata.
        """
        pdfplumber = self._lazy_import_pdfplumber()
        result = PDFExtractionResult()

        try:
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                for page_num, page in enumerate(pdf.pages, start=1):
                    page_result = self._extract_page(
                        page, page_num, pdf_bytes
                    )
                    result.pages.append(page_result)

        except Exception as e:
            logger.error(f"Failed to process PDF: {e}")
            raise

        result.compute_stats()
        return result

    def _extract_page(
        self,
        page,
        page_num: int,
        pdf_bytes: bytes,
    ) -> PageExtractionResult:
        """
        Extract text from a single page, using OCR if needed.

        Args:
            page: pdfplumber Page object
            page_num: 1-based page number
            pdf_bytes: Full PDF bytes (needed for OCR conversion)

        Returns:
            PageExtractionResult for this page.
        """
        # Try native extraction first
        text = page.extract_text() or ""
        text = text.strip()

        # If sufficient text, use native extraction
        if len(text) >= self.min_chars_threshold:
            return PageExtractionResult(
                page_number=page_num,
                text=text,
                method="native",
            )

        # Otherwise, try OCR
        logger.debug(f"Page {page_num} has {len(text)} chars, attempting OCR")
        try:
            ocr_text, confidence = self._ocr_page(pdf_bytes, page_num)
            if ocr_text:
                return PageExtractionResult(
                    page_number=page_num,
                    text=ocr_text,
                    method="ocr",
                    ocr_confidence=confidence,
                )
        except Exception as e:
            logger.warning(f"OCR failed for page {page_num}: {e}")

        # Fall back to whatever native text we got
        return PageExtractionResult(
            page_number=page_num,
            text=text,
            method="native",
        )

    def _ocr_page(
        self,
        pdf_bytes: bytes,
        page_num: int,
    ) -> tuple[str, float | None]:
        """
        Perform OCR on a specific page.

        Args:
            pdf_bytes: Full PDF bytes
            page_num: 1-based page number to OCR

        Returns:
            Tuple of (extracted_text, confidence_score)
        """
        pytesseract, convert_from_bytes = self._lazy_import_ocr()

        # Convert just this page to image
        images = convert_from_bytes(
            pdf_bytes,
            dpi=self.dpi,
            first_page=page_num,
            last_page=page_num,
        )

        if not images:
            return "", None

        image = images[0]

        # Get OCR data with confidence scores
        ocr_data = pytesseract.image_to_data(
            image,
            lang=self.ocr_lang,
            output_type=pytesseract.Output.DICT,
        )

        # Extract text and compute average confidence
        words = []
        confidences = []

        for i, word in enumerate(ocr_data["text"]):
            conf = ocr_data["conf"][i]
            if word.strip() and conf > 0:  # conf -1 means no text detected
                words.append(word)
                confidences.append(conf)

        text = " ".join(words)
        avg_confidence = (
            sum(confidences) / len(confidences) / 100.0
            if confidences else None
        )

        return text, avg_confidence

    def extract_native_only(self, pdf_bytes: bytes) -> PDFExtractionResult:
        """
        Extract text using only native extraction (no OCR).

        Useful when OCR dependencies aren't available or for faster processing.

        Args:
            pdf_bytes: Raw PDF file content as bytes.

        Returns:
            PDFExtractionResult with extracted text.
        """
        pdfplumber = self._lazy_import_pdfplumber()
        result = PDFExtractionResult()

        try:
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                for page_num, page in enumerate(pdf.pages, start=1):
                    text = page.extract_text() or ""
                    result.pages.append(PageExtractionResult(
                        page_number=page_num,
                        text=text.strip(),
                        method="native",
                    ))

        except Exception as e:
            logger.error(f"Failed to process PDF: {e}")
            raise

        result.compute_stats()
        return result
