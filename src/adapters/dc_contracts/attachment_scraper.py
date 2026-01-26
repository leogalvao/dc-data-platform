"""
Contract Attachment Scraper using Playwright.

Scrapes contract detail pages at contracts.ocp.dc.gov to discover and
download PDF attachments, then extracts and tokenizes their text content.

The detail pages are JavaScript SPAs requiring a headless browser to render.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import re
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

from ...core.base_scraper import BaseScraper, ScraperResult
from ...core.registry import ScraperRegistry, register_scraper
from ...schemas.base import LineageInfo, RawPayload
from ...schemas.metadata import RunStatus, SourceConfig, SourceType
from ...schemas.textual import ContractAttachmentRecord, ExtractionMethod

logger = logging.getLogger(__name__)


def get_default_config() -> SourceConfig:
    """Get default configuration for contract attachment scraper."""
    return SourceConfig(
        name="dc_contract_attachments",
        display_name="DC Contract Attachments",
        description="PDF attachments from DC government contract detail pages",
        source_type=SourceType.WEB_SCRAPE,
        base_url="https://contracts.ocp.dc.gov",
        requires_auth=False,
        requests_per_second=1.0,  # Be gentle with scraping
        max_workers=3,
        batch_size=10,
        timeout_seconds=60,
        max_retries=3,
        adapter_class="src.adapters.dc_contracts.attachment_scraper.ContractAttachmentAdapter",
        output_record_types=["ContractAttachmentRecord"],
        extra={
            "page_load_timeout": 30000,  # ms
            "attachment_selector": "a[href*='.pdf'], a[href*='/download/']",
            "headless": True,
        },
    )


@register_scraper("dc_contract_attachments")
class ContractAttachmentAdapter(BaseScraper):
    """
    Playwright-based scraper for contract PDF attachments.

    Workflow:
    1. discover(): Load contracts with contract_url from silver layer
    2. fetch(): Use Playwright to render JS page and find PDF links
    3. parse(): Download PDFs and extract text via PDFProcessor
    4. normalize(): Create ContractAttachmentRecord with tokenized text
    """

    SOURCE_NAME = "dc_contract_attachments"
    SOURCE_VERSION = "1.0.0"
    SUPPORTED_RECORD_TYPES = [ContractAttachmentRecord]

    def __init__(self, config: SourceConfig, **kwargs):
        super().__init__(config, **kwargs)
        self._browser = None
        self._context = None
        self._playwright = None
        self._pdf_processor = None
        self._tokenizer = None
        self._contracts_cache: list[dict[str, Any]] = []

    # =========================================================================
    # Lifecycle
    # =========================================================================

    def setup(self) -> None:
        """Initialize Playwright browser and processing utilities."""
        super().setup()
        # Lazy init - will be created in async context

    def teardown(self) -> None:
        """Clean up browser resources."""
        # Browser cleanup happens in run_async
        super().teardown()

    def _init_processors(self) -> None:
        """Initialize PDF processor and tokenizer."""
        if self._pdf_processor is None:
            from ...utils.pdf_processor import PDFProcessor
            self._pdf_processor = PDFProcessor()

        if self._tokenizer is None:
            from ...utils.tokenizer import ContractTokenizer
            self._tokenizer = ContractTokenizer(
                remove_stopwords=True,
                extract_ngrams=True,
                max_ngrams=50,
            )

    # =========================================================================
    # Discovery
    # =========================================================================

    def discover(self) -> Iterator[dict[str, Any]]:
        """
        Discover contracts with detail page URLs.

        Loads from silver layer parquet files or from provided list.
        """
        # Try to load contracts from silver layer
        contracts = self._load_contracts_from_silver()

        if not contracts:
            self._logger.warning("No contracts with URLs found in silver layer")
            return

        self._logger.info(f"Discovered {len(contracts)} contracts with detail URLs")

        for contract in contracts:
            yield contract

    def _load_contracts_from_silver(self) -> list[dict[str, Any]]:
        """Load contracts with contract_url from silver layer."""
        if self._contracts_cache:
            return self._contracts_cache

        try:
            import pyarrow.parquet as pq

            silver_path = Path("data/silver/source=dc_contracts")
            if not silver_path.exists():
                self._logger.warning(f"Silver path not found: {silver_path}")
                return []

            # Find all parquet files and read them individually to avoid schema conflicts
            parquet_files = list(silver_path.glob("**/*.parquet"))
            if not parquet_files:
                self._logger.warning("No parquet files found in silver layer")
                return []

            # Sort by modification time (newest first) and read files with contract_url
            parquet_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)

            contracts = []
            seen_contract_numbers = set()

            for pq_file in parquet_files:
                try:
                    table = pq.read_table(pq_file)
                    df = table.to_pandas()

                    # Skip files without contract_url
                    if "contract_url" not in df.columns:
                        continue

                    df_with_url = df[df["contract_url"].notna() & (df["contract_url"] != "")]

                    for _, row in df_with_url.iterrows():
                        contract_num = row.get("contract_number")
                        # Deduplicate by contract number
                        if contract_num and contract_num in seen_contract_numbers:
                            continue
                        if contract_num:
                            seen_contract_numbers.add(contract_num)

                        contracts.append({
                            "contract_id": row.get("record_id", row.get("source_record_id")),
                            "contract_number": contract_num,
                            "contract_url": row["contract_url"],
                            "agency_name": row.get("agency_name"),
                            "supplier_name": row.get("supplier_name"),
                        })

                except Exception as e:
                    self._logger.debug(f"Skipping {pq_file.name}: {e}")
                    continue

            self._contracts_cache = contracts
            self._logger.info(f"Loaded {len(contracts)} contracts with URLs from silver layer")
            return contracts

        except Exception as e:
            self._logger.error(f"Failed to load contracts from silver: {e}")
            return []

    # =========================================================================
    # Async Implementation
    # =========================================================================

    async def _init_browser(self) -> None:
        """Initialize Playwright browser."""
        if self._browser is not None:
            return

        try:
            from playwright.async_api import async_playwright
            self._playwright = await async_playwright().start()
            self._browser = await self._playwright.chromium.launch(
                headless=self.config.extra.get("headless", True),
            )
            self._context = await self._browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
            )
        except ImportError as e:
            raise ImportError(
                "Playwright is required. Install with: pip install playwright && playwright install chromium"
            ) from e

    async def _close_browser(self) -> None:
        """Close Playwright browser."""
        if self._context:
            await self._context.close()
            self._context = None
        if self._browser:
            await self._browser.close()
            self._browser = None
        if self._playwright:
            await self._playwright.stop()
            self._playwright = None

    async def discover_async(self) -> AsyncIterator[dict[str, Any]]:
        """Async version of discover."""
        for item in self.discover():
            yield item

    async def fetch_async(self, item: dict[str, Any]) -> RawPayload | None:
        """
        Fetch contract detail page and discover PDF attachments.

        Uses Playwright to render the JavaScript SPA, then extracts
        PDF download links from the rendered page.
        """
        await self._init_browser()

        contract_url = item.get("contract_url")
        if not contract_url:
            return None

        page = None
        try:
            page = await self._context.new_page()

            # Navigate to contract detail page
            timeout = self.config.extra.get("page_load_timeout", 30000)
            await page.goto(contract_url, timeout=timeout, wait_until="networkidle")

            # Wait for content to load
            await page.wait_for_load_state("domcontentloaded")
            await asyncio.sleep(1)  # Give JS time to render

            # Find PDF attachment links
            selector = self.config.extra.get(
                "attachment_selector",
                "a[href*='.pdf'], a[href*='/download/']",
            )
            pdf_links = await page.query_selector_all(selector)

            attachments = []
            for link in pdf_links:
                href = await link.get_attribute("href")
                text = await link.text_content()

                if href:
                    # Make absolute URL
                    full_url = urljoin(contract_url, href)
                    attachments.append({
                        "url": full_url,
                        "filename": self._extract_filename(full_url, text),
                        "link_text": text.strip() if text else None,
                    })

            # Create payload with attachment info
            payload_data = {
                "contract": item,
                "attachments": attachments,
                "page_url": contract_url,
            }

            import json
            return RawPayload(
                content_type="application/json",
                content=json.dumps(payload_data).encode("utf-8"),
                lineage=self._make_lineage(contract_url),
                fetch_status_code=200,
            )

        except Exception as e:
            self._logger.error(f"Failed to fetch {contract_url}: {e}")
            return None

        finally:
            if page:
                await page.close()

    def _extract_filename(self, url: str, link_text: str | None) -> str:
        """Extract filename from URL or link text."""
        # Try to get from URL path
        parsed = urlparse(url)
        path = parsed.path

        if path.endswith(".pdf"):
            return path.split("/")[-1]

        # Try from link text
        if link_text and ".pdf" in link_text.lower():
            # Extract filename-like pattern
            match = re.search(r"[\w\-_]+\.pdf", link_text, re.IGNORECASE)
            if match:
                return match.group()

        # Generate from URL hash
        url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
        return f"attachment_{url_hash}.pdf"

    def parse(self, payload: RawPayload) -> Iterator[dict[str, Any]]:
        """
        Parse payload and download PDF attachments.

        For each attachment found, downloads the PDF and extracts text.
        """
        import json

        try:
            data = json.loads(payload.content.decode("utf-8"))
        except Exception as e:
            self._logger.error(f"Failed to parse payload: {e}")
            return

        contract = data.get("contract", {})
        attachments = data.get("attachments", [])

        if not attachments:
            self._logger.debug(f"No attachments found for {contract.get('contract_number')}")
            return

        self._init_processors()

        for attachment in attachments:
            url = attachment.get("url")
            if not url:
                continue

            try:
                # Download PDF
                pdf_bytes = self._download_pdf(url)
                if not pdf_bytes:
                    continue

                # Extract text
                extraction_result = self._pdf_processor.extract(pdf_bytes)

                # Tokenize
                tokenization_result = self._tokenizer.tokenize(
                    extraction_result.full_text
                )

                yield {
                    "contract": contract,
                    "attachment": attachment,
                    "extraction": extraction_result,
                    "tokenization": tokenization_result,
                    "pdf_bytes": pdf_bytes,
                }

            except Exception as e:
                self._logger.error(f"Failed to process attachment {url}: {e}")
                continue

    def _download_pdf(self, url: str) -> bytes | None:
        """Download PDF from URL."""
        import httpx

        try:
            with httpx.Client(timeout=self.config.timeout_seconds) as client:
                response = client.get(url, follow_redirects=True)
                if response.status_code == 200:
                    return response.content
                else:
                    self._logger.warning(f"PDF download failed: {response.status_code} for {url}")
                    return None
        except Exception as e:
            self._logger.error(f"PDF download error for {url}: {e}")
            return None

    def validate(self, data: dict[str, Any]) -> tuple[bool, list[str]]:
        """Validate extracted attachment data."""
        errors = []

        contract = data.get("contract", {})
        if not contract.get("contract_number") and not contract.get("contract_id"):
            errors.append("Missing contract identifier")

        extraction = data.get("extraction")
        if extraction is None:
            errors.append("Missing extraction result")
        elif extraction.total_chars < 10:
            errors.append(f"Insufficient text extracted: {extraction.total_chars} chars")

        return len(errors) == 0, errors

    def normalize(self, data: dict[str, Any]) -> ContractAttachmentRecord:
        """Normalize to ContractAttachmentRecord."""
        contract = data.get("contract", {})
        attachment = data.get("attachment", {})
        extraction = data["extraction"]
        tokenization = data["tokenization"]

        # Map extraction method
        method_map = {
            "native": ExtractionMethod.NATIVE,
            "ocr": ExtractionMethod.OCR,
            "mixed": ExtractionMethod.MIXED,
        }
        extraction_method = method_map.get(
            extraction.extraction_method,
            ExtractionMethod.NATIVE,
        )

        record = ContractAttachmentRecord(
            lineage=self._make_lineage(attachment.get("url")),
            # Content from TextualRecord
            title=attachment.get("filename") or attachment.get("link_text"),
            content=extraction.full_text,
            # Parent contract linkage
            parent_contract_id=contract.get("contract_id"),
            parent_contract_number=contract.get("contract_number"),
            # Attachment metadata
            attachment_url=attachment.get("url"),
            original_filename=attachment.get("filename"),
            page_count=extraction.total_pages,
            # Extraction info
            extraction_method=extraction_method,
            ocr_confidence=extraction.average_ocr_confidence,
            pages_native=extraction.pages_native,
            pages_ocr=extraction.pages_ocr,
            # Tokenization
            tokens=tokenization.tokens[:10000],  # Limit tokens stored
            token_count=tokenization.token_count,
            sentences=tokenization.sentences[:1000],  # Limit sentences stored
            sentence_count=tokenization.sentence_count,
            bigrams=tokenization.bigrams,
            trigrams=tokenization.trigrams,
        )

        # Add content hash
        content_hash = hashlib.sha256(extraction.full_text.encode()).hexdigest()
        record.lineage = record.lineage.model_copy(
            update={"raw_hash": content_hash}
        )

        return record

    # =========================================================================
    # Main Entry Point
    # =========================================================================

    def run(self, *, max_records: int | None = None) -> ScraperResult:
        """Execute the scraping pipeline."""
        import asyncio

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(self.run_async(max_records=max_records))
        finally:
            loop.close()

    async def run_async(self, *, max_records: int | None = None) -> ScraperResult:
        """Execute the full scraping pipeline asynchronously."""
        result = ScraperResult()
        self.metadata.start()

        try:
            self.setup()
            await self._init_browser()

            # Process contracts
            async for item in self.discover_async():
                result.discovered_count += 1

                # Fetch (renders page, finds PDF links)
                payload = await self.fetch_async(item)
                if payload is None:
                    continue
                result.add_raw(payload)

                # Parse (downloads PDFs, extracts text)
                for data in self.parse(payload):
                    result.parsed_count += 1

                    # Validate
                    is_valid, errors = self.validate(data)
                    if not is_valid:
                        result.quarantine(
                            data={"contract": data.get("contract"), "errors": errors},
                            error_type="validation_error",
                            error_message="; ".join(errors),
                            lineage=self._make_lineage(
                                data.get("attachment", {}).get("url")
                            ),
                        )
                        continue

                    # Normalize
                    try:
                        record = self.normalize(data)
                    except Exception as e:
                        result.quarantine(
                            data={"contract": data.get("contract")},
                            error_type="normalization_error",
                            error_message=str(e),
                            lineage=self._make_lineage(),
                        )
                        continue

                    # Deduplicate
                    if not self.deduplicate(record):
                        result.deduplicated_count += 1
                        continue

                    # Transform and add
                    record = self.transform(record)
                    result.add_record(record)

                    if max_records and result.validated_count >= max_records:
                        break

                if max_records and result.validated_count >= max_records:
                    break

                # Rate limiting
                await asyncio.sleep(1.0 / self.config.requests_per_second)

            self.metadata.complete(RunStatus.COMPLETED)

        except Exception as e:
            self._logger.exception(f"Scraper failed: {e}")
            self.metadata.complete(RunStatus.FAILED)
            raise

        finally:
            await self._close_browser()
            self.teardown()

        return result

    def fetch(self, item: dict[str, Any]) -> RawPayload | None:
        """Sync wrapper for fetch_async."""
        import asyncio
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(self.fetch_async(item))
        finally:
            loop.close()


# =============================================================================
# Config Registration
# =============================================================================


def _register_config():
    """Register the default config for this scraper."""
    ScraperRegistry.register_config(get_default_config())


# Register on module import
_register_config()
