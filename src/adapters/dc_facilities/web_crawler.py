"""
DC Facilities Web Crawler with depth-limited crawling and PDF OCR.

Crawls DGS, DCPS, DC GIS, Quickbase, and ArcGIS REST endpoints up to depth 3.
Stores ONLY raw extracted data + metadata for downstream processing.
NO tokenization, chunking, embedding, or summarization.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
from collections.abc import AsyncIterator, Iterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse, urlunparse, parse_qs, urlencode

import aiohttp
from bs4 import BeautifulSoup

from ...core.base_scraper import BaseScraper, ScraperResult
from ...core.registry import register_scraper
from ...schemas.base import LineageInfo, RawPayload
from ...schemas.crawl import CrawlRecord, PageType
from ...schemas.metadata import RunStatus, SourceConfig, SourceType

logger = logging.getLogger(__name__)


# =============================================================================
# Seed URLs - Append-only list (do not remove/reorder existing entries)
# =============================================================================

SEED_URLS: list[str] = [
    # --- Existing seeds (if any) would be above this line ---
    # --- New seeds appended below ---
    "https://sites.google.com/dc.gov/dcps-facilities/home",
    "https://sites.google.com/dc.gov/dcps-facilities/modernizations",
    "https://sites.google.com/dc.gov/dcps-facilities/recentlycompleted-projects",
    "https://dgs.dc.gov/page/dcps-school-projects",
    "https://dgs.dc.gov/page/capital-construction-services-project-lifecycle-faq",
    "https://dme.dc.gov/service/public-education-facility-planning",
    "https://www.arcgis.com/home/item.html?id=ca04bb3e818141f6b5143364ba1c6d9b#data",
    "https://dck12.sharepoint.com/sites/TESTdcps/Shared%20Documents/Forms/AllItems.aspx?id=%2Fsites%2FTESTdcps%2FShared%20Documents%2FFAC%2FFPD%2F03%20Planning%2F01%20CIP%2FPACE%20Prioritization%20FY22%2F2022%2FPACE%20Overview%202022%20FINAL%2Epdf&parent=%2Fsites%2FTESTdcps%2FShared%20Documents%2FFAC%2FFPD%2F03%20Planning%2F01%20CIP%2FPACE%20Prioritization%20FY22%2F2022&p=true&ga=1",
    "https://services.arcgis.com/neT9SoYxizqTHZPH/arcgis/rest/services/ITSPE_View_07082025/FeatureServer/0/query?where=1%3D1&outFields=*&outSR=4326&f=json",
    "https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Administrative_Other_Boundaries_WebMercator/MapServer/53",
    "https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/DC_Basemap_LightGray_WebMercator/MapServer",
    "https://octo.quickbase.com/db/bpizdhed2?a=showpage&pageid=51&ifv=20",
]


# =============================================================================
# Domain Allowlist Configuration
# =============================================================================

DOMAIN_ALLOWLISTS: dict[str, list[str]] = {
    # sites.google.com: only DCPS facilities subsite
    "sites.google.com": ["/dc.gov/dcps-facilities/"],
    # dgs.dc.gov: only /page/ paths
    "dgs.dc.gov": ["/page/"],
    # dme.dc.gov: only /service/ paths
    "dme.dc.gov": ["/service/"],
    # arcgis.com: item pages and direct downloads
    "www.arcgis.com": ["/home/item.html"],
    # ArcGIS REST endpoints - handled specially (JSON crawl, not HTML)
    "services.arcgis.com": [],  # REST only, no HTML crawl
    "maps2.dcgis.dc.gov": [],  # REST only, no HTML crawl
    # Quickbase: priority domain, allow all subpages
    "octo.quickbase.com": ["/db/"],
    # SharePoint: allow the specific site
    "dck12.sharepoint.com": ["/sites/TESTdcps/"],
}

# Domains that are ArcGIS REST endpoints (fetch JSON, not HTML)
ARCGIS_REST_DOMAINS = {"services.arcgis.com", "maps2.dcgis.dc.gov"}

# Domains that require Playwright (Cloudflare-protected or JS-heavy)
PLAYWRIGHT_DOMAINS = {"dgs.dc.gov", "dme.dc.gov", "octo.quickbase.com"}

# OCR threshold: if native text extraction yields fewer chars, run OCR
OCR_CHAR_THRESHOLD = 100


def get_default_config() -> SourceConfig:
    """Get default configuration for DC facilities web crawler."""
    return SourceConfig(
        name="dc_facilities_crawler",
        display_name="DC Facilities Web Crawler",
        description=(
            "Depth-limited crawler for DGS, DCPS, DC GIS, Quickbase pages. "
            "Extracts raw text and PDFs with OCR. No tokenization."
        ),
        source_type=SourceType.WEB_SCRAPE,
        base_url="",  # Multiple seeds
        requires_auth=False,
        requests_per_second=1.0,
        max_workers=3,
        batch_size=50,
        timeout_seconds=60,
        max_retries=3,
        adapter_class="src.adapters.dc_facilities.web_crawler.DCFacilitiesCrawler",
        output_record_types=["CrawlRecord"],
        extra={
            "max_depth": 3,
            "ocr_char_threshold": OCR_CHAR_THRESHOLD,
            "headless": True,
            "user_agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36 DCDataPlatform/1.0"
            ),
            "output_dir": "data/crawl/dc_facilities",
        },
    )


@register_scraper("dc_facilities_crawler")
class DCFacilitiesCrawler(BaseScraper):
    """
    Depth-limited web crawler for DC Facilities data sources.

    Crawls:
    - HTML pages: extracts visible text, links, headings
    - PDFs: downloads and extracts text with OCR fallback
    - ArcGIS REST: fetches JSON with pagination

    Storage: JSONL with one CrawlRecord per fetch.
    """

    SOURCE_NAME = "dc_facilities_crawler"
    SOURCE_VERSION = "1.0.0"
    SUPPORTED_RECORD_TYPES = [CrawlRecord]

    def __init__(self, config: SourceConfig, **kwargs):
        super().__init__(config, **kwargs)
        self._session: aiohttp.ClientSession | None = None
        self._visited: set[str] = set()  # Canonicalized URLs
        self._pdf_processor = None
        self._output_path: Path | None = None
        self._records_written = 0
        # Playwright browser for Cloudflare-protected pages
        self._playwright = None
        self._browser = None
        self._browser_context = None

    # =========================================================================
    # Lifecycle
    # =========================================================================

    def setup(self) -> None:
        """Initialize resources."""
        super().setup()
        output_dir = self.config.extra.get("output_dir", "data/crawl/dc_facilities")
        self._output_path = Path(output_dir)
        self._output_path.mkdir(parents=True, exist_ok=True)

    def teardown(self) -> None:
        """Clean up resources."""
        super().teardown()

    def _init_pdf_processor(self):
        """Lazy-init PDF processor."""
        if self._pdf_processor is None:
            from ...utils.pdf_processor import PDFProcessor
            self._pdf_processor = PDFProcessor(
                min_chars_threshold=self.config.extra.get(
                    "ocr_char_threshold", OCR_CHAR_THRESHOLD
                ),
            )

    async def _init_browser(self) -> None:
        """Lazy-init Playwright browser for Cloudflare-protected pages."""
        if self._browser is not None:
            return

        try:
            from playwright.async_api import async_playwright
            self._playwright = await async_playwright().start()

            # Use non-headless mode for better Cloudflare bypass, or headless with args
            headless = self.config.extra.get("headless", True)
            self._browser = await self._playwright.chromium.launch(
                headless=headless,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                ] if headless else [],
            )

            # Create context with realistic browser fingerprint
            self._browser_context = await self._browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1920, "height": 1080},
                locale="en-US",
                timezone_id="America/New_York",
                permissions=["geolocation"],
                java_script_enabled=True,
            )

            # Add stealth scripts to evade detection
            await self._browser_context.add_init_script("""
                // Mask webdriver property
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
                // Mask automation properties
                window.chrome = { runtime: {} };
                // Mask permissions
                const originalQuery = window.navigator.permissions.query;
                window.navigator.permissions.query = (parameters) => (
                    parameters.name === 'notifications' ?
                        Promise.resolve({ state: Notification.permission }) :
                        originalQuery(parameters)
                );
            """)

            self._logger.info("Playwright browser initialized with stealth mode")
        except ImportError as e:
            raise ImportError(
                "Playwright required for Cloudflare-protected pages. "
                "Install with: pip install playwright && playwright install chromium"
            ) from e

    async def _close_browser(self) -> None:
        """Close Playwright browser."""
        if self._browser_context:
            await self._browser_context.close()
            self._browser_context = None
        if self._browser:
            await self._browser.close()
            self._browser = None
        if self._playwright:
            await self._playwright.stop()
            self._playwright = None

    def _needs_playwright(self, url: str) -> bool:
        """Check if URL requires Playwright (Cloudflare-protected domain)."""
        domain = self.get_domain(url)
        return domain in PLAYWRIGHT_DOMAINS

    # =========================================================================
    # URL Utilities
    # =========================================================================

    @staticmethod
    def canonicalize_url(url: str) -> str:
        """Canonicalize URL for deduplication (remove fragments, normalize)."""
        parsed = urlparse(url)
        # Remove fragment, lowercase host
        canonical = urlunparse((
            parsed.scheme.lower(),
            parsed.netloc.lower(),
            parsed.path,
            parsed.params,
            parsed.query,
            "",  # No fragment
        ))
        return canonical

    @staticmethod
    def get_domain(url: str) -> str:
        """Extract domain from URL."""
        return urlparse(url).netloc.lower()

    def is_allowed(self, url: str, seed_url: str) -> bool:
        """Check if URL is within allowlist for the seed's domain."""
        domain = self.get_domain(url)
        seed_domain = self.get_domain(seed_url)
        parsed = urlparse(url)
        path = parsed.path

        # Must match seed domain or be in same allowlist group
        if domain != seed_domain:
            # Check if both are in the same allowlist
            if domain not in DOMAIN_ALLOWLISTS:
                return False
            if seed_domain not in DOMAIN_ALLOWLISTS:
                return False

        # Check path prefixes
        prefixes = DOMAIN_ALLOWLISTS.get(domain, [])
        if not prefixes:
            # Empty prefixes = allow all paths for this domain
            return True

        return any(path.startswith(prefix) for prefix in prefixes)

    def is_arcgis_rest(self, url: str) -> bool:
        """Check if URL is an ArcGIS REST endpoint."""
        domain = self.get_domain(url)
        if domain in ARCGIS_REST_DOMAINS:
            return True
        # Also check for REST patterns
        path = urlparse(url).path.lower()
        return "/rest/services/" in path or "/featureserver" in path or "/mapserver" in path

    def is_pdf_url(self, url: str) -> bool:
        """Check if URL appears to be a PDF."""
        path = urlparse(url).path.lower()
        return path.endswith(".pdf")

    # =========================================================================
    # Discovery
    # =========================================================================

    def discover(self) -> Iterator[dict[str, Any]]:
        """Yield seed URLs as initial crawl items."""
        for seed_url in SEED_URLS:
            yield {
                "url": seed_url,
                "seed_url": seed_url,
                "depth": 0,
            }

    async def discover_async(self) -> AsyncIterator[dict[str, Any]]:
        """Async version of discover."""
        for item in self.discover():
            yield item

    # =========================================================================
    # Fetching
    # =========================================================================

    async def _ensure_session(self) -> aiohttp.ClientSession:
        """Ensure aiohttp session exists."""
        if self._session is None:
            timeout = aiohttp.ClientTimeout(total=self.config.timeout_seconds)
            headers = {"User-Agent": self.config.extra.get("user_agent", "")}
            self._session = aiohttp.ClientSession(timeout=timeout, headers=headers)
        return self._session

    async def fetch_url_playwright(
        self,
        url: str,
        seed_url: str,
        depth: int,
    ) -> CrawlRecord | None:
        """
        Fetch URL using Playwright browser (for Cloudflare-protected pages).

        Renders JavaScript and solves Cloudflare challenges automatically.
        """
        canonical = self.canonicalize_url(url)
        if canonical in self._visited:
            return None
        self._visited.add(canonical)

        await self._init_browser()

        record = CrawlRecord(
            source_seed=seed_url,
            crawl_depth=depth,
            url_requested=url,
            final_url=url,
            fetched_at=datetime.now(timezone.utc),
        )

        page = None
        try:
            page = await self._browser_context.new_page()

            # Navigate with extended timeout for Cloudflare challenge
            response = await page.goto(
                url,
                timeout=self.config.timeout_seconds * 1000,
                wait_until="domcontentloaded",
            )

            # Wait for page to render
            await asyncio.sleep(2)

            # Wait for network to settle (with short timeout)
            try:
                await page.wait_for_load_state("networkidle", timeout=5000)
            except Exception:
                pass  # Timeout is OK, page might have long-polling

            # Get final response status by checking if page loaded successfully
            final_url = page.url
            # If we got redirected to an error page, treat as error
            record.http_status = response.status if response else 200
            record.final_url = page.url
            record.content_type = response.headers.get("content-type", "") if response else ""

            # Update visited with final URL
            final_canonical = self.canonicalize_url(record.final_url)
            self._visited.add(final_canonical)

            # Get rendered HTML content FIRST before checking status
            html = await page.content()
            content = html.encode("utf-8")

            # Check page status and content
            record.http_status = response.status if response else None

            # Check if this is a "page removed" error (DGS returns 403 for deleted pages)
            page_title = await page.title()
            is_removed_page = (
                "no longer available" in page_title.lower() or
                "page not found" in page_title.lower() or
                "404" in page_title
            )

            if is_removed_page:
                record.notes = f"Page removed: {page_title}"
                record.extraction_errors.append(f"Page no longer exists (HTTP {response.status if response else 'unknown'})")
                # Still process the error page to capture metadata
                await self._process_html(record, content, seed_url)
                return record

            if response and response.status in (401, 403):
                record.notes = f"Access denied: {response.status}"
                record.extraction_errors.append(f"HTTP {response.status}")
                return record

            if response and response.status != 200:
                record.extraction_errors.append(f"HTTP {response.status}")
                return record

            # Check content type for PDF links that might have been followed
            ct = record.content_type.lower()
            if "application/pdf" in ct or self.is_pdf_url(record.final_url):
                # For PDFs, we need to download via aiohttp since Playwright returns HTML wrapper
                pdf_bytes = await self._download_pdf_direct(record.final_url)
                if pdf_bytes:
                    await self._process_pdf(record, pdf_bytes)
                else:
                    record.extraction_errors.append("Failed to download PDF")
            elif "text/html" in ct or not ct:
                await self._process_html(record, content, seed_url)
                record.notes = "Fetched via Playwright (Cloudflare bypass)"
            else:
                record.page_type = PageType.OTHER
                record.notes = f"Unhandled content type via Playwright: {ct}"

        except Exception as e:
            record.extraction_errors.append(f"Playwright error: {e}")
            self._logger.warning(f"Playwright fetch failed for {url}: {e}")

        finally:
            if page:
                await page.close()

        return record

    async def _download_pdf_direct(self, url: str) -> bytes | None:
        """Download PDF directly via aiohttp."""
        session = await self._ensure_session()
        try:
            async with session.get(url, allow_redirects=True) as response:
                if response.status == 200:
                    return await response.read()
        except Exception as e:
            self._logger.warning(f"PDF download failed: {e}")
        return None

    async def fetch_url(
        self,
        url: str,
        seed_url: str,
        depth: int,
    ) -> CrawlRecord | None:
        """
        Fetch a single URL and create a CrawlRecord.

        Handles HTML, PDF, and JSON content types.
        """
        canonical = self.canonicalize_url(url)
        if canonical in self._visited:
            return None
        self._visited.add(canonical)

        session = await self._ensure_session()
        record = CrawlRecord(
            source_seed=seed_url,
            crawl_depth=depth,
            url_requested=url,
            final_url=url,
            fetched_at=datetime.now(timezone.utc),
        )

        try:
            async with session.get(url, allow_redirects=True) as response:
                record.http_status = response.status
                record.final_url = str(response.url)
                record.content_type = response.headers.get("Content-Type", "")

                # Update visited with final URL
                final_canonical = self.canonicalize_url(record.final_url)
                self._visited.add(final_canonical)

                if response.status in (401, 403):
                    record.notes = f"Auth required or forbidden: {response.status}"
                    record.extraction_errors.append(f"HTTP {response.status}")
                    return record

                if response.status != 200:
                    record.extraction_errors.append(f"HTTP {response.status}")
                    return record

                content = await response.read()

                # Route by content type
                ct = record.content_type.lower()
                if "application/pdf" in ct or self.is_pdf_url(url):
                    await self._process_pdf(record, content)
                elif "application/json" in ct or self.is_arcgis_rest(url):
                    await self._process_json(record, content, url)
                elif "text/html" in ct:
                    await self._process_html(record, content, seed_url)
                else:
                    record.page_type = PageType.OTHER
                    record.notes = f"Unhandled content type: {ct}"

        except asyncio.TimeoutError:
            record.extraction_errors.append("Timeout")
            record.notes = "Request timed out"
        except aiohttp.ClientError as e:
            record.extraction_errors.append(f"Client error: {e}")
        except Exception as e:
            record.extraction_errors.append(f"Unexpected error: {e}")
            self._logger.exception(f"Error fetching {url}")

        return record

    # =========================================================================
    # Content Processing
    # =========================================================================

    async def _process_html(
        self,
        record: CrawlRecord,
        content: bytes,
        seed_url: str,
    ) -> None:
        """Extract text and links from HTML."""
        record.page_type = PageType.HTML

        try:
            html = content.decode("utf-8", errors="replace")
            soup = BeautifulSoup(html, "html.parser")

            # Remove script/style
            for tag in soup(["script", "style", "noscript"]):
                tag.decompose()

            # Title
            title_tag = soup.find("title")
            record.title = title_tag.get_text(strip=True) if title_tag else None

            # Meta description
            meta_desc = soup.find("meta", attrs={"name": "description"})
            if meta_desc and meta_desc.get("content"):
                record.meta_description = meta_desc["content"]

            # Headings H1-H3
            headings = []
            for level in ["h1", "h2", "h3"]:
                for h in soup.find_all(level):
                    text = h.get_text(strip=True)
                    if text:
                        headings.append(f"{level.upper()}: {text}")
            record.headings = headings[:50]  # Limit

            # Visible text
            record.raw_content = soup.get_text(separator="\n", strip=True)

            # Optionally store raw HTML
            record.raw_html_snapshot = html[:500000]  # Limit size

            # Extract links
            internal, external, pdfs = [], [], []
            base_url = record.final_url

            for a in soup.find_all("a", href=True):
                href = a["href"]
                if not href or href.startswith(("#", "javascript:", "mailto:")):
                    continue

                full_url = urljoin(base_url, href)

                if self.is_pdf_url(full_url):
                    pdfs.append(full_url)
                elif self.is_allowed(full_url, seed_url):
                    internal.append(full_url)
                else:
                    external.append(full_url)

            record.out_links_internal = list(set(internal))[:200]
            record.out_links_external = list(set(external))[:100]
            record.pdf_links = list(set(pdfs))[:50]

        except Exception as e:
            record.extraction_errors.append(f"HTML parse error: {e}")

    async def _process_pdf(self, record: CrawlRecord, content: bytes) -> None:
        """Extract text from PDF with OCR fallback."""
        record.page_type = PageType.PDF
        self._init_pdf_processor()

        try:
            result = self._pdf_processor.extract(content)
            record.raw_content = result.full_text
            record.pdf_page_count = result.total_pages
            record.pdf_pages_native = result.pages_native
            record.pdf_pages_ocr = result.pages_ocr
            record.ocr_performed = result.pages_ocr > 0
            record.ocr_confidence = result.average_ocr_confidence
            record.title = record.url_requested.split("/")[-1]

        except Exception as e:
            record.extraction_errors.append(f"PDF extraction error: {e}")
            # Try OCR-only as fallback
            try:
                result = self._pdf_processor.extract_native_only(content)
                record.raw_content = result.full_text
                record.pdf_page_count = result.total_pages
                record.notes = "Native extraction only (OCR failed)"
            except Exception as e2:
                record.extraction_errors.append(f"Fallback extraction failed: {e2}")

    async def _process_json(
        self,
        record: CrawlRecord,
        content: bytes,
        url: str,
    ) -> None:
        """Process JSON (typically ArcGIS REST response)."""
        record.page_type = PageType.JSON

        try:
            raw_json = content.decode("utf-8")
            record.raw_content = raw_json

            data = json.loads(raw_json)

            # ArcGIS metadata extraction
            if "name" in data:
                record.arcgis_service_name = data.get("name")
            if "id" in data:
                record.arcgis_layer_id = data.get("id")
            if "geometryType" in data:
                record.arcgis_geometry_type = data.get("geometryType")
            if "fields" in data:
                record.arcgis_fields = [f.get("name") for f in data.get("fields", [])]

            # Feature count
            if "features" in data:
                record.arcgis_record_count = len(data["features"])
            elif "count" in data:
                record.arcgis_record_count = data["count"]

            # Check for exceeded transfer limit
            if data.get("exceededTransferLimit"):
                record.notes = "ArcGIS transfer limit exceeded - pagination needed"

        except json.JSONDecodeError as e:
            record.extraction_errors.append(f"JSON decode error: {e}")
        except Exception as e:
            record.extraction_errors.append(f"JSON processing error: {e}")

    # =========================================================================
    # ArcGIS REST Pagination
    # =========================================================================

    async def fetch_arcgis_paginated(
        self,
        base_url: str,
        seed_url: str,
        depth: int,
    ) -> list[CrawlRecord]:
        """
        Fetch ArcGIS REST endpoint with robust pagination.

        Handles maxRecordCount limits by using resultOffset/resultRecordCount.
        Returns list of CrawlRecords (one per page).
        """
        records = []
        session = await self._ensure_session()

        # First, get service metadata to find maxRecordCount
        parsed = urlparse(base_url)
        path_parts = parsed.path.rstrip("/").split("/")

        # Determine if this is already a query URL
        is_query_url = "query" in parsed.path.lower() or "f=json" in parsed.query.lower()

        # Get metadata URL (strip /query if present)
        if is_query_url:
            # Extract base service URL
            meta_path = "/".join(path_parts[:-1]) if path_parts[-1].lower() == "query" else parsed.path
            meta_url = f"{parsed.scheme}://{parsed.netloc}{meta_path}?f=json"
        else:
            meta_url = f"{base_url}?f=json"

        max_record_count = 1000  # Default
        total_count = None

        # Fetch metadata
        try:
            async with session.get(meta_url) as resp:
                if resp.status == 200:
                    meta = await resp.json()
                    max_record_count = meta.get("maxRecordCount", 1000)
                    # Store metadata record
                    meta_record = CrawlRecord(
                        source_seed=seed_url,
                        crawl_depth=depth,
                        url_requested=meta_url,
                        final_url=str(resp.url),
                        fetched_at=datetime.now(timezone.utc),
                        http_status=200,
                        content_type="application/json",
                        page_type=PageType.JSON,
                        raw_content=json.dumps(meta),
                        arcgis_service_name=meta.get("name"),
                        arcgis_layer_id=meta.get("id"),
                        arcgis_geometry_type=meta.get("geometryType"),
                        arcgis_fields=[f.get("name") for f in meta.get("fields", [])],
                    )
                    records.append(meta_record)
        except Exception as e:
            self._logger.warning(f"Failed to get ArcGIS metadata: {e}")

        # Get total count
        count_url = f"{meta_url.replace('?f=json', '')}query?where=1%3D1&returnCountOnly=true&f=json"
        try:
            async with session.get(count_url) as resp:
                if resp.status == 200:
                    count_data = await resp.json()
                    total_count = count_data.get("count")
        except Exception as e:
            self._logger.warning(f"Failed to get record count: {e}")

        # Build query base URL
        if is_query_url:
            # Use existing query params but add pagination
            query_base = base_url.split("?")[0]
            existing_params = parse_qs(parsed.query)
        else:
            query_base = f"{base_url}/query"
            existing_params = {}

        # Default query params
        params = {
            "where": existing_params.get("where", ["1=1"])[0],
            "outFields": existing_params.get("outFields", ["*"])[0],
            "outSR": existing_params.get("outSR", ["4326"])[0],
            "f": "json",
        }

        # Paginate
        offset = 0
        page_index = 0
        while True:
            params["resultOffset"] = str(offset)
            params["resultRecordCount"] = str(max_record_count)

            query_url = f"{query_base}?{urlencode(params)}"

            try:
                async with session.get(query_url) as resp:
                    if resp.status != 200:
                        self._logger.warning(f"ArcGIS query failed: {resp.status}")
                        break

                    data = await resp.json()
                    features = data.get("features", [])

                    if not features:
                        break  # No more data

                    # Create record for this page
                    page_record = CrawlRecord(
                        source_seed=seed_url,
                        crawl_depth=depth,
                        url_requested=query_url,
                        final_url=str(resp.url),
                        fetched_at=datetime.now(timezone.utc),
                        http_status=200,
                        content_type="application/json",
                        page_type=PageType.JSON,
                        raw_content=json.dumps(data),
                        arcgis_record_count=len(features),
                        arcgis_page_index=page_index,
                    )

                    if data.get("exceededTransferLimit"):
                        page_record.notes = "Transfer limit exceeded, more pages exist"

                    records.append(page_record)

                    # Check if more pages
                    if len(features) < max_record_count:
                        break  # Last page

                    offset += len(features)
                    page_index += 1

                    # Rate limiting
                    await asyncio.sleep(1.0 / self.config.requests_per_second)

            except Exception as e:
                self._logger.error(f"ArcGIS pagination error at offset {offset}: {e}")
                break

        return records

    # =========================================================================
    # Main Crawl Loop
    # =========================================================================

    async def crawl_seed(
        self,
        seed_url: str,
        max_depth: int = 3,
    ) -> list[CrawlRecord]:
        """
        Crawl from a single seed URL up to max_depth.

        Uses BFS to explore links level by level.
        Uses Playwright for Cloudflare-protected domains.
        """
        all_records = []
        queue: list[tuple[str, int]] = [(seed_url, 0)]  # (url, depth)

        while queue:
            url, depth = queue.pop(0)

            if depth > max_depth:
                continue

            canonical = self.canonicalize_url(url)
            if canonical in self._visited:
                continue

            self._logger.debug(f"Crawling depth={depth}: {url}")

            # Handle ArcGIS REST specially
            if self.is_arcgis_rest(url):
                records = await self.fetch_arcgis_paginated(url, seed_url, depth)
                all_records.extend(records)
                # Don't follow links from ArcGIS REST
                continue

            # Choose fetch method based on domain
            if self._needs_playwright(url):
                # Use Playwright for Cloudflare-protected domains
                record = await self.fetch_url_playwright(url, seed_url, depth)
            else:
                # Try aiohttp first
                record = await self.fetch_url(url, seed_url, depth)

                # Retry with Playwright if we got a 403 (likely Cloudflare)
                if record and record.http_status == 403:
                    self._logger.info(f"Got 403, retrying with Playwright: {url}")
                    # Remove from visited so Playwright can try
                    self._visited.discard(canonical)
                    final_canonical = self.canonicalize_url(record.final_url)
                    self._visited.discard(final_canonical)
                    # Retry with Playwright
                    record = await self.fetch_url_playwright(url, seed_url, depth)

            if record is None:
                continue

            all_records.append(record)

            # Enqueue child links if within depth limit
            if depth < max_depth:
                # Internal links
                for link in record.out_links_internal:
                    if self.canonicalize_url(link) not in self._visited:
                        queue.append((link, depth + 1))

                # PDF links (treat as depth + 1)
                for pdf_link in record.pdf_links:
                    if self.canonicalize_url(pdf_link) not in self._visited:
                        queue.append((pdf_link, depth + 1))

            # Rate limiting
            await asyncio.sleep(1.0 / self.config.requests_per_second)

        return all_records

    # =========================================================================
    # JSONL Output
    # =========================================================================

    def _write_jsonl(self, record: CrawlRecord) -> None:
        """Append a single record to JSONL output file."""
        if self._output_path is None:
            return

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d")
        filename = self._output_path / f"crawl_{timestamp}.jsonl"

        with open(filename, "a", encoding="utf-8") as f:
            f.write(json.dumps(record.to_jsonl_dict(), ensure_ascii=False) + "\n")

        self._records_written += 1

    # =========================================================================
    # Pipeline Entry Points
    # =========================================================================

    def run(self, *, max_records: int | None = None) -> ScraperResult:
        """Execute the crawling pipeline (sync wrapper)."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(self.run_async(max_records=max_records))
        finally:
            loop.close()

    async def run_async(self, *, max_records: int | None = None) -> ScraperResult:
        """Execute the full crawling pipeline."""
        result = ScraperResult()
        self.metadata.start()

        try:
            self.setup()
            max_depth = self.config.extra.get("max_depth", 3)

            self._logger.info(f"Starting crawl with {len(SEED_URLS)} seeds, max_depth={max_depth}")

            for seed_url in SEED_URLS:
                self._logger.info(f"Crawling seed: {seed_url}")
                result.discovered_count += 1

                try:
                    records = await self.crawl_seed(seed_url, max_depth)

                    for record in records:
                        # Write to JSONL (raw storage)
                        self._write_jsonl(record)
                        result.parsed_count += 1
                        result.validated_count += 1

                        if max_records and result.validated_count >= max_records:
                            break

                except Exception as e:
                    self._logger.error(f"Error crawling seed {seed_url}: {e}")
                    result.quarantine(
                        data={"seed_url": seed_url},
                        error_type="crawl_error",
                        error_message=str(e),
                        lineage=self._make_lineage(seed_url),
                    )

                if max_records and result.validated_count >= max_records:
                    break

            self._logger.info(
                f"Crawl complete: {result.validated_count} records written to JSONL"
            )
            self.metadata.complete(RunStatus.COMPLETED)

        except Exception as e:
            self._logger.exception(f"Crawl failed: {e}")
            self.metadata.complete(RunStatus.FAILED)
            raise

        finally:
            if self._session:
                await self._session.close()
                self._session = None
            await self._close_browser()
            self.teardown()

        return result

    # =========================================================================
    # BaseScraper Interface (minimal implementation)
    # =========================================================================

    def fetch(self, item: dict[str, Any]) -> RawPayload | None:
        """Sync fetch - not used, crawl uses async internally."""
        return None

    def parse(self, payload: RawPayload) -> Iterator[dict[str, Any]]:
        """Parse - not used, crawl creates records directly."""
        yield from []

    def validate(self, data: dict[str, Any]) -> tuple[bool, list[str]]:
        """Validate - always valid for raw crawl records."""
        return True, []

    def normalize(self, data: dict[str, Any]) -> CrawlRecord:
        """Normalize - identity for CrawlRecord."""
        return data  # type: ignore
