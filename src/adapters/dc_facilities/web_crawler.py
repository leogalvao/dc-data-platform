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
    # ProjectTeam - requires authentication (uses browser session)
    "https://app.projectteam.com/",
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
    # ProjectTeam: construction project management (requires auth)
    "app.projectteam.com": [],  # Allow all paths
}

# Domains that are ArcGIS REST endpoints (fetch JSON, not HTML)
ARCGIS_REST_DOMAINS = {"services.arcgis.com", "maps2.dcgis.dc.gov"}

# Domains that require Playwright (Cloudflare-protected or JS-heavy)
PLAYWRIGHT_DOMAINS = {"dgs.dc.gov", "dme.dc.gov", "octo.quickbase.com"}

# Domains that require authentication (use Chrome profile with saved session)
AUTH_DOMAINS = {"app.projectteam.com"}

# Chrome profile path for authenticated sessions
CHROME_PROFILE_PATH = Path.home() / ".config" / "google-chrome"

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
        # Authenticated browser using Chrome profile
        self._auth_playwright = None
        self._auth_browser = None
        self._auth_context = None

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
        # Close authenticated browser
        if self._auth_context:
            await self._auth_context.close()
            self._auth_context = None
        if self._auth_browser:
            await self._auth_browser.close()
            self._auth_browser = None
        if self._auth_playwright:
            await self._auth_playwright.stop()
            self._auth_playwright = None

    # Pre-configured ProjectTeam cookies (provided by user)
    PROJECTTEAM_COOKIES = [
        {"name": "_gcl_au", "value": "1.1.1680652389.1767442638", "domain": "projectteam.com", "path": "/"},
        {"name": "_ga", "value": "GA1.2.1801179529.1767442638", "domain": "projectteam.com", "path": "/"},
        {"name": "messagesUtk", "value": "17d6d593e3aa4c3a86304d849b9a8064", "domain": "projectteam.com", "path": "/"},
        {"name": "hubspotutk", "value": "93b5cf4e919e7ce12d4a2f33a300279b", "domain": "projectteam.com", "path": "/"},
        {"name": "_gid", "value": "GA1.2.428364250.1769425734", "domain": "projectteam.com", "path": "/"},
        {"name": "_gcl_gs", "value": "2.1.k1$i1769491934$u57271275", "domain": "projectteam.com", "path": "/"},
        {"name": "_gac_UA-71552008-1", "value": "1.1769491936.Cj0KCQiAvtzLBhCPARIsALwhxdrleW6WvAFkhtK4iTXroHFAYyS1wUvfkTuRMXLO_pCOqD6wXSqWfU8aAhjbEALw_wcB", "domain": "projectteam.com", "path": "/"},
        {"name": "_gcl_aw", "value": "GCL.1769491937.Cj0KCQiAvtzLBhCPARIsALwhxdrleW6WvAFkhtK4iTXroHFAYyS1wUvfkTuRMXLO_pCOqD6wXSqWfU8aAhjbEALw_wcB", "domain": "projectteam.com", "path": "/"},
        {"name": "_ga_ZTW9KPQPZL", "value": "GS2.2.s1769491936$o4$g1$t1769491936$j60$l0$h0", "domain": "projectteam.com", "path": "/"},
        {"name": "__hstc", "value": "176454330.93b5cf4e919e7ce12d4a2f33a300279b.1767442639282.1769425735382.1769491937160.4", "domain": "projectteam.com", "path": "/"},
        {"name": "__hssrc", "value": "1", "domain": "projectteam.com", "path": "/"},
        {"name": "__hssc", "value": "176454330.1.1769491937160", "domain": "projectteam.com", "path": "/"},
    ]

    async def _init_auth_browser(self) -> None:
        """Initialize Playwright browser with authentication cookies."""
        if self._auth_browser is not None:
            return

        try:
            from playwright.async_api import async_playwright

            self._auth_playwright = await async_playwright().start()

            # Launch a fresh browser (works even when Chrome is running)
            self._auth_browser = await self._auth_playwright.chromium.launch(
                headless=False,  # Non-headless for manual login if needed
                args=["--disable-blink-features=AutomationControlled"],
            )

            self._auth_context = await self._auth_browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1920, "height": 1080},
            )

            # Add pre-configured cookies for ProjectTeam
            await self._auth_context.add_cookies(self.PROJECTTEAM_COOKIES)
            self._logger.info(f"Added {len(self.PROJECTTEAM_COOKIES)} pre-configured cookies")

            self._logger.info("Authenticated browser initialized")

        except ImportError as e:
            raise ImportError(
                "Playwright required. Install with: pip install playwright && playwright install chromium"
            ) from e

    def _needs_auth(self, url: str) -> bool:
        """Check if URL requires authentication (Chrome profile)."""
        domain = self.get_domain(url)
        return domain in AUTH_DOMAINS

    async def fetch_url_authenticated(
        self,
        url: str,
        seed_url: str,
        depth: int,
    ) -> CrawlRecord | None:
        """
        Fetch URL using authenticated browser (Chrome profile with saved session).

        Used for sites like ProjectTeam that require login.
        """
        canonical = self.canonicalize_url(url)
        if canonical in self._visited:
            return None
        self._visited.add(canonical)

        await self._init_auth_browser()

        record = CrawlRecord(
            source_seed=seed_url,
            crawl_depth=depth,
            url_requested=url,
            final_url=url,
            fetched_at=datetime.now(timezone.utc),
        )

        page = None
        try:
            page = await self._auth_context.new_page()

            # Navigate to the page
            response = await page.goto(
                url,
                timeout=self.config.timeout_seconds * 1000,
                wait_until="domcontentloaded",
            )

            # Wait for page to load
            await asyncio.sleep(3)
            try:
                await page.wait_for_load_state("networkidle", timeout=10000)
            except Exception:
                pass

            # Get page content
            html = await page.content()
            content = html.encode("utf-8")

            record.http_status = response.status if response else None
            record.final_url = page.url
            record.content_type = response.headers.get("content-type", "") if response else "text/html"

            # Update visited with final URL
            final_canonical = self.canonicalize_url(record.final_url)
            self._visited.add(final_canonical)

            # Check if we got redirected to login page
            page_title = await page.title()
            current_url = page.url.lower()
            if "login" in page_title.lower() or "sign in" in page_title.lower() or "login" in current_url:
                self._logger.info("Login page detected - waiting for manual login...")
                record.notes = "Login required - waiting for user to authenticate"

                # Wait for user to log in (check every 2 seconds for up to 2 minutes)
                for _ in range(60):
                    await asyncio.sleep(2)
                    current_url = page.url.lower()
                    page_title = await page.title()
                    if "login" not in current_url and "login" not in page_title.lower() and "sign in" not in page_title.lower():
                        self._logger.info("Login successful, continuing crawl...")
                        # Refresh content after login
                        html = await page.content()
                        content = html.encode("utf-8")
                        record.final_url = page.url
                        record.notes = "Logged in via manual authentication"
                        break
                else:
                    record.extraction_errors.append("Login timeout - user did not authenticate within 2 minutes")
                    return record

            # Check for auth errors
            if response and response.status in (401, 403):
                record.notes = f"Access denied: {response.status}"
                record.extraction_errors.append(f"HTTP {response.status}")
                return record

            if response and response.status != 200:
                record.extraction_errors.append(f"HTTP {response.status}")
                return record

            # Process HTML content
            await self._process_html(record, content, seed_url)
            record.notes = "Fetched via authenticated browser (Chrome profile)"

        except Exception as e:
            record.extraction_errors.append(f"Auth browser error: {e}")
            self._logger.warning(f"Authenticated fetch failed for {url}: {e}")

        finally:
            if page:
                await page.close()

        return record

    # Patterns to identify drawings (to be excluded)
    DRAWING_PATTERNS = [
        r"drawing",
        r"dwg",
        r"cad",
        r"plan[s]?\s*(sheet|set)",
        r"architectural\s*plan",
        r"structural\s*plan",
        r"mep\s*plan",
        r"civil\s*plan",
        r"floor\s*plan",
        r"site\s*plan",
        r"elevation",
        r"section\s*view",
        r"detail\s*sheet",
        r"[A-Z]\d{3}",  # Drawing numbers like A101, M201
    ]

    def _is_drawing(self, filename: str, link_text: str = "") -> bool:
        """Check if a file appears to be a drawing (to be excluded)."""
        combined = f"{filename} {link_text}".lower()
        for pattern in self.DRAWING_PATTERNS:
            if re.search(pattern, combined, re.IGNORECASE):
                return True
        return False

    async def crawl_projectteam_full(self, seed_url: str, max_depth: int = 3) -> list[CrawlRecord]:
        """
        Comprehensively crawl ProjectTeam: ALL projects, ALL tabs, ALL PDFs (except drawings).

        Navigates the complete ProjectTeam structure:
        1. Main page -> Projects list (ALL projects, no limit)
        2. Each project -> ALL tabs (RFIs, Submittals, Daily Logs, Documents, etc.)
        3. Download and OCR ALL PDFs (excluding drawings)
        """
        all_records = []
        pt_visited: set[str] = set()  # Track visited URLs within this crawl
        await self._init_auth_browser()

        page = await self._auth_context.new_page()

        # ProjectTeam URL structure: /project/{id}/{section}/{subsection}
        # Based on observed patterns like /project/6858/home/dashboard
        PROJECT_SECTIONS = [
            # (section, subsection) tuples
            ("home", "dashboard"),
            ("general", "view"),
            ("general", "info"),
            ("documents", "list"),
            ("documents", "all"),
            ("files", "list"),
            ("rfi", "list"),
            ("rfi", "all"),
            ("rfis", "list"),
            ("submittal", "list"),
            ("submittal", "all"),
            ("submittals", "list"),
            ("daily-log", "list"),
            ("daily-logs", "list"),
            ("dailylog", "list"),
            ("punch-list", "list"),
            ("punchlist", "list"),
            ("change-order", "list"),
            ("change-orders", "list"),
            ("pay-app", "list"),
            ("pay-applications", "list"),
            ("meeting", "list"),
            ("meetings", "list"),
            ("schedule", "view"),
            ("photos", "list"),
            ("issues", "list"),
            ("correspondence", "list"),
            ("transmittal", "list"),
            ("transmittals", "list"),
            ("contracts", "list"),
            ("budget", "view"),
            ("costs", "list"),
            ("safety", "list"),
            ("inspections", "list"),
            ("closeout", "list"),
        ]

        try:
            # Step 1: Navigate to main page
            self._logger.info("Navigating to ProjectTeam...")
            response = await page.goto(seed_url, timeout=60000, wait_until="domcontentloaded")
            await asyncio.sleep(3)

            # Check for login
            page_title = await page.title()
            current_url = page.url.lower()

            if "login" in page_title.lower() or "login" in current_url:
                self._logger.info("="*60)
                self._logger.info("LOGIN REQUIRED - Please log in to ProjectTeam in the browser window!")
                self._logger.info("You have 10 minutes to complete login...")
                self._logger.info("="*60)
                for _ in range(300):  # Wait up to 10 minutes
                    await asyncio.sleep(2)
                    current_url = page.url.lower()
                    page_title = await page.title()
                    if "login" not in current_url and "login" not in page_title.lower():
                        self._logger.info("Login successful!")
                        break
                else:
                    self._logger.error("Login timeout")
                    return all_records

            await asyncio.sleep(2)

            # Record the main page
            main_url = page.url
            if main_url not in pt_visited:
                pt_visited.add(main_url)
                main_record = await self._create_record_from_page(page, seed_url, 0)
                all_records.append(main_record)
                self._write_jsonl(main_record)

            # Step 2: Find ALL project links
            self._logger.info("Finding ALL projects...")

            # Try multiple navigation paths to find projects
            project_list_urls = [
                "https://app.projectteam.com/dashboard/project",  # Primary URL from user
                "https://app.projectteam.com/my-page",
                "https://app.projectteam.com/project",
                "https://app.projectteam.com/projects",
                "https://app.projectteam.com/home",
                "https://app.projectteam.com/",
            ]

            all_project_links = []
            for plist_url in project_list_urls:
                try:
                    self._logger.debug(f"Trying project list URL: {plist_url}")
                    await page.goto(plist_url, timeout=60000, wait_until="domcontentloaded")
                    await asyncio.sleep(3)

                    # Scroll to load all projects (infinite scroll handling)
                    prev_height = 0
                    for scroll_attempt in range(20):  # More scroll attempts
                        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                        await asyncio.sleep(1)
                        new_height = await page.evaluate("document.body.scrollHeight")
                        if new_height == prev_height:
                            break  # No more content to load
                        prev_height = new_height

                    # Find project links using multiple selectors
                    # Try different patterns that ProjectTeam might use
                    selectors = [
                        "a[href*='/project/']",
                        "a[href*='project/']",
                        "[data-project-id]",
                        ".project-link",
                        ".project-item a",
                        "tr[data-id] a",  # Table rows with project links
                        ".card a[href*='project']",  # Card-based layouts
                    ]

                    for selector in selectors:
                        try:
                            links = await page.eval_on_selector_all(
                                selector,
                                "elements => elements.map(e => ({href: e.href || '', text: e.innerText || '', dataId: e.getAttribute('data-project-id') || ''}))"
                            )
                            if links:
                                self._logger.debug(f"Found {len(links)} links with selector: {selector}")
                                all_project_links.extend(links)
                        except Exception:
                            continue

                    # Also extract project IDs from the page HTML directly
                    html = await page.content()
                    # Look for patterns like /project/12345 in the HTML
                    import re
                    project_pattern = re.compile(r'/project/(\d+)')
                    matches = project_pattern.findall(html)
                    for match in matches:
                        all_project_links.append({"href": f"https://app.projectteam.com/project/{match}", "text": ""})

                except Exception as e:
                    self._logger.debug(f"Failed to fetch {plist_url}: {e}")
                    continue

            # Deduplicate and filter project links
            seen_projects = set()
            projects = []
            for link in all_project_links:
                href = link.get("href", "")
                data_id = link.get("dataId", "")

                # Try to extract project ID
                project_id = None

                # From data attribute
                if data_id and data_id.isdigit():
                    project_id = data_id

                # From URL pattern /project/12345
                if not project_id and "/project/" in href:
                    parts = href.split("/project/")
                    if len(parts) > 1:
                        id_part = parts[1].split("/")[0].split("?")[0]
                        if id_part.isdigit():
                            project_id = id_part

                if project_id and project_id not in seen_projects:
                    seen_projects.add(project_id)
                    projects.append({
                        "url": f"https://app.projectteam.com/project/{project_id}",
                        "name": link.get("text", "").strip()[:100],
                        "id": project_id,
                    })

            self._logger.info(f"Found {len(projects)} unique projects - crawling ALL")

            # If still no projects found, log warning and return what we have
            if not projects:
                self._logger.warning("No projects found! Check if login succeeded and page structure changed.")
                # Try to capture the current page for debugging
                debug_record = await self._create_record_from_page(page, seed_url, 0)
                debug_record.notes = "DEBUG: No projects found - captured page for analysis"
                all_records.append(debug_record)
                self._write_jsonl(debug_record)
                return all_records

            # Step 3: Crawl EACH project comprehensively (NO LIMIT)
            for idx, project in enumerate(projects):
                project_url = project["url"]
                project_id = project["id"]
                project_name = project["name"]

                self._logger.info(f"[{idx+1}/{len(projects)}] Crawling project {project_id}: {project_name[:50]}...")

                try:
                    await page.goto(project_url, timeout=60000, wait_until="domcontentloaded")
                    await asyncio.sleep(2)

                    # Record project main page (if not already visited)
                    current_url = page.url
                    if current_url not in pt_visited:
                        pt_visited.add(current_url)
                        project_record = await self._create_record_from_page(page, seed_url, 1)
                        project_record.notes = f"Project: {project_name}"
                        all_records.append(project_record)
                        self._write_jsonl(project_record)

                    # Step 4: Navigate ALL sections for this project
                    pdf_urls_to_download = []

                    for section, subsection in PROJECT_SECTIONS:
                        section_url = f"{project_url}/{section}/{subsection}"

                        # Skip if already visited
                        if section_url in pt_visited:
                            continue

                        try:
                            await page.goto(section_url, timeout=30000, wait_until="domcontentloaded")
                            await asyncio.sleep(2)

                            # Check if we actually landed on the requested page
                            # (ProjectTeam may redirect invalid URLs back to dashboard)
                            final_url = page.url
                            if final_url in pt_visited:
                                # Already recorded this page, skip
                                continue

                            # Check if page loaded successfully (not 404 or redirected away)
                            current_lower = final_url.lower()
                            if "404" in current_lower or "error" in current_lower or "not-found" in current_lower:
                                continue

                            # Check if we were redirected back to a different page
                            # If the final URL doesn't contain our section, navigation likely failed
                            if section not in final_url.lower() and subsection not in final_url.lower():
                                self._logger.debug(f"Section {section}/{subsection} redirected to {final_url}")
                                continue

                            pt_visited.add(final_url)

                            # Scroll to load dynamic content
                            for _ in range(3):
                                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                                await asyncio.sleep(0.5)

                            # Record the section page
                            section_record = await self._create_record_from_page(page, seed_url, 2)
                            section_record.notes = f"Project {project_name} - Section: {section}/{subsection}"
                            all_records.append(section_record)
                            self._write_jsonl(section_record)

                            self._logger.debug(f"  Recorded section: {section}/{subsection}")

                            # Find ALL downloadable links on this section
                            links = await page.eval_on_selector_all(
                                "a[href*='.pdf'], a[href*='/document/'], a[href*='/download'], "
                                "a[href*='/attachment'], a[href*='/file'], a[href*='download='], "
                                "button[data-url], [data-download-url]",
                                """elements => elements.map(e => ({
                                    href: e.href || e.getAttribute('data-url') || e.getAttribute('data-download-url') || '',
                                    text: e.innerText || e.getAttribute('title') || ''
                                }))"""
                            )

                            for link in links:
                                pdf_url = link.get("href", "")
                                link_text = link.get("text", "")
                                if pdf_url and not self._is_drawing(pdf_url, link_text):
                                    pdf_urls_to_download.append({
                                        "url": pdf_url,
                                        "text": link_text,
                                        "section": f"{section}/{subsection}",
                                    })

                        except Exception as e:
                            self._logger.debug(f"Section {section}/{subsection} not accessible: {e}")
                            continue

                    # Step 5: Download and process ALL PDFs for this project (excluding drawings)
                    unique_pdf_urls = list({p["url"]: p for p in pdf_urls_to_download}.values())
                    self._logger.info(f"  Found {len(unique_pdf_urls)} PDFs (excluding drawings)")

                    for pdf_info in unique_pdf_urls:
                        pdf_url = pdf_info["url"]
                        pdf_text = pdf_info["text"]
                        pdf_section = pdf_info["section"]

                        # Skip if already downloaded
                        if pdf_url in pt_visited:
                            continue
                        pt_visited.add(pdf_url)

                        # Double-check not a drawing
                        if self._is_drawing(pdf_url, pdf_text):
                            self._logger.debug(f"  Skipping drawing: {pdf_text[:40]}")
                            continue

                        self._logger.info(f"  Downloading: {pdf_text[:50] or pdf_url[-50:]}...")

                        try:
                            pdf_record = await self._download_and_process_pdf_authenticated(
                                pdf_url, seed_url, 3, f"{project_name} / {pdf_section}"
                            )
                            if pdf_record:
                                pdf_record.notes = f"PDF from {project_name} / {pdf_section}: {pdf_text[:100]}"
                                all_records.append(pdf_record)
                                self._write_jsonl(pdf_record)
                        except Exception as e:
                            self._logger.warning(f"  Failed to download PDF: {e}")

                        # Rate limiting between PDFs
                        await asyncio.sleep(0.5)

                    # Rate limiting between projects
                    await asyncio.sleep(1)

                except Exception as e:
                    self._logger.warning(f"Error crawling project {project_id}: {e}")
                    continue

            self._logger.info(f"ProjectTeam crawl complete: {len(all_records)} records from {len(projects)} projects")

        except Exception as e:
            self._logger.error(f"ProjectTeam crawl error: {e}")

        finally:
            await page.close()

        return all_records

    async def _create_record_from_page(
        self,
        page,
        seed_url: str,
        depth: int,
    ) -> CrawlRecord:
        """Create a CrawlRecord from current Playwright page state."""
        html = await page.content()
        content = html.encode("utf-8")

        record = CrawlRecord(
            source_seed=seed_url,
            crawl_depth=depth,
            url_requested=page.url,
            final_url=page.url,
            fetched_at=datetime.now(timezone.utc),
            http_status=200,
            content_type="text/html",
        )

        await self._process_html(record, content, seed_url)
        return record

    async def _get_auth_cookies_header(self) -> str:
        """Get cookies from authenticated browser context as a header string."""
        if self._auth_context is None:
            return ""
        cookies = await self._auth_context.cookies()
        return "; ".join(f"{c['name']}={c['value']}" for c in cookies)

    async def _download_and_process_pdf_authenticated(
        self,
        url: str,
        seed_url: str,
        depth: int,
        project_name: str = "",
    ) -> CrawlRecord | None:
        """Download PDF via authenticated session and process with OCR."""
        record = CrawlRecord(
            source_seed=seed_url,
            crawl_depth=depth,
            url_requested=url,
            final_url=url,
            fetched_at=datetime.now(timezone.utc),
        )

        pdf_bytes = None

        try:
            # Method 1: Use Playwright's API request context (preserves auth)
            if self._auth_context:
                try:
                    # Create API request context from browser context
                    api_context = self._auth_context.request
                    response = await api_context.get(url, timeout=120000)

                    if response.ok:
                        pdf_bytes = await response.body()
                        record.http_status = response.status
                        record.content_type = response.headers.get("content-type", "application/pdf")
                        record.final_url = response.url
                        self._logger.debug(f"Downloaded {len(pdf_bytes)} bytes via API context")
                except Exception as e:
                    self._logger.debug(f"API context download failed: {e}")

            # Method 2: Use aiohttp with cookies from browser
            if not pdf_bytes:
                try:
                    cookie_header = await self._get_auth_cookies_header()
                    headers = {
                        "Cookie": cookie_header,
                        "User-Agent": (
                            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) "
                            "Chrome/120.0.0.0 Safari/537.36"
                        ),
                        "Accept": "application/pdf,*/*",
                    }

                    session = await self._ensure_session()
                    async with session.get(url, headers=headers, allow_redirects=True, timeout=aiohttp.ClientTimeout(total=120)) as response:
                        if response.status == 200:
                            pdf_bytes = await response.read()
                            record.http_status = 200
                            record.content_type = response.headers.get("Content-Type", "application/pdf")
                            record.final_url = str(response.url)
                            self._logger.debug(f"Downloaded {len(pdf_bytes)} bytes via aiohttp with cookies")
                        else:
                            record.extraction_errors.append(f"HTTP {response.status}")
                except Exception as e:
                    self._logger.debug(f"aiohttp download failed: {e}")

            # Method 3: Use Playwright page with download handling
            if not pdf_bytes and self._auth_context:
                page = None
                try:
                    page = await self._auth_context.new_page()

                    # Try to trigger download
                    try:
                        async with page.expect_download(timeout=60000) as download_info:
                            await page.goto(url, timeout=30000)

                        download = await download_info.value
                        temp_path = await download.path()

                        if temp_path:
                            with open(temp_path, "rb") as f:
                                pdf_bytes = f.read()
                            record.http_status = 200
                            record.content_type = "application/pdf"
                            self._logger.debug(f"Downloaded {len(pdf_bytes)} bytes via Playwright download")
                    except Exception:
                        # If no download triggered, try getting response body
                        response = await page.goto(url, timeout=60000, wait_until="load")
                        if response and response.status == 200:
                            content_type = response.headers.get("content-type", "")
                            if "pdf" in content_type.lower() or "octet-stream" in content_type.lower():
                                pdf_bytes = await response.body()
                                record.http_status = 200
                                record.content_type = content_type
                                self._logger.debug(f"Downloaded {len(pdf_bytes)} bytes via Playwright response")
                finally:
                    if page:
                        await page.close()

            # Process PDF if we got content
            if pdf_bytes and len(pdf_bytes) > 100:
                # Verify it's actually a PDF (starts with %PDF)
                if pdf_bytes[:4] == b'%PDF':
                    await self._process_pdf(record, pdf_bytes)
                    record.notes = f"PDF from project: {project_name}" if project_name else "PDF downloaded via authenticated session"
                    self._logger.info(f"    Processed PDF: {len(record.raw_content or '')} chars extracted")
                    return record
                else:
                    record.extraction_errors.append("Downloaded content is not a valid PDF")
                    self._logger.warning(f"    Not a PDF: {pdf_bytes[:20]}")
            else:
                record.extraction_errors.append("Failed to download PDF content")

        except Exception as e:
            record.extraction_errors.append(f"PDF processing error: {e}")
            self._logger.warning(f"PDF download error for {url}: {e}")

        return record if record.raw_content else None

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

    async def fetch_pdf_direct(
        self,
        url: str,
        seed_url: str,
        depth: int,
    ) -> CrawlRecord | None:
        """
        Fetch PDF directly via aiohttp (bypasses Playwright which triggers downloads).

        Downloads the PDF bytes and extracts text with OCR fallback.
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

                if response.status != 200:
                    record.extraction_errors.append(f"HTTP {response.status}")
                    return record

                content = await response.read()

                # Process as PDF
                await self._process_pdf(record, content)
                record.notes = "PDF downloaded directly (bypassed Playwright)"

        except asyncio.TimeoutError:
            record.extraction_errors.append("Timeout downloading PDF")
        except aiohttp.ClientError as e:
            record.extraction_errors.append(f"Download error: {e}")
        except Exception as e:
            record.extraction_errors.append(f"Unexpected error: {e}")
            self._logger.exception(f"Error fetching PDF {url}")

        return record

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
        """Extract text from PDF with OCR fallback and parse structured content."""
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

            # Store page-by-page content for granular tokenization
            record.pdf_pages = [
                {
                    "page_num": page.page_number,
                    "text": page.text,
                    "char_count": page.char_count,
                    "method": page.method,
                }
                for page in result.pages
            ]

            # Extract sections/headings from the text
            record.pdf_sections = self._extract_pdf_sections(result.full_text)

            # Extract PDF metadata
            record.pdf_metadata = self._extract_pdf_metadata(content)

            # Extract tables if possible
            record.pdf_tables = self._extract_pdf_tables(content)

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

    def _extract_pdf_sections(self, text: str) -> list[dict[str, str]]:
        """Extract sections/headings from PDF text."""
        sections = []
        lines = text.split("\n")

        # Common section header patterns (all caps, numbered, etc.)
        section_patterns = [
            r"^([A-Z][A-Z\s]{2,50})$",  # ALL CAPS headers
            r"^(\d+\.?\s+[A-Z][A-Za-z\s]+)$",  # Numbered sections (1. Introduction)
            r"^(SECTION\s+\d+[:\s].+)$",  # SECTION 1: ...
            r"^(ARTICLE\s+\d+[:\s].+)$",  # ARTICLE 1: ...
            r"^(PART\s+[A-Z0-9]+[:\s].+)$",  # PART A: ...
        ]

        current_heading = None
        current_content = []

        for line in lines:
            line = line.strip()
            if not line:
                continue

            is_heading = False
            for pattern in section_patterns:
                if re.match(pattern, line) and len(line) < 100:
                    is_heading = True
                    break

            if is_heading:
                # Save previous section
                if current_heading and current_content:
                    sections.append({
                        "heading": current_heading,
                        "content": "\n".join(current_content)[:5000],  # Limit size
                    })
                current_heading = line
                current_content = []
            else:
                current_content.append(line)

        # Save last section
        if current_heading and current_content:
            sections.append({
                "heading": current_heading,
                "content": "\n".join(current_content)[:5000],
            })

        return sections[:100]  # Limit number of sections

    def _extract_pdf_metadata(self, content: bytes) -> dict[str, Any] | None:
        """Extract PDF document metadata."""
        try:
            import io
            pdfplumber = self._pdf_processor._lazy_import_pdfplumber()

            with pdfplumber.open(io.BytesIO(content)) as pdf:
                metadata = pdf.metadata or {}
                return {
                    "title": metadata.get("Title"),
                    "author": metadata.get("Author"),
                    "subject": metadata.get("Subject"),
                    "creator": metadata.get("Creator"),
                    "producer": metadata.get("Producer"),
                    "creation_date": metadata.get("CreationDate"),
                    "mod_date": metadata.get("ModDate"),
                    "page_count": len(pdf.pages),
                }
        except Exception as e:
            self._logger.debug(f"Failed to extract PDF metadata: {e}")
            return None

    def _extract_pdf_tables(self, content: bytes) -> list[dict[str, Any]] | None:
        """Extract tables from PDF."""
        try:
            import io
            pdfplumber = self._pdf_processor._lazy_import_pdfplumber()

            tables = []
            with pdfplumber.open(io.BytesIO(content)) as pdf:
                for page_num, page in enumerate(pdf.pages[:20], start=1):  # Limit pages
                    page_tables = page.extract_tables()
                    for table_idx, table in enumerate(page_tables or []):
                        if not table or len(table) < 2:
                            continue

                        # First row as headers
                        headers = [str(cell or "").strip() for cell in table[0]]
                        rows = [
                            [str(cell or "").strip() for cell in row]
                            for row in table[1:]
                        ]

                        tables.append({
                            "page_num": page_num,
                            "table_index": table_idx,
                            "headers": headers,
                            "rows": rows[:100],  # Limit rows
                            "row_count": len(rows),
                        })

                        if len(tables) >= 50:  # Limit total tables
                            break
                    if len(tables) >= 50:
                        break

            return tables if tables else None
        except Exception as e:
            self._logger.debug(f"Failed to extract PDF tables: {e}")
            return None

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

            # Handle ProjectTeam specially - comprehensive crawl
            if self._needs_auth(url) and "projectteam" in url.lower() and depth == 0:
                records = await self.crawl_projectteam_full(url, max_depth)
                all_records.extend(records)
                continue

            # Choose fetch method based on URL type and domain
            # PDFs should always use direct download (Playwright triggers download dialog)
            if self.is_pdf_url(url):
                record = await self.fetch_pdf_direct(url, seed_url, depth)
            elif self._needs_auth(url):
                # Use authenticated browser (Chrome profile) for auth-required sites
                record = await self.fetch_url_authenticated(url, seed_url, depth)
            elif self._needs_playwright(url):
                # Use Playwright for JS-heavy domains
                record = await self.fetch_url_playwright(url, seed_url, depth)
            else:
                # Try aiohttp first
                record = await self.fetch_url(url, seed_url, depth)

                # Retry with Playwright if we got a 403 (might be JS-required)
                if record and record.http_status == 403 and not self.is_pdf_url(record.final_url):
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
