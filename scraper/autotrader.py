"""
scraper/autotrader.py
Playwright-based scraper for AutoTrader UK used car listings.
Designed to be resource-light: single browser instance, sequential pages,
generous delays. Prioritises completeness over speed.

Anti-detection:
- Randomised jitter on all delays
- Stealth JS patches (hides navigator.webdriver, spoofs plugins/languages)
- Realistic viewport, locale, timezone
- Cookie consent handled on first load
- Exponential backoff retry on transient failures
- Tracker/font request blocking (~40% bandwidth saving)
"""

import asyncio
import logging
import random
import re
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlencode

from playwright.async_api import (
    async_playwright, Page, BrowserContext,
    TimeoutError as PWTimeout,
)

logger = logging.getLogger(__name__)

# Injected into every page to mask headless Chromium fingerprints
_STEALTH_JS = """
() => {
    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
    Object.defineProperty(navigator, 'plugins',   {get: () => [1,2,3,4,5]});
    Object.defineProperty(navigator, 'languages', {get: () => ['en-GB','en']});
    window.chrome = {runtime: {}};
    const origQuery = window.navigator.permissions.query;
    window.navigator.permissions.query = (p) =>
        p.name === 'notifications'
            ? Promise.resolve({state: Notification.permission})
            : origQuery(p);
}
"""


@dataclass
class Listing:
    listing_id: str
    title: str
    price: Optional[int]
    year: Optional[int]
    mileage: Optional[int]
    location: str
    distance_miles: Optional[int]
    seller_type: str       # "dealer" or "private"
    seller_name: str
    spec_summary: str
    url: str
    source: str = "autotrader"
    image_urls: list = field(default_factory=list)
    attention_check: str = ""
    search_name: str = ""
    scraped_at: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


def _jitter(base: float, factor: float = 0.3) -> float:
    """Return base ± up to factor*base seconds of random jitter."""
    return max(0.5, base + random.uniform(-base * factor, base * factor))


def build_autotrader_url(cfg: dict, page_num: int = 1) -> str:
    params = {
        "sort": "price-asc",
        "fuel-type": cfg.get("fuel_type", "Electric"),
        "make": cfg.get("make", ""),
        "model": cfg.get("model", ""),
        "postcode": cfg.get("postcode", "EH1 1YZ"),
        "radius": cfg.get("radius", 200),
        "price-to": cfg.get("price_max", 25000),
        "year-from": cfg.get("year_from", 2020),
        "maximum-mileage": cfg.get("mileage_max", 120000),
        "page": page_num,
    }
    return "https://www.autotrader.co.uk/car-search?" + urlencode(params)


class AutoTraderScraper:
    def __init__(self, config: dict):
        self.config = config
        lim = config.get("limits", {})
        self.timeout        = lim.get("page_timeout_ms", 30000)
        self.scroll_pause   = lim.get("scroll_pause_ms", 1500) / 1000
        self.request_delay  = lim.get("request_delay_ms", 3000) / 1000
        self.search_delay   = lim.get("search_delay_ms", 8000) / 1000
        self.max_per_search = lim.get("max_listings_per_search", 20)
        self.max_pages      = lim.get("max_pages_per_search", 2)
        self.max_scrapes    = lim.get("max_scrapes_per_search", self.max_per_search * 4)
        self.max_images     = lim.get("max_images_per_listing", 3)
        self._cookie_accepted = False   # only need to do this once per session

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    async def scrape_all(self, searches: list, on_search_done=None) -> list:
        """Run all enabled searches sequentially, single browser instance."""
        all_listings = []
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                    "--disable-extensions",
                    "--disable-background-networking",
                    "--disable-background-timer-throttling",
                    "--js-flags=--max-old-space-size=256",
                ],
            )
            context = await browser.new_context(
                viewport={"width": 1366, "height": 768},
                user_agent=(
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
                ),
                locale="en-GB",
                timezone_id="Europe/London",
                java_script_enabled=True,
            )

            # Inject stealth script before any page load
            await context.add_init_script(_STEALTH_JS)

            # Block fonts, trackers, and ad networks
            await context.route(
                re.compile(
                    r"\.(woff2?|ttf|otf|eot)(\?|$)|"
                    r"(google-analytics|googletagmanager|gtm\.js|"
                    r"facebook\.net|hotjar|doubleclick|googlesyndication|"
                    r"clarity\.ms|adsystem)"
                ),
                lambda r: r.abort(),
            )

            for i, search in enumerate(searches):
                if not search.get("enabled", True):
                    logger.info(f"Skipping disabled search: {search['name']}")
                    continue
                logger.info(
                    f"[{i+1}/{len(searches)}] Starting: {search['name']}"
                )
                try:
                    results = await self._scrape_search(context, search)
                    all_listings.extend(results)
                    logger.info(
                        f"  → {len(results)} listings kept for {search['name']}"
                    )
                    if on_search_done is not None:
                        try:
                            on_search_done(results)
                        except Exception as cb_exc:
                            logger.error(
                                f"on_search_done callback failed for "
                                f"'{search['name']}': {cb_exc}", exc_info=True
                            )
                except Exception as exc:
                    logger.error(
                        f"Search '{search['name']}' failed: {exc}", exc_info=True
                    )
                if i < len(searches) - 1:
                    delay = _jitter(self.search_delay)
                    logger.debug(f"  Waiting {delay:.1f}s before next search…")
                    await asyncio.sleep(delay)

            await browser.close()
        return all_listings

    # ------------------------------------------------------------------
    # Private: search page → listing URLs
    # ------------------------------------------------------------------

    async def _scrape_search(self, context: BrowserContext, search: dict) -> list:
        listings = []
        at_cfg   = search["autotrader"]
        require  = [k.lower() for k in search.get("require_keywords", [])]
        exclude  = [k.lower() for k in search.get("exclude_keywords", [])]
        page_num = 1
        scrapes  = 0

        while len(listings) < self.max_per_search:
            if page_num > self.max_pages:
                logger.info(
                    f"  Page limit ({self.max_pages}) reached — stopping pagination"
                )
                break
            url = build_autotrader_url(at_cfg, page_num)
            page = await context.new_page()
            try:
                listing_urls = await self._get_listing_urls_with_retry(page, url)
            except Exception as exc:
                logger.warning(f"  Could not load search page {page_num}: {exc}")
                await page.close()
                break
            finally:
                # close() is idempotent — safe even if already closed
                try:
                    await page.close()
                except Exception:
                    pass

            if not listing_urls:
                logger.info(f"  Page {page_num}: no results — stopping pagination")
                break

            logger.info(
                f"  Page {page_num}: {len(listing_urls)} URLs found"
            )

            for url_idx, listing_url in enumerate(listing_urls):
                if len(listings) >= self.max_per_search:
                    break
                if scrapes >= self.max_scrapes:
                    logger.info(
                        f"  Scrape limit ({self.max_scrapes}) reached — stopping"
                    )
                    break
                scrapes += 1
                logger.info(
                    f"    [{url_idx+1}/{len(listing_urls)}] Scraping: "
                    f"{listing_url.split('/')[-1]}"
                )
                detail = await context.new_page()
                try:
                    listing = await self._scrape_listing_with_retry(
                        detail, listing_url, search["name"]
                    )
                    if listing and self._passes_filters(listing, require, exclude):
                        listings.append(listing)
                        price_str   = f"£{listing.price:,}" if listing.price else "£?"
                        mileage_str = f"{listing.mileage:,}mi" if listing.mileage else "?mi"
                        logger.info(
                            f"    ✓ {listing.title[:60]} | "
                            f"{price_str} | "
                            f"{mileage_str} | "
                            f"{listing.location[:30]}"
                        )
                    elif listing:
                        logger.debug(f"    ✗ Filtered out: {listing.title[:50]}")
                except Exception as exc:
                    logger.warning(f"    Failed {listing_url}: {exc}")
                finally:
                    try:
                        await detail.close()
                    except Exception:
                        pass
                await asyncio.sleep(_jitter(self.request_delay))

            page_num += 1

        return listings

    def _passes_filters(self, listing: Listing, require: list, exclude: list) -> bool:
        combined = f"{listing.title} {listing.spec_summary}".lower()
        if require and not all(k in combined for k in require):
            return False
        if any(k in combined for k in exclude):
            return False
        return True

    # ------------------------------------------------------------------
    # Retry wrappers
    # ------------------------------------------------------------------

    async def _get_listing_urls_with_retry(
        self, page: Page, url: str, max_retries: int = 3
    ) -> list:
        for attempt in range(max_retries):
            try:
                return await self._get_listing_urls(page, url)
            except PWTimeout:
                if attempt == max_retries - 1:
                    raise
                wait = (2 ** attempt) + _jitter(2)
                logger.warning(
                    f"  Timeout on search page (attempt {attempt+1}), "
                    f"retrying in {wait:.1f}s…"
                )
                await asyncio.sleep(wait)
        return []

    async def _scrape_listing_with_retry(
        self, page: Page, url: str, search_name: str, max_retries: int = 2
    ) -> Optional[Listing]:
        for attempt in range(max_retries):
            try:
                return await self._scrape_listing(page, url, search_name)
            except PWTimeout:
                if attempt == max_retries - 1:
                    return None
                await asyncio.sleep(_jitter(3))
        return None

    # ------------------------------------------------------------------
    # Search results page
    # ------------------------------------------------------------------

    async def _get_listing_urls(self, page: Page, url: str) -> list:
        await page.goto(url, timeout=self.timeout, wait_until="domcontentloaded")
        await asyncio.sleep(_jitter(1.0))

        # Handle cookie banner — only click once per session
        if not self._cookie_accepted:
            for sel in [
                'button:has-text("Accept all")',
                'button:has-text("Accept All")',
                'button:has-text("ACCEPT ALL")',
                '[data-testid="cookie-accept-all"]',
                '#onetrust-accept-btn-handler',
            ]:
                try:
                    btn = page.locator(sel).first
                    if await btn.is_visible(timeout=2500):
                        await btn.click()
                        await asyncio.sleep(0.8)
                        self._cookie_accepted = True
                        logger.debug("  Cookie banner dismissed")
                        break
                except Exception:
                    pass

        await self._scroll_page(page)

        # Extract hrefs from listing cards
        links = await page.eval_on_selector_all(
            'a[href*="/car-details/"]',
            "els => els.map(e => e.href)",
        )

        # Deduplicate and strip promoted/featured/YMAL injections.
        # AutoTrader injects these on every results page — they often don't
        # match the search criteria and would waste the entire page budget.
        _INJECTED_JOURNEYS = (
            "journey=PROMOTED_LISTING_JOURNEY",
            "journey=FEATURED_LISTING_JOURNEY",
            "journey=YOU_MAY_ALSO_LIKE_JOURNEY",
        )
        seen, clean = set(), []
        for link in links:
            base = link.split("?")[0]
            if base not in seen and "/car-details/" in link:
                if not any(j in link for j in _INJECTED_JOURNEYS):
                    seen.add(base)
                    clean.append(link)
        if len(clean) < len(links):
            logger.debug(
                f"  Filtered {len(links) - len(clean)} promoted/injected listings"
            )
        return clean

    # ------------------------------------------------------------------
    # Individual listing page
    # ------------------------------------------------------------------

    async def _scrape_listing(
        self, page: Page, url: str, search_name: str
    ) -> Optional[Listing]:
        await page.goto(url, timeout=self.timeout, wait_until="domcontentloaded")
        await asyncio.sleep(_jitter(0.8))

        # Listing ID from URL
        id_match = re.search(r"/car-details/(\d+)", url)
        listing_id = id_match.group(1) if id_match else url.split("/")[-1]

        # ---------- Price ----------
        price = None
        for sel in [
            '[data-testid="listing-price"]',
            '[class*="price--"] h2',
            '.vehicle-price',
            'h2[class*="price"]',
            'span[class*="price"]',
        ]:
            try:
                raw = await page.locator(sel).first.inner_text(timeout=3000)
                digits = re.sub(r"[^\d]", "", raw)
                if digits and int(digits) > 500:   # sanity: not a partial number
                    price = int(digits)
                    break
            except Exception:
                pass

        # ---------- Title ----------
        title = ""
        for sel in [
            '[data-testid="advert-title"]',
            'h1[class*="title"]',
            'h1',
        ]:
            try:
                t = (await page.locator(sel).first.inner_text(timeout=3000)).strip()
                if t and len(t) > 5:
                    title = t
                    break
            except Exception:
                pass

        # ---------- Spec block ----------
        spec_summary = ""
        for sel in [
            '[data-testid="spec-list"]',
            '[class*="key-specs"]',
            '.key-specifications',
            'ul[class*="spec"]',
        ]:
            try:
                raw = await page.locator(sel).first.inner_text(timeout=3000)
                spec_summary = raw.replace("\n", " | ").strip()
                if spec_summary:
                    break
            except Exception:
                pass

        # ---------- Year, Mileage (from body text fallback) ----------
        year, mileage = None, None
        try:
            body = await page.inner_text("body", timeout=8000)
            y = re.search(r"(?:Year|Reg(?:istration)?)\s*[:\|]?\s*(\d{4})", body)
            m = re.search(r"([\d,]+)\s*miles?", body)
            if y:
                year = int(y.group(1))
            if m:
                candidate = int(m.group(1).replace(",", ""))
                if 100 < candidate < 300000:   # sanity bounds
                    mileage = candidate
        except Exception:
            pass

        # ---------- Location ----------
        location = ""
        for sel in [
            '[data-testid="seller-location"]',
            '[class*="seller-location"]',
            '[class*="location"]',
        ]:
            try:
                loc = (await page.locator(sel).first.inner_text(timeout=2000)).strip()
                if loc:
                    location = loc
                    break
            except Exception:
                pass
        dist_m = re.search(r"(\d+)\s*miles?\s*away", location, re.I)
        distance_miles = int(dist_m.group(1)) if dist_m else None

        # ---------- Seller ----------
        seller_name, seller_type = "", "dealer"
        for sel in [
            '[data-testid="seller-name"]',
            '[class*="seller-name"]',
            '[class*="dealer-name"]',
        ]:
            try:
                sn = (await page.locator(sel).first.inner_text(timeout=2000)).strip()
                if sn:
                    seller_name = sn
                    break
            except Exception:
                pass
        try:
            content_snippet = (await page.content())[:2000].lower()
            if "private seller" in content_snippet or "private" in seller_name.lower():
                seller_type = "private"
        except Exception:
            pass

        # ---------- Images ----------
        image_urls = []
        try:
            imgs = await page.eval_on_selector_all(
                'img[src*="media.autotrader"], img[src*="cdn.at"], '
                'img[src*="cdnimages"]',
                "els => els.map(e => e.src)",
            )
            image_urls = [
                i for i in imgs
                if i and "placeholder" not in i.lower() and len(i) > 40
            ][: self.max_images]
        except Exception:
            pass

        # ---------- Attention flags ----------
        flags = []
        try:
            page_text = (await page.inner_text("body", timeout=5000)).lower()
        except Exception:
            page_text = ""

        if "battery" in page_text and any(
            w in page_text for w in ("certificate", "health report", "aviloo", "soh")
        ):
            flags.append("🔋 Battery cert mentioned")
        if mileage and mileage > 80000:
            flags.append(f"⚠️ High mileage ({mileage:,}mi)")
        if mileage and mileage < 25000:
            flags.append("✅ Low mileage")
        if any(w in page_text for w in ("write-off", "category s", "category n", "cat s", "cat n")):
            flags.append("🚨 Write-off language detected — check HPI")
        if seller_type == "private":
            flags.append("👤 Private seller")
        if "warranty" in page_text:
            flags.append("🛡️ Warranty mentioned")
        if price and price < 13000:
            flags.append("💰 Very low price — verify condition carefully")
        if "double-sided" in page_text or "reversible boot" in page_text:
            flags.append("🐕 Dog-friendly boot liner mentioned")
        if "service history" in page_text:
            flags.append("📋 Service history mentioned")

        return Listing(
            listing_id=listing_id,
            title=title or f"AutoTrader listing {listing_id}",
            price=price,
            year=year,
            mileage=mileage,
            location=location,
            distance_miles=distance_miles,
            seller_type=seller_type,
            seller_name=seller_name,
            spec_summary=spec_summary,
            url=url,
            source="autotrader",
            image_urls=image_urls,
            attention_check=" | ".join(flags),
            search_name=search_name,
            scraped_at=datetime.now(timezone.utc).isoformat(),
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _scroll_page(self, page: Page):
        """Scroll to trigger lazy-loaded listing cards."""
        try:
            for _ in range(5):
                await page.keyboard.press("End")
                await asyncio.sleep(_jitter(self.scroll_pause))
            # Scroll back up slightly (more human-like)
            await page.keyboard.press("Home")
            await asyncio.sleep(0.3)
        except Exception:
            pass