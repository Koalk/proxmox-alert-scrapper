"""
scraper/autotrader.py — AutoTrader UK Playwright scraper

ARCHITECTURE:
  AutoTraderScraper.scrape_all(searches, on_search_done, known_ids)
    → _scrape_search()   builds search URL, paginates up to max_pages
      → _get_listing_urls()  extracts href list from results page
      → _scrape_listing()    visits EACH detail page individually (slow, rich)
  Listing dataclass is defined HERE and imported by motors.py + cargurus.py.
  _jitter() and _STEALTH_JS are also imported by both other scrapers.

KEY GOTCHAS:
  - price, year, mileage MUST all be initialized to None together at the top
    of _scrape_listing() — Python treats any assigned-to name as local, so
    leaving year out causes UnboundLocalError on every listing (was a bug).
  - Promoted/injected listings (PROMOTED_LISTING_JOURNEY etc.) are stripped
    from the URL list before scraping — they don't match search criteria.
  - known_ids skips pages where every result is already in the DB; it tries
    the next page instead (up to max_pages).
  - _passes_filters() does require_keywords / exclude_keywords matching.

CONFIG KEYS (under search.autotrader): make, model, price_max, year_from,
  mileage_max, postcode, radius, fuel_type.
"""

import asyncio
import json
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
        self.max_scrapes          = lim.get("max_scrapes_per_search", self.max_per_search * 4)
        self.max_images           = lim.get("max_images_per_listing", 3)
        self.max_concurrent_pages = lim.get("max_concurrent_pages", 1)
        self._cookie_accepted = False   # only need to do this once per session

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    async def scrape_all(self, searches: list, on_search_done=None, known_ids=None) -> list:
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
                    f"[AutoTrader {i+1}/{len(searches)}] {search['name']}"
                )
                try:
                    results = await self._scrape_search(context, search, known_ids=known_ids)
                    all_listings.extend(results)
                    logger.info(
                        f"  → {len(results)} listings for {search['name']}"
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

    async def _scrape_search(
        self, context: BrowserContext, search: dict, known_ids: set | None = None
    ) -> list:
        listings = []
        at_cfg   = search["autotrader"]
        require  = [k.lower() for k in search.get("require_keywords", [])]
        exclude  = [k.lower() for k in search.get("exclude_keywords", [])]
        page_num = 1
        _id_re   = re.compile(r"/car-details/(\d+)")

        # Walk pages until we find one that contains at least one unknown listing,
        # then scrape that page (up to max_per_search) and stop.
        # This means we only ever visit ONE page of results per search — the very
        # first page where new listings exist. If page 1 is entirely familiar
        # (all IDs already in the DB), we try page 2, and so on up to max_pages.
        while True:
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

            logger.info(f"  Page {page_num}: {len(listing_urls)} URLs found")

            # If a known-IDs set was supplied, skip pages where every result is
            # already in the database — those listings won't produce anything new.
            if known_ids is not None:
                page_ids = set()
                for u in listing_urls:
                    m = _id_re.search(u)
                    if m:
                        page_ids.add(m.group(1))
                if page_ids and page_ids.issubset(known_ids):
                    logger.info(
                        f"  Page {page_num}: all {len(page_ids)} results already in DB "
                        f"— checking page {page_num + 1} for new listings"
                    )
                    page_num += 1
                    continue

            # Scrape this page's listings concurrently, bounded by max_concurrent_pages.
            # Pre-slice so we never visit more detail pages than max_scrapes allows.
            urls_to_scrape = listing_urls[: min(len(listing_urls), self.max_scrapes)]
            logger.info(
                f"  Scraping {len(urls_to_scrape)} listings "
                f"(concurrency: {self.max_concurrent_pages})…"
            )
            sem = asyncio.Semaphore(self.max_concurrent_pages)

            async def _scrape_one(listing_url: str) -> "Listing | None":
                async with sem:
                    detail = await context.new_page()
                    try:
                        result = await self._scrape_listing_with_retry(
                            detail, listing_url, search["name"]
                        )
                        if result and self._passes_filters(result, require, exclude):
                            price_str   = f"£{result.price:,}" if result.price else "£?"
                            mileage_str = f"{result.mileage:,}mi" if result.mileage else "?mi"
                            logger.info(
                                f"    ✓ {result.title[:60]} | "
                                f"{price_str} | {mileage_str} | "
                                f"{result.location[:30]}"
                            )
                            return result
                        if result:
                            logger.debug(f"    ✗ Filtered out: {result.title[:50]}")
                        return None
                    except Exception as exc:
                        logger.warning(f"    Failed {listing_url}: {exc}")
                        return None
                    finally:
                        try:
                            await detail.close()
                        except Exception:
                            pass
                        await asyncio.sleep(_jitter(self.request_delay))

            raw = await asyncio.gather(*[_scrape_one(u) for u in urls_to_scrape])
            listings = [r for r in raw if r is not None][: self.max_per_search]

            # Always stop after scraping one page — one page per search
            break

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
        await asyncio.sleep(_jitter(1.5))

        # Listing ID from URL
        id_match = re.search(r"/car-details/(\d+)", url)
        listing_id = id_match.group(1) if id_match else url.split("/")[-1]

        # ---------- JSON-LD (most stable — AutoTrader embeds schema.org data) ----------
        price, year, mileage = None, None, None
        try:
            ld_scripts = await page.eval_on_selector_all(
                'script[type="application/ld+json"]',
                'els => els.map(e => e.textContent)',
            )
            for ld_text in ld_scripts:
                try:
                    ld = json.loads(ld_text)
                    items = ld if isinstance(ld, list) else ld.get('@graph', [ld])
                    for item in (items if isinstance(items, list) else [items]):
                        if not isinstance(item, dict):
                            continue
                        if price is None:
                            offer = item.get('offers', {})
                            p = (offer.get('price') if isinstance(offer, dict) else None) or item.get('price')
                            if p is not None:
                                try:
                                    candidate = int(float(str(p).replace(',', '')))
                                    if 500 < candidate < 200000:
                                        price = candidate
                                except (ValueError, TypeError):
                                    pass
                        if mileage is None:
                            m_obj = item.get('mileageFromOdometer', {})
                            if isinstance(m_obj, dict) and m_obj.get('value'):
                                try:
                                    candidate = int(float(str(m_obj['value'])))
                                    if 100 < candidate < 300000:
                                        mileage = candidate
                                except (ValueError, TypeError):
                                    pass
                except Exception:
                    pass
        except Exception:
            pass

        # ---------- Price (CSS selectors + body text fallback) ----------
        if price is None:
            for sel in [
                '[data-testid="listing-price"]',
                '[data-testid="advert-price"]',
                '[class*="price--"] h2',
                '[class*="priceIndicator"]',
                '.vehicle-price',
                'h2[class*="price"]',
                'span[class*="price"]',
            ]:
                try:
                    raw = await page.locator(sel).first.inner_text(timeout=2000)
                    digits = re.sub(r"[^\d]", "", raw)
                    if digits and int(digits) > 500:
                        price = int(digits)
                        break
                except Exception:
                    pass

        # ---------- Title ----------
        # Try the browser tab title first — AutoTrader sets it to the full
        # variant name, e.g. "2021 Skoda Enyaq iV 60 58kWh 132PS | AutoTrader".
        # This is the richest source for exclude_keywords matching (e.g. "iV 60").
        title = ""
        try:
            tab_title = await page.title()
            for suffix in (" | AutoTrader", " | Auto Trader", " - AutoTrader"):
                if tab_title.endswith(suffix):
                    tab_title = tab_title[: -len(suffix)].strip()
                    break
            if tab_title and len(tab_title) > 5:
                title = tab_title
        except Exception:
            pass
        if not title:
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
        try:
            body = await page.inner_text("body", timeout=8000)
        except Exception:
            body = ""

        if year is None:
            try:
                y = re.search(r"(?:Year|Reg(?:istration)?)\s*[:\|]?\s*(\d{4})", body)
                if y:
                    year = int(y.group(1))
            except Exception:
                pass

        if mileage is None:
            try:
                # Exclude "X miles away" — match mileage odometer readings only
                candidates = re.findall(
                    r'([\d,]+)\s+miles?(?!\s+away)(?!\s+from)', body
                )
                for mc in candidates:
                    candidate = int(mc.replace(',', ''))
                    if 100 < candidate < 300000:
                        mileage = candidate
                        break
            except Exception:
                pass

        if price is None:
            try:
                price_matches = re.findall(r'£([\d,]+)', body)
                for p_str in price_matches:
                    candidate = int(p_str.replace(',', ''))
                    if 1000 < candidate < 150000:
                        price = candidate
                        break
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
        # Fallback: parse location from the page title when the selector returns nothing.
        # e.g. "2023 White Skoda Enyaq for sale for £11,552 in Wetherby, LEEDS"
        # Stopgap until the proper CSS selector is verified working.
        if not location and title:
            in_match = re.search(r" in (.+)$", title)
            if in_match:
                location = in_match.group(1).strip()

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
                'img[src*="atcdn.co.uk"], img[src*="media.autotrader"], '
                'img[src*="cdn.at"], img[src*="cdnimages"]',
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
            page_text = body.lower() if body else (await page.inner_text("body", timeout=5000)).lower()
        except Exception:
            page_text = ""

        if "battery" in page_text and any(
            w in page_text for w in ("certificate", "health report", "aviloo", "soh")
        ):
            flags.append("🔋 Battery cert mentioned")
        if mileage is not None:
            if mileage < 25000:
                flags.append("✅ Low mileage")
            elif mileage > 85000:
                flags.append(f"🔴 Very high mileage — battery check essential ({mileage:,}mi)")
            elif mileage > 60000:
                flags.append(f"⚠️ Higher mileage ({mileage:,}mi)")
            # 25k–60k is normal — no flag needed
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
        commercial_signals = [
            "pco", "private hire", "taxi", "uber", "bolt driver", "lyft",
            "fleet", "rental", "hire car", "ex fleet", "ex-fleet",
            "company car", "lease return", "minicab", "hackney",
        ]
        if any(s in page_text for s in commercial_signals):
            flags.append("🚕 Commercial use signals found — verify history")
        if search_name == "Hyundai Ioniq 5" and year in (2022, 2023, 2024):
            flags.append("⚠️ ICCU recall risk — verify software fix applied (2022-24 models)")

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