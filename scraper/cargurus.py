"""
scraper/cargurus.py
CarGurus UK scraper — complements AutoTrader to catch listings
that don't appear on both platforms.

CarGurus is generally more scraper-tolerant than AutoTrader and adds
a useful "deal rating" (Great Deal / Good Deal / Fair / Overpriced)
based on market price analysis — this is extracted and included in
the attention flags.
"""

import asyncio
import logging
import random
import re
from datetime import datetime
from typing import Optional
from urllib.parse import urlencode

from playwright.async_api import (
    async_playwright, Page, BrowserContext, TimeoutError as PWTimeout,
)

from scraper.autotrader import Listing, _jitter, _STEALTH_JS

logger = logging.getLogger(__name__)

# CarGurus deal-rating colour keywords → human label
_DEAL_RATINGS = {
    "great deal": "🟢 Great Deal",
    "good deal":  "🟡 Good Deal",
    "fair deal":  "🟠 Fair Deal",
    "overpriced": "🔴 Overpriced",
    "no price":   "⚪ No Price Analysis",
}

_BASE_URL = (
    "https://www.cargurus.co.uk/Cars/inventorylisting/"
    "viewDetailsFilterViewInventoryListing.action"
)

# (make_id, model_id or None)
# model_id=None → make-only search with fuelTypes=ELECTRIC fallback
_MAKE_MODEL_IDS: dict[tuple[str, str], tuple[str, str | None]] = {
    ("Kia",        "EV6"):     ("m306", "d6251"),
    ("Kia",        "EV3"):     ("m306", None),
    ("Hyundai",    "Ioniq 5"): ("m279", None),   # model ID unconfirmed → make+fuel
    ("Hyundai",    "Ioniq 6"): ("m279", None),
    ("Skoda",      "Enyaq"):   ("m207", None),   # model ID unconfirmed → make+fuel
    ("Volkswagen", "ID.3"):    ("m203", "d6006"),
    ("Volkswagen", "ID.4"):    ("m203", None),
    ("Volkswagen", "ID.5"):    ("m203", None),
    ("Tesla",      "Model 3"): ("m266", "d5176"),
    ("Tesla",      "Model Y"): ("m266", "d6299"),
    ("BMW",        ""):        ("m256", None),
    ("BYD",        ""):        ("m451", None),
    ("Mercedes",   ""):        ("m297", None),
    ("Citroen",    ""):        ("m263", None),
    ("Peugeot",    ""):        ("m318", None),
}

# Fallback: make-only IDs for unknown (make, model) combos
_MAKE_IDS: dict[str, str] = {
    "Kia":        "m306",
    "Hyundai":    "m279",
    "Skoda":      "m207",
    "Volkswagen": "m203",
    "BMW":        "m256",
    "BYD":        "m451",
    "Tesla":      "m266",
    "Mercedes":   "m297",
    "Citroen":    "m263",
    "Peugeot":    "m318",
}


def build_cargurus_url(cfg: dict, page_num: int = 1) -> str:
    """
    Build a CarGurus UK inventory search URL using the new
    viewDetailsFilterViewInventoryListing.action endpoint.

    CarGurus requires makeModelTrimPaths to appear twice when a model
    is specified: once as 'm{X}%2Fd{Y}' (make+model) and once as 'm{X}'
    (make only). urllib urlencode handles repeated keys via a list of
    tuples.
    """
    make  = cfg.get("make", "")
    model = cfg.get("model", "")

    make_id, model_id = _MAKE_MODEL_IDS.get(
        (make, model),
        (_MAKE_IDS.get(make), None),
    )

    params: list[tuple[str, str]] = [
        ("zip",               cfg.get("postcode", "EH1 1YZ").replace(" ", "")),
        ("distance",          str(cfg.get("radius", 200))),
        ("maxPrice",          str(cfg.get("price_max", 25000))),
        ("maxMileage",        str(cfg.get("mileage_max", 120000))),
        ("startYear",         str(cfg.get("year_from", 2020))),
        ("sortDir",           "ASC"),
        ("sortType",          "PRICE"),
        ("isDeliveryEnabled", "true"),
        ("srpVariation",      "DEFAULT_SEARCH"),
    ]

    if page_num > 1:
        params.append(("startIndex", str((page_num - 1) * 15)))

    if make_id and model_id:
        params.append(("makeModelTrimPaths", f"{make_id}/{model_id}"))
        params.append(("makeModelTrimPaths", make_id))
        params.append(("entitySelectingHelper.selectedEntity", model_id))
    elif make_id:
        params.append(("makeModelTrimPaths", make_id))
        params.append(("fuelTypes",          "ELECTRIC"))
    else:
        params.append(("fuelTypes", "ELECTRIC"))

    return _BASE_URL + "?" + urlencode(params)


class CarGurusScraper:
    """
    Lightweight CarGurus scraper sharing the same Listing dataclass
    as AutoTraderScraper so results from both sources merge cleanly
    into the same database and email digest.
    """

    def __init__(self, config: dict):
        self.config = config
        lim = config.get("limits", {})
        self.timeout         = lim.get("page_timeout_ms", 35000)
        self.scroll_pause    = lim.get("scroll_pause_ms", 1500) / 1000
        self.request_delay   = lim.get("request_delay_ms", 3500) / 1000
        self.search_delay    = lim.get("search_delay_ms", 10000) / 1000
        self.max_per_search  = lim.get("max_listings_per_search", 20)
        self.max_images      = lim.get("max_images_per_listing", 3)
        # Hard ceiling on any single search — prevents one hung page freezing
        # the entire run. Default 4 minutes; CarGurus pages can be slow.
        self.search_timeout  = lim.get("cargurus_search_timeout_s", 240)
        self._cookie_done    = False

    async def scrape_all(self, searches: list, on_search_done=None, known_ids=None) -> list:
        """Run all enabled searches sequentially."""
        all_listings = []
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu",
                    "--js-flags=--max-old-space-size=256",
                ],
            )
            context = await browser.new_context(
                viewport={"width": 1440, "height": 900},
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                locale="en-GB",
                timezone_id="Europe/London",
            )
            await context.add_init_script(_STEALTH_JS)
            await context.route(
                re.compile(
                    r"\.(woff2?|ttf|otf)(\?|$)|"
                    r"(google-analytics|googletagmanager|hotjar|"
                    r"doubleclick|criteo|bidswitch)"
                ),
                lambda r: r.abort(),
            )

            for i, search in enumerate(searches):
                if not search.get("enabled", True):
                    continue
                logger.info(
                    f"[CarGurus {i+1}/{len(searches)}] {search['name']}"
                )
                try:
                    results = await asyncio.wait_for(
                        self._scrape_search(context, search),
                        timeout=self.search_timeout,
                    )
                    all_listings.extend(results)
                    logger.info(
                        f"  → {len(results)} CarGurus listings for {search['name']}"
                    )
                    if on_search_done and results:
                        on_search_done(results)
                except asyncio.TimeoutError:
                    logger.error(
                        f"CarGurus search '{search['name']}' timed out after "
                        f"{self.search_timeout}s — skipping"
                    )
                except Exception as exc:
                    logger.error(
                        f"CarGurus search '{search['name']}' failed: {exc}",
                        exc_info=True,
                    )
                if i < len(searches) - 1:
                    await asyncio.sleep(_jitter(self.search_delay))

            await browser.close()
        return all_listings

    async def _scrape_search(self, context: BrowserContext, search: dict) -> list:
        listings = []
        at_cfg   = search["autotrader"]   # reuse same config block
        require  = [k.lower() for k in search.get("require_keywords", [])]
        exclude  = [k.lower() for k in search.get("exclude_keywords", [])]
        page_num = 1

        while len(listings) < self.max_per_search:
            url  = build_cargurus_url(at_cfg, page_num)
            page = await context.new_page()
            try:
                cards = await self._get_listing_cards(page, url)
            except Exception as exc:
                logger.warning(f"  CarGurus page {page_num} failed: {exc}")
                await page.close()
                break
            finally:
                try:
                    await page.close()
                except Exception:
                    pass

            if not cards:
                logger.info(f"  CarGurus page {page_num}: no results")
                break

            logger.info(f"  CarGurus page {page_num}: {len(cards)} cards")

            for card_data in cards:
                if len(listings) >= self.max_per_search:
                    break
                # Cards already contain most data — no need for detail page
                listing = self._card_to_listing(card_data, search["name"])
                if listing:
                    combined = f"{listing.title} {listing.spec_summary}".lower()
                    if require and not all(k in combined for k in require):
                        continue
                    if any(k in combined for k in exclude):
                        continue
                    listings.append(listing)
                await asyncio.sleep(_jitter(self.request_delay * 0.5))

            page_num += 1

        return listings

    async def _get_listing_cards(self, page: Page, url: str) -> list:
        """
        Load a CarGurus search page and extract structured card data.
        CarGurus renders listings in React — we wait for the listing
        container then extract key info from each card without visiting
        individual detail pages (saves significant time and resources).
        """
        # networkidle waits for the React app to finish its initial data fetch.
        # Fall back to domcontentloaded + extra sleep on timeout.
        try:
            await page.goto(url, timeout=self.timeout, wait_until="networkidle")
        except PWTimeout:
            logger.warning("  CarGurus: networkidle timed out, retrying with domcontentloaded")
            await page.goto(url, timeout=self.timeout, wait_until="domcontentloaded")
            await asyncio.sleep(3)

        # Log what we actually landed on — helps spot redirects / captchas
        try:
            page_title = await asyncio.wait_for(page.title(), timeout=5)
            logger.debug(f"  CarGurus page title: {page_title!r}  url: {page.url[:100]}")
        except Exception:
            pass

        await asyncio.sleep(_jitter(1.5))

        # Dismiss cookie banner
        if not self._cookie_done:
            for sel in [
                'button:has-text("Accept All")',
                'button:has-text("Accept all Cookies")',
                '#onetrust-accept-btn-handler',
                '[class*="accept"]',
            ]:
                try:
                    btn = page.locator(sel).first
                    if await btn.is_visible(timeout=3000):
                        await btn.click()
                        self._cookie_done = True
                        await asyncio.sleep(0.8)
                        break
                except Exception:
                    pass

        # Scroll to load lazy listings — each keyboard press bounded so a
        # hung page can't block the whole run indefinitely.
        for _ in range(4):
            try:
                await asyncio.wait_for(page.keyboard.press("End"), timeout=5)
            except (asyncio.TimeoutError, Exception):
                break
            await asyncio.sleep(_jitter(self.scroll_pause))

        # Extract listing data from card elements.
        # page.evaluate() has no built-in timeout — wrap it ourselves.
        # Selectors target the new inventory listing page structure (2025+).
        _JS_EXTRACT = """
        () => {
            const results = [];
            // Primary: new structured listing cards
            const containers = document.querySelectorAll(
                '[data-cg-ft="car-blade"], '
                + '[class*="CarCard"], '
                + '[class*="listing-row"], '
                + '[data-testid*="listing"], '
                + 'li[class*="result"]'
            );
            containers.forEach(el => {
                const getText = (sel) => {
                    const e = el.querySelector(sel);
                    return e ? e.innerText.trim() : '';
                };
                const getAttr = (sel, attr) => {
                    const e = el.querySelector(sel);
                    return e ? e.getAttribute(attr) : '';
                };
                const title    = getText('h4, h3, h2, [class*="title"]');
                const price    = getText('[class*="price"], [data-cg-ft="price"]');
                const mileage  = getText('[class*="mileage"], [class*="miles"]');
                const location = getText('[class*="dealer-name"], [class*="location"], [class*="seller"]');
                const deal     = getText('[class*="deal-rating"], [class*="priceAnalysis"], [class*="dealScore"]');
                // Links go to /Cars/inventorylisting/vdp.action?listingId=NNNNN
                const link     = getAttr('a[href*="listingId"]', 'href') ||
                                 getAttr('a[href*="vdp.action"]', 'href') ||
                                 getAttr('a', 'href');
                const img      = getAttr('img[src*="vehicleimage"]', 'src') ||
                                 getAttr('img[src*="cargurus"]', 'src') ||
                                 getAttr('img', 'src');
                if (title || price) {
                    results.push({title, price, mileage, location, deal, link, img});
                }
            });
            return results;
        }
        """
        cards = []
        try:
            cards = await asyncio.wait_for(page.evaluate(_JS_EXTRACT), timeout=15)
        except asyncio.TimeoutError:
            logger.warning("  CarGurus: JS card extraction timed out")
        except Exception as exc:
            logger.debug(f"CarGurus card extraction error: {exc}")

        if not cards:
            # Diagnostic: log what elements are present so we can tune selectors.
            try:
                counts = await asyncio.wait_for(
                    page.evaluate("""
                    () => ({
                        blade:     document.querySelectorAll('[data-cg-ft="car-blade"]').length,
                        carcard:   document.querySelectorAll('[class*="CarCard"]').length,
                        row:       document.querySelectorAll('[class*="listing-row"]').length,
                        testid:    document.querySelectorAll('[data-testid*="listing"]').length,
                        result_li: document.querySelectorAll('li[class*="result"]').length,
                        all_links: document.querySelectorAll('a[href*="listingId"]').length,
                        body_len:  document.body ? document.body.innerText.length : 0,
                        title:     document.title,
                    })
                    """),
                    timeout=5,
                )
                logger.info(
                    f"  CarGurus selector counts: "
                    f"blade={counts.get('blade')}, carcard={counts.get('carcard')}, "
                    f"row={counts.get('row')}, testid={counts.get('testid')}, "
                    f"result_li={counts.get('result_li')}, "
                    f"listing_links={counts.get('all_links')}, "
                    f"body_chars={counts.get('body_len')}, "
                    f"page_title={counts.get('title')!r}"
                )
            except Exception:
                pass

        return cards

    def _card_to_listing(self, card: dict, search_name: str) -> Optional[Listing]:
        """Convert a raw card dict to a Listing object."""
        title    = card.get("title", "").strip()
        price_raw = card.get("price", "")
        mileage_raw = card.get("mileage", "")
        location = card.get("location", "").strip()
        deal_raw = (card.get("deal", "") or "").lower()
        link     = card.get("link", "")
        img      = card.get("img", "")

        if not title and not link:
            return None

        # Normalise URL
        if link and not link.startswith("http"):
            link = "https://www.cargurus.co.uk" + link

        # Parse price
        price = None
        digits = re.sub(r"[^\d]", "", price_raw)
        if digits and int(digits) > 500:
            price = int(digits)

        # Parse mileage
        mileage = None
        m = re.search(r"([\d,]+)", mileage_raw)
        if m:
            candidate = int(m.group(1).replace(",", ""))
            if 100 < candidate < 300000:
                mileage = candidate

        # Year from title
        year = None
        y = re.search(r"\b(20[12]\d)\b", title)
        if y:
            year = int(y.group(1))

        # Deal rating flag
        deal_flag = ""
        for key, label in _DEAL_RATINGS.items():
            if key in deal_raw:
                deal_flag = label
                break

        flags = [deal_flag] if deal_flag else []
        if mileage and mileage > 80000:
            flags.append(f"⚠️ High mileage ({mileage:,}mi)")
        if mileage and mileage < 25000:
            flags.append("✅ Low mileage")

        # Listing ID: extract integer listingId from URL, or fall back to
        # the hash fragment from the old format, or use a title hash.
        listing_id = ""
        if link:
            m_id = re.search(r"listingId=?(\d+)", link)
            if not m_id:
                # Older-format hash fragment: #listing=156913750/...
                m_id = re.search(r"#listing=(\d+)", link)
            if m_id:
                listing_id = "cg_" + m_id.group(1)
        if not listing_id:
            listing_id = "cg_" + re.sub(r"\s+", "_", title[:30])

        return Listing(
            listing_id=listing_id,
            title=title or "CarGurus listing",
            price=price,
            year=year,
            mileage=mileage,
            location=location,
            distance_miles=None,
            seller_type="dealer",
            seller_name=location,
            spec_summary="",
            url=link or "https://www.cargurus.co.uk",
            source="cargurus",
            image_urls=[img] if img and len(img) > 20 else [],
            attention_check=" | ".join(flags),
            search_name=search_name,
            scraped_at=datetime.utcnow().isoformat(),
        )