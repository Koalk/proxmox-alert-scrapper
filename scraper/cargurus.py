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
from urllib.parse import urlencode, quote_plus

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

# Map our config make/model names to CarGurus search slugs
# CarGurus uses a dropdown-based search — easiest to hit their
# internal listing search URL directly
_MAKE_MAP = {
    "Skoda":      "SKODA",
    "Kia":        "KIA",
    "Hyundai":    "HYUNDAI",
    "Volkswagen": "VOLKSWAGEN",
    "BMW":        "BMW",
    "BYD":        "BYD",
    "Tesla":      "TESLA",
    "Mercedes":   "MERCEDES-BENZ",
    "Citroen":    "CITROEN",
    "Peugeot":    "PEUGEOT",
}


def build_cargurus_url(cfg: dict, page_num: int = 1) -> str:
    """
    Build a CarGurus UK search URL.
    Uses their /UsedCars/listing endpoint with query params.
    """
    make = _MAKE_MAP.get(cfg.get("make", ""), cfg.get("make", ""))
    model = cfg.get("model", "")
    params = {
        "zip":          cfg.get("postcode", "EH1 1YZ").replace(" ", ""),
        "distance":     cfg.get("radius", 200),
        "trim":         "",
        "priceMax":     cfg.get("price_max", 25000),
        "mileageMax":   cfg.get("mileage_max", 120000),
        "startYear":    cfg.get("year_from", 2020),
        "fuelTypes":    "electric",
        "sortDir":      "ASC",
        "sortType":     "PRICE",
        "offset":       (page_num - 1) * 15,
    }
    slug = f"{quote_plus(make)}/{quote_plus(model)}"
    return f"https://www.cargurus.co.uk/Cars/new/nl__{slug}#listing?" + urlencode(params)


class CarGurusScraper:
    """
    Lightweight CarGurus scraper sharing the same Listing dataclass
    as AutoTraderScraper so results from both sources merge cleanly
    into the same database and email digest.
    """

    def __init__(self, config: dict):
        self.config = config
        lim = config.get("limits", {})
        self.timeout        = lim.get("page_timeout_ms", 35000)
        self.scroll_pause   = lim.get("scroll_pause_ms", 1500) / 1000
        self.request_delay  = lim.get("request_delay_ms", 3500) / 1000
        self.search_delay   = lim.get("search_delay_ms", 10000) / 1000
        self.max_per_search = lim.get("max_listings_per_search", 40)
        self.max_images     = lim.get("max_images_per_listing", 3)
        self._cookie_done   = False

    async def scrape_all(self, searches: list, on_search_done=None) -> list:
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
                    results = await self._scrape_search(context, search)
                    all_listings.extend(results)
                    logger.info(
                        f"  → {len(results)} CarGurus listings for {search['name']}"
                    )
                    if on_search_done and results:
                        on_search_done(results)
                except Exception as exc:
                    logger.error(f"CarGurus search failed: {exc}", exc_info=True)
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
        await page.goto(url, timeout=self.timeout, wait_until="domcontentloaded")
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

        # Scroll to load lazy listings
        for _ in range(4):
            await page.keyboard.press("End")
            await asyncio.sleep(_jitter(self.scroll_pause))

        # Extract listing data from card elements
        cards = []
        try:
            cards = await page.evaluate("""
            () => {
                const results = [];
                // CarGurus listing cards — selectors based on current structure
                const containers = document.querySelectorAll(
                    '[data-cg-ft="car-blade"], [class*="CarCard"], [class*="listing-row"]'
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
                    const title   = getText('h4, h3, [class*="title"]');
                    const price   = getText('[class*="price"], [data-cg-ft="price"]');
                    const mileage = getText('[class*="mileage"], [class*="miles"]');
                    const location= getText('[class*="dealer-name"], [class*="location"]');
                    const deal    = getText('[class*="deal-rating"], [class*="priceAnalysis"]');
                    const link    = getAttr('a[href*="/usedcars/"]', 'href') ||
                                    getAttr('a', 'href');
                    const img     = getAttr('img', 'src');
                    if (title || price) {
                        results.push({title, price, mileage, location, deal, link, img});
                    }
                });
                return results;
            }
            """)
        except Exception as exc:
            logger.debug(f"CarGurus card extraction error: {exc}")

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

        # Deduplicate ID — use URL fragment or title hash
        listing_id = "cg_" + re.sub(r"[^\w]", "", link)[-20:] if link else (
            "cg_" + re.sub(r"\s+", "_", title[:30])
        )

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