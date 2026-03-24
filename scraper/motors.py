"""
scraper/motors.py
Motors.co.uk scraper — UK's third-largest used car classifieds.
Complements AutoTrader and CarGurus with dealer-only listings,
all HPI-checked before advertising.

URL format: https://www.motors.co.uk/search/?make=Kia&model=EV+6&...
Page rendering: React SPA — Playwright required.
Listing IDs: numeric, extracted from /car-{id}/ URL slugs.
"""

import asyncio
import logging
import re
from datetime import datetime
from typing import Optional
from urllib.parse import urlencode

from playwright.async_api import (
    async_playwright, BrowserContext, Page, TimeoutError as PWTimeout,
)

from scraper.autotrader import Listing, _jitter, _STEALTH_JS

logger = logging.getLogger(__name__)

# Motors.co.uk uses URL-friendly model strings (spaces → +, special chars stripped)
# Map config model names to Motors query values where they differ from the
# plain model name.
_MODEL_OVERRIDES: dict[str, str] = {
    "Ioniq 5":  "Ioniq+5",
    "ID.3":     "ID+3",
    "ID.4":     "ID+4",
    "ID.5":     "ID+5",
    "Sealion 7": "Sealion+7",
    "EV6":      "EV+6",
    "EV3":      "EV+3",
    "iX3":      "iX3",
}

_MOTORS_SEARCH = "https://www.motors.co.uk/search/"


def build_motors_url(cfg: dict, page_num: int = 1) -> str:
    """
    Build a Motors.co.uk search URL.
    Parameters are standard query strings; pagination uses 'page'.
    """
    make  = cfg.get("make", "")
    model = cfg.get("model", "")
    # Motors uses + for spaces in model names (already URL-encoded)
    model_q = _MODEL_OVERRIDES.get(model, model.replace(" ", "+"))

    params = [
        ("make",       make),
        ("model",      model_q),
        ("fuel-type",  "electric"),
        ("price-to",   cfg.get("price_max", 25000)),
        ("year-from",  cfg.get("year_from", 2020)),
        ("mileage-to", cfg.get("mileage_max", 120000)),
        ("postcode",   cfg.get("postcode", "EH1 1YZ").replace(" ", "")),
        ("radius",     cfg.get("radius", 200)),
        ("sort",       "price-asc"),
    ]
    if page_num > 1:
        params.append(("page", page_num))

    # Don't urlencode the model value — it already contains + for spaces,
    # and urlencode would percent-encode those.  Build the base and append
    # model manually.
    base_params = [(k, v) for k, v in params if k != "model"]
    qs = urlencode(base_params)
    if model_q:
        qs += f"&model={model_q}"
    return _MOTORS_SEARCH + "?" + qs


class MotorsScraper:
    """
    Playwright-based Motors.co.uk scraper, structurally identical to
    CarGurusScraper so it slots into the same scrape_all / on_search_done
    pipeline in main.py.
    """

    def __init__(self, config: dict):
        self.config = config
        lim = config.get("limits", {})
        self.timeout        = lim.get("page_timeout_ms", 35000)
        self.scroll_pause   = lim.get("scroll_pause_ms", 1500) / 1000
        self.request_delay  = lim.get("request_delay_ms", 3500) / 1000
        self.search_delay   = lim.get("search_delay_ms", 10000) / 1000
        self.max_per_search = lim.get("max_listings_per_search", 20)
        self.search_timeout = lim.get("motors_search_timeout_s", 240)
        self._cookie_done   = False

    async def scrape_all(
        self,
        searches: list,
        on_search_done=None,
        known_ids: set | None = None,
    ) -> list:
        """Run all enabled searches sequentially, one browser instance."""
        all_listings: list[Listing] = []
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
            # Block trackers and fonts to reduce bandwidth/latency
            await context.route(
                re.compile(
                    r"\.(woff2?|ttf|otf)(\?|$)|"
                    r"(google-analytics|googletagmanager|hotjar|"
                    r"doubleclick|criteo|permutive|onetrust)"
                ),
                lambda r: r.abort(),
            )

            for i, search in enumerate(searches):
                if not search.get("enabled", True):
                    continue
                logger.info(
                    f"[Motors {i+1}/{len(searches)}] {search['name']}"
                )
                try:
                    results = await asyncio.wait_for(
                        self._scrape_search(context, search, known_ids),
                        timeout=self.search_timeout,
                    )
                    all_listings.extend(results)
                    logger.info(
                        f"  → {len(results)} Motors listings for {search['name']}"
                    )
                    if on_search_done and results:
                        on_search_done(results)
                except asyncio.TimeoutError:
                    logger.error(
                        f"Motors search '{search['name']}' timed out after "
                        f"{self.search_timeout}s — skipping"
                    )
                except Exception as exc:
                    logger.error(
                        f"Motors search '{search['name']}' failed: {exc}",
                        exc_info=True,
                    )
                if i < len(searches) - 1:
                    await asyncio.sleep(_jitter(self.search_delay))

            await browser.close()
        return all_listings

    async def _scrape_search(
        self,
        context: BrowserContext,
        search: dict,
        known_ids: set | None,
    ) -> list[Listing]:
        listings: list[Listing] = []
        at_cfg  = search["autotrader"]
        require = [k.lower() for k in search.get("require_keywords", [])]
        exclude = [k.lower() for k in search.get("exclude_keywords", [])]

        url  = build_motors_url(at_cfg, page_num=1)
        page = await context.new_page()
        try:
            cards = await self._get_listing_cards(page, url)
        except Exception as exc:
            logger.warning(f"  Motors page 1 failed: {exc}")
            await page.close()
            return listings
        finally:
            try:
                await page.close()
            except Exception:
                pass

        if not cards:
            logger.info("  Motors page 1: no results")
            return listings

        logger.info(f"  Motors page 1: {len(cards)} cards")
        for card in cards:
            if len(listings) >= self.max_per_search:
                break
            listing = self._card_to_listing(card, search["name"])
            if not listing:
                continue
            if known_ids and listing.listing_id in known_ids:
                continue
            combined = f"{listing.title} {listing.spec_summary}".lower()
            if require and not all(k in combined for k in require):
                continue
            if any(k in combined for k in exclude):
                continue
            listings.append(listing)

        return listings

    async def _get_listing_cards(self, page: Page, url: str) -> list:
        """Load a Motors.co.uk search page and extract card data via JS."""
        try:
            await page.goto(url, timeout=self.timeout, wait_until="networkidle")
        except PWTimeout:
            logger.warning("  Motors: networkidle timed out, retrying with domcontentloaded")
            await page.goto(url, timeout=self.timeout, wait_until="domcontentloaded")
            await asyncio.sleep(3)

        try:
            title = await asyncio.wait_for(page.title(), timeout=5)
            logger.debug(f"  Motors page title: {title!r}  url: {page.url[:100]}")
        except Exception:
            pass

        await asyncio.sleep(_jitter(1.5))

        # Dismiss cookie/consent banner
        if not self._cookie_done:
            for sel in [
                'button:has-text("Accept all")',
                'button:has-text("Accept All")',
                '#onetrust-accept-btn-handler',
                '[class*="accept-all"]',
                '[id*="accept"]',
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

        # Scroll to trigger lazy loading
        for _ in range(4):
            try:
                await asyncio.wait_for(page.keyboard.press("End"), timeout=5)
            except Exception:
                break
            await asyncio.sleep(_jitter(self.scroll_pause))

        # Motors.co.uk renders listing cards as <article> elements or
        # divs with data-vehicle / data-listing attributes.
        # The JS extractor tries multiple container patterns and falls back
        # to walking up from /car-{id}/ href anchors.
        _JS_EXTRACT = """
        () => {
            const results = [];

            // Try known container selectors first
            const CONTAINER_SELS = [
                'article[data-vehicle-id]',
                'article[data-listing-id]',
                'article[data-id]',
                '[class*="vehicle-card"]',
                '[class*="VehicleCard"]',
                '[class*="listing-card"]',
                '[class*="ListingCard"]',
                '[class*="car-card"]',
                '[class*="search-result"]',
                'article',
            ];
            let containers = [];
            for (const sel of CONTAINER_SELS) {
                const found = Array.from(document.querySelectorAll(sel))
                    .filter(el => el.querySelector('a[href*="/car-"]'));
                if (found.length > 0) { containers = found; break; }
            }

            // Fallback: group by /car-{id}/ link proximity
            if (containers.length === 0) {
                const carLinks = document.querySelectorAll('a[href*="/car-"]');
                const seen = new Set();
                carLinks.forEach(a => {
                    let el = a.parentElement;
                    for (let i = 0; i < 8; i++) {
                        if (!el || el === document.body) break;
                        const inner = el.querySelectorAll('a[href*="/car-"]');
                        if (inner.length === 1 && !seen.has(el)) {
                            seen.add(el);
                            containers.push(el);
                            break;
                        }
                        el = el.parentElement;
                    }
                });
            }

            containers.forEach(el => {
                const g = (...sels) => {
                    for (const s of sels) {
                        const e = el.querySelector(s);
                        if (e && e.innerText.trim()) return e.innerText.trim();
                    }
                    return '';
                };
                const title    = g('h2','h3','h4',
                                   '[class*="title"]','[class*="Title"]',
                                   '[class*="name"]');
                const price    = g('[class*="price"]','[class*="Price"]',
                                   '[data-testid*="price"]');
                const mileage  = g('[class*="mileage"]','[class*="Mileage"]',
                                   '[class*="odometer"]');
                const location = g('[class*="location"]','[class*="Location"]',
                                   '[class*="dealer"]','[class*="distance"]');
                const spec     = g('[class*="spec"]','[class*="Spec"]',
                                   '[class*="description"]');
                const linkEl   = el.querySelector('a[href*="/car-"]');
                const link     = linkEl ? linkEl.getAttribute('href') : '';
                const imgEl    = el.querySelector('img[src]:not([src=""])')
                               || el.querySelector('img[data-src]');
                const img      = imgEl
                    ? (imgEl.src || imgEl.dataset.src || '')
                    : '';
                if (title || link) {
                    results.push({title, price, mileage, location, spec, link, img});
                }
            });
            return results;
        }
        """
        cards: list = []
        try:
            cards = await asyncio.wait_for(page.evaluate(_JS_EXTRACT), timeout=15)
        except asyncio.TimeoutError:
            logger.warning("  Motors: JS card extraction timed out")
        except Exception as exc:
            logger.debug(f"  Motors card extraction error: {exc}")

        if not cards:
            try:
                counts = await asyncio.wait_for(
                    page.evaluate("""
                    () => ({
                        articles:  document.querySelectorAll('article').length,
                        car_links: document.querySelectorAll('a[href*="/car-"]').length,
                        body_len:  document.body ? document.body.innerText.length : 0,
                    })
                    """),
                    timeout=5,
                )
                logger.info(
                    f"  Motors selector counts: articles={counts.get('articles')}, "
                    f"car_links={counts.get('car_links')}, "
                    f"body_chars={counts.get('body_len')}"
                )
            except Exception:
                pass

        return cards

    def _card_to_listing(
        self, card: dict, search_name: str
    ) -> Optional[Listing]:
        title    = (card.get("title") or "").strip()
        price_s  = card.get("price") or ""
        mileage_s = card.get("mileage") or ""
        location = (card.get("location") or "").strip()
        spec     = (card.get("spec") or "").strip()
        link     = (card.get("link") or "").strip()
        img      = (card.get("img") or "").strip()

        if not title and not link:
            return None

        if link and not link.startswith("http"):
            link = "https://www.motors.co.uk" + link

        # Listing ID from URL: /car-123456789/
        listing_id: str
        m = re.search(r"/car-(\d+)/", link)
        if m:
            listing_id = "mt_" + m.group(1)
        elif link:
            listing_id = "mt_" + re.sub(r"[^\w]", "", link)[-20:]
        else:
            listing_id = "mt_" + re.sub(r"\s+", "_", title[:30])

        # Price
        price: Optional[int] = None
        digits = re.sub(r"[^\d]", "", price_s)
        if digits and int(digits) > 500:
            price = int(digits)

        # Mileage
        mileage: Optional[int] = None
        m2 = re.search(r"([\d,]+)", mileage_s)
        if m2:
            candidate = int(m2.group(1).replace(",", ""))
            if 100 < candidate < 300000:
                mileage = candidate

        # Year from title
        year: Optional[int] = None
        y = re.search(r"\b(20[12]\d)\b", title)
        if y:
            year = int(y.group(1))

        flags: list[str] = []
        if mileage and mileage > 80000:
            flags.append(f"⚠️ High mileage ({mileage:,}mi)")
        if mileage and mileage < 25000:
            flags.append("✅ Low mileage")

        return Listing(
            listing_id=listing_id,
            title=title or "Motors listing",
            price=price,
            year=year,
            mileage=mileage,
            location=location,
            distance_miles=None,
            seller_type="dealer",
            seller_name=location,
            spec_summary=spec,
            url=link or "https://www.motors.co.uk",
            source="motors",
            image_urls=[img] if img and img.startswith("http") else [],
            attention_check=" | ".join(flags),
            search_name=search_name,
            scraped_at=datetime.utcnow().isoformat(),
        )
