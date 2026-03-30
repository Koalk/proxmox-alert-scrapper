"""
scraper/cargurus.py — CarGurus UK Playwright scraper

ARCHITECTURE:
  CarGurusScraper.scrape_all(searches, on_search_done)
    → _scrape_search()       builds CG URL, paginates while < max_per_search
      → _get_listing_cards()   extracts cards via JS (no detail pages)
      → _card_to_listing()     parses card dict into a Listing
  Imports Listing, _jitter, _STEALTH_JS from autotrader.py.

KEY GOTCHAS:
  - URL structure is completely different: viewDetailsFilterViewInventoryListing
    .action with numeric make/model IDs from _MAKE_MODELS dict.
  - Does NOT accept known_ids (no pagination skip logic) — always scrapes page 1+.
  - Deal rating (Great/Good/Fair/Overpriced) is extracted from card text and
    appended to attention_check flags.
  - Listing IDs prefixed with 'cg_'.
  - Config block is still called 'autotrader' (at_cfg) — same config key reused.

CONFIG KEYS: same search.autotrader block as autotrader.py.
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

# CarGurus numeric IDs for makes and specific EV models.
# (config_make, config_model) -> (make_id, model_id)
# model_id=None means make-only search filtered to electric via fuelTypes param.
_MAKE_MODELS: dict[tuple, tuple] = {
    ("Kia",        "EV6"):      ("m306", "d6251"),
    ("Hyundai",    "Ioniq 5"):  ("m279", "d6229"),
    ("Skoda",      "Enyaq"):    ("m207", None),
    ("Volkswagen", "ID.3"):     ("m203", "d6006"),
    ("Volkswagen", "ID.4"):     ("m203", "d6206"),
    ("Volkswagen", "ID.5"):     ("m203", None),
    ("Tesla",      "Model 3"):  ("m266", "d5176"),
    ("Tesla",      "Model Y"):  ("m266", "d6299"),
    ("BMW",        "iX3"):      ("m256", "d6213"),
    ("BMW",        None):       ("m256", None),
    ("BYD",        None):       ("m451", None),
    ("Mercedes",   None):       ("m297", None),
    ("Citroen",    None):       ("m263", None),
    ("Peugeot",    None):       ("m318", None),
}

_CG_BASE = (
    "https://www.cargurus.co.uk/Cars/inventorylisting/"
    "viewDetailsFilterViewInventoryListing.action"
)


def _get_make_model_ids(make: str, model: str) -> tuple:
    """Return (make_id, model_id) for the given make/model config names."""
    ids = _MAKE_MODELS.get((make, model))
    if ids:
        return ids
    # Wildcard: make-only entry (model=None)
    ids = _MAKE_MODELS.get((make, None))
    if ids:
        return ids[0], None
    return None, None


def build_cargurus_url(cfg: dict, page_num: int = 1) -> str:
    """
    Build a CarGurus UK inventory search URL using the current
    viewDetailsFilterViewInventoryListing.action endpoint.
    makeModelTrimPaths must appear twice when a model ID is known:
    once as make/model path and once as make-only (CarGurus requirement).
    """
    make = cfg.get("make", "")
    model = cfg.get("model", "")
    make_id, model_id = _get_make_model_ids(make, model)
    if not make_id:
        logger.warning(
            f"CarGurus: no ID mapping for make={make!r} — search will be skipped"
        )
        return ""

    params: list[tuple] = [
        ("zip",               cfg.get("postcode", "EH1 1YZ").replace(" ", "")),
        ("distance",          cfg.get("radius", 200)),
        ("maxPrice",          cfg.get("price_max", 25000)),
        ("maxMileage",        cfg.get("mileage_max", 120000)),
        ("startYear",         cfg.get("year_from", 2020)),
        ("sortDir",           "ASC"),
        ("sortType",          "PRICE"),
        ("srpVariation",      "DEFAULT_SEARCH"),
        ("isDeliveryEnabled", "true"),
        ("offset",            (page_num - 1) * 15),
    ]
    if model_id:
        # makeModelTrimPaths must appear twice for model-scoped searches
        params.append(("makeModelTrimPaths", f"{make_id}/{model_id}"))
        params.append(("makeModelTrimPaths", make_id))
        params.append(("entitySelectingHelper.selectedEntity", model_id))
    else:
        params.append(("makeModelTrimPaths", make_id))
        params.append(("fuelTypes", "ELECTRIC"))

    return _CG_BASE + "?" + urlencode(params)


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
                        f"  → {len(results)} listings for {search['name']}"
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
        at_cfg         = search["autotrader"]   # reuse same config block
        require        = [k.lower() for k in search.get("require_keywords", [])]
        exclude        = [k.lower() for k in search.get("exclude_keywords", [])]
        expected_make  = at_cfg.get("make", "").lower()
        expected_model = at_cfg.get("model", "").lower()
        page_num = 1

        while len(listings) < self.max_per_search:
            url  = build_cargurus_url(at_cfg, page_num)
            page = await context.new_page()
            try:
                cards = await self._get_listing_cards(page, url)
            except Exception as exc:
                logger.warning(f"  Page {page_num} failed: {exc}")
                await page.close()
                break
            finally:
                try:
                    await page.close()
                except Exception:
                    pass

            if not cards:
                logger.info(f"  Page {page_num}: no results")
                break

            logger.info(f"  Page {page_num}: {len(cards)} cards")

            for card_data in cards:
                if len(listings) >= self.max_per_search:
                    break
                # Cards already contain most data — no need for detail page
                listing = self._card_to_listing(card_data, search["name"])
                if listing:
                    combined = f"{listing.title} {listing.spec_summary}".lower()
                    # CarGurus often returns a broader set than requested (make-level
                    # results when no model_id exists).  Reject cards that don't
                    # match the expected model (or make when model is absent) —
                    # mirrors the same guard already present in motors.py.
                    if expected_model:
                        if expected_model not in combined:
                            logger.debug(f"  Skipping (wrong model): '{listing.title[:50]}'")
                            continue
                    elif expected_make:
                        if expected_make not in combined:
                            logger.debug(f"  Skipping (wrong make): '{listing.title[:50]}'")
                            continue
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
        # Use networkidle so the React SPA has time to fetch and render
        # listings driven by hash-fragment params.  Fall back to a plain
        # load if networkidle times out (can happen on slow connections).
        try:
            await page.goto(url, timeout=self.timeout, wait_until="networkidle")
        except PWTimeout:
            logger.warning("  CarGurus: networkidle timed out, retrying with domcontentloaded")
            await page.goto(url, timeout=self.timeout, wait_until="domcontentloaded")
            await asyncio.sleep(3)   # give the SPA a moment to render

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
        _JS_EXTRACT = """
        () => {
            const results = [];

            // Try primary container selectors for different CarGurus page versions
            const containerSelectors = [
                '[data-cg-ft="car-blade"]',
                '[class*="CarCard"]',
                '[class*="car-blade"]',
                '[class*="listingCard"]',
                '[class*="listing-row"]',
                '[class*="car-listing"]',
            ];
            let containers = [];
            for (const sel of containerSelectors) {
                const found = Array.from(document.querySelectorAll(sel));
                if (found.length > 0) { containers = found; break; }
            }

            // Fallback: walk up from VDP links to find per-card containers
            if (containers.length === 0) {
                const vdpLinks = document.querySelectorAll(
                    'a[href*="listingId="], a[href*="/usedcars/"]'
                );
                const seen = new Set();
                vdpLinks.forEach(a => {
                    let el = a.parentElement;
                    for (let i = 0; i < 8; i++) {
                        if (!el || el === document.body) break;
                        const inner = el.querySelectorAll(
                            'a[href*="listingId="], a[href*="/usedcars/"]'
                        );
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
                const title    = g('h4','h3','h2',
                                   '[class*="title"]','[class*="Title"]');
                const price    = g('[class*="price"]','[class*="Price"]',
                                   '[data-cg-ft="price"]');
                const mileage  = g('[class*="mileage"]','[class*="Mileage"]',
                                   '[class*="miles"]');
                const location = g('[class*="dealer-name"]','[class*="location"]',
                                   '[class*="Location"]','[class*="dealer"]');
                const deal     = g('[class*="deal-rating"]','[class*="DealRating"]',
                                   '[class*="priceAnalysis"]','[class*="dealRating"]');
                const linkEl   = el.querySelector('a[href*="listingId="]')
                               || el.querySelector('a[href*="/usedcars/"]')
                               || el.querySelector('a[href]');
                const link     = linkEl ? linkEl.getAttribute('href') : '';
                const imgEl    = el.querySelector('img[src], img[data-src]');
                const img      = imgEl ? (imgEl.src || imgEl.dataset.src || '') : '';
                if (title || link) {
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
            # Diagnostic: log how many matching elements Playwright can see
            # so we know whether the selectors are wrong vs the page is empty.
            try:
                counts = await asyncio.wait_for(
                    page.evaluate("""
                    () => ({
                        blade:     document.querySelectorAll('[data-cg-ft="car-blade"]').length,
                        carcard:   document.querySelectorAll('[class*="CarCard"]').length,
                        row:       document.querySelectorAll('[class*="listing-row"]').length,
                        vdp_links: document.querySelectorAll('a[href*="listingId="]').length,
                        car_links: document.querySelectorAll('a[href*="/usedcars/"]').length,
                        body_len:  document.body ? document.body.innerText.length : 0,
                    })
                    """),
                    timeout=5,
                )
                logger.info(
                    f"  CarGurus selector counts: "
                    f"blade={counts.get('blade')}, carcard={counts.get('carcard')}, "
                    f"row={counts.get('row')}, vdp_links={counts.get('vdp_links')}, "
                    f"car_links={counts.get('car_links')}, body_chars={counts.get('body_len')}"
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

        # Parse location: CarGurus returns "City\nX mi away" in a single text node.
        # Split so location shows just the city and distance_miles is a number.
        location_parts = location.split("\n", 1)
        city = location_parts[0].strip()
        distance_miles: Optional[int] = None
        if len(location_parts) > 1:
            dm = re.search(r"(\d+)\s*mi", location_parts[1])
            if dm:
                distance_miles = int(dm.group(1))

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

        # Stable ID: prefer numeric listingId from VDP URL
        listing_id: str
        if link:
            m = re.search(r"listingId=(\d+)", link)
            listing_id = "cg_" + (m.group(1) if m else re.sub(r"[^\w]", "", link)[-20:])
        else:
            listing_id = "cg_" + re.sub(r"\s+", "_", title[:30])

        return Listing(
            listing_id=listing_id,
            title=title or "CarGurus listing",
            price=price,
            year=year,
            mileage=mileage,
            location=city,
            distance_miles=distance_miles,
            seller_type="dealer",
            seller_name=city,
            spec_summary="",
            url=link or "https://www.cargurus.co.uk",
            source="cargurus",
            image_urls=[img] if img and len(img) > 20 else [],
            attention_check=" | ".join(flags),
            search_name=search_name,
            scraped_at=datetime.utcnow().isoformat(),
        )