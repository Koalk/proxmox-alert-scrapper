"""
tests/test_integration_cargurus.py
Temporary integration test for CarGurus scraper debugging.

Requires a real network connection and Playwright browsers to be installed.

Run manually (target the file directly):
    pytest tests/test_integration_cargurus.py -v -s
"""

import asyncio
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from scraper.cargurus import CarGurusScraper, build_cargurus_url

_DUMP_DIR = Path("/tmp/cargurus_debug")


def _p(*args):
    """print() wrapper that flushes immediately â€” visible even under pytest -s."""
    print(*args, flush=True)


# ---------------------------------------------------------------------------
# Minimal config for a single known-good search
# ---------------------------------------------------------------------------

_ONE_SEARCH = [
    {
        "name": "Kia EV6 (integration test)",
        "enabled": True,
        "autotrader": {
            "make":       "Kia",
            "model":      "EV6",
            "postcode":   "EH1 1YZ",
            "radius":     200,
            "price_max":  30000,
            "year_from":  2021,
            "mileage_max": 150000,
            "fuel_type":  "Electric",
        },
    }
]

_CONFIG = {
    "limits": {
        "page_timeout_ms":           45000,
        "scroll_pause_ms":           2000,
        "request_delay_ms":          1000,
        "search_delay_ms":           2000,
        "max_listings_per_search":   5,
        "cargurus_search_timeout_s": 120,
    }
}


def _run(coro):
    return asyncio.run(coro)


class TestCarGurusIntegration:

    # --- URL builder sanity ---

    def test_url_builder_produces_valid_url(self):
        cfg = _ONE_SEARCH[0]["autotrader"]
        url = build_cargurus_url(cfg, page_num=1)
        _p(f"\nBuilt URL: {url}")
        assert "cargurus.co.uk" in url
        assert "viewDetailsFilterViewInventoryListing.action" in url
        assert "maxPrice=30000" in url
        assert "makeModelTrimPaths" in url
        assert "m306" in url          # Kia make ID
        assert "d6251" in url         # EV6 model ID
        assert "#listing?" not in url, "Old hash-fragment URL format should not be used"

    # --- Live page diagnostics ---

    def test_page_loads_and_logs_title(self):
        """Load the page, dump title/selectors/body-text and save screenshot + HTML."""
        async def _inner():
            from playwright.async_api import async_playwright
            from scraper.autotrader import _STEALTH_JS

            cfg = _ONE_SEARCH[0]["autotrader"]
            url = build_cargurus_url(cfg)
            _p(f"\nLoading: {url}")

            async with async_playwright() as p:
                browser = await p.chromium.launch(
                    headless=True,
                    args=["--no-sandbox", "--disable-dev-shm-usage"],
                )
                context = await browser.new_context(
                    viewport={"width": 1440, "height": 900},
                    locale="en-GB",
                    timezone_id="Europe/London",
                )
                await context.add_init_script(_STEALTH_JS)
                page = await context.new_page()

                try:
                    await page.goto(url, timeout=45000, wait_until="networkidle")
                    _p("  wait_until=networkidle: OK")
                except Exception as exc:
                    _p(f"  networkidle timed out ({exc}), retrying with domcontentloaded")
                    await page.goto(url, timeout=45000, wait_until="domcontentloaded")
                    await asyncio.sleep(4)

                title     = await page.title()
                final_url = page.url
                _p(f"  Page title : {title!r}")
                _p(f"  Final URL  : {final_url}")

                if any(w in title.lower() for w in ("captcha", "robot", "verify", "challenge", "access denied")):
                    _p("  âš ï¸  ANTI-BOT PAGE DETECTED")

                counts = await page.evaluate("""
                () => ({
                    blade:        document.querySelectorAll('[data-cg-ft="car-blade"]').length,
                    carcard:      document.querySelectorAll('[class*="CarCard"]').length,
                    row:          document.querySelectorAll('[class*="listing-row"]').length,
                    usedcars_a:   document.querySelectorAll('a[href*="/usedcars/"]').length,
                    listing_a:    document.querySelectorAll('a[href*="listing"]').length,
                    all_classes:  Array.from(new Set(
                                    Array.from(document.querySelectorAll('[class]'))
                                         .flatMap(e => Array.from(e.classList))
                                         .filter(c => c.match(/car|listing|result|blade|card/i))
                                  )).slice(0, 30),
                    h4_count:     document.querySelectorAll('h4').length,
                    body_chars:   document.body ? document.body.innerText.length : 0,
                })
                """)
                _p(f"  Counts    : blade={counts['blade']}, carcard={counts['carcard']}, "
                   f"row={counts['row']}, usedcars_links={counts['usedcars_a']}, "
                   f"listing_links={counts['listing_a']}, h4={counts['h4_count']}, "
                   f"body_chars={counts['body_chars']}")
                _p(f"  Relevant classes: {counts['all_classes']}")

                body_text = await page.evaluate(
                    "() => document.body ? document.body.innerText.slice(0, 3000) : ''"
                )
                _p(f"\n--- Page body (first 3000 chars) ---\n{body_text}\n--- end ---")

                # Save screenshot + HTML for offline inspection
                _DUMP_DIR.mkdir(parents=True, exist_ok=True)
                await page.screenshot(path=str(_DUMP_DIR / "page.png"), full_page=False)
                html = await page.content()
                (_DUMP_DIR / "page.html").write_text(html, encoding="utf-8")
                _p(f"\n  Screenshot saved â†’ {_DUMP_DIR}/page.png")
                _p(f"  HTML saved      â†’ {_DUMP_DIR}/page.html")

                await browser.close()
                return title, counts

        title, counts = _run(_inner())
        assert title, "Page title was empty"

    def test_scraper_returns_without_hanging(self):
        """Run the full scraper for one search â€” must complete, result can be empty."""
        async def _inner():
            scraper = CarGurusScraper(_CONFIG)
            results = await scraper.scrape_all(_ONE_SEARCH)
            return results

        results = _run(_inner())
        _p(f"\n  Scraper returned {len(results)} listing(s)")
        for r in results:
            _p(f"  â€¢ {r.title[:60]} | "
               f"{'Â£'+str(r.price) if r.price else 'Â£?'} | "
               f"{r.mileage or '?'}mi | {r.url[:80]}")
        assert isinstance(results, list)

    def test_card_data_shape_when_results_present(self):
        """Validate listing field shapes â€” skips if site returned nothing."""
        async def _inner():
            scraper = CarGurusScraper(_CONFIG)
            return await scraper.scrape_all(_ONE_SEARCH)

        results = _run(_inner())
        if not results:
            pytest.skip(
                "No results returned â€” check selector diagnostic output from "
                "test_page_loads_and_logs_title and /tmp/cargurus_debug/"
            )
        for listing in results:
            _p(f"  title={listing.title!r}, price={listing.price}, "
               f"url={listing.url[:60]}, source={listing.source}")
            assert listing.source == "cargurus"
            assert listing.listing_id.startswith("cg_")
            assert listing.url.startswith("http")

