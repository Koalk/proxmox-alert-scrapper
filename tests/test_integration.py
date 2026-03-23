"""
tests/test_integration.py
Live smoke test — hits AutoTrader UK with a single minimal search,
fetches exactly one listing, and asserts the scraper can parse its details.

Requirements:
  - Internet access
  - Playwright Chromium installed  (playwright install chromium)

Run with:
  pytest tests/test_integration.py --run-integration -v -s

The -s flag lets you see live log output so you can watch what the browser
is doing. Integration tests are skipped by default — see conftest.py.
"""

import asyncio
import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from scraper.autotrader import AutoTraderScraper, build_autotrader_url


# ---------------------------------------------------------------------------
# Minimal config that hits AutoTrader for 1 listing only
# ---------------------------------------------------------------------------

MINIMAL_CONFIG = {
    "limits": {
        "page_timeout_ms":        30000,
        "scroll_pause_ms":        1000,
        "request_delay_ms":       1000,
        "search_delay_ms":        1000,
        "max_listings_per_search": 1,    # fetch exactly 1
        "max_images_per_listing":  1,
    }
}

MINIMAL_SEARCH = {
    "name": "integration-test",
    "enabled": True,
    "autotrader": {
        "make":        "Kia",
        "model":       "EV6",
        "postcode":    "EH1 1YZ",
        "radius":      200,
        "price_max":   30000,
        "fuel_type":   "Electric",
        "year_from":   2020,
        "mileage_max": 130000,
    },
    "require_keywords": [],
    "exclude_keywords": [],
}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.integration
def test_autotrader_returns_at_least_one_listing():
    """
    Full end-to-end: open AutoTrader, collect URLs from page 1,
    fetch the first listing's detail page, assert we got back a
    populated Listing object.
    """
    scraper  = AutoTraderScraper(MINIMAL_CONFIG)
    listings = asyncio.run(scraper.scrape_all([MINIMAL_SEARCH]))

    assert len(listings) >= 1, (
        "Expected at least one listing from AutoTrader — "
        "check your internet connection or whether AutoTrader changed their HTML."
    )

    car = listings[0]

    # Title must be a non-empty string
    assert isinstance(car.title, str) and len(car.title) > 5, (
        f"Title looks wrong: {car.title!r}"
    )

    # Price must be a plausible integer (£500 – £200k)
    assert car.price is None or (500 <= car.price <= 200_000), (
        f"Price out of expected range: {car.price}"
    )

    # URL must point back to AutoTrader
    assert "autotrader.co.uk/car-details/" in car.url, (
        f"URL doesn't look like an AutoTrader listing: {car.url}"
    )

    # listing_id must be numeric (AutoTrader IDs are all digits)
    assert car.listing_id.isdigit(), (
        f"listing_id should be numeric: {car.listing_id!r}"
    )

    # Source tag
    assert car.source == "autotrader"

    print(f"\n[integration] Fetched: {car.title} | "
          f"£{car.price:,}" if car.price else f"£? | {car.url}")
