"""
tests/test_unit.py
Fast unit tests — no network, no browser, no config file needed.
Run with:  pytest tests/test_unit.py -v
"""

import sys
import tempfile
from pathlib import Path

# Make sure the repo root is on sys.path when running from any directory
sys.path.insert(0, str(Path(__file__).parent.parent))

from scraper.autotrader import build_autotrader_url, Listing
from scraper.database import ListingDatabase
from main import apply_defaults


# ---------------------------------------------------------------------------
# apply_defaults
# ---------------------------------------------------------------------------

class TestApplyDefaults:
    BASE_DEFAULTS = {
        "postcode":  "EH1 1YZ",
        "radius":    200,
        "fuel_type": "Electric",
        "year_from": 2021,
        "mileage_max": 110000,
    }

    def _search(self, overrides=None):
        at = {"make": "Kia", "model": "EV6", "price_max": 22000}
        at.update(overrides or {})
        return {"name": "Test", "enabled": True, "autotrader": at}

    def test_defaults_are_merged(self):
        result = apply_defaults([self._search()], self.BASE_DEFAULTS)
        at = result[0]["autotrader"]
        assert at["postcode"]    == "EH1 1YZ"
        assert at["radius"]      == 200
        assert at["fuel_type"]   == "Electric"
        assert at["year_from"]   == 2021
        assert at["mileage_max"] == 110000

    def test_per_search_value_wins_over_default(self):
        result = apply_defaults(
            [self._search({"radius": 300, "postcode": "G1 1AA"})],
            self.BASE_DEFAULTS,
        )
        at = result[0]["autotrader"]
        assert at["radius"]   == 300       # override kept
        assert at["postcode"] == "G1 1AA"  # override kept

    def test_explicit_search_fields_not_clobbered(self):
        result = apply_defaults(
            [self._search({"make": "Hyundai", "model": "Ioniq 5"})],
            self.BASE_DEFAULTS,
        )
        at = result[0]["autotrader"]
        assert at["make"]  == "Hyundai"
        assert at["model"] == "Ioniq 5"

    def test_empty_defaults_leaves_search_unchanged(self):
        search = self._search()
        result = apply_defaults([search], {})
        assert result[0]["autotrader"] == search["autotrader"]

    def test_multiple_searches_each_get_defaults(self):
        searches = [self._search(), self._search({"radius": 50})]
        result = apply_defaults(searches, self.BASE_DEFAULTS)
        assert result[0]["autotrader"]["radius"] == 200
        assert result[1]["autotrader"]["radius"] == 50

    def test_original_search_dict_not_mutated(self):
        search = self._search()
        original_at = dict(search["autotrader"])
        apply_defaults([search], self.BASE_DEFAULTS)
        assert search["autotrader"] == original_at


# ---------------------------------------------------------------------------
# build_autotrader_url
# ---------------------------------------------------------------------------

class TestBuildAutotraderUrl:
    BASE_CFG = {
        "make":        "Kia",
        "model":       "EV6",
        "postcode":    "EH1 1YZ",
        "radius":      100,
        "price_max":   25000,
        "fuel_type":   "Electric",
        "year_from":   2021,
        "mileage_max": 110000,
    }

    def test_url_starts_with_autotrader_domain(self):
        url = build_autotrader_url(self.BASE_CFG)
        assert url.startswith("https://www.autotrader.co.uk/car-search?")

    def test_key_params_present(self):
        url = build_autotrader_url(self.BASE_CFG)
        assert "make=Kia"            in url
        assert "model=EV6"           in url
        assert "fuel-type=Electric"  in url
        assert "radius=100"          in url
        assert "price-to=25000"      in url
        assert "year-from=2021"      in url
        assert "maximum-mileage=110000" in url

    def test_page_number_included(self):
        url1 = build_autotrader_url(self.BASE_CFG, page_num=1)
        url2 = build_autotrader_url(self.BASE_CFG, page_num=3)
        assert "page=1" in url1
        assert "page=3" in url2

    def test_defaults_used_when_keys_missing(self):
        url = build_autotrader_url({})
        # Should not raise and should still be a valid URL
        assert "autotrader.co.uk" in url

    def test_postcode_spaces_encoded(self):
        url = build_autotrader_url(self.BASE_CFG)
        # urlencode should encode the space in "EH1 1YZ"
        assert "EH1" in url
        assert " " not in url.split("?")[1]


# ---------------------------------------------------------------------------
# Listing.to_dict
# ---------------------------------------------------------------------------

class TestListingToDict:
    def _make_listing(self, **kwargs):
        defaults = dict(
            listing_id="123", title="Kia EV6 2022", price=18000, year=2022,
            mileage=30000, location="Edinburgh", distance_miles=5,
            seller_type="dealer", seller_name="EV Cars Ltd",
            spec_summary="Auto | Electric | 4dr", url="https://example.com",
        )
        defaults.update(kwargs)
        return Listing(**defaults)

    def test_to_dict_contains_expected_keys(self):
        d = self._make_listing().to_dict()
        for key in ("listing_id", "title", "price", "year", "mileage",
                    "location", "url", "source"):
            assert key in d

    def test_none_price_serialises(self):
        d = self._make_listing(price=None).to_dict()
        assert d["price"] is None

    def test_source_defaults_to_autotrader(self):
        d = self._make_listing().to_dict()
        assert d["source"] == "autotrader"


# ---------------------------------------------------------------------------
# ListingDatabase — email_sent lifecycle
# ---------------------------------------------------------------------------

class TestListingDatabase:
    def _db(self):
        tmp = tempfile.mkdtemp()
        return ListingDatabase(f"{tmp}/test.db")

    def _listing(self, listing_id="L1", price=18000, **kwargs):
        return Listing(
            listing_id=listing_id, title="Kia EV6 2022", price=price,
            year=2022, mileage=30000, location="Edinburgh", distance_miles=5,
            seller_type="dealer", seller_name="EV Cars Ltd",
            spec_summary="Auto | Electric", url="https://example.com",
            search_name="Test Search",
        )

    def test_new_listing_has_email_sent_false(self):
        db = self._db()
        db.process_listings([self._listing()])
        unsent = db.get_unsent_listings()
        assert len(unsent) == 1
        assert unsent[0]["email_sent"] == 0

    def test_get_unsent_returns_full_data(self):
        db = self._db()
        db.process_listings([self._listing()])
        unsent = db.get_unsent_listings()
        assert unsent[0]["title"] == "Kia EV6 2022"
        assert unsent[0]["price"] == 18000
        assert isinstance(unsent[0]["image_urls"], list)

    def test_mark_as_sent_sets_flag(self):
        db = self._db()
        db.process_listings([self._listing()])
        db.mark_as_sent(["L1"])
        assert db.get_unsent_listings() == []

    def test_mark_as_sent_strips_rich_data(self):
        db = self._db()
        db.process_listings([self._listing()])
        db.mark_as_sent(["L1"])
        # Record still exists (needed for dedup) but display fields are gone
        active = db.get_all_active()
        assert len(active) == 0   # get_all_active filters on title IS NOT NULL

    def test_sent_listing_still_deduplicates(self):
        db = self._db()
        listing = self._listing()
        new1, _ = db.process_listings([listing])
        assert len(new1) == 1
        db.mark_as_sent(["L1"])
        # Processing the same listing again should NOT produce a new entry
        new2, _ = db.process_listings([listing])
        assert len(new2) == 0

    def test_price_change_resets_email_sent(self):
        db = self._db()
        db.process_listings([self._listing(price=18000)])
        db.mark_as_sent(["L1"])
        # New run, same car, different price
        _, updated = db.process_listings([self._listing(price=17000)])
        assert len(updated) == 1
        unsent = db.get_unsent_listings()
        assert len(unsent) == 1   # price change re-queues the listing for email

    def test_no_price_change_stays_sent(self):
        db = self._db()
        db.process_listings([self._listing(price=18000)])
        db.mark_as_sent(["L1"])
        # Same price — should stay marked as sent
        _, updated = db.process_listings([self._listing(price=18000)])
        assert len(updated) == 0
        assert db.get_unsent_listings() == []

    def test_mark_as_sent_empty_list_is_safe(self):
        db = self._db()
        db.mark_as_sent([])   # should not raise

    def test_migration_adds_column_to_existing_db(self):
        """A db created without email_sent column should be migrated on init."""
        import sqlite3, tempfile
        tmp = tempfile.mkdtemp()
        db_path = f"{tmp}/legacy.db"
        # Create a bare-bones db without the email_sent column
        with sqlite3.connect(db_path) as conn:
            conn.execute("""
                CREATE TABLE listings (
                    listing_id TEXT PRIMARY KEY,
                    url TEXT,
                    price INTEGER,
                    first_seen TEXT,
                    last_seen TEXT,
                    times_seen INTEGER DEFAULT 1,
                    is_new INTEGER DEFAULT 1
                )
            """)
        # Opening through ListingDatabase should not raise and should add the column
        db = ListingDatabase(db_path)
        cols = {row[1] for row in db._connect().execute("PRAGMA table_info(listings)")}
        assert "email_sent" in cols
