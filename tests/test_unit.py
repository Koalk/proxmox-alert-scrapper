"""
tests/test_unit.py
Fast unit tests — no network, no browser, no config file needed.
Run with:  pytest tests/test_unit.py -v
"""

import sys
import tempfile
from datetime import datetime, timezone, timedelta
from pathlib import Path
from unittest.mock import patch, MagicMock

# Make sure the repo root is on sys.path when running from any directory
sys.path.insert(0, str(Path(__file__).parent.parent))

from scraper.autotrader import AutoTraderScraper, build_autotrader_url, Listing
from scraper.cargurus  import build_cargurus_url, _get_make_model_ids, CarGurusScraper
from scraper.motors    import build_motors_url, MotorsScraper
from scraper.database  import ListingDatabase
from scraper.emailer   import build_html_email, _car_card
from main import apply_defaults, check_for_update, dedup_across_sources


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

    def _listing(self, listing_id="L1", price=18000, search_name="Test Search", **kwargs):
        return Listing(
            listing_id=listing_id, title="Kia EV6 2022", price=price,
            year=2022, mileage=30000, location="Edinburgh", distance_miles=5,
            seller_type="dealer", seller_name="EV Cars Ltd",
            spec_summary="Auto | Electric", url="https://example.com",
            search_name=search_name,
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

    def test_resume_detects_recent_unsent_search(self):
        """Searches with unsent data scraped recently are returned."""
        db = self._db()
        db.process_listings([self._listing(listing_id="L1", search_name="Kia EV6")])
        db.process_listings([self._listing(listing_id="L2", search_name="BMW iX3")])
        recent = db.get_searches_with_recent_unsent(max_age_hours=1)
        assert "Kia EV6" in recent
        assert "BMW iX3" in recent

    def test_resume_ignores_sent_listings(self):
        """Searches whose listings are already sent are not returned."""
        db = self._db()
        db.process_listings([self._listing(listing_id="L1", search_name="Kia EV6")])
        db.mark_as_sent(["L1"])
        recent = db.get_searches_with_recent_unsent(max_age_hours=1)
        assert "Kia EV6" not in recent

    def test_resume_ignores_old_unsent_listings(self):
        """Unsent listings older than max_age_hours are not returned (yesterday's run)."""
        db = self._db()
        db.process_listings([self._listing(listing_id="L1", search_name="Kia EV6")])
        # Back-date last_seen to 25 hours ago to simulate a previous day's run
        old_ts = (
            datetime.now(timezone.utc) - timedelta(hours=25)
        ).isoformat()
        with db._connect() as conn:
            conn.execute(
                "UPDATE listings SET last_seen = ? WHERE listing_id = 'L1'",
                (old_ts,)
            )
        recent = db.get_searches_with_recent_unsent(max_age_hours=20)
        assert "Kia EV6" not in recent


# ---------------------------------------------------------------------------
# check_for_update
# ---------------------------------------------------------------------------

class TestCheckForUpdate:
    """Tests use a real temp git repo so no subprocess mocking is needed."""

    def _make_repo_with_remote(self):
        """
        Create two real bare git repos (origin + clone) so we can simulate
        being behind by committing to origin without pulling into the clone.
        Returns the clone path as a string.
        """
        import subprocess
        tmp = Path(tempfile.mkdtemp())

        origin = tmp / "origin.git"
        origin.mkdir()
        subprocess.run(["git", "init", "--bare", str(origin)], capture_output=True, check=True)

        clone = tmp / "clone"
        subprocess.run(["git", "clone", str(origin), str(clone)], capture_output=True, check=True)

        # Configure git identity for commits
        for cmd in [
            ["git", "-C", str(clone), "config", "user.email", "test@test.com"],
            ["git", "-C", str(clone), "config", "user.name", "Test"],
        ]:
            subprocess.run(cmd, capture_output=True, check=True)

        # Make an initial commit so the branch and upstream exist
        (clone / "README.md").write_text("initial")
        subprocess.run(["git", "-C", str(clone), "add", "."], capture_output=True, check=True)
        subprocess.run(["git", "-C", str(clone), "commit", "-m", "init"], capture_output=True, check=True)
        subprocess.run(["git", "-C", str(clone), "push"], capture_output=True, check=True)

        return str(clone), str(origin)

    def test_returns_none_when_no_git_dir(self):
        tmp = tempfile.mkdtemp()
        assert check_for_update(tmp) is None

    def test_returns_none_when_up_to_date(self):
        clone, _ = self._make_repo_with_remote()
        assert check_for_update(clone) is None

    def test_detects_new_remote_commits(self):
        import subprocess
        clone, origin = self._make_repo_with_remote()

        # Make a second clone to push new commits to origin without updating `clone`
        tmp2 = Path(tempfile.mkdtemp())
        pusher = tmp2 / "pusher"
        subprocess.run(["git", "clone", str(origin), str(pusher)], capture_output=True, check=True)
        for cmd in [
            ["git", "-C", str(pusher), "config", "user.email", "test@test.com"],
            ["git", "-C", str(pusher), "config", "user.name", "Test"],
        ]:
            subprocess.run(cmd, capture_output=True, check=True)
        (pusher / "new.txt").write_text("change")
        subprocess.run(["git", "-C", str(pusher), "add", "."], capture_output=True, check=True)
        subprocess.run(["git", "-C", str(pusher), "commit", "-m", "new commit"], capture_output=True, check=True)
        subprocess.run(["git", "-C", str(pusher), "push"], capture_output=True, check=True)

        result = check_for_update(clone)
        assert result is not None
        assert result["behind"] == 1
        assert len(result["local"]) == 8
        assert len(result["remote"]) == 8
        assert result["local"] != result["remote"]

    def test_returns_none_on_git_failure(self):
        # A path with a .git dir but git fetch fails (no network / bad remote)
        tmp = Path(tempfile.mkdtemp())
        (tmp / ".git").mkdir()
        # No valid git repo — git commands will fail, should return None gracefully
        result = check_for_update(str(tmp))
        assert result is None


# ---------------------------------------------------------------------------
# ListingDatabase — get_known_listing_ids
# ---------------------------------------------------------------------------

class TestGetKnownListingIds:
    def _db(self):
        tmp = tempfile.mkdtemp()
        return ListingDatabase(f"{tmp}/test.db")

    def _listing(self, listing_id, search_name="Test"):
        return Listing(
            listing_id=listing_id, title="Kia EV6 2022", price=18000,
            year=2022, mileage=30000, location="Edinburgh", distance_miles=5,
            seller_type="dealer", seller_name="EV Cars Ltd",
            spec_summary="Auto | Electric", url="https://example.com",
            search_name=search_name,
        )

    def test_empty_db_returns_empty_set(self):
        db = self._db()
        assert db.get_known_listing_ids() == set()

    def test_returns_all_stored_ids(self):
        db = self._db()
        db.process_listings([self._listing("A1"), self._listing("B2"), self._listing("C3")])
        ids = db.get_known_listing_ids()
        assert ids == {"A1", "B2", "C3"}

    def test_returns_ids_regardless_of_email_sent_status(self):
        """IDs of already-sent listings must still appear so pages can be skipped."""
        db = self._db()
        db.process_listings([self._listing("SENT1"), self._listing("UNSENT2")])
        db.mark_as_sent(["SENT1"])
        ids = db.get_known_listing_ids()
        assert "SENT1" in ids
        assert "UNSENT2" in ids

    def test_duplicate_process_does_not_duplicate_ids(self):
        """Processing the same listing twice should still yield one ID entry."""
        db = self._db()
        listing = self._listing("DUP1")
        db.process_listings([listing])
        db.process_listings([listing])
        ids = db.get_known_listing_ids()
        assert ids == {"DUP1"}

    def test_returns_set_not_list(self):
        db = self._db()
        db.process_listings([self._listing("X1")])
        assert isinstance(db.get_known_listing_ids(), set)


# ---------------------------------------------------------------------------
# emailer — max_email_listings cap
# ---------------------------------------------------------------------------

class TestEmailCap:
    """Tests for the max_email_listings cap in build_html_email."""

    def _listing_dict(self, listing_id, title="Kia EV6 2022", price=18000, is_new=1):
        return {
            "listing_id": listing_id,
            "title": title,
            "price": price,
            "year": 2022,
            "mileage": 30000,
            "location": "Edinburgh",
            "distance_miles": 5,
            "seller_type": "dealer",
            "seller_name": "EV Cars Ltd",
            "spec_summary": "Auto | Electric",
            "url": "https://example.com",
            "source": "autotrader",
            "image_urls": [],
            "attention_check": "",
            "search_name": "Test Search",
            "is_new": is_new,
        }

    def _build(self, new_count=0, updated_count=0, cap=20):
        new = [self._listing_dict(f"N{i}") for i in range(new_count)]
        updated = [self._listing_dict(f"U{i}", is_new=0) for i in range(updated_count)]
        return build_html_email(
            new_listings=new,
            updated_listings=updated,
            all_listings=new + updated,
            stats={"total_in_db": new_count + updated_count},
            run_date="Monday 24 March 2026, 02:00",
            max_email_listings=cap,
        )

    def test_under_cap_shows_all_listings(self):
        html = self._build(new_count=5, cap=20)
        # 5 listing titles should all appear
        for i in range(5):
            assert f"N{i}" in html or "Kia EV6" in html  # cards rendered

    def test_over_cap_shows_overflow_note(self):
        html = self._build(new_count=25, cap=20)
        assert "more listing" in html.lower()
        assert "latest_results.json" in html

    def test_exactly_at_cap_no_overflow_note(self):
        html = self._build(new_count=20, cap=20)
        assert "more listing" not in html.lower()

    def test_new_listings_take_priority_over_updated(self):
        """With cap=5 and 5 new + 3 updated, all 8 combined sorted by price, cap trims to 5."""
        html = self._build(new_count=5, updated_count=3, cap=5)
        # 8 total, only 5 shown → 3 omitted
        assert "3 more listing" in html
        # Both new and price-change badges may appear
        assert "Price Changes" not in html  # old section header gone

    def test_remaining_slots_fill_with_updated(self):
        """With cap=8 and 5 new + 5 updated, 8 shown, 2 omitted."""
        html = self._build(new_count=5, updated_count=5, cap=8)
        assert "2 more listing" in html

    def test_zero_new_zero_updated_no_overflow(self):
        html = self._build(new_count=0, updated_count=0, cap=20)
        assert "more listing" not in html.lower()

    def test_custom_cap_respected(self):
        html = self._build(new_count=10, cap=3)
        assert "7 more listing" in html


# ---------------------------------------------------------------------------
# Motors.co.uk URL builder
# ---------------------------------------------------------------------------

class TestBuildMotorsUrl:
    _CFG = {
        "make":       "Kia",
        "model":      "EV6",
        "postcode":   "EH11YZ",
        "radius":     100,
        "price_max":  25000,
        "mileage_max": 90000,
        "year_from":  2021,
    }

    def test_base_url(self):
        url = build_motors_url(self._CFG, page_num=1)
        assert url.startswith("https://www.motors.co.uk/search/?")

    def test_make_present(self):
        url = build_motors_url(self._CFG)
        assert "make=Kia" in url

    def test_model_override_applied(self):
        # EV6 maps to "EV+6" in _MODEL_OVERRIDES
        url = build_motors_url(self._CFG)
        assert "model=EV+6" in url

    def test_price_and_mileage(self):
        url = build_motors_url(self._CFG)
        assert "price-to=25000" in url
        assert "mileage-to=90000" in url

    def test_fuel_type_electric(self):
        url = build_motors_url(self._CFG)
        assert "fuel-type=electric" in url

    def test_sort_price_asc(self):
        url = build_motors_url(self._CFG)
        assert "sort=price-asc" in url

    def test_page_1_no_page_param(self):
        url = build_motors_url(self._CFG, page_num=1)
        assert "page=" not in url

    def test_page_2_has_page_param(self):
        url = build_motors_url(self._CFG, page_num=2)
        assert "page=2" in url

    def test_ioniq5_model_override(self):
        cfg = dict(self._CFG, make="Hyundai", model="Ioniq 5")
        url = build_motors_url(cfg)
        assert "model=Ioniq+5" in url

    def test_no_model_override_uses_plain_name(self):
        cfg = dict(self._CFG, model="Enyaq")
        url = build_motors_url(cfg)
        assert "model=Enyaq" in url


# ---------------------------------------------------------------------------
# CarGurus URL builder (new endpoint)
# ---------------------------------------------------------------------------

class TestBuildCarGurusUrl:
    _CFG = {
        "make":       "Kia",
        "model":      "EV6",
        "postcode":   "EH11YZ",
        "radius":     100,
        "price_max":  30000,
        "mileage_max": 90000,
        "year_from":  2021,
    }

    def test_new_endpoint(self):
        url = build_cargurus_url(self._CFG)
        assert "viewDetailsFilterViewInventoryListing.action" in url

    def test_no_hash_fragment(self):
        url = build_cargurus_url(self._CFG)
        assert "#listing?" not in url

    def test_make_model_ids_in_url(self):
        url = build_cargurus_url(self._CFG)
        assert "m306" in url    # Kia make ID
        assert "d6251" in url   # EV6 model ID

    def test_double_make_model_paths(self):
        # makeModelTrimPaths must appear twice — once with model, once make-only
        url = build_cargurus_url(self._CFG)
        assert url.count("makeModelTrimPaths") == 2

    def test_model_path_encoded(self):
        url = build_cargurus_url(self._CFG)
        assert "m306%2Fd6251" in url

    def test_entity_selector(self):
        url = build_cargurus_url(self._CFG)
        assert "entitySelectingHelper.selectedEntity=d6251" in url

    def test_max_price(self):
        url = build_cargurus_url(self._CFG)
        assert "maxPrice=30000" in url

    def test_make_only_fallback_uses_fueltype(self):
        # BYD has only a make-level entry (no per-model ID), so it uses fuelTypes filter
        cfg = dict(self._CFG, make="BYD", model="Atto 3")
        url = build_cargurus_url(cfg)
        assert "fuelTypes=ELECTRIC" in url
        assert "makeModelTrimPaths=m451" in url
        assert url.count("makeModelTrimPaths") == 1

    def test_unknown_make_returns_empty(self):
        cfg = dict(self._CFG, make="UnknownMake", model="Unknown")
        url = build_cargurus_url(cfg)
        assert url == ""

    def test_get_make_model_ids_known(self):
        make_id, model_id = _get_make_model_ids("Kia", "EV6")
        assert make_id == "m306"
        assert model_id == "d6251"

    def test_get_make_model_ids_with_model(self):
        # BMW iX3 has a dedicated model ID in the mapping
        make_id, model_id = _get_make_model_ids("BMW", "iX3")
        assert make_id == "m256"
        assert model_id == "d6213"

    def test_get_make_model_ids_make_only_fallback(self):
        # BMW with unknown model falls back to the (BMW, None) wildcard entry
        make_id, model_id = _get_make_model_ids("BMW", "M3")
        assert make_id == "m256"
        assert model_id is None

    def test_get_make_model_ids_unknown(self):
        make_id, model_id = _get_make_model_ids("Lada", "Niva")
        assert make_id is None
        assert model_id is None


# ---------------------------------------------------------------------------
# Motors scraper — _card_to_listing
# ---------------------------------------------------------------------------

class TestMotorsCardToListing:
    def _scraper(self):
        return MotorsScraper({"limits": {}})

    def _card(self, **kwargs):
        base = {
            "title":    "2022 Kia EV6 E GT-Line",
            "price":    "£21,995",
            "mileage":  "18,000 miles",
            "location": "Edinburgh",
            "spec":     "77kWh RWD",
            "link":     "/car-123456789/?i=0",
            "img":      "https://cdn.motors.co.uk/img/car.jpg",
        }
        base.update(kwargs)
        return base

    def test_listing_id_from_url(self):
        s = self._scraper()
        l = s._card_to_listing(self._card(), "Kia EV6")
        assert l.listing_id == "mt_123456789"

    def test_listing_id_fallback_no_numeric_id(self):
        s = self._scraper()
        l = s._card_to_listing(self._card(link="/car-search-abc/"), "Test")
        assert l.listing_id.startswith("mt_")

    def test_source_is_motors(self):
        l = self._scraper()._card_to_listing(self._card(), "x")
        assert l.source == "motors"

    def test_price_parsed(self):
        l = self._scraper()._card_to_listing(self._card(), "x")
        assert l.price == 21995

    def test_price_none_on_garbage(self):
        l = self._scraper()._card_to_listing(self._card(price="POA"), "x")
        assert l.price is None

    def test_mileage_parsed(self):
        l = self._scraper()._card_to_listing(self._card(), "x")
        assert l.mileage == 18000

    def test_year_extracted_from_title(self):
        l = self._scraper()._card_to_listing(self._card(), "x")
        assert l.year == 2022

    def test_url_absolute(self):
        l = self._scraper()._card_to_listing(self._card(), "x")
        assert l.url.startswith("https://www.motors.co.uk")

    def test_url_already_absolute(self):
        l = self._scraper()._card_to_listing(
            self._card(link="https://www.motors.co.uk/car-999/"), "x"
        )
        assert l.url == "https://www.motors.co.uk/car-999/"

    def test_none_on_empty_card(self):
        l = self._scraper()._card_to_listing({"title": "", "link": ""}, "x")
        assert l is None

    def test_high_mileage_flag(self):
        l = self._scraper()._card_to_listing(
            self._card(mileage="95,000 miles"), "x"
        )
        assert "High mileage" in l.attention_check

    def test_low_mileage_flag(self):
        l = self._scraper()._card_to_listing(
            self._card(mileage="12,000 miles"), "x"
        )
        assert "Low mileage" in l.attention_check

    def test_image_url_included(self):
        l = self._scraper()._card_to_listing(self._card(), "x")
        assert l.image_urls == ["https://cdn.motors.co.uk/img/car.jpg"]

    def test_non_http_image_excluded(self):
        l = self._scraper()._card_to_listing(self._card(img="data:image/gif;..."), "x")
        assert l.image_urls == []

    def test_spec_captured(self):
        l = self._scraper()._card_to_listing(self._card(), "x")
        assert "77kWh" in l.spec_summary


# ---------------------------------------------------------------------------
# dedup_across_sources
# ---------------------------------------------------------------------------

class TestDedupAcrossSources:
    def _listing(self, source, price, mileage, title):
        return Listing(
            listing_id=f"{source}_{price}",
            title=title,
            price=price,
            year=2022,
            mileage=mileage,
            location="Edinburgh",
            distance_miles=None,
            seller_type="dealer",
            seller_name="Test",
            spec_summary="",
            url="https://example.com",
            source=source,
            scraped_at=datetime.utcnow().isoformat(),
        )

    def test_at_always_kept(self):
        at = self._listing("autotrader", 20000, 30000, "Kia EV6 2022")
        result = dedup_across_sources([at])
        assert len(result) == 1
        assert result[0].source == "autotrader"

    def test_cg_with_same_price_mileage_title_dropped(self):
        at = self._listing("autotrader", 20000, 30000, "Kia EV6 2022")
        cg = self._listing("cargurus",   20000, 30000, "Kia EV6 2022")
        result = dedup_across_sources([at, cg])
        assert len(result) == 1
        assert result[0].source == "autotrader"

    def test_cg_with_different_price_kept(self):
        at = self._listing("autotrader", 20000, 30000, "Kia EV6 2022")
        cg = self._listing("cargurus",   19500, 30000, "Kia EV6 2022")
        result = dedup_across_sources([at, cg])
        assert len(result) == 2

    def test_motors_duplicate_dropped(self):
        at = self._listing("autotrader", 21000, 25000, "Hyundai Ioniq 5 2023")
        mt = self._listing("motors",     21000, 25000, "Hyundai Ioniq 5 2023")
        result = dedup_across_sources([at, mt])
        sources = {l.source for l in result}
        assert "autotrader" in sources
        assert "motors" not in sources

    def test_motors_unique_kept(self):
        at = self._listing("autotrader", 21000, 25000, "Hyundai Ioniq 5 2023")
        mt = self._listing("motors",     20000, 30000, "Hyundai Ioniq 5 2023")
        result = dedup_across_sources([at, mt])
        assert len(result) == 2

    def test_no_at_listings_all_cg_kept(self):
        cg1 = self._listing("cargurus", 18000, 40000, "Kia EV6 2021")
        cg2 = self._listing("cargurus", 19000, 35000, "Kia EV6 2022")
        result = dedup_across_sources([cg1, cg2])
        assert len(result) == 2


# ---------------------------------------------------------------------------
# Email — source label on car card button
# ---------------------------------------------------------------------------

class TestCarCardSourceLabel:
    def _listing_dict(self, source="autotrader"):
        return {
            "title":      "Test Car",
            "price":      20000,
            "year":       2022,
            "mileage":    30000,
            "location":   "Edinburgh",
            "url":        "https://example.com",
            "source":     source,
            "search_name": "Test",
            "image_urls": [],
            "attention_check": "",
            "distance_miles":  None,
            "seller_name":     "",
            "spec_summary":    "",
        }

    def test_autotrader_button_label(self):
        html = _car_card(self._listing_dict("autotrader"))
        assert "AutoTrader" in html

    def test_cargurus_button_label(self):
        html = _car_card(self._listing_dict("cargurus"))
        assert "CarGurus" in html

    def test_motors_button_label(self):
        html = _car_card(self._listing_dict("motors"))
        assert "Motors.co.uk" in html


# ---------------------------------------------------------------------------
# AutoTrader — _passes_filters (exclude/require keyword logic)
# This is the function that guards against unwanted variants slipping through.
# The bug: titles like "Skoda Enyaq" (no variant) meant "iV 60" was never found
# to exclude.  Fix: title is now sourced from the browser tab, e.g.
# "2021 Skoda Enyaq iV 60 58kWh 132PS | AutoTrader", which contains the variant.
# ---------------------------------------------------------------------------

class TestPassesFilters:
    def _scraper(self):
        return AutoTraderScraper({"limits": {}})

    def _listing(self, title="", spec_summary=""):
        return Listing(
            listing_id="test_1", title=title, price=18000, year=2022,
            mileage=30000, location="Edinburgh", distance_miles=5,
            seller_type="dealer", seller_name="Dealer", spec_summary=spec_summary,
            url="https://www.autotrader.co.uk/car-details/test_1",
        )

    def test_no_filters_always_passes(self):
        s = self._scraper()
        assert s._passes_filters(self._listing("Skoda Enyaq iV 80"), [], []) is True

    def test_exclude_keyword_in_title_rejected(self):
        """The core bug scenario: iV 60 variant must be excluded when title is full."""
        s = self._scraper()
        listing = self._listing(title="2021 Skoda Enyaq iV 60 58kWh 132PS")
        assert s._passes_filters(listing, [], ["iv 60"]) is False

    def test_enyaq_iv80_not_excluded_by_iv60_rule(self):
        """iV 80 listing must NOT be caught by the iV 60 exclude rule."""
        s = self._scraper()
        listing = self._listing(title="2022 Skoda Enyaq iV 80 82kWh 204PS")
        assert s._passes_filters(listing, [], ["iv 60"]) is True

    def test_exclude_keyword_in_spec_summary_rejected(self):
        s = self._scraper()
        listing = self._listing(title="Skoda Enyaq", spec_summary="60kWh | RWD")
        assert s._passes_filters(listing, [], ["60kwh"]) is False

    def test_title_is_lowercased_for_matching(self):
        """combined is lowercased so titles with any casing match the (pre-lowercased) keyword.
        In production, _scrape_search lowercases keywords before passing them here."""
        s = self._scraper()
        # Mixed-case and upper-case titles are both caught by the lowercased keyword
        assert s._passes_filters(self._listing(title="Skoda Enyaq iV 60"), [], ["iv 60"]) is False
        assert s._passes_filters(self._listing(title="SKODA ENYAQ IV 60"), [], ["iv 60"]) is False
        assert s._passes_filters(self._listing(title="skoda enyaq iv 60 58kwh"), [], ["iv 60"]) is False

    def test_exclude_keyword_absent_passes(self):
        s = self._scraper()
        listing = self._listing(title="2022 Skoda Enyaq iV 80 82kWh")
        assert s._passes_filters(listing, [], ["vRS", "Coupe", "iV 60"]) is True

    def test_multiple_excludes_any_match_rejects(self):
        s = self._scraper()
        listing = self._listing(title="Skoda Enyaq vRS Coupe 2023")
        assert s._passes_filters(listing, [], ["iv 60", "vrs", "coupe"]) is False

    def test_require_keyword_present_passes(self):
        s = self._scraper()
        listing = self._listing(title="Kia EV6 GT-Line RWD 77kWh")
        assert s._passes_filters(listing, ["77kwh"], []) is True

    def test_require_keyword_missing_rejected(self):
        s = self._scraper()
        listing = self._listing(title="Kia EV6 GT-Line RWD 58kWh")
        assert s._passes_filters(listing, ["77kwh"], []) is False

    def test_require_title_lowercased_for_matching(self):
        """combined is lowercased so require keyword (pre-lowercased by caller) matches any title casing."""
        s = self._scraper()
        assert s._passes_filters(self._listing(title="Kia EV6 GT-Line 77kWh"), ["77kwh"], []) is True
        assert s._passes_filters(self._listing(title="KIA EV6 GT-LINE 77KWH"), ["77kwh"], []) is True

    def test_require_multiple_all_must_match(self):
        s = self._scraper()
        listing = self._listing(title="Kia EV6 GT-Line RWD 77kWh")
        assert s._passes_filters(listing, ["77kwh", "rwd"], []) is True
        assert s._passes_filters(listing, ["77kwh", "awd"], []) is False

    def test_require_and_exclude_both_applied(self):
        """Require passes but exclude hits → listing rejected."""
        s = self._scraper()
        listing = self._listing(title="Skoda Enyaq iV 60 58kWh")
        # Require "enyaq" (present) but exclude "iv 60" (also present)
        assert s._passes_filters(listing, ["enyaq"], ["iv 60"]) is False

    def test_exclude_keyword_in_spec_not_title(self):
        """Exclusion in spec_summary works even when title is clean."""
        s = self._scraper()
        listing = self._listing(
            title="Skoda Enyaq 2022",
            spec_summary="Standard Range | iV 60 | 58kWh"
        )
        assert s._passes_filters(listing, [], ["iv 60"]) is False

    def test_short_title_no_crash(self):
        """Edge case: very short or empty title should not raise."""
        s = self._scraper()
        assert s._passes_filters(self._listing(title=""), [], ["iv 60"]) is True
        assert s._passes_filters(self._listing(title="Ok"), [], []) is True


# ---------------------------------------------------------------------------
# CarGurus scraper — _card_to_listing (location / seller_name parsing)
# ---------------------------------------------------------------------------

class TestCarGurusCardToListing:
    def _scraper(self):
        return CarGurusScraper({"limits": {}})

    def _card(self, **kwargs):
        base = {
            "title":    "2022 Kia EV6",
            "price":    "£21,498",
            "mileage":  "25,235 miles",
            "location": "Washington\n95 mi away",
            "deal":     "Fair Deal",
            "link":     "https://www.cargurus.co.uk/Cars/inventorylisting/vdp.action?listingId=157132176",
            "img":      "https://static-eu.cargurus.com/images/car.jpg",
        }
        base.update(kwargs)
        return base

    def test_location_city_only(self):
        """location field should contain only the city, not the distance text."""
        l = self._scraper()._card_to_listing(self._card(), "Kia EV6")
        assert l.location == "Washington"

    def test_distance_parsed_from_location(self):
        """Distance in miles should be extracted from the 'X mi away' part."""
        l = self._scraper()._card_to_listing(self._card(), "Kia EV6")
        assert l.distance_miles == 95

    def test_seller_name_is_city_not_full_location(self):
        """seller_name must be just the city, not 'City\nX mi away'."""
        l = self._scraper()._card_to_listing(self._card(), "Kia EV6")
        assert l.seller_name == "Washington"
        assert "\n" not in l.seller_name
        assert "mi away" not in l.seller_name

    def test_location_without_distance(self):
        """Location with no newline/distance suffix should work cleanly."""
        l = self._scraper()._card_to_listing(self._card(location="Edinburgh"), "Kia EV6")
        assert l.location == "Edinburgh"
        assert l.seller_name == "Edinburgh"
        assert l.distance_miles is None

    def test_location_empty(self):
        l = self._scraper()._card_to_listing(self._card(location=""), "Kia EV6")
        assert l.location == ""
        assert l.seller_name == ""
        assert l.distance_miles is None

    def test_price_parsed(self):
        l = self._scraper()._card_to_listing(self._card(), "x")
        assert l.price == 21498

    def test_mileage_parsed(self):
        l = self._scraper()._card_to_listing(self._card(), "x")
        assert l.mileage == 25235

    def test_year_extracted_from_title(self):
        l = self._scraper()._card_to_listing(self._card(), "x")
        assert l.year == 2022

    def test_listing_id_from_url(self):
        l = self._scraper()._card_to_listing(self._card(), "x")
        assert l.listing_id == "cg_157132176"

    def test_source_is_cargurus(self):
        l = self._scraper()._card_to_listing(self._card(), "x")
        assert l.source == "cargurus"

    def test_deal_rating_in_attention_check(self):
        l = self._scraper()._card_to_listing(self._card(), "x")
        assert "Fair Deal" in l.attention_check

    def test_low_mileage_flag(self):
        l = self._scraper()._card_to_listing(self._card(mileage="12,000 miles"), "x")
        assert "Low mileage" in l.attention_check

    def test_high_mileage_flag(self):
        l = self._scraper()._card_to_listing(self._card(mileage="95,000 miles"), "x")
        assert "High mileage" in l.attention_check

    def test_none_on_empty_card(self):
        l = self._scraper()._card_to_listing({"title": "", "link": ""}, "x")
        assert l is None

    def test_relative_url_made_absolute(self):
        l = self._scraper()._card_to_listing(
            self._card(link="/Cars/inventorylisting/vdp.action?listingId=999"), "x"
        )
        assert l.url.startswith("https://www.cargurus.co.uk")

    def test_image_url_included(self):
        l = self._scraper()._card_to_listing(self._card(), "x")
        assert l.image_urls == ["https://static-eu.cargurus.com/images/car.jpg"]

    def test_search_name_stored(self):
        l = self._scraper()._card_to_listing(self._card(), "Kia EV6")
        assert l.search_name == "Kia EV6"


# ---------------------------------------------------------------------------
# Regression: null-price false price-change  (Bug fix: database.py)
# ---------------------------------------------------------------------------
# When a listing is first inserted with price=None (extraction failed) and then
# re-scraped with a real price, the old code evaluated None != 18000 → True and
# wrongly treated it as a "price change", resetting email_sent=0.
# ---------------------------------------------------------------------------

class TestNullPriceFalsePriceChange:
    def _db(self):
        tmp = tempfile.mkdtemp()
        return ListingDatabase(f"{tmp}/test.db")

    def _listing(self, listing_id="L1", price=18000):
        return Listing(
            listing_id=listing_id, title="Kia EV6 2022", price=price,
            year=2022, mileage=30000, location="Edinburgh", distance_miles=5,
            seller_type="dealer", seller_name="EV Cars Ltd",
            spec_summary="Auto | Electric", url="https://example.com",
            search_name="Test Search",
        )

    def test_null_to_real_price_not_flagged_as_price_change(self):
        """Listing stored with NULL price → re-scraped with real price must NOT
        be treated as price change (would flood email with already-sent listings)."""
        db = self._db()
        # Insert with no price (failed extraction)
        db.process_listings([self._listing(price=None)])
        db.mark_as_sent(["L1"])
        # Re-scrape: extraction now succeeds → real price available
        # This must NOT trigger price_changed=True
        _, updated = db.process_listings([self._listing(price=18000)])
        assert len(updated) == 0, (
            "A listing with NULL stored price must not produce a price-change "
            "event — null ≠ value is a false positive from a missing original price"
        )
        assert db.get_unsent_listings() == [], (
            "Re-scraping a sent listing whose stored price was NULL must keep "
            "email_sent=1, not reset it to 0"
        )

    def test_real_price_change_still_detected(self):
        """Genuine price drops must still be detected after the null-price fix."""
        db = self._db()
        db.process_listings([self._listing(price=18000)])
        db.mark_as_sent(["L1"])
        # Price dropped: this IS a real change and must still be surfaced
        _, updated = db.process_listings([self._listing(price=17500)])
        assert len(updated) == 1, "Genuine price drop must still produce an updated listing"
        unsent = db.get_unsent_listings()
        assert len(unsent) == 1, "Price-dropped listing must be re-queued for email"

    def test_null_to_null_price_not_flagged(self):
        """Listing stored with NULL price re-scraped with NULL price: no price change."""
        db = self._db()
        db.process_listings([self._listing(price=None)])
        db.mark_as_sent(["L1"])
        _, updated = db.process_listings([self._listing(price=None)])
        assert len(updated) == 0


# ---------------------------------------------------------------------------
# Regression: CarGurus wrong-model passthrough  (Bug fix: cargurus.py)
# ---------------------------------------------------------------------------
# When _MAKE_MODELS has no specific model ID for a search (e.g. BYD Sealion 7
# falls back to the make-only BYD entry), CarGurus returns ALL electric BYD
# models. Without a model-name check the scraper would forward Atto 3, Seal,
# Dolphin etc. to the email despite require_keywords=[].
# ---------------------------------------------------------------------------

class TestCarGurusModelFilter:
    def _scraper(self):
        return CarGurusScraper({"limits": {"max_listings_per_search": 20}})

    def _search(self, make, model, require=None, exclude=None):
        return {
            "name": f"{make} {model}",
            "enabled": True,
            "autotrader": {"make": make, "model": model, "price_max": 30000},
            "require_keywords": require or [],
            "exclude_keywords": exclude or [],
        }

    def _card(self, title):
        return {
            "title": title,
            "price": "£22,000",
            "mileage": "15,000 miles",
            "location": "Edinburgh",
            "deal": "",
            "link": "https://www.cargurus.co.uk/Cars/inventorylisting/vdp.action?listingId=1",
            "img": "",
        }

    async def _run_scrape_search(self, scraper, search, cards):
        """Helper: patch _get_listing_cards so no browser is needed."""
        from unittest.mock import AsyncMock, patch, MagicMock

        mock_page = AsyncMock()
        mock_context = MagicMock()
        mock_context.new_page = AsyncMock(return_value=mock_page)

        with patch.object(scraper, "_get_listing_cards", new=AsyncMock(side_effect=[cards, []])):
            return await scraper._scrape_search(mock_context, search)

    def test_wrong_model_filtered_out(self):
        """BYD Atto 3, Seal, Dolphin must be rejected when searching for Sealion 7."""
        import asyncio
        scraper = self._scraper()
        search  = self._search("BYD", "Sealion 7")
        cards   = [
            self._card("2024 BYD Atto 3 60.5kWh"),
            self._card("2024 BYD Seal AWD"),
            self._card("2024 BYD Sealion 7 AWD 82kWh"),
            self._card("2023 BYD Dolphin"),
        ]
        results = asyncio.run(self._run_scrape_search(scraper, search, cards))
        titles = [r.title for r in results]
        assert len(results) == 1, f"Expected 1 result (Sealion 7 only), got: {titles}"
        assert "Sealion 7" in results[0].title

    def test_correct_model_passes(self):
        """A Sealion 7 matching all filters must still be returned."""
        import asyncio
        scraper = self._scraper()
        search  = self._search("BYD", "Sealion 7")
        cards   = [self._card("2024 BYD Sealion 7 AWD 82kWh")]
        results = asyncio.run(self._run_scrape_search(scraper, search, cards))
        assert len(results) == 1

    def test_model_filter_with_exclude_keyword(self):
        """Model filter and exclude_keywords must both be applied independently."""
        import asyncio
        scraper = self._scraper()
        # Enyaq search: exclude iV 60, only want iV 80
        search = self._search("Skoda", "Enyaq", exclude=["enyaq 60", "iv 60"])
        cards  = [
            self._card("2022 Skoda Enyaq iV 60 58kWh"),    # excluded by keyword
            self._card("2022 Skoda Enyaq iV 80 82kWh"),    # passes
            self._card("2022 VW ID.4 Pro 77kWh"),          # wrong model (Enyaq absent)
        ]
        results = asyncio.run(self._run_scrape_search(scraper, search, cards))
        titles = [r.title for r in results]
        assert len(results) == 1, f"Expected 1 result (iV 80 only), got: {titles}"
        assert "80" in results[0].title

    def test_make_only_fallback_filters_by_make(self):
        """When model is empty, filter by make name instead."""
        import asyncio
        scraper = self._scraper()
        search  = self._search("BMW", "")
        cards   = [
            self._card("2023 BMW iX3 M Sport"),
            self._card("2022 Mercedes EQC 400"),   # wrong make
        ]
        results = asyncio.run(self._run_scrape_search(scraper, search, cards))
        titles = [r.title for r in results]
        assert len(results) == 1, f"Expected only BMW, got: {titles}"
        assert "BMW" in results[0].title


# ---------------------------------------------------------------------------
# Regression: filter_text broadens keyword matching  (Bug fix: autotrader.py)
# ---------------------------------------------------------------------------
# When AutoTrader renders a listing with a generic tab title like
# "2021 Black Skoda Enyaq for sale for £14,594" the variant ("iV 60", "58kWh")
# is NOT in the title and spec_summary is often empty too. filter_text pulls
# the rich heading/description/spec text from the detail page via JS and gives
# _passes_filters a richer corpus to check against.
# ---------------------------------------------------------------------------

class TestFilterText:
    def _scraper(self):
        return AutoTraderScraper({"limits": {}})

    def _listing(self, title="", spec_summary="", filter_text=""):
        return Listing(
            listing_id="test_1", title=title, price=18000, year=2022,
            mileage=30000, location="Edinburgh", distance_miles=5,
            seller_type="dealer", seller_name="Dealer", spec_summary=spec_summary,
            url="https://www.autotrader.co.uk/car-details/test_1",
            filter_text=filter_text,
        )

    def test_exclude_keyword_in_filter_text_rejected(self):
        """Core regression: generic tab title + spec variant only in filter_text."""
        s = self._scraper()
        listing = self._listing(
            title="2021 Black 2021 Skoda Enyaq for sale for £14,594 in Bootle",
            spec_summary="",
            filter_text="Skoda Enyaq iV 60 58kWh 132PS | Manual",
        )
        assert s._passes_filters(listing, [], ["iv 60"]) is False

    def test_exclude_keyword_in_filter_text_kwh_variant(self):
        """58kWh exclusion works via filter_text even with generic title.
        Note: the iV 60 has a 58kWh battery; AutoTrader lists it as '58kWh' not '60kWh'."""
        s = self._scraper()
        listing = self._listing(
            title="2021 Blue Skoda Enyaq for sale for £12,500",
            spec_summary="",
            filter_text="Enyaq iV 60 58kWh Electric",
        )
        assert s._passes_filters(listing, [], ["58kwh"]) is False
        assert s._passes_filters(listing, [], ["iv 60"]) is False

    def test_correct_variant_not_excluded(self):
        """iV 80 must NOT be rejected by the iV 60 rule, even via filter_text."""
        s = self._scraper()
        listing = self._listing(
            title="2022 White 2022 Skoda Enyaq for sale for £18,500",
            spec_summary="",
            filter_text="Enyaq iV 80 82kWh | 204PS | AWD",
        )
        assert s._passes_filters(listing, [], ["iv 60", "enyaq 60", "58kwh"]) is True

    def test_filter_text_does_not_appear_in_to_dict(self):
        """filter_text must be excluded from serialised output."""
        listing = self._listing(
            title="Generic title", filter_text="iV 60 58kWh"
        )
        d = listing.to_dict()
        assert "filter_text" not in d

    def test_filter_text_default_empty(self):
        """Existing code constructing Listing without filter_text is unaffected."""
        listing = Listing(
            listing_id="x", title="Test", price=None, year=None, mileage=None,
            location="", distance_miles=None, seller_type="dealer",
            seller_name="", spec_summary="", url="https://example.com",
        )
        assert listing.filter_text == ""

    def test_require_keyword_found_in_filter_text(self):
        """require_keywords match against filter_text when title is generic."""
        s = self._scraper()
        listing = self._listing(
            title="2022 Grey Hyundai IONIQ 5 for sale for £19,000",
            spec_summary="",
            filter_text="IONIQ 5 73kWh RWD | Luxury trim",
        )
        assert s._passes_filters(listing, ["ioniq 5"], []) is True

    def test_require_keyword_absent_from_all_fields_rejected(self):
        """If require keyword isn't in title, spec_summary, or filter_text → reject."""
        s = self._scraper()
        listing = self._listing(
            title="2022 Grey Hyundai car for sale for £19,000",
            spec_summary="",
            filter_text="Tucson 2.0 diesel",
        )
        assert s._passes_filters(listing, ["ioniq 5"], []) is False


# ---------------------------------------------------------------------------
# Regression: discarded_listings DB table  (Bug fix: database.py)
# ---------------------------------------------------------------------------

class TestDiscardedListings:
    def _db(self):
        tmp = tempfile.mkdtemp()
        return ListingDatabase(f"{tmp}/test.db")

    def test_record_discarded_stores_ids(self):
        db = self._db()
        db.record_discarded(["AT123", "AT456"])
        known = db.get_known_listing_ids()
        assert "AT123" in known
        assert "AT456" in known

    def test_discarded_ids_in_known_ids(self):
        """Discarded IDs must appear in get_known_listing_ids so pages can be skipped."""
        db = self._db()
        db.record_discarded(["DISC1"])
        assert "DISC1" in db.get_known_listing_ids()

    def test_record_discarded_empty_list_is_safe(self):
        db = self._db()
        db.record_discarded([])   # should not raise

    def test_record_discarded_idempotent(self):
        """Re-recording the same ID twice does not raise or duplicate."""
        db = self._db()
        db.record_discarded(["DUP1"])
        db.record_discarded(["DUP1"])   # INSERT OR IGNORE
        known = db.get_known_listing_ids()
        assert "DUP1" in known

    def test_discarded_ids_pruned_after_cutoff(self):
        """IDs discarded more than max_age_days ago must not appear in known_ids."""
        db = self._db()
        db.record_discarded(["OLD1"])
        # Back-date the entry
        old_ts = (datetime.now(timezone.utc) - timedelta(days=15)).isoformat()
        with db._connect() as conn:
            conn.execute(
                "UPDATE discarded_listings SET discarded_at = ? WHERE listing_id = 'OLD1'",
                (old_ts,)
            )
        # get_known_listing_ids prunes entries older than max_age_days (14)
        known = db.get_known_listing_ids(max_age_days=14)
        assert "OLD1" not in known

    def test_recent_discarded_ids_kept(self):
        """IDs discarded within max_age_days must remain in known_ids."""
        db = self._db()
        db.record_discarded(["RECENT1"])
        known = db.get_known_listing_ids(max_age_days=14)
        assert "RECENT1" in known

    def test_known_ids_union_of_listings_and_discarded(self):
        """known_ids = recent DB listings ∪ recent discarded listings."""
        db = self._db()
        listing = Listing(
            listing_id="REAL1", title="Kia EV6", price=18000, year=2022,
            mileage=30000, location="Edinburgh", distance_miles=5,
            seller_type="dealer", seller_name="Dealer",
            spec_summary="", url="https://example.com", search_name="Kia EV6",
        )
        db.process_listings([listing])
        db.record_discarded(["DISC1"])
        known = db.get_known_listing_ids()
        assert "REAL1" in known
        assert "DISC1" in known
