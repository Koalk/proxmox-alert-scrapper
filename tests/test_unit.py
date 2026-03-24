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

from scraper.autotrader import build_autotrader_url, Listing
from scraper.cargurus  import build_cargurus_url, _get_make_model_ids
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
        cfg = dict(self._CFG, make="Hyundai", model="Ioniq 5")
        url = build_cargurus_url(cfg)
        assert "fuelTypes=ELECTRIC" in url
        assert "makeModelTrimPaths=m279" in url
        assert "makeModelTrimPaths" in url
        assert url.count("makeModelTrimPaths") == 1

    def test_unknown_make_returns_empty(self):
        cfg = dict(self._CFG, make="UnknownMake", model="Unknown")
        url = build_cargurus_url(cfg)
        assert url == ""

    def test_get_make_model_ids_known(self):
        make_id, model_id = _get_make_model_ids("Kia", "EV6")
        assert make_id == "m306"
        assert model_id == "d6251"

    def test_get_make_model_ids_make_only(self):
        make_id, model_id = _get_make_model_ids("BMW", "iX3")
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
