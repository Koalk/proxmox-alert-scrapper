"""
scraper/database.py — SQLite listing store

KEY METHODS:
  process_listings(listings)     upsert scraped Listing objects; sets is_new=1
                                  on first insert, detects price changes.
  get_unsent_listings()          returns rows where email_sent=0 (new + price
                                  changes) — this is what goes in the email.
  mark_sent(listing_ids)         clears email_sent=0 → 1 and strips raw data
                                  after a successful email send.
  get_known_listing_ids()        returns set of all IDs in DB — passed to
                                  scrapers so they can skip already-seen pages.
  get_searches_with_recent_unsent() used for crash-resume: skips re-scraping
                                  searches that already have queued unsent data.
  get_stats() / get_all_active() used for email header summary and JSON export.

SCHEMA: listings table — listing_id (PK), source, search_name, title, price,
  year, mileage, location, url, image_urls (JSON), is_new, email_sent,
  first_seen, last_seen, times_seen, scraped_at, + all other Listing fields.
"""

import json
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class ListingDatabase:
    def __init__(self, db_path: str):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        with self._connect() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS listings (
                    listing_id      TEXT PRIMARY KEY,
                    search_name     TEXT,
                    title           TEXT,
                    price           INTEGER,
                    year            INTEGER,
                    mileage         INTEGER,
                    location        TEXT,
                    distance_miles  INTEGER,
                    seller_type     TEXT,
                    seller_name     TEXT,
                    spec_summary    TEXT,
                    url             TEXT,
                    image_urls      TEXT,
                    attention_check TEXT,
                    scraped_at      TEXT,
                    first_seen      TEXT,
                    last_seen       TEXT,
                    times_seen      INTEGER DEFAULT 1,
                    is_new          INTEGER DEFAULT 1,
                    email_sent      INTEGER DEFAULT 0,
                    source          TEXT DEFAULT 'autotrader'
                );

                CREATE TABLE IF NOT EXISTS run_log (
                    run_id       INTEGER PRIMARY KEY AUTOINCREMENT,
                    started_at   TEXT,
                    finished_at  TEXT,
                    total_found  INTEGER,
                    new_count    INTEGER
                );
            """)
            # Migrate existing databases
            existing_cols = {row[1] for row in conn.execute("PRAGMA table_info(listings)")}
            if "email_sent" not in existing_cols:
                conn.execute("ALTER TABLE listings ADD COLUMN email_sent INTEGER DEFAULT 0")
            if "source" not in existing_cols:
                conn.execute("ALTER TABLE listings ADD COLUMN source TEXT DEFAULT 'autotrader'")

    def reset(self):
        """Drop and recreate all tables. Wipes everything — use with care."""
        with self._connect() as conn:
            conn.executescript("DROP TABLE IF EXISTS listings; DROP TABLE IF EXISTS run_log;")
        self._init_db()

    def mark_all_unsent(self):
        """
        Reset email_sent=0 on every listing so they all re-appear in the next
        email. Also sets is_new=1 so they show as new rather than price-changes.
        Useful after a run that produced garbage results but still marked valid
        listings as sent.
        """
        with self._connect() as conn:
            conn.execute("UPDATE listings SET email_sent=0, is_new=1")

    def mark_run_start(self) -> int:
        with self._connect() as conn:
            cur = conn.execute(
                "INSERT INTO run_log (started_at) VALUES (?)",
                (datetime.now(timezone.utc).isoformat(),)
            )
            # Keep only the most recent 90 run_log rows
            conn.execute("""
                DELETE FROM run_log WHERE run_id NOT IN (
                    SELECT run_id FROM run_log ORDER BY run_id DESC LIMIT 90
                )
            """)
            return cur.lastrowid

    def mark_run_end(self, run_id: int, total: int, new_count: int):
        with self._connect() as conn:
            conn.execute(
                "UPDATE run_log SET finished_at=?, total_found=?, new_count=? "
                "WHERE run_id=?",
                (datetime.now(timezone.utc).isoformat(), total, new_count, run_id)
            )

    def process_listings(self, listings: list) -> tuple[list, list]:
        """
        Separate listings into new and previously seen.
        Updates DB accordingly.
        Returns (new_listings, updated_listings).
        """
        now = datetime.now(timezone.utc).isoformat()
        new_listings = []
        updated_listings = []

        with self._connect() as conn:
            for listing in listings:
                existing = conn.execute(
                    "SELECT * FROM listings WHERE listing_id = ?",
                    (listing.listing_id,)
                ).fetchone()

                image_json = json.dumps(listing.image_urls)

                if existing is None:
                    # Brand new listing
                    conn.execute("""
                        INSERT INTO listings
                        (listing_id, search_name, title, price, year, mileage,
                         location, distance_miles, seller_type, seller_name,
                         spec_summary, url, image_urls, attention_check,
                         scraped_at, first_seen, last_seen, times_seen, is_new, source)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,1,?)
                    """, (
                        listing.listing_id, listing.search_name, listing.title,
                        listing.price, listing.year, listing.mileage,
                        listing.location, listing.distance_miles,
                        listing.seller_type, listing.seller_name,
                        listing.spec_summary, listing.url, image_json,
                        listing.attention_check, listing.scraped_at, now, now,
                        getattr(listing, "source", "autotrader"),
                    ))
                    new_listings.append(listing)
                else:
                    # Seen before — update last_seen and price (may have changed)
                    # Guard against NULL price in DB (stripped by mark_as_sent):
                    # None != value is True in Python, which would falsely flag
                    # every re-scraped sent listing as a price change.
                    price_changed = (
                        existing["price"] is not None
                        and listing.price is not None
                        and existing["price"] != listing.price
                    )
                    # Detect re-appearance: listing was outside the 14-day known_ids
                    # window so the scraper visited it again — possibly a re-posted ad
                    try:
                        last_seen_dt = datetime.fromisoformat(existing["last_seen"])
                        now_dt       = datetime.fromisoformat(now)
                        days_since   = (now_dt - last_seen_dt).days
                        re_appeared  = days_since >= 14
                    except Exception:
                        days_since  = 0
                        re_appeared = False
                    attention_note = listing.attention_check or ""
                    if re_appeared:
                        re_flag = f"\u267b\ufe0f Re-listed after ~{days_since}d gap"
                        attention_note = (
                            f"{re_flag} | {attention_note}" if attention_note
                            else re_flag
                        )
                    conn.execute("""
                        UPDATE listings SET
                            last_seen       = ?,
                            times_seen      = times_seen + 1,
                            price           = COALESCE(?, price),
                            title           = ?,
                            year            = COALESCE(?, year),
                            mileage         = COALESCE(?, mileage),
                            location        = COALESCE(?, location),
                            distance_miles  = COALESCE(?, distance_miles),
                            seller_type     = COALESCE(?, seller_type),
                            seller_name     = COALESCE(?, seller_name),
                            spec_summary    = COALESCE(?, spec_summary),
                            image_urls      = ?,
                            attention_check = ?,
                            is_new          = 0,
                            email_sent      = CASE WHEN ? THEN 0 ELSE email_sent END
                        WHERE listing_id = ?
                    """, (
                        now,
                        listing.price,
                        listing.title,
                        listing.year,
                        listing.mileage,
                        listing.location,
                        listing.distance_miles,
                        listing.seller_type,
                        listing.seller_name,
                        listing.spec_summary,
                        json.dumps(listing.image_urls),
                        attention_note,
                        price_changed or re_appeared,   # reset email_sent on change or re-list
                        listing.listing_id,
                    ))
                    if price_changed:
                        old_price = existing['price']
                        old_str = f"£{old_price:,}" if old_price is not None else "unknown"
                        listing.attention_check += f" | 💲 Price changed from {old_str}"
                        updated_listings.append(listing)
                    elif re_appeared:
                        listing.attention_check = attention_note
                        updated_listings.append(listing)

        return new_listings, updated_listings

    def get_unsent_listings(self) -> list[dict]:
        """Return all listings not yet included in a sent email, with full data."""
        with self._connect() as conn:
            rows = conn.execute("""
                SELECT * FROM listings
                WHERE email_sent = 0 AND title IS NOT NULL
                ORDER BY first_seen ASC, price ASC
            """).fetchall()
            result = []
            for row in rows:
                d = dict(row)
                d["image_urls"] = json.loads(d.get("image_urls") or "[]")
                result.append(d)
            return result

    def get_known_listing_ids(self, max_age_days: int = 14) -> set[str]:
        """Return listing IDs last seen within max_age_days.
        Older IDs are excluded so the scraper re-discovers (and re-flags) them."""
        cutoff = (
            datetime.now(timezone.utc) - timedelta(days=max_age_days)
        ).isoformat()
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT listing_id FROM listings WHERE last_seen >= ?",
                (cutoff,)
            ).fetchall()
            return {row[0] for row in rows}

    def get_searches_with_recent_unsent(self, max_age_hours: int = 20) -> set[str]:
        """
        Return search names that have unsent listings scraped within the last
        max_age_hours.  Used to skip re-scraping searches that already completed
        in a previous run that failed before sending the email.
        max_age_hours=20 means yesterday's run (which already sent its email
        and stripped the data) won't match, but a run from 2 hours ago will.
        """
        cutoff = (
            datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
        ).isoformat()
        with self._connect() as conn:
            rows = conn.execute("""
                SELECT DISTINCT search_name FROM listings
                WHERE email_sent = 0
                  AND title IS NOT NULL
                  AND last_seen >= ?
            """, (cutoff,)).fetchall()
            return {row[0] for row in rows}

    def mark_as_sent(self, listing_ids: list[str]):
        """
        Mark listings as sent and strip rich display data.
        Keeps only the fields needed to detect duplicates on future runs:
        listing_id, url, price, first_seen, last_seen, times_seen.
        """
        if not listing_ids:
            return
        placeholders = ",".join("?" * len(listing_ids))
        with self._connect() as conn:
            conn.execute(f"""
                UPDATE listings SET
                    email_sent      = 1,
                    title           = NULL,
                    year            = NULL,
                    mileage         = NULL,
                    location        = NULL,
                    distance_miles  = NULL,
                    seller_type     = NULL,
                    seller_name     = NULL,
                    spec_summary    = NULL,
                    image_urls      = NULL,
                    attention_check = NULL,
                    scraped_at      = NULL
                WHERE listing_id IN ({placeholders})
            """, listing_ids)
        # VACUUM cannot run inside a transaction — use a fresh autocommit connection
        vacuum_conn = sqlite3.connect(self.db_path)
        try:
            vacuum_conn.isolation_level = None
            vacuum_conn.execute("VACUUM")
        finally:
            vacuum_conn.close()

    def delete_listings(self, listing_ids: list[str]):
        """Permanently delete listings from the DB (used to remove cross-source duplicates)."""
        if not listing_ids:
            return
        placeholders = ",".join("?" * len(listing_ids))
        with self._connect() as conn:
            conn.execute(
                f"DELETE FROM listings WHERE listing_id IN ({placeholders})",
                listing_ids,
            )

    def get_all_active(self) -> list[dict]:
        """Return all listings that still have full data (not yet stripped after send)."""
        with self._connect() as conn:
            rows = conn.execute("""
                SELECT * FROM listings
                WHERE title IS NOT NULL
                ORDER BY price ASC
            """).fetchall()
            result = []
            for row in rows:
                d = dict(row)
                d["image_urls"] = json.loads(d.get("image_urls") or "[]")
                result.append(d)
            return result

    def get_stats(self) -> dict:
        with self._connect() as conn:
            total = conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
            last_run = conn.execute(
                "SELECT * FROM run_log ORDER BY run_id DESC LIMIT 1"
            ).fetchone()
            return {
                "total_in_db": total,
                "last_run": dict(last_run) if last_run else {},
            }