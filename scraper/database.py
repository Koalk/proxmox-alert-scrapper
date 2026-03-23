"""
scraper/database.py
SQLite-backed store for seen listings.
Tracks which listing IDs have been seen before so only genuinely
new listings trigger email alerts.
Also stores full listing data for the JSON export.
"""

import json
import logging
import sqlite3
from datetime import datetime, timezone
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
                    email_sent      INTEGER DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS run_log (
                    run_id       INTEGER PRIMARY KEY AUTOINCREMENT,
                    started_at   TEXT,
                    finished_at  TEXT,
                    total_found  INTEGER,
                    new_count    INTEGER
                );
            """)
            # Migrate existing databases that predate the email_sent column
            existing_cols = {row[1] for row in conn.execute("PRAGMA table_info(listings)")}
            if "email_sent" not in existing_cols:
                conn.execute("ALTER TABLE listings ADD COLUMN email_sent INTEGER DEFAULT 0")

    def mark_run_start(self) -> int:
        with self._connect() as conn:
            cur = conn.execute(
                "INSERT INTO run_log (started_at) VALUES (?)",
                (datetime.now(timezone.utc).isoformat(),)
            )
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
                         scraped_at, first_seen, last_seen, times_seen, is_new)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,1)
                    """, (
                        listing.listing_id, listing.search_name, listing.title,
                        listing.price, listing.year, listing.mileage,
                        listing.location, listing.distance_miles,
                        listing.seller_type, listing.seller_name,
                        listing.spec_summary, listing.url, image_json,
                        listing.attention_check, listing.scraped_at, now, now
                    ))
                    new_listings.append(listing)
                else:
                    # Seen before — update last_seen and price (may have changed)
                    price_changed = (
                        existing["price"] != listing.price
                        and listing.price is not None
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
                        listing.attention_check,
                        price_changed,   # reset email_sent only if price changed
                        listing.listing_id,
                    ))
                    if price_changed:
                        listing.attention_check += (
                            f" | 💲 Price changed from £{existing['price']:,}"
                        )
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