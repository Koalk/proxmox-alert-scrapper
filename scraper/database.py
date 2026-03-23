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
from datetime import datetime
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
                    image_urls      TEXT,   -- JSON array
                    attention_check TEXT,
                    scraped_at      TEXT,
                    first_seen      TEXT,
                    last_seen       TEXT,
                    times_seen      INTEGER DEFAULT 1,
                    is_new          INTEGER DEFAULT 1  -- 1 = new this run
                );

                CREATE TABLE IF NOT EXISTS run_log (
                    run_id       INTEGER PRIMARY KEY AUTOINCREMENT,
                    started_at   TEXT,
                    finished_at  TEXT,
                    total_found  INTEGER,
                    new_count    INTEGER
                );
            """)

    def mark_run_start(self) -> int:
        with self._connect() as conn:
            cur = conn.execute(
                "INSERT INTO run_log (started_at) VALUES (?)",
                (datetime.utcnow().isoformat(),)
            )
            return cur.lastrowid

    def mark_run_end(self, run_id: int, total: int, new_count: int):
        with self._connect() as conn:
            conn.execute(
                "UPDATE run_log SET finished_at=?, total_found=?, new_count=? "
                "WHERE run_id=?",
                (datetime.utcnow().isoformat(), total, new_count, run_id)
            )

    def process_listings(self, listings: list) -> tuple[list, list]:
        """
        Separate listings into new and previously seen.
        Updates DB accordingly.
        Returns (new_listings, updated_listings).
        """
        now = datetime.utcnow().isoformat()
        new_listings = []
        updated_listings = []

        with self._connect() as conn:
            # Reset is_new flag from previous run
            conn.execute("UPDATE listings SET is_new = 0")

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
                            last_seen = ?,
                            times_seen = times_seen + 1,
                            price = COALESCE(?, price),
                            attention_check = ?,
                            is_new = 0
                        WHERE listing_id = ?
                    """, (
                        now,
                        listing.price,
                        listing.attention_check,
                        listing.listing_id
                    ))
                    if price_changed:
                        listing.attention_check += (
                            f" | 💲 Price changed from £{existing['price']:,}"
                        )
                        updated_listings.append(listing)

        return new_listings, updated_listings

    def get_all_active(self) -> list[dict]:
        """Return all listings seen in the most recent run as dicts."""
        with self._connect() as conn:
            rows = conn.execute("""
                SELECT * FROM listings
                WHERE last_seen >= (
                    SELECT MAX(last_seen) FROM listings
                )
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