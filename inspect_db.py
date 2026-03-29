#!/usr/bin/env python3
"""
inspect_db.py — standalone DB inspector, no external dependencies.
Uses Python's built-in sqlite3 module only.

Usage:
    python3 inspect_db.py [db_path]

Default db_path: /opt/ev-scraper/data/listings.db
"""

import sqlite3
import sys
from datetime import datetime, timezone, timedelta

DB_PATH = sys.argv[1] if len(sys.argv) > 1 else "/opt/ev-scraper/data/listings.db"
KNOWN_IDS_WINDOW_DAYS = 14

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

now = datetime.now(timezone.utc)
cutoff_14d = (now - timedelta(days=KNOWN_IDS_WINDOW_DAYS)).isoformat()

print(f"\nDB path : {DB_PATH}")
print(f"Now     : {now.strftime('%Y-%m-%d %H:%M UTC')}")
print(f"14d ago : {(now - timedelta(days=14)).strftime('%Y-%m-%d %H:%M UTC')}")

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
print("\n" + "=" * 70)
print("SUMMARY")
print("=" * 70)

cur.execute("SELECT COUNT(*) FROM listings")
total = cur.fetchone()[0]

cur.execute("SELECT COUNT(*) FROM listings WHERE email_sent = 0 AND title IS NOT NULL")
unsent = cur.fetchone()[0]

cur.execute("SELECT COUNT(*) FROM listings WHERE last_seen >= ?", (cutoff_14d,))
in_window = cur.fetchone()[0]

cur.execute("SELECT COUNT(*) FROM listings WHERE last_seen < ? AND title IS NOT NULL", (cutoff_14d,))
stale = cur.fetchone()[0]

cur.execute("SELECT COUNT(*) FROM listings WHERE attention_check LIKE '%Re-listed%' AND title IS NOT NULL")
relisted = cur.fetchone()[0]

print(f"  Total listings in DB              : {total}")
print(f"  In 14-day known_ids window        : {in_window}")
print(f"  Stale (>14d, will re-trigger)     : {stale}")
print(f"  Unsent (queued for next email)    : {unsent}")
print(f"  Currently flagged as re-listed    : {relisted}")

# ---------------------------------------------------------------------------
# Stale listings — will be re-discovered on the next run
# ---------------------------------------------------------------------------
print("\n" + "=" * 70)
print(f"STALE LISTINGS  (last_seen > 14 days ago — will be re-scraped next run)")
print("=" * 70)

cur.execute("""
    SELECT listing_id, search_name, title, price, mileage,
           last_seen, times_seen, email_sent
    FROM listings
    WHERE last_seen < ? AND title IS NOT NULL
    ORDER BY search_name, last_seen DESC
""", (cutoff_14d,))
rows = cur.fetchall()

if rows:
    current_search = None
    for r in rows:
        if r["search_name"] != current_search:
            current_search = r["search_name"]
            print(f"\n  [{current_search}]")
        price_str   = f"£{r['price']:,}"  if r["price"]   else "£?"
        mileage_str = f"{r['mileage']:,}mi" if r["mileage"] else "?mi"
        sent_str    = "sent" if r["email_sent"] else "UNSENT"
        last_str    = r["last_seen"][:10] if r["last_seen"] else "?"
        print(f"    {r['title'][:55]}")
        print(f"    {price_str} | {mileage_str} | last seen: {last_str} | seen {r['times_seen']}x | {sent_str}")
else:
    print("  (none — all listings are within the 14-day window)")

# ---------------------------------------------------------------------------
# Already flagged as re-listed
# ---------------------------------------------------------------------------
print("\n" + "=" * 70)
print("ALREADY FLAGGED AS RE-LISTED  (♻️ in attention_check)")
print("=" * 70)

cur.execute("""
    SELECT listing_id, search_name, title, price, mileage,
           attention_check, last_seen, email_sent
    FROM listings
    WHERE attention_check LIKE '%Re-listed%' AND title IS NOT NULL
    ORDER BY last_seen DESC
""")
rows = cur.fetchall()

if rows:
    for r in rows:
        price_str   = f"£{r['price']:,}"  if r["price"]   else "£?"
        mileage_str = f"{r['mileage']:,}mi" if r["mileage"] else "?mi"
        sent_str    = "sent" if r["email_sent"] else "UNSENT"
        print(f"\n  [{r['search_name']}] {r['title'][:55]}")
        print(f"  {price_str} | {mileage_str} | {sent_str}")
        print(f"  ⚠  {r['attention_check']}")
else:
    print("  (none yet — will appear after the next scrape cycle picks up a stale listing)")

# ---------------------------------------------------------------------------
# Unsent queue
# ---------------------------------------------------------------------------
print("\n" + "=" * 70)
print("UNSENT QUEUE  (will appear in next email)")
print("=" * 70)

cur.execute("""
    SELECT search_name, title, price, mileage, attention_check, first_seen
    FROM listings
    WHERE email_sent = 0 AND title IS NOT NULL
    ORDER BY search_name, price ASC
""")
rows = cur.fetchall()

if rows:
    current_search = None
    for r in rows:
        if r["search_name"] != current_search:
            current_search = r["search_name"]
            print(f"\n  [{current_search}]")
        price_str   = f"£{r['price']:,}"  if r["price"]   else "£?"
        mileage_str = f"{r['mileage']:,}mi" if r["mileage"] else "?mi"
        first_str   = r["first_seen"][:10] if r["first_seen"] else "?"
        attn        = f"\n    ⚠  {r['attention_check']}" if r["attention_check"] else ""
        print(f"    {r['title'][:55]}")
        print(f"    {price_str} | {mileage_str} | first seen: {first_str}{attn}")
else:
    print("  (nothing queued — inbox is up to date)")

conn.close()
print("\n" + "=" * 70 + "\n")
