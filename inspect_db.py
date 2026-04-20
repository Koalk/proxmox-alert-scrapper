#!/usr/bin/env python3
"""
inspect_db.py — standalone DB inspector, no external dependencies.
Uses Python's built-in sqlite3 module only.

Usage:
    python3 inspect_db.py [db_path]
    python3 inspect_db.py [db_path] --purge "Kia EV6 (test)"
    python3 inspect_db.py [db_path] --reset-unsent
    python3 inspect_db.py [db_path] --wipe

Default db_path: /opt/ev-scraper/data/listings.db

Flags:
    --purge "Search Name"   Delete all listings for that search_name and exit.
                            Prints a preview and asks for confirmation first.
    --reset-unsent          Mark ALL listings as unsent (email_sent=0, is_new=1)
                            so they re-appear in the next email run.
    --wipe                  Delete ALL data (listings + run_log + discarded).
                            Use when starting fresh with a new search config.
                            Asks for confirmation first.
"""

import sqlite3
import sys
from datetime import datetime, timezone, timedelta

args = sys.argv[1:]
purge_name = None
reset_unsent = False
wipe = False
db_args = []
i = 0
while i < len(args):
    if args[i] == "--purge" and i + 1 < len(args):
        purge_name = args[i + 1]
        i += 2
    elif args[i] == "--reset-unsent":
        reset_unsent = True
        i += 1
    elif args[i] == "--wipe":
        wipe = True
        i += 1
    else:
        db_args.append(args[i])
        i += 1

DB_PATH = db_args[0] if db_args else "/opt/ev-scraper/data/listings.db"
KNOWN_IDS_WINDOW_DAYS = 14

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

# ---------------------------------------------------------------------------
# --purge mode
# ---------------------------------------------------------------------------
if purge_name:
    cur.execute(
        "SELECT COUNT(*) FROM listings WHERE search_name = ?", (purge_name,)
    )
    count = cur.fetchone()[0]
    if count == 0:
        print(f"No listings found for search_name '{purge_name}'. Nothing to delete.")
        conn.close()
        sys.exit(0)

    cur.execute(
        "SELECT listing_id, title, price, source FROM listings WHERE search_name = ? ORDER BY price ASC",
        (purge_name,),
    )
    rows = cur.fetchall()
    print(f"\nAbout to delete {count} listing(s) with search_name='{purge_name}':\n")
    for r in rows:
        price_str = f"£{r['price']:,}" if r["price"] else "£?"
        print(f"  [{r['source']}] {(r['title'] or '(no title)')[:60]}  {price_str}")

    print(f"\nType YES to confirm deletion: ", end="")
    answer = input().strip()
    if answer != "YES":
        print("Aborted — nothing deleted.")
        conn.close()
        sys.exit(0)

    conn.execute("DELETE FROM listings WHERE search_name = ?", (purge_name,))
    conn.commit()
    vacuum_conn = sqlite3.connect(DB_PATH)
    try:
        vacuum_conn.isolation_level = None
        vacuum_conn.execute("VACUUM")
    finally:
        vacuum_conn.close()
    print(f"Deleted {count} listing(s) for '{purge_name}'.")
    conn.close()
    sys.exit(0)

# ---------------------------------------------------------------------------
# --reset-unsent mode
# ---------------------------------------------------------------------------
if reset_unsent:
    conn = sqlite3.connect(DB_PATH)
    cur2 = conn.cursor()
    cur2.execute("SELECT COUNT(*) FROM listings WHERE title IS NOT NULL")
    total_count = cur2.fetchone()[0]
    cur2.execute("SELECT COUNT(*) FROM listings WHERE email_sent = 1 AND title IS NOT NULL")
    sent_count = cur2.fetchone()[0]
    print(f"\nThis will mark all {sent_count} previously-sent listings as unsent")
    print(f"(out of {total_count} total). They will re-appear in the next email run.")
    print(f"\nType YES to confirm: ", end="")
    answer = input().strip()
    if answer != "YES":
        print("Aborted — nothing changed.")
        conn.close()
        sys.exit(0)
    conn.execute("UPDATE listings SET email_sent = 0, is_new = 1 WHERE title IS NOT NULL")
    conn.commit()
    print(f"Done — {sent_count} listings reset to unsent.")
    conn.close()
    sys.exit(0)

# ---------------------------------------------------------------------------
# --wipe mode
# ---------------------------------------------------------------------------
if wipe:
    conn = sqlite3.connect(DB_PATH)
    cur2 = conn.cursor()
    cur2.execute("SELECT COUNT(*) FROM listings")
    total = cur2.fetchone()[0]
    tables = [r[0] for r in cur2.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()]
    print(f"\nThis will DELETE ALL DATA from the database:")
    print(f"  {total} listing(s) in 'listings'")
    print(f"  All rows in: {', '.join(t for t in tables if t != 'listings')}")
    print(f"\nType YES to confirm complete wipe: ", end="")
    answer = input().strip()
    if answer != "YES":
        print("Aborted — nothing deleted.")
        conn.close()
        sys.exit(0)
    for table in tables:
        conn.execute(f"DELETE FROM {table}")
    conn.commit()
    vacuum_conn = sqlite3.connect(DB_PATH)
    try:
        vacuum_conn.isolation_level = None
        vacuum_conn.execute("VACUUM")
    finally:
        vacuum_conn.close()
    print(f"Done — all data wiped. DB is now empty.")
    conn.close()
    sys.exit(0)


now = datetime.now(timezone.utc)
cutoff_14d = (now - timedelta(days=KNOWN_IDS_WINDOW_DAYS)).isoformat()
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
