#!/usr/bin/env python3
"""
main.py — EV Car Alert Scraper
Entry point. Loads config, runs AutoTrader + CarGurus scrapers,
deduplicates results, updates SQLite DB, sends HTML email digest,
and writes latest_results.json.

Usage:
    python3 main.py                          # normal overnight run
    python3 main.py --dry-run                # scrape, save JSON, no email
    python3 main.py --force-email            # send digest even if nothing new
    python3 main.py --test-email             # send a test email immediately
    python3 main.py --autotrader-only        # skip CarGurus (faster)
    python3 main.py --config /path/to/config.yaml
"""

import argparse
import asyncio
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).parent))

from scraper.autotrader import AutoTraderScraper
from scraper.cargurus   import CarGurusScraper
from scraper.database   import ListingDatabase
from scraper.emailer    import send_email


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def setup_logging(log_path: str):
    log_dir = Path(log_path).parent
    log_dir.mkdir(parents=True, exist_ok=True)
    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        handlers=[
            logging.FileHandler(log_path),
            logging.StreamHandler(sys.stdout),
        ],
    )
    # Quieten noisy Playwright internals
    logging.getLogger("playwright").setLevel(logging.WARNING)


def load_config(config_path) -> dict:
    with open(config_path) as f:
        return yaml.safe_load(f)


def apply_defaults(searches: list, defaults: dict) -> list:
    """
    Merge top-level defaults into each search's autotrader block.
    Per-search values always win over defaults.
    """
    result = []
    for search in searches:
        s = dict(search)
        at = dict(s.get("autotrader", {}))
        for key, value in defaults.items():
            if key not in at:
                at[key] = value
        s["autotrader"] = at
        result.append(s)
    return result


def dedup_across_sources(listings: list) -> list:
    """
    Remove cross-source duplicates by matching on (price, mileage, title prefix).
    AutoTrader listings take precedence (richer data) over CarGurus.
    """
    at_listings  = [l for l in listings if l.source == "autotrader"]
    cg_listings  = [l for l in listings if l.source == "cargurus"]

    at_keys = set()
    for l in at_listings:
        key = (l.price, l.mileage, l.title[:20].lower())
        at_keys.add(key)

    unique_cg = []
    for l in cg_listings:
        key = (l.price, l.mileage, l.title[:20].lower())
        if key not in at_keys:
            unique_cg.append(l)

    dupes = len(cg_listings) - len(unique_cg)
    if dupes:
        logging.getLogger("main").info(
            f"Removed {dupes} cross-source duplicates"
        )
    return at_listings + unique_cg


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main():
    parser = argparse.ArgumentParser(description="EV car alert scraper")
    parser.add_argument("--config",
        default=Path(__file__).parent / "config.yaml")
    parser.add_argument("--dry-run", action="store_true",
        help="Scrape and save but do not send email")
    parser.add_argument("--force-email", action="store_true",
        help="Send email even if no new listings")
    parser.add_argument("--test-email", action="store_true",
        help="Send a test email immediately without scraping")
    parser.add_argument("--autotrader-only", action="store_true",
        help="Skip CarGurus scraper (faster, less comprehensive)")
    args = parser.parse_args()

    config = load_config(args.config)

    log_path = config.get("output", {}).get(
        "log_path", "/opt/ev-scraper/logs/scraper.log"
    )
    setup_logging(log_path)
    logger = logging.getLogger("main")

    logger.info("=" * 60)
    logger.info(f"EV Scraper run started at {datetime.now().isoformat()}")
    logger.info("=" * 60)

    # --- Test email mode ---
    if args.test_email:
        logger.info("Test email mode — sending immediately")
        db_path = config.get("database", {}).get(
            "path", "/opt/ev-scraper/data/listings.db"
        )
        db    = ListingDatabase(db_path)
        stats = db.get_stats()
        ok = send_email(config, [], [], db.get_all_active(), stats,
                        subject_override="🚗 EV Scraper — Test Email")
        logger.info("Test email sent" if ok else "Test email FAILED — check SMTP config")
        return

    # --- Init DB ---
    db_path = config.get("database", {}).get(
        "path", "/opt/ev-scraper/data/listings.db"
    )
    db     = ListingDatabase(db_path)
    run_id = db.mark_run_start()

    searches = config.get("searches", [])
    enabled  = [s for s in searches if s.get("enabled", True)]
    enabled  = apply_defaults(enabled, config.get("defaults", {}))
    logger.info(
        f"{len(enabled)} enabled searches "
        f"({len(searches) - len(enabled)} disabled)"
    )

    all_scraped = []

    # Save each search's results to DB immediately as it completes.
    # This means a crash or restart mid-run won't lose already-scraped data —
    # unsent listings from a previous partial run are included in the next email.
    def _save_partial(listings: list):
        db.process_listings(listings)

    # --- AutoTrader ---
    logger.info("--- AutoTrader ---")
    at_results = []
    try:
        at_results = await AutoTraderScraper(config).scrape_all(
            enabled, on_search_done=_save_partial
        )
        all_scraped.extend(at_results)
        logger.info(f"AutoTrader total: {len(at_results)} listings")
    except Exception as exc:
        logger.error(f"AutoTrader scraper crashed: {exc}", exc_info=True)

    # --- CarGurus (optional) ---
    if not args.autotrader_only:
        logger.info("--- CarGurus ---")
        try:
            cg_results = await CarGurusScraper(config).scrape_all(enabled)
            all_scraped.extend(cg_results)
            # Cross-source dedup: only save CG listings that aren't already
            # represented by an AutoTrader entry (same price+mileage+title prefix)
            unique_cg = [
                l for l in dedup_across_sources(at_results + cg_results)
                if l.source == "cargurus"
            ]
            if unique_cg:
                db.process_listings(unique_cg)
            logger.info(
                f"CarGurus total: {len(cg_results)} listings "
                f"({len(unique_cg)} unique after cross-source dedup)"
            )
        except Exception as exc:
            logger.error(f"CarGurus scraper crashed: {exc}", exc_info=True)

    stats = db.get_stats()

    # --- Gather unsent listings (includes leftovers from any previous crashed run) ---
    unsent = db.get_unsent_listings()
    new_listings     = [l for l in unsent if l.get("is_new")]
    updated_listings = [l for l in unsent if not l.get("is_new")]

    db.mark_run_end(run_id, len(all_scraped), len(new_listings))

    logger.info(
        f"Unsent: {len(new_listings)} new | {len(updated_listings)} price changes | "
        f"Total in DB: {stats['total_in_db']}"
    )

    # --- JSON export ---
    json_path = config.get("output", {}).get(
        "json_path", "/opt/ev-scraper/data/latest_results.json"
    )
    Path(json_path).parent.mkdir(parents=True, exist_ok=True)
    export = {
        "generated_at": datetime.utcnow().isoformat(),
        "run_stats": {
            "total_scraped":   len(all_scraped),
            "new_listings":    len(new_listings),
            "price_changes":   len(updated_listings),
            "total_in_db":     stats["total_in_db"],
        },
        "new_listings":         new_listings,
        "price_changes":        updated_listings,
        "all_current_listings": db.get_all_active(),
    }
    with open(json_path, "w") as f:
        json.dump(export, f, indent=2, default=str)
    logger.info(f"JSON written → {json_path}")

    # --- Email ---
    if args.dry_run:
        logger.info(f"Dry run — email skipped ({len(unsent)} unsent listings queued)")
    elif unsent or args.force_email:
        ok = send_email(config, new_listings, updated_listings,
                        db.get_all_active(), stats)
        if ok:
            db.mark_as_sent([l["listing_id"] for l in unsent])
            logger.info(f"Email sent — {len(unsent)} listings marked as sent and data stripped")
    else:
        logger.info("Nothing new — email skipped")

    logger.info(f"Run complete at {datetime.now().isoformat()}")
    logger.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())