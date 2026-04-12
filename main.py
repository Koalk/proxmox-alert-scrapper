#!/usr/bin/env python3
"""
main.py — orchestrator

RUN FLOW:
  load_config → apply_defaults → get known_ids from DB
    → AutoTraderScraper.scrape_all()   (on_search_done=_save_partial)
    → MotorsScraper.scrape_all()       (results saved via _save_partial too)
    → CarGurusScraper.scrape_all()     (results saved via _save_partial too)
    → dedup_across_sources()           AT wins; dedup key=(year, price, mileage)
    → db.get_unsent_listings()         new + price-changed rows where email_sent=0
    → send_email()                     marks rows sent + strips raw data
    → JSON export to output.json_path

KEY HELPERS:
  apply_defaults()   merges config.defaults into each search.autotrader block.
  _save_partial()    callback — writes each search's results to DB immediately
                     so a mid-run crash doesn't lose already-scraped data.
  get_searches_with_recent_unsent()  crash-resume: skips re-scraping searches
                     that already have queued unsent data in the DB.
  check_for_update() non-fatal git fetch; result shown in email header.

CLI:
  --dry-run       scrape + save, no email
  --quick         1 listing/search, no email (sanity check)
  --force-email   send even if nothing new
  --test-email    send immediately, no scraping
  --skip-motors   skip Motors.co.uk
  --reset-db      wipe the entire DB and exit (all history lost, next run is fresh)
  --mark-unsent   reset email_sent=0 on all rows so they re-appear in next email
  --config PATH   alternate config file
"""

import argparse
import asyncio
import json
import logging
import subprocess
import sys
from logging.handlers import RotatingFileHandler
from datetime import datetime
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).parent))

from scraper.autotrader import AutoTraderScraper
from scraper.motors     import MotorsScraper
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
            RotatingFileHandler(
                log_path,
                maxBytes=5 * 1024 * 1024,  # 5 MB per file
                backupCount=3,             # keep .log + .log.1 + .log.2 + .log.3 = 20 MB max
            ),
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


def check_for_update(install_dir: str) -> dict | None:
    """
    Fetch remote refs and compare with local HEAD.
    Returns a dict with 'local', 'remote', and 'behind' count if behind,
    or None if the check fails or the repo is up to date.
    Runs as a subprocess so a git failure never crashes the scraper.
    """
    logger = logging.getLogger("main")
    try:
        repo = Path(install_dir)
        if not (repo / ".git").exists():
            return None
        subprocess.run(
            ["git", "-C", str(repo), "fetch", "--quiet"],
            timeout=15, capture_output=True,
        )
        local = subprocess.run(
            ["git", "-C", str(repo), "rev-parse", "HEAD"],
            capture_output=True, text=True, timeout=5,
        ).stdout.strip()
        remote = subprocess.run(
            ["git", "-C", str(repo), "rev-parse", "@{u}"],
            capture_output=True, text=True, timeout=5,
        ).stdout.strip()
        if not local or not remote or local == remote:
            return None
        behind = int(subprocess.run(
            ["git", "-C", str(repo), "rev-list", "--count", f"{local}..{remote}"],
            capture_output=True, text=True, timeout=5,
        ).stdout.strip() or 0)
        if behind > 0:
            logger.info(f"Update available: {behind} new commit(s) on remote")
            return {"local": local[:8], "remote": remote[:8], "behind": behind}
    except Exception as exc:
        logger.debug(f"Update check failed (non-fatal): {exc}")
    return None



def post_to_lurch(export: dict, lurch_url: str = "http://192.168.194.148:8766") -> bool:
    """
    POST the scraper results JSON to Lurch's dashboard endpoint.
    Returns True if Lurch accepted it, False on any failure.
    Lurch will handle LLM filtering and Telegram notification.
    Email is skipped if this returns True.
    """
    import httpx as _httpx
    logger = logging.getLogger("main")
    try:
        resp = _httpx.post(
            f"{lurch_url}/api/ev-results",
            json=export,
            timeout=10,
        )
        resp.raise_for_status()
        task_id = resp.json().get("task_id", "?")
        logger.info(f"Results handed off to Lurch — task_id={task_id}")
        return True
    except Exception as e:
        logger.warning(f"Lurch handoff failed ({e}) — falling back to direct email")
        return False

def dedup_across_sources(listings: list) -> list:
    """
    Remove cross-source duplicates by matching on (year, price, mileage).
    AutoTrader listings take precedence (richer data) over all other sources.
    Title is intentionally excluded: AT titles ("2022 White 2022 Kia EV6...") and
    CG/Motors titles ("2022 Kia EV6") differ too much to be a reliable key.
    """
    at_listings    = [l for l in listings if l.source == "autotrader"]
    other_listings = [l for l in listings if l.source != "autotrader"]

    at_keys: set = set()
    for l in at_listings:
        # Only use as dedup key when both price and mileage are known —
        # (None, None) would falsely match unrelated listings for the same model
        if l.price is not None and l.mileage is not None:
            key = (l.year, l.price, l.mileage)
            at_keys.add(key)

    unique_other = []
    for l in other_listings:
        if l.price is not None and l.mileage is not None:
            key = (l.year, l.price, l.mileage)
            if key in at_keys:
                continue
        unique_other.append(l)

    dupes = len(other_listings) - len(unique_other)
    if dupes:
        logging.getLogger("main").info(
            f"Removed {dupes} cross-source duplicate(s)"
        )
    return at_listings + unique_other


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main():
    parser = argparse.ArgumentParser(description="EV car alert scraper")
    parser.add_argument("--config",
        default=Path(__file__).parent / "config.yaml")
    parser.add_argument("--dry-run", action="store_true",
        help="Scrape and save but do not send email")
    parser.add_argument("--quick", action="store_true",
        help="1 page, 1 listing per search — fast sanity check that scraping works")
    parser.add_argument("--force-email", action="store_true",
        help="Send email even if no new listings")
    parser.add_argument("--test-email", action="store_true",
        help="Send a test email immediately without scraping")
    parser.add_argument("--skip-motors", action="store_true",
        help="Skip Motors.co.uk scraper")
    parser.add_argument("--skip-cargurus", action="store_true",
        help="Skip CarGurus scraper")
    parser.add_argument("--reset-db", action="store_true",
        help="Wipe the entire database and exit — all history lost, next run starts fresh")
    parser.add_argument("--mark-unsent", action="store_true",
        help="Mark all DB listings as unsent so they appear in the next email, then exit")
    args = parser.parse_args()

    config = load_config(args.config)

    db_path = config.get("database", {}).get("path", "/opt/ev-scraper/data/listings.db")

    if args.reset_db:
        setup_logging(config.get("output", {}).get("log_path", "/opt/ev-scraper/logs/scraper.log"))
        logger = logging.getLogger("main")
        ListingDatabase(db_path).reset()
        logger.info("Database wiped — all history cleared. Next run starts fresh.")
        return

    if args.mark_unsent:
        setup_logging(config.get("output", {}).get("log_path", "/opt/ev-scraper/logs/scraper.log"))
        logger = logging.getLogger("main")
        db = ListingDatabase(db_path)
        db.mark_all_unsent()
        stats = db.get_stats()
        logger.info(f"All {stats['total_in_db']} listings marked as unsent — they will appear in the next email.")
        return

    if args.quick:
        config.setdefault("limits", {})
        config["limits"]["max_pages_per_search"]    = 1
        config["limits"]["max_scrapes_per_search"]  = 1  # visit 1 URL per search max
        config["limits"]["max_listings_per_search"] = 1
        args.dry_run = True   # quick mode never sends email

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
        db    = ListingDatabase(db_path)
        stats = db.get_stats()
        ok = send_email(config, [], [], db.get_all_active(), stats,
                        subject_override="🚗 EV Scraper — Test Email")
        logger.info("Test email sent" if ok else "Test email FAILED — check SMTP config")
        return

    # --- Init DB ---
    db     = ListingDatabase(db_path)
    run_id = db.mark_run_start()

    # Collect known listing IDs so scrapers can skip pages where every result
    # is already in the DB (they'll try the next page instead).
    # Disabled in --quick mode: that's a sanity check, always scrape page 1.
    known_ids = None if args.quick else db.get_known_listing_ids()

    searches = config.get("searches", [])
    enabled  = [s for s in searches if s.get("enabled", True)]
    enabled  = apply_defaults(enabled, config.get("defaults", {}))
    logger.info(
        f"{len(enabled)} enabled searches "
        f"({len(searches) - len(enabled)} disabled)"
    )

    # --- Resume detection ---
    # If a previous run saved partial results but didn't finish (e.g. crashed
    # or timed out), skip searches that already have fresh unsent data in the DB
    # so we don't re-scrape them from scratch.
    already_done = db.get_searches_with_recent_unsent()
    if already_done:
        searches_to_run = [s for s in enabled if s["name"] not in already_done]
        logger.info(
            f"Resuming partial run — {len(already_done)} search(es) already "
            f"have unsent data, skipping: {sorted(already_done)}"
        )
        if searches_to_run:
            logger.info(
                f"Continuing with {len(searches_to_run)} remaining search(es): "
                f"{[s['name'] for s in searches_to_run]}"
            )
        else:
            logger.info(
                "All searches already complete — skipping scraping, "
                "proceeding to email"
            )
    else:
        searches_to_run = enabled

    all_scraped = []
    run_errors: list[str] = []

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
            searches_to_run, on_search_done=_save_partial, known_ids=known_ids,
            on_discarded=db.record_discarded,
        )
        all_scraped.extend(at_results)
        logger.info(f"AutoTrader total: {len(at_results)} listings")
    except Exception as exc:
        msg = f"AutoTrader scraper crashed: {exc}"
        logger.error(msg, exc_info=True)
        run_errors.append(msg)

    # --- Motors.co.uk (optional) ---
    if not args.skip_motors:
        logger.info("--- Motors.co.uk ---")
        try:
            mt_results = await MotorsScraper(config).scrape_all(
                searches_to_run, known_ids=known_ids
            )
            all_scraped.extend(mt_results)
            # dedup against AT listings
            unique_mt = [
                l for l in dedup_across_sources(at_results + mt_results)
                if l.source == "motors"
            ]
            if unique_mt:
                db.process_listings(unique_mt)
            logger.info(
                f"Motors total: {len(mt_results)} listings "
                f"({len(unique_mt)} unique after cross-source dedup)"
            )
        except Exception as exc:
            msg = f"Motors scraper crashed: {exc}"
            logger.error(msg, exc_info=True)
            run_errors.append(msg)

    # --- CarGurus (optional) ---
    if not args.skip_cargurus:
        logger.info("--- CarGurus ---")
        try:
            cg_results = await CarGurusScraper(config).scrape_all(
                searches_to_run, on_search_done=_save_partial, known_ids=known_ids
            )
            all_scraped.extend(cg_results)
            # dedup against AT + Motors listings
            unique_cg = [
                l for l in dedup_across_sources(at_results + cg_results)
                if l.source == "cargurus"
            ]
            # CG was already saved to DB via _save_partial callbacks, so
            # explicitly delete any duplicates that were just deduped away.
            unique_cg_ids = {l.listing_id for l in unique_cg}
            duped_cg_ids  = [
                l.listing_id for l in cg_results
                if l.listing_id not in unique_cg_ids
            ]
            if duped_cg_ids:
                db.delete_listings(duped_cg_ids)
            logger.info(
                f"CarGurus total: {len(cg_results)} listings "
                f"({len(unique_cg)} unique after cross-source dedup, "
                f"{len(duped_cg_ids)} duplicate(s) removed from DB)"
            )
        except Exception as exc:
            msg = f"CarGurus scraper crashed: {exc}"
            logger.error(msg, exc_info=True)
            run_errors.append(msg)

    stats = {}
    unsent = []
    new_listings = []
    updated_listings = []

    # --- Gather results and update DB ---
    logger.info("--- Post-scrape: collecting results ---")
    try:
        stats = db.get_stats()
        logger.info(f"DB stats: {stats['total_in_db']} total listings in database")
    except Exception as exc:
        msg = f"DB stats failed: {exc}"
        logger.error(msg, exc_info=True)
        run_errors.append(msg)
        stats = {"total_in_db": 0}

    try:
        unsent = db.get_unsent_listings()
        new_listings     = [l for l in unsent if l.get("is_new")]
        updated_listings = [l for l in unsent if not l.get("is_new")]
        logger.info(
            f"Unsent: {len(new_listings)} new | {len(updated_listings)} price changes | "
            f"Total in DB: {stats['total_in_db']}"
        )
    except Exception as exc:
        msg = f"Failed to read unsent listings from DB: {exc}"
        logger.error(msg, exc_info=True)
        run_errors.append(msg)

    try:
        db.mark_run_end(run_id, len(all_scraped), len(new_listings))
    except Exception as exc:
        logger.warning(f"mark_run_end failed (non-fatal): {exc}")

    # --- JSON export ---
    json_path = config.get("output", {}).get(
        "json_path", "/opt/ev-scraper/data/latest_results.json"
    )
    try:
        logger.info(f"Writing JSON export → {json_path}")
        Path(json_path).parent.mkdir(parents=True, exist_ok=True)
        export = {
            "generated_at": datetime.now().isoformat(),
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
    except Exception as exc:
        msg = f"JSON export failed: {exc}"
        logger.error(msg, exc_info=True)
        run_errors.append(msg)

    # --- Notify: Lurch first, email fallback ---
    # Try to hand off to Lurch (MediaCentre) for LLM filtering + Telegram summary.
    # Fall back to direct email only if Lurch is unreachable.
    if args.dry_run:
        logger.info(f"Dry run — skipping Lurch handoff and email ({len(unsent)} unsent listings queued)")
    else:
        lurch_ok = post_to_lurch(export)
        if lurch_ok:
            # Lurch accepted — mark as sent so listings don't pile up
            if unsent:
                db.mark_as_sent([l["listing_id"] for l in unsent])
                logger.info(f"Lurch accepted {len(unsent)} listing(s) — marked as sent")
        else:
            # Lurch down — fall back to direct email as before
            if not unsent and not args.force_email and not run_errors:
                logger.info("Nothing new and Lurch unavailable — email skipped")
            else:
                try:
                    all_active = db.get_all_active()
                except Exception as exc:
                    logger.warning(f"Could not fetch all_active for email (non-fatal): {exc}")
                    all_active = []
                try:
                    update_info = check_for_update(str(Path(args.config).parent))
                except Exception as exc:
                    logger.warning(f"Update check failed (non-fatal): {exc}")
                    update_info = None
                max_email_listings = config.get("limits", {}).get("max_email_listings", 20)
                logger.info(
                    f"Sending fallback email: {len(new_listings)} new, {len(updated_listings)} price changes"
                    + (f", {len(run_errors)} error(s)" if run_errors else "")
                )
                ok = send_email(config, new_listings, updated_listings,
                                all_active, stats, update_info=update_info,
                                run_errors=run_errors if run_errors else None,
                                max_email_listings=max_email_listings,
                                json_path=json_path)
                if ok:
                    if unsent:
                        db.mark_as_sent([l["listing_id"] for l in unsent])
                        logger.info(f"Fallback email sent — {len(unsent)} listings marked as sent")
                    else:
                        logger.info("Fallback email sent (errors-only or force)")
                else:
                    logger.error("Fallback email also FAILED — listings remain queued for next run")

    logger.info(f"Run complete at {datetime.now().isoformat()}")
    logger.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())