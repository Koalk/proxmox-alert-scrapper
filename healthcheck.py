#!/usr/bin/env python3
"""
healthcheck.py
Verifies the scraper installation is healthy and last run succeeded.
Designed to be run as a separate daily check (e.g. 30 mins after the
main scraper) — alerts by email if something went wrong.

Usage:
    python3 healthcheck.py --config /opt/ev-scraper/config.yaml
    python3 healthcheck.py --config config.yaml --alert-on-failure
"""

import argparse
import json
import logging
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path

import yaml

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("healthcheck")

PASS = "✅"
WARN = "⚠️ "
FAIL = "❌"


def check_db(db_path: str) -> tuple[bool, str]:
    if not Path(db_path).exists():
        return False, f"{FAIL} Database not found at {db_path}"
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        run = conn.execute(
            "SELECT * FROM run_log ORDER BY run_id DESC LIMIT 1"
        ).fetchone()
        if not run:
            return False, f"{WARN} No runs recorded in database yet"
        last_run = datetime.fromisoformat(run["started_at"])
        age = datetime.utcnow() - last_run
        total = conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
        conn.close()
        if age > timedelta(hours=36):
            return False, (
                f"{FAIL} Last run was {age.total_seconds()/3600:.1f}h ago "
                f"(expected < 36h). Total listings: {total}"
            )
        return True, (
            f"{PASS} Last run: {last_run.strftime('%Y-%m-%d %H:%M')} UTC "
            f"({age.total_seconds()/3600:.1f}h ago) | "
            f"Found: {run['total_found'] or 0} | "
            f"New: {run['new_count'] or 0} | "
            f"Total in DB: {total}"
        )
    except Exception as exc:
        return False, f"{FAIL} DB error: {exc}"


def check_json(json_path: str) -> tuple[bool, str]:
    if not Path(json_path).exists():
        return False, f"{WARN} JSON output not found at {json_path} (first run?)"
    try:
        with open(json_path) as f:
            data = json.load(f)
        ts = data.get("generated_at", "")
        if ts:
            age = datetime.utcnow() - datetime.fromisoformat(ts)
            if age > timedelta(hours=36):
                return False, (
                    f"{FAIL} JSON is {age.total_seconds()/3600:.1f}h old — "
                    f"scraper may not have run"
                )
        total = len(data.get("all_current_listings", []))
        return True, f"{PASS} JSON OK — {total} listings, generated {ts}"
    except Exception as exc:
        return False, f"{FAIL} JSON parse error: {exc}"


def check_log(log_path: str) -> tuple[bool, str]:
    if not Path(log_path).exists():
        return False, f"{WARN} Log file not found at {log_path}"
    try:
        with open(log_path) as f:
            lines = f.readlines()
        # Look at last 200 lines for errors
        recent = lines[-200:]
        errors = [l.strip() for l in recent if "[ERROR]" in l]
        crashes = [l.strip() for l in recent if "Traceback" in l or "crashed" in l]
        if crashes:
            return False, (
                f"{FAIL} Crash detected in recent logs. "
                f"Last error: {errors[-1] if errors else 'see log'}"
            )
        if len(errors) > 5:
            return False, (
                f"{WARN} {len(errors)} errors in recent log. "
                f"Last: {errors[-1]}"
            )
        return True, (
            f"{PASS} Log OK — {len(errors)} errors in recent 200 lines"
        )
    except Exception as exc:
        return False, f"{WARN} Could not read log: {exc}"


def check_disk(paths: list) -> tuple[bool, str]:
    import shutil
    results = []
    ok = True
    for p in paths:
        parent = Path(p).parent
        if parent.exists():
            usage = shutil.disk_usage(parent)
            free_gb = usage.free / (1024 ** 3)
            pct_used = (usage.used / usage.total) * 100
            if free_gb < 0.5:
                ok = False
                results.append(
                    f"{FAIL} Low disk space: {free_gb:.2f}GB free ({pct_used:.0f}% used)"
                )
            else:
                results.append(
                    f"{PASS} Disk: {free_gb:.1f}GB free ({pct_used:.0f}% used)"
                )
    return ok, " | ".join(results) if results else f"{PASS} Disk OK"


def send_alert_email(config: dict, issues: list):
    """Send a failure alert email."""
    from scraper.emailer import send_email
    # Build a fake minimal listing set and pass issues as a message
    stats = {"total_in_db": "?"}
    # We'll abuse the email system slightly — inject issues into subject
    email_cfg = config.get("email", {})
    import smtplib, ssl
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart

    subject = f"🚨 EV Scraper Health Alert — {len(issues)} issue(s) detected"
    body = "<h2>EV Scraper Health Check Failed</h2><ul>"
    for issue in issues:
        body += f"<li>{issue}</li>"
    body += "</ul>"
    body += f"<p>Check: <code>{config.get('output',{}).get('log_path','')}</code></p>"

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = email_cfg.get("smtp_user", "")
    msg["To"]      = email_cfg.get("to", "")
    msg.attach(MIMEText(body, "html"))

    try:
        ctx = ssl.create_default_context()
        with smtplib.SMTP(email_cfg["smtp_host"], int(email_cfg["smtp_port"])) as s:
            s.starttls(context=ctx)
            s.login(email_cfg["smtp_user"], email_cfg["smtp_password"])
            s.sendmail(email_cfg["smtp_user"], email_cfg["to"], msg.as_string())
        logger.info(f"Alert email sent to {email_cfg['to']}")
    except Exception as exc:
        logger.error(f"Could not send alert email: {exc}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config",
        default=Path(__file__).parent / "config.yaml"
    )
    parser.add_argument(
        "--alert-on-failure",
        action="store_true",
        help="Send email if any check fails"
    )
    args = parser.parse_args()

    with open(args.config) as f:
        config = yaml.safe_load(f)

    db_path   = config.get("database", {}).get("path", "/opt/ev-scraper/data/listings.db")
    json_path = config.get("output", {}).get("json_path", "/opt/ev-scraper/data/latest_results.json")
    log_path  = config.get("output", {}).get("log_path", "/opt/ev-scraper/logs/scraper.log")

    checks = [
        check_db(db_path),
        check_json(json_path),
        check_log(log_path),
        check_disk([db_path, json_path, log_path]),
    ]

    print("\n" + "="*60)
    print("EV Scraper Health Check")
    print("="*60)
    issues = []
    for ok, msg in checks:
        print(f"  {msg}")
        if not ok:
            issues.append(msg)
    print("="*60)

    if issues:
        print(f"\n{FAIL} {len(issues)} issue(s) found")
        if args.alert_on_failure:
            send_alert_email(config, issues)
        sys.exit(1)
    else:
        print(f"\n{PASS} All checks passed")
        sys.exit(0)


if __name__ == "__main__":
    main()