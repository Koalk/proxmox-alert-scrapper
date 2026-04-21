"""
scraper/emailer.py — HTML email digest builder and sender

ENTRY POINT:
  send_email(config, new_listings, updated_listings, all_active, stats,
             update_info=None, run_errors=None, max_email_listings=20)
  All listing args are plain dicts (as returned by database.get_unsent_listings).

STRUCTURE:
  _car_card(listing, badge)  → HTML string for one car card (inline styles,
                                images, price, mileage, flags, link).
  send_email()               → builds full HTML, connects via SMTP STARTTLS,
                                honours max_email_listings (excess → JSON only).

KEY GOTCHAS:
  - All CSS is inline — email clients strip <style> blocks.
  - Images embedded as <img src="url"> (hotlinked, not attached) to keep
    message size small.
  - subject_prefix comes from config.email.subject_prefix.
  - If run_errors is non-empty a red error banner is prepended to the email.
"""

import json
import logging
import smtplib
import ssl
from datetime import datetime
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Candidate reference info — shown as a collapsible spoiler on each card.
# Keys are matched against listing search_name (case-insensitive substring).
# ---------------------------------------------------------------------------
_CANDIDATE_INFO: dict[str, dict] = {
    "Skoda Enyaq iV 80": {
        "status": "ACTIVE",
        "blurb": "Primary pick. Biggest boot of the mid-size SUV set (585L), flat floor with rubber mat, strong Scottish supply.",
        "gotcha": "Two HV battery recalls (93Q3 and 94R6) — confirm both closed at VIN level before buying, and check software is ME3.1 or later.",
    },
    "Tesla Model Y": {
        "status": "ACTIVE",
        "blurb": "The only car on this list that actually beats the Touran's boot (854L + 117L frunk). Supercharger network is a genuine advantage for Scotland.",
        "gotcha": "The tailgate opening is saloon-shaped rather than a proper estate mouth — measure it against your dog crate before committing.",
    },
    "Hyundai Ioniq 5": {
        "status": "ACTIVE",
        "blurb": "Big battery only (77.4kWh). Good boot (527L) plus a useful 57L frunk for cables, and the sliding rear bench can reshape around a crate.",
        "gotcha": "ICCU failure is a known issue across the Hyundai/Kia platform — confirm recall campaign 272 is closed at VIN level.",
    },
    "VW ID.4": {
        "status": "ACTIVE",
        "blurb": "Direct Enyaq cousin, 543L boot, strong supply.",
        "gotcha": "Heat pump was a cost option and many used cars don't have it — check per listing, it matters for Scottish winters. Also confirm software recall 919A closed.",
    },
    "BMW iX3": {
        "status": "ACTIVE",
        "blurb": "Premium option, 510L boot with a low load lip and power tailgate — arguably the easiest of the set to load dogs into. Fewer recall headaches than the others.",
        "gotcha": "Real-world range is only around 200–220 miles as it's a converted combustion platform. Make sure you're looking at the G08 generation (2021–2024), not the all-new Neue Klasse iX3 (late 2025+), which is a completely different £58k car.",
    },
    "Kia Niro EV": {
        "status": "ACTIVE",
        "blurb": "Second-gen (2022+) only. Square load bay, flat floor, 475L + 20L frunk, well-regarded by dog owners. Good value.",
        "gotcha": "Heat pump was optional on early 2022 cars — verify per spec.",
    },
    "Kia EV6": {
        "status": "ACTIVE",
        "blurb": "Sporty and capable, but ranked last for dog-loading on this list — the tailgate opening is narrow, the load lip is high, and the boot shape is more saloon than SUV.",
        "gotcha": "Same ICCU recall risk as the Ioniq 5 (campaigns SC327 and SA533A). Worth having on the list but view it last.",
    },
    "Renault Scenic": {
        "status": "ACTIVE",
        "blurb": "European Car of the Year 2024, 545L boot, heat pump standard, real-world range around 280 miles. Genuinely good car.",
        "gotcha": "New enough that there's no long reliability track record yet — keep mileage tight (under 25k) to reduce risk on an unproven used example.",
    },
    "MG5": {
        "status": "ACTIVE",
        "blurb": "The only electric estate on the UK used market, and the best boot shape of the lot for dogs (proper estate mouth, low lip, 464–578L seats up). Very affordable.",
        "gotcha": "MG finished bottom of the WhatCar 2025 reliability survey, so factor in the possibility of higher running costs.",
    },
    "Kia EV3": {
        "status": "WATCH",
        "blurb": "Sensible smaller SUV, but the Long Range (81.4kWh) is sitting at £26–28k today. Alert is set to £23k so it'll fire when the first examples drop into budget, likely late 2026.",
        "gotcha": None,
    },
    "BYD Sealion 7": {
        "status": "WATCH",
        "blurb": "520L boot plus 58L frunk, good value on paper. UK has no Chinese EV tariffs (unlike the EU's 17% on BYD), so used prices are falling faster here. Floor is around £31k today — realistically £23k by late 2027.",
        "gotcha": "BYD ranked 30th out of 31 brands in Driver Power 2025, so reliability is a real question mark. The 6-year/8-year warranty does a lot of work.",
    },
    "Citroen e-C3 Aircross": {
        "status": "WATCH",
        "blurb": "Launched late 2024, barely any used stock nationally. The 7-seat version is interesting as a Touran-ish replacement.",
        "gotcha": "5-seat boot is only 460L and the third-row version drops to 330L behind it, so it's not actually that practical with all seats in use.",
    },
    "Citroen e-Berlingo": {
        "status": "WATCH",
        "blurb": "Probably the closest functional Touran replacement that exists today: sliding rear doors, 775L+ boot, available as a 7-seater.",
        "gotcha": "The battery is only 50kWh, giving around 120 real-world miles — a dealbreaker for anything beyond Edinburgh city use. On watch in case a bigger-battery facelift arrives (rumoured for 2026–27).",
    },
    "Kia EV5": {
        "status": "WATCH",
        "blurb": "566L boot plus 44L frunk, genuinely practical. Floor is around £36k today. On watch to catch the first examples when they eventually drop to £23k, probably 2027–28.",
        "gotcha": None,
    },
    "Skoda Epiq": {
        "status": "WATCH",
        "blurb": "Not on UK sale yet (deliveries expected Q1 2027). Revisit 2028–29 for used examples.",
        "gotcha": None,
    },
    "Kia PV5": {
        "status": "WATCH",
        "blurb": "The true spiritual Touran successor: sliding doors, flat floor, 1,320L boot, even has a dedicated Dog Mode. First UK deliveries early 2026 at £30k+. Budget-realistic around 2028.",
        "gotcha": None,
    },
}


def _candidate_info_block(search_name: str) -> str:
    """Return a collapsible <details> block with candidate notes for this model.

    Matches by checking if any key from _CANDIDATE_INFO appears as a
    case-insensitive substring of search_name.  Returns "" if no match.
    """
    search_lower = search_name.lower()
    info = None
    for key, val in _CANDIDATE_INFO.items():
        if key.lower() in search_lower:
            info = val
            break
    if info is None:
        return ""

    status_color = "#1a6b3a" if info["status"] == "ACTIVE" else "#7c5a00"
    status_bg    = "#d4edda" if info["status"] == "ACTIVE" else "#fff3cd"
    status_border= "#28a745" if info["status"] == "ACTIVE" else "#ffc107"

    gotcha_html = (
        f'<p style="margin:6px 0 0;font-size:12px;color:#7b3f00;">'
        f'<strong>⚠️ Gotcha:</strong> {info["gotcha"]}</p>'
    ) if info.get("gotcha") else ""

    return f"""
    <details style="margin-top:10px;">
      <summary style="cursor:pointer;font-size:12px;color:#555;
                      padding:4px 0;list-style:none;-webkit-appearance:none;">
        <span style="text-decoration:underline;text-decoration-style:dotted;">
          📋 About this model
        </span>
        &nbsp;<span style="background:{status_bg};border:1px solid {status_border};
          color:{status_color};border-radius:8px;padding:1px 7px;
          font-size:11px;font-weight:bold;">{info["status"]}</span>
      </summary>
      <div style="margin-top:8px;padding:10px 12px;background:#f8f9fa;
                  border-left:3px solid {status_border};border-radius:0 6px 6px 0;
                  font-size:12px;color:#444;">
        <p style="margin:0 0 4px;">{info["blurb"]}</p>
        {gotcha_html}
      </div>
    </details>
    """


def _car_card(listing: dict, badge: str = "", ai_review: dict | None = None) -> str:
    """Render one car listing as an HTML card.

    ai_review: optional dict with keys 'action' ('approved'|'flagged') and 'reason' (str).
    """
    images_html = ""
    for img_url in (listing.get("image_urls") or [])[:3]:
        images_html += (
            f'<img src="{img_url}" style="width:200px;height:130px;'
            f'object-fit:cover;border-radius:6px;margin:3px;" '
            f'alt="car photo">'
        )

    price_str = (
        f"£{listing['price']:,}" if listing.get("price") else "POA"
    )
    mileage_str = (
        f"{listing['mileage']:,} miles" if listing.get("mileage") else "—"
    )
    year_str = str(listing.get("year") or "—")
    dist_str = (
        f"{listing['distance_miles']} miles away"
        if listing.get("distance_miles")
        else listing.get("location", "")
    )
    attention = listing.get("attention_check", "")
    flags_html = ""
    if attention:
        for flag in attention.split(" | "):
            if flag.strip():
                flags_html += (
                    f'<span style="display:inline-block;background:#f0f4ff;'
                    f'border:1px solid #c5d0f5;border-radius:12px;padding:2px 8px;'
                    f'font-size:12px;margin:2px;">{flag.strip()}</span>'
                )

    badge_html = ""
    if badge:
        color = "#d4edda" if "New" in badge else "#fff3cd"
        border = "#28a745" if "New" in badge else "#ffc107"
        badge_html = (
            f'<span style="background:{color};border:1px solid {border};'
            f'color:#333;border-radius:10px;padding:2px 10px;'
            f'font-size:12px;font-weight:bold;">{badge}</span> '
        )

    ai_badge_html = ""
    if ai_review:
        action = ai_review.get("action", "")
        reason = ai_review.get("reason", "")
        if action == "approved":
            ai_badge_html = (
                f'<div style="margin-top:8px;padding:6px 10px;'
                f'background:#eaf7ec;border-left:3px solid #28a745;'
                f'border-radius:4px;font-size:12px;color:#155724;">'
                f'✅ <strong>AI Pick:</strong> {reason}</div>'
            )
        elif action == "flagged":
            ai_badge_html = (
                f'<div style="margin-top:8px;padding:6px 10px;'
                f'background:#fff8e1;border-left:3px solid #ffc107;'
                f'border-radius:4px;font-size:12px;color:#856404;">'
                f'⚠️ <strong>AI Flag:</strong> {reason}</div>'
            )

    seller_str = listing.get("seller_name", "") or listing.get("seller_type", "")
    spec = listing.get("spec_summary", "")[:120]
    source_labels = {
        "autotrader": "AutoTrader",
        "cargurus": "CarGurus",
        "motors": "Motors.co.uk",
    }
    source_label = source_labels.get(listing.get("source", ""), "AutoTrader")

    return f"""
    <div style="border:1px solid #ddd;border-radius:10px;padding:16px;
                margin-bottom:16px;background:#fafafa;max-width:700px;">
      <div style="margin-bottom:8px;">
        {badge_html}
        <span style="font-size:11px;color:#888;">
          {listing.get('search_name','')}
        </span>
      </div>
      <h3 style="margin:0 0 4px;font-size:16px;">
        <a href="{listing['url']}" style="color:#1a4fa3;text-decoration:none;">
          {listing.get('title','')}
        </a>
      </h3>
      <div style="font-size:22px;font-weight:bold;color:#222;margin:4px 0;">
        {price_str}
        <span style="font-size:14px;font-weight:normal;color:#666;margin-left:8px;">
          {year_str} &bull; {mileage_str} &bull; {dist_str}
        </span>
      </div>
      {f'<div style="color:#555;font-size:13px;margin:4px 0;">{spec}</div>' if spec else ''}
      {f'<div style="color:#777;font-size:12px;margin:4px 0;">Seller: {seller_str}</div>' if seller_str else ''}
      <div style="margin:8px 0;">{images_html}</div>
      <div style="margin-top:8px;">{flags_html}</div>
      {ai_badge_html}
      {_candidate_info_block(listing.get('search_name', ''))}
      <div style="margin-top:10px;">
        <a href="{listing['url']}"
           style="background:#1a4fa3;color:white;padding:8px 18px;
                  border-radius:6px;text-decoration:none;font-size:13px;">
          View on {source_label} →
        </a>
      </div>
    </div>
    """


def _update_banner(update_info: dict) -> str:
    behind = update_info["behind"]
    local  = update_info["local"]
    remote = update_info["remote"]
    commit_word = "commit" if behind == 1 else "commits"
    update_cmd = (
        "pct exec 300 -- su -s /bin/bash evscraper -c "
        "&quot;cd /opt/ev-scraper &amp;&amp; git pull &amp;&amp; "
        "venv/bin/pip install -q -r requirements.txt&quot;"
    )
    return f"""
    <div style="background:#fff8e1;border:1px solid #ffe082;border-radius:8px;
                padding:14px 18px;margin:20px 0;">
      <strong style="color:#f57c00;">⬆️ Update available</strong>
      <p style="margin:6px 0 10px;color:#555;font-size:13px;">
        Your scraper is <strong>{behind} {commit_word}</strong> behind the remote
        (<code>{local}</code> → <code>{remote}</code>).
        Run this on your Proxmox host to update:
      </p>
      <code style="display:block;background:#fff3e0;border:1px solid #ffcc80;
                   border-radius:4px;padding:8px 12px;font-size:12px;
                   word-break:break-all;color:#333;">{update_cmd}</code>
    </div>
    """


def _error_banner(errors: list[str]) -> str:
    items = "".join(f"<li style='margin:4px 0;'>{e}</li>" for e in errors)
    return f"""
    <div style="background:#fff0f0;border:1px solid #f5c6cb;border-radius:8px;
                padding:14px 18px;margin:20px 0;">
      <strong style="color:#c0392b;">⚠️ Run completed with errors</strong>
      <p style="margin:6px 0 4px;color:#555;font-size:13px;">
        The scraper encountered the following problems during this run.
        Partial results may be incomplete.
      </p>
      <ul style="margin:4px 0;padding-left:18px;color:#555;font-size:13px;">
        {items}
      </ul>
    </div>
    """


def _ai_summary_banner(annotations: dict) -> str:
    """Render a compact AI review summary banner."""
    approved = [v for v in annotations.values() if isinstance(v, dict) and v.get("action") == "approved"]
    flagged  = [v for v in annotations.values() if isinstance(v, dict) and v.get("action") == "flagged"]
    verdict  = annotations.get("_verdict", "")
    parts = []
    if approved:
        parts.append(f"{len(approved)} recommended")
    if flagged:
        parts.append(f"{len(flagged)} flagged")
    summary_line = " &bull; ".join(parts) if parts else "No listings reviewed"
    verdict_html = f'<p style="margin:4px 0 0;font-size:13px;color:#555;">{verdict}</p>' if verdict else ""
    return f"""
    <div style="background:#f0f7ff;border:1px solid #b8d4f8;border-radius:8px;
                padding:14px 18px;margin:20px 0;">
      <strong style="color:#1a4fa3;">🤖 AI Review</strong>
      <span style="font-size:13px;color:#555;margin-left:8px;">{summary_line}</span>
      {verdict_html}
    </div>
    """


def build_html_email(
    new_listings: list,
    updated_listings: list,
    all_listings: list,
    stats: dict,
    run_date: str,
    update_info: dict | None = None,
    run_errors: list[str] | None = None,
    max_email_listings: int = 20,
    annotations: dict | None = None,
) -> str:
    """Build the full HTML email body.

    annotations: optional dict keyed by listing_id → {action, reason}.
    A special '_verdict' key may hold the AI's overall verdict string.
    Listings with action=='flagged' are sorted to the bottom of the email.
    """

    # Combine new and price-changed, sort by price ascending (None/POA last)
    ann = annotations or {}

    def _ai(l):
        return ann.get(l.get("listing_id")) or ann.get(str(l.get("listing_id")))

    all_unsent = new_listings + updated_listings

    def _sort_key(l):
        return (
            (l.get("price") or 0) == 0,   # POA / None sorts last
            l.get("price") or 0,
        )

    approved_listings = sorted(
        [l for l in all_unsent if (ann.get(l.get("listing_id")) or ann.get(str(l.get("listing_id"))or"") or {}).get("action") == "approved"],
        key=_sort_key,
    )
    unreviewed_listings = sorted(
        [l for l in all_unsent if not ann.get(l.get("listing_id")) and not ann.get(str(l.get("listing_id") or ""))],
        key=_sort_key,
    )
    flagged_listings = [
        l for l in all_unsent
        if (ann.get(l.get("listing_id")) or ann.get(str(l.get("listing_id") or "")) or {}).get("action") == "flagged"
    ]

    # Approved first, then unreviewed; flagged excluded from main list
    priority_listings = approved_listings + unreviewed_listings
    to_show       = priority_listings[:max_email_listings]
    total_omitted = len(priority_listings) - len(to_show)

    def _badge(l):
        return "🆕 New" if l.get("is_new") else "💲 Price Changed"

    listings_section = ""
    if to_show:
        cards = "".join(_car_card(l, _badge(l), ai_review=_ai(l)) for l in to_show)
        flagged_note = (
            f'<p style="color:#856404;font-size:12px;margin-top:8px;">'
            f'⚠️ {len(flagged_listings)} listing(s) flagged by AI and hidden from this view.</p>'
        ) if flagged_listings else ""
        listings_section = f"""
        <h2 style="color:#155724;border-bottom:2px solid #28a745;padding-bottom:6px;">
          Listings ({len(approved_listings)} ✅ approved · {len(unreviewed_listings)} unreviewed · {len(flagged_listings)} 🚩 hidden)
        </h2>
        {flagged_note}
        {cards}
        """
    else:
        listings_section = """
        <h2 style="color:#155724;border-bottom:2px solid #28a745;padding-bottom:6px;">
          Listings
        </h2>
        <p style="color:#666;">No new listings since last run.</p>
        """

    overflow_note = ""
    if total_omitted > 0:
        overflow_note = f"""
    <div style="background:#f0f4ff;border:1px solid #c5d0f5;border-radius:8px;
                padding:12px 18px;margin:16px 0;font-size:13px;color:#555;">
      ℹ️ <strong>{total_omitted} more listing(s)</strong> not shown here to keep
      the email readable. The full list is attached as
      <code>latest_results.json</code> — pass it to an AI assistant to find
      hidden gems.
    </div>
    """

    # Summary table by search
    from collections import defaultdict
    by_search = defaultdict(list)
    for l in all_listings:
        by_search[l.get("search_name", "Other")].append(l)

    summary_rows = ""
    for name, items in sorted(by_search.items()):
        prices = [i["price"] for i in items if i.get("price")]
        min_p = f"£{min(prices):,}" if prices else "—"
        summary_rows += f"""
        <tr>
          <td style="padding:6px 10px;">{name}</td>
          <td style="padding:6px 10px;text-align:center;">{len(items)}</td>
          <td style="padding:6px 10px;text-align:right;">{min_p}</td>
        </tr>
        """

    return f"""
    <!DOCTYPE html>
    <html>
    <head>
      <meta charset="utf-8">
      <meta name="viewport" content="width=device-width, initial-scale=1">
    </head>
    <body style="font-family:Arial,sans-serif;max-width:760px;
                 margin:0 auto;padding:20px;color:#333;">

      <div style="background:#1a4fa3;color:white;padding:20px;
                  border-radius:10px;margin-bottom:24px;">
        <h1 style="margin:0;font-size:22px;">🚗 EV Car Alert Digest</h1>
        <p style="margin:6px 0 0;opacity:0.85;font-size:14px;">
          {run_date} &bull;
          {len(new_listings)} new, {len(updated_listings)} price changes &bull;
          {stats.get('total_in_db',0)} total in database
        </p>
      </div>

      <!-- Summary Table -->
      <table style="width:100%;border-collapse:collapse;
                    margin-bottom:24px;font-size:14px;">
        <thead>
          <tr style="background:#f0f4ff;">
            <th style="padding:8px 10px;text-align:left;">Search</th>
            <th style="padding:8px 10px;text-align:center;">Found</th>
            <th style="padding:8px 10px;text-align:right;">Cheapest</th>
          </tr>
        </thead>
        <tbody>{summary_rows}</tbody>
      </table>

      {_error_banner(run_errors) if run_errors else ""}
      {_ai_summary_banner(ann) if ann and any(k != '_verdict' for k in ann) else ""}
      {listings_section}
      {overflow_note}

      {_update_banner(update_info) if update_info else ""}

      <hr style="margin:30px 0;border:none;border-top:1px solid #eee;">
      <p style="color:#aaa;font-size:11px;text-align:center;">
        Generated by ev-scraper on your Proxmox server &bull;
        Data from AutoTrader UK &amp; Motors.co.uk &bull;
        Always verify listings independently before travelling
      </p>
    </body>
    </html>
    """


def send_email(
    config: dict,
    new_listings: list,
    updated_listings: list,
    all_listings: list,
    stats: dict,
    subject_override: str = "",
    update_info: dict | None = None,
    run_errors: list[str] | None = None,
    max_email_listings: int = 20,
    json_path: str | None = None,
    annotations: dict | None = None,
) -> bool:
    """Send the digest email. Returns True on success."""
    email_cfg = config.get("email", {})
    subject_prefix = email_cfg.get("subject_prefix", "🚗 EV Alert")
    run_date = datetime.now().strftime("%A %d %B %Y, %H:%M")

    error_tag = " ⚠️ errors" if run_errors else ""
    subject = subject_override or (
        f"{subject_prefix}: {len(new_listings)} new listing"
        f"{'s' if len(new_listings) != 1 else ''}{error_tag} — {run_date}"
    )

    html_body = build_html_email(
        new_listings, updated_listings, all_listings, stats, run_date,
        update_info=update_info,
        run_errors=run_errors,
        max_email_listings=max_email_listings,
        annotations=annotations,
    )

    # Use 'mixed' to support both HTML body and file attachment
    msg = MIMEMultipart("mixed")
    msg["Subject"] = subject
    msg["From"] = email_cfg.get("smtp_user", "")
    msg["To"] = email_cfg.get("to", "")

    # HTML body goes inside a nested alternative part
    body_part = MIMEMultipart("alternative")
    body_part.attach(MIMEText(html_body, "html"))
    msg.attach(body_part)

    # Attach JSON export if it exists
    if json_path and Path(json_path).exists():
        try:
            with open(json_path, "rb") as f:
                attachment = MIMEApplication(f.read(), _subtype="json")
            attachment.add_header(
                "Content-Disposition",
                "attachment",
                filename="latest_results.json",
            )
            msg.attach(attachment)
        except Exception as exc:
            logger.warning(f"Could not attach JSON file (non-fatal): {exc}")

    try:
        context = ssl.create_default_context()
        with smtplib.SMTP(
            email_cfg["smtp_host"], int(email_cfg["smtp_port"])
        ) as server:
            server.ehlo()
            server.starttls(context=context)
            server.login(email_cfg["smtp_user"], email_cfg["smtp_password"])
            server.sendmail(
                email_cfg["smtp_user"],
                email_cfg["to"],
                msg.as_string()
            )
        logger.info(f"Email sent to {email_cfg['to']}")
        return True
    except Exception as e:
        logger.error(f"Failed to send email: {e}")
        return False