"""
scraper/emailer.py
Builds and sends a rich HTML email digest.
Each car gets its own card with images, price, mileage, flags,
and a direct link to the AutoTrader listing.
"""

import logging
import smtplib
import ssl
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Optional

logger = logging.getLogger(__name__)


def _car_card(listing: dict, badge: str = "") -> str:
    """Render one car listing as an HTML card."""
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

    seller_str = listing.get("seller_name", "") or listing.get("seller_type", "")
    spec = listing.get("spec_summary", "")[:120]

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
      <div style="margin-top:10px;">
        <a href="{listing['url']}"
           style="background:#1a4fa3;color:white;padding:8px 18px;
                  border-radius:6px;text-decoration:none;font-size:13px;">
          View on AutoTrader →
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


def build_html_email(
    new_listings: list,
    updated_listings: list,
    all_listings: list,
    stats: dict,
    run_date: str,
    update_info: dict | None = None,
    run_errors: list[str] | None = None,
    max_email_listings: int = 20,
) -> str:
    """Build the full HTML email body."""

    # Cap total car cards shown to max_email_listings (new listings take priority)
    new_to_show     = new_listings[:max_email_listings]
    remaining_slots = max(0, max_email_listings - len(new_to_show))
    updated_to_show = updated_listings[:remaining_slots]
    total_omitted   = (
        (len(new_listings) - len(new_to_show))
        + (len(updated_listings) - len(updated_to_show))
    )

    new_section = ""
    if new_to_show:
        cards = "".join(
            _car_card(l.to_dict() if hasattr(l, "to_dict") else l, "🆕 New")
            for l in new_to_show
        )
        new_section = f"""
        <h2 style="color:#155724;border-bottom:2px solid #28a745;
                   padding-bottom:6px;">
          🆕 New Listings ({len(new_listings)})
        </h2>
        {cards}
        """
    else:
        new_section = """
        <h2 style="color:#155724;border-bottom:2px solid #28a745;
                   padding-bottom:6px;">
          🆕 New Listings
        </h2>
        <p style="color:#666;">No new listings since last run.</p>
        """

    updated_section = ""
    if updated_to_show:
        cards = "".join(
            _car_card(
                l.to_dict() if hasattr(l, "to_dict") else l,
                "💲 Price Changed"
            )
            for l in updated_to_show
        )
        updated_section = f"""
        <h2 style="color:#856404;border-bottom:2px solid #ffc107;
                   padding-bottom:6px;">
          💲 Price Changes ({len(updated_listings)})
        </h2>
        {cards}
        """

    overflow_note = ""
    if total_omitted > 0:
        overflow_note = f"""
    <div style="background:#f0f4ff;border:1px solid #c5d0f5;border-radius:8px;
                padding:12px 18px;margin:16px 0;font-size:13px;color:#555;">
      ℹ️ <strong>{total_omitted} more listing(s)</strong> not shown here to keep
      the email readable. The full list is saved in
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
          {len(new_listings)} new &bull;
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
      {new_section}
      {updated_section}
      {overflow_note}

      {_update_banner(update_info) if update_info else ""}

      <hr style="margin:30px 0;border:none;border-top:1px solid #eee;">
      <p style="color:#aaa;font-size:11px;text-align:center;">
        Generated by ev-scraper on your Proxmox server &bull;
        Data from AutoTrader UK &bull;
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
    )

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = email_cfg.get("smtp_user", "")
    msg["To"] = email_cfg.get("to", "")
    msg.attach(MIMEText(html_body, "html"))

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