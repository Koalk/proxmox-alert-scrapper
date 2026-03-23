# EV Car Alert Scraper 🚗

A resource-conscious Playwright-based scraper that monitors AutoTrader UK
for used EV listings matching your criteria, emails you a rich HTML digest
each morning, and saves a JSON file you can load into Claude for discussion.

## What it does

- Runs headless Chromium overnight (default 2am) on your Proxmox server
- Searches AutoTrader UK for your configured models within your radius
- Tracks seen listings in SQLite — only emails you genuinely **new** ones
- Flags price changes on previously seen listings
- Produces a clean HTML email with photos, price, mileage, location, flags
- Saves `latest_results.json` for easy review or pasting into Claude

---

## Quick Start

### Install on Proxmox (recommended)

Run this on your Proxmox host shell — it creates a self-contained Debian LXC
container and installs everything inside it:

```bash
REPO_URL=https://github.com/Koalk/proxmox-alert-scrapper bash <(curl -fsSL https://raw.githubusercontent.com/Koalk/proxmox-alert-scrapper/main/install.sh)
```

The installer will:
- Create a lightweight Debian LXC container (512MB RAM, 4GB disk, 1 core)
- Install all dependencies inside it
- Prompt you for SMTP credentials, postcode, and search radius
- Set up a systemd timer for 2am daily runs
- Do a dry-run to verify everything works

### Already inside an LXC or VM

```bash
git clone https://github.com/Koalk/proxmox-alert-scrapper
cd proxmox-alert-scrapper
FORCE_LOCAL=1 bash install.sh
```

---

## Local Development (test before deploying)

You can run the scraper on your own machine (Windows/Mac/Linux) to iterate
quickly without touching Proxmox.

```bash
# 1. Clone and enter the repo
git clone https://github.com/Koalk/proxmox-alert-scrapper
cd proxmox-alert-scrapper

# 2. Create a virtualenv and install dependencies
python -m venv venv
# Windows:
venv\Scripts\activate
# Mac/Linux:
source venv/bin/activate

pip install -r requirements.txt
playwright install chromium

# 3. Set up a local config (uses relative ./data/ paths, one search enabled)
cp config.local.yaml.example config.local.yaml
# Edit config.local.yaml — fill in your postcode and SMTP details

# 4. Dry run (scrapes, writes JSON, no email sent)
python main.py --dry-run --config config.local.yaml

# 5. Once happy, send a test email
python main.py --test-email --config config.local.yaml
```

Output lands in `./data/` and `./logs/` (both gitignored).

---

## Email Setup

The scraper uses standard SMTP. Two easy options:

### Gmail (easiest)
1. Enable 2FA on your Google account
2. Go to: myaccount.google.com → Security → App Passwords
3. Create an App Password for "Mail"
4. Use that 16-character password in the config (NOT your real password)

```yaml
email:
  smtp_host: "smtp.gmail.com"
  smtp_port: 587
  smtp_user: "you@gmail.com"
  smtp_password: "abcd efgh ijkl mnop"   # App Password
  to: "you@gmail.com"
```

### Self-hosted mail (e.g. Mailcow on Proxmox)
```yaml
email:
  smtp_host: "your.mailserver.local"
  smtp_port: 587
  smtp_user: "alerts@yourdomain.com"
  smtp_password: "your_smtp_password"
  to: "you@yourdomain.com"
```

---

## Customising Searches

Edit `config.yaml`. Set shared values once in `defaults:` — any field can still
be overridden per-search under `autotrader:`:

```yaml
defaults:
  postcode: "EH1 1YZ"   # your postcode — applies to all searches
  radius: 200            # miles
  fuel_type: "Electric"
  year_from: 2021
  mileage_max: 110000

searches:
  - name: "Skoda Enyaq iV 80"
    enabled: true
    autotrader:
      make: "Skoda"
      model: "Enyaq"
      price_max: 18000   # only override what differs
    require_keywords: ["82kWh", "80"]   # ALL must appear in title/spec
    exclude_keywords: ["iV 60"]          # ANY triggers exclusion

  - name: "BYD Sealion 7 (Watch)"
    enabled: true
    autotrader:
      make: "BYD"
      model: "Sealion 7"
      price_max: 26000
      radius: 300         # per-search override — wider net for rarer model
    require_keywords: []
    exclude_keywords: []
```

Set `enabled: false` to pause a search without deleting it.

---

## Resource Limits

Tune in `config.yaml` for your Proxmox server:

```yaml
limits:
  max_concurrent_pages: 1      # keep at 1 — single browser tab at a time
  page_timeout_ms: 30000       # 30s per page
  scroll_pause_ms: 1500        # pause to trigger lazy-load
  request_delay_ms: 3000       # pause between listings (be polite)
  search_delay_ms: 8000        # pause between different car models
  max_pages_per_search: 2      # results sorted price-asc — pages 1-2 = the cheapest
  max_listings_per_search: 20  # keep the 20 cheapest matching listings per search
  max_images_per_listing: 3    # images embedded in email
```

Results are sorted by price ascending, so the first 2 pages always contain
the cheapest available listings. Combined with per-search `require_keywords`
filters, 20 results per search is plenty — you won't miss bargains that
don't appear in the first ~26 organic listings.

---

## Manual Commands

```bash
# Run now (sends email if new listings found)
systemctl start ev-scraper

# Run now, don't send email
sudo -u evscraper /opt/ev-scraper/venv/bin/python /opt/ev-scraper/main.py --dry-run

# Force send email even if nothing new
sudo -u evscraper /opt/ev-scraper/venv/bin/python /opt/ev-scraper/main.py --force-email

# Watch live log output
journalctl -u ev-scraper -f

# Check when next run is scheduled
systemctl list-timers ev-scraper

# View JSON output
cat /opt/ev-scraper/data/latest_results.json | python3 -m json.tool | head -100
```

### Updating the code in your container

Run this on the **Proxmox host** shell to pull the latest code and refresh dependencies:

```bash
pct exec 300 -- su -s /bin/bash evscraper -c \
  "cd /opt/ev-scraper && git pull && venv/bin/pip install -q -r requirements.txt"
```

The scraper will also notify you in the email digest when new commits are available on the remote, so you'll know when an update is worth applying.

---

## The JSON file

`latest_results.json` is structured for easy reading and for pasting into Claude:

```json
{
  "generated_at": "2026-03-23T02:14:22",
  "run_stats": {
    "total_scraped": 47,
    "new_listings": 3,
    "price_changes": 1,
    "total_in_db": 142
  },
  "new_listings": [ ... ],
  "price_changes": [ ... ],
  "all_current_listings": [ ... ]
}
```

Paste the `new_listings` array into a Claude conversation and ask:
"Which of these are the best value based on what we've discussed?"

---

## Testing

```bash
# Install pytest (already in requirements.txt)
pip install -r requirements.txt

# Unit tests — instant, no network needed
pytest tests/test_unit.py -v

# Integration test — launches a real browser, fetches 1 listing
# Takes ~30-60s depending on AutoTrader response time
pytest tests/test_integration.py --run-integration -v -s
```

The integration test verifies the full pipeline:
1. Builds a real AutoTrader search URL
2. Opens it in headless Chromium and collects listing links from page 1
3. Fetches the first listing's detail page
4. Asserts the result has a non-empty title, a plausible price, and a valid AutoTrader URL

Integration tests are skipped by default (they hit a live site) — pass
`--run-integration` to run them.

---

## Updating

```bash
cd /opt/ev-scraper
git pull
/opt/ev-scraper/venv/bin/pip install -r requirements.txt
systemctl restart ev-scraper.timer
```

---

## Uninstall

```bash
bash /opt/ev-scraper/install.sh --uninstall
# Your data in /opt/ev-scraper/data is preserved
# Remove manually with: rm -rf /opt/ev-scraper
```

---

## Notes

- This scraper is for personal use only — be respectful of AutoTrader's
  servers (the default delays are intentionally generous)
- Listing data belongs to AutoTrader and the dealers — use it for personal
  car-buying research only
- Always verify listings directly on AutoTrader before travelling