# EV Scraper — Coding Assistant Context
# Repo: https://github.com/Koalk/proxmox-alert-scrapper

## What this project is

Playwright-based headless Chromium scraper running nightly (2am) on a
resource-constrained Proxmox LXC (512MB RAM, 1 core). Monitors AutoTrader UK
for used EVs, deduplicates against SQLite, emails an HTML digest with car
photos, writes latest_results.json. Personal use, Edinburgh, Scotland.

---

## Actual repo structure (verified from GitHub)

```
proxmox-alert-scrapper/
├── main.py
├── healthcheck.py
├── install.sh
├── pytest.ini
├── requirements.txt
├── config.yaml.example         # committed — update this
├── config.local.yaml.example   # for local dev — update this too
├── ev-scraper.log              # (shouldn't be committed, ignore)
├── .github/workflows/
├── scraper/                    # scraper modules
└── tests/                      # pytest unit + integration tests
    ├── test_unit.py
    └── test_integration.py
```

## Current config.yaml.example defaults

```yaml
defaults:
  postcode: "EH1 3HU"
  radius: 120
  fuel_type: "Electric"
  year_from: 2021
  mileage_max: 110000

limits:
  max_concurrent_pages: 1
  page_timeout_ms: 30000
  scroll_pause_ms: 1500
  request_delay_ms: 3000
  search_delay_ms: 8000
  max_pages_per_search: 2
  max_listings_per_search: 20
  max_images_per_listing: 3
  max_email_listings: 20
```

## Current searches in config.yaml.example

- Skoda Enyaq iV 80    price_max: 18000  require: ["82kWh","80"]  excl: ["60kWh","iV 60"]
- Kia EV6              price_max: 22000  excl: ["GT"]
- Hyundai Ioniq 5      price_max: 22000
- VW ID.4 Pro          price_max: 18000  require: ["Pro"]
- BMW iX3              price_max: 23000
- BYD Sealion 7        price_max: 26000  radius: 300  year_from: 2023  mileage_max: 60000
- Skoda Peaq           price_max: 40000  radius: 300  year_from: 2026  (future watch)
- Kia PV5 7-seat       price_max: 35000  radius: 300  year_from: 2026  (future watch)

---

## CHANGES REQUESTED — implement all of these

### 1. Restructure Ioniq 5 search: lower mileage cap significantly

The Ioniq 5 is one of the most common Uber Exec / PCO cars in the UK — actively
recommended in PCO guides and used commercially at 50,000-80,000 miles/year.
A used Ioniq 5 with 80,000 miles may have been charged 3x daily on rapid DC,
which degrades the battery far faster than normal family use.

The owner does NOT want high-mileage Ioniq 5s. Two options — implement BOTH:

**Option A: Tighter mileage cap**
Change Ioniq 5 `mileage_max` to 40000 (override the 110000 default).
This eliminates the bulk of ex-PCO stock which typically returns at 60k-100k.

**Option B: Add a per-search mileage_max override key in config**
The config already supports per-search autotrader overrides. Just document that
`mileage_max` can be set per-search as well, and set it on Ioniq 5:
```yaml
- name: "Hyundai Ioniq 5"
  enabled: true
  autotrader:
    make: "Hyundai"
    model: "Ioniq 5"
    price_max: 20000
    mileage_max: 40000    # tighter than default — heavy Uber/PCO use in UK
  require_keywords: []
  exclude_keywords: []
```

**Also add attention flag in scraper code for ICCU recall risk:**
2022–2024 Ioniq 5 and Ioniq 6 have a known ICCU (Integrated Charging Control
Unit) fault that stops the 12V battery charging, causing loss of drive. Subject
to a Hyundai recall. Flag any Ioniq 5 from these years:
```python
# In _scrape_listing(), attention flags section:
if listing.search_name == "Hyundai Ioniq 5" and listing.year in (2022, 2023, 2024):
    flags.append("⚠️ ICCU recall risk — verify software fix applied (2022-24 models)")
```

### 2. Add Kia EV3 search — active used market, genuinely good option

The Kia EV3 launched in the UK in 2025. There are already 286 used examples on
AutoTrader (mostly ex-demo / early lease returns), starting from ~£25,000.
This is significant: 7-year/100,000-mile Kia warranty, same platform as EV9,
128kW DC charging (10-80% in ~30 min), 81kWh battery, up to 375 miles WLTP.
Highly reviewed — Carwow 2026 Car of the Year Highly Commended. Zero taxi
exposure (too new, wrong price bracket for PCO use). Boot: 460L seats up.

**One issue for this buyer: 460L boot is smaller than ideal for 2 dogs.**
Flag this in a comment but include the search — the owner wants to see it.

```yaml
- name: "Kia EV3"
  enabled: true
  autotrader:
    make: "Kia"
    model: "EV3"
    price_max: 30000
    radius: 200
    year_from: 2025
    mileage_max: 20000   # only low-mileage ex-demo/early returns are worth it
  require_keywords: []
  exclude_keywords: ["Standard Range"]  # want the 81kWh not the 58kWh
```

Note on `require_keywords`: do NOT add ["81kWh", "GT-Line"] as require_keywords —
spec_summary extraction is currently unreliable. Use exclude_keywords only.

### 3. Add Kia EV5 search — family SUV, just arrived in used market

The EV5 is Kia's family SUV (larger than EV3, smaller than EV9). Launched UK
late 2025, a handful of used examples are appearing (~46 on AutoUncle).
FWD 81.4kWh battery, 329 miles WLTP, 7yr warranty. Boot is larger than EV3.
New from ~£37,000, used from ~£30,000+ currently. Worth watching.

```yaml
- name: "Kia EV5 (Watch)"
  enabled: true
  autotrader:
    make: "Kia"
    model: "EV5"
    price_max: 33000
    radius: 200
    year_from: 2025
    mileage_max: 20000
  require_keywords: []
  exclude_keywords: []
```

### 4. Update Skoda Enyaq search — fix radius and keywords

The Enyaq is the primary target. Scotland has thin stock — radius 120mi returns
almost nothing. Must use 200mi minimum to catch Newcastle, Glasgow, wider.

The `require_keywords: ["82kWh", "80"]` filter is currently risky because
spec_summary extraction is unreliable — if spec_summary comes back empty (which
it did on all 63 listings in the first real run), this silently returns zero
results. Change to rely on exclude_keywords only until spec extraction is fixed.

```yaml
- name: "Skoda Enyaq iV 80"
  enabled: true
  autotrader:
    make: "Skoda"
    model: "Enyaq"
    price_max: 22000
    radius: 200          # MUST be 200 — thin Scottish market
  require_keywords: []   # don't use until spec_summary extraction is verified working
  exclude_keywords: ["iV 60", "60kWh", "vRS", "Coupe"]
```

Also raise `price_max` from 18000 to 22000 — the Enyaq holds value better than
the Ioniq 5 and good 82kWh examples rarely appear below £18k now.

### 5. Update Kia EV6 search — tighten mileage slightly

EV6 has some PCO exposure (less than Ioniq 5, but real). Cap at 70000 miles.
The `exclude_keywords: ["GT"]` is correct — keep it.

```yaml
- name: "Kia EV6"
  enabled: true
  autotrader:
    make: "Kia"
    model: "EV6"
    price_max: 22000
    mileage_max: 70000   # slightly tighter than default
  require_keywords: []
  exclude_keywords: ["GT"]
```

### 6. Update VW ID.4 Pro search — fix radius and drop require_keywords

VW ID.4 Pro with require_keywords: ["Pro"] has same problem as Enyaq — will
return zero results if spec_summary is empty. Drop the keyword filter.
AutoTrader's `model` field for ID.4 variants is unreliable anyway — better to
let all ID.4 through and filter by price (Pro is the larger battery variant,
tends to be priced higher than the base ID.4).

```yaml
- name: "VW ID.4"
  enabled: true
  autotrader:
    make: "Volkswagen"
    model: "ID.4"
    price_max: 20000
    radius: 200
  require_keywords: []
  exclude_keywords: ["ID.4 GTX"]   # AWD version, tends to be overpriced
```

### 7. Add commercial-use detection to attention flags in scraper code

In `scraper/autotrader.py`, in the `_scrape_listing()` attention flags section,
add detection for commercial/taxi history. This is a FLAG not a hard exclude —
the owner wants to see the listing and decide.

```python
commercial_signals = [
    "pco", "private hire", "taxi", "uber", "bolt driver", "lyft",
    "fleet", "rental", "hire car", "ex fleet", "ex-fleet",
    "company car", "lease return", "minicab", "hackney"
]
if any(s in page_text for s in commercial_signals):
    flags.append("🚕 Commercial use signals found — verify history")
```

### 8. Improve mileage flag bands — replace binary high/low with 3 tiers

Currently: mileage < 25000 → "✅ Low mileage", mileage > 80000 → "⚠️ High"

Replace with:
```python
if mileage is not None:
    if mileage < 25000:
        flags.append("✅ Low mileage")
    elif mileage > 85000:
        flags.append("🔴 Very high mileage — battery check essential")
    elif mileage > 60000:
        flags.append("⚠️ Higher mileage")
    # 25k–60k is normal — no flag needed
```

---

## DO NOT CHANGE

- Single-browser, single-tab, sequential architecture. No concurrency.
- SQLite dedup logic in database.py.
- All CLI flags: --dry-run, --force-email, --test-email, --autotrader-only,
  --quick, --mark-unsent, --reset-db
- The `defaults:` block in config — per-search overrides work, keep that pattern.
- Jitter on all delays (`_jitter()` function). Do not use fixed intervals.
- Cookie handling (`_cookie_accepted` flag). Accepts once per session.
- The `max_pages_per_search: 2` limit — results sorted price-asc means pages
  1-2 always contain the cheapest listings. This is intentional.
- Tests in `tests/` — update tests if you change function signatures, but
  don't remove them. Integration test hits live AutoTrader — keep it.

---

## Important: require_keywords is currently unreliable

In the first real run (63 listings, 2026-03-24), ALL listings had empty
`spec_summary`, `location`, and `seller_name` fields — AutoTrader's HTML
structure didn't match the selectors. Any search with `require_keywords` set
will silently return zero results when spec_summary is empty.

**Until the spec_summary selector is verified working in tests:**
- Set `require_keywords: []` on all searches
- Use `exclude_keywords` only for filtering
- The integration test should assert spec_summary is non-empty

---

## Owner context (for understanding why these changes make sense)

- Buying a used EV, Edinburgh, postcode EH15 3HU
- 2 medium dogs — boot size matters (aim for 490L+, ideally 520L+)
- Budget ~£19k effective deposit, willing to use 0% Scottish EST loan if available
- Waiting until April 2026 for Scottish grant/loan cycle reset before buying
- NOT in a rush — using scraper to monitor market and wait for right car
- Wants quality matches, not volume. Would rather see 5 great listings
  per run than 20 mediocre ones.
- Primary targets in priority order:
  1. Skoda Enyaq iV 80 (82kWh) — best boot, lowest taxi risk, local dealer
  2. VW ID.4 (Pro spec preferred) — same platform as Enyaq, often cheaper
  3. Kia EV3 — new, excellent warranty, zero taxi exposure (boot slightly small)
  4. Kia EV5 — newer, watch this space as prices fall
  5. Kia EV6 — good car, some PCO exposure, 490L boot
  6. BMW iX3 — premium option, local BMW dealer Edinburgh
  7. Hyundai Ioniq 5 — deprioritised due to PCO market saturation, flag heavily
- Willing to travel up to ~150 miles for the right car