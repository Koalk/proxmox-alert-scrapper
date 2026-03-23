#!/usr/bin/env bash
# =============================================================================
# EV Car Alert Scraper — Proxmox Installer
# =============================================================================
# Run this on your Proxmox HOST shell (or inside a Debian/Ubuntu LXC):
#
#   bash install.sh
#
# What it does:
#   1. Creates a dedicated Debian LXC container (512MB RAM, 4GB disk)
#      OR installs directly if you're already inside an LXC / VM
#   2. Installs Python 3, pip, Playwright + Chromium dependencies
#   3. Pulls the repo from GitHub
#   4. Creates systemd service + timer (runs 02:00 daily)
#   5. Prompts for email config and writes config.yaml
#   6. Does a dry-run to verify everything works
#
# Requirements:
#   - Proxmox VE 7+ host OR any Debian 11/12 / Ubuntu 22.04+ system
#   - Internet access from the container
#   - Your GitHub repo URL (or use the default)
#
# Usage with your own GitHub repo:
#   REPO_URL=https://github.com/YOURUSERNAME/ev-scraper bash install.sh
# =============================================================================

set -euo pipefail

# --- Colours ---
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; CYAN='\033[0;36m'; NC='\033[0m'

info()    { echo -e "${BLUE}[INFO]${NC}  $*"; }
success() { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error()   { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }
prompt()  { echo -e "${CYAN}[INPUT]${NC} $*"; }

print_section_banner() {
    local title="${1:-Configuration}"
    echo ""
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${CYAN}  ${title}${NC}"
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
}

# Interactively collect SMTP and location config, storing results in CFG_* globals.
collect_config_interactively() {
    echo "  --- Email / SMTP ---"
    echo "  For Gmail: use an App Password, not your account password."
    echo "  Enable at: myaccount.google.com → Security → App Passwords"
    echo "  (Requires 2FA to be enabled on your Google account)"
    echo ""
    echo "  For self-hosted mail (e.g. Mailcow on Proxmox):"
    echo "  Use your SMTP relay settings instead."
    echo ""

    prompt "SMTP host [smtp.gmail.com]: "
    read -r CFG_SMTP_HOST; CFG_SMTP_HOST="${CFG_SMTP_HOST:-smtp.gmail.com}"

    prompt "SMTP port [587]: "
    read -r CFG_SMTP_PORT; CFG_SMTP_PORT="${CFG_SMTP_PORT:-587}"

    prompt "SMTP username (sender email address): "
    read -r CFG_SMTP_USER

    prompt "SMTP password / App Password: "
    read -rs CFG_SMTP_PASS; echo ""

    prompt "Send digest TO this address [${CFG_SMTP_USER}]: "
    read -r CFG_SMTP_TO; CFG_SMTP_TO="${CFG_SMTP_TO:-$CFG_SMTP_USER}"

    echo ""
    echo "  --- Search Defaults ---"
    echo "  These are applied to all searches in the config."
    echo ""

    prompt "Your postcode for distance calculations [EH1 3HU]: "
    read -r CFG_POSTCODE; CFG_POSTCODE="${CFG_POSTCODE:-EH1 3HU}"

    prompt "Default search radius in miles [120]: "
    read -r CFG_RADIUS; CFG_RADIUS="${CFG_RADIUS:-120}"
    echo ""
}

# =============================================================================
# CONFIGURATION — edit these or pass as env vars
# =============================================================================
REPO_URL="${REPO_URL:-https://github.com/YOURUSERNAME/ev-scraper}"
INSTALL_DIR="${INSTALL_DIR:-/opt/ev-scraper}"
SERVICE_USER="${SERVICE_USER:-evscraper}"
RUN_HOUR="${RUN_HOUR:-2}"      # 2am daily
RUN_MINUTE="${RUN_MINUTE:-0}"

# LXC settings (only used if running on Proxmox host)
LXC_ID="${LXC_ID:-300}"
LXC_HOSTNAME="${LXC_HOSTNAME:-ev-scraper}"
LXC_MEMORY="${LXC_MEMORY:-512}"    # MB — enough for headless Chromium
LXC_DISK="${LXC_DISK:-4}"          # GB
LXC_CORES="${LXC_CORES:-1}"
LXC_BRIDGE="${LXC_BRIDGE:-vmbr0}"

# =============================================================================
# DETECT ENVIRONMENT
# =============================================================================
detect_env() {
    # Any of these indicate we are on the Proxmox VE host (not inside a container)
    if [ -d /etc/pve ] || command -v pct &>/dev/null || command -v pveversion &>/dev/null; then
        echo "proxmox_host"
    elif grep -qi "debian\|ubuntu" /etc/os-release 2>/dev/null; then
        echo "linux_container"
    else
        echo "unknown"
    fi
}

ENV_TYPE=$(detect_env)
info "Detected environment: ${ENV_TYPE}"

# =============================================================================
# PROXMOX HOST: Create and configure LXC
# =============================================================================
setup_lxc() {
    info "Setting up LXC container on Proxmox host..."

    # --- Collect config answers HERE, on the host, where we have a real TTY ---
    print_section_banner "Configuration (collected before entering container)"
    collect_config_interactively

    # Find a Debian template
    TEMPLATE=""
    for t in $(pveam list local 2>/dev/null | grep -i "debian-12\|debian-11" | awk '{print $1}' | head -1); do
        TEMPLATE="$t"
    done

    if [ -z "$TEMPLATE" ]; then
        info "Downloading Debian 12 template..."
        pveam update
        TEMPLATE=$(pveam available --section system | grep "debian-12" | awk '{print $2}' | head -1)
        [ -n "$TEMPLATE" ] || error "Could not find a Debian template. Run: pveam update"
        pveam download local "$TEMPLATE"
        TEMPLATE="local:vztmpl/${TEMPLATE}"
    fi

    # Check if container already exists
    if pct status "$LXC_ID" &>/dev/null; then
        warn "LXC $LXC_ID already exists. Skipping creation."
    else
        info "Creating LXC container $LXC_ID ($LXC_HOSTNAME)..."
        pct create "$LXC_ID" "$TEMPLATE" \
            --hostname "$LXC_HOSTNAME" \
            --memory "$LXC_MEMORY" \
            --cores "$LXC_CORES" \
            --rootfs "local-lvm:${LXC_DISK}" \
            --net0 "name=eth0,bridge=${LXC_BRIDGE},ip=dhcp" \
            --unprivileged 1 \
            --features "nesting=1" \
            --ostype debian \
            --start 1

        info "Waiting for container to boot..."
        sleep 8
        success "LXC $LXC_ID created and started"
    fi

    # Make sure it's running
    pct start "$LXC_ID" 2>/dev/null || true
    sleep 5

    info "Downloading install script into container..."
    # Minimal Debian template may not have curl — install it first
    pct exec "$LXC_ID" -- bash -c "apt-get update -qq && apt-get install -y --no-install-recommends curl ca-certificates"
    # Always download fresh — avoids $0 being empty when run via bash <(curl ...)
    INSTALL_SH_URL="https://raw.githubusercontent.com/Koalk/proxmox-alert-scrapper/main/install.sh"
    pct exec "$LXC_ID" -- bash -c "curl -fsSL '$INSTALL_SH_URL' -o /tmp/install.sh"
    pct exec "$LXC_ID" -- env \
        CFG_SMTP_HOST="$CFG_SMTP_HOST" \
        CFG_SMTP_PORT="$CFG_SMTP_PORT" \
        CFG_SMTP_USER="$CFG_SMTP_USER" \
        CFG_SMTP_PASS="$CFG_SMTP_PASS" \
        CFG_SMTP_TO="$CFG_SMTP_TO" \
        CFG_POSTCODE="$CFG_POSTCODE" \
        CFG_RADIUS="$CFG_RADIUS" \
        bash /tmp/install.sh

    success "LXC setup complete."
    echo ""
    info "To get a shell in the container: pct enter $LXC_ID"
    info "To check logs:  pct exec $LXC_ID -- journalctl -u ev-scraper -f"
    info "To run manually: pct exec $LXC_ID -- systemctl start ev-scraper"
}

# =============================================================================
# LINUX CONTAINER / VM: Install everything
# =============================================================================
install_in_container() {
    info "Installing EV scraper on this system ($(hostname))..."

    # --- System packages ---
    info "Updating packages..."
    apt-get update -qq

    info "Installing system dependencies..."
    # Base packages
    apt-get install -y --no-install-recommends \
        python3 python3-pip python3-venv \
        git curl wget ca-certificates \
        2>/dev/null || warn "Some optional packages may not have installed"

    success "System packages installed"

    # --- Create service user ---
    if ! id "$SERVICE_USER" &>/dev/null; then
        useradd -r -m -s /bin/bash "$SERVICE_USER"
        success "Created user: $SERVICE_USER"
    else
        info "User $SERVICE_USER already exists"
    fi

    # --- Clone or update repo ---
    if [ -d "$INSTALL_DIR/.git" ]; then
        info "Updating existing repo at $INSTALL_DIR..."
        cd "$INSTALL_DIR"
        git pull --quiet
    else
        info "Cloning repo from $REPO_URL..."
        if [ "$REPO_URL" = "https://github.com/YOURUSERNAME/ev-scraper" ]; then
            warn "REPO_URL is still the placeholder value."
            warn "Either:"
            warn "  1. Set REPO_URL=https://github.com/yourusername/ev-scraper before running"
            warn "  2. Or copy files manually to $INSTALL_DIR"
            warn "Continuing with manual file copy mode..."
            mkdir -p "$INSTALL_DIR"
            # If running from a local copy, try to find project files
            SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
            if [ -f "$SCRIPT_DIR/main.py" ]; then
                cp -r "$SCRIPT_DIR/." "$INSTALL_DIR/"
                success "Copied from $SCRIPT_DIR"
            else
                error "No repo URL and no local files found at $SCRIPT_DIR. " \
                      "Please set REPO_URL or copy project files to $INSTALL_DIR manually."
            fi
        else
            git clone "$REPO_URL" "$INSTALL_DIR"
            success "Repo cloned to $INSTALL_DIR"
        fi
    fi

    # --- Python venv + deps ---
    info "Creating Python virtual environment..."
    python3 -m venv "$INSTALL_DIR/venv"
    "$INSTALL_DIR/venv/bin/pip" install --quiet --upgrade pip
    "$INSTALL_DIR/venv/bin/pip" install --quiet -r "$INSTALL_DIR/requirements.txt"
    success "Python dependencies installed"

    # --- Install Playwright browsers ---
    info "Installing Playwright Chromium (this downloads ~200MB)..."
    PLAYWRIGHT_BROWSERS_PATH="$INSTALL_DIR/.playwright" \
        "$INSTALL_DIR/venv/bin/python" -m playwright install chromium 2>&1 | \
        grep -E "Downloading|Installing|✓" || true

    info "Installing Chromium system dependencies via Playwright..."
    PLAYWRIGHT_BROWSERS_PATH="$INSTALL_DIR/.playwright" \
        "$INSTALL_DIR/venv/bin/python" -m playwright install-deps chromium
    success "Playwright Chromium installed"

    # --- Create data directories ---
    mkdir -p "$INSTALL_DIR/data" "$INSTALL_DIR/logs"
    chown -R "$SERVICE_USER:$SERVICE_USER" "$INSTALL_DIR"
    success "Directories created"

    # --- Interactive config setup ---
    configure_app

    # --- Systemd service ---
    setup_systemd

    # --- Test run ---
    info "Running a dry-run to verify the setup..."
    sudo -u "$SERVICE_USER" \
        PLAYWRIGHT_BROWSERS_PATH="$INSTALL_DIR/.playwright" \
        "$INSTALL_DIR/venv/bin/python" "$INSTALL_DIR/main.py" \
        --dry-run --config "$INSTALL_DIR/config.yaml" \
        && success "Dry-run completed successfully" \
        || warn "Dry-run had errors — check $INSTALL_DIR/logs/scraper.log"

    echo ""
    success "========================================="
    success "Installation complete!"
    success "========================================="
    echo ""
    info "Config file:    $INSTALL_DIR/config.yaml"
    info "Log file:       $INSTALL_DIR/logs/scraper.log"
    info "JSON output:    $INSTALL_DIR/data/latest_results.json"
    info "Database:       $INSTALL_DIR/data/listings.db"
    echo ""
    info "Manual run:     systemctl start ev-scraper"
    info "Watch logs:     journalctl -u ev-scraper -f"
    info "Check timer:    systemctl list-timers ev-scraper"
    info "Edit config:    nano $INSTALL_DIR/config.yaml"
    echo ""
    info "The scraper will run daily at ${RUN_HOUR}:$(printf '%02d' $RUN_MINUTE)."
}

# =============================================================================
# CONFIGURATION PROMPT
# =============================================================================
configure_app() {
    CONFIG_FILE="$INSTALL_DIR/config.yaml"

    # Create config.yaml from example if it doesn't exist yet
    if [ ! -f "$CONFIG_FILE" ]; then
        if [ -f "$INSTALL_DIR/config.yaml.example" ]; then
            cp "$INSTALL_DIR/config.yaml.example" "$CONFIG_FILE"
            info "Created config.yaml from example"
        else
            error "No config.yaml or config.yaml.example found in $INSTALL_DIR"
        fi
    fi

    # Skip prompts only if all placeholders have already been replaced
    if ! grep -qE 'your_app_password|you@gmail\.com|EH1 3HU' "$CONFIG_FILE" 2>/dev/null; then
        info "Config already fully configured — skipping prompt"
        return
    fi

    # If config values were passed in as env vars (e.g. from the Proxmox host
    # before entering a container where there is no TTY), use them directly.
    # Otherwise fall back to interactive prompts.
    if [ -z "${CFG_SMTP_USER:-}" ]; then
        print_section_banner "Configuration"
        collect_config_interactively
    else
        info "Applying config from environment variables (passed from host)..."
    fi

    # Apply all values to config file.
    # Use python3 for the radius replacement so we only target defaults:, not per-search overrides.
    sed -i \
        -e "s|smtp_host:.*|smtp_host: \"${CFG_SMTP_HOST:-smtp.gmail.com}\"|" \
        -e "s|smtp_port:.*|smtp_port: ${CFG_SMTP_PORT:-587}|" \
        -e "s|smtp_user:.*|smtp_user: \"$CFG_SMTP_USER\"|" \
        -e "s|smtp_password:.*|smtp_password: \"$CFG_SMTP_PASS\"|" \
        -e "s|^  to:.*|  to: \"${CFG_SMTP_TO:-$CFG_SMTP_USER}\"|" \
        -e "s|EH1 3HU|${CFG_POSTCODE:-EH1 3HU}|g" \
        "$CONFIG_FILE"
    python3 - "$CONFIG_FILE" "${CFG_RADIUS:-120}" << 'PYEOF'
import sys, re
path, radius = sys.argv[1], sys.argv[2]
text = open(path).read()
# Replace radius only inside the defaults: block (before the first 'searches:')
marker = text.find('searches:')
if marker == -1:
    marker = len(text)
head = re.sub(r'(\bradius:\s*)\d+', r'\g<1>' + radius, text[:marker])
open(path, 'w').write(head + text[marker:])
PYEOF

    success "Config written to $CONFIG_FILE"
}

# =============================================================================
# SYSTEMD SERVICE + TIMER
# =============================================================================
setup_systemd() {
    info "Creating systemd service and timer..."

    cat > /etc/systemd/system/ev-scraper.service << EOF
[Unit]
Description=EV Car Alert Scraper
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
User=${SERVICE_USER}
WorkingDirectory=${INSTALL_DIR}
Environment="PLAYWRIGHT_BROWSERS_PATH=${INSTALL_DIR}/.playwright"
ExecStart=${INSTALL_DIR}/venv/bin/python ${INSTALL_DIR}/main.py --config ${INSTALL_DIR}/config.yaml
StandardOutput=journal
StandardError=journal
TimeoutStartSec=43200
# 12 hour timeout — matches your requirement
# Memory limit to protect Proxmox host
MemoryMax=600M
CPUQuota=80%

[Install]
WantedBy=multi-user.target
EOF

    cat > /etc/systemd/system/ev-scraper.timer << EOF
[Unit]
Description=Run EV Car Alert Scraper daily at ${RUN_HOUR}:$(printf '%02d' $RUN_MINUTE)
Requires=ev-scraper.service

[Timer]
OnCalendar=*-*-* ${RUN_HOUR}:$(printf '%02d' $RUN_MINUTE):00
Persistent=true
RandomizedDelaySec=300
# Up to 5min random delay so it doesn't hammer sites at exactly the same time

[Install]
WantedBy=timers.target
EOF

    systemctl daemon-reload
    systemctl enable ev-scraper.timer
    systemctl start ev-scraper.timer

    success "Systemd timer enabled — next run: $(systemctl list-timers ev-scraper.timer --no-pager | tail -2 | head -1)"
}

# =============================================================================
# UNINSTALL (pass --uninstall)
# =============================================================================
uninstall() {
    warn "Removing EV scraper..."
    systemctl stop ev-scraper.timer 2>/dev/null || true
    systemctl disable ev-scraper.timer 2>/dev/null || true
    systemctl stop ev-scraper.service 2>/dev/null || true
    systemctl disable ev-scraper.service 2>/dev/null || true
    rm -f /etc/systemd/system/ev-scraper.{service,timer}
    systemctl daemon-reload
    warn "Install dir $INSTALL_DIR NOT removed — your data is safe."
    warn "To fully remove: rm -rf $INSTALL_DIR"
    success "Uninstalled"
}

# =============================================================================
# MAIN
# =============================================================================
if [ "${1:-}" = "--uninstall" ]; then
    uninstall
    exit 0
fi

if [ "$ENV_TYPE" = "proxmox_host" ] && [ "${FORCE_LOCAL:-0}" != "1" ]; then
    info "Running on Proxmox host — will create an LXC container."
    info "To install directly on this host instead: FORCE_LOCAL=1 bash install.sh"
    echo ""
    prompt "LXC container ID to use [$LXC_ID]: "
    read -r input_id; LXC_ID="${input_id:-$LXC_ID}"

    prompt "LXC hostname [$LXC_HOSTNAME]: "
    read -r input_host; LXC_HOSTNAME="${input_host:-$LXC_HOSTNAME}"

    setup_lxc
else
    install_in_container
fi