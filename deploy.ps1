<#
.SYNOPSIS
    Push local ev-scraper code changes to the LXC and restart the timer.

.DESCRIPTION
    Base64-encodes changed Python/config files and sends them directly to the
    ev-scraper LXC via a single SSH connection.  Safe to re-run at any time —
    never touches the database or config.yaml.

    What it deploys:
      main.py
      config.yaml
      scraper/autotrader.py
      scraper/motors.py
      scraper/cargurus.py
      scraper/database.py
      scraper/emailer.py
      healthcheck.py

    After deploying it optionally runs --dry-run to verify the code works.

.PARAMETER LxcHost
    SSH hostname or IP of the ev-scraper LXC (default: 192.168.194.91).

.PARAMETER SshUser
    SSH user on the LXC (default: root).

.PARAMETER InstallDir
    Install directory inside the LXC (default: /opt/ev-scraper).

.PARAMETER ServiceUser
    User that runs the scraper inside the LXC (default: evscraper).

.PARAMETER DryRun
    After deploying, run 'python main.py --dry-run' inside the LXC to verify.

.PARAMETER NoPip
    Skip 'pip install -r requirements.txt'. Useful for pure code-only changes.

.PARAMETER UpdateCron
    Also update the systemd service ExecStart line to use --defer-email and
    install the ev-scraper-review timer (needed after the April 2026 agent split).

.EXAMPLE
    .\deploy.ps1
    .\deploy.ps1 -DryRun
    .\deploy.ps1 -UpdateCron -DryRun
    .\deploy.ps1 -LxcHost 192.168.194.91
#>
param(
    [string]$LxcHost           = "192.168.194.91",
    [string]$SshUser           = "root",
    [string]$InstallDir        = "/opt/ev-scraper",
    [string]$ServiceUser       = "evscraper",
    [string]$AgentDashboardUrl = "http://192.168.194.148:8766",
    [switch]$DryRun,
    [switch]$NoPip,
    [switch]$UpdateCron
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$RepoRoot = $PSScriptRoot
$Target   = "${SshUser}@${LxcHost}"

# ── 1. Collect files to deploy ───────────────────────────────────────────────
$FilesToDeploy = @(
    @{ Local = "main.py";                Remote = "main.py" },
    @{ Local = "config.yaml";            Remote = "config.yaml" },
    @{ Local = "scraper\autotrader.py";  Remote = "scraper/autotrader.py" },
    @{ Local = "scraper\motors.py";      Remote = "scraper/motors.py" },
    @{ Local = "scraper\cargurus.py";    Remote = "scraper/cargurus.py" },
    @{ Local = "scraper\database.py";    Remote = "scraper/database.py" },
    @{ Local = "scraper\emailer.py";     Remote = "scraper/emailer.py" },
    @{ Local = "healthcheck.py";         Remote = "healthcheck.py" },
    @{ Local = "requirements.txt";       Remote = "requirements.txt" }
)

foreach ($f in $FilesToDeploy) {
    $fullPath = Join-Path $RepoRoot $f.Local
    if (-not (Test-Path $fullPath)) {
        Write-Error "File not found: $fullPath"
    }
}

# ── 2. Base64-encode every file ──────────────────────────────────────────────
function Get-B64([string]$Path) {
    [Convert]::ToBase64String([IO.File]::ReadAllBytes($Path))
}

Write-Host "Encoding files ..."
$encoded = @{}
foreach ($f in $FilesToDeploy) {
    $encoded[$f.Remote] = Get-B64 (Join-Path $RepoRoot $f.Local)
    Write-Host "  $($f.Local)"
}

# ── 3. Build remote bash script ──────────────────────────────────────────────
$writeLines = ($FilesToDeploy | ForEach-Object {
    $remote = $_.Remote
    $b64    = $encoded[$remote]
    # Heredoc avoids printf quoting issues with long base64 strings
    @"
base64 -d > "${InstallDir}/${remote}" << 'B64EOF'
$b64
B64EOF
"@
}) -join "`n"

$pipLine = if ($NoPip) {
    "echo 'Skipping pip (--NoPip)'"
} else {
    @"
echo 'Running pip install ...'
$InstallDir/venv/bin/pip install --quiet -r $InstallDir/requirements.txt
echo 'pip done'
"@
}

$cronLines = if ($UpdateCron) {
    @"
echo 'Patching systemd units for --defer-email + review timer ...'

# Patch main scraper service to use --defer-email
sed -i 's|ExecStart=.*main.py.*|ExecStart=$InstallDir/venv/bin/python $InstallDir/main.py --config $InstallDir/config.yaml --defer-email|' /etc/systemd/system/ev-scraper.service

# Write ev-scraper-review.service — fetches review from agent and sends email
cat > /etc/systemd/system/ev-scraper-review.service << 'UNIT'
[Unit]
Description=EV Scraper — send AI-reviewed email
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
User=$ServiceUser
WorkingDirectory=$InstallDir
Environment="PLAYWRIGHT_BROWSERS_PATH=$InstallDir/.playwright"
ExecStart=$InstallDir/venv/bin/python $InstallDir/main.py --config $InstallDir/config.yaml --send-reviewed-email $AgentDashboardUrl/api/ev-review
StandardOutput=journal
StandardError=journal
TimeoutStartSec=120
UNIT

# Write ev-scraper-review.timer — runs 45 min after main scraper
cat > /etc/systemd/system/ev-scraper-review.timer << 'UNIT'
[Unit]
Description=Send AI-reviewed EV email (runs after agent finishes reviewing)
Requires=ev-scraper-review.service

[Timer]
OnCalendar=*-*-* 02:30:00
Persistent=false

[Install]
WantedBy=timers.target
UNIT

systemctl daemon-reload
systemctl enable ev-scraper-review.timer
systemctl start ev-scraper-review.timer
echo 'Units patched and review timer enabled.'
"@
} else {
    "# --UpdateCron not requested, skipping service patch"
}

$dryRunLines = if ($DryRun) {
    @"
echo ''
echo '--- Dry run ---'
PLAYWRIGHT_BROWSERS_PATH=$InstallDir/.playwright \
    $InstallDir/venv/bin/python $InstallDir/main.py \
    --dry-run --config $InstallDir/config.yaml \
    && echo 'Dry run OK' \
    || echo 'Dry run had errors — check $InstallDir/logs/scraper.log'
"@
} else {
    "# --DryRun not requested"
}

$remoteScript = @"
set -e

echo 'Writing files to $InstallDir ...'
$writeLines

chown $ServiceUser`:$ServiceUser $InstallDir/scraper/*.py $InstallDir/main.py $InstallDir/config.yaml $InstallDir/healthcheck.py $InstallDir/requirements.txt 2>/dev/null || true

$pipLine

$cronLines

echo 'Restarting ev-scraper.timer ...'
systemctl restart ev-scraper.timer
echo ''
systemctl status ev-scraper.timer --no-pager -l
echo ''
systemctl list-timers ev-scraper.timer --no-pager

$dryRunLines

echo ''
echo 'Deploy complete.'
"@

# ── 4. Single SSH call directly to the LXC ──────────────────────────────────
# Decode the script to a temp file on the remote before executing it.
# This avoids the bash stdin-leak bug where child processes (pip, systemctl)
# inherit the still-open pipe and consume stray input.
$remoteScript = $remoteScript -replace "`r", ""
$scriptB64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($remoteScript))

# The bootstrap command decodes b64 from stdin into a temp file, then runs it.
$bootstrap = 'TMPF=$(mktemp /tmp/deploy.XXXXXX.sh) && base64 -d > $TMPF && bash $TMPF; RC=$?; rm -f $TMPF; exit $RC'

Write-Host ""
Write-Host "Connecting to $Target — enter your password once ..."
[Text.Encoding]::UTF8.GetBytes($scriptB64) | ssh $Target $bootstrap

Write-Host ""
Write-Host "Done."
