#!/usr/bin/env bash
# trinity (macOS): install v0.4.32 .app from dmg + launchd agent + start.
# OWNER-GATED (PG=legit-3 + incident-lead ACK). Run ON trinity.
set -euo pipefail

DMG="$HOME/vault-sync-v0432-staging/Nexus.Vault.Sync_0.4.32_aarch64.dmg"   # scp from link staging first
APPDST="/Applications/Nexus Vault Sync.app"
AGENT_SRC="$(dirname "$0")/com.lattice.nexus-vault-sync.plist"
AGENT_DST="$HOME/Library/LaunchAgents/com.lattice.nexus-vault-sync.plist"

echo ">> preflight"
test -f "$DMG" || { echo "FATAL: dmg missing on trinity: $DMG (scp from link:~/vault-sync-v0432-staging/)"; exit 1; }
test -d "${APPDST}.pre-v0432.bak" || { echo "FATAL: R2 app backup missing"; exit 1; }

echo ">> 1. mount dmg"
MNT="$(hdiutil attach "$DMG" -nobrowse -readonly | tail -1 | awk '{print $NF}')"
echo "mounted at $MNT"

echo ">> 2. replace app (backup already at *.pre-v0432.bak)"
rm -rf "$APPDST"
cp -a "$MNT/Nexus Vault Sync.app" "$APPDST"
xattr -dr com.apple.quarantine "$APPDST" || true
hdiutil detach "$MNT" || true

echo ">> 3. verify bundle version = 0.4.32"
/usr/libexec/PlistBuddy -c "Print :CFBundleShortVersionString" "$APPDST/Contents/Info.plist"

echo ">> 4. install + load launchd agent"
mkdir -p "$HOME/Library/LaunchAgents" "$HOME/Library/Logs"
install -m 0644 "$AGENT_SRC" "$AGENT_DST"
launchctl unload "$AGENT_DST" 2>/dev/null || true
launchctl load -w "$AGENT_DST"
sleep 6

echo ">> 5. verify running"
launchctl list | grep nexus-vault-sync || echo "WARN: agent not listed"
pgrep -fl vault-sync-daemon || echo "WARN: daemon process not found"
echo "--- recent daemon log (expect version 0.4.32 + 'migrated keys' once) ---"
tail -40 "$HOME/Library/Logs/nexus-vault-sync.err.log" 2>/dev/null | grep -E 'version=|migrated keys|CONFLICT' || true
echo ">> trinity start done. Begin 30-min soak."
