#!/usr/bin/env bash
# link: install v0.4.32 + unmask + start. OWNER-GATED (PG=legit-3 + incident-lead ACK).
set -euo pipefail

STG="$HOME/vault-sync-v0432-staging"
APP="$HOME/Applications/Nexus-Vault-Sync.AppImage"
UNITDIR="$HOME/.config/systemd/user"
UNIT="nexus-vault-sync.service"
BACKUP_UNIT="$UNITDIR/${UNIT}.incident-paused-20260718"
SRC="$STG/Nexus.Vault.Sync_0.4.32_amd64.AppImage"

echo ">> preflight"
test -f "$SRC" || { echo "FATAL: staged AppImage missing: $SRC"; exit 1; }
test -f "$HOME/Applications/Nexus-Vault-Sync.AppImage.pre-v0432.bak" || { echo "FATAL: R2 binary backup missing"; exit 1; }
test -f "$BACKUP_UNIT" || { echo "FATAL: pre-mask unit backup missing: $BACKUP_UNIT"; exit 1; }
# sanity: staged binary really is 0.4.32
grep -aqx . <(strings -n 6 "$SRC" 2>/dev/null | grep -aoE '0\.4\.32' | head -1) || { echo "FATAL: staged binary is not 0.4.32"; exit 1; }

echo ">> 1. swap binary (backup already at *.pre-v0432.bak)"
install -m 0755 "$SRC" "$APP"

echo ">> 2. unmask + restore real unit"
systemctl --user unmask "$UNIT" || true          # removes the /dev/null mask symlink
install -m 0644 "$BACKUP_UNIT" "$UNITDIR/$UNIT"   # restore real ExecStart
systemctl --user daemon-reload

echo ">> 3. start"
systemctl --user enable --now "$UNIT"
sleep 6

echo ">> 4. verify"
systemctl --user is-active "$UNIT"
echo "--- version + migration line (expect version=0.4.32 and 'migrated keys' EXACTLY once) ---"
journalctl --user -u "$UNIT" -S "-3 min" --no-pager | grep -E 'version=|shadow store: migrated keys' || true
echo "--- conflict mints in last 3 min (expect 0) ---"
journalctl --user -u "$UNIT" -S "-3 min" --no-pager | grep -c 'CONFLICT (R4/R5)' || true
echo ">> link start done. Begin 30-min soak: watch for zero 'CONFLICT (R4/R5)' + zero new *.conflict-from-*."
