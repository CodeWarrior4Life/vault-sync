#!/bin/bash
# LSEC-G707 mandatory follow-up (sentinel F3), run at T_RM+1250 s: ssh trinity '/bin/bash /Users/cyril/.cache/g707/g707_followup.sh'
# Read-only, except one step: `launchctl bootout` of the SPENT watcher label (F8), and only once the receipt exists.
# Exit 0 = PASS (receipt PASS, one main pid, no HELD). Anything else = exit 1: the caller pages P0 to pitboss-trinity + the gate.
set -u
export PATH=/usr/bin:/bin:/usr/sbin:/sbin
UIDN=$(id -u); B=/Users/cyril/.claude/backups/vault-sync/lift-g707-r2; LABEL=com.lattice.lift-watch-g707
S="/Users/cyril/Library/Application Support/Nexus/.lattice-runtime/f2383e35-2e9d-4da2-b5ed-de8a35778fa3/sync-state"
bad=0
n=$(pgrep -f '/Applications/Nexus Vault Sync.app/Contents/MacOS/vault-sync-daemon' | grep -c . || true)
echo "main pids: $n ($(echo $(pgrep -f '/Applications/Nexus Vault Sync.app/Contents/MacOS/vault-sync-daemon')))"; [ "$n" = 1 ] || bad=1
if [ -e "$B/HELD" ]; then echo "HELD: $(cat "$B/HELD")"; bad=1; fi
if [ -f "$B/lift-watch-receipt.json" ]; then
  /usr/bin/python3 -c 'import json,sys;d=json.load(open(sys.argv[1]));print("receipt:",d["verdict"],"version",d.get("version"),"replayed",d["replayed_seen"],"trip",d["trip"],"stopped",d["stopped"],"marker_restored",d["marker_restored"]);sys.exit(0 if d["verdict"]=="PASS" else 1)' "$B/lift-watch-receipt.json" || bad=1
  launchctl bootout "gui/$UIDN/$LABEL" 2>/dev/null; launchctl print "gui/$UIDN/$LABEL" >/dev/null 2>&1 && { echo "label still loaded"; bad=1; } || echo "spent label booted out"
else
  echo "NO RECEIPT yet"; launchctl print "gui/$UIDN/$LABEL" 2>/dev/null | grep -E 'state|pid' | head -3; bad=1
fi
[ -e "$S/delete-burst-PAUSED" ] && echo "marker PRESENT (latched)" || echo "marker absent (lifted)"
[ -e "$S/delete-burst-SUPPRESSED" ] && echo "ledger present: $(wc -l < "$S/delete-burst-SUPPRESSED" | tr -d ' ') lines" || echo "ledger absent"
[ "$bad" = 0 ] && echo "FOLLOW-UP PASS" || echo "FOLLOW-UP FAIL -> PAGE P0"
exit $bad
