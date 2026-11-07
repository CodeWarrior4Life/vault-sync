#!/bin/bash
# LSEC-G707 mandatory follow-up (sentinel F3; delta-read N2/N3), run at T_RM+1320 s: ssh trinity '/bin/bash /Users/cyril/.cache/g707/g707_followup.sh'
# Read-only, except `launchctl bootout` of the SPENT watcher label (F8), and only once the receipt exists.
# Waits up to 180 s for the receipt (N2). Exit 0 = PASS, or TRIPPED-RELATCHED (an expected false trip, on record in
# EXPECTED.json; N3). Exit 1 = PAGE P0: HELD, no receipt, main pid count != 1, stopped=false, or marker absent after a trip.
set -u
export PATH=/usr/bin:/bin:/usr/sbin:/sbin
UIDN=$(id -u); B=/Users/cyril/.claude/backups/vault-sync/lift-g707-r2; LABEL=com.lattice.lift-watch-g707
S="/Users/cyril/Library/Application Support/Nexus/.lattice-runtime/f2383e35-2e9d-4da2-b5ed-de8a35778fa3/sync-state"
R="$B/lift-watch-receipt.json"
i=0; while [ ! -f "$R" ] && [ $i -lt 90 ]; do sleep 2; i=$((i+1)); done
bad=0; cls=FAIL
n=$(pgrep -f '/Applications/Nexus Vault Sync.app/Contents/MacOS/vault-sync-daemon' | grep -c . || true)
echo "main pids: $n ($(echo $(pgrep -f '/Applications/Nexus Vault Sync.app/Contents/MacOS/vault-sync-daemon')))"; [ "$n" = 1 ] || bad=1
if [ -e "$B/HELD" ]; then echo "HELD: $(cat "$B/HELD")"; bad=1; fi
[ -e "$S/delete-burst-PAUSED" ] && MK=PRESENT || MK=ABSENT; echo "marker $MK"
[ -e "$S/delete-burst-SUPPRESSED" ] && echo "ledger present: $(wc -l < "$S/delete-burst-SUPPRESSED" | tr -d ' ') lines" || echo "ledger absent"
if [ -f "$R" ]; then
  v=$(/usr/bin/python3 -c 'import json,sys;d=json.load(open(sys.argv[1]));print(d["verdict"],"stopped="+str(d["stopped"]),"stop_rc="+str(d.get("stop_rc")));print("receipt:",json.dumps({k:d.get(k) for k in ("version","verdict","replayed_seen","trip","stopped","stop_rc","marker_restored")}),file=sys.stderr)' "$R")
  echo "receipt: $v"
  case "$v" in
    "PASS "*) [ "$MK" = ABSENT ] && cls=PASS || { echo "PASS receipt but marker present?"; bad=1; } ;;
    "TRIPPED stopped=True stop_rc=0") [ "$MK" = PRESENT ] && cls=TRIPPED-RELATCHED || { echo "trip but marker ABSENT"; bad=1; } ;;
    *) bad=1 ;;
  esac
  launchctl bootout "gui/$UIDN/$LABEL" 2>/dev/null; launchctl print "gui/$UIDN/$LABEL" >/dev/null 2>&1 && { echo "label still loaded"; bad=1; } || echo "spent label booted out"
else
  echo "NO RECEIPT after T_RM+1320+180 s"; bad=1
fi
[ -f "$B/EXPECTED.json" ] && echo "expected: $(cat "$B/EXPECTED.json")"
if [ "$bad" = 0 ]; then echo "FOLLOW-UP $cls"; exit 0; else echo "FOLLOW-UP FAIL -> PAGE P0"; exit 1; fi
