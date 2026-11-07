#!/bin/bash
# LSEC-G707 trip action for lift_watch r3.1 --stop-cmd (Trinity MAIN f2383e35). macOS bash 3.2.
# lift_watch has ALREADY restored delete-burst-PAUSED before calling this (r3 restore-first).
# This script: STOP the main daemon the G375 way (boot out watchdog + daemon agent, TERM then KILL
# the /Applications bundle pid only; the memory instance under vault-sync-memory/ is never matched),
# then RE-OPEN it so it boots RE-LATCHED from the marker (a re-latch happens only at boot).
#   RE-LATCHED within 60 s -> re-bootstrap the two jobs, exit 0  (sync stays UP, valve latched)
#   marker missing, or no RE-LATCHED -> stop again and leave 0 main procs, exit 1 (HELD: page P0)
# Hard budget: under 100 s, inside lift_watch's 120 s subprocess timeout.
set -u
export PATH=/usr/bin:/bin:/usr/sbin:/sbin
UIDN=$(id -u)
APP="/Applications/Nexus Vault Sync.app"
SS="$HOME/Library/Application Support/Nexus/.lattice-runtime/f2383e35-2e9d-4da2-b5ed-de8a35778fa3/sync-state"
MARKER="$SS/delete-burst-PAUSED"
LOGDIR="$HOME/Library/Application Support/Nexus/logs"
LA="$HOME/Library/LaunchAgents"
WD="$LA/com.lattice.vaultsync-liveness-watchdog.plist"; DP="$LA/com.lattice.vaultsync-daemon.plist"
OUT="$HOME/.claude/backups/vault-sync/lift-g707-r2/stop-relatch.log"
mkdir -p "$(dirname "$OUT")" 2>/dev/null; exec >>"$OUT" 2>&1 || true
ts() { date -u '+%H:%M:%SZ'; }
main_pids() { pgrep -f '/Applications/Nexus Vault Sync.app/Contents/MacOS/vault-sync-daemon' || true; }
LOG() { ls -t "$LOGDIR"/daemon.log.* 2>/dev/null | grep -v '\.gz$' | head -1; }
stop_all() {
  launchctl bootout "gui/$UIDN/com.lattice.vaultsync-liveness-watchdog" 2>/dev/null
  launchctl bootout "gui/$UIDN/com.lattice.vaultsync-daemon" 2>/dev/null
  for p in $(main_pids); do kill -TERM "$p"; done
  i=0; while [ $i -lt 20 ] && [ -n "$(main_pids)" ]; do sleep 0.5; i=$((i+1)); done
  for p in $(main_pids); do kill -KILL "$p"; done; sleep 1
}
echo "== $(ts) G707 trip: stop + re-latch (main pids: $(echo $(main_pids)))"
stop_all
[ -z "$(main_pids)" ] && echo "$(ts) 0 main procs" || echo "$(ts) WARN main still present: $(echo $(main_pids))"
if [ ! -f "$MARKER" ]; then echo "$(ts) HELD: marker absent, NOT reopening (a boot would not re-latch)"; exit 1; fi
L=$(LOG); mark=$(wc -l < "$L" | tr -d ' ')
open -n "$APP" --args --silent
i=0; while [ $i -lt 30 ]; do
  tail -n +"$((mark+1))" "$(LOG)" 2>/dev/null | grep -q 'valve RE-LATCHED' && {
    echo "$(ts) RE-LATCHED, main pid $(echo $(main_pids))"
    launchctl bootstrap "gui/$UIDN" "$WD" 2>/dev/null; sleep 5
    launchctl bootstrap "gui/$UIDN" "$DP" 2>/dev/null; sleep 5
    echo "$(ts) jobs re-bootstrapped; main pids $(echo $(main_pids))"; exit 0; }
  sleep 2; i=$((i+1))
done
echo "$(ts) NO RE-LATCHED within 60 s -> stopping again (HELD, 0 main procs)"; stop_all; exit 1
