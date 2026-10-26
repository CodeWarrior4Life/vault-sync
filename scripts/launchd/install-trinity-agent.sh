#!/usr/bin/env bash
# install-trinity-agent.sh: install the SUPERVISED vault-sync LaunchAgent on a Mac (Trinity), and PROVE the result.
# Gate C2 on PR #29 (TKT-a6e5c227). Run it ONLY under a release/placement GO; nothing runs it automatically.
#
# Why this exists. The vault (non-managed) instance is guarded by the tauri single-instance SOCKET, not by the
# flock(2) config-dir lock (which guards MANAGED instances only). Whichever copy starts first wins; a later copy
# exits 0. So a Finder, Login Items or open-at-login copy that starts first makes the launchd agent's copy exit 0,
# and launchd (KeepAlive SuccessfulExit=false) never relaunches it: the daemon runs UNSUPERVISED. This script
# removes every other launch route, installs the one agent, and then requires a receipt.
#
# Steps (each logged; --dry-run prints them and changes nothing):
#   1. Park every OTHER LaunchAgent in the agents dir that launches the daemon (its plist names the app bundle or
#      the daemon binary): `launchctl bootout` it and MOVE the plist to <agents-dir>.vault-sync-parked/ (never
#      deleted, so it is reversible).
#   2. Remove the app from Login Items (System Events, by app name; absence is fine).
#   3. Boot out this agent if loaded, then stop (SIGTERM, then wait) every remaining daemon copy.
#   4. Install the agent plist (beside this script) and `launchctl bootstrap` it.
#   5. RECEIPT (also the only step with --verify-only): exactly ONE daemon pid; its parent is launchd (ppid 1); it is
#      this job's pid in `launchctl print`; and the job's environment carries VAULT_SYNC_SUPERVISED => 1.
#
# usage: install-trinity-agent.sh [--dry-run | --verify-only] [--plist PATH] [--agents-dir DIR] [--timeout SECS]
# exit:  0 receipt OK (or dry run done) | 2 usage / not macOS / missing plist | 20 more than one daemon pid |
#        21 the daemon's parent is not launchd, or it is not this job's pid | 22 VAULT_SYNC_SUPERVISED=1 not in the
#        job's environment | 23 no daemon running | 24 a daemon copy did not stop
set -uo pipefail
LABEL=com.lattice.vaultsync-daemon
APP_NAME="Nexus Vault Sync"
BIN=vault-sync-daemon
HERE="$(cd "$(dirname "$0")" && pwd)"
PLIST_SRC="$HERE/$LABEL.plist"
AGENTS="$HOME/Library/LaunchAgents"
TIMEOUT=30
MODE=install
while [ $# -gt 0 ]; do
  case "$1" in
    --dry-run) MODE=dry; shift ;;
    --verify-only) MODE=verify; shift ;;
    --plist) [ $# -ge 2 ] || exit 2; PLIST_SRC="$2"; shift 2 ;;
    --agents-dir) [ $# -ge 2 ] || exit 2; AGENTS="$2"; shift 2 ;;
    --timeout) [ $# -ge 2 ] || exit 2; TIMEOUT="$2"; shift 2 ;;
    -h|--help) sed -n '2,24p' "$0"; exit 2 ;;
    *) echo "install-trinity-agent: unknown argument: $1" >&2; exit 2 ;;
  esac
done
[ "$(uname -s)" = Darwin ] || { echo "install-trinity-agent: macOS only" >&2; exit 2; }
DOMAIN="gui/$(id -u)"
PARK="$AGENTS.vault-sync-parked"
log(){ echo "install-trinity-agent: $*"; }
run(){ if [ "$MODE" = dry ]; then log "DRY: $*"; else "$@"; fi; }
daemon_pids(){ pgrep -x "$BIN" 2>/dev/null | tr '\n' ' ' | sed 's/ *$//'; }

receipt(){
  local i=0 pids n pid ppid pr jobpid
  while :; do
    pids=$(daemon_pids); n=$(printf '%s\n' $pids | grep -c .)
    [ "$n" = 1 ] && break
    i=$((i+1)); [ "$i" -ge "$TIMEOUT" ] && break; sleep 1
  done
  if [ "$n" = 0 ]; then log "RECEIPT FAIL: no $BIN running"; return 23; fi
  if [ "$n" -gt 1 ]; then log "RECEIPT FAIL: $n $BIN pids ($pids): exactly one is required"; return 20; fi
  pid=$pids
  ppid=$(ps -o ppid= -p "$pid" 2>/dev/null | tr -d ' ')
  if [ "$ppid" != 1 ]; then log "RECEIPT FAIL: pid $pid has parent ${ppid:-?}, not launchd (1)"; return 21; fi
  pr=$(launchctl print "$DOMAIN/$LABEL" 2>/dev/null)
  jobpid=$(printf '%s\n' "$pr" | sed -n 's/^[[:space:]]*pid = \([0-9][0-9]*\).*/\1/p' | head -1)
  if [ "$jobpid" != "$pid" ]; then log "RECEIPT FAIL: pid $pid is not the pid of $DOMAIN/$LABEL (${jobpid:-none})"; return 21; fi
  if ! printf '%s\n' "$pr" | grep -Eq '^[[:space:]]*VAULT_SYNC_SUPERVISED => 1[[:space:]]*$'; then
    log "RECEIPT FAIL: $DOMAIN/$LABEL environment has no VAULT_SYNC_SUPERVISED => 1"; return 22
  fi
  log "RECEIPT OK: one $BIN pid=$pid, parent launchd (ppid 1), job $DOMAIN/$LABEL, VAULT_SYNC_SUPERVISED=1"
  return 0
}

if [ "$MODE" = verify ]; then receipt; exit $?; fi
[ -f "$PLIST_SRC" ] || { echo "install-trinity-agent: agent plist not found: $PLIST_SRC" >&2; exit 2; }

# 1. park every other agent that launches the daemon
for f in "$AGENTS"/*.plist; do
  [ -f "$f" ] || continue
  [ "$(basename "$f")" = "$LABEL.plist" ] && continue
  if grep -qE "$APP_NAME\.app|/$BIN</string>|>$BIN<" "$f" 2>/dev/null; then
    other=$(tr -d '\n\t' < "$f" | sed -n 's|.*<key>Label</key>[[:space:]]*<string>\([^<]*\)</string>.*|\1|p')
    log "competing launch route: $f (label ${other:-?}): booting out and parking it"
    [ -n "$other" ] && run launchctl bootout "$DOMAIN/$other"
    run mkdir -p "$PARK"
    run mv "$f" "$PARK/"
  fi
done
# 2. Login Items
log "removing '$APP_NAME' from Login Items (if present)"
run osascript -e "tell application \"System Events\" to delete (every login item whose name is \"$APP_NAME\")" || true
# 3. this agent out, every remaining copy stopped
run launchctl bootout "$DOMAIN/$LABEL" || true
for pid in $(daemon_pids); do
  log "stopping running $BIN pid $pid (SIGTERM)"
  run "${INSTALL_KILL:-kill}" -TERM "$pid"   # INSTALL_KILL: a test seam only (kill is a shell builtin)
done
if [ "$MODE" != dry ]; then
  i=0; while [ -n "$(daemon_pids)" ] && [ "$i" -lt "$TIMEOUT" ]; do sleep 1; i=$((i+1)); done
  [ -z "$(daemon_pids)" ] || { log "FAIL: $BIN still running ($(daemon_pids)) after SIGTERM"; exit 24; }
fi
# 4. install + bootstrap the one agent
run mkdir -p "$AGENTS"
run install -m 0644 "$PLIST_SRC" "$AGENTS/$LABEL.plist"
run launchctl bootstrap "$DOMAIN" "$AGENTS/$LABEL.plist"
[ "$MODE" = dry ] && { log "dry run: nothing changed"; exit 0; }
# 5. the receipt
receipt
exit $?
