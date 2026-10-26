#!/usr/bin/env bash
# install-trinity-agent.sh: install the SUPERVISED vault-sync LaunchAgent on a Mac (Trinity), and PROVE the result.
# Gate C2 on PR #29 (TKT-a6e5c227), hardened per the #29 follow-up (P1-P5). Run it ONLY under a release/placement GO;
# nothing runs it automatically.
#
# Why this exists. The vault (non-managed) instance is guarded by the tauri single-instance SOCKET, not by the
# flock(2) config-dir lock (which guards MANAGED instances only). Whichever copy starts first wins; a later copy
# exits 0. So a Finder, Login Items or open-at-login copy that starts first makes the launchd agent's copy exit 0,
# and launchd (KeepAlive SuccessfulExit=false) never relaunches it: the daemon runs UNSUPERVISED. This script removes
# the other launch routes, installs the one agent, and then requires a receipt.
#
# What is a "vault-sync daemon" here (P1, P2). EXACT paths only, never a substring:
#   --main-binary   the app's daemon binary (default /Applications/Nexus Vault Sync.app/Contents/MacOS/vault-sync-daemon).
#   --memory-binary the MANAGED (memory) instance's binary, if it is a different file (default: none). Agents and
#                   processes of the memory instance are NEVER parked or counted: it is a separate, supervised daemon.
#   A LaunchAgent competes only if its Program, or else its ProgramArguments[0], EQUALS the main binary and its
#   EnvironmentVariables do not set a NON-EMPTY NEXUS_SYNC_CONFIG_DIR (that would be a managed instance). Values are
#   read with `plutil -extract … raw`; a plist plutil cannot parse is skipped. A `.app.bak` path, a `pkill -x
#   vault-sync-daemon` agent, a wrapper that merely mentions the app, or a commented-out <key>Program</key> is not a
#   match. --memory-binary must differ from --main-binary (refused otherwise).
#   A running copy is a process whose full command line starts with the main binary path, and whose environment
#   (ps -E) does not carry NEXUS_SYNC_CONFIG_DIR. `pgrep -x` is NOT used: macOS truncates the process name to 16
#   characters (MAXCOMLEN) and `vault-sync-daemon` is 17, so `pgrep -x vault-sync-daemon` can miss it. The ps-based
#   match must be MEASURED on Trinity at the canary (`ps -axo pid=,command=` and `ps -E` output shapes).
#
# The pre-supervisor tray window (P5). Between the old app's quit and launchd's first start of the agent, a user
# click on the app (Finder, Dock, Spotlight) starts an UNSUPERVISED copy, and the agent's copy then exits 0 against
# its socket. The receipt catches it: the copy found is not launchd's job pid (exit 21), or two copies run (exit 20).
# The remedy is to quit that copy (tray Quit) and run `--verify-only` again after `launchctl kickstart -k`.
#
# Steps (each logged; --dry-run prints them and changes nothing):
#   1. Park every OTHER LaunchAgent in the agents dir that competes (exact match above): `launchctl bootout` it and
#      move its plist with `mv -n` (never clobber) to <agents-dir>.vault-sync-parked/, recording each move in a
#      manifest and printing the exact restore commands (P3). `--restore <manifest>` undoes the moves.
#   2. Remove the app from Login Items (System Events, by app name; absence is fine).
#   3. Boot out this agent if loaded. A vault-mode copy still running is NOT killed (P4): the script lists its pid
#      and exits 24, asking the operator to quit it (tray Quit) and rerun. Only --test-kill-cmd (tests) kills.
#   4. Install the agent plist (beside this script) and `launchctl bootstrap` it.
#   5. RECEIPT (also the only step with --verify-only): exactly ONE vault-mode daemon pid; its parent is launchd
#      (ppid 1); it is this job's pid in `launchctl print`; and the job's environment carries
#      VAULT_SYNC_SUPERVISED => 1.
#
# usage: install-trinity-agent.sh [--dry-run | --verify-only | --restore MANIFEST] [--plist PATH] [--agents-dir DIR]
#                                 [--main-binary PATH] [--memory-binary PATH] [--timeout SECS] [--test-kill-cmd PATH]
# exit:  0 receipt OK (or dry run / restore done) | 2 usage, not macOS, missing plist | 20 more than one daemon pid |
#        21 the daemon's parent is not launchd, or it is not this job's pid | 22 VAULT_SYNC_SUPERVISED=1 not in the
#        job's environment | 23 no daemon running | 24 a vault-mode copy is still running (quit it, then rerun) |
#        25 a restore could not put a plist back (the original path exists, or the parked file is missing) |
#        26 a park move did not happen (the restore commands for everything parked so far are printed first)
set -uo pipefail
LABEL=com.lattice.vaultsync-daemon
APP_NAME="Nexus Vault Sync"
MAIN_BIN="/Applications/$APP_NAME.app/Contents/MacOS/vault-sync-daemon"
MEM_BIN=""
HERE="$(cd "$(dirname "$0")" && pwd)"
PLIST_SRC="$HERE/$LABEL.plist"
AGENTS="$HOME/Library/LaunchAgents"
TIMEOUT=30
MODE=install
RESTORE=""
TEST_KILL=""
while [ $# -gt 0 ]; do
  case "$1" in
    --dry-run) MODE=dry; shift ;;
    --verify-only) MODE=verify; shift ;;
    --restore) [ $# -ge 2 ] || exit 2; MODE=restore; RESTORE="$2"; shift 2 ;;
    --plist) [ $# -ge 2 ] || exit 2; PLIST_SRC="$2"; shift 2 ;;
    --agents-dir) [ $# -ge 2 ] || exit 2; AGENTS="$2"; shift 2 ;;
    --main-binary) [ $# -ge 2 ] || exit 2; MAIN_BIN="$2"; shift 2 ;;
    --memory-binary) [ $# -ge 2 ] || exit 2; MEM_BIN="$2"; shift 2 ;;
    --timeout) [ $# -ge 2 ] || exit 2; TIMEOUT="$2"; shift 2 ;;
    --test-kill-cmd) [ $# -ge 2 ] || exit 2; TEST_KILL="$2"; shift 2 ;;   # tests only (P4)
    -h|--help) sed -n '2,49p' "$0"; exit 2 ;;
    *) echo "install-trinity-agent: unknown argument: $1" >&2; exit 2 ;;
  esac
done
[ "$(uname -s)" = Darwin ] || { echo "install-trinity-agent: macOS only" >&2; exit 2; }
# P1-c: the memory instance's binary must be a DIFFERENT file from the main binary, or the exclusion rules collapse.
if [ -n "$MEM_BIN" ] && [ "$MEM_BIN" = "$MAIN_BIN" ]; then
  echo "install-trinity-agent: REFUSED: --memory-binary equals --main-binary ($MAIN_BIN); give the memory instance's own binary path, or omit it" >&2
  exit 2
fi
DOMAIN="gui/$(id -u)"
PARK="$AGENTS.vault-sync-parked"
log(){ echo "install-trinity-agent: $*"; }
run(){ if [ "$MODE" = dry ]; then log "DRY: $*"; else "$@"; fi; }

# P1-a (#34 gate): plists are read with plutil, never a hand parser. The gate MEASURED that a commented-out
# <key>Program</key> was parked by the old line-based parser; plutil ignores comments. Any plist plutil cannot parse
# (`plutil -lint` fails) is SKIPPED, never parked. Missing key = empty value.
pl_get(){ plutil -extract "$1" raw -o - "$2" 2>/dev/null; }   # $1 key path  $2 plist
# Prints LABEL=, PROG= (Program, else ProgramArguments.0) and MANAGED=1|0 (EnvironmentVariables sets a NON-EMPTY
# NEXUS_SYNC_CONFIG_DIR; P1-b: an empty value is not a managed instance). Returns 1 on a parse error.
plist_facts(){
  local prog cfg
  plutil -lint "$1" >/dev/null 2>&1 || return 1
  prog=$(pl_get Program "$1"); [ -n "$prog" ] || prog=$(pl_get ProgramArguments.0 "$1")
  cfg=$(pl_get EnvironmentVariables.NEXUS_SYNC_CONFIG_DIR "$1")
  printf 'LABEL=%s\nPROG=%s\nMANAGED=%s\n' "$(pl_get Label "$1")" "$prog" "$([ -n "$cfg" ] && echo 1 || echo 0)"
}
# Vault-mode daemon pids: full command line starting with the exact main binary path (then a space or the end),
# not a managed instance (no NEXUS_SYNC_CONFIG_DIR in its environment), never the memory binary.
daemon_pids(){
  local pid cmd out=""
  while IFS= read -r line; do
    pid=$(printf '%s' "$line" | awk '{print $1}'); cmd=${line#*"$pid"}; cmd=${cmd# }
    case "$cmd" in
      "$MAIN_BIN"|"$MAIN_BIN "*) ;;
      *) continue ;;
    esac
    # P1-b: managed only with a NON-EMPTY value, matched as a whole env entry (start of line or after a space).
    if ps -E -ww -o command= -p "$pid" 2>/dev/null | grep -qE '(^|[[:space:]])NEXUS_SYNC_CONFIG_DIR=[^[:space:]]'; then continue; fi
    out="$out $pid"
  done < <(ps -axo pid=,command= 2>/dev/null | sed 's/^[[:space:]]*//')
  printf '%s' "${out# }"
}

receipt(){
  local i=0 pids n pid ppid pr jobpid
  while :; do
    pids=$(daemon_pids); n=$(printf '%s\n' $pids | grep -c .)
    [ "$n" = 1 ] && break
    i=$((i+1)); [ "$i" -ge "$TIMEOUT" ] && break; sleep 1
  done
  if [ "$n" = 0 ]; then log "RECEIPT FAIL: no vault-mode $MAIN_BIN running"; return 23; fi
  if [ "$n" -gt 1 ]; then log "RECEIPT FAIL: $n vault-mode daemon pids ($pids): exactly one is required"; return 20; fi
  pid=$pids
  ppid=$(ps -o ppid= -p "$pid" 2>/dev/null | tr -d ' ')
  if [ "$ppid" != 1 ]; then log "RECEIPT FAIL: pid $pid has parent ${ppid:-?}, not launchd (1)"; return 21; fi
  pr=$(launchctl print "$DOMAIN/$LABEL" 2>/dev/null)
  jobpid=$(printf '%s\n' "$pr" | sed -n 's/^[[:space:]]*pid = \([0-9][0-9]*\).*/\1/p' | head -1)
  if [ "$jobpid" != "$pid" ]; then
    log "RECEIPT FAIL: pid $pid is not the pid of $DOMAIN/$LABEL (${jobpid:-none}): an unsupervised copy (pre-supervisor tray window?)"
    return 21
  fi
  if ! printf '%s\n' "$pr" | grep -Eq '^[[:space:]]*VAULT_SYNC_SUPERVISED => 1[[:space:]]*$'; then
    log "RECEIPT FAIL: $DOMAIN/$LABEL environment has no VAULT_SYNC_SUPERVISED => 1"; return 22
  fi
  log "RECEIPT OK: one vault-mode daemon pid=$pid, parent launchd (ppid 1), job $DOMAIN/$LABEL, VAULT_SYNC_SUPERVISED=1"
  return 0
}

if [ "$MODE" = verify ]; then receipt; exit $?; fi
if [ "$MODE" = restore ]; then
  [ -f "$RESTORE" ] || { log "restore: manifest not found: $RESTORE"; exit 2; }
  rc=0
  while IFS=$'\t' read -r parked orig label; do
    [ -n "$parked" ] || continue
    if [ ! -f "$parked" ] || [ -e "$orig" ]; then
      log "restore FAIL: cannot move '$parked' back to '$orig' (parked file missing, or the original path exists)"; rc=25; continue
    fi
    mv -n "$parked" "$orig" && log "restored $orig"
    [ -n "$label" ] && launchctl bootstrap "$DOMAIN" "$orig" 2>/dev/null && log "bootstrapped $label"
  done < "$RESTORE"
  exit $rc
fi
[ -f "$PLIST_SRC" ] || { echo "install-trinity-agent: agent plist not found: $PLIST_SRC" >&2; exit 2; }

# 1. park every other agent that competes (exact Program / ProgramArguments[0] match; never a managed instance)
MANIFEST="$PARK/manifest-$(date +%Y%m%dT%H%M%S).tsv"
parked_any=0
# P3: the restore commands for everything parked so far; printed before ANY non-zero exit after parking starts.
print_restore(){
  [ "$parked_any" = 1 ] && [ "$MODE" != dry ] && [ -f "$MANIFEST" ] || return 0
  log "parked moves recorded in $MANIFEST. To undo:"
  log "  $0 --restore '$MANIFEST' --agents-dir '$AGENTS'"
  while IFS=$'\t' read -r parked orig label; do
    log "  or by hand: mv -n '$parked' '$orig' && launchctl bootstrap $DOMAIN '$orig'"
  done < "$MANIFEST"
}
die_after_park(){ print_restore; exit "$1"; }
for f in "$AGENTS"/*.plist; do
  [ -f "$f" ] || continue
  [ "$(basename "$f")" = "$LABEL.plist" ] && continue
  if ! facts=$(plist_facts "$f"); then log "skipping $f: plutil cannot parse it (never parked)"; continue; fi
  prog=$(printf '%s\n' "$facts" | sed -n 's/^PROG=//p'); other=$(printf '%s\n' "$facts" | sed -n 's/^LABEL=//p')
  managed=$(printf '%s\n' "$facts" | sed -n 's/^MANAGED=//p')
  [ "$prog" = "$MAIN_BIN" ] || continue
  [ -n "$MEM_BIN" ] && [ "$prog" = "$MEM_BIN" ] && continue
  if [ "$managed" = 1 ]; then log "not parking $f: a managed instance (NEXUS_SYNC_CONFIG_DIR set)"; continue; fi
  dest="$PARK/$(basename "$f")"
  if [ -e "$dest" ]; then log "FAIL: $dest already exists; not clobbering it (mv -n). Resolve by hand, then rerun."; die_after_park 2; fi
  log "competing launch route: $f (label ${other:-?}, program $prog): booting out and parking it"
  [ -n "$other" ] && run launchctl bootout "$DOMAIN/$other"
  run mkdir -p "$PARK"
  run mv -n "$f" "$dest"
  # P3: `mv -n` may "succeed" without moving (an existing target); check the result, not only the return code.
  if [ "$MODE" != dry ] && { [ ! -e "$dest" ] || [ -e "$f" ]; }; then
    log "FAIL: could not park $f to $dest (mv -n did not move it); stopping"; die_after_park 26
  fi
  if [ "$MODE" != dry ]; then printf '%s\t%s\t%s\n' "$dest" "$f" "$other" >> "$MANIFEST"; fi
  parked_any=1
done
print_restore
# 2. Login Items
log "removing '$APP_NAME' from Login Items (if present)"
run osascript -e "tell application \"System Events\" to delete (every login item whose name is \"$APP_NAME\")" || true
# 3. this agent out; a vault-mode copy still running is never killed by default (P4)
run launchctl bootout "$DOMAIN/$LABEL" || true
left=$(daemon_pids)
if [ -n "$left" ]; then
  if [ -n "$TEST_KILL" ]; then
    for pid in $left; do log "TEST: stopping pid $pid via --test-kill-cmd"; run "$TEST_KILL" -TERM "$pid"; done
  elif [ "$MODE" != dry ]; then
    log "FAIL: a vault-mode copy is still running (pid $left). Quit it from its tray menu (Quit), then rerun. This script never kills."
    exit 24   # the restore commands were already printed after step 1
  else
    log "DRY: a vault-mode copy is running (pid $left); a real run would stop here (exit 24) and ask for a tray Quit"
  fi
fi
# 4. install + bootstrap the one agent
run mkdir -p "$AGENTS"
run install -m 0644 "$PLIST_SRC" "$AGENTS/$LABEL.plist"
run launchctl bootstrap "$DOMAIN" "$AGENTS/$LABEL.plist"
[ "$MODE" = dry ] && { log "dry run: nothing changed"; exit 0; }
# 5. the receipt
receipt
exit $?
