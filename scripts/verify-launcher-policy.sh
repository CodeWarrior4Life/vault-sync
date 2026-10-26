#!/usr/bin/env bash
# Unit CS-3 (TKT-b6d8c4ba) T-CS3-24, gates C5 and C10 (and E1), hardened by
# gates K2, L1 and L2: verify that the launcher supervising the vault-sync daemon
# restarts it on exit 70 (a Required task ended, CS-3) AND on exit 74
# (custody lost, CS-2), prevents at most 78 (the CS-4 refusal), and
# supervises the daemon binary itself. Read-only: it never starts, stops,
# enables or edits anything.
#
# systemd: the EFFECTIVE values come from `systemctl --user show` (so drop-ins
# and masking are accounted for), never from parsing unit files, and the unit
# must be running (ActiveState=active, MainPID>0; L1). launchd: the agent
# plist, with interpreter wrappers resolved to their script, and the LIVE
# agent state from `launchctl print` / `print-disabled` (loaded, not
# disabled, a PID; L1). A pass-through wrapper (cage) is accepted only when
# its exact absolute path is given with --via (L2); there is no default
# wrapper allowlist.
#
# Usage:
#   verify-launcher-policy.sh [--name NAME] [--daemon PATH] [--via PATH]...
#                             [--systemd-show FILE] [--agent-dir DIR]...
#                             [--launchctl-print FILE]
#                             [--launchctl-disabled FILE]
#
#   --name          the systemd unit (NAME.service) or launchd agent
#                   (NAME.plist, or a plist whose Label is NAME). Default:
#                   com.lattice.vaultsync-daemon on macOS, nexus-vault-sync
#                   elsewhere.
#   --daemon        the daemon binary's absolute path (allowlist). Default:
#                   any path whose basename is `vault-sync-daemon`.
#   --via           an exact absolute path of a pass-through wrapper (e.g.
#                   /usr/bin/cage) that ExecStart may run; repeatable. The
#                   daemon must still be identified right after it: the next
#                   non-option argument is the daemon or its AppImage, or a
#                   script whose last command execs it. NOTE: --via does not
#                   make a host E1-clean; E1 also needs the 7.1 #2 journald
#                   receipts (status=70 and status=74) on that host.
#   --systemd-show  fixture mode: read the `systemctl --user show` output
#                   from FILE instead of running systemctl (CI needs no live
#                   units).
#   --launchctl-print, --launchctl-disabled
#                   fixture mode: read the `launchctl print gui/$UID/NAME` and
#                   `launchctl print-disabled gui/$UID` output from FILEs
#                   (an empty print FILE means the agent is not loaded).
#   --agent-dir     a launchd agent directory. Default (macOS only):
#                   ~/Library/LaunchAgents /Library/LaunchAgents
#                   /Library/LaunchDaemons.
#   With no --systemd-show and no --agent-dir, live mode: systemd (via
#   systemctl) everywhere except macOS, launchd on macOS.
#
# Informational (TKT-a6e5c227; never changes the exit code): one line names
# the supervisor's VAULT_SYNC_SUPERVISED value (systemd Environment=, launchd
# EnvironmentVariables). Only `1` makes a daemon restart exit 70 for the
# supervisor to relaunch instead of self-spawning through app.restart().
#
# Exit codes (spec section 4.3 item 11; unchanged by K2):
#   0   pass: a supervisor exists, restarts on both 70 and 74, prevents at
#       most 78, and runs the daemon binary directly (or, on launchd, through
#       an interpreter script whose last command `exec`s the daemon)
#   2   usage or parse error (including an unknown exit-status name)
#   10  no supervisor: no unit and no agent, a masked or not-found unit, a
#       unit that is not running (ActiveState not active, MainPID 0), a
#       launchd agent with Disabled=true, not loaded (booted out), disabled in
#       launchctl print-disabled, or with no PID
#   11  the policy does not restart on 70 and 74: Restart=no, on-success,
#       on-abnormal, on-watchdog, on-abort or unset; SuccessExitStatus making
#       70 or 74 (numeric or symbolic, e.g. SOFTWARE IOERR) a success; a
#       plist with no KeepAlive, or a KeepAlive that does not restart on a
#       failed exit
#   12  RestartPreventExitStatus names any status other than 78 (70 and 74
#       included; names are mapped, so CONFIG is 78)
#   13  the supervised program is not the daemon: `open`, `env`, a shell or
#       any other binary or wrapper (unless its exact path is given with
#       --via and the daemon follows it), or a script that does not end by
#       `exec`ing the daemon
set -uo pipefail

usage() {
  sed -n '/^# Unit CS-3/,/^set -uo pipefail/p' "$0" | sed '$d' >&2
  exit 2
}

name=""
daemon=""
show_file=""
agent_dirs=()
via=()
lc_print=""
lc_disabled=""
while [ $# -gt 0 ]; do
  case "$1" in
    --name) [ $# -ge 2 ] || usage; name="$2"; shift 2 ;;
    --daemon) [ $# -ge 2 ] || usage; daemon="$2"; shift 2 ;;
    --systemd-show) [ $# -ge 2 ] || usage; show_file="$2"; shift 2 ;;
    --agent-dir) [ $# -ge 2 ] || usage; agent_dirs+=("$2"); shift 2 ;;
    --via)
      [ $# -ge 2 ] || usage
      case "$2" in /*) ;; *) echo "verify-launcher-policy: --via needs an absolute path: $2" >&2; exit 2 ;; esac
      via+=("$2"); shift 2 ;;
    --launchctl-print) [ $# -ge 2 ] || usage; lc_print="$2"; shift 2 ;;
    --launchctl-disabled) [ $# -ge 2 ] || usage; lc_disabled="$2"; shift 2 ;;
    -h|--help) usage ;;
    *) echo "verify-launcher-policy: unknown argument: $1" >&2; usage ;;
  esac
done

os=$(uname -s)
if [ -z "$name" ]; then
  if [ "$os" = "Darwin" ]; then name="com.lattice.vaultsync-daemon"; else name="nexus-vault-sync"; fi
fi
use_systemd=0
if [ -n "$show_file" ]; then
  use_systemd=1
elif [ ${#agent_dirs[@]} -eq 0 ]; then
  if [ "$os" = "Darwin" ]; then
    agent_dirs=("$HOME/Library/LaunchAgents" /Library/LaunchAgents /Library/LaunchDaemons)
  else
    use_systemd=1
  fi
fi

# The daemon allowlist: an exact path when --daemon is given, else any path
# whose basename is vault-sync-daemon.
is_daemon() {
  local p="$1"
  [ -n "$p" ] || return 1
  if [ -n "$daemon" ]; then
    [ "$p" = "$daemon" ]
  else
    [ "$(basename -- "$p")" = "vault-sync-daemon" ]
  fi
}

# The daemon's AppImage: the exact --daemon path when given, else an
# `.AppImage` whose basename names vault-sync.
is_appimage() {
  local p="$1" b
  [ -n "$p" ] || return 1
  if [ -n "$daemon" ]; then
    [ "$p" = "$daemon" ]
    return
  fi
  b=$(basename -- "$p" | tr '[:upper:]' '[:lower:]')
  case "$b" in
    *vault-sync*.appimage) return 0 ;;
    *) return 1 ;;
  esac
}

# L2: true iff $1 is one of the exact --via paths. No default list exists.
is_via() {
  local v
  [ ${#via[@]} -gt 0 ] || return 1
  for v in "${via[@]}"; do
    [ "$1" = "$v" ] && return 0
  done
  return 1
}

# TKT-a6e5c227, informational only: report the supervisor's
# VAULT_SYNC_SUPERVISED (only `1` means supervised). Never affects the exit.
supervised_note() {
  local who="$1" v="$2"
  if [ "$v" = "1" ]; then
    echo "$who: VAULT_SYNC_SUPERVISED=1 (a daemon restart exits 70 for the supervisor to relaunch)"
  else
    echo "$who: VAULT_SYNC_SUPERVISED=${v:-<unset>} (informational, TKT-a6e5c227: without =1 a daemon restart self-spawns through app.restart() and exits 0)"
  fi
}

# The unit's effective Environment (systemd), for `exec "$VAR"` targets.
unit_env=""
env_value() {
  printf '%s\n' "$unit_env" | tr ' ' '\n' | sed -n "s/^$1=//p" | tail -1
}

# Resolve a script's exec target: a literal path, or `$VAR` / `${VAR}`
# looked up in the unit Environment, else in the script's own last
# `VAR=...` assignment (one level: a literal, or `${OTHER...}` from the
# unit Environment).
resolve_target() {
  local t="$1" script="$2" var val inner
  case "$t" in
    \$\{*\}) var=${t#\$\{}; var=${var%\}} ;;
    \$*) var=${t#\$} ;;
    *) printf '%s\n' "$t"; return 0 ;;
  esac
  val=$(env_value "$var")
  if [ -z "$val" ]; then
    val=$(grep -E "^[[:space:]]*(export[[:space:]]+|readonly[[:space:]]+)?$var=" "$script" | tail -1 | sed -E "s/^[[:space:]]*(export[[:space:]]+|readonly[[:space:]]+)?$var=//" | tr -d '"'"'")
    case "$val" in
      \$\{*)
        inner=${val#\$\{}; inner=${inner%%[:\}]*}
        val=$(env_value "$inner") ;;
    esac
  fi
  printf '%s\n' "$val"
}

# Exit-status names systemd accepts for the BSD sysexits range.
status_number() {
  case "$1" in
    ''|*[!0-9]*) ;;
    *) echo "$1"; return 0 ;;
  esac
  case "$1" in
    USAGE) echo 64 ;; DATAERR) echo 65 ;; NOINPUT) echo 66 ;; NOUSER) echo 67 ;;
    NOHOST) echo 68 ;; UNAVAILABLE) echo 69 ;; SOFTWARE) echo 70 ;; OSERR) echo 71 ;;
    OSFILE) echo 72 ;; CANTCREAT) echo 73 ;; IOERR) echo 74 ;; TEMPFAIL) echo 75 ;;
    PROTOCOL) echo 76 ;; NOPERM) echo 77 ;; CONFIG) echo 78 ;;
    *) return 1 ;;
  esac
}

is_signal() {
  case "$1" in
    SIG*|HUP|INT|QUIT|ILL|TRAP|ABRT|BUS|FPE|KILL|USR1|SEGV|USR2|PIPE|ALRM|TERM|STKFLT|CHLD|CONT|STOP|TSTP|TTIN|TTOU|URG|XCPU|XFSZ|VTALRM|PROF|WINCH|IO|PWR|SYS) return 0 ;;
    *) return 1 ;;
  esac
}

# ---------------------------------------------------------------------------
# systemd: the effective properties.
# ---------------------------------------------------------------------------
check_systemd() {
  local show
  if [ -n "$show_file" ]; then
    show=$(cat -- "$show_file") || { echo "cannot read $show_file" >&2; return 2; }
  else
    if ! command -v systemctl >/dev/null 2>&1; then return 10; fi
    show=$(systemctl --user show "$name.service" \
      -p LoadState -p UnitFileState -p Restart -p RestartPreventExitStatus \
      -p SuccessExitStatus -p ExecStart -p FragmentPath \
      -p ActiveState -p MainPID -p Environment 2>/dev/null) || return 10
  fi
  prop() { printf '%s\n' "$show" | sed -n "s/^$1=//p" | tail -1; }
  local load ufs restart prevent success exec active mainpid
  load=$(prop LoadState); ufs=$(prop UnitFileState)
  active=$(prop ActiveState); mainpid=$(prop MainPID)
  unit_env=$(prop Environment)
  restart=$(prop Restart); prevent=$(prop RestartPreventExitStatus)
  success=$(prop SuccessExitStatus); exec=$(prop ExecStart)
  echo "systemd $name: LoadState=${load:-?} UnitFileState=${ufs:-?} ActiveState=${active:-?} MainPID=${mainpid:-?} Restart=${restart:-?} RestartPreventExitStatus=[$prevent] SuccessExitStatus=[$success]"
  supervised_note "systemd $name" "$(env_value VAULT_SYNC_SUPERVISED)"
  case "$load" in
    loaded) ;;
    masked)
      echo "systemd $name: the unit is masked; nothing supervises the daemon" >&2
      return 10 ;;
    ''|not-found)
      return 10 ;;
    *)
      echo "systemd $name: LoadState=$load" >&2
      return 10 ;;
  esac
  if [ "$ufs" = "masked" ] || [ "$ufs" = "masked-runtime" ]; then
    echo "systemd $name: UnitFileState=$ufs" >&2
    return 10
  fi
  # L1: the live state. A loaded but stopped unit supervises nothing.
  if [ "$active" != "active" ]; then
    echo "systemd $name: ActiveState=${active:-<unset>}; the unit is not running, so nothing supervises the daemon" >&2
    return 10
  fi
  case "$mainpid" in
    ''|*[!0-9]*|0)
      echo "systemd $name: MainPID=${mainpid:-<unset>}; no main process" >&2
      return 10 ;;
  esac
  case "$restart" in
    always|on-failure) ;;
    *)
      echo "systemd $name: Restart=${restart:-<unset>} does not restart on exit 70 and 74" >&2
      return 11 ;;
  esac
  local s n
  for s in $success; do
    if n=$(status_number "$s"); then
      if [ "$n" = "70" ] || [ "$n" = "74" ]; then
        echo "systemd $name: SuccessExitStatus makes $s ($n) a success; it is never restarted" >&2
        return 11
      fi
    elif ! is_signal "$s"; then
      echo "systemd $name: unknown SuccessExitStatus entry $s (fail closed)" >&2
      return 2
    fi
  done
  for s in $prevent; do
    n=$(status_number "$s") || n="$s"
    if [ "$n" != "78" ]; then
      echo "systemd $name: RestartPreventExitStatus names $s (only 78 may be listed)" >&2
      return 12
    fi
  done
  # ExecStart={ path=/x/y ; argv[]=/x/y ... ; ... }
  local path
  path=$(printf '%s\n' "$exec" | sed -n 's/.*path=\([^ ;]*\).*/\1/p' | head -1)
  if [ -z "$path" ]; then
    echo "systemd $name: no ExecStart path in the effective unit" >&2
    return 2
  fi
  if is_daemon "$path"; then
    return 0
  fi
  # L2: a pass-through wrapper, ONLY at an exact --via path.
  if is_via "$path"; then
    local argv next a
    argv=$(printf '%s\n' "$exec" | sed -n 's/.*argv\[\]=\([^;]*\) ;.*/\1/p' | head -1)
    next=""
    set -f
    # shellcheck disable=SC2086
    set -- $argv
    set +f
    shift  # argv[0], the wrapper
    for a in "$@"; do
      case "$a" in
        -*) continue ;;
        *) next="$a"; break ;;
      esac
    done
    if is_daemon "$next" || is_appimage "$next"; then
      echo "systemd $name: wrapper $path accepted via --via (exact path); the daemon follows it: $next"
      return 0
    fi
    if [ -n "$next" ] && [ -f "$next" ] && script_execs_daemon "$next" appimage; then
      echo "systemd $name: wrapper $path accepted via --via (exact path); the daemon follows it through $next"
      return 0
    fi
    echo "systemd $name: wrapper $path is allowed via --via, but no daemon follows it (next: ${next:-<none>})" >&2
    return 13
  fi
  echo "systemd $name: ExecStart runs $path, not the daemon binary; exit codes are not the daemon's (a pass-through wrapper needs its exact path given with --via)" >&2
  return 13
}

# ---------------------------------------------------------------------------
# launchd: an agent plist (XML; binary plists are converted with plutil).
# ---------------------------------------------------------------------------
plist_tags() {
  local f="$1"
  if head -c 6 "$f" | grep -q '^bplist'; then
    if command -v plutil >/dev/null 2>&1; then
      plutil -convert xml1 -o - "$f" || return 2
    else
      echo "binary plist $f and no plutil" >&2
      return 2
    fi
  else
    cat "$f"
  fi | tr -d '\n\r\t' | sed 's/>[[:space:]]*</>\
</g'
}

find_agent() {
  local d f
  for d in "${agent_dirs[@]}"; do
    [ -d "$d" ] || continue
    if [ -f "$d/$name.plist" ]; then echo "$d/$name.plist"; return 0; fi
    for f in "$d"/*.plist; do
      [ -f "$f" ] || continue
      if plist_tags "$f" 2>/dev/null | awk -v n="$name" 'prev=="<key>Label</key>" && $0=="<string>" n "</string>" {found=1} {prev=$0} END {exit found?0:1}'; then
        echo "$f"; return 0
      fi
    done
  done
  return 1
}

is_interpreter() {
  case "$(basename -- "$1")" in
    sh|bash|zsh|dash|ksh|mksh) return 0 ;;
    *) return 1 ;;
  esac
}

# 0 iff the script's last command is `exec <daemon> ...` (with a second
# argument `appimage`, the daemon's AppImage also counts).
script_execs_daemon() {
  local script="$1" allow="${2:-}" last target
  [ -f "$script" ] || return 1
  last=$(grep -v '^[[:space:]]*#' "$script" | grep -v '^[[:space:]]*$' | tail -1 | sed 's/^[[:space:]]*//')
  case "$last" in
    exec\ *) ;;
    *) echo "launchd $name: script $script does not end by exec'ing the daemon (last command: $last)" >&2; return 1 ;;
  esac
  target=$(printf '%s\n' "$last" | awk '{print $2}' | tr -d '"'"'")
  target=$(resolve_target "$target" "$script")
  if [ "$allow" = "appimage" ] && is_appimage "$target"; then
    return 0
  fi
  if ! is_daemon "$target"; then
    echo "launchd $name: script $script execs $target, not the daemon binary" >&2
    return 1
  fi
  return 0
}

check_launchd() {
  local plist
  plist=$(find_agent) || return 10
  local parsed
  parsed=$(plist_tags "$plist") || return 2
  # Emits KEEPALIVE=, DISABLED=, PROG0=, PROG1= and SUPERVISED=.
  local out
  out=$(printf '%s\n' "$parsed" | awk '
    function text(t) { sub(/^<string>/, "", t); sub(/<\/string>$/, "", t); return t }
    BEGIN { ka = "none"; dis = "false"; p0 = ""; p1 = ""; nargs = 0; depth = 0; kadepth = -1; envdepth = -1; sup = ""; pending = "" }
    {
      t = $0
      if (t ~ /^<dict>$/) { depth++ }
      if (t ~ /^<\/dict>$/) { if (depth == kadepth) { kadepth = -1 } ; if (depth == envdepth) { envdepth = -1 } ; depth--; next }
      if (pending == "Env") { pending = ""; if (t == "<dict>") envdepth = depth; next }
      if (pending == "Supervised") { pending = ""; if (t ~ /^<string>/) sup = text(t); next }
      if (pending == "KeepAlive") {
        pending = ""
        if (t == "<true/>") ka = "true"
        else if (t == "<false/>") ka = "false"
        else if (t == "<dict>") { ka = "other"; kadepth = depth }
        next
      }
      if (pending == "Disabled") { pending = ""; if (t == "<true/>") dis = "true"; next }
      if (pending == "SuccessfulExit") {
        pending = ""
        if (t == "<false/>") ka = "failure"; else if (t == "<true/>" && ka != "failure") ka = "success"
        next
      }
      if (pending == "Program") { pending = ""; if (t ~ /^<string>/) p0 = text(t); next }
      if (pending == "ProgramArguments") {
        if (t == "<array>") next
        if (t ~ /^<string>/) {
          nargs++
          if (nargs == 1 && p0 == "") p0 = text(t)
          if (nargs == 2) p1 = text(t)
          next
        }
        pending = ""
        next
      }
      if (t == "<key>EnvironmentVariables</key>" && depth == 1) { pending = "Env"; next }
      if (t == "<key>VAULT_SYNC_SUPERVISED</key>" && envdepth >= 0 && depth == envdepth) { pending = "Supervised"; next }
      if (t == "<key>KeepAlive</key>" && depth == 1) { pending = "KeepAlive"; next }
      if (t == "<key>Disabled</key>" && depth == 1) { pending = "Disabled"; next }
      if (t == "<key>SuccessfulExit</key>" && kadepth >= 0 && depth == kadepth) { pending = "SuccessfulExit"; next }
      if (t == "<key>Program</key>" && depth == 1) { pending = "Program"; p0 = ""; next }
      if (t == "<key>ProgramArguments</key>" && depth == 1) { pending = "ProgramArguments"; next }
    }
    END { print "KEEPALIVE=" ka; print "DISABLED=" dis; print "PROG0=" p0; print "PROG1=" p1; print "SUPERVISED=" sup }
  ')
  local ka dis p0 p1 sup
  ka=$(printf '%s\n' "$out" | sed -n 's/^KEEPALIVE=//p')
  dis=$(printf '%s\n' "$out" | sed -n 's/^DISABLED=//p')
  p0=$(printf '%s\n' "$out" | sed -n 's/^PROG0=//p')
  p1=$(printf '%s\n' "$out" | sed -n 's/^PROG1=//p')
  sup=$(printf '%s\n' "$out" | sed -n 's/^SUPERVISED=//p')
  echo "launchd $name: plist=$plist Disabled=$dis KeepAlive=$ka program=${p0:-<none>} arg1=${p1:-<none>}"
  supervised_note "launchd $name" "$sup"
  if [ "$dis" = "true" ]; then
    echo "launchd $name: Disabled=true; launchd does not run or supervise the agent" >&2
    return 10
  fi
  # L1: the live agent state.
  local print disabled pid
  if [ -n "$lc_print" ]; then
    print=$(cat -- "$lc_print" 2>/dev/null) || print=""
  else
    print=$(launchctl print "gui/$(id -u)/$name" 2>/dev/null) || print=""
  fi
  if [ -z "$print" ]; then
    echo "launchd $name: launchctl print found no loaded agent (booted out or never bootstrapped)" >&2
    return 10
  fi
  if [ -n "$lc_disabled" ]; then
    disabled=$(cat -- "$lc_disabled" 2>/dev/null) || disabled=""
  elif [ -n "$lc_print" ]; then
    disabled=""
  else
    disabled=$(launchctl print-disabled "gui/$(id -u)" 2>/dev/null) || disabled=""
  fi
  if printf '%s\n' "$disabled" | grep -Eq "\"$name\"[[:space:]]*=>[[:space:]]*(disabled|true)"; then
    echo "launchd $name: launchctl print-disabled lists the agent as disabled" >&2
    return 10
  fi
  pid=$(printf '%s\n' "$print" | sed -n 's/^[[:space:]]*pid = \([0-9][0-9]*\).*/\1/p' | head -1)
  if [ -z "$pid" ] || [ "$pid" -eq 0 ]; then
    echo "launchd $name: the loaded agent has no PID (not running)" >&2
    return 10
  fi
  if [ -z "$p0" ]; then
    echo "launchd $name: no Program or ProgramArguments" >&2
    return 2
  fi
  case "$ka" in
    true|failure) ;;
    *)
      echo "launchd $name: KeepAlive=$ka does not restart a failed exit (70/74)" >&2
      return 11 ;;
  esac
  # The allowlist: the daemon binary itself, or an interpreter script (run
  # by an interpreter, or directly via its shebang) whose last command
  # `exec`s the daemon. `open`, `env` and every other program fail.
  if is_daemon "$p0"; then
    return 0
  fi
  if is_interpreter "$p0"; then
    if script_execs_daemon "$p1"; then return 0; fi
    return 13
  fi
  if [ -f "$p0" ] && head -c 2 "$p0" | grep -q '^#!'; then
    if script_execs_daemon "$p0"; then return 0; fi
    return 13
  fi
  echo "launchd $name: the agent runs $p0, not the daemon binary; launchd supervises that program, not the daemon's exit" >&2
  return 13
}

found=0
worst=0
if [ $use_systemd -eq 1 ]; then
  check_systemd; rc=$?
  if [ $rc -ne 10 ]; then found=1; fi
  [ $rc -ne 0 ] && [ $worst -eq 0 ] && worst=$rc
fi
if [ ${#agent_dirs[@]} -gt 0 ]; then
  check_launchd; rc=$?
  if [ $rc -ne 10 ]; then found=1; fi
  [ $rc -ne 0 ] && [ $worst -eq 0 ] && worst=$rc
fi
if [ $found -eq 0 ] && [ $worst -eq 0 ]; then
  echo "verify-launcher-policy: no systemd unit and no launchd agent found for $name" >&2
  exit 10
fi
if [ $worst -eq 0 ]; then
  echo "verify-launcher-policy: PASS ($name supervises the daemon, restarts on 70 and 74, prevents at most 78)"
fi
exit $worst
