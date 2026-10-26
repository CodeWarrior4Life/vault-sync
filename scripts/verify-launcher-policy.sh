#!/usr/bin/env bash
# Unit CS-3 (TKT-b6d8c4ba) T-CS3-24, gates C5 and C10 (and E1), hardened by
# gate K2: verify that the launcher supervising the vault-sync daemon
# restarts it on exit 70 (a Required task ended, CS-3) AND on exit 74
# (custody lost, CS-2), prevents at most 78 (the CS-4 refusal), and
# supervises the daemon binary itself. Read-only: it never starts, stops,
# enables or edits anything.
#
# systemd: the EFFECTIVE values come from `systemctl --user show` (so drop-ins
# and masking are accounted for), never from parsing unit files. launchd: the
# agent plist, with interpreter wrappers resolved to their script.
#
# Usage:
#   verify-launcher-policy.sh [--name NAME] [--daemon PATH]
#                             [--systemd-show FILE] [--agent-dir DIR]...
#
#   --name          the systemd unit (NAME.service) or launchd agent
#                   (NAME.plist, or a plist whose Label is NAME). Default:
#                   com.lattice.vaultsync-daemon on macOS, nexus-vault-sync
#                   elsewhere.
#   --daemon        the daemon binary's absolute path (allowlist). Default:
#                   any path whose basename is `vault-sync-daemon`.
#   --systemd-show  fixture mode: read the `systemctl --user show` output
#                   from FILE instead of running systemctl (CI needs no live
#                   units).
#   --agent-dir     a launchd agent directory. Default (macOS only):
#                   ~/Library/LaunchAgents /Library/LaunchAgents
#                   /Library/LaunchDaemons.
#   With no --systemd-show and no --agent-dir, live mode: systemd (via
#   systemctl) everywhere except macOS, launchd on macOS.
#
# Exit codes (spec section 4.3 item 11; unchanged by K2):
#   0   pass: a supervisor exists, restarts on both 70 and 74, prevents at
#       most 78, and runs the daemon binary directly (or, on launchd, through
#       an interpreter script whose last command `exec`s the daemon)
#   2   usage or parse error (including an unknown exit-status name)
#   10  no supervisor: no unit and no agent, a masked or not-found unit, or a
#       launchd agent with Disabled=true
#   11  the policy does not restart on 70 and 74: Restart=no, on-success,
#       on-abnormal, on-watchdog, on-abort or unset; SuccessExitStatus making
#       70 or 74 (numeric or symbolic, e.g. SOFTWARE IOERR) a success; a
#       plist with no KeepAlive, or a KeepAlive that does not restart on a
#       failed exit
#   12  RestartPreventExitStatus names any status other than 78 (70 and 74
#       included; names are mapped, so CONFIG is 78)
#   13  the supervised program is not the daemon: `open`, `env`, a shell or
#       any other binary or wrapper, or a script that does not end by
#       `exec`ing the daemon
set -uo pipefail

usage() {
  sed -n '2,49p' "$0" >&2
  exit 2
}

name=""
daemon=""
show_file=""
agent_dirs=()
while [ $# -gt 0 ]; do
  case "$1" in
    --name) [ $# -ge 2 ] || usage; name="$2"; shift 2 ;;
    --daemon) [ $# -ge 2 ] || usage; daemon="$2"; shift 2 ;;
    --systemd-show) [ $# -ge 2 ] || usage; show_file="$2"; shift 2 ;;
    --agent-dir) [ $# -ge 2 ] || usage; agent_dirs+=("$2"); shift 2 ;;
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
      -p SuccessExitStatus -p ExecStart -p FragmentPath 2>/dev/null) || return 10
  fi
  prop() { printf '%s\n' "$show" | sed -n "s/^$1=//p" | tail -1; }
  local load ufs restart prevent success exec
  load=$(prop LoadState); ufs=$(prop UnitFileState)
  restart=$(prop Restart); prevent=$(prop RestartPreventExitStatus)
  success=$(prop SuccessExitStatus); exec=$(prop ExecStart)
  echo "systemd $name: LoadState=${load:-?} UnitFileState=${ufs:-?} Restart=${restart:-?} RestartPreventExitStatus=[$prevent] SuccessExitStatus=[$success]"
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
  if ! is_daemon "$path"; then
    echo "systemd $name: ExecStart runs $path, not the daemon binary; exit codes are not the daemon's" >&2
    return 13
  fi
  return 0
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

# 0 iff the script's last command is `exec <daemon> ...`.
script_execs_daemon() {
  local script="$1" last target
  [ -f "$script" ] || return 1
  last=$(grep -v '^[[:space:]]*#' "$script" | grep -v '^[[:space:]]*$' | tail -1 | sed 's/^[[:space:]]*//')
  case "$last" in
    exec\ *) ;;
    *) echo "launchd $name: script $script does not end by exec'ing the daemon (last command: $last)" >&2; return 1 ;;
  esac
  target=$(printf '%s\n' "$last" | awk '{print $2}' | tr -d '"'"'")
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
  # Emits KEEPALIVE=, DISABLED=, PROG0= and PROG1=.
  local out
  out=$(printf '%s\n' "$parsed" | awk '
    function text(t) { sub(/^<string>/, "", t); sub(/<\/string>$/, "", t); return t }
    BEGIN { ka = "none"; dis = "false"; p0 = ""; p1 = ""; nargs = 0; depth = 0; kadepth = -1; pending = "" }
    {
      t = $0
      if (t ~ /^<dict>$/) { depth++ }
      if (t ~ /^<\/dict>$/) { if (depth == kadepth) { kadepth = -1 } ; depth--; next }
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
      if (t == "<key>KeepAlive</key>" && depth == 1) { pending = "KeepAlive"; next }
      if (t == "<key>Disabled</key>" && depth == 1) { pending = "Disabled"; next }
      if (t == "<key>SuccessfulExit</key>" && kadepth >= 0 && depth == kadepth) { pending = "SuccessfulExit"; next }
      if (t == "<key>Program</key>" && depth == 1) { pending = "Program"; p0 = ""; next }
      if (t == "<key>ProgramArguments</key>" && depth == 1) { pending = "ProgramArguments"; next }
    }
    END { print "KEEPALIVE=" ka; print "DISABLED=" dis; print "PROG0=" p0; print "PROG1=" p1 }
  ')
  local ka dis p0 p1
  ka=$(printf '%s\n' "$out" | sed -n 's/^KEEPALIVE=//p')
  dis=$(printf '%s\n' "$out" | sed -n 's/^DISABLED=//p')
  p0=$(printf '%s\n' "$out" | sed -n 's/^PROG0=//p')
  p1=$(printf '%s\n' "$out" | sed -n 's/^PROG1=//p')
  echo "launchd $name: plist=$plist Disabled=$dis KeepAlive=$ka program=${p0:-<none>} arg1=${p1:-<none>}"
  if [ "$dis" = "true" ]; then
    echo "launchd $name: Disabled=true; launchd does not run or supervise the agent" >&2
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
