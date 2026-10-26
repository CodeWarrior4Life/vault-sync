#!/usr/bin/env bash
# Unit CS-3 (TKT-b6d8c4ba) T-CS3-24, gates C5 and C10 (and E1): verify that
# the launcher supervising the vault-sync daemon restarts it on exit 70
# (a Required task ended, CS-3) AND on exit 74 (custody lost, CS-2), and
# prevents at most 78 (the CS-4 refusal). Read-only: it reads unit files,
# drop-ins and plists; it never starts, stops or edits anything.
#
# Usage:
#   verify-launcher-policy.sh [--name NAME] [--unit-dir DIR]... [--agent-dir DIR]...
#
#   --name       the systemd unit (NAME.service) or launchd agent (NAME.plist,
#                or a plist whose Label is NAME). Default:
#                com.lattice.vaultsync-daemon on macOS, nexus-vault-sync
#                elsewhere.
#   --unit-dir   a systemd unit directory, in precedence order (first wins).
#                Default: ~/.config/systemd/user /etc/systemd/user
#                /run/systemd/user /usr/lib/systemd/user.
#   --agent-dir  a launchd agent directory. Default (macOS only):
#                ~/Library/LaunchAgents /Library/LaunchAgents
#                /Library/LaunchDaemons.
#
# Exit codes (spec section 4.3 item 11):
#   0   pass: a supervisor exists, restarts on both 70 and 74, prevents at
#       most 78, and on macOS runs the daemon directly or through a script
#       that `exec`s it
#   2   usage or parse error
#   10  no unit and no agent found for the daemon
#   11  the policy does not restart on 70 and 74 (Restart=no, on-success,
#       on-abnormal, on-watchdog, on-abort, a missing Restart=; a plist with
#       no KeepAlive, or a KeepAlive that does not restart on a failed exit)
#   12  RestartPreventExitStatus names any status other than 78 (70 and 74
#       included)
#   13  macOS: the agent's program (Program or ProgramArguments[0]) is
#       /usr/bin/open or another non-exec shim
set -uo pipefail

usage() {
  sed -n '2,33p' "$0" >&2
  exit 2
}

name=""
unit_dirs=()
agent_dirs=()
while [ $# -gt 0 ]; do
  case "$1" in
    --name) [ $# -ge 2 ] || usage; name="$2"; shift 2 ;;
    --unit-dir) [ $# -ge 2 ] || usage; unit_dirs+=("$2"); shift 2 ;;
    --agent-dir) [ $# -ge 2 ] || usage; agent_dirs+=("$2"); shift 2 ;;
    -h|--help) usage ;;
    *) echo "verify-launcher-policy: unknown argument: $1" >&2; usage ;;
  esac
done

os=$(uname -s)
if [ -z "$name" ]; then
  if [ "$os" = "Darwin" ]; then name="com.lattice.vaultsync-daemon"; else name="nexus-vault-sync"; fi
fi
if [ ${#unit_dirs[@]} -eq 0 ] && [ ${#agent_dirs[@]} -eq 0 ]; then
  unit_dirs=("$HOME/.config/systemd/user" /etc/systemd/user /run/systemd/user /usr/lib/systemd/user)
  if [ "$os" = "Darwin" ]; then
    agent_dirs=("$HOME/Library/LaunchAgents" /Library/LaunchAgents /Library/LaunchDaemons)
  fi
fi

# ---------------------------------------------------------------------------
# systemd: the base unit (first dir wins) plus every NAME.service.d/*.conf,
# sorted by file name (a same-named drop-in in an earlier dir wins).
# ---------------------------------------------------------------------------
check_systemd() {
  local base="" d f
  for d in "${unit_dirs[@]}"; do
    if [ -f "$d/$name.service" ]; then base="$d/$name.service"; break; fi
  done
  local -a dropins=()
  local seen="|"
  for d in "${unit_dirs[@]}"; do
    [ -d "$d/$name.service.d" ] || continue
    for f in "$d/$name.service.d"/*.conf; do
      [ -f "$f" ] || continue
      local b; b=$(basename "$f")
      case "$seen" in *"|$b|"*) continue ;; esac
      seen="$seen$b|"
      dropins+=("$b|$f")
    done
  done
  if [ -z "$base" ] && [ ${#dropins[@]} -eq 0 ]; then
    return 10
  fi
  if [ -z "$base" ]; then
    echo "systemd $name: drop-ins without a base unit" >&2
    return 2
  fi
  local -a files=("$base")
  if [ ${#dropins[@]} -gt 0 ]; then
    while IFS= read -r line; do files+=("${line#*|}"); done < <(printf '%s\n' "${dropins[@]}" | sort -t'|' -k1,1)
  fi
  local restart="" prevent="" success=""
  local section="" raw key val
  for f in "${files[@]}"; do
    section=""
    while IFS= read -r raw || [ -n "$raw" ]; do
      raw="${raw%$'\r'}"
      case "$raw" in
        ''|'#'*|';'*) continue ;;
        '['*']') section="$raw"; continue ;;
      esac
      [ "$section" = "[Service]" ] || continue
      case "$raw" in *=*) ;; *) continue ;; esac
      key="${raw%%=*}"; key="${key//[[:space:]]/}"
      val="${raw#*=}"; val="$(echo "$val" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
      case "$key" in
        Restart) restart="$val" ;;
        RestartPreventExitStatus) if [ -z "$val" ]; then prevent=""; else prevent="$prevent $val"; fi ;;
        SuccessExitStatus) if [ -z "$val" ]; then success=""; else success="$success $val"; fi ;;
      esac
    done < "$f"
  done
  echo "systemd $name: base=$base dropins=$(( ${#files[@]} - 1 )) Restart=${restart:-<unset:no>} RestartPreventExitStatus=[${prevent# }] SuccessExitStatus=[${success# }]"
  case "$restart" in
    always) ;;
    on-failure)
      local s
      for s in $success; do
        if [ "$s" = "70" ] || [ "$s" = "74" ]; then
          echo "systemd $name: SuccessExitStatus makes $s a success; on-failure will not restart it" >&2
          return 11
        fi
      done
      ;;
    *)
      echo "systemd $name: Restart=${restart:-<unset>} does not restart on exit 70 and 74" >&2
      return 11
      ;;
  esac
  local p
  for p in $prevent; do
    if [ "$p" != "78" ]; then
      echo "systemd $name: RestartPreventExitStatus names $p (only 78 may be listed)" >&2
      return 12
    fi
  done
  return 0
}

# ---------------------------------------------------------------------------
# launchd: an agent plist (XML; binary plists are converted with plutil).
# ---------------------------------------------------------------------------
plist_tags() {
  # One XML tag (or text run) per line.
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

check_launchd() {
  local plist
  plist=$(find_agent) || return 10
  local parsed
  parsed=$(plist_tags "$plist") || return 2
  # Emits: KEEPALIVE=<none|true|false|failure|success|other> and PROGRAM=<path>
  local out
  out=$(printf '%s\n' "$parsed" | awk '
    function text(t) { sub(/^<string>/, "", t); sub(/<\/string>$/, "", t); return t }
    BEGIN { ka = "none"; prog = ""; mode = ""; depth = 0; kadepth = -1; pending = "" }
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
      if (pending == "SuccessfulExit") {
        pending = ""
        if (t == "<false/>") ka = "failure"; else if (t == "<true/>" && ka != "failure") ka = "success"
        next
      }
      if (pending == "Program") { pending = ""; if (t ~ /^<string>/) prog = text(t); next }
      if (pending == "ProgramArguments") {
        if (t == "<array>") next
        pending = ""
        if (t ~ /^<string>/ && prog == "") prog = text(t)
        next
      }
      if (t == "<key>KeepAlive</key>" && depth == 1) { pending = "KeepAlive"; next }
      if (t == "<key>SuccessfulExit</key>" && kadepth >= 0 && depth == kadepth) { pending = "SuccessfulExit"; next }
      if (t == "<key>Program</key>" && depth == 1) { pending = "Program"; prog = ""; next }
      if (t == "<key>ProgramArguments</key>" && depth == 1) { pending = "ProgramArguments"; next }
    }
    END { print "KEEPALIVE=" ka; print "PROGRAM=" prog }
  ')
  local ka prog
  ka=$(printf '%s\n' "$out" | sed -n 's/^KEEPALIVE=//p')
  prog=$(printf '%s\n' "$out" | sed -n 's/^PROGRAM=//p')
  echo "launchd $name: plist=$plist KeepAlive=$ka program=${prog:-<none>}"
  if [ -z "$prog" ]; then
    echo "launchd $name: no Program or ProgramArguments" >&2
    return 2
  fi
  # 13 first: a shim means launchd never sees the daemon's exit at all.
  case "$prog" in
    /usr/bin/open|open)
      echo "launchd $name: the agent runs $prog; launchd supervises the shim, never the daemon's exit" >&2
      return 13
      ;;
  esac
  if [ -f "$prog" ] && head -c 2 "$prog" | grep -q '^#!'; then
    local last
    last=$(grep -v '^[[:space:]]*#' "$prog" | grep -v '^[[:space:]]*$' | tail -1 | sed 's/^[[:space:]]*//')
    case "$last" in
      exec\ *) ;;
      *)
        echo "launchd $name: launch script $prog does not exec the daemon (last command: $last)" >&2
        return 13
        ;;
    esac
  fi
  case "$ka" in
    true|failure) return 0 ;;
    *)
      echo "launchd $name: KeepAlive=$ka does not restart a failed exit (70/74)" >&2
      return 11
      ;;
  esac
}

found=0
worst=0
if [ ${#unit_dirs[@]} -gt 0 ]; then
  check_systemd; rc=$?
  if [ $rc -ne 10 ]; then found=1; [ $rc -ne 0 ] && [ $worst -eq 0 ] && worst=$rc; fi
fi
if [ ${#agent_dirs[@]} -gt 0 ]; then
  check_launchd; rc=$?
  if [ $rc -ne 10 ]; then found=1; [ $rc -ne 0 ] && [ $worst -eq 0 ] && worst=$rc; fi
fi
if [ $found -eq 0 ]; then
  echo "verify-launcher-policy: no systemd unit and no launchd agent found for $name" >&2
  exit 10
fi
if [ $worst -eq 0 ]; then
  echo "verify-launcher-policy: PASS ($name restarts on 70 and 74, prevents at most 78)"
fi
exit $worst
