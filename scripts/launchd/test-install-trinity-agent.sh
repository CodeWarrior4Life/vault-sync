#!/usr/bin/env bash
# Tests for install-trinity-agent.sh (gate C2 on PR #29). Linux-runnable: uname, id, launchctl, pgrep, ps, osascript
# and kill are shims driven by files under a temp dir, so nothing touches a real agent, process or Login Item.
#   usage: bash scripts/launchd/test-install-trinity-agent.sh [path/to/install-trinity-agent.sh]
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
INST="${1:-$HERE/install-trinity-agent.sh}"
PLIST="$HERE/com.lattice.vaultsync-daemon.plist"
T="$(mktemp -d)"; trap 'rm -rf "$T"' EXIT
FAILS=0; PASSES=0
check(){ local w="$1"; shift; if "$@"; then PASSES=$((PASSES+1)); echo "ok   - $w"; else FAILS=$((FAILS+1)); echo "FAIL - $w"; fi; }
LABEL=com.lattice.vaultsync-daemon

box(){  # $1 name -> a sandbox with shims; state files under ctl/
  local d="$T/$1" c="$T/$1/ctl" b="$T/$1/bin"; mkdir -p "$c" "$b" "$d/agents"
  printf '#!/bin/sh\necho Darwin\n' > "$b/uname"
  printf '#!/bin/sh\necho 501\n' > "$b/id"
  cat > "$b/launchctl" <<EOF
#!/usr/bin/env bash
echo "\$*" >> "$c/launchctl.calls"
case "\$1" in
  bootstrap) cat "$c/after_bootstrap_pids" > "$c/pids" 2>/dev/null; touch "$c/loaded" ;;
  bootout) [ "\$2" = "gui/501/$LABEL" ] && rm -f "$c/loaded"; exit 0 ;;
  print) [ -f "$c/loaded" ] && cat "$c/print" ;;
esac
EOF
  printf '#!/usr/bin/env bash\ncat "%s/pids" 2>/dev/null | tr " " "\\n" | grep .\n' "$c" > "$b/pgrep"
  printf '#!/usr/bin/env bash\ncat "%s/ppid" 2>/dev/null || echo 1\n' "$c" > "$b/ps"
  printf '#!/usr/bin/env bash\necho "$*" >> "%s/osascript.calls"\n' "$c" > "$b/osascript"
  cat > "$b/kill" <<EOF
#!/usr/bin/env bash
echo "\$*" >> "$c/kill.calls"
p="\$2"; sed -i "s/\\b\$p\\b//" "$c/pids"; [ -n "\$(tr -d ' ' < "$c/pids")" ] || : > "$c/pids"
EOF
  chmod +x "$b"/*
  printf 'gui/501/%s = {\n\tstate = running\n\tpid = 777\n\tenvironment = {\n\t\tVAULT_SYNC_SUPERVISED => 1\n\t}\n}\n' "$LABEL" > "$c/print"
  echo 777 > "$c/after_bootstrap_pids"
  echo "$d"
}
inst(){  # $1 box, rest = args
  local d="$1"; shift
  env -i HOME="$d/home" PATH="$d/bin:/usr/bin:/bin" INSTALL_KILL="$d/bin/kill" \
    bash "$INST" --agents-dir "$d/agents" --plist "$PLIST" --timeout 2 "$@" > "$d/out" 2>&1
  echo $? > "$d/rc"
}
rc(){ cat "$1/rc"; }
competing(){  # a login-launched agent for the same app (what tauri-plugin-autostart or a hand install writes)
  cat > "$1/agents/Nexus Vault Sync.plist" <<'EOF'
<?xml version="1.0"?><plist version="1.0"><dict><key>Label</key><string>Nexus Vault Sync</string>
<key>ProgramArguments</key><array><string>/Applications/Nexus Vault Sync.app/Contents/MacOS/vault-sync-daemon</string><string>--silent</string></array>
<key>RunAtLoad</key><true/></dict></plist>
EOF
  cat > "$1/agents/com.example.other.plist" <<'EOF'
<?xml version="1.0"?><plist version="1.0"><dict><key>Label</key><string>com.example.other</string>
<key>ProgramArguments</key><array><string>/usr/local/bin/other</string></array></dict></plist>
EOF
}

# 1. the full install: a competing agent + a Finder copy running -> parked, stopped, one supervised daemon
D=$(box full); competing "$D"; echo "500" > "$D/ctl/pids"
inst "$D"
check "install: receipt OK, exit 0" bash -c "[ \$(cat '$D/rc') = 0 ] && grep -q 'RECEIPT OK: one vault-sync-daemon pid=777, parent launchd' '$D/out'"
check "install: the competing agent was booted out" grep -qx "bootout gui/501/Nexus Vault Sync" "$D/ctl/launchctl.calls"
check "install: the competing plist was parked, not deleted" bash -c "[ -f '$D/agents.vault-sync-parked/Nexus Vault Sync.plist' ] && [ ! -f '$D/agents/Nexus Vault Sync.plist' ]"
check "install: an unrelated agent is untouched" bash -c "[ -f '$D/agents/com.example.other.plist' ] && ! grep -q 'com.example.other' '$D/ctl/launchctl.calls'"
check "install: Login Items removal requested by app name" grep -q 'delete (every login item whose name is "Nexus Vault Sync")' "$D/ctl/osascript.calls"
check "install: the running Finder copy was stopped (SIGTERM)" grep -qx -- "-TERM 500" "$D/ctl/kill.calls"
check "install: the agent plist installed and bootstrapped" bash -c "cmp -s '$PLIST' '$D/agents/$LABEL.plist' && grep -qx 'bootstrap gui/501 $D/agents/$LABEL.plist' '$D/ctl/launchctl.calls'"

# 2. receipt failures, each its own exit code
D=$(box two); echo "777 888" > "$D/ctl/after_bootstrap_pids"; inst "$D"
check "two daemon pids after install: exit 20" test "$(rc "$D")" = 20
D=$(box noenv); sed -i '/VAULT_SYNC_SUPERVISED/d' "$D/ctl/print"; inst "$D"
check "job env without VAULT_SYNC_SUPERVISED => 1: exit 22" test "$(rc "$D")" = 22
D=$(box env0); sed -i 's/VAULT_SYNC_SUPERVISED => 1/VAULT_SYNC_SUPERVISED => 0/' "$D/ctl/print"; inst "$D"
check "job env VAULT_SYNC_SUPERVISED => 0: exit 22" test "$(rc "$D")" = 22
D=$(box parent); echo 4242 > "$D/ctl/ppid"; inst "$D"
check "daemon parent is not launchd: exit 21" test "$(rc "$D")" = 21
D=$(box jobpid); sed -i 's/pid = 777/pid = 999/' "$D/ctl/print"; inst "$D"
check "daemon pid is not the job's pid (a stray copy): exit 21" test "$(rc "$D")" = 21
D=$(box none); : > "$D/ctl/after_bootstrap_pids"; inst "$D"
check "nothing running after bootstrap: exit 23" test "$(rc "$D")" = 23

# 3. --verify-only changes nothing; --dry-run changes nothing
D=$(box verify); competing "$D"; echo 777 > "$D/ctl/pids"; touch "$D/ctl/loaded"; inst "$D" --verify-only
check "--verify-only: receipt OK on a good host" test "$(rc "$D")" = 0
check "--verify-only: no bootout, kill, Login Items or file change" \
  bash -c "! grep -q bootout '$D/ctl/launchctl.calls' 2>/dev/null && [ ! -f '$D/ctl/kill.calls' ] && [ ! -f '$D/ctl/osascript.calls' ] && [ -f '$D/agents/Nexus Vault Sync.plist' ]"
D=$(box dry); competing "$D"; echo 500 > "$D/ctl/pids"; inst "$D" --dry-run
check "--dry-run: exit 0, nothing changed" \
  bash -c "[ \$(cat '$D/rc') = 0 ] && [ ! -f '$D/ctl/launchctl.calls' ] && [ ! -f '$D/ctl/kill.calls' ] && [ -f '$D/agents/Nexus Vault Sync.plist' ] && [ ! -f '$D/agents/$LABEL.plist' ]"
check "--dry-run: the competing route is still reported" grep -q 'competing launch route: .*Nexus Vault Sync.plist' "$D/out"
D=$(box linux); printf '#!/bin/sh\necho Linux\n' > "$D/bin/uname"; inst "$D"
check "not macOS: exit 2" test "$(rc "$D")" = 2

echo "# $PASSES passed, $FAILS failed"
[ "$FAILS" = 0 ]
