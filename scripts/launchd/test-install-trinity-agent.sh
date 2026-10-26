#!/usr/bin/env bash
# Tests for install-trinity-agent.sh (gate C2 on PR #29 + follow-up P1-P5). Linux-runnable: uname, id, launchctl,
# pgrep, ps, osascript and the test kill command are shims driven by files under a temp dir, so nothing touches a real
# agent, process or Login Item.
#   usage: bash scripts/launchd/test-install-trinity-agent.sh [path/to/install-trinity-agent.sh]
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
INST="${1:-$HERE/install-trinity-agent.sh}"
PLIST="$HERE/com.lattice.vaultsync-daemon.plist"
T="${TEST_KEEP_DIR:-$(mktemp -d)}"; [ -n "${TEST_KEEP_DIR:-}" ] || trap 'rm -rf "$T"' EXIT
FAILS=0; PASSES=0
check(){ local w="$1"; shift; if "$@"; then PASSES=$((PASSES+1)); echo "ok   - $w"; else FAILS=$((FAILS+1)); echo "FAIL - $w"; fi; }
LABEL=com.lattice.vaultsync-daemon
MAIN="/Applications/Nexus Vault Sync.app/Contents/MacOS/vault-sync-daemon"
MEM="/Users/u/.local/libexec/nexus-vault-sync-memory/vault-sync-daemon"

box(){  # $1 name -> a sandbox with shims; state under ctl/ (procs: "pid command...", env.<pid>, ppid.<pid>)
  local d="$T/$1" c="$T/$1/ctl" b="$T/$1/bin"; mkdir -p "$c" "$b" "$d/agents"; : > "$c/procs"
  printf '#!/bin/sh\necho Darwin\n' > "$b/uname"
  printf '#!/bin/sh\necho 501\n' > "$b/id"
  cat > "$b/launchctl" <<EOF
#!/usr/bin/env bash
echo "\$*" >> "$c/launchctl.calls"
case "\$1" in
  bootstrap) [ "\$3" = "$d/agents/$LABEL.plist" ] && { cat "$c/after_bootstrap" >> "$c/procs" 2>/dev/null; touch "$c/loaded"; } ;;
  bootout) [ "\$2" = "gui/501/$LABEL" ] && rm -f "$c/loaded"; exit 0 ;;
  print) [ -f "$c/loaded" ] && cat "$c/print" ;;
esac
EOF
  # pgrep -x on macOS matches the 16-char-truncated name: model that it MISSES vault-sync-daemon (17 chars).
  printf '#!/bin/sh\necho "pgrep $*" >> "%s/pgrep.calls"\nexit 1\n' "$c" > "$b/pgrep"
  cat > "$b/ps" <<EOF
#!/usr/bin/env bash
C="$c"
case "\$*" in
  "-axo pid=,command=") sed 's/^/  /' "\$C/procs" ;;
  "-E -ww -o command= -p "*) p="\${@: -1}"; l=\$(grep "^\$p " "\$C/procs"); [ -n "\$l" ] || exit 1; echo "\${l#\$p } \$(cat "\$C/env.\$p" 2>/dev/null)" ;;
  "-o ppid= -p "*) p="\${@: -1}"; cat "\$C/ppid.\$p" 2>/dev/null || echo 1 ;;
  "-axo comm=") sed 's|.*/\\(vault-sync-daemon\\).*|\\1|' "\$C/procs" | cut -c1-16 ;;   # MAXCOMLEN: 16 chars
esac
EOF
  printf '#!/usr/bin/env bash\necho "$*" >> "%s/osascript.calls"\n' "$c" > "$b/osascript"
  # plutil (macOS) modelled with plistlib: -lint and -extract KEYPATH raw -o - FILE (comments are not keys).
  cat > "$b/plutil" <<'EOF'
#!/usr/bin/env python3
import plistlib, sys
a = sys.argv[1:]
def load(f):
    with open(f, "rb") as fh:
        return plistlib.load(fh)
if a[0] == "-lint":
    try:
        load(a[-1]); print(a[-1] + ": OK"); sys.exit(0)
    except Exception:
        sys.exit(1)
if a[0] == "-extract":
    try:
        v = load(a[-1])
        for k in a[1].split("."):
            v = v[int(k)] if isinstance(v, list) else v[k]
    except Exception:
        sys.exit(1)
    print(v if not isinstance(v, bool) else str(v).lower()); sys.exit(0)
sys.exit(2)
EOF
  # mv: a real mv, except that a source whose basename is listed in ctl/mvfail is NOT moved (rc 0, like mv -n skipping)
  cat > "$b/mv" <<EOF
#!/usr/bin/env bash
for a in "\$@"; do case "\$a" in -*) ;; *) src="\$a"; break ;; esac; done
[ -f "$c/mvfail" ] && grep -qxF "\$(basename "\$src")" "$c/mvfail" && exit 0
exec /usr/bin/mv "\$@"
EOF
  cat > "$b/testkill" <<EOF
#!/usr/bin/env bash
echo "\$*" >> "$c/kill.calls"; grep -v "^\$2 " "$c/procs" > "$c/procs.n"; mv "$c/procs.n" "$c/procs"
EOF
  chmod +x "$b"/*
  printf 'gui/501/%s = {\n\tstate = running\n\tpid = 777\n\tenvironment = {\n\t\tVAULT_SYNC_SUPERVISED => 1\n\t}\n}\n' "$LABEL" > "$c/print"
  echo "777 $MAIN --silent" > "$c/after_bootstrap"
  echo "$d"
}
inst(){  # $1 box, rest = args
  local d="$1"; shift
  env -i HOME="$d/home" PATH="$d/bin:/usr/bin:/bin" \
    bash "$INST" --agents-dir "$d/agents" --plist "$PLIST" --timeout 2 "$@" > "$d/out" 2>&1
  echo $? > "$d/rc"
}
rc(){ cat "$1/rc"; }
agent(){  # $1 box  $2 file  $3 label  $4 program-args xml  [$5 extra xml]
  printf '<?xml version="1.0"?><plist version="1.0"><dict><key>Label</key><string>%s</string>\n<key>ProgramArguments</key><array>%s</array>\n%s<key>RunAtLoad</key><true/></dict></plist>\n' \
    "$3" "$4" "${5:-}" > "$1/agents/$2"
}
s(){ printf '<string>%s</string>' "$@"; }
decoys(){
  agent "$1" "Nexus Vault Sync.plist" "Nexus Vault Sync" "$(s "$MAIN" --silent)"                                   # competes
  agent "$1" "old.bak.plist" "com.example.bak" "$(s "/Applications/Nexus Vault Sync.app.bak/Contents/MacOS/vault-sync-daemon")"
  agent "$1" "killer.plist" "com.example.killer" "$(s /usr/bin/pkill -x vault-sync-daemon)"
  agent "$1" "wrapper.plist" "com.example.wrapper" "$(s /bin/sh -c "open -a 'Nexus Vault Sync.app'; exec $MAIN")"
  agent "$1" "managed.plist" "com.lattice.vaultsync-memory" "$(s "$MAIN" --silent)" \
    '<key>EnvironmentVariables</key><dict><key>NEXUS_SYNC_CONFIG_DIR</key><string>/Users/u/.config/mem</string></dict>'
  agent "$1" "memory.plist" "com.lattice.vaultsync-memory2" "$(s "$MEM" --silent)"
  agent "$1" "other.plist" "com.example.other" "$(s /usr/local/bin/other)"
  # P1-a decoys: a commented-out Program key naming the main binary; a plist that does not parse.
  agent "$1" "commented.plist" "com.example.commented" "$(s /usr/local/bin/other)" \
    "<!-- <key>Program</key><string>$MAIN</string> -->"
  printf '<?xml version="1.0"?><plist version="1.0"><dict><key>Label</key><string>broken</string><key>Program</key><string>%s</string>\n' "$MAIN" > "$1/agents/broken.plist"
}

# 1. P1: exact-path matching; decoys are never parked
D=$(box full); decoys "$D"
inst "$D" --memory-binary "$MEM"
check "install with decoys: receipt OK, exit 0" bash -c "[ \$(cat '$D/rc') = 0 ] && grep -q 'RECEIPT OK: one vault-mode daemon pid=777' '$D/out'"
check "P1: the exact-match competing agent is booted out and parked" \
  bash -c "grep -qx 'bootout gui/501/Nexus Vault Sync' '$D/ctl/launchctl.calls' && [ -f '$D/agents.vault-sync-parked/Nexus Vault Sync.plist' ]"
for f in old.bak killer wrapper managed memory other commented broken; do
  check "P1: decoy $f.plist is NOT parked" bash -c "[ -f '$D/agents/$f.plist' ] && [ ! -f '$D/agents.vault-sync-parked/$f.plist' ]"
done
check "P1: no decoy label was booted out" bash -c "! grep -qE 'example|vaultsync-memory' '$D/ctl/launchctl.calls'"
check "P1: the managed instance is logged as not parked" grep -q 'not parking .*managed.plist: a managed instance' "$D/out"
check "P1-a: the unparseable plist is skipped and logged" grep -q 'skipping .*broken.plist: plutil cannot parse it' "$D/out"
check "P1-a: a commented-out <key>Program</key> naming the main binary is NOT a match" \
  bash -c "[ -f '$D/agents/commented.plist' ] && ! grep -q 'commented' '$D/ctl/launchctl.calls'"

# P1-b: an EMPTY NEXUS_SYNC_CONFIG_DIR is not a managed instance (agent parked; process counted); not-anchored names neither
D=$(box emptycfg)
agent "$D" "emptycfg.plist" "com.example.emptycfg" "$(s "$MAIN")" \
  '<key>EnvironmentVariables</key><dict><key>NEXUS_SYNC_CONFIG_DIR</key><string></string></dict>'
inst "$D"
check "P1-b: an agent with an EMPTY NEXUS_SYNC_CONFIG_DIR competes (parked)" test -f "$D/agents.vault-sync-parked/emptycfg.plist"
D=$(box emptyenv); printf '777 %s --silent\n888 %s\n' "$MAIN" "$MAIN" > "$D/ctl/after_bootstrap"
echo "NEXUS_SYNC_CONFIG_DIR=" > "$D/ctl/env.888"; inst "$D"
check "P1-b: a process with an EMPTY NEXUS_SYNC_CONFIG_DIR is counted (two copies: exit 20)" test "$(rc "$D")" = 20
D=$(box unanchored); printf '777 %s --silent\n889 %s\n' "$MAIN" "$MAIN" > "$D/ctl/after_bootstrap"
echo "X_NEXUS_SYNC_CONFIG_DIR=/x" > "$D/ctl/env.889"; inst "$D"
check "P1-b: X_NEXUS_SYNC_CONFIG_DIR=… is not the managed marker (counted: exit 20)" test "$(rc "$D")" = 20
D=$(box mgdok); printf '777 %s --silent\n890 %s\n' "$MAIN" "$MAIN" > "$D/ctl/after_bootstrap"
echo "HOME=/u NEXUS_SYNC_CONFIG_DIR=/Users/u/.config/mem" > "$D/ctl/env.890"; inst "$D"
check "P1-b: a NON-EMPTY NEXUS_SYNC_CONFIG_DIR after another env entry excludes the process (receipt OK)" test "$(rc "$D")" = 0

# P1-c: the memory binary may not be the main binary
D=$(box samebin); decoys "$D"; inst "$D" --memory-binary "$MAIN"
check "P1-c: --memory-binary == --main-binary is refused (exit 2) before anything changes" \
  bash -c "[ \$(cat '$D/rc') = 2 ] && grep -q 'REFUSED: --memory-binary equals --main-binary' '$D/out' && [ ! -f '$D/ctl/launchctl.calls' ]"

# 2. P2: process detection by exact full path, not pgrep -x (the 16-char-truncated name)
D="$T/full"   # back to the full install box (the P1-b/P1-c boxes above replaced D)
check "P2: pgrep was never used" test ! -f "$D/ctl/pgrep.calls"
check "P2: the truncated comm fixture really is truncated (16 chars, not vault-sync-daemon)" \
  bash -c "PATH='$D/bin:/usr/bin:/bin' ps -axo comm= | grep -qx 'vault-sync-daemo' && ! PATH='$D/bin:/usr/bin:/bin' ps -axo comm= | grep -qx 'vault-sync-daemon'"
D=$(box mgdproc); echo "777 $MAIN --silent" > "$D/ctl/after_bootstrap"
printf '900 %s --silent\n' "$MAIN" > "$D/ctl/procs"; echo "NEXUS_SYNC_CONFIG_DIR=/Users/u/.config/mem" > "$D/ctl/env.900"
printf '901 %s --silent\n' "$MEM" >> "$D/ctl/procs"
printf '902 /Applications/Nexus Vault Sync.app.bak/Contents/MacOS/vault-sync-daemon\n' >> "$D/ctl/procs"
inst "$D" --memory-binary "$MEM"
check "P2: a managed copy of the same binary, the memory binary and a .app.bak copy are not counted (receipt OK)" test "$(rc "$D")" = 0

# 3. P4: a running vault-mode copy is never killed by default
D=$(box finder); echo "500 $MAIN" > "$D/ctl/procs"; inst "$D"
check "P4: default install with a running copy: exit 24, asks for a tray Quit" \
  bash -c "[ \$(cat '$D/rc') = 24 ] && grep -q 'Quit it from its tray menu' '$D/out'"
check "P4: default install never kills" test ! -f "$D/ctl/kill.calls"
check "P4: and installs nothing" test ! -f "$D/agents/$LABEL.plist"
D=$(box finderkill); echo "500 $MAIN" > "$D/ctl/procs"; inst "$D" --test-kill-cmd "$D/bin/testkill"
check "P4: only --test-kill-cmd stops it (then receipt OK)" \
  bash -c "grep -qx -- '-TERM 500' '$D/ctl/kill.calls' && [ \$(cat '$D/rc') = 0 ]"

# 4. P3: mv -n, a manifest, printed restore commands, --restore
D=$(box restore); decoys "$D"; inst "$D"
M=$(ls "$D"/agents.vault-sync-parked/manifest-*.tsv 2>/dev/null | head -1)
check "P3: a manifest records the move" bash -c "[ -n '$M' ] && grep -q \"Nexus Vault Sync.plist\" '$M'"
check "P3: the exact restore commands are printed" \
  bash -c "grep -q -- '--restore' '$D/out' && grep -q \"or by hand: mv -n '$D/agents.vault-sync-parked/Nexus Vault Sync.plist' '$D/agents/Nexus Vault Sync.plist' && launchctl bootstrap gui/501\" '$D/out'"
inst "$D" --restore "$M"
check "P3: --restore puts the plist back and bootstraps it" \
  bash -c "[ \$(cat '$D/rc') = 0 ] && [ -f '$D/agents/Nexus Vault Sync.plist' ] && grep -qx \"bootstrap gui/501 $D/agents/Nexus Vault Sync.plist\" '$D/ctl/launchctl.calls'"
mv "$D/agents/Nexus Vault Sync.plist" "$D/agents.vault-sync-parked/Nexus Vault Sync.plist"; touch "$D/agents/Nexus Vault Sync.plist"
inst "$D" --restore "$M"
check "P3: --restore never clobbers an existing original: exit 25" \
  bash -c "[ \$(cat '$D/rc') = 25 ] && [ -f '$D/agents.vault-sync-parked/Nexus Vault Sync.plist' ] && [ ! -s '$D/agents/Nexus Vault Sync.plist' ]"
# P3: the second park fails -> exit 26, and the FIRST park's restore commands are printed before exiting
D=$(box secondfail)
agent "$D" "a-first.plist" "com.example.first" "$(s "$MAIN")"
agent "$D" "b-second.plist" "com.example.second" "$(s "$MAIN" --silent)"
echo b-second.plist > "$D/ctl/mvfail"; inst "$D"
check "P3: a park whose mv -n does not move: exit 26" test "$(rc "$D")" = 26
check "P3: the first park's restore commands are still printed" \
  bash -c "grep -q \"or by hand: mv -n '$D/agents.vault-sync-parked/a-first.plist' '$D/agents/a-first.plist'\" '$D/out' && grep -q -- '--restore' '$D/out'"
check "P3: the manifest holds the first park only" \
  bash -c "M=\$(ls '$D'/agents.vault-sync-parked/manifest-*.tsv); grep -q a-first.plist \"\$M\" && ! grep -q b-second.plist \"\$M\""
D=$(box clobber); decoys "$D"; mkdir -p "$D/agents.vault-sync-parked"; echo keep > "$D/agents.vault-sync-parked/Nexus Vault Sync.plist"
inst "$D"
check "P3: an already-parked file of the same name is never clobbered: exit 2" \
  bash -c "[ \$(cat '$D/rc') = 2 ] && [ \"\$(cat '$D/agents.vault-sync-parked/Nexus Vault Sync.plist')\" = keep ] && [ -f '$D/agents/Nexus Vault Sync.plist' ]"

# 5. receipt failures, each its own exit code (P5: the pre-supervisor window shows up as 20 or 21)
D=$(box two); printf '777 %s --silent\n888 %s\n' "$MAIN" "$MAIN" > "$D/ctl/after_bootstrap"; inst "$D"
check "two vault-mode pids after install (a tray-window copy beside the agent's): exit 20" test "$(rc "$D")" = 20
D=$(box jobpid); sed -i 's/pid = 777/pid = 999/' "$D/ctl/print"; inst "$D"
check "the only copy is not the job's pid (a tray-window copy won the socket): exit 21" \
  bash -c "[ \$(cat '$D/rc') = 21 ] && grep -q 'pre-supervisor tray window' '$D/out'"
D=$(box noenv); sed -i '/VAULT_SYNC_SUPERVISED/d' "$D/ctl/print"; inst "$D"
check "job env without VAULT_SYNC_SUPERVISED => 1: exit 22" test "$(rc "$D")" = 22
D=$(box parent); echo 4242 > "$D/ctl/ppid.777"; inst "$D"
check "daemon parent is not launchd: exit 21" test "$(rc "$D")" = 21
D=$(box none); : > "$D/ctl/after_bootstrap"; inst "$D"
check "nothing running after bootstrap: exit 23" test "$(rc "$D")" = 23

# 6. --verify-only and --dry-run change nothing; non-macOS refused
D=$(box verify); decoys "$D"; echo "777 $MAIN --silent" > "$D/ctl/procs"; touch "$D/ctl/loaded"; inst "$D" --verify-only
check "--verify-only: receipt OK, no bootout, Login Items or file change" \
  bash -c "[ \$(cat '$D/rc') = 0 ] && ! grep -q bootout '$D/ctl/launchctl.calls' 2>/dev/null && [ ! -f '$D/ctl/osascript.calls' ] && [ -f '$D/agents/Nexus Vault Sync.plist' ]"
D=$(box dry); decoys "$D"; echo "500 $MAIN" > "$D/ctl/procs"; inst "$D" --dry-run
check "--dry-run: exit 0, nothing changed, no manifest" \
  bash -c "[ \$(cat '$D/rc') = 0 ] && [ ! -f '$D/ctl/launchctl.calls' ] && [ -f '$D/agents/Nexus Vault Sync.plist' ] && [ ! -f '$D/agents/$LABEL.plist' ] && ! ls '$D'/agents.vault-sync-parked/manifest-* >/dev/null 2>&1"
check "--dry-run: reports the competing route and the running copy" \
  bash -c "grep -q 'competing launch route: .*Nexus Vault Sync.plist' '$D/out' && grep -q 'a real run would stop here (exit 24)' '$D/out'"
D=$(box linux); printf '#!/bin/sh\necho Linux\n' > "$D/bin/uname"; inst "$D"
check "not macOS: exit 2" test "$(rc "$D")" = 2

echo "# $PASSES passed, $FAILS failed"
[ "$FAILS" = 0 ]
