#!/usr/bin/env bash
# Tests for nexus-vault-sync-supervisor.sh: the O5-2a inactive-unit alert (send-time arranger resolution, P0-tier
# page, operator_escalate fallback, lnotify notice) and the UNCHANGED not_receiving path.
# Self-contained: bash + python3 only. Every external command the supervisor calls is a shim driven by files under a
# temp dir: systemctl, curl, lnotify, logger, `date +%s`, hostname on PATH; the key resolver at its default path
# ~/.local/bin/nexus-api-key.sh and operator_escalate.py at ~/.claude/scripts/ under a temp HOME. Nothing touches a
# real unit, the journal, the network, the notify rail, a conductor inbox or the operator.
#   usage: bash scripts/supervisor/test-supervisor.sh [path/to/supervisor.sh]
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
SUP="${1:-$HERE/nexus-vault-sync-supervisor.sh}"
ORIG="$HERE/testdata/nexus-vault-sync-supervisor.imported.sh"
ORIG_SHA=a09ff327fdbcf151d00c78200f0ffe79b6baaeae99c404901da6580ae9183096   # the imported link/icarus copy
REAL_DATE="$(command -v date)"
PY="$(command -v python3)"
T="$(mktemp -d)"; trap 'rm -rf "$T"' EXIT
FAILS=0; PASSES=0
ok(){ PASSES=$((PASSES+1)); echo "ok   - $*"; }
bad(){ FAILS=$((FAILS+1)); echo "FAIL - $*"; }
check(){ local what="$1"; shift; if "$@"; then ok "$what"; else bad "$what"; fi; }

U1=nexus-vault-sync.service
U2=nexus-vault-sync-memory.service
CSID=0f6e3c2a-1b2c-4d5e-8f90-a1b2c3d4e5f6
CSID2=7a7b7c7d-0000-4111-8222-933344445555
CSID3=99990000-aaaa-4bbb-8ccc-dddddddddddd
NKEY=NEXUSkeyVALUE0123456789abcdefXYZ        # what the stub resolver injects as NEXUS_API_KEY

mkshims(){  # $1 box dir
  local b="$1/bin" c="$1/ctl" h="$1/home"
  mkdir -p "$b" "$c/units" "$h/.local/bin" "$h/.claude/scripts" "$h/.claude/runtime" "$1/backups"
  cat > "$b/systemctl" <<EOF
#!/usr/bin/env bash
C="$c"
[ "\$1" = --user ] && shift
case "\$1" in
  show)
    shift; u=""; p=""
    while [ \$# -gt 0 ]; do case "\$1" in -p) p="\$2"; shift 2 ;; --value) shift ;; *) u="\$1"; shift ;; esac; done
    case "\$p" in
      LoadState) [ -f "\$C/units/\$u.load" ] && cat "\$C/units/\$u.load" || echo not-found ;;
      ExecMainStartTimestamp) cat "\$C/units/\$u.start" 2>/dev/null || echo "" ;;
    esac ;;
  is-active) a=\$(cat "\$C/units/\$2.active" 2>/dev/null || echo inactive); echo "\$a"; [ "\$a" = active ] ;;
  restart) echo "\$2" >> "\$C/restarts"; exit 0 ;;
esac
EOF
  # curl: records argv (NUL-separated) and stdin per call; -w "\n%{http_code}" is honoured like real curl.
  cat > "$b/curl" <<EOF
#!/usr/bin/env bash
C="$c"
w=0; for a in "\$@"; do [ "\$a" = -w ] && w=1; done
emit(){ printf '%s' "\$1"; [ \$w = 1 ] && printf '\n%s' "\$2"; }
for a in "\$@"; do case "\$a" in
  *coord/emit*) n=\$(ls "\$C" | grep -c '^page.argv'); printf '%s\0' "\$@" > "\$C/page.argv.\$n"; cat > "\$C/page.stdin.\$n"
                code=\$(cat "\$C/page.code" 2>/dev/null || echo 200); emit "{\"event_id\":\"ev-\$n\"}" "\$code"; exit 0 ;;
  *sessions/list*) printf '%s\0' "\$@" > "\$C/roster.argv"; cat > "\$C/roster.stdin"
                [ -f "\$C/roster.json" ] && emit "\$(cat "\$C/roster.json")" 200 || emit "{}" 503; exit 0 ;;
esac; done
cat "\$C/health.json" 2>/dev/null || exit 7
EOF
  cat > "$b/lnotify" <<EOF
#!/usr/bin/env bash
{ printf 'ARGV'; printf ' %q' "\$@"; printf '\nMSG '; cat; printf '\n'; } >> "$c/lnotify.log"
EOF
  printf '#!/usr/bin/env bash\nexit 0\n' > "$b/logger"
  cat > "$b/date" <<EOF
#!/usr/bin/env bash
[ "\$*" = "+%s" ] && { cat "$c/now"; exit 0; }
exec "$REAL_DATE" "\$@"
EOF
  printf '#!/usr/bin/env bash\ncat "%s/hostname" 2>/dev/null || echo testhost\n' "$c" > "$b/hostname"
  # the host's key resolver, EXEC form only (nexus-api-key.sh contract): `exec -- cmd...` runs cmd with the key in
  # ITS env. Any other form is refused here, printing nothing.
  cat > "$h/.local/bin/nexus-api-key.sh" <<EOF
#!/usr/bin/env bash
printf '%s\0' "\$@" >> "$c/keyhelper.argv"; printf '\n' >> "$c/keyhelper.argv"
[ -n "\${NEXUS_API_KEY:-}" ] && echo parent-had-key >> "$c/keyhelper.parentenv"
[ "\${1:-}" = exec ] && [ "\${2:-}" = -- ] || { echo "refused: not the exec form" >&2; exit 9; }
shift 2; NEXUS_API_KEY=$NKEY exec "\$@"
EOF
  cat > "$h/.claude/scripts/operator_escalate.py" <<EOF
import json, sys
open("$c/escalate.log", "a").write(json.dumps(sys.argv[1:]) + "\n")
print(json.dumps({"severity": "stub", "acked": False}))
EOF
  chmod +x "$b"/* "$h/.local/bin/nexus-api-key.sh"
}

# one tick at epoch $2 in box $1, with a user unit's minimal env (env -i: HOME, PATH, XDG_RUNTIME_DIR) plus the
# drop-in style NVS_* settings a test needs. Records the exit status in ctl/rc.
tick(){
  local d="$1"; echo "$2" > "$d/ctl/now"
  env -i HOME="$d/home" PATH="$d/bin:$(dirname "$PY"):/usr/bin:/bin" XDG_RUNTIME_DIR="$d/xdg" \
      NVS_SUBSCRIBER_ID=sub-test ${OVR:+NVS_PAGE_CSID=$OVR} ${EXTRA_ENV:-} \
      bash "${SCRIPT:-$SUP}" >/dev/null 2>&1
  local rc=$?; echo $rc > "$d/ctl/rc"; return $rc
}
nlog(){ grep -c '^ARGV' "$1/ctl/lnotify.log" 2>/dev/null || echo 0; }
npage(){ ls "$1/ctl" | grep -c '^page.argv' ; }
nesc(){ [ -f "$1/ctl/escalate.log" ] && wc -l < "$1/ctl/escalate.log" | tr -d ' ' || echo 0; }
rc(){ cat "$1/ctl/rc"; }
st(){ echo "$1/home/.local/state/nexus-vault-sync-supervisor"; }
setunit(){ echo "$3" > "$1/ctl/units/$2.load"; echo "$4" > "$1/ctl/units/$2.active"; }
health(){ printf '{"subscribers":[{"subscriber_id":"sub-test","receipt_state":"%s"}]}' "$2" > "$1/ctl/health.json"; }
marker(){ printf '{"role":"arranger","csid":"%s","host":"testhost","claimed_at":"%s","by":"test"}\n' "$2" "$3" \
  > "$1/home/.claude/runtime/conductor.role.arranger-$2"; }
release(){ mv "$1/home/.claude/runtime/conductor.role.arranger-$2" "$1/backups/"; }
nomarker(){ rm -f "$1"/home/.claude/runtime/conductor.role.arranger-*; }
newbox(){ local d="$T/$1"; mkshims "$d"; health "$d" receiving; marker "$d" "$CSID" 2026-10-26T10:00:00+00:00; echo "$d"; }
pagefield(){ tr '\0' '\n' < "$1/ctl/page.argv.$2" | grep '^{' | "$PY" -c "import json,sys; d=json.loads(sys.stdin.read()); print(eval(sys.argv[1]))" "$3"; }
lastpage(){ pagefield "$1" $(($(npage "$1")-1)) 'd["session_id"]'; }
roster(){  # $1 box, then display_name=csid pairs (all live)
  local d="$1"; shift; local rows="" sep=""
  for p in "$@"; do rows="$rows$sep{\"session_id\":\"${p#*=}\",\"display_name\":\"${p%%=*}\",\"ended_at\":null,\"started_at\":\"2026-10-26T09:00:00\"}"; sep=,; done
  printf '{"sessions":[%s]}' "$rows" > "$d/ctl/roster.json"
}

T0=1800000000

# --- 0. the script minus the O5-2a block is the imported original, byte for byte ---------------------------
STRIPPED="$T/stripped.sh"
"$PY" - "$SUP" "$STRIPPED" <<'PY'
import sys
s = open(sys.argv[1]).read()
a = s.index("# ---- O5-2a (LSEC-9325-B6D8-O5-ONFAILURE-RULING")
b = s.index("# ---- end O5-2a\n") + len("# ---- end O5-2a\n")
open(sys.argv[2], "w").write(s[:a] + s[b:])
PY
check "stripping the O5-2a block yields the imported original (sha256 $ORIG_SHA)" \
  test "$(sha256sum "$STRIPPED" | cut -d' ' -f1)" = "$ORIG_SHA"
check "pinned imported copy is intact (sha256 $ORIG_SHA)" test "$(sha256sum "$ORIG" | cut -d' ' -f1)" = "$ORIG_SHA"

# --- 1. threshold; a delivered P0-tier page; the key never on argv -----------------------------------------
D=$(newbox a); setunit "$D" $U1 loaded inactive; setunit "$D" $U2 loaded active
tick "$D" $T0; tick "$D" $((T0+600)); tick "$D" $((T0+899))
check "inactive 899s < 900s: no lnotify, no page, no escalate" test "$(nlog "$D")/$(npage "$D")/$(nesc "$D")" = 0/0/0
check "inactive window opened at the first not-active tick" test "$(cat "$(st "$D")/unit.$U1.inactive_since")" = $T0
tick "$D" $((T0+900))
check "inactive 900s: exactly one lnotify notice and one page" test "$(nlog "$D")/$(npage "$D")" = 1/1
check "page 2xx: no operator escalate" test "$(nesc "$D")" = 0
check "page 2xx: the tick exits 0" test "$(rc "$D")" = 0
check "lnotify: notice claim, unit.stopped, per host+unit identity key" \
  grep -q -- "--event-type unit.stopped --claim notice --identity-key nexus-vault-sync.unit-inactive:testhost:$U1" "$D/ctl/lnotify.log"
check "lnotify principal is the host-scoped supervisor" grep -q -- "--principal testhost.nexus-vault-sync-supervisor@lattice" "$D/ctl/lnotify.log"
check "page goes to the csid in the live role marker" test "$(lastpage "$D")" = "$CSID"
check "page: kind=page, tier=decision (the fleet's P0 pager shape)" test "$(pagefield "$D" 0 'd["kind"]+"/"+d["body"]["tier"]')" = page/decision
check "page: reason starts with 'P0 vault-sync unit'" test "$(pagefield "$D" 0 'd["body"]["reason"][:19]')" = "P0 vault-sync unit "
check "page: delivered event id logged" grep -q "PAGE-OK kind=raised unit=$U1 $CSID role-marker event=ev-0" "$(st "$D")/log"
check "resolver called in EXEC form (exec -- ...)" bash -c "tr '\\0' ' ' < '$D/ctl/keyhelper.argv' | grep -q '^exec -- python3 '"
check "resolver: the parent had no NEXUS_API_KEY (injected into the child only)" test ! -f "$D/ctl/keyhelper.parentenv"
check "the key never appears on any recorded argv (curl, resolver, lnotify)" \
  bash -c "! cat '$D'/ctl/*.argv* '$D/ctl/lnotify.log' 2>/dev/null | tr '\\0' '\\n' | grep -q '$NKEY'"
check "the key reaches curl on stdin (-K -)" grep -q "Authorization: Bearer $NKEY" "$D/ctl/page.stdin.0"
check "the healthy unit raised nothing" bash -c "! grep -q -- '$U2' '$D/ctl/lnotify.log'"

# --- 2. coalescing; escalate (b): still inactive 30 min after a delivered page -> exactly one -----------------
tick "$D" $((T0+960)); tick "$D" $((T0+2400))
check "< 60 min since the alert: no duplicate notice or page" test "$(nlog "$D")/$(npage "$D")" = 1/1
check "< 30 min after the delivered page: no escalate" test "$(nesc "$D")" = 0
tick "$D" $((T0+900+1800))
check "30 min after a delivered page, still inactive: exactly one escalate" test "$(nesc "$D")" = 1
check "escalate called like pb-seat-parity: --subject --body --severity critical" \
  bash -c "head -1 '$D/ctl/escalate.log' | \"$PY\" -c 'import json,sys; a=json.loads(sys.stdin.read()); sys.exit(not (a[0]==\"--subject\" and a[2]==\"--body\" and a[4:]==[\"--severity\",\"critical\"]))'"
tick "$D" $((T0+900+1860)); tick "$D" $((T0+900+3599))
check "later ticks: still exactly one escalate (coalesced)" test "$(nesc "$D")" = 1
tick "$D" $((T0+900+3600))
check "60 min after the alert: one re-alert (notice 2, page 2), no second escalate" test "$(nlog "$D")/$(npage "$D")/$(nesc "$D")" = 2/2/1

# --- 3. recovery ---------------------------------------------------------------------------------------------
setunit "$D" $U1 loaded active
tick "$D" $((T0+5000))
check "recovery: one more notice and page" test "$(nlog "$D")/$(npage "$D")" = 3/3
check "recovery: unit.recovered on the SAME identity key" \
  grep -q -- "--event-type unit.recovered --claim notice --identity-key nexus-vault-sync.unit-inactive:testhost:$U1" "$D/ctl/lnotify.log"
check "recovery: page reason says RECOVERED" test "$(pagefield "$D" 2 '"RECOVERED" in d["body"]["reason"]')" = True
check "recovery: state cleared" bash -c "! ls '$(st "$D")' | grep -q '^unit\.$U1\.'"
tick "$D" $((T0+5060))
check "after recovery: no further emits" test "$(nlog "$D")/$(nesc "$D")" = 3/1
D=$(newbox early); setunit "$D" $U1 loaded inactive
tick "$D" $T0; tick "$D" $((T0+900)); tick "$D" $((T0+1500)); setunit "$D" $U1 loaded active; tick "$D" $((T0+1600))
setunit "$D" $U1 loaded inactive; tick "$D" $((T0+1700)); tick "$D" $((T0+2800))
check "recovery before 30 min after the page: no escalate (and the next episode starts fresh)" test "$(nesc "$D")" = 0

# --- 4. absent unit skipped; recovery before the threshold is silent; failed counts -------------------------
D=$(newbox b); setunit "$D" $U1 loaded active   # U2 has no load file -> LoadState=not-found (icarus shape)
tick "$D" $T0; tick "$D" $((T0+5000))
check "absent unit: skipped, no alert, no state" bash -c "[ \"\$(grep -c '^ARGV' '$D/ctl/lnotify.log' 2>/dev/null || echo 0)\" = 0 ] && ! ls '$(st "$D")' | grep -q '^unit\.'"
setunit "$D" $U1 loaded failed; tick "$D" $((T0+6000)); setunit "$D" $U1 loaded active; tick "$D" $((T0+6300))
check "recovered before 900s: silent" test "$(nlog "$D")" = 0
setunit "$D" $U1 loaded failed; tick "$D" $((T0+7000)); tick "$D" $((T0+7900))
check "a failed unit counts as not-active (alert at 900s)" test "$(nlog "$D")" = 1

# --- 5. send-time resolution of the HOST arranger --------------------------------------------------------------
D=$(newbox rot); setunit "$D" $U1 loaded inactive
tick "$D" $T0; tick "$D" $((T0+900))
check "rotation: the first alert pages the original arranger" test "$(lastpage "$D")" = "$CSID"
release "$D" "$CSID"; marker "$D" "$CSID2" 2026-10-26T12:00:00+00:00
tick "$D" $((T0+900+3600))
check "rotation: the re-alert pages the NEW arranger (nothing pinned)" test "$(lastpage "$D")" = "$CSID2"
marker "$D" "$CSID3" 2026-10-26T11:00:00+00:00
setunit "$D" $U1 loaded active; tick "$D" $((T0+9000))
check "two live markers: pages the newest claimed_at" test "$(lastpage "$D")" = "$CSID2"
check "two live markers: logged loudly" grep -q 'LOUD: 2 live arranger role markers' "$(st "$D")/log"

D=$(newbox rlink); nomarker "$D"; echo link > "$D/ctl/hostname"; setunit "$D" $U1 loaded inactive
roster "$D" pitboss=$CSID2 pitboss-icarus=$CSID3
tick "$D" $T0; tick "$D" $((T0+900))
check "link, no marker: pages the live roster row 'pitboss'" test "$(lastpage "$D")" = "$CSID2"
check "roster read: the key goes to curl on stdin, never argv" \
  bash -c "grep -q 'Authorization: Bearer $NKEY' '$D/ctl/roster.stdin' && ! tr '\\0' '\\n' < '$D/ctl/roster.argv' | grep -q '$NKEY'"
D=$(newbox ricarus); nomarker "$D"; echo icarus > "$D/ctl/hostname"; setunit "$D" $U1 loaded inactive
roster "$D" pitboss=$CSID2 pitboss-icarus=$CSID3
tick "$D" $T0; tick "$D" $((T0+900))
check "icarus, no marker: pages 'pitboss-icarus' (its own arranger), never link's 'pitboss'" test "$(lastpage "$D")" = "$CSID3"
D=$(newbox rtrin); nomarker "$D"; echo trinity > "$D/ctl/hostname"; setunit "$D" $U1 loaded inactive
roster "$D" pitboss=$CSID2 pitboss-trinity=$CSID
tick "$D" $T0; tick "$D" $((T0+900))
check "trinity, no marker: pages 'pitboss-trinity'" test "$(lastpage "$D")" = "$CSID"
D=$(newbox runk); nomarker "$D"; echo zion > "$D/ctl/hostname"; setunit "$D" $U1 loaded inactive
roster "$D" pitboss=$CSID2 pitboss-zion=$CSID3
tick "$D" $T0; tick "$D" $((T0+900))
check "unknown host, no override: no name fallback, no page" test "$(npage "$D")" = 0
check "unknown host: operator escalate + notice + exit 3" test "$(nesc "$D")/$(nlog "$D")/$(rc "$D")" = 1/1/3
EXTRA_ENV=NVS_ROSTER_NAME=pitboss-zion tick "$D" $((T0+900+3600))
check "unknown host with NVS_ROSTER_NAME (drop-in override): pages that row" test "$(lastpage "$D")" = "$CSID3"

# --- 6. fail loud: nothing resolves / the page is undeliverable -> escalate, notice, exit 3, deaf path ran ----
D=$(newbox none); nomarker "$D"; setunit "$D" $U1 loaded inactive; health "$D" not_receiving
tick "$D" $T0; tick "$D" $((T0+900))
check "no arranger resolves: the tick exits 3" test "$(rc "$D")" = 3
check "no arranger resolves: exactly one operator escalate" test "$(nesc "$D")" = 1
check "no arranger resolves: the lnotify notice still fires, saying the page FAILED" \
  bash -c "[ \"\$(grep -c '^ARGV' '$D/ctl/lnotify.log')\" = 1 ] && grep -q 'page to the host arranger FAILED (UNRESOLVED)' '$D/ctl/lnotify.log'"
check "no arranger resolves: logged loudly" grep -q 'PAGE FAILED (LOUD): UNRESOLVED' "$(st "$D")/log"
check "no arranger resolves: the deaf path still ran this tick" test "$(cat "$(st "$D")/last_confirmed" 2>/dev/null)" = $((T0+900))
tick "$D" $((T0+960))
check "no arranger resolves: no escalate on the next tick (coalesced to the alert cadence)" test "$(nesc "$D")" = 1
D=$(newbox undeliv); setunit "$D" $U1 loaded inactive; echo 422 > "$D/ctl/page.code"
tick "$D" $T0; tick "$D" $((T0+900))
check "page non-2xx: escalate + notice + exit 3" test "$(nesc "$D")/$(nlog "$D")/$(rc "$D")" = 1/1/3
check "page non-2xx: logged as UNDELIVERABLE http=422" grep -q "PAGE FAILED (LOUD): UNDELIVERABLE $CSID role-marker http=422" "$(st "$D")/log"
tick "$D" $((T0+900+1800))
check "page non-2xx: no second escalate from the 30-min rule" test "$(nesc "$D")" = 1

# malformed / path-injection markers are rejected
D=$(newbox bad); nomarker "$D"; setunit "$D" $U1 loaded inactive
R="$D/home/.claude/runtime"
echo '{}' > "$R/conductor.role.arranger-ADEB0222-F303-4E70-9E9B-70EC52076179"
echo '{}' > "$R/conductor.role.arranger-..%2f..%2fetc"
mkdir -p "$D/outside"; marker "$D" "$CSID3" x; mv "$R/conductor.role.arranger-$CSID3" "$D/outside/"
ln -s "$D/outside/conductor.role.arranger-$CSID3" "$R/conductor.role.arranger-$CSID3"
printf '{"role":"arranger","csid":"%s"}\n' "$CSID3" > "$R/conductor.role.arranger-$CSID2"
tick "$D" $T0; tick "$D" $((T0+900))
check "malformed csid / symlink escape / content naming another csid: all rejected, no page" test "$(npage "$D")" = 0
check "malformed filename: rejection logged" grep -q 'role marker REJECTED: filename csid is not a full uuid' "$(st "$D")/log"
check "symlinked marker escaping the runtime dir: rejected" grep -q "REJECTED: conductor.role.arranger-$CSID3 escapes" "$(st "$D")/log"
check "content naming another csid: rejected" grep -q "REJECTED: conductor.role.arranger-$CSID2 content names another" "$(st "$D")/log"
check "only malformed markers: escalate, exit 3" test "$(nesc "$D")/$(rc "$D")" = 1/3

# NVS_PAGE_CSID is an explicit override (tests/emergency) and is still validated
D=$(newbox ovr); setunit "$D" $U1 loaded inactive
OVR=$CSID3 tick "$D" $T0; OVR=$CSID3 tick "$D" $((T0+900))
check "override: pages the override csid, not the marker" test "$(lastpage "$D")" = "$CSID3"
check "override: logged as an override" grep -q 'OVERRIDE in force' "$(st "$D")/log"
OVR=0f6e3c2a tick "$D" $((T0+900+3600))
check "override that is not a full uuid: refused (logged), exit 3" \
  bash -c "grep -q 'override is not a full session uuid' '$(st "$D")/log' && [ \"\$(cat '$D/ctl/rc')\" = 3 ]"

# --- 7. unit context: ONLY HOME, PATH, XDG_RUNTIME_DIR (a user unit's env); the resolver at its default path ----
D=$(newbox unitctx); setunit "$D" $U1 loaded inactive
for t in $T0 $((T0+900)); do
  echo $t > "$D/ctl/now"
  env -i HOME="$D/home" PATH="$D/bin:$(dirname "$PY"):/usr/bin:/bin" XDG_RUNTIME_DIR="$D/xdg" bash "$SUP" >/dev/null 2>&1
  echo $? > "$D/ctl/rc"
done
check "unit context (env -i HOME/PATH/XDG_RUNTIME_DIR only): page delivered, exit 0" test "$(npage "$D")/$(rc "$D")" = 1/0
check "unit context: the key came from the default-path resolver in exec form" \
  bash -c "grep -q 'Authorization: Bearer $NKEY' '$D/ctl/page.stdin.0' && tr '\\0' ' ' < '$D/ctl/keyhelper.argv' | grep -q '^exec -- '"

# --- 8. the not_receiving path is unchanged: differential run vs the pinned imported original ----------------
diffrun(){  # $1 label  $2 unit state for U1
  local dn do_ t i ok=1 states s
  dn=$(newbox "dn-$1"); do_=$(newbox "do-$1")
  for d in "$dn" "$do_"; do setunit "$d" $U1 loaded "$2"; echo "Mon 2026-10-26 00:00:00 UTC" > "$d/ctl/units/$U1.start"; done
  states="receiving not_receiving not_receiving FETCH not_receiving not_receiving not_receiving not_receiving"
  t=$T0; i=0
  for s in $states $(for _ in $(seq 1 40); do echo not_receiving; done) receiving not_receiving; do
    i=$((i+1))
    for d in "$dn" "$do_"; do
      if [ "$s" = FETCH ]; then rm -f "$d/ctl/health.json"; else health "$d" "$s"; fi
    done
    if [ $i = 5 ]; then t=$((t+300)); else t=$((t+60)); fi   # a 5-min gap exercises the 180 s confirmation rule
    SCRIPT="$SUP" tick "$dn" $t; SCRIPT="$ORIG" tick "$do_" $t
    for f in deaf_since last_confirmed last_restart; do
      [ "$(cat "$(st "$dn")/$f" 2>/dev/null)" = "$(cat "$(st "$do_")/$f" 2>/dev/null)" ] || { ok=0; echo "  diverged at tick $i ($s): $f"; }
    done
    [ "$(cat "$dn/ctl/restarts" 2>/dev/null)" = "$(cat "$do_/ctl/restarts" 2>/dev/null)" ] || { ok=0; echo "  restarts diverged at tick $i"; }
  done
  [ $ok = 1 ] && [ -s "$dn/ctl/restarts" ]
}
EXTRA_ENV=NVS_DEAF_SECS=1800 diffrun active active
check "not_receiving path: identical window/cooldown/restart behaviour to the original (unit active)" test $? = 0
EXTRA_ENV=NVS_DEAF_SECS=1800 diffrun inactive inactive
check "not_receiving path: identical behaviour while the inactive-unit alert is firing" test $? = 0
D=$(newbox r); setunit "$D" $U1 loaded active; health "$D" receiving
tick "$D" $T0
check "receiving: no deaf window, no restart" bash -c "[ ! -f '$(st "$D")/deaf_since' ] && [ ! -f '$D/ctl/restarts' ]"

echo "# $PASSES passed, $FAILS failed"
[ "$FAILS" = 0 ]
