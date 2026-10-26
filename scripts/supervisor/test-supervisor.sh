#!/usr/bin/env bash
# Tests for nexus-vault-sync-supervisor.sh (O5-2a inactive-unit alert + the unchanged not_receiving path).
# Self-contained: bash + python3 only. Every external command the supervisor calls (systemctl, curl, lnotify,
# logger, date +%s, hostname) is a PATH shim driven by files under a temp dir, so nothing touches a real unit,
# the journal, the network or the notify rail.
#   usage: bash scripts/supervisor/test-supervisor.sh [path/to/supervisor.sh]
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
SUP="${1:-$HERE/nexus-vault-sync-supervisor.sh}"
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
KEYVAL=TESTKEYvalue0123456789abcdef

mkshims(){  # $1 dir
  local b="$1/bin" c="$1/ctl"; mkdir -p "$b" "$c/units" "$1/home"
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
  cat > "$b/curl" <<EOF
#!/usr/bin/env bash
C="$c"
for a in "\$@"; do case "\$a" in *coord/emit*) printf '%s\0' "\$@" > "\$C/page.argv.\$(ls "\$C" | grep -c '^page.argv')"; cat >> "\$C/page.stdin"; printf '{"ok":true}\n200'; exit 0 ;; esac; done
cat "\$C/health.json" 2>/dev/null || exit 7
EOF
  cat > "$b/lnotify" <<EOF
#!/usr/bin/env bash
C="$c"
{ printf 'ARGV'; printf ' %q' "\$@"; printf '\nMSG '; cat; printf '\n'; } >> "\$C/lnotify.log"
EOF
  printf '#!/usr/bin/env bash\nexit 0\n' > "$b/logger"
  cat > "$b/date" <<EOF
#!/usr/bin/env bash
[ "\$*" = "+%s" ] && { cat "$c/now"; exit 0; }
exec "$REAL_DATE" "\$@"
EOF
  printf '#!/usr/bin/env bash\necho testhost\n' > "$b/hostname"
  chmod +x "$b"/*
  echo "CONDUCTOR_SERVICE_KEY_NVS_SUPERVISOR=$KEYVAL" > "$1/key.env"
}

# one tick of the supervisor in sandbox $1 at epoch $2 (extra env via the caller's exported vars)
tick(){
  local d="$1"; echo "$2" > "$d/ctl/now"
  env -i PATH="$d/bin:$(dirname "$PY"):/usr/bin:/bin" HOME="$d/home" \
      NVS_SUBSCRIBER_ID=sub-test NVS_HEALTH_URL=http://health.invalid/api/sync/health \
      NVS_PAGE_KEY_FILE="$d/key.env" ${CSID_ENV-NVS_PAGE_CSID=$CSID} ${EXTRA_ENV:-} \
      bash "${SCRIPT:-$SUP}" >/dev/null 2>&1
}
nlog(){ grep -c '^ARGV' "$1/ctl/lnotify.log" 2>/dev/null || echo 0; }
npage(){ ls "$1/ctl" | grep -c '^page.argv' ; }
st(){ echo "$1/home/.local/state/nexus-vault-sync-supervisor"; }
setunit(){ echo "$3" > "$1/ctl/units/$2.load"; echo "$4" > "$1/ctl/units/$2.active"; }
health(){ printf '{"subscribers":[{"subscriber_id":"sub-test","receipt_state":"%s"}]}' "$2" > "$1/ctl/health.json"; }
newbox(){ local d="$T/$1"; mkshims "$d"; health "$d" receiving; echo "$d"; }

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
# The differential below runs against the PINNED imported copy, never against a derivation of the script under test.
ORIG="$HERE/testdata/nexus-vault-sync-supervisor.imported.sh"
check "pinned imported copy is intact (sha256 $ORIG_SHA)" test "$(sha256sum "$ORIG" | cut -d' ' -f1)" = "$ORIG_SHA"

# --- 1. inactive < threshold: no alert; >= threshold: exactly one lnotify + one page ------------------------
D=$(newbox a); setunit "$D" $U1 loaded inactive; setunit "$D" $U2 loaded active
tick "$D" $T0; tick "$D" $((T0+600)); tick "$D" $((T0+899))
check "inactive 899s < 900s: no lnotify" test "$(nlog "$D")" = 0
check "inactive 899s < 900s: no page" test "$(npage "$D")" = 0
check "inactive window opened at first not-active tick" test "$(cat "$(st "$D")/unit.$U1.inactive_since")" = $T0
tick "$D" $((T0+900))
check "inactive 900s: exactly one lnotify" test "$(nlog "$D")" = 1
check "inactive 900s: exactly one page" test "$(npage "$D")" = 1
check "lnotify: notice claim, unit.stopped, per host+unit identity key" \
  grep -q -- "--event-type unit.stopped --claim notice --identity-key nexus-vault-sync.unit-inactive:testhost:$U1" "$D/ctl/lnotify.log"
check "lnotify principal is the host-scoped supervisor" grep -q -- "--principal testhost.nexus-vault-sync-supervisor@lattice" "$D/ctl/lnotify.log"
check "page addressed to the full csid" grep -q "\"session_id\": \"$CSID\"" "$D/ctl/page.argv.0"
check "page: service key never on curl argv" bash -c "! grep -q '$KEYVAL' '$D/ctl/page.argv.0'"
check "page: service key reaches curl on stdin (-K -)" grep -q "Authorization: Bearer $KEYVAL" "$D/ctl/page.stdin"
check "the healthy unit raised nothing" bash -c "! grep -q -- '$U2' '$D/ctl/lnotify.log'"

# --- 2. coalescing: no duplicate inside 60 min; one re-alert at 60 min ----------------------------------------
tick "$D" $((T0+960)); tick "$D" $((T0+2400)); tick "$D" $((T0+900+3599))
check "still inactive, < 60 min since the alert: no duplicate lnotify" test "$(nlog "$D")" = 1
check "still inactive, < 60 min since the alert: no duplicate page" test "$(npage "$D")" = 1
tick "$D" $((T0+900+3600))
check "60 min after the alert: one re-alert (lnotify 2)" test "$(nlog "$D")" = 2
check "60 min after the alert: one re-alert (page 2)" test "$(npage "$D")" = 2

# --- 3. recovery: one recovered emit (same identity key), state cleared ---------------------------------------
setunit "$D" $U1 loaded active
tick "$D" $((T0+5000))
check "recovery: one more lnotify" test "$(nlog "$D")" = 3
check "recovery: one more page" test "$(npage "$D")" = 3
check "recovery: unit.recovered on the SAME identity key" \
  grep -q -- "--event-type unit.recovered --claim notice --identity-key nexus-vault-sync.unit-inactive:testhost:$U1" "$D/ctl/lnotify.log"
check "recovery: page topic says RECOVERED" grep -q 'RECOVERED' "$D/ctl/page.argv.2"
check "recovery: state cleared" bash -c "! ls '$(st "$D")' | grep -q '^unit\.$U1\.'"
tick "$D" $((T0+5060))
check "after recovery: no further emits" test "$(nlog "$D")" = 3

# --- 4. absent unit skipped; recovery before the threshold is silent; failed counts as not-active ------------
D=$(newbox b); setunit "$D" $U1 loaded active   # U2 has no load file -> LoadState=not-found (icarus shape)
tick "$D" $T0; tick "$D" $((T0+5000))
check "absent unit: skipped, no alert" test "$(nlog "$D")" = 0
check "absent unit: no state file" bash -c "! ls '$(st "$D")' | grep -q '^unit\.'"
setunit "$D" $U1 loaded failed; tick "$D" $((T0+6000)); setunit "$D" $U1 loaded active; tick "$D" $((T0+6300))
check "recovered before 900s: silent" test "$(nlog "$D")" = 0
setunit "$D" $U1 loaded failed; tick "$D" $((T0+7000)); tick "$D" $((T0+7900))
check "a failed unit counts as not-active (alert at 900s)" test "$(nlog "$D")" = 1

# --- 5. NVS_PAGE_CSID unset: lnotify still fires, the page is skipped loudly ----------------------------------
D=$(newbox c); setunit "$D" $U1 loaded inactive
CSID_ENV= tick "$D" $T0; CSID_ENV= tick "$D" $((T0+900))
check "csid unset: lnotify still fires" test "$(nlog "$D")" = 1
check "csid unset: no page" test "$(npage "$D")" = 0
check "csid unset: logged loudly" grep -q 'PAGE SKIPPED (LOUD): NVS_PAGE_CSID is unset' "$(st "$D")/log"
CSID_ENV=NVS_PAGE_CSID=0f6e3c2a tick "$D" $((T0+900+3600))
check "csid not a full uuid: refused, no page" test "$(npage "$D")" = 0

# --- 6. the not_receiving path is unchanged: differential run vs the pinned imported original ------------------------
# Same scenario through both scripts; restarts and the window/cooldown state files must match after every tick.
diffrun(){  # $1 label  $2 unit state for U1
  local dn do_ t i ok=1 states
  dn=$(newbox "dn-$1"); do_=$(newbox "do-$1")
  for d in "$dn" "$do_"; do setunit "$d" $U1 loaded "$2"; echo "Mon 2026-10-26 00:00:00 UTC" > "$d/ctl/units/$U1.start"; done
  states="receiving not_receiving not_receiving FETCH not_receiving not_receiving not_receiving not_receiving"
  t=$T0; i=0
  for s in $states $(for _ in $(seq 1 40); do echo not_receiving; done) receiving not_receiving; do
    i=$((i+1))
    for d in "$dn" "$do_"; do
      if [ "$s" = FETCH ]; then rm -f "$d/ctl/health.json"; else health "$d" "$s"; fi
    done
    # a 5-minute gap at tick 5 exercises the 180 s confirmation-gap rule
    if [ $i = 5 ]; then t=$((t+300)); else t=$((t+60)); fi
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
check "not_receiving path: identical behaviour even while the inactive-unit alert is firing" test $? = 0
D=$(newbox r); setunit "$D" $U1 loaded active; health "$D" receiving
tick "$D" $T0
check "receiving: no deaf window, no restart" bash -c "[ ! -f '$(st "$D")/deaf_since' ] && [ ! -f '$D/ctl/restarts' ]"

echo "# $PASSES passed, $FAILS failed"
[ "$FAILS" = 0 ]
