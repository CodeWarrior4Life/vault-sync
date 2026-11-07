#!/usr/bin/env bash
# Synthetic-log tests for scripts/lift_watch.py (r3.1). Usage: scripts/test_lift_watch.sh [path/to/lift_watch.py]
# Exit 0 only if every case gives its expected verdict. Run it in the BACKGROUND (it waits on files).
set -u
LW=$(readlink -f "${1:-$(dirname "$0")/lift_watch.py}")
RES='2026-11-07T10:00:00Z  WARN x: OUTBOUND DELETE PROPAGATION RESUMED'
REP0='2026-11-07T10:00:01Z  WARN x: REPLAYED suppressed deletes replayed=0 skipped_present=3'
fail=0
check(){ local name=$1 want=$2 got; got=$(python3 -c 'import json,sys;d=json.load(open(sys.argv[1]));print(d["verdict"]+"|"+((d.get("trip") or {}).get("reason","")))' "$3/r.json")
  if [[ "$got" == $want ]]; then echo "PASS $name ($got)"; else echo "FAIL $name: want $want got $got"; fail=1; fi; }
mk(){ D=$(mktemp -d); echo PAUSED > $D/marker.bak; : > $D/daemon.log.2026-11-07; }
run(){ python3 "$LW" --log-glob "$D/daemon.log.*" --marker $D/marker --marker-backup $D/marker.bak --receipt $D/r.json --ready-file $D/READY "$@" >/dev/null; }
waitready(){ until [ -f "$D/READY" ]; do sleep 0.2; done; }

mk; (run --duration 3 --resume-timeout 10 --stop-cmd '["true"]' --dry-run) & waitready; printf '%s\n%s\n' "$RES" "$REP0" >> $D/daemon.log.2026-11-07; wait; check "1 PASS after READY handshake" "PASS|" $D; rm -rf $D
mk; printf '%s\n%s\n' "$RES" "$REP0" > $D/daemon.log.2026-11-07; run --duration 3 --resume-timeout 3 --stop-cmd '["true"]' --dry-run; check "2 RED: started after RESUMED -> BLIND, never PASS" "TRIPPED|BLIND*" $D; rm -rf $D
mk; run --duration 3 --resume-timeout 3 --stop-cmd "[\"sh\",\"-c\",\"test -e $D/marker && touch $D/STOPPED\"]"; check "3 BLIND live: restore then stop" "TRIPPED|BLIND*" $D; [ -e $D/marker ] && [ -e $D/STOPPED ] && echo "PASS 3b marker restored BEFORE stop" || { echo "FAIL 3b"; fail=1; }; rm -rf $D
mk; (run --duration 6 --resume-timeout 10 --stop-cmd '["true"]' --dry-run) & waitready; printf '%s\n' "${RES/RESUMED/RESUMED}" '2026-11-07T10:00:01Z  WARN x: REPLAYED suppressed deletes replayed=3' >> $D/daemon.log.2026-11-07; wait; check "4 replayed=3 trips" "TRIPPED|replayed=3*" $D; rm -rf $D
mk; (run --duration 6 --resume-timeout 10 --stop-cmd '["true"]' --dry-run) & waitready; printf '%s\n%s\n' "$RES" '2026-11-07T10:00:02Z  INFO p: push accepted path=z y.md seq=0 hash=' >> $D/daemon.log.2026-11-07; wait; check "5 outbound delete push trips" "TRIPPED|outbound delete push*" $D; rm -rf $D
mk; (run --duration 6 --resume-timeout 10 --stop-cmd '["/nonexistent/stop"]') & waitready; printf '%s\n' "$RES" '2026-11-07T10:00:02Z ERROR x: boom' >> $D/daemon.log.2026-11-07; wait; check "6 ERROR trips" "TRIPPED|daemon ERROR*" $D; python3 -c 'import json,sys;d=json.load(open(sys.argv[1]));assert d["marker_restored"] and not d["stopped"];print("PASS 6b nonexistent stop caught, marker restored")' $D/r.json || fail=1; rm -rf $D
exit $fail
