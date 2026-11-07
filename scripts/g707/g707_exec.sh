#!/bin/bash
# LSEC-G707 executor: Trinity MAIN f2383e35 delete-valve lift, P-zero (bind request r2).
# Runs ON Trinity from its frozen copy (macOS, bash 3.2). The ONE run command (from link) is in the bind:
#   ssh trinity 'cd /Users/cyril/.cache/g707 && echo "<SUMS sha>  SHA256SUMS" | shasum -a 256 -c - && shasum -a 256 -c SHA256SUMS && /bin/bash g707_exec.sh'
# Fail-stop at every step. The marker is removed ONLY in step 5, ONLY if the watcher is READY and the
# ledger sha is still the post-prune sha. Never uses lsof or tmutil (both hang on this host, TKT-e2414ec0).
set -euo pipefail
umask 077
export PATH=/usr/bin:/bin:/usr/sbin:/sbin
UIDN=$(id -u)
G=/Users/cyril/.cache/g707
S="/Users/cyril/Library/Application Support/Nexus/.lattice-runtime/f2383e35-2e9d-4da2-b5ed-de8a35778fa3/sync-state"
V=/Users/cyril/Vaults/Mainframe
M="$S/delete-burst-PAUSED"; LED="$S/delete-burst-SUPPRESSED"
LOGDIR="/Users/cyril/Library/Application Support/Nexus/logs"
B=/Users/cyril/.claude/backups/vault-sync/lift-g707-r2
KP="02_Projects/Operations/_attachments/Kirk/docket/"
LABEL=com.lattice.lift-watch-g707
EXE="/Applications/Nexus Vault Sync.app/Contents/MacOS/vault-sync-daemon"
EXE_SHA=129e17df55695a89ad9f483cff9254e65f208e22cdbda6e63315b9ce1a7d52d1   # 0.4.47
MARKER_SHA=f57e34db6bccfd1e93dd43b6ca30361841ecdce1559003098b14fa66fcd46ff8
log(){ echo "[$(date -u +%H:%M:%SZ)] $*"; }
die(){ log "STOP: $*"; exit 1; }
sha(){ shasum -a 256 "$1" | cut -c1-64; }
newlog(){ ls -t "$LOGDIR"/daemon.log.* | grep -v '\.gz$' | head -1; }
main_pids(){ pgrep -f '/Applications/Nexus Vault Sync.app/Contents/MacOS/vault-sync-daemon' || true; }

log "== step 0 pre-flight (read-only)"
MIN=$((10#$(date +%M)))
[ "$MIN" -ge 8 ] && [ "$MIN" -le 20 ] || die "window: start only at minute 08-20 (the 1080 s watch must end before the ~xx:50 memory-instance deletes and the xx:58 pull_backfill); now minute $MIN"
P=$(main_pids); [ "$(echo "$P" | grep -c .)" = 1 ] || die "main daemon count != 1: $(echo $P)"
log "main pid $P started $(ps -o lstart= -p "$P")"; echo "$P" > /tmp/g707-main-pid-before
log "memory pid $(pgrep -f 'vault-sync-memory/Nexus Vault Sync.app/Contents/MacOS/vault-sync-daemon' | head -1)"
[ "$(sha "$EXE")" = "$EXE_SHA" ] || die "exe is not 0.4.47 129e17df"
[ -f "$M" ] && [ "$(sha "$M")" = "$MARKER_SHA" ] || die "marker missing or sha != f57e34db"
[ ! -e "$B" ] || die "$B already exists (a previous run): refuse"
launchctl print "gui/$UIDN/$LABEL" >/dev/null 2>&1 && die "$LABEL already loaded"
f=$(newlog); m1=$(stat -f %m "$f"); sleep 35; m2=$(stat -f %m "$(newlog)")
[ "$m2" -gt "$m1" ] || die "C3: daemon log mtime did not advance in 35 s"
[ "$(grep -c 'push accepted' "$f")" -gt 0 ] || die "C3: no 'push accepted' line today"
H=$(for g in "$LOGDIR"/daemon.log.*; do case $g in *.gz) gzcat "$g";; *) cat "$g";; esac; done | grep -c -E 'push accepted path=.* hash=$' || true)
[ "$H" -gt 0 ] || die "C3: no historical empty-hash delete line (watch regex unproven)"
log "C3 see-proofs PASS (mtime $m1->$m2; historical delete lines $H)"

log "== step 1 backups -> $B"
mkdir -m 700 -p "$B"
cp -p "$M" "$LED" "$B/"; chmod 600 "$B"/delete-burst-*
shasum -a 256 "$B"/delete-burst-*
# O2 (eb97 ruling 15:2x): APFS clone (clonefile, no tmutil) of the whole sync-state dir (marker + ledger + siblings), path + size checked
cp -c -R "$S" "$B/sync-state.clone" || die "O2: APFS clone of sync-state failed"
n0=$(find "$S" -type f | wc -l | tr -d ' '); n1=$(find "$B/sync-state.clone" -type f | wc -l | tr -d ' ')
k0=$(du -sk "$S" | cut -f1); k1=$(du -sk "$B/sync-state.clone" | cut -f1)
[ "$n0" = "$n1" ] || die "O2: clone file count $n1 != source $n0"
[ "$(sha "$B/sync-state.clone/delete-burst-PAUSED")" = "$MARKER_SHA" ] || die "O2: cloned marker sha mismatch"
[ "$(sha "$B/sync-state.clone/delete-burst-SUPPRESSED")" = "$(sha "$LED")" ] || die "O2: cloned ledger sha mismatch (ledger moved during clone: re-run)"
log "O2 clone $B/sync-state.clone: files $n1 (src $n0), size ${k1} KiB (src ${k0} KiB)"
( cd "$V" && find "$KP" -type f -name '*.md' -exec shasum -a 256 {} + ) > "$B/docket-before.sha256" || true
# eb97 ruling (b): the expected false-trip rate goes on record, so a trip is not misread as a fault
printf '%s\n' '{"expected_false_trip_rate_per_window":"~15% (shared log: ~12 ERROR/day + memory-instance _session_summary.md deletes)","trip_action":"marker restored + stop + re-open RE-LATCHED (no outage)","ruling":"pitboss-eb97 52d6ac81 2026-11-07 15:2x EST (b) ACCEPTED"}' > "$B/EXPECTED.json"
log "docket md before: $(wc -l < "$B/docket-before.sha256" | tr -d ' ') files (manifest sha16 $(sha "$B/docket-before.sha256" | cut -c1-16))"

log "== step 2 T-0 ledger read"
L0=$(sha "$LED"); log "T0 ledger sha $L0 lines $(wc -l < "$LED" | tr -d ' ')"

log "== step 3 listing + prune (dry, then apply) + exclusion assertion"
python3 "$G/ledger_list_pzero.py" --ledger "$LED" --vault-root "$V" --out "$B/listing-T0.tsv" --protect-prefix "$KP" 2> "$B/listing-T0.json"
cat "$B/listing-T0.json"
python3 "$G/ledger_prune_pzero.py" --ledger "$LED" --vault-root "$V" --expect-sha "$L0"
set +e; OUT=$(python3 "$G/ledger_prune_pzero.py" --ledger "$LED" --vault-root "$V" --expect-sha "$L0" --apply); RC=$?; set -e
if [ "$RC" = 3 ]; then echo "$OUT"; die "prune rc 3 (RACE: the daemon appended); ledger NOT restored, so the appended line is kept (sentinel F6). Marker NOT removed. Re-run needs a fresh bind of a new \$B"; fi
if [ "$RC" != 0 ]; then echo "$OUT"; cp -p "$B/delete-burst-SUPPRESSED" "$LED"; die "prune rc $RC (ledger restored from backup). Marker NOT removed"; fi
echo "$OUT"
echo "$OUT" | python3 -c 'import json,sys;d=json.load(sys.stdin);assert d["verdict"]=="OK" and d["absent_after"]==0, d' || die "prune verdict"
L1=$(sha "$LED"); log "post-prune ledger sha $L1"
python3 "$G/ledger_list_pzero.py" --ledger "$LED" --vault-root "$V" --out "$B/listing-post.tsv" --protect-prefix "$KP" --assert-clear 2> "$B/listing-post.json" || die "EXCLUSION: a ledger line under $KP would still replay"
cat "$B/listing-post.json"
grep -q '^DROP' "$B/listing-post.tsv" && die "post-prune listing still has DROP rows"
log "EXCLUSION assertion PASS: no ledger line under $KP can replay"

log "== step 4 arm the launchd-carried watcher, block on READY (fail closed)"
cp "$G/$LABEL.plist" "$B/$LABEL.plist"; chmod 600 "$B/$LABEL.plist"
launchctl bootstrap "gui/$UIDN" "$B/$LABEL.plist" || die "watcher bootstrap failed (marker NOT removed)"
i=0; while [ ! -f "$B/READY" ] && [ $i -lt 60 ]; do sleep 1; i=$((i+1)); done
[ -f "$B/READY" ] || { launchctl bootout "gui/$UIDN/$LABEL" 2>/dev/null; die "watcher not READY in 60 s (marker NOT removed)"; }
log "watcher READY: $(cat "$B/READY")"

log "== step 5 lift (C1: sha-matched rm, one command)"
mark=$(wc -l < "$(newlog)" | tr -d ' ')
[ "$(sha "$LED")" = "$L1" ] && rm -v "$M" || { launchctl bootout "gui/$UIDN/$LABEL" 2>/dev/null; die "ledger changed after prune (marker NOT removed)"; }
log "T_RM $(date -u +%s)"

log "== step 6 confirm RESUMED + REPLAYED (<= 90 s), then hand off to the watcher"
i=0; while [ $i -lt 45 ]; do tail -n +"$((mark+1))" "$(newlog)" | grep -q 'REPLAYED suppressed deletes' && break; sleep 2; i=$((i+1)); done
tail -n +"$((mark+1))" "$(newlog)" | grep -E 'OUTBOUND DELETE PROPAGATION RESUMED|REPLAYED suppressed deletes' | cut -c1-240 || log "WARN: RESUMED/REPLAYED not yet seen (the watcher fails closed at 120 s)"
[ -e "$LED" ] && log "ledger still present: $(wc -l < "$LED" | tr -d ' ') lines" || log "ledger cleared"
log "watch runs 1080 s after RESUMED under launchd $LABEL; receipt -> $B/lift-watch-receipt.json"
log "FOLLOW-UP due at T_RM+1250 s: /bin/bash $G/g707_followup.sh (sentinel F3; it pages on HELD / no receipt / main != 1)"
echo "BACKUP_DIR=$B"
