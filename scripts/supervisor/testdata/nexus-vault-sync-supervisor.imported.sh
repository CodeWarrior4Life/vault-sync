#!/usr/bin/env bash
# INTERIM restart-on-deaf supervisor for the link vault-sync daemon (TKT-978e7918).
# Rule (pitboss 4c5755d7, 2026-09-10): restart ONLY when the server reports this daemon's
# receipt_state == not_receiving CONTINUOUSLY for >= 15 min (NOT on event age — 847f MEASURED
# state and age are not coupled), and never more than once per 20 min. Everything is logged
# with timestamps to the journal (tag nexus-vault-sync-supervisor) and to a state dir.
# Reads the reconciler health route over the tailnet UNAUTHENTICATED (it is public read-only);
# no secret is handled. Reversible: `systemctl --user disable --now nexus-vault-sync-supervisor.timer`.
set -u
SID="${NVS_SUBSCRIBER_ID:-a6f8219e-2fcb-4a9a-a2c6-0d3471919d1c}"
URL="${NVS_HEALTH_URL:-http://100.115.13.82:5557/api/sync/health}"
UNIT="${NVS_UNIT:-nexus-vault-sync.service}"
DEAF_SECS="${NVS_DEAF_SECS:-1500}"       # 25 min continuous not_receiving (pitboss ruling v2 22:14Z: natural windows observed 4 & 14 min)
COOLDOWN_SECS="${NVS_COOLDOWN_SECS:-1200}" # >= 20 min between restarts
ST="$HOME/.local/state/nexus-vault-sync-supervisor"; mkdir -p "$ST"
NOW=$(date +%s)
log(){ logger -t nexus-vault-sync-supervisor -- "$*"; echo "$(date -u +%FT%TZ) $*" >> "$ST/log"; }
STATE=$(curl -fsS -m 10 -A curl/8.7.1 "$URL" 2>/dev/null | python3 -c '
import sys,json
try: d=json.load(sys.stdin)
except Exception: print("FETCH_FAIL"); raise SystemExit
for s in d.get("subscribers",[]):
    if s.get("subscriber_id")==sys.argv[1]: print(s.get("receipt_state") or "UNKNOWN"); break
else: print("ABSENT")' "$SID")
case "$STATE" in
  receiving) rm -f "$ST/deaf_since" "$ST/last_confirmed"; exit 0 ;;
  not_receiving) ;;
  *) log "health read inconclusive ($STATE) — no action"; exit 0 ;;   # fail OPEN on a bad read: never restart on missing evidence
esac
# C2 (lsec): a window is only as good as its confirmations. If the last CONFIRMED not_receiving sample is
# older than 3 ticks (180 s) — e.g. the tailnet was unreadable in between — the window re-opens from now.
LASTC=$(cat "$ST/last_confirmed" 2>/dev/null || echo 0)
if [ -f "$ST/deaf_since" ] && [ $((NOW-LASTC)) -gt 180 ]; then log "confirmation gap $((NOW-LASTC))s > 180s — window re-opened"; rm -f "$ST/deaf_since"; fi
echo "$NOW" > "$ST/last_confirmed"
[ -f "$ST/deaf_since" ] || { echo "$NOW" > "$ST/deaf_since"; log "not_receiving observed for $SID — window opened"; exit 0; }
SINCE=$(cat "$ST/deaf_since"); DEAF=$((NOW-SINCE))
[ "$DEAF" -ge "$DEAF_SECS" ] || exit 0
# C1 (lsec): the cooldown floors on the unit's REAL last start too, so a manual/nexus-issued restart counts.
LAST=$(cat "$ST/last_restart" 2>/dev/null || echo 0)
UNIT_START=$(systemctl --user show "$UNIT" -p ExecMainStartTimestamp --value 2>/dev/null); UNIT_EPOCH=$(date -d "$UNIT_START" +%s 2>/dev/null || echo 0)
[ "$UNIT_EPOCH" -gt "$LAST" ] && LAST=$UNIT_EPOCH
[ $((NOW-LAST)) -ge "$COOLDOWN_SECS" ] || { log "deaf ${DEAF}s but cooldown active — no action"; exit 0; }
SINCE_ISO=$(date -u -d "@$SINCE" +%FT%TZ)
log "INTENT: INTERRUPTED-WINDOW restart of $UNIT — $SID not_receiving since $SINCE_ISO, duration_at_interruption=${DEAF}s (>= ${DEAF_SECS}s). This is NOT a cure: natural windows of 4 and 14 min self-healed untouched (TKT-978e7918)."
if systemctl --user restart "$UNIT"; then echo "$NOW" > "$ST/last_restart"; rm -f "$ST/deaf_since" "$ST/last_confirmed"; log "OUTCOME: INTERRUPTED-WINDOW restart issued ok window_start=$SINCE_ISO duration=${DEAF}s (RESTART — counts against the 60-min fixed bar)"; else log "OUTCOME: INTERRUPTED-WINDOW restart FAILED rc=$? window_start=$SINCE_ISO duration=${DEAF}s"; fi
