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
# ---- O5-2a (LSEC-9325-B6D8-O5-ONFAILURE-RULING, O5-2 "No Silent Holds"): alert on an INACTIVE vault-sync unit.
# The not_receiving logic below cannot see a STOPPED unit (the server then reports nothing for it, which reads
# "inconclusive"). So, first and independently: for each watched unit that exists on this host, track how long it
# has been not-active; at >= NVS_INACTIVE_SECS raise ONE alert (lnotify notice on the infra rail + a conductor page
# to the arranger), re-alert at most every NVS_REALERT_SECS while still inactive, and emit one "recovered" when it
# is active again. ALERT ONLY: this block never starts, stops or restarts anything. It never exits the script.
WATCH_UNITS="${NVS_WATCH_UNITS:-nexus-vault-sync.service nexus-vault-sync-memory.service}"
INACTIVE_SECS="${NVS_INACTIVE_SECS:-900}"     # 15 min (O5-2)
REALERT_SECS="${NVS_REALERT_SECS:-3600}"      # at most one re-alert per 60 min per unit
HOST_SHORT="$(hostname -s 2>/dev/null || echo unknown-host)"
LNOTIFY="${NVS_LNOTIFY:-$(command -v lnotify 2>/dev/null || echo "$HOME/.local/bin/lnotify")}"
PAGE_CSID="${NVS_PAGE_CSID:-}"                # the arranger's FULL conductor session id; no default
PAGE_URL="${NVS_PAGE_URL:-https://nexus.obsidian-inc.com/api/conductor/coord/emit}"
PAGE_KEY_NAME="${NVS_PAGE_KEY_NAME:-CONDUCTOR_SERVICE_KEY_NVS_SUPERVISOR}"
PAGE_KEY_FILE="${NVS_PAGE_KEY_FILE:-$HOME/whetstone/.env}"
unit_alert(){  # $1 unit  $2 raised|recovered  $3 one-line detail
  local u="$1" kind="$2" detail="$3" ev claim=notice
  [ "$kind" = recovered ] && ev=unit.recovered || ev=unit.stopped
  local ikey="nexus-vault-sync.unit-inactive:${HOST_SHORT}:${u}"
  local msg="[nexus-vault-sync-supervisor@${HOST_SHORT}] ${u}: ${detail}"
  # (1) lnotify, claim notice: the vault-note route + the ledger, never the interruptive nexus_nudge surface.
  if [ -x "$LNOTIFY" ] && printf '%s' "$msg" | "$LNOTIFY" legacy --principal "${HOST_SHORT}.nexus-vault-sync-supervisor@lattice" \
       --app nexus-vault-sync --venture lattice --event-type "$ev" --claim "$claim" --identity-key "$ikey" \
       --subject "$u" --sender nexus-vault-sync-supervisor --caused-by "$u" >/dev/null 2>>"$ST/lnotify.err"; then
    log "ALERT-NOTIFY-OK kind=$kind unit=$u ikey=$ikey"
  else
    log "ALERT-NOTIFY-FAILED kind=$kind unit=$u ikey=$ikey (lnotify=$LNOTIFY; see $ST/lnotify.err)"
  fi
  # (2) conductor page to the arranger, by FULL csid only. The service key is read by NAME inside python and
  # reaches curl on STDIN (-K -), never argv, env or stdout.
  case "$PAGE_CSID" in
    "") log "PAGE SKIPPED (LOUD): NVS_PAGE_CSID is unset — no arranger page for kind=$kind unit=$u"; return 0 ;;
    [0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f]-[0-9a-f][0-9a-f][0-9a-f][0-9a-f]-[0-9a-f][0-9a-f][0-9a-f][0-9a-f]-[0-9a-f][0-9a-f][0-9a-f][0-9a-f]-[0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f]) ;;
    *) log "PAGE SKIPPED (LOUD): NVS_PAGE_CSID is not a full session uuid (len=${#PAGE_CSID}) — refusing to page"; return 0 ;;
  esac
  local out
  out=$(python3 - "$PAGE_KEY_FILE" "$PAGE_KEY_NAME" "$PAGE_URL" "$PAGE_CSID" "$HOST_SHORT" "$u" "$kind" "$msg" <<'PY' 2>&1
import json, re, subprocess, sys
kf, kn, url, csid, host, unit, kind, msg = sys.argv[1:9]
if not re.fullmatch(r"CONDUCTOR_SERVICE_KEY_[A-Z0-9_]+", kn):
    print("PAGE SKIPPED: key NAME is not a CONDUCTOR_SERVICE_KEY_* name"); sys.exit(0)
key = None
try:
    for line in open(kf, encoding="utf-8", errors="replace"):
        line = line.strip()
        if line.startswith("export "): line = line[7:].lstrip()
        if line.startswith(kn + "="):
            key = line[len(kn) + 1:].strip().strip('"').strip("'")
except OSError:
    pass
if not key:
    print("PAGE SKIPPED (PROVISIONING GAP): key %s not found in %s" % (kn, kf)); sys.exit(0)
if not re.fullmatch(r"[A-Za-z0-9._~+/=-]+", key):
    print("PAGE SKIPPED: key value has a disallowed character (not echoed)"); sys.exit(0)
payload = {"kind": "talk", "session_id": csid, "body": {
    "from": "nexus-vault-sync-supervisor@%s (automated)" % host,
    "topic": "vault-sync unit %s on %s: %s" % (unit, host, "RECOVERED" if kind == "recovered" else "INACTIVE"),
    "detail": msg}}
r = subprocess.run(["curl", "-sS", "-m", "25", "-X", "POST", url, "-H", "Content-Type: application/json",
                    "-A", "curl/8.7.1", "-K", "-", "-w", "\n%{http_code}", "-d", json.dumps(payload)],
                   input='header = "Authorization: Bearer %s"\n' % key, capture_output=True, text=True,
                   errors="replace")
body, _, code = r.stdout.rpartition("\n")
if r.returncode == 0 and code.strip().startswith("2"):
    print("PAGE-OK http=%s" % code.strip())
else:
    print("PAGE-FAILED rc=%s http=%s" % (r.returncode, code.strip() or "?"))
PY
)
  log "$out kind=$kind unit=$u csid=$PAGE_CSID"
}
check_inactive_units(){
  local u load act f_since f_alert since dur last
  for u in $WATCH_UNITS; do
    f_since="$ST/unit.$u.inactive_since"; f_alert="$ST/unit.$u.last_alert"
    load=$(systemctl --user show -p LoadState --value "$u" 2>/dev/null)
    case "$load" in
      not-found) rm -f "$f_since" "$f_alert"; continue ;;   # not on this host (e.g. icarus has no -memory): skipped
      "") log "unit $u: systemctl unreadable — inconclusive, no change"; continue ;;
    esac
    act=$(systemctl --user is-active "$u" 2>/dev/null)
    case "$act" in
      "") log "unit $u: is-active unreadable — inconclusive, no change"; continue ;;
      active)
        if [ -f "$f_since" ]; then
          since=$(cat "$f_since"); dur=$((NOW-since))
          if [ -f "$f_alert" ]; then
            log "unit $u RECOVERED: active again after ${dur}s not-active"
            unit_alert "$u" recovered "active again after ${dur}s not-active (since $(date -u -d "@$since" +%FT%TZ))"
          else
            log "unit $u active again after ${dur}s (< ${INACTIVE_SECS}s, never alerted)"
          fi
          rm -f "$f_since" "$f_alert"
        fi ;;
      *)
        [ -f "$f_since" ] || { echo "$NOW" > "$f_since"; log "unit $u not active ($act, LoadState=$load) — inactive window opened"; }
        since=$(cat "$f_since"); dur=$((NOW-since))
        [ "$dur" -ge "$INACTIVE_SECS" ] || continue
        last=$(cat "$f_alert" 2>/dev/null || echo 0)
        [ $((NOW-last)) -ge "$REALERT_SECS" ] || continue
        echo "$NOW" > "$f_alert"
        log "unit $u INACTIVE ${dur}s (>= ${INACTIVE_SECS}s; state=$act) — alerting"
        unit_alert "$u" raised "NOT ACTIVE ($act) for ${dur}s since $(date -u -d "@$since" +%FT%TZ) (threshold ${INACTIVE_SECS}s). Alert only: nothing was restarted. Re-alert at most every ${REALERT_SECS}s." ;;
    esac
  done
}
check_inactive_units || log "inactive-unit check errored — continuing to the not_receiving check"
# ---- end O5-2a
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
