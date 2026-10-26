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
# ---- O5-2a (LSEC-9325-B6D8-O5-ONFAILURE-RULING, O5-2 "No Silent Holds"; route+key per LSEC-40A8-B6D8-O52-ROUTE-AND-KEY-RULING
# and Pitboss PB-ADEB-O52A*): alert on an INACTIVE vault-sync unit. The not_receiving logic below cannot see a STOPPED
# unit (a stopped daemon reads "inconclusive"). So, first and independently, for each watched unit that exists here:
#   >= NVS_INACTIVE_SECS not active  -> lnotify NOTICE (the durable record) + a P0-tier conductor page to the HOST
#                                       arranger, resolved AT SEND TIME (role marker, then the host's roster name);
#   no arranger / page undeliverable -> operator_escalate at once, and this tick exits 3 (after the deaf path ran);
#   still inactive NVS_ESCALATE_SECS after a delivered page -> operator_escalate, exactly once per episode;
#   re-alert at most every NVS_REALERT_SECS; on recovery one "recovered" notice + page; state cleared.
# ALERT ONLY: this block never starts, stops or restarts a unit, and never exits the script.
WATCH_UNITS="${NVS_WATCH_UNITS:-nexus-vault-sync.service nexus-vault-sync-memory.service}"
INACTIVE_SECS="${NVS_INACTIVE_SECS:-900}"     # 15 min (O5-2)
REALERT_SECS="${NVS_REALERT_SECS:-3600}"      # at most one re-alert per 60 min per unit
ESCALATE_SECS="${NVS_ESCALATE_SECS:-1800}"    # operator_escalate if still inactive 30 min after a delivered page
HOST_SHORT="$(hostname -s 2>/dev/null || echo unknown-host)"
LNOTIFY="${NVS_LNOTIFY:-$(command -v lnotify 2>/dev/null || echo "$HOME/.local/bin/lnotify")}"
# The page target is never pinned (an arranger csid dies at every seat rotation). NVS_PAGE_CSID is an explicit
# OVERRIDE for tests and emergencies only; never set it in a shipped unit or drop-in.
PAGE_OVERRIDE="${NVS_PAGE_CSID:-}"
ROLE_RUNTIME="${NVS_ROLE_RUNTIME:-$HOME/.claude/runtime}"
# The HOST's own arranger name, never a constant (PB-ADEB-O52A-NAME). Unknown host, no override -> no name fallback.
case "$HOST_SHORT" in link) _rn=pitboss ;; icarus) _rn=pitboss-icarus ;; trinity) _rn=pitboss-trinity ;; *) _rn="" ;; esac
ROSTER_NAME="${NVS_ROSTER_NAME:-$_rn}"
NEXUS_BASE="${NVS_NEXUS_BASE:-https://nexus.obsidian-inc.com}"
# The host's standard Nexus key resolver, EXEC form only: the key is injected into the child's env (NEXUS_API_KEY,
# the variable pb-prompt-watch.py reads) and reaches curl on stdin (-K -). Never argv, never stdout.
KEY_HELPER="${NVS_KEY_HELPER:-$HOME/.local/bin/nexus-api-key.sh}"
ESCALATE="${NVS_ESCALATE:-$HOME/.claude/scripts/operator_escalate.py}"
ESCALATE_SEVERITY="${NVS_ESCALATE_SEVERITY:-critical}"
TICK_RC=0                                     # 3 = no arranger resolved, or the page was undeliverable, this tick
escalate_op(){  # $1 unit  $2 subject  $3 body
  local out rc
  if [ ! -f "$ESCALATE" ]; then
    log "ESCALATE MISSING ($ESCALATE) — operator NOT paged for unit=$1: $2"; return 1
  fi
  out=$(timeout 180 python3 "$ESCALATE" --subject "$2" --body "$3" --severity "$ESCALATE_SEVERITY" 2>&1); rc=$?
  log "ESCALATE rc=$rc severity=$ESCALATE_SEVERITY unit=$1 subject=$2"
  [ "$rc" = 0 ] || printf '%s\n' "$out" | tail -3 >> "$ST/escalate.err"
  return "$rc"
}
page_arranger(){  # $1 unit  $2 raised|recovered  $3 message   -> prints LOG lines + one RESULT line
  local helper=() py
  [ -x "$KEY_HELPER" ] && helper=("$KEY_HELPER" exec --)
  py=$(cat <<'PY'
import json, os, re, subprocess, sys
override, runtime, rname, base, host, unit, kind, msg = sys.argv[1:9]
UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
PREFIX = "conductor.role.arranger-"
key = os.environ.get("NEXUS_API_KEY") or ""          # injected by the resolver's exec form (pb-prompt-watch.py:236)

def curl(path, body=None):
    """GET/POST base+path; the bearer goes to curl on STDIN (-K -), never argv. Returns (http_code, body)."""
    argv = ["curl", "-sS", "-m", "25", "-A", "curl/8.7.1", "-K", "-", "-w", "\n%{http_code}", base + path]
    if body is not None:
        argv[1:1] = ["-X", "POST", "-H", "Content-Type: application/json", "-d", json.dumps(body)]
    try:
        r = subprocess.run(argv, input='header = "Authorization: Bearer %s"\n' % key, capture_output=True,
                           text=True, errors="replace", timeout=40)
    except (OSError, subprocess.SubprocessError) as e:
        return "000", str(e)
    out, _, code = r.stdout.rpartition("\n")
    return (code.strip() or "000") if r.returncode == 0 else "000", out

def resolve():
    if override:
        if UUID.fullmatch(override):
            print("LOG page target: NVS_PAGE_CSID OVERRIDE in force (tests/emergency only)")
            return override, "override", []
        return None, None, ["NVS_PAGE_CSID override is not a full session uuid (len=%d)" % len(override)]
    why, cands, root = [], [], os.path.realpath(runtime)
    # (1) the live arranger role marker (writer pb-role-marker.py; its FILENAME is authoritative). The csid is
    # validated BEFORE any path use; the path must stay inside the runtime dir; content naming another csid is a
    # tamper signal; content only orders (claimed_at, else mtime). Released markers sit in backups/, never here.
    try:
        names = sorted(os.listdir(runtime))
    except OSError:
        names = []
    for n in names:
        if not n.startswith(PREFIX):
            continue
        c = n[len(PREFIX):]
        if not UUID.fullmatch(c):
            print("LOG role marker REJECTED: filename csid is not a full uuid (len=%d)" % len(c))
            continue
        p = os.path.realpath(os.path.join(root, n))
        if os.path.dirname(p) != root or not os.path.isfile(p):
            print("LOG role marker REJECTED: %s escapes %s or is not a regular file" % (n, root))
            continue
        try:
            d = json.loads(open(p, encoding="utf-8").readline())
        except (OSError, ValueError):
            d = None
        if not isinstance(d, dict):
            print("LOG role marker %s: content is not one JSON object; ordering it by mtime" % n)
            d = {}
        if d.get("csid", c) != c or d.get("role", "arranger") != "arranger":
            print("LOG role marker REJECTED: %s content names another csid/role than its filename" % n)
            continue
        cands.append((str(d.get("claimed_at") or ""), os.path.getmtime(p), c))
    if cands:
        cands.sort()
        if len(cands) > 1:
            print("LOG LOUD: %d live arranger role markers in %s (%s); paging the newest claimed_at: %s"
                  % (len(cands), runtime, ", ".join(x[2] for x in cands), cands[-1][2]))
        return cands[-1][2], "role-marker", []
    why.append("no valid arranger role marker in %s" % runtime)
    # (2) the live roster row carrying the HOST's own arranger name (never a constant).
    if not rname:
        why.append("no roster name for host %s (set NVS_ROSTER_NAME)" % host)
        return None, None, why
    if not key:
        why.append("no Nexus key (resolver exec form unavailable)")
        return None, None, why
    code, out = curl("/api/conductor/sessions/list?status=live")
    try:
        d = json.loads(out) if code.startswith("2") else None
    except ValueError:
        d = None
    if d is None:
        why.append("roster unreadable (http %s)" % code)
        return None, None, why
    rows = d if isinstance(d, list) else (d.get("sessions") or d.get("items") or d.get("data") or [])
    live = [r for r in rows if isinstance(r, dict) and r.get("display_name") == rname and not r.get("ended_at")
            and UUID.fullmatch(str(r.get("session_id") or ""))]
    if not live:
        why.append("no live roster row named %s" % rname)
        return None, None, why
    live.sort(key=lambda r: str(r.get("started_at") or ""))
    if len(live) > 1:
        print("LOG LOUD: %d live roster rows named %s; paging the newest started_at" % (len(live), rname))
    return live[-1]["session_id"], "roster:" + rname, []

csid, src, why = resolve()
if not csid:
    print("RESULT UNRESOLVED - - " + "; ".join(why))
    sys.exit(0)
print("LOG page target: %s (source: %s)" % (csid, src))
if not key:
    print("RESULT UNDELIVERABLE %s %s no Nexus key (resolver exec form unavailable)" % (csid, src))
    sys.exit(0)
# P0-tier page, the fleet's pager shape (pb-prompt-watch.py:293-294, pb-p0-watch.py:1525-1526): kind=page,
# body.tier "decision" (a routing value: block|decision|info), a "P0" reason, the message.
state = "RECOVERED" if kind == "recovered" else "INACTIVE"
body = {"kind": "page", "session_id": csid,
        "body": {"tier": "decision", "reason": ("P0 vault-sync unit %s on %s: %s" % (unit, host, state))[:200],
                 "message": msg}}
code, out = curl("/api/conductor/coord/emit", body)
if code.startswith("2"):
    try:
        r = json.loads(out)
        ev = r.get("event_id") or r.get("id") or "ok"
    except ValueError:
        ev = "ok"
    print("RESULT SENT %s %s event=%s http=%s" % (csid, src, ev, code))
else:
    print("RESULT UNDELIVERABLE %s %s http=%s" % (csid, src, code))
PY
)
  "${helper[@]}" python3 -c "$py" "$PAGE_OVERRIDE" "$ROLE_RUNTIME" "$ROSTER_NAME" "$NEXUS_BASE" "$HOST_SHORT" "$1" "$2" "$3" 2>&1
}
unit_alert(){  # $1 unit  $2 raised|recovered  $3 one-line detail  -> sets PAGE_OK=1 when the page was delivered
  local u="$1" kind="$2" detail="$3" ev out res st csid
  PAGE_OK=0
  [ "$kind" = recovered ] && ev=unit.recovered || ev=unit.stopped
  local ikey="nexus-vault-sync.unit-inactive:${HOST_SHORT}:${u}"
  local msg="[nexus-vault-sync-supervisor@${HOST_SHORT}] ${u}: ${detail}"
  # (1) the P0-tier page to the host arranger, target resolved now.
  out=$(page_arranger "$u" "$kind" "$msg")
  while IFS= read -r line; do case "$line" in "LOG "*) log "${line#LOG }" ;; esac; done <<< "$out"
  res=$(printf '%s\n' "$out" | grep '^RESULT ' | tail -1); st=$(printf '%s' "$res" | cut -d' ' -f2)
  csid=$(printf '%s' "$res" | cut -d' ' -f3)
  case "$st" in
    SENT) PAGE_OK=1; log "PAGE-OK kind=$kind unit=$u ${res#RESULT SENT }" ;;
    *) TICK_RC=3
       [ -n "$res" ] || res="RESULT UNDELIVERABLE - - pager produced no result: $(printf '%s' "$out" | tail -1)"
       log "PAGE FAILED (LOUD): ${res#RESULT } — kind=$kind unit=$u; escalating to the operator; this tick exits 3"
       msg="${msg}
page to the host arranger FAILED (${st:-UNDELIVERABLE}): $(printf '%s' "$res" | cut -d' ' -f5-)" ;;
  esac
  # (2) lnotify NOTICE: the durable record of every alert (vault-note route + ledger; never the nexus_nudge surface).
  if [ -x "$LNOTIFY" ] && printf '%s' "$msg" | "$LNOTIFY" legacy --principal "${HOST_SHORT}.nexus-vault-sync-supervisor@lattice" \
       --app nexus-vault-sync --venture lattice --event-type "$ev" --claim notice --identity-key "$ikey" \
       --subject "$u" --sender nexus-vault-sync-supervisor --caused-by "$u" >/dev/null 2>>"$ST/lnotify.err"; then
    log "ALERT-NOTIFY-OK kind=$kind unit=$u ikey=$ikey"
  else
    log "ALERT-NOTIFY-FAILED kind=$kind unit=$u ikey=$ikey (lnotify=$LNOTIFY; see $ST/lnotify.err)"
  fi
  # (3) operator_escalate when the arranger could not be reached (O52 condition a).
  if [ "$PAGE_OK" = 0 ]; then
    escalate_op "$u" "[nexus-vault-sync] $u on $HOST_SHORT: ${kind^^}, arranger page FAILED" "$msg"
    [ "$kind" = raised ] && echo "$NOW" > "$ST/unit.$u.escalated"
  fi
}
check_inactive_units(){
  local u load act f_since f_alert f_paged f_esc since dur last
  for u in $WATCH_UNITS; do
    f_since="$ST/unit.$u.inactive_since"; f_alert="$ST/unit.$u.last_alert"
    f_paged="$ST/unit.$u.paged_at"; f_esc="$ST/unit.$u.escalated"
    load=$(systemctl --user show -p LoadState --value "$u" 2>/dev/null)
    case "$load" in
      not-found) rm -f "$f_since" "$f_alert" "$f_paged" "$f_esc"; continue ;;   # not on this host: skipped
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
            unit_alert "$u" recovered "RECOVERED: active again after ${dur}s not-active (since $(date -u -d "@$since" +%FT%TZ))"
          else
            log "unit $u active again after ${dur}s (< ${INACTIVE_SECS}s, never alerted)"
          fi
          rm -f "$f_since" "$f_alert" "$f_paged" "$f_esc"
        fi ;;
      *)
        [ -f "$f_since" ] || { echo "$NOW" > "$f_since"; log "unit $u not active ($act, LoadState=$load) — inactive window opened"; }
        since=$(cat "$f_since"); dur=$((NOW-since))
        [ "$dur" -ge "$INACTIVE_SECS" ] || continue
        # O52 condition (b): still inactive ESCALATE_SECS after a DELIVERED page -> operator_escalate, once per episode.
        if [ -f "$f_paged" ] && [ ! -f "$f_esc" ] && [ $((NOW-$(cat "$f_paged"))) -ge "$ESCALATE_SECS" ]; then
          echo "$NOW" > "$f_esc"
          escalate_op "$u" "[nexus-vault-sync] $u on $HOST_SHORT: STILL INACTIVE ${dur}s after a delivered arranger page" \
            "[nexus-vault-sync-supervisor@${HOST_SHORT}] ${u} NOT ACTIVE ($act) for ${dur}s since $(date -u -d "@$since" +%FT%TZ); the host arranger was paged $((NOW-$(cat "$f_paged")))s ago and the unit is still down."
        fi
        last=$(cat "$f_alert" 2>/dev/null || echo 0)
        [ $((NOW-last)) -ge "$REALERT_SECS" ] || continue
        echo "$NOW" > "$f_alert"
        log "unit $u INACTIVE ${dur}s (>= ${INACTIVE_SECS}s; state=$act) — alerting"
        unit_alert "$u" raised "NOT ACTIVE ($act) for ${dur}s since $(date -u -d "@$since" +%FT%TZ) (threshold ${INACTIVE_SECS}s). Alert only: nothing was restarted. Re-alert at most every ${REALERT_SECS}s."
        [ "$PAGE_OK" = 1 ] && [ ! -f "$f_paged" ] && echo "$NOW" > "$f_paged" ;;
    esac
  done
}
check_inactive_units || log "inactive-unit check errored — continuing to the not_receiving check"
# Fail loud: an unresolved/undeliverable page makes THIS tick exit 3, but only after the not_receiving check below
# has run, whatever exit it takes.
[ "$TICK_RC" = 0 ] || trap 'exit 3' EXIT
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
