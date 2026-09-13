#!/bin/sh
# nexus-obs counter watch v10 (TKT-baf8f447; supersedes v9).
#
# v10 exists because v9's triggers were keyed to things it could not actually
# observe. Three requirements, ordered by the arranger (outcomes, not mechanism):
#
#  R1. TRIGGER 2 CANNOT FIRE WITHOUT A REAL NEW CONFLICT EVENT.
#      v9 win10() counted FILE MTIMES. Conflict copies are themselves .md files
#      in the vault, so nexus-sync materialises them and REFRESHES their mtime —
#      an old stash re-enters the 10-min window with no new conflict. MEASURED
#      2026-09-12: the Nexus/Memory/_next_session_prompt.nexus stash had its
#      conflict at 18:15:38Z and an mtime of 18:21:28Z (+5m50s drift, no event),
#      while push-side stashes sat ~2s BEFORE their log lines (undrifted control).
#      Refresh six mtimes in one window and v9 pages a storm that never happened.
#      SAME CLASS as the 04:30Z archive trap, except that was a ONE-OFF bulk move
#      and this is CONTINUOUS.
#      NOTE THE FIX IS *NOT* "count log events instead". A push
#      `ConflictUnrecoverable` does NOT always produce a stash (MEASURED: the
#      20:14-20:16Z burst was 3 failures with ZERO stashes), so an event count
#      OVERCOUNTS stashes — swapping one wrong proxy for another. Instead each
#      mtime candidate is VERIFIED against a real log conflict event near its
#      mtime. Unverified candidates are reported separately, never silently.
#
#  R2. EVERY BREAK CONDITION MUST BE A PREDICATE THE INSTRUMENT OBSERVES, and
#      FAILURE-ONLY BURSTS must be visible BETWEEN hourlies. v9 alerted only on
#      new stashes, so the retry path could absorb arbitrarily many failures
#      while the stash counter stayed flat and nothing surfaced. A break
#      condition declared on the FAILURE rate was therefore undetectable — the
#      promise outran the instrument (20:05Z condition fired at 20:16:22Z unseen).
#
#  R3. THE ORACLE AND SUBSCRIBER LSNs ARE WATCHED. v9 never read either: its only
#      `still_divergent` was a COMMENT, and ovf() parsed ONLY fanout_overflows.
#      So COND 1 — the P0 rung — was hourly-sampled while being described as
#      continuous, and COND 4 (loss) had no predicate at all. The ladder inherited
#      PHANTOM COVERAGE on its two most severe rungs, and the two weakest
#      conditions were the two guarding IRREVERSIBLE outcomes.
#
#  LABEL RULE (earned the hard way): the 20:00Z and 21:00Z hourlies reported
#  "FROZEN 4TH DAEMON" beside age=5s and age=9s because the LABEL was a string
#  constant while only the VALUES were computed. EVERY LABEL HERE IS DERIVED FROM
#  THE SAME DATA AS ITS VALUES, or omitted. UNITS ARE PRINTED.
#
# DECLARED LATENCIES (say them, do not imply continuity):
#   stash count / verified window / failure burst / pid / oracle : 60s
#   subscriber LSN divergence + fanout_overflows                 : 300s (remote)
# DEPLOY: stop-then-start, NEVER edit in place — /bin/sh re-reads a running
# script by byte offset.

V=${OBS_VAULT:-/Users/cyril/Vaults/Mainframe}
C=${OBS_CURSOR:-"/Users/cyril/Library/Application Support/Nexus/.lattice-runtime/f2383e35-2e9d-4da2-b5ed-de8a35778fa3/sync-state/last_event_id"}
T=${OBS_DAEMON:-'/Applications/Nexus Vault Sync.app/Contents/MacOS/vault-sync-daemon'}
LOGDIR=${OBS_LOGDIR:-"/Users/cyril/Library/Application Support/Nexus/logs"}
HEALTH=${OBS_HEALTH:-https://nexus.obsidian-inc.com/api/sync/health}
PAT=${OBS_PAT:-conflict-from-f2383e35}
WIN=${OBS_WIN:-600}          # seconds, the trigger-2 window
TOL=${OBS_TOL:-180}          # seconds, mtime<->event correlation tolerance
T2=${OBS_T2:-6}              # stashes in WIN to trip trigger 2
FB=${OBS_FB:-3}              # log events in WIN to surface a failure burst
POLL=${OBS_POLL:-60}
STALL=${OBS_STALL:-300}      # age_s at/above which a subscriber is NOT receiving
STALE_SERVED=${OBS_STALE_SERVED:-600}   # s; sse_served_age_s above this = not being served
FROZEN_SPAN=${OBS_FROZEN_SPAN:-300}     # s; min span for a stationary-cursor verdict
SNAP=/tmp/.obs_snap.$$                  # per-subscriber lsn memory across samples
COND1_FLOOR=${OBS_COND1_FLOOR:-580}   # MEASURED 2026-09-13 08:26:41-08:57:26Z, 31 min, both 0.4.38
                                     # harness-memory instances: skew 0-58 rows, p99=58, so 10x p99
                                     # = 580. FINAL, superseding the interim 500 taken from n=10
                                     # (p99=16) -- that thin sample under-read the skew by ~3.6x.
                                     # NOTE the two instances track each other exactly, so the 124
                                     # rows are 62 PAIRED observations, not 124 independent ones.  # LSN gap below which a majority split is treated as
                                     # catch-up jitter, not divergence. 500 sits above the
                                     # measured healthy ceiling (~315, 2026-09-12) and far
                                     # below LAGT. Calibrated against a REAL false positive:
                                     # at 08:23:05Z COND 1 fired with lag_max=59361, and 31 s
                                     # later both off-max peers were behind by THREE events.
HURL=${OBS_HURL:-https://nexus.obsidian-inc.com/api/sync/health}
LAGT=${OBS_LAGT:-5000}       # single-peer lag (LSN) worth surfacing; see calibration in health()      # age_s at/above which a subscriber is NOT receiving

logf()  { f=$(ls -t "$LOGDIR"/daemon.log.* 2>/dev/null | grep -v '\.gz$' | head -1)
          [ -n "$f" ] && echo "$f" || echo "$LOGDIR/daemon.log.$(date '+%Y-%m-%d')"; }
count()    { find "$V" -name "*$PAT*" -newermt "$(date '+%Y-%m-%d') 00:00:00" -not -path '*/_archive/*' 2>/dev/null | wc -l | tr -d ' '; }
count_all(){ find "$V" -name "*$PAT*" -newermt "$(date '+%Y-%m-%d') 00:00:00" 2>/dev/null | wc -l | tr -d ' '; }
pidof_() { ps -axo pid=,args= | awk -v t="$T" 'index($0,t)>0 && index($0,t)==index($0,$2) {print $1; exit}'; }

# ARRANGER-RUNG DETECTOR (ruling 2026-09-13). Emits: headlsn|stale_list|frozen_list
# Keeps a per-subscriber lsn snapshot file so a STATIONARY cursor can be judged
# across a real span rather than from one sample. Uses sse_served_age_s and
# sse_served_lsn ONLY -- last_event_age_s and receiving_actual are deliberately
# never consulted, because both read healthy throughout a 7h58m wedge.
served_state() {
  # Program passed BY PATH so stdin stays free for the piped payload. The first
  # cut used `python3 - ARGS <<'PYX'` and the heredoc consumed stdin, discarding
  # the curl output and returning "?" for every input -- trap #1 of this lane's
  # brief, reproduced by the seat that had been briefed on it.
  curl -sS -m 12 -A 'curl/8.7.1' "$HURL" 2>/dev/null \
    | python3 /Users/cyril/.claude/scripts/obs_served_state.py "$SNAP" "$STALE_SERVED" "$FROZEN_SPAN"
}

# R4a CORRECTED 2026-09-13 after the detector's FIRST live fire was a FALSE POSITIVE.
# The old predicate counted CONFLICT EVENTS with no nearby copy. That conflated a
# PUSH REJECTION with a conflict REQUIRING PRESERVATION. Measured: of three
# ConflictUnrecoverable events, one was "enqueued CREATE (content preserved)",
# one "409 refetch/merge ... outcome=Wrote" (a successful MERGE), and only one
# actually stashed. So ~2/3 of push failures legitimately produce no copy -- the
# benign shape the predecessor already characterised -- and the old predicate
# would have paged CANDIDATE LOSS on most of them. A loss detector that fires on
# the healthy path is a push-failure detector wearing a loss label.
#
# THE DAEMON NAMES THE STASH IT WROTE: "stashed losing local bytes before ack
# (S511 D4) path=<doc> stash=<abs path>". So the ladder's exact wording -- "a
# stash without its conflict copy" -- is a DIRECT EXISTENCE CHECK on a path the
# daemon itself claims to have written. No correlation window, no tolerance
# heuristic, and it CANNOT fire on enqueued-CREATE or merge-Wrote outcomes.
STASH_GRACE=${OBS_STASH_GRACE:-30}   # seconds; only guards a write/fsync race,
                                     # since the stash line is emitted after the write
orphan_stashes() {
  python3 - "$1" "$WIN" "$STASH_GRACE" <<'PYS'
import sys, os, re, time, calendar, datetime
log, win, grace = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
now = time.time(); out = []
try:
    for ln in open(log, errors="replace"):
        if "stashed losing local bytes" not in ln or "stash=" not in ln:
            continue
        m = re.search(r"stash=(.*?)\s*$", ln)
        if not m:
            continue
        sp = m.group(1)
        try:
            ep = calendar.timegm(datetime.datetime.strptime(ln[:19], "%Y-%m-%dT%H:%M:%S").timetuple())
        except ValueError:
            continue
        age = now - ep
        if age > win or age < grace:
            continue
        if not os.path.exists(sp):
            out.append("%s|%s" % (ln[:19], sp))
except OSError:
    pass
print("\n".join(out))
PYS
}

# R4 (COND 4). Snapshot of path->size for every preserved conflict copy plus every
# shared/partitioned recorder. Depth-bounded on the recorder leg (the brief's
# observer-effect warning: never a vault-wide unbounded find in a contention probe).
sizes() {
  { find "$V" -name "*$PAT*" -newermt "$(date '+%Y-%m-%d') 00:00:00" -not -path '*/_archive/*' -print0 2>/dev/null
    find "$V/02_Projects" -maxdepth 3 -name 'active-work*.md' -print0 2>/dev/null
  } | xargs -0 stat -f '%z %i %N' 2>/dev/null | sort -k3
}

# R1 CORE. Emits: verified unverified mtime_total log_events newest_ev orphan_ev ev_gradeable
# verified   = mtime candidates in WIN that correlate to a real log event (±TOL)
# unverified = mtime candidates in WIN with NO nearby event  => sync-refreshed
# TRIGGER 2 USES `verified` ONLY. `unverified` is printed, never dropped.
window_state() {
  L="$1"
  find "$V" -name "*$PAT*" -not -path '*/_archive/*' -print0 2>/dev/null \
    | xargs -0 stat -f '%m' 2>/dev/null > /tmp/.obs_mt.$$ || :
  grep -hE 'ConflictUnrecoverable|materializer CONFLICT' "$L" 2>/dev/null \
    | awk '{print substr($1,1,19)}' > /tmp/.obs_ev.$$ || :
  python3 - "$WIN" "$TOL" /tmp/.obs_mt.$$ /tmp/.obs_ev.$$ <<'PY'
import sys,time,calendar,datetime
win,tol,mtf,evf=int(sys.argv[1]),int(sys.argv[2]),sys.argv[3],sys.argv[4]
now=time.time(); cut=now-win
mt=[]
try:
    for l in open(mtf):
        l=l.strip()
        if l:
            try: mt.append(float(l))
            except ValueError: pass
except OSError: pass
ev=[]
try:
    for l in open(evf):
        l=l.strip()
        if not l: continue
        try: ev.append(calendar.timegm(datetime.datetime.strptime(l,'%Y-%m-%dT%H:%M:%S').timetuple()))
        except ValueError: pass
except OSError: pass
cand=[m for m in mt if m>=cut]
evw=[e for e in ev if e>=cut-tol]
ver=sum(1 for m in cand if any(abs(m-e)<=tol for e in evw))
# R4 (COND 4). v10 correlated files->events and never the INVERSE. An event with
# NO file is the LOSS direction: the daemon logged a conflict and no preserving
# copy appeared. Computed over a window inset by TOL on the RECENT edge so a copy
# still being written is not miscounted as missing.
evw_in=[e for e in ev if e>=cut and e<=now-tol]
orph=sum(1 for e in evw_in if not any(abs(m-e)<=tol for m in mt))
print("%d %d %d %d %s %d %d"%(ver,len(cand)-ver,len(cand),len([e for e in ev if e>=cut]),
      (datetime.datetime.utcfromtimestamp(max(ev)).strftime('%H:%M:%SZ') if ev else '-'),
      orph,len(evw_in)))
PY
  rm -f /tmp/.obs_mt.$$ /tmp/.obs_ev.$$
}

# R3a. Oracle from the newest completed reconcile pass. LOCAL log read => 60s.
# Label DERIVED: GREEN iff still_divergent==0 AND cycle=="green".
oracle() {
  grep 'reconciliation: pass complete' "$1" 2>/dev/null | tail -1 \
    | awk '{d="?";c="?";for(i=1;i<=NF;i++){if($i~/^still_divergent=/){split($i,a,"=");d=a[2]}
            if($i~/^cycle=/){split($i,b,"=");c=b[2];gsub(/"/,"",c)}}
            print (d=="0" && c=="green" ? "GREEN" : "NOT-GREEN") " still_divergent=" d " cycle=" c}'
}

# R3b + trigger 4. ONE remote call serves overflows AND LSN divergence.
# Label DERIVED from age_s vs STALL; never a hardcoded state word.
health() {
  # DATA VIA FILE, NOT STDIN. `curl | python3 - <<EOF` is BROKEN: the heredoc IS
  # stdin, so python reads its PROGRAM from it and the piped payload is silently
  # DISCARDED — the probe then returns "?" forever and the condition can never
  # fire. I shipped exactly that and only caught it by forcing the positive
  # verdict (prove-both-verdicts-reachable). v9's ovf() was correct because it
  # used `python3 -c` with the program as an ARGUMENT, leaving stdin as the pipe.
  hf=/tmp/.obs_health.$$
  curl -s -m 10 -A 'curl/8.7.1' "$HEALTH" -o "$hf" 2>/dev/null
  python3 - "$STALL" "$hf" <<'PY'
import sys,json
stall=int(sys.argv[1])
try: d=json.load(open(sys.argv[2]))
except Exception: print("? ? ? ?"); raise SystemExit
subs=[]
def w(o):
    if isinstance(o,dict):
        l=o.get('sse_served_lsn') or o.get('served_lsn')
        if l is not None:
            subs.append((str(o.get('host') or '?'),int(l),o.get('last_event_age_s')))
        for v in o.values(): w(v)
    elif isinstance(o,list):
        for v in o: w(v)
w(d)
ovf=d.get('fanout_overflows','?')
if not subs: print("%s ? ? ?"%ovf); raise SystemExit
recv=[s for s in subs if (s[2] is not None and s[2]<stall)]
stalled=len(subs)-len(recv)
# CALIBRATED 2026-09-12 21:27-21:29Z, 14 samples of the REAL endpoint on a healthy
# floor: spread across ALL receiving peers is 271-315 and slowly GROWING, so a
# `spread>0` test is effectively CONSTANT-TRUE on live data — it would have paged
# the P0 rung every 5 minutes forever. Per-host measurement showed WHY: THREE peers
# sit in EXACT lockstep at the max (-0) and ONE known laggard trails. That matches
# the arranger's COND 1 wording ("the three lockstep subscriber LSNs diverge from
# each other"), so the metric is the MAJORITY splitting, not the raw spread.
#   off_max  = receiving peers NOT at the max LSN
#   COND 1 fires at off_max >= 2  (one trailing peer is TOLERATED and reported
#            separately as lag; two or more means the lockstep group itself split)
#   lag_max  = largest single-peer lag, reported ALWAYS, thresholded separately
lsns=[s[1] for s in recv]
mx=max(lsns) if lsns else 0
at_max=sum(1 for l in lsns if l==mx)
off_max=len(recv)-at_max
lag_max=(mx-min(lsns)) if lsns else 0
print("%s %d %d %d %d %d"%(ovf,off_max,stalled,len(subs),lag_max,len(recv)))
PY
  rm -f "$hf"
}

L=$(logf)
prev_n=$(count); prev_cur=$(cat "$C" 2>/dev/null); prev_pid=$(pidof_)
set -- $(health); prev_ovf=$1; prev_off=$2; nsubs=$4; prev_lag=$5; nrecv=$6
# sentinel, NOT $3: seeding prev_stalled from the arm-time reading is what makes an
# already-stalled peer invisible forever. -1 guarantees the first poll reports the level.
prev_stalled=-1; arm_stalled=$3
prev_mz=$(grep -c 'materializer CONFLICT' "$L" 2>/dev/null)
prev_push=$(grep -c 'ConflictUnrecoverable' "$L" 2>/dev/null)
prev_orc=$(oracle "$L")
i=0
last_hour_reported=$(date -u '+%H')
c1_streak=0; stale_last_emit=0; frozen_last_emit=0; cur_max_delta=0; cur_last_emit=0; fb_last_emit=0; dv_last_emit=0; prev_unver=-1; orc_last_emit=0; st_last_emit=0; lag_last_emit=0
# R4: LEVEL not EDGE for the size baseline too — seeded from a real read, and the
# first diff happens on tick 1, so a shrink already in progress at arm time is
# still caught on the next tick rather than being absorbed into the baseline.
SZPREV=/tmp/.obs_sz_prev.$$; SZCUR=/tmp/.obs_sz_cur.$$
sizes > "$SZPREV" 2>/dev/null || : ; loss_last_emit=0; orph_last_emit=0
trap 'rm -f "$SZPREV" "$SZCUR" "$SNAP"' EXIT INT TERM
set -- $(window_state "$L"); ver=$1; unver=$2; mtot=$3; lev=$4; orph=$6; evg=$7
arm_stalled_now=$arm_stalled
echo "WATCH-v11 armed $(date -u '+%H:%M:%SZ') stash=$prev_n cursor=$prev_cur pid=$prev_pid overflows=$prev_ovf | win${WIN}s verified=$ver unverified=$unver mtime_total=$mtot log_events=$lev | oracle=$prev_orc | subs=$nsubs off_max=$prev_off lag_max=$prev_lag stalled=$arm_stalled | UNITS: age_s=last_event_age_s (seconds since that subscriber last RECEIVED an event); verified=mtime candidate correlated to a real log conflict event within ${TOL}s; unverified=mtime moved with NO nearby event (sync refresh) | LATENCY: stash/window/failure-burst/pid/oracle ${POLL}s, lsn+overflows $((POLL*5))s | trigger2 fires on VERIFIED>=${T2} only | R4/COND 4 WATCHED (v10 had NO loss predicate): orphan_ev=$orph of $evg gradeable, size-baseline seeded from $(wc -l < "$SZPREV" | tr -d ' ') tracked files; loss = a logged conflict with no copy, a copy absent from EVERY vault path, or any tracked file LOSING bytes. COND 4 FIRES ARE GRADED CANDIDATE (arranger ruling, first week) and route to `pitboss` as an arranger page after oracle + direct-stat confirmation, NEVER to the operator from this lane"

while :; do
  sleep "$POLL"; i=$((i+1))
  L=$(logf)
  n=$(count); cur=$(cat "$C" 2>/dev/null); pid=$(pidof_)
  set -- $(window_state "$L"); ver=$1; unver=$2; mtot=$3; lev=$4; orph=$6; evg=$7

  if [ "$n" -lt "$prev_n" ] 2>/dev/null; then
    echo "DAY BOUNDARY / COUNTER RESET $(date -u '+%H:%M:%SZ') total=$n (was $prev_n, delta=$((n-prev_n))) — LOCAL-MIDNIGHT boundary, the day rolling over, NOT loss. Verify: find \"$V\" -name '*$PAT*' -newermt '<prev-date> 00:00:00' | wc -l"
    prev_n=$n
  elif [ "$n" -gt "$prev_n" ] 2>/dev/null; then
    mz=$(grep -c 'materializer CONFLICT' "$L" 2>/dev/null)
    pushn=$(grep -c 'ConflictUnrecoverable' "$L" 2>/dev/null)
    who=""; extra=""
    if [ "$mz" -gt "$prev_mz" ] 2>/dev/null; then
      who="materializer"
      extra=$(grep 'materializer CONFLICT' "$L" 2>/dev/null | tail -1 | sed 's/.*\(shadow_present=[a-z]*\).*/\1/')
    fi
    if [ "$pushn" -gt "$prev_push" ] 2>/dev/null; then
      [ -n "$who" ] && who="$who+push" || who="push"
      extra="$extra ConflictUnrecoverable"
    fi
    [ -n "$who" ] || who="UNATTRIBUTED(neither producer counter moved — investigate)"
    echo "NEW STASH $(date -u '+%H:%M:%SZ') total=$n delta=$((n-prev_n)) verified_win=$ver unverified_win=$unver producer=$who $extra materializer_log=$mz push_conflicts=$pushn pid=$pid"
    prev_n=$n; prev_mz=$mz; prev_push=$pushn
  fi

  # R1: TRIGGER 2 on VERIFIED stashes only.
  if [ -n "$ver" ] && [ "$ver" -ge "$T2" ] 2>/dev/null; then
    echo "*** TRIGGER 2: >=${T2} VERIFIED STASHES IN ${WIN}s *** $(date -u '+%H:%M:%SZ') verified=$ver unverified=$unver mtime_total=$mtot log_events=$lev total=$n pid=$pid — PAGE (each verified stash correlates to a real log conflict event within ${TOL}s)"
  fi
  # R1: divergence between the two views is EMITTED, never silent.
  # DAMPENED like v6's cursor line: a persistent divergence is a CHARACTERISED
  # condition, so re-emitting it every tick is pure noise. Emit on CHANGE, or
  # once per 30 min as a persistence heartbeat. TRIGGER 2 IS NEVER DAMPENED.
  if [ -n "$unver" ] && [ "$unver" -gt 0 ] 2>/dev/null; then
    now_s=$(date +%s); dwhy=""
    if [ "$unver" != "$prev_unver" ]; then dwhy="CHANGED (was $prev_unver)"
    elif [ $((now_s - dv_last_emit)) -ge 1800 ]; then dwhy="30-min heartbeat; condition persists"
    fi
    if [ -n "$dwhy" ]; then
      echo "WINDOW VIEWS DIVERGE $(date -u '+%H:%M:%SZ') mtime_total=$mtot verified=$ver unverified=$unver — $unver stash file(s) had an mtime inside the ${WIN}s window with NO log conflict event within ${TOL}s. Sync-refreshed conflict copies show exactly this shape; v9 would have counted them toward trigger 2. — $dwhy"
      dv_last_emit=$now_s
    fi
  fi
  prev_unver=$unver

  # R2: FAILURE-ONLY BURST — visible between hourlies. Rate-limited to 1/10min.
  if [ -n "$lev" ] && [ "$lev" -ge "$FB" ] 2>/dev/null; then
    now_s=$(date +%s)
    if [ $((now_s - fb_last_emit)) -ge 600 ]; then
      echo "FAILURE BURST $(date -u '+%H:%M:%SZ') log_events=$lev in ${WIN}s (n=$lev, rate=$(python3 -c "print('%.1f'%($lev*3600.0/$WIN))")/hr) verified_stashes=$ver — failures can be absorbed by retry with the stash counter FLAT, which is why this is reported separately from trigger 2"
      fb_last_emit=$now_s
    fi
  fi

  # =====================================================================
  # R4. COND 4 — LOSS, not churn. v10 had NO predicate for this rung at all
  # (its only loss-adjacent line was the DAY BOUNDARY guard, which exists to say
  # "NOT loss"). The ladder called COND 4 "NOT WATCHED AT ALL"; this watches it.
  # Two independent predicates, because loss has two shapes:
  #   (a) ORPHAN EVENT — the daemon logged a conflict and NO preserving copy
  #       appeared. Displaced content that was never stashed.
  #   (b) VANISHED / SHRUNK — a preserved copy or a recorder LOST BYTES.
  # Every label below is DERIVED from its own numbers (trap 17: never hardcode a
  # state word beside live values).
  # =====================================================================
  # R4a: orphan STASHES (the daemon named a stash file that does not exist).
  orphstash=$(orphan_stashes "$L")
  if [ -n "$orphstash" ]; then
    now_s=$(date +%s)
    if [ $((now_s - orph_last_emit)) -ge 600 ]; then
      echo "$orphstash" | while IFS='|' read -r ots osp; do
        [ -z "$osp" ] && continue
        echo "*** COND 4: STASH NAMED BY THE DAEMON IS ABSENT — CANDIDATE LOSS *** $(date -u '+%H:%M:%SZ') event_ts=${ots}Z stash=[$osp] — the daemon logged \"stashed losing local bytes before ack\" and NAMED this file, and it is not on disk. GRADE: CANDIDATE (arranger ruling, detector's first week) — NOT a confirmed loss. CONFIRM BEFORE ESCALATING, two independent checks: (1) the reconcile oracle's still_divergent, (2) a direct stat of the named stash path (use an ABSOLUTE -newermt; the relative form errors and still exits 0 on this host). THEN PAGE \`pitboss\` AS AN ARRANGER PAGE — NOT the operator from this lane: loss is exactly where a false positive costs trust"
      done
      orph_last_emit=$now_s
    fi
  fi
  # Push failures that resolve via "enqueued CREATE (content preserved)" or
  # "409 refetch/merge ... outcome=Wrote" are the HEALTHY paths and are counted
  # for the hourly only -- never paged. This is the FALSE POSITIVE that the first
  # live COND 4 fire taught: the flagged event was a successful MERGE.
  # `grep -c` PRINTS a count AND EXITS 1 when the count is zero, so a trailing
  # `|| echo 0` appends a SECOND zero: the capture becomes "0\n0", every integer
  # test on it errors out, and the derived COND4 label flips to NOT-GREEN while
  # orphan_ev is genuinely 0. It also splits the hourly line mid-field. Measured
  # live at the 02:00Z hourly. grep -c needs no fallback; guard only an EMPTY
  # capture, and never fall back on a NON-ZERO EXIT that carries a valid value.
  orph=$(printf '%s' "$orphstash" | grep -c . 2>/dev/null); orph=${orph:-0}
  evg=$(grep -c 'stashed losing local bytes' "$L" 2>/dev/null); evg=${evg:-0}

  sizes > "$SZCUR" 2>/dev/null || :
  # A MISSING BASELINE MUST NOT BE A SILENT SKIP. The baseline lives in /tmp, and
  # /tmp aging is a MEASURED fleet condition (link: 2d override via
  # /etc/tmpfiles.d/tmp.conf, broadcast 2026-09-12). If it is swept, the guard
  # below would quietly stop detecting loss forever -- the loss detector itself
  # going dark with no output, which is the phantom-coverage shape this whole
  # instrument exists to avoid. So: re-seed, and SAY that a window was skipped.
  if [ ! -s "$SZPREV" ] && [ -s "$SZCUR" ]; then
    echo "COND 4 BASELINE RESEEDED $(date -u '+%H:%M:%SZ') the size baseline at $SZPREV was missing or empty (likely /tmp aging) — re-seeded from $(wc -l < "$SZCUR" | tr -d ' ') tracked files. ONE comparison window was skipped, so a shrink/vanish in that window is NOT observable; orphan-event detection was unaffected"
    mv -f "$SZCUR" "$SZPREV" 2>/dev/null || :
  elif [ -s "$SZPREV" ] && [ -s "$SZCUR" ]; then
    loss=$(python3 - "$SZPREV" "$SZCUR" <<'PYL'
import sys
def load(f):
    d={}
    try:
        for l in open(f, errors="replace"):
            l=l.rstrip("\n")
            if not l: continue
            parts=l.split(" ",2)
            if len(parts)!=3: continue
            sz,ino,path=parts
            try: d[path]=(int(sz),ino)
            except ValueError: pass
    except OSError: pass
    return d
prev,cur=load(sys.argv[1]),load(sys.argv[2])
out=[]
for path,(ps,pino) in prev.items():
    if path in cur:
        cs=cur[path][0]
        if cs<ps: out.append("SHRANK|%s|%d|%d|%s"%(path,ps,cs,pino))
    else:
        out.append("ABSENT|%s|%d|0|%s"%(path,ps,pino))
print("\n".join(out))
PYL
)
    if [ -n "$loss" ]; then
      # SUBSHELL TRAP: this while loop is fed by a pipe, so it runs in a
      # subshell -- a shell variable incremented inside it reads 0 again after
      # `done`. The relocation tally therefore goes through a FILE, which
      # survives the boundary. (Caught before shipping by asking where the
      # counter lives, not by watching it read zero in production.)
      RELF=/tmp/.obs_reloc.$$; : > "$RELF"
      echo "$loss" | while IFS='|' read -r kind path was now ino; do
        [ -z "$kind" ] && continue
        if [ "$kind" = "ABSENT" ]; then
          # DISCRIMINATOR, and it is the whole point: absent-from-the-find is NOT
          # absent-from-disk. A day-boundary roll or a move into _archive/ both
          # drop a file from the find while the bytes are safe. Only a file that
          # exists NOWHERE is loss. (v6 shipped the day-boundary version of this
          # mistake; the brief's trap-11 scope lesson is the same shape.)
          if [ -e "$path" ]; then
            continue
          fi
          # FAIL-CLOSED ON THE PAGE. Found in fixture testing: when basename/head
          # were unavailable the old form silently produced an empty relocation
          # result and asserted VANISHED — i.e. a TOOLING failure rendered as a
          # P0 loss page. A missing tool must never manufacture a loss claim.
          # basename -> shell builtin expansion; find -print -quit stops at hit 1;
          # and a find that FAILS is graded UNDETERMINED, never LOSS.
          # RE-DERIVED 2026-09-13 after the basename version produced FALSE
          # RELOCATED verdicts at scale during the nightly bulk archive move: it
          # named a DIFFERENT project's file as the destination (a Nexus copy
          # "relocated" into Pitboss's archive), because dozens of conflict
          # copies share one basename -- which is the very defect class this
          # observer exists to study. Worse than cosmetic: ANY absence matched
          # SOME same-named file, so a GENUINE VANISH would have read RELOCATED
          # and SUPPRESSED its own loss page. A false negative on the
          # irreversible rung.
          #
          # INODE IS THE PROOF: a rename/move within a filesystem preserves it.
          # Verdicts are now GRADED and each says what established it:
          #   RELOCATED (inode)  - same inode found elsewhere. Proven same file.
          #   RELOCATED (size)   - inode gone but exactly ONE conflict copy of
          #                        that exact byte size exists. Probable copy+delete.
          #   VANISHED           - neither. Candidate loss.
          #   UNDETERMINED       - a probe failed; never graded as loss.
          bn=${path##*/}
          elsewhere=$(find "$V" -inum "$ino" -print -quit 2>/dev/null); fst=$?
          if [ "$fst" -ne 0 ]; then
            echo "COND 4 UNDETERMINED $(date -u '+%H:%M:%SZ') ${path#$V/} was ${was}B and is absent from its own path, but the relocation probe FAILED (find exit=$fst) — NOT graded as loss. Re-check by hand: find \"$V\" -name '$bn'"
          elif [ -n "$elsewhere" ]; then
            echo "inode|${path#$V/} -> ${elsewhere#$V/}" >> "$RELF"
          else
            # inode gone. Fall back to an EXACT-SIZE search among conflict
            # copies, which is far stronger than a basename but still not proof.
            szmatch=$(find "$V" -name "*$PAT*" -size "${was}c" -print 2>/dev/null | head -2)
            szn=$(printf '%s' "$szmatch" | grep -c . 2>/dev/null); szn=${szn:-0}
            if [ "$szn" = "1" ]; then
              echo "size|${path#$V/} -> $(printf '%s' "$szmatch" | head -1 | sed "s#$V/##")" >> "$RELF"
            else
              echo "*** COND 4: STASH VANISHED — LOSS *** $(date -u '+%H:%M:%SZ') ${path#$V/} was ${was}B, now absent from disk, its INODE is gone, and NO conflict copy of its exact byte size exists — GRADE: CANDIDATE (arranger ruling 2026-09-12) — NOT a confirmed loss. CONFIRM BEFORE ESCALATING: (1) the reconcile oracle's still_divergent, (2) a direct stat of the named path. THEN PAGE `pitboss` AS AN ARRANGER PAGE — NOT the operator from this lane: loss is exactly where a false positive costs trust. Confirm: find \"$V\" -size ${was}c -name '*$PAT*'"
            fi
          fi
        else
          echo "*** COND 4: FILE SHRANK — LOSS *** $(date -u '+%H:%M:%SZ') ${path#$V/} ${was}B -> ${now}B (delta=-$((was-now))B) — a recorder/stash only ever APPENDS, so a byte DECREASE is content displaced, not churn — GRADE: CANDIDATE (arranger ruling 2026-09-12, detector's first week) — NOT a confirmed loss. CONFIRM BEFORE ESCALATING, two independent checks: (1) the reconcile oracle's still_divergent, (2) a direct stat of the named path. THEN PAGE `pitboss` AS AN ARRANGER PAGE — NOT the operator from this lane: loss is exactly where a false positive costs trust"
        fi
      done
      rn=$(grep -c . "$RELF" 2>/dev/null); rn=${rn:-0}
      if [ "$rn" -gt 0 ] 2>/dev/null; then
        ri=$(grep -c '^inode|' "$RELF" 2>/dev/null); ri=${ri:-0}
        rs=$(grep -c '^size|' "$RELF" 2>/dev/null); rs=${rs:-0}
        echo "STASHES RELOCATED $(date -u '+%H:%M:%SZ') n=$rn (inode-proven=$ri size-matched=$rs) — NOT loss, the bytes exist at new paths. Example: $(head -1 "$RELF" | cut -d'|' -f2). Verdict basis is the INODE (a move preserves it), NOT the basename: dozens of conflict copies share one basename, and a basename match reported a DIFFERENT project's file as the destination"
      fi
      rm -f "$RELF"
    fi
    mv -f "$SZCUR" "$SZPREV" 2>/dev/null || :
  fi

  [ "$pid" != "$prev_pid" ] && { echo "*** TRIGGER 3: PRIMARY PID CHANGE *** $(date -u '+%H:%M:%SZ') was=[$prev_pid] now=[$pid] total=$n — PAGE"; prev_pid=$pid; }

  # R3a: ORACLE every tick. COND 1 half that needs no network.
  orc=$(oracle "$L")
  case "$orc" in
    NOT-GREEN*)
      # Dampened DELIBERATELY TIGHTER than the benign conditions (10 min, not 30):
      # a P0 rung must stay loud, but 60 identical pages an hour trains the reader
      # to ignore it, which is how a real one gets missed. Transition + heartbeat.
      now_s=$(date +%s); owhy=""
      if [ "$orc" != "$prev_orc" ]; then owhy="TRANSITION from: $prev_orc"
      elif [ $((now_s - orc_last_emit)) -ge 600 ]; then owhy="10-min heartbeat; condition PERSISTS"
      fi
      if [ -n "$owhy" ]; then
        echo "*** COND 1: ORACLE NOT GREEN *** $(date -u '+%H:%M:%SZ') $orc — PAGE THE OPERATOR DIRECTLY — $owhy"
        orc_last_emit=$now_s
      fi ;;
  esac
  [ -n "$orc" ] && prev_orc="$orc"

  if [ -n "$cur" ] && [ -n "$prev_cur" ] && [ "$cur" -lt "$prev_cur" ] 2>/dev/null; then
    d=$((cur-prev_cur)); ad=${d#-}; now_s=$(date +%s); why=""
    if [ "$ad" -gt "$cur_max_delta" ] 2>/dev/null; then
      why="NEW EXCURSION (|$d| exceeds previous max $cur_max_delta)"; cur_max_delta=$ad
    elif [ $((now_s - cur_last_emit)) -ge 1800 ]; then
      why="30-min heartbeat; condition persists, max excursion so far $cur_max_delta"
    fi
    [ -n "$why" ] && { echo "CURSOR REGRESSED $(date -u '+%H:%M:%SZ') $prev_cur -> $cur (delta=$d) — $why"; cur_last_emit=$now_s; }
  fi
  [ -n "$cur" ] && prev_cur=$cur

  # R3b + trigger 4: one remote call per 5 ticks.
  if [ $((i % 5)) -eq 0 ]; then
    set -- $(health); o=$1; off=$2; stalled=$3; nsubs=$4; lag=$5; nrecv=$6
    ss=$(served_state); headlsn=${ss%%|*}; rest=${ss#*|}; stale_list=${rest%%|*}; frozen_list=${rest#*|}
    if [ -n "$o" ] && [ "$o" != "?" ] && [ "$o" != "$prev_ovf" ]; then
      echo "*** TRIGGER 4: OVERFLOWS MOVED *** $(date -u '+%H:%M:%SZ') $prev_ovf -> $o — PAGE"; prev_ovf=$o
    fi
    # COND 1: the LOCKSTEP MAJORITY split. One trailing peer is tolerated (today's
    # measured healthy shape is 3-at-max + 1 laggard) and surfaces as LAG below.
    # RE-DERIVED 2026-09-13, TWICE. First after a REAL false positive paged the
    # operator rung at 04:23 local on a THREE-EVENT skew; then again because my
    # own first fix was WRONG in the worse direction.
    #
    # WHAT HAPPENED: the original encoding was off_max>=2, correct for the fleet
    # it was measured on (FOUR subscribers, exactly ONE trailing memory instance,
    # so off_max could never reach 2 healthily). A FIFTH subscriber appeared
    # (fdb48e31, link harness-memory, same 0.4.38 / poll_since_lsn=None shape as
    # 3a934c79) and TWO trailing memory instances make off_max=2 the STEADY
    # STATE -- turning the operator rung constant-true via a fleet-shape change
    # rather than a code change.
    #
    # MY FIRST FIX replaced it with "the majority must share the head", matching
    # the previous seat's stated INTENT. It killed the false positive and ALSO
    # went silent on n=5 with two peers genuinely 5000 behind -- a FALSE NEGATIVE
    # on the rung that pages a sleeping operator. Trading a false page for a
    # missed page is the worse trade, so that cut was discarded.
    #
    # WHAT ACTUALLY SHIPS: fire on EITHER a true majority split OR the original
    # off_max>=2 with a magnitude floor, and require the condition to PERSIST
    # across TWO consecutive health reads before paging. Persistence is the right
    # instrument for a transient: the 59361 spike was real AT THAT INSTANT and 31
    # seconds later the gap was 3, so no magnitude threshold could have
    # distinguished it -- only a second look could.
    if [ -n "$off" ] && [ "$off" != "?" ] && [ -n "$nrecv" ] && [ "$nrecv" != "?" ] 2>/dev/null; then
      at_max=$((nrecv - off)); c1why=""
      # ARRANGER RULING 2026-09-13: COND 1 = off_max>=2 AND lag_max above a floor
      # clear of normal memory-instance skew. A majority-split clause was
      # considered and DROPPED: for n>=4 a majority split always implies off>=2,
      # so it only added "majority split with a sub-floor gap" -- jitter, which
      # is exactly what the floor is for. Build-based exclusion was explicitly
      # refused by the ruling: a WEDGED memory instance must stay visible, just
      # not to the operator.
      if [ "$off" -ge 2 ] && [ -n "$lag" ] && [ "$lag" != "?" ] && [ "$lag" -ge "$COND1_FLOOR" ] 2>/dev/null; then
        c1why="off_max=$off of $nrecv with lag_max=$lag at/above the measured floor ${COND1_FLOOR} (at_max=$at_max)"
      fi
      if [ -n "$c1why" ]; then
        c1_streak=$((c1_streak + 1))
        if [ "$c1_streak" -ge 2 ]; then
          echo "*** COND 1: LOCKSTEP DIVERGENCE *** $(date -u '+%H:%M:%SZ') $c1why — CONFIRMED on $c1_streak consecutive reads (lag_max=$lag subs=$nsubs stalled=$stalled) — PAGE THE OPERATOR DIRECTLY"
        else
          echo "COND 1 UNCONFIRMED $(date -u '+%H:%M:%SZ') $c1why on ONE read only (lag_max=$lag) — HELD, not paged, pending a second consecutive read. A catch-up instant leaves every cursor at a different LSN for one sample; that is what this hold exists for"
        fi
      else
        if [ "${c1_streak:-0}" -gt 0 ]; then
          echo "COND 1 CLEARED $(date -u '+%H:%M:%SZ') condition no longer present after $c1_streak read(s) (at_max=$at_max of $nrecv, off_max=$off, lag_max=$lag) — the held condition was TRANSIENT, which is the outcome the hold is designed to find"
        fi
        c1_streak=0
      fi
    fi
    # ===================================================================
    # ARRANGER RUNG (ruling 2026-09-13). Pages the ARRANGER BY NAME, NEVER the
    # operator. Fires on the two signals that tonight's wedge proved actually
    # discriminate, and DELIBERATELY uses neither last_event_age_s nor
    # receiving_actual -- both read healthy for all five subscribers while one
    # had been served nothing for 7 h 58 m, which is how the wedge hid.
    #   (a) sse_served_age_s > STALE_SERVED while the head is advancing
    #   (b) a stationary sse_served_lsn across two samples >= 5 min apart
    # ===================================================================
    # ================= HELD, NOT LIVE (arranger ruling 2026-09-13 04:28) =========
    # Both rungs below are gated OFF. The ruling held the sse_served_age_s rung
    # because it would page on every QUIET ROUTE: a named-route subscriber gets
    # nothing for cross-route events, and catch-up never records sse_served, so
    # served-age grows on an idle route EXACTLY as on a wedge. Measured
    # server-side 08:16-08:19Z: zero rows above 3a934c79's cursor on its own
    # route, and that cursor WAS the route max -- it was idle, not wedged.
    #
    # EXTENDED BEYOND THE RULING, deliberately: the ruling named only the
    # served-age rung, but the STATIONARY-CURSOR rung has the IDENTICAL defect
    # for the identical reason -- an idle route's cursor sits at its route head
    # and is stationary BY DEFINITION. Shipping it would page on every quiet
    # route too. Holding both is the ruling's intent; holding only the named one
    # would have reproduced the same false pager under a different name.
    #
    # The correct rung is per-subscriber "behind its ROUTE head by N rows", which
    # nexus is adding to /api/sync/health. This code stays, unexecuted, until
    # that figure exists.
    ARRANGER_RUNG=${OBS_ARRANGER_RUNG:-0}
    if [ "$ARRANGER_RUNG" = "1" ] && [ -n "$stale_list" ]; then
      now_s=$(date +%s)
      if [ $((now_s - stale_last_emit)) -ge 1800 ]; then
        echo "*** ARRANGER RUNG: SUBSCRIBER NOT BEING SERVED *** $(date -u '+%H:%M:%SZ') $stale_list (head=$headlsn advancing; threshold sse_served_age_s>${STALE_SERVED}s) — PAGE \`pitboss\` BY NAME, NOT the operator. NOTE: last_event_age_s and receiving_actual will look HEALTHY here; they read 0 and 5/5 during a 7h58m wedge on 2026-09-13. Confirm with: sse_served_age_s and a second sse_served_lsn sample >=5 min later"
        stale_last_emit=$now_s
      fi
    fi
    if [ "$ARRANGER_RUNG" = "1" ] && [ -n "$frozen_list" ]; then
      now_s=$(date +%s)
      if [ $((now_s - frozen_last_emit)) -ge 1800 ]; then
        echo "*** ARRANGER RUNG: STATIONARY CURSOR *** $(date -u '+%H:%M:%SZ') $frozen_list — sse_served_lsn UNCHANGED across samples >=${FROZEN_SPAN}s apart while the head advanced. A client restart is MEASURED not to clear this (2026-09-13, pid 12557->58043, cursor unmoved) — PAGE \`pitboss\` BY NAME, NOT the operator"
        frozen_last_emit=$now_s
      fi
    fi
    # LAG is a separate, dampened condition: a single peer trailing. Threshold
    # ${LAGT} is ~16x the measured healthy ceiling (315 on 2026-09-12), chosen to
    # sit far above jitter and far below a real stall's accumulation.
    if [ -n "$lag" ] && [ "$lag" != "?" ] && [ "$lag" -ge "$LAGT" ] 2>/dev/null; then
      now_s=$(date +%s)
      if [ $((now_s - lag_last_emit)) -ge 1800 ]; then
        echo "SUBSCRIBER LAG $(date -u '+%H:%M:%SZ') lag_max=$lag LSN behind the lockstep max (threshold ${LAGT}, measured healthy ceiling ~315 on 2026-09-12); off_max=$off of $nrecv — NOT COND 1 unless off_max>=2. ARRANGER RULING 2026-09-13: crossing this threshold IS a declared break condition — PAGE `pitboss` BY NAME (arranger page, not the operator). Detection latency is ${POLL}s times 5 because lag is read on every 5th tick, and this line is dampened to 1 per 1800s, so a FIRST crossing surfaces within 300s and repeats are half-hourly"
        lag_last_emit=$now_s
      fi
    fi
    [ -n "$lag" ] && [ "$lag" != "?" ] && prev_lag=$lag
    # LEVEL DETECTOR, NOT AN EDGE — this is the TKT-92740334 lesson applied here:
    # THE TRIGGER IS AN EDGE WHILE THE CONDITION IS A LEVEL. A change-only check
    # seeded at arm time can NEVER report a peer that was ALREADY stalled when
    # v10 started, which is precisely the case that matters (today's 6-8h stall
    # would have been invisible to an arm-seeded edge detector). So: emit on
    # TRANSITION, and while stalled>0 persists, heartbeat every 30 min.
    if [ -n "$stalled" ] && [ "$stalled" != "?" ] 2>/dev/null; then
      now_s=$(date +%s); swhy=""
      if [ "$prev_stalled" = "-1" ]; then swhy="BASELINE — first health read after arm; -1 is the seed sentinel, NOT a prior state, so this is NOT a transition"
      elif [ "$stalled" != "$prev_stalled" ]; then swhy="TRANSITION from stalled=$prev_stalled"
      elif [ "$stalled" -gt 0 ] 2>/dev/null && [ $((now_s - st_last_emit)) -ge 1800 ]; then swhy="30-min heartbeat; condition PERSISTS"
      fi
      if [ -n "$swhy" ]; then
        echo "SUBSCRIBER RECEIVE-STATE $(date -u '+%H:%M:%SZ') stalled=$stalled of $nsubs (STALLED iff age_s >= ${STALL}s; label DERIVED from age_s, not asserted) — $swhy"
        st_last_emit=$now_s; prev_stalled=$stalled
      fi
    fi
  fi

  hh=$(date -u '+%H')
  if [ "$hh" != "$last_hour_reported" ]; then
    hr=$(find "$V" -name "*$PAT*" -newermt "$(date -v-1H '+%Y-%m-%d %H:00:00')" -not -path '*/_archive/*' 2>/dev/null | wc -l | tr -d ' ')
    mz=$(grep -c 'materializer CONFLICT' "$L" 2>/dev/null); pushn=$(grep -c 'ConflictUnrecoverable' "$L" 2>/dev/null)
    nu=$(find "$V" -name "*$PAT*" -newermt "$(date -v-1H '+%Y-%m-%d %H:00:00')" -not -path '*/_archive/*' 2>/dev/null -print0 \
          | xargs -0 stat -f '%m %N' 2>/dev/null | sort -n | tail -1 \
          | while read -r ep p; do echo "$(date -u -r "$ep" '+%H:%M:%SZ') ${p#$V/}"; done)
    echo "HOURLY ${hh}:00Z | live=$n | incl_archive=$(count_all) | last_hour=$hr | verified_win=$ver | unverified_win=$unver | log_events_win=$lev | materializer=$mz | push=$pushn | pid=[$pid] | overflows=$prev_ovf | oracle=$prev_orc | off_max=$prev_off lag_max=$prev_lag | stalled=$prev_stalled/$nsubs | cursor=$cur | newest_mtime=[${nu:--}] (newest_mtime is an MTIME, not an event time) | orphan_ev=$orph/$evg gradeable | tracked_files=$(wc -l < "$SZPREV" 2>/dev/null | tr -d ' ') | COND4=$([ "${orph:-0}" -eq 0 ] 2>/dev/null && echo GREEN || echo NOT-GREEN) (COND4 label DERIVED from orphan_ev this window; shrink/vanish emit on transition, so silence here means no byte-loss seen since the last tick)"
    last_hour_reported="$hh"
  fi
done
