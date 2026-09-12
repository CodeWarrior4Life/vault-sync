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
LAGT=${OBS_LAGT:-5000}       # single-peer lag (LSN) worth surfacing; see calibration in health()      # age_s at/above which a subscriber is NOT receiving

logf()  { f=$(ls -t "$LOGDIR"/daemon.log.* 2>/dev/null | grep -v '\.gz$' | head -1)
          [ -n "$f" ] && echo "$f" || echo "$LOGDIR/daemon.log.$(date '+%Y-%m-%d')"; }
count()    { find "$V" -name "*$PAT*" -newermt "$(date '+%Y-%m-%d') 00:00:00" -not -path '*/_archive/*' 2>/dev/null | wc -l | tr -d ' '; }
count_all(){ find "$V" -name "*$PAT*" -newermt "$(date '+%Y-%m-%d') 00:00:00" 2>/dev/null | wc -l | tr -d ' '; }
pidof_() { ps -axo pid=,args= | awk -v t="$T" 'index($0,t)>0 && index($0,t)==index($0,$2) {print $1; exit}'; }

# R1 CORE. Emits: verified unverified mtime_total log_events newest_ev_epoch
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
print("%d %d %d %d %s"%(ver,len(cand)-ver,len(cand),len([e for e in ev if e>=cut]),
      (datetime.datetime.utcfromtimestamp(max(ev)).strftime('%H:%M:%SZ') if ev else '-')))
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
cur_max_delta=0; cur_last_emit=0; fb_last_emit=0; dv_last_emit=0; prev_unver=-1; orc_last_emit=0; st_last_emit=0; lag_last_emit=0
set -- $(window_state "$L"); ver=$1; unver=$2; mtot=$3; lev=$4
arm_stalled_now=$arm_stalled
echo "WATCH-v10 armed $(date -u '+%H:%M:%SZ') stash=$prev_n cursor=$prev_cur pid=$prev_pid overflows=$prev_ovf | win${WIN}s verified=$ver unverified=$unver mtime_total=$mtot log_events=$lev | oracle=$prev_orc | subs=$nsubs off_max=$prev_off lag_max=$prev_lag stalled=$arm_stalled | UNITS: age_s=last_event_age_s (seconds since that subscriber last RECEIVED an event); verified=mtime candidate correlated to a real log conflict event within ${TOL}s; unverified=mtime moved with NO nearby event (sync refresh) | LATENCY: stash/window/failure-burst/pid/oracle ${POLL}s, lsn+overflows $((POLL*5))s | trigger2 fires on VERIFIED>=${T2} only"

while :; do
  sleep "$POLL"; i=$((i+1))
  L=$(logf)
  n=$(count); cur=$(cat "$C" 2>/dev/null); pid=$(pidof_)
  set -- $(window_state "$L"); ver=$1; unver=$2; mtot=$3; lev=$4

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
    if [ -n "$o" ] && [ "$o" != "?" ] && [ "$o" != "$prev_ovf" ]; then
      echo "*** TRIGGER 4: OVERFLOWS MOVED *** $(date -u '+%H:%M:%SZ') $prev_ovf -> $o — PAGE"; prev_ovf=$o
    fi
    # COND 1: the LOCKSTEP MAJORITY split. One trailing peer is tolerated (today's
    # measured healthy shape is 3-at-max + 1 laggard) and surfaces as LAG below.
    if [ -n "$off" ] && [ "$off" != "?" ] && [ "$off" -ge 2 ] 2>/dev/null; then
      echo "*** COND 1: LOCKSTEP MAJORITY SPLIT *** $(date -u '+%H:%M:%SZ') off_max=$off of $nrecv receiving peers are NOT at the max LSN (lag_max=$lag subs=$nsubs stalled=$stalled) — PAGE THE OPERATOR DIRECTLY"
    fi
    # LAG is a separate, dampened condition: a single peer trailing. Threshold
    # ${LAGT} is ~16x the measured healthy ceiling (315 on 2026-09-12), chosen to
    # sit far above jitter and far below a real stall's accumulation.
    if [ -n "$lag" ] && [ "$lag" != "?" ] && [ "$lag" -ge "$LAGT" ] 2>/dev/null; then
      now_s=$(date +%s)
      if [ $((now_s - lag_last_emit)) -ge 1800 ]; then
        echo "SUBSCRIBER LAG $(date -u '+%H:%M:%SZ') lag_max=$lag LSN behind the lockstep max (threshold ${LAGT}, measured healthy ceiling ~315 on 2026-09-12); off_max=$off of $nrecv — NOT COND 1 unless off_max>=2"
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
      if [ "$stalled" != "$prev_stalled" ]; then swhy="TRANSITION from stalled=$prev_stalled"
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
    echo "HOURLY ${hh}:00Z | live=$n | incl_archive=$(count_all) | last_hour=$hr | verified_win=$ver | unverified_win=$unver | log_events_win=$lev | materializer=$mz | push=$pushn | pid=[$pid] | overflows=$prev_ovf | oracle=$prev_orc | off_max=$prev_off lag_max=$prev_lag | stalled=$prev_stalled/$nsubs | cursor=$cur | newest_mtime=[${nu:--}] (newest_mtime is an MTIME, not an event time)"
    last_hour_reported="$hh"
  fi
done
