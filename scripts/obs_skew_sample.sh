#!/bin/sh
# Sample the skew of the two 0.4.38 harness-memory instances against the head.
# Purpose: pick the COND 1 floor from MEASURED skew rather than a guessed number
# (arranger ruling 2026-09-13: floor from measured p99 skew, minimum a few hundred).
OUT=/Users/cyril/.claude/logs/obs_skew_samples.tsv
[ -s "$OUT" ] || printf 'utc\thead\tsub\tbehind\tserved_age_s\n' > "$OUT"
END=$(( $(date +%s) + 1860 ))    # ~31 min
while [ "$(date +%s)" -lt "$END" ]; do
  curl -sS -m 10 -A 'curl/8.7.1' 'https://nexus.obsidian-inc.com/api/sync/health' 2>/dev/null \
  | python3 -c "
import sys,json,datetime
try: d=json.load(sys.stdin)
except Exception: raise SystemExit
subs=d.get('subscribers') or []
lsns=[s.get('sse_served_lsn') for s in subs if isinstance(s.get('sse_served_lsn'),int)]
if not lsns: raise SystemExit
mx=max(lsns); now=datetime.datetime.now(datetime.timezone.utc).strftime('%H:%M:%S')
for s in subs:
    if s.get('daemon_version')=='0.4.38':
        l=s.get('sse_served_lsn')
        if isinstance(l,int):
            print('%s\t%d\t%s\t%d\t%s' % (now,mx,s['subscriber_id'][:8],mx-l,s.get('sse_served_age_s')))
" >> "$OUT" 2>/dev/null
  sleep 30
done
echo "SKEW SAMPLING COMPLETE $(date -u '+%H:%M:%SZ') rows=$(wc -l < "$OUT")"
