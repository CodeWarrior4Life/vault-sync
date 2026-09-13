#!/usr/bin/env python3
"""Arranger-rung detector: read health JSON on STDIN, emit headlsn|stale|frozen.

Lives in its own FILE, called by path, for one reason: the first cut of this
detector was written as `curl ... | python3 - ARGS <<'PYX'`, and THE HEREDOC IS
STDIN. Python read its program from the heredoc and the piped payload was
silently discarded, so the probe returned "?" for every input -- a CONSTANT that
looks identical to a clean negative. That is trap #1 in this lane's inherited
brief, reproduced verbatim by the seat that had been briefed on it, and caught
only because all four fixture cases returned the same answer.

A program passed as a file (or via -c) leaves stdin free for data. Never pipe
data into a heredoc-supplied program.

Uses sse_served_age_s and sse_served_lsn ONLY. last_event_age_s and
receiving_actual are deliberately never consulted: both read healthy for all
five subscribers throughout a 7h58m wedge on 2026-09-13.
"""
import sys, json, time

def main():
    snap, stale_t, span = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
    try:
        d = json.load(sys.stdin)
    except Exception:
        print("?||")
        return
    subs = d.get("subscribers") or []
    cur = {}
    for x in subs:
        l = x.get("sse_served_lsn")
        if isinstance(l, int):
            cur[x["subscriber_id"][:8]] = (l, x.get("sse_served_age_s"))
    if not cur:
        print("?||")
        return
    head = max(v[0] for v in cur.values())
    now = time.time()
    old = []
    try:
        for ln in open(snap):
            parts = ln.split()
            if len(parts) == 3:
                old.append((float(parts[0]), parts[1], int(parts[2])))
    except OSError:
        pass
    stale, frozen = [], []
    for sid, (lsn, age) in sorted(cur.items()):
        if isinstance(age, int) and age > stale_t and lsn < head:
            stale.append("%s(served_age_s=%d behind=%d)" % (sid, age, head - lsn))
        prior = [t for (t, s2, l2) in old if s2 == sid and l2 == lsn and now - t >= span]
        if prior and lsn < head:
            frozen.append("%s(lsn=%d unchanged %ds behind=%d)"
                          % (sid, lsn, int(now - min(prior)), head - lsn))
    with open(snap, "a") as f:
        for sid, (lsn, _a) in cur.items():
            f.write("%.0f %s %d\n" % (now, sid, lsn))
    print("%d|%s|%s" % (head, " ".join(stale), " ".join(frozen)))

if __name__ == "__main__":
    main()
