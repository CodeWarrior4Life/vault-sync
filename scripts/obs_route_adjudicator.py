#!/usr/bin/env python3
"""On-demand IDLE-vs-WEDGED adjudicator for a vault-sync route.

RULED IN by pitboss (arranger 6868f2d9) 2026-09-13 08:56 EDT, behind BOTH gated
detector rungs. It SUPPLEMENTS and does not replace nexus's per-route
"behind head by N rows" figure for continuous detection (constraint 4).

WHY IT EXISTS: `head - laggard` crossing any floor proves nothing. An idle route
leaves a stationary cursor while the head advances on OTHER routes, producing an
unbounded sustained gap. The pair-difference test rules out a SINGLE-instance
fault, but not a SHARED cause (both wedged, or the route's publisher dead). This
answers it directly: publish to the route, watch the cursor.

THE HARD GATE (constraint 2, and the reason this is a script and not a habit):
a NULL result means "wedged" ONLY if the write actually reached the route. If the
publish cannot be CONFIRMED, the verdict is INCONCLUSIVE and the rung stays at
'watch' -- it NEVER becomes wedged/P0. Otherwise a mirror/hook failure
masquerades as a wedge and manufactures a P0.

This gate is not hypothetical: the 2026-09-13 12:5xZ one-off run that motivated
the design did NOT confirm attribution. The cursor advanced +667 rows, which
proves the subscribers were being SERVED (so: not wedged, route idle) -- that
verdict is robust whoever's bytes arrived. But the run could not prove ITS OWN
write caused the advance, and other lanes publish to this route constantly
(lattice-security wrote one at 08:56:22). Without attribution you cannot tell
"no advance because wedged" from "no advance because my write never published".

Constraint 1: the payload is a memory file the seat OWES ANYWAY. Never a
synthetic note into the vault -- an observer must not manufacture vault content
to take a reading.
Constraint 3: output is for `pitboss` BY NAME. Never the operator.
"""
import json, os, subprocess, sys, time, urllib.request

HEALTH = os.environ.get("OBS_HEALTH", "https://nexus.obsidian-inc.com/api/sync/health")
MIRROR_LOG = os.path.expanduser("~/.claude/hooks/memory_mirror.log")
UA = "curl/8.7.1"          # public host requires this UA
SETTLE_S = int(os.environ.get("ADJ_SETTLE_S", "90"))
POLL_S = int(os.environ.get("ADJ_POLL_S", "25"))


def health():
    req = urllib.request.Request(HEALTH, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.load(r)


def cohorts(d):
    """Split subscribers by daemon_version; return (laggards, peers) by served_lsn."""
    subs = d.get("subscribers") or []
    rows = [(str(s.get("subscriber_id") or s.get("id"))[:10],
             str(s.get("daemon_version") or "?"),
             s.get("sse_served_lsn"), s.get("sse_served_age_s")) for s in subs]
    lsns = [r[2] for r in rows if isinstance(r[2], int)]
    if not lsns:
        return rows, [], []
    head = max(lsns)
    lag = [r for r in rows if isinstance(r[2], int) and r[2] < head]
    peers = [r for r in rows if isinstance(r[2], int) and r[2] == head]
    return rows, lag, peers


def publish_confirmed(memfile, since_ts):
    """CONSTRAINT 2. Two independent receipts; either suffices, neither is assumed.

    (a) vault twin exists with mtime >= the write  -> the mirror completed
    (b) a mirror-log line naming this basename after the write -> hook receipt
    Returns (bool, list_of_evidence_strings).
    """
    ev = []
    base = os.path.basename(memfile)
    twin = None
    try:
        for line in open(memfile, encoding="utf-8", errors="replace"):
            if line.startswith("  vault_destination:"):
                twin = "/Users/cyril/Vaults/Mainframe/" + line.split(":", 1)[1].strip()
                break
    except OSError:
        pass
    if twin and os.path.exists(twin):
        mt = os.stat(twin).st_mtime
        if mt >= since_ts - 2:
            ev.append("vault twin present, mtime %+.0fs vs write" % (mt - since_ts))
    if os.path.exists(MIRROR_LOG):
        try:
            tail = subprocess.run(["tail", "-40", MIRROR_LOG], capture_output=True,
                                  text=True, timeout=10).stdout
            for ln in tail.splitlines():
                if base.rsplit(".md", 1)[0] in ln and "mirrored" in ln:
                    ev.append("mirror-log receipt: " + ln.strip()[:90])
                    break
        except Exception:
            pass
    return (len(ev) > 0), ev


def adjudicate(memfile, write_ts):
    d0 = health()
    _, lag0, peers0 = cohorts(d0)
    base0 = {r[0]: r[2] for r in lag0}
    ok, ev = publish_confirmed(memfile, write_ts)

    deadline = time.time() + SETTLE_S
    advanced, obs = {}, []
    while time.time() < deadline:
        time.sleep(POLL_S)
        d = health()
        _, lag, peers = cohorts(d)
        cur = {r[0]: r[2] for r in lag}
        allr = {r[0]: (r[2], r[3]) for r in (lag + peers)}
        for sid, b in base0.items():
            now = allr.get(sid, (None, None))[0]
            if isinstance(now, int) and isinstance(b, int) and now > b:
                advanced[sid] = now - b
        obs.append({sid: allr.get(sid, (None, None)) for sid in base0})
        if advanced and len(advanced) == len(base0):
            break

    if not base0:
        # EDGE CASE, found in selftest: with every subscriber at head there is no
        # laggard cohort, so there is NOTHING to adjudicate. Returning INCONCLUSIVE
        # here would imply an unresolved wedge question that does not exist, and a
        # successor would chase it. Distinct verdict, deliberately.
        return {"verdict": "NO-LAGGARDS", "detail":
                "every subscriber is at head; no laggard cohort exists, so there is nothing to "
                "adjudicate. This is NOT a wedge question and NOT inconclusive.",
                "publish_confirmed": ok, "publish_evidence": ev, "baseline": {},
                "advanced": {}, "peers_at_head": [(r[0], r[3]) for r in peers0],
                "route_payload": memfile}

    if advanced and len(advanced) == len(base0):
        verdict = "IDLE-HEALTHY"
        detail = ("every laggard cursor ADVANCED after a publish (%s) => the route was IDLE and the "
                  "subscribers are being served. Not wedged." %
                  ", ".join("%s +%d" % (k, v) for k, v in advanced.items()))
    elif not ok:
        verdict = "INCONCLUSIVE"
        detail = ("no cursor advance, AND the publish could NOT be confirmed reaching the route. "
                  "Per the hard gate this is NOT 'wedged': an unpublished write is indistinguishable "
                  "from a wedge. Rung stays at WATCH. Confirm the publish, then re-run.")
    else:
        verdict = "WEDGED-CANDIDATE"
        detail = ("publish CONFIRMED (%s) but %d of %d laggard cursors did not advance within %ds => "
                  "subscribers are not being served on a route that received traffic. P0-class; "
                  "grade CANDIDATE and route to `pitboss` for oracle + direct-stat confirmation."
                  % ("; ".join(ev), len(base0) - len(advanced), len(base0), SETTLE_S))

    return {"verdict": verdict, "detail": detail, "publish_confirmed": ok,
            "publish_evidence": ev, "baseline": base0, "advanced": advanced,
            "peers_at_head": [(r[0], r[3]) for r in peers0], "route_payload": memfile}


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        # Force each verdict WITHOUT touching the live route: an instrument that
        # cannot return the other answer is a CONSTANT (this lane shipped two).
        print("SELFTEST — forcing all three verdicts on fixtures:")
        fake = "/tmp/.adj_no_such_memory_file.md"
        ok, ev = publish_confirmed(fake, time.time())
        print("  publish_confirmed(missing file) -> %s  %s" % (ok, "PASS (=> INCONCLUSIVE path)" if not ok else "FAIL"))
        base = {"a": 100, "b": 100}
        for adv, okf, want in (({"a": 5, "b": 5}, False, "IDLE-HEALTHY"),
                               ({}, False, "INCONCLUSIVE"),
                               ({}, True, "WEDGED-CANDIDATE")):
            if adv and len(adv) == len(base):
                got = "IDLE-HEALTHY"
            elif not okf:
                got = "INCONCLUSIVE"
            else:
                got = "WEDGED-CANDIDATE"
            print("  advanced=%-14s publish_ok=%-5s -> %-17s %s" % (adv, okf, got, "PASS" if got == want else "FAIL"))
        sys.exit(0)
    if len(sys.argv) < 2:
        print("usage: obs_route_adjudicator.py <memory-file-the-seat-owes> [write_epoch]", file=sys.stderr)
        sys.exit(2)
    mf = sys.argv[1]
    wts = float(sys.argv[2]) if len(sys.argv) > 2 else os.stat(mf).st_mtime
    r = adjudicate(mf, wts)
    print("ROUTE ADJUDICATION: %s" % r["verdict"])
    print("  %s" % r["detail"])
    print("  publish_confirmed=%s evidence=%s" % (r["publish_confirmed"], r["publish_evidence"] or "none"))
    print("  peers_at_head(age_s)=%s" % r["peers_at_head"])
    print("  NOTE: supplements, does not replace, nexus's per-route behind-head figure. "
          "Route output to `pitboss` by name, never the operator.")
