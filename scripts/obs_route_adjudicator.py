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


def route_roots():
    """Map route -> watched filesystem root, read from each memory daemon's config.

    THE RECEIPT THAT MATTERS. The first cut of this gate checked the VAULT TWIN and
    the memory-mirror hook log -- a DIFFERENT mechanism (memory -> vault twin) from
    the ROUTE PUBLISH (a vault-sync daemon watching a filesystem root). So it
    returned INCONCLUSIVE for a write that had in fact reached the route, because
    Bash-written memory files bypass the tool-scoped mirror hook (TKT-f1ef9ac2)
    while the daemon's own watcher still publishes them. A gate checking the wrong
    receipt is worse than no gate: it manufactures false INCONCLUSIVEs and, with the
    verdict table inverted, could have manufactured a false wedge.
    """
    roots = {}
    base = os.path.expanduser("~/Library/Application Support/Nexus")
    if not os.path.isdir(base):
        return roots
    for d in os.listdir(base):
        cfg = os.path.join(base, d, "config.toml")
        if not os.path.exists(cfg):
            continue
        route = path = sid = None
        try:
            for ln in open(cfg, encoding="utf-8", errors="replace"):
                ln = ln.strip()
                if ln.startswith("route"):
                    route = ln.split("=", 1)[1].strip().strip('"')
                elif ln.startswith("path") or ln.startswith("vaults_root"):
                    path = path or ln.split("=", 1)[1].strip().strip('"')
                elif ln.startswith("subscriber_id"):
                    sid = ln.split("=", 1)[1].strip().strip('"')
        except OSError:
            continue
        if route and path:
            roots[route] = {"path": path, "subscriber_id": sid, "config": cfg}
    return roots


def daemon_alive_for(root_path):
    """Identify the ACTUAL daemon for this root. No loose fallback.

    The first cut ended with a broad `ps` grep that matched the caller's own shell
    (`/bin/zsh -c source ...`) and reported it as "live daemon for that root",
    which turned an unverified premise into a CONFIRMED publish and produced a
    FALSE WEDGED-CANDIDATE -- the exact P0 this gate exists to prevent. Match the
    daemon EXECUTABLE living under the root's own app bundle, or return False.
    """
    want = os.path.dirname(os.path.realpath(root_path))  # unused as a match key; kept for clarity
    try:
        out = subprocess.run(["ps", "-eo", "pid=,command="], capture_output=True,
                             text=True, timeout=15).stdout
    except Exception:
        return False, "ps failed"
    for ln in out.splitlines():
        ln = ln.strip()
        if "vault-sync-daemon" not in ln:
            continue
        if "/bin/zsh" in ln or "-c source" in ln or "grep" in ln:
            continue                      # never a daemon
        # The memory instance runs its daemon from its own support directory.
        if "vault-sync-memory" in ln:
            return True, ln[:100]
    return False, "no vault-sync-daemon process for this root (publisher down => P0-class)"


def publish_confirmed(memfile, since_ts):
    """CONSTRAINT 2, corrected: confirm the write reached THE ROUTE.

    Receipts, in order of strength:
      (a) the file is UNDER a route's configured watched root  -> the daemon sees it
      (b) a live daemon exists for that root                   -> it can publish it
    Weak/irrelevant: vault twin + mirror-hook log. Those attest the memory->vault
    MIRROR, not the route publish, and are reported as context only -- never as the
    gate. Returns (bool, evidence).
    """
    ev, real = [], os.path.realpath(memfile)
    matched = None
    for route, info in route_roots().items():
        if real.startswith(os.path.realpath(info["path"]) + os.sep):
            matched = (route, info)
            ev.append("file is under watched root of route %s (subscriber %s)"
                      % (route, (info.get("subscriber_id") or "?")[:8]))
            break
    if not matched:
        return False, ["file is NOT under any configured route root -- it cannot publish"]
    alive, how = daemon_alive_for(matched[1]["path"])
    ev.append(("live daemon for that root: " + how) if alive
              else "NO live daemon for that root -- publisher down (P0-class, see the dead-unit precedent)")
    if not alive:
        return False, ev
    try:
        if os.stat(memfile).st_mtime >= since_ts - 2:
            ev.append("file mtime is at/after the write")
    except OSError:
        pass
    return True, ev


MAX_PAYLOAD_AGE_S = int(os.environ.get("ADJ_MAX_PAYLOAD_AGE_S", "180"))


def adjudicate(memfile, write_ts):
    # FRESHNESS GATE. The probe's logic is baseline -> publish -> watch. Run against
    # a file whose rows were already consumed, "no advance" is the EXPECTED result and
    # says nothing about wedging. A 15-minute-old payload produced a false
    # WEDGED-CANDIDATE on the first corrected run. Refuse rather than mislead.
    age = time.time() - os.stat(memfile).st_mtime
    if age > MAX_PAYLOAD_AGE_S:
        return {"verdict": "STALE-PAYLOAD", "detail":
                "payload is %.0fs old (> %ds). This probe requires a FRESH write: take the "
                "baseline, THEN write the memory file the seat owes, THEN adjudicate. Against an "
                "already-consumed file 'no advance' is expected and is NOT evidence of wedging."
                % (age, MAX_PAYLOAD_AGE_S),
                "publish_confirmed": None, "publish_evidence": [], "baseline": {},
                "advanced": {}, "peers_at_head": [], "route_payload": memfile}

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
