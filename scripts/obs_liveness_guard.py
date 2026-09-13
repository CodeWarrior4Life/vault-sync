#!/usr/bin/env python3
"""Liveness guard for nexus_obs_watch_v11.sh — SPEAKS ONLY WHEN THE OBSERVER IS NOT LIVE.

WHY THIS EXISTS (nexus-obs-2, d09c4015, 2026-09-13 12:2xZ): the observer polls every
60 s but emits only on TRANSITION; every one of its heartbeats is gated on a condition
PERSISTING (30-min while divergence persists, 10-min while oracle not green, 30-min
while stalled>0). So when everything is GREEN its only unconditional output is the
HOURLY at :00 — and a WEDGED or DEAD observer is therefore indistinguishable from a
healthy quiet one for up to 60 minutes. This seat's whole value is that instrument, so
an hour-long blind window on it is the wrong trade.

Alive-but-mute is the exact failure class this lane catalogued (TKT-71429995, and the
'&'/nohup watcher that held a pidfile slot while emitting nothing).

DESIGN, deliberately: a pid being alive PROVES NOTHING (that was the original mistake).
Liveness is proven by STATE CHURN — the script rewrites /tmp/.obs_sz_prev.<shellpid>
every poll. We resolve the observer's shell pid dynamically each tick, so this guard
survives observer re-arms instead of pinning a stale pid.

SILENT WHEN HEALTHY, on purpose: no output => no Monitor flood cap, and silence here
cannot be mistaken for health because THIS guard is what breaks the silence on failure.
"""
import os, re, glob, subprocess, sys, time

SCRIPT = "nexus_obs_watch_v11.sh"
STALE_S = int(os.environ.get("OBS_GUARD_STALE_S", "180"))   # 3x the 60s poll
POLL_S  = int(os.environ.get("OBS_GUARD_POLL_S", "60"))

def observer_pids():
    try:
        out = subprocess.run(["pgrep", "-f", SCRIPT], capture_output=True, text=True, timeout=15).stdout
    except Exception:
        return []
    pids = []
    for tok in out.split():
        if tok.isdigit():
            p = int(tok)
            if p != os.getpid():
                pids.append(p)
    return pids

def newest_state_age(pids):
    """Age in seconds of the freshest state file belonging to any observer pid."""
    best = None
    for p in pids:
        for pat in (f"/tmp/.obs_sz_prev.{p}", f"/tmp/.obs_sz_cur.{p}", f"/tmp/.obs_snap.{p}"):
            for f in glob.glob(pat):
                try:
                    age = time.time() - os.stat(f).st_mtime
                except OSError:
                    continue
                if best is None or age < best:
                    best = age
    return best

def main():
    if "--selftest" in sys.argv:
        pids = observer_pids()
        age = newest_state_age(pids)
        print("SELFTEST pids=%s newest_state_age=%s stale_threshold=%ss -> verdict=%s"
              % (pids, ("%.1fs" % age) if age is not None else None, STALE_S,
                 "ABSENT" if not pids else ("NO-STATE" if age is None else ("STALE" if age > STALE_S else "LIVE"))))
        return 0

    last_state = None
    while True:
        pids = observer_pids()
        if not pids:
            verdict = "ABSENT"
            msg = ("OBS-GUARD 🔴 OBSERVER ABSENT — no process matching %s. This lane is BLIND: "
                   "the conflict-copy count, the oracle rung and COND 1 are all unmeasured until it is re-armed. "
                   "Re-arm from the pointer's Monitor shape and verify by ancestry to your harness pid." % SCRIPT)
        else:
            age = newest_state_age(pids)
            if age is None:
                verdict = "NO-STATE"
                msg = ("OBS-GUARD ⚠️ OBSERVER pid(s) %s alive but NO state file found — cannot prove the poll loop "
                       "is turning. A live pid is not a live instrument; treat as UNVERIFIED, not healthy." % pids)
            elif age > STALE_S:
                verdict = "STALE"
                msg = ("OBS-GUARD 🔴 OBSERVER ALIVE-BUT-MUTE — pid(s) %s present, but newest state write was "
                       "%.0fs ago (> %ds = 3x the 60s poll). The poll loop has STOPPED TURNING while the process "
                       "still exists: the exact alive-but-mute class this lane catalogued. Silence from the "
                       "observer is NOT health right now." % (pids, age, STALE_S))
            else:
                verdict = "LIVE"
                msg = None

        # Speak on entry to a bad state, and on RECOVERY (so a cleared alarm is visible).
        if verdict != "LIVE" and verdict != last_state:
            print(msg, flush=True)
        elif verdict == "LIVE" and last_state not in (None, "LIVE"):
            print("OBS-GUARD ✅ observer LIVE again (state churn resumed, newest write %.0fs ago) — "
                  "prior alarm CLEARED." % (newest_state_age(pids) or 0), flush=True)
        last_state = verdict
        time.sleep(POLL_S)

if __name__ == "__main__":
    sys.exit(main())
