#!/usr/bin/env python3
"""Post-lift delete watch for a Nexus Sync delete-burst valve lift (LSEC-G706/G707).

Lifting the valve (removing delete-burst-PAUSED) does two things, per v0.4.47
(file_watcher.rs replay_suppressed_deletes, redflag.rs poll_marker_resume):
  * it replays every ledger path that is absent on disk as an Unknown-base Delete;
  * it grants GRACE_DELETES=5000 over GRACE_WINDOW=900 s, so for 15 minutes the burst
    valve will NOT trip on its own.
Re-creating the marker at runtime does NOT re-latch: only a boot reads it.

So for the grace window this script stands in for the valve. It tails the daemon's
log and STOPS THE UNIT ITSELF on the first:
  * `REPLAYED suppressed deletes ... replayed=N` with N != --expect-replayed;
  * outbound delete push (`push accepted path=... hash=` with an empty hash);
  * ERROR line from the daemon.
On a stop it runs --stop-cmd, then restores the marker from --marker-backup (so any
later start re-latches at boot). It never starts the daemon. It writes a JSON receipt
either way. A false stop on a legitimate user delete is acceptable by the gate's ruling.

Read-only apart from: the stop command, the marker restore, and the receipt file.
Stdlib only; runs on Linux and macOS.
"""
import argparse
import glob
import json
import os
import re
import shutil
import subprocess
import sys
import time

RE_REPLAYED = re.compile(r"REPLAYED suppressed deletes")
RE_REPLAYED_N = re.compile(r"\breplayed[=:]\s*(\d+)")
RE_RESUMED = re.compile(r"OUTBOUND DELETE PROPAGATION RESUMED")
RE_DELETE_PUSH = re.compile(r"push accepted path=(.*?) seq=\S* hash=\s*$")
RE_ERROR = re.compile(r"^\S+\s+ERROR\s")


def newest(pattern):
    files = [f for f in glob.glob(pattern) if not f.endswith(".gz")]
    return max(files, key=os.path.getmtime) if files else None


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--log-glob", required=True, help="daemon log glob; newest non-.gz file is followed (rotation-safe)")
    ap.add_argument("--duration", type=int, default=900)
    ap.add_argument("--expect-replayed", type=int, default=0)
    ap.add_argument("--stop-cmd", required=True, help="JSON argv list run on a trip")
    ap.add_argument("--marker", required=True)
    ap.add_argument("--marker-backup", required=True)
    ap.add_argument("--receipt", required=True)
    ap.add_argument("--dry-run", action="store_true", help="never run the stop or restore; report what would happen")
    a = ap.parse_args()
    stop_argv = json.loads(a.stop_cmd)
    assert isinstance(stop_argv, list) and stop_argv and all(isinstance(x, str) for x in stop_argv)
    if not os.path.isfile(a.marker_backup):
        sys.exit(f"marker backup missing: {a.marker_backup}")

    t0 = time.time()
    rec = {"t0_epoch": t0, "duration": a.duration, "expect_replayed": a.expect_replayed,
           "log_glob": a.log_glob, "resumed_seen": None, "replayed_seen": None,
           "trip": None, "stopped": False, "marker_restored": False, "dry_run": a.dry_run}
    cur = newest(a.log_glob)
    pos = os.path.getsize(cur) if cur else 0   # only lines written AFTER we start
    buf = ""

    def trip(reason, line):
        rec["trip"] = {"reason": reason, "line": line[:400], "at": time.time()}
        if not a.dry_run:
            p = subprocess.run(stop_argv, capture_output=True, text=True, timeout=120)
            rec["stopped"] = p.returncode == 0
            rec["stop_rc"] = p.returncode
            rec["stop_err"] = p.stderr[-400:]
            if not os.path.exists(a.marker):
                shutil.copy2(a.marker_backup, a.marker)
            rec["marker_restored"] = os.path.exists(a.marker)

    while time.time() - t0 < a.duration and rec["trip"] is None:
        n = newest(a.log_glob)
        if n and n != cur:                      # rotated: drain the old file, then follow the new one
            cur, pos = n, 0
        if cur and os.path.exists(cur):
            with open(cur, "r", encoding="utf-8", errors="replace") as f:
                f.seek(pos)
                chunk = f.read()
                pos = f.tell()
            buf += chunk
            *lines, buf = buf.split("\n")
            for ln in lines:
                if RE_RESUMED.search(ln) and rec["resumed_seen"] is None:
                    rec["resumed_seen"] = time.time()
                if RE_REPLAYED.search(ln):
                    m = RE_REPLAYED_N.search(ln)
                    nrep = int(m.group(1)) if m else -1
                    rec["replayed_seen"] = nrep
                    if nrep != a.expect_replayed:
                        trip(f"replayed={nrep} != expected {a.expect_replayed}", ln)
                        break
                m = RE_DELETE_PUSH.search(ln)
                if m:
                    trip(f"outbound delete push: {m.group(1)}", ln)
                    break
                if RE_ERROR.search(ln):
                    trip("daemon ERROR line", ln)
                    break
        time.sleep(2)

    rec["ended"] = time.time()
    rec["verdict"] = "TRIPPED" if rec["trip"] else ("PASS" if rec["resumed_seen"] else "NO-RESUME-SEEN")
    with open(a.receipt, "w") as f:
        json.dump(rec, f, indent=2)
    print(json.dumps({k: rec[k] for k in ("verdict", "resumed_seen", "replayed_seen", "trip", "stopped", "marker_restored")}))
    sys.exit(0 if rec["verdict"] == "PASS" else 2)


if __name__ == "__main__":
    main()
