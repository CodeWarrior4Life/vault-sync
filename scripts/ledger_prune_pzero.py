#!/usr/bin/env python3
"""P-zero prune of a Nexus Sync delete-burst-SUPPRESSED ledger (LSEC-G706/G707).

Keeps ONLY the ledger lines whose path is PRESENT on disk under --vault-root (the replay
skips those anyway) and drops every line whose path is ABSENT (each of which a lift would
replay as an Unknown-base Delete). After the prune a lift must log `replayed=0`.

Method (the OP-26511 M2 precedent): write a sibling temp file in the SAME dir, fsync,
os.replace() over the ledger (atomic rename), then RE-READ and verify. v0.4.47 reads the
ledger by path on every call (delete_ledger.rs DeleteLedger::paths), so a rename is picked
up at once. Race guard: if the daemon appended between our read and the rename, the re-read
shows absent paths != 0 or an unexpected line set. Then we exit 3 and the operator re-runs.

Stat errors count as PRESENT (kept), mirroring v0.4.47's TKT-6e7fec3e rule, and are reported.
--expect-sha guards against operating on a ledger other than the one the bind names.
The original is NOT deleted: the caller backs it up first (bind step 2a).
"""
import argparse
import hashlib
import json
import os
import sys


def parse(body):
    # delete_ledger.rs parse_ledger: one path per line; blank lines ignored; a torn final
    # line (no trailing newline) is dropped.
    lines = body.split("\n")
    if lines and lines[-1] != "":
        lines = lines[:-1]
    return [l for l in lines if l.strip()]


def state(vault_root, p):
    # Mirror Rust Path::try_exists (follows symlinks): NotFound -> ABSENT (a dangling
    # symlink is ABSENT and WOULD replay); any other stat error -> UNSTATABLE (kept).
    try:
        os.stat(os.path.join(vault_root, p))
        return "PRESENT"
    except (FileNotFoundError, NotADirectoryError):
        return "ABSENT"
    except OSError:
        return "UNSTATABLE"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ledger", required=True)
    ap.add_argument("--vault-root", required=True)
    ap.add_argument("--expect-sha", required=True, help="sha256 of the ledger as bound")
    ap.add_argument("--apply", action="store_true", help="without it: dry run, nothing written")
    a = ap.parse_args()

    raw = open(a.ledger, "rb").read()
    sha = hashlib.sha256(raw).hexdigest()
    if sha != a.expect_sha:
        print(json.dumps({"verdict": "REFUSE", "why": "ledger sha != bound sha", "sha": sha}))
        sys.exit(4)
    lines = parse(raw.decode("utf-8", "replace"))
    st = {p: state(a.vault_root, p) for p in set(lines)}
    keep = [l for l in lines if st[l] != "ABSENT"]
    drop = sorted({l for l in lines if st[l] == "ABSENT"})
    out = {"ledger_sha_before": sha, "lines_before": len(lines), "distinct_before": len(st),
           "absent_distinct_dropped": len(drop), "lines_after": len(keep),
           "present_kept": sum(1 for v in st.values() if v == "PRESENT"),
           "unstatable_kept": sorted(p for p, v in st.items() if v == "UNSTATABLE")}
    if not a.apply:
        out["verdict"] = "DRY-RUN"
        print(json.dumps(out))
        return

    d = os.path.dirname(os.path.abspath(a.ledger))
    tmp = os.path.join(d, ".delete-burst-SUPPRESSED.pzero.tmp")
    body = "".join(l + "\n" for l in keep).encode()
    with open(tmp, "wb") as f:
        f.write(body)
        f.flush()
        os.fsync(f.fileno())
    os.chmod(tmp, os.stat(a.ledger).st_mode & 0o777)
    os.replace(tmp, a.ledger)
    dfd = os.open(d, os.O_RDONLY)
    try:
        os.fsync(dfd)
    finally:
        os.close(dfd)

    after = open(a.ledger, "rb").read()
    al = parse(after.decode("utf-8", "replace"))
    absent_after = sorted({p for p in al if state(a.vault_root, p) == "ABSENT"})
    out.update({"ledger_sha_after": hashlib.sha256(after).hexdigest(), "lines_after_reread": len(al),
                "absent_after": len(absent_after), "absent_after_paths": absent_after[:20]})
    ok = (al == keep) and not absent_after
    out["verdict"] = "OK" if ok else "RACE-OR-MISMATCH"
    print(json.dumps(out))
    sys.exit(0 if ok else 3)


if __name__ == "__main__":
    main()
