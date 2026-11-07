#!/usr/bin/env python3
"""Read-only per-path listing of a delete-burst-SUPPRESSED ledger under the P-zero rule (LSEC-G707).

Companion to ledger_prune_pzero.py (bound sha adcc1744). It IMPORTS that tool's parse() and
state(), so the listing classifies every path exactly as the prune will. Writes nothing to
the ledger, ever.

Output: a TSV on --out (or stdout): action <TAB> state <TAB> line_count <TAB> path, where
action is DROP (ABSENT, would replay; the prune removes it) or KEEP (PRESENT or UNSTATABLE;
replay skips PRESENT, retains UNSTATABLE). A JSON summary goes to stderr.

--protect-prefix P (repeatable) reports every ledger line under P. With --assert-clear the
exit code is 5 if any line under a protected prefix would be REPLAYED (state ABSENT), i.e. if
the replay could touch that prefix. Run after the prune, it is the exclusion proof's runtime
assertion: after a P-zero prune it must exit 0.
"""
import argparse
import collections
import hashlib
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from ledger_prune_pzero import parse, state  # noqa: E402  (same classification as the prune)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ledger", required=True)
    ap.add_argument("--vault-root", required=True)
    ap.add_argument("--out", help="TSV path (default stdout)")
    ap.add_argument("--protect-prefix", action="append", default=[])
    ap.add_argument("--assert-clear", action="store_true")
    a = ap.parse_args()

    raw = open(a.ledger, "rb").read()
    lines = parse(raw.decode("utf-8", "replace"))
    cnt = collections.Counter(lines)
    st = {p: state(a.vault_root, p) for p in cnt}
    rows = sorted(((("DROP" if s == "ABSENT" else "KEEP"), s, cnt[p], p) for p, s in st.items()),
                  key=lambda r: (r[0], r[3]))
    body = "".join(f"{r[0]}\t{r[1]}\t{r[2]}\t{r[3]}\n" for r in rows)
    if a.out:
        fd = os.open(a.out, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w") as f:
            f.write(body)
    else:
        sys.stdout.write(body)

    prot = {}
    for pre in a.protect_prefix:
        hit = [(p, st[p], cnt[p]) for p in st if p.startswith(pre)]
        prot[pre] = {"distinct": len(hit), "lines": sum(h[2] for h in hit),
                     "would_replay": sorted(h[0] for h in hit if h[1] == "ABSENT"),
                     "paths": sorted(f"{h[1]}:{h[0]}" for h in hit)}
    summ = {"ledger_sha": hashlib.sha256(raw).hexdigest(), "lines": len(lines), "distinct": len(st),
            "drop_absent": sum(1 for s in st.values() if s == "ABSENT"),
            "keep_present": sum(1 for s in st.values() if s == "PRESENT"),
            "keep_unstatable": sum(1 for s in st.values() if s == "UNSTATABLE"),
            "listing_sha": hashlib.sha256(body.encode()).hexdigest(), "protect": prot}
    clear = all(not v["would_replay"] for v in prot.values())
    summ["protect_clear"] = clear
    sys.stderr.write(json.dumps(summ) + "\n")
    if a.assert_clear and not clear:
        sys.exit(5)


if __name__ == "__main__":
    main()
