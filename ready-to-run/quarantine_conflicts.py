#!/usr/bin/env python3
# R6: move *.conflict-from-* OUT of the vault to the quarantine tree, preserving
# relative paths, with MANIFEST.txt + README.md. NEVER deletes. Fully reversible
# (README documents the reverse). Run ON the target host (trinity) AFTER the
# daemon is confirmed stopped and AFTER the R2 vault snapshot exists.
# OWNER-GATED per the dispatcher (execute only on ACK).
import os, shutil, datetime
HOME = os.path.expanduser("~")
SRC = os.path.join(HOME, "vaults", "Mainframe")
Q = os.path.join(HOME, ".local/share/Nexus/quarantine/conflict-storm-2026-07-18")
os.makedirs(Q, exist_ok=True)
moved, errors = [], []
for dirpath, _dirnames, filenames in os.walk(SRC):
    for fn in filenames:
        if ".conflict-from-" in fn:
            src = os.path.join(dirpath, fn)
            rel = os.path.relpath(src, SRC)
            dest = os.path.join(Q, rel)
            try:
                os.makedirs(os.path.dirname(dest), exist_ok=True)
                sz = os.path.getsize(src)
                shutil.move(src, dest)
                moved.append((rel, sz))
            except Exception as e:
                errors.append((rel, repr(e)))
total = sum(s for _, s in moved)
with open(os.path.join(Q, "MANIFEST.txt"), "w") as f:
    f.write("# Conflict-storm quarantine MANIFEST\n")
    f.write(f"# generated: {datetime.datetime.now().isoformat()}\n")
    f.write(f"# source_root: {SRC}\n# quarantine_root: {Q}\n")
    f.write(f"# files_moved: {len(moved)}  total_bytes: {total}  errors: {len(errors)}\n")
    f.write("#\n# <bytes>\\t<relative_path_within_vault>\n")
    for rel, sz in sorted(moved):
        f.write(f"{sz}\t{rel}\n")
    if errors:
        f.write("\n# ERRORS (NOT moved):\n")
        for rel, e in errors:
            f.write(f"# {rel}\t{e}\n")
with open(os.path.join(Q, "README.md"), "w") as f:
    f.write(f"""# Conflict-storm quarantine - 2026-07-18

`*.conflict-from-*` files minted by the vault-sync conflict storm (TKT-86ae42a3),
moved OUT of `{SRC}` into this tree preserving relative paths. NONE deleted.
files_moved: {len(moved)}  total_bytes: {total}  errors: {len(errors)}

## Reverse (restore into the vault)
```sh
Q="{Q}"; SRC="{SRC}"; cd "$Q"
find . -type f ! -name MANIFEST.txt ! -name README.md -print0 | while IFS= read -r -d '' f; do
  rel="${{f#./}}"; mkdir -p "$SRC/$(dirname "$rel")"; mv "$f" "$SRC/$rel"
done
```
Do NOT restore blindly: canonical bytes already live in PG / the live note.
""")
print(f"MOVED={len(moved)} TOTAL_BYTES={total} ERRORS={len(errors)}")
for rel, e in errors[:10]:
    print("ERR", rel, e)
