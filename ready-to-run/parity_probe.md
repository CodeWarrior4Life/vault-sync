# R5 — Verified Parity Protocol (per host, OWNER-GATED)

Run only after the daemon is started and the 30-min soak is clean. Both directions,
byte-exact. Delete probe notes after. Record subscriber-row `version` for each.

Subscribers: link `a6f8219e-2fcb-4a9a-a2c6-0d3471919d1c` · trinity `f2383e35-2e9d-4da2-b5ed-de8a35778fa3`.

## Direction A — server -> host (materialization)
1. Create a probe note server-side via the Nexus API (choke-point):
   `POST {nexus}/api/notes` body `{ "path": "99_Probe/parity-<host>-<ts>.md", "body": "<random-token>\n" }`
   (CLI Direct Path: on cypher use `docker exec nexus sqlite3`/PG choke-point equivalent; do NOT hand-write vault_notes.)
2. Wait for SSE materialization on the host; then:
   `diff <(printf '%s' "<random-token>\n") "$HOME/vaults/Mainframe/99_Probe/parity-<host>-<ts>.md"`
   Expect: identical (rc 0). Record the materialized bytes' sha256 == server payload sha256.

## Direction B — host -> PG (push via choke-point)
1. Edit a local probe note: `printf '<token2>\n' > "$HOME/vaults/Mainframe/99_Probe/parity-<host>-<ts>-local.md"`
2. Wait for push; then confirm byte-exact arrival in PG `vault_notes` **via the choke-point read path**
   (not a raw row read that could show pre-canonicalization bytes). Record the subscriber row `version`.
   Expect: PG body == local bytes, sha256 match.

## Evidence to capture into BURN_REPORT.md
- per host, per direction: probe path, token, sha256 both ends, PASS/FAIL, subscriber row `version`.

## Cleanup
Delete both probe notes (server-side delete + confirm local removal). Confirm no `*.conflict-from-*`
sibling was minted for either probe.
