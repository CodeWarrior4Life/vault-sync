# nexus-vault-sync-supervisor (interim deaf supervisor, TKT-978e7918)

A host-side oneshot script that a user timer fires every minute. It reads the reconciler health
route (`/api/sync/health`, public and read-only) and restarts the vault-sync unit only when the
server reports this daemon's `receipt_state == not_receiving` continuously for `NVS_DEAF_SECS`
(1800 s on both hosts via the unit environment), at most once per `NVS_COOLDOWN_SECS` (1200 s).
Any other reading is "inconclusive, no action". It handles no secret.

Until this import it lived in no repository.

## Provenance (import commit)

| File | Source | sha256 |
|---|---|---|
| `nexus-vault-sync-supervisor.sh` | link `~/.local/bin/nexus-vault-sync-supervisor.sh` (mtime 2026-09-10 18:14), copied byte for byte | `a09ff327fdbcf151d00c78200f0ffe79b6baaeae99c404901da6580ae9183096` |
| `systemd/link/nexus-vault-sync-supervisor.service` | link `~/.config/systemd/user/` | `0bf7dd9af79e2a776f08a14e6aaacbd29446a3e4dd9209eb0b05a8124fe8160e` |
| `systemd/link/nexus-vault-sync-supervisor.service.d/threshold.conf` | link drop-in (`NVS_DEAF_SECS=1800`, pitboss GO-30) | `122133689d07d23e220df42ee6f36d55dd83187041b8f68141ca34106a3523bf` |
| `systemd/link/nexus-vault-sync-supervisor.timer` | link `~/.config/systemd/user/` | `6d2b798226763066a2774ad140db11203d7b5c0b37d8f253b1f48e4dbc8ea029` |
| `systemd/link/nexus-vault-sync-supervisor.timer.d/10-wallclock-failsafe.conf` | link drop-in (wall-clock failsafe, TKT-60fa717c) | `c1a7c7b6107806416db25c7ef6c08e4348f29593868260e539458e1106e75dfa` |
| `systemd/icarus/nexus-vault-sync-supervisor.service` | icarus `~/.config/systemd/user/` (no drop-ins on icarus) | `e21f0d14a7d524c57f7916133eee86085bc4e794200b937119863e5c5cccd121` |
| `systemd/icarus/nexus-vault-sync-supervisor.timer` | icarus `~/.config/systemd/user/` | `54d174de8207563e585eef9b68bd99c934624003453024b0b0559f30314617a0` |

Measured 2026-10-26 09:57 EDT on link. The script on **icarus is byte-identical** (same sha256, read over ssh at
the same time). The unit files differ per host: icarus folds the threshold and the wall-clock
failsafe into the unit files and sets its own `NVS_SUBSCRIBER_ID` (an identifier, not a secret).
The only `Environment=` lines carry `NVS_DEAF_SECS` and `NVS_SUBSCRIBER_ID`; nothing is
credential-shaped. `/usr/lib/systemd/user/service.d/10-timeout-abort.conf` (Fedora's systemd package
drop-in) is not copied.

The repo copy is the source. Nothing in this repo installs it; placement on a host is a separate GO.

## O5-2a: alert on an inactive vault-sync unit

Rulings: LSEC-9325-B6D8-O5-ONFAILURE-RULING O5-2 ("No Silent Holds"), LSEC-40A8-B6D8-O52-ROUTE-AND-KEY-RULING, and
Pitboss PB-ADEB-O52A, -O52A-KEY and -O52A-NAME. Before any host switches its units to `Restart=on-failure`, a
vault-sync unit that has been inactive for more than 15 min must alert.

The not_receiving logic cannot see that case: a stopped daemon reads "inconclusive". So the supervisor runs one
extra check each tick, **before** the not_receiving logic and independent of it. That logic is unchanged byte for
byte: the tests strip the O5-2a block and get the imported sha256 back, and they run a differential against the
pinned imported copy.

The check is **alert only**. It never starts, stops or restarts a unit, and it never exits the script.

### Per unit (default `nexus-vault-sync.service nexus-vault-sync-memory.service`)

| State | Action |
|---|---|
| `LoadState=not-found` (not on this host; icarus has no `-memory`) | skipped; stale state cleared |
| `systemctl` unreadable | inconclusive: no change |
| not active (`inactive`, `failed`, masked, ...) | `unit.<u>.inactive_since` written at the first such tick |
| not active for `>= NVS_INACTIVE_SECS` (900) | ALERT: a P0-tier page to the host arranger, plus the lnotify notice; at most once per `NVS_REALERT_SECS` (3600) |
| page undeliverable or no arranger resolves | `operator_escalate` at once; the notice body says the page FAILED; **this tick exits 3** (after the deaf path has run) |
| still not active `NVS_ESCALATE_SECS` (1800) after a DELIVERED page | `operator_escalate`, **once per episode** |
| active again after an alert | one "recovered" page and notice (same identity key); state cleared |
| active again before the threshold | logged only |

### The page target: resolved at send time, never pinned

An arranger csid dies at every seat rotation, so a pinned csid pages a dead inbox. The target is resolved at
each send:

1. **The live arranger role marker**, `~/.claude/runtime/conductor.role.arranger-<csid>`.
   - Its one writer is `~/.claude/scripts/pb-role-marker.py`. By that writer's contract the FILENAME is
     authoritative.
   - The csid must fullmatch the uuid shape before any path use. The resolved path must stay inside the runtime
     dir, so a symlink that escapes it is rejected.
   - A content line naming a different csid is a tamper signal, and the marker is rejected.
   - If more than one marker is live, the newest `claimed_at` wins (mtime if absent), and the choice is logged
     LOUD.
   - Released markers live in `~/.claude/backups/role-markers/` and are never read.
2. **The live roster row carrying the HOST's own arranger name** (`GET /api/conductor/sessions/list?status=live`).
   - The name map is `link` → `pitboss`, `icarus` → `pitboss-icarus`, `trinity` → `pitboss-trinity`, overridable
     with `NVS_ROSTER_NAME` in the host's drop-in.
   - **An unknown host with no override has no name fallback.** A constant `pitboss` would page link's seat from
     icarus.
3. **Neither resolves: FAIL LOUD.** `operator_escalate` fires, the notice still fires, and the tick exits 3.

`NVS_PAGE_CSID` is an explicit **override for tests and emergencies only**. It is validated, logged as an override,
and must never be set in a shipped unit or drop-in.

### Route and key

- **Page:** `POST /api/conductor/coord/emit` with
  `{"kind":"page","session_id":<csid>,"body":{"tier":"decision","reason":"P0 vault-sync unit <u> on <host>: INACTIVE|RECOVERED","message":...}}`.
  This is the fleet pager shape of `pb-prompt-watch.py:293-294`. The fleet's P0 pagers (`pb-p0-watch.py:1525-1526`,
  `LOUD_TIER = "decision"` at `:1654`) mark P0 the same way: tier `decision` (a routing value) with a "P0" reason.
- **Key:** the host's standard Nexus key resolver in its **EXEC form**, `~/.local/bin/nexus-api-key.sh exec --
  python3 ...`. The value is injected only into the child's env as `NEXUS_API_KEY`, the variable
  `pb-prompt-watch.py:236` reads. It reaches curl on stdin (`-K -`), never argv and never stdout. No conductor
  service key is used. The sync-nightly key is never used.
- **Escalate:** `python3 ~/.claude/scripts/operator_escalate.py --subject S --body B --severity critical`, the same
  invocation as `pb-seat-parity.py:282-285` (severity `critical`, no `--await-ack` in a oneshot), bounded at 180 s.
- **Notice:** every alert also writes the durable record:
  `lnotify legacy --principal <host>.nexus-vault-sync-supervisor@lattice --app nexus-vault-sync --venture lattice
  --event-type unit.stopped|unit.recovered --claim notice --identity-key nexus-vault-sync.unit-inactive:<host>:<unit>`.
  - It goes to the vault-note route and the ledger. The interruptive `nexus_nudge` route accepts only
    decision/block, so the notice never reaches it.
  - It needs no grant. No coalesce threshold is registered for this principal.

| Env | Default |
|---|---|
| `NVS_WATCH_UNITS` | `nexus-vault-sync.service nexus-vault-sync-memory.service` |
| `NVS_INACTIVE_SECS` / `NVS_REALERT_SECS` / `NVS_ESCALATE_SECS` | `900` / `3600` / `1800` |
| `NVS_ROSTER_NAME` | from the hostname map (link, icarus, trinity); unset for other hosts |
| `NVS_ROLE_RUNTIME` | `~/.claude/runtime` |
| `NVS_KEY_HELPER` | `~/.local/bin/nexus-api-key.sh` (exec form) |
| `NVS_ESCALATE` / `NVS_ESCALATE_SEVERITY` | `~/.claude/scripts/operator_escalate.py` / `critical` |
| `NVS_NEXUS_BASE` | `https://nexus.obsidian-inc.com` |
| `NVS_LNOTIFY` | `lnotify` on PATH, else `~/.local/bin/lnotify` |
| `NVS_PAGE_CSID` | unset. OVERRIDE for tests and emergencies only |

### Placement proof required before any Restart-policy change (O52-4)

On each host, after a separate placement GO:

1. A real page event id within 15 min of stopping a canary unit cleanly.
2. An `operator_escalate` on a forced-undeliverable page.

The resolver's unit context is checked for real at placement.

## Tests

`bash scripts/supervisor/test-supervisor.sh` runs in about 8 s with bash and python3 only. It uses PATH shims
for systemctl, curl, lnotify, logger, `date +%s` and hostname, plus a stub resolver and a stub `operator_escalate.py`
under a temp HOME. Nothing touches a real unit, the journal, the network, the rail, an inbox or the operator.
CI runs it on ubuntu.
