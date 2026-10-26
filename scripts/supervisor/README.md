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
| not active for `>= NVS_INACTIVE_SECS` (900) | ALERT (a RAISE): a P0-tier page to the host arranger, plus the lnotify notice; at most once per `NVS_REALERT_SECS` (3600) within an episode |
| a raise inside `NVS_RAISE_COOLDOWN_SECS` (3600) of this unit's previous raise, even across a recovery (flapping) | held: recorded ONCE per episode as a notice only. No page, no escalate. The `unit.<u>.last_raise` clock survives recovery. |
| page undeliverable or no arranger resolves | `operator_escalate` at once; the notice body says the page FAILED; **this tick exits 3** (after the deaf path has run) |
| still not active `NVS_ESCALATE_SECS` (1800) after a DELIVERED page | `operator_escalate`, **once per episode** |
| active again after an alert | one "recovered" **notice** (same identity key). A recovery is never paged and never escalated. Episode state is cleared; `last_raise` stays. |
| active again before the threshold | logged only |

### The tick is bounded (N2)

The deaf path below re-opens its not_receiving window when two confirmations are more than 180 s apart, so a hung
notify call must never hold a tick. Three bounds apply:

- **Every external call has its own timeout.** `systemctl` `NVS_SYSTEMCTL_TIMEOUT` (2 s per call). lnotify
  `NVS_LNOTIFY_TIMEOUT` (5 s). The resolve and page child `NVS_PAGE_TIMEOUT` (12 s), with each curl inside it at
  `NVS_CURL_MAX_TIME` (5 s). `operator_escalate` `NVS_ESCALATE_TIMEOUT` (12 s).
- **At most `NVS_MAX_ALERTS_PER_TICK` (1) paging alerts per tick.** A raise or escalate beyond that is deferred to
  the next tick and logged. Notices (recovery, held raise) are not capped; each is bounded at 5 s.
- **The unit carries `TimeoutStartSec=55`** (`systemd/*/nexus-vault-sync-supervisor.service.d/20-tick-bound.conf`).
  The worst case, recomputed per #33 gate C2:

  | Part | Seconds |
  |---|---|
  | `systemctl`, 2 units x 2 calls x 2 s | 8 |
  | the one paging unit: page 12 + lnotify 5 + escalate 12 | 29 |
  | the other unit's single notice | 5 |
  | the deaf path's health fetch (`curl -m 10`) and parse | ~11 |
  | **total** | **~53 (under 55)** |

  The deaf path's own `systemctl show` / `restart` run only in its restart branch, and TimeoutStartSec is their
  bound. A killed `systemctl restart` client does not cancel the queued job.

### State files are atomic and validated (TKT-8b4f38a0, #33 gate C1)

Every state file holds one epoch. That covers `deaf_since`, `last_confirmed` and `last_restart`, and every
`unit.<u>.*` file.

- **Writes** go to a temp file in the same dir, are fsync'd (coreutils `sync FILE`), then renamed into place
  (`mv -f`). A reader sees the old value or the new one, never a torn one.
- **At the start of every tick:**
  - any state file that is not digits only is logged `STATE CORRUPT (LOUD)` and removed. That covers empty files,
    letters, NUL bytes and an interior newline. Removing it means treated as absent: a window re-opens and a clock
    restarts.
  - leftover `*.tmp.*` files from a killed write are removed.

  Before this, an empty file from a mid-write kill made `$((NOW-$(cat f)))` abort every later tick with rc 1, the
  deaf path included.

The deaf path's only edits are its three state writes. The tests map them back and get the imported sha256.

### Flapping (#33 gate C4)

A unit that goes down, recovers and goes down again within the hour raises (pages) **once**. The
`unit.<u>.last_raise` clock survives recovery. Every later episode inside `NVS_RAISE_COOLDOWN_SECS` (3600):

- is recorded ONCE as a notice ("raise held by the cross-episode cooldown"). There is no page and no escalate;
- has its `last_alert` SEEDED from `last_raise` (C3), so its first possible raise is `last_raise + 3600`.

The same holds after a supervisor restart, because the state lives in files.

Each recovery is a notice. So a unit flapping every few minutes produces one page per hour plus a ledger trail of
notices on one identity key. The operator hears about it only through escalate (b), when a raised episode is still
inactive 30 min after its page.

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

**Stale role markers (N3).** A released marker is moved to `~/.claude/backups/role-markers/` by `pb-role-marker.py`
and is never read. A marker can still linger in `runtime/` when its seat died without a release: a crash, or a
manual edit. Nothing here can tell that a csid is dead, because the marker is a flag, not a liveness proof. So:

- **Validation** rejects only malformed markers: a non-uuid filename, a path escaping the runtime dir, or content
  naming another csid.
- **Newest `claimed_at` wins** when more than one valid marker is live, and the choice is logged LOUD. A successor's
  marker (`handover` or `claim`) is newer than a lingering predecessor's, so the live seat wins.
- **If the only marker left is a dead seat's**, the page goes to that dead inbox. The conductor accepts it (2xx), so
  the script cannot see the failure. The backstop is escalate (b): still inactive 30 min after a "delivered" page,
  `operator_escalate` fires once.

So a stale-only marker costs at most 30 min before the operator hears. The fix for a stale marker is
`pb-role-marker.py release <csid> --force --reason ...`; this script never deletes markers.

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
| `NVS_RAISE_COOLDOWN_SECS` | `3600` (cross-episode) |
| `NVS_SYSTEMCTL_TIMEOUT` / `NVS_LNOTIFY_TIMEOUT` / `NVS_PAGE_TIMEOUT` / `NVS_ESCALATE_TIMEOUT` / `NVS_CURL_MAX_TIME` | `2` / `5` / `12` / `12` / `5` |
| `NVS_MAX_ALERTS_PER_TICK` | `1` |
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

`bash scripts/supervisor/test-supervisor.sh` runs in about 12 s with bash and python3 only. It uses PATH shims
for systemctl, curl, lnotify, logger, `date +%s` and hostname, plus a stub resolver and a stub `operator_escalate.py`
under a temp HOME. Nothing touches a real unit, the journal, the network, the rail, an inbox or the operator.
CI runs it on ubuntu.
