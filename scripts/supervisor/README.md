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

Gate ruling LSEC-9325-B6D8-O5-ONFAILURE-RULING, O5-2 ("No Silent Holds"): before any host switches its
units to `Restart=on-failure`, a vault-sync unit that is not active for more than 15 min must alert.
The not_receiving logic cannot see that case: a stopped daemon reports nothing, and that reads as
"inconclusive". So the supervisor now runs one extra check each tick, **before** and independent of
the not_receiving logic. The not_receiving logic is unchanged byte for byte: the tests strip the
O5-2a block and get the imported sha256 back, and they run a differential against the pinned
imported copy.

For each watched unit:

| State | Action |
|---|---|
| `LoadState=not-found` (the unit does not exist here, e.g. icarus has no `-memory`) | skipped; any stale state for it is cleared |
| `systemctl` unreadable (empty answer) | inconclusive: no state change, no alert |
| not active (`inactive`, `failed`, `activating`, ...; a masked unit counts too) | `unit.<u>.inactive_since` is written at the first such tick |
| not active for `>= NVS_INACTIVE_SECS` | ONE alert; re-alert at most once per `NVS_REALERT_SECS` while it stays not active |
| active again after an alert | one "recovered" emit on the same identity key; state cleared |
| active again before the threshold | logged, no emit; state cleared |

The check only alerts. It never starts, stops or restarts a unit. It never exits the script, so the
not_receiving check always runs after it.

| Env | Default | Meaning |
|---|---|---|
| `NVS_WATCH_UNITS` | `nexus-vault-sync.service nexus-vault-sync-memory.service` | units to watch (space-separated) |
| `NVS_INACTIVE_SECS` | `900` | not-active time before the first alert |
| `NVS_REALERT_SECS` | `3600` | minimum gap between alerts for one unit |
| `NVS_LNOTIFY` | `lnotify` on PATH, else `~/.local/bin/lnotify` | the lattice-notify CLI |
| `NVS_PAGE_CSID` | *(none)* | the arranger's FULL conductor session id. If unset, the page is skipped and logged loudly (`PAGE SKIPPED (LOUD)`). A value that is not a full uuid is refused the same way. |
| `NVS_PAGE_KEY_NAME` | `CONDUCTOR_SERVICE_KEY_NVS_SUPERVISOR` | the conductor service key NAME used for the page |
| `NVS_PAGE_KEY_FILE` | `~/whetstone/.env` | where that NAME is looked up |
| `NVS_PAGE_URL` | `https://nexus.obsidian-inc.com/api/conductor/coord/emit` | the conductor emit route |

### The two alert legs

1. **lnotify, infra rail, claim `notice`.** The command is `lnotify legacy --principal <host>.nexus-vault-sync-supervisor@lattice --app nexus-vault-sync --venture lattice --event-type unit.stopped|unit.recovered --claim notice --identity-key nexus-vault-sync.unit-inactive:<host>:<unit>`. `notice` needs no grant, so it cannot be clamped. It routes to `nexus_vault_note` and the ledger only. `nexus_nudge` (the interruptive operator surface) accepts only `decision` and `block`, so this alert can **never** reach the main Cody chat. No coalesce-count promotion threshold is registered for this principal, so a repeat stays `notice`. The identity key is per host+unit, so repeats bump one ledger fact (`coalesce_count`) instead of opening new threads.
2. **Conductor page to the arranger.** `POST /api/conductor/coord/emit` with `{"kind":"talk","session_id":<NVS_PAGE_CSID>,"body":{from,topic,detail}}`, the route the fleet's scripts use (`~/.claude/scripts/emit-fleet`, `myaccess_monitor.py`). It authenticates with a named CONDUCTOR service key, never `NEXUS_API_KEY`, which is the operator. The key is read by NAME inside python and reaches curl on stdin (`-K -`), never argv, env or stdout. If the key is missing, the page is skipped and the log names the provisioning gap.

## Tests

`bash scripts/supervisor/test-supervisor.sh` runs in about 6 s with bash and python3 only. Every external
command (systemctl, curl, lnotify, logger, `date +%s`, hostname) is a PATH shim, so it touches no real unit,
journal, network or notify rail. CI runs it on ubuntu.
