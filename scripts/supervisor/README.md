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
