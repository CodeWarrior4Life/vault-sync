# Trinity LaunchAgent (TKT-a6e5c227)

- `com.lattice.vaultsync-daemon.plist` is the PROPOSED agent: the daemon binary run directly with `--silent`,
  `KeepAlive {SuccessfulExit:false}`, and `VAULT_SYNC_SUPERVISED=1`.
- `install-trinity-agent.sh` installs it and proves the result. Run it ONLY under the release/placement GO. Nothing
  runs it automatically.
- `test-install-trinity-agent.sh` is its shim test suite. It is Linux-runnable and CI runs it on ubuntu.

## Why an install script

The vault (non-managed) instance is guarded by the tauri single-instance SOCKET, not by the flock(2) config-dir lock,
which guards MANAGED instances only. The first copy to start wins, and a later copy exits 0. So a copy started first
from Finder, Login Items or open-at-login makes the agent's copy exit 0. launchd does not relaunch a 0, so the daemon
runs unsupervised.

The script parks the other launch routes, removes the Login Item, installs and bootstraps the agent, and then
requires a **receipt**:

- exactly ONE vault-mode daemon pid;
- its parent is launchd (ppid 1);
- it is the job's pid in `launchctl print`;
- the job environment has `VAULT_SYNC_SUPERVISED => 1`.

## Matching rules (#29 follow-up P1, P2)

- **Agents: exact paths only, never a substring.** An agent competes only if its `Program`, or else its
  `ProgramArguments[0]`, EQUALS the main binary, and its `EnvironmentVariables` do not set `NEXUS_SYNC_CONFIG_DIR`.
  - The main binary is set by `--main-binary`, default `/Applications/Nexus Vault Sync.app/Contents/MacOS/vault-sync-daemon`.
  - These are not parked: a `.app.bak` copy, a `pkill -x vault-sync-daemon` agent, a wrapper that mentions the app,
    the managed (memory) instance, and the `--memory-binary` instance.
- **Processes: full command line, not `pgrep -x`.** macOS truncates the process name to 16 characters (MAXCOMLEN),
  and `vault-sync-daemon` has 17, so `pgrep -x vault-sync-daemon` can miss the daemon.
  - A running copy is a line of `ps -axo pid=,command=` that starts with the exact main-binary path, and whose
    environment (`ps -E`) has no `NEXUS_SYNC_CONFIG_DIR`.
  - **MEASURE both output shapes on Trinity at the canary** before trusting the receipt.

## Parking is reversible (P3)

- Parked plists are moved with `mv -n`, so nothing is clobbered, into `<agents-dir>.vault-sync-parked/`.
- Each move is recorded in `manifest-<timestamp>.tsv`, and the script prints the exact restore commands.
- `install-trinity-agent.sh --restore <manifest>` moves each plist back and bootstraps it. It refuses (exit 25) when
  the original path already exists.

## The script never kills (P4)

A vault-mode copy still running after the agent is booted out is listed by pid. The run then stops with exit 24 and
asks the operator to quit it from its tray menu and rerun. Only `--test-kill-cmd`, used by the tests, stops a process.

## The pre-supervisor tray window (P5)

Between the old app's quit and launchd's first start of the agent, a user click on the app (Finder, Dock or
Spotlight) starts an UNSUPERVISED copy. That copy wins the socket, and the agent's copy exits 0.

The receipt catches it:

- the only copy found is not the job's pid: exit 21, and the message names this window;
- or two copies run: exit 20.

The remedy is to quit that copy, run `launchctl kickstart -k gui/$(id -u)/com.lattice.vaultsync-daemon`, then run
`--verify-only`.

## Exit codes

| Code | Meaning |
|---|---|
| 0 | OK |
| 2 | usage, not macOS, plist missing, or a parked file of the same name already exists |
| 20 | more than one daemon pid |
| 21 | parent not launchd, or not the job's pid |
| 22 | no `VAULT_SYNC_SUPERVISED => 1` in the job environment |
| 23 | none running |
| 24 | a vault-mode copy is still running |
| 25 | restore blocked |
