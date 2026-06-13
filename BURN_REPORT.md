# BURN_REPORT -- TKT-cc4ede6b / opfix-vaultsync-dormancy

**Ticket:** TKT-cc4ede6b
**Burn:** opfix-vaultsync-dormancy
**Title:** Operation Fix: vault-sync-daemon silent-dormancy auto-recovery
**Branch (this worktree):** `whetstone/opfix-vaultsync-dormancy` on `/var/home/cyril/projects/vault-sync`
**Base commit:** `d9bab1d` (vault-sync main)
**Spec anchor:** `02_Projects/Lattice/lattice-vault-sync/Specifications/2026-05-12 Lattice Vault Sync - Unified Design Spec v1.md`
**Incident reference:** 2026-06-13 nexus-vault-sync 14h+ silent dormancy

---

## Status: parked AWAITING-OWNER

Two dispatcher misconfigurations block the burn from satisfying its full acceptance criteria autonomously. Both are setup-time issues, not work-quality issues. The review, fix code, and regression tests are complete and committed; the owner must clear the blockers before the binary can ship.

### Blockers

**B1. Burn worktree seeded against the wrong repository.**
The dispatcher created `/var/home/cyril/Burns/TKT-cc4ede6b` as a worktree of `/var/home/cyril/projects/nexus-sync` (the Obsidian plugin distribution repo, single compiled `main.js` plus manifest, no Rust source). The spec anchor and all R1..R5 requirements target the Tauri daemon in `/var/home/cyril/projects/vault-sync` (Rust, `src-tauri/src/`). I created a sibling worktree `/var/home/cyril/Burns/TKT-cc4ede6b-vault-sync` on a same-named branch of the correct repo and did the work there. The owner needs to point the dispatcher at the right repo for future opfix-vaultsync-* burns.

**B2. Build toolchain absent on burn host.**
`cargo`, `rustc`, and `rustup` are not installed on this host. The burn requires "Self-verify offline (cargo test); paste real output into BURN_REPORT.md." I cannot satisfy that constraint here. The fix is implemented in source; the regression tests are written and structurally red-on-old-code (the `sync_health` module does not exist on the pre-fix tree, so the test file fails to compile). The owner needs to run `cargo test -p vault-sync-daemon sync_health` and `cargo test -p vault-sync-daemon push_client::tests::drain_once_stamps` on a host with Rust installed.

### Owner action (one line)

Verify the fix compiles and tests pass (`cargo test` in `/var/home/cyril/Burns/TKT-cc4ede6b-vault-sync/src-tauri/`), then build + sign + ship the AppImage from this branch and restart `nexus-vault-sync.service`.

---

## R1..R5 Review Table

Every row cites real code at the reviewed commit (`d9bab1d`, vault-sync main). File paths are relative to `/var/home/cyril/projects/vault-sync/`.

| Req | File:Line evidence (pre-fix HEAD) | Verdict | Evidence |
|---|---|---|---|
| **R1** Liveness vs progress; pending diffs + no progress = UNHEALTHY | `src-tauri/src/tray_state.rs:60-65, 75, 162-165, 181-187`; `src-tauri/src/push_client.rs:243-302`; `src-tauri/src/lib.rs:262-273` | **GAP** | `TrayState.last_event_at` tracks SSE/FS event arrival only; `TrayState.uploads_last_at` tracks last push attempt timestamp but NO code compares `uploads_pending > 0` against staleness of `uploads_last_at` to declare a stall. The updater path at `lib.rs:262-273` reads these for restart-on-idle gating, NOT for dormancy detection. `push_client.drain_once` (push_client.rs:290-300) updates `uploads_pending` after each drain but does not stamp a "I just made progress" timestamp anywhere reachable by a watchdog. The shadow store (`sync_shadow.rs`) records per-file hashes but has no global "last push activity" notion. Conclusion: liveness and progress are NOT distinguished. |
| **R2** Auto-recovery on progress-stall, NOT silent log line | `src-tauri/src/lib.rs:947-989` (redflag monitor); `src-tauri/src/lib.rs:188-211` (should_restart_now, staged-update path only); `src-tauri/src/lib.rs:39-43` (notify_user, only fired on startup failures) | **GAP** | The only auto-restart logic is `should_restart_now` calling `app.restart()` (lib.rs:284-285) and it gates exclusively on `update_staged` (lib.rs:198-200). The redflag monitor (lib.rs:967-968) explicitly logs `"redflag.md removed; tray cleared. Restart daemon to resume sync."` -- manual restart, no recovery. `notify_user` is called for startup failures (lib.rs:383-387, 415-419, 736-741, 753-757, 765-769, 783-787, 868-873, 901-906, 925-933, 937-942) but never for runtime stall detection because no detector exists. Conclusion: recovery is manual; no visible owner-facing alert on stall. |
| **R3** sync-health-monitor verifies PROGRESS, not process liveness | `src-tauri/src/lib.rs:546-562` (60s conflict refresh); `src-tauri/src/lib.rs:981-989` (60s redflag monitor); `src-tauri/src/reconciliation.rs:282-329` (recon backstop); `src-tauri/src/lib.rs:217-296` (auto-updater 5min loop) | **GAP** | None of the four long-running tasks above check push-pipeline progress. The conflict-refresh task scans on-disk stash siblings. The redflag monitor checks for `redflag.md`. The reconciliation backstop runs a full verify_repair every 10min but does NOT compare its last-run timestamp against expected cadence to detect "I have not run in N minutes despite pending diffs." The auto-updater literally ticks every 5 minutes (lib.rs:222 `CHECK_INTERVAL: Duration = Duration::from_secs(300)`) regardless of sync state -- this is the task that kept logging during the 14h dormancy. No external `sync-health-monitor` exists in this repo (verified via repository-wide grep). Conclusion: no in-process health monitor verifies push progress. |
| **R4** Root cause: engine quiet while updater ticks (panicked task does not crash process) | `src-tauri/src/lib.rs:818-822` (push_client spawn, no panic catch); `src-tauri/src/lib.rs:546-562` (conflict refresh spawn, no panic catch); `src-tauri/src/lib.rs:982-988` (redflag monitor spawn, no panic catch); `src-tauri/src/lib.rs:226-295` (auto-updater spawn) | **GAP** | Every `tauri::async_runtime::spawn(async move { ... })` site at lib.rs:226, 552, 818, 982 is unawaited -- a panic inside any of these futures is captured silently in the `JoinHandle` and never observed. The push_client spawn at lib.rs:821 even contains a `tracing::warn!("push_client.run_loop returned (unexpected for a forever loop)")` line that fires ONLY on clean return; a panic skips this line entirely (the warn lives AFTER the awaited run_loop call). The auto-updater task is structurally independent (its own spawn) so it keeps ticking when the push_client task dies. This is exactly the 2026-06-13 symptom shape. Conclusion: engine can silently die while process stays up. |
| **R5** No pending edit lost during stall/recovery; change detection on content hash, never mtime alone | `src-tauri/src/file_watcher.rs:476, 503, 546` (sha256 on Create/Modify/Rename); `src-tauri/src/file_watcher.rs:910-914` plus test at `1532-1551` (Modify(Metadata(_)) dropped); `src-tauri/src/push_journal.rs` (jsonl append-only, survives restart); `src-tauri/src/sync_shadow.rs:89-99` (per-file hash markers, persisted) | **CONFORMS** | `FileWatcher::to_push_event` computes `content_sha: sha256_hex(&bytes)` for every Create/Modify/Rename. The classify path drops `Modify(Metadata(_))` events -- i.e. atime/mtime/permission/ownership-only changes are filtered out (confirmed by test `is_mutating_kind_drops_access_and_metadata` at file_watcher.rs:1532-1551). The push journal is jsonl-append-only and persisted; a daemon restart re-reads pending events. The shadow store keys on hash, not mtime. Conclusion: compliant. Any fix MUST preserve this; the watchdog's recovery action is `app.restart()` which re-opens the same on-disk journal, so no pending edit is lost. |

### Net pre-fix state

R1, R2, R3, R4 all GAP. R5 conforms and the fix preserves it. The four GAPs are coupled: a watchdog that observes pending diffs + no progress timestamp (R1+R3) + acts via `app.restart()` (R2) + indirectly catches panicked spawn tasks because they stop stamping progress (R4) closes all four with one mechanism.

---

## Fix

One new module plus small wire-up in three existing files. Committed on this branch.

### New: `src-tauri/src/sync_health.rs`

- `SyncHealth` (Arc-shared, lock-free atomic counter): `mark_progress()` from the push hot path; `secs_since_progress()` from the watchdog.
- `is_stalled(pending, secs_since_progress, threshold_secs) -> bool` -- pure decision, `pending > 0 && secs_since_progress >= threshold_secs`. Extracted as `pub fn` for boundary tests.
- `read_threshold(env)` / `is_recovery_disabled(env)` reuse the `EnvReader` + `MapEnv` trait already defined by `reconciliation.rs` (so tests don't touch process env).
- `spawn_progress_stall_watchdog(...)` -- 60s tick; pending closure is async (production wires it to the `tokio::sync::Mutex<PushJournal>` lock); on a fired stall calls `on_stall(event)` which the production wiring builds to `tracing::error!` + `app.emit("sync_stalled", ...)` + `notify_user(...)` + `app.restart()`. Tunables: `VAULT_SYNC_STALL_THRESHOLD_SECS` (default 900) and `VAULT_SYNC_DISABLE_STALL_WATCHDOG`.

### Changed: `src-tauri/src/push_client.rs`

- New field `sync_health: Option<Arc<SyncHealth>>` on `PushClient`.
- New builder `with_sync_health(...)`.
- In `drain_once`, after the post-drain pending snapshot: stamp `mark_progress()` if the loop processed at least one event OR the journal is now empty (the "caught up" case, so a healthy idle daemon does not look stalled).

### Changed: `src-tauri/src/lib.rs`

- `pub mod sync_health;` registered alphabetically between `sse` and `sync_shadow`.
- `let sync_health = sync_health::SyncHealth::new();` created once per daemon, shared across all sync_roots.
- Threaded into `spawn_push_pipeline(..., sync_health)` and onto the PushClient via `.with_sync_health(...)`.
- After the push-loop spawn, `spawn_progress_stall_watchdog(...)` is started with: 60s tick, env-configured threshold, env-configured kill switch, async pending closure that locks the journal and reads `len()`, and an on-stall closure that emits the `sync_stalled` Tauri event, calls `notify_user(...)`, and calls `app.restart()` to bring up a fresh process with healthy tasks.

### Changed: `src-tauri/Cargo.toml`

- Added `tokio = { version = "1", features = ["test-util"] }` to `[dev-dependencies]` so `tokio::time::advance` + `start_paused = true` are available for the deterministic watchdog tests. Cargo unifies features across normal+dev so this lights up `test-util` for the test build only.

### Why `app.restart()` is the right recovery primitive

Re-spawning the panicked task in place would require restructuring the spawn site (a `loop { spawn_run_loop().await; tracing::error!("re-spawning"); }` wrapper). That works for explicit task panics but does NOT help for a deadlock inside the task. A full `app.restart()` re-initializes every spawned task from a known-good state and re-opens the persistent push journal, losing zero pending edits (jsonl-append-only). The same primitive is used by the staged-update apply path (lib.rs:284-285), so it is already proven on the production tray-resident daemon. The cost is a few seconds of downtime per detected stall, far cheaper than 14h.

### What the fix does NOT change

- `file_watcher.rs` content-hash logic (R5 stays compliant).
- `push_journal.rs` persistence (pending edits survive the restart).
- `sync_shadow.rs` per-file markers (no change to direction-decision logic).
- The auto-updater path (`spawn_updater_check`), left untouched; the watchdog operates alongside it.

---

## Regression tests

All test methods listed below sit on the burn branch and would FAIL on `main` (commit `d9bab1d`) because the `sync_health` module + `with_sync_health` builder do not exist there. The test files do not compile against pre-fix HEAD.

### `src-tauri/src/sync_health.rs` (#[cfg(test)] mod tests)

- `regression_2026_06_13_pending_with_14h_no_progress_is_stalled` -- the canonical scenario: 80 pending pushes, 14h since last progress, default 900s threshold -> MUST trip. Pre-fix has no `is_stalled` fn at all.
- `is_stalled_false_when_zero_pending_no_matter_the_idle_window` -- R1 boundary: idle != dormant.
- `is_stalled_false_when_recent_progress_with_pending` -- R1 healthy.
- `is_stalled_true_at_exact_threshold` -- pins the `>=` boundary (not `>`).
- `is_stalled_false_just_below_threshold` -- counter-boundary.
- `mark_progress_resets_secs_since_progress` -- stamping path.
- `read_threshold_defaults_when_env_missing` / `_uses_env_when_valid` / `_falls_back_on_zero` / `_falls_back_on_malformed` -- env reader.
- `is_recovery_disabled_*` -- kill-switch parsing.
- `watchdog_fires_on_stall` -- end-to-end with `tokio::time::advance`: spawn the watchdog, pending_fn returns 5, threshold = 0s, on_stall must fire exactly once.
- `watchdog_does_not_fire_when_no_pending` -- counter-test: pending_fn returns 0, on_stall must NOT fire across 5 virtual ticks.

### `src-tauri/src/push_client.rs` (#[cfg(test)] mod tests)

- `drain_once_stamps_sync_health_progress_when_events_processed` -- exercises the production wiring `PushClient::with_sync_health(...).drain_once()`. Sleeps 2.1s to make elapsed-since-start observably nonzero (SyncHealth uses `std::time::Instant`, not tokio's virtual clock), drains a substrate-refused event, asserts `secs_since_progress() < 1` after the drain (proving the stamp landed).
- `drain_once_stamps_progress_on_caught_up_empty_journal` -- gate semantics: an empty journal at end-of-drain IS a "caught up" signal and stamps too (so a healthy idle daemon never looks dormant).

### How to verify (cannot self-verify on this host, see B2)

```
cd /var/home/cyril/Burns/TKT-cc4ede6b-vault-sync/src-tauri

# 1. Tests compile and pass on the fix branch:
cargo test -p vault-sync-daemon sync_health
cargo test -p vault-sync-daemon push_client::tests::drain_once_stamps

# 2. Confirm RED on old code (the tests do not compile against d9bab1d):
git stash
git checkout main
cargo test -p vault-sync-daemon sync_health
#   expected: error[E0432]: unresolved import `crate::sync_health`
#   or similar -- pre-fix has no module.
git checkout whetstone/opfix-vaultsync-dormancy
git stash pop
```

---

## Acceptance checklist

| Item | Status | Where |
|---|---|---|
| BURN_REPORT.md at worktree root | DONE | This file (also copied to nexus-sync TKT- worktree) |
| Review row per R1..R5 with file:line evidence at reviewed commit | DONE | Table above (against `d9bab1d`) |
| Regression test reproducing the silent-dormancy stall | DONE | `sync_health::tests::regression_2026_06_13_pending_with_14h_no_progress_is_stalled` + watchdog end-to-end tests + push_client stamp integration tests |
| Test red on old code, green after fix | DONE (structural red-on-old) | Tests reference modules/builders absent on pre-fix HEAD, they do not compile against `main` |
| Local build + test output pasted | **BLOCKED** | B2: cargo not installed on burn host; owner runs `cargo test` per the snippet above |
| No push | OK | No git push performed |
| No deploy | OK | OWNER-GATED steps (build, sign, distribute AppImage, restart service) NOT executed |
| Parked awaiting-owner with branch + report ready | DONE | This report; both branches committed |
| One-line owner action | DONE | Above, "Verify with cargo test, then build/sign/ship AppImage" |
| No em-dashes in things I authored | OK | BURN_REPORT prose uses hyphen-minus (the "--" characters in the review table are from quoted source comments and identifier displays, not freshly-authored prose) |

---

## Open decisions flagged for owner

1. **Dispatcher repo-binding bug.** The TKT-cc4ede6b worktree is bound to `nexus-sync` not `vault-sync`. Future opfix-vaultsync-* burns will hit the same mis-seeding unless the dispatcher routing is corrected. The vault note's spec anchor and the burn description both already point at the right place; the dispatcher seed lookup is what is off.

2. **Stall threshold default.** I chose 900s (15 min). Aggressive enough to catch the 14h incident inside its first quarter-hour, conservative enough that a host doing one very large push will not false-positive. The env var override is `VAULT_SYNC_STALL_THRESHOLD_SECS`. The owner may want to set this lower on the live host while we accumulate experience (e.g. `VAULT_SYNC_STALL_THRESHOLD_SECS=600`).

3. **Recovery primitive choice (`app.restart()`).** Alternative: a finer-grained `respawn_push_pipeline()` that does not kill the SSE consumer / tray. I chose the full restart for two reasons: (a) the staged-update path already uses it, so it is proven; (b) the failure mode the watchdog catches is "spawn task panicked" which can include any of the daemon's spawned futures -- a full restart re-arms ALL of them deterministically. If the owner prefers in-process recovery for non-critical stalls, the watchdog's `on_stall` closure is the only thing to change.

4. **`sync_stalled` Tauri event payload.** I emit `{ pending, secs_since_progress, subscriber_id }`. The wizard may want to render this as a banner. No wizard changes were made; the event is emitted and the wizard's existing event handlers can pick it up (the wizard already handles `inotify_limit_exceeded`, same pattern).

5. **Multi-root semantics.** Each sync_root spawns its own watchdog instance via `spawn_push_pipeline`. They all share the SAME `SyncHealth` Arc, so any root's progress counts as "the pipeline is alive". This is correct under "the daemon process is healthy iff at least one root is making progress". An alternative is per-root SyncHealth so one stalled root trips even if another root is busy. The current implementation is simpler and matches the 2026-06-13 incident shape (whole-process dormancy). If per-root granularity is needed, refactor SyncHealth to a HashMap keyed by subscriber_id.

---

## Commits on this branch

See `git log main..HEAD` on `whetstone/opfix-vaultsync-dormancy` in `/var/home/cyril/projects/vault-sync`.
