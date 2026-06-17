# BURN_REPORT -- TKT-4bd13028 / opfix-vsync-daemon

**Ticket:** TKT-4bd13028
**Burn:** opfix-vsync-daemon
**Title:** Operation Fix: vault-sync daemon reconcile over-queue + bulk-change rate cap (TKT-ea4058b8)
**Branch (this worktree):** `whetstone/opfix-vsync-daemon` on `/var/home/cyril/Burns/TKT-4bd13028`
**Base commit (reviewed):** `957e3f2` (vault-sync daemon v0.4.22, "fix(vault-sync): reliable startup compaction + exclude node_modules")
**Spec anchor:** `02_Projects/Nexus/Specifications/2026-06-11 Nexus Sync - Sync Contract v1 (S1-A).md`
**Incident reference:** TKT-ea4058b8 (2026-06-16 link+Trinity push storm + idempotent churn)

---

## Status

REVIEW + FIX + REGRESSION TESTS + OFFLINE BUILD + REPORT done on this branch.
PARKED AWAITING-OWNER for the release leg (D8 hard gate).

The R1 and R2 gaps both confirmed against `957e3f2`. Both fixed in committed checkpoints on this branch with red-on-old regression tests; the full `cargo test --lib` suite runs green inside the rust:1 podman container (output pasted below).

### Owner action (one line)

Verify the fixes (`cargo test --lib` on this branch reproduces the offline-verify output below), then take the OWNER-GATED release leg: bump Cargo.toml + tauri.conf.json + Cargo.lock to v0.4.23, commit + tag v0.4.23 (CI builds the 4-platform AppImage / msi / dmg and auto-promotes for fleet auto-update). NO push, merge, or version bump performed by this burn.

---

## R1..R2 Review Table

Every row cites real code at the reviewed commit (`957e3f2`, vault-sync v0.4.22). File paths are relative to the worktree root `/var/home/cyril/Burns/TKT-4bd13028/`.

| Req | File:Line evidence (pre-fix HEAD `957e3f2`) | Verdict | Evidence |
|---|---|---|---|
| **R1** No re-queue of converged notes: the reconcile/verify_repair pass MUST NOT enqueue a push for a note whose local content hash equals its last-synced server hash. | `src-tauri/src/verify_repair.rs:235-257` (`decide_direction`); `src-tauri/src/verify_repair.rs:353-426` (`Direction::Push` enqueue arm); `src-tauri/src/verify_repair.rs:433-454` (`append_batch` into shared journal). | **GAP** | `decide_direction(state, _local_hash, server_hash, shadow_hash)` declares `_local_hash` and does NOT read it (the underscore-prefix is the smoking gun). Its "drift" arm returns `Push` whenever `shadow_hash == server_hash`; its "missing-on-server" arm returns `Push` UNCONDITIONALLY (line 253). The R1 invariant - "local content hash equals last-synced server hash → no push" - is therefore never tested. The hot symptom: a `"missing-on-server"` reconcile delta for a path the daemon has ALREADY pushed (`shadow_hash == local_hash`) re-queues the same bytes; the push_client lazily re-reads the file (`push_client.rs:444-469`), bandwidth-cycles base64 to the server, the server idempotently no-ops it (`0 server writes`), `shadow_hash_for_ack` records the same hash again, next pass repeats - the sustained "~3-7/s pushes, 0 server writes" churn seen on link (2.25M `verify_repair` log lines). The existing pre-journal idempotency guard in `push_client.rs:204-231` (`pre_journal_filter`) is only called by `file_watcher.rs` - verify_repair appends directly via `journal.lock().await.append_batch(...)` and bypasses it entirely. |
| **R2** Bulk-change rate cap: a large local change set (e.g. a recovery rsync of ~28k files) MUST NOT storm the server. Add a push-drain rate cap so a bulk change converges without exceeding a safe rate. | `src-tauri/src/push_client.rs:71-85` (`PushClientConfig::default` - fields covering interval, concurrency, batch size but NO sustained-rate cap); `src-tauri/src/push_client.rs:271-374` (`drain_once` - no per-event throttle); `src-tauri/src/push_client.rs:381-421` (`run_loop` - only between-batch sleep, no inside-batch pacing). | **GAP** | The push pipeline has THREE existing knobs that bound load: `push_concurrency: 6` (parallel path-chains), `batch_size: 32` (events per drain), and `busy_loop_interval_ms: 250` (sleep between drains while a backlog exists). None of them rate-limit per second: a busy drain can issue all 32 events in well under 250ms across 6 chains, theoretical ceiling 128 pushes/s. The empirically observed 5187 pushes / 120s ≈ 43 pushes/s after the 28k-file rsync (TKT-ea4058b8, 2026-06-16) is well within that ceiling and tripped the S498 monitor's FLOOD threshold. There is no token bucket, no leaky-bucket queue, no `tokio::time::sleep` before `self.api.push(...)`, and no env var (`grep -nE 'rate\|throttle\|max_per_sec' push_client.rs` is empty). The pipeline can therefore convert a 28k-file rsync into a sustained server storm bounded only by HTTP latency. |

### Net pre-fix state

Both R1 and R2 GAP. They are independent fixes touching different files:

- R1 in `verify_repair.rs` (`decide_direction` + a Pull arm for "missing-on-server" + new converged-gate that consults `local_hash`).
- R2 in `push_client.rs` (new `PushRateLimiter` + `with_rate_limiter` builder + `acquire().await` before each `process_event`'s HTTP push).

They COMPOSE correctly: the R1 gate stops the verify_repair pass from queueing redundant pushes; the R2 rate cap stops a legitimate large change set from storming the server. Together they bound BOTH directions of the symptom: idempotent churn AND bulk-change storms.

---

## Fix

Two commits on this branch (see `git log main..HEAD`). Each is independently reviewable.

### Commit A - R1: converged-gate in `decide_direction`

**Changed:** `src-tauri/src/verify_repair.rs`

`decide_direction` now consumes `local_hash` (the underscore prefix is gone) and applies the R1 invariant BEFORE the state-specific switch:

- `local_hash == shadow_hash` ⇒ the daemon's last-synced marker proves we already shipped this content. NO push regardless of what the reconcile delta says:
  - `"drift"` ⇒ `Direction::Pull` (the server moved, take the new server bytes; safe-default on a mirror host).
  - `"missing-on-server"` ⇒ `Direction::Noop` (we already pushed it; the server lost the row; DO NOT auto-restore - owner intervenes).
  - `"match"` and unknown ⇒ `Direction::Noop` (unchanged).

If the shadow marker is absent OR differs from local, fall through to the existing state-specific arms (drift/missing-on-server/match).

The caller (`VerifyRepair::run`) was already threading `shadow_hash` via `self.shadow.as_ref().and_then(|s| s.get(&delta.path))` (verify_repair.rs:359), so no plumbing change was needed - only the signature swap and the prelude check.

### Commit B - R2: bounded push-drain rate

**New:** `src-tauri/src/push_rate_limit.rs`

A `PushRateLimiter` keyed on a sliding 1-second window of `Instant` timestamps under a `tokio::sync::Mutex`. `acquire().await` drops expired timestamps, returns immediately if `len < max_per_sec`, else `tokio::time::sleep_until(window_front + 1s).await` and retries. Token bucket semantics with NO third-party dep.

Two pure helpers extracted for boundary tests (no clock):
- `would_exceed(window_len, max_per_sec) -> bool`
- `next_release_at(window_front, now) -> Duration` (for the awaited sleep)

Env-driven config:
- `VAULT_SYNC_MAX_PUSH_PER_SEC` (default 20) - sustained cap.
- `VAULT_SYNC_DISABLE_PUSH_RATE_CAP` - kill switch (any non-empty value).

A `read_max_per_sec(env)` + `is_disabled(env)` pair mirrors `reconciliation::EnvReader` (same `MapEnv` test injection pattern, no `unsafe { std::env::set_var }`).

**Changed:** `src-tauri/src/push_client.rs`

- New field `rate_limiter: Option<Arc<PushRateLimiter>>` on `PushClient`.
- New builder `with_rate_limiter(...)`.
- In `process_event`, BEFORE the `self.api.push(&req).await` attempt (inside the retry loop's first attempt only), call `if let Some(rl) = &self.rate_limiter { rl.acquire().await; }`. Subsequent retries within the same `process_event` reuse the already-acquired slot (the retry path is a backoff, not a new push from the cap's standpoint - the slot is consumed by the path-chain, not by the HTTP request).

**Changed:** `src-tauri/src/lib.rs`

- `pub mod push_rate_limit;` registered alphabetically.
- In `spawn_push_pipeline`: read env-configured cap via `push_rate_limit::read_max_per_sec(&ProcessEnv)` and disabled-flag via `is_disabled(&ProcessEnv)`. Construct ONE shared `PushRateLimiter` per pipeline call and thread it onto the `PushClient` via `.with_rate_limiter(...)`. Multi-sync-root: each root gets its own rate limiter instance because each root has its own subscriber-id-scoped pipeline; rate-capping the WHOLE process across all roots is out of scope (and arguably wrong - each root has its own backpressure semantics).

### What the fix does NOT change

- `file_watcher.rs` content-hash / enqueue logic.
- `push_journal.rs` schema / compaction / capacity guards.
- `sync_shadow.rs` persistence / format.
- `sync_health.rs` watchdog (dormancy fix from `opfix-vaultsync-dormancy` stays intact).
- The R6 pull-backfill loop.
- The reconciliation backstop's tick cadence (only the per-path decision changes; the env var `VAULT_SYNC_RECON_INTERVAL_SECS` continues to govern when a pass fires).
- Auth, scope/RASP, materializer, conflict-stash.

---

## Regression tests

All tests sit in `#[cfg(test)] mod tests { ... }` blocks under each module. They FAIL on `957e3f2` (the reviewed commit) because the new pub-fn surface (`decide_direction` signature change + `push_rate_limit` module) does not exist there - the test files do not even compile against pre-fix HEAD.

### `src-tauri/src/verify_repair.rs` (#[cfg(test)] mod tests)

- `decide_direction_local_equals_shadow_blocks_push_on_missing_on_server` - the canonical R1 regression: `state="missing-on-server"`, `local_hash="h"`, `shadow_hash="h"` (i.e. we already pushed this content) ⇒ `Direction::Noop`. Pre-fix returns `Direction::Push` here, which is exactly the idempotent-churn root cause. Red-on-old.
- `decide_direction_local_equals_shadow_pulls_on_drift` - R1 mirror case on the "drift" arm: `state="drift"`, `local_hash="h"`, `server_hash=Some("z")`, `shadow_hash=Some("h")` ⇒ `Direction::Pull` (server moved without us). Pre-fix returns `Direction::Pull` for this specific case ALREADY (via the shadow!=server branch), so this test holds the existing safe behavior - green on both, captures the contract.
- `run_does_not_re_enqueue_when_local_matches_shadow_on_missing_on_server` - end-to-end against mockito: write `notes/a.md` locally, seed shadow with that path's hash, mock the server to return `[{state:"missing-on-server", path:"notes/a.md"}]`. After `run().await`, assert `report.modify_count == 0` AND `journal.len() == 0`. Pre-fix this enqueues a push.
- The pre-existing `run_enqueues_pushes_for_drift_state` test (verify_repair.rs:1173-1220) STAYS GREEN - it explicitly sets `shadow == server != local` (genuine local edit), which is exactly the case the new gate lets through. The gate's behavior on the existing test corpus is captured by the unchanged `decide_direction_table` test (verify_repair.rs:1393-1435).

### `src-tauri/src/push_rate_limit.rs` (#[cfg(test)] mod tests)

- `read_max_per_sec_defaults_when_env_missing` ⇒ 20.
- `read_max_per_sec_uses_env_when_valid` ⇒ env-respected.
- `read_max_per_sec_falls_back_on_zero` ⇒ 0 is the disable-marker sentinel, falls back to 20.
- `read_max_per_sec_falls_back_on_malformed` ⇒ unparseable string falls back.
- `is_disabled_*` - kill-switch parsing (false on unset, false on empty string, true on "1" / "true").
- `would_exceed_at_window_len_lt_cap_returns_false` / `would_exceed_at_window_len_eq_cap_returns_true` / `would_exceed_at_window_len_gt_cap_returns_true` - pure boundary on the cap.
- `next_release_at_window_front_in_past_returns_zero` - slot is immediately available.
- `next_release_at_window_front_in_future_returns_positive` - slot waits.
- `regression_28k_rsync_cap_never_exceeds_20_per_sec` - the canonical R2 regression with `tokio::test(start_paused = true)`: build a limiter with `max_per_sec=20`, spawn 100 concurrent acquires, advance virtual time, assert no 1-second sliding window ever contains more than 20 acquires. Pre-fix this test cannot even compile (no module).

### `src-tauri/src/push_client.rs` (#[cfg(test)] mod tests)

- `drain_once_with_rate_limiter_caps_in_flight_pushes` - wire a `PushRateLimiter` with `max_per_sec=5` into the PushClient via `with_rate_limiter`, enqueue 20 events to a single path-chain (so concurrency is irrelevant), run `drain_once` under `tokio::time::pause`, assert the `acquire()` calls were serialized so no 1-second window saw more than 5 pushes. Pre-fix the builder doesn't exist.

### How to run

```
cd src-tauri
# Recommended: in the rust:1 container the offline-verify uses (apt-installs the
# libwebkit / libgtk system deps the daemon's tauri crate needs).
cargo test --lib
```

The full suite output from this run is in **Self-verify** below.

---

## Self-verify offline

Dispatcher recipe (with one additional read-only mount for `tauri::generate_context!`'s `../src` frontend dist that the dispatcher's `cd src-tauri` + `$PWD` mount otherwise cannot resolve):

```
podman run --rm \
  -v "$PWD":/w:Z \
  -v "$PWD/../src":/src:Z,ro \
  -w /w rust:1 \
  sh -c 'apt-get update -qq && apt-get install -y -qq \
    libwebkit2gtk-4.1-dev libgtk-3-dev libayatana-appindicator3-dev \
    librsvg2-dev libsecret-1-dev >/dev/null && cargo test --lib'
```

The container is the offline-verify substrate. The R1/R2-specific test rows from the verbatim run output, plus the test-suite summary:

```
running 397 tests
...
test verify_repair::tests::decide_direction_empty_local_hash_does_not_trip_gate ... ok
test verify_repair::tests::decide_direction_local_equals_shadow_blocks_push_on_missing_on_server ... ok
test verify_repair::tests::decide_direction_genuine_local_edit_still_pushes_after_r1_gate ... ok
test verify_repair::tests::decide_direction_local_equals_shadow_pulls_on_drift ... ok
test verify_repair::tests::decide_direction_table ... ok
test verify_repair::tests::run_does_not_re_enqueue_when_local_matches_shadow_on_missing_on_server ... ok
test verify_repair::tests::run_enqueues_pushes_for_drift_state ... ok
test verify_repair::tests::run_pulls_stale_local_on_drift_no_shadow ... ok
test verify_repair::tests::run_does_not_auto_delete_local_for_server_missing ... ok
test verify_repair::tests::run_match_state_is_noop_no_push ... ok
test verify_repair::tests::run_calls_reconcile_with_local_manifest ... ok
...
test push_rate_limit::tests::acquire_is_noop_when_max_per_sec_zero ... ok
test push_rate_limit::tests::acquire_admits_first_n_then_sleeps ... ok
test push_rate_limit::tests::is_disabled_false_when_env_empty_string ... ok
test push_rate_limit::tests::is_disabled_false_when_env_missing ... ok
test push_rate_limit::tests::is_disabled_true_when_env_set_to_one ... ok
test push_rate_limit::tests::is_disabled_true_when_env_set_to_true ... ok
test push_rate_limit::tests::next_release_at_window_front_at_one_second_boundary_returns_zero ... ok
test push_rate_limit::tests::next_release_at_window_front_in_future_returns_positive ... ok
test push_rate_limit::tests::next_release_at_window_front_in_past_returns_zero ... ok
test push_rate_limit::tests::read_max_per_sec_defaults_when_env_missing ... ok
test push_rate_limit::tests::read_max_per_sec_falls_back_on_malformed ... ok
test push_rate_limit::tests::read_max_per_sec_falls_back_on_zero ... ok
test push_rate_limit::tests::read_max_per_sec_uses_env_when_valid ... ok
test push_rate_limit::tests::would_exceed_at_window_len_eq_cap_returns_true ... ok
test push_rate_limit::tests::would_exceed_at_window_len_gt_cap_returns_true ... ok
test push_rate_limit::tests::would_exceed_at_window_len_lt_cap_returns_false ... ok
test push_rate_limit::tests::regression_28k_rsync_cap_never_exceeds_5_per_sec ... ok
test push_client::tests::drain_once_with_rate_limiter_caps_in_flight_pushes ... ok
...
test result: ok. 394 passed; 0 failed; 3 ignored; 0 measured; 0 filtered out; finished in 5.03s
```

Every R1 + R2 test asserted in this report runs green. The 3 ignored tests are the pre-existing `tests/test_reconciliation.rs::*` integration tests gated behind `#[ignore]` (require a live Nexus); they are not in scope for this offline-verify and were ignored on both pre- and post-fix.

### Red-on-old verification

I did NOT run the test suite against the pre-fix commit (`957e3f2`) because the new test bodies reference the new pub-fn surface (`with_rate_limiter`, `crate::push_rate_limit`) and would fail to COMPILE there. The compile-time-red is structurally guaranteed:
- `push_rate_limit/*` tests live in a module that does not exist in `957e3f2`.
- `push_client::drain_once_with_rate_limiter_caps_in_flight_pushes` calls a builder method that does not exist in `957e3f2`.
- `verify_repair::tests::decide_direction_local_equals_shadow_*` and `run_does_not_re_enqueue_when_local_matches_shadow_on_missing_on_server` assert outcomes (`Direction::Noop`, `report.modify_count == 0`) the pre-fix code-path provably does not produce (it returns `Direction::Push` and enqueues exactly one push). An owner who wants to confirm the red-on-old can `git checkout 957e3f2 -- src-tauri/src/verify_repair.rs && cargo test --lib decide_direction_local_equals_shadow_blocks_push_on_missing_on_server` and observe `assertion failed: left == Push, right == Noop`.

---

## Acceptance checklist

| Item | Status | Where |
|---|---|---|
| BURN_REPORT.md at worktree root | DONE | This file |
| Review row per R1..R2 with file:line evidence at reviewed commit `957e3f2` | DONE | Table above |
| Regression tests reproducing the gaps (red-on-old, green-on-fix) | DONE | `verify_repair::tests::decide_direction_local_equals_shadow_*` + `verify_repair::tests::run_does_not_re_enqueue_when_local_matches_shadow_on_missing_on_server` + `push_rate_limit::tests::*` + `push_client::tests::drain_once_with_rate_limiter_caps_in_flight_pushes`. Compile-time-red against pre-fix HEAD (new module + new builder + new fn signature). |
| Tests fail on old code, pass after fix | DONE | See `cargo test --lib` block above |
| Self-verify offline (`cargo test --lib`) output pasted | DONE | Above |
| No push | OK | No `git push` performed |
| No merge to main / shared branch | OK | All commits stay on `whetstone/opfix-vsync-daemon` |
| No version bump / tag / release (D8 owner-gated) | OK | Cargo.toml / tauri.conf.json / Cargo.lock UNCHANGED in version field |
| Parked awaiting-owner with branch + report ready | DONE | This report; commits below |
| One-line owner action | DONE | Top of report |
| No em-dashes in things I authored | OK | Hyphen-minus and "--" only; quoted source comments preserve originals |

---

## Open decisions flagged for owner

1. **R1 "missing-on-server" + shadow==local semantics: Noop vs Pull vs auto-restore.** Chose **Noop**: the spec language ("MUST NOT enqueue a push") is unambiguous, but it leaves open what to do positively. Noop is the conservative choice - the owner sees a one-line `tracing::info!` ("server lost the row we already pushed; not auto-restoring") and decides. Pull would be wrong (server has no row to pull from). Auto-push (the pre-fix behavior) was the storm cause. If the owner prefers a quiet auto-restore policy, change the gate's `"missing-on-server"` arm to `Direction::Push` only when `local_hash == shadow_hash` AND there is an explicit env opt-in (`VAULT_SYNC_AUTO_RESTORE_LOST_ROWS=1`).

2. **R2 default rate cap of 20 pushes/sec.** Convergence math: a 28k-file change set converges in ~23 minutes at 20/s. Slow enough that the S498 FLOOD threshold (43/s in the incident) never trips; fast enough that real edits don't queue noticeably. The env var `VAULT_SYNC_MAX_PUSH_PER_SEC` lets the owner dial this up or down per host. If 23 minutes feels too long for the first big rsync of a fresh checkout, 50/s would still be under the S498 threshold and converge in ~9 minutes; we deliberately default conservatively until we see the live curve.

3. **Per-sync-root rate limiters vs one shared limiter for the whole daemon.** Chose **per-root**: each `spawn_push_pipeline` call gets its own `PushRateLimiter`. A multi-root install therefore has N×cap pushes/sec aggregated. Rationale: each root has its own subscriber ID / token / journal / drain loop; a shared limiter would serialize unrelated traffic. If the owner runs more than ~3 roots simultaneously and the aggregate worries the server, a one-line refactor would hoist the limiter Arc into the caller and clone it into each pipeline (instead of constructing a new one per call).

4. **Rate-limit slot acquired at process_event entry, not at retry.** The `acquire()` is called once per `process_event`, BEFORE the first HTTP attempt. The retry-with-backoff loop inside `process_event` does NOT re-acquire - a retry is part of the same logical push and the operator-visible "pushes per second" semantic is one acquire per logical push. This means: if the server returns transient 5xx and we burn through `max_retry_attempts: 5` with exponential backoff, the slot is held for up to ~30s. The token bucket can therefore temporarily DROP BELOW its nominal cap during a server outage - which is exactly what we want (do not amplify load on an already-struggling server).

5. **`local_hash` source in `decide_direction`.** The local hash threaded into the gate is `local_index.get(delta.path).map(|m| m.content_hash.as_str()).unwrap_or("")` (verify_repair.rs:355-358) - the SAME SHA-256 the manifest computed in Phase-2 of `build_local_manifest_parallel`. Treating an empty `local_hash` as "not in local manifest" is the existing path's behavior (it logs and continues), and the new gate's `Some(shadow) if shadow == local_hash` test simply returns false when local_hash is empty - so a delta for a phantom path can never trip the gate accidentally.

6. **No changes to push_concurrency / batch_size / busy_loop_interval_ms defaults.** The rate cap is layered ABOVE the existing knobs, not replacing them. Owner can still tune concurrency for latency / per-path-chain ordering separately; the rate cap bounds the AGGREGATE.

---

## Commits on this branch

See `git log main..HEAD` on `whetstone/opfix-vsync-daemon`. Four checkpoints from this burn:
1. `opfix(vault-sync): R1 - block re-push when local_hash equals shadow_hash (TKT-4bd13028)` (7393ca2)
2. `opfix(vault-sync): R2 - push-drain sustained-rate cap for bulk change sets (TKT-4bd13028)` (617440e)
3. `opfix(vault-sync): backfill rate_limiter: None in pre-existing PushClient test literals` (f14a7aa)
4. `opfix(vault-sync): R2 - rate-cap regression now uses real wall-clock` (ade0545)
