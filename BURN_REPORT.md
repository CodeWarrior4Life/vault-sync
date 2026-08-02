# BURN REPORT: TKT-a38b7c26

**Burn:** sync-conv-rc2-client-truthfulness
**Title:** Vault Sync Convergence RC-2 (client truthfulness): record base_seq on 409-refetch + heartbeat registry PATCH
**Spec anchor:** 02_Projects/Nexus/Operations/2026-07-29 Operation - Vault Sync Convergence.md
**Status:** DELIVERED on branch `whetstone/sync-conv-rc2-client-truthfulness` (vault-sync repo). Tests green (478 passed / 0 failed / 3 ignored). No push, no merge, no deploy.

---

## 0. IMPORTANT: repo-mapping note (read first)

The dispatcher seeded this burn into a worktree of **`nexus-sync`** (the Obsidian
plugin repo: bundled `main.js`, no Rust, no `Cargo.toml`, host has no `cargo`).
Every requirement here targets the **Rust Tauri daemon in `vault-sync`**. This is
a known, recurring dispatcher repo-mapping bug (at least 4 `sync-conv-*` burns
have hit it).

- **Seeded (wrong) worktree:** `/var/home/cyril/Burns/TKT-a38b7c26` (nexus-sync plugin)
- **Actual work delivered in:** `/var/home/cyril/Burns/TKT-a38b7c26-vault-sync`
  (sibling worktree of `vault-sync`, branch `whetstone/sync-conv-rc2-client-truthfulness`)
- **Verified with:** the podman link-host recipe (below), NOT bare `cargo` (host has none).

**OWNER ACTION:** fix the dispatcher's `sync-conv-*` -> repo mapping so it seeds
`vault-sync`, not `nexus-sync`. This BURN_REPORT is written to BOTH worktree
roots; the delivered code + this report are committed on the vault-sync branch.

## 1. Dependency / branch base (sequencing)

Deliverable depended on `sync-conv-client-receipt` (P2b, TKT-f74edf99), which was
**still unmerged** at burn time: `main` is at `c7853bc` (the merge-base), and
`whetstone/sync-conv-client-receipt` carries 3 unmerged commits on top
(`0ef7e0c` feat, `f272c81` + `3e2bde4` docs).

Per the ticket ("branch from the post-merge tip, or rebase onto
whetstone/sync-conv-client-receipt if still unmerged, note it"), this burn
**branched from the dep tip** `3e2bde4`. When the dep merges to main, this branch
either replays cleanly on the merge or rebases onto it (no conflicts expected -
disjoint additions).

- This branch tip: `679876c`
- Base (dep tip): `3e2bde4`  (dep's own base = `c7853bc` = current `main`)

## 2. What was already done by the dependency (do not re-litigate)

Deliverable #1's **recording path** (record the server base_seq on the 409
preserve-local-edit path) already landed on the dep branch (TKT-f74edf99) as
`push_client::record_verified_receipt`. On a typed 409 the client refetches the
named head, byte-verifies the body, and records the observed `base_seq` into the
`BaseSeqStore` **independently of the merge direction** - so a divergent note
whose local edit is PRESERVED still earns its baseline (from a *verified* server
observation, never a forged number, never the preserve branch firing). Dep tests
`divergent_baseline_absent_note_recovers_via_verified_receipt_not_forged_baseline`
and `hash_mismatched_refetch_mints_no_baseline` cover it.

That is the base_seq write path this ticket asked about. This burn therefore:
(a) adds the **end-to-end** acceptance proof that the stuck-note class is broken
(Deliverable 3a), and (b) delivers the genuinely-new RC-1 client half - the
**heartbeat** (Deliverables 2, 3b, 3c).

## 3. base_seq write path touched (acceptance requires documenting this)

The base_seq that unsticks a note is declared on the wire from
`BaseSeqStore.get(path)` (`push_client.rs:677`) and recorded on a 409 by
`record_verified_receipt` -> `BaseSeqStore.record(path, seq)`
(`push_client.rs:1208-1210`), where `seq` is the verified receipt's
`revision_seq` (the server `change_seq`). No change to that write was needed;
this burn's `push_client` change is the drain-path `mark_synced_now()` stamp
(see 4.3) and the end-to-end regression test (4.4). The report documents the
path per the acceptance criterion; it was not modified because the dep already
made it correct and the E2E test now proves it.

## 4. What this burn built

### 4.1 `api_client.rs` - the heartbeat PATCH (RC-1 client half)

- New `HeartbeatOutcome { Acknowledged, Unsupported }`.
- New `ApiClient::patch_self_heartbeat(last_seen, last_sync)` -> PATCHes
  `/api/sync/subscribers/me` with `daemon_version` + `daemon_platform` (same
  source as the existing startup `patch_self_version`) + `last_seen` (RFC3339,
  always) + `last_sync` (RFC3339, **omitted entirely when unknown**, never sent
  as a bare `null` that could clobber a good `last_sync_at`).
- HTTP **405** from an older server -> `Ok(Unsupported)` (tolerated), every other
  non-200 -> the usual `ApiError` (caller treats as non-fatal).
- Pure `heartbeat_body(...)` builder extracted so the field contract is
  unit-testable without a mock server (Rust's `regex` has no lookahead, so
  "body must NOT contain last_sync" is asserted on the builder, not via mockito).

### 4.2 `heartbeat.rs` (new module) - the periodic loop + wiring

- `run_once(api, health, now, unsupported_logged)`: builds the freshness fields
  (deriving `last_sync` from `SyncHealth`), PATCHes, and handles the outcome -
  **log-once** on 405 via an `AtomicBool` latch, WARN + retry-next-tick on any
  other error, DEBUG on ack. `now` is injected so the tick is deterministic
  under test. Returns the outcome for assertions.
- `spawn(api, health)`: 5-minute forever loop, first tick immediate (refreshes
  the row shortly after connect). Fire-and-forget; never blocks the pipeline.
- Wired in `lib.rs` right after `SyncHealth::new()`:
  `heartbeat::spawn(api.clone(), sync_health.clone())`. `api` was promoted to
  `Arc<ApiClient>` (shares the existing connection pool; `health()` /
  `patch_self_version()` still work via `Deref`).

### 4.3 `sync_health.rs` - truthful wall-clock `last_sync`

`SyncHealth` previously tracked only monotonic seconds-since-start (for the stall
watchdog). Added `last_sync_epoch: AtomicI64` (0 = none) with
`mark_synced_at(epoch)` (pure), `mark_synced_now()` (real clock), and
`last_sync_epoch() -> Option<i64>`. Stamped from the push drain
(`push_client.rs`) **only when the server actually Accepted/Merged at least one
event this tick** - a genuine upstream sync, never idle catch-up, skips, or
failures. So the heartbeat reports an honest `last_sync` or omits it; it can
never report a process-relative or fabricated time.

### 4.4 `push_client.rs` - end-to-end stuck-note regression (Deliverable 3a)

`stuck_note_recovers_second_push_accepted_after_409`: a divergent,
baseline-absent note declares `base_seq:null` and 409s; the handler refetches,
records `base_seq=77`, preserves the local edit; a **fresh** local edit is then
pushed, DECLARES `base_seq:77` on the wire, and the server **ACCEPTS** it. The
two push mocks are keyed on the declared base_seq, so a green run proves the
second push carried the recovered baseline (not `null` again) - i.e. the RC-2
eternal-CONFLICT is broken end-to-end.

## 5. Regression tests added (7 total; 471 -> 478)

| # | Deliverable | Test | File |
|---|-------------|------|------|
| 1 | 3a | `stuck_note_recovers_second_push_accepted_after_409` | push_client.rs |
| 2 | 3b | `heartbeat_issues_patch_with_version_and_timestamps` | api_client.rs |
| 3 | 3b | `heartbeat_body_omits_last_sync_when_unknown_and_includes_it_when_known` | api_client.rs |
| 4 | 3c | `heartbeat_tolerates_405_from_older_server` | api_client.rs |
| 5 | 3c | `run_once_tolerates_405_and_logs_once` (log-once latch, 2 ticks) | heartbeat.rs |
| 6 | 2  | `run_once_reports_last_sync_from_sync_health` (SyncHealth -> wire) | heartbeat.rs |
| 7 | 2  | `sync_health_last_sync_epoch_absent_until_stamped` | sync_health.rs |

## 6. Verification output (link-host recipe)

```
podman run --rm --cap-drop=DAC_OVERRIDE \
  -v <worktree>:/work:z -v vsync-cargo-registry:/cargo:Z -v vsync-target:/target:Z \
  -e CARGO_HOME=/cargo -e CARGO_TARGET_DIR=/target \
  -w /work/src-tauri localhost/vault-sync-build:latest cargo test --lib
```

- **Baseline (dep tip 3e2bde4, before changes):** `test result: ok. 471 passed; 0 failed; 3 ignored`
- **After this burn (679876c):** `test result: ok. 478 passed; 0 failed; 3 ignored; 0 measured; 0 filtered out`
- Compiled with **zero warnings** in the touched files.
- `cargo clippy` was attempted but the build image has no clippy component
  (`cargo-clippy is not installed for toolchain 1.97.1`). Clippy is not part of
  the acceptance recipe (`cargo test --lib`); noted for the owner. `cargo test`'s
  own compile is warning-clean.

## 7. Acceptance checklist

- [x] **D1 (base_seq on 409, incl. preserve-local-edit):** recording path landed
      on the dep (`record_verified_receipt`); this burn proves it end-to-end
      (`stuck_note_recovers_second_push_accepted_after_409`). base_seq write path
      documented (section 3).
- [x] **D2 (heartbeat PATCH /subscribers/me w/ version + last_seen + last_sync):**
      `patch_self_heartbeat` + `heartbeat::spawn`, wired in `lib.rs`. last_sync is
      truthful (SyncHealth wall-clock) and omitted when unknown.
- [x] **D2 (tolerate 405: log-once, no crash):** 405 -> `HeartbeatOutcome::Unsupported`;
      `run_once` logs once via `AtomicBool` latch, keeps beating.
- [x] **D3a:** `stuck_note_recovers_second_push_accepted_after_409` (no eternal CONFLICT).
- [x] **D3b:** heartbeat issues the PATCH (tests #2, #3, #6).
- [x] **D3c:** 405 tolerated (tests #4, #5).
- [x] **Cargo tests green via link-host recipe:** 478 passed / 0 failed.
- [x] **No AppImage build, no deploy, no push, no merge.** Committed on branch only.
- [x] **No em-dashes** in delivered code/docs.

## 8. Open decisions flagged for the owner

1. **Server field-name alignment (wire contract).** The heartbeat PATCH body
   sends `{daemon_version, daemon_platform, last_seen, last_sync}` (RFC3339). The
   RC-1 **server half** is a separate Nexus burn (`sync-conv-rc1-registry-server`)
   that mounts `PATCH /api/sync/subscribers/me` on the public reconciler and
   maps these to `vault_subscribers.last_seen_at / last_sync_at` (per
   `docs/SYNC_CONTRACT.md` I11, which documents the sibling `POST
   /api/sync/heartbeat` using `last_seen_at/last_sync_at`). **Confirm the server
   route accepts exactly these JSON field names**; if it expects `_at` suffixes
   or a different shape, adjust `heartbeat_body()` (one function, one test) to
   match. The existing startup `patch_self_version` already uses
   `daemon_version`/`daemon_platform`, so those two are known-good.

2. **`last_sync` provenance = push leg only (for now).** `mark_synced_now()` is
   stamped on a successful upstream push (Accepted/Merged). Pull-leg
   materialization (SSE) does not yet stamp it, so `last_sync` currently means
   "last successful upstream push," not "last sync either direction." This is
   honest and non-misleading (it never over-reports), but if the registry wants
   "last activity either way," stamp `mark_synced_now()` in the materializer pull
   path too (small, additive follow-up).

3. **Clippy not in the build image** (section 6). If the fleet gate wants clippy,
   add the component to `localhost/vault-sync-build:latest`.

4. **Dispatcher repo-mapping bug** (section 0) - recurring; needs a dispatcher fix.
