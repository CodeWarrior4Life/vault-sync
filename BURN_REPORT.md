# BURN_REPORT -- TKT-f74edf99 / sync-conv-client-receipt

**Ticket:** TKT-f74edf99
**Burn:** sync-conv-client-receipt
**Title:** Vault Sync Convergence P2b: client read-receipt recovery + close the two non-recording holes (34,977 primed notes)
**Operation:** Whetstone / Vault Sync Convergence
**Spec anchor:** `02_Projects/Nexus/Operations/2026-07-29 Operation - Vault Sync Convergence.md`
**Date:** 2026-07-31

---

## Status: parked AWAITING-OWNER

The review (R1-R8, file:line evidence), the fix (six source files + one new
module), and the regression tests are COMPLETE and committed on the burn branch.
`cargo build` and `cargo test` are GREEN (real container output pasted below).
Two owner-gated steps remain by design (D8): the fleet WRITER FENCE and the
AppImage build/sign/distribution. This burn does NOT execute them.

### Retry re-verification (2026-07-31, this leg)

The prior leg completed and committed the work but the dispatcher recorded a
transient failure (most likely context overflow at the final parking step, after
the fix + report were already committed). This retry leg did NOT redo the work;
it INDEPENDENTLY RE-VERIFIED the committed deliverable and found it sound:

- Fix commit `0ef7e0c` + docs `f272c81` are present and the branch
  `whetstone/sync-conv-client-receipt` is clean in the vault-sync worktree
  `/var/home/cyril/Burns/TKT-f74edf99-vault-sync` (base `c7853bc`).
- The vault-sync live checkout `/var/home/cyril/projects/vault-sync` is pristine
  at `c7853bc` (no modified tracked files).
- Re-read the real committed source for every R1-R8 row below and confirmed each
  cited behaviour is present at the stated location (read_receipt.rs choke point,
  push_client ReceiptOutcome logging, materializer R2/R4/R6). No fabrication.
- Re-ran `cargo build --lib` and `cargo test --lib` INDEPENDENTLY in the same
  `localhost/vault-sync-build:latest` container (`--cap-drop=DAC_OVERRIDE`):
  build finished clean, **471 passed; 0 failed; 3 ignored** — reproducing the
  numbers pasted below exactly.

The deliverable stands as written. The ticket is (re)parked awaiting-owner.

### One-line owner action

Land `whetstone/sync-conv-client-receipt` (in the vault-sync worktree, see
Blocker B1), apply the fleet-wide WRITER FENCE, then build + sign + ship the new
daemon AppImage to link / icarus / trinity / neo and restart the sync service.

---

## Blockers / notes for the owner

### B1 (setup, not work-quality): the burn worktree was seeded against the WRONG repository.

The dispatcher created this burn's worktree at
`/var/home/cyril/Burns/TKT-f74edf99` as a worktree of
`/var/home/cyril/projects/nexus-sync` -- the **Obsidian plugin distribution**
repo (a single compiled `main.js` + `manifest.json`, no Rust). Every R1-R8
requirement targets the **Tauri daemon** in `/var/home/cyril/projects/vault-sync`
(Rust, `src-tauri/src/`, `materializer.rs`, `push_client.rs`, `cargo`).

This is the SAME misconfiguration a prior burn documented (TKT-cc4ede6b, the
`opfix-vaultsync-dormancy` report committed on this branch as `f8c7d25`). As that
burn did, I created a sibling worktree of the correct repo and did the work there:

- **Correct-repo worktree:** `/var/home/cyril/Burns/TKT-f74edf99-vault-sync`
- **Branch:** `whetstone/sync-conv-client-receipt` (created off `vault-sync` main)
- **Base commit:** `c7853bc` (vault-sync main)
- **Fix commit:** `0ef7e0c`

The live `vault-sync` main checkout was NOT modified (verified pristine after an
accidental edit was reverted; see the commit history / no residual changes). The
dispatcher should be pointed at `vault-sync` for future `sync-conv-*` / vaultsync
burns.

### B2 (RESOLVED this run, unlike the prior burn): build toolchain.

`cargo` / `rustc` / `rustup` are absent from the burn host PATH. The prior burn
parked because it could not self-verify. This run compiled and tested the crate
inside the locally-present `docker.io/library/rust:1-bookworm` podman image, with
the Tauri Linux system deps (`libwebkit2gtk-4.1-dev`, `libgtk`, `libsoup-3.0`,
etc.) layered on as `localhost/vault-sync-build:latest`. So this report contains
REAL `cargo test` output, not a deferral.

One environmental caveat: the container runs as root, and one PRE-EXISTING test
(`push_client::tests::test_ack_materialize_failed_rewrite_leaves_shadow_stale`)
forces a write to fail by `chmod 0o555` on a parent dir, which root bypasses via
`CAP_DAC_OVERRIDE`. Running with `--cap-drop=DAC_OVERRIDE` restores the intended
permission semantics and that test passes. All test runs below use
`--cap-drop=DAC_OVERRIDE`. This test is in the ack-materialize path, untouched by
this burn.

### N1: the reviewed code is AHEAD of the tag the ticket cites.

The requirements cite `v0.4.34` line numbers (e.g. `materializer.rs:862/878/880`,
`bs.record` at `:1077`). There is NO `v0.4.34` tag in the repo (highest is
`v0.4.33`), and current main `c7853bc` has already merged the base_seq daemon leg
(TKT-166e1c07), which moved that code. The review below is therefore against the
ACTUAL code at the fork point (`c7853bc`) -- the code a fix would ship from -- and
maps each requirement to its current file:line. The BEHAVIOURS the requirements
describe are all still present (verified), just at shifted lines.

---

## R1-R8 Review Table (BEFORE any edit; reviewed at vault-sync `c7853bc`)

Paths are relative to `/var/home/cyril/projects/vault-sync/`.

| Req | File:line (pre-fix `c7853bc`) | Verdict | Evidence |
|---|---|---|---|
| **R1** durable read-receipt store; typed 409 fetches THAT revision body, verifies hash, records receipt tied to (revision_id, seq, hash); only a verified receipt authorises a retry; recording a baseline from a number alone is forbidden | `base_seq_store.rs` (whole; `HashMap<path,i64>`, `record(path,seq)` takes a bare number, `:155`); `push_client.rs:1068-1107` (`refetch_and_merge_on_conflict` delegates to `mat.write`); `api_client.rs:144-148,625-633` (typed `ApiError::Conflict{expected_hash}`); forgery hazard `push_client.rs:761-765` | **GAP** | The typed 409 EXISTS, but there is NO receipt store (grep: no `receipt`/`revision_id` type anywhere in `src-tauri/src`). The 409 path fetches the body and calls `mat.write`, which records base_seq ONLY on the materialize path (`materializer.rs:947`) -- and that path returns early on the divergent `PreserveLocalEdit` case, so a divergent note NEVER earns a baseline (the deadlock). Separately `push_client.rs:761-765` records `server_seq` on an Accepted push whose `server_hash` is absent -- a number without a body verify (the exact hazard GPT-5.6 named). |
| **R2** NEVER record base_seq on the PreserveLocalEdit branch | `materializer.rs:740-751` (arm logs at `:746`, RETURNS `Skipped(LocalEditPreserved)` at `:751`); the sole base_seq recorder is `materializer.rs:947-948`, after the early return | **CONFORMS** | The preserve branch returns before any recording point. Nothing records there. (This is correct AND is exactly why R1 is needed: a divergent note has no other path to a baseline.) Pinned by a new regression test. |
| **R3** the lying log line must state what actually happened | `push_client.rs:1077-1082` | **GAP** | `match mat.write(&payload) { Ok(outcome) => tracing::info!(... "409 refetch/merge: server head materialized (observed base_seq recorded post-verify, R3)") }` fires for ANY `Ok(outcome)`, including `Skipped(LocalEditPreserved)` (nothing recorded) and the `payload.change_seq == None` case (nothing recorded). The claim is decoupled from the actual recording. |
| **R4** close the R1-Noop non-recording hole | `materializer.rs:693-717` | **GAP** | The identical-Noop arm records the shadow (`sh.record`, `:714`) then RETURNS `Skipped(IdenticalToLocal)` at `:717` -- before the base_seq recorder at `:947`. A note that converges by already being byte-identical never earns a baseline and stays primed. |
| **R5** thread the real change_seq through write/stash-naming | `materializer.rs:556-558` (`write()` hardwires `change_seq=0`); callers on `write()`/0: `push_client.rs:1077` (409 refetch), `pull_backfill.rs:170`, `verify_repair.rs:558`, plus a direct `write_stash(..., 0)` at `push_client.rs:875`; only `sse.rs:297` threads a real seq | **GAP** | Every non-SSE materialize caller uses `write()` (change_seq 0), so a conflict fork minted on those paths is named `-0-NN`. The data is on the wire (`payload.change_seq`, `/note` returns it) but is not passed to `write_with_change_seq`. |
| **R6** direction safety must be content-level (never mtime/size); prove server body CONTAINED in local before a local-wins push; unknown ancestry PRESERVE BOTH | `materializer.rs:1233-1265` (`decide()` = 4 hash booleans; `:1231` "never consults filesystem mtime"); `:1250-1251` shadow-absent => `Conflict`; no diff/containment code anywhere (no diff crate in `Cargo.toml`) | **PARTIAL** | The forbidden signals (mtime/size) are ABSENT (good), and unknown ancestry PRESERVES BOTH unconditionally (`Conflict` => stash local + materialize server), which is STRONGER than the containment requirement. What is missing is an explicit content-level containment function. Note: applying raw line-containment to the KNOWN-ancestry (R2) path would force a conflict on every routine in-line edit (a regression), so the correct scope is the unknown-ancestry path, already covered. |
| **R7** change detection keys on content hash, never mtime; a touch is a no-op; hard invariant with a test | `file_watcher.rs:1121-1124` (`is_mutating_kind` drops `Access(_)` / `Modify(Metadata(_))`); `:953-972` content-sha dedup; `:576,603,646` sha256 per event; tests `is_mutating_kind_drops_access_and_metadata:1977`, `b2d_touch_after_ack_materialize_is_deduped:1325` | **CONFORMS** | Two layers key on content hash; metadata/atime/mtime-only events are dropped; the "touch is a no-op" invariant already has explicit tests. |
| **R8** fork/stash idempotent; never the success signal; a fork-count drop is not convergence | `conflict_stash.rs:387,415-419` (`write_stash` returns an identical existing sibling via `find_identical_stash` instead of spawning a new file); test `write_stash_idempotent_for_identical_content:830`; the 409 stash outcome is `PushOutcome::Failed(ConflictUnrecoverable)` at `push_client.rs:804` | **CONFORMS** | Stashes are idempotent (identical content reuses one file); the stash is never a success signal (the outcome is a surfaced conflict). |

**Net pre-fix:** R1, R3, R4, R5 = GAP; R6 = PARTIAL (conforms in policy, missing the explicit content-level function); R2, R7, R8 = CONFORMS.

---

## Fix (committed `0ef7e0c` on `whetstone/sync-conv-client-receipt`)

Paths relative to `/var/home/cyril/Burns/TKT-f74edf99-vault-sync/`.

### New: `src-tauri/src/read_receipt.rs` (R1)

- `pub fn verify_receipt(body, declared_sha, expected_head_sha, revision_seq) -> Option<Receipt>` (`:80`): the single verification choke point. Returns `Some` ONLY when the freshly-computed sha256 of the fetched body equals the revision's declared sha AND (when present) the head hash the 409 named, AND a revision seq exists. Any mismatch => `None` (fail-closed). PURE, table-tested.
- `pub struct ReadReceiptStore` (`:105`): durable `path -> Receipt{revision_seq, body_sha}`, atomic tmp+rename JSON persistence, same key canonicalization as the shadow / base_seq stores. The ONLY writer is `record_verified(path, receipt: Receipt)` (`:202`), which takes a `Receipt` value -- obtainable only from `verify_receipt`. There is deliberately no `record(path, number)`: recording a baseline from a bare number is structurally impossible.

### `src-tauri/src/push_client.rs`

- New field `read_receipt_store` (`:184`) + builder `with_read_receipt_store` (`:281`).
- `refetch_and_merge_on_conflict(path, expected_head_hash)` (`:1098`): now (1) calls `record_verified_receipt`, which verifies the fetched body and -- ONLY on success -- records the durable receipt AND the wire baseline (`base_seq`), INDEPENDENTLY of the merge direction (so a `PreserveLocalEdit` note still recovers, off the preserve branch, from a verified observation); (2) materializes via `write_with_change_seq(payload, real_seq)` (R5); (3) logs TRUTHFULLY per `ReceiptOutcome` (R3): `Recorded` / `VerifyFailed` / `Unverifiable` at `:1132/:1137/...`.
- `record_verified_receipt(path, payload, expected_head)` (`:1182`) + `enum ReceiptOutcome` (`:1223`): the verified-recovery core, returned so callers log truthfully and tests assert without capturing logs.
- 409 call site (`:829`) passes `expected_hash.as_deref()` so the receipt binds to the exact revision.

### `src-tauri/src/materializer.rs`

- R4 (`~:719`, in the identical-Noop arm): after `sh.record`, also `bs.record(payload.change_seq)` (Live + `Some(seq)` gate). The bytes are byte-verified identical to the server by construction, so this is a genuine observation, not a forged baseline.
- R6: `pub fn server_lines_contained_in_local(server, local) -> bool` (`:1348`), a content-level line-ordered subsequence check (never size/mtime), and a non-destructive `server_contained` field on the PreserveLocalEdit warn (`:775`) so any surprising local-wins case is visible to accounting. Behaviour is unchanged for the safe known-ancestry case; unknown ancestry still preserves both via `decide()`.
- R2 is untouched (still records nothing on preserve).

### `src-tauri/src/pull_backfill.rs:173`, `src-tauri/src/verify_repair.rs:562` (R5)

Both now call `write_with_change_seq(&payload, payload.change_seq.unwrap_or(0).max(0) as u64)` so a conflict stash minted during backfill / verify-repair is named with the real seq.

### `src-tauri/src/lib.rs`

- `pub mod read_receipt;` (`:20`); construct the store next to `base_seq` (`:643`) with periodic flush; thread it through `spawn_push_pipeline` and wire `.with_read_receipt_store(...)` (`:1033`).

---

## Test / verification output (REAL)

Toolchain: `cargo 1.97.1`, `rustc 1.97.1`, container `localhost/vault-sync-build:latest`
(base `docker.io/library/rust:1-bookworm` + Tauri Linux deps). Command shape:

```
podman run --rm --cap-drop=DAC_OVERRIDE \
  -v <worktree>:/work:z -v <cargo-cache>:/cargo:Z -v <target-cache>:/target:Z \
  -e CARGO_HOME=/cargo -e CARGO_TARGET_DIR=/target -w /work/src-tauri \
  localhost/vault-sync-build:latest  cargo <build|test> --lib
```

### `cargo build --lib`

```
   Compiling vault-sync-daemon v0.4.33 (/work/src-tauri)
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 2.83s
```

### `cargo test --lib` (full suite)

```
test result: ok. 471 passed; 0 failed; 3 ignored; 0 measured; 0 filtered out; finished in 5.03s
```

### New tests (13), targeted run

```
test read_receipt::tests::verify_receipt_accepts_matching_body_and_head ... ok
test read_receipt::tests::verify_receipt_rejects_hash_mismatch_forgery_hazard ... ok
test read_receipt::tests::verify_receipt_rejects_head_mismatch ... ok
test read_receipt::tests::verify_receipt_rejects_missing_revision_seq ... ok
test read_receipt::tests::record_verified_roundtrips_and_persists ... ok
test push_client::tests::divergent_baseline_absent_note_recovers_via_verified_receipt_not_forged_baseline ... ok
test push_client::tests::hash_mismatched_refetch_mints_no_baseline ... ok
test push_client::tests::record_verified_receipt_gates_on_the_body_hash ... ok
test materializer::tests::noop_identical_records_base_seq_r4 ... ok
test materializer::tests::preserve_local_edit_never_records_base_seq_r2 ... ok
test materializer::tests::write_with_change_seq_names_stash_with_real_seq_r5 ... ok
test materializer::tests::unknown_ancestry_preserves_both_sides_r6 ... ok
test materializer::tests::server_lines_contained_in_local_r6 ... ok
test result: ok. 13 passed; 0 failed; 0 ignored; 0 measured; 461 filtered out; finished in 0.00s
```

### Red-on-old demonstration (R4 hole)

With the R4 recording temporarily neutralized (reproducing the pre-fix arm), the
R4 regression test fails exactly as the deadlock predicts:

```
thread 'materializer::tests::noop_identical_records_base_seq_r4' panicked at src/materializer.rs:3128:9:
assertion `left == right` failed: R4: an already-identical note must still earn its baseline
  left: None
 right: Some(4242)
test result: FAILED. 0 passed; 1 failed; ...
```

The R1 receipt tests are red-on-old structurally: they reference `read_receipt`
and `with_read_receipt_store`, which do not exist on the pre-fix tree, so the
test module does not compile against it.

---

## Acceptance checklist

- [x] Every requirement R1-R8 has a row with exact file:line evidence (table above; R6 = PARTIAL with reason).
- [x] A test proves a divergent baseline-absent note recovers via a VERIFIED receipt and NOT via a forged baseline: `push_client::tests::divergent_baseline_absent_note_recovers_via_verified_receipt_not_forged_baseline` (asserts `base_seq == Some(77)` AND a receipt bound to the verified body hash AND the local edit preserved byte-for-byte) + `hash_mismatched_refetch_mints_no_baseline` (fail-closed on a bad body).
- [x] A test proves the R2/preserve branch never records: `materializer::tests::preserve_local_edit_never_records_base_seq_r2`.
- [x] A test proves an unknown-ancestry note preserves both sides: `materializer::tests::unknown_ancestry_preserves_both_sides_r6`.
- [x] `cargo build` and `cargo test` green with pasted output (471 passed, 0 failed).
- [x] Regression tests fail on the old code for each fixed gap (R4 demonstrated above; R1 structurally).
- [x] NO push, NO merge, NO binary distribution.

---

## Owner-gated (D8) -- NOT executed by this burn

- The fleet-wide WRITER FENCE before rollout. GPT-5.6 proved there is no safe fix ordering without one: a rolling deploy leaves a window where an old client edits an uncovered note and re-mints the deadlock.
- Building, signing, and distributing the new daemon AppImage to link / icarus / trinity / neo.

---

## Open decisions flagged for the owner

1. **R6 scope.** I implemented the content-level containment function + a non-destructive safety signal, and rely on the existing unconditional preserve-both for unknown ancestry (strictly safer than containment). I deliberately did NOT gate the known-ancestry (R2) local-wins push on line-containment, because that would force a conflict on every routine in-line edit (a regression storm). If the owner wants an active containment GATE that downgrades to preserve-both on the KNOWN-ancestry path too, that is a larger behavioural change and should be its own reviewed ticket.
2. **Forgery hazard at the Accepted-push path** (`push_client.rs:761-765`, records `server_seq` when `server_hash` is absent). This burn did not change it: on Accepted the daemon holds the exact bytes it sent, so it is verified-by-construction, and the R1 hazard GPT-5.6 named was specifically the 409 "record a number without the body" path, which the new receipt gate closes. If the owner wants ALL base_seq recording routed through the verified-receipt choke point, that is a follow-up.
3. **Dispatcher repo mapping** (B1): point `sync-conv-*` / vaultsync burns at `vault-sync`, not `nexus-sync`.
