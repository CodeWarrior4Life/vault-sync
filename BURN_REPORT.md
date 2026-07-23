# BURN REPORT: vault-sync daemon durability leg v0.4.34

**Ticket:** TKT-989ad5f2
**Burn:** opfix-vsync-durability-daemon
**Branch:** whetstone/opfix-vsync-durability-daemon (delivery on this branch only; NO tag / release / merge / push beyond the branch)
**Version target:** v0.4.34 candidate (owner-gated release train)
**Reviewed at commit:** c7853bc (base of this burn)

## Goal

Implement the daemon leg of the reviewed emergency spec: break the S513 guard
refusal deadlock (8,081 phantom pulls/pass on trinity) WITHOUT re-opening the
frontmatter-strip data-loss hole; make refusals honest RED; scope the I29
base-hash backfill; make vault symlinks visible; pin it all with regression
tests that fail on current code.

## Spec-note resolution note

The spec anchor `02_Projects/Nexus/Specifications/Vault-Sync Durability + Fleet
Script Distribution — Emergency Fix Spec (2026-07-22).md` (sha 90e2fec7) and its
Adversarial Review Report were authored 2026-07-22 and have NOT synced to this
burn host's local vault copy (searched
`/var/home/cyril/vaults/**/Nexus/Specifications/`; not present, not in
`_backups`/STALE trees). The binding EMPIRICALLY-CONFIRMED requirements R1-R9
are embedded verbatim in the ticket with exact file:line anchors and adversarial
dispositions; this burn treats the ticket's embedded requirements as the
authority. Where the spec would disambiguate a detail (e.g. the T-number ->
requirement mapping), the choice is documented inline and flagged for owner
confirmation.

## REVIEW TABLE (completed BEFORE any edit, @ c7853bc)

| Req | Requirement | Reviewed code (file:line @ c7853bc) | Verdict | Evidence |
|-----|-------------|-------------------------------------|---------|----------|
| R1 | F-B1.1 two-arm resolution on guard-hit paths. Arm 1 (frontmatter-normalized bodies EQUAL): preserve local + enqueue compensating UP push, CAS base = server hash from pull. Arm 2 (bodies differ): stash-then-align (stash local OUTSIDE scope, align local to server, update shadow). Guard NEVER silently strips; nothing converges to server without a stash. | `guard_no_frontmatter_strip` materializer.rs:1292-1301; guard-hit branch in `write()` materializer.rs:681-751 | **GAP** | The guard has ONE arm: it downgrades `PullClean`/`Conflict` -> `PreserveLocalEdit` (materializer.rs:1296-1298) and `write()` returns `Skipped(LocalEditPreserved)` (line 751) with NO enqueued push. When the server merely stripped frontmatter (body identical) the local file is byte-unchanged, so the file_watcher never fires and NO push is ever queued -> the pull re-hits the guard every pass (the 8,081 phantom-pull deadlock). No body-equal-vs-body-differ discrimination exists; no compensating-push mechanism exists (the Materializer holds no journal handle: struct materializer.rs:285-335). |
| R2 | F-B1.2 honest accounting: `Ok(Skipped(LocalEditPreserved))` and every refusal skip map to Deferred/still_divergent in `classify_pull_outcome`, so the cycle is RED, a retry-ledger entry is written, and it is never soak-eligible. Must land in the same release as R1. | `classify_pull_outcome` verify_repair.rs:314-324; `cycle_red` verify_repair.rs:218-220; pull accounting loop verify_repair.rs:576-620 | **GAP** | `classify_pull_outcome` maps `Ok(_)` -> `Succeeded` (line 322), and its own doc comment (verify_repair.rs:311-313) explicitly calls `LocalEditPreserved` "a resolved SUCCEEDED state." Only `ConflictStormBreakerOpen` is `Deferred`. So a preserved-local refusal counts as a converged pull, `still_divergent` stays 0, `cycle_red()` is false, and the strand becomes soak-eligible while divergence persists (AR-5). Unit test verify_repair.rs:1776 pins the wrong behavior (`LocalEditPreserved` -> `Succeeded`). |
| R3 | F-B1.3 log truth: the PreserveLocalEdit log line states which arm was taken; the bare "will push up" promise without an enqueued push is removed. | guard warn materializer.rs:685-690; R2 warn materializer.rs:746-750 | **GAP** | Both lines emit "PRESERVING local (will push up)" with no arm attribution and no enqueued push behind the promise (materializer.rs:689, 749). On trinity this is 74,592 lying warns / 9 passes (RC-B1). |
| R4 | F-B3.2 I29 scoping: `PushEvent.base_hash` becomes three-state (KnownBase(h)/NoRow/Unknown); shadow backfill applies ONLY to Unknown (watcher events), never to reconcile-determined NoRow. | `PushEvent.base_hash: Option<String>` push_journal.rs:172; backfill conflation push_client.rs:639-642; reconcile push build verify_repair.rs:477 + `build_modify_push` verify_repair.rs:900-908; watcher `to_push_event` file_watcher.rs:565-645 (`base_hash: None`) | **GAP** | `base_hash` is two-state `Option<String>`. push_client.rs:639-642 conflates: `None` -> shadow-backfill. But reconcile passes `None` for `missing-on-server` (verify_repair.rs:477 forwards `delta.server_hash` which is `None` when the server has no row; `build_modify_push` doc verify_repair.rs:895 "missing-on-server -> None"), meaning reconcile-determined NoRow gets wrongly shadow-backfilled with a stale hash -> server CAS-409 on a note it has no row for -> the perpetual 409 push loop (40/pass trinity, 17/pass link). Watcher Unknown (file_watcher.rs:602) also sends `None`; the two are indistinguishable. |
| R5 | F-B3.3 terminal 409+404 resolution: stash local, clear shadow, then explicit re-create or accept-delete per tombstone; never silent drop-and-requeue. NO tombstone + no server row -> CREATE (content preserved, never a local delete). | `refetch_and_merge_on_conflict` NotFound arm push_client.rs:1089-1100; 409 handler push_client.rs:778-807 | **GAP (partial)** | On 409 the caller stashes local (push_client.rs:788-790, good) and refetches. On terminal 404 (server has no row) the NotFound arm drops only the `base_seq` lineage (push_client.rs:1093-1095) and logs; it does NOT clear the shadow and does NOT enqueue an explicit re-create. The note therefore re-drifts and re-409s next pass (silent drop-and-requeue); content is preserved locally only as the stash, never re-created on the server. |
| R6 | F-A4.1 symlink visibility: verify_repair walker and file_watcher classify emit WARN + a `symlinks_skipped` report counter for ANY symlink inside the vault scan path. | walker `collect_candidate_paths` verify_repair.rs:785-817; `VerifyRepairReport` verify_repair.rs:173-211; watcher `normalize_path` file_watcher.rs:670-689 + FilterDecision file_watcher.rs:126-133 | **GAP** | verify_repair.rs:798 `if !entry.file_type().is_file() { continue; }` drops symlinks (WalkDir `follow_links(false)` -> a symlink is not `is_file()`) with zero log. The escape check at 814 only fires for canonicalized files, and internal symlinks are silently followed. `VerifyRepairReport` has no `symlinks_skipped` field. In the watcher, `normalize_path` (file_watcher.rs:670-689) silently returns `None` for a symlink escape and silently follows an internal symlink; no WARN, no counter. trinity's 6 dangling conductor symlinks appear 0 times in a 1.4GB daily log (RC-A4). |
| R7 | F-A6 log hygiene: default daemon log level INFO with DEBUG opt-in via config. | `init_logging` main.rs:35-76; directive main.rs:60-62 | **GAP** | main.rs:61 hardcodes `.add_directive("vault_sync_daemon=debug")`, which forces DEBUG for the crate regardless of `RUST_LOG`/env. No INFO default, no opt-in. trinity logs 1.4GB/day at DEBUG. |
| R8 | Regression tests T3, T6a, T6b, T7, T8, T10 (each FAILS on current code) + T11 characterization (canonical_form.rs vs server canonicalize() CRLF/BOM/NFC parity). Full suite green in podman vsync-ci. | tests dir src-tauri/tests/*; canonical_form.rs:58-168 | **GAP** | None of T3/T6a/T6b/T7/T8/T10 exist (`grep fn t3_/t6a_/...` -> nothing). canonical_form.rs has a vendored-vector test but no explicit CRLF/BOM/NFC characterization matrix named T11. Burn host cannot build natively (no cargo/rustc; S561) -> must build in podman rust:1-bookworm + webkit/gtk/keyring. |
| R9 | NO tag, NO release, NO merge to main, NO push beyond the burn branch (release train auto-promotes link+trinity; coordinate soak-RED window with P2-E3 nexus-356f via owner). | n/a (process gate) | **CONFORMS (enforced)** | Delivery is branch-only; the release ride is owner-gated (D8 hard gate). No tag/release/merge/push will be executed by this burn. |

### T-number -> requirement mapping (inferred; spec note not resolvable locally, flagged for owner)

- **T3** -> R1/R3: guard arm-1 (body-equal) preserves local AND enqueues a compensating push (base = server hash); arm-2 (body-differ) stashes + aligns; log states the arm.
- **T6a** -> R2: `classify_pull_outcome(Ok(Skipped(LocalEditPreserved)))` == `Deferred`.
- **T6b** -> R2: a pass with a preserved-local refusal is `cycle_red()` (still_divergent > 0, not soak-eligible).
- **T7** -> R4: reconcile-determined `NoRow` base is NOT shadow-backfilled (forces CREATE); watcher `Unknown` IS backfilled; `KnownBase(h)` honored verbatim.
- **T8** -> R5: terminal 409+404 with no tombstone clears shadow and enqueues a CREATE (content preserved on server).
- **T10** -> R6: verify_repair walker increments `symlinks_skipped` + WARNs for a symlink in the scan path.
- **T11** -> canonical_form CRLF/BOM/NFC byte-parity characterization matrix (documents current parity; not required to fail pre-fix).

## Implementation plan (derived from the review)

1. **R4 (substrate for R1/R5):** introduce `PushBase { KnownBase(String), NoRow, Unknown }` in push_journal.rs with `#[serde(from/into = Option<String>)]` (wire stays `null`/`""`/`"<hex>"`, fully back-compat). Change `PushEvent.base_hash` to it. Watcher -> `Unknown`; reconcile drift -> `KnownBase(h)`; reconcile missing-on-server -> `NoRow`. push_client backfill: KnownBase honored, NoRow -> `""` (CREATE, never backfilled), Unknown -> shadow-backfill.
2. **R1:** add body-only frontmatter-stripped comparison; split the guard hit into Arm 1 (body-equal: preserve + enqueue compensating `KnownBase(payload.sha256)` Modify push via a new optional journal handle on the Materializer) and Arm 2 (body-differ: fall through to the existing stash-then-write-server Conflict path). New outcome/skip variant carries the arm for accounting + logging.
3. **R2:** `classify_pull_outcome`: `LocalEditPreserved` and the arm-1 pending-push skip -> `Deferred` (still divergent -> RED + retry ledger). Fix the pinned unit test.
4. **R3:** rewrite the two log lines to name the arm; remove the bare "will push up" where no push is enqueued.
5. **R5:** terminal 404 arm of `refetch_and_merge_on_conflict`: clear shadow, then per tombstone re-create (enqueue a `NoRow` CREATE push, content preserved) or accept-delete.
6. **R6:** verify_repair walker: detect symlink entries, WARN + increment new `report.symlinks_skipped`. file_watcher: detect symlink on the raw path, WARN + counter, skip.
7. **R7:** default the tracing directive to INFO; DEBUG opt-in (env/config); pure testable `resolve_log_directive` helper.
8. **R8:** tests T3/T6a/T6b/T7/T8/T10 (fail pre-fix) + T11; build fmt/clippy/test in podman vsync-ci; paste output.
9. **Version:** bump 0.4.33 -> 0.4.34 (Cargo.toml, tauri.conf.json, package.json) + `daemon_version` pin test. Reversible, branch-only; release stays owner-gated (R9). Flagged as a decision.

## What was built (fix delivered, file:line at HEAD ab72b78)

All changes are on branch `whetstone/opfix-vsync-durability-daemon`, committed as durable checkpoints (see the commit log at the end). Nothing tagged, released, merged, or pushed.

| Req | Fix summary | Key file:line (HEAD) |
|-----|-------------|----------------------|
| R4 | New three-state `PushBase { KnownBase(String) \| NoRow \| Unknown }` replaces `PushEvent.base_hash: Option<String>`. Wire-compatible with the old `Option<String>` (`null`/`""`/`hex`) via manual Serialize/Deserialize, so old journal lines load AND old daemons read new lines. `push_client` backfill scoped via `resolve_backfilled_base`: `Unknown` (watcher) shadow-backfills, `NoRow` (reconcile no-row) forces CREATE and is NEVER backfilled, `KnownBase` honored verbatim. Watcher emits `Unknown`; reconcile drift emits `KnownBase(h)`, missing-on-server emits `NoRow`. | `push_journal.rs:183` (enum), `push_client.rs:1199` (`resolve_backfilled_base`), `push_client.rs:639` (call), `verify_repair.rs:477`/`:900` (reconcile), `file_watcher.rs:575+` (watcher) |
| R1 | Guard-hit resolves into two arms via `classify_guard_arm` (frontmatter-normalized body equality, `body_after_frontmatter_normalized`). ARM 1 (pure strip): preserve local + `enqueue_compensating_push` (CAS base = pull's server hash) via a new optional `Materializer` push-journal handle wired in `lib.rs`; returns `Skipped(GuardPreserveLocalPushUp { enqueued_push })`. ARM 2 (divergence): fall through to the Conflict stash-then-align. | `materializer.rs:1478` (`classify_guard_arm`), `materializer.rs:1447` (`body_after_frontmatter_normalized`), `materializer.rs:765-808` (two-arm dispatch in `write`), `materializer.rs:431` (`enqueue_compensating_push`), `lib.rs:663-694` (journal wire-up) |
| R2 | `classify_pull_outcome` maps `LocalEditPreserved` AND `GuardPreserveLocalPushUp` to `Deferred` (still divergent -> `cycle_red()` -> retry ledger, never soak-eligible). | `verify_repair.rs:339-343` |
| R3 | Guard log lines state the arm (`ARM 1 (pure server-strip ...)` / `ARM 2 (genuine divergence ...)`); the R2 `PreserveLocalEdit` line no longer promises a push the materializer did not enqueue. | `materializer.rs:782-807`, `materializer.rs:866-880` |
| R5 | Terminal 409+404 `NotFound` arm: clears the shadow (`ShadowStore::remove`, new) + base_seq lineage, then per tombstone — DELETE -> accept-delete; CREATE/MODIFY -> `enqueue_recreate_after_terminal_404` (explicit `NoRow` CREATE, lazy content ref, content preserved, never a local delete). | `push_client.rs:1089-1140` (arm), `push_client.rs:1151` (re-create), `sync_shadow.rs:269` (`remove`) |
| R6 | `verify_repair` walker WARNs + increments new `report.symlinks_skipped` for a symlink entry (was `!is_file() -> continue`, zero log). `file_watcher::normalize_event` lstat-checks the raw path, WARNs + bumps a `symlinks_skipped` counter (getter `FileWatcher::symlinks_skipped`), and skips (was: silently followed internal symlinks / dropped escapes). | `verify_repair.rs:195` (field), `verify_repair.rs:824-838` (walker), `file_watcher.rs:694-717` (`normalize_event`) |
| R7 | `init_logging` defaults the crate directive to INFO with DEBUG opt-in via `VAULT_SYNC_LOG_DEBUG` (RUST_LOG still overrides). Pure `resolve_log_directive`. (Opt-in is env-based because logging initializes before the daemon config loads and a tracing subscriber cannot be re-inited — see Open Decisions.) | `main.rs:37` (`resolve_log_directive`), `main.rs:49` (`debug_logging_opt_in`), `main.rs:93` (wire) |

## R8 — regression tests, PROVEN failing on pre-fix code

Each test below FAILS on the pre-fix behavior and PASSES on the fix. The tests reference new APIs (so they cannot compile against the raw base commit), so the failure was proven rigorously by reverting ONLY the behavioral hunk for each requirement (keeping the new types/signatures so the test compiles), running the test, and capturing the assertion failure — then restoring the fix. Exact captured output:

| Test | Requirement | Location | Pre-fix failure (captured) |
|------|-------------|----------|-----------------------------|
| `t3_guard_arm1_pure_strip_preserves_local_and_enqueues_compensating_push` | R1 ARM 1 | materializer.rs | `left: Skipped(LocalEditPreserved)` vs `right: Skipped(GuardPreserveLocalPushUp { enqueued_push: true })` — "ARM 1 must preserve local AND enqueue a compensating push" |
| `t3_guard_arm2_divergence_stashes_then_aligns` | R1 ARM 2 | materializer.rs | pre-fix returns `LocalEditPreserved` (no stash, no align) instead of `Stashed` |
| `t3_classify_guard_arm_table` | R1 | materializer.rs | new pure fn; encodes body-equal->ARM1 / body-differ->ARM2 |
| `t6a_local_edit_preserved_is_deferred_not_succeeded` | R2 | verify_repair.rs | `left: Succeeded` vs `right: Deferred` — "a preserved-local refusal is still divergent" |
| `t6b_preserved_local_pull_makes_cycle_red_not_soak_eligible` | R2 | verify_repair.rs | pre-fix credits `pulls_succeeded` -> `still_divergent=0` -> `cycle_red()` false (soak-eligible while divergent) |
| `t7_no_row_base_is_never_shadow_backfilled` | R4 | push_client.rs | `left: Some("staleshadowhash")` vs `right: Some("")` — "NoRow must force CREATE, NEVER the stale shadow hash" |
| `t8_terminal_409_404_recreates_content_never_local_delete` | R5 | push_client.rs | `left: 0` vs `right: 1` re-create enqueued — "a re-create must be enqueued (never a silent drop-and-requeue)"; shadow also not cleared pre-fix |
| `t10_symlink_in_scan_path_is_counted_not_silently_dropped` | R6 | verify_repair.rs | `left: 0` vs `right: 1` — "the symlink must be COUNTED (was silently dropped pre-fix)" |
| `t11_crlf_bom_nfc_byte_parity_characterization` | R8 char. | canonical_form.rs | characterization (not required to fail pre-fix); pins CRLF/BOM fold + NO-NFC byte parity |
| `push_base_wire_is_option_string_compatible` | R4 back-compat | push_journal.rs | pins the `null`/`""`/`hex` wire mapping (R5 back-compat) |

Pre-fix proof run (all 7 behavioral tests, with the fixes reverted):
```
test verify_repair::tests::t6a_local_edit_preserved_is_deferred_not_succeeded ... FAILED
test push_client::tests::t7_no_row_base_is_never_shadow_backfilled ... FAILED
test verify_repair::tests::t6b_preserved_local_pull_makes_cycle_red_not_soak_eligible ... FAILED
test materializer::tests::t3_guard_arm2_divergence_stashes_then_aligns ... FAILED
test materializer::tests::t3_guard_arm1_pure_strip_preserves_local_and_enqueues_compensating_push ... FAILED
test verify_repair::tests::t10_symlink_in_scan_path_is_counted_not_silently_dropped ... FAILED
test push_client::tests::t8_terminal_409_404_recreates_content_never_local_delete ... FAILED
test result: FAILED. 0 passed; 7 failed; 0 ignored; 0 measured; 462 filtered out
```

## Self-verification output (podman vsync-ci: `rust:1-bookworm` + libwebkit2gtk-4.1/gtk-3/ayatana-appindicator3/rsvg2/secret-1/dbus + gnome-keyring, mount `:z`)

`cargo fmt --check` — clean:
```
FMT_RC=0
```

`cargo clippy --all-targets -- -D warnings` — clean:
```
CLIPPY_RC=0
```

`cargo test --all-targets` — full suite GREEN (lib + all integration binaries):
```
test result: ok. 466 passed; 0 failed; 3 ignored; 0 measured; 0 filtered out   # lib (incl. all T3/T6a/T6b/T7/T8/T10/T11)
test result: ok. 14 passed; 0 failed; 0 ignored   # main (CLI parse)
test result: ok. 7 passed;  0 failed; 0 ignored   # test_alignment_pull (incl. updated anti-strip)
test result: ok. 7 passed;  0 failed; 0 ignored
test result: ok. 2 passed;  0 failed; 0 ignored
test result: ok. 3 passed;  0 failed; 0 ignored
test result: ok. 3 passed;  0 failed; 0 ignored
test result: ok. 7 passed;  0 failed; 0 ignored
test result: ok. 4 passed;  0 failed; 0 ignored
test result: ok. 13 passed; 0 failed; 0 ignored
test result: ok. 4 passed;  0 failed; 2 ignored
# 0 failed across every binary. RC=0.
```

Note on the CI env: the burn host cannot build the crate natively (no cargo/rustc; S561), so all builds ran in the podman `vsync-ci` image. Containers run as root; one existing test (`test_ack_materialize_failed_rewrite_leaves_shadow_stale`) forces a rewrite failure by `chmod 0o555` on a dir, which root bypasses via `CAP_DAC_OVERRIDE` — so it (and only it) reds under root and is unrelated to this burn (it also reds on the pristine base commit 622c2ac). The green run above drops `DAC_OVERRIDE`/`FOWNER` (`podman run --cap-drop=DAC_OVERRIDE --cap-drop=FOWNER`) to mirror the non-root GitHub `ubuntu-latest` CI user; the test then passes, confirming it is purely a root-in-container artifact, not a code defect.

## ACCEPTANCE CHECKLIST

- [x] **Review table with file:line evidence for R1-R9** — completed BEFORE any edit, committed at 622c2ac (table above; R1-R8 GAPs cited, R9 CONFORMS).
- [x] **R1 F-B1.1 two-arm resolution** — ARM 1 preserve+compensating-push, ARM 2 stash-then-align; guard never silently strips; nothing converges to server without a stash (ARM 2 stashes). `materializer.rs:765-808`.
- [x] **R2 F-B1.2 honest RED accounting** — refusal skips -> Deferred/still_divergent/RED/retry-ledger; lands in the SAME release as R1. `verify_repair.rs:339-343`.
- [x] **R3 F-B1.3 log truth** — arm named in the log; bare "will push up" without an enqueued push removed.
- [x] **R4 F-B3.2 I29 scoping** — three-state base_hash; backfill only on `Unknown`, never reconcile `NoRow`.
- [x] **R5 F-B3.3 terminal 409+404** — stash (caller) + clear shadow + explicit re-create/accept-delete; no-tombstone+no-row -> CREATE, never a local delete.
- [x] **R6 F-A4.1 symlink visibility** — WARN + `symlinks_skipped` counter in verify_repair walker AND file_watcher.
- [x] **R7 F-A6 log hygiene** — default INFO, DEBUG opt-in.
- [x] **R8 regression tests** — T3/T6a/T6b/T7/T8/T10 present + PROVEN failing on pre-fix (output above); T11 characterization present; full suite GREEN in vsync-ci (output above).
- [x] **cargo fmt --check clean** — output above.
- [x] **No tag / no release / no merge to main / no push beyond the branch (R9, D8)** — verified: no `v0.4.34` tag created; delivery is branch-only.
- [ ] **Release train ride (tag v0.4.34 + release; merge to vault-sync main)** — OWNER-GATED, intentionally NOT executed by this burn (see below).

## Open decisions flagged for the owner

1. **Version bump to 0.4.34 (decision taken, reversible):** R1-R9 did not explicitly list a version bump, but this IS the v0.4.34 durability leg, so the branch carries version 0.4.34 (`Cargo.toml`, `tauri.conf.json`, `package.json`) and the `daemon_version` pin test was moved to 0.4.34. Branch-only and reversible; the release itself remains owner-gated. Revert the bump if the owner prefers the version move to ride with the release commit.
2. **R7 opt-in is env-based (`VAULT_SYNC_LOG_DEBUG`), not a config-file field.** Logging initializes before the daemon config loads and a `tracing` subscriber cannot be re-inited, so a config-file flag could not take effect at init without a larger refactor. The env var is the config knob available at that point; `RUST_LOG` still overrides. Confirm this satisfies "DEBUG opt-in via config" or request the deferred re-init refactor.
3. **Multi-root compensating-push journal:** the ARM-1 compensating push and the SSE materializer both target the PRIMARY sync_root's journal (`cfg.subscriber_id`). This matches the existing single-root-effective SSE behavior (multi-root materialization is already a documented deferred task), but a multi-root host's secondary-root ARM-1 pushes would route via the primary journal. Single-root is the trinity/link incident reality. Flag if a multi-root host is in scope before release.
4. **Spec note not resolvable locally:** the 2026-07-22 emergency fix spec + adversarial review note had not synced to the burn host's vault; the ticket's embedded EMPIRICALLY-CONFIRMED R1-R9 (with file:line anchors) were treated as authority. The T-number -> requirement mapping was inferred (documented above). Confirm the mapping matches the spec's intent.

## PARK — awaiting owner

**Status:** branch `whetstone/opfix-vsync-durability-daemon` is ready at HEAD `ab72b78`; report complete; full suite green in vsync-ci; fmt/clippy clean; no tag/release/merge/push performed.

**One-line owner action:** coordinate the trinity soak-RED window with the P2-E3 lane (nexus-356f), then tag `v0.4.34` + run the release train and merge the branch to vault-sync `main` (the auto-promote reaches link+trinity and flips trinity's soak RED until the 8,081 phantom-pull set drains — AR-5 / R9, D8 hard gate).

## Commit log (this burn, on-branch)
```
ab72b78 test(sync): update test_alignment_pull_respects_anti_strip for R1 two-arm
79ca13f style(sync): cargo fmt + clippy -D warnings clean (R8 CI hygiene)
053ae46 test(sync): R8 regression suite T3/T6a/T6b/T7/T8/T10 + T11 + version 0.4.34
4c2b9c3 fix(sync): R6 symlink visibility + R7 log hygiene (default INFO)
b708a94 fix(sync): R5 F-B3.3 terminal 409+404 explicit resolution (no silent drop-requeue)
2aa8f5d fix(sync): R1/R2/R3 two-arm anti-strip guard + honest RED accounting + log truth
f90753c fix(sync): R4 F-B3.2 three-state PushEvent.base_hash (I29 scoping)
622c2ac docs(burn): R1-R9 review table for vsync durability daemon leg (TKT-989ad5f2)
```
