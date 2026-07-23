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

<!-- BUILD/TEST OUTPUT + ACCEPTANCE CHECKLIST appended as the burn proceeds -->
