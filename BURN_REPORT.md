# BURN_REPORT — opfix-vaultsync (TKT-2643db73)

**Burn:** opfix-vaultsync (Operation Fix, first application) · **Ticket:** TKT-2643db73 (p0, Nexus/whetstone)
**Repo:** vault-sync · **Branch:** `whetstone/opfix-vaultsync` (off HEAD `6520bb0`) · **Version:** 0.4.13 → **0.4.14**
**Date:** 2026-06-11 · **Legs:** 3 (two context-checkpoint resumes)
**Constraint honored (D8):** NO push / merge / deploy / sign / distribute. Branch exists only in the burn worktree; release is owner-gated (nexus-sync-link builds+signs+distributes; nexus-sync-trinity owns Trinity).

## 1. Scope

Durable fix for incident S498 (2026-06-04..05): re-enabling sync re-polluted Obsidian's
"Created" sort (old notes jumping to "today") and stormed the server (~29k content-unchanged
files re-pushed at ~2.8/s after a server-side mtime re-stamp). Reviewed the daemon against the
Vault Sync Design Spec (2026-05-09), Vault Sync Architecture (2026-04-30), Multi-Tree Forward
Requirements (2026-05-29), and the three empirically-confirmed binding requirements R1/R2/R3
from the ticket; implemented the fixes plus regression tests.

## 2. Review table (requirement → file:line → verdict → fix)

| Req | Requirement | Where reviewed (file:line, post-fix) | Pre-fix verdict | Fix |
|---|---|---|---|---|
| R1 | **Push idempotency** — never push a file whose CONTENT hash is unchanged, regardless of mtime. Change detection must key on content hash, never mtime alone. | `src-tauri/src/push_client.rs:160-186` (pre-existing `pre_journal_filter`, normalized-SHA gate — watcher direction only); `src-tauri/src/push_client.rs:375-397` (NEW drain-time guard); `src-tauri/src/verify_repair.rs:265-281` (NEW reconcile-pass guard) | **GAP.** The only content gate was the watcher-side `pre_journal_filter`; the reconcile (verify/repair) pass keyed drift handling on the server's delta state — a server-side mtime re-stamp marked ~29k content-identical files "drift" and every one was pushed. Drain-time push had no content check at all. | Two-layer guard. (a) `verify_repair.rs:272-280`: in the `drift` arm, if `delta.server_hash == local manifest content_hash`, the file is in sync — `continue`, no push enqueued (stops the storm at the source). (b) `push_client.rs:384-397` (`process_event`): before POSTing, if `sha256_hex(content_bytes) == evt.base_hash` (the server's current hash, CAS base), return `PushOutcome::Skipped(SkipReason::IdenticalToServer)` — last-line guard for any path into the queue. Deletes exempt (no body). |
| R2 | **Birthtime preservation** — on materialize, restore the file's create time (birthtime) from canonical `created`; Obsidian's "Created" sort reads filesystem birthtime. Per-platform; do NOT fake it on Linux. | `src-tauri/src/materializer.rs:455-470` (step 8 tmp+rename, step 8b call site); `src-tauri/src/materializer.rs:610-682` (`apply_canonical_times` + doc comment); `src-tauri/src/materializer.rs:597-608` (`parse_server_timestamp`) | **GAP.** Step 8's `NamedTempFile` → `persist` (rename) creates a NEW inode: birthtime AND mtime reset to "now" on every materialize. Nothing restored either timestamp; frontmatter `created` was written but Obsidian's sort ignores it. | New `apply_canonical_times(&target, payload)` called at `materializer.rs:470` after persist: **mtime** from `file_mtime` (unix-seconds float) else parsed `modified`; **birthtime** from `created` via `FileTimes::set_created` on macOS (APFS, setattrlist ATTR_CMN_CRTIME) and Windows (NTFS, SetFileTime). Best-effort by contract: failure logs WARN, never fails the write; the resulting metadata-only FS event is dropped by the file_watcher's `is_mutating_kind` filter so it can't re-enqueue a push. **Linux limitation documented** (§5 below + in-code doc `materializer.rs:619-623`). |
| R3 | **Consume server `created`** — server already sends `created` on GET /api/sync/note (Nexus commit `7c8b07a6`); the daemon must carry it in `NotePayload` and apply it. | `src-tauri/src/api_client.rs:100-106` (`NotePayload.created`); applied at `src-tauri/src/materializer.rs:636` | **GAP.** `NotePayload` had no `created` field — the server's canonical create time was silently dropped on deserialize. | Added `pub created: Option<String>` with `#[serde(default)]` (back-compat with older servers that omit it; materializer falls back to frontmatter reconstruction). Consumed by `apply_canonical_times` (R2). Frontmatter created/modified hygiene unchanged. |

Pre-existing conformance confirmed during review: the materialize (server→local) direction's
idempotency skip — content identical → no write, mtime untouched (`identical_local_skips_no_write`
suite) — already existed and is untouched; the S498 gap was exactly the push (local→server)
direction (R1) and the timestamps (R2/R3), as the ticket stated.

## 3. Regression tests added (5)

| Test | File:line | Guards |
|---|---|---|
| `push_skips_on_content_identical_regardless_of_mtime` | `src-tauri/src/push_client.rs:812` | R1 drain-time: event whose content hashes to the CAS base is `Skipped(IdenticalToServer)` even with a fresh mtime; no POST hits the (mock) server. |
| `lazy_reconcile_push_skips_when_disk_content_matches_server_hash` | `src-tauri/src/push_client.rs:844` | R1: lazy reconcile push (body read at drain time) skips when on-disk bytes hash to the server hash — the exact 2026-06-05 storm shape (mtime re-stamped, content identical). |
| `drift_with_hash_equal_to_local_is_not_pushed` | `src-tauri/src/verify_repair.rs:979` | R1 reconcile-pass: a `drift` delta whose `server_hash` equals the local manifest hash enqueues NO push (storm stopped at the source). |
| `write_restores_canonical_mtime_and_birthtime` | `src-tauri/src/materializer.rs:1104` | R2/R3: after materialize, mtime equals canonical `file_mtime`/`modified` on every platform; birthtime equals `created` where settable (macOS/Windows); asserted only where settable. |
| `parse_server_timestamp_accepts_rfc3339_and_naive` | `src-tauri/src/materializer.rs:1155` | R2 helper: server timestamp parsing accepts RFC3339 (`Z`/offset) and naive `Y-m-d H:M:S[.f]` / `Y-m-dTH:M:S[.f]` forms. |

## 4. Validation (podman `rust:1`, ci.yml deps)

Container: `rust:1` + `libwebkit2gtk-4.1-dev libgtk-3-dev libayatana-appindicator3-dev librsvg2-dev libsecret-1-dev`,
`rustup component add rustfmt clippy`. Worktree mounted at `/repo`, cwd `/repo/src-tauri`.

Two findings surfaced and fixed during validation (commit "checkpoint 3"): three rustfmt diffs
in checkpoint-2 test code, and `clippy --all-targets` failing on the integration test
`tests/test_materializer_write.rs` whose `NotePayload` literal was missing the new `created`
field (E0063). Output below is the post-fix run, verbatim:

```text
rustc 1.96.0 (ac68faa20 2026-05-25)
cargo 1.96.0 (30a34c682 2026-05-25)
=== cargo fmt --check ===
fmt exit: 0
=== cargo clippy --all-targets -- -D warnings ===
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 5.17s
clippy exit: 0
=== cargo test --lib ===
test push_client::tests::failed_event_nacks_journal ... ok
test verify_repair::tests::run_match_state_is_noop_no_push ... ok
test verify_repair::tests::run_enqueues_pushes_for_drift_state ... ok
test verify_repair::tests::run_calls_reconcile_with_local_manifest ... ok
test verify_repair::tests::manifest_canonicalizes_forward_slash ... ok
test verify_repair::tests::manifest_includes_root_family_md_after_rasp_rebuild ... ok
test verify_repair::tests::run_does_not_auto_delete_local_for_server_missing ... ok
test verify_repair::tests::modify_for_path_not_in_local_manifest_is_reported_as_error ... ok
test verify_repair::tests::drift_with_hash_equal_to_local_is_not_pushed ... ok
test verify_repair::tests::report_elapsed_ms_is_set ... ok
test verify_repair::tests::verify_repair_manifest_rooted_at_passed_sync_root ... ok
test verify_repair::tests::reconcile_5xx_is_surfaced_as_error ... ok
test verify_repair::tests::manifest_excludes_obsidian_lattice_trash_dirs ... ok
test verify_repair::tests::manifest_excludes_substrate_paths ... ok
test verify_repair::tests::parallel_manifest_respects_zero_concurrency_default ... ok
test verify_repair::tests::parallel_manifest_matches_sequential_sorted ... ok
test push_client::tests::push_client_increments_tray_on_failed ... ok
test verify_repair::tests::report_samples_first_50_paths ... ok
test materializer::tests::atomic_write_no_partial_visible ... ok
test materializer::tests::identical_local_skips_no_write ... ok
test push_journal::tests::append_batch_writes_all_events_in_one_call ... ok
test integrity_check::tests::subprocess_timeout_returns_error ... ok

test result: ok. 329 passed; 0 failed; 3 ignored; 0 measured; 0 filtered out; finished in 5.03s

test exit: 0
PODMAN_EXIT=0
```

(`cargo test --lib` output abridged to the final lines by the harness's `tail -25`; the
summary line — **329 passed; 0 failed** — covers the full suite.)

Targeted run of the five new regression tests, verbatim:

```text
test materializer::tests::parse_server_timestamp_accepts_rfc3339_and_naive ... ok
test materializer::tests::write_restores_canonical_mtime_and_birthtime ... ok
test push_client::tests::push_skips_on_content_identical_regardless_of_mtime ... ok
test push_client::tests::lazy_reconcile_push_skips_when_disk_content_matches_server_hash ... ok
test verify_repair::tests::drift_with_hash_equal_to_local_is_not_pushed ... ok
test result: ok. 5 passed; 0 failed; 0 ignored; 0 measured; 327 filtered out; finished in 0.01s
```

## 5. Documented limitation — Linux (btrfs/ext4) birthtime is read-only

Linux exposes birthtime (statx `btime`) **read-only**; there is no kernel API to write it on
btrfs, ext4, or any mainline filesystem. `apply_canonical_times` therefore restores **mtime only**
on Linux and does not fake birthtime (no debugfs tricks, no clock games). Consequence: on a Linux
vault, Obsidian's "Created" sort reflects materialize time, not canonical note creation —
mitigated by frontmatter `created` (still written, hygiene) for plugins/queries that read it.
macOS (APFS) and Windows (NTFS) — the fleet's actual desktop vaults — get full birthtime
restoration. In-code documentation: `src-tauri/src/materializer.rs:619-623` and the
`#[cfg(not(any(target_os = "macos", windows)))]` arm at `materializer.rs:660-665`.

## 6. Version bump

0.4.13 → 0.4.14 in `src-tauri/Cargo.toml`, `src-tauri/Cargo.lock`, `src-tauri/tauri.conf.json`, `package.json`.

## 7. Owner action required (ticket parked awaiting-owner)

vault-sync durable fix reviewed + implemented + tested on branch `whetstone/opfix-vaultsync`
(worktree `/home/cyril/Burns/TKT-2643db73`, HEAD = checkpoint 3). Ready for owner
**build / sign / distribute** per the S498 lane split (nexus-sync-link: build+sign+distribute;
nexus-sync-trinity: Trinity). Nothing has been pushed, merged, deployed, or released.
