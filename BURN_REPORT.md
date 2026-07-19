# BURN_REPORT -- TKT-c41c2225 / vaultsync-pull-defect-pair

**Ticket:** TKT-c41c2225
**Burn:** vaultsync-pull-defect-pair
**Title:** vault-sync v0.4.33: fix the two every-cycle pull failures + failure-honest reconcile accounting (THESEUS AR-003/008/009)
**Branch (this worktree):** `whetstone/vaultsync-pull-defect-pair` on `/var/home/cyril/Burns/TKT-c41c2225`
**Reviewed commit:** `60766af` (branch tip == vault-sync main tip at burn start)
**Spec anchor:** `02_Projects/Lattice Meta/Specifications/THESEUS -- Nexus Sync Adversarial Review and P2-E3 Burn Intake (2026-07-19).md`
**Scope:** THESEUS Burn C only (AR-003 / AR-008 / AR-009). R7b/base_seq (Burn B), substrate custody (Burn A), fleet convergence (Burn D) are explicitly out of scope.

---

## Live journal reproduction (R4, PRE-EDIT)

Daemon on link: `nexus-vault-sync.service`, PID 1310111, v0.4.32-era code. `journalctl --user -u nexus-vault-sync.service`. The identical failure pair recurs EVERY reconcile cycle (00:08, 00:18, 00:38, 00:48, 00:58, 01:08, 01:18, 01:28, 01:38, ...). The canonical 00:58 cycle the spec cites:

```
2026-07-19T04:58:20.045546Z  WARN vault_sync_daemon::verify_repair: reconciliation: pull failed path=01_Periodic/Daily/2026-07-04-Saturday - Quiet holiday inbox noise, no meetings or plans on July 4th.md error=fetch: network error: error decoding response body
2026-07-19T04:58:20.045554Z  WARN vault_sync_daemon::verify_repair: reconciliation: pull failed path=03_Media/Social/X/@theblacktruth/@Lemelson - 𝗖𝗵𝗿𝗶𝘀𝘁𝗶𝗮𝗻 𝗭𝗶𝗼𝗻𝗶𝘀𝗺 𝗘𝗫𝗣𝗢𝗦𝗘𝗗 - 𝗛𝗼𝘄 𝗮 𝗛𝗲𝗿𝗲𝘀𝘆 𝗛𝗶𝗷𝗮𝗰𝗸𝗲𝗱 𝗔𝗺𝗲𝗿𝗶𝗰𝗮 𝗘𝗽 𝟰𝟭 A war is.md error=materialize: conflict-stash error: io error: File name too long (os error 36)
2026-07-19T04:58:20.045557Z  INFO vault_sync_daemon::verify_repair: reconciliation: server-wins pull pass complete requested=2 pulled=0
2026-07-19T04:58:20.061255Z  INFO vault_sync_daemon::reconciliation: reconciliation: pass complete files_scanned=59922 files_in_sync=59922 pulls_pending=0 pushes_queued=0 substrate_refused=0 elapsed_ms=4308
```

**The false green:** two pulls fail (`requested=2 pulled=0`), then the very next line reports `files_in_sync=59922 pulls_pending=0 pushes_queued=0` -- a cycle that looks clean to any soak counter despite two persistent failures. This is AR-003 exactly.

**AR-009 root cause (confirmed against the live server, read-only GET `/api/sync/note`):** HTTP 200, `content-type: application/json`, 6122 bytes. The JSON body has `"modified": null`. The client's `NotePayload.modified` is a **non-optional `String`** (`api_client.rs:90`), so serde rejects `null` -> reqwest surfaces the generic `error decoding response body`. The fields `file_mtime`, `created`, `updated_at` are also `null` but are already typed `Option`; `modified` is the one field that was left required. Field-level evidence:

```
path: str    frontmatter: dict    body: str    sha256: str
modified: NoneType None      <-- decode killer (declared String, not Option)
file_mtime: NoneType None    created: NoneType None    updated_at: NoneType None
enriched_body: str           content_hash: str
```

**AR-008 root cause:** the X-note basename uses mathematical-bold Unicode (each glyph is 4 UTF-8 bytes); the stem alone is ~230+ bytes. `compute_stash_path` (`conflict_stash.rs:266-278`) appends `.conflict-from-<device>-<lsn>.md` producing a basename well past the ext4 `NAME_MAX` of 255 bytes, so `write_stash`'s atomic persist (`conflict_stash.rs:343`) fails with `ENAMETOOLONG` (os error 36).

---

## R1..R5 Review Table (against commit `60766af`, PRE-EDIT)

| Req | File:Line evidence | Verdict | Evidence |
|---|---|---|---|
| **R1** AR-008: length-safe stash names (hash-bounded basename + manifest) + filename-limit tests (Linux/macOS/Windows) + regression fixture on the exact live path | `conflict_stash.rs:266-278` (`compute_stash_path` builds `{stem}.conflict-from-{device}-{lsn}.md`); `conflict_stash.rs:301-347` (`write_stash` persists to that name); `conflict_stash.rs:340-344` (tmp+persist) | **GAP** | The basename is `stem` (unbounded, verbatim from the note path) plus a fixed suffix. There is NO length guard, NO hash-fallback, NO manifest. For the live X-note the persist at `:343` raises ENAMETOOLONG. No filename-limit test exists (grep: no `NAME_MAX` / `255` / `os error 36` anywhere in the module). |
| **R2** AR-009: decode failures capture status/content-type/body-length/request-id/bounded sample (no leak); fix the contract mismatch for the Daily note; not in-sync until bytes materialize + hash-verify | `api_client.rs:302-328` (`fetch_note`), esp. `:311` `Ok(resp.json().await?)`; `api_client.rs:48-49` (`ApiError::Network(#[from] reqwest::Error)`); `api_client.rs:90` (`pub modified: String`) | **GAP** | `.json().await?` collapses every decode failure into reqwest's opaque `error decoding response body` via the blanket `#[from]` -- no status, content-type, length, request-id, or sample is captured. The contract mismatch is `modified: String` vs server `null`. (Sub-part "hash-verify before in-sync": the materializer DOES post-write integrity-check at `materializer.rs:861-869`, but the fetch fails before materialize, and AR-003 rounds that failure to zero -- see R3.) |
| **R3** AR-003: report carries attempted/succeeded/failed/deferred/still-divergent; any failed pull => RED, ineligible for soak; failed items keep durable retry state | `verify_repair.rs:507` (`report.add_count = pulled` -- successes only); `verify_repair.rs:501-504` (failures pushed to `report.errors`, never summarized); `verify_repair.rs:172-193` (`VerifyRepairReport` has no attempted/failed/deferred/divergent/red fields); `reconciliation.rs:177-185` (summary emits `pulls_pending=add_count`, no failed count, no red flag) | **GAP** | Failures are logged (`:502`) and collected in `report.errors` (`:503`) but never counted into the summary. `add_count` is successes only, so `pulls_pending=0` after a total-failure cycle. There is no attempted/failed/deferred/still-divergent tally, no RED/soak-eligibility flag, and no durable per-item retry record (retry is only the implicit next full scan). |
| **R4** Evidence discipline: reproduce both failures from live journal pre-edit; post-fix run daemon suite + new regressions; regressions FAIL on v0.4.32 | Journal repro above; test harness `src-tauri/tests/` + in-module `#[cfg(test)]` | **PARTIAL (repro DONE)** | Both failures reproduced from the live journal and the AR-009 contract mismatch confirmed against the live server (above). Test half pending implementation; regressions will be structurally red on old code (reference fields/fns absent pre-fix). |
| **R5** No deploy, no daemon restart, no version-bump ship; deliver on burn branch as v0.4.33 candidate | n/a (process constraint) | **CONFORMS (commitment)** | No deploy/restart/push performed or planned by this burn. The live daemon (PID 1310111) is left running untouched; the read-only diagnostic GET does not mutate server or vault state. Delivery is branch-only. |

**Net pre-fix state:** R1, R2, R3 all GAP. R4 repro half satisfied. R5 is an honored constraint. The three GAPs are independent code paths (stash naming, response decode, reconcile accounting) but AR-003 is the reason the other two stayed invisible: it rounds the two persistent failures down to a clean summary.

---

## Fix (implemented on `whetstone/vaultsync-pull-defect-pair`)

### AR-008 -- length-safe stash names (`conflict_stash.rs`)

- `build_stash_filename(original_path, stem, device, lsn) -> StashFilename` (`conflict_stash.rs`): builds the natural `<stem>.conflict-from-<device>-<lsn>.md`; if that basename exceeds the cross-platform component cap it replaces the stem with `<truncated-stem>.<sha256(path)[..16]>` and flags `hashed = true`.
- `STASH_BASENAME_MAX = 250` and `basename_fits()` enforce BOTH the 255-byte (Linux) and 255-UTF-16-unit (macOS/Windows) component limits, with headroom for the `-2/-3` collision suffix.
- The hash is derived from the FULL original path, so the same note deterministically maps to the same stash base -- the S514 idempotency + collision-reuse logic keeps working for long paths (proved by `ar008_long_path_stash_is_still_idempotent`).
- `write_stash` now records a manifest line in `<vault_root>/.sync-conflict-stash-manifest.jsonl` (dot-prefixed, Obsidian-invisible) mapping the bounded stash back to its original note path, device, lsn, ts. Best-effort: a manifest write failure never fails the stash (the losing bytes are the load-bearing artifact).
- Short (normal) names are UNCHANGED -- no manifest, verbatim readable name (`stash_filename_short_names_unchanged_no_manifest`).

### AR-009 -- diagnosable pull decode + contract fix (`api_client.rs`)

- **Contract fix:** `NotePayload.modified` changed from `String` to `#[serde(default)] Option<String>`. The live server returns `modified: null` for this Daily note; `String` made serde reject it. `Option` matches the already-optional `file_mtime`/`created`; no code reads the field.
- **Diagnosability:** new `ApiError::Decode { context, status, content_type, body_len, request_id, serde_error, body_sample }` and a `decode_json()` helper that reads the raw body, then deserializes, and on failure captures HTTP status, content-type, body length, a request-id (`x-request-id`/`x-correlation-id`/`cf-ray`), and the structural serde error (field position + expected/found type). `fetch_note` now decodes through it.
- **No content leak:** a `body_sample` is attached ONLY when the content-type is not JSON (an HTML/proxy error page is diagnostic, not note content, and is capped at 256 bytes). A JSON structural mismatch means the body IS the note, so it is never sampled -- the serde position/type carries the signal.
- **Not in-sync until verified:** a fetch that returns `ApiError::Decode` is an error, so AR-003 counts it as a failed pull (still-divergent, RED). The materializer's existing post-write integrity check (`materializer.rs:861-869`) still hash-verifies any bytes that do materialize.

### AR-003 -- failure-honest reconcile accounting (`verify_repair.rs`, `reconciliation.rs`, new `retry_ledger.rs`)

- `VerifyRepairReport` gains `pulls_attempted / pulls_succeeded / pulls_failed / pulls_deferred / still_divergent` + `unresolved_paths_sample`, plus `cycle_red()` and `soak_eligible()`.
- `classify_pull_outcome()` (pure, table-tested) maps each pull result to Succeeded / Failed (Err or IntegrityFailed) / Deferred (conflict-storm breaker). The pull loop tallies all five counts; `add_count` now equals `pulls_succeeded` (a failed pull can no longer inflate it). The no-materializer path now counts pulls as DEFERRED (RED), not a silent green.
- The `reconciliation.rs` pass-complete summary now emits `pulls_attempted/succeeded/failed/deferred`, `still_divergent`, `retry_ledger_pending`, and an explicit `cycle=RED|green` + `soak_eligible` verdict; a RED cycle also emits a WARN naming the unresolved sample. The 00:58-style false green is now structurally impossible: any failed/deferred pull sets `cycle_red()` true.
- **Durable retry state:** new `retry_ledger.rs` -- a JSON-persisted, thread-safe ledger keyed by note path (`record_failure` on fail/defer, `clear` on success). It survives daemon restarts (`state_is_durable_across_reload`), caps error strings, and loads-empty on corruption. Wired into `run_reconciliation_pass` at `<config_dir>/reconcile-retry-ledger.json` (NOT in the vault). It is an observability layer on top of the already-idempotent rescan; a persistence failure is logged, never fatal.

### Scope discipline / what the fix does NOT touch

- No change to `decide_direction`, shadow logic, push pipeline, conflict-storm breaker thresholds, or the SSE consumer.
- `ChangeRow.modified` (a different struct) stays `String`.
- Other `.json().await?` decode sites are left as-is; `decode_json` is available for later adoption. Only `fetch_note` (the reviewed AR-009 path) was rerouted, to keep the sync-daemon blast radius minimal.

---

## Verification (R4)

### Build environment

The link host is bootc-immutable and has no `-devel` headers (dbus/gtk/webkit) for a native `cargo` build. The full daemon test suite was built and run inside the existing `insync-box` distrobox (Ubuntu 26.04, which ships `libdbus-1-dev`/`libgtk-3-dev`/`libwebkit2gtk-4.1-dev`/`libsoup-3.0-dev`) sharing `$HOME`, so the same `~/.cargo` rustc 1.97.1 toolchain and `target/` are reused. No system package was installed on the host; no daemon was restarted.

### New tests pass on the fix branch (full suite, distrobox)

```
Running unittests src/lib.rs
test result: ok. 441 passed; 0 failed; 3 ignored; 0 measured; 0 filtered out; finished in 5.05s
Running unittests src/main.rs
test result: ok. 13 passed; 0 failed; ...
Running tests/test_alignment_pull.rs   test result: ok. 7 passed; 0 failed
Running tests/test_api_client.rs       test result: ok. 7 passed; 0 failed
Running tests/test_b1_alternation.rs   test result: ok. 2 passed; 0 failed
Running tests/test_config.rs           test result: ok. 3 passed; 0 failed
Running tests/test_keyring.rs          test result: ok. 3 passed; 0 failed
Running tests/test_materializer_write.rs test result: ok. 7 passed; 0 failed
Running tests/test_pairing.rs          test result: ok. 4 passed; 0 failed
Running tests/test_scope.rs            test result: ok. 13 passed; 0 failed
Running tests/test_sse.rs              test result: ok. 4 passed; 0 failed; 2 ignored
```

`cargo clippy --all-targets -- -D warnings` also passes clean (the strict gate CI enforces). Pre-fix baseline was 420 lib tests; the +21 new tests bring lib to 441. New tests added:

- **AR-008** (`conflict_stash.rs`): `regression_ar008_long_x_note_path_stashes_length_safe` (exact live X-note path), `stash_filename_length_safe_linux_bytes`, `stash_filename_length_safe_macos_windows_utf16`, `stash_filename_short_names_unchanged_no_manifest`, `ar008_long_path_stash_is_still_idempotent`.
- **AR-009** (`tests/test_api_client.rs`): `regression_ar009_daily_note_null_modified_decodes` (exact live Daily path, `modified: null`), `ar009_json_decode_failure_is_diagnosable_and_leak_free`, `ar009_non_json_decode_attaches_bounded_sample`.
- **AR-003** (`verify_repair.rs`): `regression_ar003_failed_pull_is_red_not_false_green` (e2e: a failed pull -> RED + ledger), `ar003_no_materializer_drift_is_deferred_and_red`, `classify_pull_outcome_table`, `report_cycle_red_semantics`.
- **Retry ledger** (`retry_ledger.rs`): `record_and_clear_roundtrip`, `repeated_failures_increment_attempts_preserve_first_ts`, `state_is_durable_across_reload`, `corrupt_file_loads_as_empty_not_error`, `error_string_is_capped`.

### Regressions FAIL on v0.4.32 code (red-on-old)

The AR-008 and AR-003 regressions are `#[cfg(test)]`-inline (they travel with the impl), so red-on-old was demonstrated by compiling the NEW test functions against a pristine `git worktree` at `60766af` (v0.4.32). The tests exercise symbols and behavior that do not exist pre-fix, so the old tree fails to compile them (a strictly stronger failure than an assertion miss). Extracted verbatim and compiled against the old sources (`git worktree add --detach /tmp/vsync-old 60766af`, new tests injected, `cargo test --no-run`):

```
# AR-008 (conflict_stash inline) + AR-003 (verify_repair inline) + retry_ledger, against v0.4.32:
error[E0433]: cannot find `retry_ledger` in `crate`
error[E0425]: cannot find value `STASH_BASENAME_MAX` in this scope        (x5)
error[E0425]: cannot find function `basename_fits` in this scope
error[E0425]: cannot find function `build_stash_filename` in this scope   (x3)
error[E0599]: no associated function or constant named `MANIFEST_RELPATH` found for struct `ConflictStash`  (x2)
error[E0425]: cannot find function `classify_pull_outcome` in this scope   (x5)
error[E0433]: cannot find type `PullResultClass` in this scope            (x5)
error[E0599]: no method named `cycle_red` found for struct `VerifyRepairReport`  (x5)
error[E0599]: no method named `soak_eligible` found for struct `VerifyRepairReport`
error[E0609]: no field `pulls_attempted`/`pulls_succeeded`/`pulls_failed`/`pulls_deferred`/`still_divergent` on `VerifyRepairReport`
error[E0599]: no method named `with_retry_ledger` found for struct `VerifyRepair`

# AR-009 (tests/test_api_client.rs) against v0.4.32 (pristine lib):
error[E0599]: no method named `is_none` found for struct `std::string::String`   <- proves `modified` was `String`, so `modified: null` cannot decode
error[E0599]: no variant named `Decode` found for enum `ApiError`   (x2)          <- proves decode diagnosability absent
error: could not compile `vault-sync-daemon` (test "test_api_client") due to 3 previous errors
```

Mapping (each error proves a specific regression cannot pass on v0.4.32):
- `ApiError::Decode` absent -> AR-009 diagnosability tests cannot compile/pass.
- `NotePayload.modified` is `String` -> the `modified: null` note still fails to decode.
- `build_stash_filename` / `basename_fits` / `STASH_BASENAME_MAX` / `ConflictStash::MANIFEST_RELPATH` absent -> AR-008 tests cannot compile; the long path still hits ENAMETOOLONG.
- `PullResultClass` / `classify_pull_outcome` / report fields `pulls_failed`/`still_divergent` / `cycle_red()` / `with_retry_ledger` / module `retry_ledger` absent -> AR-003 tests cannot compile; the cycle still reports false green.

---

## Acceptance checklist

| Item | Status | Evidence |
|---|---|---|
| Both live defects reproduced from journal evidence PRE-edit | DONE | Journal block above (00:58 cycle + AR-009 live-server field dump) |
| AR-008 fix: hash-bounded stash name + manifest | DONE | `conflict_stash.rs` `build_stash_filename` + `record_stash_manifest` |
| AR-009 fix: diagnosable decode + contract + hash-verify gate | DONE | `api_client.rs` `ApiError::Decode`/`decode_json`, `modified: Option`; materializer integrity check unchanged |
| AR-003 fix: attempted/succeeded/failed/deferred/still-divergent + RED + durable retry | DONE | `verify_repair.rs` report fields + `classify_pull_outcome`; `reconciliation.rs` summary; `retry_ledger.rs` |
| Regression fixtures use EXACT live failing paths | DONE | Daily path + X-note path embedded verbatim in tests |
| Regressions fail on old code | DONE | Red-on-old compile output above |
| Full daemon test suite + regressions pass, output pasted | DONE | 441 lib + all integration green (above) |
| No deploy / no daemon restart / no version-bump ship | HELD | Live daemon PID 1310111 untouched; no `install.sh`/service action; `CARGO_PKG_VERSION` NOT bumped (still 0.4.32 in-tree) |
| No push / no merge | HELD | Work committed to `whetstone/vaultsync-pull-defect-pair` only |
| BURN_REPORT at worktree root | DONE | This file |
| No em-dashes in authored prose | OK | Hyphen-minus only in my prose |

---

## Open decisions flagged for the owner

1. **Version bump deferred (R5).** The in-tree `Cargo.toml` version is still `0.4.32`. Per R5 this burn does NOT bump to `0.4.33` or ship. When the owner decides to cut the v0.4.33 candidate, bump `[package].version` and build/sign/distribute as a separate, lease-gated step after the icarus/fleet picture settles.
2. **`decode_json` adoption breadth.** I rerouted only `fetch_note` (the reviewed AR-009 path). `get_changes`, `reconcile_batch`, and the push/health decodes still use `.json().await?` and would surface the same opaque error if THEY ever fail to decode. Low-risk follow-up: route them through `decode_json` too. Left out here to keep the sync-daemon blast radius minimal.
3. **Retry ledger consumers.** The ledger is written and logged (`retry_ledger_pending=N` in the summary) but no tray/Conductor surface reads it yet. AR-014 (health tiers) is the natural consumer; wiring it to the tray "still-divergent" badge is a Burn D item.
4. **Stash manifest + the AR-007 cleanup burn.** My AR-008 fix writes `<vault_root>/.sync-conflict-stash-manifest.jsonl` at RUN time (only once deployed). The sibling burn TKT-af36918a (AR-007) is consolidating conflict artifacts into `.sync-quarantine/`. The manifest path is dot-prefixed and Obsidian-invisible, consistent with that burn's R8 convention; if the owner wants a single quarantine root, point `MANIFEST_RELPATH` under `.sync-quarantine/` before deploy. No conflict at delivery time (this burn wrote nothing to the live vault).
5. **P2-E3 soak semantics.** `cycle_red()`/`soak_eligible()` give the honest per-cycle signal the receipt schema needs, but this burn does NOT wire the three-consecutive-clean-cycle counter (that is Burn D). A soak counter must read `soak_eligible()` and reset on any RED cycle.

---

## Commits on this branch

```
b930078 docs(burn): pre-edit review table + live journal reproduction
19d3115 fix(sync): v0.4.33 candidate - AR-008 / AR-009 / AR-003 (+ regressions)
HEAD    fix(sync): box ApiError::Decode, clippy -D warnings clean, em-dash cleanup, finalize report
```
Run `git log --oneline 60766af..HEAD` on this branch for exact hashes.

## Coordination note

The THESEUS review (vaults-d0ba, Pi/GPT-5.6) that seeded this burn is advisory (capacity-unproven) and explicitly says do NOT check P2-E3. This burn closes Burn C only; it does not close P2-E3 and makes no such claim. A concurrent PerimeterScope release-train broadcast on the fleet channel is unrelated to vault-sync.

