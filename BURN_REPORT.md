# BURN REPORT: vault-sync daemon conflict-copy generator (TKT-cb731dc0 remaining half)

**Ticket:** TKT-372e31b2
**Burn:** opfix-vaultsync-conflict-generator
**Branch:** whetstone/opfix-vaultsync-conflict-generator (delivery on this branch only)
**Reviewed at commit:** c7853bc (pinned HEAD of the dispatcher worktree; merge of the base_seq daemon leg, TKT-166e1c07)
**Spec anchor:** 02_Projects/Nexus/Operations/2026-07-29 Operation - Vault Sync Convergence.md

## Goal

The daemon manufactures false conflict copies. Fixed means: single-writer files never
produce conflict copies, a racing local write still reaches the server, and the stash
mechanism is reserved for true multi-writer divergence. This is the generator half; the
detection half shipped in sync-verify.py on 2026-08-01.

---

## PART 1: REVIEW (completed BEFORE any edit)

### Forensic correction to the ticket's starting hypothesis

The ticket points at `src-tauri/src/materializer.rs` ("stash writer around line 193,
stash sweep around line 343"). Those line numbers do not correspond to the reviewed
commit (materializer.rs:193 is `restore_server_times`, :343 is inside `impl Clone`).
Reading the code end to end plus the on-disk artifacts shows **two distinct generators**,
and the one that produced the R1 evidence artifact is **not** in materializer.rs:

| Generator | Site | Stash lsn | Confirmed by |
| --- | --- | --- | --- |
| **G1 (dominant)** push-side CAS/causal-409 pre-stash | `push_client.rs:788-790` -> `push_client.rs:870-891` | always `0` (`write_stash(..., 0)`, push_client.rs:875) | The R1 artifact is named `canary-trinity.conflict-from-f2383e35-...-**0**.md`. `f2383e35...` is trinity's own subscriber UUID (vault: Nexus/Incidents/2026-08-02 TKT-f91e840b, line 47). lsn `0` is only reachable from `stash_local_on_conflict` (push_client.rs:875) or a `write()` with no change_seq (materializer.rs:557). Operation log line 90 quotes the exact daemon line: `push_client: CAS-409 conflict, stashed losing local bytes before ack (S511 D4)`. |
| **G2 (reader-host)** pull-side R4/R5 Conflict arm | `materializer.rs:761-826`, stash written at `materializer.rs:811` | real server change_seq | The two forks live in `_sync/` right now are `canary-link.conflict-from-c7702ee9-...-1003627563.md` and `canary-trinity.conflict-from-c7702ee9-...-1003646077.md`. `c7702ee9...` is **icarus** (Operation log line 108). Non-zero lsn => `write_with_change_seq` from the SSE leg (sse.rs:295-297). Contents are stale *pulled* copies (nonce `link-1784439675` written 07-19, stashed 07-24; nonce `trinity-probe-1785615866` written 08-01T16:24, stashed 08-01T16:59) - icarus never authors either file, so both forks are false by construction. |

### ROOT CAUSE of the sideline race, named at file:line

**G1, the R1/R2 artifact (trinity's own canary sidelined, nonce never reached the server):**

`src-tauri/src/push_client.rs:788-790` stashes the pushed bytes **unconditionally** on
every 409, before any comparison against the server head:

```
if !content_bytes.is_empty() {
    self.stash_local_on_conflict(&evt.path, &content_bytes);   // push_client.rs:789
}
self.refetch_and_merge_on_conflict(&evt.path).await;           // push_client.rs:800
```

`content_bytes` for a file_watcher push is read **from disk at drain time**
(`push_client.rs:566-591`; file_watcher deliberately embeds nothing,
`file_watcher.rs:584/611/623/654`). So on a 409 the stashed bytes are byte-identical to
the live canonical file - the fork preserves nothing. Independently corroborated in the
Operation log (line 101: *"the 'losing' bytes stashed to `conflict-from-*` are
byte-identical to local"*; line 384, same finding from nexus-6388).

The 409 itself is now systematic for single-writer files: the R7b causal gate declares
`base_seq: Option<i64>` on every push (`push_client.rs:644-651`) and `None` means unknown
lineage, which the server fail-closes to 409 (Operation log line 34: a v0.4.32 baseline
migration reset per-note baselines to `None`; base_seq coverage measured at 15.0%, line
370). So: rapid rewrite -> push -> `base_seq=None` -> 409 -> **fork minted + journal entry
acked as `ConflictUnrecoverable`** -> the write never reaches the server. That is exactly
R1 (single-writer file acquired a conflict copy) and R2 (newer local write sidelined,
nothing pushed) in one code path. `forks accrue per local-bytes-change` (Operation log
line 376), and the canary changes bytes on every session-init, which is R5's trigger
profile.

**G2, the reader-host false fork:** `materializer.rs:1250-1252` (`decide()` R5) maps
"shadow absent" to `Decision::Conflict`, and the Conflict arm at
`materializer.rs:761-826` mints a stash unconditionally. For a path whose local copy is a
pure prior materialization, that fork is false by construction. The shadow is absent for
exactly the drifted paths: the D9 empty-shadow seed **deliberately leaves `drift`
unseeded** (`reconciliation.rs:252-254`, gate at `reconciliation.rs:272-275`), and
`materializer.rs:1243-1249` records that a global R5 flip was already tried and reverted
(S514, TKT-d1a41f94).

### REVIEW TABLE (R1..R5)

| Req | Requirement | Reviewed code (file:line @ c7853bc) | Verdict | Evidence / why |
| --- | --- | --- | --- | --- |
| **R1** | A single-writer file MUST NEVER acquire a conflict copy | `push_client.rs:788-790` (unconditional pre-stash), `push_client.rs:870-891` (`stash_local_on_conflict`, lsn hardcoded `0` at :875), `materializer.rs:761-826` (pull-side Conflict arm, stash at :811), `materializer.rs:1250-1252` (R5 shadow-absent -> Conflict) | **VIOLATED (2 generators)** | Neither generator consults "is this file single-writer" or "do the two byte-sets actually differ". G1: 409 -> fork of bytes identical to the live file; artifact `canary-trinity.conflict-from-f2383e35-...-0.md` carries nonce `trinity-probe-1785615447` (ticket metadata, S557). G2: two live forks in `_sync/` minted by icarus (`c7702ee9`) for canaries icarus never writes. |
| **R2** | A local write racing the daemon's own materialization of the SAME path must be resolved by causal/content comparison, and must still reach the server | Pull leg: `materializer.rs:644-680` (decide inputs), `:675-680` (`decide()` call), `:1233-1265` (`decide()`); push leg: `push_client.rs:566-604` (lazy drain read), `:778-806` (409 arm), `:1068-1107` (`refetch_and_merge_on_conflict`) | **VIOLATED on the push leg; partially met on the pull leg** | Pull leg *does* compare causally and has the correct R2 arm (`materializer.rs:740-751` `PreserveLocalEdit`, honored by the SSE consumer at `sse.rs:298-309`), so a healthy shadow resolves the self-race with no fork. Push leg has **no** comparison: the 409 arm stashes, refetches, materializes the server head over the local file (`push_client.rs:1077`), and returns `ConflictUnrecoverable` (`:804-806`) which the caller acks. The pending lazy push then re-reads the *overwritten* file (`push_client.rs:572-573`), so the racing write is pushed nowhere. "Sidelining the newer local write and pushing nothing" reproduced at file:line. |
| **R3** | Conflict stash must be idempotent and bounded; no additional siblings when content is byte-identical to the canonical or to an existing stash | `conflict_stash.rs:415-421` + `:500-518` (`find_identical_stash`), `:520-541` (`resolve_collision`), `:423-424`; global bound `materializer.rs:394-416` (`conflict_breaker_open`), config `materializer.rs:242-252`; tests `conflict_stash.rs:830` (identical-content), `:1056` (AR-008 long-path), `materializer.rs:2101` (breaker cap) | **PARTIALLY HOLDS - the v0.4.26 idempotency claim is TRUE for the "existing stash" half, FALSE for the "canonical" half** | *Existing-stash half HOLDS:* `find_identical_stash` (conflict_stash.rs:500-518) keys off the original-note prefix so it matches across device/lsn/collision-suffix and returns the existing sibling before `resolve_collision` can append `-2`. Independently confirmed in the field (Operation log line 376: *"the stash is idempotent, `-0-37` was reused across 5 ticks"*). *Canonical half LEAKS:* `write_stash` never compares `local_content` against the canonical file, and the only caller that could hit that case does not either - `push_client.rs:789` stashes bytes that ARE the canonical file's current bytes. On the pull leg the case is unreachable because R1 `Noop` (`materializer.rs:1240-1242`) intercepts first. *Bounded:* only globally, by `conflict_storm_threshold` (default 50 / 600 s, `materializer.rs:262-263`) and only on the **pull** leg (`materializer.rs:770-781`); the push-side generator G1 has **no bound at all**. Multiplication mechanism named: forks accrue per local-bytes-change, so a nonce-per-rewrite file mints one fork per rewrite - that is the three-canary-copies observation and the `06_Archive/conflict-sweep-2026-05-14/` historical duplicates. |
| **R4** | Must not regress S534/S553 serialization-storm fixes (client B1/B2/B3, 723c1f0; daemon v0.4.31 B1 shadow-forge) | B1 shadow-forge guards: `materializer.rs:701-717` (Noop arm, Live-only + records `local_raw_sha`, not the server hash) and `materializer.rs:928-950` (post-write, Live-only); B2' ack-materialize-back ordering `materializer.rs:961-986` + `push_client.rs:893-916`; B2' anti-strip `materializer.rs:167-178`; D9 fail-closed seed gate `reconciliation.rs:260-275`; storm breaker `materializer.rs:390-416` | **READ AND HONORED - no regression introduced by this burn** | Change descriptions read before touching the write path. Invariants the fix must not break: (a) shadow/base_seq recorded ONLY in `MaterializerMode::Live` (`materializer.rs:712`, `:935`); (b) observed base_seq recorded ONLY after the post-write integrity check passes (`materializer.rs:939-949`, unreachable from any early return); (c) never seed a baseline not proven equal (`reconciliation.rs:272-275`); (d) no new stash-suppression that could silently overwrite (I-83). The fix adds no recording site, moves no recording site, and changes no early-return ordering; every suppression it adds also suppresses the overwrite. Guarded by the pre-existing tests for those invariants staying green (`materializer.rs:2422` `b1_shadow_mode_write_does_not_record_baseline`, `:2403` `integrity_failed_write_does_not_record_shadow`, `:2984` `records_observed_base_seq_only_from_server_change_seq`). |
| **R5** | High-frequency rewrite is the trigger profile; regression test must cover rapid successive rewrites of a single-writer file while a materialization for that path is in flight | Trigger path: `file_watcher.rs:565-588` + `:584` (lazy event, no embedded bytes) -> `push_client.rs:566-591` (drain-time read) -> `:788-806` (409 arm). In-flight materialization serialization: `materializer.rs:542-547` + `:634-642` (per-path advisory lock, D2c). Pre-existing test coverage searched for and **absent** | **VIOLATED - no such test existed; the profile is exactly what breaks** | The per-path lock (`materializer.rs:641-642`) serializes *materializer* writers against each other but NOT against the external editor, and the push leg takes no path lock at all, so "local write races the daemon's own materialization" is unserialized by design. Nothing in the tree exercised rapid successive rewrites of one path against an in-flight materialization: the closest tests are `materializer.rs:2718` (`r2_local_edit_is_preserved_not_overwritten`, single edit) and `push_client.rs:2565` (`cas_409_stashes_local_bytes_before_ack`, single 409 with the file **absent** from disk - which is precisely why the byte-identical-to-canonical leak was never caught). Gap closed in Part 2. |

### Server-side component of the defect (REPORTED, NOT TOUCHED - cross-repo)

The review concludes the defect is **partly server-side**, and the server half is already
diagnosed in the spec anchor. Reporting only, per the ticket's park gate:

1. **Nexus `_conflict_response` omits `current_seq`.** Operation log line 309(a):
   `sync_routes_p1.py:1569` returns only `expected_hash`, so the causal gate demands
   `base_seq == current_seq` and then declines to tell the client what `current_seq` is.
   That is the true origin of the 409 fixed point that drives G1. The ~3-line server
   change (add `current_seq` to the 409 body) dissolves the deadlock at the root, needs no
   mass baseline write, and avoids the B1 forgery hazard. **No Nexus edit made.**
2. **The daemon-side companion is an owner-gated design decision, not a bug fix.**
   Operation log line 285: R2 says "do not materialize, local is newer" while safe
   recovery requires "you must have observed the server"; GPT-5.6 ruled these
   *"conceptually inconsistent and must be redesigned explicitly, not patched with a
   sequence field"*, and the R6 conflict-policy ruling is listed as an owner gate
   (Operation log line 205). Concretely: `materializer.rs:740-751` (`PreserveLocalEdit`)
   returns **before** the base_seq record at `materializer.rs:947-949`, so a preserved
   local edit can never acquire lineage and its next push declares `base_seq=None`
   forever. This is the F3 finding verified at tag v0.4.34 (Operation log line 371).
   **Not patched here**: recording an observed seq for a version we deliberately did not
   materialize would violate proof-of-observation (R3 of TKT-166e1c07) and would pre-empt
   the parked policy ruling.

---

## PART 2: FIX

Three targeted changes. Every one **fails closed to the pre-existing behavior** when its
evidence is absent, so a pre-R7b server, an unobserved note, or a readable shadow store all
behave exactly as before. Commit `20b33f4` (+ `0fb98fe` fmt).

### Fix 1 - causal gate before the stash (R1 + R2). `materializer.rs:778-848`

New first arm of `Decision::Conflict`, before the storm breaker and before any stash:

```rust
let causally_not_newer = match (&self.base_seq_store, payload.change_seq) {   // :830
    (Some(bs), Some(incoming)) => bs
        .get(&payload.path)
        .is_some_and(|observed| incoming <= observed),
    _ => false,
};
if causally_not_newer {                                                       // :836
    warn!(...);
    return Ok(MaterializeOutcome::Skipped(SkipReason::LocalEditPreserved));
}
```

`decide()` is content-relational only: it sees that local and server both differ from the
last-synced shadow and calls every such pair a conflict. It cannot see which side is
causally newer. The R7b proof-of-observation store shipped in TKT-166e1c07 is exactly the
missing input: `payload.change_seq` is the server's version token for the bytes we are
being asked to write, and `base_seq_store.get(path)` is the newest token this daemon
byte-verified for that path. `incoming <= observed` therefore proves the incoming version
is one we already materialized, so the local bytes (which differ from it, else R1 `Noop`
would have caught them) are a **later local write** - the racing write. Resolution:
preserve local, write nothing, stash nothing.

R2's "the local write must still reach the server" is then satisfied **structurally**: the
newer bytes stay at the canonical path, and the pending push is lazy
(`push_client.rs:566-591` reads the file at drain time), so it POSTs them. The old code
overwrote the canonical path first, which is precisely why the pending push re-read server
bytes and pushed nothing.

Deliberate non-actions: it **reads** lineage and never records any, so it cannot forge a
baseline (the B1 hazard) and it does not pre-empt the owner-gated R6 conflict-policy
ruling. No token on either side => no causal evidence => falls through unchanged.

### Fix 2 - scope-suspect shadow mints no fork (R1). `materializer.rs:850-874`

```rust
if shadow.is_none()
    && self.shadow_store.as_ref().is_some_and(|s| s.vault_scope_suspect())   // :861
{
    warn!(...);
    return Ok(MaterializeOutcome::Skipped(SkipReason::ShadowScopeSuspect));
}
```

"Shadow absent" is R5's unknown-provenance signal only when the store is *readable*. When
it loaded `vault_scope_suspect` (`vault_folders` empty while the store holds
vault-prefixed keys, `sync_shadow.rs:102-115`, the 2026-07-18 trinity incident) every
lookup mis-keys and misses, so R5 fires vault-wide and **every** fork it mints is false by
construction. The push leg already parks wholesale on this state (`lib.rs:873`,
`push_client.rs:369`), so continuing to mint pull-side forks while no push can leave the
host was incoherent as well as wrong. We refuse the whole write: local untouched, no
stash, no overwrite. New `SkipReason::ShadowScopeSuspect` (`materializer.rs:113-132`).

Deliberately narrow: it does **not** touch the general R5 policy, which the S514 revert
note (`materializer.rs:1243-1249` pre-fix numbering) and the owner-gated R6 ruling both
reserve.

### Fix 3 - stash only when the stash preserves something (R1 + R3). `push_client.rs:788-795`, `:875-928`

```rust
if !content_bytes.is_empty()
    && self.stash_would_preserve_bytes(&evt.path, &content_bytes)   // :793
{
    self.stash_local_on_conflict(&evt.path, &content_bytes);
}
```

```rust
fn stash_would_preserve_bytes(&self, wire_path: &str, pushed_bytes: &[u8]) -> bool {  // :915
    let abs = self.vault_root.join(forward_slash_to_path(wire_path));
    match std::fs::read(&abs) {
        Ok(on_disk) => on_disk != pushed_bytes,   // identical to the live file => no-op fork
        Err(_) => true,                           // bytes exist nowhere else => genuine D4
    }
}
```

This is the dominant generator. Because file_watcher pushes are lazy, on a 409 the pushed
bytes are normally byte-for-byte the live canonical file, so the "losing revision" we
preserved was the winning revision still sitting at its own path. One junk sibling per
local-bytes-change; for a nonce-per-rewrite file, one per session-init.

Safety argument (no silent loss, I-83 holds). The only thing that can destroy those bytes
afterwards is the `refetch_and_merge_on_conflict` materialize, and every branch preserves
them - table in the doc comment at `push_client.rs:895-911`: `Conflict` stashes the current
local bytes before overwriting; `PreserveLocalEdit` (including the new Fix 1 arm) does not
overwrite; `PullClean` means local was never a local edit; `Noop`/`AlignedToCanonical` are
content-identical; and no-materializer / failed-refetch return before any write. The
overwrite-time stash inside the materializer is the real floor; this pre-stash is only
needed for bytes **not** at their canonical path, which is exactly what the guard tests.

### What this does NOT change (scope discipline)

* `decide()` itself is untouched - the R1-R5 truth table is byte-identical.
* The always-stash floor for genuine divergence is untouched (proved by
  `causal_arm_does_not_suppress_a_genuinely_newer_server_version`).
* No recording site added, moved, or reordered => R4 no-regression.
* `conflict_stash.rs` untouched.
* No version bump, no build, no deploy, no cross-repo edit, no fork deletion.

---

## PART 3: REGRESSION TESTS

| Test | file:line | Requirement | Fails on old code? |
| --- | --- | --- | --- |
| `r1_single_writer_rapid_rewrite_race_mints_zero_conflict_copies` | materializer.rs:3202 | **R1 + R2 + R5 (deliverable a)** | **YES** |
| `r1_scope_suspect_shadow_mints_no_conflict_copy` | materializer.rs:3311 | R1 | **YES** |
| `causal_409_does_not_fork_when_pushed_bytes_are_the_canonical_file` | push_client.rs:2679 | **R1 + R3 (deliverable b)** | **YES** |
| `causal_arm_does_not_suppress_a_genuinely_newer_server_version` | materializer.rs:3269 | R2 counter-guard (true divergence still stashes) | No (guard) |
| `cas_409_still_stashes_when_bytes_are_not_the_canonical_file` | push_client.rs:2740 | No-silent-loss guard for Fix 3 | No (guard) |
| `r3_repeated_sweeps_with_identical_content_keep_exactly_one_stash` | materializer.rs:3364 | R3 idempotency under re-sweep | No (already held) |

The R5 deliverable test drives **five rapid successive rewrites** of `_sync/canary-trinity.md`,
each racing an in-flight materialization of the same path, and asserts after **every** one:
outcome is `Skipped(LocalEditPreserved)`, the conflict-copy count is zero, and the racing
bytes are still at the canonical path (which is what the lazy push reads). The final
assertion pins the newest nonce as the content a push would carry up.

### Proof the tests fail on the old code

Production hunks reverted (tests kept verbatim), same container, real output:

```
---- materializer::tests::r1_single_writer_rapid_rewrite_race_mints_zero_conflict_copies stdout ----
assertion `left == right` failed: rewrite 1: a not-newer server version must resolve to
preserve-local, got Stashed { stash_path:
".../Mainframe/_sync/canary-trinity.conflict-from-morpheus-1003646077.md" }
  left: Stashed { stash_path: ".../canary-trinity.conflict-from-morpheus-1003646077.md" }
 right: Skipped(LocalEditPreserved)

---- materializer::tests::r1_scope_suspect_shadow_mints_no_conflict_copy stdout ----
assertion `left == right` failed: a scope-suspect shadow must refuse the write, not mint a fork
  left: Stashed { stash_path: ".../Mainframe/_sync/canary-link.conflict-from-morpheus-1003627563.md" }
 right: Skipped(ShadowScopeSuspect)

---- push_client::tests::causal_409_does_not_fork_when_pushed_bytes_are_the_canonical_file ----
a 409 whose pushed bytes ARE the canonical file must mint no fork, found
["canary-trinity.conflict-from-dev-test-0.md"]

test result: FAILED. 458 passed; 4 failed; 3 ignored
```

Note the shapes the old code produced: `canary-trinity.conflict-from-<device>-1003646077.md`
reproduces the live G2 artifact in `_sync/`, and `canary-trinity.conflict-from-dev-test-0.md`
reproduces the **lsn-0** R1 evidence artifact from the ticket. The reverted-hunk run also
carries the one pre-existing failure described below.

---

## PART 4: VERIFICATION OUTPUT (real, pasted)

Host is immutable Bazzite with no native cargo, so everything ran in the pinned
`localhost/vsync-ci` container. **`--userns=keep-id` matters**: as root, the
pre-existing test `test_ack_materialize_failed_rewrite_leaves_shadow_stale` fails because
its fixture makes a directory `0o555` and root bypasses directory permissions. Under a
non-root uid the suite is fully green. That failure is **inherited, not caused by this
burn** - proved by `git stash`ing all changes and running the single test at the base
commit c7853bc, where it fails identically.

### `cargo test --manifest-path src-tauri/Cargo.toml`

```
$ podman run --rm --userns=keep-id -v .:/w:z -w /w -e CARGO_HOME=/tmp/cargo -e HOME=/tmp \
    localhost/vsync-ci bash -c 'export PATH=/usr/local/cargo/bin:$PATH; \
    cargo test --manifest-path src-tauri/Cargo.toml'

   Compiling vault-sync-daemon v0.4.33 (/w/src-tauri)
    Finished `test` profile [unoptimized + debuginfo] target(s)
     Running unittests src/lib.rs

running 465 tests
...
test conflict_stash::tests::write_stash_idempotent_for_identical_content ... ok
test materializer::tests::decide_truth_table_r1_to_r5 ... ok
test materializer::tests::guard_downgrades_frontmatter_stripping_pulls ... ok
test materializer::tests::b1_shadow_mode_write_does_not_record_baseline ... ok
test materializer::tests::conflict_storm_breaker_caps_mints ... ok
test materializer::tests::causal_arm_does_not_suppress_a_genuinely_newer_server_version ... ok
test materializer::tests::integrity_failed_write_does_not_record_shadow ... ok
test materializer::tests::r1_scope_suspect_shadow_mints_no_conflict_copy ... ok
test materializer::tests::r2_local_edit_is_preserved_not_overwritten ... ok
test materializer::tests::r1_single_writer_rapid_rewrite_race_mints_zero_conflict_copies ... ok
test materializer::tests::records_observed_base_seq_only_from_server_change_seq ... ok
test materializer::tests::r3_clean_pull_no_stash ... ok
test materializer::tests::r3_repeated_sweeps_with_identical_content_keep_exactly_one_stash ... ok
test push_client::tests::cas_409_stashes_local_bytes_before_ack ... ok
test push_client::tests::cas_409_still_stashes_when_bytes_are_not_the_canonical_file ... ok
test push_client::tests::causal_409_does_not_fork_when_pushed_bytes_are_the_canonical_file ... ok

test result: ok. 462 passed; 0 failed; 3 ignored; 0 measured; 0 filtered out; finished in 5.04s

     (integration suites)
test result: ok. 13 passed; 0 failed; 0 ignored
test result: ok.  7 passed; 0 failed; 0 ignored
test result: ok.  7 passed; 0 failed; 0 ignored
test result: ok.  2 passed; 0 failed; 0 ignored
test result: ok.  3 passed; 0 failed; 0 ignored
test result: ok.  3 passed; 0 failed; 0 ignored
test result: ok.  7 passed; 0 failed; 0 ignored
test result: ok.  4 passed; 0 failed; 0 ignored
test result: ok. 13 passed; 0 failed; 0 ignored
test result: ok.  4 passed; 0 failed; 2 ignored
test result: ok.  0 passed; 0 failed; 0 ignored
```

**Totals: 462 lib tests + 63 integration tests pass, 0 failed, 5 ignored.**

### `cargo clippy --all-targets -- -D warnings`

```
$ podman run --rm -v .:/w:z -w /w -e CARGO_HOME=/usr/local/cargo localhost/vsync-ci \
    bash -c 'export PATH=/usr/local/cargo/bin:$PATH; \
    cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings'

   Compiling vault-sync-daemon v0.4.33 (/w/src-tauri)
    Checking tauri-plugin-single-instance v2.4.2
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 42.20s
```

Zero warnings, zero errors.

### `cargo fmt -- --check`

```
$ ... cargo fmt --manifest-path src-tauri/Cargo.toml -- --check
FMT CLEAN
```

### `cargo build`

```
   Compiling vault-sync-daemon v0.4.33 (/w/src-tauri)
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 1m 03s
```

`src-tauri/Cargo.lock` moved `vault-sync-daemon 0.4.32 -> 0.4.33` on first build: a
mechanical catch-up to the `Cargo.toml` version the previous burn already set. **This burn
performed no version bump.**

---

## ACCEPTANCE CHECKLIST

| Criterion | Status | Evidence |
| --- | --- | --- |
| BURN_REPORT.md review table complete, R1..R5 each with file:line evidence | **DONE** | Part 1 review table; every cell carries `file:line @ c7853bc` |
| Root cause of the sideline race named at file:line | **DONE** | `push_client.rs:788-790` -> `:870-891` (G1, the lsn-0 R1 artifact) and `materializer.rs:1250-1252` + `:761-826` (G2, the live `_sync/` forks). Forensic attribution by stash filename lsn + device uuid |
| Fix + regression tests on the burn branch | **DONE** | `20b33f4`, `0fb98fe` on `whetstone/opfix-vaultsync-conflict-generator` |
| cargo test output pasted green | **DONE** | Part 4: 462 + 63 pass, 0 failed. Pre-existing root-only fixture failure diagnosed and proved inherited |
| No push, no merge, no deploy, no cross-repo edits | **HELD** | Three local commits only; no `git push`; Nexus repo read for contract facts, never edited; no build artifacts distributed |
| Parked awaiting-owner with the exact owner actions listed | **DONE** | Below |
| Single-writer files never produce conflict copies | **DONE for both confirmed generators**; one residual class is owner-gated | Fixes 1-3 + tests. Residual: the general R5 shadow-absent case (see Open decisions #1) |
| A racing local write still reaches the server | **DONE (structurally)** | Fix 1 leaves the newer bytes at the canonical path; the lazy push re-reads them at drain time. Asserted per-rewrite in `r1_single_writer_rapid_rewrite_race_mints_zero_conflict_copies` |
| Stash reserved for true multi-writer divergence | **DONE for the paths fixed** | `causal_arm_does_not_suppress_a_genuinely_newer_server_version` + `cas_409_still_stashes_when_bytes_are_not_the_canonical_file` pin that true divergence still stashes |

### Honest limits of this fix

1. **The general R5 case is NOT fixed and must not be, by me.** When the shadow is absent
   for an ordinary reason (D9 left a `drift` path unseeded, `reconciliation.rs:252-254`)
   and no observed base_seq exists either, the daemon has no local evidence at all and the
   always-stash floor stands. Narrowing that is the **R6 conflict-policy ruling** already
   parked on the operator (Operation log lines 205, 285). A global flip was tried and
   reverted once (S514, TKT-d1a41f94).
2. **The 409 fixed point still exists.** Fix 3 stops the *forks*; it does not make a
   `base_seq=None` push succeed. That needs the server-side `current_seq`-in-409 change
   (Operation log line 309a) - reported, not touched.
3. **Verified by unit test, not on the fleet.** No daemon was built or installed, so the
   end-to-end canary behavior on trinity/link/icarus is unverified by construction. That is
   the first owner gate.

### Open decisions flagged for the owner

1. **R6 conflict-policy ruling** (materialized-vs-read): does an unknown-provenance local
   copy lose to the server, or is it preserved? Blocks narrowing the general R5 case
   (limit 1 above). Already parked; this burn did not pre-empt it.
2. **Server-side `current_seq` in the 409 body** (Nexus, ~3 lines, `sync_routes_p1.py:1569`
   per the Operation log). Root fix for the deadlock that drives G1. Cross-repo: reported
   only.
3. **F3 / base_seq on the PreserveLocalEdit path.** `materializer.rs` returns before the
   base_seq record, so a preserved local edit never acquires lineage. Fixing it requires
   the #1 ruling, since recording a seq for bytes we did not materialize breaks
   proof-of-observation.
4. **Pre-existing red test under root.** `test_ack_materialize_failed_rewrite_leaves_shadow_stale`
   fails when the suite runs as uid 0 (its `0o555` fixture is a no-op for root). Suggest
   either gating it on `uid != 0` or documenting `--userns=keep-id` as the required CI
   invocation. Inherited from TKT-166e1c07, left alone here.

---

## PARK: AWAITING OWNER

**Ticket state: awaiting-owner.**

**Owner action:** approve and perform the gated steps - (1) version bump + release build of
the Tauri daemon, (2) distribute/install to trinity (LaunchAgent), link and icarus (systemd
units), (3) decide the Nexus server-side `current_seq`-in-409 change reported above (the
review concludes the defect is partly server-side; no cross-repo edit was made), and (4)
delete the existing `.conflict-from-*` copies in `_sync/` as post-fix operator cleanup.

