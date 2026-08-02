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

(filled in after implementation)
