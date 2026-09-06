# 2026-09-05 — Append-only files fork whole-file on every append race (TKT-ddff1877)

**Status:** fix on `fix/tkt-ddff1877-append-merge` (PR pending review; NO deploy without the arranger's GO — the daemon is substrate).
**Regression of:** the 2026-05-25 append-concatenation P0 (vault: `02_Projects/Lattice/lattice-vault-sync/Incidents/2026-05-25 Append-Concatenation Duplication - Root Cause + Fleet Inventory.md`). That record's path (A) — replace the Track A Obsidian plugin with this Tauri daemon — SHIPPED (every fleet main runs 0.4.41, MEASURED per host on 2026-09-05), and traded the plugin's in-file concatenation for whole-file `conflict-from-*` forks on the same class of file.

## Symptom (MEASURED by the Pitboss arranger seats, 2026-09-05)
- link: 14 forks `active-work.conflict-from-a6f8219e-…` (1.0–2.35 MB each, ~31 MB) of `02_Projects/Lattice/Pitboss/active-work.md`; `comm -23` found **78 flight-recorder lines present only in the forks** — appends the race dropped from the live file.
- trinity: 11 forks from `f2383e35` (34.4 MB); recurred from 2026-09-03.
- Each fork's device id is the host's OWN daemon.

## Mechanism (MEASURED from link's `journalctl --user -u nexus-vault-sync`)
Two entry points reach the same arm, `Materializer::write_with_change_seq` → `Decision::Conflict` (R4/R5):

```
2026-09-02T00:24:39Z WARN materializer CONFLICT (R4/R5): stashed local divergent revision BEFORE overwrite … stash=…active-work.conflict-from-a6f8219e-…-1003868951.md class=C
2026-09-02T00:24:39Z INFO push_client: 409 refetch/merge: VERIFIED read-receipt recorded … outcome=Stashed { … }
2026-09-02T00:24:39Z WARN push_client: push failed … reason=ConflictUnrecoverable { expected_hash: Some("71e920f3…") }
2026-09-02T00:24:44Z INFO push_client: push accepted path=02_Projects/Lattice/Pitboss/active-work.md hash=71e920f3…
2026-09-03T16:12:10Z WARN sse: CONFLICT, stashed local revision then materialized server winner … -1003876076.md
2026-09-03T18:17:07Z WARN materializer CONFLICT (R4/R5) … -1003876423.md   (via 409 refetch/merge, expected_hash 343c38c6…)
2026-09-03T18:17:12Z INFO push_client: push accepted … hash=343c38c6…
```

Reading: local = `head + appended lines`; the arm stashes LOCAL as the fork and overwrites the live file with the OLDER head; five seconds later the pending (lazy) push re-reads the file and pushes the head bytes back — the hash equals the 409's `expected_hash`. The appended lines now exist only in the fork. The device id in the fork name is the **stasher's**, not the head author's, so "the daemon conflicts with itself" was an inference from the filename (retired). A fork requires the head to differ from this host's last accepted push (otherwise R2 preserves local), i.e. a peer moved the head with different bytes — see the sibling ticket on icarus's non-owner re-pushes.

## Fix — shape (a) of the May record: LINE-APPEND MERGE
New pure module `src/append_merge.rs`, wired into the `Conflict` arm AFTER the causal gate, scope-suspect refusal and storm breaker, BEFORE the always-stash floor (Live mode only):

| arm | condition | action |
| --- | --- | --- |
| **A** `LocalSupersedesServer` | the head is a strict LINE-prefix of local (`is_strict_line_prefix`) | preserve local; record the head's `change_seq` as OBSERVED lineage (bytes verified); enqueue a compensating push, CAS base = head hash → `Skipped(AppendPreservedPushUp)` |
| **B** `Merged` | `sha256(common line-boundary prefix) == shadow` (the verified last-synced base) and BOTH sides appended | write `base + shared_run + server_tail + local_tail` (every line once) atomically, echo-guarded; observed lineage; compensating push → `Merged` |
| `ServerSupersedesLocal` | local is a strict line-prefix of the head | clean pull, no stash |
| `Unresolved` | anything else (edit inside shared history, deleted line, unrelated content, mid-line cut, shadow absent/stale for B) | **unchanged**: always-stash floor |

Shape (b) "skip CAS for host-owned files" was not needed (requires config + server change; (a) is lossless by construction and needs neither). Shape (c) "two same-host instances overlap" was excluded by the arrangers (disjoint roots).

## Tests
- `append_merge::tests` — pure-function table (prefix/boundary, verified base incl. unterminated base and shared-run backtrack, merge newline rules, dedupe, every negative).
- `materializer::tests::append_*` — the 78-line-loss replay on a SYNTHESIZED recorder-shaped fixture: ARM A (stale head) and ARM B (two appenders) with **0 lines lost, 0 forks, 1 compensating push**; requirement (3) negatives (shadow absent / stale-foreign → stash floor); edited-history negative → stash floor; Live-mode-only; local-prefix-of-head clean pull.
- `push_client::tests::cas_409_against_stale_head_prefix_preserves_local_no_fork_and_requeues` — end-to-end through the push leg with mockito: 409 → refetch → no fork → compensating push accepted on the next drain.
- `materializer::tests::append_replay_forked_recorder_fixture` (`#[ignore]`) — the same two shapes on a forked copy of the REAL recorder via `VAULT_SYNC_REPLAY_FIXTURE=… cargo test --lib append_replay_forked_recorder_fixture -- --ignored --nocapture`. The fixture is never committed (public repo).

## Residual risk, stated
ARM A prefers local when the head is a strict line-prefix of it. If a peer INTENTIONALLY truncated the tail of a note while this host appended, the truncated lines come back with the append (nothing is destroyed; the peer's truncation is superseded and the note carries both intents to the server's version history). For an append-only file this is the correct call; for general notes it is the conservative one. An edit anywhere INSIDE the shared history still takes the stash floor.
