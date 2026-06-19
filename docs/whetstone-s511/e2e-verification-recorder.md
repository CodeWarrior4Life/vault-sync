# Nexus Sync Daemon E2E Bake — S511 Flight Recorder

## Baseline (verified before any action)
- Server: sha-16029834e, /api/health=200
- Daemon binary: /Users/cyril/Dev/vault-sync/src-tauri/target/release/vault-sync-daemon (23MB, built Jun 19 12:18)
- Subscriber f2383e35: materializer_mode=shadow (server-side, confirmed via psql)
- Real vault: 35,544 .md notes, 0 conflict-from files, no _sync-probe-s511 dir
- Shadow tree: 14,398 notes (pre-seeded), 0 conflict-from copies
- last_event_id=1003033167, token file present (43 bytes), push_journal empty (not wedged)

## SAFETY: only touch _sync-probe-s511/. Stop+FAIL on any other-note revert.

## PHASE A — SHADOW BAKE: PASS
- Daemon v0.4.22 started shadow mode 16:23:50, token via FILE fallback (no Keychain hang).
- SSE resumed from last_event_id=1003033167, drained backlog to head (gap closed 559→36).
- 380 R4 CONFLICTs (all shadow_present=true): stale Jun-8 shadow bookmarks vs current server. 1:1 unique paths↔files↔log lines (NO loop, deterministic change_seq naming).
- R2 fired organically (PerimeterScope notes): "local edit diverges, server unchanged, PRESERVING local, NOT overwriting" — the T1-fix behavior, live.
- SAFETY: real vault 35,544 (unchanged), 0 conflict-from in real vault, 0 daemon writes to real-vault paths, all stashes into shadow tree. key-manifest.md -mmin match = GDrive inode churn (content mtime 12:21, 4h pre-daemon, never logged/shadowed).
- 0 panic, 0 ERROR. Shadow grew 14,398→24,078.

## PHASE B — LIVE MODE: FAIL (avalanche defect; data-safe but ships vault pollution)
- Flipped subscriber f2383e35 -> live. Daemon restarted live 16:29:25, resumed last_event_id=1001797154 (live position, earlier than shadow's).
- R1 skip=339 (identical NOOPs, correct). R2=0. 
- DEFECT 1 (R5 avalanche): 1150 conflict-from files spawned in REAL vault, 920 under 04_Entities/Individuals — a subtree NOT in the shadow store (14,398 seeded, Individuals absent) -> every note hits R5 (shadow==None) with change_seq=0 -> deterministic-name dedup collapses (all suffix '-0') -> avalanche. This is the D9 conflict-copy avalanche D9 was meant to prevent; D9 seed didn't cover this subtree in live mode. Stopped daemon at 1151 to halt it.
- DEFECT 2 (under-logging / I-83): only 1 'materializer CONFLICT' warn! line in log vs 1151 conflict files on disk. The avalanche stash path does NOT warn!-log each stash (.tmpR5MHeg temp writes in Individuals confirm a non-decide() write path). I-83 requires warn! on every preserve-before-overwrite.
- DATA SAFETY (no hard-bound-#2 violation): 0 missing canonicals; 1150/1151 conflict copies BYTE-IDENTICAL to intact canonical (pure redundant pollution); 200-bookmark canary UNCHANGED (no revert of pre-existing real notes). 1 conflict (@TMZ UFO X-post, change_seq=1001939159, shadow_present=false) is GENUINE divergence: loser=local w/ full frontmatter (1119B), winner=server frontmatter-STRIPPED (635B) -> server still serves stripped body for some notes; R4 correctly preserved the richer local copy (I-83 honored for THIS one).
- DID NOT run T1/T2/T3: live mode unsafe; would add more pollution. Reverting sub->shadow, cleaning 1150 identical copies, PRESERVING the 1 genuine divergent conflict.

## CLEANUP + FINAL STATE
- Subscriber reverted live->shadow (confirmed).
- Daemon OFF (pkill, confirmed no process).
- Deleted 1150 byte-identical redundant conflict-from from real vault; restored the 1 genuine divergent note (@TMZ UFO) to its pre-test 1119B content (full frontmatter) and removed its stash.
- Real-vault conflict-from = 0 (back to baseline). No daemon temp leftovers. 200-bookmark canary byte-identical to pre-test.
- No _sync-probe-s511 notes ever created (correctly aborted before T1/T2/T3 once live avalanche surfaced) — nothing to clean on local OR server.
- Vault count 35,546 vs initial 35,544 read = +2 benign external churn (GDrive/brain ingest during the test window; 0 .md born in either run window by birthtime; not daemon-created).
- ADDITIONAL LOG FINDINGS on @TMZ note: D5 conflict-copy exclude WORKS (DropExclude exclude_rule="conflict-copy" — copies not re-pushed/re-fanned). Push path hit CAS-409 -> stashed (S511 D4) -> ConflictUnrecoverable (note could not converge up; data-safe but non-converging).

## CORRECTION (post-analysis) — source of the 1150 conflict files
- The 1150 real-vault conflict-from came predominantly from the PUSH path, not the materialize path:
  - push_client CAS-409 stashes (warn!-logged, S511 D4): 1150
  - ConflictUnrecoverable push failures: 1150
  - materializer CONFLICT (warn!): 1
- So "Defect 2 under-logging" is RECANTED: the push-path stashes ARE warn!-logged (1150 lines). The 1-vs-1151 mismatch I flagged was materializer-only count; push path logs its own 1150. Logging law (I-83) substantially HONORED.
- TRUE root: notes where local≠server-hash AND shadow==None. On live start the daemon (a) cannot push them up (server CAS-409, expected_hash mismatch → ConflictUnrecoverable) and (b) stashes losing local bytes each time → 1 conflict-from per note → avalanche. Data-safe (canonical intact, loser preserved) but NON-CONVERGING and pollutes the real vault with ~1150 files.
- Why CAS-409: server body for these diverges from local (e.g. @TMZ note: server=frontmatter-stripped 635B vs local 1119B). The push expected_hash never matches → unrecoverable.
