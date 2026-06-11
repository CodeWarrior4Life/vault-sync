# BURN REPORT — S1-A Sync Contract v1 (TKT-b9cbe732)

**Status:** DELIVERED — awaiting owner M2 review (acceptance gate "defined and AGREED": defined ✅, agreement pending)
**Branch:** `whetstone/S1-A`
**Anchor:** Master Plan A0.4.1 [Rank 1] / A0.5 Lane S / §10.3 Wave 1
**Mode:** owner | **Model:** claude-fable-5 | **Session:** burn-S1-A (conductor)

## Deliverable (shipped)

**Sync Contract v1** — every implicit invariant of the Nexus↔vault-sync protocol
as numbered invariants **I1..I82**, with code evidence (file:line, both repos),
the 5-row PUSH/PULL decision table (I37), an explicit catchup-guarantee statement
(I22), an invariant→test traceability table (38 ✅ / 27 ◐ / 17 ❌ MISSING), an
incident→invariant index (N1..N11, the S1-B golden-scenario corpus), and an
8-item POLICY review queue for the owner (M2).

Triple Write, vault FIRST:
1. **Vault (canonical, full content):** `02_Projects/Nexus/Specifications/2026-06-11 Nexus Sync - Sync Contract v1 (S1-A).md`
2. **Memory:** project memory `s1a-sync-contract-v1.md` + MEMORY.md index line
3. **Repo docs:** `docs/SYNC_CONTRACT.md` (this branch)

## Method

3 parallel read-only extraction agents (daemon Rust source 15,271 lines / server
sync surface `sync_routes_p1.py` + `services/vault_sync/*` / vault incident corpus
2026-05→06) → main-context synthesis from summaries only. This kept the authoring
leg inside one context window after legs 1-2 died loading sources directly.

## Acceptance gate status

- Sync contract **defined**: ✅ (I1–I82 + traceability per A0.4.1/A0.5 expected exit state)
- **Agreed**: ⏳ owner M2 review of the 8 POLICY invariants (spec §16: I32 ServerWins,
  I39 stash matrix, I40 echo TTL, I44 journal cap, I48 retry bounds, I50 storm
  threshold, I71 recon cadence, I74 24h tombstone retention — incl. the flagged
  offline->24h delete gap). Conductor message sent to owner; session parked awaiting reply.
- **Gates S1-B**: scenario seed list = §17 MISSING rows (17) + §18 incident corpus
  (N1..N11), keyed 1:1 to invariant IDs. S1-B authoring can start on agreement (6/12).

## Explicitly deferred (per retry-note scope reduction)

- **M3 distribution** (Opus, 6/12 per A0.4.1): nexus-repo docs copy, README refs
  in both repos, lattice-pvd-preflight wiring, PR-template invariant-citation rule.
  D4 forbids touching the nexus live tree from this worktree.
- Tombstone-vs-offline>24h resolution (owner decision; S1-B/S1-H follow-up).
- Dedicated enrichment-pipeline contract (S4 lane candidate).
- Machine-checkable JSON-Schema wire files (S1-B may generate from spec).

## Repo assignment check (per launch-note caveat)

Ticket repo "nexus / vault-sync" — CONFIRMED correct for S1-A: the contract spans
both repos; spec authored from this vault-sync worktree; nexus docs/ copy left to
M3 (D4: never touch other live trees from a burn).

## SESSION LOG

- **leg 1** (prior context window): no commits, no BURN_REPORT, no resume file found. Treated as lost; restarted.
- **leg 2**: committed only the breadcrumb skeleton before hitting context; extraction agents never persisted output.
- **leg 3 (2026-06-11)**: session named `burn-S1-A`. Re-confirmed sources; found prior art `2026-05-29 Nexus Sync - Sync Contract Design.md` (approved S480) — referenced, not superseded.
- **leg 3**: 3 extraction agents complete (daemon 54 candidates / server ~70 / incidents 11 + S498 guardrails).
- **leg 3**: spec authored + Triple Written (vault FIRST, then memory, then `docs/SYNC_CONTRACT.md`). Files: vault spec note, memory `s1a-sync-contract-v1.md`, `docs/SYNC_CONTRACT.md`, BURN_REPORT.md.
- **leg 3**: committed; conductor notification to owner for M2 review; parked awaiting owner verdict (no auto-merge, D4).
