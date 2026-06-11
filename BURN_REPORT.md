# BURN REPORT — S1-A Sync Contract v1 (TKT-b9cbe732)

**Status:** IN PROGRESS (leg 2 — leg 1 hit context window without persisting work; restarted from scratch)
**Branch:** `whetstone/S1-A`
**Anchor:** Master Plan A0.4.1 [Rank 1] / A0.5 Lane S / §10.3 Wave 1
**Mode:** owner | **Model:** claude-fable-5

## Deliverable
Normative Sync Contract v1: every implicit invariant of the Nexus↔vault-sync
protocol extracted into numbered invariants I1..In + traceability table
(invariant → existing-or-MISSING test). Triple Write, vault FIRST.

## SESSION LOG
- **leg 1** (prior context window): no commits, no BURN_REPORT, no resume file found. Treated as lost; restarted.
- **{T0}** Leg 2 start. Reconstructed context: machine_config, Whetstone launch note, Master Plan A0.4.1/A0.5 (at `~/whetstone/docs/`). Located sources: daemon `src-tauri/src/*.rs` (15,271 lines, this worktree), server `~/projects/Nexus/server/nexus/api/sync_routes_p1.py` + `services/vault_sync/` + reconciler.
- **{T0}** Fanning out 3 extraction agents (daemon / server / incident notes). Files: BURN_REPORT.md (this).

## Repo assignment check (per launch-note caveat)
Ticket repo "nexus / vault-sync" — CONFIRMED correct for S1-A: the contract
spans both repos; spec authored from this vault-sync worktree, with the nexus
repo `docs/` copy left to M3 distribution (Opus, per A0.4.1 milestones) since
D4 forbids touching other live trees from this burn.
