# nexus-vault-sync (placeholder)

This repository is reserved for the **Nexus Vault Sync daemon** (Phase C of the [Nexus Vault Sync v2 design spec](https://github.com/CodeWarrior4Life/Nexus/blob/master/docs/superpowers/specs/2026-05-20-nexus-vault-sync-v2-rule-engine-scope-brat.md)).

The daemon will:

- LISTEN on the `vault_note_change` PostgreSQL trigger (payload v2 with `origin`, `lint_state`, `phase`).
- Implement the rule engine + skill resolver (read vault skill -> parse YAML -> cache in `skill_cache`).
- Implement per-subscriber scope filter (`scope_roots` + `scope_excludes`).
- Stamp `origin='reconciler-cypher'` (or env-driven `NEXUS_RECONCILER_ORIGIN`) on derivative writes.

**Status: empty placeholder.** Source code lands here when Phase C ships. Until then, this repo holds only this README and the URL reservation.

Created 2026-05-21 by Session 449 (S449).
