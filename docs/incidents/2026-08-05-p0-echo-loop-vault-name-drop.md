# P0 2026-08-05 — Silent Write Revert/Split Echo Loop (vault_name drop → mis-rooted daemon)

Repo-side pointer. **Canonical incident note (full detail, remediation phases, operator decisions):**
vault `02_Projects/Lattice/lattice-vault-sync/Incidents/2026-08-05 P0 — Silent Write Revert-Split
Echo Loop (vault_name drop → mis-rooted daemon).md`. Forensics: link `~/vault-sync-incident-20260805/`.

## Summary

- `pair_inner` (src/pairing.rs:57-68) rewrites config through `Config`, which has no `vault_name`
  field (config.rs:52-63) → every enroll/re-enroll silently drops `vault_name`.
- Root synthesis rule 2b (config.rs:119-137) then roots the daemon at `vaults_root` instead of the
  vault → all keys gain a `Mainframe/` prefix.
- Shadow/base_seq stores are keyed watch-root-relative → entire baseline orphaned
  (sync_shadow.rs:201 migration no-ops because `vault_folders` = sync-root basenames = `["vaults"]`,
  lib.rs:603-606). First divergent edit per path → R5 conflict → server head materialized as winner,
  fresh local bytes stashed to `*.conflict-from-<own-subscriber>` (materializer.rs:918-978,
  1446-1454). Silent revert; interleaved edits produce splits.
- Server `_strip_bare_vault_prefix` (nexus sync_routes_p1.py:582) silently stores `Mainframe/X` at
  `X` → SSE echoes bare keys → materializer writes them under `vaults_root` → stray vault tree +
  perpetual double-push echo (~5.5s rhythm). No stale prefixed doc-space exists server-side; the
  bare space is canonical.
- Collateral: 22,578 Mainframe files tombstone-renamed `*.deleted-…` on Aug 4 13:43Z (replay loop:
  181k `nothing to delete` lines); 39,049 `_skeleton-test/` rows mass-pushed to the server; link 641
  sidecars (133 content-at-risk), trinity 10,992 (6,670 divergent — frontmatter-strip class).
- v0.4.36 (running AppImage) has no tag/branch in this repo — unreproducible build.

## Containment (2026-08-05)

Daemons stopped: link (systemd unit disabled + **masked**; unit file →
`~/.config/systemd/user/nexus-vault-sync.service.DISABLED-P0-20260805`), trinity (launchd
`com.lattice.vaultsync-daemon` disabled). icarus left running (server-side read_only). Explicit
`[[sync_roots]]` pinned on all three hosts (backups `.bak-P0-echo-20260805*`).

## Fix queue (Phase 3 of the incident note — before any restart)

1. `pair_inner` round-trips `RawConfig` (preserve unknown fields).
2. Enroll guard: key-shape preflight fail-closed; refuse rule-2b synthesis when `vaults_root`
   contains a subdir matching the server bare vault name.
3. Identity-invariant baseline keying (server storage key or in-vault `.vault-sync-root` marker).
4. Park gate on pathological shadow-orphan rate (extend `detect_vault_scope_suspect`).
5. Server circuit breaker replacing silent prefix strip (contract I35, currently MISSING).
6. No-op push gate + tombstone cursor gate (change_seq ≤ high-water ⇒ refuse).
7. Resolve v0.4.36 provenance; ship as tagged build.

Contract refs: I29, I35, I37, I64; incident N4 (2026-06-02). Prior related:
`2026-07-18-conflict-storm-root-cause-v0432.md` (same vault_name disease, earlier trigger).

## Closure (2026-08-06)

RESOLVED by v0.4.37 (merge 5cd67f3, tag v0.4.37): enrollment round-trips the on-disk config via
`toml::Table` merge (never drops `vault_name`), plus the fail-closed mis-root refusal guard.
Fleet restarted (link/icarus/trinity) after per-host shadow-store reconciliation; convergence
proven byte-identically: 41,846/41,846 four-way sha256 hash-tree (3 hosts + server), six-way
canary round-trips in 15s, zero conflict stashes, idempotent passes. Full closure record with
evidence and D1–D4 resolutions: vault
`02_Projects/Lattice/lattice-vault-sync/Incidents/2026-08-05 P0 — Silent Write Revert-Split Echo Loop (vault_name drop → mis-rooted daemon).md`.

Known follow-ups filed: reconcile-batch drift⇒push has no pull direction (multi-writer ping-pong
hazard); case-fold tombstone loop class (fixed for family.md, needs a general guard); Nexus Sync
product rename migration (post-soak).
