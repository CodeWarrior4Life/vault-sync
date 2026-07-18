---
type: incident
created: "2026-07-18T15:10:00-04:00"
tags: [nexus-sync, vault-sync, incident, p0, conflict-storm]
status: resolved
ticket: TKT-86ae42a3
session: nexus-9cb6
related:
  - "[[2026-07-15 Vault-Sync Conflict Storm (S553) — Incident + Containment]]"
  - "[[Nexus Sync Hardening - Charter Seed]]"
summary: "Conflict-file storm (3,153 on link, 4,247 on trinity) root-caused to the v0.4.28 B2 path-shape cutover orphaning the ShadowStore key namespace; fixed in v0.4.32 (shadow-key migration + conflict-storm circuit breaker); PG D-8 sentinel contamination stripped no-loss via S4 choke-point."
---

# 2026-07-18 Vault-Sync Conflict Storm — Root Cause + Fix (TKT-86ae42a3)

## Timeline of damage
- Conflict files by creation day (link): 07-15 = 96, 07-16 = 483, 07-17 = 7, 07-18 = 2,422 (2,876 by exact `.conflict-from-` pattern at quarantine time). Trinity: 4,247 total, 745 on 07-18 alone.
- Obsidian unlaunchable on link (conflict files + 121,696-file vault).
- Purged "Obsidian Nexus" trees reappearing under `02_Projects/Nexus/Legacy/Consolidation 2026-07-16/` (server-side copies re-pulled; no delete propagation in the daemon).

## ROOT CAUSE (file:line)
The v0.4.28 "B2" cutover moved every daemon pipeline from vaults-root-relative paths (`Mainframe/01_Notes/x.md`) to sync-root-relative paths (`01_Notes/x.md`) — `lib.rs:909-911` — but **never migrated the ShadowStore keys**. The store on link held 30,991 legacy prefixed keys (the entire pre-B2 sync history) that post-B2 lookups could never hit.

Decision chain per divergent path: `materializer.rs decide()` — `!shadow_present => Decision::Conflict` (R5, materializer.rs:1145-1146 at v0.4.31) → `write_stash(...)` mints `<stem>.conflict-from-<device>-0.md`. Journal fingerprint: 2,395 `materializer CONFLICT (R4/R5)` events on 07-18, every one `shadow_present=false`, `change_seq=0` (the reconcile/verify_repair entry point).

The storm needed a mass server-side divergence event to detonate:
- **07-16 (483):** the folder consolidation — moves seen as delete+create; server-side rows for old paths re-pulled (no delete propagation), new local paths had no shadow entry.
- **07-18 (2,422):** D-8 wiki-linker managed-region sentinels contaminated 8,588 PG `vault_notes` base rows; link FS was sentinel-clean (email-standup strip) → verify_repair classified thousands as stale-local → server-wins pull → R5 mint per path, overwriting clean local files with sentinel-bearing bodies, which the FS strip re-cleaned → the acceleration loop.

The D8 NFC key migration (`sync_shadow.rs`) was the exact precedent for this bug class (key-form mismatch → shadow miss → storm), sitting one abstraction away from where B2 needed the same migration.

## Containment (done, 2026-07-18)
- link: `nexus-vault-sync.service` stopped + masked 14:06 (S535 pause, D-8 incident). 0 new conflicts since.
- trinity: daemon killed + unit masked ~14:20 by this session (was still minting).
- cypher server vault: 0 conflicts. zion/megacity: no daemons. switch: unreachable (asleep). neo: offline.

## Obsidian mitigation (done)
- 2,876 `.conflict-from-` files moved OUT of the vault to `~/.local/share/Nexus/quarantine/conflict-storm-2026-07-18/` (relative tree + `MANIFEST.txt` + README with exact reverse procedure). Nothing deleted — some 07-15/16 stashes may hold genuine local edits; dedupe pass is owner-gated.
- `userIgnoreFilters` gained `/\.conflict-from-/` and the 06-22 quarantine dir.

## PG sentinel strip (done, zero-loss)
- Backup: 8,920 rows / 95 MB jsonl ×2 hosts (`cypher:~/backups/vault_notes_sentinel_backup_20260718.jsonl` + same on link). 160 rows MORE than the prior intel sweep — END-only chunk-boundary halves don't match a BEGIN LIKE filter.
- Applied via the S4 choke-point `_write_note_body_canonical` for 8,582 base rows (I14 constraint requires it); 323 chunk pseudo-rows raw-guarded-updated (outside the `.md` trigger predicate); 9 fence-parity chunk rows textually cut.
- Residue = 6 legit documentation carriers (Wiki-Linker Design Spec ×3 copies + Plan ×3 copies, sentinels inside fenced code blocks, fail-closed correctly).
- 5/5 random spot checks PG == FS byte-equal.

## Fix (v0.4.32, merged + tagged)
Repo `vault-sync`, PR #6, merge 1e2ee68, tag `v0.4.32`:
1. **B2' shadow-key migration** (`sync_shadow.rs`): `load_with_vault_folders()` strips known sync-root basenames off legacy keys on load (one-time, mirrors D8 NFC migration); `get`/`record` are shape-invariant; current-era values win on collision (legacy gap-fills only — a stale value degrades to the always-stash floor, never silent loss).
2. **Conflict-storm circuit breaker** (`materializer.rs`): max `conflict_storm_threshold` (50) R4/R5 stash mints per sliding window (600 s); past it, Conflict writes are refused fail-closed toward local (`Skipped(ConflictStormBreakerOpen)` — no stash, no overwrite). A mass server-side divergence event can never again mint thousands of files.
3. 6 new regression tests; the E2E storm test (`b2_prefix_migrated_shadow_prevents_r5_conflict_storm`) is red on pre-fix code. 427 tests green; clippy + fmt clean (also unblocked CI, red fleet-wide since a runner toolchain update).

## Deploy (whetstone burn, unattended per operator directive)
Operator (2026-07-18): "once you've root caused and have the fix (which includes complete install and running on link and trinity, unattended), launch a whetstone burn to get it done and keep me posted on tg. no performative approval gates that make no sense. be prudent. use the backup tool per subscriber first if risk of data loss."
Burn: install v0.4.32 AppImage on link + trinity, backup-first (AppImage + `shadow_hashes.json` + vault snapshot), unmask + start, verify migration log line + version self-report + 30-min zero-mint soak + Verified Parity Protocol probes, quarantine trinity's 4,247 conflicts, then resolve TKT-86ae42a3.

## Rollback
Stop daemon → restore prior AppImage from backup → restore pre-migration `shadow_hashes.json` → re-mask. PG strip rollback: restore bodies from the 8,920-row jsonl backup via the same choke-point.

## Residual / follow-ups (fold into Nexus Sync Hardening charter)
- Delete propagation: local deletes never reach the server; purged trees resurrect via pull. (Charter item.)
- Vault-scope separation: 121,696 files (42K `_resources`, 21K `_backups`) choke Obsidian's indexer; recommend corpora move out of the Obsidian-visible tree. (Recommend-only, R6.)
- Dedupe/delete of quarantined conflict stashes (link 2,876 + trinity 4,247): owner-gated; verify stash bytes equal live/server before any deletion.
- Windows CI: pre-existing `canonical_form` golden-vector failure (CRLF); windows release job also failing since v0.4.31.
- Multi-sync-root shadow keying: unprefixed keys from two roots could collide (documented limitation; fleet is single-vault today).

## DEPLOY CORRECTIONS (BINDING for burn opfix-vaultsync-v0432-deploy / TKT-8a70148c — supersedes R3 platform detail)
1. **Trinity is macOS Apple Silicon (Darwin), NOT Linux.** Asset = darwin-aarch64 `.app.tar.gz`, install target `/Applications/Nexus Vault Sync.app` (cyril:admin, no sudo needed). Backup = APFS clone (`cp -c`). Service = launchd agent `com.lattice.nexus-vault-sync-daemon`: use `launchctl load -w` (NEVER `bootstrap` — EIO(5) from non-GUI SSH) + `launchctl kickstart`. No systemd on trinity; R3's "install S535-style systemd unit" applies to LINK-class hosts only. Verified 2026-07-18 15:25: no daemon process running on trinity, 0 new conflicts since 14:30.
2. **Link deploy trap (2026-06-14, cost 2 silent non-deploys):** the AppImage's FUSE child escapes the unit cgroup and survives restarts; the NEW instance exits via the tauri single-instance plugin (logs "starting version=X" then dies) → old version silently keeps running. Procedure: `systemctl --user stop nexus-vault-sync` → kill any survivor by `/proc/*/exe` path (NEVER `pkill -f vault-sync` — self-match, exit 144) → verify ZERO vault-sync exes remain → install → unmask → start → CONFIRM the running `/proc/*/exe` is the NEW binary AND the v0.4.32 "shadow store: migrated keys" line appears (count==1 daemon).
3. **Dispatcher verify.cmd caveat:** the seeded verify command uses `systemctl --user` over ssh for trinity — invalid on Darwin. If the mechanical verify fails ONLY on that, the deploy may still be good; re-verify trinity with `pgrep -f -i vault.sync` + conflict-count 0 + version self-report, document in BURN_REPORT.md, and park with that evidence rather than rolling back.
4. Trinity paths: vault `~/vaults/Mainframe` (confirmed live); quarantine target `~/.local/share/Nexus/quarantine/` may not exist on macOS — use `~/Library/Application Support/Nexus/quarantine/conflict-storm-2026-07-18/` if the XDG path is absent, record which.

## RESOLUTION (2026-07-18 17:45 EDT)
Both hosts live + verified on v0.4.32. Trinity needed two extra rounds: (1) config drift — `config.toml` missing `vault_name = "Mainframe"` disabled the B2' migration (fixed, backup kept; v0.4.33 hardening filed: guard/park when vault_folders is empty but the store holds prefixed keys); (2) stale-replica catch-up pushes (2,326 + 2,858) — audited 620 samples across all waves against the pre-deploy snapshot: ALL byte-identical, zero regressions (anti-strip refusals = S513 designed healing). Verified Parity Protocol PASSED both hosts (push+pull byte-exact, sha256-matched probes, cleaned after; daemon delete-propagation confirmed live). Tickets TKT-86ae42a3 + TKT-8a70148c resolved. Whetstone ops notes: burn legs die at wait-points (4 legs; incident lead bridged the gates inline); tickets API ignores `body_append` (use full-body PATCH); trinity launchd label = `com.lattice.vaultsync-daemon` (not the plist filename the burn drafted); link daemon self-report → HTTP 405 (server route, open); link weld timer was found INACTIVE and re-armed (TKT-82a8bb2c interim still required on 0.4.32).
