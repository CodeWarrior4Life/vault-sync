# vault-sync

**Nexus Sync** — desktop daemon for a **multi-root folder sync**: it watches a list of `[[sync_roots]]` (`{path, route, subscriber_id}` in `~/.config/nexus-vault-sync/config.toml`), pushes route-relative wire paths, and materializes Postgres-canonical content back to the local filesystem on subscribing hosts (Win / Mac / Linux). Pairs with Nexus via the admin UI at `https://nexus.obsidian-inc.com/admin/vault-sync`.

## Architecture: multi-root by design (read this before concluding anything from names)

The 2026-05-29 contract rebuild (Nexus repo `docs/superpowers/plans/2026-05-29-nexus-sync-contract-implementation.md`) deliberately replaced the vault-era `vaults_root`+`vault_name` config with `sync_roots: Vec<SyncRoot>`, made wire paths route-relative, and renamed the product **Nexus Sync**. The Mainframe Obsidian vault is the one root registered today — **by intention, not by architecture**. "Vault" in identifiers across this repo (crate name `vault-sync-daemon`, config dir `nexus-vault-sync`, struct fields, tray strings, table names quoted from the server) is naming residue from the pre-contract product; those identifiers are load-bearing (systemd units, launchd labels, updater endpoints, enrollment) and renaming them is a **behaviour change** — do not "clean them up".

Known implementation gaps vs the multi-root design (catalogued with evidence in Nexus repo `docs/audits/2026-08-08-vault-centric-assumptions/AUDIT-REPORT.md`): the extension allowlist is a hardcoded `.md`/`.canvas` gate at three stages (see `SYNC_CONTRACT.md` I55 — daemon layer, not a server or contract property); SSE materialization, reconcile pulls, backfill, redflag gate and shadow seeding currently serve `sync_roots[0]` only — **running with more than one root is unsafe until the multi-root completion ticket lands**. Wire-route authority is the server-side subscriber registration, not the `SyncRoot.route` field.

This repo houses the **Tauri desktop daemon only**. The server-side cache writer and reconciler live in the main `obsidian-nexus` repo. Container image at `ghcr.io/codewarrior4life/nexus-vault-sync` is the cache-writer (separate artifact, separate repo).

See vault `[[2026-05-25 S466 Vault-Sync v2 Phase E2 - Tauri Daemon Scaffold + Subscriber Registry - Spec]]` for the full design.
