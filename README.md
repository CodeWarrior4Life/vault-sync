# vault-sync

Lattice Vault Sync — desktop daemon that materializes Postgres-canonical vault notes to the local filesystem on subscribing hosts (Win / Mac / Linux). Pairs with Nexus via the admin UI at `https://nexus.obsidian-inc.com/admin/vault-sync`.

This repo houses the **Tauri desktop daemon only**. The server-side cache writer and reconciler live in the main `obsidian-nexus` repo. Container image at `ghcr.io/codewarrior4life/nexus-vault-sync` is the cache-writer (separate artifact, separate repo).

See vault `[[2026-05-25 S466 Vault-Sync v2 Phase E2 - Tauri Daemon Scaffold + Subscriber Registry - Spec]]` for the full design.
