# ready-to-run — OWNER-GATED resume scripts (TKT-8a70148c)

These execute the **parked** legs of the v0.4.32 deploy. Do **NOT** run any of them
until BOTH gate conditions are met (see BURN_REPORT.md "STATUS"):

1. PG `vault_notes` confirmed stripped to the agreed legit-carrier residue, AND
2. the incident lead ACKs restart sequencing.

Order:

1. `link-install-start.sh`   — swap AppImage, unmask+restore unit, start, verify (link)
2. `trinity-install-start.sh`— install 0.4.32 .app from dmg, install launchd agent, start (trinity)
3. soak 30 min on both; watch for `shadow store: migrated keys` (once) + zero `CONFLICT (R4/R5)`
4. `quarantine_conflicts.py` — run ON trinity to move the 4,247 conflicts out (reversible)
5. `parity_probe.md`         — Verified Parity Protocol, both directions, both hosts
6. close: PATCH TKT-86ae42a3 resolved + TG via `~/whetstone/notify.sh`

Rollback for every step is in BURN_REPORT.md "Rollback". Backups (R2) already exist on both hosts.

Discovered facts baked into these scripts:
- link subscriber:    `a6f8219e-2fcb-4a9a-a2c6-0d3471919d1c`
- trinity subscriber: `f2383e35-2e9d-4da2-b5ed-de8a35778fa3`
- link unit backup:   `~/.config/systemd/user/nexus-vault-sync.service.incident-paused-20260718`
- trinity app binary: `/Applications/Nexus Vault Sync.app/Contents/MacOS/vault-sync-daemon` (bundle id com.lattice.vault-sync, currently 0.4.31)
- staged artifacts:   `link:~/vault-sync-v0432-staging/`
