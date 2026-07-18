# OWNER ACK — ALL THREE UN-GATE CONDITIONS (2026-07-18 16:05 EDT, BINDING, no proxies)

**OPERATOR VERBATIM (owner ACK of the parked legs):** "my only requirement is that we don't lose data. beyond that, do what needs to be done. thats why everything starts with a back up by all subscribers."

**(1) PG vault_notes strip status — stated by the incident lead (nexus-9cb6, dedicated sync session, who EXECUTED the strip): COMPLETE.**
Agreed legit-carrier residue = **6 rows** (Wiki-Linker Design Spec x3 copies + Wiki-Linker Plan x3 copies; sentinels inside fenced code blocks, fail-closed correctly). The earlier "3" figure was pre-discovery intel; the sweep also cleaned 160 END-only chunk-boundary rows that intel missed. Live counts BEGIN=6 END=6. Backup: 8,920 rows x2 hosts (cypher + link `~/backups/vault_notes_sentinel_backup_20260718.jsonl`). 5/5 random spot checks PG==FS byte-equal.

**(2) Incident-lead restart sequencing (granted by nexus-9cb6):**
LINK FIRST via `ready-to-run/link-install-start.sh` (honor the FUSE-child kill-by-/proc/*/exe procedure) → confirm "shadow store: migrated keys" line count==1 + version 0.4.32 + 15 min zero "CONFLICT (R4/R5)" mints → THEN TRINITY: execute R6 quarantine FIRST (move out of vault, MANIFEST, delete NOTHING), then `trinity-install-start.sh` (launchctl load -w + kickstart), same verification → full 30-min soak + R5 parity probes → R8 close.

**(3) Owner ACK:** given above, operator verbatim. Both disclosures accepted as benign; no rollback required.

**BINDING CONSTRAINT: ZERO DATA LOSS.** The R2 backups (your own resume leg verified them intact) are the rollback path. Never delete or overwrite anything not restorable from them. On ANY anomaly (conflict mint during soak, parity mismatch): STOP the affected daemon and re-park with evidence.

Same text persisted in the ticket body (verified) as `OWNER RESPONSE (2026-07-18 16:05 EDT, BINDING)`.

## ADDENDUM (16:55 EDT): trinity anomaly verdict — PROCEED
Data audit: ZERO loss (300-sample: 213 byte-equal, 86 never-in-PG, 1 pre-existing live-note lag). Remedy applied: vault_name = "Mainframe" added to trinity config (backup .pre-v0432-vaultname.bak). Final legs: reinstall plist only (app already 0.4.32; do NOT re-run the buggy dmg-mount block), load -w + kickstart, verify migration line fires NONZERO, joint 30-min soak, R5 parity, R8 close. Zero-mint or re-park.

## ADDENDUM 2 (17:20 EDT): wave-2 audit CLEAN — FINAL PROCEED
200/200 sampled wave-2 pushes byte-identical to pre-deploy canonical snapshot. Zero regressions. Anti-strip refusals = S513 designed healing. Link restarted. Execute final legs: trinity plist start (migration NOT expected to re-fire — already migrated), joint 30-min soak (zero mints hard gate + push-volume decay gate; ping-pong = stop + re-park), R5 parity, R8 close.
