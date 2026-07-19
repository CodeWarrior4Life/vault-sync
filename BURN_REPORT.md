# BURN_REPORT -- TKT-9d927317 / icarus-vault-sync-subscriber

**Ticket:** TKT-9d927317
**Burn:** icarus-vault-sync-subscriber (Operation Whetstone)
**Title:** Icarus build-out 5/5: enroll icarus as Nexus Sync subscriber with live Mainframe copy (v0.4.32 fixed daemon ONLY)
**Branch (this worktree):** `whetstone/icarus-vault-sync-subscriber`
**Repo:** vault-sync (Tauri/Rust daemon) -- worktree correctly bound (full `src-tauri/src/*.rs` present)
**Reviewed commit:** `60766af` (v0.4.32 fix wave HEAD); tests added at `20c87c7`
**Burn host:** link. **Target:** icarus (`cyril@100.70.246.59`).
**Written:** 2026-07-19 01:38 EDT
**Spec anchor:** `02_Projects/Nexus/_Children/vault-sync/Specifications/2026-05-20 Nexus Vault Sync - Unified Design Spec v2 - Rule Engine + Scope + BRAT.md`

---

## Status: PARKED AWAITING-OWNER (D8 hard gate)

The GOAL (icarus becomes a live subscriber) is, by definition, a **deploy +
subscriber registration + distribution** -- every part of it is on the burn's
OWNER-GATED list (D8). So this burn did all the REVERSIBLE work (review, runbook,
config template, regression/guard tests, read-only state capture) and PARKED
before the irreversible enrollment. icarus was never mutated; only read.

Two independent conditions make parking not just procedural but SUBSTANTIVE -- it
would be unsafe to enroll icarus right now even if D8 allowed it:

1. **Fleet reconcile-direction is contested.** link's v0.4.32 deploy fixed the
   shadow-key storm but trinity re-detonated a mass-push as a long-offline STALE
   REPLICA (2326 `push accepted` via the anti-strip / R2-preserve-local path), and
   link is propagating it (TKT-8a70148c BURN_REPORT; my memory
   `[[v0432-trinity-vault-name-configdrift]]`). PG is a CONTESTED canonical. A
   fresh subscriber backfilling from it inherits the contest.
2. **THESEUS adversarial review (2026-07-19, `vaults-d0ba`)** declares the Nexus
   Sync gate NOT closeable: six active conflict artifacts remain
   (incl. `CLAUDE.conflict-from-*`), live link reconcile logs `requested=2 pulled=0`
   with failures yet FALSELY reports files_in_sync, long-path/response-decode pulls
   fail persistently.

**Owner action (one line):** Clear the reconcile-direction + THESEUS blockers (or
decide icarus enrolls PULL-ONLY seeded from link's on-disk vault, not PG), then run
`docs/icarus-vault-sync-runbook.md` sections 1-6; STOP on any icarus-attributed conflict.

---

## Requirement Review Table

Every row cites real code/config/state at the reviewed commit. "GAP" = does not
conform; "PREP" = reversible prep complete, live execution owner-gated; "UNVERIFIED"
= cannot verify without the owner-gated enrollment.

| Req | Evidence (file:line / command) | Verdict | Notes |
|---|---|---|---|
| **R1** Mirror link's known-good v0.4.32 install | link: `~/.config/systemd/user/nexus-vault-sync.service` (user unit) + `.d/10-desktop-env.conf`; binary `/home/cyril/Applications/Nexus-Vault-Sync.AppImage` (84384248 B, sha256 `8a305ee8...`, mtime Jul 18 15:50); config `~/.config/nexus-vault-sync/config.toml`; `src-tauri/Cargo.toml:3` version="0.4.32"; `tauri.conf.json` version 0.4.32 | **PREP / CONFORMS** | Full parity table + exact-artifact copy (NOT `install.sh` latest-tag) in runbook s1. Version proof = journal `version="0.4.32"`; on-disk `daemon_version` field is a stale self-report ("0.4.20" on link) and must not be trusted. |
| **R2** Subscriber registration via server API; token -> 0600 config only | endpoint `POST /api/sync/subscribers/issue-token`; admin key name `NEXUS_API_KEY` present in `~/whetstone/.env` (name only, value not read); token path `token_store.rs:115-118` (`token-<subscriber_id>.bin`), mode `token_store.rs:123-133` `set_mode(0o600)`; pairing `pairing.rs:46-68` | **PREP** | Executing the POST mutates server state = owner-gated. Runbook s2 has the exact command (names only). link layout confirms design: `config.toml` 0644 (no secret) + `token-*.bin` 0600. |
| **R3** Vault target `/var/home/cyril/vaults/Mainframe`; backfill rails; STOP on conflict mint | icarus read-only: vault ABSENT, `/var/home` nvme1n1p3 930G/880G free; footgun `config.rs:119-137` (empty sync_roots + no vault_name -> bare-parent root); test `test_r3_vault_name_synthesis.rs` | **PREP** | Space ample (R3 said FireCuda 916G; actual nvme1n1p3 880G free -- confirm intended disk). vault_name=Mainframe MANDATORY (footgun locked by 3 passing tests). Backfill rails + before/during/after capture in runbook s4. Backfill is live-only -> owner-gated. |
| **R4/R8** Conflict/temp/quarantine material under dot-prefixed dirs invisible to Obsidian | `conflict_stash.rs:264-278` writes `<vault_root>/<dir>/<stem>.conflict-from-<device>-<lsn>.md` (VISIBLE sibling `.md`); `test_r8_conflict_visibility.rs` characterizes + `#[ignore]`d R4 spec fails on current code | **GAP** | v0.4.32 conflict copies are VISIBLE in Obsidian. THESEUS flagged these exact `CLAUDE.conflict-from-*` artifacts. Fix is fleet-wide (diverges every host's binary) + owner-gated -> NOT made in this burn. Config lines that would honor R4: none exist; there is no dot-dir stash option. Temp files: `NamedTempFile` in note dir, atomic-renamed; daemon state lives outside vault under `~/.config`/`~/.local`. |
| **R5** Completion proof (file-count/tree parity, sha256 20/20, SYNC-VERIFY canary, reboot survival) | procedures in runbook s6; acceptance verify-cmd output below | **UNVERIFIED** | All five proofs require the enrolled daemon + populated vault, both owner-gated. Reproducible commands provided. Cannot be produced by a parked burn. |
| **R6** Heavy self-documentation runbook + proposed icarus.md / Memory-Vault-Pairing updates | `docs/icarus-vault-sync-runbook.md`; `docs/icarus-config.toml.example`; drafts in runbook s8 | **CONFORMS** | Install steps, token issuance (names only), backfill timeline/metrics template, rails/rollback, parity evidence, morning follow-up drafts all present. |

---

## Self-verify output (real, pasted)

**Acceptance verify-cmd (exact ticket command, read-only against icarus):**
```
$ ssh -o BatchMode=yes -o ConnectTimeout=8 cyril@100.70.246.59 \
    'systemctl is-active nexus-vault-sync 2>/dev/null || systemctl --user is-active nexus-vault-sync; \
     test -d /var/home/cyril/vaults/Mainframe/02_Projects && echo VAULT_PRESENT || echo VAULT_ABSENT'
inactive
inactive
VAULT_ABSENT
```
Interpretation: RED, and CORRECTLY so -- daemon not active, vault not present,
because enrollment is owner-gated and was not executed. This line goes GREEN only
after the owner runs the runbook.

**icarus full read-only state (burn time):**
```
HOST=icarus ; daemon-not-active ; VAULT_ABSENT ; no-appimage
/dev/nvme1n1p3  930G  49G  880G  6%  /var/home
~/.config/systemd/user writable: yes
```

**Regression/guard tests (verified GREEN offline on link):**
The full worktree crate does not build on link: `keyring` (feature
`sync-secret-service`) pulls `libdbus-sys`, whose build.rs needs `dbus-1` dev
headers; only the runtime `libdbus-1.so.3` is present (`pkg-config --exists dbus-1`
= MISSING, no `/usr/include/dbus-1.0`). The two modules under test
(`config`, `conflict_stash`) have zero keyring/dbus deps, so they were verified via
a path-include scratch crate (`/tmp/vs-scratch`, lib name `vault_sync_daemon`,
same source files, no worktree Cargo.toml change):
```
running 34 tests   (config + conflict_stash inline tests)
test result: ok. 34 passed; 0 failed; 0 ignored
     Running tests/test_r3_vault_name_synthesis.rs
running 3 tests
test result: ok. 3 passed; 0 failed; 0 ignored
     Running tests/test_r8_conflict_visibility.rs
running 2 tests
test result: ok. 1 passed; 0 failed; 1 ignored
```
R4 desired-state spec fails on current code (executable record of the gap):
```
$ cargo test --test test_r8_conflict_visibility -- --ignored
test r4_conflict_material_must_live_under_dot_prefixed_dir ... FAILED
  R4: conflict material must live under a dot-prefixed dir invisible to Obsidian;
  got "02_Projects/Foo/Note.conflict-from-icarus-42.md"
test result: FAILED. 0 passed; 1 failed
```
`rustfmt --check` clean on both new test files. In the real worktree these
compile once `dbus-1` dev headers exist (owner/CI); they reference only
`vault_sync_daemon::config` and `::conflict_stash`, which exist unchanged.

---

## Acceptance checklist

| Acceptance item | State | Evidence |
|---|---|---|
| verify-cmd green (daemon active + vault present) | **RED (expected, owner-gated)** | Pasted above; goes green after runbook run |
| v0.4.32 version proof pasted | **DONE (artifact-level)** | Cargo.toml/tauri.conf 0.4.32; link AppImage sha256 `8a305ee8...`; journal-version method documented (runtime proof = post-enrollment) |
| zero icarus-attributed conflict files during backfill (measured) | **UNVERIFIED** | No backfill run (owner-gated); rails + baseline (`*.conflict-from-icarus-*.md` count) in runbook s4 |
| sha256 spot-parity 20/20 | **UNVERIFIED** | Procedure in runbook s6; needs populated vault |
| R8 dot-dir compliance shown | **GAP SHOWN** | conflict_stash.rs:264-278 + failing R4 spec test; config lines that would honor R4 do not exist |
| survived reboot | **UNVERIFIED** | Procedure in runbook s6 |
| token never exposed | **DONE** | No token issued/read; only key/subscriber NAMES appear in repo/report |

---

## What this burn built

- `docs/icarus-vault-sync-runbook.md` -- full owner-executable enrollment runbook (R6): stop-gate, install parity, token issuance (names only), vault seed, backfill rails, R4 gap, completion proofs, rollback table, morning follow-up drafts.
- `docs/icarus-config.toml.example` -- chmod-644 config template mirroring link's shape with icarus identity + mandatory `vault_name`; token stays in a separate 0600 file.
- `src-tauri/tests/test_r3_vault_name_synthesis.rs` -- 3 guard tests locking the vault_name -> sync-root synthesis (the mass-push footgun).
- `src-tauri/tests/test_r8_conflict_visibility.rs` -- characterization of the visible-sibling conflict copy + an `#[ignore]`d R4 spec that fails on current code.
- Read-only ground-truth capture of link (known-good install) and icarus (clean slate).

## What this burn did NOT do (owner-gated, D8)

- No AppImage copy/install on icarus. No systemd unit installed/started on icarus.
- No `POST /api/sync/subscribers/issue-token` (no subscriber created; no token issued).
- No vault seed/rsync to icarus. No backfill. No reboot. No push/merge/deploy/deletion.
- No daemon source change (would diverge the binary from link's known-good; R1 + owner-gated).

---

## Open decisions flagged for owner

1. **Reconcile-direction (blocking).** Resolve trinity's stale-replica mass-push /
   contested PG before adding icarus, OR enroll icarus PULL-ONLY seeded from link's
   on-disk vault so it cannot push a divergent view. This is the single biggest gate.
2. **R4 dot-dir gap (fleet-wide).** Decide whether conflict copies move under a
   dot-prefixed dir (e.g. `.nexus-conflicts/`) invisible to Obsidian. Affects every
   host's binary; needs ratification. `test_r8_conflict_visibility.rs` has the spec ready.
3. **THESEUS blockers.** The 2026-07-19 review says the sync gate is not closeable;
   enrolling a new subscriber into a mid-incident system compounds it.
4. **Physical disk for the vault.** R3 says FireCuda 916G; icarus's `/var/home` is
   nvme1n1p3 (880G free). Confirm the Mainframe copy lands on the intended disk.
5. **Version-proof field.** The on-disk `config.toml` `daemon_version` is a stale
   self-report (link shows 0.4.20 while running 0.4.32). Consider having the daemon
   rewrite it on boot, or drop the field; trust the journal line meanwhile.

---

## Commits on this branch (this burn)

- `20c87c7` test(icarus): R3 vault_name-synthesis guard + R4/R8 conflict-visibility spec
- (docs + report committed in the following checkpoint)

Prior commits `60766af..1e2ee68` are the v0.4.32 fix wave (pre-existing on branch).
