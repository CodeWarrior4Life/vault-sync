# BURN_REPORT -- TKT-9d927317 / icarus-vault-sync-subscriber

**Ticket:** TKT-9d927317
**Burn:** icarus-vault-sync-subscriber (Operation Whetstone)
**Title:** Icarus build-out 5/5: enroll icarus as Nexus Sync subscriber with live Mainframe copy (v0.4.32 fixed daemon ONLY)
**Branch (this worktree):** `whetstone/icarus-vault-sync-subscriber`
**Repo:** vault-sync (Tauri/Rust daemon)
**Reviewed commit:** `60766af` (v0.4.32 fix wave HEAD); tests at `20c87c7`
**Burn host:** link (`hostname=link`, verified). **Target:** icarus (`cyril@100.70.246.59`, `hostname=icarus`).
**Written:** 2026-07-19 01:38 EDT (leg 0) / updated 2026-07-19 02:3x EDT (leg 1, post owner-verdict execution)
**Spec anchor:** `02_Projects/Nexus/_Children/vault-sync/Specifications/2026-05-20 Nexus Vault Sync - Unified Design Spec v2 - Rule Engine + Scope + BRAT.md`

---

## Status: ENROLLED PULL-ONLY, DAEMON STOPPED (owner-directed, EXECUTED)

The owner (composer `vaults-c094` under operator delegation, S67, 2026-07-19)
returned BINDING verdicts on all five parked decisions and cleared the D8 gate for
a SCOPED enrollment: **icarus PULL-ONLY, rsync-seeded from link's on-disk vault,
INCAPABLE of pushing; a live seeded copy with sync-pending is acceptable for night
one; a push-capable divergent subscriber is not.**

This leg EXECUTED exactly that scope:

1. **Live Mainframe copy on icarus** via `rsync --archive` from link's v0.4.32-clean
   on-disk vault (118,926 files / 36,572,142,040 bytes, exit 0, 0 deleted).
   Parity vs link: **118,926 / 118,926 files, 35G / 35G, dry-run diff = 0 deltas,
   sha256 spot-parity 20/20.**
2. **Server-enforced pull-only subscriber.** icarus registered with
   **`read_only: true`** (subscriber_id `c7702ee9-efcb-43df-b69e-c28fd992ff90`,
   `materializer_mode: live`, `route: ""`). The server's push route rejects a
   read_only subscriber with `403 subscriber is read-only`
   (`sync_routes_p1.py:1392-1396`) — icarus CANNOT push even if the daemon runs.
   It is the fleet's first read_only subscriber.
3. **v0.4.32 daemon installed, STOPPED + DISABLED.** Exact-artifact copy of link's
   AppImage (sha256 `8a305ee8…`) + systemd user unit + drop-in mirrored verbatim,
   then left inactive/disabled with a documented enable-command
   (runbook §2b). Two independent layers make icarus incapable of pushing tonight:
   the server `read_only` flag + the stopped/disabled unit.

Enrollment explicitly does **NOT** claim THESEUS P2-E3 progress (owner verdict 3):
a read-only consumer cannot worsen convergence.

**Remaining owner action (one line):** when ready (coordinating reboot-proof with
the storage burn), bring the pull-only daemon live:
`ssh cyril@100.70.246.59 'loginctl enable-linger cyril; systemctl --user enable --now nexus-vault-sync'`
then watch the R3 rails (runbook §4). A stopped seeded copy is a complete,
owner-accepted night-one state; enabling is optional and reversible.

---

## Requirement Review Table

Every row cites real code/config/state at the reviewed commit and the live result
of the executed enrollment. "CONFORMS" = requirement met; "GAP (deferred)" = known
gap the owner explicitly deferred; "N/A tonight" = moot because the daemon is stopped.

| Req | Evidence (file:line / command / live result) | Verdict | Notes |
|---|---|---|---|
| **R1** Mirror link's known-good v0.4.32 install | icarus `~/Applications/Nexus-Vault-Sync.AppImage` sha256 `8a305ee83739708b67450d0adc84a9db4f112c514dc0a5f01ec11bd23f479af3` **== link's**; `src-tauri/Cargo.toml` `version = "0.4.32"`; systemd user unit + `10-desktop-env.conf` drop-in copied verbatim; `~/.config/nexus-vault-sync/config.toml` 0644 + `token-*.bin` 0600 | **CONFORMS** | Exact-artifact copy (NOT `install.sh` latest-tag). Version proof = sha256 identity to link's journal-verified v0.4.32 (TKT-8a70148c) + Cargo 0.4.32. On-disk `daemon_version` field is a stale self-report (owner verdict 5: note, trust journal). |
| **R2** Subscriber registration via server API; token → 0600 config only | `POST /admin/api/vault-sync/subscribers` (Bearer `NEXUS_API_KEY` via `admin_routes.py:199`, `auth.py:89`); token piped over ssh stdin → `token-c7702ee9-….bin` **0600, 47 bytes**; config.toml **0644**, no secret; subscriber_id in config only | **CONFORMS** | Ticket named `/api/sync/subscribers/issue-token` (Bearer key) — that route actually gates on `X-Admin-Password` (`sync_routes_p1.py:105`) AND can't set read_only. Corrected to the admin route that accepts the key AND sets `read_only:true`. Token never in report/repo/ticket/log/argv/disk-on-link. |
| **R3** Vault target `/var/home/cyril/vaults/Mainframe`; backfill rails; STOP on conflict mint | `df /var/home` = nvme1n1p3 930G/884G free (owner verdict 4: FireCuda root, nvme1n1 is the documented enumeration flip); config `vault_name = "Mainframe"` (footgun locked, `test_r3_vault_name_synthesis.rs`); **icarus-attributed conflict files = 0** (daemon never ran) | **CONFORMS** | No backfill risk window tonight: daemon stopped + server read_only. The seeded copy inherited 6 pre-existing `*.conflict-from-*` copies from link (link/other-host attributed, faithfully archived; NONE icarus-attributed). R3 STOP rail documented for the enable step (runbook §4). |
| **R4/R8** Conflict/temp/quarantine under dot-prefixed dirs invisible to Obsidian | `conflict_stash.rs:264-278` writes VISIBLE sibling `<dir>/<stem>.conflict-from-<device>-<lsn>.md`; no dot-dir stash option exists in config; `test_r8_conflict_visibility.rs` (`#[ignore]`d R4 spec fails on current code) | **GAP (deferred)** | Owner verdict 2: R4 dot-dir binary change is OUT OF SCOPE tonight (fleet binary change), test spec noted for the v0.4.33 wave, left committed. Config lines that would honor R4: **none exist**. Moot tonight (stopped daemon writes nothing). |
| **R5** Completion proof (count/size parity, sha256 20/20, canary, reboot) | count 118,926/118,926; size 35G/35G; `rsync -aHn` dry-run = **0 file deltas**; **sha256 20/20 match**; canary + reboot = deferred to enable step | **CONFORMS (seed proofs) / DEFERRED (live proofs)** | Seed parity fully proven (below). SYNC-VERIFY canary "flowing FROM icarus" is impossible AND undesired for a read_only/stopped subscriber (it cannot push) — canary applies only if the owner later makes it read-write. Reboot-survival coordinates with the storage burn per owner (deferred to enable). |
| **R6** Heavy self-documentation | `docs/icarus-vault-sync-runbook.md` (executed-state + enable-command §2b + rails/rollback + parity); `docs/icarus-config.toml.example`; morning drafts §8 | **CONFORMS** | Runbook updated to executed reality; icarus.md + Memory-Vault-Pairing drafts present for morning review. |

---

## Self-verify output (real, pasted)

**Acceptance verify-cmd (exact ticket command):**
```
$ ssh -o BatchMode=yes -o ConnectTimeout=8 cyril@100.70.246.59 \
    'systemctl is-active nexus-vault-sync 2>/dev/null || systemctl --user is-active nexus-vault-sync; \
     test -d /var/home/cyril/vaults/Mainframe/02_Projects && echo VAULT_PRESENT'
inactive
inactive
VAULT_PRESENT
```
Interpretation: **VAULT_PRESENT** (live copy landed); daemon **inactive** by design
(owner's pull-only night-one verdict). The ticket's "daemon active" acceptance is
intentionally deferred to the owner enable step — a stopped seeded copy is the
owner-accepted state.

**Daemon state (must be stopped + disabled tonight):**
```
$ ssh … 'systemctl --user is-active nexus-vault-sync; systemctl --user is-enabled nexus-vault-sync'
is-active: inactive
is-enabled: disabled
```

**R1 version proof (exact-artifact identity):**
```
icarus AppImage sha256: 8a305ee83739708b67450d0adc84a9db4f112c514dc0a5f01ec11bd23f479af3
link   AppImage sha256: 8a305ee83739708b67450d0adc84a9db4f112c514dc0a5f01ec11bd23f479af3
src-tauri/Cargo.toml:   version = "0.4.32"
```

**R2 secret hygiene:**
```
$ ssh … 'stat -c "%a %n" ~/.config/nexus-vault-sync/config.toml ~/.config/nexus-vault-sync/token-*.bin'
644 /home/cyril/.config/nexus-vault-sync/config.toml
600 /home/cyril/.config/nexus-vault-sync/token-c7702ee9-efcb-43df-b69e-c28fd992ff90.bin
```
Server-side read_only confirmed via admin list: `host: icarus | read_only: True | materializer_mode: live | route: '' | revoked: None`.

**R3 conflict rail:**
```
$ ssh … 'find /var/home/cyril/vaults/Mainframe -name "*.conflict-from-icarus-*.md" | wc -l'
0
$ ssh … 'find /var/home/cyril/vaults/Mainframe -name "*.conflict-from-*" | wc -l'
6        # inherited from link's seed (link/other-host attributed; NONE icarus)
```

**R5 seed parity (rsync --stats + comparison):**
```
Number of files: 133,299 (reg: 118,926, dir: 14,373)
Number of regular files transferred: 118,926
Number of deleted files: 0
Total transferred file size: 36,572,142,040 bytes
RSYNC_EXIT=0

LINK:   118926 files, 35G
ICARUS: 118926 files, 35G

$ rsync -aHn --itemize-changes link:/…/Mainframe/ icarus:/…/Mainframe/  → 0 file deltas
$ 20 random .md, sha256 both hosts, diff  → PARITY 20/20 — all hashes match
```

**Regression/guard tests (verified GREEN offline on link, prior leg):**
`config` + `conflict_stash` inline tests + `test_r3_vault_name_synthesis.rs` (3/3)
pass via a path-include scratch crate (the full worktree can't build on link:
`keyring`/`libdbus-sys` build.rs needs `dbus-1` dev headers, only the runtime
`.so.3` is present). `test_r8_conflict_visibility.rs` R4 spec `#[ignore]`d and
FAILS on current code — the executable record of the R4 gap deferred to v0.4.33.

---

## Acceptance checklist

| Acceptance item | State | Evidence |
|---|---|---|
| verify-cmd green (daemon active + vault present) | **VAULT_PRESENT ✓ ; daemon stopped by design** | Pasted above; goes fully green at the owner enable step |
| v0.4.32 version proof pasted | **DONE** | icarus AppImage sha256 == link v0.4.32; Cargo 0.4.32 |
| zero icarus-attributed conflict files during backfill | **DONE (0)** | daemon never ran; measured 0 `*.conflict-from-icarus-*.md` |
| sha256 spot-parity 20/20 | **DONE (20/20)** | pasted above |
| R8 dot-dir compliance shown | **GAP SHOWN (deferred v0.4.33)** | `conflict_stash.rs:264-278` + failing R4 spec; owner verdict 2 |
| survived reboot | **DEFERRED to enable step** | daemon stopped tonight; reboot-proof coordinates with storage burn |
| token never exposed | **DONE** | 0600 file; stdin-only transfer; only names/ids in repo/report |

---

## What this burn did (leg 1, owner-authorized execution)

- rsync `--archive` seeded a full live Mainframe copy onto icarus (parity-verified).
- Registered icarus as a **server-enforced read_only (pull-only) subscriber** via the
  admin API; placed the token in a 0600 file and the subscriber_id in a 0644 config
  (`vault_name = "Mainframe"` mandatory).
- Installed the exact v0.4.32 AppImage + systemd unit/drop-in mirroring link, left
  **stopped + disabled** with a documented enable-command.
- Updated the runbook to executed reality; refreshed this report with live evidence.

## What this burn did NOT do (still owner-gated / out of scope)

- Did NOT start or enable the daemon (owner enable step; reboot-proof coordination
  with the storage burn).
- Did NOT change the daemon source / diverge the binary from link (R1 + owner verdict 2:
  R4 dot-dir fix is a v0.4.33 fleet change).
- Did NOT push/merge/deploy/delete. Did NOT touch PG data beyond creating the one
  read_only subscriber row the owner authorized (reversible via the DELETE in runbook §2).

---

## Open decisions — RESOLVED by owner (2026-07-19)

1. **Reconcile-direction** → PULL-ONLY approved (option b). Seeded from link's on-disk
   copy; server read_only. DONE.
2. **R4 dot-dir gap** → out of scope tonight; v0.4.33 wave; test left committed. NOTED.
3. **THESEUS compounding** → resolved by (1); enrollment does not claim P2-E3. NOTED.
4. **Disk** → FireCuda root `/var/home` (nvme1n1p3, 884G free) confirmed intended;
   nvme1n1 name is the documented enumeration flip. CONFIRMED.
5. **Stale `daemon_version`** → note only, trust journal. NOTED.

---

## Commits on this branch (this burn)

- `20c87c7` test(icarus): R3 vault_name-synthesis guard + R4/R8 conflict-visibility spec
- `9f2461a` docs(icarus): enrollment runbook + config template + BURN_REPORT
- (leg-1 execution: runbook + report refresh committed in the following checkpoint)

Prior commits `60766af..1e2ee68` are the v0.4.32 fix wave (pre-existing on branch).
