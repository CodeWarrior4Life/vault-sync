# icarus Vault Sync Enrollment Runbook (TKT-9d927317)

Enroll icarus as a Nexus Sync subscriber with a live Mainframe copy, running the
FIXED daemon v0.4.32 ONLY. This runbook is the owner-executable procedure. The
burn itself prepared and verified everything reversible and PARKED before the
irreversible enrollment (D8). Nothing below was executed against a live tree by
the burn except read-only state checks.

Spec anchor: `02_Projects/Nexus/_Children/vault-sync/Specifications/2026-05-20 Nexus Vault Sync - Unified Design Spec v2 - Rule Engine + Scope + BRAT.md`
Topology: `02_Projects/Lattice/Topology/icarus.md`

- link (host running this burn): Tailscale/LAN, hostname `link`
- icarus (target): `cyril@100.70.246.59`, hostname `icarus`

---

## 0. STOP-FIRST GATE (read before doing anything)

Do NOT begin enrollment until BOTH of these are cleared by the owner:

1. **Fleet reconcile-direction is unresolved.** link's v0.4.32 deploy cured the
   shadow-key storm but trinity re-detonated a mass-push as a long-offline STALE
   REPLICA (2326 pushes via the anti-strip / R2-preserve-local path), and link is
   propagating it (TKT-8a70148c BURN_REPORT). PG is therefore a CONTESTED
   canonical right now. A fresh subscriber that backfills from a contested PG
   inherits the contest.
2. **THESEUS adversarial review (2026-07-19) says the Nexus Sync gate is NOT
   closeable**: six active conflict artifacts remain (incl. `CLAUDE.conflict-from-*`),
   live link reconcile logs `requested=2 pulled=0` with failures yet falsely
   reports files_in_sync, long-path/response-decode pulls fail persistently.
   (`02_Projects/Lattice Meta/Specifications/THESEUS - Nexus Sync Adversarial Review and P2-E3 Burn Intake (2026-07-19).md`)

Enrolling icarus now would add a fourth replica to a system that is mid-incident.
Clear 1 and 2 first, OR make the explicit owner decision to enroll icarus as a
PULL-ONLY / read-mostly replica seeded from link's on-disk vault (not from PG) so
it cannot push a divergent view up.

---

## 1. Mirror link's known-good v0.4.32 install (R1)

Grounded on link at burn time:

| Component | link (known-good) |
|---|---|
| Binary | `/home/cyril/Applications/Nexus-Vault-Sync.AppImage` |
| Size / sha256 | 84384248 bytes / `8a305ee83739708b67450d0adc84a9db4f112c514dc0a5f01ec11bd23f479af3` |
| Unit | `~/.config/systemd/user/nexus-vault-sync.service` (systemd USER unit, S535) |
| Drop-in | `~/.config/systemd/user/nexus-vault-sync.service.d/10-desktop-env.conf` (DISPLAY/WAYLAND/DBUS env + `Restart=always`, TKT-cc4ede6b) |
| Config | `~/.config/nexus-vault-sync/config.toml` (0644) |
| Token | `~/.config/nexus-vault-sync/token-<subscriber_id>.bin` (0600) |

**Do NOT use `install.sh` for icarus.** It curls the LATEST GitHub release tag,
which can drift off v0.4.32. Instead copy link's EXACT artifact and verify the hash:

```bash
# from link
scp /home/cyril/Applications/Nexus-Vault-Sync.AppImage cyril@100.70.246.59:/home/cyril/Applications/
ssh cyril@100.70.246.59 'chmod +x /home/cyril/Applications/Nexus-Vault-Sync.AppImage; \
  sha256sum /home/cyril/Applications/Nexus-Vault-Sync.AppImage'
# MUST equal 8a305ee83739708b67450d0adc84a9db4f112c514dc0a5f01ec11bd23f479af3
```

Replicate the systemd user unit + drop-in verbatim on icarus (icarus already has
`~/.config/systemd/user`). Then:

```bash
ssh cyril@100.70.246.59 'systemctl --user daemon-reload; loginctl enable-linger cyril'
# enable-linger so the user unit survives logout / runs headless (icarus is a server role)
```

Version proof after start (R1 / acceptance): the daemon journal must show
`version="0.4.32"` (matches `src-tauri/Cargo.toml` version = "0.4.32" and
`tauri.conf.json` version 0.4.32 at the reviewed commit). Note the on-disk config
field `daemon_version` is a stale self-report on link ("0.4.20"); trust the
journal line, not that field.

---

## 2. Issue the subscriber token (R2) -- OWNER-GATED

This mutates server state (creates a subscriber) and is therefore owner-gated.
Names only below; no secret values in this repo/report/ticket.

```bash
# on link, admin key sourced at runtime from ~/whetstone/.env (key name: NEXUS_API_KEY)
set +o history
source ~/whetstone/.env   # provides NEXUS_API_URL, NEXUS_API_KEY
curl -sS -X POST "$NEXUS_API_URL/api/sync/subscribers/issue-token" \
  -H "Authorization: Bearer $NEXUS_API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"label":"icarus","platform":"linux-x86_64"}'
# response carries: subscriber_id + one-time token
```

Place the results on icarus ONLY:
- `subscriber_id` -> `~/.config/nexus-vault-sync/config.toml` (0644) per the template
  `docs/icarus-config.toml.example`. Keep `vault_name = "Mainframe"` (R3 footgun).
- token -> `~/.config/nexus-vault-sync/token-<subscriber_id>.bin`, `chmod 600`.
  The daemon's pairing flow writes this file at 0600 automatically
  (`token_store.rs:123-133`); if placing by hand, set 0600 explicitly.

The token must NEVER appear in a report, repo, ticket, or log. Verify:

```bash
ssh cyril@100.70.246.59 'stat -c "%a %n" ~/.config/nexus-vault-sync/config.toml \
  ~/.config/nexus-vault-sync/token-*.bin'
# expect: 644 config.toml ; 600 token-*.bin
```

---

## 3. Seed the Mainframe vault target (R3)

icarus current state (read-only, burn time): vault ABSENT, `/var/home` = nvme1n1p3,
930G total / 880G free (ample; R3 said FireCuda 916G -- confirm the vault lands on
the intended physical disk). Target: `/var/home/cyril/vaults/Mainframe`.

Preferred seed = rsync from link's on-disk vault (NOT a cold PG backfill), so
icarus starts byte-aligned with a real host and the daemon has minimal work:

```bash
ssh cyril@100.70.246.59 'mkdir -p /var/home/cyril/vaults'
# dry-run first
rsync -aHn --delete --exclude '.git/' /home/cyril/vaults/Mainframe/ \
  cyril@100.70.246.59:/var/home/cyril/vaults/Mainframe/ | tail
# then real
rsync -aH --delete --exclude '.git/' /home/cyril/vaults/Mainframe/ \
  cyril@100.70.246.59:/var/home/cyril/vaults/Mainframe/
```

---

## 4. Backfill = the risk window. LIVE RAILS (R3)

Capture the server conflict-mint rate and icarus journal BEFORE / DURING / AFTER
the first daemon start. **If ANY conflict file attributed to icarus is minted
during backfill, STOP the daemon immediately (`systemctl --user stop
nexus-vault-sync` = rollback), preserve logs, and PARK awaiting owner with the
evidence.**

Pre-start baseline:
```bash
# icarus-attributed conflict copies would be named *.conflict-from-icarus-*.md
ssh cyril@100.70.246.59 'find /var/home/cyril/vaults/Mainframe -name "*.conflict-from-icarus-*.md" | wc -l'   # expect 0
# server-side mint rate baseline (however the ops dashboard exposes it)
```

Start + watch in one motion:
```bash
ssh cyril@100.70.246.59 'systemctl --user start nexus-vault-sync'
# live watch (leave running through backfill):
ssh cyril@100.70.246.59 'journalctl --user -u nexus-vault-sync -f' | \
  tee /tmp/icarus-backfill-$(date +%Y%m%dT%H%M%S).log
```

Rails to watch in the journal (any of these = STOP + park):
- `CONFLICT (R4/R5)` mint lines attributed to icarus
- new `*.conflict-from-icarus-*.md` files on disk
- `ANTI-STRIP GUARD (S513)` firing in bulk (the trinity stale-replica shape)
- `push accepted` counts climbing into the hundreds/thousands during initial
  backfill (a fresh puller should PULL, not mass-PUSH)

Record the timeline in the BURN_REPORT / vault note: start ts, first pull ts,
pulls_pending -> 0 ts, total pulls, total pushes, conflicts minted (target 0),
end ts.

Rollback (any anomaly): `systemctl --user stop nexus-vault-sync`, keep the tee'd
log, do not delete the vault, park.

---

## 5. R8 / dot-dir compliance (R4) -- KNOWN GAP, read before enrolling

R4 requires all conflict/temp/quarantine material to live under dot-prefixed dirs
invisible to Obsidian. **v0.4.32 does NOT satisfy this for conflict copies.** It
writes the losing revision as a VISIBLE sibling:

```
# conflict_stash.rs:264-278 compute_stash_path
<vault_root>/<dir>/<stem>.conflict-from-<device_id>-<lsn>.md
# e.g. 02_Projects/Foo/Note.conflict-from-icarus-42.md  <-- Obsidian indexes this
```

THESEUS independently flagged these exact `CLAUDE.conflict-from-*` artifacts.
`test_r8_conflict_visibility.rs` characterizes this and carries an `#[ignore]`d
R4 spec that fails on current code. This is a fleet-wide design decision (changing
it diverges every host's binary and needs owner ratification), so it is NOT fixed
in this burn. Implication for icarus: if backfill mints even one conflict copy, it
will be a VISIBLE file. This is a second reason the STOP-first gate (section 0)
matters. Temp files: the stash uses `NamedTempFile` in the note's own dir then
atomic-renames; daemon shadow/state lives outside the vault under `~/.config` /
`~/.local`.

---

## 6. Completion proof (R5)

- File-count + tree-size parity vs link:
  ```bash
  ssh cyril@100.70.246.59 'find /var/home/cyril/vaults/Mainframe -type f | wc -l; du -sh /var/home/cyril/vaults/Mainframe'
  # compare to link: find /home/cyril/vaults/Mainframe -type f | wc -l; du -sh ...
  # rsync -aHn --delete link->icarus should report ~no differences (active-churn tolerance)
  ```
- sha256 spot-parity on 20 random notes vs link (20/20 must match):
  ```bash
  # on link: pick 20, hash them; on icarus: hash same relpaths; diff the two lists
  cd /home/cyril/vaults/Mainframe && find . -name '*.md' | shuf | head -20 > /tmp/sample.txt
  while read f; do sha256sum "$f"; done < /tmp/sample.txt | sort > /tmp/link.sums
  ssh cyril@100.70.246.59 "cd /var/home/cyril/vaults/Mainframe && while read f; do sha256sum \"\$f\"; done" < /tmp/sample.txt | sort > /tmp/icarus.sums
  diff <(awk '{print $1, $2}' /tmp/link.sums) <(awk '{print $1, $2}' /tmp/icarus.sums) && echo "20/20 PARITY"
  ```
- SYNC-VERIFY canary observed FLOWING FROM icarus (write a canary note on icarus,
  confirm it appears on link/PG within the expected window; see the SYNC-VERIFY
  canary convention in the vault).
- Daemon healthy across ONE reboot:
  ```bash
  ssh cyril@100.70.246.59 'sudo systemctl reboot'   # owner action
  # after reboot:
  ssh cyril@100.70.246.59 'systemctl --user is-active nexus-vault-sync'   # expect active
  ```

---

## 7. Rollback summary

| Trigger | Action |
|---|---|
| icarus-attributed conflict minted during backfill | `systemctl --user stop nexus-vault-sync`; preserve tee'd log; park |
| mass-push shape (hundreds of `push accepted` on a fresh puller) | stop; park; the config vault_name / reconcile-direction is wrong |
| daemon dormant / not active after reboot | check the `10-desktop-env.conf` drop-in env; `Restart=always` should re-arm |
| full unwind | stop + disable the user unit; the vault dir can stay (read-only copy is harmless) |

---

## 8. Proposed morning follow-ups (owner review)

### `02_Projects/Lattice/Topology/icarus.md` addition (draft)
```
## Vault Sync
- Role: Nexus Sync subscriber (v0.4.32), live Mainframe replica.
- Enrolled: <DATE> (TKT-9d927317). Subscriber id: <icarus subscriber_id>.
- Binary: ~/Applications/Nexus-Vault-Sync.AppImage (sha256 8a305ee8...), systemd
  --user unit nexus-vault-sync.service + 10-desktop-env.conf drop-in, linger on.
- Vault: /var/home/cyril/vaults/Mainframe (nvme1n1p3). Config vault_name=Mainframe (mandatory).
- Rails: STOP on any *.conflict-from-icarus-*.md mint; R4 dot-dir gap open fleet-wide.
```

### Memory-Vault Pairing update (draft)
Add a project-memory twin noting icarus is enrolled and mirrors to
`02_Projects/<project>/Memory/`. Canonical convention:
`02_Projects/Protocols/Memory-Vault Pairing Convention.md`. Link to the existing
memory `[[v0432-trinity-vault-name-configdrift]]` (the footgun this runbook guards).

---

## Owner action (one line)

Clear the section-0 stop gate (fleet reconcile-direction + THESEUS blockers), then
run sections 1-6 to enroll icarus; STOP on any icarus-attributed conflict.
