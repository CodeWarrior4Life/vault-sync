# icarus Vault Sync Enrollment Runbook (TKT-9d927317)

Enroll icarus as a Nexus Sync subscriber with a live Mainframe copy, running the
FIXED daemon v0.4.32 ONLY.

Spec anchor: `02_Projects/Nexus/_Children/vault-sync/Specifications/2026-05-20 Nexus Vault Sync - Unified Design Spec v2 - Rule Engine + Scope + BRAT.md`
Topology: `02_Projects/Lattice/Topology/icarus.md`

- link (host running this burn): Tailscale/LAN, hostname `link`
- icarus (target): `cyril@100.70.246.59`, hostname `icarus`

---

## STATUS: ENROLLED PULL-ONLY, DAEMON STOPPED (2026-07-19)

The owner (composer `vaults-c094` under operator delegation, S67, 2026-07-19)
CLEARED the section-0 gate with a BINDING verdict: **enroll icarus PULL-ONLY,
seeded via rsync from link's on-disk vault; icarus must be INCAPABLE of pushing;
a live seeded copy with sync-pending is acceptable for night one.**

This burn EXECUTED the owner-authorized enrollment:

1. **rsync --archive seed** from link's on-disk vault (`/var/home/cyril/vaults/Mainframe`,
   the v0.4.32-clean copy) → icarus. Faithful copy (no excludes; link has no `.git`).
2. **Server-enforced pull-only subscriber.** icarus registered via
   `POST /admin/api/vault-sync/subscribers` (Bearer `NEXUS_API_KEY`) with
   **`read_only: true`** — the server's push route rejects any push from a
   read_only subscriber with `403 subscriber is read-only`
   (`sync_routes_p1.py:1395`). This is STRONGER than a daemon-side flag: even if
   the daemon is later started, icarus CANNOT push. `materializer_mode: live`,
   `route: ""`. subscriber_id `c7702ee9-efcb-43df-b69e-c28fd992ff90`.
3. **v0.4.32 binary + systemd unit installed** on icarus, mirroring link exactly
   (sha256 `8a305ee8…`), but the unit is left **STOPPED and DISABLED** with a
   documented enable-command (section "Enable command" below), per the owner's
   night-one instruction. A stopped daemon plus a server-side read_only flag =
   two independent layers making icarus incapable of pushing tonight.

Because the daemon binary has NO pull-only mode (the push pipeline is spawned
unconditionally per sync_root — `lib.rs:702`; `materializer_mode=disabled` only
skips WRITES, not pushes — `materializer.rs:552`), the pull-only guarantee is
carried by the **server-side `read_only` flag** + the stopped/disabled unit.

Enrollment explicitly does **NOT** claim THESEUS P2-E3 progress: a read-only
consumer cannot worsen convergence (owner verdict 3).

---

## 0. HISTORICAL: the STOP-FIRST GATE (now cleared)

Before the owner's 2026-07-19 verdict, enrollment was gated on two blockers.
Retained for the record; both are RESOLVED by the pull-only decision above.

1. **Fleet reconcile-direction was unresolved.** link's v0.4.32 deploy cured the
   shadow-key storm but trinity re-detonated a mass-push as a long-offline STALE
   REPLICA (2326 pushes via the anti-strip / R2-preserve-local path), and link is
   propagating it (TKT-8a70148c BURN_REPORT). PG is therefore a CONTESTED
   canonical. → RESOLVED: icarus seeds from link's on-disk copy (not a cold PG
   backfill) AND is server-side read_only, so it cannot push the contest onward.
2. **THESEUS adversarial review (2026-07-19) said the Nexus Sync gate is NOT
   closeable.** → RESOLVED for icarus: a read-only consumer adds no push traffic
   and cannot compound the open conflict set (owner verdict 3).
   (`02_Projects/Lattice Meta/Specifications/THESEUS - Nexus Sync Adversarial Review and P2-E3 Burn Intake (2026-07-19).md`)

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

## 2. Register the subscriber (R2) -- EXECUTED as read_only (pull-only)

**IMPORTANT correction to the ticket:** the ticket named
`POST /api/sync/subscribers/issue-token` with Bearer `NEXUS_API_KEY`. That route
actually gates on the `X-Admin-Password` header (`require_admin`,
`sync_routes_p1.py:105`) AND cannot set `read_only`. The route that (a) accepts
`Bearer NEXUS_API_KEY` (via `require_admin_auth` → `_check_admin_auth`,
`admin_routes.py:199`; `_api_key` = legacy `NEXUS_API_KEY`, `auth.py:89`) AND
(b) can set `read_only: true` is:

```bash
# on link, admin key sourced at runtime from ~/whetstone/.env (key name: NEXUS_API_KEY)
set +o history
source ~/whetstone/.env   # provides NEXUS_API_URL, NEXUS_API_KEY
curl -sS -X POST "$NEXUS_API_URL/admin/api/vault-sync/subscribers" \
  -H "Authorization: Bearer $NEXUS_API_KEY" -H 'Content-Type: application/json' \
  -d '{"host":"icarus","device_label":"icarus …","route":"",
       "materializer_mode":"live","pull_attachments":true,"read_only":true}'
# response carries: subscriber_id + plaintext_token (shown ONCE, never stored server-side)
```

`read_only:true` is the server-enforced pull-only guarantee: `push_file` rejects
a read_only subscriber with `403 subscriber is read-only`
(`sync_routes_p1.py:1392-1396`).

Placed on icarus ONLY (done by this burn):
- `subscriber_id` (`c7702ee9-efcb-43df-b69e-c28fd992ff90`) →
  `~/.config/nexus-vault-sync/config.toml` (0644) per `docs/icarus-config.toml.example`,
  `vault_name = "Mainframe"` (R3 footgun locked).
- token → `~/.config/nexus-vault-sync/token-<subscriber_id>.bin`, `chmod 600`.
  The token was piped over ssh via **stdin only** (never in argv, never to disk on
  link, never printed). `token_store::load` reads this file first
  (`token_store.rs:188-213`, trailing `\n` trimmed).

The token NEVER appears in a report, repo, ticket, or log. Verified:

```bash
ssh cyril@100.70.246.59 'stat -c "%a %n" ~/.config/nexus-vault-sync/config.toml \
  ~/.config/nexus-vault-sync/token-*.bin'
# observed: 644 config.toml ; 600 token-c7702ee9-….bin (47 bytes = "vsk_"+urlsafe(32))
```

To revoke (reversible unwind of the registration):
```bash
curl -sS -X DELETE "$NEXUS_API_URL/admin/api/vault-sync/subscribers/c7702ee9-efcb-43df-b69e-c28fd992ff90" \
  -H "Authorization: Bearer $NEXUS_API_KEY"
```

---

## 2b. Enable command (OWNER — start the pull-only daemon when ready)

The v0.4.32 daemon is installed on icarus but **stopped + disabled** tonight.
When the owner decides to bring it live (coordinating reboot-proofing with the
storage burn per its rail), enable it in one motion and immediately watch the
R3 rails (section 4):

```bash
ssh cyril@100.70.246.59 'loginctl enable-linger cyril; systemctl --user enable --now nexus-vault-sync'
# then watch (section 4):
ssh cyril@100.70.246.59 'journalctl --user -u nexus-vault-sync -f'
```

Even enabled, icarus is server-side `read_only` → it will PULL/materialize but
every push attempt is rejected 403. The R3 STOP rail still applies to any
icarus-attributed conflict *copy* minted during the first materialization pass.

---

## 3. Seed the Mainframe vault target (R3)

icarus current state (read-only, burn time): vault ABSENT, `/var/home` = nvme1n1p3,
930G total / 880G free (ample; R3 said FireCuda 916G -- confirm the vault lands on
the intended physical disk). Target: `/var/home/cyril/vaults/Mainframe`.

Seed = rsync `--archive` from link's on-disk vault (NOT a cold PG backfill), so
icarus starts byte-aligned with a real host. **EXECUTED** this leg (link has no
`.git`, so no exclude needed — a faithful copy gives exact parity):

```bash
ssh cyril@100.70.246.59 'mkdir -p /var/home/cyril/vaults'
rsync -aH --stats /var/home/cyril/vaults/Mainframe/ \
  cyril@100.70.246.59:/var/home/cyril/vaults/Mainframe/
# result: 118,926 regular files, 36,572,142,040 bytes, 0 deleted, exit 0
```

Parity verified (R5, §6): count 118,926/118,926, size 35G/35G, `rsync -aHn`
dry-run diff = 0 file deltas, sha256 spot-parity 20/20.

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
- Role: Nexus Sync subscriber (v0.4.32), live Mainframe replica, PULL-ONLY (read_only).
- Enrolled: 2026-07-19 (TKT-9d927317). Subscriber id: c7702ee9-efcb-43df-b69e-c28fd992ff90.
  Server read_only=true, materializer_mode=live, route="" (first read_only subscriber in fleet).
- Binary: ~/Applications/Nexus-Vault-Sync.AppImage (sha256 8a305ee8..., == link v0.4.32),
  systemd --user unit nexus-vault-sync.service + 10-desktop-env.conf drop-in.
- Night-one state: seeded (rsync --archive from link, 118,926 files/35G, parity 20/20),
  daemon STOPPED + DISABLED. Enable: `systemctl --user enable --now nexus-vault-sync`
  (+ `loginctl enable-linger cyril`) after coordinating reboot-proof with the storage burn.
- Vault: /var/home/cyril/vaults/Mainframe (nvme1n1p3 = FireCuda root, enumeration flip).
  Config vault_name=Mainframe (mandatory footgun guard).
- Rails: server read_only rejects all pushes (403); STOP on any *.conflict-from-icarus-*.md
  mint once enabled; R4 dot-dir gap open fleet-wide (v0.4.33).
```

### Memory-Vault Pairing update (draft)
Add a project-memory twin noting icarus is enrolled and mirrors to
`02_Projects/<project>/Memory/`. Canonical convention:
`02_Projects/Protocols/Memory-Vault Pairing Convention.md`. Link to the existing
memory `[[v0432-trinity-vault-name-configdrift]]` (the footgun this runbook guards).

---

## Owner action (one line)

Enrollment is DONE (pull-only, seeded, daemon stopped). Only remaining owner step,
when ready: `ssh cyril@100.70.246.59 'loginctl enable-linger cyril; systemctl --user enable --now nexus-vault-sync'`
(coordinate reboot-proof with the storage burn), then watch the §4 rails.
