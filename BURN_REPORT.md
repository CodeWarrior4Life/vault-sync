# BURN_REPORT -- TKT-c41c2225 / vaultsync-pull-defect-pair

**Ticket:** TKT-c41c2225
**Burn:** vaultsync-pull-defect-pair
**Title:** vault-sync v0.4.33: fix the two every-cycle pull failures + failure-honest reconcile accounting (THESEUS AR-003/008/009)
**Branch (this worktree):** `whetstone/vaultsync-pull-defect-pair` on `/var/home/cyril/Burns/TKT-c41c2225`
**Reviewed commit:** `60766af` (branch tip == vault-sync main tip at burn start)
**Spec anchor:** `02_Projects/Lattice Meta/Specifications/THESEUS -- Nexus Sync Adversarial Review and P2-E3 Burn Intake (2026-07-19).md`
**Scope:** THESEUS Burn C only (AR-003 / AR-008 / AR-009). R7b/base_seq (Burn B), substrate custody (Burn A), fleet convergence (Burn D) are explicitly out of scope.

---

## Live journal reproduction (R4, PRE-EDIT)

Daemon on link: `nexus-vault-sync.service`, PID 1310111, v0.4.32-era code. `journalctl --user -u nexus-vault-sync.service`. The identical failure pair recurs EVERY reconcile cycle (00:08, 00:18, 00:38, 00:48, 00:58, 01:08, 01:18, 01:28, 01:38, ...). The canonical 00:58 cycle the spec cites:

```
2026-07-19T04:58:20.045546Z  WARN vault_sync_daemon::verify_repair: reconciliation: pull failed path=01_Periodic/Daily/2026-07-04-Saturday - Quiet holiday inbox noise, no meetings or plans on July 4th.md error=fetch: network error: error decoding response body
2026-07-19T04:58:20.045554Z  WARN vault_sync_daemon::verify_repair: reconciliation: pull failed path=03_Media/Social/X/@theblacktruth/@Lemelson - 𝗖𝗵𝗿𝗶𝘀𝘁𝗶𝗮𝗻 𝗭𝗶𝗼𝗻𝗶𝘀𝗺 𝗘𝗫𝗣𝗢𝗦𝗘𝗗 - 𝗛𝗼𝘄 𝗮 𝗛𝗲𝗿𝗲𝘀𝘆 𝗛𝗶𝗷𝗮𝗰𝗸𝗲𝗱 𝗔𝗺𝗲𝗿𝗶𝗰𝗮 𝗘𝗽 𝟰𝟭 A war is.md error=materialize: conflict-stash error: io error: File name too long (os error 36)
2026-07-19T04:58:20.045557Z  INFO vault_sync_daemon::verify_repair: reconciliation: server-wins pull pass complete requested=2 pulled=0
2026-07-19T04:58:20.061255Z  INFO vault_sync_daemon::reconciliation: reconciliation: pass complete files_scanned=59922 files_in_sync=59922 pulls_pending=0 pushes_queued=0 substrate_refused=0 elapsed_ms=4308
```

**The false green:** two pulls fail (`requested=2 pulled=0`), then the very next line reports `files_in_sync=59922 pulls_pending=0 pushes_queued=0` -- a cycle that looks clean to any soak counter despite two persistent failures. This is AR-003 exactly.

**AR-009 root cause (confirmed against the live server, read-only GET `/api/sync/note`):** HTTP 200, `content-type: application/json`, 6122 bytes. The JSON body has `"modified": null`. The client's `NotePayload.modified` is a **non-optional `String`** (`api_client.rs:90`), so serde rejects `null` -> reqwest surfaces the generic `error decoding response body`. The fields `file_mtime`, `created`, `updated_at` are also `null` but are already typed `Option`; `modified` is the one field that was left required. Field-level evidence:

```
path: str    frontmatter: dict    body: str    sha256: str
modified: NoneType None      <-- decode killer (declared String, not Option)
file_mtime: NoneType None    created: NoneType None    updated_at: NoneType None
enriched_body: str           content_hash: str
```

**AR-008 root cause:** the X-note basename uses mathematical-bold Unicode (each glyph is 4 UTF-8 bytes); the stem alone is ~230+ bytes. `compute_stash_path` (`conflict_stash.rs:266-278`) appends `.conflict-from-<device>-<lsn>.md` producing a basename well past the ext4 `NAME_MAX` of 255 bytes, so `write_stash`'s atomic persist (`conflict_stash.rs:343`) fails with `ENAMETOOLONG` (os error 36).

---

## R1..R5 Review Table (against commit `60766af`, PRE-EDIT)

| Req | File:Line evidence | Verdict | Evidence |
|---|---|---|---|
| **R1** AR-008: length-safe stash names (hash-bounded basename + manifest) + filename-limit tests (Linux/macOS/Windows) + regression fixture on the exact live path | `conflict_stash.rs:266-278` (`compute_stash_path` builds `{stem}.conflict-from-{device}-{lsn}.md`); `conflict_stash.rs:301-347` (`write_stash` persists to that name); `conflict_stash.rs:340-344` (tmp+persist) | **GAP** | The basename is `stem` (unbounded, verbatim from the note path) plus a fixed suffix. There is NO length guard, NO hash-fallback, NO manifest. For the live X-note the persist at `:343` raises ENAMETOOLONG. No filename-limit test exists (grep: no `NAME_MAX` / `255` / `os error 36` anywhere in the module). |
| **R2** AR-009: decode failures capture status/content-type/body-length/request-id/bounded sample (no leak); fix the contract mismatch for the Daily note; not in-sync until bytes materialize + hash-verify | `api_client.rs:302-328` (`fetch_note`), esp. `:311` `Ok(resp.json().await?)`; `api_client.rs:48-49` (`ApiError::Network(#[from] reqwest::Error)`); `api_client.rs:90` (`pub modified: String`) | **GAP** | `.json().await?` collapses every decode failure into reqwest's opaque `error decoding response body` via the blanket `#[from]` -- no status, content-type, length, request-id, or sample is captured. The contract mismatch is `modified: String` vs server `null`. (Sub-part "hash-verify before in-sync": the materializer DOES post-write integrity-check at `materializer.rs:861-869`, but the fetch fails before materialize, and AR-003 rounds that failure to zero -- see R3.) |
| **R3** AR-003: report carries attempted/succeeded/failed/deferred/still-divergent; any failed pull => RED, ineligible for soak; failed items keep durable retry state | `verify_repair.rs:507` (`report.add_count = pulled` -- successes only); `verify_repair.rs:501-504` (failures pushed to `report.errors`, never summarized); `verify_repair.rs:172-193` (`VerifyRepairReport` has no attempted/failed/deferred/divergent/red fields); `reconciliation.rs:177-185` (summary emits `pulls_pending=add_count`, no failed count, no red flag) | **GAP** | Failures are logged (`:502`) and collected in `report.errors` (`:503`) but never counted into the summary. `add_count` is successes only, so `pulls_pending=0` after a total-failure cycle. There is no attempted/failed/deferred/still-divergent tally, no RED/soak-eligibility flag, and no durable per-item retry record (retry is only the implicit next full scan). |
| **R4** Evidence discipline: reproduce both failures from live journal pre-edit; post-fix run daemon suite + new regressions; regressions FAIL on v0.4.32 | Journal repro above; test harness `src-tauri/tests/` + in-module `#[cfg(test)]` | **PARTIAL (repro DONE)** | Both failures reproduced from the live journal and the AR-009 contract mismatch confirmed against the live server (above). Test half pending implementation; regressions will be structurally red on old code (reference fields/fns absent pre-fix). |
| **R5** No deploy, no daemon restart, no version-bump ship; deliver on burn branch as v0.4.33 candidate | n/a (process constraint) | **CONFORMS (commitment)** | No deploy/restart/push performed or planned by this burn. The live daemon (PID 1310111) is left running untouched; the read-only diagnostic GET does not mutate server or vault state. Delivery is branch-only. |

**Net pre-fix state:** R1, R2, R3 all GAP. R4 repro half satisfied. R5 is an honored constraint. The three GAPs are independent code paths (stash naming, response decode, reconcile accounting) but AR-003 is the reason the other two stayed invisible: it rounds the two persistent failures down to a clean summary.

---
