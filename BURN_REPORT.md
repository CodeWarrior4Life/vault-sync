# BURN REPORT: base_seq daemon client leg (causal gate E2E) + empty-vault_folders guard

**Ticket:** TKT-166e1c07
**Burn:** vaultsync-baseseq-daemon-leg
**Branch:** whetstone/vaultsync-baseseq-daemon-leg (delivery on this branch only)
**Version target:** v0.4.33 candidate (no tag, no release, no deploy)
**Reviewed at commit:** 66bfbb6 (base of this burn, atop merged truthful-reconcile leg TKT-c41c2225)

## Goal

Close the E2E causal-gate gap that invalidated the P2-E3 closure. The server half of
base_seq (Burn B, THESEUS AR-002) is LIVE on Nexus master; the daemon half was never
built, so NEXUS_FF_SYNC_CONVERGENCE is inert end-to-end. This burn builds the daemon
leg: proof-of-observation base_seq on every push/delete, 409 refetch/merge, observed
seq recorded only after byte-verified materialization, fail-closed empty lineage,
flag-off clean degrade, plus the empty-vault_folders guard.

## Server contract (authoritative, read from Nexus origin/master)

The daemon leg is built to match the LIVE server contract exactly:

- Request field: `SyncPushRequest.base_seq: Optional[int]` (server/nexus/core/sync/models.py:50). Optional so pre-R7b daemons still deserialize; extra field ignored by servers without it.
- Causal gate (server/nexus/core/sync/causal_gate.py:37-80): flag OFF -> ALLOW (byte-identical); server has no version -> ALLOW (create); `base_seq is None` + tracked version -> CONFLICT (unknown/empty lineage); `base_seq != current_seq` -> CONFLICT; `base_seq == current_seq` -> ALLOW. Server-side mirror of R4.
- Push accept response surfaces the new version token in `server_seq = _read_change_seq(...)` (server/nexus/api/sync_routes_p1.py:2333-2343), "record as observed base_seq only AFTER FS bytes materialized + hash-verified locally".
- Push gate at sync_routes_p1.py:1904-1925; delete gate at sync_routes_p1.py:2136-2145 (both `causal_gate_decision(base_seq, current_seq, True)`).
- `GET /api/sync/note` payload includes `"change_seq"` (sync_routes_p1.py:1422, S549) so the pull path can capture the observed seq. Older servers omit it.

## REVIEW TABLE (completed BEFORE any edit)

| Req | Requirement | Reviewed code (file:line @ 66bfbb6) | Verdict | Evidence |
|-----|-------------|-------------------------------------|---------|----------|
| R1 | Daemon declares last-observed base_seq on EVERY push and delete | `PushRequest` struct api_client.rs:254-270; `process_event` builds request push_client.rs:613-623; single push endpoint push_client.rs:627; shadow store is `HashMap<String,String>` sync_shadow.rs:69 (no seq persisted) | **GAP** | `PushRequest` has no `base_seq` field; no per-note seq is stored anywhere; `git grep base_seq` matches BURN_REPORT.md only. Push+delete share one `push()` path so one field covers both. |
| R2 | On 409 from the causal gate, refetch current server version and merge; never blind-retry or overwrite | 409 -> `ApiError::Conflict` api_client.rs:605-612; handled push_client.rs:698-714 | **GAP (partial)** | Current 409 path stashes local bytes (`stash_local_on_conflict` push_client.rs:709) then returns `ConflictUnrecoverable`. Does NOT blind-retry and does NOT overwrite (good), but does NOT refetch server version and merge. Needs refetch + byte-verified materialize of server head, local preserved as stash. |
| R3 | Merged result never recorded as observed until exact merged bytes materialize on FS and verify byte-equal; observed seq from server response only | byte-verify-after-write materializer.rs:861-891 (IntegrityChecker); shadow hash recorded only on pass materializer.rs:918; push-accept record push_client.rs:667-689; `NotePayload` api_client.rs:178-216 | **GAP** | Byte-verify infra EXISTS and is the correct hook point, but no base_seq is recorded there. `NotePayload` has no `change_seq` (pull-side observed source) though the server returns it. `PushResponse.server_seq` (api_client.rs:278) is parsed but unused for observation. |
| R4 | Fail CLOSED on unknown/empty lineage: unknown base_seq -> unobserved (refetch/merge), never fabricate a seq | server causal_gate.py:73 (None+version -> CONFLICT); client has no base_seq concept | **GAP (client side)** | Server fails closed correctly. Client cannot participate: it never sends base_seq, so under flag-on the server 409s and the client treats it as a plain CAS conflict (stash) rather than the intended refetch/merge. Client must send None for unknown and route None-conflict through refetch/merge, never defaulting a seq. |
| R5 | Backward compatible: flag-off server ignores base_seq; daemon degrades cleanly against servers that do not return seq (no hard dep, no error spam) | back-compat via serde `Option` + `#[serde(default)]` (NotePayload optional fields api_client.rs:192-215; ReconcileBatchResponse/ChangesResponse serde default api_client.rs:325,351); no `deny_unknown_fields` anywhere; server flag-off ALLOW causal_gate.py:70 | **CONFORMS (by design; preserve + test)** | Additive `Option` request field + `#[serde(default)]` response field degrade cleanly: old server ignores extra `base_seq`, omits `server_seq`/`change_seq` -> None -> no observation recorded -> no error. Must be preserved and covered by a regression test. |
| R6 | Burn C truthful-reconcile accounting intact; base_seq conflicts surface, never silently swallowed | accounting struct + `cycle_red` verify_repair.rs:194-220; summary emit reconciliation.rs:182-217 | **CONFORMS (preserve; test)** | Accounting present and unchanged by this burn. New 409/base_seq conflicts must keep surfacing as conflict/divergent outcomes (not reclassified as success). Guard with a test. |
| R7 | Empty-vault_folders guard: vault_folders empty but shadow holds vault-prefixed keys -> WARN and refuse (park, not no-op) | vault_name empty -> silent fallback to vaults_root config.rs:128-131; vault_folders can resolve empty lib.rs:602-605; strip/migration no-ops when empty sync_shadow.rs:119-202 | **GAP** | No guard exists. When vault_folders is empty the shadow strip/migration silently no-ops and prefixed keys mismatch with no warning. This is the 2026-07-18 trinity incident (config.toml missing vault_name -> B2 no-op -> 2,249 notes mass-pushed). |
| R8 | CI hygiene at delivery: fmt --check, clippy -D warnings, full test suite green in CI-equivalent env | tauri.conf.json / Cargo.toml (Bazzite host cannot build natively) | **UNVERIFIED (pending CI-equiv build)** | Host is immutable Bazzite; will build in a distrobox/podman ubuntu env with libdbus-1-dev/pkg-config/gnome-keyring and paste real output. |
| R9 | Bump to 0.4.33 candidate in tauri.conf.json + Cargo.toml; no tag, no release | tauri.conf.json:4 = 0.4.32; Cargo.toml:3 = 0.4.32; package.json:4 = 0.4.31; `daemon_version` test asserts 0.4.32 api_client.rs:653 | **GAP** | Version still 0.4.32; the daemon_version guard test pins 0.4.32 and must move to 0.4.33. |

## Implementation plan (derived from the review)

1. **Per-note observed-seq store** (new `base_seq_store.rs`, modeled on sync_shadow.rs): `HashMap<String,i64>` path -> last-observed change_seq, same canon_key/vault_folders/atomic-flush discipline. Wired alongside `ShadowStore` (R1/R3/R4 substrate).
2. **R1**: add `base_seq: Option<i64>` to `PushRequest`; populate in `process_event` from the seq store (None when unknown). Covers push AND delete (shared path).
3. **R3**: add `change_seq: Option<i64>` (`#[serde(default)]`) to `NotePayload`; record observed seq into the store ONLY at the post-byte-verify points (materializer.rs:918 for pulls; push-accept canonical/aligned points push_client.rs:667-689 using `resp.server_seq`).
4. **R2 + R4**: on `ApiError::Conflict`, refetch via `fetch_note`, materialize server head (byte-verified), record observed seq, preserve local as existing stash; unknown lineage (None) routes through the same path. Never blind-retry, never overwrite-lose.
5. **R7**: detect "vault_folders empty AND shadow holds prefixed-looking keys" and refuse pushes/migrations with a WARN (park), not a silent no-op.
6. **R5/R6**: preserve serde-default degrade + Burn C accounting; add regression tests.
7. **R9**: bump versions + daemon_version guard to 0.4.33.
8. **R8**: build + fmt/clippy/test in CI-equivalent podman env; paste output below.

<!-- BUILD/TEST OUTPUT + ACCEPTANCE CHECKLIST appended as the burn proceeds -->
