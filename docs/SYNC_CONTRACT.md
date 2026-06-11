---
title: Nexus Sync — Sync Contract v1 (Normative Invariant Spec)
type: spec
project: Nexus
sub_project: nexus-sync
status: proposed-pending-cyril
created: 2026-06-11
owner: cyril
author: cody
session: burn-S1-A (TKT-b9cbe732, Operation Whetstone Lane S)
anchor: Master Plan A0.4.1 [Rank 1] / A0.5 Lane S / §10.3 Wave 1
gates: S1-B (hermetic sync harness — scenarios key 1:1 off invariant IDs below)
review: "M2 owner review required for POLICY-flagged invariants (see §16)"
supersedes: none
related:
  - "[[2026-05-29 Nexus Sync - Sync Contract Design]]"
  - "[[2026-05-29 Nexus Sync - Sync Contract Implementation Plan]]"
  - "[[Substrate-Only Guardrail — Runtime LLMs Never Touch Vault Content (S498)]]"
  - "[[2026-06-11 S498 vault-sync Durable Fix (opfix-vaultsync TKT-2643db73) - Outcome]]"
tags:
  - project/nexus
  - nexus-sync
  - sync
  - spec
  - contract
  - whetstone
---

# Nexus Sync — Sync Contract v1 (Normative Invariant Spec)

> **What this is.** Every implicit invariant of the Nexus↔vault-sync protocol,
> extracted from the daemon source (vault-sync `src-tauri/src/*.rs`, ~15.3K lines,
> v0.4.13/v0.4.14 era), the server sync surface (nexus
> `server/nexus/api/sync_routes_p1.py` + `services/vault_sync/*` + reconciler), and
> the 2026-05→2026-06 incident corpus — stated normatively, numbered **I1..I82**,
> with code evidence and an invariant→test traceability table (§17).
>
> **What it is for.** Cheaper models implement and review against these numbered
> invariants instead of re-deriving distributed-systems semantics. S1-B's hermetic
> harness scenarios key 1:1 off these IDs. Every future sync plan/PR must cite the
> invariant IDs it touches (PVD wiring lands in M3).
>
> **Authority.** MUST/MUST NOT statements are binding on both repos. Where an
> invariant encodes a tunable *policy* rather than a correctness law, it is flagged
> **POLICY** and listed in §16 for owner (M2) review. Evidence line numbers are as
> of 2026-06-11 (daemon branch `whetstone/S1-A` base v0.4.13; nexus master).

## 1. Scope and definitions

- **Daemon**: the vault-sync (Nexus Sync) Tauri/Rust client. **Server**: the nexus
  reconciler/API (`sync_routes_p1.py` and services). The legacy SQLite
  `sync_routes.py` surface (`/api/sync/reconcile`, `sync_devices`, `sync_files`)
  is **DEAD** and outside this contract (see I6).
- **Wire path**: forward-slash, sync_root-relative, route-relative path as it
  appears on the wire. **Storage key**: the server-internal canonical path
  (bare core or route-prefixed) per [[2026-05-29 Nexus Sync - Sync Contract Design]].
- **fs_hash**: SHA-256 hex of the note's raw file bytes (server-side canonical hash,
  column `vault_reconcile_state.fs_hash`).
- **change_seq / lsn**: the server's durable per-note change sequence
  (`vault_notes.change_seq`); on the wire it is called `lsn`.
- This contract covers the sync protocol and convergence layer only. Out of scope:
  pairing/Keychain, tray/updater UX, CF tunnel, share endpoints (S485 share guard
  lives in the share spec), and the substrate-only guardrail enforcement hook
  (referenced as deployment context in §15).

## 2. Reading the invariants

Each invariant: **[In] statement** — *evidence* (file:line) — *notes/provenance*.
Test status lives ONLY in the §17 traceability table to avoid divergence.
Incident provenance uses the incident index in §18.

---

## 3. Wire schemas

**[I1] NotePayload (server→daemon, `GET /api/sync/note`) MUST carry exactly:**
`path: String` (wire-relative), `frontmatter: object`, `body: String`,
`sha256: String` (hex SHA-256 of the served bytes), `modified: String` (ISO),
`file_mtime: number|null` (**unix-timestamp float, NOT string**),
`enriched_body: String|null` (the exact bytes the server hashed; null tolerated for
older servers), `created: String|null` (ISO; consumed since v0.4.14, S498 R3).
Daemon MUST deserialize unknown/missing optional fields via defaults, never hard-fail.
— *daemon `api_client.rs:77-106`; server `sync_routes_p1.py:1071-1085`* —
`file_mtime` as string broke the ENTIRE pull path pre-v0.4.3 (commit c7d17a2).

**[I2] PushRequest (daemon→server, `POST /api/sync/push`) MUST be**
`{path, content, base_hash, action}` where `content` is **base64** of the raw note
bytes (field name `content`, NOT `content_b64` — server 422s on the wrong name),
`base_hash` is a **non-null string** (`""` means "create; server must have no row"),
and `action ∈ {"create","modify","delete"}` lowercase.
— *daemon `api_client.rs:137-153`, `push_client.rs:377-386`; server
`sync_routes_p1.py:1345-1499`*

**[I3] Pushed content MUST be UTF-8-decodable and NUL-free.** Server rejects
non-UTF-8 bytes and embedded NUL with 422; the daemon MUST NOT attempt to push
binary content. — *server `sync_routes_p1.py:1462-1466`*

**[I4] PushResponse is a 4-state envelope:** `status ∈ {accepted, merged,
conflict_markers, error}` plus `seq?`, `content_hash?` (server's accepted content
SHA), `server_hash?` (server's current fs_hash), `merged_content?`. Idempotent
no-ops return `accepted` with `message: "idempotent (no change)"`.
— *daemon `api_client.rs:156-164`, `push_client.rs:440-457`; server push handler*

**[I5] CAS conflict is HTTP 409 with body `{expected_hash: string|null}`** —
the server's current fs_hash, so the daemon can re-fetch/re-base.
— *server `sync_routes_p1.py:1124-1130, 1488-1489`; daemon `api_client.rs:406-414`*

**[I6] Reconcile traffic MUST use `POST /api/sync/reconcile-batch` (Postgres path)
and NEVER the legacy `/api/sync/reconcile` (SQLite `sync_devices`) endpoint.**
Request `{paths: [{path, fs_hash}]}` with NO `device_id`/`manifest` legacy fields.
— *daemon `api_client.rs:186-189` (v0.4.10, commit 3c1a349)* — Incident N7
(reconcile-404): the daemon registered as a subscriber, not a device; the legacy
endpoint 404'd and its plan would have diffed against tables the push path never
writes.

**[I7] ReconcileBatchResponse vocabulary is closed:** `{deltas: [{path, state,
server_hash?}]}` with `state ∈ {"match","drift","missing-on-server"}`;
`server_hash` present for match/drift, absent for missing-on-server.
— *daemon `api_client.rs:191-206`; server `sync_routes_p1.py:342-427`*

**[I8] `GET /api/sync/changes` takes `since: int>=0, limit: 1..5000` and returns
`{changes: [{path, file_mtime, modified, indexed_at, lsn}], next_lsn}`,** rows
ordered by `change_seq ASC`; `lsn` is the row's change_seq.
— *server `sync_routes_p1.py:245-334`*

**[I9] SSE frames on `GET /api/sync/events` are
`event: <phase>\nid: <lsn>\ndata: <json>\n\n`** with data envelope
`{op, phase, note_id, path, content_hash, updated_at, lsn}`. The daemon MUST
tolerate `lsn` arriving as int OR string and treat it as opaque.
— *server `_format_sse` `sync_routes_p1.py:627-647`; daemon `sse.rs:240-293`*

**[I10] An SSE/catchup envelope with NO `op` field MUST default to UPSERT** on the
daemon — catchup payloads omit `op`; dropping them silently was the S476 regression.
— *daemon `sse.rs:13-27` (`#[serde(default = "default_op")]`)*

**[I11] `POST /api/sync/heartbeat`** takes `{lag_seconds, last_seq, last_seen_lsn,
connection_state}`, returns `{status:"ok", host, ack_at, lag_seconds}`, and updates
`vault_subscribers.last_seen_at/last_sync_at`. — *server `sync_routes_p1.py:435-459`*

**[I12] All wire paths are forward-slash, sync_root-relative, route-relative.**
No absolute paths, no backslashes, no vault-name first segment (see I35).
— *both repos, throughout; design ratified in [[2026-05-29 Nexus Sync - Sync Contract Design]] §3.2*

## 4. The hash contract

**[I13] `fs_hash` is SHA-256 over the note's raw FS file bytes;** when no FS file
exists (DB-only note) it falls back to SHA-256 of `vault_notes.body` UTF-8 bytes.
— *server `sync_routes_p1.py:1053, 1063`*

**[I14] THE CENTRAL LAW: bytes served == bytes hashed == fs_hash.** `/api/sync/note`
serves the FS file bytes if present, else `vault_notes.body` — **NEVER**
`vault_sync_cache.enriched_body` — and `NotePayload.sha256` is computed over those
same served bytes, so `sha256(served) == fs_hash` **by construction**. Violation of
this single law caused the S481 perpetual-churn storm and the 2026-06-08 served-hash
≠ fs_hash churn. — *server `sync_routes_p1.py:1006-1085`*

**[I15] The daemon MUST verify materialized bytes against `payload.sha256`
post-write** (integrity check), using `enriched_body` when present as the exact
hashed bytes, else reconstructing frontmatter+body. Mismatch is a hard
materialization failure, not a warning. — *daemon `materializer.rs:376-378, 467-506`
(S486 BUG 2 fix, commit 2bbf32d)*

**[I16] Diff-normalization is identical in push and materialize paths:** strip the
configured frontmatter fields (default `["updated"]`) line-oriented (top-level YAML
keys only, no serde_yaml round-trip, no whitespace/quoting changes); non-UTF-8
content passes through unmodified. The two implementations
(`push_client::normalize_for_diff`, `materializer::normalize_for_diff`) MUST stay
byte-identical in behavior. — *daemon `push_client.rs:193-222`,
`materializer.rs:588-614`*

**[I17] Change detection MUST key on content hash, never on mtime alone.**
(a) reconcile drift handling skips when `delta.server_hash == local content hash`;
(b) drain-time push skips (`Skipped(IdenticalToServer)`) when
`sha256(content) == evt.base_hash`. — *daemon v0.4.14 `verify_repair.rs:265-281`,
`push_client.rs:375-397`* — S498 R1: mtime-only drift marked ~29,000
content-identical files diverged and amplified into a push loop.

**[I18] The enrichment-cache hash (`sha256(enriched_body)` in `vault_sync_cache`)
is a DIFFERENT hash from fs_hash and MUST never be served on the sync path.**
— *server `cache_writer.py:48-71`*

**[I19] Enrichment MUST NOT mutate the canonical body.** Push upserts
`vault_notes.body` from the ORIGINAL pushed bytes; enrichment output lives only in
the cache layer. A rule that wrote enrichment output into `vault_notes.body` would
break I14. — *server `sync_routes_p1.py:1442-1453`; `enrichment_runner.py:286-299`*

## 5. change_seq, cursor, and the catchup guarantee

**[I20] `vault_notes.change_seq` is a monotonic BIGINT from a sequence starting at
1e9** (freeze-proof vs xmin wraparound), stamped by trigger ON INSERT (path LIKE
'%.md' only) and ON UPDATE only when `body` actually changed; chunk pseudo-rows
(`…md#chunk_N`) stay NULL and are auto-excluded by `WHERE change_seq > %s`.
— *server migration 2026_05_30_005; `sync_routes_p1.py:258-279`*

**[I21] `/changes` cursor MUST always advance past every fetched row, including
rows skipped by route mapping;** an empty page holds the cursor at `since`. A
cross-route key stream can therefore never stall a subscriber.
— *server `sync_routes_p1.py:311-334`*

**[I22] CATCHUP GUARANTEE (explicit).** On SSE connect with `Last-Event-ID: N`, the
server MUST replay, in monotonic change_seq order, EVERY note with
`change_seq > N` (from `vault_notes`, regardless of cache state) plus every
tombstone from `vault_deleted_notes` within the retention window that has not been
recreated/restored. With no `Last-Event-ID`, catchup is from 0 (full). Catchup is
idempotent — replaying any suffix is safe. Combined with I24 the protocol is
**at-least-once, never at-most-once**: events emitted while the daemon was down are
delivered on reconnect; the catchup-snapshot race is closed by sourcing catchup from
change_seq-ordered rows, not a point-in-time cache snapshot.
— *server `_catchup_from_pull` `sync_routes_p1.py:650-732, 768-787`*

**[I23] DELETE events MUST NOT carry an SSE `id:` line (lsn omitted / None)** so a
delete can never regress or stall the cursor. — *server `sync_routes_p1.py:644,
720-732`; `cache_writer.py:154-159`*

**[I24] The daemon MUST persist `last_event_id` atomically (tmp+rename) BEFORE
updating its in-memory cursor,** at
`<workspace>/.lattice-runtime/<subscriber_id>/sync-state/last_event_id`, and send it
verbatim as `Last-Event-ID` on reconnect. Crash between persist and apply replays
exactly one event (safe, per I22 idempotency). — *daemon `sse.rs:94-133, 280-289`;
`lib.rs:598-619`*

**[I25] The daemon materializes ONLY `event_type == "enrichment_complete"` events;**
intermediate phases (`lint_pending`, `lint_complete`) are dropped at the SSE layer.
Non-DELETE ops re-fetch the note via `/api/sync/note` (pull-through; the SSE
envelope is a notification, not a payload). DELETE ops invoke idempotent
soft-delete. — *daemon `sse.rs:240-293` (v0.4.8/v0.4.9)*

**[I26] One effective SSE connection per subscriber:** a new registration for the
same subscriber_id supersedes the prior stream (superseded flag; old stream
terminates gracefully). — *server `sync_routes_p1.py:751, 798-804`*

**[I27] On fanout buffer overflow the server MUST emit
`event: buffer_overflow` with `id: <last_served_lsn>` and terminate the stream** —
never hang or silently drop; the daemon reconnects and catches up from that lsn
(per I22). — *server `sync_routes_p1.py:789-796`*

**[I28] The SSE `id` is the change_seq; the daemon treats it as an opaque string**
and never parses, compares, or arithmetics it client-side.
— *daemon `sse.rs` (threads id through verbatim)*

## 6. CAS push rules

**[I29] The CAS token `base_hash` is the SERVER's hash (server-hash CAS base),
never the local content hash:** for modify, the last-known server fs_hash; for
create, `""`. — *daemon `push_client.rs:377-386` (v0.4.11, commit bf8c38b — using
the local hash made every conflicted push ConflictUnrecoverable forever)*

**[I30] Server CAS is one atomic critical section:** acquire `FOR UPDATE` row lock,
re-read current fs_hash in-transaction, then dual-upsert `vault_notes` +
`vault_reconcile_state` in the SAME transaction. A crash can never leave the two
tables split. — *server `_cas_write_note_and_state` `sync_routes_p1.py:1263-1327,
1146-1176`*

**[I31] Idempotent push:** if `current == sha256(pushed raw bytes)` the server
accepts as a no-op (`"idempotent (no change)"`) without rewriting.
— *server `sync_routes_p1.py:1305-1307`*

**[I32] Conflict policy is ServerWins-at-the-CAS (stale base loses), with NO
server-side merge.** `base_hash == "" AND row exists` → 409; `base_hash != current`
→ 409. Merge burden is entirely on the daemon. **POLICY** (M2).
— *server `sync_routes_p1.py:1310-1319, 1099-1102`*

**[I33] On 409 the daemon maps to `ConflictUnrecoverable{expected_hash}`, ACKS the
journal entry (no blind retry loop), and recovery is refetch→merge/stash→replay.**
A 409 MUST never be nacked into an infinite retry.
— *daemon `push_client.rs:397-402, 240-256`*

**[I34] The daemon MUST NOT push content identical to the server's:** drain-time
guard `sha256(content) == evt.base_hash → Skipped(IdenticalToServer)` (the second
layer of I17). — *daemon v0.4.14 `push_client.rs:375-397` (S498 R1)*

**[I35] The server MUST reject or normalize any pushed wire path whose first
segment equals the vault name** — never trust the client's path. Incident N4
(Mainframe-prefix bleed): an unmigrated daemon config pushed `Mainframe/…` paths;
the server stored them verbatim; a correctly-configured peer materialized
`Mainframe/Mainframe/…` (~9,894 files) and re-pushed in an infinite cross-host
loop; 22,574 rows purged. — *server hardening 2026-06-04 (post-incident)*

**[I36] Server push/reconcile ingest MUST parse frontmatter and persist
`created`/`modified` into `vault_notes`** (never leave them empty; non-ISO values
sort as NULL LAST). — *server hardening 2026-06-04; `_iso_to_epoch`
`sync_routes_p1.py:957-977`* — empty created/modified rows were half of incident N4.

## 7. The PUSH/PULL decision table, conflict stash, echo guard

**[I37] The shadow decision table.** The daemon's push/pull decision compares
`local` (normalized local content hash), `shadow` (last-pulled server version), and
`server` (current server hash). The five rows are NORMATIVE:

| Row | State | Decision | Action |
|-----|-------|----------|--------|
| 1 | `local == server` (after normalization) | SKIP | idempotent — no push (I17/I34) |
| 2 | `local != server` AND `shadow == server` | PUSH | only local changed; CAS base = server hash (I29) |
| 3 | `local != server` AND `shadow != server` | CONFLICT | concurrent edit: stash per I38/I39, materialize server canonical |
| 4 | no server row | CREATE | push with `base_hash=""` (I2/I29) |
| 5 | local absent, server row exists | DELETE | soft-delete propagation (I23/I73-I75) |

— *daemon `push_client.rs:159-187`, `materializer.rs:395-442`*

**[I38] Conflict stash mechanics:** stash filename
`<stem>.conflict-from-<device_id>-<lsn>.md` (collision suffixes `-2`, `-3`…);
live mode stashes next to the canonical file, shadow mode in the shadow tree
mirror; writes are atomic tmp+rename with the I62 traversal guard applied to the
stash path. — *daemon `conflict_stash.rs:177-252`*

**[I39] Stash policy matrix** **POLICY** (M2):

| Class | ServerWins | Manual | NewerWins |
|-------|-----------|--------|-----------|
| D (security-sensitive) | STASH | STASH | STASH |
| C (content) | OVERWRITE | STASH | STASH if `local_lsn < server_lsn` else OVERWRITE |

Class D (ALWAYS stash, regardless of mode): `Credentials.md`, anything under
`Credentials/`, `02_Projects/Protocols/Infrastructure*`,
`02_Projects/Protocols/Bootstrap*`. — *daemon `conflict_stash.rs:122-142, 187-252`*

**[I40] Echo guard:** before completing a server-driven write the materializer
records `(path, content_hash)`; the file-watcher consults the guard before
enqueuing and skips on match. Entries are **consumed on hit** (a later genuine
identical edit is not suppressed), expire after **TTL 15s** (**POLICY**, M2), and
the guard fails OPEN (unwired guard = no suppression, never blocked pushes).
Breaks the SSE→materialize→watcher→push loop (v0.4.8, commit a25a2e0).
— *daemon `echo_guard.rs:1-74`, `materializer.rs:386-388`*

**[I41] The file-watcher MUST drop filesystem-notify Access and Metadata event
kinds** — only content-meaningful events enqueue pushes. Read-walks (e.g. backup
scans) over the vault must produce ZERO journal entries (v0.4.9, commit cc1e314:
the read-walk journal storm). — *daemon `file_watcher.rs` event classify*

## 8. Journal, retry, and storm bounds

**[I42] The push journal is JSONL** (one object per line) at
`<workspace>/.lattice-runtime/<subscriber_id>/sync-state/push_journal.jsonl`,
`schema_version: 1`; unknown schema versions are skipped with a warning, never a
crash. — *daemon `push_journal.rs:1-530`*

**[I43] Journal `content_bytes` serializes as base64** (NOT a JSON int-array;
int-array bloat ~4-6x caused the journal-capacity storms, v0.4.6 commit 941a244);
reads MUST remain backward-compatible with legacy int-array and null.
— *daemon `push_journal.rs:71-100`*

**[I44] Journal capacity is capped (default 100 MB)** **POLICY** (M2);
`append_batch` writes what fits and drops the remainder WITH a warning (graceful
degradation, never a deadlock — v0.4.5 commit 00424f4 fixed the cap deadlock).
— *daemon `push_journal.rs:385-433`*

**[I45] Corrupt-tail policy:** on read, stop scanning at the first unparseable
line; all earlier lines remain valid. A torn final write never poisons the journal.
— *daemon `push_journal.rs:263-346`*

**[I46] Multi-handle journal correctness:** three handles (watcher append, push
drain/ack, verify-repair append) share one file with NO per-handle cache — drain
and ack re-read the file every call; entries carry a stable per-event `id`
(`<unix_nanos>-<counter>`; deterministic `legacy-<hash>` for pre-id lines); ack is
by-id and idempotent. Cross-process contention is excluded by the single-instance
guarantee (tauri_plugin_single_instance). — *daemon `push_journal.rs:14-25,
141-149, 440-489`*

**[I47] Ack/nack discipline:** ACK (remove) on all terminal outcomes — Accepted,
Merged, ConflictMarkers, Skipped, Unauthorized, Forbidden, ConflictUnrecoverable.
NACK (retain for next drain) ONLY on NetworkExhausted. Nothing else may loop.
— *daemon `push_client.rs:240-256`*

**[I48] Retry/backoff bounds** **POLICY** (M2): max 5 attempts per drain,
exponential `500ms × 2^attempt` capped at 60s, applied ONLY to transient errors
(5xx/network/timeout) — never to 401/403/409/400; backoff state resets per drain
cycle. — *daemon `push_client.rs:39-72, 389-432, 482-486`*

**[I49] Lazy content refs:** journal entries MAY carry `content_bytes: null` and be
read at drain time (verify-repair enqueues tens of thousands without embedding). A
lazily-referenced file that has vanished is **skip+ack** (orphan safety), never a
retry — the orphan-local unbounded-retry (node_modules-class) failure is forbidden.
— *daemon `push_journal.rs:174-177`, `push_client.rs:349-374`,
`verify_repair.rs:285-291`*

**[I50] Storm circuit-breaker** **POLICY** (M2): 200 consecutive
CapacityExceeded journal-append failures fence the watcher (loud diagnostic, halt);
any non-capacity failure resets the streak. Encodes the S489/S490 storm (journal
wedged at cap, watcher hot-spinning, 745k failures, 85% CPU).
— *daemon `file_watcher.rs:179-257`*

## 9. Scope, exclusion, route, and traversal gates

**[I51] The server V9 baseline excludes are un-overridable** (gitignore semantics
via pathspec): `.obsidian/`, `.trash/`, `.cody-tmp/`, `node_modules/`,
`__pycache__/`, `.git/`, `*.pyc`, `*.swp`, `*.tmp`, `.DS_Store`, `._*`,
`Thumbs.db`. Push of a V9-matched path is rejected with 400. Per-subscriber scope
may only narrow further (I53). — *server `scope_filter.py:28-59`;
`sync_routes_p1.py:1401`*

**[I52] Server scope evaluation order:** V9 baseline (deny) → `scope_roots`
(if non-empty, path must match one; union) → `scope_excludes` (any match denies;
union) → allow. Scope is evaluated against the STORAGE KEY, consistently across
/note, /changes, and SSE fanout. — *server `scope_filter.py:61-88`;
`sync_routes_p1.py:999-1004`*

**[I53] Per-subscriber scoping is strictly narrowing** — it can never re-include a
V9-excluded path. — *server `scope_filter.py` check order*

**[I54] Daemon scope:** empty `scope_roots` = include everything; non-empty = path
must prefix-match a root; excludes always win; vault-relative forward-slash
matching. — *daemon `scope.rs:1-23`*

**[I55] Extension allowlist is `.md` + `.canvas`, case-insensitive,** enforced at
watcher and push; delete events bypass the existence-dependent part of the check.
— *daemon `push_client.rs:40-43, 466-480`, `file_watcher.rs:70-71`*

**[I56] Daemon hardcoded excludes (defense-in-depth, regardless of config):**
directories `.obsidian/`, `.lattice-sync/`, `.trash/`, `._/`; basename prefix `.%`
(machine-local convention, S477). — *daemon `file_watcher.rs:54-68`*

**[I57] RASP substrate fence: substrate paths are NEVER materialized and NEVER
pushed** (checked at materialize and pre-HTTP at push): vault pointers
`00_VAULT.md`, `CLAUDE.md`, `GEMINI.md`, `AGENTS.md` (any depth,
case-insensitive); `Family.md`/`Mission.md` under `02_Projects/`; prefix dirs
`02_Projects/Protocols/`, `_project/`, `_rapport/people/`, `_rapport/groups/`,
`_rapport/triage/`. Rules are checked against both the original path and the
stripped-first-segment form (multi-vault, S477).
— *daemon `rasp_fence.rs:19-165`, `materializer.rs:348-358`,
`push_client.rs:330-332`*

**[I58] Junk-path exclusion (daemon mirror of V9):** any segment starting `._`
(AppleDouble), `.DS_Store`, `Thumbs.db`, segments `.obsidian`, `.trash`, `.git`,
`node_modules`, `.cody-tmp`, `__pycache__`, `.lattice-sync`, `.lattice-runtime`,
extensions `*.pyc|*.swp|*.tmp`. — *daemon `rasp_fence.rs:193-237`*

**[I59] `.nx-<host>` machine namespaces are explicitly INCLUDED in sync**
(structurally distinct from `._`; the one dot-class that syncs) **but NEVER
embeddable** (I61). — *daemon `rasp_fence.rs` carve-out; design §3.3-3.4 of
[[2026-05-29 Nexus Sync - Sync Contract Design]]*

**[I60] Route mapping:** storage keys stay bare/canonical in PG; `to_storage`
prepends the subscriber's registered route prefix inbound, `to_wire` strips it
outbound; route `""` is the identity (bare subscriber sees ALL keys unchanged,
including other routes' prefixed keys); a cross-route key raises internally and is
**silently skipped with the cursor still advancing** (I21). Applied at all four
boundaries: /changes, SSE live, SSE catchup, /note.
— *server `route_map.py:13-27`; `sync_routes_p1.py:201-238, 294-334, 772, 810, 997`*

**[I61] Embeddable gate:** only `route == ""` AND no path segment starting `.nx-`
embeds into pgvector (`embeddable` column). Named-route content and machine
artifacts never reach embeddings. — *server `sync_routes_p1.py:1105-1121, 1467`*

**[I62] Path-safety gates (both sides):** reject `..` as an exact SEGMENT (not
substring — `title....md` is legal; S490 regression, v0.4.5 commit 00424f4),
absolute paths, Windows drive paths; verify-repair additionally canonicalizes and
asserts the resolved path stays under the sync root (symlink-escape, R7). Applied
before every materialize/stash write and on SSE paths.
— *daemon `scope.rs:39-45`, `materializer.rs:344`, `sse.rs:258`,
`verify_repair.rs:350-380`*

## 10. Timestamp and birthtime preservation

**[I63] `file_mtime` is a unix-timestamp float end-to-end** (`vault_notes.file_mtime`
→ NotePayload f64). — *server `sync_routes_p1.py` ChangeRow/NotePayload; daemon
`api_client.rs:88`*

**[I64] Materialization MUST restore the file's mtime from the canonical
`modified` field on ALL platforms.** Atomic tmp+rename creates a new inode; without
explicit restoration every materialized note's timestamps reset to "now" (incident
N5: full-vault ctime-clobber; Obsidian sort destroyed). — *daemon v0.4.14
`materializer.rs:455-470, 610-682` (`apply_canonical_times`; parses RFC3339 and
naive forms)* — S498 R2.

**[I65] Birthtime restoration from canonical `created`:** macOS (APFS,
setattrlist ATTR_CMN_CRTIME) and Windows (NTFS, SetFileTime) MUST restore it; Linux
birthtime is kernel-read-only — restore mtime only and rely on frontmatter
`created` as mitigation. The server MUST send `created` in NotePayload (S498 R3);
the daemon deserializes it with a default (back-compat) and soft-fails when absent
or unparseable (skip restore, never abort the write). — *daemon v0.4.14
`api_client.rs:100-106`, `materializer.rs`; server `sync_routes_p1.py:957-977`*

**[I66] All daemon writes (materialize, stash, cursor files) are atomic
tmp+rename;** the journal cursor file is `<path>.tmp.<pid>` → rename.
— *daemon `materializer.rs:461-464`, `sse.rs:97-117`*

## 11. Probe safety, verify-repair, reconcile

**[I67] verify-repair is READ-ONLY on the vault:** it walks (sequential filter
pass, then bounded-parallel hashing), compares, and ENQUEUES journal pushes — it
never writes, deletes, stashes, or materializes files itself. All mutation flows
through the journal + push pipeline. — *daemon `verify_repair.rs:30-72, 350-380`*

**[I68] Server-side reconcile-batch is READ-ONLY:** it compares client fs_hashes
against `vault_reconcile_state` and returns deltas; it mutates nothing (the
best-effort prefix observation upsert in I72n is telemetry, not sync state).
— *server `sync_routes_p1.py:342-427`*

**[I69] Reconcile push CAS bases:** drift → `base_hash = delta.server_hash`;
missing-on-server → create (`""`). Enqueued as lazy refs (I49).
— *daemon `verify_repair.rs:287-291`*

**[I70] The reconcile drift arm MUST skip when the server hash equals the local
manifest content hash** (first layer of I17; S498 R1a — the mtime-keyed version of
this arm caused the 29k-file push amplification). — *daemon v0.4.14
`verify_repair.rs:265-281`*

**[I71] Periodic reconciliation backstop** **POLICY** (M2): default every 600s,
override `VAULT_SYNC_RECON_INTERVAL_SECS`, kill switch `VAULT_SYNC_DISABLE_RECON`;
one independent task per sync_root; errors are non-fatal.
— *daemon `reconciliation.rs:1-250`, `lib.rs:800-822`*

**[I72] Reconcile never pulls:** reconcile-batch only ever produces PUSHES of local
files; server-only files reach the daemon exclusively via SSE/catchup (I22). The
verify-repair manifest applies the full filter stack (R7 symlink, hardcoded
excludes, RASP fence, extension list) before hashing, and surfaces
refused/filtered counters. (Best-effort: the server observes and WARN-logs new
first-segment prefixes per subscriber — telemetry only.)
— *daemon `verify_repair.rs`; server `sync_routes_p1.py:384-413`*

## 12. Deletion and retention

**[I73] Server delete = row removal, not a tombstone flag:** `DELETE FROM
vault_notes` + `DELETE FROM vault_reconcile_state` in one transaction; dropping
reconcile state is REQUIRED so a later `base_hash=""` create (restore) is
accepted. The AFTER DELETE trigger emits the DELETE fanout op.
— *server `sync_routes_p1.py:1330-1342`*

**[I74] Offline-delete tombstones:** an AFTER DELETE trigger snapshots into
`vault_deleted_notes` with a **24h retention window** **POLICY** (M2); catchup
replays tombstones recreate-safely (excluded if restored or if the path exists
again in `vault_notes`) with `lsn = None` (I23). — *server migration 2026_05_30_004;
`sync_routes_p1.py:705-732`*

**[I75] Daemon delete handling is idempotent soft-delete;** replaying a DELETE for
an already-absent path is a no-op. — *daemon `sse.rs` DELETE arm,
materializer soft_delete*

**[I76] Delete propagation cleans the cache layer too:** `vault_sync_cache` row
removed, DELETE envelope fanned out with `lsn: None`.
— *server `cache_writer.py:122-184`*

## 13. Subscribers, auth, health

**[I77] Bearer tokens are stored ONLY as hashes** (`bearer_token_hash`); plaintext
is returned exactly once at issuance (`secrets.token_urlsafe(32)`, admin-gated);
`revoked_at IS NOT NULL` → 401 on next request (soft revoke, history kept).
— *server `sync_routes_p1.py:83-102, 467-512`*

**[I78] `read_only` subscribers are rejected (403) on ALL mutations** —
create, modify, AND delete — before any write decision.
— *server `sync_routes_p1.py:1354-1358`*

**[I79] Subscriber identity:** `subscriber_id` (uuid) + registered `route`
(default `""` = bare Mainframe) + `scope_roots`/`scope_excludes` (PG TEXT[]) live
on `vault_subscribers`; the daemon authenticates as a subscriber — the
device/sync_devices model is dead (I6). — *server migrations 2026_05_14_002,
2026_05_20_002*

**[I80] `/health` MUST never fail on auth:** unauthenticated fast path (PG ping,
fanout snapshot); bearer parsing is best-effort and soft-fails; when authenticated
it enriches with subscriber_id, scope, materializer_mode, shadow_path (the daemon's
HealthSnapshot contract). — *server `sync_routes_p1.py:147-197`*

## 14. Encoding and process invariants

**[I81] UTF-8 end-to-end; no console-codepage decoding anywhere in the path.** On
Windows, paths are handled as UTF-16 via native APIs (Path/OsStr → CreateFileW) —
never through CP437. Any non-daemon filename writer (ingest pipelines) MUST force
UTF-8 (`PYTHONUTF8=1`, `PYTHONIOENCODING=utf-8`). Incident N6 (90 mojibake
filenames, `ΓÇª` for `…`): root cause was the legacy ingest layer, daemon audited
clean — the daemon-side guard test (materialize `Probe … 'q' – "d" 🚨.md`
byte-identically) keeps it that way. — *S479 Task E1 audit*

**[I82] Exactly one daemon instance per host** (tauri_plugin_single_instance);
journal multi-handle correctness (I46) assumes no cross-process writers.
— *daemon lib.rs single-instance wiring*

---

## 15. Deployment-context invariants (referenced, owned elsewhere)

These are binding on the surrounding system, specified in
[[Substrate-Only Guardrail — Runtime LLMs Never Touch Vault Content (S498)]]:
runtime/prod agents never FS-mount the core vault (read-only HTTP + `create_note`
only); server-materialized vault, zion FS backup, and Dropbox copies are ONE-WAY
derivatives (substrate → FS → cloud), never written as source. S1-A defers their
enforcement-hook design to that spec.

## 16. POLICY-flagged invariants — owner (M2) review list

Per A0.4.1 M2 (6/11): these encode tunable policy, not correctness law. Defaults
ship as written; owner may adjust values without breaking the contract (S1-B
scenarios parameterize them).

| ID | Policy | Current value | Question for owner |
|----|--------|---------------|--------------------|
| I32 | Conflict policy | ServerWins at CAS, no server merge | Keep ServerWins as the only server policy? (daemon-side NewerWins/Manual exist per I39) |
| I39 | Stash decision matrix + Class D path list | as tabulated | Confirm Class D list is complete (Credentials, Protocols/Infrastructure*, Bootstrap*) |
| I40 | Echo-guard TTL | 15s | OK for slow disks / large notes? |
| I44 | Journal cap | 100 MB | Keep cap + drop-with-warning degradation? |
| I48 | Retry bounds | 5 attempts, 500ms×2^n, 60s cap | OK? |
| I50 | Storm breaker threshold | 200 consecutive capacity failures → fence | OK? Manual-recovery requirement acceptable? |
| I71 | Reconcile cadence | 600s, env-overridable + kill switch | OK? |
| I74 | Tombstone retention | 24h | Long enough for multi-day-offline hosts? (A host offline >24h misses deletes until next reconcile-class mechanism — note reconcile never pulls, I72. Known gap, flagged.) |

## 17. Traceability table — invariant → existing-or-MISSING test

Status: ✅ = direct test exists; ◐ = partial/implicit coverage; ❌ MISSING = no test
(S1-B golden-scenario candidates). Daemon tests are Rust `#[test]`/integration
tests in vault-sync; server tests are pytest in nexus.

| ID | Test(s) | Status |
|----|---------|--------|
| I1 | daemon `tests/test_api_client.rs` (live-contract deserialization); ❌ no NotePayload round-trip incl. `enriched_body`+`created` | ◐ |
| I2 | daemon `push_client.rs` tests; server `test_sync_routes_push_pg.py` | ✅ |
| I3 | server `test_sync_routes_push_pg.py` (utf-8/NUL reject) | ◐ |
| I4 | daemon `push_client.rs::map_response` tests | ✅ |
| I5 | daemon `push_client.rs` 409-mapping tests | ✅ |
| I6 | daemon `test_api_client.rs::reconcile_batch_request_serializes_paths_fs_hash` | ✅ |
| I7 | daemon `test_api_client.rs::reconcile_batch_response_deserializes_live_server_contract` | ✅ |
| I8 | server `test_fanout_e2e_integration.py::test_changes_feed_ordered_by_change_seq_excludes_chunks` | ✅ |
| I9 | daemon `tests/test_sse.rs`; server `_format_sse` (implicit) | ◐ |
| I10 | daemon `sse.rs` default_op tests | ✅ |
| I11 | ❌ MISSING (heartbeat round-trip) | ❌ |
| I12 | ◐ implicit across route/scope tests | ◐ |
| I13 | server `test_reconciler_sweep.py`, `test_sync_routes_contract_e2e.py` | ✅ |
| I14 | ❌ MISSING explicit "served bytes hash == fs_hash" assertion (S481/2026-06-08 regression class) | ❌ |
| I15 | daemon `tests/test_materializer_write.rs` integrity tests | ✅ |
| I16 | daemon normalize tests in both modules; ❌ MISSING cross-module identity test (push vs materializer byte-equal) | ◐ |
| I17 | daemon v0.4.14 `push_skips_on_content_identical_regardless_of_mtime`, `drift_with_hash_equal_to_local_is_not_pushed` | ✅ |
| I18 | ◐ implicit in fanout tests | ◐ |
| I19 | ❌ MISSING (enrichment-must-not-mutate-body guard) | ❌ |
| I20 | server `test_fanout_e2e_integration.py::test_change_seq_stamped_on_insert_and_bumps_on_body_change` | ✅ |
| I21 | server changes-feed test (ordering); ❌ cursor-advance-past-skipped-rows not explicit | ◐ |
| I22 | server `test_fanout_e2e_integration.py::test_catchup_from_pull_replays_by_change_seq_incl_non_cached` | ✅ |
| I23 | ◐ implicit (catchup test); ❌ no explicit cursor-no-regress-on-delete assertion | ◐ |
| I24 | ❌ MISSING last_event_id persistence round-trip / crash-replay test | ❌ |
| I25 | daemon `tests/test_sse.rs` (event filtering) | ✅ |
| I26 | ❌ MISSING (superseded-connection behavior) | ❌ |
| I27 | ❌ MISSING (buffer-overflow event) | ❌ |
| I28 | ◐ implicit in SSE tests | ◐ |
| I29 | daemon `push_client.rs` base_hash tests | ✅ |
| I30 | server `test_sync_routes_push_pg.py` CAS tests | ✅ |
| I31 | server idempotent-push test; daemon S486 benchmark (live) | ✅ |
| I32 | server CAS conflict tests | ✅ |
| I33 | daemon `push_client.rs` conflict-ack tests | ✅ |
| I34 | daemon v0.4.14 drain-guard regression tests | ✅ |
| I35 | ❌ MISSING (vault-name-prefix reject/normalize — incident N4 class) | ❌ |
| I36 | ❌ MISSING (push writes created/modified — incident N4 class) | ❌ |
| I37 | ◐ rows 1,2,4 covered via push tests; rows 3,5 partial; ❌ no single table-driven test | ◐ |
| I38 | daemon `conflict_stash.rs` tests (filename parse, stash path, write) | ✅ |
| I39 | daemon `conflict_stash.rs::decide` policy-matrix tests | ✅ |
| I40 | daemon `echo_guard.rs` tests (suppressed-once, TTL, different-hash); ❌ MISSING watcher+materializer integration | ◐ |
| I41 | ❌ MISSING (Access/Metadata drop — read-walk storm class, v0.4.9) | ❌ |
| I42 | daemon `push_journal.rs` tests | ✅ |
| I43 | daemon `push_journal.rs` base64 + legacy int-array back-compat tests | ✅ |
| I44 | daemon append_batch graceful-degradation tests | ✅ |
| I45 | daemon corrupt-tail tests | ✅ |
| I46 | daemon ack/drain tests; ❌ MISSING true concurrent three-handle test | ◐ |
| I47 | daemon `push_client.rs` ack/nack tests | ✅ |
| I48 | daemon backoff-formula tests | ✅ |
| I49 | daemon lazy-read tests (incl. FileVanished skip) | ✅ |
| I50 | daemon `file_watcher.rs` breaker-threshold tests; ❌ trip+recovery e2e | ◐ |
| I51 | ◐ implicit in scope tests; ❌ no explicit V9-list conformance test | ◐ |
| I52 | server scope tests (implicit in fanout) | ◐ |
| I53 | ❌ MISSING (narrowing-only property) | ❌ |
| I54 | daemon `tests/test_scope.rs` | ✅ |
| I55 | daemon extension tests (push_client + file_watcher) | ✅ |
| I56 | daemon classify tests; ❌ `.%` basename prefix untested | ◐ |
| I57 | daemon `rasp_fence.rs` substrate tests | ✅ |
| I58 | daemon `rasp_fence.rs` junk tests | ✅ |
| I59 | daemon `rasp_fence.rs::includes_nx_host_namespace` | ✅ |
| I60 | server `test_sync_routes_contract_e2e.py` (route isolation) | ✅ |
| I61 | server `test_sync_routes_push_pg.py::test_embeddable_for` | ✅ |
| I62 | daemon `scope.rs` tests incl. S490 `allows_dots_inside_filenames` regression | ✅ |
| I63 | daemon `test_api_client.rs` live-contract test | ✅ |
| I64 | daemon v0.4.14 `write_restores_canonical_mtime_and_birthtime` | ✅ |
| I65 | daemon v0.4.14 `parse_server_timestamp_accepts_rfc3339_and_naive`; ❌ per-platform birthtime e2e (mac/Win) | ◐ |
| I66 | daemon `tests/test_materializer_write.rs` atomic-write tests | ✅ |
| I67 | daemon `verify_repair.rs` tests (no-write property by construction) | ✅ |
| I68 | server reconcile tests (read-only implicit) | ◐ |
| I69 | daemon `verify_repair.rs::build_modify_push` tests | ✅ |
| I70 | daemon v0.4.14 `lazy_reconcile_push_skips_when_disk_content_matches_server_hash` | ✅ |
| I71 | daemon `reconciliation.rs` env-parsing + kill-switch tests | ✅ |
| I72 | daemon manifest-filter tests; ❌ "reconcile never pulls" property test | ◐ |
| I73 | ◐ implicit via cache_writer.delete path | ◐ |
| I74 | ◐ catchup test covers replay; ❌ recreate-safe + 24h-window edges | ◐ |
| I75 | daemon soft-delete tests | ✅ |
| I76 | ◐ implicit in fanout tests | ◐ |
| I77 | ❌ MISSING (token-hash + revocation tests) | ❌ |
| I78 | ❌ MISSING (read_only 403 on create/modify/delete) | ❌ |
| I79 | ◐ implicit (migrations + contract e2e) | ◐ |
| I80 | server `test_sync_routes_health.py` | ✅ |
| I81 | ❌ MISSING daemon guard test (materialize `Probe … 'q' – "d" 🚨.md` byte-identical on Windows) — specified by S479 E1, not yet implemented | ❌ |
| I82 | ◐ single-instance wired, untested | ◐ |

**Coverage summary:** 38 ✅ direct, 27 ◐ partial/implicit, 17 ❌ MISSING.
The 17 MISSING rows (I11, I14, I19, I24, I26, I27, I35, I36, I41, I53, I77, I78,
I81 + explicit-gap halves of I16/I37/I46/I74) are the priority S1-B golden-scenario
seed list, alongside the incident replays below.

## 18. Incident index → invariants (S1-B golden-scenario corpus)

| # | Incident | Date | Invariants encoded |
|---|----------|------|--------------------|
| N1 | SSE→materialize→watcher→push echo loop | v0.4.8, 2026-06 | I40, I25 |
| N2 | Read-walk journal storm (notify Access/Metadata) | v0.4.9 | I41 |
| N3 | Journal capacity storm + cap deadlock + int-array bloat (S489/S490) | v0.4.5-v0.4.7 | I43, I44, I45, I50 |
| N4 | Mainframe-prefix sync dup loop (22,574 rows; multi-host cross-chase) | 2026-06-02→04 | I35, I36, I12 |
| N5 | ctime-clobber + enrichment churn (641 files; 1588×/20min re-enrich) | 2026-06-04 | I64, I65, I19 (+ enrichment idempotency `default=str` fix) |
| N6 | CP437 mojibake filenames (90 dupes, ingest-layer origin) | S479, 2026-05-28 | I81 |
| N7 | Reconcile 404 (legacy SQLite endpoint) + ConflictUnrecoverable | S493/S494 | I6, I29, I33 |
| N8 | S498 push idempotency + birthtime (29k mtime-only "drift") | 2026-06-04→11 | I17, I34, I70, I64, I65 |
| N9 | Served-hash ≠ fs_hash churn (S481 / 2026-06-08) | 2026-06 | I14, I15, I18 |
| N10 | Catchup-snapshot race / missing-op envelopes (S476) | 2026-05 | I10, I22, I24 |
| N11 | Orphan-local unbounded retry (node_modules class) | 2026-05 | I49, I51, I58 |

Multi-host cross-chase (N4's loop engine) is the canonical convergence scenario:
N daemons + server MUST reach `every local sha == fs_hash` with bounded push
counts and zero steady-state churn — the S1-B property assertions.

## 19. Explicitly deferred (recorded per burn retry-note)

- **M3 distribution** (Opus, 6/12 per A0.4.1): nexus-repo `docs/` copy, README
  references in both repos, lattice-pvd-preflight wiring, PR-template
  invariant-citation rule. The vault-sync repo `docs/SYNC_CONTRACT.md` copy ships
  with this burn; the nexus live tree is NOT touched from this worktree (D4).
- **Tombstone-vs-offline>24h gap** (I74 note): contract flags it; resolution
  (longer retention vs a pull-capable reconcile) is an owner decision, likely an
  S1-B/S1-H follow-up.
- **Enrichment idempotency-hash invariant** (N5's `default=str` fix): captured in
  N5/I19 context; a dedicated enrichment-pipeline contract is outside the sync
  protocol surface — candidate for the S4 lane.
- Wire-schema JSON-Schema files (machine-checkable) — S1-B may generate them from
  this spec; not hand-authored here.

## 20. Acceptance against A0.5 expected exit state

- Normative spec spanning both repos with numbered invariants I1–I82: **this note**.
- Traceability table invariant → existing-or-MISSING test: **§17**.
- Vault note with FULL content (Triple Write, vault first): **this note**.
- Repo docs copy: `vault-sync:docs/SYNC_CONTRACT.md` (branch `whetstone/S1-A`).
- Owner M2 review queue: **§16** (8 policy invariants).
- S1-B gate: scenario seed list = §17 MISSING rows + §18 incident corpus, keyed to
  invariant IDs. **Gate opens on owner agreement (M2).**
