---
type: plan
project: Nexus
created: 2026-06-09
updated: 2026-06-09
status: in-progress
priority: P0
pvd_conformant: true
tags: [nexus, sync, vault-sync, convergence, daemon, rust, implementation-plan]
related:
  - "[[2026-06-09 Nexus Sync Convergence - Root Cause + Completion Handoff]]"
---

# Nexus Sync Daemon — Reconcile Server-Wins + Shadow Marker (Convergence Completion)

> **Mandate (from the P0 handoff):** Nexus Sync must COMPLETELY converge — all enabled
> hosts reach a flat server head, stale local files self-correct to canonical, a fresh
> note propagates fleet-wide within seconds and stays reconcile-stable, autostart re-enabled
> fleet-wide. Server-side root cause already fixed + deployed + verified
> (`nexus-reconciler:sha-aab25170a`). **This plan delivers the remaining daemon-side fix.**

## Problem (proven, byte-level)

The vault-sync daemon's reconcile backstop (`verify_repair.rs:256–304`) handles a server
`"drift"` delta (local file SHA ≠ server `vault_reconcile_state.fs_hash`) by
**unconditionally enqueueing a PUSH of the stale local**. For a host meant to MIRROR the
server, a stale prior-materialization must resolve **server-wins (PULL + overwrite local)**,
not push. The daemon cannot currently distinguish a *genuine new local user edit* (push)
from a *stale prior-materialization* (pull) because **there is no persistent per-file
"last-synced server hash" (shadow) marker** — the only hash memory is `echo_guard`
(in-memory, 15 s TTL). Result on re-enable: sustained push churn (~5/s, ~70 pushes/min),
the test note never corrects, head never flattens.

Two prior-known sub-issues are subsumed by this fix:
- **(b) Stale locals materialized before the server serve-fix** hold the old served body and
  the daemon never re-pulls them (their `change_seq < cursor`; SSE catchup only delivers
  `> cursor`). The reconcile backstop's full-manifest `reconcile-batch` comparison sees these
  as `"drift"`, so once reconcile PULLs-on-stale they self-correct — **no manual cursor-reset
  re-pull needed** (it remains a documented fallback).
- **(catchup-snapshot race)** Notes created during a catchup window are stranded below the
  cursor. Same mechanism: the periodic reconcile-batch sweep re-detects them as drift and the
  server-wins PULL delivers them. The backstop IS the durable closure of the race.

## Existing primitives we build on (verified in source)

- **PULL primitive already exists + already server-wins:** `api_client.fetch_note(path)` →
  `NotePayload`; `materializer.write(payload)` overwrites a divergent local server-wins with
  conflict-stash safety (`materializer.rs:450–485`, `ConflictPolicy::ServerWins`).
- **`Materializer` is `Clone`** (shallow — `PathBuf`/`String` + `Arc` fields), so the
  reconcile task can hold its own handle sharing the same `Arc` substores.
- **Reconcile backstop** spawns per `sync_root`, every `VAULT_SYNC_RECON_INTERVAL_SECS`
  (default 600 s), skipping the first immediate tick (`reconciliation.rs:268–311`).
- **Runtime-state path convention:** `<workspace_root>/.lattice-runtime/<subscriber_id>/sync-state/`
  (alongside `last_event_id`, `push_journal.jsonl`).

## Design decision (judgment call — documented, not blocking)

**Shadow-absent → PULL (server-wins, with conflict-stash).** On first run after deploy, no
file has a shadow entry, so every `"drift"` resolves server-wins → the reconcile pass itself
converges all pre-fix stale locals (subsumes the manual cursor-reset re-pull). This matches
the handoff's intent ("a genuine new local user edit is the *only* case that should push").
Lossless: divergent locals are conflict-stashed by the existing `ServerWins` policy before
overwrite, so any un-pushed pre-tracking local edit is recoverable from the `.conflict` stash.
After the first pass, shadow entries exist and genuine local edits (local ≠ shadow == server)
correctly push.

**Decision table (per `"drift"` delta):**

| local vs server | shadow present? | shadow vs server | action | rationale |
|---|---|---|---|---|
| `local == server` | — | — | no-op (server returns `"match"`) | in sync |
| `local != server` | yes | `local == shadow` | **PULL** (server-wins) | server changed since last sync; local is stale |
| `local != server` | yes | `local != shadow` | **PUSH** (base = server_hash) | genuine local edit since last sync |
| `local != server` | **no** | — | **PULL** (server-wins, stash) | no sync record → mirror-safe default; converges pre-fix stale |
| `missing-on-server` | — | — | **PUSH** (create) | genuine new local file (unchanged behavior) |

Durability note: the shadow store tolerates loss — a crash between flushes reverts affected
paths to shadow-absent → an extra (idempotent) server-wins PULL on next reconcile. So
debounced/periodic persistence (no per-write fsync) is correct and cheap.

## Tasks

### Task 1 — `sync_shadow.rs`: persistent per-file shadow-hash store
- [ ] Create `src-tauri/src/sync_shadow.rs` with `pub struct ShadowStore` holding
      `inner: Mutex<HashMap<String,String>>` (path → last_synced_server_hash) + the on-disk
      path + a dirty flag.
- [ ] `ShadowStore::load(path: PathBuf) -> Arc<Self>` — read the file if present (JSON map or
      JSONL), tolerate a missing/corrupt file (start empty, log WARN — never panic).
- [ ] `record(&self, path: &str, server_hash: &str)` — upsert + mark dirty (cheap, no I/O).
- [ ] `get(&self, path: &str) -> Option<String>` — read for the reconcile decision.
- [ ] `flush(&self) -> io::Result<()>` — atomic tmp+rename write of the full map ONLY if dirty;
      clear dirty on success. Bound memory: it is one entry per synced note (~32k × ~80 B ≈ 2.5 MB).
- [ ] `spawn_periodic_flush(self: Arc<Self>, interval)` — background task flushing every ~30 s;
      also expose a `flush()` for shutdown.
- [ ] Register the module in `lib.rs` (`pub mod sync_shadow;`).
- [ ] `[V:test]` Unit tests: load-empty, record+get, persist→reload round-trip, corrupt-file →
      empty+WARN (no panic), dirty-gating (flush no-op when clean).
- [ ] `[V:shell]` `cd ~/Dev/vault-sync/src-tauri && cargo test sync_shadow` → green.

### Task 2 — Materializer records shadow on every successful write
- [ ] Add `shadow_store: Option<Arc<ShadowStore>>` field to `Materializer` + builder
      `with_shadow_store(self, Arc<ShadowStore>) -> Self` (mirror `with_echo_guard`).
- [ ] In `Materializer::write`, on the success paths that actually wrote canonical bytes
      (`Wrote`, `Stashed`, AND the idempotent `IdenticalToLocal` skip — local already equals
      server, so the marker is correct), call `shadow.record(&payload.path, &payload.sha256)`.
      Do NOT record on `IntegrityFailed` / `Skipped(SubstrateRefused|DisabledMode)`.
- [ ] `[V:test]` Unit test: write a payload through a `Materializer` wired with a `ShadowStore`,
      assert `shadow.get(path) == Some(payload.sha256)`; integrity-fail path does NOT record.
- [ ] `[V:shell]` `cargo test materializer` → green.

### Task 3 — PushClient records shadow on successful ack
- [ ] Add `shadow_store: Option<Arc<ShadowStore>>` to `PushClient` + a builder/param.
- [ ] On a push that the server ACCEPTED/MERGED (the local content is now the server's
      canonical), call `shadow.record(&evt.path, &content_sha)` where `content_sha` is the
      hash the daemon pushed (the server's new `fs_hash`). For `Merged`, record the server's
      returned `server_hash`/`content_hash` (the merged result is canonical), not the local.
- [ ] Do NOT record on `ConflictUnrecoverable` / `Error` / delete.
- [ ] `[V:test]` Unit test (or focused logic test) asserting the shadow is updated on Accepted
      and NOT on Conflict.
- [ ] `[V:shell]` `cargo test push_client` → green.

### Task 4 — VerifyRepair: shadow-aware reconcile direction + PULL execution
- [ ] Give `VerifyRepair` a `materializer: Materializer` (clone) + `shadow: Arc<ShadowStore>`
      (via constructor params or builders). The materializer is the PULL executor; the shadow
      is the direction oracle.
- [ ] In `run()`'s `"drift"` arm, replace the unconditional `pending_pushes.push(...)` with the
      Decision table above:
      - compute `local_hash = local.content_hash` (already in the manifest entry),
        `server_hash = delta.server_hash`, `shadow_hash = self.shadow.get(&delta.path)`.
      - **PUSH** iff `shadow_hash == Some(local_hash)`-distinct... precisely: PUSH iff
        `shadow_hash.is_some() && shadow_hash.as_deref() != server_hash.as_deref()`
        (local edit: server unchanged since sync, local diverged) — equivalently
        `local != server && local == shadow` is impossible here (drift means local≠server) so the
        push test is `shadow == local_at_last_sync && server == shadow`→ implement as:
        `let is_local_edit = matches!(&shadow_hash, Some(s) if Some(s.as_str()) == server_hash.as_deref());`
        (shadow records the last server hash we synced; if it still equals the current server
        hash, the server has NOT changed, so the only reason local≠server is a genuine local
        edit → PUSH). Otherwise (shadow absent, or shadow ≠ server ⇒ server moved) → PULL.
- [ ] Collect PULL deltas into a `pending_pulls: Vec<String>` (paths). Keep PUSH batching as-is.
- [ ] After the loop, execute pulls: for each path, `api.fetch_note(path)` → `materializer.write(payload)`
      (materializer records shadow + server-wins-overwrites + stashes). Bound concurrency
      (e.g. a small buffered stream, ≤4 in flight) so a full-corpus first pass doesn't hammer
      the server; log a per-pull INFO and a final pulled-count. A fetch/write error is recorded
      in `report.errors` and SKIPS that path (no crash, retried next pass — idempotent).
- [ ] Surface counts honestly: set `report.add_count` (= pulls executed) + `add_paths_sample`
      (the long-dead pull-reporting fields finally carry real data); keep `modify_count` = pushes.
      No silent caps — if pulls are truncated for any reason, `log()`/report it.
- [ ] `[V:test]` Unit tests with a mock api + temp vault: (a) shadow-absent drift → PULL (local
      file overwritten to server bytes, shadow updated); (b) shadow==server drift → PUSH queued;
      (c) shadow≠server drift → PULL; (d) `match` → no-op; (e) `missing-on-server` → PUSH.
- [ ] `[V:shell]` `cargo test verify_repair` → green.

### Task 5 — Plumb shadow + materializer through construction (lib.rs / reconciliation.rs)
- [ ] In `run()` (lib.rs ~515) construct `let shadow = ShadowStore::load(<workspace>/.lattice-runtime/<subscriber_id>/sync-state/shadow_hashes)` (one per subscriber, like `echo_guard`); spawn its periodic flush.
- [ ] Wire `.with_shadow_store(shadow.clone())` onto the `Materializer` (line ~529).
- [ ] Pass `shadow.clone()` into `spawn_push_pipeline` → `PushClient` and into the reconcile spawn.
- [ ] Extend `spawn_reconciliation_task` / `spawn_reconciliation_tasks_for_roots` /
      `run_reconciliation_pass` signatures to accept `materializer: Materializer` +
      `shadow: Arc<ShadowStore>`, threading them into `VerifyRepair::new`.
- [ ] The reconcile task constructs its `Materializer` by cloning the daemon's (shared shadow +
      echo guard + same mode/root) — do NOT build a second divergent materializer.
- [ ] `[V:shell]` `cargo build` (full daemon) → compiles clean.

### Task 6 — Build + full test suite on Trinity
- [ ] `[V:shell]` `cd ~/Dev/vault-sync/src-tauri && cargo build --release` → success.
- [ ] `[V:shell]` `cargo test` (whole crate) → green (fix any regressions).
- [ ] Bump version → **v0.4.15** (`package.json`, `tauri.conf.json`, `Cargo.toml` all to 0.4.15).
- [ ] Produce a runnable artifact (macOS bundle / binary) for the Trinity one-host test.

### Task 7 — One-host monitored convergence test (Trinity only)
- [ ] Install the v0.4.15 daemon on Trinity ONLY; link stays OFF. Coordinate with link-cyril.
- [ ] Start under tight monitoring (server head growth/s, reconciler push rate, daemon CPU).
      Kill immediately if it storms (head sustained-growth or CPU pegged).
- [ ] `[V:manual]` After the first reconcile pass: `04_Entities/Individuals/Aaron Johnstone.md`
      local SHA == `3391d934` (server `fs_hash`); the note no longer churns.
- [ ] `[V:manual]` Server head growth → ~0/s within minutes; reconciler push rate → ~0.
- [ ] `[V:manual]` Plant a fresh server-side probe → propagates to Trinity within seconds,
      birthtime preserved, reconcile-stable (no subsequent re-push).

### Task 8 — DoD verification + fleet re-enable + retire manual mission
- [ ] `[V:skill]` Run `nexus-sync-postupdate-test` → GREEN (CREATE/EDIT/DELETE propagation +
      byte-faithfulness + idempotency + tombstoning) — only after all on-version.
- [ ] Install v0.4.15 on link; re-enable autostart fleet-wide (Trinity + link; **neo is DOWN**).
- [ ] `[V:manual]` Head stays FLAT for 30+ min with all daemons running concurrently
      (no multi-host cross-chase).
- [ ] Update `[[active_alert_continual_sync_mission]]` to retire the manual-propagation regime;
      close ticket TKT-1b16e492.
- [ ] Triple-write the OUTCOME (vault + memory + this plan status → done).

## Definition of Done (COMPLETELY fixed — all must hold)
1. Re-enable one host → head growth → ~0/s within minutes; reconciler push rate → ~0.
2. A previously-churning note (Aaron Johnstone) has local SHA == `fs_hash` (3391d934) after the pass.
3. Fresh probe propagates to every enabled host within seconds, birthtime preserved, reconcile-stable.
4. `nexus-sync-postupdate-test` GREEN across hosts.
5. Autostart re-enabled fleet-wide; head flat 30+ min with all daemons concurrent.

## Risk controls
- Most incident-prone component (CPU storms, ctime-clobber history). Therefore: TDD per module,
  full `cargo test`, **one-host monitored** rollout before any fleet re-enable, kill-on-storm.
- v0.4.14 birthtime fix is in the base → re-materialization is clobber-safe.
- Lossless server-wins: existing `ConflictPolicy::ServerWins` conflict-stash preserves any
  divergent local before overwrite.
- Daemon stays OFF on all hosts until the build is tested; no fleet action without DoD §1–3 on Trinity.

## Key file:line references
- Decision site: `verify_repair.rs:256–304` (`run()` drift arm), `build_modify_push` ~546–569.
- PULL primitives: `api_client.rs:267` (`fetch_note`), `materializer.rs:378–567` (`write`), server-wins `:450–485`.
- Reconcile spawn: `reconciliation.rs:138–196` (`run_reconciliation_pass`), `:268–311` (`spawn_reconciliation_task`).
- Wiring: `lib.rs:507–529` (materializer build), `:584–596` (push pipeline), `:811–817` (recon spawn).
- Echo guard pattern to mirror: `echo_guard.rs`, `materializer.rs:432–434`.
- Runtime-state path: `lib.rs:602–606`, `commands.rs:40–45`.
