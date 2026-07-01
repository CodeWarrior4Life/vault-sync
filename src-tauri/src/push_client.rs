//! v0.3 push client. Drains `PushJournal`, applies pre-HTTP guards
//! (substrate fence + extension gate + idempotency hash-compare with
//! frontmatter normalization), POSTs via `api_client::push()` with
//! exponential-backoff retry, then ack/nack journal entries based on
//! outcome.
//!
//! References:
//! * Mandate §4.1 (push_client module + api_client.push() method)
//! * Mandate §5 (push endpoint contract, 4-state response envelope, R20 UA)
//! * Mandate §1 row 4 (idempotency / toast-loop guard)
//! * Mandate §1 row 6 + R11 (substrate fence on push — never POST substrate)
//! * Mandate §1 row 10 + R16 (frontmatter race / normalize `updated:` rewrites)
//! * Mandate §1 row 11 (extension-gate outbound: only allowed exts pushed)
//! * Mandate §2 R2 (CAS / base_hash mismatch → caller refetches+merges+replays)
//!
//! Concurrency: `PushJournal` requires `&mut self`, so we hold it under a
//! single async `Mutex`. R12 single-instance guarantees no cross-process
//! contention.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use crate::api_client::{
    ApiClient, ApiError, PushApiAction, PushRequest, PushResponse, PushStatus,
};
use crate::push_journal::{JournalCursor, PushAction, PushEvent, PushJournal};
use crate::rasp_fence::{classify_path, PathClassification};
use crate::tray_state::SharedTrayState;

/// S5 (v0.4.28): how long `drain_once` stands down after the server's
/// min-daemon-version gate answers 426. Long enough to avoid a retry-loop
/// against a gate only an upgrade can pass; short enough that a fleet-wide
/// gate flip-back is picked up without a restart.
const GATE_COOLDOWN_SECS: u64 = 900;

/// Static configuration for the push client. Cheap to clone; cloned into
/// each retry-loop spawn.
#[derive(Debug, Clone)]
pub struct PushClientConfig {
    /// Extensions (with leading dot) the daemon is allowed to push.
    /// Anything outside this list is `Skipped(ExtensionFiltered)`.
    /// Mandate §1 row 11.
    pub allowed_extensions: Vec<String>,
    /// Frontmatter fields stripped before computing the diff-hash.
    /// E.g. `["updated"]` so Obsidian's automatic `updated:` rewrites
    /// don't trigger a spurious push (mandate §1 row 10 / R16).
    pub strip_frontmatter_fields_for_diff: Vec<String>,
    /// Max retry attempts on transient errors (5xx / network) before
    /// giving up with `FailureReason::NetworkExhausted`.
    pub max_retry_attempts: usize,
    /// First backoff sleep in ms. Doubles each attempt, capped at
    /// `max_backoff_ms`.
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
    /// How many events to pull per `drain_once()`.
    pub batch_size: usize,
    /// Sleep between drain ticks in `run_loop` when the journal is CAUGHT UP
    /// (idle). Long is fine — nothing to do.
    pub loop_interval_ms: u64,
    /// Max concurrent in-flight push CHAINS per drain. The batch is grouped by
    /// path; each path is a sequential chain (so the server's per-path CAS never
    /// races and per-path order + version history are preserved), and up to this
    /// many DISTINCT-path chains run concurrently. Bounds server load / 429 risk.
    pub push_concurrency: usize,
    /// Sleep between drain ticks when the journal STILL has work (backlog) —
    /// short so the loop drains a deep backlog fast instead of waiting a full
    /// `loop_interval_ms` between each batch.
    pub busy_loop_interval_ms: u64,
}

impl Default for PushClientConfig {
    fn default() -> Self {
        Self {
            allowed_extensions: vec![".md".into(), ".canvas".into()],
            strip_frontmatter_fields_for_diff: vec!["updated".into()],
            max_retry_attempts: 5,
            initial_backoff_ms: 500,
            max_backoff_ms: 60_000,
            batch_size: 32,
            loop_interval_ms: 5_000,
            push_concurrency: 6,
            busy_loop_interval_ms: 250,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SkipReason {
    SubstrateRefused {
        rule: &'static str,
    },
    ExtensionFiltered {
        ext: String,
    },
    IdenticalToServer {
        hash: String,
    },
    EmptyContent,
    /// A lazy (content_bytes: None) event whose file no longer exists at
    /// drain time — deleted since enqueue. No-op; ack so it doesn't wedge.
    FileVanished,
    /// Server rejected the push with HTTP 400 (e.g. path excluded by the V9
    /// baseline scope filter). A 400 is a PERMANENT reject — skip + ack, never
    /// retry (S481: previously retry-stormed). Defense-in-depth: the fence
    /// (`is_junk_path`) should already keep these from being enqueued.
    /// v0.4.28 (D4/M5a): HTTP 422 (non-UTF-8 / NUL content) is the same
    /// permanent-reject class and carries `status: 422`.
    ServerRejected {
        status: u16,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FailureReason {
    NetworkExhausted { last_error: String },
    ConflictUnrecoverable { expected_hash: Option<String> },
    Unauthorized,
    Forbidden,
    /// S5 (v0.4.28): the server's min-daemon-version gate answered HTTP 426.
    /// Permanent until the daemon binary is upgraded. NOT acked (the edit
    /// stays journaled); the push client enters a drain cooldown so it never
    /// retry-loops against a gate that cannot pass.
    UpgradeRequired { detail: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PushOutcome {
    Sent { seq: u64, content_hash: String },
    Merged { merged_content: String },
    ConflictMarkers { merged_content: String },
    Skipped(SkipReason),
    Failed(FailureReason),
}

pub struct PushClient {
    api: Arc<ApiClient>,
    journal: Arc<Mutex<PushJournal>>,
    device_id: String,
    config: PushClientConfig,
    /// Absolute path to the configured `vaults_root`. Used to resolve a
    /// `PushEvent.path` (forward-slash, relative to vaults_root with the
    /// vault folder as its first segment per S477) to an on-disk file so
    /// `content_bytes: None` (lazy) events can be read at drain time.
    vault_root: PathBuf,
    /// Optional tray telemetry sink (mandate §9 AG13). If set, the client
    /// increments `uploads_sent` / `uploads_failed` and updates
    /// `uploads_pending` after each `drain_once`.
    tray_state: Option<SharedTrayState>,
    /// Persistent per-file shadow-hash store (fix/reconcile-server-wins-shadow).
    /// After a push the server accepts (`Accepted`) the pushed local hash is
    /// now the server's canonical; on `Merged` the server's returned canonical
    /// hash is. Recording it keeps the reconcile backstop from later mistaking
    /// a freshly-pushed file for a stale-pull candidate. `None` = no recording.
    shadow_store: Option<Arc<crate::sync_shadow::ShadowStore>>,
    /// opfix-vaultsync-dormancy: shared progress-tracking handle. When set,
    /// every `drain_once` that processed at least one event stamps a fresh
    /// progress marker so the watchdog can distinguish "pipeline healthy"
    /// from "pipeline silent with pending diffs" (R1+R3).
    sync_health: Option<Arc<crate::sync_health::SyncHealth>>,
    /// S5 (v0.4.28): when Some(t) and now < t, drain_once returns immediately
    /// without touching the journal or the network - the server's
    /// min-daemon-version gate rejected us and only an upgrade (or gate
    /// change) can help. Std mutex: held only for a read/write of an Option.
    gate_cooldown_until: Arc<std::sync::Mutex<Option<std::time::Instant>>>,
}

impl PushClient {
    pub fn new(
        api: Arc<ApiClient>,
        journal: Arc<Mutex<PushJournal>>,
        device_id: String,
        config: PushClientConfig,
        vault_root: PathBuf,
    ) -> Self {
        Self {
            api,
            journal,
            device_id,
            config,
            vault_root,
            tray_state: None,
            shadow_store: None,
            sync_health: None,
            gate_cooldown_until: Arc::new(std::sync::Mutex::new(None)),
        }
    }

    /// S5 (v0.4.28): arm the min-version gate cooldown for `duration` from
    /// now. Production always passes `Duration::from_secs(GATE_COOLDOWN_SECS)`;
    /// tests can pass a short duration to prove expiry without a real sleep
    /// through the 900s window. Write side of the poisoning-recovery pair
    /// with the drain_once read check — both sides recover the same way so a
    /// panic while holding this lock can never wedge the gate open or closed.
    fn arm_gate_cooldown(&self, duration: Duration) {
        let mut g = self
            .gate_cooldown_until
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        *g = Some(std::time::Instant::now() + duration);
    }

    /// Builder-style: attach a SharedTrayState so push outcomes update the
    /// tray menu / tooltip in near-real-time. Backwards-compatible.
    pub fn with_tray_state(mut self, state: SharedTrayState) -> Self {
        self.tray_state = Some(state);
        self
    }

    /// Builder-style: attach the shared persistent
    /// [`ShadowStore`](crate::sync_shadow::ShadowStore). After this, an
    /// `Accepted`/`Merged` push records the now-canonical server hash for the
    /// pushed path. Backwards-compatible — without it, no recording.
    pub fn with_shadow_store(mut self, store: Arc<crate::sync_shadow::ShadowStore>) -> Self {
        self.shadow_store = Some(store);
        self
    }

    /// opfix-vaultsync-dormancy: attach the shared
    /// [`SyncHealth`](crate::sync_health::SyncHealth) handle. After this,
    /// every `drain_once` that processed at least one event stamps a fresh
    /// progress marker. Backwards-compatible; without it, no stamping
    /// (which would defeat the watchdog, so the production wire-up sets it).
    pub fn with_sync_health(mut self, health: Arc<crate::sync_health::SyncHealth>) -> Self {
        self.sync_health = Some(health);
        self
    }

    /// Public pre-journal gate. The file_watcher calls this BEFORE writing
    /// to the journal so we never queue events we'd just skip later.
    /// Returns `Some(reason)` to drop the event, `None` to journal it.
    pub fn pre_journal_filter(
        &self,
        path: &str,
        content_bytes: &[u8],
        last_server_hash: Option<&str>,
    ) -> Option<SkipReason> {
        // 1. Substrate fence (R11).
        if let PathClassification::Substrate { rule } = classify_path(path) {
            return Some(SkipReason::SubstrateRefused { rule });
        }
        // 2. Extension gate (§1 row 11).
        if let Some(ext_skip) = check_extension(path, &self.config.allowed_extensions) {
            return Some(ext_skip);
        }
        // 3. Empty-content guard.
        if content_bytes.is_empty() {
            return Some(SkipReason::EmptyContent);
        }
        // 4. Idempotency via normalized SHA (§1 row 4 + R16).
        if let Some(server) = last_server_hash {
            let normalized = self.normalize_for_diff(content_bytes);
            let local = sha256_hex(&normalized);
            if local == server {
                return Some(SkipReason::IdenticalToServer { hash: local });
            }
        }
        None
    }

    /// Strip designated frontmatter fields before hashing-for-diff. If the
    /// content has no leading `---\n` frontmatter, return it unchanged. If
    /// the closing `---` is missing, also return unchanged (don't risk
    /// corrupting non-frontmatter content). R16.
    pub fn normalize_for_diff(&self, content: &[u8]) -> Vec<u8> {
        // We treat content as UTF-8; non-UTF-8 binary just passes through.
        let s = match std::str::from_utf8(content) {
            Ok(s) => s,
            Err(_) => return content.to_vec(),
        };
        if !s.starts_with("---\n") && !s.starts_with("---\r\n") {
            return content.to_vec();
        }
        // Locate end of frontmatter: a line of just `---` (with optional \r).
        let body_start = match find_frontmatter_end(s) {
            Some(i) => i,
            None => return content.to_vec(),
        };
        let fm_block = &s[4..body_start.fm_inner_end];
        let body = &s[body_start.body_start..];

        let stripped_fm =
            strip_yaml_fields(fm_block, &self.config.strip_frontmatter_fields_for_diff);

        let mut out = String::with_capacity(content.len());
        out.push_str("---\n");
        out.push_str(&stripped_fm);
        if !stripped_fm.ends_with('\n') {
            out.push('\n');
        }
        out.push_str("---\n");
        out.push_str(body);
        out.into_bytes()
    }

    /// Drain up to `batch_size` events, process them through retry+backoff,
    /// ack/nack each in the journal. Returns the (event, outcome) pairs in
    /// drain order for the caller's tray/log layer.
    pub async fn drain_once(&self) -> Vec<(PushEvent, PushOutcome)> {
        // S5 (v0.4.28): min-daemon-version gate cooldown. After a 426 we stand
        // down entirely - no journal drain, no HTTP - until the window passes.
        {
            let until = *self
                .gate_cooldown_until
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            if let Some(until) = until {
                if std::time::Instant::now() < until {
                    tracing::debug!(
                        "push_client: min-version gate cooldown active - skipping drain tick"
                    );
                    return Vec::new();
                }
            }
        }

        let batch: Vec<(PushEvent, JournalCursor)> = {
            let mut j = self.journal.lock().await;
            match j.drain(self.config.batch_size) {
                Ok(v) => v,
                Err(e) => {
                    tracing::error!("push_journal drain failed: {e}");
                    return Vec::new();
                }
            }
        };

        // Router-by-path concurrency: group the drained batch by path so each
        // path becomes a SEQUENTIAL chain — the server's per-path CAS never
        // races, and per-path order + version-history granularity are preserved
        // (we push every event, never collapse). Up to `push_concurrency`
        // DISTINCT-path chains then run CONCURRENTLY. Grouping preserves drain
        // order within each path's chain (HashMap insertion appends in order).
        use futures::stream::{self, StreamExt};
        let mut groups: std::collections::HashMap<String, Vec<(PushEvent, JournalCursor)>> =
            std::collections::HashMap::new();
        for (evt, cur) in batch {
            groups.entry(evt.path.clone()).or_default().push((evt, cur));
        }
        let chain_results: Vec<Vec<(PushEvent, JournalCursor, PushOutcome)>> =
            stream::iter(groups.into_values())
                .map(|chain| async move {
                    let mut chain_out = Vec::with_capacity(chain.len());
                    for (evt, cur) in chain {
                        // Sequential within a path → no same-path CAS race.
                        let outcome = self.process_event(&evt).await;
                        chain_out.push((evt, cur, outcome));
                    }
                    chain_out
                })
                .buffer_unordered(self.config.push_concurrency.max(1))
                .collect()
                .await;

        // Fold results: ack terminal-success cursors (batched under one lock),
        // leave failures in the journal (nack is a no-op → retried next tick),
        // and update tray telemetry. `out` is in chain-completion order.
        let mut to_ack: Vec<JournalCursor> = Vec::new();
        let mut out = Vec::new();
        for (evt, cur, outcome) in chain_results.into_iter().flatten() {
            let should_ack = matches!(
                outcome,
                PushOutcome::Sent { .. }
                    | PushOutcome::Merged { .. }
                    | PushOutcome::ConflictMarkers { .. }
                    | PushOutcome::Skipped(_)
                    | PushOutcome::Failed(FailureReason::Unauthorized)
                    | PushOutcome::Failed(FailureReason::Forbidden)
                    | PushOutcome::Failed(FailureReason::ConflictUnrecoverable { .. })
            );
            if should_ack {
                to_ack.push(cur);
            }
            // v0.3 tray telemetry — increment on Sent (success) or Failed
            // (NetworkExhausted only; auth/conflict outcomes are explicit
            // skip-class, not "failure" for the dashboard's purposes).
            if let Some(tray) = &self.tray_state {
                if let Ok(mut w) = tray.write() {
                    match &outcome {
                        PushOutcome::Sent { .. } => w.inc_uploads_sent(),
                        PushOutcome::Failed(FailureReason::NetworkExhausted { .. })
                        | PushOutcome::Failed(FailureReason::UpgradeRequired { .. }) => {
                            w.inc_uploads_failed()
                        }
                        _ => {}
                    }
                }
            }
            out.push((evt, outcome));
        }
        if !to_ack.is_empty() {
            let mut j = self.journal.lock().await;
            let _ = j.ack_batch(to_ack);
        }

        // Snapshot the journal depth so the tray's "Pending uploads" item
        // reflects what's still queued.
        let pending_after = {
            let j = self.journal.lock().await;
            j.len()
        };
        if let Some(tray) = &self.tray_state {
            if let Ok(mut w) = tray.write() {
                w.set_uploads_pending(pending_after);
            }
        }
        // opfix-vaultsync-dormancy (R1+R3): stamp progress whenever the
        // pipeline DID work this tick: either it processed events, OR the
        // journal is now empty (which means a prior drain caught up). The
        // stamp is a never-failing test that the loop is actually pumping.
        // Stamping on `out.is_empty() && pending_after > 0` would lie about
        // progress on a hung-but-pending pipeline; we deliberately mark only
        // when we observed forward motion this tick OR the backlog is gone.
        if let Some(health) = &self.sync_health {
            if !out.is_empty() || pending_after == 0 {
                health.mark_progress();
            }
        }
        out
    }

    /// Run drain_once on an ADAPTIVE interval until shutdown signal flips to
    /// `true`. When a drain did work (a backlog is being chewed through) the
    /// next tick fires after the short `busy_loop_interval_ms`; when caught up
    /// (no outcomes) it waits the full `loop_interval_ms`. This drains a deep
    /// backlog quickly without busy-spinning while idle.
    pub async fn run_loop(&self, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        loop {
            // Drain one batch.
            let outcomes = self.drain_once().await;
            // Backlog present (we did work) → short interval; idle → full interval.
            let interval = if outcomes.is_empty() {
                Duration::from_millis(self.config.loop_interval_ms)
            } else {
                Duration::from_millis(self.config.busy_loop_interval_ms)
            };
            for (evt, outcome) in &outcomes {
                match outcome {
                    PushOutcome::Sent { seq, content_hash } => {
                        tracing::info!(path = %evt.path, seq, hash=%content_hash, "push accepted");
                    }
                    PushOutcome::Merged { .. } => {
                        tracing::info!(path = %evt.path, "push merged");
                    }
                    PushOutcome::ConflictMarkers { .. } => {
                        tracing::warn!(path = %evt.path, "push produced conflict markers");
                    }
                    PushOutcome::Skipped(r) => {
                        tracing::debug!(path = %evt.path, reason=?r, "push skipped");
                    }
                    PushOutcome::Failed(r) => {
                        tracing::warn!(path = %evt.path, reason=?r, "push failed");
                    }
                }
            }
            // Sleep with cancel.
            tokio::select! {
                _ = tokio::time::sleep(interval) => {},
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        tracing::info!("push_client.run_loop received shutdown");
                        return;
                    }
                }
            }
        }
    }

    async fn process_event(&self, evt: &PushEvent) -> PushOutcome {
        // Pre-HTTP guards (defense-in-depth — also applied at pre-journal).
        if let PathClassification::Substrate { rule } = classify_path(&evt.path) {
            tracing::warn!(path = %evt.path, rule, "substrate path reached journal — refusing to POST");
            return PushOutcome::Skipped(SkipReason::SubstrateRefused { rule });
        }
        if let Some(ext_skip) = check_extension(&evt.path, &self.config.allowed_extensions) {
            return PushOutcome::Skipped(ext_skip);
        }

        let action = match evt.action {
            PushAction::Create => PushApiAction::Create,
            PushAction::Modify => PushApiAction::Modify,
            PushAction::Delete => PushApiAction::Delete,
        };

        // Resolve content. `Some` → use the embedded bytes. `None` (lazy,
        // e.g. enqueued by verify_repair) → read from disk NOW. This read
        // happens AFTER the substrate + extension gates above so we never
        // touch a file we'd only skip. A Delete carries no body. If the file
        // vanished since enqueue (deleted concurrently), skip + ack.
        let content_bytes: Vec<u8> = match &evt.content_bytes {
            Some(bytes) => bytes.clone(),
            None => {
                if matches!(evt.action, PushAction::Delete) {
                    Vec::new()
                } else {
                    let abs = self.vault_root.join(forward_slash_to_path(&evt.path));
                    match std::fs::read(&abs) {
                        Ok(b) => b,
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                            tracing::info!(
                                path = %evt.path,
                                "push_client: lazy file vanished before push — skip + ack"
                            );
                            return PushOutcome::Skipped(SkipReason::FileVanished);
                        }
                        Err(e) => {
                            tracing::warn!(path = %evt.path, error = %e, "push_client: lazy read failed");
                            return PushOutcome::Failed(FailureReason::NetworkExhausted {
                                last_error: format!("lazy read failed: {e}"),
                            });
                        }
                    }
                }
            }
        };
        let content_b64 = B64.encode(&content_bytes);

        // I29 (S513, TKT-2dc9a17e): backfill the CAS base from the shadow store.
        //
        // file_watcher emits real-time Create/Modify pushes with base_hash=None —
        // it explicitly delegates base-sourcing to THIS layer (see
        // file_watcher::to_push_event: "the push_client backfills base_hash by
        // reading the materializer index before sending"). That backfill was never
        // implemented: we sent "" for None. The server treats base "" + an
        // existing row as a CAS conflict (sync_routes_p1 _cas_write_note_and_state:
        // `if base_hash == "" && current is not None -> conflict`). So every
        // real-time push of an already-synced note that genuinely diverged 409'd →
        // D4 stash → conflict-copy avalanche on live start, AND a conflict copy on
        // every steady-state local edit to an existing note.
        //
        // The shadow store holds our last-known server hash for the path (D9 seeds
        // it = server on startup for `match` ONLY — never `drift`, S531 fail-closed;
        // we record() it on every accepted push/pull below), so it IS the correct
        // CAS base. Sending it makes the
        // server see base == current → WRITE (operator-ratified local-wins-push-up
        // for a genuine edit); identical content is already a server-side
        // idempotent no-op; and a stale shadow (server truly moved since we synced)
        // still 409s → correctly stashed as a REAL conflict. Pushes that already
        // carry an explicit base (verify_repair::build_modify_push passes the
        // reconcile delta's server_hash) are honored verbatim. A DELETE ALSO has a
        // meaningful base — the last-synced hash of the file being removed — so we
        // shadow-backfill it too (S520, TKT-2c2e9d0f): the prior `Delete => None`
        // sent base_hash="" which the server's delete-CAS refuses whenever a
        // reconcile-state row exists (`base=="" && current!=None → 409`), so EVERY
        // delete of an already-synced note 409'd as ConflictUnrecoverable and was
        // dropped — deletes never propagated. With the shadow base: base==current →
        // server deletes (tombstones); base!=current (server edited since we synced)
        // → 409 → edit-beats-delete, no silent wipe; a never-synced file has no
        // shadow → "" → server no-op. A genuine new file's CREATE still has no
        // shadow → "" → CREATE, unchanged.
        let backfilled_base: Option<String> = match &evt.base_hash {
            Some(b) => Some(b.clone()),
            None => self.shadow_store.as_ref().and_then(|s| s.get(&evt.path)),
        };

        let req = PushRequest {
            device_id: &self.device_id,
            path: &evt.path,
            content_b64: &content_b64,
            // The server-side CAS base (checked against
            // vault_reconcile_state.fs_hash). Prefer the event's explicit base
            // (reconcile pushes), else the shadow-backfilled base (file_watcher
            // pushes); "" only when we truly have no known base → server CREATEs.
            base_hash: backfilled_base.as_deref().unwrap_or(""),
            action,
        };

        let mut last_err: Option<String> = None;
        for attempt in 0..self.config.max_retry_attempts {
            match self.api.push(&req).await {
                Ok(resp) => {
                    // fix/reconcile-server-wins-shadow: a push the server
                    // accepts means the canonical server hash is now known —
                    // record it so the reconcile backstop won't later mistake
                    // this just-pushed file for a stale-pull candidate.
                    // Accepted → our pushed local hash IS the canonical.
                    // Merged → the server's returned canonical hash is.
                    // ConflictMarkers / Error → no clean canonical, skip.
                    if let Some(sh) = &self.shadow_store {
                        if let Some(h) = shadow_hash_for_ack(
                            &resp.status,
                            &evt.content_sha,
                            resp.server_hash.as_deref(),
                            resp.content_hash.as_deref(),
                        ) {
                            sh.record(&evt.path, &h);
                        }
                    }
                    return map_response(resp);
                }
                Err(ApiError::Unauthorized) => {
                    return PushOutcome::Failed(FailureReason::Unauthorized);
                }
                Err(ApiError::Forbidden) => {
                    return PushOutcome::Failed(FailureReason::Forbidden);
                }
                Err(ApiError::Conflict { expected_hash }) => {
                    // D4 (S511, TKT-2dc9a17e): the local push lost the server CAS
                    // race (409). Before we consume/ack this journal entry (the
                    // caller acks ConflictUnrecoverable), PRESERVE the local bytes
                    // we tried to push as a `.conflict-from-*` sibling, so a local
                    // edit that loses the race is never silently dropped (the
                    // stash floor used to be pull-path-only). Best-effort: a stash
                    // failure is logged and the conflict still surfaces, but we
                    // never block on it. Only stash real content (a Delete carries
                    // none).
                    if !content_bytes.is_empty() {
                        self.stash_local_on_conflict(&evt.path, &content_bytes);
                    }
                    return PushOutcome::Failed(FailureReason::ConflictUnrecoverable {
                        expected_hash,
                    });
                }
                // S5 (v0.4.28): the server's min-daemon-version gate rejected
                // this daemon (HTTP 426). Fail LOUDLY, arm the drain cooldown,
                // do NOT retry, do NOT ack (the caller leaves UpgradeRequired
                // in the journal so the edit survives the upgrade).
                Err(ApiError::UpgradeRequired { detail }) => {
                    tracing::error!(
                        path = %evt.path,
                        detail = %detail,
                        cooldown_secs = GATE_COOLDOWN_SECS,
                        "SERVER MIN-DAEMON-VERSION GATE rejected this daemon (HTTP 426). \
                         This daemon is below the server's NEXUS_SYNC_MIN_DAEMON_VERSION. \
                         Standing down all pushes for the cooldown window; upgrade the daemon binary."
                    );
                    self.arm_gate_cooldown(Duration::from_secs(GATE_COOLDOWN_SECS));
                    return PushOutcome::Failed(FailureReason::UpgradeRequired { detail });
                }
                // HTTP 400 = permanent reject (e.g. path excluded by V9 baseline
                // scope filter). Skip + ack so it never retry-storms (S481).
                // HTTP 422 = permanent reject too (D4/M5a, v0.4.28): the Piece 1
                // server 422s non-UTF-8 / NUL content; re-sending the same bytes
                // can never succeed. Before this arm, 422 fell into the generic
                // transient arm -> 5 retries -> NetworkExhausted -> NOT acked ->
                // journal-wedged, retried every drain tick forever.
                Err(ApiError::Server(status @ (400 | 422))) => {
                    tracing::info!(
                        path = %evt.path,
                        status,
                        "push rejected permanently (HTTP {status}) - skip + ack, no retry"
                    );
                    return PushOutcome::Skipped(SkipReason::ServerRejected { status });
                }
                Err(e) => {
                    last_err = Some(e.to_string());
                    let backoff = compute_backoff(
                        attempt,
                        self.config.initial_backoff_ms,
                        self.config.max_backoff_ms,
                    );
                    tracing::debug!(
                        path = %evt.path,
                        attempt,
                        backoff_ms = backoff,
                        err = %last_err.as_deref().unwrap_or(""),
                        "push retry"
                    );
                    tokio::time::sleep(Duration::from_millis(backoff)).await;
                }
            }
        }
        PushOutcome::Failed(FailureReason::NetworkExhausted {
            last_error: last_err.unwrap_or_else(|| "unknown".to_string()),
        })
    }

    /// D4 (S511, TKT-2dc9a17e): preserve the local bytes that lost a server
    /// CAS-409 race as a `<stem>.conflict-from-<device>-<seq>.md` sibling,
    /// BEFORE the journal entry is acked/consumed, so a losing local edit is
    /// never silently dropped. `change_seq` is unknown on a bare 409 (we only
    /// get `expected_hash`), so the stash uses `0`; the conflict copy is then
    /// excluded from sync by name (D5) and surfaced in the tray count.
    /// Best-effort: any stash error is logged, never fatal (the conflict is
    /// still surfaced to the caller).
    fn stash_local_on_conflict(&self, wire_path: &str, local_bytes: &[u8]) {
        let stasher = crate::conflict_stash::ConflictStash::new(
            self.vault_root.clone(),
            crate::conflict_stash::ConflictPolicy::NewerWins,
        );
        match stasher.write_stash(wire_path, local_bytes, &self.device_id, 0) {
            Ok(stash) => {
                tracing::warn!(
                    path = %wire_path,
                    stash = %stash.display(),
                    "push_client: CAS-409 conflict, stashed losing local bytes before ack (S511 D4)"
                );
            }
            Err(e) => {
                tracing::warn!(
                    path = %wire_path,
                    error = ?e,
                    "push_client: CAS-409 conflict stash FAILED, local bytes may be at risk"
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/// Decide which hash (if any) to record into the ShadowStore for a push the
/// server acked. Pure + table-tested.
///
/// * `Accepted` → the local bytes we pushed are now the server's canonical →
///   record `local_sha`.
/// * `Merged` → the server merged a concurrent edit and its returned canonical
///   differs from what we pushed → record the server's canonical
///   (`server_hash`, falling back to `content_hash` if the server only echoes
///   that field). If neither is present, fall back to `local_sha` so we still
///   record SOMETHING (better an approximate marker than none — drift will
///   self-correct on the next pass).
/// * `ConflictMarkers` / `Error` → no clean canonical was established → record
///   nothing.
fn shadow_hash_for_ack(
    status: &PushStatus,
    local_sha: &str,
    server_hash: Option<&str>,
    content_hash: Option<&str>,
) -> Option<String> {
    match status {
        PushStatus::Accepted => Some(local_sha.to_string()),
        PushStatus::Merged => Some(
            server_hash
                .or(content_hash)
                .unwrap_or(local_sha)
                .to_string(),
        ),
        PushStatus::ConflictMarkers | PushStatus::Error => None,
    }
}

fn map_response(resp: PushResponse) -> PushOutcome {
    match resp.status {
        PushStatus::Accepted => PushOutcome::Sent {
            seq: resp.seq.unwrap_or(0),
            content_hash: resp.content_hash.unwrap_or_default(),
        },
        PushStatus::Merged => PushOutcome::Merged {
            merged_content: resp.merged_content.unwrap_or_default(),
        },
        PushStatus::ConflictMarkers => PushOutcome::ConflictMarkers {
            merged_content: resp.merged_content.unwrap_or_default(),
        },
        PushStatus::Error => PushOutcome::Failed(FailureReason::NetworkExhausted {
            last_error: resp
                .message
                .unwrap_or_else(|| "server reported error".to_string()),
        }),
    }
}

/// Resolve a forward-slash vault-relative path to a `PathBuf`. `PathBuf`
/// accepts forward slashes on every platform we target.
fn forward_slash_to_path(s: &str) -> PathBuf {
    PathBuf::from(s)
}

fn check_extension(path: &str, allowed: &[String]) -> Option<SkipReason> {
    let lower = path.to_ascii_lowercase();
    if allowed
        .iter()
        .any(|ext| lower.ends_with(&ext.to_ascii_lowercase()))
    {
        None
    } else {
        let ext = lower
            .rsplit_once('.')
            .map(|(_, e)| format!(".{e}"))
            .unwrap_or_else(|| String::from("<none>"));
        Some(SkipReason::ExtensionFiltered { ext })
    }
}

fn compute_backoff(attempt: usize, initial_ms: u64, max_ms: u64) -> u64 {
    // initial * 2^attempt, capped. Saturating shifts to avoid overflow.
    let factor = 1u64.checked_shl(attempt as u32).unwrap_or(u64::MAX);
    initial_ms.saturating_mul(factor).min(max_ms)
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(bytes);
    hex::encode(h.finalize())
}

struct FrontmatterEnd {
    /// Byte offset where the inner YAML ends (exclusive — does NOT include
    /// the trailing `---` line).
    fm_inner_end: usize,
    /// Byte offset where the body starts (after the closing `---\n`).
    body_start: usize,
}

/// Locate the closing `---` of a YAML frontmatter block. Returns the
/// offsets needed to slice `[fm_inner]` and `[body]` out of the source.
/// Skips the leading `---\n`/`---\r\n` (4-5 bytes — handled by caller via
/// `s[4..]` slice when calling).
fn find_frontmatter_end(s: &str) -> Option<FrontmatterEnd> {
    // Start search after the opening `---\n` or `---\r\n`.
    let after_open = if s.starts_with("---\r\n") { 5 } else { 4 };
    let mut cursor = after_open;
    let bytes = s.as_bytes();
    while cursor < bytes.len() {
        // Find the next line start.
        let line_end = match bytes[cursor..].iter().position(|&b| b == b'\n') {
            Some(p) => cursor + p,
            None => return None,
        };
        let mut line = &s[cursor..line_end];
        // Trim trailing \r.
        if line.ends_with('\r') {
            line = &line[..line.len() - 1];
        }
        if line == "---" {
            return Some(FrontmatterEnd {
                fm_inner_end: cursor,
                body_start: line_end + 1,
            });
        }
        cursor = line_end + 1;
    }
    None
}

/// Cheap line-oriented strip of top-level YAML keys. We deliberately
/// avoid round-tripping through serde_yaml because (a) it would reorder
/// keys (unstable diff!), (b) it would normalize whitespace / quoting,
/// and (c) frontmatter often contains tag arrays or anchors that aren't
/// round-trip safe. A line-strip is sufficient for the diff-stability
/// goal (R16) — we only need DETERMINISTIC removal of the `updated:`
/// field, not full YAML rewrite.
fn strip_yaml_fields(fm_block: &str, fields: &[String]) -> String {
    if fields.is_empty() {
        return fm_block.to_string();
    }
    let mut out = String::with_capacity(fm_block.len());
    let mut skipping = false;
    for line in fm_block.lines() {
        // Determine if this is a top-level key line: starts at column 0
        // with `key:` and no leading whitespace.
        let is_top_level = !line.starts_with(' ') && !line.starts_with('\t');
        if is_top_level {
            let key = line.split_once(':').map(|(k, _)| k.trim()).unwrap_or("");
            if fields.iter().any(|f| f == key) {
                skipping = true;
                continue;
            }
            skipping = false;
        }
        if skipping {
            // Continuation of a stripped multi-line value.
            continue;
        }
        out.push_str(line);
        out.push('\n');
    }
    out
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api_client::ApiClient;
    use crate::push_journal::{PushAction, PushEvent, PushJournal, CURRENT_SCHEMA};
    use chrono::Utc;
    use mockito::Server;
    use std::path::PathBuf;
    use tempfile::TempDir;

    fn config_for_test() -> PushClientConfig {
        PushClientConfig {
            allowed_extensions: vec![".md".into(), ".canvas".into()],
            strip_frontmatter_fields_for_diff: vec!["updated".into()],
            max_retry_attempts: 3,
            initial_backoff_ms: 1,
            max_backoff_ms: 10,
            batch_size: 8,
            loop_interval_ms: 50,
            push_concurrency: 4,
            busy_loop_interval_ms: 5,
        }
    }

    fn evt(path: &str, body: &[u8]) -> PushEvent {
        PushEvent {
            schema_version: CURRENT_SCHEMA,
            id: crate::push_journal::new_event_id(),
            path: path.to_string(),
            action: PushAction::Modify,
            base_hash: Some("0".repeat(64)),
            content_sha: sha256_hex(body),
            content_bytes: Some(body.to_vec()),
            queued_at: Utc::now(),
            device_id: "dev-test".into(),
        }
    }

    fn make_journal_with(events: Vec<PushEvent>) -> (TempDir, Arc<Mutex<PushJournal>>) {
        let dir = TempDir::new().unwrap();
        let p: PathBuf = dir.path().join("push_journal.jsonl");
        let mut j = PushJournal::open(&p).unwrap();
        for e in events {
            j.append(e).unwrap();
        }
        (dir, Arc::new(Mutex::new(j)))
    }

    async fn make_client(base_url: &str, journal: Arc<Mutex<PushJournal>>) -> PushClient {
        let api = Arc::new(ApiClient::new(base_url, "vsk_test").unwrap());
        PushClient::new(
            api,
            journal,
            "dev-test".into(),
            config_for_test(),
            PathBuf::from("/nonexistent-vault-root"),
        )
    }

    async fn make_client_with_root(
        base_url: &str,
        journal: Arc<Mutex<PushJournal>>,
        vault_root: PathBuf,
    ) -> PushClient {
        let api = Arc::new(ApiClient::new(base_url, "vsk_test").unwrap());
        PushClient::new(
            api,
            journal,
            "dev-test".into(),
            config_for_test(),
            vault_root,
        )
    }

    // opfix-vaultsync-dormancy: regression test for the progress-stamping
    // path. drain_once MUST stamp the SyncHealth progress marker whenever it
    // processed at least one event; without this stamp the watchdog cannot
    // tell a working pipeline from a dormant one. The test exercises the
    // production wiring: PushClient::with_sync_health then drain_once then
    // stamp.
    //
    // Pre-fix this file does not even compile against HEAD (no `sync_health`
    // module, no `with_sync_health` builder). Red-on-old-code is therefore
    // structural, not behavioral.
    //
    // Uses real-time sleep (not tokio paused time) because SyncHealth measures
    // wall-clock via std::time::Instant, which tokio's virtual clock does
    // NOT advance.
    #[tokio::test]
    async fn drain_once_stamps_sync_health_progress_when_events_processed() {
        use crate::sync_health::SyncHealth;

        // Substrate-refused path is the cheapest "processed an event" path:
        // no HTTP call required, and we already know the test setup for it.
        let mut srv = Server::new_async().await;
        let _m = srv
            .mock("POST", "/api/sync/push")
            .expect(0)
            .with_status(200)
            .create_async()
            .await;
        let health = SyncHealth::new();

        // Sleep BEFORE seeding the journal so that the elapsed-since-start
        // clock advances measurably. After the sleep, secs_since_progress()
        // reads ~2s (last_progress was initialized to 0 in `new()`).
        tokio::time::sleep(std::time::Duration::from_millis(2100)).await;
        let before = health.secs_since_progress();
        assert!(
            before >= 1,
            "without mark_progress, elapsed should be >= 1s after sleep, got {before}s"
        );

        let (_d, journal) = make_journal_with(vec![evt("00_VAULT.md", b"x")]);
        let client = make_client(&srv.url(), journal.clone())
            .await
            .with_sync_health(health.clone());

        let outcomes = client.drain_once().await;
        assert_eq!(
            outcomes.len(),
            1,
            "drain_once must have processed the event"
        );

        let after = health.secs_since_progress();
        assert!(
            after < 1,
            "mark_progress must have stamped during drain_once \
             (elapsed before = {before}s, after = {after}s); \
             without the stamp `after` would still be ~{before}s"
        );
    }

    // opfix-vaultsync-dormancy: gate semantics. An empty journal (backlog
    // caught up) is a "progress" signal too. A daemon that finishes its
    // backlog and idles is healthy; the watchdog must not later interpret
    // that idleness as a stall. The stamp covers that case.
    #[tokio::test]
    async fn drain_once_stamps_progress_on_caught_up_empty_journal() {
        use crate::sync_health::SyncHealth;

        let mut srv = Server::new_async().await;
        let _m = srv
            .mock("POST", "/api/sync/push")
            .expect(0)
            .with_status(200)
            .create_async()
            .await;
        let health = SyncHealth::new();
        tokio::time::sleep(std::time::Duration::from_millis(2100)).await;
        let before = health.secs_since_progress();
        assert!(before >= 1, "elapsed should be measurable after sleep");

        let (_d, journal) = make_journal_with(vec![]); // empty
        let client = make_client(&srv.url(), journal.clone())
            .await
            .with_sync_health(health.clone());

        let outcomes = client.drain_once().await;
        assert!(outcomes.is_empty(), "no events queued; empty outcomes");

        let after = health.secs_since_progress();
        assert!(
            after < 1,
            "empty journal at end of drain stamps progress (caught-up signal); \
             elapsed before = {before}s, after = {after}s"
        );
    }

    // --- pre-journal filter / pure-function tests (no HTTP) ---

    #[tokio::test]
    async fn former_substrate_path_now_pushes() {
        // "substrate must sync" (2026-06-20): the push fence is lifted, so a
        // former-substrate path (00_VAULT.md) is pushed like any note — one
        // HTTP call, accepted.
        let mut srv = Server::new_async().await;
        let m = srv
            .mock("POST", "/api/sync/push")
            .expect(1)
            .with_status(200)
            .with_body(
                r#"{"status":"accepted","seq":1,"content_hash":"h","server_hash":null,"server_seq":null,"merged_content":null,"message":null}"#,
            )
            .create_async()
            .await;
        let (_d, journal) = make_journal_with(vec![evt("00_VAULT.md", b"x")]);
        let client = make_client(&srv.url(), journal.clone()).await;
        let outcomes = client.drain_once().await;
        assert_eq!(outcomes.len(), 1);
        assert!(
            matches!(outcomes[0].1, PushOutcome::Sent { .. }),
            "expected Sent (substrate pushes as content), got {:?}",
            outcomes[0].1
        );
        m.assert_async().await;
    }

    #[tokio::test]
    async fn disallowed_extension_skipped() {
        let mut srv = Server::new_async().await;
        let m = srv
            .mock("POST", "/api/sync/push")
            .expect(0)
            .with_status(200)
            .create_async()
            .await;
        let (_d, journal) = make_journal_with(vec![evt("notes/a.exe", b"x")]);
        let client = make_client(&srv.url(), journal.clone()).await;
        let outcomes = client.drain_once().await;
        match &outcomes[0].1 {
            PushOutcome::Skipped(SkipReason::ExtensionFiltered { ext }) => {
                assert_eq!(ext, ".exe");
            }
            other => panic!("expected ExtensionFiltered, got {other:?}"),
        }
        m.assert_async().await;
    }

    #[tokio::test]
    async fn identical_hash_skipped() {
        let (_d, journal) = make_journal_with(vec![]);
        let client = make_client("http://127.0.0.1:1", journal).await;
        let bytes = b"hello world";
        let server_hash = sha256_hex(bytes);
        let skip = client.pre_journal_filter("notes/a.md", bytes, Some(&server_hash));
        match skip {
            Some(SkipReason::IdenticalToServer { hash }) => assert_eq!(hash, server_hash),
            other => panic!("expected IdenticalToServer, got {other:?}"),
        }
    }

    #[test]
    fn normalize_strips_updated_field() {
        let cfg = config_for_test();
        let client = PushClient {
            api: Arc::new(ApiClient::new("http://127.0.0.1:1", "x").unwrap()),
            journal: Arc::new(Mutex::new(
                PushJournal::open(&TempDir::new().unwrap().path().join("j.jsonl")).unwrap(),
            )),
            device_id: "d".into(),
            config: cfg,
            vault_root: PathBuf::from("/v"),
            tray_state: None,
            shadow_store: None,
            sync_health: None,
            gate_cooldown_until: Arc::new(std::sync::Mutex::new(None)),
        };
        let raw = b"---\nupdated: 2026-05-27\ntitle: x\n---\nbody\n";
        let normalized = client.normalize_for_diff(raw);
        let s = String::from_utf8(normalized).unwrap();
        assert!(!s.contains("updated:"));
        assert!(s.contains("title: x"));
        assert!(s.contains("body"));
        // Raw hash != normalized hash.
        assert_ne!(sha256_hex(raw), sha256_hex(s.as_bytes()));
    }

    #[test]
    fn normalize_no_frontmatter_passthrough() {
        let cfg = config_for_test();
        let client = PushClient {
            api: Arc::new(ApiClient::new("http://127.0.0.1:1", "x").unwrap()),
            journal: Arc::new(Mutex::new(
                PushJournal::open(&TempDir::new().unwrap().path().join("j.jsonl")).unwrap(),
            )),
            device_id: "d".into(),
            config: cfg,
            vault_root: PathBuf::from("/v"),
            tray_state: None,
            shadow_store: None,
            sync_health: None,
            gate_cooldown_until: Arc::new(std::sync::Mutex::new(None)),
        };
        let raw = b"plain markdown body, no frontmatter\n";
        let out = client.normalize_for_diff(raw);
        assert_eq!(out, raw);
    }

    #[test]
    fn normalize_unusual_frontmatter_delimiters() {
        // `---` not at start → no frontmatter, passthrough.
        let cfg = config_for_test();
        let client = PushClient {
            api: Arc::new(ApiClient::new("http://127.0.0.1:1", "x").unwrap()),
            journal: Arc::new(Mutex::new(
                PushJournal::open(&TempDir::new().unwrap().path().join("j.jsonl")).unwrap(),
            )),
            device_id: "d".into(),
            config: cfg,
            vault_root: PathBuf::from("/v"),
            tray_state: None,
            shadow_store: None,
            sync_health: None,
            gate_cooldown_until: Arc::new(std::sync::Mutex::new(None)),
        };
        let raw = b"# heading\n---\nupdated: x\n---\nbody\n";
        let out = client.normalize_for_diff(raw);
        assert_eq!(out, raw);
    }

    #[test]
    fn frontmatter_only_rewrite_is_skipped() {
        let cfg = config_for_test();
        let client = PushClient {
            api: Arc::new(ApiClient::new("http://127.0.0.1:1", "x").unwrap()),
            journal: Arc::new(Mutex::new(
                PushJournal::open(&TempDir::new().unwrap().path().join("j.jsonl")).unwrap(),
            )),
            device_id: "d".into(),
            config: cfg,
            vault_root: PathBuf::from("/v"),
            tray_state: None,
            shadow_store: None,
            sync_health: None,
            gate_cooldown_until: Arc::new(std::sync::Mutex::new(None)),
        };
        let yesterday = b"---\nupdated: 2026-05-26\ntitle: x\n---\nbody\n";
        let today = b"---\nupdated: 2026-05-27\ntitle: x\n---\nbody\n";
        // server has yesterday's normalized hash
        let server_hash = sha256_hex(&client.normalize_for_diff(yesterday));
        // local has today's bytes; only `updated:` differs
        let skip = client.pre_journal_filter("notes/a.md", today, Some(&server_hash));
        assert!(matches!(skip, Some(SkipReason::IdenticalToServer { .. })));
    }

    // --- HTTP-driven behaviors ---

    #[tokio::test]
    async fn accepted_event_acks_journal() {
        let mut srv = Server::new_async().await;
        let _m = srv
            .mock("POST", "/api/sync/push")
            .with_status(200)
            .with_body(
                r#"{"status":"accepted","seq":1,"content_hash":"h","server_hash":null,"server_seq":null,"merged_content":null,"message":null}"#,
            )
            .create_async()
            .await;
        let (_d, journal) = make_journal_with(vec![evt("notes/a.md", b"hello")]);
        let client = make_client(&srv.url(), journal.clone()).await;
        let outcomes = client.drain_once().await;
        assert_eq!(outcomes.len(), 1);
        assert!(matches!(outcomes[0].1, PushOutcome::Sent { .. }));
        let j = journal.lock().await;
        assert_eq!(j.len(), 0);
    }

    /// Router-by-path drain: distinct paths run as concurrent chains while
    /// multiple events for the SAME path stay in one sequential chain. EVERY
    /// event is pushed (no collapse → version fidelity) and the journal fully
    /// drains. Regression for the parallelization redesign.
    #[tokio::test]
    async fn drain_routes_by_path_processes_all_events_and_drains() {
        let mut srv = Server::new_async().await;
        let _m = srv
            .mock("POST", "/api/sync/push")
            .with_status(200)
            .with_body(
                r#"{"status":"accepted","seq":1,"content_hash":"h","server_hash":null,"server_seq":null,"merged_content":null,"message":null}"#,
            )
            .create_async()
            .await;
        // 3 distinct paths + a 2nd event for one of them (same-path chain).
        let (_d, journal) = make_journal_with(vec![
            evt("notes/a.md", b"a1"),
            evt("notes/b.md", b"b1"),
            evt("notes/c.md", b"c1"),
            evt("notes/a.md", b"a2"), // same path as a1 → same sequential chain
        ]);
        let client = make_client(&srv.url(), journal.clone()).await;

        let outcomes = client.drain_once().await;
        assert_eq!(
            outcomes.len(),
            4,
            "every event is pushed (no collapse — preserves version history)"
        );
        assert!(
            outcomes
                .iter()
                .all(|(_, o)| matches!(o, PushOutcome::Sent { .. })),
            "all four accepted"
        );
        assert_eq!(
            journal.lock().await.len(),
            0,
            "journal fully drained (batch-acked)"
        );
        let mut paths: Vec<&str> = outcomes.iter().map(|(e, _)| e.path.as_str()).collect();
        paths.sort();
        paths.dedup();
        assert_eq!(paths, vec!["notes/a.md", "notes/b.md", "notes/c.md"]);
    }

    #[tokio::test]
    async fn lazy_none_content_reads_from_disk_at_drain() {
        // A content_bytes: None event must read the file body from the vault
        // root at drain time and POST those bytes.
        let vault = TempDir::new().unwrap();
        let rel = "notes/lazy.md";
        let body = b"lazily-read-body";
        let abs = vault.path().join(rel);
        std::fs::create_dir_all(abs.parent().unwrap()).unwrap();
        std::fs::write(&abs, body).unwrap();

        let mut srv = Server::new_async().await;
        // Assert the server receives the base64 of the on-disk body.
        let expected_b64 = B64.encode(body);
        let m = srv
            .mock("POST", "/api/sync/push")
            .match_body(mockito::Matcher::PartialJsonString(format!(
                r#"{{"content":"{expected_b64}"}}"#
            )))
            .with_status(200)
            .with_body(
                r#"{"status":"accepted","seq":1,"content_hash":"h","server_hash":null,"server_seq":null,"merged_content":null,"message":null}"#,
            )
            .expect(1)
            .create_async()
            .await;

        let lazy = PushEvent {
            schema_version: CURRENT_SCHEMA,
            id: crate::push_journal::new_event_id(),
            path: rel.to_string(),
            action: PushAction::Modify,
            base_hash: Some("0".repeat(64)),
            content_sha: sha256_hex(body),
            content_bytes: None,
            queued_at: Utc::now(),
            device_id: "dev-test".into(),
        };
        let (_d, journal) = make_journal_with(vec![lazy]);
        let client =
            make_client_with_root(&srv.url(), journal.clone(), vault.path().to_path_buf()).await;
        let outcomes = client.drain_once().await;
        assert_eq!(outcomes.len(), 1);
        assert!(matches!(outcomes[0].1, PushOutcome::Sent { .. }));
        m.assert_async().await;
    }

    #[tokio::test]
    async fn lazy_none_content_vanished_file_skips_and_acks() {
        // File deleted since enqueue → skip + ack (no HTTP, no wedge).
        let vault = TempDir::new().unwrap();
        let mut srv = Server::new_async().await;
        let m = srv
            .mock("POST", "/api/sync/push")
            .expect(0)
            .with_status(200)
            .create_async()
            .await;
        let lazy = PushEvent {
            schema_version: CURRENT_SCHEMA,
            id: crate::push_journal::new_event_id(),
            path: "notes/gone.md".to_string(),
            action: PushAction::Modify,
            base_hash: Some("0".repeat(64)),
            content_sha: "a".repeat(64),
            content_bytes: None,
            queued_at: Utc::now(),
            device_id: "dev-test".into(),
        };
        let (_d, journal) = make_journal_with(vec![lazy]);
        let client =
            make_client_with_root(&srv.url(), journal.clone(), vault.path().to_path_buf()).await;
        let outcomes = client.drain_once().await;
        assert!(matches!(
            outcomes[0].1,
            PushOutcome::Skipped(SkipReason::FileVanished)
        ));
        // Ack'd → drained out of the journal.
        let j = journal.lock().await;
        assert_eq!(j.len(), 0);
        m.assert_async().await;
    }

    #[tokio::test]
    async fn failed_event_nacks_journal() {
        let mut srv = Server::new_async().await;
        let _m = srv
            .mock("POST", "/api/sync/push")
            .with_status(500)
            .expect_at_least(1)
            .create_async()
            .await;
        let (_d, journal) = make_journal_with(vec![evt("notes/a.md", b"hello")]);
        let client = make_client(&srv.url(), journal.clone()).await;
        let outcomes = client.drain_once().await;
        assert!(matches!(
            outcomes[0].1,
            PushOutcome::Failed(FailureReason::NetworkExhausted { .. })
        ));
        let j = journal.lock().await;
        assert_eq!(j.len(), 1, "failed event must stay in journal for retry");
    }

    #[tokio::test]
    async fn http_409_yields_conflict_unrecoverable() {
        let mut srv = Server::new_async().await;
        let _m = srv
            .mock("POST", "/api/sync/push")
            .with_status(409)
            .with_body(r#"{"expected_hash":"srv_hash"}"#)
            .create_async()
            .await;
        let (_d, journal) = make_journal_with(vec![evt("notes/a.md", b"hello")]);
        let client = make_client(&srv.url(), journal.clone()).await;
        let outcomes = client.drain_once().await;
        match &outcomes[0].1 {
            PushOutcome::Failed(FailureReason::ConflictUnrecoverable { expected_hash }) => {
                assert_eq!(expected_hash.as_deref(), Some("srv_hash"));
            }
            other => panic!("expected ConflictUnrecoverable, got {other:?}"),
        }
        // Conflict is terminal — event was ack'd (resolver handles refetch+replay).
        let j = journal.lock().await;
        assert_eq!(j.len(), 0);
    }

    /// D4 (v0.4.28, M5a): HTTP 422 (non-UTF-8 / NUL content the server
    /// permanently rejects) must behave exactly like 400 - permanent reject,
    /// skip + ack, exactly ONE attempt, never the transient retry arm
    /// (which wedged the journal: 5 retries -> NetworkExhausted -> not acked
    /// -> retried every drain tick forever).
    #[tokio::test]
    async fn test_422_permanent_skip_and_ack() {
        let mut srv = Server::new_async().await;
        let m = srv
            .mock("POST", "/api/sync/push")
            .expect(1)
            .with_status(422)
            .with_body(r#"{"detail":"content is not valid UTF-8"}"#)
            .create_async()
            .await;
        let (_d, journal) = make_journal_with(vec![evt("notes/bad.md", b"x")]);
        let client = make_client(&srv.url(), journal.clone()).await;

        let outcomes = client.drain_once().await;
        assert_eq!(outcomes.len(), 1);
        assert_eq!(
            outcomes[0].1,
            PushOutcome::Skipped(SkipReason::ServerRejected { status: 422 }),
            "422 must be a permanent ServerRejected skip"
        );
        // Acked: the journal entry is consumed, never retried.
        assert_eq!(
            journal.lock().await.len(),
            0,
            "422 outcome must be acked (journal drained)"
        );
        m.assert_async().await; // exactly one HTTP attempt
    }

    /// I29 (S513, TKT-2dc9a17e): a file_watcher push enqueues base_hash=None and
    /// delegates base-sourcing to the push_client (see file_watcher::to_push_event).
    /// The push MUST backfill the CAS base from the shadow store (our last-known
    /// server hash) — NOT send "". Sending "" made the server reject every
    /// divergent already-synced-note push as a CAS conflict (base "" + row exists
    /// → 409) → D4 stash → conflict-copy avalanche. With the shadow base on the
    /// wire the server sees base == current → accepts (operator-ratified local
    /// wins). This asserts the BASE ON THE WIRE is the shadow hash.
    #[tokio::test]
    async fn modify_with_none_base_backfills_cas_base_from_shadow() {
        let server_hash = "a".repeat(64); // the shadow's last-known server hash
        let mut srv = Server::new_async().await;
        // The mock ONLY matches if the request carries base_hash == shadow hash;
        // if the fix regresses to "" the request won't match → push not Sent.
        let m = srv
            .mock("POST", "/api/sync/push")
            .match_body(mockito::Matcher::PartialJsonString(format!(
                r#"{{"base_hash":"{server_hash}"}}"#
            )))
            .with_status(200)
            .with_body(
                r#"{"status":"accepted","seq":1,"content_hash":"h","server_hash":null,"server_seq":null,"merged_content":null,"message":null}"#,
            )
            .expect(1)
            .create_async()
            .await;

        // file_watcher-style event: base_hash=None, Modify, content differs from
        // the shadow (a genuine local edit on an already-synced note).
        let fw_evt = PushEvent {
            schema_version: CURRENT_SCHEMA,
            id: crate::push_journal::new_event_id(),
            path: "notes/edited.md".to_string(),
            action: PushAction::Modify,
            base_hash: None,
            content_sha: sha256_hex(b"new local body"),
            content_bytes: Some(b"new local body".to_vec()),
            queued_at: Utc::now(),
            device_id: "dev-test".into(),
        };
        let (_d, journal) = make_journal_with(vec![fw_evt]);

        let sdir = TempDir::new().unwrap();
        let shadow = crate::sync_shadow::ShadowStore::load(sdir.path().join("shadow.json"));
        shadow.record("notes/edited.md", &server_hash);
        let client = make_client(&srv.url(), journal.clone())
            .await
            .with_shadow_store(shadow);

        let outcomes = client.drain_once().await;
        assert!(
            matches!(outcomes[0].1, PushOutcome::Sent { .. }),
            "shadow-backfilled base must let the push be accepted, got {:?}",
            outcomes[0].1
        );
        m.assert_async().await; // proves base_hash on the wire == shadow hash
    }

    /// I29 corollary: a genuine NEW file (Modify/None base, NO shadow entry) must
    /// still send base "" so the server CREATEs the row — the backfill must never
    /// invent a base out of thin air.
    #[tokio::test]
    async fn modify_with_none_base_and_no_shadow_entry_sends_empty_base() {
        let mut srv = Server::new_async().await;
        let m = srv
            .mock("POST", "/api/sync/push")
            .match_body(mockito::Matcher::PartialJsonString(
                r#"{"base_hash":""}"#.to_string(),
            ))
            .with_status(200)
            .with_body(
                r#"{"status":"accepted","seq":1,"content_hash":"h","server_hash":null,"server_seq":null,"merged_content":null,"message":null}"#,
            )
            .expect(1)
            .create_async()
            .await;

        let fw_evt = PushEvent {
            schema_version: CURRENT_SCHEMA,
            id: crate::push_journal::new_event_id(),
            path: "notes/brand-new.md".to_string(),
            action: PushAction::Modify,
            base_hash: None,
            content_sha: sha256_hex(b"brand new"),
            content_bytes: Some(b"brand new".to_vec()),
            queued_at: Utc::now(),
            device_id: "dev-test".into(),
        };
        let (_d, journal) = make_journal_with(vec![fw_evt]);

        // Shadow store present but EMPTY (no entry for this path).
        let sdir = TempDir::new().unwrap();
        let shadow = crate::sync_shadow::ShadowStore::load(sdir.path().join("shadow.json"));
        let client = make_client(&srv.url(), journal.clone())
            .await
            .with_shadow_store(shadow);

        let outcomes = client.drain_once().await;
        assert!(matches!(outcomes[0].1, PushOutcome::Sent { .. }));
        m.assert_async().await;
    }

    /// S520 (TKT-2c2e9d0f): a DELETE of an already-synced note must backfill the
    /// CAS base from the shadow (the last-known server hash), NOT send "". The
    /// prior `Delete => None` sent base_hash="" which the server's delete-CAS
    /// refuses whenever a reconcile-state row exists (base "" + row → 409), so
    /// every delete of a synced note 409'd as ConflictUnrecoverable and was
    /// dropped → deletes never propagated. This asserts the BASE ON THE WIRE for a
    /// delete is the shadow hash, letting the server see base == current → delete.
    #[tokio::test]
    async fn delete_with_none_base_backfills_cas_base_from_shadow() {
        let server_hash = "b".repeat(64); // shadow's last-known server hash
        let mut srv = Server::new_async().await;
        // Mock matches ONLY if the delete carries base_hash == shadow hash; a
        // regression to "" would not match → push not Sent.
        let m = srv
            .mock("POST", "/api/sync/push")
            .match_body(mockito::Matcher::PartialJsonString(format!(
                r#"{{"action":"delete","base_hash":"{server_hash}"}}"#
            )))
            .with_status(200)
            .with_body(
                r#"{"status":"accepted","seq":1,"content_hash":"","server_hash":null,"server_seq":null,"merged_content":null,"message":null}"#,
            )
            .expect(1)
            .create_async()
            .await;

        // file_watcher-style delete event: base_hash=None, no content body.
        let fw_evt = PushEvent {
            schema_version: CURRENT_SCHEMA,
            id: crate::push_journal::new_event_id(),
            path: "notes/deleted.md".to_string(),
            action: PushAction::Delete,
            base_hash: None,
            content_sha: String::new(),
            content_bytes: None,
            queued_at: Utc::now(),
            device_id: "dev-test".into(),
        };
        let (_d, journal) = make_journal_with(vec![fw_evt]);

        let sdir = TempDir::new().unwrap();
        let shadow = crate::sync_shadow::ShadowStore::load(sdir.path().join("shadow.json"));
        shadow.record("notes/deleted.md", &server_hash);
        let client = make_client(&srv.url(), journal.clone())
            .await
            .with_shadow_store(shadow);

        let outcomes = client.drain_once().await;
        assert!(
            matches!(outcomes[0].1, PushOutcome::Sent { .. }),
            "shadow-backfilled delete base must let the delete be accepted, got {:?}",
            outcomes[0].1
        );
        m.assert_async().await; // proves base_hash on the wire == shadow hash
    }

    /// D4 (S511, TKT-2dc9a17e): a local push that loses the server CAS race
    /// (409) must PRESERVE the local bytes it tried to push as a
    /// `.conflict-from-*` sibling BEFORE the journal entry is acked, so a losing
    /// local edit is never silently dropped. Pre-S511 the 409 just returned
    /// ConflictUnrecoverable and dropped the bytes.
    #[tokio::test]
    async fn cas_409_stashes_local_bytes_before_ack() {
        let vault = TempDir::new().unwrap();
        std::fs::create_dir_all(vault.path().join("notes")).unwrap();

        let mut srv = Server::new_async().await;
        let _m = srv
            .mock("POST", "/api/sync/push")
            .with_status(409)
            .with_body(r#"{"expected_hash":"srv_hash"}"#)
            .create_async()
            .await;

        let body = b"my local edit that lost the CAS race";
        let (_d, journal) = make_journal_with(vec![evt("notes/raced.md", body)]);
        let client =
            make_client_with_root(&srv.url(), journal.clone(), vault.path().to_path_buf()).await;

        let outcomes = client.drain_once().await;
        assert!(matches!(
            outcomes[0].1,
            PushOutcome::Failed(FailureReason::ConflictUnrecoverable { .. })
        ));

        // The local bytes were preserved as a conflict-from sibling under the
        // vault root, NOT silently dropped.
        let dir = vault.path().join("notes");
        let stash: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.contains(".conflict-from-"))
            .collect();
        assert_eq!(
            stash.len(),
            1,
            "CAS-409 must stash the losing local bytes, got {stash:?}"
        );
        let stashed = std::fs::read(dir.join(&stash[0])).unwrap();
        assert_eq!(
            stashed, body,
            "stash must hold the exact local bytes pushed"
        );
    }

    #[tokio::test]
    async fn unauthorized_does_not_retry() {
        let mut srv = Server::new_async().await;
        // Expect EXACTLY one hit — no retry on 401.
        let m = srv
            .mock("POST", "/api/sync/push")
            .with_status(401)
            .expect(1)
            .create_async()
            .await;
        let (_d, journal) = make_journal_with(vec![evt("notes/a.md", b"hello")]);
        let client = make_client(&srv.url(), journal.clone()).await;
        let outcomes = client.drain_once().await;
        assert!(matches!(
            outcomes[0].1,
            PushOutcome::Failed(FailureReason::Unauthorized)
        ));
        m.assert_async().await;
    }

    #[test]
    fn retry_backoff_capped_at_max_backoff_ms() {
        // initial=500 max=60_000 — at attempt=20, 500 * 2^20 = 524_288_000
        // should clamp to 60_000.
        assert_eq!(compute_backoff(0, 500, 60_000), 500);
        assert_eq!(compute_backoff(1, 500, 60_000), 1000);
        assert_eq!(compute_backoff(2, 500, 60_000), 2000);
        assert_eq!(compute_backoff(20, 500, 60_000), 60_000);
        assert_eq!(compute_backoff(100, 500, 60_000), 60_000);
    }

    #[test]
    fn extension_check_canvas_allowed() {
        let cfg = config_for_test();
        assert!(check_extension("foo.canvas", &cfg.allowed_extensions).is_none());
        assert!(check_extension("foo.MD", &cfg.allowed_extensions).is_none());
        assert!(check_extension("foo.png", &cfg.allowed_extensions).is_some());
    }

    // ---- v0.3 tray-wire-up sanity ----

    fn make_shared_tray() -> crate::tray_state::SharedTrayState {
        std::sync::Arc::new(std::sync::RwLock::new(crate::tray_state::TrayState::new(
            "sub".into(),
            "https://x".into(),
            std::path::PathBuf::from("/v"),
        )))
    }

    #[tokio::test]
    async fn push_client_increments_tray_on_sent() {
        let mut srv = Server::new_async().await;
        let _m = srv
            .mock("POST", "/api/sync/push")
            .with_status(200)
            .with_body(
                r#"{"status":"accepted","seq":1,"content_hash":"h","server_hash":null,"server_seq":null,"merged_content":null,"message":null}"#,
            )
            .create_async()
            .await;
        let (_d, journal) = make_journal_with(vec![evt("notes/a.md", b"hello")]);
        let tray = make_shared_tray();
        let client = make_client(&srv.url(), journal.clone())
            .await
            .with_tray_state(tray.clone());
        let _ = client.drain_once().await;
        let s = tray.read().unwrap();
        assert_eq!(s.uploads_sent, 1);
        assert_eq!(s.uploads_failed, 0);
        assert_eq!(s.uploads_pending, 0);
        assert!(s.uploads_last_at.is_some());
    }

    #[tokio::test]
    async fn push_client_increments_tray_on_failed() {
        let mut srv = Server::new_async().await;
        let _m = srv
            .mock("POST", "/api/sync/push")
            .with_status(500)
            .expect_at_least(1)
            .create_async()
            .await;
        let (_d, journal) = make_journal_with(vec![evt("notes/a.md", b"hello")]);
        let tray = make_shared_tray();
        let client = make_client(&srv.url(), journal.clone())
            .await
            .with_tray_state(tray.clone());
        let _ = client.drain_once().await;
        let s = tray.read().unwrap();
        assert_eq!(s.uploads_sent, 0);
        assert_eq!(s.uploads_failed, 1);
        // Event nack'd → still in journal.
        assert_eq!(s.uploads_pending, 1);
    }

    // ---- B4: per-sync_root push_client tests --------------------------------

    /// B4: the push_client resolves lazy content_bytes reads relative to
    /// the `vault_root` it was constructed with (the sync_root.path), NOT
    /// any global vaults_root or working directory.
    ///
    /// Two separate vault roots (root_a, root_b) each contain a file at the
    /// SAME relative path ("notes/shared.md"). A PushClient constructed for
    /// root_a must read from root_a; one for root_b reads from root_b.
    #[tokio::test]
    async fn lazy_read_uses_passed_vault_root_not_global() {
        let root_a = TempDir::new().unwrap();
        let root_b = TempDir::new().unwrap();

        let rel = "notes/shared.md";
        let body_a = b"content-from-root-a";
        let body_b = b"content-from-root-b";

        std::fs::create_dir_all(root_a.path().join("notes")).unwrap();
        std::fs::create_dir_all(root_b.path().join("notes")).unwrap();
        std::fs::write(root_a.path().join(rel), body_a).unwrap();
        std::fs::write(root_b.path().join(rel), body_b).unwrap();

        // Build two servers, each asserting on the content they receive.
        let expected_b64_a = B64.encode(body_a);
        let expected_b64_b = B64.encode(body_b);

        let mut srv_a = Server::new_async().await;
        let m_a = srv_a
            .mock("POST", "/api/sync/push")
            .match_body(mockito::Matcher::PartialJsonString(format!(
                r#"{{"content":"{expected_b64_a}"}}"#
            )))
            .with_status(200)
            .with_body(
                r#"{"status":"accepted","seq":1,"content_hash":"h","server_hash":null,"server_seq":null,"merged_content":null,"message":null}"#,
            )
            .expect(1)
            .create_async()
            .await;

        let mut srv_b = Server::new_async().await;
        let m_b = srv_b
            .mock("POST", "/api/sync/push")
            .match_body(mockito::Matcher::PartialJsonString(format!(
                r#"{{"content":"{expected_b64_b}"}}"#
            )))
            .with_status(200)
            .with_body(
                r#"{"status":"accepted","seq":2,"content_hash":"h","server_hash":null,"server_seq":null,"merged_content":null,"message":null}"#,
            )
            .expect(1)
            .create_async()
            .await;

        let lazy_evt = |device_suffix: &str| PushEvent {
            schema_version: CURRENT_SCHEMA,
            id: crate::push_journal::new_event_id(),
            path: rel.to_string(),
            action: PushAction::Modify,
            base_hash: Some("0".repeat(64)),
            content_sha: "a".repeat(64),
            content_bytes: None, // lazy — will be read from vault_root at drain time
            queued_at: chrono::Utc::now(),
            device_id: format!("dev-{device_suffix}"),
        };

        let (_da, journal_a) = make_journal_with(vec![lazy_evt("a")]);
        let (_db, journal_b) = make_journal_with(vec![lazy_evt("b")]);

        let client_a =
            make_client_with_root(&srv_a.url(), journal_a.clone(), root_a.path().to_path_buf())
                .await;
        let client_b =
            make_client_with_root(&srv_b.url(), journal_b.clone(), root_b.path().to_path_buf())
                .await;

        // Each client drains its journal. Mockito asserts the server received
        // the correct body for each root.
        let outcomes_a = client_a.drain_once().await;
        assert_eq!(outcomes_a.len(), 1);
        assert!(
            matches!(outcomes_a[0].1, PushOutcome::Sent { .. }),
            "client_a should send; got {:?}",
            outcomes_a[0].1
        );

        let outcomes_b = client_b.drain_once().await;
        assert_eq!(outcomes_b.len(), 1);
        assert!(
            matches!(outcomes_b[0].1, PushOutcome::Sent { .. }),
            "client_b should send; got {:?}",
            outcomes_b[0].1
        );

        m_a.assert_async().await;
        m_b.assert_async().await;
    }

    /// B4: pushed paths are relative to the sync_root the PushClient was
    /// constructed for. The `path` field on the PushEvent is already the
    /// root-relative path (wired by file_watcher per B2). Verify the
    /// pre-journal filter does NOT try to absolutize or re-root the path.
    #[test]
    fn pre_journal_filter_treats_path_as_root_relative() {
        let cfg = config_for_test();
        // Construct with an arbitrary root — filter is path-string-only,
        // does no filesystem I/O (just rasp_fence + extension + idempotency).
        let client = PushClient {
            api: Arc::new(ApiClient::new("http://127.0.0.1:1", "x").unwrap()),
            journal: Arc::new(Mutex::new(
                PushJournal::open(&TempDir::new().unwrap().path().join("j.jsonl")).unwrap(),
            )),
            device_id: "d".into(),
            config: cfg,
            vault_root: PathBuf::from("/sync/root/a"), // per-root path
            tray_state: None,
            shadow_store: None,
            sync_health: None,
            gate_cooldown_until: Arc::new(std::sync::Mutex::new(None)),
        };
        // Root-relative path "01_Inbox/note.md" — not prefixed with the
        // sync root string. The filter should pass (not substrate, allowed ext).
        let result = client.pre_journal_filter("01_Inbox/note.md", b"some content", None);
        assert!(
            result.is_none(),
            "root-relative path must not be filtered; got {result:?}"
        );
    }

    // --- shadow_hash_for_ack (fix/reconcile-server-wins-shadow) ---

    #[test]
    fn shadow_hash_for_ack_table() {
        // Accepted → the pushed local hash is the new canonical.
        assert_eq!(
            shadow_hash_for_ack(&PushStatus::Accepted, "local", Some("srv"), Some("ch")),
            Some("local".to_string())
        );
        // Merged → prefer server_hash.
        assert_eq!(
            shadow_hash_for_ack(&PushStatus::Merged, "local", Some("srv"), Some("ch")),
            Some("srv".to_string())
        );
        // Merged, no server_hash → fall back to content_hash.
        assert_eq!(
            shadow_hash_for_ack(&PushStatus::Merged, "local", None, Some("ch")),
            Some("ch".to_string())
        );
        // Merged, neither → fall back to local (record SOMETHING).
        assert_eq!(
            shadow_hash_for_ack(&PushStatus::Merged, "local", None, None),
            Some("local".to_string())
        );
        // ConflictMarkers / Error → record nothing.
        assert_eq!(
            shadow_hash_for_ack(&PushStatus::ConflictMarkers, "local", Some("srv"), None),
            None
        );
        assert_eq!(
            shadow_hash_for_ack(&PushStatus::Error, "local", Some("srv"), None),
            None
        );
    }

    /// S5 client half (v0.4.28): a gated daemon must fail LOUDLY and BACK OFF,
    /// not retry-loop. Contract: exactly ONE HTTP attempt; outcome carries the
    /// server detail; the journal entry is NOT acked (the local edit survives
    /// for after the upgrade); the next drain tick inside the cooldown window
    /// makes ZERO HTTP calls.
    #[tokio::test]
    async fn test_426_upgrade_required_fails_loudly_and_backs_off() {
        let mut srv = Server::new_async().await;
        let m = srv
            .mock("POST", "/api/sync/push")
            .expect(1)
            .with_status(426)
            .with_body(
                r#"{"detail":{"error":"daemon_version_below_minimum","minimum":"0.4.28","reported":"0.4.27"}}"#,
            )
            .create_async()
            .await;
        let (_d, journal) = make_journal_with(vec![evt("notes/a.md", b"body\n")]);
        let client = make_client(&srv.url(), journal.clone()).await;

        let outcomes = client.drain_once().await;
        assert_eq!(outcomes.len(), 1);
        match &outcomes[0].1 {
            PushOutcome::Failed(FailureReason::UpgradeRequired { detail }) => {
                assert!(
                    detail.contains("0.4.28"),
                    "detail must name the required version: {detail}"
                );
            }
            other => panic!("expected Failed(UpgradeRequired), got {other:?}"),
        }
        // NOT acked: the local edit stays journaled for after the upgrade.
        assert_eq!(
            journal.lock().await.len(),
            1,
            "426 must NOT ack - the edit is preserved, not dropped"
        );
        // Cooldown: the immediate next drain tick makes ZERO HTTP calls.
        let outcomes2 = client.drain_once().await;
        assert!(
            outcomes2.is_empty(),
            "gate cooldown must skip the drain tick entirely (no retry-loop)"
        );
        m.assert_async().await; // exactly ONE attempt total across both drains
    }

    /// S5 client half (v0.4.28), review follow-up: the existing 426 test only
    /// proves the cooldown ENGAGES; nothing proved it CLEARS. Uses the
    /// `arm_gate_cooldown(Duration)` seam to arm a short window instead of
    /// the real 900s one, so the test can prove EXPIRY with a small real
    /// sleep rather than mocking `Instant` or waiting 900s.
    #[tokio::test]
    async fn test_426_gate_cooldown_expires_after_window() {
        let mut srv = Server::new_async().await;
        // Two attempts expected: one is suppressed by the cooldown check
        // never reaching the network, so only the pre-arm and post-expiry
        // ticks actually hit the mock.
        let m = srv
            .mock("POST", "/api/sync/push")
            .expect(1)
            .with_status(200)
            .with_body(r#"{"status":"accepted","content_hash":"deadbeef"}"#)
            .create_async()
            .await;
        let (_d, journal) = make_journal_with(vec![evt("notes/a.md", b"body\n")]);
        let client = make_client(&srv.url(), journal.clone()).await;

        // Arm the cooldown directly via the injectable-duration seam — no
        // need to force a real 426 response to get into the armed state.
        client.arm_gate_cooldown(Duration::from_millis(50));

        // Phase 1: tick during the window — suppressed, empty outcomes, no HTTP.
        let outcomes = client.drain_once().await;
        assert!(
            outcomes.is_empty(),
            "tick inside the cooldown window must be fully suppressed"
        );

        // Real sleep past the short window — cheap since it's 50ms, not 900s.
        tokio::time::sleep(Duration::from_millis(60)).await;

        // Phase 2: tick after expiry — journal drains normally.
        let outcomes2 = client.drain_once().await;
        assert_eq!(
            outcomes2.len(),
            1,
            "tick after cooldown expiry must drain the journal normally"
        );
        assert!(
            matches!(outcomes2[0].1, PushOutcome::Sent { .. }),
            "expected Sent after cooldown expiry; got {:?}",
            outcomes2[0].1
        );
        m.assert_async().await; // exactly ONE HTTP attempt (the post-expiry tick)
    }
}
