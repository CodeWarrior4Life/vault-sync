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
    NetworkExhausted {
        last_error: String,
    },
    ConflictUnrecoverable {
        expected_hash: Option<String>,
    },
    Unauthorized,
    Forbidden,
    /// S5 (v0.4.28): the server's min-daemon-version gate answered HTTP 426.
    /// Permanent until the daemon binary is upgraded. NOT acked (the edit
    /// stays journaled); the push client enters a drain cooldown so it never
    /// retry-loops against a gate that cannot pass.
    UpgradeRequired {
        detail: String,
    },
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
    /// R7b (THESEUS AR-002, TKT-166e1c07): per-note observed-base_seq store.
    /// Read to populate `PushRequest.base_seq` on EVERY push/delete (R1); the
    /// server-returned `server_seq` is recorded here ONLY after the pushed bytes
    /// are confirmed on the local FS (R3). `None` = no lineage tracking (unit
    /// tests / degraded wiring); the push then declares `base_seq: null` which
    /// the server (flag on) fails closed, exactly the unknown-lineage path (R4).
    base_seq_store: Option<Arc<crate::base_seq_store::BaseSeqStore>>,
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
    /// D2 (v0.4.28): write machinery for ack-materialize-back. The rewrite
    /// MUST ride `Materializer::write_aligned_bytes` (per-path lock, echo
    /// guard, atomic tmp+rename) - a bespoke write path would miss echo
    /// suppression and re-trigger the watcher (the S492 feedback loop).
    /// `None` = no rewrite (unit tests / degraded wiring); the shadow then
    /// records the server hash so the next reconcile pass pulls (D1 aligns).
    materializer: Option<crate::materializer::Materializer>,
    /// D2/B2'd (v0.4.28): the file_watcher's enqueue-dedup map, SHARED (see
    /// lib.rs spawn_push_pipeline). After an ack-materialize rewrite we set
    /// `map[path] = canonical sha` so a later touch event past the echo TTL
    /// is suppressed by the watcher's layer-2 dedup instead of emitting an
    /// idempotent echo push.
    enqueued_hashes: Option<Arc<std::sync::Mutex<std::collections::HashMap<String, String>>>>,
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
            base_seq_store: None,
            sync_health: None,
            gate_cooldown_until: Arc::new(std::sync::Mutex::new(None)),
            materializer: None,
            enqueued_hashes: None,
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

    /// R7b (TKT-166e1c07): attach the per-note observed-base_seq store. After
    /// this, every push/delete declares the note's last-observed `change_seq`
    /// as proof-of-observation (R1), and an accepted push records the server's
    /// new `server_seq` once the bytes are confirmed local (R3).
    /// Backwards-compatible - without it, pushes declare `base_seq: null`.
    pub fn with_base_seq_store(mut self, store: Arc<crate::base_seq_store::BaseSeqStore>) -> Self {
        self.base_seq_store = Some(store);
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

    /// D2 (v0.4.28): attach the materializer whose write machinery
    /// ack-materialize-back rides. Backwards-compatible - without it, no
    /// local rewrite happens (the shadow still records the server hash, so
    /// convergence falls to the next reconcile pull).
    pub fn with_materializer(mut self, m: crate::materializer::Materializer) -> Self {
        self.materializer = Some(m);
        self
    }

    /// D2/B2'd (v0.4.28): attach the enqueue-dedup map SHARED with the
    /// file_watcher (see `FileWatcher::with_enqueued_hashes`).
    pub fn with_enqueued_hashes(
        mut self,
        map: Arc<std::sync::Mutex<std::collections::HashMap<String, String>>>,
    ) -> Self {
        self.enqueued_hashes = Some(map);
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
        // R7 (TKT-166e1c07): empty-vault_folders guard, defense-in-depth. The
        // pipeline spawn already parks on this state (lib.rs), so in production
        // drain_once is never reached while suspect; this is the belt-and-braces
        // chokepoint (and the unit-test seam). PARK: refuse to drain rather than
        // mass-mis-key and re-push the vault (2026-07-18 trinity incident).
        if let Some(sh) = &self.shadow_store {
            if sh.vault_scope_suspect() {
                tracing::debug!(
                    "push_client: PARKED (R7) - vault scope suspect (empty vault_folders + prefixed keys); refusing drain"
                );
                return Vec::new();
            }
        }
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

        // D2/D3 (v0.4.28): the sha of the bytes we ACTUALLY drained and will
        // POST. NOT `evt.content_sha` - that is ENQUEUE-time, and pushes are
        // lazy (file_watcher sets content_bytes: None; we read at drain time
        // above), so the two can differ if the file changed in between.
        // Deletes carry no body; keep the enqueue-time value ("") so delete
        // shadow semantics are unchanged.
        let drained_sha: String = if matches!(evt.action, PushAction::Delete) {
            evt.content_sha.clone()
        } else {
            sha256_hex(&content_bytes)
        };

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

        // R1 (THESEUS AR-002, TKT-166e1c07): declare our last-observed base_seq
        // (proof-of-observation) on EVERY push and delete. `None` when we have
        // no recorded lineage for this path (a create, or a note we never
        // byte-verified) - the fail-closed unknown-lineage signal (R4). We NEVER
        // fabricate or default a seq; the server (flag on) fails the causal gate
        // closed on `None`-against-a-tracked-version and we refetch/merge (R2).
        // Push and delete share this one request path, so both carry it.
        let base_seq: Option<i64> = self.base_seq_store.as_ref().and_then(|s| s.get(&evt.path));

        let req = PushRequest {
            device_id: &self.device_id,
            path: &evt.path,
            content_b64: &content_b64,
            // The server-side CAS base (checked against
            // vault_reconcile_state.fs_hash). Prefer the event's explicit base
            // (reconcile pushes), else the shadow-backfilled base (file_watcher
            // pushes); "" only when we truly have no known base → server CREATEs.
            base_hash: backfilled_base.as_deref().unwrap_or(""),
            base_seq,
            action,
        };

        let mut last_err: Option<String> = None;
        for attempt in 0..self.config.max_retry_attempts {
            match self.api.push(&req).await {
                Ok(resp) => {
                    // D2 trigger (v0.4.28, B2'): the server ACCEPTED but its
                    // canonical hash differs from what we sent - it
                    // canonicalized (or region-defended) our bytes. NOT keyed
                    // off Merged (M2: PushOutcome::Merged has no materialize
                    // consumer; ack-materialize is a NEW accepted-keyed
                    // behavior). Compat note (corrected, final-review fix
                    // wave): a server that never rewrites bytes on accept
                    // echoes our hash (or omits server_hash), so this stays
                    // false against it - but that is NOT "any real server
                    // today". Today's server legitimately returns
                    // effective_hash != pushed on REGION-DEFENDED accepts
                    // (splices bytes into a protected region we don't have
                    // locally), so needs_align DOES fire there in normal
                    // operation: local-canonicalize-and-verify fails (we
                    // never had the spliced bytes to canonicalize), falls to
                    // the /note fetch fallback, then a GUARDED rewrite
                    // (write_aligned_bytes, including the anti-strip guard
                    // above). That converges the file to the exact state the
                    // pre-D2 daemon only reached indirectly, on its NEXT pull
                    // pass. Deletes have no local file to align.
                    // R3 (TKT-166e1c07): the server's new authoritative version
                    // token for this accepted push. Recorded as our observed
                    // base_seq ONLY on the branches where the local FS is
                    // confirmed to hold the exact accepted bytes (below); never
                    // a local assumption (it comes straight from the response).
                    let server_seq = resp.server_seq;
                    let needs_align = matches!(resp.status, PushStatus::Accepted)
                        && !matches!(evt.action, PushAction::Delete)
                        && resp
                            .server_hash
                            .as_deref()
                            .is_some_and(|h| h != drained_sha);
                    if needs_align {
                        let server_hash = resp
                            .server_hash
                            .clone()
                            .expect("needs_align checked is_some");
                        self.ack_materialize_back(
                            &evt.path,
                            &content_bytes,
                            &drained_sha,
                            &server_hash,
                            server_seq,
                        )
                        .await;
                    } else if let Some(sh) = &self.shadow_store {
                        // fix/reconcile-server-wins-shadow + D3 (v0.4.28): a
                        // push the server accepts means the canonical server
                        // hash is now known - record it so the reconcile
                        // backstop won't later mistake this just-pushed file
                        // for a stale-pull candidate. D2/D3 reality (this is
                        // the else-of-needs_align branch): Accepted with
                        // server_hash == drained_sha means our pushed bytes
                        // WERE already canonical, so the drained hash IS the
                        // canonical to record; Merged records the server's
                        // returned canonical instead. When needs_align is
                        // true the D2 branch above owns recording (via
                        // ack_materialize_back / write_aligned_bytes), not
                        // this fallback.
                        if let Some(h) = shadow_hash_for_ack(
                            &resp.status,
                            &drained_sha,
                            resp.server_hash.as_deref(),
                            resp.content_hash.as_deref(),
                        ) {
                            sh.record(&evt.path, &h);
                        }
                    }
                    // R3 (TKT-166e1c07): record the observed base_seq for the
                    // non-align accept path. The `needs_align` branch records
                    // inside ack_materialize_back AFTER its own byte-verify, so
                    // it is excluded here to avoid recording before the aligned
                    // bytes land.
                    if !needs_align {
                        if let Some(bs) = &self.base_seq_store {
                            match (&resp.status, evt.action) {
                                // Accepted delete: the note is tombstoned; drop
                                // its lineage so a later re-create starts from
                                // unknown lineage (fail-closed), not a stale seq.
                                (PushStatus::Accepted, PushAction::Delete) => bs.remove(&evt.path),
                                // Accepted create/modify on the non-align branch:
                                // the local FS holds EXACTLY the accepted bytes
                                // (== server canonical), so the version is
                                // confirmed materialized (R3). Record the server's
                                // returned token as observed - never a local
                                // assumption. Merged is deliberately excluded: the
                                // server-merged bytes are not on our FS yet, so
                                // recording would violate R3 (converges via pull).
                                (PushStatus::Accepted, _) => {
                                    if let Some(seq) = server_seq {
                                        bs.record(&evt.path, seq);
                                    }
                                }
                                _ => {}
                            }
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
                    // R2/R4 (causal gate, TKT-166e1c07): under
                    // NEXUS_FF_SYNC_CONVERGENCE a 409 means our declared base_seq
                    // was unknown (None -> fail-closed, R4) or stale/forged vs the
                    // current head; flag-off it is the base_hash CAS losing. The
                    // contract is the same either way: refetch the current server
                    // version and MERGE, never blind-retry or overwrite. The
                    // losing local bytes are already stashed above (never dropped);
                    // now converge the canonical file to the server head
                    // (byte-verified) and learn the fresh observed base_seq (R3).
                    self.refetch_and_merge_on_conflict(&evt.path).await;
                    // Still surface the conflict so Burn C accounting counts it
                    // (R6) and the journal entry is acked (the edit survives as the
                    // stash, not an infinite blind retry).
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

    /// D2 (v0.4.28, B2'): ack-materialize-back. The server accepted our push
    /// but stored DIFFERENT canonical bytes (server_hash != sha of the
    /// drained bytes). Rewrite the local file to the canonical form so the
    /// next pass is a byte-exact no-op instead of a one-pass drift.
    ///
    /// * Byte source (B2'b, option i - chosen): canonicalize the drained
    ///   bytes locally and VERIFY sha256(local_canonical) == server_hash.
    ///   Unverified local canonicalization is banned - it would itself be a
    ///   new unstable-hash family member. On mismatch (dual-implementation
    ///   drift, or a region-defense splice added bytes we don't have) fetch
    ///   GET /note and use the served bytes, whose sha is verified against
    ///   the payload's advertised hash (self-consistent by server S3).
    /// * Guard + ordering live in `Materializer::write_aligned_bytes` (B2'a
    ///   pre-rewrite re-read; B2'c rewrite-first-shadow-second).
    /// * On SkippedConcurrentEdit we STILL record server_hash: it is
    ///   factually the server's current canonical, it classifies the local
    ///   edit as PUSH on the next reconcile pass, and it is the CAS base the
    ///   already-pending push of that edit needs to be accepted (without it,
    ///   push2 backfills a stale base and 409-stashes the user's edit).
    /// * On rewrite FAILURE we record NOTHING: shadow stays stale -> next
    ///   pass classifies PULL - the fail-closed direction (recording would
    ///   arm the shadow==server phantom-push-per-pass trap).
    /// * Never returns an error: every failure degrades to "converge on the
    ///   next reconcile pass" and the push outcome stays what the server said.
    async fn ack_materialize_back(
        &self,
        path: &str,
        drained_bytes: &[u8],
        drained_sha: &str,
        server_hash: &str,
        server_seq: Option<i64>,
    ) {
        let Some(mat) = &self.materializer else {
            // No write machinery wired: record NOTHING. Recording server_hash
            // here would be the INVERSE of what it looks like at first glance:
            // the local file still holds the drained (pre-canonicalization)
            // bytes, which diverge from server_hash. If we set
            // shadow == server_hash while local != server_hash, the next
            // reconcile pass's decide() sees shadow_eq_server == true and
            // local_eq_server == false -> Decision::PreserveLocalEdit, i.e. it
            // classifies this as a genuine LOCAL EDIT and pushes the stale
            // drained bytes back UP - the opposite of convergence. Leaving the
            // shadow at its prior (stale, pre-push) value means the next pass
            // instead sees shadow_present with shadow != server (server moved)
            // and, per R3/R5, resolves to a PULL (or conflict-then-pull),
            // which is what actually converges the local file to the server's
            // canonical bytes. Production always wires the materializer (see
            // lib.rs); this arm is unreachable there but must not mislead.
            tracing::info!(
                path,
                "D2: server canonicalized push but no materializer wired - shadow left stale (fail-closed), pull converges next pass"
            );
            return;
        };

        // B2'b: local canonicalize + verify, else /note fetch fallback.
        let canonical: Vec<u8> = match crate::canonical_form::canonicalize_bytes(drained_bytes) {
            Ok(local_canon) if sha256_hex(local_canon.as_bytes()) == server_hash => {
                local_canon.into_bytes()
            }
            _ => {
                match self.api.fetch_note(path).await {
                    Ok(payload) => {
                        let bytes = payload.enriched_body.unwrap_or(payload.body).into_bytes();
                        let fetched_sha = sha256_hex(&bytes);
                        if fetched_sha != payload.sha256 {
                            tracing::warn!(
                                path,
                                advertised = %payload.sha256,
                                actual = %fetched_sha,
                                "D2: /note served bytes do not hash to the advertised sha - skipping ack-materialize (pull converges next pass)"
                            );
                            return; // shadow stays stale -> PULL (fail-closed)
                        }
                        if fetched_sha != server_hash {
                            // The server moved again since our push; its
                            // CURRENT canonical is authoritative.
                            tracing::debug!(
                                path,
                                "D2: server advanced past our push - aligning to its current canonical"
                            );
                        }
                        bytes
                    }
                    Err(e) => {
                        tracing::warn!(
                            path,
                            error = %e,
                            "D2: /note fetch fallback failed - skipping ack-materialize (shadow stays stale, pull converges next pass)"
                        );
                        return;
                    }
                }
            }
        };
        let canonical_sha = sha256_hex(&canonical);

        match mat.write_aligned_bytes(path, &canonical, &canonical_sha, drained_sha) {
            Ok(crate::materializer::AlignOutcome::Rewrote { .. }) => {
                // Rewrite happened FIRST; write_aligned_bytes recorded the
                // shadow SECOND (B2'c). B2'd: update the shared enqueue-dedup
                // so a later touch event past the echo TTL is suppressed by
                // the watcher's layer-2 dedup.
                if let Some(map) = &self.enqueued_hashes {
                    if let Ok(mut m) = map.lock() {
                        // Key-scheme note: the watcher's own enqueued_hashes
                        // keys are already NFC+forward-slash (normalize_event
                        // -> normalize_path -> canonical_sync_path). Re-applying
                        // canonical_sync_path here is therefore idempotent — it
                        // does not double-canonicalize, it just guarantees this
                        // writer (push_client, not the watcher) agrees on the
                        // same key form so the two producers never diverge.
                        m.insert(
                            crate::sync_shadow::canonical_sync_path(path),
                            canonical_sha.clone(),
                        );
                    }
                }
                // R3 (TKT-166e1c07): the server-canonical bytes (pre-verified to
                // hash to server_hash) were atomically materialized on the local
                // FS by write_aligned_bytes, so the exact version is now confirmed
                // present. Record the server's returned token as observed. This is
                // the ONLY align-path arm that records base_seq: SkippedConcurrentEdit
                // leaves a NEWER local edit on the FS (server bytes NOT materialized),
                // and the error/other arms did not write, so both stay unobserved
                // (fail-closed - the next push declares base_seq=null and refetches).
                if let (Some(bs), Some(seq)) = (&self.base_seq_store, server_seq) {
                    bs.record(path, seq);
                }
                tracing::info!(
                    path,
                    sha = %canonical_sha,
                    "D2: ack-materialize-back rewrote local to server canonical"
                );
            }
            Ok(crate::materializer::AlignOutcome::SkippedConcurrentEdit { current_sha }) => {
                // Ambiguity-resolution #1 (T6 deliberately does NOT record on
                // skip; the caller must): the shadow MUST advance to the
                // server's current canonical here even though the rewrite
                // was skipped. It is load-bearing for the already-pending
                // push of the user's mid-flight edit - that push backfills
                // its CAS base from this shadow entry (I29), so leaving it
                // stale would 409-stash the user's own edit against itself.
                if let Some(sh) = &self.shadow_store {
                    sh.record(path, server_hash);
                }
                tracing::info!(
                    path,
                    current = %current_sha,
                    "D2: concurrent edit detected (B2'a) - rewrite SKIPPED, shadow=server, pending push converges"
                );
            }
            Ok(other) => {
                tracing::info!(path, outcome = ?other, "D2: ack-materialize skipped");
            }
            Err(e) => {
                tracing::warn!(
                    path,
                    error = %e,
                    "D2: ack-materialize rewrite FAILED - shadow left stale (pull converges next pass, B2'c fail-closed)"
                );
            }
        }
    }

    /// R2/R4 (TKT-166e1c07): on a causal-gate / CAS 409, refetch the current
    /// server version and materialize it locally (byte-verified) so the daemon
    /// converges to the server head AND learns the fresh observed base_seq. The
    /// observed seq is recorded by the materializer ONLY after the exact bytes
    /// land + verify (R3), and only from the server response's `change_seq`
    /// (never a local assumption). Never blind-retries and never overwrites an
    /// unobserved local edit: the losing local bytes are stashed by the caller
    /// BEFORE this runs. Best-effort and fail-honest: with no materializer wired,
    /// or on a fetch/materialize error, convergence falls to the next reconcile
    /// pull and the conflict is still surfaced to the accounting layer (R6).
    async fn refetch_and_merge_on_conflict(&self, path: &str) {
        let Some(mat) = &self.materializer else {
            tracing::info!(
                path,
                "409 refetch/merge: no materializer wired - conflict surfaced, pull converges next pass"
            );
            return;
        };
        match self.api.fetch_note(path).await {
            Ok(payload) => match mat.write(&payload) {
                Ok(outcome) => tracing::info!(
                    path,
                    ?outcome,
                    "409 refetch/merge: server head materialized (observed base_seq recorded post-verify, R3)"
                ),
                Err(e) => tracing::warn!(
                    path,
                    error = %e,
                    "409 refetch/merge: materialize failed - pull converges next reconcile pass"
                ),
            },
            Err(ApiError::NotFound(_)) => {
                // The server has no such note (deleted since our push). Drop the
                // stale lineage so a later re-create starts from unknown lineage
                // (fail-closed, R4) rather than a stale observed seq.
                if let Some(bs) = &self.base_seq_store {
                    bs.remove(path);
                }
                tracing::info!(
                    path,
                    "409 refetch/merge: server has no such note (deleted) - lineage dropped (fail-closed)"
                );
            }
            Err(e) => tracing::warn!(
                path,
                error = %e,
                "409 refetch/merge: refetch failed - pull converges next reconcile pass"
            ),
        }
    }
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/// Decide which hash (if any) to record into the ShadowStore for a push the
/// server acked. Pure + table-tested.
///
/// * `Accepted` (D3, v0.4.28) → record the SERVER's canonical hash
///   (`server_hash`, falling back to `content_hash`, then `local_sha` for
///   pre-Piece-1 servers that omit both). The Piece 1 server may have
///   CANONICALIZED (or region-defended) what we sent, so the local sha is no
///   longer guaranteed to be the canonical; recording it unconditionally was
///   step 3 of the B1' eternal push/pull alternation (push_client.rs:656 in
///   v0.4.27). With this change an idempotent accept of raw CRLF bytes
///   records the canonical hash, so the next reconcile pass classifies the
///   file as PULL (→ the D1 alignment rewrite) instead of ping-ponging.
/// * `Merged` → the server merged a concurrent edit; record its returned
///   canonical (`server_hash` → `content_hash` → `local_sha`). Unchanged.
/// * `ConflictMarkers` / `Error` → no clean canonical was established →
///   record nothing. Unchanged.
fn shadow_hash_for_ack(
    status: &PushStatus,
    local_sha: &str,
    server_hash: Option<&str>,
    content_hash: Option<&str>,
) -> Option<String> {
    match status {
        PushStatus::Accepted | PushStatus::Merged => Some(
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
        let line_end = cursor + bytes[cursor..].iter().position(|&b| b == b'\n')?;
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

    // --- D2 (v0.4.28): ack-materialize-back fixtures ---

    use crate::echo_guard::EchoGuard;
    use crate::materializer::{Materializer, MaterializerConfig, MaterializerMode};
    use crate::sync_shadow::ShadowStore;
    use std::collections::HashMap;
    use std::sync::Mutex as StdMutex;

    struct AlignFx {
        _vault: TempDir,
        _ws: TempDir,
        _sdir: TempDir,
        vault_root: PathBuf,
        shadow: Arc<ShadowStore>,
        enq: Arc<StdMutex<HashMap<String, String>>>,
    }

    /// PushClient wired exactly like production (spawn_push_pipeline):
    /// materializer (Live, echo-guarded, shadow-backed) + shared shadow +
    /// shared enqueued_hashes map.
    async fn make_align_client(
        base_url: &str,
        journal: Arc<Mutex<PushJournal>>,
    ) -> (PushClient, AlignFx) {
        let vault = TempDir::new().unwrap();
        let ws = TempDir::new().unwrap();
        let sdir = TempDir::new().unwrap();
        let shadow = ShadowStore::load(sdir.path().join("shadow.json"));
        let enq: Arc<StdMutex<HashMap<String, String>>> = Arc::new(StdMutex::new(HashMap::new()));
        let mat = Materializer::new(
            vault.path().to_path_buf(),
            None,
            MaterializerMode::Live,
            ws.path().to_path_buf(),
            "sub-test".into(),
            MaterializerConfig {
                device_id: "dev-test".into(),
                ..Default::default()
            },
        )
        .with_shadow_store(shadow.clone())
        .with_echo_guard(Arc::new(EchoGuard::new()));
        let api = Arc::new(ApiClient::new(base_url, "vsk_test").unwrap());
        let client = PushClient::new(
            api,
            journal,
            "dev-test".into(),
            config_for_test(),
            vault.path().to_path_buf(),
        )
        .with_shadow_store(shadow.clone())
        .with_materializer(mat)
        .with_enqueued_hashes(enq.clone());
        let vault_root = vault.path().to_path_buf();
        (
            client,
            AlignFx {
                _vault: vault,
                _ws: ws,
                _sdir: sdir,
                vault_root,
                shadow,
                enq,
            },
        )
    }

    fn accepted_body(server_hash: &str) -> String {
        format!(
            r#"{{"status":"accepted","seq":1,"content_hash":"{server_hash}","server_hash":"{server_hash}","server_seq":1,"merged_content":null,"message":null}}"#
        )
    }

    /// Lazy event (content_bytes: None) like the real file_watcher emits - the
    /// push client reads the file from disk at drain time.
    fn lazy_evt(path: &str, enqueue_sha_of: &[u8]) -> PushEvent {
        PushEvent {
            schema_version: CURRENT_SCHEMA,
            id: crate::push_journal::new_event_id(),
            path: path.to_string(),
            action: PushAction::Modify,
            base_hash: Some("0".repeat(64)),
            content_sha: sha256_hex(enqueue_sha_of),
            content_bytes: None,
            queued_at: Utc::now(),
            device_id: "dev-test".into(),
        }
    }

    /// Steady state: server_hash == sha of the drained bytes -> NO rewrite, NO
    /// /note fetch, shadow records the server hash (D3 path).
    #[tokio::test]
    async fn test_no_ack_materialize_when_hashes_equal() {
        let body = b"already canonical\n";
        let sha = sha256_hex(body);
        let mut srv = Server::new_async().await;
        let m_push = srv
            .mock("POST", "/api/sync/push")
            .expect(1)
            .with_status(200)
            .with_body(accepted_body(&sha))
            .create_async()
            .await;
        let m_note = srv
            .mock("GET", "/api/sync/note")
            .match_query(mockito::Matcher::Any)
            .expect(0)
            .create_async()
            .await;

        let rel = "notes/a.md";
        let (_d, journal) = make_journal_with(vec![lazy_evt(rel, body)]);
        let (client, fx) = make_align_client(&srv.url(), journal).await;
        let abs = fx.vault_root.join(rel);
        std::fs::create_dir_all(abs.parent().unwrap()).unwrap();
        std::fs::write(&abs, body).unwrap();
        let mtime_before = std::fs::metadata(&abs).unwrap().modified().unwrap();

        let outcomes = client.drain_once().await;
        assert!(matches!(outcomes[0].1, PushOutcome::Sent { .. }));
        assert_eq!(std::fs::read(&abs).unwrap(), body, "file untouched");
        assert_eq!(
            std::fs::metadata(&abs).unwrap().modified().unwrap(),
            mtime_before,
            "steady state stays zero-write"
        );
        assert_eq!(fx.shadow.get(rel).as_deref(), Some(sha.as_str()));
        m_push.assert_async().await;
        m_note.assert_async().await;
    }

    /// Pre-Piece-1 compat (T7 review fold): a server that predates the D2
    /// canonicalization contract omits `server_hash` from its Accepted
    /// response entirely (JSON `null`), same as it always has. `needs_align`
    /// must short-circuit false on `resp.server_hash.as_deref().is_some_and`
    /// (None never satisfies is_some_and) - so process_event must NEVER call
    /// ack_materialize_back / attempt a rewrite / fetch /note against such a
    /// server. The shadow still advances via shadow_hash_for_ack's
    /// content_hash fallback, so reconcile does not misclassify the file as
    /// stale on the next pass.
    #[tokio::test]
    async fn test_pre_piece1_server_omits_server_hash_no_align() {
        let body = b"local bytes, non-canonicalizing server\n";
        let local_sha = sha256_hex(body);
        let mut srv = Server::new_async().await;
        // Pre-Piece-1 accepted body: server_hash is JSON null (field omitted
        // by an old server would deserialize the same way via Option<String>).
        let body_json = format!(
            r#"{{"status":"accepted","seq":1,"content_hash":"{local_sha}","server_hash":null,"server_seq":1,"merged_content":null,"message":null}}"#
        );
        let m_push = srv
            .mock("POST", "/api/sync/push")
            .expect(1)
            .with_status(200)
            .with_body(body_json)
            .create_async()
            .await;
        let m_note = srv
            .mock("GET", "/api/sync/note")
            .match_query(mockito::Matcher::Any)
            .expect(0)
            .create_async()
            .await;

        let rel = "notes/a.md";
        let (_d, journal) = make_journal_with(vec![lazy_evt(rel, body)]);
        let (client, fx) = make_align_client(&srv.url(), journal).await;
        let abs = fx.vault_root.join(rel);
        std::fs::create_dir_all(abs.parent().unwrap()).unwrap();
        std::fs::write(&abs, body).unwrap();
        let mtime_before = std::fs::metadata(&abs).unwrap().modified().unwrap();

        let outcomes = client.drain_once().await;
        assert!(matches!(outcomes[0].1, PushOutcome::Sent { .. }));
        assert_eq!(
            std::fs::read(&abs).unwrap(),
            body,
            "file untouched - no align attempted"
        );
        assert_eq!(
            std::fs::metadata(&abs).unwrap().modified().unwrap(),
            mtime_before,
            "pre-Piece-1 compat path must be zero-write"
        );
        // shadow_hash_for_ack falls back to content_hash when server_hash is None.
        assert_eq!(
            fx.shadow.get(rel).as_deref(),
            Some(local_sha.as_str()),
            "shadow still advances via the content_hash fallback"
        );
        m_push.assert_async().await;
        m_note.assert_async().await;
    }

    /// Final-review fix wave (piece1): the no-materializer fallback arm of
    /// `ack_materialize_back` (`self.materializer` is `None` — never true in
    /// production, see lib.rs, but must not mislead) must record NOTHING in
    /// the shadow store, not `server_hash`. Recording `server_hash` while the
    /// local file still holds the pre-canonicalization drained bytes would set
    /// shadow == server with local != server, which decide() classifies as
    /// R2 PreserveLocalEdit (a "genuine local edit" to push back UP) — the
    /// inverse of convergence. Leaving the shadow at its prior value lets the
    /// next reconcile pass fall to a PULL, which actually converges the file.
    #[tokio::test]
    async fn test_no_materializer_fallback_records_nothing_in_shadow() {
        let body = b"drained pre-canonicalization bytes\n";
        let drained_sha = sha256_hex(body);
        let server_hash = sha256_hex(b"different canonical bytes the server stored\n");
        let mut srv = Server::new_async().await;
        let m_push = srv
            .mock("POST", "/api/sync/push")
            .expect(1)
            .with_status(200)
            .with_body(accepted_body(&server_hash))
            .create_async()
            .await;

        let rel = "notes/a.md";
        let (_d, journal) = make_journal_with(vec![lazy_evt(rel, body)]);

        // No materializer wired (default PushClient::new) but a shadow store
        // IS attached, so the fallback arm's (non-)recording is observable.
        let sdir = TempDir::new().unwrap();
        let shadow = crate::sync_shadow::ShadowStore::load(sdir.path().join("shadow.json"));
        let vault = TempDir::new().unwrap();
        let abs = vault.path().join(rel);
        std::fs::create_dir_all(abs.parent().unwrap()).unwrap();
        std::fs::write(&abs, body).unwrap();

        let client = make_client_with_root(&srv.url(), journal.clone(), vault.path().to_path_buf())
            .await
            .with_shadow_store(shadow.clone());

        let outcomes = client.drain_once().await;
        assert!(
            matches!(outcomes[0].1, PushOutcome::Sent { .. }),
            "server still ACCEPTED the push; got {:?}",
            outcomes[0].1
        );
        assert!(
            shadow.get(rel).is_none(),
            "no-materializer fallback must record NOTHING (fail-closed -> PULL next pass), \
             got {:?}",
            shadow.get(rel)
        );
        // Sanity: drained_sha is genuinely not the server's canonical, i.e.
        // this exercises needs_align == true, not the steady-state branch.
        assert_ne!(drained_sha, server_hash);
        m_push.assert_async().await;
    }

    /// D2 core (B2'b option i + B2'c + B2'd): server canonicalized our CRLF
    /// push. Local canonicalize verifies against server_hash -> rewrite from
    /// LOCAL bytes (zero /note fetches), file becomes canonical, shadow ==
    /// server_hash, enqueued_hashes[path] == canonical sha.
    #[tokio::test]
    async fn test_ack_materialize_rewrite_then_shadow() {
        let drained = b"line one\r\nline two\r\n";
        let canonical = b"line one\nline two\n";
        let canon_sha = sha256_hex(canonical);
        let mut srv = Server::new_async().await;
        let m_push = srv
            .mock("POST", "/api/sync/push")
            .expect(1)
            .with_status(200)
            .with_body(accepted_body(&canon_sha))
            .create_async()
            .await;
        let m_note = srv
            .mock("GET", "/api/sync/note")
            .match_query(mockito::Matcher::Any)
            .expect(0) // local canonicalization verified - no fetch needed
            .create_async()
            .await;

        let rel = "notes/crlf.md";
        let (_d, journal) = make_journal_with(vec![lazy_evt(rel, drained)]);
        let (client, fx) = make_align_client(&srv.url(), journal).await;
        let abs = fx.vault_root.join(rel);
        std::fs::create_dir_all(abs.parent().unwrap()).unwrap();
        std::fs::write(&abs, drained).unwrap();

        let outcomes = client.drain_once().await;
        assert!(matches!(outcomes[0].1, PushOutcome::Sent { .. }));
        assert_eq!(
            std::fs::read(&abs).unwrap(),
            canonical,
            "local must be rewritten to the server canonical bytes"
        );
        assert_eq!(fx.shadow.get(rel).as_deref(), Some(canon_sha.as_str()));
        // B2'd: the shared enqueue-dedup map now holds the canonical sha so a
        // later touch event (past the echo TTL) is layer-2 deduped.
        assert_eq!(
            fx.enq.lock().unwrap().get(rel).map(String::as_str),
            Some(canon_sha.as_str()),
            "enqueued_hashes must be updated to the canonical sha"
        );
        m_push.assert_async().await;
        m_note.assert_async().await;
    }

    /// B2'c failing-rewrite half: the rewrite fails (read-only dir) -> the
    /// shadow must stay UNRECORDED at the server hash (stale -> next pass
    /// classifies PULL, fail-closed), and enqueued_hashes must not advance.
    #[cfg(unix)]
    #[tokio::test]
    async fn test_ack_materialize_failed_rewrite_leaves_shadow_stale() {
        use std::os::unix::fs::PermissionsExt;
        let drained = b"x\r\n";
        let canon_sha = sha256_hex(b"x\n");
        let mut srv = Server::new_async().await;
        let _m_push = srv
            .mock("POST", "/api/sync/push")
            .with_status(200)
            .with_body(accepted_body(&canon_sha))
            .create_async()
            .await;

        let rel = "notes/ro/g.md";
        let (_d, journal) = make_journal_with(vec![lazy_evt(rel, drained)]);
        let (client, fx) = make_align_client(&srv.url(), journal).await;
        let abs = fx.vault_root.join(rel);
        std::fs::create_dir_all(abs.parent().unwrap()).unwrap();
        std::fs::write(&abs, drained).unwrap();
        let parent = abs.parent().unwrap().to_path_buf();
        std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o555)).unwrap();

        let outcomes = client.drain_once().await;

        std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o755)).unwrap();

        assert!(matches!(outcomes[0].1, PushOutcome::Sent { .. }));
        assert_ne!(
            fx.shadow.get(rel).as_deref(),
            Some(canon_sha.as_str()),
            "B2'c: failed rewrite must NOT record shadow == server (phantom-push trap)"
        );
        assert!(
            fx.enq.lock().unwrap().get(rel).is_none(),
            "enqueued_hashes must not advance on a failed rewrite"
        );
        assert_eq!(std::fs::read(&abs).unwrap(), drained);
    }

    /// B2'a (the concurrent-edit guard test, spec-mandated): the file is
    /// edited between drain and ack -> rewrite SKIPPED, the edit's bytes
    /// survive, and the shadow records the SERVER hash (so the pending push2
    /// backfills the correct CAS base and the drift classifies PUSH, never a
    /// spurious 409-stash of the user's edit).
    #[tokio::test]
    async fn test_ack_materialize_concurrent_edit_guard() {
        let drained = b"first version\r\n";
        let edited = b"the user edited this mid-flight\n";
        let canon_sha = sha256_hex(b"first version\n");
        let mut srv = Server::new_async().await;
        let m_push = srv
            .mock("POST", "/api/sync/push")
            .expect(1)
            .with_status(200)
            .with_body(accepted_body(&canon_sha))
            .create_async()
            .await;

        let rel = "notes/edit.md";
        // EAGER event (content_bytes embedded) so "drained bytes" = drained
        // while the DISK already holds the newer edit - exactly the mid-flight
        // edit race compressed into one drain.
        let (_d, journal) = make_journal_with(vec![evt(rel, drained)]);
        let (client, fx) = make_align_client(&srv.url(), journal).await;
        let abs = fx.vault_root.join(rel);
        std::fs::create_dir_all(abs.parent().unwrap()).unwrap();
        std::fs::write(&abs, edited).unwrap();

        let outcomes = client.drain_once().await;
        assert!(matches!(outcomes[0].1, PushOutcome::Sent { .. }));
        assert_eq!(
            std::fs::read(&abs).unwrap(),
            edited,
            "B2'a: the mid-flight edit must NEVER be overwritten"
        );
        assert_eq!(
            fx.shadow.get(rel).as_deref(),
            Some(canon_sha.as_str()),
            "shadow records the server's current canonical (push2's CAS base)"
        );
        assert!(
            fx.enq.lock().unwrap().get(rel).is_none(),
            "enqueued_hashes must NOT advance past the edit (push2 must enqueue)"
        );
        m_push.assert_async().await;
    }

    /// B2'b fallback: local canonicalize does NOT reproduce server_hash (the
    /// server region-defense spliced bytes we don't have) -> fetch /note,
    /// verify served bytes hash to the advertised sha, write THOSE bytes.
    #[tokio::test]
    async fn test_ack_materialize_hash_verify_fallback() {
        let drained = b"body only\r\n";
        // The server spliced a managed region we don't have locally:
        let server_body = "<!-- nx:begin -->\nmanaged\n<!-- nx:end -->\n\nbody only\n";
        let server_sha = sha256_hex(server_body.as_bytes());
        let mut srv = Server::new_async().await;
        let m_push = srv
            .mock("POST", "/api/sync/push")
            .expect(1)
            .with_status(200)
            .with_body(accepted_body(&server_sha))
            .create_async()
            .await;
        let note_json = serde_json::json!({
            "path": "notes/defended.md",
            "frontmatter": null,
            "body": server_body,
            "sha256": server_sha,
            "modified": "2026-07-01T00:00:00Z",
            "file_mtime": null,
            "enriched_body": server_body,
            "created": null,
        })
        .to_string();
        let m_note = srv
            .mock("GET", "/api/sync/note")
            .match_query(mockito::Matcher::UrlEncoded(
                "path".into(),
                "notes/defended.md".into(),
            ))
            .expect(1)
            .with_status(200)
            .with_body(note_json)
            .create_async()
            .await;

        let rel = "notes/defended.md";
        let (_d, journal) = make_journal_with(vec![lazy_evt(rel, drained)]);
        let (client, fx) = make_align_client(&srv.url(), journal).await;
        let abs = fx.vault_root.join(rel);
        std::fs::create_dir_all(abs.parent().unwrap()).unwrap();
        std::fs::write(&abs, drained).unwrap();

        let outcomes = client.drain_once().await;
        assert!(matches!(outcomes[0].1, PushOutcome::Sent { .. }));
        assert_eq!(
            std::fs::read(&abs).unwrap(),
            server_body.as_bytes(),
            "the /note-served bytes must be written"
        );
        assert_eq!(fx.shadow.get(rel).as_deref(), Some(server_sha.as_str()));
        assert_eq!(
            fx.enq.lock().unwrap().get(rel).map(String::as_str),
            Some(server_sha.as_str())
        );
        m_push.assert_async().await;
        m_note.assert_async().await;
    }

    /// B2'd (spec: test_ack_materialize_updates_enqueued_hashes): after an
    /// ack-materialize rewrite, BOTH suppression inputs the file_watcher's
    /// layer-2 dedup checks (enqueued_hashes[p] == sha AND shadow.get(p) ==
    /// sha, file_watcher.rs:941-960) hold for the canonical sha, so a touch
    /// event past the echo TTL with unchanged bytes cannot enqueue a push.
    /// (The watcher-level end-to-end assertion is Task 8's
    /// b2d_touch_after_ack_materialize_is_deduped.)
    #[tokio::test]
    async fn test_ack_materialize_updates_enqueued_hashes() {
        let drained = b"t\r\n";
        let canonical = b"t\n";
        let canon_sha = sha256_hex(canonical);
        let mut srv = Server::new_async().await;
        let _m = srv
            .mock("POST", "/api/sync/push")
            .with_status(200)
            .with_body(accepted_body(&canon_sha))
            .create_async()
            .await;

        let rel = "notes/touch.md";
        let (_d, journal) = make_journal_with(vec![lazy_evt(rel, drained)]);
        let (client, fx) = make_align_client(&srv.url(), journal).await;
        let abs = fx.vault_root.join(rel);
        std::fs::create_dir_all(abs.parent().unwrap()).unwrap();
        std::fs::write(&abs, drained).unwrap();

        client.drain_once().await;

        let enqueued = fx.enq.lock().unwrap().get(rel).cloned();
        let shadowed = fx.shadow.get(rel);
        assert_eq!(enqueued.as_deref(), Some(canon_sha.as_str()));
        assert_eq!(shadowed.as_deref(), Some(canon_sha.as_str()));
        assert_eq!(std::fs::read(&abs).unwrap(), canonical);
    }

    /// Regression (T4 review): the event's ENQUEUE-time `content_sha` is a
    /// snapshot of whatever the file held when the watcher observed it -
    /// pushes are lazy (content_bytes: None), so by DRAIN time the file may
    /// hold different bytes (a second, fast edit landed before the drain
    /// tick). This proves the D2 ack-materialize path's basis is the DRAINED
    /// sha (what was actually read off disk and POSTed), never the stale
    /// enqueue-time `content_sha`:
    /// * the server response is keyed off a hash of the DRAINED bytes
    ///   (`accepted_body` echoes what `drain_once` actually sent — if the
    ///   client used the enqueue-time sha anywhere in the request/compare
    ///   path this mock's `server_hash` would not line up and the align
    ///   would never trigger correctly);
    /// * `write_aligned_bytes`'s pre-rewrite re-read (B2'a) is compared
    ///   against `drained_sha`, not `evt.content_sha` — proven here because
    ///   the on-disk bytes at drain time equal the DRAINED sha (not the
    ///   enqueue-time one), so a rewrite keyed off the wrong basis would spin
    ///   this into `SkippedConcurrentEdit` instead of `Rewrote`.
    #[tokio::test]
    async fn test_ack_materialize_uses_drained_sha_not_enqueue_time_sha() {
        let stale_content_at_enqueue = b"version at enqueue time\r\n";
        let drained = b"version at drain time\r\n"; // file changed before drain
        let canonical = b"version at drain time\n";
        let canon_sha = sha256_hex(canonical);
        let mut srv = Server::new_async().await;
        let m_push = srv
            .mock("POST", "/api/sync/push")
            .expect(1)
            .with_status(200)
            .with_body(accepted_body(&canon_sha))
            .create_async()
            .await;
        let m_note = srv
            .mock("GET", "/api/sync/note")
            .match_query(mockito::Matcher::Any)
            .expect(0) // local canonicalization verifies — no fetch needed
            .create_async()
            .await;

        let rel = "notes/stale-enqueue.md";
        // lazy_evt's content_sha reflects `stale_content_at_enqueue`, NOT the
        // bytes that will actually be on disk at drain time.
        let (_d, journal) = make_journal_with(vec![lazy_evt(rel, stale_content_at_enqueue)]);
        let (client, fx) = make_align_client(&srv.url(), journal).await;
        let abs = fx.vault_root.join(rel);
        std::fs::create_dir_all(abs.parent().unwrap()).unwrap();
        // The file on disk holds the NEWER (drained) content by the time
        // drain_once reads it — the enqueue-time snapshot is stale.
        std::fs::write(&abs, drained).unwrap();

        let outcomes = client.drain_once().await;
        assert!(matches!(outcomes[0].1, PushOutcome::Sent { .. }));
        assert_eq!(
            std::fs::read(&abs).unwrap(),
            canonical,
            "rewrite must succeed (Rewrote, not SkippedConcurrentEdit) — proving the \
             pre-rewrite guard compared against the DRAINED sha, not the stale enqueue-time sha"
        );
        assert_eq!(fx.shadow.get(rel).as_deref(), Some(canon_sha.as_str()));
        assert_eq!(
            fx.enq.lock().unwrap().get(rel).map(String::as_str),
            Some(canon_sha.as_str())
        );
        m_push.assert_async().await;
        m_note.assert_async().await;
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
            base_seq_store: None,
            sync_health: None,
            gate_cooldown_until: Arc::new(std::sync::Mutex::new(None)),
            materializer: None,
            enqueued_hashes: None,
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
            base_seq_store: None,
            sync_health: None,
            gate_cooldown_until: Arc::new(std::sync::Mutex::new(None)),
            materializer: None,
            enqueued_hashes: None,
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
            base_seq_store: None,
            sync_health: None,
            gate_cooldown_until: Arc::new(std::sync::Mutex::new(None)),
            materializer: None,
            enqueued_hashes: None,
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
            base_seq_store: None,
            sync_health: None,
            gate_cooldown_until: Arc::new(std::sync::Mutex::new(None)),
            materializer: None,
            enqueued_hashes: None,
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
            base_seq_store: None,
            sync_health: None,
            gate_cooldown_until: Arc::new(std::sync::Mutex::new(None)),
            materializer: None,
            enqueued_hashes: None,
        };
        // Root-relative path "01_Inbox/note.md" — not prefixed with the
        // sync root string. The filter should pass (not substrate, allowed ext).
        let result = client.pre_journal_filter("01_Inbox/note.md", b"some content", None);
        assert!(
            result.is_none(),
            "root-relative path must not be filtered; got {result:?}"
        );
    }

    // --- shadow_hash_for_ack (D3, v0.4.28 + fix/reconcile-server-wins-shadow) ---

    #[test]
    fn shadow_hash_for_ack_table() {
        // D3 (v0.4.28): Accepted with a server_hash records the SERVER's
        // canonical hash, NOT the local sha. Recording the local sha here was
        // step 3 of the B1' eternal alternation: idempotent-accept of raw CRLF
        // bytes -> shadow=local -> next pass classifies PUSH again, forever.
        assert_eq!(
            shadow_hash_for_ack(&PushStatus::Accepted, "local", Some("srv"), Some("ch")),
            Some("srv".to_string())
        );
        // Accepted, no server_hash -> content_hash (pre-Piece-1 server compat).
        assert_eq!(
            shadow_hash_for_ack(&PushStatus::Accepted, "local", None, Some("ch")),
            Some("ch".to_string())
        );
        // Accepted, neither field -> the local sha (oldest-server compat; a
        // non-canonicalizing server stored exactly what we sent).
        assert_eq!(
            shadow_hash_for_ack(&PushStatus::Accepted, "local", None, None),
            Some("local".to_string())
        );
        // Merged -> prefer server_hash (unchanged behavior).
        assert_eq!(
            shadow_hash_for_ack(&PushStatus::Merged, "local", Some("srv"), Some("ch")),
            Some("srv".to_string())
        );
        // Merged, no server_hash -> fall back to content_hash.
        assert_eq!(
            shadow_hash_for_ack(&PushStatus::Merged, "local", None, Some("ch")),
            Some("ch".to_string())
        );
        // Merged, neither -> fall back to local (record SOMETHING).
        assert_eq!(
            shadow_hash_for_ack(&PushStatus::Merged, "local", None, None),
            Some("local".to_string())
        );
        // ConflictMarkers / Error -> record nothing (unchanged).
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
        client.arm_gate_cooldown(Duration::from_millis(200));

        // Phase 1: tick during the window — suppressed, empty outcomes, no HTTP.
        let outcomes = client.drain_once().await;
        assert!(
            outcomes.is_empty(),
            "tick inside the cooldown window must be fully suppressed"
        );

        // Real sleep past the short window — cheap since it's 50ms, not 900s.
        tokio::time::sleep(Duration::from_millis(300)).await;

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

    // --- R7b base_seq daemon leg integration (TKT-166e1c07) ---

    /// Fully-wired push client (materializer Live + shadow + base_seq) like
    /// production, plus a base_seq store handle for assertions. Vault root is a
    /// TempDir so refetch/merge materialization lands somewhere real.
    async fn make_baseseq_client(
        base_url: &str,
        journal: Arc<Mutex<PushJournal>>,
    ) -> (
        PushClient,
        TempDir,
        Arc<crate::base_seq_store::BaseSeqStore>,
        Arc<ShadowStore>,
    ) {
        let vault = TempDir::new().unwrap();
        let ws = TempDir::new().unwrap();
        let sdir = TempDir::new().unwrap();
        let shadow = ShadowStore::load(sdir.path().join("shadow.json"));
        let bs = crate::base_seq_store::BaseSeqStore::load(sdir.path().join("base_seq.json"));
        let mat = Materializer::new(
            vault.path().to_path_buf(),
            None,
            MaterializerMode::Live,
            ws.path().to_path_buf(),
            "sub-test".into(),
            MaterializerConfig {
                device_id: "dev-test".into(),
                ..Default::default()
            },
        )
        .with_shadow_store(shadow.clone())
        .with_base_seq_store(bs.clone())
        .with_echo_guard(Arc::new(EchoGuard::new()));
        let api = Arc::new(ApiClient::new(base_url, "vsk_test").unwrap());
        let client = PushClient::new(
            api,
            journal,
            "dev-test".into(),
            config_for_test(),
            vault.path().to_path_buf(),
        )
        .with_shadow_store(shadow.clone())
        .with_base_seq_store(bs.clone())
        .with_materializer(mat);
        // Keep the vault TempDir alive by returning it.
        (client, vault, bs, shadow)
    }

    /// R1 (wire): a push of a note with a recorded lineage DECLARES base_seq on
    /// the request body sent to /api/sync/push. The mock only matches when the
    /// body carries the seq, so a miss makes the push fail -> the test fails.
    #[tokio::test]
    async fn push_declares_recorded_base_seq_on_the_wire() {
        let mut srv = Server::new_async().await;
        let body = "hello base_seq";
        let drained = sha256_hex(body.as_bytes());
        let accepted = format!(
            r#"{{"status":"accepted","seq":1,"content_hash":"{drained}","server_hash":"{drained}","server_seq":5,"merged_content":null,"message":null}}"#
        );
        let m = srv
            .mock("POST", "/api/sync/push")
            .match_body(mockito::Matcher::Regex(r#""base_seq":91"#.to_string()))
            .with_status(200)
            .with_body(accepted)
            .create_async()
            .await;

        let (_d, journal) = make_journal_with(vec![evt("01_Notes/x.md", body.as_bytes())]);
        let (client, _vault, bs, _shadow) = make_baseseq_client(&srv.url(), journal).await;
        // Record a prior observation so the push declares it.
        bs.record("01_Notes/x.md", 91);

        let outcomes = client.drain_once().await;
        assert!(matches!(outcomes[0].1, PushOutcome::Sent { .. }));
        m.assert_async().await; // matched => base_seq:91 was on the wire (R1)
    }

    /// R3: an accepted push whose bytes are canonical (server_hash == our bytes,
    /// non-align) records the server's returned server_seq as the observed
    /// base_seq - and ONLY from the response, never a local guess.
    #[tokio::test]
    async fn accepted_push_records_server_seq_as_observed() {
        let mut srv = Server::new_async().await;
        let body = "canonical body";
        let drained = sha256_hex(body.as_bytes());
        let accepted = format!(
            r#"{{"status":"accepted","seq":1,"content_hash":"{drained}","server_hash":"{drained}","server_seq":4242,"merged_content":null,"message":null}}"#
        );
        let _m = srv
            .mock("POST", "/api/sync/push")
            .with_status(200)
            .with_body(accepted)
            .create_async()
            .await;

        let (_d, journal) = make_journal_with(vec![evt("01_Notes/c.md", body.as_bytes())]);
        let (client, _vault, bs, _shadow) = make_baseseq_client(&srv.url(), journal).await;
        assert_eq!(bs.get("01_Notes/c.md"), None); // unobserved before
        let outcomes = client.drain_once().await;
        assert!(matches!(outcomes[0].1, PushOutcome::Sent { .. }));
        assert_eq!(bs.get("01_Notes/c.md"), Some(4242)); // observed after (R3)
    }

    /// R2 + R4: on a 409 (causal gate / CAS) the daemon REFETCHES the current
    /// server version and MERGES it - materializes the server head locally
    /// (byte-verified), records the fresh observed base_seq from the /note
    /// response, and PRESERVES the losing local bytes as a conflict stash. It
    /// never blind-retries (one push attempt) and never overwrites-loses local.
    #[tokio::test]
    async fn conflict_refetches_merges_and_records_observed() {
        let mut srv = Server::new_async().await;
        let server_body = "server wins\n";
        let server_sha = sha256_hex(server_body.as_bytes());
        // Push is rejected by the causal gate / CAS.
        let mp = srv
            .mock("POST", "/api/sync/push")
            .with_status(409)
            .with_body(r#"{"expected_hash":"srv"}"#)
            .expect(1) // NO blind retry: exactly one push attempt
            .create_async()
            .await;
        // Refetch of the current server version (the merge source).
        let note = format!(
            r#"{{"path":"01_Notes/x.md","frontmatter":{{}},"body":"{server_body}","sha256":"{server_sha}","modified":null,"file_mtime":null,"created":null,"change_seq":9,"enriched_body":"{server_body}"}}"#
        );
        let mn = srv
            .mock("GET", "/api/sync/note")
            .match_query(mockito::Matcher::Any)
            .with_status(200)
            .with_body(note)
            .create_async()
            .await;

        let (_d, journal) = make_journal_with(vec![evt("01_Notes/x.md", b"local edit\n")]);
        let (client, vault, bs, _shadow) = make_baseseq_client(&srv.url(), journal).await;

        let outcomes = client.drain_once().await;
        // Conflict is surfaced (never silently swallowed, R6-friendly).
        assert!(
            matches!(
                outcomes[0].1,
                PushOutcome::Failed(FailureReason::ConflictUnrecoverable { .. })
            ),
            "got {:?}",
            outcomes[0].1
        );
        mp.assert_async().await; // exactly one push (no blind retry, R2)
        mn.assert_async().await; // the daemon refetched the server version (R2)

        // Merge converged the canonical file to the server head (byte-verified).
        let materialized = std::fs::read_to_string(vault.path().join("01_Notes/x.md")).unwrap();
        assert_eq!(materialized, server_body);
        // Observed base_seq recorded from the /note response, only after the
        // bytes materialized + verified (R3).
        assert_eq!(bs.get("01_Notes/x.md"), Some(9));
        // Losing local bytes preserved (never overwritten-lost): a conflict
        // sibling exists somewhere under the vault root.
        let mut found_stash = false;
        for e in walk_files(vault.path()) {
            if e.file_name().to_string_lossy().contains("conflict-from") {
                found_stash = true;
                break;
            }
        }
        assert!(
            found_stash,
            "losing local bytes must be stashed, not dropped"
        );
    }

    /// R7: a push client backed by a shadow store in the suspect state
    /// (vault_folders empty but vault-prefixed keys present) PARKS - drain_once
    /// refuses to drain and no push is sent (guards the 2026-07-18 trinity
    /// mass-re-push). The mock is set to reject if a push is attempted.
    #[tokio::test]
    async fn drain_parks_on_suspect_vault_scope() {
        let mut srv = Server::new_async().await;
        // Any push at all is a failure of the guard.
        let m = srv
            .mock("POST", "/api/sync/push")
            .expect(0)
            .create_async()
            .await;

        // Craft a suspect shadow: empty vault_folders + a vault-prefixed key.
        let sdir = TempDir::new().unwrap();
        let spath = sdir.path().join("shadow.json");
        let mut map = std::collections::HashMap::new();
        map.insert("Mainframe/01_Notes/x.md".to_string(), "h".to_string());
        std::fs::write(&spath, serde_json::to_vec(&map).unwrap()).unwrap();
        let shadow = ShadowStore::load_with_vault_folders(spath, vec![]);
        assert!(shadow.vault_scope_suspect());

        let (_d, journal) = make_journal_with(vec![evt("01_Notes/x.md", b"edit\n")]);
        let api = Arc::new(ApiClient::new(&srv.url(), "vsk_test").unwrap());
        let client = PushClient::new(
            api,
            journal,
            "dev-test".into(),
            config_for_test(),
            PathBuf::from("/nonexistent"),
        )
        .with_shadow_store(shadow);

        let outcomes = client.drain_once().await;
        assert!(outcomes.is_empty(), "parked drain must process nothing");
        m.assert_async().await; // expect(0): no push was attempted (R7)
    }

    /// Minimal recursive file walk for the conflict-stash assertion.
    fn walk_files(root: &std::path::Path) -> Vec<std::fs::DirEntry> {
        let mut out = Vec::new();
        if let Ok(rd) = std::fs::read_dir(root) {
            for e in rd.flatten() {
                let p = e.path();
                if p.is_dir() {
                    out.extend(walk_files(&p));
                } else {
                    out.push(e);
                }
            }
        }
        out
    }
}
