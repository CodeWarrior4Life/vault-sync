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
    /// Sleep between drain ticks in `run_loop`.
    pub loop_interval_ms: u64,
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
        }
    }

    /// Builder-style: attach a SharedTrayState so push outcomes update the
    /// tray menu / tooltip in near-real-time. Backwards-compatible.
    pub fn with_tray_state(mut self, state: SharedTrayState) -> Self {
        self.tray_state = Some(state);
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

        let mut out = Vec::with_capacity(batch.len());
        for (evt, cur) in batch {
            let outcome = self.process_event(&evt).await;
            // Ack on terminal-success (Sent / Merged / ConflictMarkers / Skipped).
            // Nack on Failed → event stays in journal, retried next tick.
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
            let mut j = self.journal.lock().await;
            let _ = if should_ack { j.ack(cur) } else { j.nack(cur) };
            drop(j);

            // v0.3 tray telemetry — increment on Sent (success) or Failed
            // (NetworkExhausted only; auth/conflict outcomes are explicit
            // skip-class, not "failure" for the dashboard's purposes).
            if let Some(tray) = &self.tray_state {
                if let Ok(mut w) = tray.write() {
                    match &outcome {
                        PushOutcome::Sent { .. } => w.inc_uploads_sent(),
                        PushOutcome::Failed(FailureReason::NetworkExhausted { .. }) => {
                            w.inc_uploads_failed()
                        }
                        _ => {}
                    }
                }
            }
            out.push((evt, outcome));
        }
        // Snapshot the journal depth so the tray's "Pending uploads" item
        // reflects what's still queued.
        if let Some(tray) = &self.tray_state {
            let pending = {
                let j = self.journal.lock().await;
                j.len()
            };
            if let Ok(mut w) = tray.write() {
                w.set_uploads_pending(pending);
            }
        }
        out
    }

    /// Run drain_once on a periodic interval until shutdown signal flips
    /// to `true`. Sleeps `loop_interval_ms` between ticks. Designed to be
    /// spawned by lib::run() once Wave 2 wire-up lands.
    pub async fn run_loop(&self, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        let interval = Duration::from_millis(self.config.loop_interval_ms);
        loop {
            // Drain one batch.
            let outcomes = self.drain_once().await;
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

        let req = PushRequest {
            device_id: &self.device_id,
            path: &evt.path,
            content_b64: &content_b64,
            // Server requires base_hash as a non-optional string. When the
            // client has no known server base (None), send "" — server treats
            // it as "no known base" (accept if absent, else 409 conflict).
            base_hash: evt.base_hash.as_deref().unwrap_or(""),
            action,
        };

        let mut last_err: Option<String> = None;
        for attempt in 0..self.config.max_retry_attempts {
            match self.api.push(&req).await {
                Ok(resp) => return map_response(resp),
                Err(ApiError::Unauthorized) => {
                    return PushOutcome::Failed(FailureReason::Unauthorized);
                }
                Err(ApiError::Forbidden) => {
                    return PushOutcome::Failed(FailureReason::Forbidden);
                }
                Err(ApiError::Conflict { expected_hash }) => {
                    return PushOutcome::Failed(FailureReason::ConflictUnrecoverable {
                        expected_hash,
                    });
                }
                // HTTP 400 = permanent reject (e.g. path excluded by V9 baseline
                // scope filter). Skip + ack so it never retry-storms (S481).
                Err(ApiError::Server(400)) => {
                    tracing::info!(
                        path = %evt.path,
                        "push rejected HTTP 400 (permanent) — skip + ack, no retry"
                    );
                    return PushOutcome::Skipped(SkipReason::ServerRejected { status: 400 });
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
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

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

    // --- pre-journal filter / pure-function tests (no HTTP) ---

    #[tokio::test]
    async fn substrate_path_skipped_without_http() {
        // Even with a mock server, we expect zero HTTP calls.
        let mut srv = Server::new_async().await;
        let m = srv
            .mock("POST", "/api/sync/push")
            .expect(0)
            .with_status(200)
            .create_async()
            .await;
        let (_d, journal) = make_journal_with(vec![evt("00_VAULT.md", b"x")]);
        let client = make_client(&srv.url(), journal.clone()).await;
        let outcomes = client.drain_once().await;
        assert_eq!(outcomes.len(), 1);
        match &outcomes[0].1 {
            PushOutcome::Skipped(SkipReason::SubstrateRefused { rule }) => {
                assert_eq!(*rule, "00_VAULT.md");
            }
            other => panic!("expected SubstrateRefused, got {other:?}"),
        }
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
        };
        // Root-relative path "01_Inbox/note.md" — not prefixed with the
        // sync root string. The filter should pass (not substrate, allowed ext).
        let result = client.pre_journal_filter(
            "01_Inbox/note.md",
            b"some content",
            None,
        );
        assert!(result.is_none(), "root-relative path must not be filtered; got {result:?}");
    }
}
