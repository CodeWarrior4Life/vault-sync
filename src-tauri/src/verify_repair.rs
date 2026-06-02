//! Owner-invokable full-vault rescan + drift recovery (Lattice Vault Sync
//! v0.3 mandate §3 "Verify and repair all files" + §4.1).
//!
//! Borrowed pattern from obsidian-livesync's same-named admin command. When
//! the owner clicks "Verify and repair all files" in the tray (and on the
//! periodic reconciliation backstop), we walk every file in the configured
//! vault, compute its raw-bytes SHA-256, ship `{path, fs_hash}` to the server's
//! `POST /api/sync/reconcile-batch`, and react to the per-path `state` deltas:
//!
//! * `"drift"` (local fs_hash != server) / `"missing-on-server"` (server has no
//!   row) → enqueue a `PushAction::Modify` event into the push_journal. The
//!   push_client drain-loop ships it out of band. Counted under `modify_count`
//!   (field name kept for tray/dialog compatibility).
//! * `"match"` → no-op; the file is in sync.
//!
//! There is NO "pull" outcome — reconcile-batch only echoes paths the client
//! SENT, so it never asks us to fetch a server-only file; those are
//! materialized by the SSE/changes feed instead. `add_count` therefore stays 0.
//!
//! v0.4.10: migrated off the dead legacy `POST /api/sync/reconcile` (SQLite
//! `sync_devices`, which the v0.3+ subscriber daemon never registers in → every
//! call 404'd "Device not registered") to `POST /api/sync/reconcile-batch`
//! (Postgres `vault_reconcile_state`, subscriber-bearer auth). This is what
//! finally makes the reconciliation backstop catch up files created/edited
//! while the daemon was down. See `api_client::ReconcileBatchItem`.
//!
//! NOTE: the server has NO "delete" concept — a file local-only is a push, not
//! a delete. `delete_count` therefore stays 0; the field is retained only for
//! tray/dialog struct compatibility.
//!
//! Module is push-only on the write side — no `tokio::fs::write` calls happen
//! here. Verification is read-side and reporting; mutation goes through the
//! existing journal → push_client chain.
//!
//! Safety properties:
//!   * Substrate fence (`rasp_fence::classify_path`) applied during the walk.
//!     Files classified as Substrate are EXCLUDED from the manifest entirely
//!     — we don't hash them, don't send them, and don't push them. This
//!     means the server will never receive a substrate-class entry in our
//!     manifest, so it cannot ask us to push or delete one either.
//!   * Path canonicalization + `starts_with(vault_root)` assertion (R7) on
//!     every walked path catches symlink escapes.
//!   * Hardcoded exclude list shields `.obsidian/`, `.lattice-sync/`,
//!     `.trash/`, and the `_archive/` (legacy daily-rollover dir).
//!   * Extension allow-list — only `.md` / `.canvas` enter the manifest by
//!     default.
//!
//! Hashing strategy (v0.3.1): parallel hashing over a sequential walk.
//!
//! Phase 1 (`collect_candidate_paths`) — sequential `walkdir`: depth-first,
//! deterministic, cheap directory I/O. Applies ALL filters (R7 symlink-escape
//! canonicalization, hardcoded excludes, RASP substrate fence, extension
//! gate) and updates the walk counters (substrate_refused_count,
//! extension_filtered_count, errors). Produces a `Vec<CandidatePath>` of
//! `(canonical_abs, rel_str)` pairs that survived every filter.
//!
//! Phase 2 (`hash_paths_parallel`) — bounded-concurrency SHA: each surviving
//! path is hashed inside a `tokio::task::spawn_blocking` job scheduled through
//! a `tokio::task::JoinSet`, capped at `config.max_concurrent_hashes` in-flight
//! tasks (default 8 when the field is 0). This saturates the blocking pool
//! across CPUs instead of hashing 28k files one-at-a-time. We deliberately do
//! NOT pull in `rayon` — `tokio` is already a dependency and its blocking pool
//! is exactly the right tool for read+CPU bound file work.
//!
//! Manifest order is irrelevant to the server, but the parallel pass sorts by
//! path before returning so tests can assert deterministic output and the
//! parallel result is byte-identical (modulo order) to the sequential one.
//!
//! `run()` (async) uses `build_local_manifest_parallel`. `build_local_manifest`
//! (sync) keeps the simple sequential path so unit tests don't need a tokio
//! runtime; both share `collect_candidate_paths` so the filtering logic is not
//! duplicated.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::sync::Mutex;
use walkdir::WalkDir;

use crate::api_client::{ApiClient, ApiError, ReconcileBatchItem, ReconcileBatchRequest};
use crate::push_journal::{
    new_event_id, JournalError, PushAction, PushEvent, PushJournal, CURRENT_SCHEMA,
};
use crate::rasp_fence::{classify_path, PathClassification};

const SAMPLE_CAP: usize = 50;

/// Static configuration for VerifyRepair runs.
#[derive(Debug, Clone)]
pub struct VerifyRepairConfig {
    /// Extensions (with leading dot) that may enter the manifest. Anything
    /// else is filtered out before hashing.
    pub allowed_extensions: Vec<String>,
    /// Path prefixes (relative to vault_root, forward-slash) that are
    /// excluded from the walk entirely. E.g. ".obsidian/", ".lattice-sync/",
    /// ".trash/", "_archive/".
    pub hardcoded_excludes: Vec<String>,
    /// When true, files classified as Substrate by `rasp_fence` are dropped
    /// from the manifest and counted under `substrate_refused_count`. In
    /// production this MUST be true. Tests may set it false to exercise the
    /// reconcile machinery on substrate paths.
    pub respect_substrate_fence: bool,
    /// Max concurrent SHA-256 hashing tasks during the parallel Phase-2 pass
    /// (`build_local_manifest_parallel`). A value of 0 defaults to 8. The sync
    /// `build_local_manifest` path (tests/dry-run) ignores this and hashes
    /// inline.
    pub max_concurrent_hashes: usize,
}

impl Default for VerifyRepairConfig {
    fn default() -> Self {
        Self {
            allowed_extensions: vec![".md".into(), ".canvas".into()],
            hardcoded_excludes: vec![
                ".obsidian/".into(),
                ".lattice-sync/".into(),
                ".trash/".into(),
                "_archive/".into(),
            ],
            respect_substrate_fence: true,
            max_concurrent_hashes: 8,
        }
    }
}

/// Result of one parallel hash job: (rel_path, canonical_abs, hash_result).
/// Named alias keeps the `JoinSet` generic readable (clippy::type_complexity).
type HashJobOutput = (String, PathBuf, Result<(String, u64), std::io::Error>);

/// A file that survived every Phase-1 filter and is awaiting hashing.
/// Internal to the two-phase manifest build.
struct CandidatePath {
    /// Canonicalized absolute path (R7-checked, inside vault_root).
    canonical: PathBuf,
    /// Forward-slashed path relative to vault_root.
    rel: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestEntry {
    pub path: String,
    pub content_hash: String,
    pub size_bytes: u64,
}

/// Final report returned by `VerifyRepair::run`.
///
/// `Serialize`/`Deserialize` are derived so the Tauri `#[command]` surface
/// (see `commands::verify_repair_run`) can return this directly to the JS
/// front-end and so it round-trips through `serde_json` in tests.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VerifyRepairReport {
    pub files_scanned: usize,
    pub files_in_sync: usize,
    /// reconcile-batch `state` ∈ {drift, missing-on-server}. Files the daemon
    /// will upload. Field name kept for tray/dialog compatibility.
    pub modify_count: usize,
    pub modify_paths_sample: Vec<String>,
    /// Always 0 under the reconcile-batch contract — it returns no "pull"
    /// outcome (server-only files are the SSE feed's job). Retained for
    /// tray/dialog struct compatibility.
    pub add_count: usize,
    pub add_paths_sample: Vec<String>,
    /// Always 0 — the server has no delete concept. Retained for tray/dialog
    /// struct compatibility only; never incremented.
    pub delete_count: usize,
    pub delete_paths_sample: Vec<String>,
    pub substrate_refused_count: usize,
    pub extension_filtered_count: usize,
    pub errors: Vec<(String, String)>,
    pub elapsed_ms: u64,
}

#[derive(Debug, Error)]
pub enum VerifyRepairError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("api: {0}")]
    Api(#[from] ApiError),
    #[error("journal: {0}")]
    Journal(#[from] JournalError),
}

pub struct VerifyRepair {
    vault_root: PathBuf,
    api: Arc<ApiClient>,
    journal: Arc<Mutex<PushJournal>>,
    device_id: String,
    config: VerifyRepairConfig,
}

impl VerifyRepair {
    pub fn new(
        vault_root: PathBuf,
        api: Arc<ApiClient>,
        journal: Arc<Mutex<PushJournal>>,
        device_id: String,
        config: VerifyRepairConfig,
    ) -> Self {
        Self {
            vault_root,
            api,
            journal,
            device_id,
            config,
        }
    }

    /// Full owner-invoked sweep:
    /// 1. Walk + hash → local manifest.
    /// 2. POST /api/sync/reconcile-batch ({paths:[{path, fs_hash}]}).
    /// 3. Enqueue a push for every drift / missing-on-server delta.
    /// 4. Return structured report.
    pub async fn run(&self) -> Result<VerifyRepairReport, VerifyRepairError> {
        let started = Instant::now();
        let mut report = VerifyRepairReport::default();

        let manifest = self.build_local_manifest_parallel(&mut report).await?;
        report.files_scanned = manifest.len();

        // v0.4.10: send the manifest as the reconcile-batch payload —
        // `{paths:[{path, fs_hash}]}`, where fs_hash is the raw-file SHA-256 we
        // already computed during the walk (== server `vault_reconcile_state.fs_hash`).
        let api_paths: Vec<ReconcileBatchItem> = manifest
            .iter()
            .map(|m| ReconcileBatchItem {
                path: m.path.clone(),
                fs_hash: m.content_hash.clone(),
            })
            .collect();

        let req = ReconcileBatchRequest { paths: api_paths };

        let deltas = match self.api.reconcile_batch(&req).await {
            Ok(r) => r.deltas,
            Err(e) => {
                tracing::error!("reconcile-batch failed: {e}");
                return Err(VerifyRepairError::Api(e));
            }
        };

        // Build a quick lookup: path → ManifestEntry so we can locate the
        // on-disk file for push deltas and enqueue a push.
        let local_index: std::collections::HashMap<&str, &ManifestEntry> =
            manifest.iter().map(|m| (m.path.as_str(), m)).collect();

        // Collect all push events during the delta loop, then write them to the
        // journal in ONE batched append after the loop (replaces N open/flush/
        // close cycles with a single one).
        let mut pending_pushes: Vec<PushEvent> = Vec::new();

        for delta in &deltas {
            match delta.state.as_str() {
                // Local differs from the server ("drift"), or the server has no
                // row for this path ("missing-on-server") → upload the local
                // canonical. Counted under modify_count (field name kept for
                // tray/dialog compatibility). NOTE: reconcile-batch never asks
                // us to PULL — it only echoes paths we sent; server-only files
                // are materialized by the SSE/changes feed, not here.
                "drift" | "missing-on-server" => {
                    report.modify_count += 1;
                    if report.modify_paths_sample.len() < SAMPLE_CAP {
                        report.modify_paths_sample.push(delta.path.clone());
                    }

                    let Some(local) = local_index.get(delta.path.as_str()) else {
                        // reconcile-batch only echoes paths we sent, so this is
                        // unreachable in practice; guard defensively.
                        tracing::warn!(
                            path = %delta.path,
                            state = %delta.state,
                            "reconcile-batch delta for a path not in local manifest"
                        );
                        report.errors.push((
                            delta.path.clone(),
                            "delta path not in local manifest".to_string(),
                        ));
                        continue;
                    };

                    // LIGHTWEIGHT (lazy) push ref — no file read here. The
                    // push_client reads the body from disk at drain time. The
                    // CAS base is the server's CURRENT hash from the delta:
                    // Some(server_hash) for drift (overwrite the diverged row),
                    // None for missing-on-server (create). delta.server_hash is
                    // already exactly that (None when the server has no row).
                    pending_pushes.push(self.build_modify_push(local, delta.server_hash.clone()));
                }
                // In sync — no-op.
                "match" => {}
                // Forward-compat: an unrecognized state is handled
                // conservatively (no push) and logged, never silently dropped.
                other => {
                    tracing::warn!(
                        path = %delta.path,
                        state = %other,
                        "reconcile-batch: unknown delta state — skipping (no push)"
                    );
                }
            }
        }

        // Batch-write all collected push refs in ONE journal append cycle.
        // append_batch degrades gracefully: if the batch would exceed the
        // journal capacity it writes what fits and returns the count, logging
        // a warning. We surface a shortfall as a report error so the owner
        // sees that not everything queued.
        if !pending_pushes.is_empty() {
            let queued = pending_pushes.len();
            let mut j = self.journal.lock().await;
            match j.append_batch(pending_pushes) {
                Ok(written) if written < queued => {
                    tracing::warn!(
                        written,
                        queued,
                        "verify_repair: journal capacity reached — only {written}/{queued} pushes queued"
                    );
                    report.errors.push((
                        "<journal>".to_string(),
                        format!("journal capacity reached: only {written}/{queued} pushes queued"),
                    ));
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!(error = %e, "verify_repair: append_batch failed");
                    report.errors.push(("<journal>".to_string(), e.to_string()));
                }
            }
        }

        // Pull/add_count files are server-side and were NOT in our scanned
        // local set, so they don't subtract from scanned. delete_count is
        // always 0 (no server delete concept). In-sync = scanned minus the
        // local files we need to push.
        report.files_in_sync = report.files_scanned.saturating_sub(report.modify_count);
        report.elapsed_ms = started.elapsed().as_millis() as u64;
        Ok(report)
    }

    /// Walk-and-hash without API call — useful for tests and dry-run.
    /// SEQUENTIAL: runs the walk + SHA on the calling thread, so it needs no
    /// tokio runtime and stays usable from plain sync `#[test]`s. Production
    /// `run()` uses the parallel variant instead. Always returns a fresh
    /// report-less list; counters from the walk (substrate refused, extension
    /// filtered, etc.) are not surfaced here.
    pub fn build_local_manifest(&self) -> Result<Vec<ManifestEntry>, VerifyRepairError> {
        let mut throwaway = VerifyRepairReport::default();
        self.build_local_manifest_with_report(&mut throwaway)
    }

    /// SEQUENTIAL manifest build with counter surfacing. Phase 1 collects the
    /// candidate paths (filters + counters), Phase 2 hashes them inline on the
    /// current thread. Shares the exact filter logic with the parallel path
    /// via `collect_candidate_paths`.
    fn build_local_manifest_with_report(
        &self,
        report: &mut VerifyRepairReport,
    ) -> Result<Vec<ManifestEntry>, VerifyRepairError> {
        let candidates = self.collect_candidate_paths(report)?;
        let mut out: Vec<ManifestEntry> = Vec::with_capacity(candidates.len());
        for c in candidates {
            match hash_file(&c.canonical) {
                Ok((hash, size)) => out.push(ManifestEntry {
                    path: c.rel,
                    content_hash: hash,
                    size_bytes: size,
                }),
                Err(e) => {
                    tracing::warn!(path = %c.rel, error = %e, "hash_file failed");
                    report.errors.push((c.rel, e.to_string()));
                }
            }
        }
        out.sort_by(|a, b| a.path.cmp(&b.path));
        Ok(out)
    }

    /// PARALLEL manifest build used by `run()`. Phase 1 (`collect_candidate_paths`)
    /// is the same sequential walk + filtering as the sync path. Phase 2 hashes
    /// the survivors via `spawn_blocking` jobs scheduled through a bounded
    /// `JoinSet` (≤ `max_concurrent_hashes` in flight) so 28k files saturate the
    /// blocking pool instead of hashing serially. Result is sorted by path so it
    /// is byte-identical (modulo nothing — sorted) to the sequential output.
    pub async fn build_local_manifest_parallel(
        &self,
        report: &mut VerifyRepairReport,
    ) -> Result<Vec<ManifestEntry>, VerifyRepairError> {
        let candidates = self.collect_candidate_paths(report)?;

        let cap = if self.config.max_concurrent_hashes == 0 {
            8
        } else {
            self.config.max_concurrent_hashes
        };

        let mut out: Vec<ManifestEntry> = Vec::with_capacity(candidates.len());
        let mut join_set: tokio::task::JoinSet<HashJobOutput> = tokio::task::JoinSet::new();
        let mut iter = candidates.into_iter();
        let mut in_flight = 0usize;

        // Prime the pump up to `cap`, then refill as each task drains so we
        // never exceed `cap` blocking jobs concurrently.
        loop {
            while in_flight < cap {
                let Some(c) = iter.next() else { break };
                let canon = c.canonical.clone();
                let rel = c.rel.clone();
                join_set.spawn_blocking(move || (rel, canon.clone(), hash_file(&canon)));
                in_flight += 1;
            }

            let Some(joined) = join_set.join_next().await else {
                break;
            };
            in_flight -= 1;

            match joined {
                Ok((rel, _canon, Ok((hash, size)))) => out.push(ManifestEntry {
                    path: rel,
                    content_hash: hash,
                    size_bytes: size,
                }),
                Ok((rel, _canon, Err(e))) => {
                    tracing::warn!(path = %rel, error = %e, "hash_file failed");
                    report.errors.push((rel, e.to_string()));
                }
                Err(join_err) => {
                    // spawn_blocking task panicked or was cancelled. Surface it
                    // as a generic error keyed on "<hash task>".
                    tracing::warn!(error = %join_err, "hash spawn_blocking task failed");
                    report
                        .errors
                        .push(("<hash task>".to_string(), join_err.to_string()));
                }
            }
        }

        out.sort_by(|a, b| a.path.cmp(&b.path));
        Ok(out)
    }

    /// Phase 1: sequential walk + filtering. Returns the candidate files that
    /// survived every gate (R7 symlink-escape, hardcoded excludes, substrate
    /// fence, extension allow-list). Mutates `report` walk counters
    /// (substrate_refused_count, extension_filtered_count, errors). Does NOT
    /// hash — that's Phase 2's job (sequential or parallel).
    fn collect_candidate_paths(
        &self,
        report: &mut VerifyRepairReport,
    ) -> Result<Vec<CandidatePath>, VerifyRepairError> {
        let root = match self.vault_root.canonicalize() {
            Ok(r) => r,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // Empty/non-existent vault → empty manifest, no error.
                return Ok(Vec::new());
            }
            Err(e) => return Err(VerifyRepairError::Io(e)),
        };

        let mut out: Vec<CandidatePath> = Vec::new();

        for entry in WalkDir::new(&root).follow_links(false).into_iter() {
            let entry = match entry {
                Ok(e) => e,
                Err(e) => {
                    let p = e
                        .path()
                        .map(|p| p.to_string_lossy().into_owned())
                        .unwrap_or_else(|| "<unknown>".to_string());
                    tracing::warn!(path = %p, error = %e, "walkdir error");
                    report.errors.push((p, e.to_string()));
                    continue;
                }
            };
            if !entry.file_type().is_file() {
                continue;
            }
            let abs = entry.path();

            // R7 — canonicalize + assert containment. Skip symlink escapes.
            let canon = match abs.canonicalize() {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!(path = %abs.display(), error = %e, "canonicalize failed");
                    report
                        .errors
                        .push((abs.display().to_string(), e.to_string()));
                    continue;
                }
            };
            if !canon.starts_with(&root) {
                tracing::warn!(path = %abs.display(), "path escapes vault_root via symlink — skipping");
                continue;
            }

            // Relative path with forward slashes.
            let rel = match canon.strip_prefix(&root) {
                Ok(r) => r,
                Err(_) => continue,
            };
            let rel_str = path_to_forward_slash(rel);

            // Hardcoded excludes (anywhere in the path).
            if self
                .config
                .hardcoded_excludes
                .iter()
                .any(|ex| starts_with_or_contains_segment(&rel_str, ex))
            {
                continue;
            }

            // Substrate fence.
            if self.config.respect_substrate_fence {
                if let PathClassification::Substrate { rule } = classify_path(&rel_str) {
                    report.substrate_refused_count += 1;
                    tracing::debug!(path = %rel_str, rule, "verify_repair: substrate refused");
                    continue;
                }
            }

            // Extension gate.
            if !ext_allowed(&rel_str, &self.config.allowed_extensions) {
                report.extension_filtered_count += 1;
                continue;
            }

            out.push(CandidatePath {
                canonical: canon,
                rel: rel_str,
            });
        }

        Ok(out)
    }

    /// Build a LIGHTWEIGHT (lazy) Modify push for a reconcile delta. Does NOT
    /// read the file — `content_bytes: None` tells push_client to read the body
    /// from disk at drain time. `content_sha` is the LOCAL (new) hash we're
    /// writing (from the manifest walk; no re-hash).
    ///
    /// v0.4.11: `base_hash` is the server-side CAS base the push handler checks
    /// against `vault_reconcile_state.fs_hash` — it MUST be the server's CURRENT
    /// hash, NOT our local one:
    ///   * `drift`             → `Some(server_hash)` (from the reconcile delta) so
    ///                           the CAS passes and the local version overwrites
    ///                           the diverged server one.
    ///   * `missing-on-server` → `None` (sent as `""`) so the server CREATEs the
    ///                           row (an `""` base on an existing row would conflict).
    /// v0.4.10 wrongly sent our LOCAL hash here, which by definition mismatches a
    /// drifted server row → every drift push 409'd `ConflictUnrecoverable`.
    fn build_modify_push(&self, entry: &ManifestEntry, base_hash: Option<String>) -> PushEvent {
        PushEvent {
            schema_version: CURRENT_SCHEMA,
            id: new_event_id(),
            path: entry.path.clone(),
            action: PushAction::Modify,
            base_hash,
            content_sha: entry.content_hash.clone(),
            content_bytes: None,
            queued_at: chrono::Utc::now(),
            device_id: self.device_id.clone(),
        }
    }
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/// B4 (Nexus Sync): pure helper that derives the ordered list of
/// `(root_path, subscriber_id)` pairs to reconcile, one per sync root.
///
/// * `root.subscriber_id` non-empty → use it (the root has its own registered
///   subscriber).
/// * Empty → fall back to `fallback_subscriber_id` (the top-level
///   `Config.subscriber_id`; matches back-compat behaviour in lib.rs).
///
/// Returns an empty Vec when `sync_roots` is empty. Callers iterate the
/// returned pairs, constructing one `VerifyRepair` per entry.
pub fn roots_to_reconcile_pairs(
    sync_roots: &[crate::config::SyncRoot],
    fallback_subscriber_id: &str,
) -> Vec<(PathBuf, String)> {
    sync_roots
        .iter()
        .map(|root| {
            let sub_id = if !root.subscriber_id.is_empty() {
                root.subscriber_id.clone()
            } else {
                fallback_subscriber_id.to_string()
            };
            (root.path.clone(), sub_id)
        })
        .collect()
}

fn path_to_forward_slash(p: &Path) -> String {
    p.to_string_lossy().replace('\\', "/")
}

fn ext_allowed(path: &str, allowed: &[String]) -> bool {
    let lower = path.to_ascii_lowercase();
    allowed
        .iter()
        .any(|e| lower.ends_with(&e.to_ascii_lowercase()))
}

/// Match `prefix` either at the very start of `rel` (most common — e.g.
/// `.obsidian/foo`) or as a `/<prefix>` substring (rare but covers nested
/// `.trash/` dirs). The trailing slash on `prefix` is required for clean
/// segment boundaries.
fn starts_with_or_contains_segment(rel: &str, prefix: &str) -> bool {
    if rel.starts_with(prefix) {
        return true;
    }
    let needle = format!("/{prefix}");
    rel.contains(&needle)
}

fn hash_file(path: &Path) -> Result<(String, u64), std::io::Error> {
    use std::io::Read;
    let mut f = std::fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 64 * 1024];
    let mut total: u64 = 0;
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        total = total.saturating_add(n as u64);
    }
    Ok((hex::encode(hasher.finalize()), total))
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api_client::ApiClient;
    use mockito::Server;
    use std::fs;
    use std::path::PathBuf;
    use tempfile::TempDir;

    fn test_config() -> VerifyRepairConfig {
        VerifyRepairConfig::default()
    }

    fn write_file(root: &Path, rel: &str, content: &[u8]) -> PathBuf {
        let p = root.join(rel);
        if let Some(parent) = p.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(&p, content).unwrap();
        p
    }

    async fn make_journal() -> (TempDir, Arc<Mutex<PushJournal>>) {
        let dir = TempDir::new().unwrap();
        let p = dir.path().join("push_journal.jsonl");
        let j = PushJournal::open(&p).unwrap();
        (dir, Arc::new(Mutex::new(j)))
    }

    fn make_api(url: &str) -> Arc<ApiClient> {
        Arc::new(ApiClient::new(url, "vsk_test").unwrap())
    }

    async fn make_vr(
        vault_root: PathBuf,
        url: &str,
        config: VerifyRepairConfig,
    ) -> (VerifyRepair, Arc<Mutex<PushJournal>>, TempDir) {
        let (jdir, journal) = make_journal().await;
        let api = make_api(url);
        let vr = VerifyRepair::new(vault_root, api, journal.clone(), "dev-test".into(), config);
        (vr, journal, jdir)
    }

    // ─── manifest-building tests ─────────────────────────────────────────

    #[tokio::test]
    async fn build_local_manifest_walks_only_allowed_extensions() {
        let vault = TempDir::new().unwrap();
        let v = vault.path();
        write_file(v, "notes/a.md", b"alpha");
        write_file(v, "notes/b.canvas", b"{}");
        write_file(v, "notes/c.png", b"PNG-bytes");
        write_file(v, "notes/d.exe", b"EXE-bytes");
        let (vr, _j, _jd) = make_vr(v.to_path_buf(), "http://127.0.0.1:1", test_config()).await;
        let m = vr.build_local_manifest().unwrap();
        let paths: Vec<&str> = m.iter().map(|e| e.path.as_str()).collect();
        assert!(paths.contains(&"notes/a.md"));
        assert!(paths.contains(&"notes/b.canvas"));
        assert!(!paths.iter().any(|p| p.ends_with(".png")));
        assert!(!paths.iter().any(|p| p.ends_with(".exe")));
    }

    #[tokio::test]
    async fn manifest_excludes_obsidian_lattice_trash_dirs() {
        let vault = TempDir::new().unwrap();
        let v = vault.path();
        write_file(v, ".obsidian/workspace.json", b"x");
        write_file(v, ".obsidian/plugins/foo/main.js", b"x");
        write_file(v, ".lattice-sync/shadow/bar.md", b"x");
        write_file(v, ".trash/old.md", b"x");
        write_file(v, "_archive/2024.md", b"x");
        write_file(v, "notes/keeper.md", b"alpha");
        let (vr, _j, _jd) = make_vr(v.to_path_buf(), "http://127.0.0.1:1", test_config()).await;
        let m = vr.build_local_manifest().unwrap();
        let paths: Vec<&str> = m.iter().map(|e| e.path.as_str()).collect();
        assert_eq!(paths, vec!["notes/keeper.md"]);
    }

    #[tokio::test]
    async fn manifest_excludes_substrate_paths() {
        let vault = TempDir::new().unwrap();
        let v = vault.path();
        write_file(v, "00_VAULT.md", b"x");
        write_file(v, "02_Projects/Protocols/X.md", b"x");
        write_file(v, "_rapport/people/alice/notes.md", b"x");
        write_file(v, "_rapport/groups/dev.md", b"x");
        write_file(v, "_rapport/triage/inbox.md", b"x");
        write_file(v, "CLAUDE.md", b"x");
        write_file(v, "02_Projects/Foo/Family.md", b"x");
        write_file(v, "01_Inbox/note.md", b"content");
        let (vr, _j, _jd) = make_vr(v.to_path_buf(), "http://127.0.0.1:1", test_config()).await;
        let mut report = VerifyRepairReport::default();
        let m = vr.build_local_manifest_with_report(&mut report).unwrap();
        let paths: Vec<&str> = m.iter().map(|e| e.path.as_str()).collect();
        assert_eq!(paths, vec!["01_Inbox/note.md"]);
        // 7 substrate files filtered out by RASP fence.
        assert_eq!(report.substrate_refused_count, 7);
    }

    #[tokio::test]
    async fn manifest_includes_root_family_md_after_rasp_rebuild() {
        // v0.3 RASP scoped Family.md to 02_Projects/** — root Family.md
        // is now content, must appear in the manifest.
        let vault = TempDir::new().unwrap();
        let v = vault.path();
        write_file(v, "Family.md", b"root-family-content");
        let (vr, _j, _jd) = make_vr(v.to_path_buf(), "http://127.0.0.1:1", test_config()).await;
        let m = vr.build_local_manifest().unwrap();
        assert!(m.iter().any(|e| e.path == "Family.md"));
    }

    #[tokio::test]
    async fn manifest_hashes_match_known_vectors() {
        // SHA-256("abc") = ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad
        let vault = TempDir::new().unwrap();
        let v = vault.path();
        write_file(v, "abc.md", b"abc");
        let (vr, _j, _jd) = make_vr(v.to_path_buf(), "http://127.0.0.1:1", test_config()).await;
        let m = vr.build_local_manifest().unwrap();
        assert_eq!(m.len(), 1);
        assert_eq!(
            m[0].content_hash,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(m[0].size_bytes, 3);
    }

    #[tokio::test]
    async fn manifest_canonicalizes_forward_slash() {
        // Even on Windows the manifest entries should be forward-slashed.
        let vault = TempDir::new().unwrap();
        let v = vault.path();
        write_file(v, "deep/nested/path/note.md", b"x");
        let (vr, _j, _jd) = make_vr(v.to_path_buf(), "http://127.0.0.1:1", test_config()).await;
        let m = vr.build_local_manifest().unwrap();
        assert_eq!(m.len(), 1);
        assert!(!m[0].path.contains('\\'));
        assert_eq!(m[0].path, "deep/nested/path/note.md");
    }

    #[tokio::test]
    async fn manifest_handles_empty_vault_gracefully() {
        let vault = TempDir::new().unwrap();
        let v = vault.path();
        let (vr, _j, _jd) = make_vr(v.to_path_buf(), "http://127.0.0.1:1", test_config()).await;
        let m = vr.build_local_manifest().unwrap();
        assert_eq!(m.len(), 0);
    }

    #[tokio::test]
    async fn manifest_handles_missing_vault_root_gracefully() {
        // vault_root does not exist on disk.
        let vault = TempDir::new().unwrap();
        let v = vault.path().join("does-not-exist");
        let (vr, _j, _jd) = make_vr(v, "http://127.0.0.1:1", test_config()).await;
        let m = vr.build_local_manifest().unwrap();
        assert_eq!(m.len(), 0);
    }

    #[tokio::test]
    async fn extension_filtered_count_populated() {
        let vault = TempDir::new().unwrap();
        let v = vault.path();
        write_file(v, "a.md", b"x");
        write_file(v, "b.png", b"x");
        write_file(v, "c.exe", b"x");
        let (vr, _j, _jd) = make_vr(v.to_path_buf(), "http://127.0.0.1:1", test_config()).await;
        let mut report = VerifyRepairReport::default();
        let _m = vr.build_local_manifest_with_report(&mut report).unwrap();
        assert_eq!(report.extension_filtered_count, 2);
    }

    #[tokio::test]
    async fn parallel_manifest_matches_sequential_sorted() {
        // ~20 files across nested dirs + a mix of allowed/filtered/substrate;
        // the parallel build must produce the SAME entries (sorted) as the
        // sequential build.
        let vault = TempDir::new().unwrap();
        let v = vault.path();
        for i in 0..20 {
            write_file(
                v,
                &format!("notes/n{i:02}.md"),
                format!("body-{i}").as_bytes(),
            );
        }
        write_file(v, "deep/a/b/c.canvas", b"{}");
        write_file(v, "ignored.png", b"img"); // extension-filtered
        write_file(v, ".obsidian/workspace.json", b"x"); // hardcoded exclude
        write_file(v, "CLAUDE.md", b"x"); // substrate-refused

        let (vr, _j, _jd) = make_vr(v.to_path_buf(), "http://127.0.0.1:1", test_config()).await;

        let mut seq_report = VerifyRepairReport::default();
        let mut seq = vr
            .build_local_manifest_with_report(&mut seq_report)
            .unwrap();
        seq.sort_by(|a, b| a.path.cmp(&b.path));

        let mut par_report = VerifyRepairReport::default();
        let par = vr
            .build_local_manifest_parallel(&mut par_report)
            .await
            .unwrap();

        assert_eq!(seq, par, "parallel manifest differs from sequential");
        // 20 .md + 1 .canvas survive.
        assert_eq!(par.len(), 21);
        assert_eq!(
            seq_report.substrate_refused_count,
            par_report.substrate_refused_count
        );
        assert_eq!(
            seq_report.extension_filtered_count,
            par_report.extension_filtered_count
        );
    }

    #[tokio::test]
    async fn parallel_manifest_respects_zero_concurrency_default() {
        // max_concurrent_hashes = 0 must NOT divide-by-zero / spin; it should
        // default to a sane cap and still hash everything.
        let vault = TempDir::new().unwrap();
        let v = vault.path();
        for i in 0..10 {
            write_file(v, &format!("f{i}.md"), format!("c{i}").as_bytes());
        }
        let mut cfg = test_config();
        cfg.max_concurrent_hashes = 0;
        let (vr, _j, _jd) = make_vr(v.to_path_buf(), "http://127.0.0.1:1", cfg).await;
        let mut report = VerifyRepairReport::default();
        let m = vr.build_local_manifest_parallel(&mut report).await.unwrap();
        assert_eq!(m.len(), 10);
    }

    // ─── run() — HTTP-driven tests ───────────────────────────────────────

    #[tokio::test]
    async fn run_calls_reconcile_with_local_manifest() {
        let vault = TempDir::new().unwrap();
        write_file(vault.path(), "notes/a.md", b"alpha");
        write_file(vault.path(), "notes/b.md", b"beta");

        let mut srv = Server::new_async().await;
        let m = srv
            .mock("POST", "/api/sync/reconcile-batch")
            // v0.4.10 contract: request is {paths:[{path,fs_hash}]} — assert the
            // manifest paths are present (and the legacy device_id is gone).
            .match_body(mockito::Matcher::AllOf(vec![
                mockito::Matcher::Regex(r#""path":"notes/a\.md""#.to_string()),
                mockito::Matcher::Regex(r#""fs_hash""#.to_string()),
            ]))
            .with_status(200)
            // All sent paths in sync → server may return them as "match" or omit
            // them; an empty deltas list is the canonical "nothing to push".
            .with_body(r#"{"deltas":[]}"#)
            .expect(1)
            .create_async()
            .await;

        let (vr, _j, _jd) = make_vr(vault.path().to_path_buf(), &srv.url(), test_config()).await;
        let report = vr.run().await.unwrap();
        m.assert_async().await;
        assert_eq!(report.files_scanned, 2);
        assert_eq!(report.modify_count, 0);
        assert_eq!(report.add_count, 0);
        assert_eq!(report.delete_count, 0);
        assert_eq!(report.files_in_sync, 2);
    }

    #[tokio::test]
    async fn run_enqueues_pushes_for_drift_state() {
        let vault = TempDir::new().unwrap();
        write_file(vault.path(), "notes/a.md", b"alpha-local");

        let mut srv = Server::new_async().await;
        let _m = srv
            .mock("POST", "/api/sync/reconcile-batch")
            .with_status(200)
            // local differs from server → "drift" → enqueue a push.
            .with_body(
                r#"{"deltas":[{"path":"notes/a.md","state":"drift","server_hash":"deadbeef"}]}"#,
            )
            .create_async()
            .await;

        let (vr, journal, _jd) =
            make_vr(vault.path().to_path_buf(), &srv.url(), test_config()).await;
        let report = vr.run().await.unwrap();
        assert_eq!(report.modify_count, 1);
        assert_eq!(report.modify_paths_sample, vec!["notes/a.md"]);

        let j = journal.lock().await;
        assert_eq!(j.len(), 1);
        let batch = {
            // We hold the mutex via `j`; downgrade by dropping.
            drop(j);
            let mut j2 = journal.lock().await;
            j2.drain(10).unwrap()
        };
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].0.path, "notes/a.md");
        assert_eq!(batch[0].0.action, PushAction::Modify);
        // verify_repair enqueues a LAZY ref — content is read at drain time by
        // push_client, not embedded in the journal.
        assert_eq!(batch[0].0.content_bytes, None);
        // v0.4.11: a DRIFT push must carry the SERVER's current hash as the CAS
        // base (from the delta), NOT the local hash — else the server's
        // base_hash==current check fails and the push 409s ConflictUnrecoverable.
        assert_eq!(batch[0].0.base_hash.as_deref(), Some("deadbeef"));
    }

    #[tokio::test]
    async fn run_does_not_auto_delete_local_for_server_missing() {
        // A file the server has no row for comes back "missing-on-server":
        // we upload it, and we NEVER delete the local file. delete_count stays 0.
        let vault = TempDir::new().unwrap();
        let local_path = write_file(vault.path(), "notes/orphan.md", b"local-only");

        let mut srv = Server::new_async().await;
        let _m = srv
            .mock("POST", "/api/sync/reconcile-batch")
            .with_status(200)
            .with_body(r#"{"deltas":[{"path":"notes/orphan.md","state":"missing-on-server"}]}"#)
            .create_async()
            .await;

        let (vr, journal, _jd) =
            make_vr(vault.path().to_path_buf(), &srv.url(), test_config()).await;
        let report = vr.run().await.unwrap();
        // No delete concept — counted as a push, not a delete.
        assert_eq!(report.delete_count, 0);
        assert_eq!(report.modify_count, 1);
        assert_eq!(report.modify_paths_sample, vec!["notes/orphan.md"]);
        // File still on disk — verify_repair never deletes.
        assert!(local_path.exists());
        // The local-only file is pushed up.
        let batch = {
            let mut j = journal.lock().await;
            assert_eq!(j.len(), 1);
            j.drain(10).unwrap()
        };
        assert_eq!(batch.len(), 1);
        // v0.4.11: missing-on-server → base_hash None (sent as "") so the server
        // CREATEs the row (a non-empty base on a missing row would conflict).
        assert_eq!(batch[0].0.base_hash, None);
    }

    #[tokio::test]
    async fn run_match_state_is_noop_no_push() {
        // v0.4.10: reconcile-batch returns "match" for in-sync paths. A match
        // must enqueue nothing. (There is no "pull" outcome — reconcile-batch
        // only echoes paths we sent; server-only files are the SSE feed's job.)
        let vault = TempDir::new().unwrap();
        write_file(vault.path(), "notes/local.md", b"present");

        let mut srv = Server::new_async().await;
        let _m = srv
            .mock("POST", "/api/sync/reconcile-batch")
            .with_status(200)
            .with_body(
                r#"{"deltas":[{"path":"notes/local.md","state":"match","server_hash":"abc"}]}"#,
            )
            .create_async()
            .await;

        let (vr, journal, _jd) =
            make_vr(vault.path().to_path_buf(), &srv.url(), test_config()).await;
        let report = vr.run().await.unwrap();
        assert_eq!(report.modify_count, 0);
        assert_eq!(report.add_count, 0);
        assert_eq!(report.files_in_sync, 1);
        let j = journal.lock().await;
        assert_eq!(j.len(), 0);
    }

    #[tokio::test]
    async fn report_samples_first_50_paths() {
        let vault = TempDir::new().unwrap();
        // Write 60 local files; server reports all 60 as drift (need push).
        let mut diff_entries = Vec::new();
        for i in 0..60 {
            let rel = format!("notes/f{i:03}.md");
            write_file(vault.path(), &rel, format!("body-{i}").as_bytes());
            diff_entries.push(format!(
                r#"{{"path":"{rel}","state":"drift","server_hash":"h{i}"}}"#
            ));
        }
        let body = format!(r#"{{"deltas":[{}]}}"#, diff_entries.join(","));
        let mut srv = Server::new_async().await;
        let _m = srv
            .mock("POST", "/api/sync/reconcile-batch")
            .with_status(200)
            .with_body(body)
            .create_async()
            .await;
        let (vr, _j, _jd) = make_vr(vault.path().to_path_buf(), &srv.url(), test_config()).await;
        let report = vr.run().await.unwrap();
        assert_eq!(report.modify_count, 60);
        assert_eq!(report.modify_paths_sample.len(), SAMPLE_CAP);
    }

    #[tokio::test]
    async fn substrate_refused_count_populated() {
        let vault = TempDir::new().unwrap();
        let v = vault.path();
        write_file(v, "CLAUDE.md", b"x");
        write_file(v, "00_VAULT.md", b"x");
        write_file(v, "_rapport/groups/dev.md", b"x");
        write_file(v, "ok.md", b"x");
        let (vr, _j, _jd) = make_vr(v.to_path_buf(), "http://127.0.0.1:1", test_config()).await;
        let mut report = VerifyRepairReport::default();
        let _m = vr.build_local_manifest_with_report(&mut report).unwrap();
        assert_eq!(report.substrate_refused_count, 3);
    }

    #[tokio::test]
    async fn report_elapsed_ms_is_set() {
        let vault = TempDir::new().unwrap();
        write_file(vault.path(), "x.md", b"hi");
        let mut srv = Server::new_async().await;
        let _m = srv
            .mock("POST", "/api/sync/reconcile-batch")
            .with_status(200)
            .with_body(r#"{"deltas":[]}"#)
            .create_async()
            .await;
        let (vr, _j, _jd) = make_vr(vault.path().to_path_buf(), &srv.url(), test_config()).await;
        let report = vr.run().await.unwrap();
        // Lower-bound is 0 (zero-duration walks are possible on fast hardware
        // — we just confirm the field was populated by the code path, i.e. it
        // didn't remain Default::default after the explicit Instant arithmetic).
        // Use the indirect signal: `files_scanned` is 1 (not Default::default 0),
        // so we know `run()` reached the end. elapsed_ms is non-deterministic
        // upper-bound; just confirm it didn't underflow to a wildly large u64.
        assert_eq!(report.files_scanned, 1);
        assert!(report.elapsed_ms < 60_000);
    }

    #[tokio::test]
    async fn modify_for_path_not_in_local_manifest_is_reported_as_error() {
        // Defensive guard: a delta references a path we never sent (shouldn't
        // happen — reconcile-batch echoes only sent paths — but must not crash).
        let vault = TempDir::new().unwrap();
        write_file(vault.path(), "real.md", b"x");

        let mut srv = Server::new_async().await;
        let _m = srv
            .mock("POST", "/api/sync/reconcile-batch")
            .with_status(200)
            .with_body(r#"{"deltas":[{"path":"phantom.md","state":"missing-on-server"}]}"#)
            .create_async()
            .await;
        let (vr, _j, _jd) = make_vr(vault.path().to_path_buf(), &srv.url(), test_config()).await;
        let report = vr.run().await.unwrap();
        assert_eq!(report.modify_count, 1);
        assert!(!report.errors.is_empty());
        assert_eq!(report.errors[0].0, "phantom.md");
    }

    #[tokio::test]
    async fn reconcile_5xx_is_surfaced_as_error() {
        let vault = TempDir::new().unwrap();
        write_file(vault.path(), "a.md", b"x");
        let mut srv = Server::new_async().await;
        let _m = srv
            .mock("POST", "/api/sync/reconcile-batch")
            .with_status(500)
            .create_async()
            .await;
        let (vr, _j, _jd) = make_vr(vault.path().to_path_buf(), &srv.url(), test_config()).await;
        let r = vr.run().await;
        assert!(matches!(r, Err(VerifyRepairError::Api(_))));
    }

    // ─── helper-fn micro-tests ───────────────────────────────────────────

    #[test]
    fn ext_allowed_matches_canvas() {
        let allowed = vec![".md".to_string(), ".canvas".to_string()];
        assert!(ext_allowed("foo.canvas", &allowed));
        assert!(ext_allowed("foo.MD", &allowed));
        assert!(!ext_allowed("foo.png", &allowed));
    }

    #[test]
    fn segment_match_helpers() {
        assert!(starts_with_or_contains_segment(
            ".obsidian/workspace.json",
            ".obsidian/"
        ));
        assert!(starts_with_or_contains_segment(
            "nested/.trash/old.md",
            ".trash/"
        ));
        assert!(!starts_with_or_contains_segment(
            "obsidiana/x.md",
            ".obsidian/"
        ));
    }

    // ─── B4: per-sync_root verify_repair tests ────────────────────────────

    /// B4 core: VerifyRepair walks only its own vault_root — paths in the
    /// manifest are relative to that root, not to any global container.
    ///
    /// Two roots (root_a, root_b) each contain different files. A
    /// VerifyRepair rooted at root_a must return ONLY root_a's entries;
    /// one rooted at root_b must return ONLY root_b's entries.
    #[tokio::test]
    async fn verify_repair_manifest_rooted_at_passed_sync_root() {
        let root_a = TempDir::new().unwrap();
        let root_b = TempDir::new().unwrap();

        write_file(root_a.path(), "a_note.md", b"content-a");
        write_file(root_b.path(), "b_note.md", b"content-b");

        let (vr_a, _ja, _jda) = make_vr(
            root_a.path().to_path_buf(),
            "http://127.0.0.1:1",
            test_config(),
        )
        .await;
        let (vr_b, _jb, _jdb) = make_vr(
            root_b.path().to_path_buf(),
            "http://127.0.0.1:1",
            test_config(),
        )
        .await;

        let manifest_a = vr_a.build_local_manifest().unwrap();
        let manifest_b = vr_b.build_local_manifest().unwrap();

        // root_a manifest must contain only a_note.md, no b_note.md.
        let paths_a: Vec<&str> = manifest_a.iter().map(|e| e.path.as_str()).collect();
        assert_eq!(
            paths_a,
            vec!["a_note.md"],
            "root_a manifest wrong: {paths_a:?}"
        );

        // root_b manifest must contain only b_note.md, no a_note.md.
        let paths_b: Vec<&str> = manifest_b.iter().map(|e| e.path.as_str()).collect();
        assert_eq!(
            paths_b,
            vec!["b_note.md"],
            "root_b manifest wrong: {paths_b:?}"
        );
    }

    /// B4: `roots_to_reconcile_pairs` returns one entry per sync_root with
    /// the correct subscriber ID priority:
    ///   1. Root's own subscriber_id when non-empty.
    ///   2. Fallback subscriber_id when root's field is empty.
    #[test]
    fn roots_to_reconcile_pairs_two_roots_correct_subscriber_ids() {
        use super::roots_to_reconcile_pairs;
        use crate::config::SyncRoot;

        let roots = vec![
            SyncRoot {
                path: PathBuf::from("/vault/MainFrame"),
                route: String::new(),
                subscriber_id: "sub-own".to_string(), // explicit
            },
            SyncRoot {
                path: PathBuf::from("/vault/Dev"),
                route: "dev".to_string(),
                subscriber_id: String::new(), // empty → use fallback
            },
        ];

        let pairs = roots_to_reconcile_pairs(&roots, "sub-fallback");
        assert_eq!(pairs.len(), 2, "must produce one pair per sync_root");

        // First root has its own subscriber_id.
        assert_eq!(pairs[0].0, PathBuf::from("/vault/MainFrame"));
        assert_eq!(
            pairs[0].1, "sub-own",
            "first root must use its own subscriber_id"
        );

        // Second root falls back.
        assert_eq!(pairs[1].0, PathBuf::from("/vault/Dev"));
        assert_eq!(
            pairs[1].1, "sub-fallback",
            "second root (empty subscriber_id) must use fallback"
        );
    }

    /// B4: `roots_to_reconcile_pairs` returns an empty Vec for an empty
    /// sync_roots list (no roots configured → no reconciliation pairs).
    #[test]
    fn roots_to_reconcile_pairs_empty_roots_returns_empty() {
        use super::roots_to_reconcile_pairs;
        let pairs = roots_to_reconcile_pairs(&[], "sub-fallback");
        assert!(pairs.is_empty());
    }

    /// B4: single legacy root with empty subscriber_id takes the fallback.
    #[test]
    fn roots_to_reconcile_pairs_single_legacy_root_uses_fallback() {
        use super::roots_to_reconcile_pairs;
        use crate::config::SyncRoot;

        let roots = vec![SyncRoot {
            path: PathBuf::from("/vaults/Mainframe"),
            route: String::new(),
            subscriber_id: String::new(), // back-compat: empty → fallback
        }];
        let pairs = roots_to_reconcile_pairs(&roots, "sub-legacy-123");
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].1, "sub-legacy-123");
    }
}
