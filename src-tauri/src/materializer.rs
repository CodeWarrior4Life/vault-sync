//! Materializer — server→client downloads via atomic tmp+rename.
//!
//! v0.3 (Wave 3): promotes Live mode from a `NotYetImplemented` error to a
//! real atomic-write into the live vault tree.  Shadow mode now writes to
//! the per-host **workspace runtime** dir (`<workspace_root>/.lattice-runtime/
//! <slug>/shadow/<path>`) — NOT into the vault — per mandate §1 row 13.
//!
//! Every successful write is followed by an `IntegrityChecker::verify(...)`
//! pass (mandate §1 row 5 + T8).  Mismatches yield an
//! `MaterializeOutcome::IntegrityFailed`; the bad write is *not* deleted so
//! the owner can inspect.
//!
//! Before overwriting a live-mode target the materializer applies a
//! pull-side idempotency + conflict-stash hook mirroring `push_client`'s
//! frontmatter-normalized SHA check (mandate §1 row 4 + R16, §3 conflict
//! model).  Class-D paths (Credentials.md etc.) always stash regardless of
//! policy.
//!
//! Shadow mode preserves the v0.2 behavior with one path change: state
//! lives in the workspace runtime dir, not in `<vault>/.lattice-sync/`.

use crate::api_client::NotePayload;
use crate::conflict_stash::{ConflictClassifier, ConflictPolicy, ConflictStash, StashError};
use crate::integrity_check::{
    ByteLevelResult, ExpectedIntegrity, IntegrityChecker, IntegrityError, IntegrityResult,
};
use crate::rasp_fence::{classify_path, PathClassification};
use crate::scope::is_safe_path;
use crate::sync_shadow::canonical_sync_path;
use crate::tray_state::SharedTrayState;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs;
use std::io::Write;
// FileTimes::set_created (birthtime) is exposed via a platform-specific extension
// trait — macOS (setattrlist) and Windows. Linux has no std API for it.
#[cfg(target_os = "macos")]
use std::os::darwin::fs::FileTimesExt as _;
#[cfg(windows)]
use std::os::windows::fs::FileTimesExt as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use tempfile::NamedTempFile;
use thiserror::Error;
use tracing::{debug, info, warn};

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaterializerMode {
    Shadow,
    Live,
    Disabled,
}

impl MaterializerMode {
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Self {
        match s {
            "live" => Self::Live,
            "disabled" => Self::Disabled,
            _ => Self::Shadow,
        }
    }
}

#[derive(Debug, Error)]
pub enum MaterializerError {
    #[error("path traversal rejected: {0}")]
    PathTraversal(String),
    #[error("RASP substrate path refused (read-only by daemon): {0}")]
    SubstrateRefuse(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("sha mismatch: expected {expected}, got {actual}")]
    ShaMismatch { expected: String, actual: String },
    #[error("conflict-stash error: {0}")]
    Stash(#[from] StashError),
    #[error("integrity-check error: {0}")]
    Integrity(String),
}

impl From<IntegrityError> for MaterializerError {
    fn from(e: IntegrityError) -> Self {
        MaterializerError::Integrity(format!("{e:?}"))
    }
}

/// Why a write was skipped (no I/O happened beyond classification).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SkipReason {
    /// RASP substrate fence refused the path. `rule` is the static label of
    /// the matching rule, e.g. `"00_VAULT.md"` or `"_rapport/people/"`.
    SubstrateRefused { rule: &'static str },
    /// Local content already matches the server's canonical SHA after
    /// frontmatter normalization. No write needed.
    IdenticalToLocal,
    /// Materializer is configured in `Disabled` mode.
    DisabledMode,
    /// D2/R2 (S511, TKT-2dc9a17e): the local file diverges from the server,
    /// but the shadow store records the server hash as the LAST-SYNCED value,
    /// so only the LOCAL side moved since we synced. That is a genuine local
    /// user edit. We deliberately do NOT write the (older) server bytes over
    /// it; the file_watcher/push pipeline carries the edit UP. This is the
    /// exact case the daemon used to silently revert.
    LocalEditPreserved,
}

/// Outcome of a single `write()` call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MaterializeOutcome {
    /// File was written to disk at `path` (atomic tmp+rename succeeded AND
    /// post-write integrity check passed).
    Wrote { path: PathBuf },
    /// No write happened.  See `SkipReason`.
    Skipped(SkipReason),
    /// A local divergent revision was stashed before the canonical was
    /// written.  `stash_path` is the sibling stash file.  The canonical was
    /// also written to its final path.
    Stashed { stash_path: PathBuf },
    /// Write completed but the post-write integrity check failed.  The file
    /// is intentionally NOT deleted — the owner can inspect both the bad
    /// write and the resulting ticket.
    IntegrityFailed {
        path: PathBuf,
        expected_sha: String,
        actual_sha: String,
    },
}

// ---------------------------------------------------------------------------
// Materializer
// ---------------------------------------------------------------------------

/// Restore server-authoritative creation/modification times onto a freshly
/// materialized file. macOS: `FileTimes::set_created` writes the birthtime via
/// `setattrlist` — the timestamp Obsidian sorts "Created" by — so an atomic
/// tmp+rename (new inode, birthtime=now) no longer clobbers the note's true
/// created date. `set_modified` restores mtime. Best-effort by design: the file
/// is already byte-faithful, so a timestamp-set failure is logged, never fatal.
/// `created`/`file_mtime` are unix-timestamp floats from the server payload;
/// either may be absent (older server) — we set whatever we have.
fn restore_server_times(target: &Path, payload: &NotePayload) {
    let to_systime = |ts: Option<f64>| -> Option<std::time::SystemTime> {
        ts.and_then(|t| {
            (t > 0.0).then(|| std::time::UNIX_EPOCH + std::time::Duration::from_secs_f64(t))
        })
    };
    // mtime from file_mtime, falling back to created so set_times always has a base.
    let mtime = to_systime(payload.file_mtime).or_else(|| to_systime(payload.created));
    let Some(mtime) = mtime else {
        return;
    };
    #[allow(unused_mut)]
    let mut times = std::fs::FileTimes::new().set_modified(mtime);
    // Birthtime is settable only on macOS/Windows (FileTimesExt). On Linux the
    // ext4 birthtime is not writable via std; Linux clients are non-user-facing
    // for the "Created" sort, so mtime-only is sufficient there.
    #[cfg(any(target_os = "macos", windows))]
    if let Some(ctime) = to_systime(payload.created) {
        times = times.set_created(ctime);
    }
    match std::fs::File::options().write(true).open(target) {
        Ok(f) => {
            if let Err(e) = f.set_times(times) {
                warn!(path = %target.display(), error = %e, "restore_server_times: set_times failed");
            }
        }
        Err(e) => {
            warn!(path = %target.display(), error = %e, "restore_server_times: reopen failed");
        }
    }
}

/// Materializer config — opt-in feature flags.  Defaults align with
/// mandate §1 (integrity ON, ServerWins conflict default per §3).
#[derive(Debug, Clone)]
pub struct MaterializerConfig {
    /// Post-write integrity verification (mandate §1 row 5 + T8). Default ON.
    pub enable_integrity_check: bool,
    /// Pull-side conflict policy. Default `ServerWins` — silently overwrite
    /// non-class-D local divergent revisions.  Class D always stashes.
    pub conflict_policy: ConflictPolicy,
    /// Frontmatter fields stripped before computing the normalized
    /// idempotency SHA (mandate §1 row 10 / R16). Mirrors
    /// `PushClientConfig::strip_frontmatter_fields_for_diff` so push and
    /// pull use the same canonical-hash basis.
    pub strip_frontmatter_fields_for_diff: Vec<String>,
    /// Device identifier used when writing stash files
    /// (`<stem>.conflict-from-<device_id>-<lsn>.md`).
    pub device_id: String,
}

impl Default for MaterializerConfig {
    fn default() -> Self {
        Self {
            enable_integrity_check: true,
            conflict_policy: ConflictPolicy::ServerWins,
            strip_frontmatter_fields_for_diff: vec!["updated".into()],
            device_id: "unknown-device".to_string(),
        }
    }
}

/// v0.3.0 materializer.  Holds the runtime fields needed to write notes
/// into either live or shadow mode:
///
/// Note (S477): the daemon treats `vaults_root` as the actual watch +
/// materialize root. Incoming payloads carry the vault folder as the
/// first segment of their relative path, so live mode writes to
/// `<vaults_root>/<rel>` directly, allowing multiple vaults to coexist
/// under one `vaults_root`. The v0.2.0 `vault_name` field is gone as of
/// v0.3.7 — see config.rs for the legacy-tolerant load path.
///
/// - `workspace_root` — the per-host daemon state dir
///   (e.g. `%LocalAppData%\Nexus`). Shadow-mode writes go under
///   `<workspace_root>/.lattice-runtime/<subscriber_slug>/shadow/<path>`,
///   never into the vault tree.
/// - `subscriber_slug` — used to namespace the runtime dir (one host can
///   pair multiple subscribers without colliding).
/// - `config` — feature flags (integrity, conflict policy, ...).
pub struct Materializer {
    vaults_root: PathBuf,
    shadow_subdir: String,
    mode: MaterializerMode,
    workspace_root: PathBuf,
    subscriber_slug: String,
    config: MaterializerConfig,
    /// Optional tray telemetry sink (mandate §9 AG13 — Wave 4 wire-up). If
    /// set, integrity-check failures bump `tray.integrity_failures`, and
    /// `refresh_conflict_count_into_tray()` may be called by a background
    /// timer to refresh `tray.conflict_unresolved`.
    tray_state: Option<SharedTrayState>,
    /// Echo guard (S492): records the content hash of every file this
    /// materializer writes so the file_watcher can skip re-enqueueing the
    /// resulting filesystem event (a server echo, not a user edit). Shared
    /// `Arc` with the file_watcher; clones share the same guard.
    echo_guard: Option<Arc<crate::echo_guard::EchoGuard>>,
    /// Epoch-millis of the last `refresh_conflict_count_into_tray()` call.
    /// Wrapped in `Arc<AtomicI64>` so a cloned materializer (used by the
    /// 60s background refresh task in `lib::spawn_sse_consumer`) shares
    /// the debounce window with the primary write-path instance.
    last_conflict_refresh_ms: Arc<AtomicI64>,
    /// Persistent per-file shadow-hash store (fix/reconcile-server-wins-shadow).
    /// On every write where the local file now equals the server's canonical
    /// bytes, we record `path → payload.sha256` so the reconcile backstop can
    /// later tell a genuine local edit (push) from a stale materialization
    /// (pull). Optional + `Arc`-shared so a clone (reconcile, SSE consumer)
    /// shares one on-disk marker; `None` keeps pre-fix behavior (no recording).
    shadow_store: Option<Arc<crate::sync_shadow::ShadowStore>>,
    /// D2c (S511, TKT-2dc9a17e): per-path advisory lock registry. Serializes the
    /// `exists -> compare -> read-shadow -> stash -> persist` critical section
    /// for a SINGLE path so ~15 concurrent writers cannot lose a stash basis
    /// (read-old-bytes, both stash, one rename wins) or spawn N re-conflicting
    /// copies. Each distinct path gets its own `Mutex`; different paths proceed
    /// in parallel. `Arc`-shared so all clones (SSE consumer, reconcile pull,
    /// backfill) of one Materializer contend on the SAME lock per path. Coarse
    /// outer mutex only guards the small registry HashMap, never the I/O.
    path_locks: Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>,
}

impl Clone for Materializer {
    fn clone(&self) -> Self {
        Self {
            vaults_root: self.vaults_root.clone(),
            shadow_subdir: self.shadow_subdir.clone(),
            mode: self.mode,
            workspace_root: self.workspace_root.clone(),
            subscriber_slug: self.subscriber_slug.clone(),
            config: self.config.clone(),
            tray_state: self.tray_state.clone(),
            echo_guard: self.echo_guard.clone(),
            last_conflict_refresh_ms: self.last_conflict_refresh_ms.clone(),
            shadow_store: self.shadow_store.clone(),
            path_locks: self.path_locks.clone(),
        }
    }
}

/// Debounce window for `refresh_conflict_count_into_tray()` — skip a refresh
/// if the last one ran less than this many milliseconds ago.
const CONFLICT_REFRESH_DEBOUNCE_MS: i64 = 30_000;

impl Materializer {
    /// New v0.3 constructor.  See `MaterializerConfig::default` for the
    /// recommended defaults (integrity ON, ServerWins).
    pub fn new(
        vaults_root: PathBuf,
        shadow_path: Option<String>,
        mode: MaterializerMode,
        workspace_root: PathBuf,
        subscriber_slug: String,
        config: MaterializerConfig,
    ) -> Self {
        let shadow_subdir = shadow_path.unwrap_or_else(|| "shadow/".to_string());
        Self {
            vaults_root,
            shadow_subdir,
            mode,
            workspace_root,
            subscriber_slug,
            config,
            tray_state: None,
            echo_guard: None,
            last_conflict_refresh_ms: Arc::new(AtomicI64::new(0)),
            shadow_store: None,
            path_locks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Builder-style: attach the shared persistent
    /// [`ShadowStore`](crate::sync_shadow::ShadowStore). After this, every write
    /// that leaves the local file equal to the server's canonical bytes records
    /// `path → payload.sha256` so the reconcile backstop can resolve drift
    /// direction (push vs pull). Backwards-compatible — without it, no recording.
    pub fn with_shadow_store(mut self, store: Arc<crate::sync_shadow::ShadowStore>) -> Self {
        self.shadow_store = Some(store);
        self
    }

    /// Builder-style: attach the shared [`EchoGuard`](crate::echo_guard::EchoGuard)
    /// so every write records its content hash for file_watcher echo-suppression.
    /// Backwards-compatible — without it, `echo_guard = None` and no recording
    /// happens (pre-S492 behavior).
    pub fn with_echo_guard(mut self, guard: Arc<crate::echo_guard::EchoGuard>) -> Self {
        self.echo_guard = Some(guard);
        self
    }

    /// Builder-style: attach a `SharedTrayState`. After this, integrity-check
    /// failures bump `tray.integrity_failures`, and the caller may invoke
    /// `refresh_conflict_count_into_tray()` on a timer to refresh
    /// `tray.conflict_unresolved`. Backwards-compatible — pre-Wave-4
    /// constructors keep working with `tray_state = None`.
    pub fn with_tray_state(mut self, state: SharedTrayState) -> Self {
        self.tray_state = Some(state);
        self
    }

    /// Scan the live-vault tree for `*.conflict-from-*.md` stash siblings and
    /// publish the count to the tray (if a tray is attached). Debounced:
    /// returns early without scanning if a refresh ran less than
    /// `CONFLICT_REFRESH_DEBOUNCE_MS` ago. Caller-driven (mandate §4.1 — kept
    /// off the `write()` hot path).
    ///
    /// No-op when `tray_state` is None.
    pub fn refresh_conflict_count_into_tray(&self) {
        let Some(tray) = self.tray_state.as_ref() else {
            return;
        };

        // Debounce — skip if we ran recently.
        let now_ms = chrono::Utc::now().timestamp_millis();
        let last = self.last_conflict_refresh_ms.load(Ordering::Relaxed);
        if last != 0 && now_ms.saturating_sub(last) < CONFLICT_REFRESH_DEBOUNCE_MS {
            return;
        }
        self.last_conflict_refresh_ms
            .store(now_ms, Ordering::Relaxed);

        // Stash scan-root mirrors `write()`: live-mode uses the configured
        // vaults_root (which can contain multiple vaults — all scanned),
        // shadow-mode uses the shadow tree.
        let scan_root = match self.mode {
            MaterializerMode::Live => self.vaults_root.clone(),
            _ => self.shadow_root(),
        };
        let stasher = ConflictStash::new(scan_root, self.config.conflict_policy);
        match stasher.unresolved_count() {
            Ok(n) => {
                if let Ok(mut w) = tray.write() {
                    w.set_conflict_unresolved(n);
                }
            }
            Err(e) => {
                warn!(error = ?e, "refresh_conflict_count_into_tray: stash scan failed");
            }
        }
    }

    /// `<workspace_root>/.lattice-runtime/<subscriber_slug>/shadow/` — the
    /// per-subscriber shadow tree (mandate §1 row 13: daemon state OUT of
    /// vault).
    fn shadow_root(&self) -> PathBuf {
        // Allow callers to override the trailing folder name via
        // shadow_subdir, but anchor it under <workspace>/.lattice-runtime/<slug>.
        self.workspace_root
            .join(".lattice-runtime")
            .join(&self.subscriber_slug)
            .join(&self.shadow_subdir)
    }

    /// Target path for a payload, depending on mode. `rel` is expected to
    /// be relative to `vaults_root` (i.e. the vault folder is its first
    /// segment), so live mode joins straight onto `vaults_root` and
    /// shadow mode onto the per-subscriber shadow tree.
    fn target_for(&self, rel: &str) -> PathBuf {
        match self.mode {
            MaterializerMode::Live => self.vaults_root.join(rel),
            MaterializerMode::Shadow => self.shadow_root().join(rel),
            // Disabled: target unused, but provide a sensible placeholder.
            MaterializerMode::Disabled => self.shadow_root().join(rel),
        }
    }

    /// Convenience: live-vault path for a relative file (used by callers
    /// who need to compute the live target before write — e.g. tests).
    pub fn live_path_for(&self, rel: &str) -> PathBuf {
        self.vaults_root.join(rel)
    }

    /// Mode-aware on-disk target for a relative path — the exact location
    /// `write()` would materialize `rel` to. Public so the pull-backfill pass
    /// (R6) can test local presence (`target_path(rel).exists()`) WITHOUT
    /// fetching the note body: only genuinely-missing canonical notes are then
    /// fetched + written, keeping the full-enumeration backfill cheap.
    pub fn target_path(&self, rel: &str) -> PathBuf {
        self.target_for(rel)
    }

    /// Acquire (creating if needed) the per-path advisory lock for `key`.
    /// Returns an `Arc<Mutex<()>>` the caller locks across the
    /// exists -> compare -> stash -> persist critical section (D2c). The outer
    /// registry mutex is held only briefly to look up / insert the entry.
    fn path_lock_for(&self, key: &str) -> Arc<Mutex<()>> {
        let mut reg = self.path_locks.lock().unwrap_or_else(|p| p.into_inner());
        reg.entry(key.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    /// Public main entry — writes a payload into vault (live) or shadow tree.
    ///
    /// Equivalent to [`write_with_change_seq`](Self::write_with_change_seq)
    /// with `change_seq == 0`. The live SSE path threads the real server
    /// `change_seq` (from the SSE envelope lsn) so a conflict stash is named
    /// deterministically; callers without a change_seq (reconcile pull,
    /// pull-backfill) use this and get a `0`-suffixed stash name.
    pub fn write(&self, payload: &NotePayload) -> Result<MaterializeOutcome, MaterializerError> {
        self.write_with_change_seq(payload, 0)
    }

    /// D2 (S511, TKT-2dc9a17e): main write entry with the server `change_seq`
    /// threaded in. `change_seq` orders "newer" (NEVER filesystem mtime, which
    /// is arbitrary-writer-wins under clock skew + ~15 concurrent writers) and
    /// names any conflict stash deterministically so concurrent writers across
    /// the fleet converge on ONE stash filename instead of spawning N copies.
    pub fn write_with_change_seq(
        &self,
        payload: &NotePayload,
        change_seq: u64,
    ) -> Result<MaterializeOutcome, MaterializerError> {
        // 1. Mode gate.
        if matches!(self.mode, MaterializerMode::Disabled) {
            info!(
                "materializer_mode=disabled; skipping write for {}",
                payload.path
            );
            return Ok(MaterializeOutcome::Skipped(SkipReason::DisabledMode));
        }

        // 2. Path safety.
        if !is_safe_path(&payload.path) {
            return Err(MaterializerError::PathTraversal(payload.path.clone()));
        }

        // 3. RASP substrate fence — refuse with rule label.
        if let PathClassification::Substrate { rule } = classify_path(&payload.path) {
            warn!(
                rule = rule,
                path = %payload.path,
                "materializer refusing substrate path"
            );
            return Ok(MaterializeOutcome::Skipped(SkipReason::SubstrateRefused {
                rule,
            }));
        }

        // 4. Resolve canonical content + content_sha.
        //    BUG 2 (S486): the server's `sha256` is computed over the EXACT
        //    bytes it returns as `enriched_body` (server cache_writer hashes
        //    enriched_body; on a cache miss enriched_body == body_raw == the
        //    sha256 basis). Materialize those bytes verbatim so the strict
        //    integrity check passes by construction AND the note stays
        //    byte-faithful — re-serializing frontmatter through serde_yaml uses
        //    a different YAML rendering + `\n\n` separator and could never
        //    reproduce the original bytes, which failed integrity on every
        //    fronted note. Fall back to reconstruction only for older servers
        //    that don't send the field.
        let content = match &payload.enriched_body {
            Some(raw) => raw.clone(),
            None => serialize_with_frontmatter(payload),
        };
        let content_bytes = content.as_bytes();
        let actual_sha = hex::encode(Sha256::digest(content_bytes));

        // S492 echo-suppression: record what we are about to write so the
        // file_watcher skips the resulting filesystem event instead of
        // re-enqueuing it as a spurious local push (the SSE->materialize->
        // file_watcher->push feedback loop that flooded the journal). Recorded
        // here (after content+sha resolution, before the disk write) so the
        // entry is present when the watcher observes the write. Harmless on the
        // idempotent-skip paths below: the local file already equals this sha,
        // so suppressing a matching event is still correct.
        if let Some(g) = &self.echo_guard {
            g.record(&payload.path, &actual_sha);
        }

        // 5. Compute target.
        let target = self.target_for(&payload.path);

        // D2c (S511): acquire the per-path advisory lock and HOLD it across the
        // whole exists -> compare -> read-shadow -> stash -> persist sequence so
        // ~15 concurrent writers on the SAME path cannot lose a stash basis or
        // race the rename. Keyed by the NFC-canonical path so all clones agree.
        // Different paths take different locks and proceed in parallel. We tolerate
        // a poisoned lock (a prior panic) by taking the inner guard: the critical
        // section is idempotent + atomic, so proceeding is safe.
        let lock = self.path_lock_for(&canonical_sync_path(&payload.path));
        let _path_guard = lock.lock().unwrap_or_else(|p| p.into_inner());

        // 6. Unified decide() (S511 D2/D3): read the shadow store INSIDE write()
        // and resolve push-vs-pull-vs-conflict per R1-R5 instead of the old
        // policy-driven server-wins overwrite (which silently reverted genuine
        // local edits). `server` is the raw server-canonical hash; `shadow` is
        // the last-synced server hash for this path; local-vs-server is the
        // frontmatter-normalized comparison (R1 idempotency basis). The stash is
        // now DIVERGENCE-driven (always preserve the loser), never policy-driven.
        let mut stash_path: Option<PathBuf> = None;
        if target.exists() {
            let local_bytes = fs::read(&target)?;
            let local_raw_sha = hex::encode(Sha256::digest(&local_bytes));
            let local_eq_server = self.local_matches_canonical(&local_bytes, content_bytes);
            let shadow = self
                .shadow_store
                .as_ref()
                .and_then(|s| s.get(&payload.path));
            // shadow holds the last-synced server RAW sha; server is payload.sha256.
            let shadow_eq_server = shadow.as_deref() == Some(payload.sha256.as_str());
            // local untouched since last sync = its raw bytes still hash to the
            // last-synced server hash recorded in the shadow.
            let local_eq_shadow = shadow.as_deref() == Some(local_raw_sha.as_str());

            match decide(
                local_eq_server,
                shadow.is_some(),
                shadow_eq_server,
                local_eq_shadow,
            ) {
                Decision::Noop => {
                    info!(
                        path = %payload.path,
                        "materializer skip: local already identical to canonical (R1)"
                    );
                    // Record the synced server hash so the reconcile backstop
                    // sees this path as in-sync-with-server, not a stale-pull
                    // candidate.
                    if let Some(sh) = &self.shadow_store {
                        sh.record(&payload.path, &payload.sha256);
                    }
                    return Ok(MaterializeOutcome::Skipped(SkipReason::IdenticalToLocal));
                }
                Decision::PreserveLocalEdit => {
                    // R2: shadow == server (server has NOT moved since we synced)
                    // AND local diverges => a genuine LOCAL edit. NEVER overwrite
                    // it with the older server copy. Leave the file untouched so
                    // the file_watcher/push pipeline carries the edit UP. This is
                    // the exact silent-revert the operator hit (TKT-2dc9a17e).
                    warn!(
                        path = %payload.path,
                        change_seq,
                        "materializer R2: local edit diverges, server unchanged since last sync, PRESERVING local (will push up), NOT overwriting"
                    );
                    return Ok(MaterializeOutcome::Skipped(SkipReason::LocalEditPreserved));
                }
                Decision::PullClean => {
                    // R3: local == last-synced shadow, only the server moved.
                    // Clean pull, no stash needed (no unsynced local edit to lose).
                    debug!(
                        path = %payload.path,
                        "materializer R3: clean pull (local was at last-synced bytes, server advanced)"
                    );
                }
                Decision::Conflict => {
                    // R4 (both moved) / R5 (shadow absent, unknown provenance):
                    // ALWAYS-STASH-THEN-RESOLVE, regardless of Class or policy.
                    // Stash the LOSER (local bytes) FIRST, atomically, BEFORE any
                    // overwrite, so a crash mid-op never loses the loser; then the
                    // server winner is materialized below. (I-83 NEVER-SILENT-
                    // OVERWRITE.) The change_seq names the stash deterministically
                    // so N fleet writers converge on one filename.
                    let class = ConflictClassifier::classify(&payload.path);
                    let stash_root = match self.mode {
                        MaterializerMode::Live => self.vaults_root.clone(),
                        _ => self.shadow_root(),
                    };
                    let stasher = ConflictStash::new(stash_root, self.config.conflict_policy);
                    // Compute the stash path FIRST and record it in the echo_guard
                    // BEFORE writing it, so the file_watcher recognizes the stash
                    // write as an echo and never enqueues the conflict copy as a
                    // push (D5). The conflict copy is also excluded by name in the
                    // watcher, but recording here is belt-and-braces and keys the
                    // exact (path, sha) the watcher will observe.
                    let stash_target = stasher.compute_stash_path_public(
                        &payload.path,
                        &self.config.device_id,
                        change_seq,
                    );
                    if let (Some(g), Some(rel)) =
                        (&self.echo_guard, self.rel_for_stash(&stash_target))
                    {
                        g.record(&rel, &local_raw_sha);
                    }
                    let written = stasher.write_stash(
                        &payload.path,
                        &local_bytes,
                        &self.config.device_id,
                        change_seq,
                    )?;
                    warn!(
                        path = %payload.path,
                        stash = %written.display(),
                        class = ?class,
                        change_seq,
                        shadow_present = shadow.is_some(),
                        "materializer CONFLICT (R4/R5): stashed local divergent revision BEFORE overwrite, both byte-sets preserved"
                    );
                    stash_path = Some(written);
                }
            }
        } else if let Some(sh_hash) = self
            .shadow_store
            .as_ref()
            .and_then(|s| s.get(&payload.path))
        {
            // The target does not exist locally but we have a shadow record for
            // it: it was synced then deleted/moved away locally. This is benign
            // for an UPSERT (we are about to (re)create it from the server); no
            // stash is possible (no local bytes). Logged at debug only.
            debug!(
                path = %payload.path,
                shadow = %sh_hash,
                "materializer: target missing but shadow present, (re)creating from server"
            );
        }

        // 7. Path-safety + parent dir.
        let canonical_root = self.canonical_root_for_mode();
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent)?;
            let canonical_parent = parent
                .canonicalize()
                .unwrap_or_else(|_| parent.to_path_buf());
            if !canonical_parent.starts_with(&canonical_root) {
                return Err(MaterializerError::PathTraversal(payload.path.clone()));
            }
        }

        // 8. Atomic tmp+rename. Tmp file must be on the same FS as target,
        //    so we anchor it at target.parent() (same dir).
        let parent = target
            .parent()
            .expect("target has parent after create_dir_all");
        let mut tmp = NamedTempFile::new_in(parent)?;
        tmp.write_all(content_bytes)?;
        tmp.flush()?;
        // D12 (S511): on Windows, prefer ReplaceFileW (preserves the destination
        // file's ACLs/attributes and is atomic vs an open reader) over the bare
        // MoveFileExW that tempfile::persist uses, with bounded backoff retry on
        // ERROR_SHARING_VIOLATION (Obsidian holding the file) and \\?\ long-path
        // via dunce. On every other platform this is a plain tempfile::persist.
        atomic_persist(tmp, &target)?;

        // 8b. Restore server-authoritative timestamps. The atomic tmp+rename above
        // gives `target` a brand-new inode whose birthtime = now; macOS/Obsidian
        // read that as the note's "Created" date, so every re-materialization
        // reorders the operator's note list to "today" (the ctime-clobber incident,
        // 2026-06-05). Set birthtime from server `created` and mtime from
        // `file_mtime`. Best-effort: a timestamp-set failure must NOT fail the
        // (already byte-faithful) write.
        restore_server_times(&target, payload);

        // 9. Post-write integrity check.
        if self.config.enable_integrity_check {
            let expected = ExpectedIntegrity {
                sha256_hex: payload.sha256.clone(),
                size_bytes: content_bytes.len() as u64,
            };
            let checker = IntegrityChecker::new(false);
            let result: IntegrityResult = checker.verify(&target, &expected)?;
            if !result.is_ok() {
                let actual_hex = match &result.byte_level {
                    ByteLevelResult::ShaMismatch { actual_prefix, .. } => actual_prefix.clone(),
                    _ => actual_sha.clone(),
                };
                warn!(
                    expected = %payload.sha256,
                    actual = %actual_sha,
                    path = %target.display(),
                    "materializer integrity check FAILED — file kept on disk for inspection"
                );
                // Wave 4: surface the failure to the tray dashboard.
                if let Some(tray) = &self.tray_state {
                    if let Ok(mut w) = tray.write() {
                        w.inc_integrity_failures();
                    }
                }
                return Ok(MaterializeOutcome::IntegrityFailed {
                    path: target,
                    expected_sha: payload.sha256.clone(),
                    actual_sha: actual_hex,
                });
            }
        } else if actual_sha != payload.sha256 {
            // Legacy soft SHA check — log only, don't fail.
            warn!(
                expected = %payload.sha256,
                actual = %actual_sha,
                path = %payload.path,
                "materializer SHA mismatch (integrity-check disabled) — file written but does not match server hash"
            );
        }

        // The local file now equals the server's canonical bytes (we just wrote
        // them and integrity passed). Record the synced server hash for the
        // reconcile backstop's drift-direction decision. Reached only on the
        // Wrote / Stashed success paths — IntegrityFailed returned above, so a
        // failed write never records a (false) in-sync marker.
        if let Some(sh) = &self.shadow_store {
            sh.record(&payload.path, &payload.sha256);
        }

        if let Some(stash) = stash_path {
            Ok(MaterializeOutcome::Stashed { stash_path: stash })
        } else {
            Ok(MaterializeOutcome::Wrote { path: target })
        }
    }

    /// Pick the canonical-root directory for the active mode.  Used by the
    /// path-traversal sanity check.
    fn canonical_root_for_mode(&self) -> PathBuf {
        let raw_root = match self.mode {
            MaterializerMode::Live => self.vaults_root.clone(),
            _ => self.shadow_root(),
        };
        // Ensure the root exists so canonicalize() succeeds.
        let _ = fs::create_dir_all(&raw_root);
        raw_root.canonicalize().unwrap_or(raw_root)
    }

    /// True iff already-read `local_bytes` equal the incoming canonical bytes
    /// after frontmatter + CRLF/BOM normalization (R16 + D11). Pure: the caller
    /// reads the file once (inside the per-path lock) and hands the bytes in, so
    /// the compare and the subsequent stash share one consistent read.
    fn local_matches_canonical(&self, local_bytes: &[u8], canonical_bytes: &[u8]) -> bool {
        let local_norm =
            normalize_for_diff(local_bytes, &self.config.strip_frontmatter_fields_for_diff);
        let canonical_norm = normalize_for_diff(
            canonical_bytes,
            &self.config.strip_frontmatter_fields_for_diff,
        );
        local_norm == canonical_norm
    }

    /// D5 (S511): best-effort vaults-root-relative, forward-slash path of a
    /// stash target, for echo-guard keying. The echo guard is keyed by the
    /// same wire-path form the file_watcher normalizes to. Returns None if the
    /// stash path is not under the active root (it always is in practice).
    fn rel_for_stash(&self, stash_abs: &Path) -> Option<String> {
        let root = match self.mode {
            MaterializerMode::Live => &self.vaults_root,
            _ => return None, // shadow-tree stashes are never watched, so no echo to suppress
        };
        stash_abs
            .strip_prefix(root)
            .ok()
            .map(|r| r.to_string_lossy().replace('\\', "/"))
    }

    /// Soft-delete preserves the v0.2 contract (move to `<name>.deleted-<ts>`).
    /// In live mode it operates on the vault tree; in shadow mode on the
    /// runtime tree.  Disabled mode no-ops.
    pub fn soft_delete(&self, path: &str) -> Result<(), MaterializerError> {
        if !is_safe_path(path) {
            return Err(MaterializerError::PathTraversal(path.into()));
        }
        if let PathClassification::Substrate { rule: _ } = classify_path(path) {
            return Err(MaterializerError::SubstrateRefuse(path.into()));
        }
        if matches!(self.mode, MaterializerMode::Disabled) {
            info!("materializer disabled; skipping delete for {}", path);
            return Ok(());
        }
        let target = self.target_for(path);
        if !target.exists() {
            info!("soft_delete: nothing to delete at {}", path);
            return Ok(());
        }
        // D13 (S511): the suffix carries NANOSECOND precision, not just
        // second-granularity. Two deletes of the same path within one second
        // (a recreate/delete loop, or ext4 with multiple writers) previously
        // collided on a `.deleted-<YYYYMMDDTHHMMSSZ>` name and the second rename
        // clobbered the first preserved copy. Nanos make the name effectively
        // unique; a residual collision still falls through to a fresh inode
        // because we never overwrite an existing target (rename onto a distinct
        // name). No em-dashes in the format string (house rule).
        let now = chrono::Utc::now();
        let ts = now.format("%Y%m%dT%H%M%SZ");
        let nanos = now.timestamp_subsec_nanos();
        let mut renamed = target.with_file_name(format!(
            "{}.deleted-{ts}-{nanos:09}",
            target.file_name().unwrap().to_string_lossy()
        ));
        // Defensive: if that exact name somehow already exists, append a small
        // counter so we never clobber an earlier preserved deletion.
        if renamed.exists() {
            for n in 2u32..u32::MAX {
                let candidate = target.with_file_name(format!(
                    "{}.deleted-{ts}-{nanos:09}-{n}",
                    target.file_name().unwrap().to_string_lossy()
                ));
                if !candidate.exists() {
                    renamed = candidate;
                    break;
                }
            }
        }
        fs::rename(&target, &renamed)?;
        info!(from = %target.display(), to = %renamed.display(), "soft_delete done");
        Ok(())
    }
}

/// D2 (S511, TKT-2dc9a17e): the unified push-vs-pull-vs-conflict verdict for a
/// single path. Returned by [`decide`]; consumed by `Materializer::write`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    /// R1: local already equals the server canonical. Nothing to write.
    Noop,
    /// R2: shadow == server (server unchanged since last sync) AND local
    /// diverges. A genuine local edit. Preserve local, do NOT write, let push
    /// carry it up.
    PreserveLocalEdit,
    /// R3: local == last-synced shadow, only the server moved. Clean pull, no
    /// stash needed.
    PullClean,
    /// R4 (both moved) / R5 (shadow absent, unknown provenance): true
    /// concurrency. ALWAYS stash the local loser, then materialize the server
    /// winner.
    Conflict,
}

/// PURE decision function (table-tested) implementing the unified decide()
/// R1-R5 (S511 spec "Unified decide() algorithm"). All inputs are derived
/// relations so the function has no I/O and is exhaustively testable:
///
/// * `local_eq_server` - the (frontmatter+CRLF/BOM-normalized) local file
///   equals the server canonical (R1 idempotency basis).
/// * `shadow_present` - the shadow store has a last-synced hash for this path.
/// * `shadow_eq_server` - that last-synced hash equals the current server hash
///   (i.e. the server has NOT moved since we last synced).
/// * `local_eq_shadow` - the local file's raw bytes still hash to the
///   last-synced server hash (i.e. local has NOT been edited since last sync).
///
/// Ordering of "newer" is by server `change_seq` (handled by the caller naming
/// the stash); this function never consults filesystem mtime.
pub fn decide(
    local_eq_server: bool,
    shadow_present: bool,
    shadow_eq_server: bool,
    local_eq_shadow: bool,
) -> Decision {
    // R1: idempotent. Local already equals server, regardless of shadow.
    if local_eq_server {
        return Decision::Noop;
    }
    // R5: shadow absent and local diverges from server. Unknown provenance,
    // NEVER assume server wins. Treat as concurrent => conflict (stash).
    if !shadow_present {
        return Decision::Conflict;
    }
    // R2: server unchanged since last sync AND local moved => genuine local
    // edit. Must propagate UP, never be overwritten.
    if shadow_eq_server {
        return Decision::PreserveLocalEdit;
    }
    // R3: local untouched since last sync, only server moved => clean pull.
    if local_eq_shadow {
        return Decision::PullClean;
    }
    // R4: shadow present, server moved, AND local moved too => both diverged
    // from the last-synced base => true conflict (stash the local loser).
    Decision::Conflict
}

// ---------------------------------------------------------------------------
// Frontmatter normalization (mirrors push_client::normalize_for_diff exactly)
// ---------------------------------------------------------------------------

fn normalize_for_diff(content: &[u8], strip_fields: &[String]) -> Vec<u8> {
    let raw = match std::str::from_utf8(content) {
        Ok(s) => s,
        Err(_) => return content.to_vec(),
    };
    // D11 (S511, TKT-2dc9a17e): CRLF/BOM normalization is part of the
    // conflict-detection basis. A note edited on Windows (CRLF, and sometimes a
    // UTF-8 BOM) versus the same logical content on a Unix host (LF) must NOT be
    // a permanent false-conflict. We fold CRLF -> LF and strip a leading BOM
    // BEFORE the frontmatter/idempotency hashing, on BOTH the frontmatter and
    // the no-frontmatter passthrough paths. This changes only the DIFF BASIS,
    // never the bytes written to disk (the materializer always persists the
    // server's exact enriched_body verbatim).
    let no_bom = raw.strip_prefix('\u{feff}').unwrap_or(raw);
    let normalized_eol: String = if no_bom.contains('\r') {
        no_bom.replace("\r\n", "\n").replace('\r', "\n")
    } else {
        no_bom.to_string()
    };
    let s = normalized_eol.as_str();

    if !s.starts_with("---\n") {
        // No (or non-leading) frontmatter: the EOL/BOM-normalized body IS the
        // diff basis.
        return s.as_bytes().to_vec();
    }
    let body_start = match find_frontmatter_end(s) {
        Some(i) => i,
        None => return s.as_bytes().to_vec(),
    };
    // EOL already normalized to LF above, so the opening fence is always 4 bytes.
    let after_open = 4;
    let fm_block = &s[after_open..body_start.fm_inner_end];
    let body = &s[body_start.body_start..];

    let stripped_fm = strip_yaml_fields(fm_block, strip_fields);
    let mut out = String::with_capacity(s.len());
    out.push_str("---\n");
    out.push_str(&stripped_fm);
    if !stripped_fm.ends_with('\n') {
        out.push('\n');
    }
    out.push_str("---\n");
    out.push_str(body);
    out.into_bytes()
}

struct FrontmatterEnd {
    fm_inner_end: usize,
    body_start: usize,
}

fn find_frontmatter_end(s: &str) -> Option<FrontmatterEnd> {
    let after_open = if s.starts_with("---\r\n") { 5 } else { 4 };
    let mut cursor = after_open;
    let bytes = s.as_bytes();
    while cursor < bytes.len() {
        let line_end = match bytes[cursor..].iter().position(|&b| b == b'\n') {
            Some(p) => cursor + p,
            None => return None,
        };
        let mut line = &s[cursor..line_end];
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

fn strip_yaml_fields(fm_block: &str, fields: &[String]) -> String {
    if fields.is_empty() {
        return fm_block.to_string();
    }
    let mut out = String::with_capacity(fm_block.len());
    let mut skipping = false;
    for line in fm_block.lines() {
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
            continue;
        }
        out.push_str(line);
        out.push('\n');
    }
    out
}

fn serialize_with_frontmatter(payload: &NotePayload) -> String {
    // S476 v0.3.5: omit the `---\n...\n---\n` block when frontmatter is
    // missing or empty. Before this fix every shadow file got a useless
    // `---\n{}\n---\n` preamble (the server returns `frontmatter: {}` for
    // notes without YAML front-matter, and serde_yaml renders that as
    // `{}\n` -> wrapped in fences it became junk-frontmatter noise at the
    // top of every file).
    let is_empty = match &payload.frontmatter {
        serde_json::Value::Null => true,
        serde_json::Value::Object(m) => m.is_empty(),
        _ => false,
    };
    if is_empty {
        return payload.body.clone();
    }
    let fm_yaml = serde_yaml::to_string(&payload.frontmatter).unwrap_or_default();
    format!("---\n{fm_yaml}---\n\n{}", payload.body)
}

// ---------------------------------------------------------------------------
// Atomic persist (D12: Windows-aware)
// ---------------------------------------------------------------------------

/// D12 (S511): atomically move a finished temp file onto `target`.
///
/// Non-Windows: a plain `tempfile::persist` (= `rename(2)`), the prior behavior.
///
/// Windows: prefer `ReplaceFileW` (preserves the destination's ACLs/attributes
/// and is atomic against an open reader, unlike `MoveFileExW` which
/// `tempfile::persist` uses), with bounded backoff retry on
/// `ERROR_SHARING_VIOLATION` (Obsidian momentarily holding the file). Falls back
/// to `MoveFileExW`-style persist when the destination does not yet exist
/// (ReplaceFileW requires an existing target). Long paths are prefixed with
/// `\\?\` via `dunce::simplified`'s inverse (we canonicalize through dunce on the
/// target before the OS call). Code path is compiled only on Windows and must be
/// re-verified on a booted Neo before Windows sync is re-enabled.
#[cfg(not(windows))]
fn atomic_persist(tmp: NamedTempFile, target: &Path) -> Result<(), MaterializerError> {
    tmp.persist(target).map_err(|e| e.error)?;
    Ok(())
}

#[cfg(windows)]
fn atomic_persist(tmp: NamedTempFile, target: &Path) -> Result<(), MaterializerError> {
    use std::os::windows::ffi::OsStrExt;
    use std::thread::sleep;
    use std::time::Duration;

    // ERROR_SHARING_VIOLATION = 32. Bounded backoff: a few short retries while
    // Obsidian releases the handle, then surface a real error (NEVER a silent
    // .tmp orphan).
    const ERROR_SHARING_VIOLATION: i32 = 32;
    const MAX_ATTEMPTS: usize = 6;
    const BASE_BACKOFF_MS: u64 = 25;

    // Helper: widen an OS path to a NUL-terminated UTF-16 buffer for the W APIs.
    fn wide(p: &Path) -> Vec<u16> {
        p.as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect()
    }

    // Prefer the \\?\-prefixed long path form on the destination so >260-char
    // notes do not fail on a host without LongPathsEnabled. dunce::canonicalize
    // gives a clean path; if the parent cannot be canonicalized (target does not
    // exist yet) we fall back to the raw target.
    let dest_long: PathBuf = dunce::canonicalize(target.parent().unwrap_or(target))
        .ok()
        .and_then(|p| target.file_name().map(|f| p.join(f)))
        .unwrap_or_else(|| target.to_path_buf());

    // tempfile keeps the temp file; we need its path. Persist via ReplaceFileW
    // when the destination already exists, else a direct persist (create).
    if dest_long.exists() {
        // ReplaceFileW(target, source, NULL, REPLACEFILE_IGNORE_MERGE_ERRORS, ..)
        // is declared inline to avoid pulling in the full windows crate; this
        // mirrors the FFI the std library uses internally.
        #[link(name = "kernel32")]
        extern "system" {
            fn ReplaceFileW(
                lpReplacedFileName: *const u16,
                lpReplacementFileName: *const u16,
                lpBackupFileName: *const u16,
                dwReplaceFlags: u32,
                lpExclude: *mut core::ffi::c_void,
                lpReserved: *mut core::ffi::c_void,
            ) -> i32;
        }
        const REPLACEFILE_IGNORE_MERGE_ERRORS: u32 = 0x0000_0002;

        // Keep the temp file on disk under a stable path for the FFI call.
        let (_file, tmp_path) = tmp.keep().map_err(|e| MaterializerError::Io(e.error))?;
        let replaced = wide(&dest_long);
        let replacement = wide(&tmp_path);

        let mut last_err: Option<std::io::Error> = None;
        for attempt in 0..MAX_ATTEMPTS {
            let ok = unsafe {
                ReplaceFileW(
                    replaced.as_ptr(),
                    replacement.as_ptr(),
                    std::ptr::null(),
                    REPLACEFILE_IGNORE_MERGE_ERRORS,
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                )
            };
            if ok != 0 {
                return Ok(());
            }
            let err = std::io::Error::last_os_error();
            let retryable = err.raw_os_error() == Some(ERROR_SHARING_VIOLATION);
            last_err = Some(err);
            if retryable {
                sleep(Duration::from_millis(BASE_BACKOFF_MS << attempt.min(5)));
                continue;
            }
            break;
        }
        // ReplaceFileW failed for a non-retryable reason (or exhausted retries):
        // best-effort fall back to a plain rename so we still converge, but never
        // leave the temp orphaned silently.
        warn!(
            target = %dest_long.display(),
            error = ?last_err,
            "atomic_persist: ReplaceFileW failed, falling back to rename"
        );
        std::fs::rename(&tmp_path, &dest_long).map_err(MaterializerError::Io)?;
        Ok(())
    } else {
        // Destination does not exist yet: a plain persist (create) with bounded
        // sharing-violation backoff.
        let mut tmp = tmp;
        for attempt in 0..MAX_ATTEMPTS {
            match tmp.persist(&dest_long) {
                Ok(_) => return Ok(()),
                Err(e) => {
                    if e.error.raw_os_error() == Some(ERROR_SHARING_VIOLATION)
                        && attempt + 1 < MAX_ATTEMPTS
                    {
                        tmp = e.file;
                        sleep(Duration::from_millis(BASE_BACKOFF_MS << attempt.min(5)));
                        continue;
                    }
                    return Err(MaterializerError::Io(e.error));
                }
            }
        }
        unreachable!("persist loop returns inside the body")
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    const VAULT: &str = "Mainframe";
    const SLUG: &str = "subscriber-test";

    /// (vaults_root_tmp, workspace_tmp, materializer)
    fn mk(mode: MaterializerMode, cfg: MaterializerConfig) -> (TempDir, TempDir, Materializer) {
        let vaults_tmp = TempDir::new().unwrap();
        let ws_tmp = TempDir::new().unwrap();
        std::fs::create_dir_all(vaults_tmp.path().join(VAULT)).unwrap();
        let m = Materializer::new(
            vaults_tmp.path().to_path_buf(),
            Some("shadow/".to_string()),
            mode,
            ws_tmp.path().to_path_buf(),
            SLUG.to_string(),
            cfg,
        );
        (vaults_tmp, ws_tmp, m)
    }

    fn default_cfg() -> MaterializerConfig {
        MaterializerConfig {
            device_id: "morpheus".into(),
            ..Default::default()
        }
    }

    fn sha256_hex(s: &str) -> String {
        hex::encode(Sha256::digest(s.as_bytes()))
    }

    /// Test helper: builds a NotePayload with the path namespaced under
    /// the test VAULT folder. Per S477, NotePayload.path is relative to
    /// `vaults_root`, so the vault folder is the first segment. Callers
    /// keep passing intra-vault relatives ("01_Inbox/foo.md") and this
    /// helper prepends VAULT exactly once. Paths starting with "../"
    /// (traversal-attempt tests) are passed through unmodified so the
    /// path-safety check sees the raw escape attempt.
    fn payload(path: &str, body: &str) -> NotePayload {
        let prefixed = if path.starts_with("../") || path.starts_with(&format!("{VAULT}/")) {
            path.to_string()
        } else {
            format!("{VAULT}/{path}")
        };
        let fm = serde_json::json!({"title": "Test", "tags": ["a", "b"]});
        let fm_yaml = serde_yaml::to_string(&fm).unwrap_or_default();
        let serialized = format!("---\n{fm_yaml}---\n\n{body}");
        NotePayload {
            path: prefixed,
            frontmatter: fm,
            body: body.into(),
            sha256: sha256_hex(&serialized),
            modified: "2026-05-27T00:00:00Z".into(),
            file_mtime: None,
            created: None,
            // Mirror the real server: enriched_body is the exact content the
            // sha256 is computed over (S486).
            enriched_body: Some(serialized),
        }
    }

    fn payload_with_bad_sha(path: &str, body: &str) -> NotePayload {
        let mut p = payload(path, body);
        p.sha256 = "0".repeat(64);
        p
    }

    // ---- BUG 2 (S486): pull-path integrity over enriched_body -------------

    /// Real-server shape: `sha256` is computed over the EXACT bytes the server
    /// returns as `enriched_body` (server cache_writer hashes enriched_body;
    /// cache-miss path sets enriched_body == body_raw == the sha256 basis).
    /// The daemon must materialize `enriched_body` verbatim — NOT a serde_yaml
    /// reconstruction, which uses different frontmatter serialization + a
    /// `\n\n` separator and could never byte-match, so the strict integrity
    /// check failed on every fronted note (S485 e2e blocker). With the field
    /// present and integrity ENABLED, the write must succeed and reproduce the
    /// server bytes exactly.
    #[test]
    fn pull_path_materializes_server_enriched_body_verbatim_integrity_ok() {
        let mut cfg = default_cfg();
        cfg.enable_integrity_check = true;
        let (vaults, _ws, m) = mk(MaterializerMode::Live, cfg);

        // The server's faithful bytes use a SINGLE-newline frontmatter
        // separator; serde_yaml reconstruction emits `---\n{yaml}---\n\n{body}`
        // (double newline) — guaranteeing the two differ.
        let original = "---\ntitle: Real\n---\nSingle-newline body, server-faithful.\n";
        let p = NotePayload {
            path: format!("{VAULT}/01_Inbox/faithful.md"),
            frontmatter: serde_json::json!({"title": "Real"}),
            body: "Single-newline body, server-faithful.\n".into(),
            sha256: sha256_hex(original),
            modified: "2026-05-31T00:00:00Z".into(),
            file_mtime: None,
            created: None,
            enriched_body: Some(original.to_string()),
        };

        // Guard: if reconstruction happened to equal the server bytes this
        // test wouldn't exercise the bug.
        assert_ne!(
            serialize_with_frontmatter(&p),
            original,
            "reconstruction must differ from server bytes for this regression to be meaningful"
        );

        let out = m.write(&p).unwrap();
        assert!(
            matches!(out, MaterializeOutcome::Wrote { .. }),
            "strict integrity must PASS by materializing enriched_body verbatim, got {out:?}"
        );
        let on_disk =
            std::fs::read_to_string(vaults.path().join(VAULT).join("01_Inbox/faithful.md"))
                .unwrap();
        assert_eq!(
            on_disk, original,
            "must write the server's exact hashed bytes (byte-faithful)"
        );
    }

    /// Back-compat: an older server that omits `enriched_body` (field defaults
    /// to None) still materializes via frontmatter reconstruction.
    #[test]
    fn pull_path_falls_back_to_reconstruction_when_enriched_body_absent() {
        let (vaults, _ws, m) = mk(MaterializerMode::Live, default_cfg());
        let fm = serde_json::json!({"title": "Legacy"});
        let fm_yaml = serde_yaml::to_string(&fm).unwrap();
        let serialized = format!("---\n{fm_yaml}---\n\nlegacy body");
        let p = NotePayload {
            path: format!("{VAULT}/01_Inbox/legacy.md"),
            frontmatter: fm,
            body: "legacy body".into(),
            sha256: sha256_hex(&serialized),
            modified: "2026-05-31T00:00:00Z".into(),
            file_mtime: None,
            created: None,
            enriched_body: None,
        };
        let out = m.write(&p).unwrap();
        assert!(
            matches!(out, MaterializeOutcome::Wrote { .. }),
            "got {out:?}"
        );
        let on_disk =
            std::fs::read_to_string(vaults.path().join(VAULT).join("01_Inbox/legacy.md")).unwrap();
        assert_eq!(on_disk, serialized);
    }

    // ---- mode-routing -----------------------------------------------------

    #[test]
    fn live_mode_writes_to_vault_path_not_shadow() {
        let (vaults, ws, m) = mk(MaterializerMode::Live, default_cfg());
        let out = m.write(&payload("01_Inbox/foo.md", "hello")).unwrap();
        let expected = vaults.path().join(VAULT).join("01_Inbox/foo.md");
        match out {
            MaterializeOutcome::Wrote { path } => assert_eq!(path, expected),
            other => panic!("expected Wrote, got {other:?}"),
        }
        assert!(expected.exists());
        let shadow_target = ws
            .path()
            .join(".lattice-runtime")
            .join(SLUG)
            .join("shadow/01_Inbox/foo.md");
        assert!(!shadow_target.exists());
    }

    #[test]
    fn shadow_mode_writes_to_workspace_runtime_not_vault() {
        let (vaults, ws, m) = mk(MaterializerMode::Shadow, default_cfg());
        let out = m.write(&payload("01_Inbox/foo.md", "hello")).unwrap();
        // S477: payload paths now include the vault folder as the first
        // segment, so the shadow tree mirrors that prefix.
        let expected = ws
            .path()
            .join(".lattice-runtime")
            .join(SLUG)
            .join("shadow")
            .join(VAULT)
            .join("01_Inbox/foo.md");
        match out {
            MaterializeOutcome::Wrote { path } => assert_eq!(path, expected),
            other => panic!("expected Wrote, got {other:?}"),
        }
        assert!(expected.exists());
        let vault_target = vaults.path().join(VAULT).join("01_Inbox/foo.md");
        assert!(!vault_target.exists());
    }

    #[test]
    fn shadow_mode_path_outside_vault() {
        let (vaults, _ws, m) = mk(MaterializerMode::Shadow, default_cfg());
        m.write(&payload("01_Inbox/foo.md", "x")).unwrap();
        let shadow_root_canonical = m.shadow_root().canonicalize().unwrap();
        let vault_root_canonical = vaults.path().join(VAULT).canonicalize().unwrap();
        assert!(
            !shadow_root_canonical.starts_with(&vault_root_canonical),
            "shadow={} should not be inside vault={}",
            shadow_root_canonical.display(),
            vault_root_canonical.display()
        );
    }

    #[test]
    fn disabled_mode_writes_nothing_returns_skipped() {
        let (vaults, ws, m) = mk(MaterializerMode::Disabled, default_cfg());
        let out = m.write(&payload("01_Inbox/foo.md", "x")).unwrap();
        assert_eq!(out, MaterializeOutcome::Skipped(SkipReason::DisabledMode));
        assert!(!vaults.path().join(VAULT).join("01_Inbox/foo.md").exists());
        assert!(!ws
            .path()
            .join(".lattice-runtime")
            .join(SLUG)
            .join("shadow/01_Inbox/foo.md")
            .exists());
    }

    // ---- substrate refusal -----------------------------------------------

    #[test]
    fn substrate_refusal_returns_skipped_with_rule_label() {
        let (_v, _w, m) = mk(MaterializerMode::Live, default_cfg());
        let out = m.write(&payload("00_VAULT.md", "x")).unwrap();
        match out {
            MaterializeOutcome::Skipped(SkipReason::SubstrateRefused { rule }) => {
                assert_eq!(rule, "00_VAULT.md");
            }
            other => panic!("expected SubstrateRefused, got {other:?}"),
        }
    }

    #[test]
    fn substrate_refusal_protocols_returns_rule() {
        let (_v, _w, m) = mk(MaterializerMode::Live, default_cfg());
        let out = m
            .write(&payload("02_Projects/Protocols/foo.md", "x"))
            .unwrap();
        match out {
            MaterializeOutcome::Skipped(SkipReason::SubstrateRefused { rule }) => {
                assert_eq!(rule, "02_Projects/Protocols/");
            }
            other => panic!("expected SubstrateRefused, got {other:?}"),
        }
    }

    // ---- idempotency + frontmatter normalization -------------------------

    #[test]
    fn identical_local_skips_no_write() {
        let (vaults, _ws, m) = mk(MaterializerMode::Live, default_cfg());
        let p = payload("01_Inbox/foo.md", "hello");
        m.write(&p).unwrap();
        let target = vaults.path().join(VAULT).join("01_Inbox/foo.md");
        let mtime_before = std::fs::metadata(&target).unwrap().modified().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(50));
        let out = m.write(&p).unwrap();
        assert_eq!(
            out,
            MaterializeOutcome::Skipped(SkipReason::IdenticalToLocal)
        );
        let mtime_after = std::fs::metadata(&target).unwrap().modified().unwrap();
        assert_eq!(
            mtime_before, mtime_after,
            "mtime should not advance on skip"
        );
    }

    /// Regression (2026-06-05 ctime-clobber): a fresh materialize must restore the
    /// file's birthtime from server `created` (Obsidian "Created" sort) and mtime
    /// from `file_mtime`, NOT leave them at "now". macOS-only: birthtime is the
    /// platform timestamp Obsidian reads, and the one set_created writes.
    #[test]
    #[cfg(target_os = "macos")]
    fn restores_birthtime_and_mtime_from_payload() {
        use std::time::{Duration, UNIX_EPOCH};
        let (vaults, _ws, m) = mk(MaterializerMode::Live, default_cfg());
        let created_ts = 1_577_882_096.0_f64; // 2020-01-01
        let mtime_ts = 1_704_067_200.0_f64; // 2024-01-01
        let mut p = payload("01_Inbox/ts.md", "hello");
        p.created = Some(created_ts);
        p.file_mtime = Some(mtime_ts);
        m.write(&p).unwrap();
        let md = std::fs::metadata(vaults.path().join(VAULT).join("01_Inbox/ts.md")).unwrap();
        let near = |a: std::time::SystemTime, want: f64| {
            let b = UNIX_EPOCH + Duration::from_secs_f64(want);
            let d = a
                .duration_since(b)
                .or_else(|_| b.duration_since(a))
                .unwrap();
            d < Duration::from_secs(2)
        };
        assert!(
            near(md.modified().unwrap(), mtime_ts),
            "mtime not restored from file_mtime"
        );
        assert!(
            near(md.created().unwrap(), created_ts),
            "birthtime not restored from created"
        );
    }

    #[test]
    fn frontmatter_only_rewrite_treated_as_identical() {
        let (vaults, _ws, m) = mk(MaterializerMode::Live, default_cfg());
        let target = vaults.path().join(VAULT).join("01_Inbox/n.md");
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();
        // Local file: same key set as canonical except `updated: 2026-05-01`.
        // To make this test order-stable across serde_yaml versions, build
        // the local pre-existing file using the SAME serializer the
        // materializer will use for the canonical payload (just with the
        // older `updated` value). The normalize_for_diff strip will remove
        // `updated:` from both before hashing, leaving identical content.
        let local_fm =
            serde_json::json!({"title": "Test", "updated": "2026-05-01", "tags": ["a", "b"]});
        let local_fm_yaml = serde_yaml::to_string(&local_fm).unwrap();
        let local_content = format!("---\n{local_fm_yaml}---\n\nbody-text");
        std::fs::write(&target, local_content).unwrap();

        // Canonical from server: same fields, newer `updated:`.
        let fm = serde_json::json!({"title": "Test", "updated": "2026-05-27", "tags": ["a", "b"]});
        let fm_yaml = serde_yaml::to_string(&fm).unwrap();
        let serialized = format!("---\n{fm_yaml}---\n\nbody-text");
        let p = NotePayload {
            // S477: payload path is vaults-root-relative (vault folder first).
            path: format!("{VAULT}/01_Inbox/n.md"),
            frontmatter: fm,
            body: "body-text".into(),
            sha256: sha256_hex(&serialized),
            modified: "2026-05-27T00:00:00Z".into(),
            file_mtime: None,
            created: None,
            enriched_body: Some(serialized),
        };
        let out = m.write(&p).unwrap();
        assert_eq!(
            out,
            MaterializeOutcome::Skipped(SkipReason::IdenticalToLocal)
        );
    }

    // ---- conflict stash ---------------------------------------------------

    #[test]
    fn stash_written_for_conflict_class_d() {
        let (vaults, _ws, m) = mk(MaterializerMode::Live, default_cfg());
        let target = vaults.path().join(VAULT).join("02_Projects/Credentials.md");
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();
        std::fs::write(&target, "local-secrets-version").unwrap();
        let p = payload("02_Projects/Credentials.md", "server-canonical-secrets");
        let out = m.write(&p).unwrap();
        match out {
            MaterializeOutcome::Stashed { stash_path } => {
                assert!(stash_path.exists(), "stash file should exist");
                let stash_content = std::fs::read_to_string(&stash_path).unwrap();
                assert_eq!(stash_content, "local-secrets-version");
                let cur = std::fs::read_to_string(&target).unwrap();
                assert!(cur.contains("server-canonical-secrets"));
            }
            other => panic!("expected Stashed, got {other:?}"),
        }
    }

    /// S511 D2/D3 (TKT-2dc9a17e): a Class-C local divergence with NO shadow
    /// record (shadow absent => R5, unknown provenance) now ALWAYS stashes the
    /// local loser before materializing the server winner. This flips the
    /// pre-S511 `stash_not_written_for_class_c_under_server_wins` assertion:
    /// there is no longer any silent-overwrite cell for divergent content. Both
    /// byte-sets must survive on disk (I-83 NEVER-SILENT-OVERWRITE).
    #[test]
    fn class_c_divergence_no_shadow_now_stashes_r5() {
        let (vaults, _ws, m) = mk(MaterializerMode::Live, default_cfg());
        let target = vaults.path().join(VAULT).join("02_Projects/Foo/normal.md");
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();
        std::fs::write(&target, "old-local-divergent").unwrap();
        let p = payload("02_Projects/Foo/normal.md", "server-canonical");
        let out = m.write(&p).unwrap();
        match out {
            MaterializeOutcome::Stashed { stash_path } => {
                // Loser (local) preserved verbatim in the stash.
                assert!(stash_path.exists(), "stash file must exist");
                let stash_content = std::fs::read_to_string(&stash_path).unwrap();
                assert_eq!(stash_content, "old-local-divergent");
                // Winner (server) materialized at the canonical path.
                let cur = std::fs::read_to_string(&target).unwrap();
                assert!(cur.contains("server-canonical"));
            }
            other => panic!("expected Stashed (R5 always-stash), got {other:?}"),
        }
        // Exactly one conflict-from sibling was written.
        let dir = target.parent().unwrap();
        let conflict_copies: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.contains(".conflict-from-"))
            .collect();
        assert_eq!(
            conflict_copies.len(),
            1,
            "exactly one conflict copy expected, got {conflict_copies:?}"
        );
    }

    // ---- integrity check --------------------------------------------------

    #[test]
    fn integrity_check_failure_yields_outcome() {
        let (vaults, _ws, m) = mk(MaterializerMode::Live, default_cfg());
        let p = payload_with_bad_sha("01_Inbox/foo.md", "hello");
        let out = m.write(&p).unwrap();
        match out {
            MaterializeOutcome::IntegrityFailed {
                path, expected_sha, ..
            } => {
                assert_eq!(path, vaults.path().join(VAULT).join("01_Inbox/foo.md"));
                assert_eq!(expected_sha, p.sha256);
                assert!(path.exists(), "integrity-failed file must remain on disk");
            }
            other => panic!("expected IntegrityFailed, got {other:?}"),
        }
    }

    #[test]
    fn integrity_check_disabled_writes_anyway() {
        let cfg = MaterializerConfig {
            enable_integrity_check: false,
            ..default_cfg()
        };
        let (vaults, _ws, m) = mk(MaterializerMode::Live, cfg);
        let p = payload_with_bad_sha("01_Inbox/foo.md", "hello");
        let out = m.write(&p).unwrap();
        match out {
            MaterializeOutcome::Wrote { path } => {
                assert_eq!(path, vaults.path().join(VAULT).join("01_Inbox/foo.md"));
                assert!(path.exists());
            }
            other => panic!("expected Wrote (integrity disabled), got {other:?}"),
        }
    }

    // ---- atomic + parent dirs --------------------------------------------

    #[test]
    fn parent_dirs_created() {
        let (vaults, _ws, m) = mk(MaterializerMode::Live, default_cfg());
        let out = m.write(&payload("a/b/c/d.md", "deep")).unwrap();
        let expected = vaults.path().join(VAULT).join("a/b/c/d.md");
        assert_eq!(
            out,
            MaterializeOutcome::Wrote {
                path: expected.clone()
            }
        );
        assert!(expected.exists());
    }

    #[test]
    fn existing_atomic_persist_preserved_no_tmp_leftover() {
        let (_vaults, _ws, m) = mk(MaterializerMode::Live, default_cfg());
        m.write(&payload("01_Inbox/foo.md", "hello")).unwrap();
        // S477: live_path_for takes a vaults-root-relative path (vault
        // folder first segment), matching the materializer's contract.
        let dir = m.live_path_for(&format!("{VAULT}/01_Inbox/foo.md"));
        let parent = dir.parent().unwrap();
        let entries: Vec<String> = std::fs::read_dir(parent)
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            entries.len(),
            1,
            "expected only the final file, got: {entries:?}"
        );
        assert_eq!(entries[0], "foo.md");
    }

    #[test]
    fn atomic_write_no_partial_visible() {
        let (vaults, _ws, m) = mk(MaterializerMode::Live, default_cfg());
        let target = vaults.path().join(VAULT).join("loop/x.md");
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();
        for i in 0..100 {
            let body = format!("iteration-{i}");
            let p = payload("loop/x.md", &body);
            m.write(&p).unwrap();
            let read = std::fs::read_to_string(&target).unwrap();
            assert!(
                read.starts_with("---\n"),
                "iter {i} non-atomic? got: {read:?}"
            );
            assert!(
                read.contains("iteration-"),
                "iter {i} missing body: got: {read:?}"
            );
        }
    }

    // ---- preserved v0.2 surface ------------------------------------------

    #[test]
    fn write_creates_file_with_frontmatter() {
        let (_v, ws, m) = mk(MaterializerMode::Shadow, default_cfg());
        m.write(&payload("01_Inbox/foo.md", "hello")).unwrap();
        // S477: shadow tree mirrors the vault-folder-first path shape.
        let written = std::fs::read_to_string(
            ws.path()
                .join(".lattice-runtime")
                .join(SLUG)
                .join("shadow")
                .join(VAULT)
                .join("01_Inbox/foo.md"),
        )
        .unwrap();
        assert!(written.contains("title: Test"));
        assert!(written.contains("hello"));
    }

    #[test]
    fn write_rejects_path_traversal() {
        let (_v, _w, m) = mk(MaterializerMode::Shadow, default_cfg());
        let np = payload("../escape.md", "x");
        assert!(matches!(
            m.write(&np),
            Err(MaterializerError::PathTraversal(_))
        ));
    }

    #[test]
    fn write_allows_trailing_dots_in_name() {
        // S490 regression: a note whose title ends in `...` (three ASCII dots)
        // contains `..` as a substring but is NOT a traversal — it must
        // materialize, not get black-holed.
        let (_v, _w, m) = mk(MaterializerMode::Shadow, default_cfg());
        let out = m.write(&payload("01_Notes/Anysa says....md", "x"));
        assert!(
            out.is_ok(),
            "trailing-dots name should write, got {:?}",
            out
        );
    }

    #[test]
    fn delete_renames_to_deleted_ts() {
        let (_v, ws, m) = mk(MaterializerMode::Shadow, default_cfg());
        m.write(&payload("01_Inbox/foo.md", "x")).unwrap();
        // S477: soft_delete takes vaults-root-relative paths, same as write().
        m.soft_delete(&format!("{VAULT}/01_Inbox/foo.md")).unwrap();
        let shadow_dir = ws
            .path()
            .join(".lattice-runtime")
            .join(SLUG)
            .join("shadow")
            .join(VAULT)
            .join("01_Inbox");
        assert!(!shadow_dir.join("foo.md").exists());
        let entries: Vec<_> = std::fs::read_dir(&shadow_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.file_name()
                    .to_string_lossy()
                    .starts_with("foo.md.deleted-")
            })
            .collect();
        assert_eq!(entries.len(), 1, "expected one .deleted-* file");
    }

    #[test]
    fn delete_nothing_to_delete_is_not_error() {
        let (_v, _w, m) = mk(MaterializerMode::Shadow, default_cfg());
        assert!(m.soft_delete("01_Inbox/never-existed.md").is_ok());
    }

    #[test]
    fn delete_refuses_rasp_substrate_path() {
        let (_v, _w, m) = mk(MaterializerMode::Shadow, default_cfg());
        assert!(matches!(
            m.soft_delete("00_VAULT.md"),
            Err(MaterializerError::SubstrateRefuse(_))
        ));
    }

    // ---- Wave 4: tray-state wire-up ---------------------------------------

    fn make_shared_tray() -> SharedTrayState {
        std::sync::Arc::new(std::sync::RwLock::new(crate::tray_state::TrayState::new(
            "sub".into(),
            "https://x".into(),
            PathBuf::from("/v"),
        )))
    }

    #[test]
    fn integrity_failure_increments_tray_counter() {
        let (_vaults, _ws, m_base) = mk(MaterializerMode::Live, default_cfg());
        let tray = make_shared_tray();
        let m = m_base.with_tray_state(tray.clone());
        let p = payload_with_bad_sha("01_Inbox/foo.md", "hello");
        let out = m.write(&p).unwrap();
        assert!(matches!(out, MaterializeOutcome::IntegrityFailed { .. }));
        let s = tray.read().unwrap();
        assert_eq!(s.integrity_failures, 1);
    }

    #[test]
    fn successful_write_does_not_increment_integrity_failures() {
        let (_vaults, _ws, m_base) = mk(MaterializerMode::Live, default_cfg());
        let tray = make_shared_tray();
        let m = m_base.with_tray_state(tray.clone());
        let out = m.write(&payload("01_Inbox/foo.md", "hello")).unwrap();
        assert!(matches!(out, MaterializeOutcome::Wrote { .. }));
        let s = tray.read().unwrap();
        assert_eq!(s.integrity_failures, 0);
    }

    #[test]
    fn with_tray_state_is_idempotent_back_compat() {
        // Materializer without tray_state must still work — no panic, no
        // surprises, integrity-failed outcome still surfaced via return value.
        let (_vaults, _ws, m) = mk(MaterializerMode::Live, default_cfg());
        let p = payload_with_bad_sha("01_Inbox/foo.md", "hello");
        let out = m.write(&p).unwrap();
        assert!(matches!(out, MaterializeOutcome::IntegrityFailed { .. }));
        // And a successful write also fine.
        let ok = m.write(&payload("01_Inbox/bar.md", "world")).unwrap();
        assert!(matches!(ok, MaterializeOutcome::Wrote { .. }));
    }

    #[test]
    fn refresh_conflict_count_into_tray_scans_and_sets() {
        let (vaults, _ws, m_base) = mk(MaterializerMode::Live, default_cfg());
        let tray = make_shared_tray();
        let m = m_base.with_tray_state(tray.clone());
        let vault_dir = vaults.path().join(VAULT);
        std::fs::create_dir_all(vault_dir.join("01_Inbox")).unwrap();
        // Three conflict-stash siblings, varied subpaths.
        std::fs::write(
            vault_dir.join("01_Inbox/a.conflict-from-dev1-1.md"),
            "stash-a",
        )
        .unwrap();
        std::fs::write(
            vault_dir.join("01_Inbox/b.conflict-from-dev2-7.md"),
            "stash-b",
        )
        .unwrap();
        std::fs::write(vault_dir.join("c.conflict-from-dev3-12.md"), "stash-c").unwrap();
        m.refresh_conflict_count_into_tray();
        let s = tray.read().unwrap();
        assert_eq!(s.conflict_unresolved, 3);
    }

    #[test]
    fn refresh_with_no_tray_state_is_noop() {
        let (vaults, _ws, m) = mk(MaterializerMode::Live, default_cfg());
        let vault_dir = vaults.path().join(VAULT);
        std::fs::create_dir_all(&vault_dir).unwrap();
        std::fs::write(vault_dir.join("a.conflict-from-d-1.md"), "x").unwrap();
        // Must not panic, must not touch any tray (there is none).
        m.refresh_conflict_count_into_tray();
        m.refresh_conflict_count_into_tray();
    }

    // ---- shadow-store recording (fix/reconcile-server-wins-shadow) -----------

    /// A successful Live write must record the server's canonical hash
    /// (payload.sha256) into the attached ShadowStore, keyed by the wire path.
    #[test]
    fn successful_write_records_shadow_hash() {
        use crate::sync_shadow::ShadowStore;
        let dir = TempDir::new().unwrap();
        let shadow = ShadowStore::load(dir.path().join("shadow.json"));
        let (_vaults, _ws, m_base) = mk(MaterializerMode::Live, default_cfg());
        let m = m_base.with_shadow_store(shadow.clone());
        let p = payload("01_Inbox/foo.md", "hello");
        let out = m.write(&p).unwrap();
        assert!(
            matches!(out, MaterializeOutcome::Wrote { .. }),
            "got {out:?}"
        );
        assert_eq!(shadow.get(&p.path), Some(p.sha256.clone()));
    }

    /// An IntegrityFailed write must NOT record a shadow hash (the on-disk
    /// bytes don't match the server canonical, so it isn't a true in-sync state).
    #[test]
    fn integrity_failed_write_does_not_record_shadow() {
        use crate::sync_shadow::ShadowStore;
        let dir = TempDir::new().unwrap();
        let shadow = ShadowStore::load(dir.path().join("shadow.json"));
        let (_vaults, _ws, m_base) = mk(MaterializerMode::Live, default_cfg());
        let m = m_base.with_shadow_store(shadow.clone());
        let p = payload_with_bad_sha("01_Inbox/foo.md", "hello");
        let out = m.write(&p).unwrap();
        assert!(matches!(out, MaterializeOutcome::IntegrityFailed { .. }));
        assert_eq!(shadow.get(&p.path), None);
    }

    // ---- B4: per-sync_root materializer tests --------------------------------

    /// B4 core: each sync_root gets its own Materializer constructed with
    /// `sync_root.path` as `vaults_root`. Writes must land at
    /// `<sync_root.path>/<wire_path>`, NOT at some global vaults container.
    ///
    /// Simulates the two-root scenario:
    ///   root_a → /tmp/.../RootA/
    ///   root_b → /tmp/.../RootB/
    ///
    /// A Materializer constructed for root_a writes `notes/x.md` to
    /// `RootA/notes/x.md`; one for root_b writes the same wire_path to
    /// `RootB/notes/x.md`. They must NOT cross-contaminate.
    #[test]
    fn per_root_materializer_writes_to_sync_root_path_join_wire_path() {
        // Two completely separate sync roots (two vault directories).
        let ws_tmp = TempDir::new().unwrap();

        let root_a = TempDir::new().unwrap();
        let root_b = TempDir::new().unwrap();

        let mk_for_root = |root_path: &std::path::Path| {
            Materializer::new(
                root_path.to_path_buf(),
                Some("shadow/".to_string()),
                MaterializerMode::Live,
                ws_tmp.path().to_path_buf(),
                "sub-test".to_string(),
                default_cfg(),
            )
        };

        let mat_a = mk_for_root(root_a.path());
        let mat_b = mk_for_root(root_b.path());

        // Build payloads with the SAME wire path (relative to their respective root).
        let wire_path = "notes/x.md";
        let make_payload = |body: &str| {
            let fm = serde_json::json!({"title": "T"});
            let fm_yaml = serde_yaml::to_string(&fm).unwrap();
            let serialized = format!("---\n{fm_yaml}---\n\n{body}");
            NotePayload {
                path: wire_path.to_string(),
                frontmatter: fm,
                body: body.into(),
                sha256: hex::encode(Sha256::digest(serialized.as_bytes())),
                modified: "2026-05-29T00:00:00Z".into(),
                file_mtime: None,
                created: None,
                enriched_body: Some(serialized),
            }
        };

        let out_a = mat_a.write(&make_payload("body-a")).unwrap();
        let out_b = mat_b.write(&make_payload("body-b")).unwrap();

        // Materializer A must write to root_a/<wire_path>.
        let expected_a = root_a.path().join(wire_path);
        match out_a {
            MaterializeOutcome::Wrote { path } => {
                assert_eq!(path, expected_a, "root_a target mismatch")
            }
            other => panic!("expected Wrote for root_a, got {other:?}"),
        }
        assert!(expected_a.exists());
        let content_a = std::fs::read_to_string(&expected_a).unwrap();
        assert!(
            content_a.contains("body-a"),
            "root_a content wrong: {content_a:?}"
        );

        // Materializer B must write to root_b/<wire_path>.
        let expected_b = root_b.path().join(wire_path);
        match out_b {
            MaterializeOutcome::Wrote { path } => {
                assert_eq!(path, expected_b, "root_b target mismatch")
            }
            other => panic!("expected Wrote for root_b, got {other:?}"),
        }
        assert!(expected_b.exists());
        let content_b = std::fs::read_to_string(&expected_b).unwrap();
        assert!(
            content_b.contains("body-b"),
            "root_b content wrong: {content_b:?}"
        );

        // No cross-contamination: root_a must NOT contain root_b's file.
        let cross_a = root_a.path().join(wire_path);
        let cross_b = root_b.path().join(wire_path);
        let read_cross_a = std::fs::read_to_string(&cross_a).unwrap();
        let read_cross_b = std::fs::read_to_string(&cross_b).unwrap();
        assert!(
            !read_cross_a.contains("body-b"),
            "root_a must not contain root_b content"
        );
        assert!(
            !read_cross_b.contains("body-a"),
            "root_b must not contain root_a content"
        );
    }

    /// B4: `live_path_for(wire_path)` returns `<sync_root.path>/<wire_path>`.
    /// The caller uses this to locate the file before write (e.g. conflict detection).
    #[test]
    fn live_path_for_returns_sync_root_join_wire_path() {
        let sync_root = TempDir::new().unwrap();
        let ws = TempDir::new().unwrap();
        let mat = Materializer::new(
            sync_root.path().to_path_buf(),
            None,
            MaterializerMode::Live,
            ws.path().to_path_buf(),
            "sub".to_string(),
            default_cfg(),
        );
        let wire = "01_Inbox/note.md";
        let result = mat.live_path_for(wire);
        assert_eq!(result, sync_root.path().join(wire));
    }

    /// Ported from main v0.3.9 (S479 E1, commit e816439) into the sync_roots
    /// line. The S479 duplicate-filename bug (`…`→`ΓÇª`, `'`→`ΓÇÖ`, `🚨`→`≡ƒÜ¿`)
    /// came from a shared Windows ingest-layer writer decoding UTF-8 bytes as
    /// the CP437 OEM console codepage. The daemon's materializer was AUDITED
    /// CLEAN (it uses `std::fs`/`OsStr`, UTF-16/UTF-8 native on Windows), so
    /// there is no boundary to fix — this test PINS that property under the
    /// per-root (B4) materialize path: a note whose name carries non-ASCII
    /// punctuation + an emoji materializes to disk with a byte-identical UTF-8
    /// filename, never CP437-mangled, so any future OEM-decode regression fails
    /// loudly.
    #[test]
    fn materialize_preserves_unicode_filename_bytes_not_cp437() {
        let (vaults, _ws, m) = mk(MaterializerMode::Live, default_cfg());
        // Non-ASCII punctuation (… ' – " ") + emoji (🚨) — the exact mojibake
        // class from the S479 worklist.
        let name = "Probe … 'q' – \u{201C}d\u{201D} 🚨.md";
        let rel = format!("01_Inbox/{name}");
        let out = m.write(&payload(&rel, "hello")).unwrap();
        assert!(
            matches!(out, MaterializeOutcome::Wrote { .. }),
            "expected Wrote, got {out:?}"
        );
        // Per-root convention: Live writes under <vaults_root>/<VAULT>/...
        let dir = vaults.path().join(VAULT).join("01_Inbox");
        let names: Vec<String> = std::fs::read_dir(&dir)
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert!(
            names.iter().any(|n| n == name),
            "on-disk filename must be byte-identical UTF-8; got {names:?}"
        );
        assert!(
            !names.iter().any(|n| n.contains("ΓÇ") || n.contains("≡ƒ")),
            "CP437 mojibake detected on disk: {names:?}"
        );
    }

    // ---- S511 (TKT-2dc9a17e): unified decide() R1-R5 ----------------------

    use crate::sync_shadow::ShadowStore;

    /// (vaults, ws, materializer-with-shadow, shadow) for the decide() tests.
    fn mk_with_shadow(
        mode: MaterializerMode,
    ) -> (TempDir, TempDir, Materializer, Arc<ShadowStore>) {
        let (v, w, m) = mk(mode, default_cfg());
        let sdir = Box::leak(Box::new(TempDir::new().unwrap()));
        let shadow = ShadowStore::load(sdir.path().join("shadow.json"));
        let m = m.with_shadow_store(shadow.clone());
        (v, w, m, shadow)
    }

    /// PURE decide() truth table (R1-R5). This is the load-bearing decision and
    /// must be exhaustively correct.
    #[test]
    fn decide_truth_table_r1_to_r5() {
        // R1: local == server => Noop, regardless of shadow state.
        assert_eq!(decide(true, false, false, false), Decision::Noop);
        assert_eq!(decide(true, true, true, true), Decision::Noop);
        // R5: shadow absent and local != server => Conflict.
        assert_eq!(decide(false, false, false, false), Decision::Conflict);
        // R2: shadow present, shadow == server, local != server => PreserveLocalEdit.
        assert_eq!(
            decide(false, true, true, false),
            Decision::PreserveLocalEdit
        );
        // R3: shadow present, server moved (shadow != server), local == shadow => PullClean.
        assert_eq!(decide(false, true, false, true), Decision::PullClean);
        // R4: shadow present, server moved AND local moved (neither equals) => Conflict.
        assert_eq!(decide(false, true, false, false), Decision::Conflict);
    }

    /// R2 end-to-end: shadow records the server hash as last-synced, the local
    /// file has a genuine edit (diverges). write() must return
    /// Skipped(LocalEditPreserved) and MUST NOT touch the file (the push
    /// pipeline carries the edit up). This is the exact silent-revert the
    /// operator hit (TKT-2dc9a17e).
    #[test]
    fn r2_local_edit_is_preserved_not_overwritten() {
        let (vaults, _ws, m, shadow) = mk_with_shadow(MaterializerMode::Live);
        let rel = format!("{VAULT}/01_Inbox/edited.md");
        let target = vaults.path().join(VAULT).join("01_Inbox/edited.md");
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();

        // Server canonical the daemon would push down.
        let server = payload("01_Inbox/edited.md", "server-body");
        // Shadow says: the last thing we synced for this path WAS this server
        // hash (the server has NOT moved since).
        shadow.record(&rel, &server.sha256);
        // The local file is a genuine user edit, diverging from the server.
        let local_edit = "---\ntitle: Test\n---\n\nMY LOCAL EDIT, do not lose\n";
        std::fs::write(&target, local_edit).unwrap();

        let out = m.write(&server).unwrap();
        assert_eq!(
            out,
            MaterializeOutcome::Skipped(SkipReason::LocalEditPreserved),
            "R2 must preserve the local edit, not overwrite it"
        );
        // The file on disk is STILL the local edit, untouched.
        let on_disk = std::fs::read_to_string(&target).unwrap();
        assert_eq!(
            on_disk, local_edit,
            "the local edit must survive verbatim (no silent revert)"
        );
        // No conflict copy was created (R2 is not a conflict, it is a push-up).
        let dir = target.parent().unwrap();
        assert!(
            !std::fs::read_dir(dir)
                .unwrap()
                .flatten()
                .any(|e| e.file_name().to_string_lossy().contains(".conflict-from-")),
            "R2 must not write a conflict copy"
        );
    }

    /// R3 end-to-end: local is exactly the last-synced bytes (untouched), only
    /// the server moved => clean pull, server bytes written, NO stash.
    #[test]
    fn r3_clean_pull_no_stash() {
        let (vaults, _ws, m, shadow) = mk_with_shadow(MaterializerMode::Live);
        let rel = format!("{VAULT}/01_Inbox/clean.md");
        let target = vaults.path().join(VAULT).join("01_Inbox/clean.md");
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();

        // The local file holds the OLD server bytes (a prior materialization).
        let old_bytes = "---\ntitle: Test\n---\n\nold server body\n";
        std::fs::write(&target, old_bytes).unwrap();
        let old_raw_sha = sha256_hex(old_bytes);
        // Shadow records that OLD hash as the last-synced server hash, AND it is
        // the local file's raw hash (local == shadow, untouched since sync).
        shadow.record(&rel, &old_raw_sha);

        // The server has moved on to new bytes (server != shadow).
        let server = payload("01_Inbox/clean.md", "new server body");
        let out = m.write(&server).unwrap();
        match out {
            MaterializeOutcome::Wrote { .. } => {}
            other => panic!("expected clean Wrote (R3), got {other:?}"),
        }
        let on_disk = std::fs::read_to_string(&target).unwrap();
        assert!(on_disk.contains("new server body"), "server bytes pulled");
        // No conflict copy on a clean pull.
        let dir = target.parent().unwrap();
        assert!(
            !std::fs::read_dir(dir)
                .unwrap()
                .flatten()
                .any(|e| e.file_name().to_string_lossy().contains(".conflict-from-")),
            "R3 clean pull must not stash"
        );
    }

    /// R4 end-to-end: shadow present but BOTH sides moved (local edited AND
    /// server advanced, neither equals the last-synced base) => true conflict:
    /// stash the local loser, materialize the server winner, both preserved.
    /// The stash filename carries the change_seq passed in.
    #[test]
    fn r4_both_moved_stashes_with_change_seq() {
        let (vaults, _ws, m, shadow) = mk_with_shadow(MaterializerMode::Live);
        let rel = format!("{VAULT}/01_Inbox/both.md");
        let target = vaults.path().join(VAULT).join("01_Inbox/both.md");
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();

        // Last-synced base hash (neither current local nor current server).
        shadow.record(&rel, &"0".repeat(64));
        // Local diverged from base.
        let local_edit = "---\ntitle: Test\n---\n\nlocal divergent edit\n";
        std::fs::write(&target, local_edit).unwrap();
        // Server diverged from base too.
        let server = payload("01_Inbox/both.md", "server divergent body");

        let out = m.write_with_change_seq(&server, 4242).unwrap();
        match out {
            MaterializeOutcome::Stashed { stash_path } => {
                let name = stash_path
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .into_owned();
                assert!(
                    name.contains("both.conflict-from-") && name.contains("-4242.md"),
                    "stash must be named by change_seq 4242, got {name}"
                );
                assert_eq!(
                    std::fs::read_to_string(&stash_path).unwrap(),
                    local_edit,
                    "loser (local) preserved verbatim in the stash"
                );
            }
            other => panic!("expected Stashed (R4), got {other:?}"),
        }
        // Winner (server) at the canonical path.
        assert!(std::fs::read_to_string(&target)
            .unwrap()
            .contains("server divergent body"));
    }

    /// D11: a CRLF (Windows) local file vs an LF (Unix) server body with the
    /// SAME logical content must NOT be treated as a divergence. With the
    /// shadow recording the server hash (R2 setup), a CRLF-only difference must
    /// resolve as R1 NOOP (identical-after-normalization), not a false conflict
    /// or a false local-edit.
    #[test]
    fn d11_crlf_vs_lf_is_not_a_divergence() {
        let (vaults, _ws, m, _shadow) = mk_with_shadow(MaterializerMode::Live);
        let target = vaults.path().join(VAULT).join("01_Inbox/eol.md");
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();

        // Server LF body.
        let server = payload("01_Inbox/eol.md", "line one\nline two\n");
        // Local file: identical content but CRLF line endings + a leading BOM.
        let server_bytes = server.enriched_body.clone().unwrap();
        let crlf_local = format!("\u{feff}{}", server_bytes.replace('\n', "\r\n"));
        std::fs::write(&target, &crlf_local).unwrap();

        let out = m.write(&server).unwrap();
        assert_eq!(
            out,
            MaterializeOutcome::Skipped(SkipReason::IdenticalToLocal),
            "CRLF/BOM-only difference must normalize to identical (R1), got {out:?}"
        );
        // The local CRLF file is left untouched (idempotent skip, no rewrite).
        assert_eq!(std::fs::read_to_string(&target).unwrap(), crlf_local);
    }

    /// D13: soft_delete suffix carries nanosecond precision, so two deletes of
    /// the same path within one second do not collide / clobber the first
    /// preserved copy.
    #[test]
    fn d13_soft_delete_suffix_is_nanosecond_unique() {
        let (_v, ws, m) = mk(MaterializerMode::Shadow, default_cfg());
        let shadow_dir = ws
            .path()
            .join(".lattice-runtime")
            .join(SLUG)
            .join("shadow")
            .join(VAULT)
            .join("01_Inbox");
        // Two write+delete cycles on the SAME path, back to back (same second).
        m.write(&payload("01_Inbox/d.md", "v1")).unwrap();
        m.soft_delete(&format!("{VAULT}/01_Inbox/d.md")).unwrap();
        m.write(&payload("01_Inbox/d.md", "v2")).unwrap();
        m.soft_delete(&format!("{VAULT}/01_Inbox/d.md")).unwrap();

        let deleted: Vec<String> = std::fs::read_dir(&shadow_dir)
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.starts_with("d.md.deleted-"))
            .collect();
        assert_eq!(
            deleted.len(),
            2,
            "both soft-deletes must be preserved (nanosecond-unique suffixes), got {deleted:?}"
        );
    }
}
