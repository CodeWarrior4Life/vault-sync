//! R7 + R9 + R10 + R11 + R17 + R21: live-vault file watcher with multi-layer
//! filtering before pushing into the local->server `PushJournal`.
//!
//! Per the v0.3 Enterprise Bidirectional Mandate §4.1 (file_watcher.rs NEW
//! module) + §1 rows 6/11/13 + §2 R7/R9/R10/R11/R17/R21.
//!
//! ## Layers (in `classify()` order)
//!
//! 1. **Path normalization** — vault-relative, forward-slash. Outside-root paths
//!    drop as `DropOutOfScope`.
//! 2. **Hardcoded directory excludes** — `.obsidian/`, `.lattice-sync/`,
//!    `.trash/` always drop regardless of user config (defense-in-depth).
//! 3. **User scope_excludes** — drop if prefix-match.
//! 4. **scope_roots** — if non-empty, require prefix-match into one of them.
//! 5. **Extension gate** — only allowed extensions pass; everything else
//!    drops as `DropExtension`. Deletes bypass the extension check IFF the
//!    path *looks* like an allowed extension (because at delete time the file
//!    is already gone — we just trust the path).
//! 6. **RASP fence** — `rasp_fence::classify_path` drops substrate paths.
//! 7. **Delete-burst** — Deleted events consult the shared `DeleteBurstDetector`.
//!    If `Paused`, drop as `DropDeleteBurst`. Caller (daemon) is responsible
//!    for unpausing via owner prompt.
//!
//! ## Pure classify
//!
//! `classify()` performs NO filesystem I/O and NO journal mutation. All
//! filtering tests target it. The FS-watcher start/stop path is small and
//! is exercised by `#[ignore]`d integration tests (skipped on CI but
//! runnable locally with `cargo test -- --ignored`).
//!
//! ## Renames
//!
//! `notify` emits renames as a single `Rename(from, to)` pair. We forward
//! as `Renamed { old_path, new_path }`. If EITHER side fails any filter
//! (substrate, scope, exclude, extension), the WHOLE rename is dropped — we
//! never split a rename into delete+create (that's exactly the nexus-sync
//! rename bug §1 implicitly references).

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::push_journal::{new_event_id, PushAction, PushEvent, PushJournal, CURRENT_SCHEMA};
use crate::rasp_fence::{classify_path, is_junk_path, PathClassification};
use crate::redflag::DeleteBurstDetector;
use crate::tray_state::SharedTrayState;

/// Hardcoded directory excludes — applied regardless of user config.
/// Match against the forward-slash vault-relative path. A path matches if it
/// either starts with the prefix (root-level) or contains `/<prefix>` (nested).
const HARDCODED_EXCLUDES: &[&str] = &[
    ".obsidian/",
    ".lattice-sync/",
    ".trash/",
    "._/", // S477: convention for organized machine-local trees
];

/// Path-segment prefix matches. If ANY segment of the path (basename of any
/// ancestor or the file itself) starts with one of these, the path drops.
/// Lattice-wide convention per S477: `.%` marks files/folders as machine-local
/// — not synced, generated + maintained per-machine.
const HARDCODED_BASENAME_PREFIXES: &[&str] = &[".%"];

/// Default allowed text extensions (lowercase, no leading dot).
pub const DEFAULT_ALLOWED_EXTENSIONS: &[&str] = &["md"];

#[derive(Debug, Clone)]
pub struct FileWatcherConfig {
    /// Lowercase, no leading dot. Match is case-insensitive against the path's
    /// extension.
    pub allowed_extensions: Vec<String>,
    /// Empty = "all paths under vault root". Non-empty = path must
    /// prefix-match into at least one root.
    pub scope_roots: Vec<String>,
    /// Prefix-match excludes layered ON TOP OF hardcoded excludes.
    pub scope_excludes: Vec<String>,
    /// notify debouncer collection window (ms). 500 is the spec default.
    pub debounce_ms: u64,
}

impl Default for FileWatcherConfig {
    fn default() -> Self {
        Self {
            allowed_extensions: DEFAULT_ALLOWED_EXTENSIONS
                .iter()
                .map(|s| s.to_string())
                .collect(),
            scope_roots: Vec::new(),
            scope_excludes: Vec::new(),
            debounce_ms: 500,
        }
    }
}

/// One filesystem event after debouncing + normalization (vault-relative path,
/// forward-slashes).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatchEvent {
    Created { path: String },
    Modified { path: String },
    Deleted { path: String },
    Renamed { old_path: String, new_path: String },
}

/// Outcome of [`FileWatcher::classify`]. The `Allow` variant carries the
/// (possibly path-normalized) event so the caller can `to_push_event` it
/// directly. Drop variants carry attribution for logging / tray counters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FilterDecision {
    Allow(WatchEvent),
    DropSubstrate { path: String, rule: &'static str },
    DropExtension { path: String, ext: String },
    DropOutOfScope { path: String },
    DropExclude { path: String, exclude_rule: String },
    DropDeleteBurst { path: String },
}

/// Opaque handle returned by [`FileWatcher::start`]. Dropping the handle
/// stops the watcher (the `_watcher` field's Drop releases the OS handle and
/// the `_task` join handle is aborted via the embedded shutdown channel).
pub struct WatchHandle {
    _watcher: notify_debouncer_mini::Debouncer<notify::RecommendedWatcher>,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    _task: tokio::task::JoinHandle<()>,
}

impl Drop for WatchHandle {
    fn drop(&mut self) {
        // Signal shutdown; the task will exit on next tick.
        let _ = self.shutdown_tx.send(true);
    }
}

#[derive(Debug, Error)]
pub enum FileWatcherError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("notify: {0}")]
    Notify(#[from] notify::Error),
    /// S477 §3.5 (v0.3.7): Linux-only — inotify per-user watch limit exceeded.
    /// The kernel rejected `inotify_add_watch` with `ENOSPC` while attempting
    /// to register the recursive vault watch. `current` is the value read from
    /// `/proc/sys/fs/inotify/max_user_watches` (0 if the file could not be
    /// read). User remedy: raise the limit via `sudo sysctl -w
    /// fs.inotify.max_user_watches=524288` (and persist via /etc/sysctl.conf).
    #[error("inotify watch limit exceeded (current={current}); raise fs.inotify.max_user_watches")]
    InotifyLimitExceeded { current: u64 },
}

/// S477 §3.5 (v0.3.7): Linux-only helper that reads the current per-user
/// inotify watch limit from procfs. Returns None if procfs is unreadable
/// (e.g. running inside a sandbox that hides /proc) or if the value is not
/// a parseable integer.
#[cfg(target_os = "linux")]
fn read_inotify_limit() -> Option<u64> {
    std::fs::read_to_string("/proc/sys/fs/inotify/max_user_watches")
        .ok()?
        .trim()
        .parse()
        .ok()
}

pub struct FileWatcher {
    vault_root: PathBuf,
    journal: Arc<Mutex<PushJournal>>,
    burst: Arc<Mutex<DeleteBurstDetector>>,
    config: FileWatcherConfig,
    device_id: String,
    /// Optional tray telemetry sink (mandate §9 AG13). When set,
    /// `classify_and_count` (used by integration tests + the spawned
    /// FS-watcher task) increments per-reason filter counters.
    tray_state: Option<SharedTrayState>,
}

impl FileWatcher {
    pub fn new(
        vault_root: impl Into<PathBuf>,
        journal: Arc<Mutex<PushJournal>>,
        burst: Arc<Mutex<DeleteBurstDetector>>,
        config: FileWatcherConfig,
        device_id: impl Into<String>,
    ) -> Result<Self, FileWatcherError> {
        Ok(Self {
            vault_root: vault_root.into(),
            journal,
            burst,
            config,
            device_id: device_id.into(),
            tray_state: None,
        })
    }

    /// Builder-style: attach a SharedTrayState so each filtered event
    /// increments the relevant counter. Backwards-compatible.
    pub fn with_tray_state(mut self, state: SharedTrayState) -> Self {
        self.tray_state = Some(state);
        self
    }

    /// Like [`Self::classify`] but ALSO records counters into the wired
    /// `tray_state` (if any) for filter-drop telemetry. Callers that want
    /// pure classification stick with `classify`; the FS-watcher task uses
    /// this variant so the tray sees live counts.
    pub fn classify_and_count(&self, evt: &WatchEvent) -> FilterDecision {
        let decision = self.classify(evt);
        if let Some(tray) = &self.tray_state {
            if let Ok(mut w) = tray.write() {
                match &decision {
                    FilterDecision::Allow(_) => {}
                    FilterDecision::DropSubstrate { .. } => w.inc_events_dropped_substrate(),
                    FilterDecision::DropExtension { .. } => w.inc_events_dropped_extension(),
                    FilterDecision::DropOutOfScope { .. } => w.inc_events_dropped_excludes(),
                    FilterDecision::DropExclude { .. } => w.inc_events_dropped_excludes(),
                    FilterDecision::DropDeleteBurst { .. } => w.inc_events_filtered(),
                }
            }
        }
        decision
    }

    // -------------------------------------------------------------------
    // PURE FILTERING
    // -------------------------------------------------------------------

    /// Pure (no FS, no journal mutation) classifier. Takes a `WatchEvent`
    /// (already vault-relative; see [`Self::normalize_event`]) and returns
    /// a [`FilterDecision`].
    pub fn classify(&self, evt: &WatchEvent) -> FilterDecision {
        match evt {
            WatchEvent::Created { path } | WatchEvent::Modified { path } => {
                self.classify_path_for_write(path, evt.clone())
            }
            WatchEvent::Deleted { path } => self.classify_delete(path, evt.clone()),
            WatchEvent::Renamed { old_path, new_path } => {
                // Either-side-fails-drops-whole-rename. We classify both as
                // a "write" (the rename is structurally a create at new_path
                // plus a delete at old_path, but we treat both sides as
                // requiring same gates — substrate / scope / exclude /
                // extension). We do NOT consult delete-burst for renames
                // (a rename is not a bulk-delete signal).
                let from_decision = self.classify_path_for_write(
                    old_path,
                    WatchEvent::Modified {
                        path: old_path.clone(),
                    },
                );
                if !matches!(&from_decision, FilterDecision::Allow(_)) {
                    return rewrite_decision_path(from_decision, old_path.clone());
                }
                let to_decision = self.classify_path_for_write(
                    new_path,
                    WatchEvent::Modified {
                        path: new_path.clone(),
                    },
                );
                if !matches!(&to_decision, FilterDecision::Allow(_)) {
                    return rewrite_decision_path(to_decision, new_path.clone());
                }
                FilterDecision::Allow(evt.clone())
            }
        }
    }

    /// Same as `classify` but skips the delete-burst step. Used by
    /// rename-side checks above.
    fn classify_path_for_write(&self, path: &str, evt: WatchEvent) -> FilterDecision {
        // Normalize (defensive — caller is supposed to have normalized).
        let norm = normalize_path(path);

        // (1) Out-of-scope: forbid path-traversal escapes & absolute paths
        // (the normalizer should have stripped vault_root prefix already, so
        // remaining absolute paths are bugs).
        if norm.starts_with('/') || norm.starts_with('\\') || norm.contains("..") {
            return FilterDecision::DropOutOfScope { path: norm };
        }

        // (2) Hardcoded excludes (defense-in-depth). Match prefix at root OR
        // any nested `/<prefix>` segment so e.g. `Mainframe/._/state.json`
        // drops the same way `._/state.json` would.
        for ex in HARDCODED_EXCLUDES {
            if norm.starts_with(ex) || norm.contains(&format!("/{ex}")) {
                return FilterDecision::DropExclude {
                    path: norm,
                    exclude_rule: (*ex).to_string(),
                };
            }
        }

        // (2b) Hardcoded basename prefixes (Lattice-wide machine-local
        // convention per S477). If ANY path segment starts with one of these
        // prefixes, the whole path is machine-local.
        for segment in norm.split('/') {
            for prefix in HARDCODED_BASENAME_PREFIXES {
                if segment.starts_with(prefix) {
                    return FilterDecision::DropExclude {
                        path: norm,
                        exclude_rule: format!("hardcoded-basename:{prefix}"),
                    };
                }
            }
        }

        // (2c) macOS junk files — AppleDouble `._*` sidecars and `.DS_Store`.
        // Carve-out: `.nx-<host>` machine-namespace dirs are NOT junk (see
        // rasp_fence::is_junk_path). `.nx-` has 'n' after the dot, so the
        // `._` prefix check never fires on them — structurally distinct.
        if is_junk_path(&norm) {
            return FilterDecision::DropExclude {
                path: norm,
                exclude_rule: "macos-junk".to_string(),
            };
        }

        // (3) User scope_excludes.
        for ex in &self.config.scope_excludes {
            if norm.starts_with(ex.as_str()) {
                return FilterDecision::DropExclude {
                    path: norm,
                    exclude_rule: ex.clone(),
                };
            }
        }

        // (4) scope_roots.
        if !self.config.scope_roots.is_empty() {
            let in_scope = self
                .config
                .scope_roots
                .iter()
                .any(|r| norm.starts_with(r.as_str()));
            if !in_scope {
                return FilterDecision::DropOutOfScope { path: norm };
            }
        }

        // (5) Extension gate.
        let ext = path_extension(&norm).to_ascii_lowercase();
        let allowed = self
            .config
            .allowed_extensions
            .iter()
            .any(|e| e.eq_ignore_ascii_case(&ext));
        if !allowed {
            return FilterDecision::DropExtension { path: norm, ext };
        }

        // (6) RASP fence (substrate).
        match classify_path(&norm) {
            PathClassification::Substrate { rule } => {
                FilterDecision::DropSubstrate { path: norm, rule }
            }
            PathClassification::Content => {
                // Rewrite the event with the normalized path so the caller's
                // `to_push_event` sees canonical forward-slash form.
                FilterDecision::Allow(rewrite_event_path(evt, norm))
            }
        }
    }

    fn classify_delete(&self, path: &str, evt: WatchEvent) -> FilterDecision {
        // Run the same gates as a write first. The extension gate at
        // delete-time is informational — we'd never have pushed a non-md
        // create, so we'd never have a non-md delete propagate. The substrate
        // gate is still load-bearing (a substrate file delete must NOT push).
        let initial = self.classify_path_for_write(path, evt);
        let allowed_evt = match initial {
            FilterDecision::Allow(e) => e,
            other => return other,
        };

        // Now consult the delete-burst valve.
        let paused = self.burst.lock().map(|d| d.is_paused()).unwrap_or(false);
        if paused {
            let p = match &allowed_evt {
                WatchEvent::Deleted { path } => path.clone(),
                _ => String::new(),
            };
            return FilterDecision::DropDeleteBurst { path: p };
        }
        FilterDecision::Allow(allowed_evt)
    }

    // -------------------------------------------------------------------
    // PUSH-EVENT CONSTRUCTION
    // -------------------------------------------------------------------

    /// Convert an Allow'd WatchEvent into a `PushEvent`. For renames we
    /// model the result as a single Create at the new path (the old path's
    /// removal is implied by the server-side reconciler's path-uniqueness
    /// invariant). The deliberate choice is documented in the renamed test.
    ///
    /// `content` is required for Create/Modify/Rename (target body). For
    /// Delete, pass `None`.
    pub fn to_push_event(&self, evt: &WatchEvent, content: Option<Vec<u8>>) -> Option<PushEvent> {
        let now = chrono::Utc::now();
        match evt {
            WatchEvent::Created { path } => {
                let bytes = content?;
                Some(PushEvent {
                    schema_version: CURRENT_SCHEMA,
                    id: new_event_id(),
                    path: path.clone(),
                    action: PushAction::Create,
                    base_hash: None,
                    content_sha: sha256_hex(&bytes),
                    content_bytes: Some(bytes),
                    queued_at: now,
                    device_id: self.device_id.clone(),
                })
            }
            WatchEvent::Modified { path } => {
                let bytes = content?;
                Some(PushEvent {
                    schema_version: CURRENT_SCHEMA,
                    id: new_event_id(),
                    path: path.clone(),
                    action: PushAction::Modify,
                    // Caller is responsible for sourcing the base_hash from
                    // the materializer's last-known state; in this layer we
                    // do not have it. v0.3.1 spec note: the push_client
                    // backfills base_hash by reading the materializer index
                    // before sending. For now we emit None and let the
                    // push_client OR the server retry path handle it.
                    base_hash: None,
                    content_sha: sha256_hex(&bytes),
                    content_bytes: Some(bytes),
                    queued_at: now,
                    device_id: self.device_id.clone(),
                })
            }
            WatchEvent::Deleted { path } => Some(PushEvent {
                schema_version: CURRENT_SCHEMA,
                id: new_event_id(),
                path: path.clone(),
                action: PushAction::Delete,
                base_hash: None,
                content_sha: String::new(),
                content_bytes: None,
                queued_at: now,
                device_id: self.device_id.clone(),
            }),
            WatchEvent::Renamed {
                old_path: _,
                new_path,
            } => {
                // Model rename as Create at new_path. Document decision:
                // - Single PushEvent (not a pair) keeps journal ordering simple.
                // - The server-side reconciler treats receipt of a Create at
                //   a new path AS the canonical move when paired with the
                //   absence of activity at the old path (and the materializer
                //   side index handles the residual at old_path).
                // - This explicitly AVOIDS the delete+create split that
                //   the v0.3 mandate calls out as the nexus-sync rename bug.
                let bytes = content?;
                Some(PushEvent {
                    schema_version: CURRENT_SCHEMA,
                    id: new_event_id(),
                    path: new_path.clone(),
                    action: PushAction::Create,
                    base_hash: None,
                    content_sha: sha256_hex(&bytes),
                    content_bytes: Some(bytes),
                    queued_at: now,
                    device_id: self.device_id.clone(),
                })
            }
        }
    }

    // -------------------------------------------------------------------
    // PATH NORMALIZATION
    // -------------------------------------------------------------------

    /// Given an absolute filesystem path from notify, strip the vault_root
    /// prefix and normalize backslashes to forward slashes. Canonicalizes
    /// (R7 symlink-escape safety): if canonicalization yields a path outside
    /// vault_root, returns None.
    pub fn normalize_path(&self, abs: &Path) -> Option<String> {
        // For non-existent paths (delete events), canonicalize will fail.
        // Fall back to lexical strip without canonicalization in that case.
        let resolved = std::fs::canonicalize(abs).ok();
        let vault_canon = std::fs::canonicalize(&self.vault_root).ok();
        match (resolved, vault_canon) {
            (Some(ap), Some(vr)) => {
                if !ap.starts_with(&vr) {
                    return None;
                }
                let rel = ap.strip_prefix(&vr).ok()?;
                Some(normalize_path(&rel.to_string_lossy()))
            }
            _ => {
                // Lexical fallback.
                let rel = abs.strip_prefix(&self.vault_root).ok()?;
                Some(normalize_path(&rel.to_string_lossy()))
            }
        }
    }

    /// Take a raw `notify-debouncer-mini::DebouncedEvent` path and produce a
    /// `WatchEvent` of the given variant kind. Used by the FS-watcher loop.
    /// Public for integration tests that bypass the spawned task.
    pub fn normalize_event(&self, abs: &Path, kind: WatchEventKindHint) -> Option<WatchEvent> {
        let p = self.normalize_path(abs)?;
        Some(match kind {
            WatchEventKindHint::Created => WatchEvent::Created { path: p },
            WatchEventKindHint::Modified => WatchEvent::Modified { path: p },
            WatchEventKindHint::Deleted => WatchEvent::Deleted { path: p },
        })
    }

    // -------------------------------------------------------------------
    // WATCHER STARTUP
    // -------------------------------------------------------------------

    /// Start the OS watcher and spawn the background task that funnels
    /// events through `classify` and into the journal. Returns a
    /// [`WatchHandle`] — dropping it stops the watcher.
    ///
    /// NOTE: notify-debouncer-mini emits a single coarse-grained event kind
    /// per debounce window — we cannot reliably distinguish create vs modify
    /// from the debouncer alone. The current strategy:
    ///
    ///   - Path exists & journal has no Create yet → emit Created.
    ///   - Path exists & previously seen → emit Modified.
    ///   - Path does not exist → emit Deleted.
    ///
    /// Rename detection is deferred (debounced renames typically surface as
    /// a delete+create pair; we collapse only at the journal/push_client
    /// layer if we observe matching content hashes). For v0.3.0 the
    /// FS-watcher codepath is `#[ignore]`d in CI and only exercised
    /// manually; the `classify` and `to_push_event` paths (which are pure
    /// and load-bearing) are the canary the rest of the daemon depends on.
    pub fn start(self) -> Result<WatchHandle, FileWatcherError> {
        use notify::RecursiveMode;
        use notify_debouncer_mini::new_debouncer;

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let debounce = Duration::from_millis(self.config.debounce_ms);

        let mut debouncer = new_debouncer(debounce, move |res| {
            // Forward into the tokio-side queue; drop on closed channel.
            let _ = tx.send(res);
        })?;

        // S477 §3.5 (v0.3.7): on Linux, catch inotify watch-limit exhaustion
        // (`notify::ErrorKind::MaxFilesWatch`) and surface as a structured
        // `InotifyLimitExceeded` variant so the wizard can render a banner
        // with the sysctl one-liner. Non-Linux platforms preserve the prior
        // behavior via `?`-propagation.
        #[cfg(target_os = "linux")]
        {
            if let Err(e) = debouncer
                .watcher()
                .watch(&self.vault_root, RecursiveMode::Recursive)
            {
                if matches!(e.kind, notify::ErrorKind::MaxFilesWatch) {
                    let current = read_inotify_limit().unwrap_or(0);
                    return Err(FileWatcherError::InotifyLimitExceeded { current });
                }
                return Err(FileWatcherError::Notify(e));
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            debouncer
                .watcher()
                .watch(&self.vault_root, RecursiveMode::Recursive)?;
        }

        let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);

        // Move the FileWatcher's filtering state into the task. We need to
        // keep `vault_root`, config, journal, burst, device_id.
        let me = Arc::new(self);
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            tracing::info!("file_watcher: shutdown signal received");
                            break;
                        }
                    }
                    maybe = rx.recv() => {
                        match maybe {
                            None => break, // channel closed → debouncer dropped
                            Some(Ok(events)) => {
                                for de in events {
                                    me.handle_debounced_event(&de).await;
                                }
                            }
                            Some(Err(e)) => {
                                tracing::warn!("file_watcher: notify error: {e}");
                            }
                        }
                    }
                }
            }
        });

        Ok(WatchHandle {
            _watcher: debouncer,
            shutdown_tx,
            _task: task,
        })
    }

    async fn handle_debounced_event(&self, de: &notify_debouncer_mini::DebouncedEvent) {
        let kind = if de.path.exists() {
            // We don't know create-vs-modify from debouncer-mini; default
            // to Modified. The push_client / server-side reconciler is
            // create-modify-tolerant (PushAction::Create vs Modify only
            // differs in base_hash handling, which we leave None here).
            WatchEventKindHint::Modified
        } else {
            WatchEventKindHint::Deleted
        };
        let Some(evt) = self.normalize_event(&de.path, kind) else {
            tracing::debug!(
                "file_watcher: dropped event outside vault root: {:?}",
                de.path
            );
            return;
        };
        let decision = self.classify_and_count(&evt);
        // Reflect delete-burst paused-state into the tray so the user sees
        // the safety valve trip in near-real-time.
        if let Some(tray) = &self.tray_state {
            let paused = self.burst.lock().map(|d| d.is_paused()).unwrap_or(false);
            if let Ok(mut w) = tray.write() {
                w.set_delete_burst_paused(paused);
            }
        }
        match decision {
            FilterDecision::Allow(allowed) => {
                // For deletes we also tick the burst detector after allow.
                if matches!(&allowed, WatchEvent::Deleted { .. }) {
                    if let Ok(mut b) = self.burst.lock() {
                        let _ = b.record_delete();
                    }
                }

                let content = match &allowed {
                    WatchEvent::Created { path } | WatchEvent::Modified { path } => {
                        let full = self.vault_root.join(path);
                        std::fs::read(&full).ok()
                    }
                    WatchEvent::Renamed { new_path, .. } => {
                        let full = self.vault_root.join(new_path);
                        std::fs::read(&full).ok()
                    }
                    WatchEvent::Deleted { .. } => None,
                };
                let Some(push_evt) = self.to_push_event(&allowed, content) else {
                    tracing::debug!(
                        "file_watcher: to_push_event returned None for {:?}",
                        allowed
                    );
                    return;
                };
                match self.journal.lock() {
                    Ok(mut j) => {
                        if let Err(e) = j.append(push_evt) {
                            tracing::warn!("file_watcher: journal append failed: {e}");
                        }
                    }
                    Err(e) => {
                        tracing::warn!("file_watcher: journal mutex poisoned: {e}");
                    }
                }
            }
            other => {
                tracing::debug!("file_watcher: dropped event: {:?}", other);
            }
        }
    }
}

/// Hint for normalize_event because notify-debouncer-mini collapses kinds.
#[derive(Debug, Clone, Copy)]
pub enum WatchEventKindHint {
    Created,
    Modified,
    Deleted,
}

// ---------------------------------------------------------------------------
// Free-function helpers
// ---------------------------------------------------------------------------

fn normalize_path(p: &str) -> String {
    p.replace('\\', "/")
}

fn path_extension(p: &str) -> String {
    match p.rfind('.') {
        Some(i) if i + 1 < p.len() => {
            let tail = &p[i + 1..];
            // Avoid grabbing extension from a path-segment like "foo/.bar"
            // where '.' is at index 0 of the basename. In that case treat
            // as no extension.
            if tail.contains('/') {
                return String::new();
            }
            // If the '.' is at the very start of the basename (dotfile w/o
            // a real extension) it's not really an extension.
            let basename_start = p[..i].rfind('/').map(|s| s + 1).unwrap_or(0);
            if basename_start == i {
                return String::new();
            }
            tail.to_string()
        }
        _ => String::new(),
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(bytes);
    let digest = h.finalize();
    hex::encode(digest)
}

fn rewrite_event_path(evt: WatchEvent, path: String) -> WatchEvent {
    match evt {
        WatchEvent::Created { .. } => WatchEvent::Created { path },
        WatchEvent::Modified { .. } => WatchEvent::Modified { path },
        WatchEvent::Deleted { .. } => WatchEvent::Deleted { path },
        // Renames are never path-rewritten via this helper.
        WatchEvent::Renamed { old_path, new_path } => WatchEvent::Renamed { old_path, new_path },
    }
}

/// Re-stamp a drop decision's `path` field to the supplied value so the
/// caller sees the side that triggered the drop (used for rename gating).
fn rewrite_decision_path(d: FilterDecision, p: String) -> FilterDecision {
    match d {
        FilterDecision::Allow(e) => FilterDecision::Allow(e),
        FilterDecision::DropSubstrate { rule, .. } => {
            FilterDecision::DropSubstrate { path: p, rule }
        }
        FilterDecision::DropExtension { ext, .. } => FilterDecision::DropExtension { path: p, ext },
        FilterDecision::DropOutOfScope { .. } => FilterDecision::DropOutOfScope { path: p },
        FilterDecision::DropExclude { exclude_rule, .. } => FilterDecision::DropExclude {
            path: p,
            exclude_rule,
        },
        FilterDecision::DropDeleteBurst { .. } => FilterDecision::DropDeleteBurst { path: p },
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::push_journal::PushJournal;
    use std::time::Duration;
    use tempfile::TempDir;

    fn make_journal(dir: &TempDir) -> Arc<Mutex<PushJournal>> {
        let p = dir.path().join("sync-state").join("push_journal.jsonl");
        Arc::new(Mutex::new(PushJournal::open(&p).unwrap()))
    }

    fn make_burst() -> Arc<Mutex<DeleteBurstDetector>> {
        Arc::new(Mutex::new(DeleteBurstDetector::new(
            20,
            Duration::from_secs(30),
        )))
    }

    fn make_watcher(
        dir: &TempDir,
        scope_roots: Vec<&str>,
        scope_excludes: Vec<&str>,
    ) -> FileWatcher {
        let cfg = FileWatcherConfig {
            allowed_extensions: vec!["md".to_string()],
            scope_roots: scope_roots.into_iter().map(String::from).collect(),
            scope_excludes: scope_excludes.into_iter().map(String::from).collect(),
            debounce_ms: 500,
        };
        FileWatcher::new(
            dir.path().to_path_buf(),
            make_journal(dir),
            make_burst(),
            cfg,
            "dev-test",
        )
        .unwrap()
    }

    fn modified(p: &str) -> WatchEvent {
        WatchEvent::Modified {
            path: p.to_string(),
        }
    }
    fn created(p: &str) -> WatchEvent {
        WatchEvent::Created {
            path: p.to_string(),
        }
    }
    fn deleted(p: &str) -> WatchEvent {
        WatchEvent::Deleted {
            path: p.to_string(),
        }
    }
    fn renamed(o: &str, n: &str) -> WatchEvent {
        WatchEvent::Renamed {
            old_path: o.to_string(),
            new_path: n.to_string(),
        }
    }

    // ---------- classify() ----------

    #[test]
    fn allowed_md_in_scope_passes() {
        let dir = TempDir::new().unwrap();
        let w = make_watcher(&dir, vec![], vec![]);
        let evt = modified("02_Projects/Foo/note.md");
        match w.classify(&evt) {
            FilterDecision::Allow(_) => {}
            other => panic!("expected Allow, got {:?}", other),
        }
    }

    #[test]
    fn substrate_path_drops() {
        let dir = TempDir::new().unwrap();
        let w = make_watcher(&dir, vec![], vec![]);
        let evt = modified("00_VAULT.md");
        match w.classify(&evt) {
            FilterDecision::DropSubstrate { path, rule } => {
                assert_eq!(path, "00_VAULT.md");
                assert_eq!(rule, "00_VAULT.md");
            }
            other => panic!("expected DropSubstrate, got {:?}", other),
        }
    }

    #[test]
    fn substrate_scoped_drops() {
        let dir = TempDir::new().unwrap();
        let w = make_watcher(&dir, vec![], vec![]);
        let evt = modified("02_Projects/Foo/Family.md");
        assert!(matches!(
            w.classify(&evt),
            FilterDecision::DropSubstrate { .. }
        ));
    }

    #[test]
    fn substrate_scoped_at_root_allows() {
        // Family.md at vault root — Wave 1 RASP rebuild made this content.
        let dir = TempDir::new().unwrap();
        let w = make_watcher(&dir, vec![], vec![]);
        let evt = modified("Family.md");
        match w.classify(&evt) {
            FilterDecision::Allow(_) => {}
            other => panic!("expected Allow, got {:?}", other),
        }
    }

    #[test]
    fn claude_md_drops() {
        let dir = TempDir::new().unwrap();
        let w = make_watcher(&dir, vec![], vec![]);
        // Multiple flavors.
        for p in &[
            "CLAUDE.md",
            "02_Projects/Foo/CLAUDE.md",
            "claude.md",
            "02_Projects/x/claude.md",
        ] {
            let evt = modified(p);
            assert!(
                matches!(w.classify(&evt), FilterDecision::DropSubstrate { .. }),
                "expected DropSubstrate for {p}"
            );
        }
    }

    #[test]
    fn wrong_extension_drops() {
        let dir = TempDir::new().unwrap();
        let w = make_watcher(&dir, vec![], vec![]);
        let evt = modified("02_Projects/Foo/bin.exe");
        match w.classify(&evt) {
            FilterDecision::DropExtension { ext, .. } => assert_eq!(ext, "exe"),
            other => panic!("expected DropExtension, got {:?}", other),
        }
    }

    #[test]
    fn obsidian_dir_drops() {
        let dir = TempDir::new().unwrap();
        let w = make_watcher(&dir, vec![], vec![]);
        let evt = modified(".obsidian/workspace.json");
        match w.classify(&evt) {
            FilterDecision::DropExclude { exclude_rule, .. } => {
                assert_eq!(exclude_rule, ".obsidian/");
            }
            other => panic!("expected DropExclude, got {:?}", other),
        }
    }

    #[test]
    fn lattice_sync_dir_drops() {
        let dir = TempDir::new().unwrap();
        let w = make_watcher(&dir, vec![], vec![]);
        let evt = modified(".lattice-sync/anything.txt");
        match w.classify(&evt) {
            FilterDecision::DropExclude { exclude_rule, .. } => {
                assert_eq!(exclude_rule, ".lattice-sync/");
            }
            other => panic!("expected DropExclude, got {:?}", other),
        }
    }

    #[test]
    fn trash_dir_drops() {
        let dir = TempDir::new().unwrap();
        let w = make_watcher(&dir, vec![], vec![]);
        let evt = modified(".trash/X.md");
        match w.classify(&evt) {
            FilterDecision::DropExclude { exclude_rule, .. } => {
                assert_eq!(exclude_rule, ".trash/");
            }
            other => panic!("expected DropExclude, got {:?}", other),
        }
    }

    #[test]
    fn underscore_dot_folder_is_excluded() {
        let dir = TempDir::new().unwrap();
        let w = make_watcher(&dir, vec![], vec![]);
        let evt = created("._/cache.json");
        match w.classify(&evt) {
            FilterDecision::DropExclude { exclude_rule, .. } => {
                assert!(
                    exclude_rule.contains("._/"),
                    "expected ._/ rule, got {exclude_rule}"
                );
            }
            other => panic!("expected DropExclude for ._/cache.json, got {other:?}"),
        }
    }

    #[test]
    fn underscore_dot_folder_excluded_at_any_depth() {
        let dir = TempDir::new().unwrap();
        let w = make_watcher(&dir, vec![], vec![]);
        let evt = created("Mainframe/._/state.json");
        match w.classify(&evt) {
            FilterDecision::DropExclude { .. } => {}
            other => panic!("expected DropExclude for nested ._/, got {other:?}"),
        }
    }

    #[test]
    fn dot_percent_file_at_root_is_excluded() {
        let dir = TempDir::new().unwrap();
        let w = make_watcher(&dir, vec![], vec![]);
        let evt = created(".%scratch.md");
        match w.classify(&evt) {
            FilterDecision::DropExclude { exclude_rule, .. } => {
                assert!(
                    exclude_rule.contains(".%"),
                    "expected .% rule, got {exclude_rule}"
                );
            }
            other => panic!("expected DropExclude, got {other:?}"),
        }
    }

    #[test]
    fn dot_percent_file_nested_is_excluded() {
        let dir = TempDir::new().unwrap();
        let w = make_watcher(&dir, vec![], vec![]);
        let evt = created("02_Projects/.%draft.md");
        match w.classify(&evt) {
            FilterDecision::DropExclude { .. } => {}
            other => panic!("expected DropExclude, got {other:?}"),
        }
    }

    #[test]
    fn dot_percent_folder_and_children_excluded() {
        let dir = TempDir::new().unwrap();
        let w = make_watcher(&dir, vec![], vec![]);
        let evt = created("Mainframe/.%cache/index.md");
        match w.classify(&evt) {
            FilterDecision::DropExclude { .. } => {}
            other => panic!("expected DropExclude for path under .% folder, got {other:?}"),
        }
    }

    #[test]
    fn out_of_scope_drops() {
        let dir = TempDir::new().unwrap();
        let w = make_watcher(&dir, vec!["02_Projects/"], vec![]);
        let evt = modified("00_Inbox/x.md");
        assert!(matches!(
            w.classify(&evt),
            FilterDecision::DropOutOfScope { .. }
        ));
    }

    #[test]
    fn delete_burst_paused_drops_delete() {
        let dir = TempDir::new().unwrap();
        let w = make_watcher(&dir, vec![], vec![]);
        // Force burst into Paused.
        {
            let mut b = w.burst.lock().unwrap();
            // Threshold 20; pump 25 events to trip + pause.
            let now = std::time::Instant::now();
            for i in 0..25 {
                b.record_delete_at(now + Duration::from_millis(i));
            }
            assert!(b.is_paused());
        }
        let evt = deleted("02_Projects/Foo/note.md");
        match w.classify(&evt) {
            FilterDecision::DropDeleteBurst { path } => {
                assert_eq!(path, "02_Projects/Foo/note.md");
            }
            other => panic!("expected DropDeleteBurst, got {:?}", other),
        }
    }

    #[test]
    fn delete_burst_not_paused_allows_delete() {
        let dir = TempDir::new().unwrap();
        let w = make_watcher(&dir, vec![], vec![]);
        let evt = deleted("02_Projects/Foo/note.md");
        match w.classify(&evt) {
            FilterDecision::Allow(WatchEvent::Deleted { path }) => {
                assert_eq!(path, "02_Projects/Foo/note.md");
            }
            other => panic!("expected Allow(Deleted), got {:?}", other),
        }
    }

    #[test]
    fn rename_with_filtered_side_drops() {
        // Source = substrate; destination = content. Drop the whole rename.
        let dir = TempDir::new().unwrap();
        let w = make_watcher(&dir, vec![], vec![]);
        let evt = renamed("02_Projects/Protocols/x.md", "02_Projects/Foo/x.md");
        match w.classify(&evt) {
            FilterDecision::DropSubstrate { path, .. } => {
                assert_eq!(path, "02_Projects/Protocols/x.md");
            }
            other => panic!("expected DropSubstrate (source-side), got {:?}", other),
        }
    }

    #[test]
    fn rename_destination_substrate_also_drops() {
        // Symmetric: destination is substrate.
        let dir = TempDir::new().unwrap();
        let w = make_watcher(&dir, vec![], vec![]);
        let evt = renamed("02_Projects/Foo/x.md", "02_Projects/Protocols/x.md");
        match w.classify(&evt) {
            FilterDecision::DropSubstrate { path, .. } => {
                assert_eq!(path, "02_Projects/Protocols/x.md");
            }
            other => panic!("expected DropSubstrate (dest-side), got {:?}", other),
        }
    }

    #[test]
    fn rename_both_allowed_passes() {
        let dir = TempDir::new().unwrap();
        let w = make_watcher(&dir, vec![], vec![]);
        let evt = renamed("02_Projects/Foo/a.md", "02_Projects/Foo/b.md");
        match w.classify(&evt) {
            FilterDecision::Allow(WatchEvent::Renamed { old_path, new_path }) => {
                assert_eq!(old_path, "02_Projects/Foo/a.md");
                assert_eq!(new_path, "02_Projects/Foo/b.md");
            }
            other => panic!("expected Allow(Renamed), got {:?}", other),
        }
    }

    #[test]
    fn windows_backslash_path_normalized() {
        let dir = TempDir::new().unwrap();
        let w = make_watcher(&dir, vec![], vec![]);
        let evt = modified("02_Projects\\Foo\\x.md");
        match w.classify(&evt) {
            FilterDecision::Allow(WatchEvent::Modified { path }) => {
                assert_eq!(path, "02_Projects/Foo/x.md");
            }
            other => panic!("expected Allow with normalized path, got {:?}", other),
        }
    }

    #[test]
    fn windows_backslash_substrate_still_drops() {
        // RASP fence already handles backslashes, but verify end-to-end.
        let dir = TempDir::new().unwrap();
        let w = make_watcher(&dir, vec![], vec![]);
        let evt = modified("02_Projects\\Protocols\\foo.md");
        assert!(matches!(
            w.classify(&evt),
            FilterDecision::DropSubstrate { .. }
        ));
    }

    #[test]
    fn excludes_layered_with_user_config() {
        // User-config exclude `_archive/` honored alongside hardcoded `.obsidian/`.
        let dir = TempDir::new().unwrap();
        let w = make_watcher(&dir, vec![], vec!["_archive/"]);
        // Hardcoded:
        let evt1 = modified(".obsidian/x.json");
        match w.classify(&evt1) {
            FilterDecision::DropExclude { exclude_rule, .. } => {
                assert_eq!(exclude_rule, ".obsidian/");
            }
            other => panic!("expected DropExclude hardcoded, got {:?}", other),
        }
        // User-config:
        let evt2 = modified("_archive/old.md");
        match w.classify(&evt2) {
            FilterDecision::DropExclude { exclude_rule, .. } => {
                assert_eq!(exclude_rule, "_archive/");
            }
            other => panic!("expected DropExclude user-config, got {:?}", other),
        }
    }

    #[test]
    fn path_traversal_drops() {
        let dir = TempDir::new().unwrap();
        let w = make_watcher(&dir, vec![], vec![]);
        let evt = modified("../escape/x.md");
        assert!(matches!(
            w.classify(&evt),
            FilterDecision::DropOutOfScope { .. }
        ));
    }

    #[test]
    fn absolute_path_drops() {
        let dir = TempDir::new().unwrap();
        let w = make_watcher(&dir, vec![], vec![]);
        let evt = modified("/etc/passwd");
        assert!(matches!(
            w.classify(&evt),
            FilterDecision::DropOutOfScope { .. }
        ));
    }

    #[test]
    fn create_event_inscope_allows() {
        let dir = TempDir::new().unwrap();
        let w = make_watcher(&dir, vec![], vec![]);
        let evt = created("02_Projects/Foo/new.md");
        assert!(matches!(w.classify(&evt), FilterDecision::Allow(_)));
    }

    // ---------- to_push_event() ----------

    #[test]
    fn created_yields_create_action_with_content() {
        let dir = TempDir::new().unwrap();
        let w = make_watcher(&dir, vec![], vec![]);
        let evt = created("02_Projects/Foo/n.md");
        let body = b"# hello".to_vec();
        let push = w.to_push_event(&evt, Some(body.clone())).unwrap();
        assert_eq!(push.action, PushAction::Create);
        assert_eq!(push.path, "02_Projects/Foo/n.md");
        assert_eq!(push.content_bytes, Some(body));
        assert!(push.base_hash.is_none());
        assert_eq!(push.content_sha.len(), 64);
        assert_eq!(push.device_id, "dev-test");
    }

    #[test]
    fn modified_yields_modify_action() {
        let dir = TempDir::new().unwrap();
        let w = make_watcher(&dir, vec![], vec![]);
        let evt = modified("02_Projects/Foo/n.md");
        let body = b"# updated".to_vec();
        let push = w.to_push_event(&evt, Some(body.clone())).unwrap();
        assert_eq!(push.action, PushAction::Modify);
        assert_eq!(push.content_bytes, Some(body));
    }

    #[test]
    fn deleted_yields_delete_action_no_content() {
        let dir = TempDir::new().unwrap();
        let w = make_watcher(&dir, vec![], vec![]);
        let evt = deleted("02_Projects/Foo/n.md");
        let push = w.to_push_event(&evt, None).unwrap();
        assert_eq!(push.action, PushAction::Delete);
        assert!(push.content_bytes.is_none());
        assert_eq!(push.content_sha, "");
    }

    #[test]
    fn renamed_yields_create_at_new_path() {
        // Documented design decision: rename emits a SINGLE PushEvent
        // (PushAction::Create) at new_path. Old path's residual handled
        // by the materializer-side index, never by a synthetic Delete
        // (which is the nexus-sync rename bug v0.3 explicitly fixes).
        let dir = TempDir::new().unwrap();
        let w = make_watcher(&dir, vec![], vec![]);
        let evt = renamed("02_Projects/Foo/a.md", "02_Projects/Foo/b.md");
        let body = b"# moved".to_vec();
        let push = w.to_push_event(&evt, Some(body.clone())).unwrap();
        assert_eq!(push.action, PushAction::Create);
        assert_eq!(push.path, "02_Projects/Foo/b.md");
        assert_eq!(push.content_bytes, Some(body));
    }

    #[test]
    fn create_without_content_returns_none() {
        // Caller MUST supply content for Create — None signals "couldn't read".
        let dir = TempDir::new().unwrap();
        let w = make_watcher(&dir, vec![], vec![]);
        let evt = created("02_Projects/Foo/n.md");
        assert!(w.to_push_event(&evt, None).is_none());
    }

    // ---------- normalize_path() ----------

    #[test]
    fn normalize_path_strips_vault_root() {
        let dir = TempDir::new().unwrap();
        // Touch a file so canonicalize succeeds.
        std::fs::create_dir_all(dir.path().join("02_Projects/Foo")).unwrap();
        std::fs::write(dir.path().join("02_Projects/Foo/n.md"), b"x").unwrap();

        let w = make_watcher(&dir, vec![], vec![]);
        let abs = dir.path().join("02_Projects/Foo/n.md");
        let norm = w.normalize_path(&abs).unwrap();
        assert_eq!(norm, "02_Projects/Foo/n.md");
    }

    #[test]
    fn normalize_path_outside_vault_returns_none() {
        let dir = TempDir::new().unwrap();
        let outside = TempDir::new().unwrap();
        std::fs::write(outside.path().join("escape.md"), b"x").unwrap();
        let w = make_watcher(&dir, vec![], vec![]);
        assert!(w
            .normalize_path(&outside.path().join("escape.md"))
            .is_none());
    }

    // ---------- helper unit tests ----------

    #[test]
    fn extension_helper_basic() {
        assert_eq!(path_extension("02_Projects/x.md"), "md");
        assert_eq!(path_extension("a.b.c.md"), "md");
        assert_eq!(path_extension("noext"), "");
        // Hidden-file at root → no extension by our convention.
        assert_eq!(path_extension(".hidden"), "");
        // Hidden-file in subdir → no extension either.
        assert_eq!(path_extension("dir/.hidden"), "");
    }

    #[test]
    fn sha256_helper_known_vector() {
        // Standard test vector for sha256("abc").
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    // ---------- FS-integration (#[ignore]) ----------

    /// Local-only smoke test: actually spawn the OS watcher, write a file,
    /// and confirm the journal grows. Ignored in CI because:
    ///   - tokio runtime + notify timing is flaky on Windows GitHub Actions runners.
    ///   - notify-debouncer-mini fires inside a 500ms window so the test
    ///     sleeps, which is hostile to CI determinism.
    /// Run locally with `cargo test --lib file_watcher -- --ignored`.
    #[tokio::test]
    #[ignore]
    async fn fs_integration_smoke() {
        let dir = TempDir::new().unwrap();
        std::fs::create_dir_all(dir.path().join("02_Projects/Foo")).unwrap();
        let journal = make_journal(&dir);
        let burst = make_burst();
        let cfg = FileWatcherConfig::default();
        let w = FileWatcher::new(
            dir.path().to_path_buf(),
            journal.clone(),
            burst,
            cfg,
            "dev-test",
        )
        .unwrap();
        let _handle = w.start().unwrap();

        // Give the watcher a moment to attach.
        tokio::time::sleep(Duration::from_millis(200)).await;

        std::fs::write(dir.path().join("02_Projects/Foo/n.md"), b"# hi").unwrap();

        // Wait for debounce + processing.
        tokio::time::sleep(Duration::from_millis(1500)).await;

        let j = journal.lock().unwrap();
        assert!(!j.is_empty(), "expected at least one event in journal");
    }

    /// Symlink-escape FS test — requires SeCreateSymbolicLinkPrivilege on
    /// Windows (Developer Mode or admin). Skip cleanly if unavailable.
    #[test]
    #[ignore]
    fn symlink_escape_canonicalized_and_dropped() {
        let dir = TempDir::new().unwrap();
        let outside = TempDir::new().unwrap();
        std::fs::write(outside.path().join("secret.md"), b"x").unwrap();

        // Best-effort symlink creation. If it fails (typical Windows w/o
        // Developer Mode), the test is a no-op.
        let link = dir.path().join("escape.md");
        let symlink_result = {
            #[cfg(windows)]
            {
                std::os::windows::fs::symlink_file(outside.path().join("secret.md"), &link)
            }
            #[cfg(unix)]
            {
                std::os::unix::fs::symlink(outside.path().join("secret.md"), &link)
            }
            #[cfg(not(any(windows, unix)))]
            {
                let _: PathBuf = link.clone();
                Err::<(), std::io::Error>(std::io::Error::other("unsupported"))
            }
        };
        if symlink_result.is_err() {
            // Privilege not held — bail out without failing.
            eprintln!("skipping symlink test: cannot create symlinks here");
            return;
        }

        let w = make_watcher(&dir, vec![], vec![]);
        // The symlinked file's canonical form is outside the vault root —
        // normalize_path must return None.
        assert!(w.normalize_path(&link).is_none());
    }

    // ---------- v0.3 tray wire-up sanity ----------

    fn make_shared_tray() -> crate::tray_state::SharedTrayState {
        std::sync::Arc::new(std::sync::RwLock::new(crate::tray_state::TrayState::new(
            "sub".into(),
            "https://x".into(),
            std::path::PathBuf::from("/v"),
        )))
    }

    #[test]
    fn classify_and_count_increments_filtered_counters() {
        let dir = TempDir::new().unwrap();
        let tray = make_shared_tray();
        let w = make_watcher(&dir, vec![], vec![]).with_tray_state(tray.clone());

        // Substrate path → DropSubstrate.
        let _ = w.classify_and_count(&modified("00_VAULT.md"));
        // Extension drop.
        let _ = w.classify_and_count(&modified("notes/x.exe"));
        // Exclude (hardcoded).
        let _ = w.classify_and_count(&modified(".obsidian/workspace.json"));
        // Allow (no counter bump).
        let _ = w.classify_and_count(&modified("notes/ok.md"));

        let s = tray.read().unwrap();
        assert_eq!(s.events_dropped_substrate, 1);
        assert_eq!(s.events_dropped_extension, 1);
        assert_eq!(s.events_dropped_excludes, 1);
        // events_filtered is the sum (each dropped event bumps the rollup).
        assert_eq!(s.events_filtered, 3);
    }

    // ---------- S477 §3.5 inotify-limit detection (Linux-only) ----------

    #[test]
    fn inotify_limit_exceeded_error_renders_user_facing_message() {
        // Pure Display assertion — no Linux-syscall dependency. The error
        // variant must render with the canonical "inotify watch limit"
        // phrase so log scrapers + wizard-side message templates can match.
        let err = FileWatcherError::InotifyLimitExceeded { current: 8 };
        let rendered = err.to_string();
        assert!(
            rendered.contains("inotify watch limit"),
            "error must mention 'inotify watch limit'; got: {rendered}"
        );
        assert!(
            rendered.contains("current=8"),
            "error must include the current limit; got: {rendered}"
        );
        assert!(
            rendered.contains("fs.inotify.max_user_watches"),
            "error must point user at sysctl knob; got: {rendered}"
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    #[ignore]
    fn inotify_limit_exceeded_surfaces_structured_error() {
        // Local-only repro: drop fs.inotify.max_user_watches to a tiny value
        // (`sudo sysctl -w fs.inotify.max_user_watches=8`), seed a directory
        // with more entries than the limit, and verify FileWatcher::start()
        // returns InotifyLimitExceeded with the live limit attached.
        //
        // Run with: `sudo sysctl -w fs.inotify.max_user_watches=8 \
        //   && cargo test --lib inotify_limit_exceeded_surfaces -- --ignored`
        //
        // Skipped on CI: requires root + a low sysctl value, both unsafe to
        // set in shared runners.
        let dir = TempDir::new().unwrap();
        for i in 0..64 {
            std::fs::create_dir_all(dir.path().join(format!("sub{i}"))).unwrap();
        }
        let w = make_watcher(&dir, vec![], vec![]);
        match w.start() {
            Err(FileWatcherError::InotifyLimitExceeded { current }) => {
                assert!(
                    current > 0,
                    "expected non-zero current limit, got {current}"
                );
            }
            other => panic!("expected InotifyLimitExceeded, got {other:?}"),
        }
    }
}
