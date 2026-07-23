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

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::push_journal::{
    new_event_id, JournalError, PushAction, PushBase, PushEvent, PushJournal, CURRENT_SCHEMA,
};
use crate::rasp_fence::{classify_path, is_junk_path, PathClassification};
use crate::redflag::DeleteBurstDetector;
use crate::tray_state::SharedTrayState;

/// Hardcoded directory excludes — applied regardless of user config.
/// Match against the forward-slash vault-relative path. A path matches if it
/// either starts with the prefix (root-level) or contains `/<prefix>` (nested).
const HARDCODED_EXCLUDES: &[&str] = &[
    ".obsidian/",
    ".lattice-sync/",
    // Daemon runtime/state dir. NO trailing slash so the prefix also matches
    // rotated/stale variants like `.lattice-runtime.STALE-S477/`. When
    // `sync_roots` is empty the watcher roots at `vaults_root` (the PARENT of
    // the vault), so the daemon's own `.lattice-runtime` tree sits in scope and
    // was being enqueued as pushes (thousands of self-generated junk entries).
    ".lattice-runtime",
    ".trash/",
    "._/", // S477: convention for organized machine-local trees
    // A node_modules/ under the vault must never sync — it inflated the push
    // journal with tens of thousands of entries (2026-06-14). Kept aligned with
    // verify_repair::VerifyRepairConfig::default().hardcoded_excludes.
    "node_modules/",
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
    _watcher: notify_debouncer_full::Debouncer<
        notify::RecommendedWatcher,
        notify_debouncer_full::RecommendedCache,
    >,
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

/// Storm circuit-breaker threshold: this many CONSECUTIVE journal
/// `CapacityExceeded` failures trips the breaker. Normal operation never sees a
/// run this long (the journal cap is 100 MB and push_client drains it); a long
/// consecutive run means the journal is wedged at capacity and the file_watcher
/// is hot-spinning on it (the S489/S490 storm: 745k failures, 85% CPU, 290 MB
/// logs). Rather than spin and do harm, we trip, write a loud diagnostic, and
/// halt the watcher so the cause is recoverable from the log post-mortem.
const STORM_BREAKER_THRESHOLD: u64 = 200;

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
    /// Count of CONSECUTIVE journal `CapacityExceeded` append failures. Reset
    /// to 0 on any successful append. Feeds the storm circuit-breaker.
    cap_fail_streak: Arc<AtomicU64>,
    /// Set true once the storm breaker trips. The FS-watcher loop checks this
    /// and halts so a wedged-at-capacity journal can't drive a hot-spin.
    fenced: Arc<AtomicBool>,
    /// Echo guard (S492): shared with the materializer. Before enqueueing a
    /// create/modify event, the watcher checks whether the file's current
    /// content hash matches a recent materializer write; if so the event is a
    /// server echo (not a user edit) and is skipped — breaking the
    /// SSE->materialize->watcher->push feedback loop that flooded the journal.
    echo_guard: Option<Arc<crate::echo_guard::EchoGuard>>,
    /// Enqueue-time no-op dedup state: the raw content sha LAST ENQUEUED for
    /// each (watcher-relative) path. Before journaling a create/modify, if the
    /// file's current bytes hash to the same value we last enqueued for that
    /// path, the event is a redundant no-op (touch, atime, re-scan, repeated FS
    /// event for unchanged content) and is skipped — so genuine edits do not
    /// queue behind thousands of redundant no-ops. Keyed by the watcher's own
    /// path, so it is independent of the push/pull path-prefix convention and
    /// can NEVER suppress a different file or a real content change. Bounded by
    /// the number of distinct paths edited this session (≈ vault size). A delete
    /// clears its entry so a later recreate re-enqueues.
    ///
    /// D6 (S511, TKT-2dc9a17e): this in-memory hash is NO LONGER sufficient on
    /// its own. A push can be dropped (e.g. server HTTP 400 skip) WITHOUT being
    /// accepted, yet the enqueued_hashes entry is never cleared, so a later edit
    /// that reverts to that previously-enqueued-but-never-accepted content was
    /// silently dropped. The dedup is now GATED on the ShadowStore's
    /// last-server-ACCEPTED hash: an event is suppressed only when the current
    /// sha equals BOTH the last-enqueued hash AND the shadow's last-accepted
    /// hash for that path (i.e. the server has actually confirmed that content).
    enqueued_hashes: Arc<Mutex<HashMap<String, String>>>,
    /// D6 (S511): shared shadow store, the source of the last-server-ACCEPTED
    /// hash that gates the enqueue dedup (above). `None` keeps pre-S511 behavior
    /// (enqueued_hashes-only dedup); the production wire-up always sets it.
    shadow_store: Option<Arc<crate::sync_shadow::ShadowStore>>,
    /// D7 (S511): shared liveness handle. The watcher loop heartbeats into it
    /// each iteration (`mark_watcher_alive`) and sets the fence
    /// (`set_watcher_fenced`) when the storm breaker trips, so the watchdog can
    /// auto-recover a dead/fenced watcher REGARDLESS of pending count. `None`
    /// keeps pre-S511 behavior (no heartbeat); the production wire-up sets it.
    sync_health: Option<Arc<crate::sync_health::SyncHealth>>,
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
            cap_fail_streak: Arc::new(AtomicU64::new(0)),
            fenced: Arc::new(AtomicBool::new(false)),
            echo_guard: None,
            enqueued_hashes: Arc::new(Mutex::new(HashMap::new())),
            shadow_store: None,
            sync_health: None,
        })
    }

    /// D6 (S511): attach the shared shadow store so the enqueue dedup is gated
    /// on the last-server-ACCEPTED hash (not the in-memory enqueued hash alone).
    pub fn with_shadow_store(mut self, store: Arc<crate::sync_shadow::ShadowStore>) -> Self {
        self.shadow_store = Some(store);
        self
    }

    /// D2/B2'd (v0.4.28): REPLACE the internally-created enqueue-dedup map
    /// with one shared with the push_client, so an ack-materialize-back
    /// rewrite can advance `map[path]` to the canonical sha and a later touch
    /// event with unchanged bytes is suppressed by the layer-2 dedup below
    /// (enqueued_match && shadow_confirms) instead of emitting an idempotent
    /// echo push. Backwards-compatible: without it the watcher keeps its own
    /// private map (pre-v0.4.28 behavior).
    pub fn with_enqueued_hashes(mut self, map: Arc<Mutex<HashMap<String, String>>>) -> Self {
        self.enqueued_hashes = map;
        self
    }

    /// D7 (S511): attach the shared liveness handle so the watcher loop
    /// heartbeats and the storm-fence sets the watcher-fenced flag for the
    /// auto-recovery watchdog.
    pub fn with_sync_health(mut self, health: Arc<crate::sync_health::SyncHealth>) -> Self {
        self.sync_health = Some(health);
        self
    }

    /// Builder-style: attach the shared [`EchoGuard`](crate::echo_guard::EchoGuard)
    /// so materializer-written files are recognized as echoes and skipped at
    /// enqueue. Backwards-compatible — without it, no echo-suppression (every
    /// write is enqueued, the pre-S492 behavior).
    pub fn with_echo_guard(mut self, guard: Arc<crate::echo_guard::EchoGuard>) -> Self {
        self.echo_guard = Some(guard);
        self
    }

    /// True once the storm circuit-breaker has tripped. The FS-watcher loop
    /// halts when this flips; exposed for tests + the loop's halt check.
    pub fn is_fenced(&self) -> bool {
        self.fenced.load(Ordering::Relaxed)
    }

    /// D7 (S511): heartbeat the shared liveness handle (no-op if unwired).
    fn mark_watcher_alive(&self) {
        if let Some(h) = &self.sync_health {
            h.mark_watcher_alive();
        }
    }

    /// D7 (S511): publish the storm-fence into the shared liveness handle so the
    /// watchdog auto-recovers a fenced watcher (no-op if unwired).
    fn mark_watcher_fenced(&self) {
        if let Some(h) = &self.sync_health {
            h.set_watcher_fenced();
        }
    }

    /// Record the outcome of a journal append for the storm breaker.
    /// `is_capacity` = the append failed with `CapacityExceeded`. A successful
    /// (or non-capacity) append resets the consecutive streak. Returns true iff
    /// THIS call trips the breaker (crosses the threshold for the first time),
    /// so the caller logs the loud diagnostic exactly once.
    fn record_append_capacity(&self, is_capacity: bool) -> bool {
        if !is_capacity {
            self.cap_fail_streak.store(0, Ordering::Relaxed);
            return false;
        }
        let streak = self.cap_fail_streak.fetch_add(1, Ordering::Relaxed) + 1;
        if streak >= STORM_BREAKER_THRESHOLD {
            // Trip exactly once (first thread/iteration to swap false->true).
            return !self.fenced.swap(true, Ordering::Relaxed);
        }
        false
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
        // remaining absolute paths are bugs). `..` is only a traversal when it
        // is a whole path *segment* — a title ending in `...` (S490) merely
        // *contains* `..` and must NOT be dropped.
        if norm.starts_with('/')
            || norm.starts_with('\\')
            || crate::scope::has_dotdot_segment(&norm)
        {
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

        // (2d) D5 (S511, TKT-2dc9a17e): conflict-copy stashes
        // (`<stem>.conflict-from-<host>-<seq>.md`) must NEVER be pushed back to
        // the server or re-fanned to other hosts (which would re-conflict and
        // multiply copies across the fleet). The materializer writes them as
        // local-only preservation siblings; drop them here at classify so they
        // are inert to sync. Matched by the structural filename parser so a
        // legitimately-named note is never falsely excluded.
        if is_conflict_copy(&norm) {
            return FilterDecision::DropExclude {
                path: norm,
                exclude_rule: "conflict-copy".to_string(),
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
                    base_hash: PushBase::Unknown,
                    content_sha: sha256_hex(&bytes),
                    // Lazy ref (v0.4.7): hash the body for content_sha but do
                    // NOT embed it. push_client reads the file from disk at
                    // drain time (it already supports content_bytes=None, as
                    // verify_repair's refs do). Embedding full bodies bloated
                    // the journal past its 100 MB cap during reconciliation
                    // backlogs and drove the file_watcher storm (S489/S490);
                    // refs stay tiny + constant-size regardless of file size.
                    content_bytes: None,
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
                    // R4 / F-B3.2 (TKT-989ad5f2): a real-time watcher event has
                    // no known CAS base, so it is `Unknown` — push_client sources
                    // the base from the shadow store (I29). This is deliberately
                    // distinct from a reconcile `NoRow`, which must NOT be
                    // shadow-backfilled; the pre-fix `None` conflated the two.
                    base_hash: PushBase::Unknown,
                    content_sha: sha256_hex(&bytes),
                    // Lazy ref (v0.4.7): hash the body for content_sha but do
                    // NOT embed it. push_client reads the file from disk at
                    // drain time (it already supports content_bytes=None, as
                    // verify_repair's refs do). Embedding full bodies bloated
                    // the journal past its 100 MB cap during reconciliation
                    // backlogs and drove the file_watcher storm (S489/S490);
                    // refs stay tiny + constant-size regardless of file size.
                    content_bytes: None,
                    queued_at: now,
                    device_id: self.device_id.clone(),
                })
            }
            WatchEvent::Deleted { path } => Some(PushEvent {
                schema_version: CURRENT_SCHEMA,
                id: new_event_id(),
                path: path.clone(),
                action: PushAction::Delete,
                base_hash: PushBase::Unknown,
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
                    base_hash: PushBase::Unknown,
                    content_sha: sha256_hex(&bytes),
                    // Lazy ref (v0.4.7): hash the body for content_sha but do
                    // NOT embed it. push_client reads the file from disk at
                    // drain time (it already supports content_bytes=None, as
                    // verify_repair's refs do). Embedding full bodies bloated
                    // the journal past its 100 MB cap during reconciliation
                    // backlogs and drove the file_watcher storm (S489/S490);
                    // refs stay tiny + constant-size regardless of file size.
                    content_bytes: None,
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

    /// Take a raw notify event path and produce a `WatchEvent` of the given
    /// variant kind. Used by the FS-watcher loop (per-path, post kind-filter).
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
    /// NOTE: as of v0.4.9 we use notify-debouncer-FULL, which preserves the raw
    /// `notify::Event` kind. The task loop FIRST drops non-mutating kinds
    /// (`Access(*)`, `Modify(Metadata)` — see `is_mutating_kind`), then funnels
    /// the surviving paths through `classify` into the journal. We still derive
    /// create-vs-modify-vs-delete from path existence (the server reconciler is
    /// create/modify tolerant):
    ///
    ///   - Path exists → emit Modified.
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
        use notify_debouncer_full::new_debouncer;

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let debounce = Duration::from_millis(self.config.debounce_ms);

        // v0.4.9: notify-debouncer-FULL (was -mini). full preserves the raw
        // `notify::Event` kind on each DebouncedEvent, which -mini collapsed to a
        // coarse `Any`. We NEED the kind: notify 7.0's inotify mask hardcodes
        // IN_OPEN + IN_ATTRIB, so every file *read* emits `Access(Open)` and every
        // metadata/atime touch emits `Modify(Metadata)`. Those are NOT content
        // changes — forwarding them as Modified pushes was the reconciliation-
        // backstop journal storm: verify_repair's hash walk opens all ~28K .md
        // files, so each backstop produced ~28K spurious pushes that filled the
        // 100 MB journal and tripped the breaker. We filter them by kind in the
        // task loop below (see `is_mutating_kind`).
        let mut debouncer = new_debouncer(
            debounce,
            None,
            move |res: notify_debouncer_full::DebounceEventResult| {
                // Forward into the tokio-side queue; drop on closed channel.
                let _ = tx.send(res);
            },
        )?;

        // S477 §3.5 (v0.3.7): on Linux, catch inotify watch-limit exhaustion
        // (`notify::ErrorKind::MaxFilesWatch`) and surface as a structured
        // `InotifyLimitExceeded` variant so the wizard can render a banner
        // with the sysctl one-liner. Non-Linux platforms preserve the prior
        // behavior via `?`-propagation.
        #[cfg(target_os = "linux")]
        {
            if let Err(e) = debouncer.watch(&self.vault_root, RecursiveMode::Recursive) {
                if matches!(e.kind, notify::ErrorKind::MaxFilesWatch) {
                    let current = read_inotify_limit().unwrap_or(0);
                    return Err(FileWatcherError::InotifyLimitExceeded { current });
                }
                return Err(FileWatcherError::Notify(e));
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            debouncer.watch(&self.vault_root, RecursiveMode::Recursive)?;
        }

        let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);

        // Move the FileWatcher's filtering state into the task. We need to
        // keep `vault_root`, config, journal, burst, device_id.
        let me = Arc::new(self);
        let task = tokio::spawn(async move {
            // D7 (S511): periodic liveness heartbeat so an IDLE-but-alive watcher
            // (quiet vault, no FS events) still stamps SyncHealth and is not
            // mistaken for a dead/hung watch loop. 30s is well under the
            // watcher-stale window (DEFAULT_WATCHER_STALE_SECS = 300s).
            let mut heartbeat = tokio::time::interval(Duration::from_secs(30));
            // Stamp once immediately so the watchdog's staleness check arms from
            // a known-alive baseline the moment the watcher starts.
            me.mark_watcher_alive();
            loop {
                tokio::select! {
                    _ = heartbeat.tick() => {
                        // Idle or busy, the loop is alive: stamp the heartbeat.
                        me.mark_watcher_alive();
                    }
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            tracing::info!("file_watcher: shutdown signal received");
                            break;
                        }
                    }
                    maybe = rx.recv() => {
                        // An event batch arrived: the loop is alive.
                        me.mark_watcher_alive();
                        match maybe {
                            None => break, // channel closed → debouncer dropped
                            Some(Ok(events)) => {
                                for de in events {
                                    // v0.4.9 ROOT-CAUSE FIX: drop non-mutating
                                    // events. notify 7.0 watches IN_OPEN + IN_ATTRIB,
                                    // so reads emit `Access(*)` and atime/perm touches
                                    // emit `Modify(Metadata)`. Neither is a content
                                    // change. Forwarding reads as Modified pushes was
                                    // the backstop storm; dropping them here also
                                    // breaks the read→event→read feedback loop (the
                                    // handler below re-reads each file to hash it,
                                    // which would otherwise re-fire `Access(Open)`).
                                    if !is_mutating_kind(&de.event.kind) {
                                        tracing::trace!(
                                            kind = ?de.event.kind,
                                            "file_watcher: dropping non-mutating event"
                                        );
                                        continue;
                                    }
                                    for path in &de.event.paths {
                                        me.handle_fs_path(path).await;
                                    }
                                }
                                // Storm circuit-breaker: if a wedged-at-capacity
                                // journal tripped the breaker, halt the loop
                                // rather than hot-spin. The loud diagnostic was
                                // already logged at the trip; this just stops.
                                if me.is_fenced() {
                                    // D7 (S511): publish the fence into SyncHealth
                                    // so the watchdog AUTO-RECOVERS (restart =
                                    // re-establish the watch) regardless of pending
                                    // count. The old behavior just logged "restart
                                    // the daemon" and nothing restarted.
                                    me.mark_watcher_fenced();
                                    tracing::error!(
                                        "file_watcher: storm circuit-breaker fenced, halting event loop; SyncHealth watcher_fenced set so the watchdog will auto-recover (restart). See the STORM CIRCUIT-BREAKER diagnostic above."
                                    );
                                    break;
                                }
                            }
                            Some(Err(errors)) => {
                                // debouncer-full reports a batch of notify errors.
                                for e in &errors {
                                    tracing::warn!("file_watcher: notify error: {e}");
                                }
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

    async fn handle_fs_path(&self, abs: &Path) {
        let kind = if abs.exists() {
            // We keep the exists-based create-vs-modify heuristic: default to
            // Modified for any path that still exists. The push_client /
            // server-side reconciler is create-modify-tolerant (PushAction::Create
            // vs Modify only differs in base_hash handling, which we leave None
            // here). The caller has already filtered to mutating event kinds.
            WatchEventKindHint::Modified
        } else {
            WatchEventKindHint::Deleted
        };
        let Some(evt) = self.normalize_event(abs, kind) else {
            tracing::debug!("file_watcher: dropped event outside vault root: {abs:?}");
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
                // Raw content sha of the current file bytes — shared by the
                // echo-guard and the no-op dedup below (compute once).
                let content_sha = content.as_ref().map(|b| sha256_hex(b));
                let write_path = match &allowed {
                    WatchEvent::Created { path } | WatchEvent::Modified { path } => {
                        Some(path.as_str())
                    }
                    WatchEvent::Renamed { new_path, .. } => Some(new_path.as_str()),
                    WatchEvent::Deleted { .. } => None,
                };
                // S492 echo-suppression: if the file's current content matches a
                // recent materializer write, this event is a server echo (the
                // SSE consumer just wrote it), NOT a user edit — skip it instead
                // of enqueueing a spurious local push. Exact (path, sha) match
                // only, so a genuine edit (different sha) is never suppressed.
                if let (Some(guard), Some(sha), Some(p)) =
                    (&self.echo_guard, &content_sha, write_path)
                {
                    if guard.is_echo(p, sha) {
                        tracing::debug!("file_watcher: skipping materializer echo for {p}");
                        return;
                    }
                }
                // Enqueue-time no-op dedup (journal-bloat fix): if the file's
                // current bytes are byte-identical to what we LAST ENQUEUED for
                // this path, this event is a redundant no-op (touch / atime /
                // re-scan / repeated FS event for unchanged content) — skip it so
                // genuine edits don't queue behind thousands of redundant no-ops
                // (the 249k-entry journal). RAW-sha equality ⟹ truly identical
                // bytes for the SAME path, so a real edit (any byte change, incl.
                // frontmatter) differs and is NEVER suppressed; keying on the
                // watcher's own path means we can't suppress a different file.
                // A delete clears the entry (handled below) so a recreate
                // re-enqueues even if its bytes match the pre-delete content.
                //
                // D6 (S511, TKT-2dc9a17e): the dedup is now GATED on the
                // ShadowStore's last-server-ACCEPTED hash, NOT the in-memory
                // enqueued hash alone. An event is suppressed only when the
                // current sha equals BOTH (a) the last-enqueued hash for this
                // path AND (b) the shadow's last-accepted hash for this path.
                // Rationale: a push can be DROPPED (e.g. server HTTP 400 skip)
                // without ever being accepted, yet enqueued_hashes is never
                // cleared, so a later edit reverting to that content used to be
                // silently dropped (FM1 data loss). Requiring server-confirmed
                // acceptance means un-accepted content is always re-enqueued.
                // When no shadow store is wired, fall back to the prior
                // enqueued-hash-only behavior (back-compat).
                if let (Some(sha), Some(p)) = (&content_sha, write_path) {
                    let enqueued_match = self
                        .enqueued_hashes
                        .lock()
                        .map(|m| m.get(p).map(String::as_str) == Some(sha.as_str()))
                        .unwrap_or(false);
                    let shadow_confirms = match &self.shadow_store {
                        // Suppress only if the server has ACCEPTED this exact
                        // content for this path (its last-synced hash == current).
                        Some(store) => store.get(p).as_deref() == Some(sha.as_str()),
                        // No shadow store: preserve pre-S511 behavior.
                        None => true,
                    };
                    if enqueued_match && shadow_confirms {
                        tracing::debug!(
                            "file_watcher: skipping redundant no-op (content == last enqueued AND server-accepted) for {p}"
                        );
                        return;
                    }
                }
                // A delete invalidates any remembered enqueue-hash for the path
                // so a later recreate is never falsely deduped.
                if let WatchEvent::Deleted { path } = &allowed {
                    if let Ok(mut m) = self.enqueued_hashes.lock() {
                        m.remove(path);
                    }
                }
                let Some(push_evt) = self.to_push_event(&allowed, content) else {
                    tracing::debug!(
                        "file_watcher: to_push_event returned None for {:?}",
                        allowed
                    );
                    return;
                };
                match self.journal.lock() {
                    Ok(mut j) => match j.append(push_evt) {
                        Ok(()) => {
                            // Success resets the consecutive-capacity streak.
                            self.record_append_capacity(false);
                            // Remember what we just enqueued for this path so a
                            // subsequent identical-content event is deduped.
                            // Recorded only on a successful append, so a failed
                            // append never suppresses the retry.
                            if let (Some(sha), Some(p)) = (&content_sha, write_path) {
                                if let Ok(mut m) = self.enqueued_hashes.lock() {
                                    m.insert(p.to_string(), sha.clone());
                                }
                            }
                        }
                        Err(e) => {
                            let is_capacity = matches!(e, JournalError::CapacityExceeded { .. });
                            if is_capacity {
                                // DEBUG, not WARN: a capacity failure under a
                                // wedged journal can fire thousands/sec — warn
                                // here was the 290 MB log-spam half of the
                                // storm. The breaker below is the loud signal.
                                tracing::debug!("file_watcher: journal append failed: {e}");
                            } else {
                                tracing::warn!("file_watcher: journal append failed: {e}");
                            }
                            if self.record_append_capacity(is_capacity) {
                                tracing::error!(
                                    target: "vault_sync_daemon::file_watcher",
                                    threshold = STORM_BREAKER_THRESHOLD,
                                    error = %e,
                                    "STORM CIRCUIT-BREAKER TRIPPED: {STORM_BREAKER_THRESHOLD} consecutive journal CapacityExceeded failures — the push journal is wedged at its 100MB cap and the file_watcher would hot-spin (the S489/S490 storm: 85% CPU, 290MB logs, all new events dropped). HALTING the file_watcher to prevent CPU/log runaway. Likely cause: a reconciliation/materializer backlog filled the journal faster than push_client could drain it. Investigate sync-state/push_journal.jsonl (at cap?) and push_client drain health (server reachable? reconciliation HTTP errors?). The watcher will NOT auto-resume — restart the daemon after the backlog clears."
                                );
                            }
                        }
                    },
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

/// Hint for normalize_event — we still derive create/modify/delete from path
/// existence rather than the (now kind-aware) debouncer event, see `handle_fs_path`.
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
    // D8/D6c (S511, TKT-2dc9a17e): canonicalize the watcher-relative path to the
    // ONE fleet-wide form (NFC + forward-slash) so the PUSH WIRE PATH and the
    // ShadowStore KEY agree across macOS (NFD on disk), Linux/ext4, and
    // Windows/NTFS. Without this a non-ASCII note name keyed the shadow
    // differently per host, producing a shadow miss, a Pull, and a silent
    // revert. APFS and NTFS are normalization-insensitive on lookup so reading
    // back the NFC form still resolves the on-disk file; the server's canonical
    // form is also NFC, so server-materialized files are NFC on disk everywhere.
    crate::sync_shadow::canonical_sync_path(p)
}

/// D5 (S511): true iff the path's basename is a conflict-copy stash
/// (`<stem>.conflict-from-<device>-<lsn>[-<n>].md`). Uses the same structural
/// parser the conflict_stash module writes with, so a legit note name like
/// `My conflict notes.md` is NOT matched (no `.conflict-from-<device>-<lsn>`).
fn is_conflict_copy(rel: &str) -> bool {
    rel.rsplit('/')
        .next()
        .map(|name| crate::conflict_stash::parse_conflict_filename(name).is_some())
        .unwrap_or(false)
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

/// v0.4.9 ROOT-CAUSE FILTER: true iff a notify event kind represents a real
/// content/namespace mutation we should consider pushing.
///
/// We DROP exactly two non-mutating categories and keep everything else:
///   * `Access(_)` — file opened / read / closed-without-write. notify 7.0's
///     inotify mask hardcodes `IN_OPEN`, so EVERY read of a watched file emits
///     `Access(Open)`. verify_repair's reconciliation hash-walk opens all ~28K
///     `.md` files on every 600 s backstop; forwarding those as Modified pushes
///     was the journal storm. Dropping `Access` ALSO breaks the feedback loop:
///     `handle_fs_path` re-reads each file to hash it, which itself emits
///     `Access(Open)` — without this filter that re-read re-enters the queue
///     forever (the breaker was the only thing stopping the hot-spin).
///   * `Modify(Metadata(_))` — atime / mtime / permission / ownership changes
///     with no content change (e.g. `touch`, a chmod, or an atime bump). Not a
///     content edit, so nothing to sync.
///
/// Everything else — `Create`, `Remove`, `Modify(Data|Name|Other|Any)`, and the
/// coarse `Any`/`Other` kinds some non-inotify backends emit — is treated as a
/// real mutation and kept. This is deliberately fail-OPEN: when in doubt we sync
/// (a spurious push is cheap and idempotent; a dropped real edit is data loss).
/// The flood we are killing is exclusively `Access`/`Metadata`, never these.
fn is_mutating_kind(kind: &notify::EventKind) -> bool {
    use notify::event::ModifyKind;
    use notify::EventKind;
    !matches!(
        kind,
        EventKind::Access(_) | EventKind::Modify(ModifyKind::Metadata(_))
    )
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

    // ---------- enqueue-time no-op dedup (journal-bloat fix) ----------

    /// Regression: the FIRST event for a path enqueues, but a SECOND event with
    /// byte-identical content is a redundant no-op and MUST NOT be enqueued
    /// again — otherwise repeated FS events (touch / re-scan / 4-per-sec
    /// re-fires of unchanged files) bury genuine edits behind a runaway journal.
    /// A real edit (any byte change) MUST still enqueue. Red on pre-fix code:
    /// the watcher had no enqueue dedup, so every event was appended (the second
    /// identical event would make the journal len 2).
    #[tokio::test]
    async fn redundant_identical_event_deduped_but_real_edit_enqueues() {
        let dir = TempDir::new().unwrap();
        let w = make_watcher(&dir, vec![], vec![]);

        let rel = "01_Notes/note.md";
        let abs = dir.path().join(rel);
        std::fs::create_dir_all(abs.parent().unwrap()).unwrap();
        std::fs::write(&abs, b"---\ntitle: x\n---\nbody\n").unwrap();

        // First event → enqueues (and records the enqueued hash).
        w.handle_fs_path(&abs).await;
        assert_eq!(
            w.journal.lock().unwrap().len(),
            1,
            "first event for a path must enqueue"
        );

        // Second event, content UNCHANGED → redundant no-op → must NOT enqueue.
        w.handle_fs_path(&abs).await;
        assert_eq!(
            w.journal.lock().unwrap().len(),
            1,
            "redundant identical-content event must be deduped (no second entry)"
        );

        // A genuine edit (different bytes) → MUST enqueue.
        std::fs::write(&abs, b"---\ntitle: x\n---\nCHANGED body\n").unwrap();
        w.handle_fs_path(&abs).await;
        assert_eq!(
            w.journal.lock().unwrap().len(),
            2,
            "a real edit (changed bytes) MUST enqueue"
        );
    }

    /// D6 (S511, TKT-2dc9a17e): with the shadow store wired, the enqueue dedup
    /// suppresses a duplicate ONLY when the content is server-ACCEPTED (shadow
    /// records it). A push that was NEVER accepted leaves the shadow unset, so a
    /// later event re-enqueues instead of being silently dropped (FM1 fix). This
    /// is the exact "revert to previously-enqueued-but-dropped content" hole.
    #[tokio::test]
    async fn d6_dedup_does_not_drop_edit_after_non_accepted_push() {
        use crate::sync_shadow::ShadowStore;
        let dir = TempDir::new().unwrap();
        let sdir = TempDir::new().unwrap();
        let shadow = ShadowStore::load(sdir.path().join("shadow.json"));
        let w = make_watcher(&dir, vec![], vec![]).with_shadow_store(shadow.clone());

        let rel = "01_Notes/note.md";
        let abs = dir.path().join(rel);
        std::fs::create_dir_all(abs.parent().unwrap()).unwrap();
        let bytes = b"---\ntitle: x\n---\nbody\n";
        std::fs::write(&abs, bytes).unwrap();

        // First event enqueues (records enqueued_hashes[p]=sha).
        w.handle_fs_path(&abs).await;
        assert_eq!(w.journal.lock().unwrap().len(), 1, "first event enqueues");

        // The push was NOT accepted: the shadow has NO record for this path.
        // A second identical event must therefore NOT be suppressed (the content
        // is not server-confirmed) and must re-enqueue.
        w.handle_fs_path(&abs).await;
        assert_eq!(
            w.journal.lock().unwrap().len(),
            2,
            "D6: an un-accepted content must re-enqueue, not be silently dropped"
        );

        // Now simulate the server ACCEPTING that content (push_client records it
        // in the shadow). A subsequent identical event IS a true no-op and is
        // suppressed.
        let sha = {
            use sha2::Digest;
            hex::encode(sha2::Sha256::digest(bytes))
        };
        shadow.record(rel, &sha);
        w.handle_fs_path(&abs).await;
        assert_eq!(
            w.journal.lock().unwrap().len(),
            2,
            "once server-accepted, a redundant identical event is suppressed"
        );
    }

    /// B2'd (v0.4.28): after an ack-materialize-back rewrite the push_client
    /// sets enqueued_hashes[p] = canonical sha and the shadow records the same
    /// sha. A LATER touch event (past the echo-guard TTL, so echo suppression
    /// cannot help) with unchanged bytes must be suppressed by the layer-2
    /// dedup - no idempotent echo push. This test drives the watcher with the
    /// exact post-ack-materialize shared state.
    #[tokio::test]
    async fn b2d_touch_after_ack_materialize_is_deduped() {
        use crate::sync_shadow::ShadowStore;
        use std::collections::HashMap;

        let dir = TempDir::new().unwrap();
        let sdir = TempDir::new().unwrap();
        let shadow = ShadowStore::load(sdir.path().join("shadow.json"));
        let enq: Arc<Mutex<HashMap<String, String>>> = Arc::new(Mutex::new(HashMap::new()));
        let w = make_watcher(&dir, vec![], vec![])
            .with_shadow_store(shadow.clone())
            .with_enqueued_hashes(enq.clone());

        let rel = "01_Notes/aligned.md";
        let abs = dir.path().join(rel);
        std::fs::create_dir_all(abs.parent().unwrap()).unwrap();
        let canonical = b"canonical body\n";
        std::fs::write(&abs, canonical).unwrap();
        let sha = {
            use sha2::Digest;
            hex::encode(sha2::Sha256::digest(canonical))
        };

        // Simulate the push_client's post-rewrite state (D2 B2'c + B2'd):
        // shadow = canonical sha (recorded by write_aligned_bytes), shared
        // enqueued_hashes[p] = canonical sha (recorded by ack_materialize_back).
        shadow.record(rel, &sha);
        enq.lock().unwrap().insert(rel.to_string(), sha.clone());

        // A touch event past the echo TTL: unchanged bytes, no echo entry.
        w.handle_fs_path(&abs).await;
        assert_eq!(
            w.journal.lock().unwrap().len(),
            0,
            "B2'd: a touch with unchanged canonical bytes must NOT enqueue a push"
        );

        // Control: a REAL edit must still enqueue.
        std::fs::write(&abs, b"a real edit\n").unwrap();
        w.handle_fs_path(&abs).await;
        assert_eq!(
            w.journal.lock().unwrap().len(),
            1,
            "a genuine edit must never be suppressed"
        );
    }

    /// D5 (S511): a conflict-copy stash (`<stem>.conflict-from-<host>-<seq>.md`)
    /// must be dropped at classify so it is never pushed/re-fanned. A legit note
    /// name that merely contains the word "conflict" must still pass.
    #[test]
    fn d5_conflict_copy_is_excluded_but_legit_name_passes() {
        let dir = TempDir::new().unwrap();
        let w = make_watcher(&dir, vec![], vec![]);
        for p in [
            "01_Notes/note.conflict-from-trinity-123.md",
            "01_Notes/note.conflict-from-cody-link-4242-2.md",
            "deep/dir/x.conflict-from-neo-1.md",
        ] {
            match w.classify(&modified(p)) {
                FilterDecision::DropExclude { exclude_rule, .. } => {
                    assert_eq!(exclude_rule, "conflict-copy", "for {p}");
                }
                other => panic!("expected DropExclude(conflict-copy) for {p}, got {other:?}"),
            }
        }
        // A legit note whose name merely contains "conflict" is NOT a stash.
        match w.classify(&modified("01_Notes/My conflict resolution notes.md")) {
            FilterDecision::Allow(_) => {}
            other => panic!("legit 'conflict' note must pass, got {other:?}"),
        }
    }

    /// A delete clears the remembered enqueue-hash, so recreating the file with
    /// the SAME bytes it had before the delete still enqueues (the recreate is a
    /// real event the server must see, not a no-op).
    #[tokio::test]
    async fn delete_clears_dedup_so_identical_recreate_enqueues() {
        let dir = TempDir::new().unwrap();
        let w = make_watcher(&dir, vec![], vec![]);
        let rel = "01_Notes/note.md";
        let abs = dir.path().join(rel);
        std::fs::create_dir_all(abs.parent().unwrap()).unwrap();
        let bytes = b"---\ntitle: x\n---\nbody\n";

        std::fs::write(&abs, bytes).unwrap();
        w.handle_fs_path(&abs).await; // create → enqueue (len 1)
        std::fs::remove_file(&abs).unwrap();
        w.handle_fs_path(&abs).await; // delete → enqueue (len 2) + clears hash
        std::fs::write(&abs, bytes).unwrap();
        w.handle_fs_path(&abs).await; // recreate identical bytes → MUST enqueue
        assert_eq!(
            w.journal.lock().unwrap().len(),
            3,
            "recreate after delete must enqueue even with pre-delete-identical bytes"
        );
    }

    // ---------- .lattice-runtime exclude (journal-bloat / self-push fix) ----------

    /// The daemon's own runtime dir (and rotated `.STALE-*` variants) must be
    /// dropped at classify — when `sync_roots` is empty the watcher roots at
    /// `vaults_root`, the parent of the vault, so `.lattice-runtime` is in scope
    /// and was being enqueued as thousands of self-generated push entries.
    #[test]
    fn lattice_runtime_dir_is_excluded() {
        let dir = TempDir::new().unwrap();
        let w = make_watcher(&dir, vec![], vec![]);
        for p in [
            ".lattice-runtime/state.json",
            "Mainframe/.lattice-runtime/memory/x.md",
            "Mainframe/.lattice-runtime.STALE-S477/memory/y.md",
        ] {
            match w.classify(&modified(p)) {
                FilterDecision::DropExclude { .. } => {}
                other => panic!("expected DropExclude for {p}, got {other:?}"),
            }
        }
    }

    /// A node_modules/ under the vault must be dropped at classify (root + any
    /// nesting) — it inflated the push journal massively (2026-06-14). Must
    /// match verify_repair's reconcile-walk exclude.
    #[test]
    fn node_modules_is_excluded() {
        let dir = TempDir::new().unwrap();
        let w = make_watcher(&dir, vec![], vec![]);
        for p in [
            "node_modules/sharp/README.md",
            "Mainframe/node_modules/semver/range.md",
            "02_Projects/foo/node_modules/x/index.md",
        ] {
            match w.classify(&modified(p)) {
                FilterDecision::DropExclude { .. } => {}
                other => panic!("expected DropExclude for {p}, got {other:?}"),
            }
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
    fn substrate_path_now_pushes_as_content() {
        // "substrate must sync" (2026-06-20): the push fence is lifted, so
        // former-substrate paths classify as Allow and push like any note.
        let dir = TempDir::new().unwrap();
        let w = make_watcher(&dir, vec![], vec![]);
        let evt = modified("00_VAULT.md");
        match w.classify(&evt) {
            FilterDecision::Allow(WatchEvent::Modified { path }) => {
                assert_eq!(path, "00_VAULT.md");
            }
            other => panic!("expected Allow(Modified), got {:?}", other),
        }
    }

    #[test]
    fn substrate_scoped_now_pushes_as_content() {
        let dir = TempDir::new().unwrap();
        let w = make_watcher(&dir, vec![], vec![]);
        let evt = modified("02_Projects/Foo/Family.md");
        assert!(matches!(w.classify(&evt), FilterDecision::Allow(_)));
    }

    #[test]
    fn family_md_at_root_allows() {
        // Family.md at vault root was already content; still content.
        let dir = TempDir::new().unwrap();
        let w = make_watcher(&dir, vec![], vec![]);
        let evt = modified("Family.md");
        match w.classify(&evt) {
            FilterDecision::Allow(_) => {}
            other => panic!("expected Allow, got {:?}", other),
        }
    }

    #[test]
    fn claude_md_now_pushes_as_content() {
        let dir = TempDir::new().unwrap();
        let w = make_watcher(&dir, vec![], vec![]);
        // Multiple flavors — all push as content now.
        for p in &[
            "CLAUDE.md",
            "02_Projects/Foo/CLAUDE.md",
            "claude.md",
            "02_Projects/x/claude.md",
        ] {
            let evt = modified(p);
            assert!(
                matches!(w.classify(&evt), FilterDecision::Allow(_)),
                "expected Allow for {p}"
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
    fn rename_former_substrate_source_now_allows() {
        // "substrate must sync": former-substrate source is now content, so the
        // rename passes through (both sides content).
        let dir = TempDir::new().unwrap();
        let w = make_watcher(&dir, vec![], vec![]);
        let evt = renamed("02_Projects/Protocols/x.md", "02_Projects/Foo/x.md");
        match w.classify(&evt) {
            FilterDecision::Allow(WatchEvent::Renamed { old_path, new_path }) => {
                assert_eq!(old_path, "02_Projects/Protocols/x.md");
                assert_eq!(new_path, "02_Projects/Foo/x.md");
            }
            other => panic!("expected Allow(Renamed), got {:?}", other),
        }
    }

    #[test]
    fn rename_former_substrate_destination_now_allows() {
        // Symmetric: former-substrate destination is now content too.
        let dir = TempDir::new().unwrap();
        let w = make_watcher(&dir, vec![], vec![]);
        let evt = renamed("02_Projects/Foo/x.md", "02_Projects/Protocols/x.md");
        match w.classify(&evt) {
            FilterDecision::Allow(WatchEvent::Renamed { old_path, new_path }) => {
                assert_eq!(old_path, "02_Projects/Foo/x.md");
                assert_eq!(new_path, "02_Projects/Protocols/x.md");
            }
            other => panic!("expected Allow(Renamed), got {:?}", other),
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
    fn windows_backslash_former_substrate_now_allows() {
        // Former-substrate path with backslashes normalizes and pushes as
        // content ("substrate must sync"); the junk fence is unaffected.
        let dir = TempDir::new().unwrap();
        let w = make_watcher(&dir, vec![], vec![]);
        let evt = modified("02_Projects\\Protocols\\foo.md");
        match w.classify(&evt) {
            FilterDecision::Allow(WatchEvent::Modified { path }) => {
                assert_eq!(path, "02_Projects/Protocols/foo.md");
            }
            other => panic!("expected Allow with normalized path, got {:?}", other),
        }
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
    fn trailing_dots_in_name_allowed() {
        // S490 regression: a title ending in `...` (three ASCII dots) contains
        // `..` as a substring but is a legit name, not a traversal.
        let dir = TempDir::new().unwrap();
        let w = make_watcher(&dir, vec![], vec![]);
        let evt = created("01_Notes/Anysa says....md");
        assert!(matches!(w.classify(&evt), FilterDecision::Allow(_)));
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
        // v0.4.7 lazy ref: the body is NOT embedded (push_client reads it from
        // disk at drain); content_sha is still computed from the body.
        assert!(push.content_bytes.is_none());
        assert_eq!(push.content_sha, sha256_hex(&body));
        assert_eq!(push.base_hash, PushBase::Unknown);
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
        // v0.4.7 lazy ref: not embedded; content_sha still computed.
        assert!(push.content_bytes.is_none());
        assert_eq!(push.content_sha, sha256_hex(&body));
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
        // v0.4.7 lazy ref: not embedded; content_sha still computed.
        assert!(push.content_bytes.is_none());
        assert_eq!(push.content_sha, sha256_hex(&body));
    }

    #[test]
    fn create_without_content_returns_none() {
        // Caller MUST supply content for Create — None signals "couldn't read".
        let dir = TempDir::new().unwrap();
        let w = make_watcher(&dir, vec![], vec![]);
        let evt = created("02_Projects/Foo/n.md");
        assert!(w.to_push_event(&evt, None).is_none());
    }

    // ---------- storm circuit-breaker ----------

    #[test]
    fn storm_breaker_trips_after_threshold_and_resets_on_success() {
        let dir = TempDir::new().unwrap();
        let w = make_watcher(&dir, vec![], vec![]);
        // Just under the threshold: never trips, never fenced.
        for _ in 0..(STORM_BREAKER_THRESHOLD - 1) {
            assert!(!w.record_append_capacity(true));
        }
        assert!(!w.is_fenced());
        // A successful append resets the consecutive streak to zero.
        assert!(!w.record_append_capacity(false));
        assert!(!w.is_fenced());
        // Fresh run must climb from zero again, then trip exactly once.
        let mut trips = 0;
        for _ in 0..STORM_BREAKER_THRESHOLD {
            if w.record_append_capacity(true) {
                trips += 1;
            }
        }
        assert_eq!(trips, 1, "breaker should signal a trip exactly once");
        assert!(w.is_fenced());
        // Already fenced → further capacity failures do not re-signal.
        assert!(!w.record_append_capacity(true));
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

    // ---------- v0.4.9 non-mutating-event filter ----------

    #[test]
    fn is_mutating_kind_drops_access_and_metadata() {
        use notify::event::{
            AccessKind, AccessMode, CreateKind, DataChange, MetadataKind, ModifyKind, RemoveKind,
            RenameMode,
        };
        use notify::EventKind;

        // DROP: every flavor of Access (file opens/reads/closes). These are the
        // IN_OPEN events that drove the reconciliation-backstop storm — a pure
        // read-walk of the vault must produce ZERO push-worthy events.
        assert!(!is_mutating_kind(&EventKind::Access(AccessKind::Open(
            AccessMode::Any
        ))));
        assert!(!is_mutating_kind(&EventKind::Access(AccessKind::Read)));
        assert!(!is_mutating_kind(&EventKind::Access(AccessKind::Close(
            AccessMode::Read
        ))));
        assert!(!is_mutating_kind(&EventKind::Access(AccessKind::Any)));

        // DROP: metadata-only changes (atime/mtime/perms/ownership) — `touch`,
        // chmod, etc. carry no content change.
        assert!(!is_mutating_kind(&EventKind::Modify(ModifyKind::Metadata(
            MetadataKind::Any
        ))));
        assert!(!is_mutating_kind(&EventKind::Modify(ModifyKind::Metadata(
            MetadataKind::AccessTime
        ))));

        // KEEP: real content/namespace mutations.
        assert!(is_mutating_kind(&EventKind::Create(CreateKind::File)));
        assert!(is_mutating_kind(&EventKind::Remove(RemoveKind::File)));
        assert!(is_mutating_kind(&EventKind::Modify(ModifyKind::Data(
            DataChange::Any
        ))));
        assert!(is_mutating_kind(&EventKind::Modify(ModifyKind::Name(
            RenameMode::Both
        ))));
        // KEEP: coarse kinds some non-inotify backends (FSEvents/Windows) emit —
        // fail-OPEN so we never silently drop a real edit.
        assert!(is_mutating_kind(&EventKind::Modify(ModifyKind::Any)));
        assert!(is_mutating_kind(&EventKind::Any));
    }

    // ---------- FS-integration (#[ignore]) ----------

    /// Local-only smoke test: actually spawn the OS watcher, write a file,
    /// and confirm the journal grows. Ignored in CI because:
    ///   - tokio runtime + notify timing is flaky on Windows GitHub Actions runners.
    ///   - notify-debouncer-full fires inside a 500ms window so the test
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

        // Former substrate path → now Allow (no substrate counter bump).
        // "substrate must sync": substrate is content, the fence is lifted, so
        // events_dropped_substrate stays 0.
        let _ = w.classify_and_count(&modified("00_VAULT.md"));
        // Extension drop.
        let _ = w.classify_and_count(&modified("notes/x.exe"));
        // Exclude (hardcoded).
        let _ = w.classify_and_count(&modified(".obsidian/workspace.json"));
        // Allow (no counter bump).
        let _ = w.classify_and_count(&modified("notes/ok.md"));

        let s = tray.read().unwrap();
        // No event is dropped as substrate anymore.
        assert_eq!(s.events_dropped_substrate, 0);
        assert_eq!(s.events_dropped_extension, 1);
        assert_eq!(s.events_dropped_excludes, 1);
        // events_filtered is the sum (each dropped event bumps the rollup).
        assert_eq!(s.events_filtered, 2);
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
            // Split so the Ok arm need not Debug-format WatchHandle (which
            // holds a non-Debug notify watcher); FileWatcherError is Debug.
            Ok(_) => panic!("expected InotifyLimitExceeded, got Ok(WatchHandle)"),
            Err(e) => panic!("expected InotifyLimitExceeded, got {e:?}"),
        }
    }
}
