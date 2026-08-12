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
use crate::push_journal::{
    new_event_id, PushAction, PushBase, PushEvent, PushJournal, CURRENT_SCHEMA,
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
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use tempfile::NamedTempFile;
use thiserror::Error;
use tracing::{debug, error, info, warn};

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
    /// Conflict-storm circuit breaker OPEN (TKT-86ae42a3): this write resolved
    /// to an R4/R5 Conflict, but the materializer already minted
    /// `conflict_storm_threshold` stashes inside the sliding window — a mass
    /// server-side divergence event, not organic concurrent editing. The local
    /// file is left UNTOUCHED (no stash, no overwrite); reconcile retries
    /// after the window (or after the operator resolves the divergence source).
    ConflictStormBreakerOpen,
    /// R1 / F-B1.1 ARM 1 (TKT-989ad5f2): the anti-strip guard fired and the
    /// server version merely DROPPED the frontmatter block (the
    /// frontmatter-normalized bodies are byte-equal — a pure server-strip). The
    /// local frontmatter-bearing copy is preserved AND a compensating UP push
    /// was enqueued (CAS base = the server hash from the pull payload) so the
    /// local frontmatter propagates UP. The path is STILL DIVERGENT until that
    /// push lands, so this classifies as `Deferred`/RED (R2), never converged.
    /// `enqueued_push` is false only when no push-journal handle was wired (a
    /// fail-honest degrade: local is still preserved, but convergence then
    /// waits on the next reconcile pass).
    GuardPreserveLocalPushUp { enqueued_push: bool },
    /// R1 (TKT-372e31b2): this write resolved to an R5 `Conflict` (shadow absent
    /// => "unknown provenance"), but the shadow store loaded in the
    /// `vault_scope_suspect` state - `vault_folders` resolved EMPTY while the
    /// store holds vault-prefixed keys, so EVERY `shadow.get()` mis-keys and
    /// misses (`sync_shadow::detect_vault_scope_suspect`, the 2026-07-18 trinity
    /// incident). In that state "shadow absent" is a KNOWN MISCONFIGURATION
    /// artifact, not real ambiguity, so a conflict copy is false BY
    /// CONSTRUCTION: it says "two writers diverged" when the only thing that
    /// happened is that the daemon cannot read its own sync history. The push
    /// leg already PARKS wholesale on this state (lib.rs, `spawn_push_pipeline`;
    /// push_client `drain_once`), so continuing to mint pull-side forks while no
    /// push can ever leave the host is incoherent as well as wrong. We refuse
    /// the whole write: local untouched, no stash, no overwrite (fail-closed
    /// toward local, exactly like `ConflictStormBreakerOpen`). The operator
    /// fixes `vault_name` in the config and restarts.
    ShadowScopeSuspect,
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
    /// D1 (v0.4.28, B1' resolution): the local file was NORMALIZED-equal but
    /// RAW-unequal to the server canonical (a CRLF/BOM-only delta). It was
    /// rewritten in place to the server's exact canonical bytes through the
    /// standard persist machinery (echo-guarded, per-path-locked, atomic
    /// tmp+rename, timestamps restored). NO stash: normalized-equal means
    /// zero content difference by construction. This is the "alignment pull"
    /// that converges the fleet's CRLF corpus in one pull pass, zero pushes.
    AlignedToCanonical { path: PathBuf },
    /// Write completed but the post-write integrity check failed.  The file
    /// is intentionally NOT deleted — the owner can inspect both the bad
    /// write and the resulting ticket.
    IntegrityFailed {
        path: PathBuf,
        expected_sha: String,
        actual_sha: String,
    },
}

/// D2 (v0.4.28, B2'): outcome of an ack-materialize-back aligned write
/// ([`Materializer::write_aligned_bytes`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlignOutcome {
    /// The local file was atomically rewritten to the canonical bytes and the
    /// shadow store now records the canonical sha. Ordering (B2'c): rewrite
    /// FIRST, shadow record SECOND - a crash between the two is benign (next
    /// pass: raw match -> D1 Noop).
    Rewrote { path: PathBuf },
    /// Pre-rewrite guard tripped (B2'a): the file's current bytes no longer
    /// hash to the bytes the caller drained/pushed - the user edited
    /// mid-flight. Rewrite SKIPPED; the pending push of the newer edit
    /// converges. `current_sha` is the file's current raw sha (diagnostics).
    SkippedConcurrentEdit { current_sha: String },
    /// The file no longer exists locally (deleted mid-flight). Skipped.
    SkippedMissing,
    /// Materializer is configured in `Disabled` mode. Skipped.
    SkippedDisabled,
    /// S513-class anti-strip guard (close-out, v0.4.28 final-review fix wave):
    /// the CURRENT local file holds YAML frontmatter and `canonical_bytes` do
    /// NOT. Rewriting would strip the local note's frontmatter — the exact
    /// data-loss vector `guard_no_frontmatter_strip` already refuses on the
    /// pull path (`write()`). This is the D2 fetch-fallback's own copy of that
    /// guard: `ack_materialize_back`'s `/note` fetch fallback can hand back a
    /// server body that lacks frontmatter for the same reason a pull can.
    /// Rewrite SKIPPED; NOTHING is recorded in the shadow (stays stale — the
    /// next reconcile pass falls to the fail-closed PULL path, which is itself
    /// guarded by `guard_no_frontmatter_strip`, so it also refuses and the
    /// local frontmatter-bearing copy survives to push up).
    SkippedWouldStripFrontmatter,
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
    /// Conflict-storm circuit breaker (TKT-86ae42a3): maximum R4/R5 conflict
    /// stashes this materializer may mint inside `conflict_storm_window_secs`.
    /// Past the threshold, further Conflict decisions are SKIPPED (local left
    /// untouched, no stash, no overwrite — fail-closed toward local) and
    /// surfaced as `SkipReason::ConflictStormBreakerOpen`. A mass server-side
    /// divergence event (07-16 consolidation: 483 files; 07-18 D-8 sentinel
    /// contamination: 2,422 files) can then never again mint thousands of
    /// conflict copies. `0` disables the breaker.
    pub conflict_storm_threshold: u32,
    /// Sliding window (seconds) for `conflict_storm_threshold`.
    pub conflict_storm_window_secs: u64,
}

impl Default for MaterializerConfig {
    fn default() -> Self {
        Self {
            enable_integrity_check: true,
            conflict_policy: ConflictPolicy::ServerWins,
            strip_frontmatter_fields_for_diff: vec!["updated".into()],
            device_id: "unknown-device".to_string(),
            conflict_storm_threshold: 50,
            conflict_storm_window_secs: 600,
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
    /// R7b (THESEUS AR-002, TKT-166e1c07): per-note observed-base_seq store.
    /// On every Live-mode write that byte-verifies (the same point the shadow
    /// hash is recorded), the server-provided `payload.change_seq` is recorded
    /// here as the note's observed base_seq (R3 - observed seq comes from the
    /// server response, recorded only AFTER the exact bytes materialize + pass
    /// the integrity check). `None` keeps pre-R7b behavior (no seq recording).
    base_seq_store: Option<Arc<crate::base_seq_store::BaseSeqStore>>,
    /// D2c (S511, TKT-2dc9a17e): per-path advisory lock registry. Serializes the
    /// `exists -> compare -> read-shadow -> stash -> persist` critical section
    /// for a SINGLE path so ~15 concurrent writers cannot lose a stash basis
    /// (read-old-bytes, both stash, one rename wins) or spawn N re-conflicting
    /// copies. Each distinct path gets its own `Mutex`; different paths proceed
    /// in parallel. `Arc`-shared so all clones (SSE consumer, reconcile pull,
    /// backfill) of one Materializer contend on the SAME lock per path. Coarse
    /// outer mutex only guards the small registry HashMap, never the I/O.
    path_locks: Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>,
    /// Conflict-storm circuit breaker state (TKT-86ae42a3): timestamps of
    /// recent conflict-stash mints, pruned to the config window. `Arc`-shared
    /// so every clone (SSE consumer, reconcile backstop, pull backfill) counts
    /// against ONE budget — the storm arrived through the reconcile clone.
    conflict_mints: Arc<Mutex<std::collections::VecDeque<std::time::Instant>>>,
    /// TKT-372e31b2 / 2026-08-12 P1: LATCH for the conflict-storm breaker.
    ///
    /// The pruning window in `conflict_breaker_open` is a sliding-window RATE
    /// LIMITER: it drops mints older than `conflict_storm_window_secs` and then
    /// re-admits `conflict_storm_threshold` more, so it **re-arms every window,
    /// forever**. The TKT-86ae42a3 design-of-record states the invariant as "a
    /// mass server-side divergence event can never again mint thousands of
    /// conflict copies" — and the field falsified that sentence: the 2026-07-23
    /// storm minted **7,371** stashes between 15:38:48 and the next day 16:26:34
    /// (89,266 s / 600 s = 148.8 windows x 50 = 7,440 predicted, within 1%),
    /// alongside 546,523 `BREAKER OPEN` log lines.
    ///
    /// Once tripped we therefore LATCH: the breaker stays open for the rest of
    /// the process, bounding a single storm to `threshold` mints instead of
    /// `threshold` per window. This costs nothing in convergence *during* the
    /// storm — the open arm already refused the pull as well as the stash — and
    /// it converts an unbounded fork generator into a bounded one that a human
    /// or a reconcile pass resolves. `Arc`-shared for the same reason
    /// `conflict_mints` is: every clone must count against ONE latch, because
    /// the 07-23 storm arrived through the reconcile clone.
    ///
    /// Cleared by process restart (in-memory by construction) or explicitly via
    /// [`Materializer::reset_conflict_storm_breaker`]. Surfaced to the tray as
    /// `conflict_storm_latched` so a latched daemon can never be silently stale
    /// — the failure mode this project keeps paying for is absence read as
    /// health.
    conflict_breaker_latched: Arc<AtomicBool>,
    /// R1 / F-B1.1 (TKT-989ad5f2): optional push-journal handle used to enqueue
    /// the ARM-1 compensating UP push (pure server-strip: the server merely
    /// dropped the frontmatter block; the body is byte-identical after
    /// normalization). The local file is byte-unchanged in that case, so the
    /// file_watcher never fires — without this proactive enqueue the pull
    /// re-hits the anti-strip guard every pass (the 8,081 phantom-pull
    /// deadlock). Its own `PushJournal` handle on the shared jsonl file (the
    /// journal is file-authoritative; N handles converge). `None` keeps the
    /// pre-fix behavior (preserve local, rely on the watcher).
    push_journal: Option<Arc<Mutex<PushJournal>>>,
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
            base_seq_store: self.base_seq_store.clone(),
            path_locks: self.path_locks.clone(),
            conflict_mints: self.conflict_mints.clone(),
            conflict_breaker_latched: self.conflict_breaker_latched.clone(),
            push_journal: self.push_journal.clone(),
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
            base_seq_store: None,
            path_locks: Arc::new(Mutex::new(HashMap::new())),
            conflict_mints: Arc::new(Mutex::new(std::collections::VecDeque::new())),
            conflict_breaker_latched: Arc::new(AtomicBool::new(false)),
            push_journal: None,
        }
    }

    /// R1 / F-B1.1 (TKT-989ad5f2): attach the push-journal handle used to
    /// enqueue the ARM-1 compensating UP push. Builder; `None` (unset) keeps
    /// the pre-fix preserve-and-wait-for-watcher behavior.
    pub fn with_push_journal(mut self, journal: Arc<Mutex<PushJournal>>) -> Self {
        self.push_journal = Some(journal);
        self
    }

    /// R1 / F-B1.1 ARM 1 (TKT-989ad5f2): enqueue the compensating UP push that
    /// carries the preserved local (frontmatter-bearing) bytes back to the
    /// server. `server_hash` is the CAS base (the server hash from the pull
    /// payload) so the server accepts our bytes over its stripped copy;
    /// `local_sha` is the sha of the bytes on disk we are pushing. A LAZY ref
    /// (`content_bytes: None`) — push_client reads the file at drain time.
    /// Returns true iff a journal handle was wired AND the append succeeded.
    fn enqueue_compensating_push(&self, path: &str, local_sha: &str, server_hash: &str) -> bool {
        let Some(journal) = &self.push_journal else {
            warn!(
                path,
                "ARM 1: no push-journal handle wired - local preserved but compensating push NOT enqueued (convergence waits on next reconcile pass)"
            );
            return false;
        };
        let evt = PushEvent {
            schema_version: CURRENT_SCHEMA,
            id: new_event_id(),
            path: path.to_string(),
            action: PushAction::Modify,
            // The CAS base is the server hash from the pull payload: base ==
            // current server row -> the server accepts our frontmatter-bearing
            // bytes (operator-ratified local-wins-push-up for a pure strip).
            base_hash: PushBase::KnownBase(server_hash.to_string()),
            content_sha: local_sha.to_string(),
            content_bytes: None,
            queued_at: chrono::Utc::now(),
            device_id: self.config.device_id.clone(),
        };
        match journal.lock() {
            Ok(mut j) => match j.append(evt) {
                Ok(()) => true,
                Err(e) => {
                    warn!(path, error = %e, "ARM 1: compensating push append failed");
                    false
                }
            },
            Err(e) => {
                warn!(path, error = %e, "ARM 1: push-journal mutex poisoned; compensating push not enqueued");
                false
            }
        }
    }

    /// Conflict-storm circuit breaker (TKT-86ae42a3, LATCHED 2026-08-12).
    ///
    /// Called on every R4/R5 Conflict decision BEFORE stashing. Returns `true`
    /// when the breaker is OPEN (caller must refuse the write entirely — no
    /// stash, no overwrite, fail-closed toward local bytes).
    ///
    /// `threshold` mints are admitted inside `conflict_storm_window_secs`; the
    /// `threshold+1`-th TRIPS THE LATCH and the breaker stays open for the rest
    /// of the process. See [`Self::conflict_breaker_latched`] for why the
    /// original pure sliding window did not hold its own documented invariant.
    /// Threshold 0 disables the breaker (and can never latch).
    ///
    /// Returns `(open, just_latched)` so the caller can log the transition
    /// exactly once instead of once per refused path — the 07-23 storm wrote
    /// 546,523 identical WARN lines, and this daemon's log reached 442 MB in a
    /// day on trinity during the 07-30 storm.
    fn conflict_breaker_open(&self) -> (bool, bool) {
        let threshold = self.config.conflict_storm_threshold;
        if threshold == 0 {
            return (false, false);
        }
        // Latched from a previous trip: open, and NOT a fresh transition.
        if self.conflict_breaker_latched.load(Ordering::Relaxed) {
            return (true, false);
        }
        let window = std::time::Duration::from_secs(self.config.conflict_storm_window_secs);
        let now = std::time::Instant::now();
        let mut mints = self
            .conflict_mints
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        while mints
            .front()
            .is_some_and(|t| now.duration_since(*t) > window)
        {
            mints.pop_front();
        }
        if mints.len() >= threshold as usize {
            // `swap` under the mints lock: concurrent clones (SSE consumer,
            // reconcile, backfill) race here, and exactly one must observe the
            // transition so exactly one ERROR line is emitted.
            let was_latched = self.conflict_breaker_latched.swap(true, Ordering::Relaxed);
            return (true, !was_latched);
        }
        mints.push_back(now);
        (false, false)
    }

    /// True iff the conflict-storm breaker has LATCHED open in this process.
    /// While latched, every R4/R5 Conflict decision is refused: local bytes are
    /// preserved and the server version is NOT materialized, so the affected
    /// paths are deliberately left divergent pending human or reconcile action.
    pub fn conflict_storm_latched(&self) -> bool {
        self.conflict_breaker_latched.load(Ordering::Relaxed)
    }

    /// Clear a latched conflict-storm breaker and its mint budget.
    ///
    /// For explicit operator recovery once the mass-divergence cause is fixed
    /// (and for tests). Deliberately NOT called from any timer or watchdog: an
    /// automatic reset would restore the re-arming behavior this latch exists to
    /// remove, and an auto-restart on a recurring storm would restart-loop.
    pub fn reset_conflict_storm_breaker(&self) {
        self.conflict_breaker_latched
            .store(false, Ordering::Relaxed);
        self.conflict_mints
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clear();
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

    /// R7b (TKT-166e1c07): attach the per-note observed-base_seq store. After
    /// this, every Live-mode write that byte-verifies records the server's
    /// `payload.change_seq` as the note's observed base_seq (R3), at the same
    /// point the shadow hash is recorded. Backwards-compatible - without it, no
    /// seq recording (the note stays unobserved -> fail-closed on the next push).
    pub fn with_base_seq_store(mut self, store: Arc<crate::base_seq_store::BaseSeqStore>) -> Self {
        self.base_seq_store = Some(store);
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

        // 3. RASP substrate fence — LIFTED ("substrate must sync", 2026-06-20).
        //    classify_path now always returns Content, so substrate paths fall
        //    through and materialize byte-faithfully like any note, protected by
        //    the same conflict-stash / newer-wins / anti-strip machinery below.
        //    This branch is retained (never taken while the fence is empty) so
        //    the fence can be restored by repopulating SUBSTRATE_PATH_RULES.
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
        // D1 (v0.4.28): set when the Noop arm detects normalized-equal but
        // raw-unequal - fall through to the persist machinery instead of
        // returning, then report AlignedToCanonical.
        let mut alignment_pull = false;
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

            // S513 anti-strip guard (TKT-2dc9a17e): never let a pull/overwrite
            // drop YAML frontmatter the local note holds. The server still
            // serves frontmatter-stripped bodies for some notes; without this an
            // R3 pull strips local SILENTLY and an R5 pull strips it (preserving
            // a conflict copy). Gate the raw R1-R5 decision through the guard.
            let raw_decision = decide(
                local_eq_server,
                shadow.is_some(),
                shadow_eq_server,
                local_eq_shadow,
            );
            let pull_would_strip =
                starts_with_frontmatter(&local_bytes) && !starts_with_frontmatter(content_bytes);
            // R1 / F-B1.1 (TKT-989ad5f2): the anti-strip guard NEVER silently
            // strips and NEVER just parks on a bare "will push up" promise. When
            // a pull WOULD drop frontmatter local holds, resolve into one of two
            // arms based on whether the frontmatter-normalized BODIES are equal.
            let guard_hit = pull_would_strip
                && matches!(raw_decision, Decision::PullClean | Decision::Conflict);
            let effective_decision = if guard_hit {
                match classify_guard_arm(&local_bytes, content_bytes) {
                    GuardArm::PreserveAndPushUp => {
                        // ARM 1 — pure server-strip: the server dropped only the
                        // frontmatter block; the body is byte-identical. Preserve
                        // the local frontmatter-bearing copy and enqueue a
                        // compensating UP push whose CAS base is the server hash
                        // from THIS pull payload, so the local frontmatter
                        // propagates UP. The local file is byte-unchanged, so the
                        // file_watcher never fires — this proactive enqueue is
                        // what breaks the phantom-pull deadlock (RC-B1). The path
                        // stays divergent until the push lands (R2: Deferred/RED).
                        let enqueued_push = self.enqueue_compensating_push(
                            &payload.path,
                            &local_raw_sha,
                            &payload.sha256,
                        );
                        warn!(
                            path = %payload.path,
                            ?raw_decision,
                            change_seq,
                            base_hash = %payload.sha256,
                            enqueued_push,
                            "materializer ANTI-STRIP GUARD (S513) ARM 1 (pure server-strip: frontmatter-normalized bodies equal): PRESERVING local + ENQUEUED compensating UP push (CAS base = server hash from pull); still divergent until it lands"
                        );
                        return Ok(MaterializeOutcome::Skipped(
                            SkipReason::GuardPreserveLocalPushUp { enqueued_push },
                        ));
                    }
                    GuardArm::StashThenAlign => {
                        // ARM 2 — genuine divergence: the bodies differ. Fall
                        // through to the R4/R5 stash-then-align path (stash the
                        // local bytes OUTSIDE sync scope as a conflict copy, then
                        // materialize the server winner + update the shadow). No
                        // data loss: the local bytes survive as the stash.
                        warn!(
                            path = %payload.path,
                            ?raw_decision,
                            change_seq,
                            "materializer ANTI-STRIP GUARD (S513) ARM 2 (genuine divergence: frontmatter-normalized bodies differ): STASHING local then ALIGNING to server (local preserved as conflict copy)"
                        );
                        Decision::Conflict
                    }
                }
            } else {
                raw_decision
            };
            match effective_decision {
                Decision::Noop => {
                    if local_bytes == content_bytes {
                        // R1 byte-strict half: truly identical bytes. Nothing
                        // to write; no mtime churn.
                        info!(
                            path = %payload.path,
                            "materializer skip: local already identical to canonical (R1)"
                        );
                        // B1 (S534): record ONLY in Live mode, and record the
                        // LOCAL raw sha (`local_raw_sha` — what is actually on
                        // disk), NOT the server hash. In Shadow/Disabled the
                        // materialize write went to the shadow tree, not the
                        // vault, so recording a baseline here forges the "vault
                        // in sync" marker that verify_repair reads as
                        // drift+shadow==server ⇒ PUSH ⇒ the re-push storm. Even
                        // in Live, `local_raw_sha` is the honest on-disk value
                        // (identical to the server bytes on this byte-strict
                        // arm, but never a value the vault did not actually
                        // hold). In non-Live we skip the record entirely.
                        if matches!(self.mode, MaterializerMode::Live) {
                            if let Some(sh) = &self.shadow_store {
                                sh.record(&payload.path, &local_raw_sha);
                            }
                            // R4 (TKT-f74edf99): CLOSE THE R1-NOOP NON-RECORDING
                            // HOLE. This arm previously recorded only the shadow
                            // and RETURNED, so the base_seq recorder further down
                            // (the sole other recording point) was unreachable —
                            // a note that converged by ALREADY being byte-identical
                            // to the server never earned a base_seq and stayed
                            // primed to deadlock on its next local edit. The bytes
                            // are byte-verified here BY CONSTRUCTION (local_bytes ==
                            // content_bytes == the server canonical the sha256 is
                            // computed over), so recording the server-provided
                            // change_seq is a genuine OBSERVATION, not a forged
                            // baseline. Same Live-only + Some(seq) gate as the main
                            // recording point below.
                            if let (Some(bs), Some(seq)) =
                                (&self.base_seq_store, payload.change_seq)
                            {
                                // ADOPTED: local bytes == server canonical by
                                // construction on this arm (Finding 1).
                                bs.record_adopted(&payload.path, seq);
                            }
                        }
                        return Ok(MaterializeOutcome::Skipped(SkipReason::IdenticalToLocal));
                    }
                    // D1 (v0.4.28, B1'): normalized-equal but RAW-unequal - a
                    // CRLF/BOM-only delta between local and the server
                    // canonical. The old normalized-only Noop here is why the
                    // pull leg could never converge byte-level drift while
                    // every byte-strict comparer (server CAS, reconcile-batch
                    // fs_hash) kept classifying it - the B1' alternation.
                    // ALIGNMENT PULL: fall through to the persist machinery
                    // below (echo-guard already recorded pre-write; the
                    // per-path lock is held; atomic tmp+rename + timestamp
                    // restore + shadow record all apply). NO stash - safe by
                    // construction: normalized-equal means zero content
                    // difference. The anti-strip guard is structurally
                    // unaffected: normalized-equal implies identical
                    // frontmatter presence.
                    info!(
                        path = %payload.path,
                        change_seq,
                        "materializer D1: normalized-equal but raw-unequal - ALIGNMENT PULL, rewriting local to server canonical bytes"
                    );
                    alignment_pull = true;
                }
                Decision::PreserveLocalEdit => {
                    // R2: shadow == server (server has NOT moved since we synced)
                    // AND local diverges => a genuine LOCAL edit. NEVER overwrite
                    // it with the older server copy. Leave the file untouched.
                    // R3 (TKT-989ad5f2) log truth: the materializer enqueues NO
                    // push here — the user's edit that caused this divergence was
                    // already observed by the file_watcher and enqueued at edit
                    // time; that push carries it UP. We state that honestly rather
                    // than promising a push the materializer did not make. The
                    // path stays divergent until the watcher-enqueued push lands
                    // (R2: this outcome classifies as Deferred/RED, never
                    // converged). This is the exact silent-revert the operator
                    // hit (TKT-2dc9a17e), now preserved + accounted honestly.
                    //
                    // R6 (TKT-f74edf99): CONTENT-LEVEL direction-safety signal.
                    // Ancestry is KNOWN here (shadow == server), so the current
                    // server head IS the base this local edit descends from; a
                    // server line absent from local is therefore an INTENTIONAL
                    // local deletion, not lossy divergence, and preserving-local
                    // is safe. We still compute the line-level containment (never
                    // size/mtime) and surface it, so any surprising "local-wins
                    // would drop a server line" case is visible to accounting
                    // instead of silent. The unknown-ancestry case (no such base)
                    // is handled by decide() as Conflict => PRESERVE BOTH.
                    let server_contained =
                        server_lines_contained_in_local(content_bytes, &local_bytes);
                    warn!(
                        path = %payload.path,
                        change_seq,
                        server_contained,
                        "materializer R2: local edit diverges, server unchanged since last sync — PRESERVING local, NOT overwriting (watcher-enqueued push carries it up; still divergent until it lands)"
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
                    // R2 (TKT-372e31b2): CAUSAL GATE, BEFORE ANY STASH.
                    //
                    // `decide()` is content-relational only: it can see that
                    // local and server both differ from the last-synced shadow,
                    // but it CANNOT see which one is causally newer, so it calls
                    // every such pair a conflict. For a SINGLE-WRITER file that
                    // is provably wrong whenever the incoming server version is
                    // one this daemon has ALREADY observed: the local bytes are
                    // then a write layered ON TOP of that observed version (a
                    // fresh local edit racing our own materialization of the same
                    // path), and materializing the server copy would move the
                    // file BACKWARD while sidelining the newer bytes into a
                    // conflict copy that no other writer ever contested. That is
                    // the exact defect: `_sync/canary-<host>.md` is written by
                    // exactly one host by construction and still acquired
                    // `canary-trinity.conflict-from-<device>-<lsn>.md` holding
                    // the nonce of a write that then never reached the server.
                    //
                    // The proof-of-observation store (R7b, TKT-166e1c07) is
                    // exactly the missing causal input. `payload.change_seq` is
                    // the server's per-note version token for the bytes we are
                    // being asked to write; `base_seq_store.get_adopted(path)`
                    // is the token of the newest version this daemon
                    // byte-verified ONTO ITS OWN FS for that path — ADOPTED
                    // provenance only (recorded after a post-write integrity
                    // pass, `write_with_change_seq` tail / Noop-identical arm,
                    // or after a push the server accepted). PROVENANCE MATTERS
                    // (Finding 1, PR #11 review): since TKT-f74edf99 the store
                    // also holds OBSERVED entries minted by the verified
                    // read-receipt on the 409 refetch path. A receipt proves we
                    // SAW the server head, not that the local file ever held
                    // its bytes — and the 409 refetch materializes that same
                    // head immediately after recording it, so a provenance-
                    // blind read would see `incoming == observed` on EVERY
                    // genuinely divergent 409 and misclassify the true conflict
                    // as "local is newer": no stash, no pull, and the receipt-
                    // authorised retry push would land local over the head
                    // without the always-stash floor ever firing. Gating on
                    // adopted provenance restores the old base's invariant
                    // (observed => those bytes were on this disk). So:
                    //
                    //   incoming <= observed  =>  the incoming version is NOT
                    //   causally newer than something we already materialized.
                    //   Local must therefore be a descendant of it (local !=
                    //   server here, else R1 Noop caught it). Resolution:
                    //   PRESERVE LOCAL, write nothing, stash nothing - the
                    //   file_watcher/push pipeline carries the newer local bytes
                    //   UP. R2's "the local write must still reach the server" is
                    //   satisfied structurally: we leave the newer bytes at the
                    //   canonical path, and the pending push is LAZY (it reads
                    //   the file at drain time, push_client `process_event`), so
                    //   it POSTs the new bytes. The old behavior overwrote the
                    //   canonical path first, which is why the pending push then
                    //   re-read server bytes and pushed nothing.
                    //
                    //   incoming > observed  =>  the server genuinely advanced
                    //   past everything we have seen. Fall through: this may be a
                    //   real multi-writer divergence and the always-stash floor
                    //   stands.
                    //
                    // Uses ONLY tokens we already hold; records nothing, so it
                    // cannot forge lineage (B1 hazard) and does not touch the
                    // owner-gated R6 conflict-policy question. No token on either
                    // side (pre-R7b server omits `change_seq`, the note has no
                    // ADOPTED lineage, or its lineage is merely Observed /
                    // legacy-unprovenanced) => no causal evidence => fall
                    // through to the pre-existing behavior (always-stash
                    // floor). Fail-closed by construction.
                    let causally_not_newer = match (&self.base_seq_store, payload.change_seq) {
                        (Some(bs), Some(incoming)) => bs
                            .get_adopted(&payload.path)
                            .is_some_and(|adopted| incoming <= adopted),
                        _ => false,
                    };
                    if causally_not_newer {
                        warn!(
                            path = %payload.path,
                            change_seq,
                            incoming_seq = ?payload.change_seq,
                            adopted_seq = ?self
                                .base_seq_store
                                .as_ref()
                                .and_then(|bs| bs.get_adopted(&payload.path)),
                            "materializer R2 CAUSAL: incoming server version is NOT newer than the version we already ADOPTED (byte-verified) for this path - local bytes are a LATER local write racing our own materialization. PRESERVING local (push carries it up), NO conflict copy"
                        );
                        return Ok(MaterializeOutcome::Skipped(SkipReason::LocalEditPreserved));
                    }

                    // R1 (TKT-372e31b2): shadow-store scope-suspect. "Shadow
                    // absent" is R5's unknown-provenance signal ONLY when the
                    // store is readable. When it loaded in the suspect state
                    // every lookup mis-keys and misses, so R5 fires for the whole
                    // vault and every fork it mints is false. Refuse the write
                    // instead (local untouched, no stash, no overwrite); the push
                    // leg is already parked for the same reason. See
                    // SkipReason::ShadowScopeSuspect. Deliberately narrow: it
                    // does NOT touch the general R5 policy, which the S514 revert
                    // note below `decide()` and the owner-gated R6 conflict-policy
                    // ruling both reserve.
                    if shadow.is_none()
                        && self
                            .shadow_store
                            .as_ref()
                            .is_some_and(|s| s.vault_scope_suspect())
                    {
                        warn!(
                            path = %payload.path,
                            change_seq,
                            "materializer R1: shadow store is SCOPE-SUSPECT (vault_folders empty, prefixed keys present) so every lookup misses and R5 would fire vault-wide - REFUSING to mint a conflict copy, local preserved, pull skipped. Fix config vault_name and restart."
                        );
                        return Ok(MaterializeOutcome::Skipped(SkipReason::ShadowScopeSuspect));
                    }

                    // Conflict-storm circuit breaker (TKT-86ae42a3): a mass
                    // server-side divergence event (consolidation, managed-
                    // region contamination, a bulk server rewrite) resolves
                    // THOUSANDS of paths to Conflict in minutes. Organic
                    // concurrent editing never does. Past the threshold we
                    // refuse the whole write: local untouched, no stash, no
                    // overwrite — fail-closed toward local bytes; reconcile
                    // retries after the window.
                    let (breaker_open, just_latched) = self.conflict_breaker_open();
                    if breaker_open {
                        if just_latched {
                            // Exactly once per process: the transition is the
                            // event worth paging on. Subsequent refusals are
                            // debug-level so a storm cannot flood the journal
                            // (07-23: 546,523 identical WARN lines).
                            error!(
                                path = %payload.path,
                                change_seq,
                                threshold = self.config.conflict_storm_threshold,
                                window_secs = self.config.conflict_storm_window_secs,
                                "materializer CONFLICT-STORM BREAKER LATCHED: {} conflict mints inside {}s means mass server-side divergence, NOT organic concurrent editing. Refusing all further conflict mints for the lifetime of this process: local bytes preserved, pulls skipped, nothing stashed, nothing overwritten. Affected paths stay divergent ON PURPOSE. Fix the divergence cause, then restart the daemon (or call reset_conflict_storm_breaker) to re-arm.",
                                self.config.conflict_storm_threshold,
                                self.config.conflict_storm_window_secs,
                            );
                            if let Some(tray) = &self.tray_state {
                                if let Ok(mut w) = tray.write() {
                                    w.set_conflict_storm_latched(true);
                                }
                            }
                        } else {
                            debug!(
                                path = %payload.path,
                                change_seq,
                                "materializer CONFLICT-STORM BREAKER OPEN (latched): refusing conflict mint, local preserved, pull skipped"
                            );
                        }
                        return Ok(MaterializeOutcome::Skipped(
                            SkipReason::ConflictStormBreakerOpen,
                        ));
                    }
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
        // Wrote / Stashed / AlignedToCanonical success paths — IntegrityFailed
        // returned above, so a failed write never records a (false) in-sync
        // marker.
        //
        // B1 (S534): record ONLY in Live mode. In Shadow/Disabled the bytes
        // landed in the shadow tree (or nowhere), NOT the vault, so recording
        // shadow=server_hash here forges an "in-sync" baseline the VAULT never
        // received. The reconcile pass walks the vault, reads that forged marker
        // (shadow==server) against a still-stale vault file, and decides PUSH —
        // re-pushing stale bytes forever (the client half of the storm). Only a
        // genuine vault write earns the baseline marker.
        if matches!(self.mode, MaterializerMode::Live) {
            if let Some(sh) = &self.shadow_store {
                sh.record(&payload.path, &payload.sha256);
            }
            // R3 (THESEUS AR-002, TKT-166e1c07): record the observed base_seq at
            // the SAME post-byte-verify, Live-only point as the shadow hash. The
            // seq comes straight from the server response (`payload.change_seq`),
            // never a local assumption; a pre-R7b server omits it (None) so the
            // note stays unobserved and the next push fails closed (R4/R5). This
            // block is reached only after the integrity check passed (an
            // IntegrityFailed write returned earlier), so the exact bytes are
            // confirmed materialized before we claim the observation.
            if let (Some(bs), Some(seq)) = (&self.base_seq_store, payload.change_seq) {
                // ADOPTED: reached only after the post-write integrity check
                // confirmed the exact bytes on the local FS (Finding 1).
                bs.record_adopted(&payload.path, seq);
            }
        }

        if let Some(stash) = stash_path {
            Ok(MaterializeOutcome::Stashed { stash_path: stash })
        } else if alignment_pull {
            Ok(MaterializeOutcome::AlignedToCanonical { path: target })
        } else {
            Ok(MaterializeOutcome::Wrote { path: target })
        }
    }

    /// D2 (v0.4.28, B2'): guarded ack-materialize-back write. After the server
    /// ACCEPTS a push but returns a `server_hash` different from what we sent
    /// (it canonicalized or region-defended the content), the push client
    /// rewrites the local file to the canonical bytes THROUGH THIS ENTRY so
    /// the rewrite rides the standard machinery: echo-guard record-before-
    /// write (a bespoke path would re-trigger the watcher - the S492 feedback
    /// loop), per-path advisory lock, atomic tmp+rename, pre-write mtime
    /// restore (an identity rewrite is not an edit).
    ///
    /// Guards and ordering:
    /// * B2'a pre-rewrite guard: re-reads the file INSIDE the lock and
    ///   requires `sha256(file bytes) == expected_local_sha` (the sha of the
    ///   bytes the caller actually drained and POSTed - NOT the enqueue-time
    ///   `evt.content_sha`). Mismatch => `SkippedConcurrentEdit`, no write.
    /// * B2'c ordering: the shadow records `canonical_sha` strictly AFTER a
    ///   successful rename. Every error path returns BEFORE the record, so a
    ///   failed rewrite leaves the shadow STALE => the next reconcile pass
    ///   classifies PULL - the fail-closed direction (never the
    ///   shadow==server + local-diverged phantom-push-per-pass trap).
    /// * This method NEVER records the shadow on a skip; the caller decides
    ///   (the push client records the server hash on SkippedConcurrentEdit so
    ///   the pending push2 backfills the correct CAS base).
    ///
    /// `canonical_sha` MUST be the sha256 hex of `canonical_bytes` (the caller
    /// verified it against the server's hash - unverified local
    /// canonicalization is banned, B2'b).
    pub fn write_aligned_bytes(
        &self,
        rel_path: &str,
        canonical_bytes: &[u8],
        canonical_sha: &str,
        expected_local_sha: &str,
    ) -> Result<AlignOutcome, MaterializerError> {
        if matches!(self.mode, MaterializerMode::Disabled) {
            info!(
                "materializer_mode=disabled; skipping aligned write for {}",
                rel_path
            );
            return Ok(AlignOutcome::SkippedDisabled);
        }
        if !is_safe_path(rel_path) {
            return Err(MaterializerError::PathTraversal(rel_path.to_string()));
        }
        debug_assert_eq!(
            hex::encode(Sha256::digest(canonical_bytes)),
            canonical_sha,
            "caller must pass the verified sha of canonical_bytes"
        );

        let target = self.target_for(rel_path);

        // S492 echo-suppression: record BEFORE the disk write so the entry is
        // present when the watcher observes the rename (same pattern as
        // write()).
        if let Some(g) = &self.echo_guard {
            g.record(rel_path, canonical_sha);
        }

        // Same per-path lock write() takes: the read-compare-rename sequence
        // must not interleave with a concurrent materialize of this path.
        let lock = self.path_lock_for(&canonical_sync_path(rel_path));
        let _path_guard = lock.lock().unwrap_or_else(|p| p.into_inner());

        // B2'a: re-read and require the file still holds EXACTLY the drained
        // bytes.
        let current = match fs::read(&target) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                info!(path = %rel_path, "aligned write: file vanished before ack - skipping");
                return Ok(AlignOutcome::SkippedMissing);
            }
            Err(e) => return Err(MaterializerError::Io(e)),
        };
        let current_sha = hex::encode(Sha256::digest(&current));
        if current_sha != expected_local_sha {
            warn!(
                path = %rel_path,
                expected = %expected_local_sha,
                actual = %current_sha,
                "aligned write: file changed between drain and ack (B2'a) - SKIPPING rewrite, pending push will converge"
            );
            return Ok(AlignOutcome::SkippedConcurrentEdit { current_sha });
        }

        // Anti-strip guard (S513-class close-out, final-review fix wave): the
        // D2 fetch-fallback (`ack_materialize_back`'s `/note` GET when local
        // canonicalization doesn't verify) can hand back `canonical_bytes`
        // that lack YAML frontmatter the local note currently holds — the same
        // stripped-body class `write()`'s pull path already guards against via
        // `guard_no_frontmatter_strip`. A locally-canonicalized `canonical_bytes`
        // can never trip this (canonicalization always preserves frontmatter
        // presence), so this only fires on the fetch-fallback branch. Mirror
        // the pull-path guard: refuse the rewrite, record NOTHING in the
        // shadow (stays stale -> next pass falls to the guarded PULL path).
        if starts_with_frontmatter(&current) && !starts_with_frontmatter(canonical_bytes) {
            warn!(
                path = %rel_path,
                "aligned write ANTI-STRIP GUARD: canonical bytes (D2 fetch-fallback) drop YAML frontmatter local holds — REFUSING rewrite, shadow left stale"
            );
            return Ok(AlignOutcome::SkippedWouldStripFrontmatter);
        }

        // Capture the pre-write mtime: a content-identity rewrite must not
        // churn the note's modified ordering.
        let pre_mtime = fs::metadata(&target).ok().and_then(|m| m.modified().ok());

        // Atomic tmp+rename, same-dir (same pattern + platform handling as
        // write() step 8).
        let parent = target
            .parent()
            .expect("vault-relative target always has a parent");
        fs::create_dir_all(parent)?;
        let mut tmp = NamedTempFile::new_in(parent)?;
        tmp.write_all(canonical_bytes)?;
        tmp.flush()?;
        atomic_persist(tmp, &target)?;

        // Restore the pre-write mtime (best-effort, like restore_server_times).
        if let Some(mtime) = pre_mtime {
            let times = fs::FileTimes::new().set_modified(mtime);
            match fs::File::options().write(true).open(&target) {
                Ok(f) => {
                    if let Err(e) = f.set_times(times) {
                        warn!(path = %target.display(), error = %e, "aligned write: mtime restore failed");
                    }
                }
                Err(e) => {
                    warn!(path = %target.display(), error = %e, "aligned write: reopen for mtime restore failed");
                }
            }
        }

        // B2'c: rewrite FIRST (above), record SECOND (here). Every error path
        // returned before this line.
        if let Some(sh) = &self.shadow_store {
            sh.record(rel_path, canonical_sha);
        }
        info!(path = %rel_path, sha = %canonical_sha, "aligned write: local rewritten to server canonical bytes (D2)");
        Ok(AlignOutcome::Rewrote { path: target })
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

    /// TKT-6222df34 (ruling 1A, S511 D4 delete leg): preserve a divergent
    /// unpushed local edit as a `<stem>.conflict-from-<device>-<seq>.md`
    /// sibling BEFORE a server DELETE renames the file away. Same
    /// ConflictStash mechanism + naming as the push leg's CAS-409 stash
    /// (`PushClient::stash_local_on_conflict`); seq comes from the
    /// base_seq_store when known, else 0 (the push leg's unknown-seq value).
    /// Best-effort: a stash failure is logged and MUST NOT abort the delete
    /// (delete still wins; the fork is the preservation, not a veto).
    fn stash_local_before_delete(&self, path: &str, local_bytes: &[u8], local_raw_sha: &str) {
        let stash_root = match self.mode {
            MaterializerMode::Live => self.vaults_root.clone(),
            _ => self.shadow_root(),
        };
        let stasher = ConflictStash::new(stash_root, self.config.conflict_policy);
        let seq: u64 = self
            .base_seq_store
            .as_ref()
            .and_then(|s| s.get(path))
            .and_then(|s| u64::try_from(s).ok())
            .unwrap_or(0);
        // D5: key the echo_guard BEFORE the stash write so the file_watcher
        // never enqueues the conflict copy as a push (same belt-and-braces as
        // the R4/R5 Conflict arm in write_with_change_seq).
        let stash_target = stasher.compute_stash_path_public(path, &self.config.device_id, seq);
        if let (Some(g), Some(rel)) = (&self.echo_guard, self.rel_for_stash(&stash_target)) {
            g.record(&rel, local_raw_sha);
        }
        match stasher.write_stash(path, local_bytes, &self.config.device_id, seq) {
            Ok(stash) => {
                warn!(
                    path = %path,
                    stash = %stash.display(),
                    seq,
                    "materializer: server DELETE vs local edit, stashed local bytes before soft-delete (TKT-6222df34, S511 D4 delete leg)"
                );
            }
            Err(e) => {
                warn!(
                    path = %path,
                    error = ?e,
                    "materializer: delete-leg conflict stash FAILED, soft-delete proceeds, local bytes may be at risk (TKT-6222df34)"
                );
            }
        }
    }

    /// Soft-delete preserves the v0.2 contract (move to `<name>.deleted-<ts>`).
    /// In live mode it operates on the vault tree; in shadow mode on the
    /// runtime tree.  Disabled mode no-ops.
    pub fn soft_delete(&self, path: &str) -> Result<(), MaterializerError> {
        if !is_safe_path(path) {
            return Err(MaterializerError::PathTraversal(path.into()));
        }
        // RASP substrate fence — LIFTED (see write()'s step 3). classify_path
        // always returns Content now, so substrate deletes proceed as content.
        // Branch retained for fence restoration; never taken while rules empty.
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
        // TKT-6222df34 (ruling 1A): a server DELETE wins, but an unpushed
        // divergent local edit must survive as a conflict fork, exactly like
        // the push leg's CAS-409 stash (S511 D4). Divergence uses the SAME
        // basis as write()'s R2 check: the shadow store holds the last-synced
        // server RAW sha; a differing raw sha of the on-disk bytes means an
        // unpushed local edit. Shadow ABSENT = unknown provenance = NO stash
        // (deliberate: ~33k notes carry no baseline yet, and stashing on
        // absent shadow would spray forks on routine cleanups). Empty bytes
        // are never worth a fork. A read failure is logged and the delete
        // proceeds (stash is best-effort, the delete is the contract).
        if let Some(shadow) = self.shadow_store.as_ref().and_then(|s| s.get(path)) {
            match fs::read(&target) {
                Ok(local_bytes) if !local_bytes.is_empty() => {
                    let local_raw_sha = hex::encode(Sha256::digest(&local_bytes));
                    if shadow != local_raw_sha {
                        self.stash_local_before_delete(path, &local_bytes, &local_raw_sha);
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    warn!(
                        path = %path,
                        error = ?e,
                        "soft_delete: could not read local bytes for divergence check, deleting without stash (TKT-6222df34)"
                    );
                }
            }
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
    // (S514, TKT-d1a41f94: a global flip to local-wins here was reverted — it
    // breaks legitimate new-host/stale-local catch-up [verify_repair] and the
    // no-shadow case is fundamentally ambiguous. Local-wins for KNOWN paths is
    // handled by the D9 shadow-seed making them R2; the conflict storm is fixed
    // by idempotent stashing in conflict_stash::write_stash, not by this flip.)
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

/// True iff `content` begins with a YAML frontmatter fence (`---` on line 1),
/// tolerating a leading UTF-8 BOM and CRLF. Cheap structural check — does NOT
/// validate the YAML. Used only to detect the frontmatter-strip data-loss case.
pub fn starts_with_frontmatter(content: &[u8]) -> bool {
    let s = match std::str::from_utf8(content) {
        Ok(s) => s,
        Err(_) => return false,
    };
    let no_bom = s.strip_prefix('\u{feff}').unwrap_or(s);
    no_bom.starts_with("---\n") || no_bom.starts_with("---\r\n") || no_bom == "---"
}

/// S513 anti-strip guard (TKT-2dc9a17e). A pull/overwrite decision (R3
/// `PullClean` or R4/R5 `Conflict`) that would OVERWRITE a local note holding
/// YAML frontmatter with a server version that LACKS it is the frontmatter-strip
/// data-loss vector: the server still serves frontmatter-stripped bodies for
/// some notes, and a pull (R3 silently, R5 with a conflict copy) propagates that
/// stripping into the local vault — the exact disaster this whole change exists
/// to kill, reappearing on the PULL side. We refuse: downgrade to
/// `PreserveLocalEdit` so the daemon keeps the full local copy and pushes it UP
/// instead of pulling the stripped version DOWN. Structural + scope-independent:
/// it holds no matter how many server bodies are stripped. The deliberate
/// trade-off is that a *genuine* server-side frontmatter removal is not pulled
/// (rare; erring toward preserving the owner's metadata is correct). Only gates
/// the two overwriting decisions — Noop/PreserveLocalEdit are returned untouched.
pub fn guard_no_frontmatter_strip(
    decision: Decision,
    pull_would_strip_frontmatter: bool,
) -> Decision {
    if pull_would_strip_frontmatter && matches!(decision, Decision::PullClean | Decision::Conflict)
    {
        return Decision::PreserveLocalEdit;
    }
    decision
}

/// R6 (TKT-f74edf99): CONTENT-LEVEL direction safety. Returns true iff every
/// line of `server` appears in `local`, IN ORDER (a line-ordered subsequence) —
/// i.e. a local-wins push of `local` would drop NO server line. This is the
/// content-level containment check the requirement mandates: never mtime, never
/// size. The measured evidence for keying on this and not size: of 114 divergent
/// notes during the 2026-07-29/30 manual repair, size-direction called
/// local-larger 113/113, but line-level containment excluded 41 as unsafe — a
/// naive "newest/larger wins" would have discarded real server content.
///
/// PURE + exhaustively testable. A trailing final newline difference is ignored
/// (both sides are split on `\n`, so an empty trailing segment matches an empty
/// trailing segment or is absent on both). Bytes that are not valid UTF-8 fall
/// back to a whole-blob equality test (no line structure to reason about).
pub fn server_lines_contained_in_local(server: &[u8], local: &[u8]) -> bool {
    let (server, local) = match (std::str::from_utf8(server), std::str::from_utf8(local)) {
        (Ok(s), Ok(l)) => (s, l),
        // Non-text: the only safe containment claim is exact equality.
        _ => return server == local,
    };
    // Order-preserving subsequence match: walk local once, advancing through the
    // server lines as each is found. If we consume all server lines, every server
    // line is present in local in order — nothing would be lost.
    let mut server_lines = server.lines();
    let mut want = server_lines.next();
    for have in local.lines() {
        if let Some(w) = want {
            if w == have {
                want = server_lines.next();
            }
        } else {
            break;
        }
    }
    want.is_none()
}

/// R1 / F-B1.1 (TKT-989ad5f2): the two arms of a guard-hit (anti-strip)
/// resolution. Which arm applies is decided purely by whether the
/// frontmatter-normalized BODIES are byte-equal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GuardArm {
    /// Pure server-strip: the server dropped ONLY the frontmatter block; the
    /// body is byte-identical after BOM/EOL normalization. Preserve local +
    /// enqueue a compensating UP push (CAS base = the pull's server hash).
    PreserveAndPushUp,
    /// Genuine divergence: the bodies differ. Stash local OUTSIDE sync scope,
    /// align local to the server, update the shadow (the R4/R5 stash-then-align).
    StashThenAlign,
}

/// Strip a leading YAML frontmatter block (if present) and return the
/// BOM/EOL-normalized body. Unlike `normalize_for_diff` (which keeps the
/// frontmatter and strips only volatile fields), this removes the whole block
/// so a note that differs ONLY by the presence of frontmatter compares equal in
/// the body. Non-UTF-8 input passes through unchanged (never a false-equal).
pub fn body_after_frontmatter_normalized(content: &[u8]) -> Vec<u8> {
    let raw = match std::str::from_utf8(content) {
        Ok(s) => s,
        Err(_) => return content.to_vec(),
    };
    let no_bom = raw.strip_prefix('\u{feff}').unwrap_or(raw);
    let normalized: String = if no_bom.contains('\r') {
        no_bom.replace("\r\n", "\n").replace('\r', "\n")
    } else {
        no_bom.to_string()
    };
    let s = normalized.as_str();
    if !s.starts_with("---\n") {
        // No leading frontmatter: the whole (normalized) content is the body.
        return s.as_bytes().to_vec();
    }
    match find_frontmatter_end(s) {
        Some(fe) => s.as_bytes()[fe.body_start..].to_vec(),
        None => s.as_bytes().to_vec(),
    }
}

/// R1 / F-B1.1: classify a guard-hit resolution into its arm. `PreserveAndPushUp`
/// (ARM 1) iff the frontmatter-normalized bodies are byte-equal (pure
/// server-strip); otherwise `StashThenAlign` (ARM 2, genuine divergence). PURE +
/// table-tested; the caller has already established that a pull WOULD strip
/// local frontmatter.
pub fn classify_guard_arm(local_bytes: &[u8], server_bytes: &[u8]) -> GuardArm {
    if body_after_frontmatter_normalized(local_bytes)
        == body_after_frontmatter_normalized(server_bytes)
    {
        GuardArm::PreserveAndPushUp
    } else {
        GuardArm::StashThenAlign
    }
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
        let line_end = cursor + bytes[cursor..].iter().position(|&b| b == b'\n')?;
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
            modified: Some("2026-05-27T00:00:00Z".into()),
            file_mtime: None,
            created: None,
            change_seq: None,
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
            modified: Some("2026-05-31T00:00:00Z".into()),
            file_mtime: None,
            created: None,
            change_seq: None,
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
            modified: Some("2026-05-31T00:00:00Z".into()),
            file_mtime: None,
            created: None,
            change_seq: None,
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

    // ---- substrate now materializes as content ---------------------------
    //
    // "substrate must sync" (2026-06-20): the materializer no longer refuses
    // substrate paths. They write byte-faithfully like any note.

    #[test]
    fn substrate_pointer_file_materializes_as_content() {
        let (vaults, _w, m) = mk(MaterializerMode::Live, default_cfg());
        let out = m.write(&payload("00_VAULT.md", "x")).unwrap();
        assert!(
            matches!(out, MaterializeOutcome::Wrote { .. }),
            "00_VAULT.md must materialize as content, got {out:?}"
        );
        let target = vaults.path().join(VAULT).join("00_VAULT.md");
        let written = std::fs::read_to_string(&target).unwrap();
        assert!(written.contains("title: Test"), "frontmatter preserved");
        assert!(written.ends_with("x"), "body written byte-faithfully");
    }

    #[test]
    fn substrate_protocols_materializes_as_content() {
        let (vaults, _w, m) = mk(MaterializerMode::Live, default_cfg());
        let out = m
            .write(&payload("02_Projects/Protocols/foo.md", "x"))
            .unwrap();
        assert!(
            matches!(out, MaterializeOutcome::Wrote { .. }),
            "Protocols/ note must materialize as content, got {out:?}"
        );
        let target = vaults
            .path()
            .join(VAULT)
            .join("02_Projects/Protocols/foo.md");
        let written = std::fs::read_to_string(&target).unwrap();
        assert!(written.contains("title: Test"), "frontmatter preserved");
        assert!(written.ends_with("x"), "body written byte-faithfully");
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

    /// Superseded by D1 (v0.4.28): a local file whose ONLY delta from the
    /// server canonical is the diff-stripped `updated:` frontmatter value is
    /// normalized-equal but raw-unequal, so it is now an ALIGNMENT PULL
    /// (rewritten to the server's exact bytes, including the newer
    /// `updated:`) rather than a byte-preserving Noop skip. Per the brief's
    /// precision note: `updated` is deliberately excluded from the identity
    /// basis as churn-noise, and the server canonical is authoritative, so
    /// converging local to the server's `updated:` value here is intended.
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
            modified: Some("2026-05-27T00:00:00Z".into()),
            file_mtime: None,
            created: None,
            change_seq: None,
            enriched_body: Some(serialized.clone()),
        };
        let out = m.write(&p).unwrap();
        assert_eq!(
            out,
            MaterializeOutcome::AlignedToCanonical {
                path: target.clone()
            }
        );
        // D1: local is rewritten to the server's exact canonical bytes
        // (newer `updated:` included) — no stash, zero content difference.
        assert_eq!(std::fs::read_to_string(&target).unwrap(), serialized);
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
    /// record (shadow absent => R5, unknown provenance) ALWAYS stashes the
    /// local loser before materializing the server winner. There is no silent-
    /// overwrite cell for divergent content; both byte-sets survive on disk
    /// (I-83 NEVER-SILENT-OVERWRITE). S514 (TKT-d1a41f94) kept this behavior (a
    /// local-wins flip broke catch-up) and instead made the stash IDEMPOTENT so
    /// a recurring divergence yields ONE conflict copy, not the 209-file storm
    /// (see conflict_stash::write_stash_idempotent_for_identical_content).
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
                assert!(stash_path.exists(), "stash file must exist");
                let stash_content = std::fs::read_to_string(&stash_path).unwrap();
                assert_eq!(stash_content, "old-local-divergent");
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

    // ---- TKT-86ae42a3: conflict-storm regression + circuit breaker ---------

    /// THE 07-18 storm regression, end-to-end through write(). Prod shape
    /// (post-B2): vaults_root IS the sync root and payload paths are
    /// sync-root-relative. The shadow store still holds the pre-v0.4.28
    /// vaults-root-relative key (`Mainframe/...`) whose value equals the
    /// CURRENT server hash — i.e. the server has NOT moved since last sync and
    /// the local file carries a genuine edit. Correct verdict: R2
    /// LocalEditPreserved. Pre-fix code missed the prefixed key
    /// (`shadow_present=false`), fell to R5, and minted a conflict stash —
    /// 2,395 times on link on 2026-07-18. This test FAILS on pre-fix code.
    #[test]
    fn b2_prefix_migrated_shadow_prevents_r5_conflict_storm() {
        let vaults_tmp = TempDir::new().unwrap();
        let ws_tmp = TempDir::new().unwrap();
        let sync_root = vaults_tmp.path().join("Mainframe");
        std::fs::create_dir_all(sync_root.join("01_Notes")).unwrap();

        let server_body = "server canonical body\n";
        let p = NotePayload {
            path: "01_Notes/storm.md".into(),
            frontmatter: serde_json::Value::Null,
            body: server_body.into(),
            sha256: sha256_hex(server_body),
            modified: Some("2026-07-18T00:00:00Z".into()),
            file_mtime: None,
            created: None,
            change_seq: None,
            enriched_body: Some(server_body.to_string()),
        };
        // Local diverges from the server canonical: a genuine local edit.
        std::fs::write(sync_root.join("01_Notes/storm.md"), "local edit\n").unwrap();

        // Pre-B2 shadow file: legacy vault-prefixed key, value == the CURRENT
        // server hash (server unchanged since we last synced).
        let shadow_file = ws_tmp.path().join("shadow.json");
        let mut legacy = std::collections::HashMap::new();
        legacy.insert("Mainframe/01_Notes/storm.md".to_string(), p.sha256.clone());
        std::fs::write(&shadow_file, serde_json::to_vec(&legacy).unwrap()).unwrap();
        let shadow = crate::sync_shadow::ShadowStore::load_with_vault_folders(
            shadow_file,
            vec!["Mainframe".into()],
        );

        let m = Materializer::new(
            sync_root.clone(),
            Some("shadow/".to_string()),
            MaterializerMode::Live,
            ws_tmp.path().to_path_buf(),
            SLUG.to_string(),
            default_cfg(),
        )
        .with_shadow_store(shadow);

        let out = m.write(&p).unwrap();
        assert_eq!(
            out,
            MaterializeOutcome::Skipped(SkipReason::LocalEditPreserved),
            "R2 must preserve the local edit via the migrated shadow key; \
             pre-fix code read shadow_present=false and R5-minted a conflict stash"
        );
        // Local bytes untouched, and NO conflict stash sibling was minted.
        assert_eq!(
            std::fs::read_to_string(sync_root.join("01_Notes/storm.md")).unwrap(),
            "local edit\n"
        );
        for entry in std::fs::read_dir(sync_root.join("01_Notes"))
            .unwrap()
            .flatten()
        {
            assert!(
                !entry
                    .file_name()
                    .to_string_lossy()
                    .contains(".conflict-from-"),
                "no conflict stash may be minted for a genuine local edit"
            );
        }
    }

    /// Circuit breaker: with threshold N, a mass-divergence event (many
    /// distinct paths resolving to R5 Conflict) mints at most N stashes; the
    /// rest are refused with local bytes left untouched.
    #[test]
    fn conflict_storm_breaker_caps_mints() {
        let cfg = MaterializerConfig {
            conflict_storm_threshold: 3,
            conflict_storm_window_secs: 600,
            ..default_cfg()
        };
        let (vaults, _ws, m) = mk(MaterializerMode::Live, cfg);
        let dir = vaults.path().join(VAULT).join("01_Notes");
        std::fs::create_dir_all(&dir).unwrap();

        let mut stashed = 0;
        let mut refused = 0;
        for i in 0..5 {
            let rel = format!("01_Notes/mass-{i}.md");
            // Divergent local, NO shadow store attached => R5 Conflict.
            std::fs::write(dir.join(format!("mass-{i}.md")), "local bytes").unwrap();
            match m.write(&payload(&rel, "server bytes")).unwrap() {
                MaterializeOutcome::Stashed { .. } => stashed += 1,
                MaterializeOutcome::Skipped(SkipReason::ConflictStormBreakerOpen) => {
                    refused += 1;
                    // Refused => local file untouched.
                    assert_eq!(
                        std::fs::read_to_string(dir.join(format!("mass-{i}.md"))).unwrap(),
                        "local bytes"
                    );
                }
                other => panic!("expected Stashed or breaker-open, got {other:?}"),
            }
        }
        assert_eq!((stashed, refused), (3, 2), "threshold must cap the mints");
        let stash_files = std::fs::read_dir(&dir)
            .unwrap()
            .flatten()
            .filter(|e| e.file_name().to_string_lossy().contains(".conflict-from-"))
            .count();
        assert_eq!(stash_files, 3, "exactly threshold stash files on disk");
    }

    /// THE 2026-07-23 EPIDEMIC, in miniature. The pre-latch breaker pruned its
    /// mint deque to the window and then admitted `threshold` MORE, so a
    /// sustained mass-divergence event minted `threshold` forks every window
    /// forever: 7,371 real stashes over ~24.8 h at 50/600 s (148.8 windows x 50
    /// = 7,440 predicted, measured within 1%).
    ///
    /// A 1-second window makes that re-arm observable in a unit test. On the
    /// pre-fix code the post-window writes are `Stashed` again and this fails
    /// with 6 stash files; latched, the storm is bounded to `threshold` for the
    /// lifetime of the process no matter how many windows elapse.
    #[test]
    fn conflict_storm_breaker_latches_and_never_rearms_after_the_window() {
        let cfg = MaterializerConfig {
            conflict_storm_threshold: 3,
            // 2 s (not 1 s) so the four in-window writes below cannot themselves
            // outlive the window on a loaded CI box and prune the deque early —
            // that would silently turn this regression test green for the wrong
            // reason. The sleep is sized against this, not the reverse.
            conflict_storm_window_secs: 2,
            ..default_cfg()
        };
        let (vaults, _ws, m) = mk(MaterializerMode::Live, cfg);
        let dir = vaults.path().join(VAULT).join("01_Notes");
        std::fs::create_dir_all(&dir).unwrap();

        let write_one = |i: usize| {
            let rel = format!("01_Notes/storm-{i}.md");
            std::fs::write(dir.join(format!("storm-{i}.md")), "local bytes").unwrap();
            m.write(&payload(&rel, "server bytes")).unwrap()
        };

        // Window 1: threshold admitted, then the latch trips.
        for i in 0..3 {
            assert!(
                matches!(write_one(i), MaterializeOutcome::Stashed { .. }),
                "mint {i} is inside the threshold and must stash"
            );
        }
        assert!(!m.conflict_storm_latched(), "not latched until exceeded");
        assert_eq!(
            write_one(3),
            MaterializeOutcome::Skipped(SkipReason::ConflictStormBreakerOpen),
            "the threshold+1-th mint must trip the breaker"
        );

        // Let the sliding window fully expire — this is the exact moment the old
        // code re-armed and resumed minting. Asserted BEFORE the latch getter so
        // that on pre-fix code this test fails on the OBSERVABLE EPIDEMIC
        // MECHANISM (forks resuming after the window) rather than on an
        // implementation detail of the latch flag.
        std::thread::sleep(std::time::Duration::from_millis(2_500));

        for i in 4..8 {
            assert_eq!(
                write_one(i),
                MaterializeOutcome::Skipped(SkipReason::ConflictStormBreakerOpen),
                "write {i}: a LATCHED breaker must stay open across window \
                 expiry — re-arming is what minted 7,371 forks on 2026-07-23"
            );
            // Fail-closed toward local on every refusal, exactly as before.
            assert_eq!(
                std::fs::read_to_string(dir.join(format!("storm-{i}.md"))).unwrap(),
                "local bytes",
                "write {i}: refused means local bytes untouched"
            );
        }

        let stash_files = std::fs::read_dir(&dir)
            .unwrap()
            .flatten()
            .filter(|e| e.file_name().to_string_lossy().contains(".conflict-from-"))
            .count();
        assert_eq!(
            stash_files, 3,
            "a whole storm must be bounded to `threshold` forks, not \
             `threshold` per window"
        );
        assert!(
            m.conflict_storm_latched(),
            "the breaker must report itself LATCHED so the state is observable"
        );
    }

    /// The latch is shared through clones. The 07-23 storm arrived via the
    /// reconcile clone while the SSE consumer held its own handle; a per-clone
    /// latch would have given each lane its own budget and multiplied the cap by
    /// the number of lanes.
    #[test]
    fn conflict_storm_latch_is_shared_across_clones() {
        let cfg = MaterializerConfig {
            conflict_storm_threshold: 2,
            conflict_storm_window_secs: 600,
            ..default_cfg()
        };
        let (vaults, _ws, m) = mk(MaterializerMode::Live, cfg);
        let dir = vaults.path().join(VAULT).join("01_Notes");
        std::fs::create_dir_all(&dir).unwrap();
        let clone = m.clone();

        for i in 0..3 {
            let rel = format!("01_Notes/shared-{i}.md");
            std::fs::write(dir.join(format!("shared-{i}.md")), "local bytes").unwrap();
            let _ = m.write(&payload(&rel, "server bytes")).unwrap();
        }
        assert!(m.conflict_storm_latched(), "primary latched");
        assert!(
            clone.conflict_storm_latched(),
            "a clone must observe the SAME latch, not its own budget"
        );

        // And the clone refuses too, rather than starting a fresh window.
        std::fs::write(dir.join("shared-via-clone.md"), "local bytes").unwrap();
        assert_eq!(
            clone
                .write(&payload("01_Notes/shared-via-clone.md", "server bytes"))
                .unwrap(),
            MaterializeOutcome::Skipped(SkipReason::ConflictStormBreakerOpen),
            "clone must honor the shared latch"
        );
    }

    /// Explicit reset is the documented recovery path (fix the cause, then
    /// re-arm) and must restore normal minting — otherwise the latch would be a
    /// one-way wedge with no remedy short of a restart.
    #[test]
    fn reset_conflict_storm_breaker_re_arms_the_mint_budget() {
        let cfg = MaterializerConfig {
            conflict_storm_threshold: 1,
            conflict_storm_window_secs: 600,
            ..default_cfg()
        };
        let (vaults, _ws, m) = mk(MaterializerMode::Live, cfg);
        let dir = vaults.path().join(VAULT).join("01_Notes");
        std::fs::create_dir_all(&dir).unwrap();

        for i in 0..2 {
            let rel = format!("01_Notes/reset-{i}.md");
            std::fs::write(dir.join(format!("reset-{i}.md")), "local bytes").unwrap();
            let _ = m.write(&payload(&rel, "server bytes")).unwrap();
        }
        assert!(m.conflict_storm_latched(), "latched after exceeding");

        m.reset_conflict_storm_breaker();
        assert!(!m.conflict_storm_latched(), "reset clears the latch");

        std::fs::write(dir.join("reset-after.md"), "local bytes").unwrap();
        assert!(
            matches!(
                m.write(&payload("01_Notes/reset-after.md", "server bytes"))
                    .unwrap(),
                MaterializeOutcome::Stashed { .. }
            ),
            "after reset the always-stash floor for genuine divergence is back"
        );
    }

    /// Threshold 0 disables the breaker entirely (documented behavior) and must
    /// therefore never latch, or setting 0 would silently become "latch on the
    /// first conflict".
    #[test]
    fn threshold_zero_disables_the_breaker_and_never_latches() {
        let cfg = MaterializerConfig {
            conflict_storm_threshold: 0,
            conflict_storm_window_secs: 600,
            ..default_cfg()
        };
        let (vaults, _ws, m) = mk(MaterializerMode::Live, cfg);
        let dir = vaults.path().join(VAULT).join("01_Notes");
        std::fs::create_dir_all(&dir).unwrap();

        for i in 0..6 {
            let rel = format!("01_Notes/nobreaker-{i}.md");
            std::fs::write(dir.join(format!("nobreaker-{i}.md")), "local bytes").unwrap();
            assert!(
                matches!(
                    m.write(&payload(&rel, "server bytes")).unwrap(),
                    MaterializeOutcome::Stashed { .. }
                ),
                "mint {i}: threshold 0 means no breaker at all"
            );
        }
        assert!(
            !m.conflict_storm_latched(),
            "a disabled breaker must never latch"
        );
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
    fn delete_substrate_path_now_soft_deletes() {
        // "substrate must sync": soft_delete no longer refuses substrate. With
        // no target present it is a no-op Ok (same as any missing content path).
        let (_v, _w, m) = mk(MaterializerMode::Shadow, default_cfg());
        assert!(m.soft_delete("00_VAULT.md").is_ok());
    }

    // ---- TKT-6222df34: delete-leg conflict stash ---------------------------

    /// Live-mode fixture with an attached ShadowStore, plus the wire path and
    /// on-disk target for one note. Files are written directly to the vault
    /// tree (the divergence basis is raw on-disk bytes vs shadow, same as R2).
    fn mk_delete_leg() -> (
        TempDir,
        TempDir,
        TempDir,
        Materializer,
        std::sync::Arc<crate::sync_shadow::ShadowStore>,
        String,
        PathBuf,
    ) {
        use crate::sync_shadow::ShadowStore;
        let sdir = TempDir::new().unwrap();
        let shadow = ShadowStore::load(sdir.path().join("shadow.json"));
        let (vaults, ws, m_base) = mk(MaterializerMode::Live, default_cfg());
        let m = m_base.with_shadow_store(shadow.clone());
        let wire = format!("{VAULT}/01_Inbox/foo.md");
        let target = vaults.path().join(&wire);
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();
        (vaults, ws, sdir, m, shadow, wire, target)
    }

    fn count_by_prefix(dir: &Path, prefix: &str) -> usize {
        std::fs::read_dir(dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().starts_with(prefix))
            .count()
    }

    /// Shadow present + local raw sha differs => the local bytes survive as a
    /// `.conflict-from-<device>-0.md` sibling (seq 0: no base_seq known, same
    /// as the push leg's bare-409 stash) AND the delete still wins.
    #[test]
    fn delete_leg_divergent_local_edit_stashed_then_soft_deleted() {
        let (vaults, _ws, _s, m, shadow, wire, target) = mk_delete_leg();
        shadow.record(&wire, &sha256_hex("server v1"));
        std::fs::write(&target, "local edit v2").unwrap();

        m.soft_delete(&wire).unwrap();

        let dir = vaults.path().join(VAULT).join("01_Inbox");
        assert!(!target.exists(), "delete must still win");
        assert_eq!(count_by_prefix(&dir, "foo.md.deleted-"), 1);
        let stash = dir.join("foo.conflict-from-morpheus-0.md");
        assert!(stash.exists(), "divergent local edit must be forked");
        assert_eq!(
            std::fs::read_to_string(&stash).unwrap(),
            "local edit v2",
            "stash must hold the LOCAL bytes, not the server tombstone"
        );
    }

    /// Shadow present + local raw sha EQUALS shadow (nothing unpushed) => no
    /// fork, plain soft-delete.
    #[test]
    fn delete_leg_identical_local_content_no_stash() {
        let (vaults, _ws, _s, m, shadow, wire, target) = mk_delete_leg();
        std::fs::write(&target, "synced bytes").unwrap();
        shadow.record(&wire, &sha256_hex("synced bytes"));

        m.soft_delete(&wire).unwrap();

        let dir = vaults.path().join(VAULT).join("01_Inbox");
        assert!(!target.exists());
        assert_eq!(count_by_prefix(&dir, "foo.md.deleted-"), 1);
        assert_eq!(
            count_by_prefix(&dir, "foo.conflict-from-"),
            0,
            "in-sync local must not fork"
        );
    }

    /// Shadow ABSENT (unknown provenance, the ~33k no-baseline population) =>
    /// no fork, plain soft-delete. Stash is scoped to shadow-present-and-
    /// differs by ruling 1A.
    #[test]
    fn delete_leg_no_shadow_entry_no_stash() {
        let (vaults, _ws, _s, m, _shadow, wire, target) = mk_delete_leg();
        std::fs::write(&target, "whatever local holds").unwrap();

        m.soft_delete(&wire).unwrap();

        let dir = vaults.path().join(VAULT).join("01_Inbox");
        assert!(!target.exists());
        assert_eq!(count_by_prefix(&dir, "foo.md.deleted-"), 1);
        assert_eq!(
            count_by_prefix(&dir, "foo.conflict-from-"),
            0,
            "absent shadow must not fork"
        );
    }

    /// Re-deleting the same divergent state (recreate + second DELETE) reuses
    /// the byte-identical stash (S514 idempotency) instead of appending -2/-3.
    #[test]
    fn delete_leg_repeated_divergent_delete_stash_is_idempotent() {
        let (vaults, _ws, _s, m, shadow, wire, target) = mk_delete_leg();
        shadow.record(&wire, &sha256_hex("server v1"));

        for _ in 0..2 {
            std::fs::write(&target, "local edit v2").unwrap();
            m.soft_delete(&wire).unwrap();
        }

        let dir = vaults.path().join(VAULT).join("01_Inbox");
        assert_eq!(
            count_by_prefix(&dir, "foo.conflict-from-"),
            1,
            "identical divergent bytes must converge on ONE stash"
        );
        assert_eq!(count_by_prefix(&dir, "foo.md.deleted-"), 2);
    }

    /// A known base_seq names the stash (`-<seq>`), mirroring the push leg's
    /// deterministic naming; unknown stays 0 (covered above).
    #[test]
    fn delete_leg_stash_uses_base_seq_when_known() {
        let (vaults, _ws, _s, m, shadow, wire, target) = mk_delete_leg();
        let bdir = TempDir::new().unwrap();
        let bs = crate::base_seq_store::BaseSeqStore::load(bdir.path().join("base_seq.json"));
        bs.record_adopted(&wire, 7);
        let m = m.with_base_seq_store(bs);
        shadow.record(&wire, &sha256_hex("server v1"));
        std::fs::write(&target, "local edit v2").unwrap();

        m.soft_delete(&wire).unwrap();

        let dir = vaults.path().join(VAULT).join("01_Inbox");
        assert!(dir.join("foo.conflict-from-morpheus-7.md").exists());
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

    /// B1 (S534): a Shadow-mode write goes to the shadow tree, NOT the vault, so
    /// it must NEVER record a shadow baseline. Recording shadow=server_hash there
    /// forged the "vault in sync" marker verify_repair reads as drift+shadow==
    /// server ⇒ PUSH ⇒ the storm. Covers BOTH record sites: the first write
    /// (Wrote/post-write path) and the second identical write (R1 byte-strict
    /// Noop path) — neither may record while in Shadow mode.
    #[test]
    fn b1_shadow_mode_write_does_not_record_baseline() {
        use crate::sync_shadow::ShadowStore;
        let dir = TempDir::new().unwrap();
        let shadow = ShadowStore::load(dir.path().join("shadow.json"));
        let (_v, _ws, m_base) = mk(MaterializerMode::Shadow, default_cfg());
        let m = m_base.with_shadow_store(shadow.clone());
        let p = payload("01_Inbox/foo.md", "hello");

        // First write → shadow tree (post-write record site). Must NOT record.
        let out = m.write(&p).unwrap();
        assert!(
            matches!(out, MaterializeOutcome::Wrote { .. }),
            "got {out:?}"
        );
        assert_eq!(
            shadow.get(&p.path),
            None,
            "shadow-mode write must NOT forge a vault baseline (post-write site)"
        );

        // Second identical write → R1 byte-strict Noop record site. Still none.
        let out2 = m.write(&p).unwrap();
        assert_eq!(
            out2,
            MaterializeOutcome::Skipped(SkipReason::IdenticalToLocal)
        );
        assert_eq!(
            shadow.get(&p.path),
            None,
            "shadow-mode R1 Noop must NOT forge a vault baseline (Noop site)"
        );
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
                modified: Some("2026-05-29T00:00:00Z".into()),
                file_mtime: None,
                created: None,
                change_seq: None,
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

    /// R8 / T3 harness (TKT-989ad5f2): materializer wired with a shadow store
    /// AND a push-journal handle, so the anti-strip ARM-1 compensating push can
    /// be observed. Returns the journal so tests can drain it.
    fn mk_with_shadow_and_journal(
        mode: MaterializerMode,
    ) -> (
        TempDir,
        TempDir,
        Materializer,
        Arc<ShadowStore>,
        Arc<Mutex<PushJournal>>,
    ) {
        let (v, w, m) = mk(mode, default_cfg());
        let sdir = Box::leak(Box::new(TempDir::new().unwrap()));
        let shadow = ShadowStore::load(sdir.path().join("shadow.json"));
        let jdir = Box::leak(Box::new(TempDir::new().unwrap()));
        let journal = Arc::new(Mutex::new(
            PushJournal::open(&jdir.path().join("push_journal.jsonl")).unwrap(),
        ));
        let m = m
            .with_shadow_store(shadow.clone())
            .with_push_journal(journal.clone());
        (v, w, m, shadow, journal)
    }

    /// T3 (R1 / F-B1.1 ARM 1): a PURE server-strip (server dropped the
    /// frontmatter block; body byte-identical) PRESERVES local AND enqueues a
    /// compensating UP push whose CAS base is the server hash from the pull. The
    /// local file is byte-unchanged (so the watcher never fires — the enqueue is
    /// what breaks the phantom-pull deadlock). FAILS on pre-fix code: the guard
    /// only downgraded to LocalEditPreserved and enqueued NOTHING.
    #[test]
    fn t3_guard_arm1_pure_strip_preserves_local_and_enqueues_compensating_push() {
        let (vaults, _ws, m, shadow, journal) = mk_with_shadow_and_journal(MaterializerMode::Live);
        let rel = format!("{VAULT}/01_Inbox/strip.md");
        let target = vaults.path().join(VAULT).join("01_Inbox/strip.md");
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();

        // Local holds frontmatter + body; it equals the last-synced shadow
        // (local untouched since sync => the PullClean basis).
        let local = "---\ntitle: Keep Me\n---\nSHARED BODY\n";
        std::fs::write(&target, local).unwrap();
        shadow.record(&rel, &sha256_hex(local));

        // Server dropped the frontmatter, but the BODY is byte-identical.
        let server_body = "SHARED BODY\n";
        let server = NotePayload {
            path: rel.clone(),
            frontmatter: serde_json::json!({}),
            body: server_body.into(),
            sha256: sha256_hex(server_body),
            modified: None,
            file_mtime: None,
            created: None,
            change_seq: None,
            enriched_body: Some(server_body.to_string()),
        };

        let out = m.write(&server).unwrap();
        assert_eq!(
            out,
            MaterializeOutcome::Skipped(SkipReason::GuardPreserveLocalPushUp {
                enqueued_push: true
            }),
            "ARM 1 must preserve local AND enqueue a compensating push, got {out:?}"
        );
        // Local frontmatter preserved verbatim — the server strip is NOT applied.
        assert_eq!(
            std::fs::read_to_string(&target).unwrap(),
            local,
            "local frontmatter must be preserved (guard never strips)"
        );
        // A compensating UP push was enqueued, CAS base = the server hash.
        let batch = journal.lock().unwrap().drain(10).unwrap();
        assert_eq!(batch.len(), 1, "exactly one compensating push enqueued");
        assert_eq!(batch[0].0.path, rel);
        assert_eq!(batch[0].0.action, PushAction::Modify);
        assert_eq!(
            batch[0].0.base_hash,
            PushBase::KnownBase(sha256_hex(server_body)),
            "CAS base must be the server hash from the pull payload"
        );
    }

    /// T3 (R1 / F-B1.1 ARM 2): GENUINE divergence (server dropped frontmatter
    /// AND changed the body) STASHES local then ALIGNS to the server — local
    /// survives as a conflict copy, nothing enqueued. FAILS on pre-fix code:
    /// the guard downgraded to LocalEditPreserved (no stash, no align, no
    /// convergence — the phantom-pull deadlock).
    #[test]
    fn t3_guard_arm2_divergence_stashes_then_aligns() {
        let (vaults, _ws, m, shadow, journal) = mk_with_shadow_and_journal(MaterializerMode::Live);
        let rel = format!("{VAULT}/01_Inbox/diverge.md");
        let target = vaults.path().join(VAULT).join("01_Inbox/diverge.md");
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();

        let local = "---\ntitle: Keep Me\n---\nLOCAL BODY\n";
        std::fs::write(&target, local).unwrap();
        shadow.record(&rel, &sha256_hex(local));

        // Server dropped frontmatter AND changed the body -> genuine divergence.
        let server_body = "SERVER BODY (different)\n";
        let server = NotePayload {
            path: rel.clone(),
            frontmatter: serde_json::json!({}),
            body: server_body.into(),
            sha256: sha256_hex(server_body),
            modified: None,
            file_mtime: None,
            created: None,
            change_seq: None,
            enriched_body: Some(server_body.to_string()),
        };

        let out = m.write(&server).unwrap();
        assert!(
            matches!(out, MaterializeOutcome::Stashed { .. }),
            "ARM 2 must stash-then-align (Stashed), got {out:?}"
        );
        // Local aligned to server; the losing local bytes survive as a stash.
        assert_eq!(
            std::fs::read_to_string(&target).unwrap(),
            server_body,
            "ARM 2 aligns local to server"
        );
        let dir = target.parent().unwrap();
        assert!(
            std::fs::read_dir(dir)
                .unwrap()
                .flatten()
                .any(|e| e.file_name().to_string_lossy().contains(".conflict-from-")),
            "ARM 2 must stash the losing local bytes as a conflict copy"
        );
        // ARM 2 does NOT enqueue a compensating push (it converges via align).
        assert_eq!(
            journal.lock().unwrap().drain(10).unwrap().len(),
            0,
            "ARM 2 enqueues no compensating push"
        );
    }

    /// PURE classify_guard_arm table (R1 / F-B1.1): body-equal (pure strip) ->
    /// ARM 1; body-differ -> ARM 2. Independent of frontmatter presence.
    #[test]
    fn t3_classify_guard_arm_table() {
        // Local has frontmatter, server stripped it, body identical -> ARM 1.
        assert_eq!(
            classify_guard_arm(b"---\nx: 1\n---\nBODY\n", b"BODY\n"),
            GuardArm::PreserveAndPushUp
        );
        // Body differs -> ARM 2.
        assert_eq!(
            classify_guard_arm(b"---\nx: 1\n---\nBODY A\n", b"BODY B\n"),
            GuardArm::StashThenAlign
        );
        // CRLF-only body delta is still "equal" (normalized) -> ARM 1.
        assert_eq!(
            classify_guard_arm(b"---\nx: 1\n---\nL1\r\nL2\n", b"L1\nL2\n"),
            GuardArm::PreserveAndPushUp
        );
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

    /// S513 anti-strip guard (TKT-2dc9a17e): a pull/overwrite (R3 PullClean or
    /// R4/R5 Conflict) that would DROP YAML frontmatter local holds MUST be
    /// downgraded to PreserveLocalEdit (keep local, push up). Without this, the
    /// server's frontmatter-stripped bodies propagate DOWN into local vaults via
    /// pull. Noop/PreserveLocalEdit and the no-strip case pass through untouched.
    #[test]
    fn guard_downgrades_frontmatter_stripping_pulls() {
        // strip=true: the two OVERWRITING decisions become PreserveLocalEdit.
        assert_eq!(
            guard_no_frontmatter_strip(Decision::PullClean, true),
            Decision::PreserveLocalEdit,
            "R3 silent pull that strips frontmatter must be refused"
        );
        assert_eq!(
            guard_no_frontmatter_strip(Decision::Conflict, true),
            Decision::PreserveLocalEdit,
            "R5 conflict pull that strips frontmatter must be refused"
        );
        // strip=true but NON-overwriting decisions are untouched.
        assert_eq!(
            guard_no_frontmatter_strip(Decision::Noop, true),
            Decision::Noop
        );
        assert_eq!(
            guard_no_frontmatter_strip(Decision::PreserveLocalEdit, true),
            Decision::PreserveLocalEdit
        );
        // strip=false: zero behavior change (every decision passes through).
        assert_eq!(
            guard_no_frontmatter_strip(Decision::PullClean, false),
            Decision::PullClean
        );
        assert_eq!(
            guard_no_frontmatter_strip(Decision::Conflict, false),
            Decision::Conflict
        );
        assert_eq!(
            guard_no_frontmatter_strip(Decision::Noop, false),
            Decision::Noop
        );
        assert_eq!(
            guard_no_frontmatter_strip(Decision::PreserveLocalEdit, false),
            Decision::PreserveLocalEdit
        );
    }

    #[test]
    fn starts_with_frontmatter_detects_yaml_fence() {
        assert!(starts_with_frontmatter(b"---\naliases: []\n---\nbody"));
        assert!(starts_with_frontmatter(b"---\r\ntype: note\r\n---\r\nbody"));
        assert!(starts_with_frontmatter(
            "\u{feff}---\nx: 1\n---\n".as_bytes()
        ));
        assert!(!starts_with_frontmatter(
            b"> [!info] Contact Info\nno frontmatter here"
        ));
        assert!(!starts_with_frontmatter(
            b"# Heading\n\n---\nnot a leading fence"
        ));
        assert!(!starts_with_frontmatter(b""));
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

    /// D11 (superseded by D1, v0.4.28): a CRLF (Windows) local file vs an LF
    /// (Unix) server body with the SAME logical content must NOT be treated
    /// as a divergence (no conflict, no false local-edit). Pre-D1 this
    /// resolved as a NOOP skip leaving the CRLF file untouched forever (the
    /// B1' alternation: every byte-strict comparer downstream kept seeing
    /// drift). D1 splits R1 byte-strict: normalized-equal-but-raw-unequal is
    /// now an ALIGNMENT PULL that rewrites local to the server's exact
    /// canonical bytes, still with zero conflict/stash.
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
            MaterializeOutcome::AlignedToCanonical {
                path: target.clone()
            },
            "CRLF/BOM-only difference must normalize to identical (R1) and align, got {out:?}"
        );
        // D1: the local CRLF/BOM file is rewritten to the server's exact
        // canonical bytes (no conflict, no stash — zero content difference).
        assert_eq!(std::fs::read_to_string(&target).unwrap(), server_bytes);
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

    // ---- write_aligned_bytes anti-strip guard (final-review fix wave, S513-class) ----

    /// A frontmatter-bearing local file + frontmatterless `canonical_bytes`
    /// (the D2 `/note` fetch-fallback shape) must SKIP the rewrite entirely:
    /// file untouched on disk, nothing recorded in the shadow (stays stale so
    /// the next reconcile pass falls to the guarded pull path).
    #[test]
    fn write_aligned_bytes_refuses_frontmatter_strip() {
        let (vaults, _ws, m, shadow) = mk_with_shadow(MaterializerMode::Live);
        let rel = format!("{VAULT}/01_Inbox/fm.md");
        let target = vaults.path().join(&rel);
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();

        let local_bytes = b"---\ntitle: keep me\n---\nbody\n".to_vec();
        std::fs::write(&target, &local_bytes).unwrap();
        let local_sha = hex::encode(Sha256::digest(&local_bytes));

        // canonical_bytes lacks frontmatter entirely — the stripped-body shape.
        let canonical_bytes = b"body\n".to_vec();
        let canonical_sha = hex::encode(Sha256::digest(&canonical_bytes));

        let out = m
            .write_aligned_bytes(&rel, &canonical_bytes, &canonical_sha, &local_sha)
            .unwrap();
        assert_eq!(out, AlignOutcome::SkippedWouldStripFrontmatter);

        // File untouched.
        assert_eq!(std::fs::read(&target).unwrap(), local_bytes);
        // Shadow untouched (still absent — nothing recorded).
        assert!(shadow.get(&rel).is_none());
    }

    /// Control: the normal CRLF/BOM-only alignment case (identical frontmatter
    /// PRESENCE on both sides, only line-ending/BOM bytes differ) must still
    /// rewrite through `write_aligned_bytes` — the new guard only trips on a
    /// frontmatter-presence MISMATCH, never on a frontmatter-preserving byte
    /// realignment.
    #[test]
    fn write_aligned_bytes_still_rewrites_crlf_with_frontmatter_on_both_sides() {
        let (vaults, _ws, m, shadow) = mk_with_shadow(MaterializerMode::Live);
        let rel = format!("{VAULT}/01_Inbox/crlf.md");
        let target = vaults.path().join(&rel);
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();

        let canonical_bytes = b"---\ntitle: x\n---\nline one\nline two\n".to_vec();
        let canonical_sha = hex::encode(Sha256::digest(&canonical_bytes));
        // Local: same logical content, CRLF line endings + BOM — frontmatter
        // IS present on both sides (guard must not fire).
        let local_bytes = format!(
            "\u{feff}{}",
            String::from_utf8(canonical_bytes.clone())
                .unwrap()
                .replace('\n', "\r\n")
        )
        .into_bytes();
        std::fs::write(&target, &local_bytes).unwrap();
        let local_sha = hex::encode(Sha256::digest(&local_bytes));

        let out = m
            .write_aligned_bytes(&rel, &canonical_bytes, &canonical_sha, &local_sha)
            .unwrap();
        assert_eq!(
            out,
            AlignOutcome::Rewrote {
                path: target.clone()
            }
        );
        assert_eq!(std::fs::read(&target).unwrap(), canonical_bytes);
        assert_eq!(shadow.get(&rel).as_deref(), Some(canonical_sha.as_str()));
    }

    // --- R3 observed base_seq recorded only after byte-verify (TKT-166e1c07) ---

    /// R3: a Live-mode write whose bytes pass the integrity check records the
    /// server-provided change_seq as the note's observed base_seq (at the SAME
    /// post-verify point as the shadow hash); a payload with NO change_seq (a
    /// pre-R7b server, R5) records nothing, leaving the note unobserved
    /// (fail-closed). Fails on pre-R7b code: there is no base_seq store to
    /// record into, and NotePayload has no change_seq.
    #[tokio::test]
    async fn records_observed_base_seq_only_from_server_change_seq() {
        let sdir = TempDir::new().unwrap();
        let shadow = crate::sync_shadow::ShadowStore::load(sdir.path().join("shadow.json"));
        let bs = crate::base_seq_store::BaseSeqStore::load_with_vault_folders(
            sdir.path().join("base_seq.json"),
            vec!["Mainframe".to_string()],
        );

        let (_vaults, _ws, m) = mk(MaterializerMode::Live, default_cfg());
        let m = m
            .with_shadow_store(shadow.clone())
            .with_base_seq_store(bs.clone());

        // Server returned a change_seq -> observed after the write byte-verifies.
        let mut p = payload("01_Inbox/seq.md", "hello base_seq");
        p.change_seq = Some(7788);
        let out = m.write(&p).unwrap();
        assert!(
            matches!(out, MaterializeOutcome::Wrote { .. }),
            "got {out:?}"
        );
        // Keyed sync-root-relative (vault prefix stripped by canon), matching
        // the shadow store. The observed seq is exactly the server's token.
        assert_eq!(bs.get("01_Inbox/seq.md"), Some(7788));

        // Pre-R7b server omits change_seq -> nothing recorded (fail-closed, R5).
        let p2 = payload("01_Inbox/noseq.md", "no server seq");
        assert_eq!(p2.change_seq, None);
        let out2 = m.write(&p2).unwrap();
        assert!(
            matches!(out2, MaterializeOutcome::Wrote { .. }),
            "got {out2:?}"
        );
        assert_eq!(bs.get("01_Inbox/noseq.md"), None);
    }

    // --- TKT-f74edf99: close the client-half deadlock holes ---------------

    /// R4: CLOSE THE R1-NOOP NON-RECORDING HOLE. A note that converges by
    /// ALREADY being byte-identical to the server must still earn its baseline;
    /// pre-fix the identical-Noop arm recorded only the shadow and returned,
    /// leaving base_seq `None` forever (primed to deadlock on the next edit).
    /// RED on pre-fix: the second write returns IdenticalToLocal WITHOUT
    /// recording, so the `Some(4242)` assertion fails.
    #[tokio::test]
    async fn noop_identical_records_base_seq_r4() {
        let sdir = TempDir::new().unwrap();
        let shadow = crate::sync_shadow::ShadowStore::load(sdir.path().join("shadow.json"));
        let bs = crate::base_seq_store::BaseSeqStore::load_with_vault_folders(
            sdir.path().join("base_seq.json"),
            vec!["Mainframe".to_string()],
        );
        let (_vaults, _ws, m) = mk(MaterializerMode::Live, default_cfg());
        let m = m
            .with_shadow_store(shadow.clone())
            .with_base_seq_store(bs.clone());

        let mut p = payload("01_Inbox/noop.md", "identical body");
        p.change_seq = Some(4242);
        // Sync it once (materializes + records), then simulate the PRIMED state:
        // byte-identical to the server, but baseline absent.
        assert!(matches!(
            m.write(&p).unwrap(),
            MaterializeOutcome::Wrote { .. }
        ));
        bs.remove("01_Inbox/noop.md");
        assert_eq!(bs.get("01_Inbox/noop.md"), None);

        // Second write: local already identical => Noop / IdenticalToLocal.
        let out = m.write(&p).unwrap();
        assert!(
            matches!(
                out,
                MaterializeOutcome::Skipped(SkipReason::IdenticalToLocal)
            ),
            "got {out:?}"
        );
        assert_eq!(
            bs.get("01_Inbox/noop.md"),
            Some(4242),
            "R4: an already-identical note must still earn its baseline"
        );
    }

    /// R2 (binding): the PreserveLocalEdit branch must record NOTHING. A genuine
    /// local edit (shadow == server, local diverged) is preserved and pushed up;
    /// recording a baseline HERE would forge an in-sync marker the vault never
    /// received. Recovery may come ONLY from a verified receipt (push_client),
    /// never from this branch firing. This test pins the invariant.
    #[tokio::test]
    async fn preserve_local_edit_never_records_base_seq_r2() {
        let sdir = TempDir::new().unwrap();
        let shadow = crate::sync_shadow::ShadowStore::load(sdir.path().join("shadow.json"));
        let bs = crate::base_seq_store::BaseSeqStore::load_with_vault_folders(
            sdir.path().join("base_seq.json"),
            vec!["Mainframe".to_string()],
        );
        let (vaults, _ws, m) = mk(MaterializerMode::Live, default_cfg());
        let m = m
            .with_shadow_store(shadow.clone())
            .with_base_seq_store(bs.clone());

        let mut p = payload("01_Inbox/pres.md", "server body v1");
        p.change_seq = Some(500);
        // Sync v1 down: shadow == server head.
        assert!(matches!(
            m.write(&p).unwrap(),
            MaterializeOutcome::Wrote { .. }
        ));
        // User edits locally: file diverges, shadow still == server head.
        let target = vaults.path().join(VAULT).join("01_Inbox/pres.md");
        std::fs::write(&target, b"server body v1 + LOCAL EDIT").unwrap();
        // Simulate the primed state so any stray record would be visible.
        bs.remove("01_Inbox/pres.md");
        assert_eq!(bs.get("01_Inbox/pres.md"), None);

        // Re-materialize the SAME server head => decide() = PreserveLocalEdit.
        let out = m.write(&p).unwrap();
        assert!(
            matches!(
                out,
                MaterializeOutcome::Skipped(SkipReason::LocalEditPreserved)
            ),
            "got {out:?}"
        );
        assert_eq!(
            bs.get("01_Inbox/pres.md"),
            None,
            "R2: the preserve branch must NEVER record a baseline"
        );
    }

    /// R5: the REAL server change_seq threads through write/stash naming, so a
    /// conflict fork is named `-<seq>` deterministically — never the `-0-NN`
    /// hardwire that `write()` (change_seq 0) produced.
    #[tokio::test]
    async fn write_with_change_seq_names_stash_with_real_seq_r5() {
        let (vaults, _ws, m) = mk(MaterializerMode::Live, default_cfg());
        // Pre-existing divergent local file, NO shadow => decide() = Conflict.
        let target = vaults.path().join(VAULT).join("01_Inbox/fork.md");
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();
        std::fs::write(&target, b"local divergent bytes").unwrap();

        let p = payload("01_Inbox/fork.md", "server canonical bytes");
        let out = m.write_with_change_seq(&p, 4242).unwrap();
        let stash = match out {
            MaterializeOutcome::Stashed { stash_path } => stash_path,
            other => panic!("expected Stashed, got {other:?}"),
        };
        let name = stash.file_name().unwrap().to_string_lossy().into_owned();
        assert!(
            name.contains("4242"),
            "R5: stash must embed the real change_seq, got {name}"
        );
        assert!(
            !name.contains("-0.md") && !name.contains("-0-"),
            "R5: must not be the -0 hardwire, got {name}"
        );
    }

    /// R6: where ancestry is UNKNOWN (no shadow), the daemon PRESERVES BOTH
    /// sides — the local loser is stashed and the server head is materialized —
    /// never a size/mtime-based local-wins that could drop server content.
    #[tokio::test]
    async fn unknown_ancestry_preserves_both_sides_r6() {
        let (vaults, _ws, m) = mk(MaterializerMode::Live, default_cfg());
        let target = vaults.path().join(VAULT).join("01_Inbox/amb.md");
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();
        let local: &[u8] = b"LOCAL only line\n";
        std::fs::write(&target, local).unwrap();

        let p = payload("01_Inbox/amb.md", "SERVER only line");
        let out = m.write_with_change_seq(&p, 7).unwrap();
        let stash = match out {
            MaterializeOutcome::Stashed { stash_path } => stash_path,
            o => panic!("expected Stashed (preserve both), got {o:?}"),
        };
        assert_eq!(
            std::fs::read(&stash).unwrap(),
            local,
            "local side preserved in the stash"
        );
        let server_content = p.enriched_body.clone().unwrap();
        assert_eq!(
            std::fs::read_to_string(&target).unwrap(),
            server_content,
            "server side materialized to the live target"
        );
    }

    /// R6 (pure): content-level containment — every server line present in local
    /// IN ORDER — and the proof that SIZE is not the signal (a larger local can
    /// still drop a server line).
    #[test]
    fn server_lines_contained_in_local_r6() {
        // local is a superset: contains every server line, in order.
        assert!(server_lines_contained_in_local(b"a\nb\n", b"a\nx\nb\nc\n"));
        assert!(server_lines_contained_in_local(b"a\nb\n", b"a\nb\n"));
        // empty server is trivially contained.
        assert!(server_lines_contained_in_local(b"", b"anything\n"));
        // a dropped server line => NOT contained (would lose content).
        assert!(!server_lines_contained_in_local(b"a\nb\nc\n", b"a\nc\n"));
        // reordered => NOT contained (order matters).
        assert!(!server_lines_contained_in_local(b"a\nb\n", b"b\na\n"));
        // SIZE is not the signal: local is LARGER yet drops a server line.
        assert!(!server_lines_contained_in_local(
            b"keep\nDROP\n",
            b"keep\nx\ny\nz\n"
        ));
    }

    // -----------------------------------------------------------------------
    // TKT-372e31b2: false-conflict-copy generator (R1/R2/R3/R5)
    // -----------------------------------------------------------------------

    /// (vaults, ws, materializer, shadow, base_seq) with BOTH stores attached,
    /// keyed with the vault-folder strip so `payload()`'s prefixed paths and the
    /// stores agree (same discipline as production).
    fn mk_with_stores(
        mode: MaterializerMode,
    ) -> (
        TempDir,
        TempDir,
        Materializer,
        Arc<ShadowStore>,
        Arc<crate::base_seq_store::BaseSeqStore>,
    ) {
        let (v, w, m) = mk(mode, default_cfg());
        let sdir = Box::leak(Box::new(TempDir::new().unwrap()));
        let shadow = ShadowStore::load_with_vault_folders(
            sdir.path().join("shadow.json"),
            vec![VAULT.to_string()],
        );
        let bs = crate::base_seq_store::BaseSeqStore::load_with_vault_folders(
            sdir.path().join("base_seq.json"),
            vec![VAULT.to_string()],
        );
        let m = m
            .with_shadow_store(shadow.clone())
            .with_base_seq_store(bs.clone());
        (v, w, m, shadow, bs)
    }

    /// Count `*.conflict-from-*.md` siblings in a directory.
    fn conflict_copies(dir: &Path) -> Vec<String> {
        std::fs::read_dir(dir)
            .map(|rd| {
                rd.flatten()
                    .map(|e| e.file_name().to_string_lossy().into_owned())
                    .filter(|n| n.contains(".conflict-from-"))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// The canary shape sync-verify actually writes: YAML frontmatter + a nonce
    /// line that changes on EVERY session-init (R5's trigger profile).
    fn canary_bytes(host: &str, nonce: u64) -> String {
        format!("---\ntype: sync-canary\nhost: {host}\n---\nnonce: {host}-{nonce}\n")
    }

    /// R1 + R2 + R5 (TKT-372e31b2), the load-bearing regression.
    ///
    /// A single-writer file (`_sync/canary-<host>.md`, written by exactly one
    /// host by construction) is rewritten rapidly, and each rewrite races the
    /// daemon's own materialization of the SAME path - the materialization
    /// carries a server `change_seq` this daemon has ALREADY observed (the echo
    /// of its own earlier push). Requirements:
    ///
    /// * R1: ZERO conflict copies, ever, across every rewrite.
    /// * R2: resolved causally (incoming is not newer than what we observed), and
    ///   the local write still reaches the server - asserted structurally, by the
    ///   final local bytes still being at the canonical path, which is what the
    ///   LAZY push re-reads at drain time (push_client::process_event).
    ///
    /// FAILS ON THE OLD CODE: `decide()` returns `Conflict` for this state (R4:
    /// shadow present, server moved, local moved), and the old Conflict arm
    /// stashed the local bytes and overwrote the canonical path - one fork per
    /// rewrite, and the racing nonce pushed nowhere.
    #[test]
    fn r1_single_writer_rapid_rewrite_race_mints_zero_conflict_copies() {
        let (vaults, _ws, m, shadow, bs) = mk_with_stores(MaterializerMode::Live);
        let rel = "_sync/canary-trinity.md";
        let wire = format!("{VAULT}/{rel}");
        let target = vaults.path().join(VAULT).join(rel);
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();
        let dir = target.parent().unwrap().to_path_buf();

        // Lineage: this daemon pushed and byte-verified server version 1_000_500
        // for this path, so that seq is its ADOPTED proof-of-observation
        // (byte-verified on this FS — the only provenance that arms the
        // causal-preserve gate, Finding 1).
        const OBSERVED: i64 = 1_000_500;
        bs.record_adopted(&wire, OBSERVED);
        // The shadow holds a hash that is neither the local bytes nor the
        // incoming server bytes (a stale baseline - the ordinary state once the
        // file has been rewritten since the last recorded sync). That is exactly
        // the R4 input triple that used to resolve to Conflict.
        shadow.record(&wire, &sha256_hex("some older synced revision"));

        // Five rapid successive rewrites, each racing an in-flight
        // materialization for the same path.
        let mut last_local = String::new();
        for i in 1..=5u64 {
            last_local = canary_bytes("trinity", 1_785_615_000 + i);
            std::fs::write(&target, &last_local).unwrap();

            // The daemon materializes the server copy of the SAME path. Its
            // change_seq is <= OBSERVED: it is the echo of a version we already
            // materialized, NOT a newer peer revision.
            let mut server = payload(rel, "server side canary body");
            server.change_seq = Some(OBSERVED - (5 - i as i64));

            let out = m.write_with_change_seq(&server, 1_003_646_077).unwrap();
            assert_eq!(
                out,
                MaterializeOutcome::Skipped(SkipReason::LocalEditPreserved),
                "rewrite {i}: a not-newer server version must resolve to preserve-local, got {out:?}"
            );
            // R1: no fork at any point in the sequence.
            assert!(
                conflict_copies(&dir).is_empty(),
                "rewrite {i}: single-writer file must NEVER acquire a conflict copy, found {:?}",
                conflict_copies(&dir)
            );
            // R2: the racing write is still at the canonical path, so the lazy
            // push reads THESE bytes and they reach the server.
            assert_eq!(
                std::fs::read_to_string(&target).unwrap(),
                last_local,
                "rewrite {i}: the racing local write must stay at the canonical path"
            );
        }

        // Final state: the newest nonce is what a push would carry up.
        assert_eq!(std::fs::read_to_string(&target).unwrap(), last_local);
        assert!(last_local.contains("nonce: trinity-1785615005"));
        assert!(conflict_copies(&dir).is_empty());
        // Nothing was recorded: the causal arm reads lineage, it never forges it
        // (B1 hazard / R4 no-regression) — and the adopted provenance is intact.
        assert_eq!(bs.get(rel), Some(OBSERVED));
        assert_eq!(bs.get_adopted(rel), Some(OBSERVED));
    }

    /// R2 (TKT-372e31b2): the causal arm is NOT a blanket local-wins. When the
    /// incoming server version IS strictly newer than everything we observed, a
    /// genuinely divergent local file is still a real conflict: stash the loser,
    /// materialize the winner. This is the guard that keeps the fix from
    /// swallowing true multi-writer divergence.
    #[test]
    fn causal_arm_does_not_suppress_a_genuinely_newer_server_version() {
        let (vaults, _ws, m, shadow, bs) = mk_with_stores(MaterializerMode::Live);
        let rel = "01_Inbox/contested.md";
        let wire = format!("{VAULT}/{rel}");
        let target = vaults.path().join(VAULT).join(rel);
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();
        let dir = target.parent().unwrap().to_path_buf();

        bs.record_adopted(&wire, 500);
        shadow.record(&wire, &sha256_hex("stale baseline"));
        let local = "---\ntitle: Test\n---\n\nmy divergent local revision\n";
        std::fs::write(&target, local).unwrap();

        // Strictly newer than the observed 500 => a real peer revision.
        let mut server = payload(rel, "a genuinely newer peer revision");
        server.change_seq = Some(501);

        let out = m.write_with_change_seq(&server, 501).unwrap();
        match out {
            MaterializeOutcome::Stashed { .. } => {}
            other => panic!("a strictly-newer server version must still stash (R4), got {other:?}"),
        }
        assert_eq!(
            conflict_copies(&dir).len(),
            1,
            "true divergence keeps the always-stash floor"
        );
        assert!(std::fs::read_to_string(&target)
            .unwrap()
            .contains("a genuinely newer peer revision"));
    }

    /// Finding 1 (TKT-372e31b2, PR #11 review): the causal-preserve arm is
    /// gated on ADOPTED provenance ONLY. An OBSERVED entry — what the verified
    /// read-receipt (TKT-f74edf99) records on the 409 refetch path — proves we
    /// SAW the server head, not that the local file ever held its bytes. This
    /// reproduces the exact rebased-code hazard: the receipt records the head
    /// seq and the SAME head is materialized immediately after, so
    /// `incoming == observed`. A provenance-blind gate would preserve-local and
    /// swallow the true conflict; the adopted-gated arm must stand down to the
    /// always-stash floor (both byte-sets preserved).
    ///
    /// FAILS ON THE PROVENANCE-BLIND CODE: the causal arm returned
    /// `Skipped(LocalEditPreserved)`, minting no stash and skipping the pull.
    #[test]
    fn causal_arm_ignores_receipt_observed_lineage_and_keeps_stash_floor() {
        let (vaults, _ws, m, shadow, bs) = mk_with_stores(MaterializerMode::Live);
        let rel = "01_Inbox/receipt-divergent.md";
        let wire = format!("{VAULT}/{rel}");
        let target = vaults.path().join(VAULT).join(rel);
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();
        let dir = target.parent().unwrap().to_path_buf();

        // A genuinely divergent local edit (never pushed) against a stale
        // baseline: the R4 conflict triple.
        shadow.record(&wire, &sha256_hex("stale baseline"));
        let local = "---\ntitle: Test\n---\n\nmy divergent local revision\n";
        std::fs::write(&target, local).unwrap();

        // The push 409'd and the refetch recorded a VERIFIED READ-RECEIPT for
        // the server head (seq 900) — observation, not adoption.
        bs.record_observed(&wire, 900);
        assert_eq!(bs.get(rel), Some(900), "wire declaration armed (PR #9)");
        assert_eq!(bs.get_adopted(rel), None, "no adopted lineage");

        // The same head is now materialized: incoming == observed == 900.
        let mut server = payload(rel, "the server head named by the 409");
        server.change_seq = Some(900);
        let out = m.write_with_change_seq(&server, 900).unwrap();
        match out {
            MaterializeOutcome::Stashed { .. } => {}
            other => panic!(
                "an observed-only lineage must NOT enable preserve-local; expected the \
                 always-stash floor (Stashed), got {other:?}"
            ),
        }
        assert_eq!(
            conflict_copies(&dir).len(),
            1,
            "the true conflict keeps the always-stash floor"
        );
        assert!(
            std::fs::read_to_string(&target)
                .unwrap()
                .contains("the server head named by the 409"),
            "the server head materialized to the canonical path"
        );
    }

    /// R1 (TKT-372e31b2): when the shadow store loaded SCOPE-SUSPECT
    /// (`vault_folders` empty while the store holds vault-prefixed keys, the
    /// 2026-07-18 trinity incident), EVERY lookup mis-keys and misses, so R5
    /// fires vault-wide and every fork it mints is false. The materializer must
    /// refuse the write entirely: no stash, no overwrite, local preserved -
    /// matching the push leg, which already parks wholesale on this state.
    ///
    /// FAILS ON THE OLD CODE: the old R5 arm minted a conflict copy and
    /// overwrote the local file.
    #[test]
    fn r1_scope_suspect_shadow_mints_no_conflict_copy() {
        let (vaults, _ws, m) = mk(MaterializerMode::Live, default_cfg());
        let sdir = TempDir::new().unwrap();
        let spath = sdir.path().join("shadow.json");
        // A store holding vault-prefixed keys, loaded with EMPTY vault_folders.
        let mut seed = std::collections::HashMap::new();
        seed.insert(format!("{VAULT}/01_Notes/other.md"), "deadbeef".to_string());
        std::fs::write(&spath, serde_json::to_vec(&seed).unwrap()).unwrap();
        let shadow = ShadowStore::load_with_vault_folders(spath, vec![]);
        assert!(
            shadow.vault_scope_suspect(),
            "fixture must reproduce the suspect state"
        );
        let m = m.with_shadow_store(shadow);

        let rel = "_sync/canary-link.md";
        let target = vaults.path().join(VAULT).join(rel);
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();
        let dir = target.parent().unwrap().to_path_buf();
        let local = canary_bytes("link", 1_784_439_675);
        std::fs::write(&target, &local).unwrap();

        // No shadow entry for this path (every lookup misses in the suspect
        // state) => R5 Conflict on the old code.
        let server = payload(rel, "server canary body");
        let out = m.write_with_change_seq(&server, 1_003_627_563).unwrap();
        assert_eq!(
            out,
            MaterializeOutcome::Skipped(SkipReason::ShadowScopeSuspect),
            "a scope-suspect shadow must refuse the write, not mint a fork"
        );
        assert!(
            conflict_copies(&dir).is_empty(),
            "no fork may be minted on a known store misconfiguration, found {:?}",
            conflict_copies(&dir)
        );
        assert_eq!(
            std::fs::read_to_string(&target).unwrap(),
            local,
            "local bytes must be untouched (fail-closed toward local)"
        );
    }

    /// R3 (TKT-372e31b2): stash idempotency under REPEATED sweeps with
    /// byte-identical content. The same divergence re-materialized N times must
    /// converge on ONE `.conflict-from-*` sibling, never N of them.
    ///
    /// This one PASSES on the old code too (`conflict_stash::find_identical_stash`
    /// already dedups byte-identical content, and the field record confirms it:
    /// `-0-37` was reused across 5 ticks). It is kept as the boundedness guard
    /// that the new causal/scope arms must not weaken, and it pins the half of
    /// the v0.4.26 idempotency claim that DOES hold.
    #[test]
    fn r3_repeated_sweeps_with_identical_content_keep_exactly_one_stash() {
        let (vaults, _ws, m, shadow, bs) = mk_with_stores(MaterializerMode::Live);
        let rel = "01_Inbox/resweep.md";
        let wire = format!("{VAULT}/{rel}");
        let target = vaults.path().join(VAULT).join(rel);
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();
        let dir = target.parent().unwrap().to_path_buf();

        let local = "---\ntitle: Test\n---\n\nlocal divergent bytes\n";
        // A reconcile-pull / pre-R7b payload carries NO change_seq, so there is
        // no causal evidence either way and the new causal arm stands down by
        // construction (fail-closed to the pre-existing behavior). That isolates
        // the property under test: stash dedup, not causal resolution. `bs` is
        // attached but deliberately left empty for the same reason.
        let server = payload(rel, "server winner bytes");
        assert_eq!(server.change_seq, None);
        assert_eq!(bs.get(rel), None);

        for pass in 1..=4 {
            // Re-create the exact same divergence each sweep: same local bytes,
            // same server bytes, stale shadow.
            std::fs::write(&target, local).unwrap();
            shadow.record(&wire, &sha256_hex("stale baseline"));

            let out = m.write_with_change_seq(&server, 9_000).unwrap();
            match out {
                MaterializeOutcome::Stashed { .. } => {}
                other => panic!("sweep {pass}: expected Stashed, got {other:?}"),
            }
            assert_eq!(
                conflict_copies(&dir).len(),
                1,
                "sweep {pass}: byte-identical content must reuse the ONE stash, found {:?}",
                conflict_copies(&dir)
            );
        }
        // And the single stash holds the losing bytes verbatim.
        let forks = conflict_copies(&dir);
        assert_eq!(std::fs::read_to_string(dir.join(&forks[0])).unwrap(), local);
    }
}
