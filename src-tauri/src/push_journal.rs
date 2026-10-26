//! R3 + R13: persistent jsonl-line append-only journal of pending local->server pushes.
//!
//! Each line is a single JSON object representing one queued push event. The
//! journal lives outside the vault tree at
//! `<workspace>/.lattice-runtime/<slug>/sync-state/push_journal.jsonl`
//! (CS-2: the handle holds an `Arc<StateDir>`; every byte goes through the
//! held directory fd, never a path).
//!
//! Concurrency (Unit 1a, TKT-b25fce06): every operation that touches the
//! file takes an exclusive `File::lock` (flock / LockFileEx) on the sibling
//! `push_journal.lock`, held from the read through the rename. Each handle
//! opens its OWN lock fd, so separate handles in one process exclude each
//! other exactly as separate processes do. The lock file is mode 0600, holds
//! no data and is never deleted, renamed or compacted. Appends `sync_data`
//! before reporting success; rewrites use a unique `create_new` temp,
//! `sync_all`, rename, then a directory fsync (unix).
//!
//! **File is the single source of truth (v0.3 stale-state fix).** The daemon
//! constructs THREE separate `PushJournal` handles on the SAME file
//! (file_watcher appends, push_client drains+acks, verify_repair batch-appends).
//! An earlier design cached every event in an in-memory `Vec` at `open()` and
//! served `drain()` from that cache — so a handle never saw appends made by a
//! *different* handle, and the push pipeline silently did nothing. The fix:
//! `drain()` and `ack()` **re-read the on-disk file every call** (no cache).
//! Ack identity is by per-event `id` (a dep-free `<queued_at_nanos>-<counter>`
//! string set at construction), NOT by an in-memory index — so cursors are
//! meaningful across separate handles and crashes. Any number of handles now
//! converge on the same file state.
//!
//! Binary content: `content_bytes: Option<Vec<u8>>`. `Some(bytes)` embeds the
//! body inline (file_watcher already holds the bytes from the change event).
//! `None` means "content not embedded" — the reader (push_client) lazily loads
//! the file from disk at push time. This keeps the journal small for
//! verify_repair sweeps that would otherwise inline tens of thousands of file
//! bodies and blow past the capacity cap. The binary-streaming / sidecar path
//! (spec §1 row 11) is deferred future work.

use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::state_io::{NotCommitted, StateDir, StateFile};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

/// Local serde adapter for `DateTime<Utc>` since we don't pull in the
/// `chrono/serde` feature. Stores as RFC3339 string.
mod ts {
    use super::*;
    pub fn serialize<S: Serializer>(dt: &DateTime<Utc>, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&dt.to_rfc3339())
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<DateTime<Utc>, D::Error> {
        let s = String::deserialize(d)?;
        DateTime::parse_from_rfc3339(&s)
            .map(|dt| dt.with_timezone(&Utc))
            .map_err(serde::de::Error::custom)
    }
}

/// Serde adapter for `content_bytes`: encode embedded bodies as **base64**, not
/// the default `serde_json` representation of `Vec<u8>` (a JSON array of integer
/// literals, e.g. `[104,101,...]`). The int-array form bloats each body ~4-6x
/// (1-4 chars + comma per byte) against the journal's 100 MB capacity cap, so a
/// large one-time reconciliation backlog overflowed the cap far faster than
/// `push_client` could drain it — the file_watcher then hot-spun on
/// `CapacityExceeded` (the S489/S490 storm). Base64 is ~1.33x raw, cutting that
/// backlog ~3-4x and keeping it under cap.
///
/// Deserialize is backward-compatible: it accepts a base64 string (new), a
/// legacy JSON array of `u8` (old on-disk lines), or `null` (`None`). The
/// in-memory type stays `Option<Vec<u8>>`, so no caller changes.
mod content_b64 {
    use super::*;
    use base64::{engine::general_purpose::STANDARD, Engine};

    pub fn serialize<S: Serializer>(v: &Option<Vec<u8>>, s: S) -> Result<S::Ok, S::Error> {
        match v {
            Some(bytes) => s.serialize_some(&STANDARD.encode(bytes)),
            None => s.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Vec<u8>>, D::Error> {
        // Accept both the new base64 string and the legacy int-array so that
        // journal lines written by <=v0.4.5 still load after the upgrade.
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Repr {
            B64(String),
            Raw(Vec<u8>),
        }
        match Option::<Repr>::deserialize(d)? {
            None => Ok(None),
            Some(Repr::B64(s)) => STANDARD
                .decode(s.as_bytes())
                .map(Some)
                .map_err(serde::de::Error::custom),
            Some(Repr::Raw(v)) => Ok(Some(v)),
        }
    }
}

/// Bump when the on-disk event shape changes.
///
/// Stays at 1 across the v0.3 stale-state fix: the new `id` field carries a
/// `#[serde(default)]` so any pre-existing schema=1 journal lines (which lack
/// `id`) still load and are assigned a fresh deterministic id on read.
pub const CURRENT_SCHEMA: u32 = 1;

// (No compaction-ratio threshold any more: ack() rewrites the file removing
// acked lines immediately, so there is never an ack'd-prefix backlog to
// compact. The file shrinks on every ack.)

/// Monotonic per-process counter feeding the dep-free event id. Combined with
/// the queued-at nanos it yields a stable `<nanos>-<counter>` id unique within
/// a process and stable on disk once written.
static ID_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Generate a dep-free unique event id: `<unix_nanos>-<counter>`. No `uuid`
/// crate dependency. Collision-free within a process (counter) and effectively
/// across restarts (nanos clock); the id only needs to be unique among
/// currently-queued events, which this guarantees.
pub fn new_event_id() -> String {
    let nanos = Utc::now().timestamp_nanos_opt().unwrap_or(0);
    let n = ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{nanos}-{n}")
}

/// Serde default for `id`: an EMPTY string. We deliberately do NOT generate a
/// random id here — serde can't see the event's other fields, so a random id
/// would differ on every re-read of the same legacy (pre-`id`) on-disk line,
/// breaking ack-by-id (drain assigns id X, ack re-reads and assigns id Y → no
/// match → wedge). Instead `read_live_from` detects an empty id and backfills a
/// DETERMINISTIC id derived from the event's content, so every re-read of the
/// same legacy line yields the same id.
fn default_event_id() -> String {
    String::new()
}

/// Deterministic id for a legacy (pre-`id`) line, derived purely from stable
/// event fields so repeated reads agree. Format `legacy-<hex>`.
fn deterministic_legacy_id(evt: &PushEvent) -> String {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    evt.path.hash(&mut h);
    evt.content_sha.hash(&mut h);
    evt.queued_at.to_rfc3339().hash(&mut h);
    (evt.action as u8).hash(&mut h);
    format!("legacy-{:016x}", h.finish())
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum PushAction {
    Create,
    Modify,
    Delete,
}

/// R4 / F-B3.2 (TKT-989ad5f2): three-state CAS base for a push event.
///
/// The I29 shadow backfill in `push_client` must apply to real-time WATCHER
/// events ONLY — never to a reconcile-determined "the server has no row for
/// this path". The pre-fix two-state `Option<String>` conflated the two under
/// `None`, so a reconcile `missing-on-server` push had a stale shadow hash
/// backfilled onto it and 409'd forever against a row the server does not hold
/// (the perpetual CAS-409 push loop, 40/pass trinity + 17/pass link).
///
/// * `KnownBase(h)` — an explicit CAS base the server must match (reconcile
///   drift passes the delta's server hash). Honored verbatim; NEVER backfilled.
/// * `NoRow` — reconcile determined the server has no row. Force a CREATE
///   (wire base `""`); NEVER shadow-backfilled.
/// * `Unknown` — a real-time watcher event with no known base. `push_client`
///   sources the base from the shadow store (I29).
///
/// The wire form is UNCHANGED (`Option<String>`) via `#[serde(from/into)]`, so
/// pre-fix journal lines still load AND pre-fix daemons still read new lines:
///   `KnownBase(h)` <-> non-empty string; `NoRow` <-> `""`; `Unknown` <-> `null`.
/// (A pre-fix `null` line — which historically meant either watcher OR
/// missing-on-server — loads as `Unknown`. That is benign: a never-synced
/// path has no shadow entry, so the Unknown backfill yields nothing -> `""` ->
/// CREATE, the same resolution `NoRow` produces.)
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum PushBase {
    /// Explicit CAS base (reconcile drift). Honored verbatim.
    KnownBase(String),
    /// Reconcile determined no server row. Force CREATE; never backfill.
    NoRow,
    /// Watcher event, base unknown. push_client shadow-backfills (I29).
    #[default]
    Unknown,
}

impl From<Option<String>> for PushBase {
    fn from(o: Option<String>) -> Self {
        match o {
            None => PushBase::Unknown,
            Some(s) if s.is_empty() => PushBase::NoRow,
            Some(s) => PushBase::KnownBase(s),
        }
    }
}

impl From<PushBase> for Option<String> {
    fn from(b: PushBase) -> Self {
        match b {
            PushBase::Unknown => None,
            PushBase::NoRow => Some(String::new()),
            PushBase::KnownBase(s) => Some(s),
        }
    }
}

// `#[serde(from/into)]` needs Serialize/Deserialize derived on the type even
// though the wire delegates to Option<String>; supply them via the derive.
impl Serialize for PushBase {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        Option::<String>::from(self.clone()).serialize(s)
    }
}
impl<'de> Deserialize<'de> for PushBase {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Ok(PushBase::from(Option::<String>::deserialize(d)?))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PushEvent {
    pub schema_version: u32,
    /// Stable per-event identity used for content-identity ack across
    /// separate `PushJournal` handles. Set at construction via
    /// `new_event_id()`. `#[serde(default)]` so pre-`id` schema=1 lines on
    /// disk still load (they get a fresh id assigned on read).
    #[serde(default = "default_event_id")]
    pub id: String,
    /// Canonical forward-slash relative-to-vault-root path.
    pub path: String,
    pub action: PushAction,
    /// R4 / F-B3.2 (TKT-989ad5f2): three-state CAS base. `Unknown` (wire
    /// `null`) for a watcher event, `NoRow` (wire `""`) for a reconcile
    /// no-server-row, `KnownBase(h)` (wire non-empty string) for an explicit
    /// reconcile CAS base. Wire-compatible with the pre-fix `Option<String>`.
    #[serde(default)]
    pub base_hash: PushBase,
    pub content_sha: String,
    /// `Some(bytes)`: body embedded inline (caller already had the bytes).
    /// `None`: content not embedded — the reader loads it from disk at push
    /// time. Lets verify_repair enqueue lightweight refs without re-reading.
    /// On the wire this is base64 (see `content_b64`), NOT a JSON int-array —
    /// the int-array bloat was the journal-overflow storm root cause.
    #[serde(with = "content_b64", default)]
    pub content_bytes: Option<Vec<u8>>,
    #[serde(with = "ts")]
    pub queued_at: DateTime<Utc>,
    pub device_id: String,
}

/// Opaque cursor returned by `drain`. Wraps the event's stable `id` so it is
/// meaningful ACROSS separate `PushJournal` handles and across process
/// restarts — unlike the old in-memory-index cursor. Hand it back to `ack` /
/// `nack`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalCursor(String);

impl JournalCursor {
    /// The underlying event id this cursor acks.
    pub fn id(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Error)]
pub enum JournalError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("serde: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("journal corruption: {0}")]
    Corruption(String),
    #[error("capacity exceeded: current={current} max={max}")]
    CapacityExceeded { current: u64, max: u64 },
}

/// Unit 1a (TKT-b25fce06): a lock wait longer than this logs at ERROR with the
/// operation name. Lock holders do bounded synchronous file I/O only, so a
/// multi-second wait means a wedged holder (or a caller blocking an async
/// runtime thread on a contended journal).
const LOCK_WAIT_ALARM: Duration = Duration::from_secs(5);

/// Per-thread identity for the C1 re-entrancy guard (0 = no holder).
static NEXT_THREAD_TAG: AtomicU64 = AtomicU64::new(1);
thread_local! {
    static THREAD_TAG: u64 = NEXT_THREAD_TAG.fetch_add(1, Ordering::Relaxed);
}

fn nc(n: NotCommitted) -> JournalError {
    JournalError::Io(n.0)
}

/// Proof that the journal lock is held. Only [`PushJournal::with_lock`]
/// constructs it, so every `*_locked` body (which takes `&Locked`) is
/// structurally reachable only from inside the lock (C1).
struct Locked(());

/// Test-only fault injection and observation points (Unit 1a §3).
#[cfg(test)]
#[derive(Default)]
struct JournalFaults {
    fail_rewrite_sync: std::sync::atomic::AtomicBool,
    fail_append_sync: std::sync::atomic::AtomicBool,
    crash_at: std::sync::Mutex<Option<CrashPoint>>,
    lock_acquisitions: AtomicU64,
}

/// T-J6 crash-injection points inside `rewrite_locked`.
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CrashPoint {
    TempWritten,
    TempSynced,
    /// After the rename, before the directory fsync.
    Renamed,
    DirSynced,
}

#[cfg(test)]
const SIMULATED_CRASH: &str = "SIMULATED_CRASH";

#[cfg(test)]
impl JournalFaults {
    /// Stop dead at `p` when armed, WITHOUT any cleanup, the way a crash would.
    fn crash_point(&self, p: CrashPoint) -> Result<(), JournalError> {
        if *self.crash_at.lock().unwrap() == Some(p) {
            return Err(JournalError::Corruption(format!(
                "{SIMULATED_CRASH} at {p:?}"
            )));
        }
        Ok(())
    }

    /// CS-2: the rewrite now runs inside CS-1 `durable_file` (through the
    /// StateDir), so the 1a temp-side fault points are armed on its
    /// thread-local seams for the duration of one rewrite.
    fn arm_durable_seams(&self) {
        use crate::durable_file::seams;
        if self.fail_rewrite_sync.load(Ordering::SeqCst) {
            seams::fail_sync_all(5);
        }
        match *self.crash_at.lock().unwrap() {
            Some(CrashPoint::TempWritten) => seams::crash_at(seams::Point::AfterWrite),
            Some(CrashPoint::TempSynced) => seams::crash_at(seams::Point::AfterSync),
            Some(CrashPoint::Renamed) => seams::crash_at(seams::Point::AfterRename),
            _ => {}
        }
    }
}

/// Where a journal's bytes live.
enum Backing {
    /// The real journal: `push_journal.jsonl` in a held state dir.
    Dir(Arc<StateDir>),
    /// D9 seed scratch sink (CS-2 D5, gate C10): touches no filesystem and
    /// counts appends. Built only by [`PushJournal::scratch`].
    Scratch(AtomicU64),
}

/// File-authoritative journal handle. Holds NO cached event list — `drain`
/// and `ack` re-read the on-disk file every call so any number of handles on
/// the same file converge on the same state (v0.3 stale-state fix).
///
/// Unit 1a (TKT-b25fce06): every operation that touches the file runs under
/// an exclusive `flock`/`LockFileEx` on the sibling `push_journal.lock`, held
/// from the read through the rename. Each handle opens its OWN lock fd (C3),
/// so the four production handles in one process exclude each other exactly
/// as two processes do.
///
/// CS-2 (TKT-23091e72): the handle holds `(Arc<StateDir>, StateFile)`, never
/// a path. Every byte goes through the held directory fd.
pub struct PushJournal {
    backing: Backing,
    /// Current file size in bytes: a projection, re-measured from disk inside
    /// the lock before every capacity decision (J5) and after every rewrite.
    total_bytes: AtomicU64,
    /// Capacity guard in bytes (default 100 MB per spec §4.2).
    max_bytes: u64,
    /// C3: this handle's own lock fd. Never `try_clone`d. `None` only for
    /// the scratch sink.
    lock_file: Option<File>,
    /// Serializes threads that share ONE handle by reference (a shared open
    /// file description would not exclude them via flock).
    local: std::sync::Mutex<()>,
    /// C1 re-entrancy guard: tag of the thread currently inside `with_lock`
    /// on this handle, or 0.
    holder: AtomicU64,
    #[cfg(test)]
    faults: JournalFaults,
}

const DEFAULT_MAX_BYTES: u64 = 100 * 1024 * 1024;

impl PushJournal {
    /// Open (or create) the journal in the held state dir. Scans existing
    /// lines and recovers from corrupt-tail / unknown-schema lines by
    /// skipping them with a tracing warning.
    pub fn open(sd: &Arc<StateDir>) -> Result<Self, JournalError> {
        Self::open_with_capacity(sd, DEFAULT_MAX_BYTES)
    }

    pub fn open_with_capacity(sd: &Arc<StateDir>, max_bytes: u64) -> Result<Self, JournalError> {
        // W1 (CS-2): no create_dir_all. Create the FILE if absent through the
        // held dir (`O_CREAT|O_EXCL|O_NOFOLLOW`, 0600); a gone dir is ENOENT.
        sd.ensure_file(StateFile::PushJournal).map_err(nc)?;
        // W2: a fresh lock description per handle (1a C3 preserved).
        let lock_file = sd.open_lock(StateFile::PushJournalLock)?;
        let j = Self {
            backing: Backing::Dir(Arc::clone(sd)),
            total_bytes: AtomicU64::new(0),
            max_bytes,
            lock_file: Some(lock_file),
            local: std::sync::Mutex::new(()),
            holder: AtomicU64::new(0),
            #[cfg(test)]
            faults: JournalFaults::default(),
        };

        // Validate the file is readable + scannable now (surfaces IO errors at
        // open time) and seed the byte projection. We do NOT cache events.
        j.with_lock("open", |j, l| {
            let _ = j.read_live_from_locked(l)?;
            j.refresh_total_bytes_locked(l)?;
            Ok(())
        })?;
        Ok(j)
    }

    /// The D9 seed's scratch journal (CS-2 D5, gate C10): an in-memory sink
    /// that touches no filesystem. The seed never enqueues; an append is a
    /// bug, so it is counted, WARNed in every build and debug-asserted.
    /// Exactly one caller (the D9 seed), asserted by `tests/state_io_gate.rs`.
    pub(crate) fn scratch() -> Self {
        Self {
            backing: Backing::Scratch(AtomicU64::new(0)),
            total_bytes: AtomicU64::new(0),
            max_bytes: DEFAULT_MAX_BYTES,
            lock_file: None,
            local: std::sync::Mutex::new(()),
            holder: AtomicU64::new(0),
            #[cfg(test)]
            faults: JournalFaults::default(),
        }
    }

    /// Appends the scratch sink has absorbed (always 0 for a real journal).
    #[cfg(test)]
    pub(crate) fn scratch_appends(&self) -> u64 {
        match &self.backing {
            Backing::Scratch(n) => n.load(Ordering::SeqCst),
            Backing::Dir(_) => 0,
        }
    }

    fn scratch_append(&self, n: usize) -> bool {
        let Backing::Scratch(c) = &self.backing else {
            return false;
        };
        c.fetch_add(n as u64, Ordering::SeqCst);
        tracing::warn!(
            events = n,
            "push_journal: append to the D9 seed SCRATCH journal; the seed must never enqueue (CS-2 gate C10). Dropped."
        );
        #[cfg(not(test))]
        debug_assert!(false, "append to the scratch journal (CS-2 C10)");
        true
    }

    fn sd(&self) -> Option<&Arc<StateDir>> {
        match &self.backing {
            Backing::Dir(sd) => Some(sd),
            Backing::Scratch(_) => None,
        }
    }

    /// Run `f` holding this handle's exclusive journal lock. ONLY the
    /// outermost public methods call this (C1); bodies are `*_locked` fns.
    /// Re-entry on the same handle from the same thread panics in debug
    /// builds and returns an error in release builds (Windows `LockFileEx`
    /// would deadlock on a nested acquire; unix flock would silently hide it).
    fn with_lock<R>(
        &self,
        op: &'static str,
        f: impl FnOnce(&Self, &Locked) -> Result<R, JournalError>,
    ) -> Result<R, JournalError> {
        let me = THREAD_TAG.with(|t| *t);
        if self.holder.load(Ordering::SeqCst) == me {
            let msg = format!("push_journal: re-entrant lock acquisition in `{op}` (Unit 1a C1)");
            if cfg!(debug_assertions) {
                panic!("{msg}");
            }
            return Err(JournalError::Io(io::Error::other(msg)));
        }

        struct Held<'a> {
            j: &'a PushJournal,
            flocked: bool,
            _local: std::sync::MutexGuard<'a, ()>,
        }
        impl Drop for Held<'_> {
            fn drop(&mut self) {
                self.j.holder.store(0, Ordering::SeqCst);
                if self.flocked {
                    if let Some(l) = &self.j.lock_file {
                        let _ = l.unlock();
                    }
                }
            }
        }

        let started = Instant::now();
        let local = self.local.lock().unwrap_or_else(|p| p.into_inner());
        self.holder.store(me, Ordering::SeqCst);
        let mut held = Held {
            j: self,
            flocked: false,
            _local: local,
        };
        if let Some(l) = &self.lock_file {
            l.lock()?;
            held.flocked = true;
        }
        let waited = started.elapsed();
        if waited > LOCK_WAIT_ALARM {
            tracing::error!(
                op,
                waited_ms = waited.as_millis() as u64,
                "push_journal: lock wait exceeded 5 s"
            );
        }
        #[cfg(test)]
        self.faults.lock_acquisitions.fetch_add(1, Ordering::SeqCst);

        f(self, &Locked(()))
    }

    /// Re-read the on-disk size into the projection (J5). Lock held.
    fn refresh_total_bytes_locked(&self, _l: &Locked) -> Result<u64, JournalError> {
        let n = match self.sd() {
            Some(sd) => sd.len(StateFile::PushJournal)?.ok_or_else(|| {
                JournalError::Io(io::Error::new(
                    io::ErrorKind::NotFound,
                    "push journal is absent from the held state dir",
                ))
            })?,
            None => 0,
        };
        self.total_bytes.store(n, Ordering::SeqCst);
        Ok(n)
    }

    /// Re-read the on-disk journal and return all live (parseable, current
    /// schema) events in append order. This is the authoritative read — every
    /// `drain` / `ack` calls it so separate handles converge. Applies the
    /// same corrupt-tail / forward-compat tolerance as the original scan.
    /// Lock held. CS-2: read through the held dir fd (`O_NOFOLLOW`).
    fn read_live_from_locked(&self, _l: &Locked) -> Result<Vec<PushEvent>, JournalError> {
        let Some(sd) = self.sd() else {
            return Ok(Vec::new());
        };
        let file = sd.open_read(StateFile::PushJournal)?.ok_or_else(|| {
            JournalError::Io(io::Error::new(
                io::ErrorKind::NotFound,
                "push journal is absent from the held state dir",
            ))
        })?;
        let reader = BufReader::new(file);

        let mut entries: Vec<PushEvent> = Vec::new();

        for (lineno, raw) in reader.lines().enumerate() {
            let line = match raw {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!(
                        "push_journal: unreadable line {} ({}); treating as corrupt tail and stopping scan",
                        lineno + 1,
                        e
                    );
                    break;
                }
            };

            if line.trim().is_empty() {
                continue;
            }

            // Peek at schema_version first for forward-compat.
            #[derive(Deserialize)]
            struct PeekSchema {
                schema_version: u32,
            }
            let peek: Result<PeekSchema, _> = serde_json::from_str(&line);
            match peek {
                Ok(p) if p.schema_version > CURRENT_SCHEMA => {
                    tracing::warn!(
                        "push_journal: line {} schema_version={} > CURRENT={}; skipping (forward-compat)",
                        lineno + 1,
                        p.schema_version,
                        CURRENT_SCHEMA
                    );
                    continue;
                }
                Ok(p) if p.schema_version < CURRENT_SCHEMA => {
                    tracing::warn!(
                        "push_journal: line {} schema_version={} < CURRENT={}; no migrator available, skipping",
                        lineno + 1,
                        p.schema_version,
                        CURRENT_SCHEMA
                    );
                    continue;
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!(
                        "push_journal: line {} unparseable schema header ({}); treating as corrupt tail",
                        lineno + 1,
                        e
                    );
                    // Corrupt-tail policy: stop scanning further (the tail
                    // is presumed truncated). Anything before this line is
                    // already in `entries` and remains valid.
                    break;
                }
            }

            match serde_json::from_str::<PushEvent>(&line) {
                Ok(mut evt) => {
                    // Backfill a deterministic id for legacy id-less lines so
                    // ack-by-id is stable across re-reads / separate handles.
                    if evt.id.is_empty() {
                        evt.id = deterministic_legacy_id(&evt);
                    }
                    entries.push(evt);
                }
                Err(e) => {
                    tracing::warn!(
                        "push_journal: line {} failed full deserialize ({}); skipping",
                        lineno + 1,
                        e
                    );
                    continue;
                }
            }
        }

        Ok(entries)
    }

    /// Atomically append one event: write line+\n, `sync_data`.
    ///
    /// Preserves the original hard-error-on-capacity contract: a single append
    /// that would exceed the cap returns `CapacityExceeded` and writes nothing.
    /// The capacity check and the write happen under ONE lock acquisition.
    pub fn append(&mut self, evt: PushEvent) -> Result<(), JournalError> {
        if self.scratch_append(1) {
            return Ok(());
        }
        let mut line = serde_json::to_string(&evt)?;
        line.push('\n');
        self.with_lock("append", move |j, l| {
            // Re-measure from disk (J5) so the check reflects the real shared
            // file size, not a stale per-handle projection.
            let current = match j.refresh_total_bytes_locked(l) {
                Ok(n) => n,
                Err(JournalError::Io(e)) if e.kind() == io::ErrorKind::NotFound => 0,
                Err(e) => return Err(e),
            };
            if current.saturating_add(line.len() as u64) > j.max_bytes {
                return Err(JournalError::CapacityExceeded {
                    current,
                    max: j.max_bytes,
                });
            }
            let written = j.append_batch_locked(l, vec![evt])?;
            debug_assert_eq!(written, 1);
            Ok(())
        })
    }

    /// Append N events as ONE `write_all` then ONE `sync_data`.
    ///
    /// Serializes each event to one jsonl line, writes the whole batch, then
    /// `sync_data` once (J4). Returns the number of events appended; returns
    /// `Ok(n)` only after the sync succeeded. A sync error is returned as
    /// `Err` (callers treat `Err` as "not written").
    ///
    /// Capacity policy (graceful degradation): events are admitted one at a
    /// time against the running size, re-measured from disk inside the lock
    /// first (J5). The moment the NEXT event would push the file past
    /// `max_bytes`, admission stops — the admitted events are written, the
    /// rest are dropped, and a warning is logged. A huge first-sync therefore
    /// degrades to "write what fits" instead of erroring the whole batch.
    pub fn append_batch(&mut self, events: Vec<PushEvent>) -> Result<usize, JournalError> {
        if events.is_empty() {
            return Ok(0);
        }
        if self.scratch_append(events.len()) {
            return Ok(events.len());
        }
        self.with_lock("append_batch", move |j, l| j.append_batch_locked(l, events))
    }

    fn append_batch_locked(
        &self,
        _l: &Locked,
        events: Vec<PushEvent>,
    ) -> Result<usize, JournalError> {
        if events.is_empty() {
            return Ok(0);
        }
        let Some(sd) = self.sd() else {
            return Ok(0);
        };

        // W3 (CS-2): an fd-bound appender on the held dir, never a path.
        let mut f = sd.open_append(StateFile::PushJournal).map_err(nc)?;

        // J5 (and the S490 runaway fix): the capacity decision uses the size
        // re-read from disk INSIDE the lock, never the per-handle projection,
        // which is stale whenever another handle appended or rewrote.
        let mut total = f.len()?;
        self.total_bytes.store(total, Ordering::SeqCst);

        let count = events.len();
        let mut written = 0usize;
        let mut buf: Vec<u8> = Vec::new();
        for evt in events {
            let mut line = serde_json::to_string(&evt)?;
            line.push('\n');
            let added = line.len() as u64;

            if total.saturating_add(added) > self.max_bytes {
                tracing::warn!(
                    "push_journal: append_batch hit capacity (current={} max={}); wrote {}/{} events, dropping remainder",
                    total,
                    self.max_bytes,
                    written,
                    count
                );
                break;
            }

            buf.extend_from_slice(line.as_bytes());
            total = total.saturating_add(added);
            written += 1;
        }

        if written > 0 {
            f.write_all(&buf).map_err(nc)?;
            self.total_bytes.store(total, Ordering::SeqCst);
            // J4: durable before we report it written. A created file whose
            // dir sync failed is CommittedDurabilityUncertain: the bytes are
            // visible and the journal is at-least-once (1a J4), so it counts.
            #[cfg(test)]
            if self.faults.fail_append_sync.load(Ordering::SeqCst) {
                return Err(io::Error::other("injected sync_data failure").into());
            }
            let _commit = f.sync_data().map_err(nc)?;
        }
        Ok(written)
    }

    /// Read up to `n` live events from the head of the file, without removing
    /// them. **Re-reads the on-disk file every call** (no cache) so a handle
    /// always sees appends made by OTHER handles — the v0.3 stale-state fix.
    /// Returns events paired with cursors (wrapping each event's stable `id`)
    /// that the caller hands back to `ack` / `nack`.
    pub fn drain(&mut self, n: usize) -> Result<Vec<(PushEvent, JournalCursor)>, JournalError> {
        self.with_lock("drain", |j, l| {
            let live = j.read_live_from_locked(l)?;
            // Keep the byte projection honest for the capacity guard.
            j.refresh_total_bytes_locked(l)?;
            Ok(live
                .into_iter()
                .take(n)
                .map(|evt| {
                    let cur = JournalCursor(evt.id.clone());
                    (evt, cur)
                })
                .collect())
        })
    }

    /// Ack a single event by its cursor (event id) — removes the matching line
    /// from the file. Shares the `ack_batch` body; takes the lock ONCE (C1).
    pub fn ack(&mut self, cursor: JournalCursor) -> Result<(), JournalError> {
        let ack_ids: std::collections::HashSet<String> = std::iter::once(cursor.0).collect();
        self.with_lock("ack", |j, l| j.ack_batch_locked(l, &ack_ids))
    }

    /// Ack a batch of events by cursor (event id). Re-reads the file, drops
    /// every entry whose `id` is in the ack set, and atomically rewrites the
    /// file (tmp + fsync + rename + dir fsync), all under one lock hold so an
    /// append from any handle cannot land in the replaced inode (J1).
    /// Idempotent: acking an id not present is a no-op. Because identity is
    /// the on-disk `id` (not an in-memory index), this is correct across
    /// separate handles and crashes.
    pub fn ack_batch<I>(&mut self, cursors: I) -> Result<(), JournalError>
    where
        I: IntoIterator<Item = JournalCursor>,
    {
        let ack_ids: std::collections::HashSet<String> =
            cursors.into_iter().map(|JournalCursor(id)| id).collect();
        if ack_ids.is_empty() {
            return Ok(());
        }
        self.with_lock("ack_batch", |j, l| j.ack_batch_locked(l, &ack_ids))
    }

    fn ack_batch_locked(
        &self,
        l: &Locked,
        ack_ids: &std::collections::HashSet<String>,
    ) -> Result<(), JournalError> {
        if ack_ids.is_empty() {
            return Ok(());
        }
        let live = self.read_live_from_locked(l)?;
        let remaining: Vec<PushEvent> = live
            .into_iter()
            .filter(|evt| !ack_ids.contains(&evt.id))
            .collect();
        self.rewrite_locked(l, &remaining)
    }

    /// Keep the event for a later retry. No-op on the on-disk file — the event
    /// remains and will be re-drained on the next tick. Idempotent (push has
    /// its own content-hash idempotency on the server side).
    pub fn nack(&mut self, _cursor: JournalCursor) -> Result<(), JournalError> {
        Ok(())
    }

    /// Number of live events currently on disk. Re-reads the file.
    pub fn len(&self) -> usize {
        self.with_lock("len", |j, l| Ok(j.read_live_from_locked(l)?.len()))
            .unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.with_lock("is_empty", |j, l| {
            Ok(j.read_live_from_locked(l)?.is_empty())
        })
        .unwrap_or(true)
    }

    /// Total on-disk size in bytes (projection; refreshed on every read op).
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes.load(Ordering::SeqCst)
    }

    /// Atomically and durably rewrite the file to contain exactly `events`,
    /// then refresh the byte projection. Lock held (C1).
    ///
    /// W4 (CS-2): CS-1 `replace_atomic` over the StateDir anchor, still
    /// inside the 1a journal lock: a unique `create_new` 0600 temp
    /// `push_journal.jsonl.tmp-<pid>-<nanos>-<seq>` (J2), `sync_all` with its
    /// error propagated (J3), `renameat` as the commit point, then the
    /// directory sync. `Commit` of either kind counts as committed (the
    /// journal is at-least-once); `NotCommitted` propagates as `JournalError`.
    fn rewrite_locked(&self, _l: &Locked, events: &[PushEvent]) -> Result<(), JournalError> {
        let Some(sd) = self.sd() else {
            return Ok(());
        };
        let mut bytes: Vec<u8> = Vec::new();
        for evt in events {
            bytes.extend_from_slice(serde_json::to_string(evt)?.as_bytes());
            bytes.push(b'\n');
        }
        #[cfg(test)]
        self.faults.arm_durable_seams();
        let r = sd.replace_atomic(StateFile::PushJournal, &bytes);
        #[cfg(test)]
        crate::durable_file::seams::clear_crash_at();
        let _commit = r.map_err(nc)?;
        #[cfg(test)]
        self.faults.crash_point(CrashPoint::DirSynced)?;
        self.refresh_total_bytes_locked(_l)?;
        Ok(())
    }

    /// Collapse the journal to at most one entry per `path`, keeping the LATEST
    /// (last-on-disk) entry for each and preserving their relative order.
    /// Returns `(before, after)` live-entry counts.
    ///
    /// Journal-bloat remediation: the file_watcher historically appended a fresh
    /// entry for every FS event with no content dedup, so an actively-touched
    /// vault accumulated tens of thousands of redundant entries for the same
    /// handful of paths (observed 97% duplicates / a path re-queued 85×). They
    /// drain one-by-one as server no-ops, burying genuine edits for hours.
    /// Collapsing to one-per-path is SAFE: the duplicates are redundant pushes
    /// of the SAME path; the kept (latest) entry supersedes the earlier ones,
    /// content is read at drain time (entries are lazy), and push is
    /// content-hash-idempotent server-side. A no-op when nothing is redundant.
    /// The read, filter and rewrite happen under one lock hold (J1).
    pub fn compact(&mut self) -> Result<(usize, usize), JournalError> {
        self.with_lock("compact", |j, l| j.compact_locked(l))
    }

    fn compact_locked(&self, l: &Locked) -> Result<(usize, usize), JournalError> {
        let live = self.read_live_from_locked(l)?;
        let before = live.len();
        // Index of the LAST occurrence of each path.
        let mut last_idx: std::collections::HashMap<&str, usize> =
            std::collections::HashMap::with_capacity(live.len());
        for (i, evt) in live.iter().enumerate() {
            last_idx.insert(evt.path.as_str(), i);
        }
        let keep: std::collections::HashSet<usize> = last_idx.into_values().collect();
        let after = keep.len();
        if after < before {
            let compacted: Vec<PushEvent> = live
                .into_iter()
                .enumerate()
                .filter_map(|(i, e)| keep.contains(&i).then_some(e))
                .collect();
            self.rewrite_locked(l, &compacted)?;
        }
        Ok((before, after))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::disallowed_methods)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;
    use std::io::Write;
    use std::path::{Path, PathBuf};
    use tempfile::TempDir;

    /// CS-2: a test StateDir holding the directory that contains `p` (the
    /// journal is always `push_journal.jsonl` in it).
    fn sd_of(p: &Path) -> Arc<StateDir> {
        StateDir::for_test(p.parent().unwrap())
    }

    fn evt(path: &str, action: PushAction) -> PushEvent {
        PushEvent {
            schema_version: CURRENT_SCHEMA,
            id: new_event_id(),
            path: path.to_string(),
            action,
            base_hash: match action {
                PushAction::Create => PushBase::Unknown,
                _ => PushBase::KnownBase("0".repeat(64)),
            },
            content_sha: "a".repeat(64),
            content_bytes: Some(b"hello world".to_vec()),
            queued_at: Utc::now(),
            device_id: "dev-test".to_string(),
        }
    }

    /// Journal-bloat remediation: compaction collapses redundant duplicate
    /// entries for the same path to ONE (the latest), preserving distinct paths.
    #[test]
    fn compact_collapses_duplicates_keeping_latest_per_path() {
        let dir = TempDir::new().unwrap();
        let mut j = PushJournal::open(&StateDir::for_test(dir.path())).unwrap();
        // 3 redundant entries for a.md (remember the LAST id), 2 for b.md, 1 c.md.
        let mut last_a = String::new();
        for _ in 0..3 {
            let e = evt("a.md", PushAction::Modify);
            last_a = e.id.clone();
            j.append(e).unwrap();
        }
        for _ in 0..2 {
            j.append(evt("b.md", PushAction::Modify)).unwrap();
        }
        j.append(evt("c.md", PushAction::Create)).unwrap();
        assert_eq!(j.len(), 6);

        let (before, after) = j.compact().unwrap();
        assert_eq!((before, after), (6, 3), "one entry per distinct path");
        assert_eq!(j.len(), 3);

        let kept: Vec<PushEvent> = j.drain(100).unwrap().into_iter().map(|(e, _)| e).collect();
        let paths: std::collections::HashSet<&str> = kept.iter().map(|e| e.path.as_str()).collect();
        assert_eq!(
            paths,
            ["a.md", "b.md", "c.md"].into_iter().collect(),
            "exactly the distinct paths survive"
        );
        let a = kept.iter().find(|e| e.path == "a.md").unwrap();
        assert_eq!(a.id, last_a, "must keep the LATEST entry for a path");
    }

    #[test]
    fn compact_is_noop_without_duplicates() {
        let dir = TempDir::new().unwrap();
        let mut j = PushJournal::open(&StateDir::for_test(dir.path())).unwrap();
        j.append(evt("a.md", PushAction::Create)).unwrap();
        j.append(evt("b.md", PushAction::Create)).unwrap();
        assert_eq!(j.compact().unwrap(), (2, 2));
        assert_eq!(j.len(), 2);
    }

    fn journal_path(dir: &TempDir) -> PathBuf {
        dir.path().join("push_journal.jsonl")
    }

    #[test]
    fn content_bytes_serialize_as_base64_not_int_array() {
        // The storm fix: bodies must serialize as a base64 string, never the
        // default serde_json int-array (`[104,101,...]`) that bloated the cap.
        let e = evt("notes/a.md", PushAction::Create); // body = b"hello world"
        let line = serde_json::to_string(&e).unwrap();
        assert!(
            line.contains("\"content_bytes\":\"aGVsbG8gd29ybGQ=\""),
            "expected base64 content_bytes, got: {line}"
        );
        assert!(
            !line.contains("[104,101"),
            "content_bytes must NOT be a JSON int-array: {line}"
        );
        // Round-trips back to the same bytes.
        let back: PushEvent = serde_json::from_str(&line).unwrap();
        assert_eq!(back.content_bytes, e.content_bytes);
    }

    #[test]
    fn legacy_int_array_content_bytes_still_loads() {
        // <=v0.4.5 wrote content_bytes as a JSON int-array. Those on-disk lines
        // MUST still deserialize after the base64 upgrade (back-compat).
        let legacy = r#"{"schema_version":1,"id":"legacy-1","path":"notes/a.md","action":"create","base_hash":null,"content_sha":"aaaa","content_bytes":[104,105,33],"queued_at":"2026-06-01T00:00:00+00:00","device_id":"dev"}"#;
        let e: PushEvent = serde_json::from_str(legacy).unwrap();
        assert_eq!(e.content_bytes, Some(vec![104u8, 105, 33]));
    }

    #[test]
    fn base64_is_much_smaller_than_int_array() {
        // The actual fix metric: for a realistic body the base64 line is well
        // under 2x the body, whereas the int-array form was ~4-6x.
        let body = vec![b'x'; 10_000];
        let mut e = evt("notes/big.md", PushAction::Create);
        e.content_bytes = Some(body.clone());
        let line = serde_json::to_string(&e).unwrap();
        assert!(
            line.len() < body.len() * 2,
            "base64 line {} should be < 2x body {} (int-array would be ~4-6x)",
            line.len(),
            body.len()
        );
    }

    #[test]
    fn open_empty_file_returns_empty_journal() {
        let dir = TempDir::new().unwrap();
        let p = journal_path(&dir);
        let j = PushJournal::open(&sd_of(&p)).unwrap();
        assert_eq!(j.len(), 0);
        assert!(j.is_empty());
        assert_eq!(j.total_bytes(), 0);
        assert!(p.exists());
    }

    #[test]
    fn append_then_reopen_persists() {
        let dir = TempDir::new().unwrap();
        let p = journal_path(&dir);
        {
            let mut j = PushJournal::open(&sd_of(&p)).unwrap();
            j.append(evt("notes/a.md", PushAction::Create)).unwrap();
            j.append(evt("notes/b.md", PushAction::Modify)).unwrap();
            assert_eq!(j.len(), 2);
        }
        let j2 = PushJournal::open(&sd_of(&p)).unwrap();
        assert_eq!(j2.len(), 2);
        assert!(j2.total_bytes() > 0);
    }

    #[test]
    fn append_respects_capacity() {
        let dir = TempDir::new().unwrap();
        let p = journal_path(&dir);
        // Pick a max so small the first append blows it.
        let mut j = PushJournal::open_with_capacity(&sd_of(&p), 32).unwrap();
        let err = j.append(evt("notes/a.md", PushAction::Create)).unwrap_err();
        match err {
            JournalError::CapacityExceeded { max, .. } => assert_eq!(max, 32),
            other => panic!("expected CapacityExceeded, got {:?}", other),
        }
        assert_eq!(j.len(), 0);
    }

    #[test]
    fn append_refreshes_projection_across_handles() {
        // S490 regression. `total_bytes` is a PER-HANDLE projection. An
        // append-only handle (file_watcher) must not get stuck at a stale-high
        // projection after ANOTHER handle (push_client) drains+acks the shared
        // file to empty. Before the fix, handle A's projection climbed to the
        // cap and never recovered → every append failed CapacityExceeded
        // forever against a 0-byte journal (the runaway hot-spin).
        let dir = TempDir::new().unwrap();
        let p = journal_path(&dir);
        let cap = 400u64; // a few entries fit, then the cap trips

        // Handle A (file_watcher role): append until the cap is reached.
        let mut a = PushJournal::open_with_capacity(&sd_of(&p), cap).unwrap();
        let mut n = 0;
        loop {
            match a.append(evt(&format!("notes/{n}.md"), PushAction::Create)) {
                Ok(()) => n += 1,
                Err(JournalError::CapacityExceeded { .. }) => break,
                Err(e) => panic!("unexpected error: {e:?}"),
            }
            assert!(n <= 1000, "capacity never hit — bad test cap");
        }
        assert!(n > 0, "should write at least one event before the cap");

        // Handle B (push_client role): drain + ack everything → file → 0 bytes.
        let mut b = PushJournal::open_with_capacity(&sd_of(&p), cap).unwrap();
        let cursors: Vec<_> = b
            .drain(10_000)
            .unwrap()
            .into_iter()
            .map(|(_, c)| c)
            .collect();
        b.ack_batch(cursors).unwrap();
        assert_eq!(b.len(), 0, "journal should be empty after ack");

        // Handle A still holds a stale-high projection. After the fix it
        // re-measures from disk and the append SUCCEEDS instead of looping.
        a.append(evt("notes/after-drain.md", PushAction::Create))
            .expect("append must succeed once the shared file is drained to empty");
        assert_eq!(a.len(), 1);
    }

    #[test]
    fn drain_does_not_remove_until_ack() {
        let dir = TempDir::new().unwrap();
        let p = journal_path(&dir);
        let mut j = PushJournal::open(&sd_of(&p)).unwrap();
        j.append(evt("notes/a.md", PushAction::Create)).unwrap();
        j.append(evt("notes/b.md", PushAction::Modify)).unwrap();

        let batch1 = j.drain(10).unwrap();
        assert_eq!(batch1.len(), 2);
        // Re-drain returns the same events.
        let batch2 = j.drain(10).unwrap();
        assert_eq!(batch2.len(), 2);
        assert_eq!(batch1[0].0, batch2[0].0);
    }

    #[test]
    fn ack_then_reopen_skips_ackd() {
        let dir = TempDir::new().unwrap();
        let p = journal_path(&dir);
        {
            let mut j = PushJournal::open(&sd_of(&p)).unwrap();
            for i in 0..4 {
                j.append(evt(&format!("notes/{i}.md"), PushAction::Create))
                    .unwrap();
            }
            let batch = j.drain(10).unwrap();
            // Ack the first 3 → each removes its line from the file.
            for (_, cur) in batch.iter().take(3) {
                j.ack(cur.clone()).unwrap();
            }
            assert_eq!(j.len(), 1);
        }
        let mut j2 = PushJournal::open(&sd_of(&p)).unwrap();
        assert_eq!(j2.len(), 1);
        let batch = j2.drain(10).unwrap();
        assert_eq!(batch[0].0.path, "notes/3.md");
    }

    #[test]
    fn corruption_at_tail_is_recovered() {
        let dir = TempDir::new().unwrap();
        let p = journal_path(&dir);
        {
            let mut j = PushJournal::open(&sd_of(&p)).unwrap();
            for i in 0..3 {
                j.append(evt(&format!("notes/{i}.md"), PushAction::Create))
                    .unwrap();
            }
        }
        // Append garbage tail.
        {
            let mut f = OpenOptions::new().append(true).open(&p).unwrap();
            f.write_all(b"{not valid json at all").unwrap();
        }
        let j = PushJournal::open(&sd_of(&p)).unwrap();
        assert_eq!(j.len(), 3);
    }

    #[test]
    fn nack_keeps_event() {
        let dir = TempDir::new().unwrap();
        let p = journal_path(&dir);
        let mut j = PushJournal::open(&sd_of(&p)).unwrap();
        j.append(evt("notes/a.md", PushAction::Create)).unwrap();
        let batch = j.drain(10).unwrap();
        j.nack(batch[0].1.clone()).unwrap();
        assert_eq!(j.len(), 1);
        let batch2 = j.drain(10).unwrap();
        assert_eq!(batch2.len(), 1);
    }

    #[test]
    fn ack_rewrites_and_shrinks_file() {
        // New model: every ack removes the acked line(s) and atomically
        // rewrites the file, so on-disk size shrinks immediately.
        let dir = TempDir::new().unwrap();
        let p = journal_path(&dir);
        let mut j = PushJournal::open(&sd_of(&p)).unwrap();
        for i in 0..10 {
            j.append(evt(&format!("notes/{i}.md"), PushAction::Create))
                .unwrap();
        }
        let before_bytes = j.total_bytes();
        let batch = j.drain(10).unwrap();
        for (_, cur) in batch.iter().take(6) {
            j.ack(cur.clone()).unwrap();
        }
        assert_eq!(j.len(), 4);
        // After ack-rewrites, on-disk size should have shrunk.
        let after_bytes = std::fs::metadata(&p).unwrap().len();
        assert!(
            after_bytes < before_bytes,
            "ack should shrink file: before={} after={}",
            before_bytes,
            after_bytes
        );
        assert_eq!(j.total_bytes(), after_bytes);
    }

    #[test]
    fn crash_simulation() {
        let dir = TempDir::new().unwrap();
        let p = journal_path(&dir);
        {
            let mut j = PushJournal::open(&sd_of(&p)).unwrap();
            for i in 0..100 {
                j.append(evt(&format!("notes/{i}.md"), PushAction::Create))
                    .unwrap();
            }
        }
        // Simulate crash mid-flush: truncate last 10 bytes (chops final line).
        let len = std::fs::metadata(&p).unwrap().len();
        let f = OpenOptions::new().write(true).open(&p).unwrap();
        f.set_len(len.saturating_sub(10)).unwrap();

        let j = PushJournal::open(&sd_of(&p)).unwrap();
        // 99 should remain intact; the 100th line lost its trailing bytes
        // and is skipped by the corrupt-tail handler.
        assert_eq!(j.len(), 99);
    }

    /// R4 / F-B3.2 (TKT-989ad5f2): `PushBase` is wire-compatible with the
    /// pre-fix `Option<String>`. `null` -> Unknown, `""` -> NoRow, a non-empty
    /// string -> KnownBase, and each round-trips back to the same wire byte.
    #[test]
    fn push_base_wire_is_option_string_compatible() {
        // Deserialize the three wire forms.
        assert_eq!(
            serde_json::from_str::<PushBase>("null").unwrap(),
            PushBase::Unknown
        );
        assert_eq!(
            serde_json::from_str::<PushBase>("\"\"").unwrap(),
            PushBase::NoRow
        );
        assert_eq!(
            serde_json::from_str::<PushBase>("\"deadbeef\"").unwrap(),
            PushBase::KnownBase("deadbeef".to_string())
        );
        // Serialize back to the identical wire form (pre-fix daemons read it).
        assert_eq!(serde_json::to_string(&PushBase::Unknown).unwrap(), "null");
        assert_eq!(serde_json::to_string(&PushBase::NoRow).unwrap(), "\"\"");
        assert_eq!(
            serde_json::to_string(&PushBase::KnownBase("deadbeef".into())).unwrap(),
            "\"deadbeef\""
        );
        // A pre-fix journal line (base_hash null on a modify) still loads.
        let legacy = r#"{"schema_version":1,"id":"x","path":"notes/a.md","action":"modify","base_hash":null,"content_sha":"aaaa","queued_at":"2026-06-01T00:00:00+00:00","device_id":"dev"}"#;
        let ev: PushEvent = serde_json::from_str(legacy).unwrap();
        assert_eq!(ev.base_hash, PushBase::Unknown);
    }

    #[test]
    fn schema_version_forward_compat() {
        let dir = TempDir::new().unwrap();
        let p = journal_path(&dir);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();

        // Write one good event + one with schema_version=2.
        let good = evt("notes/a.md", PushAction::Create);
        let good_line = serde_json::to_string(&good).unwrap();
        let future_line = serde_json::json!({
            "schema_version": 2u32,
            "path": "notes/future.md",
            "action": "create",
            "base_hash": null,
            "content_sha": "b".repeat(64),
            "content_bytes": [1, 2, 3],
            "queued_at": Utc::now().to_rfc3339(),
            "device_id": "dev-test",
            "new_field_from_v2": "ignored"
        })
        .to_string();

        let mut f = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&p)
            .unwrap();
        writeln!(f, "{good_line}").unwrap();
        writeln!(f, "{future_line}").unwrap();
        drop(f);

        let j = PushJournal::open(&sd_of(&p)).unwrap();
        // Only the v1 event is loaded; v2 is skipped (logged as warning).
        assert_eq!(j.len(), 1);
    }

    #[test]
    fn none_content_event_round_trips() {
        // A lazy (content_bytes: None) event must serialize, persist, reopen,
        // and drain back out with its None content intact.
        let dir = TempDir::new().unwrap();
        let p = journal_path(&dir);
        let lazy = PushEvent {
            schema_version: CURRENT_SCHEMA,
            id: new_event_id(),
            path: "notes/lazy.md".to_string(),
            action: PushAction::Modify,
            base_hash: PushBase::KnownBase("0".repeat(64)),
            content_sha: "a".repeat(64),
            content_bytes: None,
            queued_at: Utc::now(),
            device_id: "dev-test".to_string(),
        };
        {
            let mut j = PushJournal::open(&sd_of(&p)).unwrap();
            j.append(lazy.clone()).unwrap();
            assert_eq!(j.len(), 1);
        }
        let mut j2 = PushJournal::open(&sd_of(&p)).unwrap();
        let batch = j2.drain(10).unwrap();
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].0.content_bytes, None);
        assert_eq!(batch[0].0.path, "notes/lazy.md");
    }

    #[test]
    fn append_batch_writes_all_events_in_one_call() {
        let dir = TempDir::new().unwrap();
        let p = journal_path(&dir);
        let events: Vec<PushEvent> = (0..1000)
            .map(|i| evt(&format!("notes/{i}.md"), PushAction::Create))
            .collect();
        {
            let mut j = PushJournal::open(&sd_of(&p)).unwrap();
            let written = j.append_batch(events).unwrap();
            assert_eq!(written, 1000);
            assert_eq!(j.len(), 1000);
        }
        // Reopen and confirm all 1000 persisted.
        let mut j2 = PushJournal::open(&sd_of(&p)).unwrap();
        assert_eq!(j2.len(), 1000);
        let batch = j2.drain(2000).unwrap();
        assert_eq!(batch.len(), 1000);
    }

    #[test]
    fn append_batch_writes_what_fits_then_stops() {
        // Capacity guard: a batch that can't fully fit writes the prefix that
        // does and returns the count, instead of erroring the whole batch.
        let dir = TempDir::new().unwrap();
        let p = journal_path(&dir);
        // Build the events first, then size the cap so EXACTLY the first two
        // fit (their two serialized lines + 1 byte of slack, but less than a
        // third line).
        let events: Vec<PushEvent> = (0..10)
            .map(|i| evt(&format!("notes/{i}.md"), PushAction::Create))
            .collect();
        let line_len = |e: &PushEvent| serde_json::to_string(e).unwrap().len() as u64 + 1;
        let cap = line_len(&events[0]) + line_len(&events[1]) + 1;
        let mut j = PushJournal::open_with_capacity(&sd_of(&p), cap).unwrap();
        let written = j.append_batch(events).unwrap();
        assert_eq!(written, 2, "only 2 of 10 events fit under the cap");
        assert_eq!(j.len(), 2);
    }

    /// concurrent_safety_documented: historically a reminder that no flock
    /// was used. Unit 1a (TKT-b25fce06) added the per-handle flock on
    /// `push_journal.lock`; the race coverage now lives in the `tj*` tests.
    #[test]
    fn concurrent_safety_documented() {
        // No runtime assertion — the doc comment IS the invariant.
        let _ = std::any::type_name::<PushJournal>();
    }

    /// THE bug regression. Two SEPARATE handles on the SAME path: handle A
    /// appends, handle B (opened independently) must see A's append on drain.
    /// Pre-fix this failed (B served a stale in-memory Vec from open()) and the
    /// push pipeline silently did nothing. Post-fix drain re-reads the file.
    #[test]
    fn separate_handles_see_each_others_appends() {
        let dir = TempDir::new().unwrap();
        let p = journal_path(&dir);

        let mut a = PushJournal::open(&sd_of(&p)).unwrap();
        let mut b = PushJournal::open(&sd_of(&p)).unwrap();

        // A appends AFTER B was already opened.
        a.append(evt("notes/from-a.md", PushAction::Create))
            .unwrap();

        // B must observe A's append even though it opened first.
        let drained = b.drain(10).unwrap();
        assert_eq!(drained.len(), 1, "B must see the event A appended");
        assert_eq!(drained[0].0.path, "notes/from-a.md");
    }

    /// Ack-by-id removes from the file and the removal is visible to a THIRD
    /// fresh handle. A appends 3; B drains + acks 1 by id; C (fresh) drains and
    /// sees the other 2 — proving content-identity ack converges across handles.
    #[test]
    fn ack_by_id_removes_from_file_across_handles() {
        let dir = TempDir::new().unwrap();
        let p = journal_path(&dir);

        let mut a = PushJournal::open(&sd_of(&p)).unwrap();
        a.append(evt("notes/0.md", PushAction::Create)).unwrap();
        a.append(evt("notes/1.md", PushAction::Create)).unwrap();
        a.append(evt("notes/2.md", PushAction::Create)).unwrap();

        let mut b = PushJournal::open(&sd_of(&p)).unwrap();
        let batch = b.drain(10).unwrap();
        assert_eq!(batch.len(), 3);
        // Ack the middle one by its cursor (event id).
        let (acked_evt, acked_cur) = batch[1].clone();
        b.ack(acked_cur).unwrap();

        // A fresh handle sees exactly the two un-acked events.
        let mut c = PushJournal::open(&sd_of(&p)).unwrap();
        let remaining = c.drain(10).unwrap();
        assert_eq!(remaining.len(), 2);
        let paths: Vec<&str> = remaining.iter().map(|(e, _)| e.path.as_str()).collect();
        assert!(!paths.contains(&acked_evt.path.as_str()));
        assert!(paths.contains(&"notes/0.md"));
        assert!(paths.contains(&"notes/2.md"));
    }

    // -----------------------------------------------------------------------
    // Unit 1a (TKT-b25fce06) §3: T-J1..T-J8
    // -----------------------------------------------------------------------

    fn lazy_evt(path: &str) -> PushEvent {
        let mut e = evt(path, PushAction::Modify);
        e.content_bytes = None;
        e
    }

    fn ids_on_disk(p: &Path) -> Vec<String> {
        let j = PushJournal::open(&sd_of(p)).unwrap();
        j.with_lock("test", |j, l| j.read_live_from_locked(l))
            .unwrap()
            .into_iter()
            .map(|e| e.id)
            .collect()
    }

    fn tmp_files_in(dir: &Path) -> Vec<String> {
        std::fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|n| n.contains(".tmp"))
            .collect()
    }

    const TJ1_CHILD_ENV: &str = "PJ_TJ1_CHILD_JOURNAL";

    /// T-J1 helper, NOT a test on its own: the second OS process. The parent
    /// test re-executes this test binary filtered to this test with the
    /// journal path in the environment; without it this returns immediately.
    #[test]
    #[ignore = "T-J1 second-process helper; spawned by tj1_no_appended_event_lost_under_concurrent_ack_and_compact"]
    fn tj1_child_process_appender() {
        let Ok(p) = std::env::var(TJ1_CHILD_ENV) else {
            return;
        };
        let n: usize = std::env::var("PJ_TJ1_CHILD_N").unwrap().parse().unwrap();
        let out = std::env::var("PJ_TJ1_CHILD_OUT").unwrap();
        let mut j = PushJournal::open(&sd_of(Path::new(&p))).unwrap();
        let mut ids = Vec::with_capacity(n);
        for i in 0..n {
            let mut e = lazy_evt(&format!("child/{i}.md"));
            e.id = format!("child-{}", new_event_id());
            ids.push(e.id.clone());
            j.append(e).unwrap();
        }
        std::fs::write(out, ids.join("\n")).unwrap();
    }

    /// T-J1: 10,000 append iterations from FOUR independently opened handles
    /// in one process (C3) plus a second OS process, racing `ack_batch` AND
    /// `compact`. Every appended event is either acked or still on disk.
    #[test]
    fn tj1_no_appended_event_lost_under_concurrent_ack_and_compact() {
        use std::collections::HashSet;
        use std::sync::atomic::AtomicBool;
        use std::sync::{Arc, Mutex};

        const TRACKED: usize = 4_000; // appender thread
        const CHILD: usize = 3_000; // second process
        const DUPS: usize = 3_000; // duplicate-path appends that compact collapses
        let dir = TempDir::new().unwrap();
        let p = journal_path(&dir);

        // Four independently opened handles (each with its own lock fd).
        let mut appender = PushJournal::open(&sd_of(&p)).unwrap();
        let mut duper = PushJournal::open(&sd_of(&p)).unwrap();
        let mut acker = PushJournal::open(&sd_of(&p)).unwrap();
        let mut compactor = PushJournal::open(&sd_of(&p)).unwrap();

        let child_out = dir.path().join("child_ids.txt");
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "push_journal::tests::tj1_child_process_appender",
                "--ignored",
                "--test-threads=1",
                "--quiet",
            ])
            .env(TJ1_CHILD_ENV, &p)
            .env("PJ_TJ1_CHILD_N", CHILD.to_string())
            .env("PJ_TJ1_CHILD_OUT", &child_out)
            .stdout(std::process::Stdio::null())
            .spawn()
            .expect("spawn second process");

        let appending_done = Arc::new(AtomicBool::new(false));
        let acked: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));

        let t_append = std::thread::spawn(move || {
            let mut ids = Vec::with_capacity(TRACKED);
            for i in 0..TRACKED {
                let e = lazy_evt(&format!("tracked/{i}.md"));
                ids.push(e.id.clone());
                appender.append(e).unwrap();
            }
            ids
        });
        let t_dup = std::thread::spawn(move || {
            for i in 0..DUPS {
                duper
                    .append(lazy_evt(&format!("dup/{}.md", i % 7)))
                    .unwrap();
            }
        });
        let done_a = Arc::clone(&appending_done);
        let acked_a = Arc::clone(&acked);
        let t_ack = std::thread::spawn(move || {
            let mut rounds = 0usize;
            while !done_a.load(Ordering::SeqCst) {
                // Never ack the `dup/` events: they must survive for `compact` to
                // collapse, otherwise a fast acker (observed on Windows CI) drains
                // them first and the compactor never actually rewrites, so the
                // positive control below cannot prove both rewriters raced.
                let batch: Vec<_> = acker
                    .drain(usize::MAX)
                    .unwrap()
                    .into_iter()
                    .filter(|(e, _)| !e.path.starts_with("dup/"))
                    .take(32)
                    .collect();
                if batch.is_empty() {
                    std::thread::yield_now();
                    continue;
                }
                let ids: Vec<String> = batch.iter().map(|(e, _)| e.id.clone()).collect();
                acker.ack_batch(batch.into_iter().map(|(_, c)| c)).unwrap();
                acked_a.lock().unwrap().extend(ids);
                rounds += 1;
            }
            rounds
        });
        let done_c = Arc::clone(&appending_done);
        let t_compact = std::thread::spawn(move || {
            let mut rewrites = 0usize;
            while !done_c.load(Ordering::SeqCst) {
                let (b, a) = compactor.compact().unwrap();
                if a < b {
                    rewrites += 1;
                }
                std::thread::yield_now();
            }
            rewrites
        });

        let tracked_ids = t_append.join().unwrap();
        t_dup.join().unwrap();
        let status = child.wait().unwrap();
        assert!(status.success(), "second process failed: {status}");
        appending_done.store(true, Ordering::SeqCst);
        let ack_rounds = t_ack.join().unwrap();
        let compact_rewrites = t_compact.join().unwrap();

        let child_ids: Vec<String> = std::fs::read_to_string(&child_out)
            .unwrap()
            .lines()
            .map(str::to_string)
            .collect();
        assert_eq!(child_ids.len(), CHILD);

        let on_disk: HashSet<String> = ids_on_disk(&p).into_iter().collect();
        let acked = acked.lock().unwrap();
        let lost: Vec<&String> = tracked_ids
            .iter()
            .chain(child_ids.iter())
            .filter(|id| !acked.contains(*id) && !on_disk.contains(*id))
            .collect();
        eprintln!(
            "tj1: ack_rounds={ack_rounds} compact_rewrites={compact_rewrites} acked={} on_disk={} lost={}",
            acked.len(),
            on_disk.len(),
            lost.len()
        );
        assert!(
            ack_rounds > 0 && compact_rewrites > 0,
            "both rewriters must have raced"
        );
        assert!(
            lost.is_empty(),
            "{} appended events lost (first: {:?})",
            lost.len(),
            lost.first()
        );
        assert!(tmp_files_in(p.parent().unwrap()).is_empty());
    }

    /// T-J2: parallel rewriters never share a temp file name and leave no
    /// temp file behind, on success or on error.
    #[test]
    fn tj2_rewriters_use_unique_temp_and_leave_none_behind() {
        let dir = TempDir::new().unwrap();
        let p = journal_path(&dir);
        // One shared StateDir so its syscall recorder sees every temp name.
        let shared = sd_of(&p);
        let mut seed = PushJournal::open(&shared).unwrap();
        for i in 0..20 {
            seed.append(lazy_evt(&format!("d/{}.md", i % 3))).unwrap();
        }
        let workers: Vec<_> = (0..2)
            .map(|w| {
                let sd = Arc::clone(&shared);
                std::thread::spawn(move || {
                    let mut j = PushJournal::open(&sd).unwrap();
                    for i in 0..40 {
                        // Re-add duplicates so every compact really rewrites.
                        j.append(lazy_evt(&format!("d/{}.md", i % 3))).unwrap();
                        j.append(lazy_evt(&format!("d/{}.md", i % 3))).unwrap();
                        j.compact().unwrap();
                        let ids: Vec<_> = j.drain(1).unwrap().into_iter().map(|(_, c)| c).collect();
                        j.ack_batch(ids).unwrap();
                    }
                    (w, ())
                })
            })
            .collect();
        for h in workers {
            h.join().unwrap();
        }
        let all: Vec<String> = shared
            .seams
            .recorder
            .lock()
            .unwrap()
            .iter()
            .filter_map(|r| r.strip_prefix("openat:"))
            .filter(|n| n.contains(".tmp-"))
            .map(str::to_string)
            .collect();
        let pid = std::process::id().to_string();
        for s in &all {
            assert!(
                s.starts_with("push_journal.jsonl.tmp-") && s.contains(&pid),
                "unexpected temp name {s}"
            );
        }
        let uniq: std::collections::HashSet<_> = all.iter().collect();
        assert_eq!(
            uniq.len(),
            all.len(),
            "two rewrites shared a temp file name"
        );
        assert!(all.len() >= 80);
        assert!(
            tmp_files_in(p.parent().unwrap()).is_empty(),
            "temp left on success"
        );

        // Error path: an injected sync failure must unlink the temp.
        seed.append(lazy_evt("d/0.md")).unwrap();
        seed.append(lazy_evt("d/0.md")).unwrap();
        seed.faults.fail_rewrite_sync.store(true, Ordering::SeqCst);
        assert!(seed.compact().is_err());
        assert!(
            tmp_files_in(p.parent().unwrap()).is_empty(),
            "temp left on error"
        );
    }

    /// T-J3: an injected `sync_all` error in rewrite returns `Err`, leaves the
    /// old journal byte-identical, and unlinks the temp.
    #[test]
    fn tj3_rewrite_sync_error_propagates_and_keeps_old_journal() {
        let dir = TempDir::new().unwrap();
        let p = journal_path(&dir);
        let mut j = PushJournal::open(&sd_of(&p)).unwrap();
        for i in 0..4 {
            j.append(lazy_evt(&format!("n/{i}.md"))).unwrap();
        }
        let before = std::fs::read(&p).unwrap();
        let cur = j.drain(1).unwrap().remove(0).1;
        j.faults.fail_rewrite_sync.store(true, Ordering::SeqCst);
        let r = j.ack(cur);
        assert!(r.is_err(), "sync_all failure must propagate, got {r:?}");
        assert_eq!(
            std::fs::read(&p).unwrap(),
            before,
            "old journal must be intact"
        );
        assert!(
            tmp_files_in(p.parent().unwrap()).is_empty(),
            "temp must be unlinked"
        );
        assert_eq!(j.len(), 4);
    }

    /// T-J4: an injected `sync_data` error in append_batch returns `Err`,
    /// never `Ok(n)`.
    #[test]
    fn tj4_append_sync_error_returns_err() {
        let dir = TempDir::new().unwrap();
        let p = journal_path(&dir);
        let mut j = PushJournal::open(&sd_of(&p)).unwrap();
        j.faults.fail_append_sync.store(true, Ordering::SeqCst);
        let r = j.append_batch(vec![lazy_evt("a.md"), lazy_evt("b.md")]);
        assert!(
            r.is_err(),
            "append_batch must not return Ok(n) on sync failure: {r:?}"
        );
        assert!(j.append(lazy_evt("c.md")).is_err());
        j.faults.fail_append_sync.store(false, Ordering::SeqCst);
        assert_eq!(j.append_batch(vec![lazy_evt("d.md")]).unwrap(), 1);
    }

    /// T-J5: handle A appends past half capacity; handle B (opened earlier,
    /// projection 0) must decide capacity on the FRESH size.
    #[test]
    fn tj5_capacity_decision_uses_fresh_size_across_handles() {
        let dir = TempDir::new().unwrap();
        let p = journal_path(&dir);
        let probe = serde_json::to_string(&lazy_evt("cap/00.md")).unwrap().len() as u64 + 1;
        let cap = probe * 10 + 5;
        let mut b = PushJournal::open_with_capacity(&sd_of(&p), cap).unwrap();
        assert_eq!(b.total_bytes(), 0);
        let mut a = PushJournal::open_with_capacity(&sd_of(&p), cap).unwrap();
        for i in 0..6 {
            a.append(lazy_evt(&format!("cap/a{i}.md"))).unwrap();
        }
        assert!(
            std::fs::metadata(&p).unwrap().len() * 2 > cap,
            "A is past half"
        );
        let batch: Vec<_> = (0..6).map(|i| lazy_evt(&format!("cap/b{i}.md"))).collect();
        let written = b.append_batch(batch).unwrap();
        let size = std::fs::metadata(&p).unwrap().len();
        assert!(size <= cap, "capacity overrun: size={size} cap={cap}");
        assert!(
            written < 6,
            "B must not believe the journal is empty (wrote {written})"
        );
    }

    /// T-J6: crash-injection points inside rewrite. After a "restart"
    /// (dropping the handle and reopening), the journal holds exactly the old
    /// or exactly the new full set, never a partial one.
    #[test]
    fn tj6_crash_points_leave_old_or_new_full_set() {
        for (point, expect_new) in [
            (CrashPoint::TempWritten, false),
            (CrashPoint::TempSynced, false),
            (CrashPoint::Renamed, true),
            (CrashPoint::DirSynced, true),
        ] {
            let dir = TempDir::new().unwrap();
            let p = journal_path(&dir);
            let mut j = PushJournal::open(&sd_of(&p)).unwrap();
            for i in 0..6 {
                j.append(lazy_evt(&format!("c/{i}.md"))).unwrap();
            }
            let batch = j.drain(10).unwrap();
            let old: Vec<String> = batch.iter().map(|(e, _)| e.id.clone()).collect();
            let new: Vec<String> = old[2..].to_vec();
            *j.faults.crash_at.lock().unwrap() = Some(point);
            let r = j.ack_batch(batch.into_iter().take(2).map(|(_, c)| c));
            assert!(r.is_err(), "{point:?}: crash must surface");
            drop(j); // "crash"
            let after = ids_on_disk(&p);
            assert!(
                after == old || after == new,
                "{point:?}: partial journal after restart: {after:?}"
            );
            assert_eq!(after == new, expect_new, "{point:?}");
            // The journal stays fully usable after the restart.
            let mut j2 = PushJournal::open(&sd_of(&p)).unwrap();
            j2.append(lazy_evt("c/after.md")).unwrap();
            assert_eq!(j2.len(), after.len() + 1);
        }
    }

    /// T-J7 (C4): single-`append` latency bench. Not run by default; run with
    ///   cargo test --lib -- --ignored tj7 --nocapture
    /// `PJ_BENCH_DIR` picks the filesystem (default: the current dir, which
    /// under cargo test is the crate dir on the real disk, not tmpfs).
    #[test]
    #[ignore = "bench: cargo test --lib -- --ignored tj7 --nocapture"]
    fn tj7_bench_single_append_latency() {
        let base = std::env::var_os("PJ_BENCH_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| std::env::current_dir().unwrap());
        let n: usize = std::env::var("PJ_BENCH_N")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(2000);
        let dir = tempfile::Builder::new()
            .prefix("pj-bench-")
            .tempdir_in(&base)
            .unwrap();
        let p = dir.path().join("push_journal.jsonl");
        let mut j = PushJournal::open(&sd_of(&p)).unwrap();
        for i in 0..20 {
            j.append(evt(&format!("warm/{i}.md"), PushAction::Modify))
                .unwrap();
        }
        let mut lat: Vec<f64> = Vec::with_capacity(n);
        for i in 0..n {
            // Watcher-path shape: an embedded small body.
            let e = evt(&format!("bench/{i}.md"), PushAction::Modify);
            let t = Instant::now();
            j.append(e).unwrap();
            lat.push(t.elapsed().as_secs_f64() * 1000.0);
        }
        lat.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let pct = |q: f64| lat[((lat.len() as f64 - 1.0) * q).round() as usize];
        let mean = lat.iter().sum::<f64>() / lat.len() as f64;
        println!(
            "T-J7 append latency over {n} appends in {}: p50={:.3} ms p99={:.3} ms max={:.3} ms mean={:.3} ms",
            base.display(),
            pct(0.50),
            pct(0.99),
            lat[lat.len() - 1],
            mean
        );
    }

    /// T-J8 (C1): `ack()` (sharing the `ack_batch` body) takes the lock
    /// exactly once; entering `with_lock` while held trips the re-entrancy
    /// guard (panic in debug, error in release); the handle stays usable.
    #[test]
    fn tj8_ack_locks_once_and_reentry_trips_guard() {
        let dir = TempDir::new().unwrap();
        let p = journal_path(&dir);
        let mut j = PushJournal::open(&sd_of(&p)).unwrap();
        for i in 0..3 {
            j.append(lazy_evt(&format!("r/{i}.md"))).unwrap();
        }
        let cur = j.drain(1).unwrap().remove(0).1;
        let before = j.faults.lock_acquisitions.load(Ordering::SeqCst);
        j.ack(cur).unwrap();
        let after = j.faults.lock_acquisitions.load(Ordering::SeqCst);
        assert_eq!(
            after - before,
            1,
            "ack must take the journal lock exactly once"
        );

        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            j.with_lock("outer", |jj, _| jj.with_lock("inner", |_, _| Ok(())))
        }));
        if cfg!(debug_assertions) {
            assert!(r.is_err(), "debug build: re-entry must panic");
        } else {
            assert!(
                matches!(r, Ok(Err(_))),
                "release build: re-entry must error"
            );
        }
        // The guard released the flock and the flag on unwind.
        assert_eq!(j.len(), 2);
        let mut other = PushJournal::open(&sd_of(&p)).unwrap();
        other.append(lazy_evt("r/x.md")).unwrap();
        assert_eq!(j.len(), 3);
    }

    /// C6: the lock file is a 0600 sibling that survives rewrites.
    #[test]
    fn lock_file_is_0600_sibling_and_never_removed() {
        let dir = TempDir::new().unwrap();
        let p = journal_path(&dir);
        let mut j = PushJournal::open(&sd_of(&p)).unwrap();
        let lp = p.parent().unwrap().join("push_journal.lock");
        assert!(lp.exists());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&lp).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        j.append(lazy_evt("a.md")).unwrap();
        j.append(lazy_evt("a.md")).unwrap();
        j.compact().unwrap();
        let c = j.drain(1).unwrap().remove(0).1;
        j.ack(c).unwrap();
        assert!(lp.exists(), "lock file must survive rewrites");
        assert_eq!(
            std::fs::metadata(&lp).unwrap().len(),
            0,
            "lock file holds no data"
        );
    }

    /// T-CS2-35 (gate C10, behaviour half): an append to the scratch journal
    /// is counted and emits the WARN (in every build profile; the debug
    /// assert is compiled out of test builds). The single-caller half is
    /// asserted by `tests/state_io_gate.rs`.
    #[test]
    fn t_cs2_35_scratch_append_warns_and_counts() {
        #[derive(Clone, Default)]
        struct Buf(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);
        impl std::io::Write for Buf {
            fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(b);
                Ok(b.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let buf = Buf::default();
        let w = buf.clone();
        let sub = tracing_subscriber::fmt()
            .with_writer(move || w.clone())
            .with_ansi(false)
            .finish();
        let mut j = PushJournal::scratch();
        tracing::subscriber::with_default(sub, || {
            assert_eq!(j.scratch_appends(), 0);
            j.append(lazy_evt("x.md")).unwrap();
            assert_eq!(j.append_batch(vec![lazy_evt("y.md")]).unwrap(), 1);
        });
        assert_eq!(j.scratch_appends(), 2);
        assert_eq!(j.len(), 0, "the scratch sink stores nothing");
        let log = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
        assert_eq!(log.matches("SCRATCH journal").count(), 2, "{log}");
        assert!(log.contains("WARN"), "{log}");
    }
}
