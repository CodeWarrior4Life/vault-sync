//! R3 + R13: persistent jsonl-line append-only journal of pending local->server pushes.
//!
//! Each line is a single JSON object representing one queued push event. The
//! journal lives outside the vault tree at
//! `<workspace>/.lattice-runtime/<slug>/sync-state/push_journal.jsonl`
//! (caller decides; this module accepts a `PathBuf` to the journal file).
//!
//! Concurrency: this module is **NOT** internally locked across processes.
//! It assumes the daemon is single-instance — enforced by R12
//! (tauri_plugin_single_instance). All methods take `&mut self`, so the
//! Rust borrow checker enforces single-thread mutation within the process.
//! If we ever drop the single-instance guarantee, we MUST add an `flock`
//! around the file handle.
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

use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

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
    /// None when action == Create.
    pub base_hash: Option<String>,
    pub content_sha: String,
    /// `Some(bytes)`: body embedded inline (caller already had the bytes).
    /// `None`: content not embedded — the reader loads it from disk at push
    /// time. Lets verify_repair enqueue lightweight refs without re-reading.
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

/// File-authoritative journal handle. Holds NO cached event list — `drain`
/// and `ack` re-read the on-disk file every call so any number of handles on
/// the same path converge on the same state (v0.3 stale-state fix). The only
/// retained state is `path`, the capacity cap, and a `total_bytes` projection
/// kept loosely in sync for the capacity guard (re-measured from disk on every
/// op that reads the file, so a stale projection self-heals).
pub struct PushJournal {
    path: PathBuf,
    /// Current file size in bytes. Updated on append (projection) and
    /// re-measured authoritatively on every read/rewrite.
    total_bytes: u64,
    /// Capacity guard in bytes (default 100 MB per spec §4.2).
    max_bytes: u64,
}

const DEFAULT_MAX_BYTES: u64 = 100 * 1024 * 1024;

impl PushJournal {
    /// Open (or create) the journal at `path`. Scans existing lines and
    /// recovers from corrupt-tail / unknown-schema lines by skipping them
    /// with a tracing warning.
    pub fn open(path: &Path) -> Result<Self, JournalError> {
        Self::open_with_capacity(path, DEFAULT_MAX_BYTES)
    }

    pub fn open_with_capacity(path: &Path, max_bytes: u64) -> Result<Self, JournalError> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }

        // Touch the file so subsequent appends always succeed.
        let _ = OpenOptions::new().create(true).append(true).open(path)?;

        // Validate the file is readable + scannable now (surfaces IO errors at
        // open time) and seed the byte projection. We do NOT cache events.
        let _ = Self::read_live_from(path)?;
        let total_bytes = std::fs::metadata(path)?.len();

        Ok(Self {
            path: path.to_path_buf(),
            total_bytes,
            max_bytes,
        })
    }

    /// Re-read the on-disk journal and return all live (parseable, current
    /// schema) events in append order. This is the authoritative read — every
    /// `drain` / `ack` calls it so separate handles converge. Applies the
    /// same corrupt-tail / forward-compat tolerance as the original scan.
    fn read_live_from(path: &Path) -> Result<Vec<PushEvent>, JournalError> {
        let file = File::open(path)?;
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

    /// Atomically append one event. Open append+create, write line+\n, flush, close.
    ///
    /// Back-compat shim — delegates to `append_batch`. Preserves the original
    /// hard-error-on-capacity contract: a single append that would exceed the
    /// cap returns `CapacityExceeded` and writes nothing (because
    /// `append_batch` writes "what fits" and here nothing fits → 0 written).
    pub fn append(&mut self, evt: PushEvent) -> Result<(), JournalError> {
        // Pre-check so a single over-cap append errors (matching the old
        // contract) rather than silently writing 0 events. Re-measure from
        // disk first (see append_batch) so the check reflects the real shared
        // file size, not a stale-high per-handle projection.
        let mut line = serde_json::to_string(&evt)?;
        line.push('\n');
        self.total_bytes = std::fs::metadata(&self.path).map(|m| m.len()).unwrap_or(0);
        if self.total_bytes.saturating_add(line.len() as u64) > self.max_bytes {
            return Err(JournalError::CapacityExceeded {
                current: self.total_bytes,
                max: self.max_bytes,
            });
        }
        let written = self.append_batch(vec![evt])?;
        debug_assert_eq!(written, 1);
        Ok(())
    }

    /// Append N events in ONE file open/flush/close cycle.
    ///
    /// Opens the journal once, serializes each event to one jsonl line, writes
    /// them all, flushes once, closes. Returns the number of events actually
    /// appended.
    ///
    /// Capacity policy (graceful degradation): events are written one at a
    /// time against the running `total_bytes` projection. The moment the NEXT
    /// event would push the file past `max_bytes`, writing stops — already
    /// written events stay durable, the rest are dropped, and a warning is
    /// logged. A huge first-sync therefore degrades to "write what fits"
    /// instead of erroring the whole batch.
    pub fn append_batch(&mut self, events: Vec<PushEvent>) -> Result<usize, JournalError> {
        if events.is_empty() {
            return Ok(0);
        }

        let mut f = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;

        // Refresh the byte projection from disk before the capacity check.
        // `total_bytes` is a PER-HANDLE projection and other handles
        // (push_client drain+ack → rewrite()) SHRINK the shared file. An
        // append-only handle (file_watcher) never drains, so without this
        // refresh its projection only ever climbs — once it touches the cap it
        // can NEVER append again, even after the journal has been fully drained
        // to 0 bytes, hot-spinning on CapacityExceeded against an empty file.
        // (S490 runaway: on-disk journal 0 bytes, projection stuck at ~100MB,
        // 500k+ capacity-exceeded warnings, all new events dropped.) Mirrors
        // the disk re-read that `drain()` and `rewrite()` already do.
        self.total_bytes = f.metadata().map(|m| m.len()).unwrap_or(self.total_bytes);

        let total = events.len();
        let mut written = 0usize;
        for evt in events {
            let mut line = serde_json::to_string(&evt)?;
            line.push('\n');
            let added = line.len() as u64;

            if self.total_bytes.saturating_add(added) > self.max_bytes {
                tracing::warn!(
                    "push_journal: append_batch hit capacity (current={} max={}); wrote {}/{} events, dropping remainder",
                    self.total_bytes,
                    self.max_bytes,
                    written,
                    total
                );
                break;
            }

            f.write_all(line.as_bytes())?;
            self.total_bytes = self.total_bytes.saturating_add(added);
            written += 1;
        }

        f.flush()?;
        drop(f);
        Ok(written)
    }

    /// Read up to `n` live events from the head of the file, without removing
    /// them. **Re-reads the on-disk file every call** (no cache) so a handle
    /// always sees appends made by OTHER handles — the v0.3 stale-state fix.
    /// Returns events paired with cursors (wrapping each event's stable `id`)
    /// that the caller hands back to `ack` / `nack`.
    pub fn drain(&mut self, n: usize) -> Result<Vec<(PushEvent, JournalCursor)>, JournalError> {
        let live = Self::read_live_from(&self.path)?;
        // Keep the byte projection honest for the capacity guard.
        self.total_bytes = std::fs::metadata(&self.path)?.len();
        let out = live
            .into_iter()
            .take(n)
            .map(|evt| {
                let cur = JournalCursor(evt.id.clone());
                (evt, cur)
            })
            .collect();
        Ok(out)
    }

    /// Ack a single event by its cursor (event id) — removes the matching line
    /// from the file. Convenience wrapper over `ack_batch`.
    pub fn ack(&mut self, cursor: JournalCursor) -> Result<(), JournalError> {
        self.ack_batch(std::iter::once(cursor))
    }

    /// Ack a batch of events by cursor (event id). Re-reads the file, drops
    /// every entry whose `id` is in the ack set, and atomically rewrites the
    /// file (tmp + rename). Idempotent: acking an id not present is a no-op.
    /// Because identity is the on-disk `id` (not an in-memory index), this is
    /// correct across separate handles and crashes.
    pub fn ack_batch<I>(&mut self, cursors: I) -> Result<(), JournalError>
    where
        I: IntoIterator<Item = JournalCursor>,
    {
        let ack_ids: std::collections::HashSet<String> =
            cursors.into_iter().map(|JournalCursor(id)| id).collect();
        if ack_ids.is_empty() {
            return Ok(());
        }

        let live = Self::read_live_from(&self.path)?;
        let remaining: Vec<PushEvent> = live
            .into_iter()
            .filter(|evt| !ack_ids.contains(&evt.id))
            .collect();
        self.rewrite(&remaining)
    }

    /// Keep the event for a later retry. No-op on the on-disk file — the event
    /// remains and will be re-drained on the next tick. Idempotent (push has
    /// its own content-hash idempotency on the server side).
    pub fn nack(&mut self, _cursor: JournalCursor) -> Result<(), JournalError> {
        Ok(())
    }

    /// Number of live events currently on disk. Re-reads the file.
    pub fn len(&self) -> usize {
        Self::read_live_from(&self.path)
            .map(|v| v.len())
            .unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Total on-disk size in bytes (projection; refreshed on every read op).
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    /// Atomically rewrite the file to contain exactly `events` (tmp + rename),
    /// then refresh the byte projection.
    fn rewrite(&mut self, events: &[PushEvent]) -> Result<(), JournalError> {
        let tmp_path = self.path.with_extension("jsonl.tmp");
        {
            let mut tmp = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&tmp_path)?;
            for evt in events {
                let mut line = serde_json::to_string(evt)?;
                line.push('\n');
                tmp.write_all(line.as_bytes())?;
            }
            tmp.flush()?;
            // Best-effort fsync — ignore platforms that don't support it.
            let _ = tmp.sync_all();
        }
        std::fs::rename(&tmp_path, &self.path)?;
        self.total_bytes = std::fs::metadata(&self.path)?.len();
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn evt(path: &str, action: PushAction) -> PushEvent {
        PushEvent {
            schema_version: CURRENT_SCHEMA,
            id: new_event_id(),
            path: path.to_string(),
            action,
            base_hash: match action {
                PushAction::Create => None,
                _ => Some("0".repeat(64)),
            },
            content_sha: "a".repeat(64),
            content_bytes: Some(b"hello world".to_vec()),
            queued_at: Utc::now(),
            device_id: "dev-test".to_string(),
        }
    }

    fn journal_path(dir: &TempDir) -> PathBuf {
        dir.path().join("sync-state").join("push_journal.jsonl")
    }

    #[test]
    fn open_empty_file_returns_empty_journal() {
        let dir = TempDir::new().unwrap();
        let p = journal_path(&dir);
        let j = PushJournal::open(&p).unwrap();
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
            let mut j = PushJournal::open(&p).unwrap();
            j.append(evt("notes/a.md", PushAction::Create)).unwrap();
            j.append(evt("notes/b.md", PushAction::Modify)).unwrap();
            assert_eq!(j.len(), 2);
        }
        let j2 = PushJournal::open(&p).unwrap();
        assert_eq!(j2.len(), 2);
        assert!(j2.total_bytes() > 0);
    }

    #[test]
    fn append_respects_capacity() {
        let dir = TempDir::new().unwrap();
        let p = journal_path(&dir);
        // Pick a max so small the first append blows it.
        let mut j = PushJournal::open_with_capacity(&p, 32).unwrap();
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
        let mut a = PushJournal::open_with_capacity(&p, cap).unwrap();
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
        let mut b = PushJournal::open_with_capacity(&p, cap).unwrap();
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
        let mut j = PushJournal::open(&p).unwrap();
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
            let mut j = PushJournal::open(&p).unwrap();
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
        let mut j2 = PushJournal::open(&p).unwrap();
        assert_eq!(j2.len(), 1);
        let batch = j2.drain(10).unwrap();
        assert_eq!(batch[0].0.path, "notes/3.md");
    }

    #[test]
    fn corruption_at_tail_is_recovered() {
        let dir = TempDir::new().unwrap();
        let p = journal_path(&dir);
        {
            let mut j = PushJournal::open(&p).unwrap();
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
        let j = PushJournal::open(&p).unwrap();
        assert_eq!(j.len(), 3);
    }

    #[test]
    fn nack_keeps_event() {
        let dir = TempDir::new().unwrap();
        let p = journal_path(&dir);
        let mut j = PushJournal::open(&p).unwrap();
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
        let mut j = PushJournal::open(&p).unwrap();
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
            let mut j = PushJournal::open(&p).unwrap();
            for i in 0..100 {
                j.append(evt(&format!("notes/{i}.md"), PushAction::Create))
                    .unwrap();
            }
        }
        // Simulate crash mid-flush: truncate last 10 bytes (chops final line).
        let len = std::fs::metadata(&p).unwrap().len();
        let f = OpenOptions::new().write(true).open(&p).unwrap();
        f.set_len(len.saturating_sub(10)).unwrap();

        let j = PushJournal::open(&p).unwrap();
        // 99 should remain intact; the 100th line lost its trailing bytes
        // and is skipped by the corrupt-tail handler.
        assert_eq!(j.len(), 99);
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

        let j = PushJournal::open(&p).unwrap();
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
            base_hash: Some("0".repeat(64)),
            content_sha: "a".repeat(64),
            content_bytes: None,
            queued_at: Utc::now(),
            device_id: "dev-test".to_string(),
        };
        {
            let mut j = PushJournal::open(&p).unwrap();
            j.append(lazy.clone()).unwrap();
            assert_eq!(j.len(), 1);
        }
        let mut j2 = PushJournal::open(&p).unwrap();
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
            let mut j = PushJournal::open(&p).unwrap();
            let written = j.append_batch(events).unwrap();
            assert_eq!(written, 1000);
            assert_eq!(j.len(), 1000);
        }
        // Reopen and confirm all 1000 persisted.
        let mut j2 = PushJournal::open(&p).unwrap();
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
        let mut j = PushJournal::open_with_capacity(&p, cap).unwrap();
        let written = j.append_batch(events).unwrap();
        assert_eq!(written, 2, "only 2 of 10 events fit under the cap");
        assert_eq!(j.len(), 2);
    }

    /// concurrent_safety_documented: this module is single-instance only.
    /// All methods take `&mut self`. R12 (tauri_plugin_single_instance)
    /// guarantees only one process holds the journal file at a time. No
    /// cross-process flock is used. If that invariant ever breaks, this
    /// test exists to remind us to add one.
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

        let mut a = PushJournal::open(&p).unwrap();
        let mut b = PushJournal::open(&p).unwrap();

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

        let mut a = PushJournal::open(&p).unwrap();
        a.append(evt("notes/0.md", PushAction::Create)).unwrap();
        a.append(evt("notes/1.md", PushAction::Create)).unwrap();
        a.append(evt("notes/2.md", PushAction::Create)).unwrap();

        let mut b = PushJournal::open(&p).unwrap();
        let batch = b.drain(10).unwrap();
        assert_eq!(batch.len(), 3);
        // Ack the middle one by its cursor (event id).
        let (acked_evt, acked_cur) = batch[1].clone();
        b.ack(acked_cur).unwrap();

        // A fresh handle sees exactly the two un-acked events.
        let mut c = PushJournal::open(&p).unwrap();
        let remaining = c.drain(10).unwrap();
        assert_eq!(remaining.len(), 2);
        let paths: Vec<&str> = remaining.iter().map(|(e, _)| e.path.as_str()).collect();
        assert!(!paths.contains(&acked_evt.path.as_str()));
        assert!(paths.contains(&"notes/0.md"));
        assert!(paths.contains(&"notes/2.md"));
    }
}
