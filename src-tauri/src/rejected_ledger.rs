//! WS-A A3 (TKT-17fc22e5 / TKT-8fc0b2a4): durable ledger of permanently
//! rejected pushes, keyed by path and bound to the EXACT bytes that were
//! refused. Revised per the plan's adversarial review (AR-007, AR-015):
//! suppression is TEMPORAL, the cap never evicts, a daemon upgrade retries
//! everything once.
//!
//! ## Why
//!
//! A push the server rejects with 400/422 (or the daemon refuses locally under
//! the I3 contract) is skipped + acked so the journal never wedges. But the
//! reconciliation backstop decides push-vs-pull from hashes alone, so on its
//! next pass it re-queued the same bytes, which were rejected again, forever:
//! TKT-17fc22e5 measured 1:1 queued-to-rejected across 16 days (215/day on
//! Trinity for 3 paths). This ledger closes that loop.
//!
//! ## Rules
//!
//! * **Temporal suppression (AR-007).** A ledgered `(path, sha)` is NOT
//!   queued by reconciliation for [`RETRY_AFTER_SECS`] (24 h) after its
//!   `last_seen`; then it is retried ONCE. On rejection `last_seen` / `count`
//!   update and the next 24 h window starts; on `Sent` / `Merged` the entry is
//!   cleared. A changed sha retries immediately. Nothing is ever suppressed
//!   permanently: the worst case is one attempt per path per day.
//! * **Hard cap, no eviction (AR-015).** [`MAX_ENTRIES`] rows. When full, a
//!   NEW path is not recorded (WARN, [`RecordOutcome::CapReached`]) and simply
//!   retries at the old cadence; nothing is evicted silently. Updates to an
//!   already-ledgered path are always applied.
//! * **Invalidation on upgrade.** The file carries the daemon version that
//!   wrote it. On load under a different version every entry is dropped
//!   (WARN with the count) so each path is retried once by the new binary.
//! * **Operator clear.** Deleting the ledger file — `StateFile::RejectedLedger`
//!   (`rejected-ledger` + `.json`) in the config-scope state dir, beside the
//!   reconcile retry ledger — is the documented way to force every rejected
//!   path to retry on the next pass. A corrupt or unreadable file loads as
//!   EMPTY with a WARN; it never crashes the daemon.
//! * **Ordering and durability.** On a permanent rejection the push client's
//!   sequence is strictly: server response -> ledger row written durably
//!   ([`StateDir::replace_atomic`]: unique temp, `sync_all`, rename, dir sync)
//!   -> journal ack. If the ledger write fails the client logs ERROR and still
//!   acks (I47 requires the ack); the path is then re-queued next pass, which
//!   is the pre-existing behaviour, not a storm.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::state_io::{StateDir, StateFile};

/// Row cap. Reached => new paths are NOT recorded (WARN); nothing is evicted.
pub const MAX_ENTRIES: usize = 1000;

/// How long a ledgered `(path, sha)` is suppressed after its `last_seen`
/// before reconciliation retries it once: 24 h.
pub const RETRY_AFTER_SECS: u64 = 24 * 60 * 60;

/// Cap on a stored reason (chars). The push-side detail is already capped at
/// `api_client::REJECTED_DETAIL_CAP`; this is the ledger's own belt.
const REASON_CAP: usize = 512;

/// `status` value for a rejection the daemon issued itself (pre-push I3).
pub const STATUS_LOCAL_I3: u16 = 0;

/// One rejected path's durable state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RejectedEntry {
    pub path: String,
    /// sha256 (hex) of the exact bytes that were rejected.
    pub content_sha256: String,
    /// HTTP status (400 / 422) or [`STATUS_LOCAL_I3`] (0) for a local refusal.
    pub status: u16,
    /// The server's `detail` (or the local I3 kind + offset). Never note content.
    pub reason: String,
    /// Unix seconds of the first rejection of THESE bytes.
    pub first_seen: u64,
    /// Unix seconds of the most recent rejection; the suppression window
    /// ([`RETRY_AFTER_SECS`]) is measured from here.
    pub last_seen: u64,
    /// Rejections recorded for these bytes (resets when the sha changes).
    pub count: u32,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct LedgerFile {
    /// The daemon version that wrote the file. A different version on load
    /// invalidates every entry (each path is retried once by the new binary).
    #[serde(default)]
    daemon_version: String,
    entries: HashMap<String, RejectedEntry>,
}

/// What [`RejectedLedger::record`] did with the row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordOutcome {
    /// The row was inserted or updated (and persisted, if `Ok`).
    Recorded,
    /// The ledger is full and `path` was not already ledgered: NOT recorded.
    /// The path retries at the old cadence; nothing was evicted.
    CapReached,
}

/// Unix-seconds clock, injectable so the temporal rules are testable.
pub type Clock = Arc<dyn Fn() -> u64 + Send + Sync>;

fn system_clock() -> Clock {
    Arc::new(|| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    })
}

fn cap_chars(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        s.chars().take(max).collect()
    }
}

/// Thread-safe durable rejected ledger persisted to a JSON file.
pub struct RejectedLedger {
    /// The config-scope state dir; `None` = in-memory only.
    sd: Option<Arc<StateDir>>,
    /// The version stamped into the file on every persist.
    daemon_version: String,
    clock: Clock,
    inner: Mutex<HashMap<String, RejectedEntry>>,
}

impl std::fmt::Debug for RejectedLedger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RejectedLedger")
            .field("persisted", &self.sd.is_some())
            .field("daemon_version", &self.daemon_version)
            .field("len", &self.len())
            .finish()
    }
}

impl RejectedLedger {
    /// Load from the config-scope state dir under the running daemon's
    /// version and the system clock. A missing or corrupt file yields an
    /// EMPTY ledger (WARN, never an error); a file written by a different
    /// daemon version is dropped (WARN with the count) so every path is
    /// retried once.
    pub fn load(sd: &Arc<StateDir>) -> Self {
        Self::load_for_version(sd, crate::api_client::daemon_version(), system_clock())
    }

    /// [`Self::load`] with an explicit version and clock (tests; the
    /// production caller passes the daemon version and the system clock).
    pub fn load_for_version(sd: &Arc<StateDir>, daemon_version: &str, clock: Clock) -> Self {
        let inner = match Self::read_file(sd) {
            ReadOutcome::Absent => HashMap::new(),
            ReadOutcome::Corrupt => {
                tracing::warn!(
                    file = StateFile::RejectedLedger.name(),
                    "rejected_ledger: file unreadable or corrupt - starting EMPTY (every rejected path retries once)"
                );
                HashMap::new()
            }
            ReadOutcome::Loaded { version, entries } if version != daemon_version => {
                tracing::warn!(
                    file = StateFile::RejectedLedger.name(),
                    from_version = %version,
                    to_version = %daemon_version,
                    dropped = entries.len(),
                    "rejected_ledger: daemon version changed - dropping every entry so each rejected path is retried once by this binary"
                );
                HashMap::new()
            }
            ReadOutcome::Loaded { entries, .. } => entries,
        };
        Self {
            sd: Some(Arc::clone(sd)),
            daemon_version: daemon_version.to_string(),
            clock,
            inner: Mutex::new(inner),
        }
    }

    /// A ledger that persists nothing (config dir not bootstrapped; tests).
    pub fn in_memory() -> Self {
        Self::in_memory_with_clock(system_clock())
    }

    /// In-memory ledger with an injectable clock (tests of the temporal rules).
    pub fn in_memory_with_clock(clock: Clock) -> Self {
        Self {
            sd: None,
            daemon_version: crate::api_client::daemon_version().to_string(),
            clock,
            inner: Mutex::new(HashMap::new()),
        }
    }

    fn read_file(sd: &StateDir) -> ReadOutcome {
        let bytes = match sd.read(StateFile::RejectedLedger) {
            Ok(Some(b)) => b,
            Ok(None) => return ReadOutcome::Absent,
            Err(_) => return ReadOutcome::Corrupt,
        };
        match serde_json::from_slice::<LedgerFile>(&bytes) {
            Ok(f) => ReadOutcome::Loaded {
                version: f.daemon_version,
                entries: f.entries,
            },
            Err(_) => ReadOutcome::Corrupt,
        }
    }

    fn now(&self) -> u64 {
        (self.clock)()
    }

    /// Record that `content_sha256` for `path` was rejected (`status`,
    /// `reason`) and persist DURABLY before returning. Same sha as the
    /// existing row: `count += 1`, `last_seen` = now, reason updated (a new
    /// 24 h window starts). Different sha: the row is REPLACED (new bytes,
    /// new `first_seen`, count 1). A new path when the ledger is full is NOT
    /// recorded ([`RecordOutcome::CapReached`], WARN). `Err` = the row is in
    /// memory but the file write failed; the caller logs ERROR and still acks.
    pub fn record(
        &self,
        path: &str,
        content_sha256: &str,
        status: u16,
        reason: &str,
    ) -> std::io::Result<RecordOutcome> {
        let now = self.now();
        let outcome = {
            let mut g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
            let reason = cap_chars(reason, REASON_CAP);
            let full = g.len() >= MAX_ENTRIES;
            match g.get_mut(path) {
                Some(e) if e.content_sha256 == content_sha256 => {
                    e.status = status;
                    e.reason = reason;
                    e.last_seen = now;
                    e.count = e.count.saturating_add(1);
                    RecordOutcome::Recorded
                }
                Some(e) => {
                    *e = RejectedEntry {
                        path: path.to_string(),
                        content_sha256: content_sha256.to_string(),
                        status,
                        reason,
                        first_seen: now,
                        last_seen: now,
                        count: 1,
                    };
                    RecordOutcome::Recorded
                }
                None if full => {
                    tracing::warn!(
                        path = %path,
                        cap = MAX_ENTRIES,
                        "rejected_ledger: FULL - not recording this path (it retries at the old cadence); nothing is evicted"
                    );
                    RecordOutcome::CapReached
                }
                None => {
                    g.insert(
                        path.to_string(),
                        RejectedEntry {
                            path: path.to_string(),
                            content_sha256: content_sha256.to_string(),
                            status,
                            reason,
                            first_seen: now,
                            last_seen: now,
                            count: 1,
                        },
                    );
                    RecordOutcome::Recorded
                }
            }
        };
        if outcome == RecordOutcome::Recorded {
            self.persist()?;
        }
        Ok(outcome)
    }

    /// Clear `path` (the server accepted a push for it). Returns whether a
    /// row was removed; persists only then (best-effort, WARN on failure).
    pub fn clear(&self, path: &str) -> bool {
        let removed = {
            let mut g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
            g.remove(path).is_some()
        };
        if removed {
            if let Err(e) = self.persist() {
                tracing::warn!(path = %path, error = %e, "rejected_ledger: persist after clear failed (non-fatal)");
            }
        }
        removed
    }

    /// The row for `path`, if any.
    pub fn get(&self, path: &str) -> Option<RejectedEntry> {
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(path)
            .cloned()
    }

    /// True iff reconciliation must NOT queue `path` right now: it is
    /// ledgered with exactly `content_sha256` AND fewer than
    /// [`RETRY_AFTER_SECS`] have passed since `last_seen`. A different sha
    /// (the bytes changed) or an expired window => not suppressed (one more
    /// attempt; on rejection the window restarts).
    pub fn should_suppress(&self, path: &str, content_sha256: &str) -> bool {
        let now = self.now();
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(path)
            .is_some_and(|e| {
                e.content_sha256 == content_sha256
                    && now < e.last_seen.saturating_add(RETRY_AFTER_SECS)
            })
    }

    /// Number of ledgered (distinct) paths.
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap_or_else(|p| p.into_inner()).len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Snapshot of every row, sorted by path (for the status surface).
    pub fn entries(&self) -> Vec<RejectedEntry> {
        let mut v: Vec<RejectedEntry> = self
            .inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .values()
            .cloned()
            .collect();
        v.sort_by(|a, b| a.path.cmp(&b.path));
        v
    }

    /// Durable persist (atomic replace). In-memory ledgers return `Ok`.
    fn persist(&self) -> std::io::Result<()> {
        let Some(sd) = &self.sd else {
            return Ok(());
        };
        let snapshot = LedgerFile {
            daemon_version: self.daemon_version.clone(),
            entries: self.inner.lock().unwrap_or_else(|p| p.into_inner()).clone(),
        };
        let bytes = serde_json::to_vec_pretty(&snapshot)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        let _commit = sd
            .replace_atomic(StateFile::RejectedLedger, &bytes)
            .map_err(|n| n.0)?;
        Ok(())
    }
}

enum ReadOutcome {
    Absent,
    Corrupt,
    Loaded {
        version: String,
        entries: HashMap<String, RejectedEntry>,
    },
}

#[cfg(test)]
#[allow(clippy::disallowed_methods)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use tempfile::tempdir;

    fn cfg_sd(d: &std::path::Path) -> Arc<StateDir> {
        StateDir::for_test_scoped(d, crate::state_io::Scope::ConfigDir)
    }

    fn test_clock(t: &Arc<AtomicU64>) -> Clock {
        let t = Arc::clone(t);
        Arc::new(move || t.load(Ordering::SeqCst))
    }

    const T0: u64 = 1_700_000_000;

    #[test]
    fn record_get_suppress_clear_roundtrip() {
        let dir = tempdir().unwrap();
        let l = RejectedLedger::load(&cfg_sd(dir.path()));
        assert!(l.is_empty());

        assert_eq!(
            l.record("a.md", "sha-a", 400, "invalid path").unwrap(),
            RecordOutcome::Recorded
        );
        l.record("b.canvas", "sha-b", 422, "content contains NUL bytes")
            .unwrap();
        assert_eq!(l.len(), 2);
        let a = l.get("a.md").unwrap();
        assert_eq!(a.content_sha256, "sha-a");
        assert_eq!(a.status, 400);
        assert_eq!(a.reason, "invalid path");
        assert_eq!(a.count, 1);

        assert!(l.should_suppress("a.md", "sha-a"));
        assert!(
            !l.should_suppress("a.md", "sha-a2"),
            "different bytes = retry now"
        );
        assert!(
            !l.should_suppress("c.md", "sha-a"),
            "unknown path is not suppressed"
        );

        assert!(l.clear("a.md"));
        assert!(!l.clear("a.md"), "second clear removes nothing");
        assert_eq!(l.len(), 1);
        assert_eq!(l.entries()[0].path, "b.canvas");
    }

    /// AR-007 (a): unchanged bytes rejected at T0 are suppressed at T0+1h and
    /// retried once at T0+25h; a re-rejection restarts the window.
    #[test]
    fn suppression_is_temporal_24h_then_one_retry() {
        let t = Arc::new(AtomicU64::new(T0));
        let l = RejectedLedger::in_memory_with_clock(test_clock(&t));
        l.record("x.md", "s1", 400, "r").unwrap();
        assert!(l.should_suppress("x.md", "s1"), "suppressed at T0");
        t.store(T0 + 3600, Ordering::SeqCst);
        assert!(l.should_suppress("x.md", "s1"), "still suppressed at T0+1h");
        t.store(T0 + RETRY_AFTER_SECS - 1, Ordering::SeqCst);
        assert!(
            l.should_suppress("x.md", "s1"),
            "suppressed until the window ends"
        );
        t.store(T0 + 25 * 3600, Ordering::SeqCst);
        assert!(!l.should_suppress("x.md", "s1"), "retried once at T0+25h");
        // The retry is rejected again: last_seen/count update, window restarts.
        l.record("x.md", "s1", 400, "r").unwrap();
        let e = l.get("x.md").unwrap();
        assert_eq!(e.count, 2);
        assert_eq!(e.first_seen, T0);
        assert_eq!(e.last_seen, T0 + 25 * 3600);
        t.store(T0 + 26 * 3600, Ordering::SeqCst);
        assert!(
            l.should_suppress("x.md", "s1"),
            "suppressed again after the re-rejection"
        );
        // Changed bytes retry immediately regardless of the window.
        assert!(!l.should_suppress("x.md", "s2"));
    }

    #[test]
    fn same_sha_increments_count_new_sha_replaces_row() {
        let t = Arc::new(AtomicU64::new(10));
        let l = RejectedLedger::in_memory_with_clock(test_clock(&t));
        l.record("x.md", "s1", 400, "r1").unwrap();
        t.store(20, Ordering::SeqCst);
        l.record("x.md", "s1", 400, "r2").unwrap();
        let e = l.get("x.md").unwrap();
        assert_eq!(e.count, 2);
        assert_eq!(e.first_seen, 10, "first_seen is stable for the same bytes");
        assert_eq!(e.last_seen, 20);
        assert_eq!(e.reason, "r2");

        t.store(30, Ordering::SeqCst);
        l.record("x.md", "s2", 422, "r3").unwrap();
        let e = l.get("x.md").unwrap();
        assert_eq!(e.content_sha256, "s2");
        assert_eq!(e.count, 1, "new bytes start a new row");
        assert_eq!(e.first_seen, 30);
        assert_eq!(e.status, 422);
        assert_eq!(l.len(), 1, "one row per path");
    }

    #[test]
    fn state_is_durable_across_reload() {
        let dir = tempdir().unwrap();
        {
            let l = RejectedLedger::load(&cfg_sd(dir.path()));
            l.record("stuck.md", "sha-s", 400, "invalid path").unwrap();
        }
        let l2 = RejectedLedger::load(&cfg_sd(dir.path()));
        assert_eq!(l2.len(), 1);
        assert!(l2.should_suppress("stuck.md", "sha-s"));
        assert_eq!(l2.get("stuck.md").unwrap().reason, "invalid path");
    }

    /// (b): a file written by a different daemon version loads EMPTY (every
    /// path is retried once by the new binary).
    #[test]
    fn version_change_on_load_clears_entries() {
        let dir = tempdir().unwrap();
        let sd = cfg_sd(dir.path());
        {
            let l = RejectedLedger::load_for_version(&sd, "0.5.1", system_clock());
            l.record("a.md", "s", 400, "r").unwrap();
            l.record("b.md", "s", 400, "r").unwrap();
        }
        let same = RejectedLedger::load_for_version(&sd, "0.5.1", system_clock());
        assert_eq!(same.len(), 2, "same version keeps the entries");
        let upgraded = RejectedLedger::load_for_version(&sd, "0.5.2", system_clock());
        assert!(upgraded.is_empty(), "a version change drops every entry");
        // A pre-version file (no daemon_version field) is also a change.
        std::fs::write(
            dir.path().join("rejected-ledger.json"),
            br#"{"entries":{"z.md":{"path":"z.md","content_sha256":"s","status":400,"reason":"r","first_seen":1,"last_seen":1,"count":1}}}"#,
        )
        .unwrap();
        let legacy = RejectedLedger::load_for_version(&sd, "0.5.2", system_clock());
        assert!(legacy.is_empty());
    }

    /// (c): a corrupt file loads as empty, never a crash.
    #[test]
    fn corrupt_file_loads_as_empty_not_error() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("rejected-ledger.json"), b"{not json").unwrap();
        let l = RejectedLedger::load(&cfg_sd(dir.path()));
        assert!(l.is_empty());
        // And it can be written over.
        l.record("a.md", "s", 400, "r").unwrap();
        assert_eq!(RejectedLedger::load(&cfg_sd(dir.path())).len(), 1);
    }

    /// (e): at the cap a NEW path is not recorded and nothing is evicted;
    /// an existing path still updates.
    #[test]
    fn cap_reached_records_nothing_new_and_evicts_nothing() {
        let l = RejectedLedger::in_memory();
        for i in 0..MAX_ENTRIES {
            assert_eq!(
                l.record(&format!("p{i:04}.md"), "s", 400, "r").unwrap(),
                RecordOutcome::Recorded
            );
        }
        assert_eq!(l.len(), MAX_ENTRIES);
        assert_eq!(
            l.record("newest.md", "s", 400, "r").unwrap(),
            RecordOutcome::CapReached
        );
        assert_eq!(l.len(), MAX_ENTRIES, "cap holds");
        assert!(l.get("newest.md").is_none(), "the new path is NOT recorded");
        assert!(l.get("p0000.md").is_some(), "nothing is evicted");
        assert!(
            !l.should_suppress("newest.md", "s"),
            "it retries at the old cadence"
        );
        // An already-ledgered path still updates at the cap.
        assert_eq!(
            l.record("p0000.md", "s", 422, "r2").unwrap(),
            RecordOutcome::Recorded
        );
        assert_eq!(l.get("p0000.md").unwrap().count, 2);
        // Clearing one frees one slot.
        assert!(l.clear("p0000.md"));
        assert_eq!(
            l.record("newest.md", "s", 400, "r").unwrap(),
            RecordOutcome::Recorded
        );
    }

    /// (d): a persistence failure is surfaced as `Err` (the caller logs ERROR
    /// and still acks); the row is still in memory.
    #[cfg(unix)]
    #[test]
    fn persist_failure_is_err_and_row_stays_in_memory() {
        let dir = tempdir().unwrap();
        let sd = cfg_sd(dir.path());
        let l = RejectedLedger::load(&sd);
        l.record("ok.md", "s", 400, "r").unwrap();
        // Make the dir unwritable: the atomic replace cannot create its temp.
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o500)).unwrap();
        let r = l.record("fail.md", "s", 400, "r");
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        assert!(r.is_err(), "persist failure must surface, got {r:?}");
        assert!(l.get("fail.md").is_some(), "the row is still in memory");
    }

    #[test]
    fn reason_is_capped() {
        let l = RejectedLedger::in_memory();
        l.record("big.md", "s", 400, &"x".repeat(5000)).unwrap();
        assert_eq!(l.get("big.md").unwrap().reason.chars().count(), REASON_CAP);
    }
}
