//! AR-003 (TKT-c41c2225): durable retry ledger for failed/deferred pulls.
//!
//! The reconcile backstop is idempotent: a failed server-wins pull is naturally
//! re-attempted on the next full scan. But before this module the daemon kept NO
//! explicit record that a specific item was stuck in a failed-retry state -- a
//! failure was WARN-logged and then rounded out of the summary (the AR-003 false
//! green). This ledger gives failed/deferred items *durable* retry state that
//! survives a daemon restart, so:
//!
//!   * the failure is observable (count + age + attempt tally + last error),
//!   * a stuck item cannot be silently forgotten, and
//!   * the P2-E3 soak gate has a persistent "still-divergent" signal to read.
//!
//! Storage is a single JSON file (a small map keyed by note path). Writes are
//! best-effort: the ledger is an OBSERVABILITY aid layered on top of the
//! already-idempotent rescan, so a persistence error is logged, never fatal --
//! losing the ledger never loses data, it only loses history.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::state_io::{StateDir, StateFile};

/// One stuck item's durable retry state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryEntry {
    pub path: String,
    /// Bounded last-error string (no note content; the pull errors are
    /// structural: fetch/decode/materialize/integrity).
    pub last_error: String,
    /// Unix seconds of the first observed failure for this path.
    pub first_failed_ts: u64,
    /// Unix seconds of the most recent failed attempt.
    pub last_attempt_ts: u64,
    /// Number of failed/deferred attempts recorded.
    pub attempts: u32,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct LedgerFile {
    entries: HashMap<String, RetryEntry>,
}

/// Thread-safe durable retry ledger persisted to a JSON file.
#[derive(Debug)]
pub struct RetryLedger {
    /// CS-2 (W13): the config-scope state dir; `None` = in-memory only.
    sd: Option<Arc<StateDir>>,
    inner: Mutex<HashMap<String, RetryEntry>>,
}

/// Cap on a stored error string so a pathological upstream message can't bloat
/// the ledger. Errors are structural, so this is generous.
const ERROR_CAP: usize = 512;

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}...", &s[..end])
}

impl RetryLedger {
    /// Load the ledger from `path`. A missing or corrupt file yields an EMPTY
    /// ledger (never an error): the ledger is observability, and a fresh empty
    /// one is always safe.
    pub fn load(sd: &Arc<StateDir>) -> Self {
        let inner = Self::read_file(sd).unwrap_or_default();
        Self {
            sd: Some(Arc::clone(sd)),
            inner: Mutex::new(inner),
        }
    }

    /// A ledger that persists nothing (the config dir is not bootstrapped).
    pub fn in_memory() -> Self {
        Self {
            sd: None,
            inner: Mutex::new(HashMap::new()),
        }
    }

    fn read_file(sd: &StateDir) -> Option<HashMap<String, RetryEntry>> {
        let bytes = sd.read(StateFile::ReconcileRetryLedger).ok()??;
        let parsed: LedgerFile = serde_json::from_slice(&bytes).ok()?;
        Some(parsed.entries)
    }

    /// Record a failed/deferred attempt for `path`, upserting its entry
    /// (incrementing the attempt count, preserving `first_failed_ts`). Persists
    /// best-effort.
    pub fn record_failure(&self, path: &str, error: &str) {
        {
            let mut g = self.inner.lock().unwrap();
            let now = now_secs();
            let e = g.entry(path.to_string()).or_insert_with(|| RetryEntry {
                path: path.to_string(),
                last_error: String::new(),
                first_failed_ts: now,
                last_attempt_ts: now,
                attempts: 0,
            });
            e.last_error = truncate(error, ERROR_CAP);
            e.last_attempt_ts = now;
            e.attempts = e.attempts.saturating_add(1);
        }
        self.persist();
    }

    /// Clear `path` from the ledger (the pull finally succeeded). Persists only
    /// if something was actually removed.
    pub fn clear(&self, path: &str) {
        let removed = {
            let mut g = self.inner.lock().unwrap();
            g.remove(path).is_some()
        };
        if removed {
            self.persist();
        }
    }

    /// Number of items currently in a failed/deferred retry state.
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Snapshot of all pending retry entries (for the log/tray/status surface).
    pub fn pending(&self) -> Vec<RetryEntry> {
        self.inner.lock().unwrap().values().cloned().collect()
    }

    /// Best-effort atomic persist (tmp write + rename). A failure is logged, not
    /// propagated: the ledger is layered on the idempotent rescan.
    fn persist(&self) {
        let snapshot = LedgerFile {
            entries: self.inner.lock().unwrap().clone(),
        };
        if let Err(e) = self.write_file(&snapshot) {
            tracing::warn!(file = StateFile::ReconcileRetryLedger.name(), error = %e, "retry_ledger: persist failed (non-fatal; rescan still retries)");
        }
    }

    /// W13 (CS-2): config-scope `replace_atomic` (CS-1 unique temp, 0600,
    /// `sync_all`, `renameat`, dir sync) instead of create_dir_all + a fixed
    /// `*.json.tmp` + rename.
    fn write_file(&self, snapshot: &LedgerFile) -> std::io::Result<()> {
        let Some(sd) = &self.sd else {
            return Ok(());
        };
        let bytes = serde_json::to_vec_pretty(snapshot)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        let _commit = sd
            .replace_atomic(StateFile::ReconcileRetryLedger, &bytes)
            .map_err(|n| n.0)?;
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::disallowed_methods)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn cfg_sd(d: &std::path::Path) -> Arc<StateDir> {
        StateDir::for_test_scoped(d, crate::state_io::Scope::ConfigDir)
    }

    #[test]
    fn record_and_clear_roundtrip() {
        let dir = tempdir().unwrap();
        let l = RetryLedger::load(&cfg_sd(dir.path()));
        assert!(l.is_empty());

        l.record_failure("a.md", "fetch: boom");
        l.record_failure("b.md", "materialize: bad");
        assert_eq!(l.len(), 2);

        l.clear("a.md");
        assert_eq!(l.len(), 1);
        assert_eq!(l.pending()[0].path, "b.md");
    }

    #[test]
    fn repeated_failures_increment_attempts_preserve_first_ts() {
        let dir = tempdir().unwrap();
        let l = RetryLedger::load(&cfg_sd(dir.path()));
        l.record_failure("x.md", "err1");
        let first = l.pending()[0].first_failed_ts;
        l.record_failure("x.md", "err2");
        let e = l.pending().into_iter().next().unwrap();
        assert_eq!(e.attempts, 2);
        assert_eq!(e.first_failed_ts, first, "first_failed_ts must be stable");
        assert_eq!(e.last_error, "err2");
    }

    #[test]
    fn state_is_durable_across_reload() {
        // The durability guarantee: a failed item survives a daemon restart.
        let dir = tempdir().unwrap();
        {
            let l = RetryLedger::load(&cfg_sd(dir.path()));
            l.record_failure("stuck.md", "fetch: decode error");
        }
        // Fresh instance == simulated daemon restart.
        let l2 = RetryLedger::load(&cfg_sd(dir.path()));
        assert_eq!(l2.len(), 1, "failed item must persist across reload");
        assert_eq!(l2.pending()[0].path, "stuck.md");
    }

    #[test]
    fn corrupt_file_loads_as_empty_not_error() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("reconcile-retry-ledger.json");
        std::fs::write(&p, b"{not valid json").unwrap();
        let l = RetryLedger::load(&cfg_sd(dir.path()));
        assert!(l.is_empty(), "corrupt ledger must load empty, never panic");
    }

    #[test]
    fn error_string_is_capped() {
        let dir = tempdir().unwrap();
        let l = RetryLedger::load(&cfg_sd(dir.path()));
        l.record_failure("big.md", &"x".repeat(5000));
        assert!(l.pending()[0].last_error.len() <= ERROR_CAP + 3);
    }
}
