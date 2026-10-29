//! WS-A A3 (TKT-17fc22e5 / TKT-8fc0b2a4): durable ledger of permanently
//! rejected pushes, keyed by path and bound to the EXACT bytes that were
//! refused.
//!
//! ## Why
//!
//! A push the server rejects with 400/422 (or the daemon refuses locally under
//! the I3 contract) is skipped + acked so the journal never wedges. But the
//! reconciliation backstop decides push-vs-pull from hashes alone, so on its
//! next pass it re-queued the same bytes, which were rejected again, forever:
//! TKT-17fc22e5 measured 1:1 queued-to-rejected across 16 days (215/day on
//! Trinity for 3 paths). This ledger closes that loop: reconciliation consults
//! it before queueing and does NOT queue a path whose current content sha
//! equals the sha that was already rejected. The moment the bytes change the
//! sha differs and the path gets exactly one more chance.
//!
//! ## Shape
//!
//! `path -> { content_sha256, status, reason, first_seen, last_seen, count }`,
//! `status` = the HTTP status (400/422) or `0` for a local I3 refusal. Capped
//! at [`MAX_ENTRIES`] rows (the oldest `last_seen` is evicted). Persisted as
//! `rejected-ledger.json` in the config-scope state dir beside
//! `reconcile-retry-ledger.json`, with the same atomic-replace discipline
//! (`StateDir::replace_atomic`). Modelled on [`crate::retry_ledger`]: a
//! persistence failure is logged, never fatal; losing the ledger only loses
//! the suppression (one extra rejected push per path), never data.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::state_io::{StateDir, StateFile};

/// Row cap. Eviction is by oldest `last_seen` (ties broken by path) so the
/// ledger is an LRU of rejected paths, never unbounded.
pub const MAX_ENTRIES: usize = 1000;

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
    /// Unix seconds of the most recent rejection.
    pub last_seen: u64,
    /// Rejections recorded for these bytes (resets when the sha changes).
    pub count: u32,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct LedgerFile {
    entries: HashMap<String, RejectedEntry>,
}

/// Thread-safe durable rejected ledger persisted to a JSON file.
#[derive(Debug)]
pub struct RejectedLedger {
    /// The config-scope state dir; `None` = in-memory only.
    sd: Option<Arc<StateDir>>,
    inner: Mutex<HashMap<String, RejectedEntry>>,
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn cap_chars(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        s.chars().take(max).collect()
    }
}

impl RejectedLedger {
    /// Load from the config-scope state dir. A missing or corrupt file yields
    /// an EMPTY ledger (never an error): the worst case is one extra rejected
    /// push per path, which the server refuses again and re-records.
    pub fn load(sd: &Arc<StateDir>) -> Self {
        let inner = Self::read_file(sd).unwrap_or_default();
        Self {
            sd: Some(Arc::clone(sd)),
            inner: Mutex::new(inner),
        }
    }

    /// A ledger that persists nothing (config dir not bootstrapped; tests).
    pub fn in_memory() -> Self {
        Self {
            sd: None,
            inner: Mutex::new(HashMap::new()),
        }
    }

    fn read_file(sd: &StateDir) -> Option<HashMap<String, RejectedEntry>> {
        let bytes = sd.read(StateFile::RejectedLedger).ok()??;
        let parsed: LedgerFile = serde_json::from_slice(&bytes).ok()?;
        Some(parsed.entries)
    }

    /// Record that `content_sha256` for `path` was rejected (`status`,
    /// `reason`). Same sha as the existing row: `count += 1`, `last_seen`
    /// refreshed, reason updated. Different sha: the row is REPLACED (new
    /// bytes, new first_seen, count 1). Persists best-effort.
    pub fn record(&self, path: &str, content_sha256: &str, status: u16, reason: &str) {
        self.record_at(path, content_sha256, status, reason, now_secs());
        self.persist();
    }

    /// Clock-injectable core of [`Self::record`] (no persist).
    fn record_at(&self, path: &str, content_sha256: &str, status: u16, reason: &str, now: u64) {
        let mut g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let reason = cap_chars(reason, REASON_CAP);
        match g.get_mut(path) {
            Some(e) if e.content_sha256 == content_sha256 => {
                e.status = status;
                e.reason = reason;
                e.last_seen = now;
                e.count = e.count.saturating_add(1);
            }
            _ => {
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
            }
        }
        while g.len() > MAX_ENTRIES {
            let victim = g
                .values()
                .min_by(|a, b| {
                    a.last_seen
                        .cmp(&b.last_seen)
                        .then_with(|| a.path.cmp(&b.path))
                })
                .map(|e| e.path.clone());
            match victim {
                Some(p) => {
                    g.remove(&p);
                }
                None => break,
            }
        }
    }

    /// Clear `path` (the server accepted a push for it). Returns whether a
    /// row was removed; persists only then.
    pub fn clear(&self, path: &str) -> bool {
        let removed = {
            let mut g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
            g.remove(path).is_some()
        };
        if removed {
            self.persist();
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

    /// True iff `path` is ledgered AND its ledgered sha equals
    /// `content_sha256`: these exact bytes were already rejected, so queueing
    /// them again cannot succeed. A different sha (the bytes changed) is NOT
    /// rejected — the path gets one more chance.
    pub fn is_rejected(&self, path: &str, content_sha256: &str) -> bool {
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(path)
            .is_some_and(|e| e.content_sha256 == content_sha256)
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

    /// Best-effort atomic persist. A failure is logged, not propagated.
    fn persist(&self) {
        let snapshot = LedgerFile {
            entries: self.inner.lock().unwrap_or_else(|p| p.into_inner()).clone(),
        };
        if let Err(e) = self.write_file(&snapshot) {
            tracing::warn!(file = StateFile::RejectedLedger.name(), error = %e, "rejected_ledger: persist failed (non-fatal; the server re-records on the next rejection)");
        }
    }

    /// Config-scope `replace_atomic` (unique temp, 0600, `sync_all`, rename,
    /// dir sync) — the same discipline as every other state file.
    fn write_file(&self, snapshot: &LedgerFile) -> std::io::Result<()> {
        let Some(sd) = &self.sd else {
            return Ok(());
        };
        let bytes = serde_json::to_vec_pretty(snapshot)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        let _commit = sd
            .replace_atomic(StateFile::RejectedLedger, &bytes)
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
    fn record_get_is_rejected_clear_roundtrip() {
        let dir = tempdir().unwrap();
        let l = RejectedLedger::load(&cfg_sd(dir.path()));
        assert!(l.is_empty());

        l.record("a.md", "sha-a", 400, "invalid path");
        l.record("b.canvas", "sha-b", 422, "content contains NUL bytes");
        assert_eq!(l.len(), 2);
        let a = l.get("a.md").unwrap();
        assert_eq!(a.content_sha256, "sha-a");
        assert_eq!(a.status, 400);
        assert_eq!(a.reason, "invalid path");
        assert_eq!(a.count, 1);

        assert!(l.is_rejected("a.md", "sha-a"));
        assert!(
            !l.is_rejected("a.md", "sha-a2"),
            "different bytes = one more chance"
        );
        assert!(
            !l.is_rejected("c.md", "sha-a"),
            "unknown path is not rejected"
        );

        assert!(l.clear("a.md"));
        assert!(!l.clear("a.md"), "second clear removes nothing");
        assert_eq!(l.len(), 1);
        assert_eq!(l.entries()[0].path, "b.canvas");
    }

    #[test]
    fn same_sha_increments_count_new_sha_replaces_row() {
        let l = RejectedLedger::in_memory();
        l.record_at("x.md", "s1", 400, "r1", 10);
        l.record_at("x.md", "s1", 400, "r2", 20);
        let e = l.get("x.md").unwrap();
        assert_eq!(e.count, 2);
        assert_eq!(e.first_seen, 10, "first_seen is stable for the same bytes");
        assert_eq!(e.last_seen, 20);
        assert_eq!(e.reason, "r2");

        l.record_at("x.md", "s2", 422, "r3", 30);
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
            l.record("stuck.md", "sha-s", 400, "invalid path");
        }
        let l2 = RejectedLedger::load(&cfg_sd(dir.path()));
        assert_eq!(l2.len(), 1);
        assert!(l2.is_rejected("stuck.md", "sha-s"));
        assert_eq!(l2.get("stuck.md").unwrap().reason, "invalid path");
    }

    #[test]
    fn corrupt_file_loads_as_empty_not_error() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("rejected-ledger.json"), b"{not json").unwrap();
        let l = RejectedLedger::load(&cfg_sd(dir.path()));
        assert!(l.is_empty());
    }

    #[test]
    fn capped_at_max_entries_evicting_oldest_last_seen() {
        let l = RejectedLedger::in_memory();
        for i in 0..MAX_ENTRIES {
            // last_seen grows with i, so entry 0 is the oldest.
            l.record_at(&format!("p{i:04}.md"), "s", 400, "r", 1000 + i as u64);
        }
        assert_eq!(l.len(), MAX_ENTRIES);
        l.record_at("newest.md", "s", 400, "r", 5000);
        assert_eq!(l.len(), MAX_ENTRIES, "cap holds");
        assert!(
            l.get("p0000.md").is_none(),
            "the oldest last_seen is evicted"
        );
        assert!(l.get("p0001.md").is_some());
        assert!(l.get("newest.md").is_some());
        // Refreshing an old row protects it: touch p0001, add another, p0002 goes.
        l.record_at("p0001.md", "s", 400, "r", 6000);
        l.record_at("newer2.md", "s", 400, "r", 6001);
        assert!(l.get("p0001.md").is_some());
        assert!(l.get("p0002.md").is_none());
    }

    #[test]
    fn reason_is_capped() {
        let l = RejectedLedger::in_memory();
        l.record("big.md", "s", 400, &"x".repeat(5000));
        assert_eq!(l.get("big.md").unwrap().reason.chars().count(), REASON_CAP);
    }
}
