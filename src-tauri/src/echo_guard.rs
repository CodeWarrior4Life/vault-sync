//! Echo guard — suppresses the sync feedback loop between the materializer and
//! the file_watcher.
//!
//! v0.3 vault-sync is event-driven: the SSE consumer materializes server pushes
//! by WRITING files into the vault. Those writes are real filesystem events, so
//! the file_watcher sees them and re-enqueues each one as a local Modify push —
//! a server change echoes straight back to the server. In steady state this is
//! the "3 idempotent pushes per write" nuisance flagged in S489; on a catchup
//! backlog it is a flood (S492 soak: ~28k files materialized, ~276k file_watcher
//! re-enqueues, journal pinned at its 100MB cap → the storm).
//!
//! This guard breaks the loop at the source. The materializer records the
//! content hash of every file it writes; the file_watcher consults the guard
//! before enqueueing and SKIPS an event whose current content hash matches a
//! recent materializer write — that event is a server echo, not a user edit.
//!
//! SAFE BY DESIGN (fail-open): suppression requires an EXACT (path, sha) match
//! within a short TTL. A genuine user edit changes the content → different sha →
//! never suppressed. If the guard is wrong or unwired it simply does not
//! suppress (the pre-existing behavior); it can NEVER drop a real local edit.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// How long a recorded materializer write stays eligible to suppress an echo.
/// Must comfortably exceed the file_watcher debounce + inotify latency; a few
/// seconds is enough, 15s is a safe margin. Old entries are pruned lazily.
const ECHO_TTL: Duration = Duration::from_secs(15);

/// Prune stale entries once the map grows past this many paths (bounds memory +
/// keeps prune cost O(n) only occasionally, never per-insert during a catchup).
const PRUNE_AT: usize = 4096;

/// Per-path record of the last materializer write: (content_sha, recorded_at).
#[derive(Default)]
pub struct EchoGuard {
    inner: Mutex<HashMap<String, (String, Instant)>>,
    /// TKT-c3605db8: paths the materializer itself just soft-deleted (inbound
    /// server deletes it materialized as `<name>.deleted-<ts>` renames). The
    /// file_watcher sees those renames as local Deleted events; without this
    /// registry it counts them toward the DeleteBurstDetector (the valve then
    /// self-trips on every fleet-wide delete wave reaching a peer) and
    /// re-pushes the delete to the server as a pointless echo. No sha — the
    /// file is gone; path + TTL identity is exact enough because a genuine
    /// user delete of the SAME path in the same instant is indistinguishable
    /// from (and converges identically to) the materialized one.
    deletes: Mutex<HashMap<String, Instant>>,
}

impl EchoGuard {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
            deletes: Mutex::new(HashMap::new()),
        }
    }

    /// Record that the materializer just wrote `path` with content hash `sha`.
    pub fn record(&self, path: &str, sha: &str) {
        if let Ok(mut m) = self.inner.lock() {
            let now = Instant::now();
            if m.len() > PRUNE_AT {
                m.retain(|_, (_, at)| now.duration_since(*at) < ECHO_TTL);
            }
            m.insert(path.to_string(), (sha.to_string(), now));
        }
    }

    /// TKT-c3605db8: record that the materializer is about to soft-delete
    /// `path` (an INBOUND server delete, not a user action). Called before the
    /// rename so the guard entry always precedes the filesystem event.
    pub fn record_delete(&self, path: &str) {
        if let Ok(mut m) = self.deletes.lock() {
            let now = Instant::now();
            if m.len() > PRUNE_AT {
                m.retain(|_, at| now.duration_since(*at) < ECHO_TTL);
            }
            m.insert(path.to_string(), now);
        }
    }

    /// TKT-c3605db8: undo a [`record_delete`](Self::record_delete) whose rename
    /// then FAILED — the file still exists, so a genuine user delete inside the
    /// TTL must not be suppressed.
    pub fn unrecord_delete(&self, path: &str) {
        if let Ok(mut m) = self.deletes.lock() {
            m.remove(path);
        }
    }

    /// True iff a file_watcher Deleted event for `path` matches a recent
    /// materializer soft-delete — i.e. it is the echo of an inbound server
    /// delete, not a user delete. Consumes the entry (a LATER user delete of a
    /// recreated file at the same path is never suppressed). Fail-open like
    /// [`is_echo`](Self::is_echo): unwired or expired means no suppression.
    pub fn is_delete_echo(&self, path: &str) -> bool {
        if let Ok(mut m) = self.deletes.lock() {
            if let Some(at) = m.get(path) {
                if Instant::now().duration_since(*at) < ECHO_TTL {
                    m.remove(path);
                    return true;
                }
                m.remove(path);
            }
        }
        false
    }

    /// True iff a file_watcher event for `path` at content hash `sha` matches a
    /// recent materializer write — i.e. it is a server echo, not a user edit.
    /// Consumes the matching entry so a LATER genuine edit of the same path is
    /// not suppressed.
    pub fn is_echo(&self, path: &str, sha: &str) -> bool {
        if let Ok(mut m) = self.inner.lock() {
            if let Some((recorded_sha, at)) = m.get(path) {
                if recorded_sha == sha && Instant::now().duration_since(*at) < ECHO_TTL {
                    m.remove(path);
                    return true;
                }
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matching_write_is_suppressed_once() {
        let g = EchoGuard::new();
        g.record("notes/a.md", "sha1");
        // First matching event = echo, suppressed.
        assert!(g.is_echo("notes/a.md", "sha1"));
        // Consumed: a second event (e.g. a later genuine edit back to the same
        // bytes) is NOT suppressed.
        assert!(!g.is_echo("notes/a.md", "sha1"));
    }

    #[test]
    fn different_hash_is_a_real_edit_not_suppressed() {
        let g = EchoGuard::new();
        g.record("notes/a.md", "server-sha");
        // User edited to different content → different sha → NEVER suppressed.
        assert!(!g.is_echo("notes/a.md", "user-edit-sha"));
    }

    #[test]
    fn unrecorded_path_is_not_suppressed() {
        let g = EchoGuard::new();
        assert!(!g.is_echo("notes/never-written.md", "sha"));
    }

    #[test]
    fn delete_echo_is_suppressed_once_then_consumed() {
        let g = EchoGuard::new();
        g.record_delete("notes/a.md");
        assert!(g.is_delete_echo("notes/a.md"));
        // Consumed: a later user delete of a recreated file is not suppressed.
        assert!(!g.is_delete_echo("notes/a.md"));
    }

    #[test]
    fn unrecorded_delete_is_a_user_delete() {
        let g = EchoGuard::new();
        assert!(!g.is_delete_echo("notes/user-deleted.md"));
    }

    #[test]
    fn unrecord_delete_after_failed_rename_restores_user_semantics() {
        let g = EchoGuard::new();
        g.record_delete("notes/a.md");
        g.unrecord_delete("notes/a.md");
        assert!(!g.is_delete_echo("notes/a.md"));
    }

    #[test]
    fn write_and_delete_registries_are_independent() {
        let g = EchoGuard::new();
        g.record("notes/a.md", "sha1");
        assert!(!g.is_delete_echo("notes/a.md"));
        g.record_delete("notes/b.md");
        assert!(!g.is_echo("notes/b.md", "sha1"));
        assert!(g.is_delete_echo("notes/b.md"));
        assert!(g.is_echo("notes/a.md", "sha1"));
    }

    #[test]
    fn record_prunes_when_over_cap_but_keeps_fresh() {
        let g = EchoGuard::new();
        for i in 0..(PRUNE_AT + 10) {
            g.record(&format!("p{i}.md"), "s");
        }
        // A fresh entry inserted after the prune threshold is still suppressible.
        g.record("recent.md", "rs");
        assert!(g.is_echo("recent.md", "rs"));
    }
}
