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
}

impl EchoGuard {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
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
