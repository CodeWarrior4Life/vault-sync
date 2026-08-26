//! Suppressed-delete ledger (P0 2026-08-26, second pass).
//!
//! ## Why this exists
//!
//! When the [`crate::redflag::DeleteBurstDetector`] latches, every further
//! delete is dropped at classify. Pre-ledger those paths were forgotten
//! entirely, which produced the worst outcome measured in this incident:
//!
//! 1. 2026-08-25 19:38 — 295 oversized notes were deleted on link.
//! 2. `2026-08-25T23:37:35` — exactly **20** delete pushes reached the server
//!    (the 20/30s threshold); the valve latched on the 20th and the remaining
//!    **275** were discarded silently.
//! 3. The server therefore still held all 275 rows.
//! 4. `2026-08-26T00:05:37` — `pull_backfill: created=275`. The hourly
//!    completeness pass enumerated the server, saw 275 paths "missing locally",
//!    and faithfully re-created every one of them, byte-identical, with the
//!    server's mtimes.
//!
//! So a suppressed delete was not merely lost — **it was actively UNDONE within
//! the hour by the very backstop designed to guarantee completeness.** No
//! recency rule could have arbitrated it: that is a restore-from-canonical, not
//! a conflict resolution. And because the deletion of those notes was itself a
//! crash fix (oversized notes fatally OOM Obsidian's metadata worker), the
//! resurrection silently reverted a safety change.
//!
//! ## What the ledger does
//!
//! It records the paths whose delete the valve suppressed, so that:
//!
//! * [`crate::pull_backfill`] REFUSES to re-create a path with a pending local
//!   delete intent — closing the resurrection loop; and
//! * on resume the suppressed deletes can be REPLAYED, so an intentional bulk
//!   delete actually completes instead of stalling at 20 notes forever.
//!
//! ## Safety
//!
//! The ledger only ever *defers* work; it never deletes anything itself. Replay
//! goes through the normal push path, so each replayed delete still carries a
//! CAS base from the shadow store: if the server was edited since suppression
//! the push 409s and edit-beats-delete, exactly as for a live delete. A stale
//! ledger entry can therefore cost a redundant delete attempt, never a silent
//! wipe of newer content.
//!
//! Format is one relative path per line (paths cannot contain `\n`), appended
//! atomically enough for our purposes: a torn final line is dropped on read
//! rather than corrupting the set.

use std::collections::BTreeSet;
use std::io::Write;
use std::path::{Path, PathBuf};

/// Hard cap on ledger size so a runaway suppression cannot grow a file without
/// bound. Well above any plausible intentional bulk delete (the incident that
/// motivated this was 295 paths).
pub const LEDGER_MAX_PATHS: usize = 100_000;

/// Append-only record of deletes the burst valve suppressed.
#[derive(Debug, Clone)]
pub struct DeleteLedger {
    path: PathBuf,
}

impl DeleteLedger {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn file_path(&self) -> &Path {
        &self.path
    }

    /// Record one suppressed delete. Best-effort: a write failure is logged by
    /// the caller's context and must never block the valve from suppressing.
    /// Returns true iff the line was written.
    pub fn record(&self, rel: &str) -> bool {
        if rel.is_empty() || rel.contains('\n') {
            return false;
        }
        if let Some(parent) = self.path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        // Cheap bound check: only pay the read when the file is already large.
        if let Ok(md) = std::fs::metadata(&self.path) {
            // ~4 KiB average path budget is generous; this is a runaway guard,
            // not an exact count.
            if md.len() > (LEDGER_MAX_PATHS as u64) * 512 {
                return false;
            }
        }
        match std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
        {
            Ok(mut f) => writeln!(f, "{rel}").is_ok(),
            Err(_) => false,
        }
    }

    /// The distinct suppressed paths. Order-independent and de-duplicated, so
    /// repeated suppression of one path replays once.
    pub fn paths(&self) -> BTreeSet<String> {
        let Ok(body) = std::fs::read_to_string(&self.path) else {
            return BTreeSet::new();
        };
        parse_ledger(&body)
    }

    pub fn is_empty(&self) -> bool {
        self.paths().is_empty()
    }

    /// Drop the ledger entirely (after a successful replay).
    pub fn clear(&self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Pure parse so the dedup/torn-line behaviour is testable without a
/// filesystem.
pub fn parse_ledger(body: &str) -> BTreeSet<String> {
    body.lines()
        .map(|l| l.trim_end_matches('\r'))
        .filter(|l| !l.trim().is_empty())
        .map(|l| l.to_string())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn ledger(dir: &TempDir) -> DeleteLedger {
        DeleteLedger::new(dir.path().join("sync-state").join("suppressed.txt"))
    }

    #[test]
    fn records_and_dedups() {
        let d = TempDir::new().unwrap();
        let l = ledger(&d);
        assert!(l.is_empty());
        assert!(l.record("a/b.md"));
        assert!(l.record("a/b.md")); // duplicate suppression of one path
        assert!(l.record("c.md"));
        let p = l.paths();
        assert_eq!(
            p.len(),
            2,
            "duplicates must collapse so replay happens once"
        );
        assert!(p.contains("a/b.md") && p.contains("c.md"));
    }

    #[test]
    fn clear_empties_it() {
        let d = TempDir::new().unwrap();
        let l = ledger(&d);
        l.record("x.md");
        assert!(!l.is_empty());
        l.clear();
        assert!(l.is_empty());
    }

    #[test]
    fn rejects_newline_and_empty_paths() {
        let d = TempDir::new().unwrap();
        let l = ledger(&d);
        assert!(!l.record(""), "empty path must not be recorded");
        assert!(
            !l.record("bad\npath.md"),
            "an embedded newline would corrupt the line format"
        );
        assert!(l.is_empty());
    }

    #[test]
    fn parse_drops_blank_and_torn_lines() {
        // A torn trailing write must not corrupt the set; blank lines ignored.
        let s = parse_ledger("a.md\n\n  \nb.md\r\nc.md");
        assert_eq!(s.len(), 3);
        assert!(s.contains("a.md") && s.contains("b.md") && s.contains("c.md"));
    }

    #[test]
    fn missing_file_reads_as_empty_not_error() {
        let d = TempDir::new().unwrap();
        let l = DeleteLedger::new(d.path().join("nope").join("absent.txt"));
        assert!(l.paths().is_empty());
        assert!(l.is_empty());
    }
}
