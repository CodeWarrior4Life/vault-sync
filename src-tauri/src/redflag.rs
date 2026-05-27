//! Operational safety valves to prevent catastrophic-delete propagation.
//!
//! Per the v0.3 Enterprise Bidirectional Mandate §3 ("Operational safety
//! valves") + §4.1. Inspired by obsidian-livesync's `redflag.md` circuit
//! breaker convention.
//!
//! Two valves live here:
//!
//! 1. [`RedflagGate`] — startup sentinel. If `<vault>/redflag.md` exists when
//!    the daemon starts, sync is aborted and a tray warning is surfaced until
//!    the file is removed. This gives the user a recovery window if a poisoned
//!    remote would otherwise wipe their vault.
//!
//! 2. [`DeleteBurstDetector`] — sliding-window threshold. If the daemon
//!    observes `threshold` or more delete events within `window` (default
//!    20 / 30s per §4.2), the delete-propagation channel is paused and the
//!    tray prompts the owner to confirm or cancel.
//!
//! Thread-safety: [`DeleteBurstDetector`] is NOT internally synchronized.
//! Wrap it in `Arc<Mutex<...>>` at the call site if multiple threads
//! record deletes concurrently. The filename `redflag.md` is matched
//! case-sensitively (livesync convention).

use std::collections::VecDeque;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime};

/// Sentinel filename, matched case-sensitively at the vault root.
const REDFLAG_FILENAME: &str = "redflag.md";

/// Startup gate. Cheap to construct; the filesystem read happens in
/// [`RedflagGate::check`].
#[derive(Debug, Clone)]
pub struct RedflagGate {
    vault_root: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RedflagStatus {
    Clear,
    Tripped { path: PathBuf, mtime: SystemTime },
}

impl RedflagGate {
    pub fn new(vault_root: impl Into<PathBuf>) -> Self {
        Self {
            vault_root: vault_root.into(),
        }
    }

    /// Inspect the filesystem for `<vault>/redflag.md`. Returns [`Tripped`]
    /// if present (any size, any contents); [`Clear`] otherwise. Called
    /// once at daemon startup and optionally periodically (every ~60s).
    pub fn check(&self) -> RedflagStatus {
        let path = self.vault_root.join(REDFLAG_FILENAME);
        // Case-sensitivity: on case-insensitive filesystems (NTFS default,
        // APFS default), a sibling `RedFlag.md` would also resolve. We
        // verify the on-disk filename matches exactly so the gate behaves
        // identically across platforms.
        let metadata = match std::fs::metadata(&path) {
            Ok(m) => m,
            Err(_) => return RedflagStatus::Clear,
        };
        if !metadata.is_file() {
            return RedflagStatus::Clear;
        }
        // Verify the directory entry's filename exactly equals
        // `redflag.md` — defense against case-insensitive FS resolution.
        if let Ok(entries) = std::fs::read_dir(&self.vault_root) {
            let mut found_exact = false;
            for entry in entries.flatten() {
                if entry.file_name().to_string_lossy() == REDFLAG_FILENAME {
                    found_exact = true;
                    break;
                }
            }
            if !found_exact {
                return RedflagStatus::Clear;
            }
        }
        let mtime = metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH);
        RedflagStatus::Tripped { path, mtime }
    }
}

/// State machine for the sliding-window delete-burst valve.
#[derive(Debug)]
pub struct DeleteBurstDetector {
    threshold: usize,
    window: Duration,
    events: VecDeque<Instant>,
    paused: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BurstStatus {
    /// Below threshold — caller may propagate the delete normally.
    BelowThreshold { current: usize, threshold: usize },
    /// Just crossed threshold this call — emit ONE tray prompt and pause.
    AtThreshold { window_start: Instant },
    /// Already paused — caller MUST suppress delete propagation until
    /// the owner clicks Confirm (which triggers [`DeleteBurstDetector::reset`])
    /// or Cancel.
    Paused,
}

impl DeleteBurstDetector {
    pub fn new(threshold: usize, window: Duration) -> Self {
        Self {
            threshold,
            window,
            events: VecDeque::new(),
            paused: false,
        }
    }

    /// Record a delete event at `Instant::now()` and return the resulting
    /// state. See [`BurstStatus`] for the transitions.
    pub fn record_delete(&mut self) -> BurstStatus {
        self.record_delete_at(Instant::now())
    }

    /// Internal hook for time-injection in tests. Public so integration
    /// callers can drive the detector deterministically if needed.
    pub fn record_delete_at(&mut self, now: Instant) -> BurstStatus {
        if self.paused {
            return BurstStatus::Paused;
        }
        // Window-zero edge case: a zero-duration window means we can never
        // accumulate two events at the "same" instant in a way that crosses
        // a non-existent window. Return BelowThreshold and don't even
        // record the event — single-event window has zero duration.
        if self.window.is_zero() {
            return BurstStatus::BelowThreshold {
                current: 0,
                threshold: self.threshold,
            };
        }
        // Trim events older than (now - window). Boundary: events at
        // exactly `now - window` are EXCLUDED (strictly newer than the
        // window edge are kept).
        let cutoff = now.checked_sub(self.window);
        if let Some(cutoff) = cutoff {
            while let Some(&front) = self.events.front() {
                if front <= cutoff {
                    self.events.pop_front();
                } else {
                    break;
                }
            }
        }
        self.events.push_back(now);

        // Threshold-zero edge case: any event trips immediately.
        if self.threshold == 0 {
            self.paused = true;
            return BurstStatus::AtThreshold { window_start: now };
        }

        if self.events.len() >= self.threshold {
            self.paused = true;
            let window_start = *self.events.front().unwrap_or(&now);
            BurstStatus::AtThreshold { window_start }
        } else {
            BurstStatus::BelowThreshold {
                current: self.events.len(),
                threshold: self.threshold,
            }
        }
    }

    /// Clear the deque and exit the paused state. Invoked when the owner
    /// confirms the tray prompt (Confirm → resume propagation).
    pub fn reset(&mut self) {
        self.events.clear();
        self.paused = false;
    }

    /// Is the detector currently in the paused state?
    pub fn is_paused(&self) -> bool {
        self.paused
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Write;
    use tempfile::TempDir;

    // ----- RedflagGate tests -----

    #[test]
    fn clear_when_no_file() {
        let dir = TempDir::new().unwrap();
        let gate = RedflagGate::new(dir.path());
        assert_eq!(gate.check(), RedflagStatus::Clear);
    }

    #[test]
    fn tripped_when_file_present() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("redflag.md");
        fs::write(&path, b"halt").unwrap();
        let gate = RedflagGate::new(dir.path());
        match gate.check() {
            RedflagStatus::Tripped { path: p, .. } => {
                assert_eq!(p, path);
            }
            other => panic!("expected Tripped, got {:?}", other),
        }
    }

    #[test]
    fn tripped_returns_mtime_correctly() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("redflag.md");
        let mut f = fs::File::create(&path).unwrap();
        f.write_all(b"x").unwrap();
        f.sync_all().unwrap();
        drop(f);
        let before = SystemTime::now();
        let gate = RedflagGate::new(dir.path());
        match gate.check() {
            RedflagStatus::Tripped { mtime, .. } => {
                // mtime should be near "now" (within 60s sanity envelope).
                let delta = before
                    .duration_since(mtime)
                    .or_else(|_| mtime.duration_since(before))
                    .unwrap_or(Duration::from_secs(0));
                assert!(
                    delta < Duration::from_secs(60),
                    "mtime delta too large: {:?}",
                    delta
                );
            }
            other => panic!("expected Tripped, got {:?}", other),
        }
    }

    #[test]
    fn case_sensitivity() {
        // `RedFlag.md` (mixed case) does NOT trip — we match `redflag.md`
        // exactly, per livesync convention.
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("RedFlag.md"), b"x").unwrap();
        let gate = RedflagGate::new(dir.path());
        assert_eq!(gate.check(), RedflagStatus::Clear);
    }

    #[test]
    fn directory_with_same_name_does_not_trip() {
        // If a directory is named `redflag.md` (perverse but possible),
        // the gate stays Clear — we require a regular file.
        let dir = TempDir::new().unwrap();
        fs::create_dir(dir.path().join("redflag.md")).unwrap();
        let gate = RedflagGate::new(dir.path());
        assert_eq!(gate.check(), RedflagStatus::Clear);
    }

    // ----- DeleteBurstDetector tests -----

    #[test]
    fn below_threshold_under_count() {
        let mut det = DeleteBurstDetector::new(20, Duration::from_secs(30));
        let now = Instant::now();
        let mut last = BurstStatus::BelowThreshold {
            current: 0,
            threshold: 20,
        };
        for i in 0..19 {
            last = det.record_delete_at(now + Duration::from_millis(i));
        }
        match last {
            BurstStatus::BelowThreshold { current, threshold } => {
                assert_eq!(current, 19);
                assert_eq!(threshold, 20);
            }
            other => panic!("expected BelowThreshold, got {:?}", other),
        }
    }

    #[test]
    fn at_threshold_at_exact_count() {
        let mut det = DeleteBurstDetector::new(20, Duration::from_secs(30));
        let now = Instant::now();
        let mut last = None;
        for i in 0..20 {
            last = Some(det.record_delete_at(now + Duration::from_millis(i)));
        }
        match last.unwrap() {
            BurstStatus::AtThreshold { .. } => {}
            other => panic!("expected AtThreshold, got {:?}", other),
        }
    }

    #[test]
    fn paused_after_threshold() {
        let mut det = DeleteBurstDetector::new(20, Duration::from_secs(30));
        let now = Instant::now();
        for i in 0..20 {
            det.record_delete_at(now + Duration::from_millis(i));
        }
        let twenty_first = det.record_delete_at(now + Duration::from_millis(20));
        assert_eq!(twenty_first, BurstStatus::Paused);
        assert!(det.is_paused());
    }

    #[test]
    fn sliding_window() {
        // 19 events at t=0, then 60s later add 1 → BelowThreshold
        // (old events outside the 30s window are expired).
        let mut det = DeleteBurstDetector::new(20, Duration::from_secs(30));
        let t0 = Instant::now();
        for i in 0..19 {
            det.record_delete_at(t0 + Duration::from_millis(i));
        }
        let later = t0 + Duration::from_secs(60);
        let result = det.record_delete_at(later);
        match result {
            BurstStatus::BelowThreshold { current, threshold } => {
                assert_eq!(current, 1, "expired events should be trimmed");
                assert_eq!(threshold, 20);
            }
            other => panic!("expected BelowThreshold, got {:?}", other),
        }
    }

    #[test]
    fn reset_clears_state() {
        let mut det = DeleteBurstDetector::new(20, Duration::from_secs(30));
        let now = Instant::now();
        for i in 0..20 {
            det.record_delete_at(now + Duration::from_millis(i));
        }
        assert!(det.is_paused());
        det.reset();
        assert!(!det.is_paused());
        let result = det.record_delete_at(now + Duration::from_millis(100));
        match result {
            BurstStatus::BelowThreshold { current, threshold } => {
                assert_eq!(current, 1);
                assert_eq!(threshold, 20);
            }
            other => panic!("expected BelowThreshold after reset, got {:?}", other),
        }
    }

    #[test]
    fn boundary_event_at_window_edge() {
        // An event exactly at `now - window` is EXCLUDED (cutoff is
        // inclusive of the boundary: events at the boundary expire).
        let mut det = DeleteBurstDetector::new(2, Duration::from_secs(10));
        let t0 = Instant::now();
        // Place a single old event at t0.
        let _ = det.record_delete_at(t0);
        // Advance exactly 10s (window edge). The old event should be
        // trimmed, so we expect BelowThreshold(current=1).
        let edge = t0 + Duration::from_secs(10);
        let result = det.record_delete_at(edge);
        match result {
            BurstStatus::BelowThreshold { current, threshold } => {
                assert_eq!(
                    current, 1,
                    "boundary event at exactly now - window should be expired"
                );
                assert_eq!(threshold, 2);
            }
            other => panic!("expected BelowThreshold, got {:?}", other),
        }
    }

    #[test]
    fn threshold_zero_edge_case() {
        // threshold=0 → first event trips immediately.
        let mut det = DeleteBurstDetector::new(0, Duration::from_secs(30));
        let now = Instant::now();
        let result = det.record_delete_at(now);
        match result {
            BurstStatus::AtThreshold { .. } => {}
            other => panic!("expected AtThreshold for threshold=0, got {:?}", other),
        }
        assert!(det.is_paused());
    }

    #[test]
    fn window_zero_edge_case() {
        // window=0 → never trips (a zero-duration window has no extent).
        let mut det = DeleteBurstDetector::new(1, Duration::from_secs(0));
        let now = Instant::now();
        for i in 0..50 {
            let result = det.record_delete_at(now + Duration::from_millis(i));
            match result {
                BurstStatus::BelowThreshold { .. } => {}
                other => panic!("window=0 should never trip, got {:?}", other),
            }
        }
        assert!(!det.is_paused());
    }

    #[test]
    fn paused_state_ignores_further_deletes() {
        // While Paused, every record_delete returns Paused (does not
        // re-emit AtThreshold). Caller suppresses propagation.
        let mut det = DeleteBurstDetector::new(2, Duration::from_secs(30));
        let now = Instant::now();
        det.record_delete_at(now);
        let trip = det.record_delete_at(now + Duration::from_millis(1));
        assert!(matches!(trip, BurstStatus::AtThreshold { .. }));
        for i in 0..5 {
            let r = det.record_delete_at(now + Duration::from_millis(100 + i));
            assert_eq!(r, BurstStatus::Paused);
        }
    }
}
