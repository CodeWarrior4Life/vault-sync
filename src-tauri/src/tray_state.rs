//! Shared state between the SSE consumer and the tray menu.
//!
//! The SSE consumer writes to this on each significant event
//! (connect, disconnect, event received, error). A background task on the
//! Tauri async runtime polls every couple seconds and rebuilds the tray menu
//! text to reflect what the daemon is actually doing — so the user can hover
//! the menu bar icon and see live status instead of a stale "connecting…".
//!
//! Concurrency model: `Arc<RwLock<TrayState>>`. Writers hold the lock for
//! microseconds at a time; the poller holds it just long enough to clone the
//! fields it needs. No async lock so the SSE hot path stays sync.
//!
//! v0.3 (Wave 3 mandate §4.1 + §9 AG5 + AG13) adds telemetry fields for the
//! new push / file_watcher / safety-valve modules. All new fields default to
//! 0 / false and are additive — pre-v0.3 callers of TrayState::new() keep
//! working unchanged.

use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::SystemTime;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionStatus {
    /// Daemon just started; no SSE attempt yet.
    Starting,
    /// SSE TCP handshake / SSE Connected event in flight.
    Connecting,
    /// SSE Connected event seen, ready for events.
    Connected,
    /// User paused via tray menu (v0.1.4+).
    Paused,
    /// Transient backoff after a 5xx / disconnect — will retry.
    Reconnecting,
    /// Fatal auth (401/403) — token revoked, need re-pair.
    AuthFailed,
    /// Generic error state with message in last_error.
    Error,
}

impl ConnectionStatus {
    pub fn label(self) -> &'static str {
        match self {
            Self::Starting => "Starting…",
            Self::Connecting => "Connecting…",
            Self::Connected => "Connected",
            Self::Paused => "Paused",
            Self::Reconnecting => "Reconnecting…",
            Self::AuthFailed => "Auth failed — re-pair",
            Self::Error => "Error",
        }
    }
}

#[derive(Debug)]
pub struct TrayState {
    pub status: ConnectionStatus,
    pub events_received: u64,
    pub last_event_at: Option<SystemTime>,
    pub last_error: Option<String>,
    pub subscriber_id: String,
    pub nexus_url: String,
    pub vault_root: PathBuf,

    // ---- v0.3 push / watcher / safety telemetry (mandate §4.1 + §9 AG5/AG13) ----
    /// Current push_journal queue depth (un-acked events).
    pub uploads_pending: usize,
    /// Monotonic counter of successfully accepted server pushes.
    pub uploads_sent: u64,
    /// Monotonic counter of push failures (network exhaustion + 5xx). Does
    /// NOT include 409 conflicts or auth/skip outcomes.
    pub uploads_failed: u64,
    /// Time of the last attempted push (Sent or Failed).
    pub uploads_last_at: Option<DateTime<Utc>>,
    /// Total file_watcher events that were filtered out (any reason).
    pub events_filtered: u64,
    /// file_watcher events dropped by the RASP substrate fence.
    pub events_dropped_substrate: u64,
    /// file_watcher events dropped by the extension allow-list.
    pub events_dropped_extension: u64,
    /// file_watcher events dropped by hardcoded or user exclude rules.
    pub events_dropped_excludes: u64,
    /// Number of `*.conflict-from-<dev>-<lsn>.md` files currently in the vault.
    pub conflict_unresolved: usize,
    /// Total post-write integrity-check failures (byte mismatch or parse fail).
    pub integrity_failures: u64,
    /// True iff `<vault>/redflag.md` is present — sync is HALTED.
    pub redflag_tripped: bool,
    /// True iff the DeleteBurstDetector is currently Paused.
    pub delete_burst_paused: bool,
    /// True while an owner-invoked "Verify and repair all files" sweep is
    /// running. Set true the instant the tray menu item is clicked (before the
    /// walk+hash+reconcile await) and false when it completes. Drives the
    /// immediate "⟳ Verifying vault…" tooltip so the owner gets instant
    /// feedback instead of staring at a stale tooltip for ~16s.
    pub verify_in_progress: bool,
    /// S477 v0.3.8 (D): periodic reconciliation backstop counters. Each
    /// background sweep updates these from its returned `VerifyRepairReport`
    /// so the tray can surface drift telemetry without exposing the full
    /// report shape. Pulls = server-only paths the sweep saw (SSE consumer
    /// materializes those). Pushes = local-only / hash-mismatch paths the
    /// sweep queued for upload via the shared push_journal.
    pub recon_pulls_total: u64,
    pub recon_pushes_total: u64,
    pub last_recon_at: Option<DateTime<Utc>>,
    /// True while a periodic reconciliation pass is in flight. Distinct from
    /// `verify_in_progress` (which is owner-invoked); the recon-sweep is the
    /// background-task variant.
    pub recon_in_progress: bool,
    /// v0.4.12: Some(version) once the updater has detected (and is staging /
    /// has staged) a newer release. Drives the obvious, persistent tray
    /// indicator + the click-to-restart-and-apply menu item. Reset to None on
    /// the freshly-restarted (already-updated) binary.
    pub update_available: Option<String>,
    /// CS-3 §3.1 (AR-1C-005): `config.toml` on disk no longer matches the
    /// snapshot this daemon runs on. A notice, not a hold: nothing reloads
    /// and nothing stops; a restart applies the new config.
    pub config_pending_restart: bool,
    /// CS-3 §3.1: the sha8 of the config snapshot this process runs on
    /// (empty when unpaired).
    pub config_sha8: String,
    /// WS-A A6 (TKT-17fc22e5 / TKT-8fc0b2a4): pushes the server rejected
    /// PERMANENTLY (HTTP 400 / 422; skip + ack, never retried). Monotonic.
    pub pushes_rejected_permanent: u64,
    /// WS-A A2 (TKT-17fc22e5 / TKT-8fc0b2a4): pushes refused LOCALLY by the
    /// pre-push I3 check (NUL byte / invalid UTF-8) — nothing was sent.
    pub pushes_refused_local_i3: u64,
    /// WS-A A3: distinct paths currently in the rejected ledger
    /// (`rejected-ledger.json`), i.e. paths whose current bytes the server
    /// (or the local I3 check) refused and that reconciliation will not
    /// re-queue until their bytes change.
    pub rejected_paths: u64,
    /// WS-A A3: reconciliation pushes NOT queued because the path's current
    /// sha equals its ledgered rejected sha (the loop this closes).
    pub reconcile_skipped_rejected: u64,

    // ---- TKT-8fc0b2a4 (WS-A4/A5/A6) auth failure: fail loud, keep the
    // ---- cursor, await credential recovery -------------------------------
    /// Epoch seconds of the FIRST 401/403 of the current auth-failure
    /// episode (SSE connect or push), `None` while auth is healthy. Set by
    /// the SSE loop and the push drain on their first rejection, kept at the
    /// earliest stamp while the episode persists, cleared by either side on
    /// the first accepted request. Carried in the heartbeat PATCH so the
    /// registry can see a revoked daemon that is still alive and retrying.
    pub auth_failed_since: Option<i64>,
}

impl TrayState {
    pub fn new(subscriber_id: String, nexus_url: String, vault_root: PathBuf) -> Self {
        Self {
            status: ConnectionStatus::Starting,
            events_received: 0,
            last_event_at: None,
            last_error: None,
            subscriber_id,
            nexus_url,
            vault_root,
            uploads_pending: 0,
            uploads_sent: 0,
            uploads_failed: 0,
            uploads_last_at: None,
            events_filtered: 0,
            events_dropped_substrate: 0,
            events_dropped_extension: 0,
            events_dropped_excludes: 0,
            conflict_unresolved: 0,
            integrity_failures: 0,
            redflag_tripped: false,
            delete_burst_paused: false,
            verify_in_progress: false,
            recon_pulls_total: 0,
            recon_pushes_total: 0,
            last_recon_at: None,
            recon_in_progress: false,
            update_available: None,
            config_pending_restart: false,
            config_sha8: String::new(),
            pushes_rejected_permanent: 0,
            pushes_refused_local_i3: 0,
            rejected_paths: 0,
            reconcile_skipped_rejected: 0,
            auth_failed_since: None,
        }
    }

    // ---- TKT-8fc0b2a4 (WS-A4/A5/A6) auth-failure setters -----------------

    /// Record the start of an auth-failure episode at `now_epoch` (seconds).
    /// Idempotent: a later rejection in the same episode keeps the FIRST
    /// stamp, so `auth_failed_since` always answers "since when".
    pub fn mark_auth_failed(&mut self, now_epoch: i64) {
        if self.auth_failed_since.is_none() {
            self.auth_failed_since = Some(now_epoch);
        }
    }

    /// Clear the auth-failure episode (an authenticated request succeeded).
    pub fn clear_auth_failed(&mut self) {
        self.auth_failed_since = None;
    }

    /// CS-3 §3.1: set/clear the config-pending-restart notice flag.
    pub fn set_config_pending_restart(&mut self, pending: bool) {
        self.config_pending_restart = pending;
    }

    /// v0.4.12: record that a newer release is available/staged (Some(version))
    /// or clear it (None). Idempotent setter for the updater task.
    pub fn set_update_available(&mut self, version: Option<String>) {
        self.update_available = version;
    }

    pub fn set_status(&mut self, status: ConnectionStatus) {
        self.status = status;
        if status == ConnectionStatus::Connected {
            self.last_error = None;
        }
    }

    pub fn record_event(&mut self) {
        self.events_received += 1;
        self.last_event_at = Some(SystemTime::now());
        self.status = ConnectionStatus::Connected;
    }

    pub fn set_error(&mut self, status: ConnectionStatus, msg: String) {
        self.status = status;
        self.last_error = Some(msg);
    }

    // ---------------- v0.3 setters / counters ----------------

    pub fn set_uploads_pending(&mut self, n: usize) {
        self.uploads_pending = n;
    }

    pub fn inc_uploads_sent(&mut self) {
        self.uploads_sent = self.uploads_sent.saturating_add(1);
        self.uploads_last_at = Some(Utc::now());
    }

    pub fn inc_uploads_failed(&mut self) {
        self.uploads_failed = self.uploads_failed.saturating_add(1);
        self.uploads_last_at = Some(Utc::now());
    }

    pub fn inc_events_filtered(&mut self) {
        self.events_filtered = self.events_filtered.saturating_add(1);
    }

    pub fn inc_events_dropped_substrate(&mut self) {
        self.events_dropped_substrate = self.events_dropped_substrate.saturating_add(1);
        self.inc_events_filtered();
    }

    pub fn inc_events_dropped_extension(&mut self) {
        self.events_dropped_extension = self.events_dropped_extension.saturating_add(1);
        self.inc_events_filtered();
    }

    pub fn inc_events_dropped_excludes(&mut self) {
        self.events_dropped_excludes = self.events_dropped_excludes.saturating_add(1);
        self.inc_events_filtered();
    }

    pub fn set_conflict_unresolved(&mut self, n: usize) {
        self.conflict_unresolved = n;
    }

    pub fn inc_integrity_failures(&mut self) {
        self.integrity_failures = self.integrity_failures.saturating_add(1);
    }

    pub fn set_redflag_tripped(&mut self, tripped: bool) {
        self.redflag_tripped = tripped;
    }

    pub fn set_delete_burst_paused(&mut self, paused: bool) {
        self.delete_burst_paused = paused;
    }

    pub fn set_verify_in_progress(&mut self, in_progress: bool) {
        self.verify_in_progress = in_progress;
    }

    /// S477 v0.3.8 (D): recon-task setters. `note_recon_pass` is the
    /// single entry point a completed pass calls — folds the report's
    /// add_count (pulls) + modify_count (pushes) into the running totals
    /// and stamps `last_recon_at` to now.
    pub fn set_recon_in_progress(&mut self, in_progress: bool) {
        self.recon_in_progress = in_progress;
    }

    /// WS-A A6: the server rejected a push permanently (HTTP 400 / 422).
    pub fn inc_pushes_rejected_permanent(&mut self) {
        self.pushes_rejected_permanent = self.pushes_rejected_permanent.saturating_add(1);
    }

    /// WS-A A6: the four fail-loud push-health counters as one serializable
    /// snapshot — the shape every export surface (status JSON, heartbeat,
    /// `sync-verify`) reads, so a new surface never re-derives field names.
    pub fn push_health(&self) -> PushHealthCounters {
        PushHealthCounters {
            pushes_rejected_permanent: self.pushes_rejected_permanent,
            rejected_paths: self.rejected_paths,
            pushes_refused_local_i3: self.pushes_refused_local_i3,
            reconcile_skipped_rejected: self.reconcile_skipped_rejected,
        }
    }

    /// WS-A A2: a push was refused by the local I3 check (never sent).
    pub fn inc_pushes_refused_local_i3(&mut self) {
        self.pushes_refused_local_i3 = self.pushes_refused_local_i3.saturating_add(1);
    }

    /// WS-A A3: the rejected ledger's current distinct-path count.
    pub fn set_rejected_paths(&mut self, n: usize) {
        self.rejected_paths = n as u64;
    }

    /// WS-A A3: `n` reconciliation pushes were suppressed by the rejected ledger.
    pub fn inc_reconcile_skipped_rejected(&mut self, n: u64) {
        self.reconcile_skipped_rejected = self.reconcile_skipped_rejected.saturating_add(n);
    }

    pub fn note_recon_pass(&mut self, pulls: u64, pushes: u64) {
        self.recon_pulls_total = self.recon_pulls_total.saturating_add(pulls);
        self.recon_pushes_total = self.recon_pushes_total.saturating_add(pushes);
        self.last_recon_at = Some(Utc::now());
    }

    /// One-line status string for the tray menu's top item.
    pub fn status_line(&self) -> String {
        let staleness = self
            .last_event_at
            .and_then(|t| t.elapsed().ok())
            .map(format_staleness);
        match (self.status, staleness) {
            (ConnectionStatus::Connected, Some(s)) => {
                format!(
                    "{} · {} events · last {}",
                    self.status.label(),
                    self.events_received,
                    s
                )
            }
            (ConnectionStatus::Connected, None) => {
                format!("{} · {} events", self.status.label(), self.events_received)
            }
            _ => self.status.label().to_string(),
        }
    }
}

fn format_staleness(d: std::time::Duration) -> String {
    let s = d.as_secs();
    match s {
        0..=59 => format!("{s}s ago"),
        60..=3599 => format!("{}m ago", s / 60),
        3600..=86399 => format!("{}h ago", s / 3600),
        _ => format!("{}d ago", s / 86400),
    }
}

pub type SharedTrayState = Arc<RwLock<TrayState>>;

/// WS-A A6 (TKT-17fc22e5 / TKT-8fc0b2a4): the push fail-loud counters,
/// serializable for the health surface. Field names are the wire names.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PushHealthCounters {
    /// Pushes the server rejected permanently (HTTP 400 / 422).
    pub pushes_rejected_permanent: u64,
    /// Distinct paths currently in `rejected-ledger.json`.
    pub rejected_paths: u64,
    /// Pushes refused locally by the pre-push I3 check (never sent).
    pub pushes_refused_local_i3: u64,
    /// Reconciliation pushes suppressed by the rejected ledger.
    pub reconcile_skipped_rejected: u64,
}

impl PushHealthCounters {
    /// True iff anything is being refused or suppressed: the daemon is
    /// converging around a path it cannot push. A health surface treats this
    /// as degraded, not stalled.
    pub fn is_degraded(&self) -> bool {
        self.rejected_paths > 0 || self.pushes_refused_local_i3 > 0
    }
}

#[cfg(test)]
#[allow(clippy::disallowed_methods)]
mod tests {
    use super::*;

    fn fresh() -> TrayState {
        TrayState::new(
            "sub-test".to_string(),
            "https://example".to_string(),
            PathBuf::from("/tmp/vault"),
        )
    }

    #[test]
    fn default_state_has_zero_counters() {
        let s = fresh();
        assert_eq!(s.uploads_pending, 0);
        assert_eq!(s.uploads_sent, 0);
        assert_eq!(s.uploads_failed, 0);
        assert!(s.uploads_last_at.is_none());
        assert_eq!(s.events_filtered, 0);
        assert_eq!(s.events_dropped_substrate, 0);
        assert_eq!(s.events_dropped_extension, 0);
        assert_eq!(s.events_dropped_excludes, 0);
        assert_eq!(s.conflict_unresolved, 0);
        assert_eq!(s.integrity_failures, 0);
        assert!(!s.redflag_tripped);
        assert!(!s.delete_burst_paused);
        assert_eq!(s.pushes_rejected_permanent, 0);
        assert_eq!(s.pushes_refused_local_i3, 0);
        assert_eq!(s.rejected_paths, 0);
        assert_eq!(s.reconcile_skipped_rejected, 0);
        assert_eq!(s.push_health(), PushHealthCounters::default());
        assert!(!s.push_health().is_degraded());
    }

    /// WS-A A6: the four counters have inc/set fns and round-trip through the
    /// serializer with their wire names and values intact.
    #[test]
    fn push_health_counters_round_trip_through_serde() {
        let mut s = fresh();
        s.inc_pushes_rejected_permanent();
        s.inc_pushes_rejected_permanent();
        s.inc_pushes_refused_local_i3();
        s.set_rejected_paths(3);
        s.inc_reconcile_skipped_rejected(5);
        s.inc_reconcile_skipped_rejected(2);
        let snap = s.push_health();
        assert_eq!(
            snap,
            PushHealthCounters {
                pushes_rejected_permanent: 2,
                rejected_paths: 3,
                pushes_refused_local_i3: 1,
                reconcile_skipped_rejected: 7,
            }
        );
        assert!(snap.is_degraded());
        let json = serde_json::to_string(&snap).unwrap();
        for name in [
            "pushes_rejected_permanent",
            "rejected_paths",
            "pushes_refused_local_i3",
            "reconcile_skipped_rejected",
        ] {
            assert!(json.contains(&format!("\"{name}\"")), "{json}");
        }
        let back: PushHealthCounters = serde_json::from_str(&json).unwrap();
        assert_eq!(back, snap, "round-trip must be lossless");
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["pushes_rejected_permanent"], 2);
        assert_eq!(v["rejected_paths"], 3);
        assert_eq!(v["pushes_refused_local_i3"], 1);
        assert_eq!(v["reconcile_skipped_rejected"], 7);
    }

    #[test]
    fn inc_uploads_sent_increments() {
        let mut s = fresh();
        s.inc_uploads_sent();
        s.inc_uploads_sent();
        assert_eq!(s.uploads_sent, 2);
        assert!(s.uploads_last_at.is_some());
    }

    #[test]
    fn inc_uploads_failed_increments_and_stamps_last_at() {
        let mut s = fresh();
        s.inc_uploads_failed();
        assert_eq!(s.uploads_failed, 1);
        assert!(s.uploads_last_at.is_some());
    }

    #[test]
    fn dropped_substrate_also_bumps_filtered_total() {
        let mut s = fresh();
        s.inc_events_dropped_substrate();
        s.inc_events_dropped_extension();
        s.inc_events_dropped_excludes();
        assert_eq!(s.events_dropped_substrate, 1);
        assert_eq!(s.events_dropped_extension, 1);
        assert_eq!(s.events_dropped_excludes, 1);
        assert_eq!(s.events_filtered, 3);
    }

    #[test]
    fn set_redflag_tripped_persists() {
        let mut s = fresh();
        s.set_redflag_tripped(true);
        assert!(s.redflag_tripped);
        s.set_redflag_tripped(false);
        assert!(!s.redflag_tripped);
    }

    /// TKT-8fc0b2a4 (A6): the episode stamp keeps its FIRST value across
    /// repeated rejections and clears on recovery.
    #[test]
    fn auth_failed_since_keeps_first_stamp_and_clears() {
        let mut s = fresh();
        assert_eq!(s.auth_failed_since, None);
        s.mark_auth_failed(1_700_000_000);
        s.mark_auth_failed(1_700_000_600);
        assert_eq!(s.auth_failed_since, Some(1_700_000_000));
        s.clear_auth_failed();
        assert_eq!(s.auth_failed_since, None);
        s.mark_auth_failed(1_700_001_000);
        assert_eq!(s.auth_failed_since, Some(1_700_001_000));
    }

    #[test]
    fn set_conflict_unresolved_and_delete_burst_persist() {
        let mut s = fresh();
        s.set_conflict_unresolved(7);
        s.set_delete_burst_paused(true);
        s.set_uploads_pending(3);
        assert_eq!(s.conflict_unresolved, 7);
        assert!(s.delete_burst_paused);
        assert_eq!(s.uploads_pending, 3);
    }
}
