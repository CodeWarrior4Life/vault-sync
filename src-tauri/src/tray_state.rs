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

use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::SystemTime;

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
        }
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
