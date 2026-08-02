//! RC-1 client half (TKT-a38b7c26): the subscriber-registry HEARTBEAT.
//!
//! ## Why this exists
//!
//! The daemon was already "live" in every observable way -- the SSE stream was
//! connected, the push pipeline drained, the tray ticked -- yet it never told
//! the server registry it was still there. The one PATCH it did send
//! (`patch_self_version`) fired exactly once, at startup. So the
//! `vault_subscribers` row's `daemon_version` / `last_seen_at` / `last_sync_at`
//! froze at first-connect time and drifted stale, even though the server route
//! to refresh them existed. Highwater's 2026-08-01 audit called this the RC-1
//! client gap.
//!
//! This module drives a periodic `PATCH /api/sync/subscribers/me` carrying the
//! running version + platform, a fresh `last_seen` (now), and a truthful
//! `last_sync` (the wall-clock time an upstream sync actually landed, sourced
//! from [`SyncHealth::last_sync_epoch`], omitted when none has). The registry
//! row now reflects reality for as long as the daemon runs.
//!
//! ## Tolerating an older server (log-once)
//!
//! A server that predates the RC-1 route answers HTTP 405. That is NOT a crash
//! and NOT a per-tick error spam: [`ApiClient::patch_self_heartbeat`] maps it to
//! [`HeartbeatOutcome::Unsupported`], and [`run_once`] logs it exactly once (an
//! `AtomicBool` latch) then keeps beating -- so a later server upgrade begins
//! refreshing the row with no daemon restart. Any other transport/HTTP error is
//! likewise non-fatal (logged, next tick retries).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};

use crate::api_client::{ApiClient, ApiError, HeartbeatOutcome};
use crate::sync_health::SyncHealth;

/// Heartbeat cadence. 5 minutes: frequent enough that the registry row never
/// looks stale to an operator watching liveness, cheap enough that it adds no
/// meaningful load (one tiny PATCH per interval). Mirrors the long-standing
/// 5-minute updater-check loop cadence.
pub const HEARTBEAT_INTERVAL_SECS: u64 = 300;

/// Run ONE heartbeat: build the freshness fields and PATCH the registry.
///
/// `now` is injected (not read from the clock here) so the tick is
/// deterministic under test. `last_sync` is derived from the shared
/// [`SyncHealth`] wall-clock stamp -- `None` until an upstream sync lands, so
/// the field is omitted rather than fabricated.
///
/// Returns the [`HeartbeatOutcome`] (or the [`ApiError`]) so tests can assert
/// without capturing logs. Every path is non-fatal for the caller: an older
/// server (405) is logged once via `unsupported_logged`; any other error is
/// logged at WARN and the next tick simply retries.
pub async fn run_once(
    api: &ApiClient,
    health: &SyncHealth,
    now: DateTime<Utc>,
    unsupported_logged: &AtomicBool,
) -> Result<HeartbeatOutcome, ApiError> {
    let last_sync = health
        .last_sync_epoch()
        .and_then(|e| DateTime::<Utc>::from_timestamp(e, 0));
    let result = api.patch_self_heartbeat(now, last_sync).await;
    match &result {
        Ok(HeartbeatOutcome::Acknowledged) => {
            tracing::debug!(
                version = crate::api_client::daemon_version(),
                last_sync = ?last_sync.map(|t| t.to_rfc3339()),
                "subscriber-registry heartbeat acknowledged (last_seen/last_sync refreshed)"
            );
        }
        Ok(HeartbeatOutcome::Unsupported) => {
            // Log EXACTLY once: an older server without the route would
            // otherwise WARN-spam every interval for the daemon's lifetime.
            if !unsupported_logged.swap(true, Ordering::Relaxed) {
                tracing::warn!(
                    "subscriber-registry heartbeat: server returned HTTP 405 \
                     (PATCH /api/sync/subscribers/me not mounted) -- tolerating; \
                     registry row will not refresh until the server is upgraded. \
                     This is logged once; the daemon keeps beating."
                );
            }
        }
        Err(e) => {
            // Non-fatal: transient network / server error. Next tick retries.
            tracing::warn!(error = %e, "subscriber-registry heartbeat failed (non-fatal; will retry next interval)");
        }
    }
    result
}

/// Spawn the forever heartbeat loop. The first tick fires immediately (a
/// `tokio::time::interval` yields at t=0), so the registry row is refreshed
/// shortly after connect, then every [`HEARTBEAT_INTERVAL_SECS`].
///
/// Fire-and-forget by contract: the loop never propagates an error out; each
/// tick's failure is handled inside [`run_once`]. The `unsupported_logged`
/// latch lives for the loop's lifetime so the 405 line prints at most once.
pub fn spawn(api: Arc<ApiClient>, health: Arc<SyncHealth>) {
    tauri::async_runtime::spawn(async move {
        let unsupported_logged = AtomicBool::new(false);
        let mut tick = tokio::time::interval(Duration::from_secs(HEARTBEAT_INTERVAL_SECS));
        loop {
            tick.tick().await;
            let _ = run_once(&api, &health, Utc::now(), &unsupported_logged).await;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    /// (Deliverable 3c, orchestration layer) Two ticks against a server that
    /// returns 405 BOTH return `Unsupported` (never an error, never a panic),
    /// and the "unsupported" WARN latch flips exactly once -- proving the
    /// log-once tolerance across repeated beats.
    #[tokio::test]
    async fn run_once_tolerates_405_and_logs_once() {
        let mut srv = mockito::Server::new_async().await;
        let _m = srv
            .mock("PATCH", "/api/sync/subscribers/me")
            .with_status(405)
            .with_body("Method Not Allowed")
            .expect(2) // both ticks hit the wire; neither crashes
            .create_async()
            .await;
        let api = ApiClient::new(&srv.url(), "vsk_test").unwrap();
        let health = SyncHealth::new();
        let latch = AtomicBool::new(false);
        let now = DateTime::parse_from_rfc3339("2026-08-02T10:00:00Z")
            .unwrap()
            .with_timezone(&Utc);

        let first = run_once(&api, &health, now, &latch).await.unwrap();
        assert_eq!(first, HeartbeatOutcome::Unsupported);
        assert!(latch.load(Ordering::Relaxed), "first 405 flips the latch");

        let second = run_once(&api, &health, now, &latch).await.unwrap();
        assert_eq!(second, HeartbeatOutcome::Unsupported);
        // Latch stays set (still true) -- no second WARN would be emitted.
        assert!(latch.load(Ordering::Relaxed));
        _m.assert_async().await;
    }

    /// A live server (200) acknowledges, and once an upstream sync has landed
    /// the tick carries the real `last_sync` derived from SyncHealth's
    /// wall-clock stamp. Proves the end-to-end wiring: SyncHealth stamp ->
    /// heartbeat field -> registry PATCH.
    #[tokio::test]
    async fn run_once_reports_last_sync_from_sync_health() {
        let mut srv = mockito::Server::new_async().await;
        let m = srv
            .mock("PATCH", "/api/sync/subscribers/me")
            // Assert the RFC3339 date derived from the SyncHealth stamp lands
            // on the wire, so the stamp really flows through to the PATCH body.
            .match_body(mockito::Matcher::Regex(r#""last_sync":"2026-08-02T"#.into()))
            .with_status(200)
            .with_body("{}")
            .expect(1)
            .create_async()
            .await;
        let api = ApiClient::new(&srv.url(), "vsk_test").unwrap();
        let health = SyncHealth::new();
        // Stamp a known upstream-sync epoch: 2026-08-02T09:00:00Z.
        let synced = DateTime::parse_from_rfc3339("2026-08-02T09:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        health.mark_synced_at(synced.timestamp());
        let latch = AtomicBool::new(false);
        let now = DateTime::parse_from_rfc3339("2026-08-02T10:00:00Z")
            .unwrap()
            .with_timezone(&Utc);

        let out = run_once(&api, &health, now, &latch).await.unwrap();
        assert_eq!(out, HeartbeatOutcome::Acknowledged);
        m.assert_async().await;
    }
}
