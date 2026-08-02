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
//! ## Tolerating an older server (log-once, then escalate)
//!
//! A server that predates the RC-1 route answers HTTP 405. That is NOT a crash
//! and NOT a per-tick error spam: [`ApiClient::patch_self_heartbeat`] maps it to
//! [`HeartbeatOutcome::Unsupported`], and [`run_once`] logs it once then keeps
//! beating -- so a later server upgrade begins refreshing the row with no
//! daemon restart. Any other transport/HTTP error is likewise non-fatal
//! (logged, next tick retries).
//!
//! But a PERMANENT 405 is not "an older server briefly behind" -- it is a
//! routing gap (e.g. the deployed origin's reconciler never mounted the PATCH
//! route), and a single WARN at startup would mask it for the daemon's entire
//! lifetime (PR #10 review, Highwater). So the latch is a consecutive-405
//! counter: after [`UNSUPPORTED_ESCALATE_TICKS`] straight 405 beats (~1 hour at
//! the 5-minute cadence) [`run_once`] escalates to a PERIODIC WARN on every
//! further tick, naming the endpoint and the likely cause, so the gap stays
//! visible in the daemon log and to any log-scraping monitor. Still non-fatal:
//! the daemon keeps beating, and one 200 resets the counter to a clean latch.

use std::sync::atomic::{AtomicU32, Ordering};
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

/// How many CONSECUTIVE 405 (`Unsupported`) beats are tolerated quietly before
/// the WARN escalates from log-once to log-every-tick. 12 ticks at the
/// 5-minute cadence = 1 hour: long enough that a mid-upgrade server window
/// stays quiet, short enough that a permanent routing gap (server never grew
/// the PATCH route) surfaces the same day it ships.
pub const UNSUPPORTED_ESCALATE_TICKS: u32 = 12;

/// What [`run_once`] should log for the `n`-th consecutive `Unsupported`
/// (HTTP 405) beat. Pure so the escalation contract is unit-testable without
/// capturing tracing output.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum UnsupportedLog {
    /// First 405 of a streak: the one tolerant "older server" WARN.
    Once,
    /// Within the tolerance window: say nothing (no per-tick spam).
    Quiet,
    /// Past [`UNSUPPORTED_ESCALATE_TICKS`]: WARN on EVERY tick -- this is no
    /// longer "an older server", it is a routing gap that must stay visible.
    Escalate,
}

/// Escalation decision for the `n`-th consecutive 405 beat (`n` is 1-based).
pub(crate) fn unsupported_log_action(n: u32) -> UnsupportedLog {
    if n == 1 {
        UnsupportedLog::Once
    } else if n <= UNSUPPORTED_ESCALATE_TICKS {
        UnsupportedLog::Quiet
    } else {
        UnsupportedLog::Escalate
    }
}

/// Run ONE heartbeat: build the freshness fields and PATCH the registry.
///
/// `now` is injected (not read from the clock here) so the tick is
/// deterministic under test. `last_sync` is derived from the shared
/// [`SyncHealth`] wall-clock stamp -- `None` until an upstream sync lands, so
/// the field is omitted rather than fabricated.
///
/// Returns the [`HeartbeatOutcome`] (or the [`ApiError`]) so tests can assert
/// without capturing logs. Every path is non-fatal for the caller: an older
/// server (405) is tolerated via the `unsupported_ticks` consecutive counter
/// (one WARN, quiet through the tolerance window, then a PERIODIC WARN once
/// the 405 has persisted past [`UNSUPPORTED_ESCALATE_TICKS`]); any other error
/// is logged at WARN and the next tick simply retries. A transient non-405
/// error does NOT reset the counter -- a network blip between 405s must not
/// silence a standing routing gap; only a real 200 acknowledgement resets it.
pub async fn run_once(
    api: &ApiClient,
    health: &SyncHealth,
    now: DateTime<Utc>,
    unsupported_ticks: &AtomicU32,
) -> Result<HeartbeatOutcome, ApiError> {
    let last_sync = health
        .last_sync_epoch()
        .and_then(|e| DateTime::<Utc>::from_timestamp(e, 0));
    let result = api.patch_self_heartbeat(now, last_sync).await;
    match &result {
        Ok(HeartbeatOutcome::Acknowledged) => {
            // A live route ends any 405 streak: reset so a later regression
            // starts a fresh log-once -> quiet -> escalate cycle.
            unsupported_ticks.store(0, Ordering::Relaxed);
            tracing::debug!(
                version = crate::api_client::daemon_version(),
                last_sync = ?last_sync.map(|t| t.to_rfc3339()),
                "subscriber-registry heartbeat acknowledged (last_seen/last_sync refreshed)"
            );
        }
        Ok(HeartbeatOutcome::Unsupported) => {
            let n = unsupported_ticks
                .fetch_add(1, Ordering::Relaxed)
                .saturating_add(1);
            match unsupported_log_action(n) {
                UnsupportedLog::Once => {
                    tracing::warn!(
                        "subscriber-registry heartbeat: server returned HTTP 405 \
                         (PATCH /api/sync/subscribers/me not mounted) -- tolerating; \
                         registry row will not refresh until the server is upgraded. \
                         The daemon keeps beating; if the 405 persists past \
                         {UNSUPPORTED_ESCALATE_TICKS} beats this escalates to a \
                         periodic warning."
                    );
                }
                UnsupportedLog::Quiet => {
                    // Tolerance window: an older/mid-upgrade server. No spam.
                }
                UnsupportedLog::Escalate => {
                    // PR #10 review (Highwater): a 405 that persists this long is
                    // not "an older server" -- it is a routing gap on the deployed
                    // origin. Log EVERY tick so daemon logs and any log-scraping
                    // monitor keep seeing it. Still non-fatal.
                    tracing::warn!(
                        consecutive_405_beats = n,
                        "subscriber-registry heartbeat: PATCH /api/sync/subscribers/me \
                         still answering HTTP 405 -- server missing PATCH route -- \
                         registry freshness is NOT being recorded (likely cause: the \
                         deployed server never mounted the RC-1 subscriber-heartbeat \
                         route). Daemon keeps beating; a server upgrade clears this."
                    );
                }
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
/// tick's failure is handled inside [`run_once`]. The `unsupported_ticks`
/// counter lives for the loop's lifetime so the 405 streak is tracked across
/// beats (log-once, quiet window, then periodic escalation).
pub fn spawn(api: Arc<ApiClient>, health: Arc<SyncHealth>) {
    tauri::async_runtime::spawn(async move {
        let unsupported_ticks = AtomicU32::new(0);
        let mut tick = tokio::time::interval(Duration::from_secs(HEARTBEAT_INTERVAL_SECS));
        loop {
            tick.tick().await;
            let _ = run_once(&api, &health, Utc::now(), &unsupported_ticks).await;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    /// (Deliverable 3c, orchestration layer) Two ticks against a server that
    /// returns 405 BOTH return `Unsupported` (never an error, never a panic),
    /// and the consecutive-405 counter tracks the streak -- proving the
    /// tolerance across repeated beats (tick 1 = the log-once WARN, tick 2 =
    /// quiet window per [`unsupported_log_action`]).
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
        let ticks = AtomicU32::new(0);
        let now = DateTime::parse_from_rfc3339("2026-08-02T10:00:00Z")
            .unwrap()
            .with_timezone(&Utc);

        let first = run_once(&api, &health, now, &ticks).await.unwrap();
        assert_eq!(first, HeartbeatOutcome::Unsupported);
        assert_eq!(
            ticks.load(Ordering::Relaxed),
            1,
            "first 405 starts the streak"
        );
        assert_eq!(unsupported_log_action(1), UnsupportedLog::Once);

        let second = run_once(&api, &health, now, &ticks).await.unwrap();
        assert_eq!(second, HeartbeatOutcome::Unsupported);
        assert_eq!(ticks.load(Ordering::Relaxed), 2);
        // Second beat is inside the tolerance window -- no second WARN.
        assert_eq!(unsupported_log_action(2), UnsupportedLog::Quiet);
        _m.assert_async().await;
    }

    /// PR #10 review (Highwater): the log-once latch must ESCALATE. The
    /// escalation contract, tick by tick: beat 1 WARNs once, beats 2..=12
    /// (the 1-hour tolerance window at 5-minute cadence) are quiet, and every
    /// beat PAST [`UNSUPPORTED_ESCALATE_TICKS`] WARNs periodically -- so a
    /// permanent routing gap (server missing the PATCH route) can never hide
    /// behind a single startup WARN.
    #[test]
    fn unsupported_405_escalates_after_tolerance_window() {
        assert_eq!(unsupported_log_action(1), UnsupportedLog::Once);
        for n in 2..=UNSUPPORTED_ESCALATE_TICKS {
            assert_eq!(
                unsupported_log_action(n),
                UnsupportedLog::Quiet,
                "beat {n} is inside the tolerance window"
            );
        }
        // Every beat past the window warns -- periodic, not once.
        for n in (UNSUPPORTED_ESCALATE_TICKS + 1)..=(UNSUPPORTED_ESCALATE_TICKS + 5) {
            assert_eq!(
                unsupported_log_action(n),
                UnsupportedLog::Escalate,
                "beat {n} must keep warning"
            );
        }
    }

    /// A 405 streak long enough to escalate is ENDED by one 200: the counter
    /// resets so a later regression starts a fresh log-once cycle, and the
    /// escalated WARN stops the moment the server grows the route (no daemon
    /// restart needed).
    #[tokio::test]
    async fn acknowledged_resets_405_streak() {
        let mut srv = mockito::Server::new_async().await;
        let m405 = srv
            .mock("PATCH", "/api/sync/subscribers/me")
            .with_status(405)
            .with_body("Method Not Allowed")
            .expect(1)
            .create_async()
            .await;
        let api = ApiClient::new(&srv.url(), "vsk_test").unwrap();
        let health = SyncHealth::new();
        // Streak already deep into escalation territory.
        let ticks = AtomicU32::new(UNSUPPORTED_ESCALATE_TICKS + 3);
        let now = DateTime::parse_from_rfc3339("2026-08-02T10:00:00Z")
            .unwrap()
            .with_timezone(&Utc);

        // One more 405: still escalating.
        let out = run_once(&api, &health, now, &ticks).await.unwrap();
        assert_eq!(out, HeartbeatOutcome::Unsupported);
        assert_eq!(
            unsupported_log_action(ticks.load(Ordering::Relaxed)),
            UnsupportedLog::Escalate
        );
        m405.assert_async().await;

        // Server upgraded: 200 resets the streak to zero.
        let m200 = srv
            .mock("PATCH", "/api/sync/subscribers/me")
            .with_status(200)
            .with_body("{}")
            .expect(1)
            .create_async()
            .await;
        let out = run_once(&api, &health, now, &ticks).await.unwrap();
        assert_eq!(out, HeartbeatOutcome::Acknowledged);
        assert_eq!(
            ticks.load(Ordering::Relaxed),
            0,
            "200 clears the 405 streak"
        );
        m200.assert_async().await;
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
            .match_body(mockito::Matcher::Regex(
                r#""last_sync":"2026-08-02T"#.into(),
            ))
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
        let latch = AtomicU32::new(0);
        let now = DateTime::parse_from_rfc3339("2026-08-02T10:00:00Z")
            .unwrap()
            .with_timezone(&Utc);

        let out = run_once(&api, &health, now, &latch).await.unwrap();
        assert_eq!(out, HeartbeatOutcome::Acknowledged);
        m.assert_async().await;
    }
}
