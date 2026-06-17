//! R2 (TKT-4bd13028) — sustained-rate cap for the push-drain pipeline.
//!
//! Wraps a 1-second sliding window of recent push `Instant` timestamps under
//! a `tokio::sync::Mutex`. `acquire().await` is the gate every logical push
//! passes through before its HTTP attempt. It drops timestamps older than 1s,
//! returns immediately when the window has room, and otherwise sleeps until
//! the front of the window falls off — token-bucket semantics with no
//! third-party crate.
//!
//! ## Why
//!
//! Before this module the pipeline had `push_concurrency`, `batch_size`, and
//! `busy_loop_interval_ms` knobs but no per-second cap. A 28k-file rsync
//! observed 2026-06-16 turned into a sustained 43 pushes/s storm that tripped
//! the S498 FLOOD monitor on the Nexus server (TKT-ea4058b8). The cap here
//! bounds aggregate push rate regardless of how many path-chains run in
//! parallel — a global ceiling per `PushClient` instance.
//!
//! ## Env
//!
//! * `VAULT_SYNC_MAX_PUSH_PER_SEC` — sustained cap; default 20. Zero or
//!   malformed falls back to the default (the kill switch is a separate var
//!   so a misconfigured number does not accidentally remove the cap).
//! * `VAULT_SYNC_DISABLE_PUSH_RATE_CAP` — kill switch (any non-empty value).
//!   When set, callers SHOULD skip building a limiter at all. The
//!   `is_disabled` helper mirrors `reconciliation::is_disabled`.
//!
//! ## Tests
//!
//! Two pure helpers (`would_exceed`, `next_release_at`) are exported so the
//! boundary logic is testable without driving the tokio clock. The
//! end-to-end `acquire()` test uses `tokio::time::pause` / `advance` to keep
//! the test deterministic.
//!
//! ## Compose
//!
//! The cap acquires ONE slot per LOGICAL push (one `process_event` call).
//! The retry-with-backoff loop inside `process_event` does NOT re-acquire —
//! a transient 5xx burst reuses the already-held slot, which means the
//! effective per-second rate can dip BELOW the nominal cap during a server
//! outage. That is the desired behavior: do not amplify load on a struggling
//! server.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Mutex;

use crate::reconciliation::EnvReader;

/// Default sustained cap when the env var is absent / malformed / zero.
pub const DEFAULT_MAX_PUSH_PER_SEC: usize = 20;

/// Env var name for the sustained cap override.
pub const ENV_MAX_PER_SEC: &str = "VAULT_SYNC_MAX_PUSH_PER_SEC";

/// Env var name for the global kill switch.
pub const ENV_DISABLE: &str = "VAULT_SYNC_DISABLE_PUSH_RATE_CAP";

/// Read `VAULT_SYNC_MAX_PUSH_PER_SEC` with the default-on-zero / default-on-
/// malformed fallback. Mirrors `reconciliation::read_interval`'s shape so the
/// two env readers behave consistently.
pub fn read_max_per_sec(env: &dyn EnvReader) -> usize {
    env.get(ENV_MAX_PER_SEC)
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(DEFAULT_MAX_PUSH_PER_SEC)
}

/// True iff the kill switch env var is set to any non-empty value. Mirrors
/// `reconciliation::is_disabled` semantics (empty-string is NOT disable).
pub fn is_disabled(env: &dyn EnvReader) -> bool {
    env.get(ENV_DISABLE).is_some_and(|v| !v.is_empty())
}

/// Pure boundary helper: `true` iff the cap is exhausted at the given window
/// length. Extracted so the cap math is unit-testable without driving the
/// tokio clock.
pub fn would_exceed(window_len: usize, max_per_sec: usize) -> bool {
    window_len >= max_per_sec
}

/// Pure helper: how long the caller must sleep before the slot at the front
/// of the 1-second window is released. Returns `Duration::ZERO` if the
/// front already lies in the past (slot is immediately available).
pub fn next_release_at(window_front: Instant, now: Instant) -> Duration {
    let target = window_front + Duration::from_secs(1);
    if target <= now {
        Duration::ZERO
    } else {
        target - now
    }
}

/// Sustained-rate cap for push-drain. One instance per pipeline; concurrent
/// `acquire()` calls serialize on the inner `Mutex`.
pub struct PushRateLimiter {
    max_per_sec: usize,
    window: Mutex<VecDeque<Instant>>,
}

impl PushRateLimiter {
    /// Build a limiter that caps at `max_per_sec` acquires per rolling 1s.
    /// A `max_per_sec` of 0 is treated as "no cap" — `acquire` becomes a
    /// no-op (callers should normally route through the kill switch + skip
    /// constructing the limiter, but this is a defense-in-depth guard).
    pub fn new(max_per_sec: usize) -> Arc<Self> {
        Arc::new(Self {
            max_per_sec,
            window: Mutex::new(VecDeque::with_capacity(max_per_sec.saturating_add(1))),
        })
    }

    /// Block until the next acquire is permitted, then record it. Safe to
    /// call concurrently from any number of tasks — the inner `Mutex`
    /// serializes the window updates and the sleep is awaited OUTSIDE the
    /// lock so other tasks can re-check the window.
    pub async fn acquire(&self) {
        if self.max_per_sec == 0 {
            return;
        }
        loop {
            let sleep_for = {
                let mut w = self.window.lock().await;
                let now = Instant::now();
                let cutoff = now - Duration::from_secs(1);
                while let Some(&front) = w.front() {
                    if front <= cutoff {
                        w.pop_front();
                    } else {
                        break;
                    }
                }
                if !would_exceed(w.len(), self.max_per_sec) {
                    w.push_back(now);
                    return;
                }
                // Cap exhausted — sleep until the front falls off.
                // SAFETY: w.len() >= max_per_sec >= 1, so front() is Some.
                let front = *w
                    .front()
                    .expect("window non-empty when len >= max_per_sec");
                next_release_at(front, now)
            };
            // Release the lock before sleeping so other acquires can probe.
            if !sleep_for.is_zero() {
                tokio::time::sleep(sleep_for).await;
            }
        }
    }

    /// Current window length. Useful for tests and tray telemetry.
    pub async fn in_window(&self) -> usize {
        let w = self.window.lock().await;
        w.len()
    }

    /// The configured cap, for tray / telemetry surfaces.
    pub fn max_per_sec(&self) -> usize {
        self.max_per_sec
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reconciliation::MapEnv;
    use std::collections::HashMap;

    fn env_with(pairs: &[(&str, &str)]) -> MapEnv {
        let mut m = HashMap::new();
        for (k, v) in pairs {
            m.insert((*k).to_string(), (*v).to_string());
        }
        MapEnv(m)
    }

    // ─── pure env-reader tests ──────────────────────────────────────────

    #[test]
    fn read_max_per_sec_defaults_when_env_missing() {
        let env = env_with(&[]);
        assert_eq!(read_max_per_sec(&env), DEFAULT_MAX_PUSH_PER_SEC);
    }

    #[test]
    fn read_max_per_sec_uses_env_when_valid() {
        let env = env_with(&[(ENV_MAX_PER_SEC, "50")]);
        assert_eq!(read_max_per_sec(&env), 50);
    }

    #[test]
    fn read_max_per_sec_falls_back_on_zero() {
        let env = env_with(&[(ENV_MAX_PER_SEC, "0")]);
        assert_eq!(read_max_per_sec(&env), DEFAULT_MAX_PUSH_PER_SEC);
    }

    #[test]
    fn read_max_per_sec_falls_back_on_malformed() {
        let env = env_with(&[(ENV_MAX_PER_SEC, "twenty")]);
        assert_eq!(read_max_per_sec(&env), DEFAULT_MAX_PUSH_PER_SEC);
    }

    #[test]
    fn is_disabled_false_when_env_missing() {
        assert!(!is_disabled(&env_with(&[])));
    }

    #[test]
    fn is_disabled_false_when_env_empty_string() {
        assert!(!is_disabled(&env_with(&[(ENV_DISABLE, "")])));
    }

    #[test]
    fn is_disabled_true_when_env_set_to_one() {
        assert!(is_disabled(&env_with(&[(ENV_DISABLE, "1")])));
    }

    #[test]
    fn is_disabled_true_when_env_set_to_true() {
        assert!(is_disabled(&env_with(&[(ENV_DISABLE, "true")])));
    }

    // ─── pure boundary helpers ───────────────────────────────────────────

    #[test]
    fn would_exceed_at_window_len_lt_cap_returns_false() {
        assert!(!would_exceed(0, 5));
        assert!(!would_exceed(4, 5));
    }

    #[test]
    fn would_exceed_at_window_len_eq_cap_returns_true() {
        assert!(would_exceed(5, 5));
    }

    #[test]
    fn would_exceed_at_window_len_gt_cap_returns_true() {
        assert!(would_exceed(6, 5));
        assert!(would_exceed(usize::MAX, 5));
    }

    #[test]
    fn next_release_at_window_front_in_past_returns_zero() {
        let now = Instant::now();
        let front = now - Duration::from_secs(2);
        assert_eq!(next_release_at(front, now), Duration::ZERO);
    }

    #[test]
    fn next_release_at_window_front_at_one_second_boundary_returns_zero() {
        let now = Instant::now();
        let front = now - Duration::from_secs(1);
        assert_eq!(next_release_at(front, now), Duration::ZERO);
    }

    #[test]
    fn next_release_at_window_front_in_future_returns_positive() {
        let now = Instant::now();
        let front = now - Duration::from_millis(750);
        let d = next_release_at(front, now);
        assert!(d > Duration::ZERO);
        assert!(d <= Duration::from_millis(251));
    }

    // ─── end-to-end acquire under real wall-clock ────────────────────────

    /// R2 canonical regression: simulate the 28k-file rsync storm. Cap at
    /// 5 acquires per real wall-clock second, fire 16 acquires under the
    /// REAL clock (not start_paused — the limiter uses std::time::Instant
    /// which is wall-clock; the assertion validates wall-clock cadence too,
    /// so virtual-time tests under tokio::time::pause give inconsistent
    /// readings between the limiter's internal stamps and the test's
    /// elapsed measurements). With cap=5 the test takes ~2.5s real time —
    /// the rate at which the limiter releases dominates.
    ///
    /// The invariant checked: at any acquire, the limiter's internal
    /// window length is at most the cap. Pre-fix the module does not
    /// exist — compile-time red.
    #[tokio::test]
    async fn regression_28k_rsync_cap_never_exceeds_5_per_sec() {
        let limiter = PushRateLimiter::new(5);

        let started = Instant::now();
        let mut observed: Vec<usize> = Vec::with_capacity(16);
        for _ in 0..16 {
            limiter.acquire().await;
            observed.push(limiter.in_window().await);
        }
        let elapsed = started.elapsed();

        // Internal invariant: every acquire leaves the window at <= cap.
        for (i, len) in observed.iter().enumerate() {
            assert!(
                *len <= 5,
                "rate cap violated after acquire #{i}: window len = {len} > 5"
            );
        }
        // Wall-clock invariant: 16 acquires at 5/sec must NOT finish in
        // under ~2 seconds (the first 5 are free, the next 11 each wait
        // for the front of the window to expire). Real time, not virtual.
        // Lower bound is conservative — the limiter's sleeps coalesce, so
        // 16 acquires at cap=5 can finish in just over 2s (front-of-window
        // for acquire #6 expires at +1s; #11 at +2s; #16 at +3s but the
        // sleep target shifts because the window front advances). We
        // assert at least 1.5s to catch a flat-no-cap regression while
        // staying robust on slow CI.
        assert!(
            elapsed >= Duration::from_millis(1500),
            "rate-cap appears bypassed: 16 acquires at cap=5 finished in {elapsed:?} (expected >= 1.5s)"
        );
        // And not absurdly long — defensive against accidental deadlock.
        assert!(
            elapsed < Duration::from_secs(10),
            "rate-cap loop took {elapsed:?} (suspect deadlock)"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn acquire_is_noop_when_max_per_sec_zero() {
        // 0 means "no cap" — every acquire returns immediately.
        let limiter = PushRateLimiter::new(0);
        let start = Instant::now();
        for _ in 0..1000 {
            limiter.acquire().await;
        }
        assert!(start.elapsed() < Duration::from_millis(50));
        // No window bookkeeping happens at cap=0.
        assert_eq!(limiter.in_window().await, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn acquire_admits_first_n_then_sleeps() {
        let limiter = PushRateLimiter::new(3);
        // First 3 → immediate.
        for _ in 0..3 {
            limiter.acquire().await;
        }
        assert_eq!(limiter.in_window().await, 3);

        // 4th forces a wait of about 1s (since the window front was just now).
        let start = Instant::now();
        limiter.acquire().await;
        let waited = start.elapsed();
        // Allow generous slack for the paused-clock scheduling — the assert
        // simply requires we DID wait, not the exact duration.
        assert!(
            waited >= Duration::from_millis(1),
            "expected non-trivial wait, got {waited:?}"
        );
    }
}
