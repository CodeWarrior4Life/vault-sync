//! TKT-8fc0b2a4 (WS-A4/A5): the ONE auth-failure retry schedule.
//!
//! On 2026-07-07 the Trinity subscriber was soft-revoked. Every request 401'd,
//! the SSE loop returned `Err` by design ("not retrying"), the push drain ACKED
//! the journal entries it could not deliver, and delivery stayed dead for three
//! days while every liveness instrument read alive. There is no server-side
//! re-register endpoint yet (Nexus PR #177 is unmerged), and a hard revoke is
//! never recovered by retrying (recovery is the server un-revoking or a
//! re-pair), so the daemon's job is: FAIL LOUD, KEEP THE CURSOR, AWAIT
//! CREDENTIAL RECOVERY - never die, retry on a bounded schedule, be visible,
//! and resume incrementally (SSE from the kept cursor; push via the next
//! reconciliation pass, since the rejected entry is acked per Sync Contract
//! I47) once the server accepts the token again.
//!
//! The schedule is a pure function so both consumers (the SSE loop and the
//! push drain gate) share it and it is unit-testable without a clock:
//! `base * 2^attempt`, capped at `cap`, then jittered by up to +/-10% so a
//! fleet revoked at the same instant does not retry in lock-step.
//! Production: 30 s, 60 s, 120 s, 240 s, 480 s, then 600 s forever (+/-10%).

use std::time::Duration;

/// First retry delay after an auth failure.
pub const AUTH_BACKOFF_BASE: Duration = Duration::from_secs(30);
/// Longest retry delay while the auth failure persists.
pub const AUTH_BACKOFF_CAP: Duration = Duration::from_secs(600);
/// Jitter half-width as a fraction of the (capped) delay.
pub const AUTH_BACKOFF_JITTER: f64 = 0.10;
/// How often the ERROR line is repeated while the failure persists (one on
/// the transition, then one per this interval; every other attempt is DEBUG).
pub const AUTH_ERROR_REPEAT: Duration = Duration::from_secs(3600);

/// The delay before retry number `attempt` (0-based), for a schedule starting
/// at `base`, doubling, capped at `cap`, with +/-10% jitter driven by
/// `jitter_unit` in `[-1.0, 1.0]` (the caller supplies the randomness; tests
/// pass 0.0 for the un-jittered value). Overflow-safe for any `attempt`.
pub fn auth_retry_delay(attempt: u32, base: Duration, cap: Duration, jitter_unit: f64) -> Duration {
    let factor = 1u32.checked_shl(attempt).unwrap_or(u32::MAX);
    let capped = base.saturating_mul(factor).min(cap);
    let unit = jitter_unit.clamp(-1.0, 1.0);
    let scale = 1.0 + AUTH_BACKOFF_JITTER * unit;
    Duration::from_secs_f64((capped.as_secs_f64() * scale).max(0.0))
}

/// A jitter unit in `[-1.0, 1.0]` from the clock's sub-second nanoseconds. Not
/// cryptographic; it only has to de-synchronise hosts revoked together.
pub fn clock_jitter_unit() -> f64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    // Fold 30 bits of nanos into [0, 2001) and recentre on zero.
    ((nanos % 2001) as f64 / 1000.0) - 1.0
}

#[cfg(test)]
mod tests {
    use super::*;

    const S: fn(u64) -> Duration = Duration::from_secs;

    /// The production schedule, un-jittered: 30, 60, 120, 240, 480, 600, 600...
    #[test]
    fn production_schedule_doubles_from_30s_and_caps_at_10min() {
        let d = |n| auth_retry_delay(n, AUTH_BACKOFF_BASE, AUTH_BACKOFF_CAP, 0.0);
        assert_eq!(d(0), S(30));
        assert_eq!(d(1), S(60));
        assert_eq!(d(2), S(120));
        assert_eq!(d(3), S(240));
        assert_eq!(d(4), S(480));
        assert_eq!(d(5), S(600), "960 is capped to the 10 min ceiling");
        assert_eq!(d(6), S(600));
        assert_eq!(
            d(40),
            S(600),
            "a shift past u32 width saturates, never overflows"
        );
        assert_eq!(d(u32::MAX), S(600));
    }

    /// Jitter is +/-10% of the CAPPED delay, so the worst case at the cap is
    /// 540 s..660 s and the first retry is 27 s..33 s.
    #[test]
    fn jitter_is_plus_minus_ten_percent_of_the_capped_delay() {
        let base = AUTH_BACKOFF_BASE;
        let cap = AUTH_BACKOFF_CAP;
        assert_eq!(auth_retry_delay(0, base, cap, -1.0), S(27));
        assert_eq!(auth_retry_delay(0, base, cap, 1.0), S(33));
        assert_eq!(auth_retry_delay(9, base, cap, -1.0), S(540));
        assert_eq!(auth_retry_delay(9, base, cap, 1.0), S(660));
        // Out-of-range units are clamped, never amplified.
        assert_eq!(auth_retry_delay(0, base, cap, 7.0), S(33));
        assert_eq!(auth_retry_delay(0, base, cap, -7.0), S(27));
    }

    /// A test-sized schedule (tiny base) keeps the same shape, so integration
    /// tests can run the real loop in milliseconds.
    #[test]
    fn tiny_base_keeps_the_shape() {
        let ms = Duration::from_millis;
        assert_eq!(auth_retry_delay(0, ms(10), ms(50), 0.0), ms(10));
        assert_eq!(auth_retry_delay(1, ms(10), ms(50), 0.0), ms(20));
        assert_eq!(auth_retry_delay(2, ms(10), ms(50), 0.0), ms(40));
        assert_eq!(auth_retry_delay(3, ms(10), ms(50), 0.0), ms(50));
    }

    #[test]
    fn clock_jitter_unit_is_in_range() {
        for _ in 0..1000 {
            let u = clock_jitter_unit();
            assert!((-1.0..=1.0).contains(&u), "{u}");
        }
    }
}
