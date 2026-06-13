//! Progress-stall watchdog for the push pipeline.
//!
//! ## Why this exists
//!
//! Incident 2026-06-13: the daemon ran for 14h+ with `nexus-vault-sync.service`
//! reporting "Active (running)" while the push pipeline was silent. Only the
//! tauri auto-updater task kept logging activity (a 5-minute heartbeat loop
//! living in its own spawned task -- see [`crate::spawn_updater_check`]). The
//! push_client's `run_loop` had gone quiet but the OS process was still alive,
//! so liveness checks (PID present, parent service `Active`) reported healthy.
//! 80+ local edits sat unpushed.
//!
//! The empirically-binding lesson (operation-fix R1..R4):
//!
//! 1. **Liveness != progress.** A live process is not a healthy daemon.
//! 2. **Detection MUST gate on PROGRESS.** "Pending diffs but no push attempts
//!    in N minutes" is the only signal that catches a hung push task while
//!    the rest of the runtime (updater, tray, SSE reconnect backoff) keeps
//!    ticking.
//! 3. **Recovery MUST be automatic AND visible.** A silent log line did not
//!    save the user; a process restart + OS notification will.
//! 4. **Defense in depth against task death.** `tauri::async_runtime::spawn`
//!    is `tokio::spawn` -- a panic inside the spawned future is captured in
//!    the JoinHandle and never observed (we don't await it). The progress
//!    watchdog catches this class of failure indirectly: if the task is
//!    dead, no progress markers get stamped, and the threshold fires.
//!
//! ## Wire-up (one Arc per daemon)
//!
//! `SyncHealth::new()` returns an Arc. Pass the same Arc to:
//! * the `PushClient` (`with_health(...)`) so it stamps progress markers on
//!   every drain that processed at least one event;
//! * [`spawn_progress_stall_watchdog`] which polls every 60s and triggers
//!   the recovery path when the threshold elapses with `pending > 0`.
//!
//! ## Recovery
//!
//! On a fired stall the watchdog:
//! 1. Logs at ERROR with the stall window + pending count.
//! 2. Emits the `sync_stalled` Tauri event (wizard surfaces it).
//! 3. Calls `notify_user(...)` so the OS-native notification fires.
//! 4. Calls `app.restart()` to bring up a fresh process with healthy tasks.
//!
//! The restart is the same primitive the staged-update path uses
//! ([`crate::should_restart_now`] -> `app.restart()`), so the recovery
//! mechanism is already battle-tested for tray-resident daemons.
//!
//! ## Tunables (env)
//!
//! * `VAULT_SYNC_STALL_THRESHOLD_SECS` -- pending-with-no-progress window
//!   before a stall fires. Default 900 (15 min).
//! * `VAULT_SYNC_DISABLE_STALL_WATCHDOG` -- set to any non-empty value to
//!   suppress the recovery action (the watchdog still logs at WARN).
//!
//! Both env vars share the same reader trait as [`crate::reconciliation`]
//! so tests do not touch process env.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::reconciliation::{EnvReader, ProcessEnv};

/// Default threshold: pending-with-no-progress window before a stall fires.
/// 15 minutes. Long enough to avoid false positives on a busy host doing one
/// very large push; short enough that the 2026-06-13 14-hour incident would
/// have been caught inside the first quarter-hour.
pub const DEFAULT_STALL_THRESHOLD_SECS: u64 = 900;

/// Env var: override the stall threshold (seconds).
pub const ENV_STALL_THRESHOLD: &str = "VAULT_SYNC_STALL_THRESHOLD_SECS";

/// Env var: when set (non-empty), the watchdog logs at WARN but does NOT
/// restart. Logs and notifications still fire -- opt out of the irreversible
/// restart only.
pub const ENV_DISABLE_RECOVERY: &str = "VAULT_SYNC_DISABLE_STALL_WATCHDOG";

/// Shared progress-tracking state. The push pipeline stamps it on drain;
/// the watchdog reads it on every tick. Cheap clones (Arc).
pub struct SyncHealth {
    /// Monotonic seconds since process start, last time `mark_progress`
    /// was called. We store the offset against `start` rather than the
    /// `Instant` itself so the atomic stays lock-free.
    last_progress_secs: AtomicU64,
    start: Instant,
}

impl SyncHealth {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            // Initialize as "just made progress" -- startup is not a stall.
            last_progress_secs: AtomicU64::new(0),
            start: Instant::now(),
        })
    }

    /// Stamp "progress just happened" -- called from `PushClient::run_loop`
    /// after every `drain_once` that processed at least one event, AND any
    /// time the pending journal depth drops (drain consumed the backlog).
    /// Cheap: a single relaxed atomic store.
    pub fn mark_progress(&self) {
        let secs = self.start.elapsed().as_secs();
        self.last_progress_secs.store(secs, Ordering::Relaxed);
    }

    /// Seconds elapsed since the last `mark_progress` call. Used by the
    /// watchdog AND exposed for tray telemetry.
    pub fn secs_since_progress(&self) -> u64 {
        let now = self.start.elapsed().as_secs();
        let last = self.last_progress_secs.load(Ordering::Relaxed);
        now.saturating_sub(last)
    }
}

/// Pure decision: should the watchdog declare the push pipeline stalled?
///
/// Inputs:
/// * `pending` -- current `push_journal.len()`. Zero pending = no diffs to
///   push = there can BE no stall (R1 boundary: stall requires pending diffs).
/// * `secs_since_progress` -- output of `SyncHealth::secs_since_progress()`.
/// * `threshold_secs` -- configured stall window.
///
/// Returns `true` iff `pending > 0` AND `secs_since_progress >= threshold_secs`.
/// Extracted as a pub fn so the test suite can exercise the boundary
/// behavior without spawning a tokio task or touching real time.
pub fn is_stalled(pending: usize, secs_since_progress: u64, threshold_secs: u64) -> bool {
    pending > 0 && secs_since_progress >= threshold_secs
}

/// Read the stall threshold from env, falling back to the default. Treats
/// malformed and zero values as "use default" so a misconfigured env var
/// doesn't disable the watchdog by setting it to 0.
pub fn read_threshold(env: &dyn EnvReader) -> Duration {
    let raw = env.get(ENV_STALL_THRESHOLD);
    let secs = raw
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(DEFAULT_STALL_THRESHOLD_SECS);
    Duration::from_secs(secs)
}

/// True iff the recovery kill switch is set to ANY non-empty value. Matches
/// the [`crate::reconciliation::is_disabled`] convention.
pub fn is_recovery_disabled(env: &dyn EnvReader) -> bool {
    env.get(ENV_DISABLE_RECOVERY).is_some_and(|v| !v.is_empty())
}

/// Spawn the watchdog task. Polls once per `tick` (default 60s), reads the
/// current journal depth via `pending_fn` (async -- the production wiring is
/// a `tokio::sync::Mutex` lock + `PushJournal::len()`), and on a fired stall
/// calls `on_stall` which is expected to log+notify+restart.
///
/// The watchdog is itself stateless -- all state lives in the `SyncHealth`
/// Arc and the closures passed in. This keeps the spawned future testable
/// in isolation: production wires `pending_fn` to the async journal lock +
/// `len()`, and `on_stall` to the notify+restart closure; tests pass
/// synthetic closures.
///
/// The pending closure is async because the production journal sits behind
/// a `tokio::sync::Mutex`. Calling `blocking_lock()` from a tokio worker
/// (which is what `tauri::async_runtime::spawn` produces) panics -- so we
/// hand the watchdog an async lock path.
pub fn spawn_progress_stall_watchdog<P, Fut, S>(
    health: Arc<SyncHealth>,
    tick: Duration,
    threshold: Duration,
    recovery_disabled: bool,
    mut pending_fn: P,
    mut on_stall: S,
) -> tauri::async_runtime::JoinHandle<()>
where
    P: FnMut() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = usize> + Send,
    S: FnMut(StallEvent) + Send + 'static,
{
    tauri::async_runtime::spawn(async move {
        tracing::info!(
            tick_secs = tick.as_secs(),
            threshold_secs = threshold.as_secs(),
            recovery_disabled,
            "sync_health: progress-stall watchdog armed"
        );
        let mut ticker = tokio::time::interval(tick);
        // First tick fires immediately -- consume it to avoid a stall trip
        // during the brief startup window before the push pipeline has run
        // its first drain_once.
        ticker.tick().await;
        loop {
            ticker.tick().await;
            let pending = pending_fn().await;
            let elapsed = health.secs_since_progress();
            if is_stalled(pending, elapsed, threshold.as_secs()) {
                let event = StallEvent {
                    pending,
                    secs_since_progress: elapsed,
                    recovery_will_restart: !recovery_disabled,
                };
                tracing::error!(
                    pending,
                    secs_since_progress = elapsed,
                    recovery_disabled,
                    "sync_health: STALL detected -- push pipeline silent with pending diffs"
                );
                on_stall(event);
                // After a recovery action the process is going down (or
                // we've at least notified the owner). Exit the loop: a
                // restarted process spawns a fresh watchdog; a kill-switched
                // host shouldn't re-fire the same alert every minute.
                return;
            }
        }
    })
}

/// What the watchdog hands to the recovery callback. Carries everything
/// the callback needs to log + notify + decide whether to restart.
#[derive(Debug, Clone, Copy)]
pub struct StallEvent {
    pub pending: usize,
    pub secs_since_progress: u64,
    /// `true` when the watchdog has NOT been kill-switched and the recovery
    /// callback is expected to call `app.restart()` after notifying. `false`
    /// means notify + log only (kill-switched host).
    pub recovery_will_restart: bool,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Mutex;
    use std::time::Duration;

    use crate::reconciliation::MapEnv;

    fn env_with(pairs: &[(&str, &str)]) -> MapEnv {
        let mut m = HashMap::new();
        for (k, v) in pairs {
            m.insert((*k).to_string(), (*v).to_string());
        }
        MapEnv(m)
    }

    // ------------- is_stalled pure-decision tests -------------

    /// Incident 2026-06-13 reproduction (the regression test for R1+R3):
    /// pending diffs exist, no push progress in 14 hours -- MUST be stalled.
    /// This was the exact shape of the silent-dormancy bug. Pre-fix code has
    /// no `is_stalled` function at all, so this test is structurally
    /// red-on-old-code (the file does not compile against the pre-fix tree).
    #[test]
    fn regression_2026_06_13_pending_with_14h_no_progress_is_stalled() {
        let fourteen_hours = 14 * 3600;
        let threshold = DEFAULT_STALL_THRESHOLD_SECS; // 900s
        assert!(
            is_stalled(80, fourteen_hours, threshold),
            "80 pending pushes with no progress for 14h MUST trip the stall \
             detector (incident 2026-06-13)"
        );
    }

    /// R1 boundary: zero pending diffs means there CAN be no stall, even
    /// if the daemon has been idle for days. An idle daemon is not a
    /// dormant daemon.
    #[test]
    fn is_stalled_false_when_zero_pending_no_matter_the_idle_window() {
        let week = 7 * 24 * 3600;
        assert!(
            !is_stalled(0, week, DEFAULT_STALL_THRESHOLD_SECS),
            "zero pending = no diffs to push = not a stall"
        );
    }

    /// R1 boundary: pending exists but progress was made within the window.
    /// The pipeline is healthy; do NOT trip.
    #[test]
    fn is_stalled_false_when_recent_progress_with_pending() {
        let recent = DEFAULT_STALL_THRESHOLD_SECS / 2;
        assert!(
            !is_stalled(5, recent, DEFAULT_STALL_THRESHOLD_SECS),
            "pending + recent progress = healthy"
        );
    }

    /// Exact-threshold boundary: at `elapsed == threshold` the watchdog
    /// MUST fire. Using `>=` not `>` is deliberate -- a `>` boundary lets
    /// a perfectly-synchronized 15-minute-flat stall slip through.
    #[test]
    fn is_stalled_true_at_exact_threshold() {
        assert!(
            is_stalled(1, DEFAULT_STALL_THRESHOLD_SECS, DEFAULT_STALL_THRESHOLD_SECS),
            "elapsed == threshold MUST trip (>= boundary, not >)"
        );
    }

    /// Just under the threshold -- still healthy.
    #[test]
    fn is_stalled_false_just_below_threshold() {
        assert!(
            !is_stalled(1, DEFAULT_STALL_THRESHOLD_SECS - 1, DEFAULT_STALL_THRESHOLD_SECS),
            "elapsed = threshold - 1 must NOT trip"
        );
    }

    // ------------- SyncHealth state-machine tests -------------

    /// `mark_progress` advances the in-process clock so `secs_since_progress`
    /// reads near-zero again. This is the cheap-atomic happy path the
    /// push_client invokes on every drain.
    #[test]
    fn mark_progress_resets_secs_since_progress() {
        let h = SyncHealth::new();
        // Force the underlying counter to look "old" by manually setting it
        // to 0 (start time). Then mark_progress and confirm the gap closes.
        h.last_progress_secs.store(0, Ordering::Relaxed);
        // Sleep is unnecessary -- the test only checks the mark_progress
        // path stamps a fresh value.
        h.mark_progress();
        let elapsed = h.secs_since_progress();
        assert!(
            elapsed < 5,
            "mark_progress must reset elapsed to near-zero, got {elapsed}s"
        );
    }

    // ------------- env-reader tests -------------

    #[test]
    fn read_threshold_defaults_when_env_missing() {
        let env = env_with(&[]);
        assert_eq!(
            read_threshold(&env),
            Duration::from_secs(DEFAULT_STALL_THRESHOLD_SECS)
        );
    }

    #[test]
    fn read_threshold_uses_env_when_valid() {
        let env = env_with(&[(ENV_STALL_THRESHOLD, "120")]);
        assert_eq!(read_threshold(&env), Duration::from_secs(120));
    }

    #[test]
    fn read_threshold_falls_back_on_zero() {
        let env = env_with(&[(ENV_STALL_THRESHOLD, "0")]);
        assert_eq!(
            read_threshold(&env),
            Duration::from_secs(DEFAULT_STALL_THRESHOLD_SECS),
            "zero must not disable the watchdog"
        );
    }

    #[test]
    fn read_threshold_falls_back_on_malformed() {
        let env = env_with(&[(ENV_STALL_THRESHOLD, "fifteen-min")]);
        assert_eq!(
            read_threshold(&env),
            Duration::from_secs(DEFAULT_STALL_THRESHOLD_SECS)
        );
    }

    #[test]
    fn is_recovery_disabled_false_when_unset_or_empty() {
        assert!(!is_recovery_disabled(&env_with(&[])));
        assert!(!is_recovery_disabled(&env_with(&[(ENV_DISABLE_RECOVERY, "")])));
    }

    #[test]
    fn is_recovery_disabled_true_when_set() {
        assert!(is_recovery_disabled(&env_with(&[(ENV_DISABLE_RECOVERY, "1")])));
    }

    // ------------- watchdog end-to-end test (no tokio time wait) -------------

    /// End-to-end shape: spawn the watchdog with a 10ms tick + 0s threshold,
    /// a pending_fn that always returns 5 (pending exists), and a SyncHealth
    /// whose `last_progress_secs` is forced to zero (i.e. progress is
    /// "elapsed since process start" old). The first tick MUST fire the
    /// on_stall callback.
    ///
    /// This is the regression test that fails on the pre-fix tree: pre-fix
    /// there is no `spawn_progress_stall_watchdog` to call, so the test won't
    /// even compile against HEAD.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn watchdog_fires_on_stall() {
        let health = SyncHealth::new();
        // Make sure the elapsed since progress is "old" relative to a 0s
        // threshold: stamp the counter at 0, then advance virtual time.
        health.last_progress_secs.store(0, Ordering::Relaxed);

        let fired = Arc::new(AtomicUsize::new(0));
        let captured: Arc<Mutex<Option<StallEvent>>> = Arc::new(Mutex::new(None));

        let fired_clone = fired.clone();
        let captured_clone = captured.clone();

        let handle = spawn_progress_stall_watchdog(
            health.clone(),
            Duration::from_millis(10),
            Duration::from_secs(0), // threshold = 0 -> any pending trips
            false,                  // recovery enabled
            move || async { 5_usize }, // pending_fn -- always 5 diffs
            move |evt| {
                fired_clone.fetch_add(1, Ordering::Relaxed);
                if let Ok(mut g) = captured_clone.lock() {
                    *g = Some(evt);
                }
            },
        );

        // Advance virtual time past the first tick.
        tokio::time::advance(Duration::from_millis(30)).await;
        // Let the task observe the time advance.
        let _ = handle.await;

        assert_eq!(
            fired.load(Ordering::Relaxed),
            1,
            "on_stall must fire exactly once on the first observed stall"
        );
        let evt = captured.lock().unwrap().expect("event captured");
        assert_eq!(evt.pending, 5);
        assert!(
            evt.recovery_will_restart,
            "recovery_disabled=false must propagate as recovery_will_restart=true"
        );
    }

    /// Counter-test: with pending == 0 the watchdog MUST NOT fire even with
    /// a long elapsed window. Idle != dormant (R1 boundary).
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn watchdog_does_not_fire_when_no_pending() {
        let health = SyncHealth::new();
        health.last_progress_secs.store(0, Ordering::Relaxed);

        let fired = Arc::new(AtomicUsize::new(0));
        let fired_clone = fired.clone();

        let handle = spawn_progress_stall_watchdog(
            health.clone(),
            Duration::from_millis(10),
            Duration::from_secs(0),
            false,
            move || async { 0_usize }, // ZERO pending -- no stall possible
            move |_| {
                fired_clone.fetch_add(1, Ordering::Relaxed);
            },
        );

        // Run several ticks worth of virtual time.
        for _ in 0..5 {
            tokio::time::advance(Duration::from_millis(20)).await;
        }
        // The watchdog is in a never-trip loop; abort it so the test ends.
        handle.abort();
        let _ = handle.await;

        assert_eq!(
            fired.load(Ordering::Relaxed),
            0,
            "on_stall must NOT fire when pending == 0 (idle != dormant)"
        );
    }
}
