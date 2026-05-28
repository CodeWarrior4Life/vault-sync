//! S477 v0.3.8 (D) — Periodic background reconciliation backstop.
//!
//! ## Why
//!
//! v0.3 vault-sync is event-driven: SSE consumer materializes server pushes,
//! file_watcher queues local edits for push, and the catchup-on-reconnect
//! protocol (Phase A of v0.3.8) papers over single-restart gaps. But
//! systemic drift between hosts still accumulates from edge cases:
//!
//! - SSE events emitted during long downtime windows (server-side LSN gap
//!   bigger than catchup buffer).
//! - Materializer write silently no-ops (path-traversal guard or integrity
//!   check fails without tray surface).
//! - Pre-v0.3.6 cache rows in the bare-path namespace that v0.3.7+ daemons
//!   never fetch (separate cleanup task (E) handles the data-side).
//!
//! Without a periodic "am I in sync with the server?" sweep, lost notes
//! stay lost. This module is that sweep.
//!
//! ## How
//!
//! Reuses `VerifyRepair::run()` — the same machinery the owner-invoked
//! "Verify and repair" tray menu item runs. The user-facing V&R reports
//! to the wizard via a Tauri command; this module wraps the same call in
//! a long-running task that:
//!
//! 1. Honors `VAULT_SYNC_DISABLE_RECON=1` (owner kill switch).
//! 2. Reads cadence from `VAULT_SYNC_RECON_INTERVAL_SECS` (default 600s).
//! 3. Skips the first tick to avoid the startup race with SSE / push pipe.
//! 4. On each tick: spawn a recon pass, log per-action results, fold
//!    pulls + pushes into the tray counters, sleep until next tick.
//!
//! ## Action semantics (mirrors VerifyRepair)
//!
//! - **Push** action = local has-it-or-newer; queue via push_journal.
//!   Counted toward `recon_pushes_total`.
//! - **Pull** action = server has-it-and-local-missing; logged but NOT
//!   fetched here — the SSE consumer materializes it on its next event
//!   round-trip. Counted toward `recon_pulls_total`.
//! - **Skip** = identical; no-op.
//!
//! RASP fence + scope filtering happen inside VerifyRepair's
//! `build_local_manifest_parallel`; the recon sweep inherits those guards.
//!
//! ## Tests
//!
//! Unit-level behavior (env var parsing, kill switch, tray-counter
//! folding) lives in the bottom `#[cfg(test)]` block. End-to-end
//! reconciliation against a real api/journal/materializer lives in
//! `tests/test_reconciliation.rs` and is gated by `--ignored` because it
//! requires a running Nexus.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;

use crate::api_client::ApiClient;
use crate::push_journal::PushJournal;
use crate::tray_state::SharedTrayState;
use crate::verify_repair::{VerifyRepair, VerifyRepairConfig, VerifyRepairError, VerifyRepairReport};

/// Default cadence between reconciliation passes when the env var isn't
/// set. 10 minutes — long enough to be idle-friendly, short enough that
/// any drift surfaces within the same workday.
const DEFAULT_RECON_INTERVAL_SECS: u64 = 600;

/// Env var name for the owner kill switch.
pub const ENV_DISABLE: &str = "VAULT_SYNC_DISABLE_RECON";

/// Env var name for the cadence override.
pub const ENV_INTERVAL_SECS: &str = "VAULT_SYNC_RECON_INTERVAL_SECS";

/// Read the cadence from the env, with the 10-minute default.
///
/// Treats malformed / zero values as "use default" so a misconfigured
/// env var doesn't spin the loop at 0s.
pub fn read_interval(env: &dyn EnvReader) -> Duration {
    let raw = env.get(ENV_INTERVAL_SECS);
    let secs = raw
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(DEFAULT_RECON_INTERVAL_SECS);
    Duration::from_secs(secs)
}

/// True iff the kill switch env var is set to ANY non-empty value.
/// Mirrors `std::env::var(...).is_ok()` plus an explicit empty-string
/// guard so `VAULT_SYNC_DISABLE_RECON=` (unset by setting empty) doesn't
/// accidentally disable.
pub fn is_disabled(env: &dyn EnvReader) -> bool {
    env.get(ENV_DISABLE).is_some_and(|v| !v.is_empty())
}

/// Tiny trait so the env-reading logic is testable without
/// `unsafe { std::env::set_var(...) }` (which is now an `unsafe` op as of
/// Rust 2024 + breaks under tokio multi-thread tests).
pub trait EnvReader {
    fn get(&self, key: &str) -> Option<String>;
}

/// Production env reader — reads from process env.
pub struct ProcessEnv;
impl EnvReader for ProcessEnv {
    fn get(&self, key: &str) -> Option<String> {
        std::env::var(key).ok()
    }
}

/// Test env reader — backed by a HashMap supplied at construction time.
#[cfg(test)]
pub struct MapEnv(pub std::collections::HashMap<String, String>);
#[cfg(test)]
impl EnvReader for MapEnv {
    fn get(&self, key: &str) -> Option<String> {
        self.0.get(key).cloned()
    }
}

/// Run ONE reconciliation pass.
///
/// Calls `VerifyRepair::run()` against the same vault/api/journal the
/// rest of the daemon uses, then folds the resulting report's counts
/// into the tray state.
///
/// Returns the report (also surfaced via tracing::info on the way out)
/// so callers can choose to emit additional telemetry. Errors are
/// propagated unchanged.
///
/// Logging contract (mandate: NO silent skips):
/// - INFO at start (with the configured cadence the caller is enforcing).
/// - INFO per action: pulled-list / pushed-count / in-sync-count.
/// - WARN on any error path.
pub async fn run_reconciliation_pass(
    vault_root: PathBuf,
    api: Arc<ApiClient>,
    journal: Arc<Mutex<PushJournal>>,
    device_id: String,
    tray_state: SharedTrayState,
) -> Result<VerifyRepairReport, VerifyRepairError> {
    if let Ok(mut w) = tray_state.write() {
        w.set_recon_in_progress(true);
    }

    let vr = VerifyRepair::new(
        vault_root,
        api,
        journal,
        device_id,
        VerifyRepairConfig::default(),
    );

    tracing::info!("reconciliation: pass starting");
    let result = vr.run().await;

    if let Ok(mut w) = tray_state.write() {
        w.set_recon_in_progress(false);
    }

    match &result {
        Ok(report) => {
            // VerifyRepairReport.add_count = Pull actions = server-only paths
            // VerifyRepairReport.modify_count = Push actions = local-only or
            //   hash-mismatch paths
            tracing::info!(
                files_scanned = report.files_scanned,
                files_in_sync = report.files_in_sync,
                pulls_pending = report.add_count,
                pushes_queued = report.modify_count,
                substrate_refused = report.substrate_refused_count,
                elapsed_ms = report.elapsed_ms,
                "reconciliation: pass complete"
            );
            if let Ok(mut w) = tray_state.write() {
                w.note_recon_pass(report.add_count as u64, report.modify_count as u64);
            }
            // Surface a sample of the pulled paths in the log so drift is
            // visible without parsing tray counters.
            for p in &report.add_paths_sample {
                tracing::info!(path = %p, "reconciliation: pull pending (SSE consumer materializes)");
            }
            for p in &report.modify_paths_sample {
                tracing::info!(path = %p, "reconciliation: push queued");
            }
        }
        Err(e) => {
            tracing::warn!(error = %e, "reconciliation: pass failed");
        }
    }

    result
}

/// Spawn the long-running recon task. Returns the JoinHandle so the
/// caller can keep it alive (the inner loop runs for the daemon's
/// lifetime). On kill switch the task logs once and returns immediately.
///
/// Called from `lib::spawn_push_pipeline`'s success path after the
/// push_journal handle is open.
pub fn spawn_reconciliation_task(
    vault_root: PathBuf,
    api: Arc<ApiClient>,
    journal: Arc<Mutex<PushJournal>>,
    device_id: String,
    tray_state: SharedTrayState,
) -> tauri::async_runtime::JoinHandle<()> {
    tauri::async_runtime::spawn(async move {
        let env = ProcessEnv;
        if is_disabled(&env) {
            tracing::info!(
                "reconciliation: disabled via {} env var; backstop task exiting",
                ENV_DISABLE
            );
            return;
        }
        let interval = read_interval(&env);
        tracing::info!(
            interval_secs = interval.as_secs(),
            "reconciliation: backstop armed"
        );

        let mut tick = tokio::time::interval(interval);
        // First tick fires immediately — skip it so we don't race the
        // SSE consumer + push pipeline that are still wiring up. The
        // catchup-on-reconnect handles the immediate-restart window;
        // recon's job is the long-tail drift, not startup parity.
        tick.tick().await;
        loop {
            tick.tick().await;
            let _ = run_reconciliation_pass(
                vault_root.clone(),
                Arc::clone(&api),
                Arc::clone(&journal),
                device_id.clone(),
                tray_state.clone(),
            )
            .await;
            // Errors are already WARN-logged by run_reconciliation_pass;
            // the loop swallows so a transient API failure doesn't kill
            // the backstop entirely.
        }
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn env_with(pairs: &[(&str, &str)]) -> MapEnv {
        let mut m = HashMap::new();
        for (k, v) in pairs {
            m.insert((*k).to_string(), (*v).to_string());
        }
        MapEnv(m)
    }

    #[test]
    fn read_interval_defaults_when_env_missing() {
        let env = env_with(&[]);
        assert_eq!(read_interval(&env), Duration::from_secs(600));
    }

    #[test]
    fn read_interval_uses_env_when_valid() {
        let env = env_with(&[(ENV_INTERVAL_SECS, "60")]);
        assert_eq!(read_interval(&env), Duration::from_secs(60));
    }

    #[test]
    fn read_interval_falls_back_to_default_on_zero() {
        // Guard against accidentally spinning the loop at 0s.
        let env = env_with(&[(ENV_INTERVAL_SECS, "0")]);
        assert_eq!(read_interval(&env), Duration::from_secs(600));
    }

    #[test]
    fn read_interval_falls_back_to_default_on_malformed() {
        let env = env_with(&[(ENV_INTERVAL_SECS, "abc")]);
        assert_eq!(read_interval(&env), Duration::from_secs(600));
    }

    #[test]
    fn is_disabled_false_when_env_missing() {
        let env = env_with(&[]);
        assert!(!is_disabled(&env));
    }

    #[test]
    fn is_disabled_false_when_env_empty_string() {
        // VAULT_SYNC_DISABLE_RECON= (empty value) must NOT disable —
        // explicit guard so dotenv files that scribble empty values
        // don't surprise-disable the backstop.
        let env = env_with(&[(ENV_DISABLE, "")]);
        assert!(!is_disabled(&env));
    }

    #[test]
    fn is_disabled_true_when_env_set_to_one() {
        let env = env_with(&[(ENV_DISABLE, "1")]);
        assert!(is_disabled(&env));
    }

    #[test]
    fn is_disabled_true_when_env_set_to_true() {
        let env = env_with(&[(ENV_DISABLE, "true")]);
        assert!(is_disabled(&env));
    }

    #[test]
    fn tray_state_note_recon_pass_folds_counts() {
        use crate::tray_state::TrayState;
        let mut s = TrayState::new("sub".into(), "url".into(), PathBuf::from("/v"));
        assert_eq!(s.recon_pulls_total, 0);
        assert_eq!(s.recon_pushes_total, 0);
        assert!(s.last_recon_at.is_none());

        s.note_recon_pass(3, 2);
        assert_eq!(s.recon_pulls_total, 3);
        assert_eq!(s.recon_pushes_total, 2);
        assert!(s.last_recon_at.is_some());

        s.note_recon_pass(5, 1);
        assert_eq!(s.recon_pulls_total, 8);
        assert_eq!(s.recon_pushes_total, 3);
    }

    #[test]
    fn tray_state_recon_in_progress_round_trips() {
        use crate::tray_state::TrayState;
        let mut s = TrayState::new("sub".into(), "url".into(), PathBuf::from("/v"));
        assert!(!s.recon_in_progress);
        s.set_recon_in_progress(true);
        assert!(s.recon_in_progress);
        s.set_recon_in_progress(false);
        assert!(!s.recon_in_progress);
    }
}
