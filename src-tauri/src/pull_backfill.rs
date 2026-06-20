//! Pull-backfill (R6) — full server→local completeness pass.
//!
//! ## Why this exists
//!
//! Incident 2026-06-13: the daemon ran a full editing/sync session and the
//! materializer logged **11,140 skips, 365 refusals, and ZERO create-writes**.
//! Notes that originated on another machine (or on Nexus directly) and never
//! existed locally were never materialized — the owner's invariant ("anything
//! in Nexus is on every subscribing machine") was silently violated.
//!
//! ## Root cause
//!
//! The daemon learns about server changes from exactly two sources, neither of
//! which can surface a server-only note:
//!
//! 1. **The SSE feed** (`sse.rs`) only materializes notes that receive a
//!    *fresh* `enrichment_complete` event. A note already on the server that
//!    gets no new event is never re-emitted.
//! 2. **`reconcile-batch`** (`verify_repair.rs`) only echoes the status of
//!    paths the client SENDS — i.e. paths that already exist locally. Its
//!    "pull" arm overwrites *stale-local* files; it has no notion of a
//!    *missing* one. `reconciliation.rs` even logged detected pulls as
//!    "SSE consumer materializes" and then dropped them on the floor.
//!
//! So a note below the daemon's saved SSE cursor that it never received stays
//! invisible forever. There was **no full enumeration of the server's
//! canonical set anywhere in the daemon.**
//!
//! ## The fix
//!
//! `GET /api/sync/changes?since=0` returns every canonical note for this
//! subscriber's route, paginated by `next_lsn`. This pass walks that
//! enumeration to exhaustion and, for each path that is *missing locally*
//! (and safe + non-substrate), performs the exact two operations the SSE
//! consumer performs: [`ApiClient::fetch_note`] → [`Materializer::write`].
//! `write()` already create-writes missing files via atomic tmp+rename, so the
//! gap is closed without any new write path.
//!
//! It runs once shortly after startup (to close the existing gap immediately)
//! and then on a cadence, sharing the same kill-switch convention as
//! [`crate::reconciliation`] and [`crate::sync_health`].
//!
//! ## Tunables (env)
//!
//! * `VAULT_SYNC_DISABLE_BACKFILL` — any non-empty value disables the pass.
//! * `VAULT_SYNC_BACKFILL_INTERVAL_SECS` — re-run cadence. Default 3600 (1h).

use std::sync::Arc;
use std::time::Duration;

use crate::api_client::{ApiClient, ChangeRow};
use crate::materializer::{MaterializeOutcome, Materializer};
use crate::rasp_fence::{classify_path, PathClassification};
use crate::reconciliation::{EnvReader, ProcessEnv};
use crate::scope::is_safe_path;

/// Env var: disable the backfill pass entirely (logs once, then returns).
pub const ENV_DISABLE_BACKFILL: &str = "VAULT_SYNC_DISABLE_BACKFILL";
/// Env var: re-run cadence in seconds.
pub const ENV_BACKFILL_INTERVAL_SECS: &str = "VAULT_SYNC_BACKFILL_INTERVAL_SECS";
/// Default re-run cadence: hourly. The first pass runs ~30s after startup.
pub const DEFAULT_BACKFILL_INTERVAL_SECS: u64 = 3600;
/// Rows per `/changes` page. Server caps at 5000; 2000 keeps each pair modest.
pub const PAGE_LIMIT: u32 = 2000;
/// Bounded concurrency for fetch+write of missing notes (mirrors
/// `verify_repair`'s server-wins-pull buffer of 4, a touch higher since these
/// are first-time creates with no conflict-stash work).
pub const FETCH_CONCURRENCY: usize = 6;

/// True iff the kill switch is set to any non-empty value.
pub fn is_backfill_disabled(env: &dyn EnvReader) -> bool {
    env.get(ENV_DISABLE_BACKFILL).is_some_and(|v| !v.is_empty())
}

/// Read the re-run cadence from env, falling back to the default. Zero and
/// malformed values fall back (a misconfigured var must not wedge the loop).
pub fn read_backfill_interval(env: &dyn EnvReader) -> Duration {
    let secs = env
        .get(ENV_BACKFILL_INTERVAL_SECS)
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(DEFAULT_BACKFILL_INTERVAL_SECS);
    Duration::from_secs(secs)
}

/// Pure decision: should the backfill PULL this canonical path?
///
/// PULL iff the path is (a) NOT already present locally, (b) safe (no
/// traversal), and (c) not substrate-fenced. Mirrors the guards
/// [`Materializer::write`] applies internally, but evaluated BEFORE fetching
/// the body so we never download a note we would only skip or refuse.
pub fn should_backfill(path: &str, target_exists: bool) -> bool {
    if target_exists {
        return false;
    }
    if !is_safe_path(path) {
        return false;
    }
    if matches!(classify_path(path), PathClassification::Substrate { .. }) {
        return false;
    }
    true
}

/// Pure planning step: given a page of server changes and a local-presence
/// oracle, return the subset of paths to pull. Extracted so the
/// enumeration→filter core — the heart of the R6 fix — is unit-testable
/// without HTTP or a filesystem.
pub fn plan_backfill<F: Fn(&str) -> bool>(changes: &[ChangeRow], target_exists: F) -> Vec<String> {
    changes
        .iter()
        .filter(|row| should_backfill(&row.path, target_exists(&row.path)))
        .map(|row| row.path.clone())
        .collect()
}

/// Outcome counters for one full pass (logged + returned for telemetry/tests).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PullBackfillStats {
    /// Total canonical rows enumerated across all pages.
    pub enumerated: usize,
    /// Notes that were missing locally and got created this pass.
    pub created: usize,
    /// Rows already present locally (no fetch attempted).
    pub already_present: usize,
    /// Rows skipped pre-fetch as unsafe/substrate, OR a post-fetch
    /// `Skipped`/refusal (TOCTOU: appeared after planning).
    pub refused_or_unsafe: usize,
    /// fetch_note or write errors / integrity failures.
    pub failed: usize,
}

/// Run ONE full pull-backfill pass: page `GET /changes` from `since=0` to
/// exhaustion and, for every canonical path missing locally, `fetch_note` +
/// `materializer.write`. Best-effort and idempotent — re-running it is cheap
/// once the gap is closed (everything reads as already-present).
pub async fn run_pull_backfill(
    api: &ApiClient,
    materializer: &Materializer,
    page_limit: u32,
    concurrency: usize,
) -> PullBackfillStats {
    use futures::stream::{self, StreamExt};

    let mut stats = PullBackfillStats::default();
    let mut since: i64 = 0;

    loop {
        let page = match api.get_changes(since, page_limit).await {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(since, error = %format!("{e:?}"), "pull_backfill: get_changes failed; aborting pass");
                break;
            }
        };
        if page.changes.is_empty() {
            break;
        }
        stats.enumerated += page.changes.len();

        // Plan: which paths are genuinely missing locally? (No body fetched yet.)
        let to_pull = plan_backfill(&page.changes, |p| materializer.target_path(p).exists());
        stats.already_present += page.changes.len() - to_pull.len();

        // Execute fetch+write with bounded concurrency. `api`/`materializer`
        // are shared `&` refs (Copy) captured by each future.
        let outcomes: Vec<Result<MaterializeOutcome, ()>> = stream::iter(to_pull)
            .map(|path| async move {
                match api.fetch_note(&path).await {
                    Ok(payload) => materializer.write(&payload).map_err(|e| {
                        tracing::warn!(path = %path, error = %format!("{e:?}"), "pull_backfill: write failed");
                    }),
                    Err(e) => {
                        tracing::warn!(path = %path, error = %format!("{e:?}"), "pull_backfill: fetch_note failed");
                        Err(())
                    }
                }
            })
            .buffer_unordered(concurrency)
            .collect()
            .await;

        for outcome in outcomes {
            match outcome {
                Ok(MaterializeOutcome::Wrote { .. }) | Ok(MaterializeOutcome::Stashed { .. }) => {
                    stats.created += 1
                }
                Ok(MaterializeOutcome::Skipped(_)) => stats.refused_or_unsafe += 1,
                Ok(MaterializeOutcome::IntegrityFailed { .. }) | Err(()) => stats.failed += 1,
            }
        }

        // Advance the cursor. The server advances `next_lsn` past skipped
        // (cross-route) rows, so a page that created nothing still moves us
        // forward. Guard against a non-advancing cursor (would loop forever).
        if page.next_lsn <= since {
            break;
        }
        since = page.next_lsn;
    }

    tracing::info!(
        enumerated = stats.enumerated,
        created = stats.created,
        already_present = stats.already_present,
        refused_or_unsafe = stats.refused_or_unsafe,
        failed = stats.failed,
        "pull_backfill: pass complete"
    );
    stats
}

/// Spawn the long-lived pull-backfill task: wait a short settle window, run an
/// immediate pass to close the existing gap, then re-run every interval. Honors
/// the kill switch. One task per `(api, materializer)` pair — the caller spawns
/// one per sync_root, mirroring `spawn_reconciliation_tasks_for_roots`.
pub fn spawn_pull_backfill_task(
    api: Arc<ApiClient>,
    materializer: Materializer,
) -> tauri::async_runtime::JoinHandle<()> {
    tauri::async_runtime::spawn(async move {
        let env = ProcessEnv;
        if is_backfill_disabled(&env) {
            tracing::info!("pull_backfill: disabled via {ENV_DISABLE_BACKFILL}; not arming");
            return;
        }
        let interval = read_backfill_interval(&env);
        tracing::info!(
            interval_secs = interval.as_secs(),
            "pull_backfill: armed (first pass in ~30s, then per interval)"
        );
        // Let SSE + push pipeline settle before the first full enumeration.
        tokio::time::sleep(Duration::from_secs(30)).await;
        let mut ticker = tokio::time::interval(interval);
        ticker.tick().await; // consume the immediate tick; we run explicitly below
        loop {
            let _ = run_pull_backfill(&api, &materializer, PAGE_LIMIT, FETCH_CONCURRENCY).await;
            ticker.tick().await;
        }
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reconciliation::MapEnv;
    use std::collections::{HashMap, HashSet};

    fn row(path: &str, lsn: i64) -> ChangeRow {
        ChangeRow {
            path: path.into(),
            file_mtime: 0.0,
            modified: String::new(),
            indexed_at: String::new(),
            lsn,
        }
    }

    // ---- should_backfill pure-decision tests ----

    #[test]
    fn should_backfill_pulls_missing_safe_nonsubstrate() {
        assert!(should_backfill("Mainframe/01_Notes/server-only.md", false));
    }

    #[test]
    fn should_backfill_skips_present() {
        assert!(!should_backfill("Mainframe/01_Notes/already-here.md", true));
    }

    #[test]
    fn should_backfill_pulls_former_substrate() {
        // "substrate must sync" (2026-06-20): the fence is lifted, so a missing
        // former-substrate path (02_Projects/Protocols/) IS backfilled.
        assert!(should_backfill(
            "Mainframe/02_Projects/Protocols/p.md",
            false
        ));
    }

    #[test]
    fn should_backfill_skips_traversal() {
        assert!(!should_backfill("../escape.md", false));
    }

    /// R6 REGRESSION (red on old code — this module / `plan_backfill` did not
    /// exist). A server-only canonical note (missing locally, safe) MUST be
    /// planned for a pull, while a present note is excluded. The pre-fix daemon
    /// enumerated NOTHING and created ZERO files; this locks in the
    /// enumeration→filter core that selects exactly the missing notes.
    /// "substrate must sync": former-substrate paths are now pulled too.
    #[test]
    fn plan_backfill_selects_server_only_notes_including_substrate() {
        let changes = vec![
            row("Mainframe/01_Notes/The Tselem Bridge.md", 10), // missing → pull
            row("Mainframe/01_Notes/already-here.md", 11),      // present → skip
            row("Mainframe/02_Projects/Protocols/p.md", 12),    // former substrate → pull
            row("../escape.md", 13),                            // unsafe → skip
        ];
        let present: HashSet<&str> = ["Mainframe/01_Notes/already-here.md"].into_iter().collect();
        let mut plan = plan_backfill(&changes, |p| present.contains(p));
        plan.sort();
        assert_eq!(
            plan,
            vec![
                "Mainframe/01_Notes/The Tselem Bridge.md".to_string(),
                "Mainframe/02_Projects/Protocols/p.md".to_string(),
            ],
            "all server-only, safe notes (incl. former substrate) must be pulled"
        );
    }

    #[test]
    fn plan_backfill_empty_when_all_present() {
        let changes = vec![
            row("Mainframe/01_Notes/a.md", 1),
            row("Mainframe/01_Notes/b.md", 2),
        ];
        let plan = plan_backfill(&changes, |_| true);
        assert!(
            plan.is_empty(),
            "nothing to pull when everything is present"
        );
    }

    // ---- env-reader tests ----

    #[test]
    fn read_backfill_interval_default_and_override() {
        assert_eq!(
            read_backfill_interval(&MapEnv(HashMap::new())),
            Duration::from_secs(DEFAULT_BACKFILL_INTERVAL_SECS)
        );
        let mut m = HashMap::new();
        m.insert(ENV_BACKFILL_INTERVAL_SECS.to_string(), "120".to_string());
        assert_eq!(read_backfill_interval(&MapEnv(m)), Duration::from_secs(120));
    }

    #[test]
    fn read_backfill_interval_falls_back_on_zero_and_malformed() {
        let mut z = HashMap::new();
        z.insert(ENV_BACKFILL_INTERVAL_SECS.to_string(), "0".to_string());
        assert_eq!(
            read_backfill_interval(&MapEnv(z)),
            Duration::from_secs(DEFAULT_BACKFILL_INTERVAL_SECS)
        );
        let mut bad = HashMap::new();
        bad.insert(ENV_BACKFILL_INTERVAL_SECS.to_string(), "hourly".to_string());
        assert_eq!(
            read_backfill_interval(&MapEnv(bad)),
            Duration::from_secs(DEFAULT_BACKFILL_INTERVAL_SECS)
        );
    }

    #[test]
    fn is_backfill_disabled_reads_env() {
        assert!(!is_backfill_disabled(&MapEnv(HashMap::new())));
        let mut m = HashMap::new();
        m.insert(ENV_DISABLE_BACKFILL.to_string(), "1".to_string());
        assert!(is_backfill_disabled(&MapEnv(m)));
    }
}
