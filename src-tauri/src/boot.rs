//! Unit CS-3 (TKT-b6d8c4ba) §3.5: the bootstrap order and the boot task.
//!
//! `.setup()` reads the config once ([`crate::ownership::BootConfig`]) and,
//! when sync starts, calls [`start_sync`]: the CS-2 bootstrap with the
//! snapshot's S (no config read), the supervised custody tick (Required),
//! the supervised `config_watch` (Optional, gate C4) and the supervised boot
//! task, which loads nothing and runs B1-B10:
//!
//! - B1 redflag gate, the top-level token, the plugin scan, api health, the
//!   version patch, the heartbeat (Optional). An early return is a Terminal
//!   `boot_aborted{stage}`: the slot stays empty, V&R refuses `NotPublished`.
//! - B2 the three stores from the top-level `StateDir`; each flusher is
//!   supervised (Required) and registered for the pre-exit flush (D14).
//! - B3 the D9 seed.
//! - B4 one [`JournalOwner`] per distinct effective sub of a ROOT (gate C2),
//!   compacted once before any clone leaves the builder.
//! - B5 build [`SyncHandles`] (every root not ready) and publish it.
//! - B6 the CS-5 hook point (empty in CS-3).
//! - B7 the materializer, with the primary root's journal as its
//!   compensating journal (G2).
//! - B8 each root's pipeline, supervised, wired to its readiness.
//! - B9 the stranded-journal diagnosis (G-6, diagnose only).
//! - B10 the SSE consumer (Required), awaited here.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use futures::FutureExt;
use tauri::{Emitter, Manager};

use crate::api_client::ApiClient;
use crate::config::Config;
use crate::file_watcher::{self, WatchHandle, WatcherEnd};
use crate::journal_owner::JournalOwner;
use crate::ownership::{run_config_watch, ConfigWatch, OwnershipSnapshot, RootKey, RootSpec};
use crate::push_journal::count_lines_by_action;
use crate::state_io::{self, StateFile};
use crate::supervise::{Host, Notice, Supervisor, TaskClass, TaskEnd, TaskSpec};
use crate::sync_handles::{
    PipelineStage, RootHandles, SyncHandles, SyncHandlesParts, SyncHandlesSlot, FIRST_DRAIN_DONE,
    WATCHER_STARTED,
};
use crate::sync_health::{self, RecoveryTrigger, StallEvent, StateHealthEvent, SyncHealth};
use crate::tray_state::SharedTrayState;
use crate::{
    api_client, base_seq_store, conflict_stash, echo_guard, heartbeat, managed_instance,
    materializer, obsidian_install_detect, obsidian_plugin_detect, pull_backfill, push_client,
    read_receipt, reconciliation, redflag, rejected_ledger, sse, sync_shadow, token_store,
};

/// The production [`Host`]: the Tauri app.
pub struct TauriHost(pub tauri::AppHandle);

impl Host for TauriHost {
    fn notify(&self, title: &str, body: &str) {
        crate::notify_user(&self.0, title, body);
    }
    fn emit(&self, event: &str, payload: serde_json::Value) {
        if let Err(e) = self.0.emit(event, payload) {
            tracing::warn!("failed to emit {event}: {e}");
        }
    }
    fn exit(&self, code: i32) {
        std::process::exit(code);
    }
    fn restart(&self) {
        self.0.restart();
    }
}

impl TauriHost {
    /// The managed [`SyncHandlesSlot`] (always managed in `.setup()`).
    pub fn slot(app: &tauri::AppHandle) -> SyncHandlesSlot {
        app.state::<SyncHandlesSlot>().inner().clone()
    }
}

/// Timing knobs. Production uses [`BootKnobs::default`]; the
/// daemon-subprocess test harness shortens them.
#[derive(Clone, Debug)]
pub struct BootKnobs {
    /// The stall watchdog's tick (60 s).
    pub watchdog_tick: Duration,
    /// The stall threshold (`None`: `VAULT_SYNC_STALL_THRESHOLD_SECS`).
    pub stall_threshold: Option<Duration>,
    /// `config_watch`'s period (60 s, §3.1).
    pub config_watch_period: Duration,
    /// The storm breaker threshold (`None`: the file_watcher default).
    pub storm_threshold: Option<u64>,
    /// The push drain loop's idle interval (`None`: the client default).
    pub push_loop_interval_ms: Option<u64>,
}

impl Default for BootKnobs {
    fn default() -> Self {
        Self {
            watchdog_tick: Duration::from_secs(60),
            stall_threshold: None,
            config_watch_period: Duration::from_secs(60),
            storm_threshold: None,
            push_loop_interval_ms: None,
        }
    }
}

/// What `.setup()` derives from the one config read (§3.5 steps 2-4).
pub struct StartupPlan {
    pub snapshot: Option<Arc<OwnershipSnapshot>>,
    pub tray: SharedTrayState,
    pub decision: crate::LaunchDecision,
}

/// §3.5 steps 2-4: the tray state (L1) and the launch decision (L2) from the
/// snapshot; nothing re-reads the file.
pub fn plan_startup(boot_cfg: &crate::ownership::BootConfig) -> StartupPlan {
    let snapshot = boot_cfg.snapshot().cloned();
    if let crate::ownership::BootConfig::Unreadable(why) = boot_cfg {
        tracing::error!("config load failed: {why}; not starting sync");
    }
    // S477: display the configured vaults_root verbatim (the watch root).
    let mut ts = match &snapshot {
        Some(s) => crate::tray_state::TrayState::new(
            s.cfg().subscriber_id.clone(),
            s.cfg().nexus_url.clone(),
            s.cfg().vaults_root.clone(),
        ),
        None => crate::tray_state::TrayState::new(String::new(), String::new(), PathBuf::new()),
    };
    ts.config_sha8 = snapshot.as_ref().map(|s| s.sha8()).unwrap_or_default();
    let tray = Arc::new(std::sync::RwLock::new(ts));
    // v0.4.29: the pure, non-prompting gate (token FILE only, never the
    // keyring), now fed by the snapshot.
    let sub = snapshot
        .as_ref()
        .map(|s| s.top_sub().to_string())
        .unwrap_or_default();
    let decision = crate::launch_decision(
        !sub.is_empty(),
        !sub.is_empty() && token_store::has_persisted_token(&sub),
    );
    StartupPlan {
        snapshot,
        tray,
        decision,
    }
}

/// Everything the sync start needs (built in `.setup()`).
pub struct BootEnv {
    pub sup: Arc<Supervisor>,
    pub snapshot: Arc<OwnershipSnapshot>,
    pub slot: SyncHandlesSlot,
    pub tray: SharedTrayState,
    pub workspace: PathBuf,
    pub knobs: BootKnobs,
}

/// `.setup()` step 6 (§3.5): bootstrap the state dirs from the snapshot's S
/// (no config read), supervise the custody tick (Required) and `config_watch`
/// (Optional, gate C4), and spawn the supervised boot task.
pub fn start_sync(env: BootEnv) {
    let host = Arc::clone(env.sup.host());
    if bootstrap_state_dirs(host.as_ref(), &env.snapshot, &env.workspace) {
        env.sup.supervise_handle(
            TaskSpec::new("custody_tick", TaskClass::Required),
            state_io::spawn_custody_tick(),
        );
    }
    // T-CS3-2: the seam between the bootstrap and the boot task.
    #[cfg(test)]
    test_hooks::run_after_bootstrap();
    env.sup.supervise(
        TaskSpec::new("config_watch", TaskClass::Optional),
        run_config_watch(
            ConfigWatch::new(Arc::clone(&env.snapshot), Some(env.tray.clone())),
            env.knobs.config_watch_period,
        ),
    );
    let sup = Arc::clone(&env.sup);
    sup.supervise(TaskSpec::new("boot", TaskClass::Required), run_boot(env));
}

/// CS-2 §3.2 with the snapshot's bootstrap set S (L3 no longer reads the
/// config): bootstrap every state dir, install the registry. A rejected or
/// failed sub gets the existing user notice; its pipeline will not start.
/// Returns true iff the registry was installed.
pub fn bootstrap_state_dirs(
    host: &dyn Host,
    snapshot: &OwnershipSnapshot,
    workspace: &Path,
) -> bool {
    let config_dir = snapshot
        .cfg_path()
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("."));
    match state_io::bootstrap(workspace, snapshot.subs(), &config_dir) {
        Ok(reg) => {
            let reg = state_io::install(reg, workspace);
            for (sub, why) in reg.rejected() {
                if sub == "<config>" {
                    continue;
                }
                host.notify(
                    "Vault Sync push failed to start",
                    &format!("state directory for subscriber {sub:?} unavailable: {why}"),
                );
            }
            true
        }
        Err(e) => {
            tracing::error!("state_io bootstrap failed: {e}; not starting sync");
            host.notify(
                "Vault Sync push failed to start",
                &format!("state directory bootstrap failed: {e}"),
            );
            false
        }
    }
}

/// D14 / G-7: register the three stores for the bounded pre-exit flush.
/// Every write goes through `guard_mutation`; errors are ignored.
pub(crate) fn register_store_flush(
    sup: &Supervisor,
    shadow: &Arc<sync_shadow::ShadowStore>,
    base_seq: &Arc<base_seq_store::BaseSeqStore>,
    read_receipt: &Arc<read_receipt::ReadReceiptStore>,
) {
    let (s, b, r) = (
        Arc::clone(shadow),
        Arc::clone(base_seq),
        Arc::clone(read_receipt),
    );
    sup.register_pre_exit_flush(move || {
        let _ = s.flush();
        let _ = b.flush();
        let _ = r.flush();
    });
}

/// A Terminal `boot_aborted{stage}` (B1): no exit, the slot stays empty.
fn boot_aborted(stage: &str) -> TaskEnd {
    TaskEnd::Terminal {
        reason: format!("boot_aborted:{stage}"),
        event: Some(StateHealthEvent::BootAborted {
            stage: stage.to_string(),
        }),
        notice: None,
    }
}

/// Test seams for the daemon-subprocess harness.
#[cfg(test)]
pub(crate) mod test_hooks {
    use std::sync::Mutex;

    static AFTER_BOOTSTRAP: Mutex<Option<Box<dyn FnMut() + Send>>> = Mutex::new(None);

    /// T-CS3-2: runs after `bootstrap_state_dirs`, before the boot task.
    pub(crate) fn set_after_bootstrap(h: Option<Box<dyn FnMut() + Send>>) {
        *AFTER_BOOTSTRAP.lock().unwrap_or_else(|p| p.into_inner()) = h;
    }
    pub(super) fn run_after_bootstrap() {
        if let Some(h) = AFTER_BOOTSTRAP
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_mut()
        {
            h();
        }
    }
}

/// The B4 get-or-open journal table, keyed by a root's effective sub. Two
/// roots on one sub share one owner (R-CS3-2).
#[derive(Default)]
pub(crate) struct JournalTable {
    opened: BTreeMap<String, Result<Arc<JournalOwner>, (PipelineStage, String)>>,
}

impl JournalTable {
    /// Gate C2 (T-CS3-25): the ONLY production call of `JournalOwner::open`.
    /// Its input is a ROOT, so a sub in S with no root (the top-level Y of a
    /// split config) is never opened or claimed.
    pub(crate) fn journal_for_root(
        &mut self,
        root: &RootSpec,
    ) -> Result<Arc<JournalOwner>, (PipelineStage, String)> {
        if let Some(r) = self.opened.get(&root.eff_sub) {
            return r.clone();
        }
        let r = match state_io::registry().get(&root.eff_sub) {
            Err(e) => Err((PipelineStage::NotBootstrapped, e.to_string())),
            Ok(sd) => match JournalOwner::open(&sd, &root.eff_sub) {
                Err(e) => Err((PipelineStage::JournalOpen, e.to_string())),
                Ok(owner) => {
                    startup_compaction(&owner);
                    Ok(owner)
                }
            },
        };
        self.opened.insert(root.eff_sub.clone(), r.clone());
        r
    }

    /// The owners that opened, by sub.
    pub(crate) fn into_owners(self) -> BTreeMap<String, Arc<JournalOwner>> {
        self.opened
            .into_iter()
            .filter_map(|(k, v)| v.ok().map(|o| (k, o)))
            .collect()
    }
}

/// Journal-bloat remediation: collapse the backlog to one entry per path,
/// once, on the owner, before any clone leaves the builder (no contender).
/// Non-fatal; kill switch `VAULT_SYNC_DISABLE_COMPACTION`.
fn startup_compaction(owner: &JournalOwner) {
    let disabled = std::env::var("VAULT_SYNC_DISABLE_COMPACTION")
        .map(|v| !v.is_empty())
        .unwrap_or(false);
    if disabled {
        tracing::info!(
            "push_journal: startup compaction disabled via VAULT_SYNC_DISABLE_COMPACTION"
        );
        return;
    }
    match owner.compact() {
        Ok((before, after)) if after < before => tracing::info!(
            before,
            after,
            removed = before - after,
            "push_journal: startup compaction collapsed redundant duplicates"
        ),
        Ok((n, _)) => tracing::info!(
            entries = n,
            "push_journal: startup compaction — nothing redundant"
        ),
        Err(e) => tracing::warn!(error = %e, "push_journal: startup compaction failed (non-fatal)"),
    }
}

/// Record a root's failed stage: `failed` (first one wins), SyncHealth
/// `pipeline_failed{root, stage}`.
fn fail_root(handles: &SyncHandles, key: RootKey, stage: PipelineStage) {
    if let Some(r) = handles.root(key) {
        if r.fail(stage) {
            crate::sync_health::raise_state_event(StateHealthEvent::PipelineFailed {
                root: key,
                stage: stage.as_str().to_string(),
            });
        }
    }
}

/// The boot task (§3.5 B1-B10). Loads nothing: every input comes from the
/// snapshot. It classifies the SSE consumer's end itself, BEFORE it drops
/// the watch handles, so the watcher ends that follow are cascade.
async fn run_boot(env: BootEnv) -> TaskEnd {
    let BootEnv {
        sup,
        snapshot,
        slot,
        tray,
        workspace,
        knobs,
    } = env;
    let host = Arc::clone(sup.host());
    let cfg: Arc<Config> = Arc::clone(snapshot.cfg());
    let roots: Vec<RootSpec> = snapshot.roots().to_vec();
    if roots.is_empty() {
        tracing::warn!(
            "sync_roots is empty — no watch pipeline will start. \
             Check config: at least one [[sync_roots]] entry is required."
        );
        return boot_aborted("no_roots");
    }

    // ---- B1 --------------------------------------------------------------
    // Redflag gate: checked against the first sync root (primary vault).
    let primary_root = roots[0].path.clone();
    let gate = redflag::RedflagGate::new(primary_root.clone());
    match gate.check() {
        redflag::RedflagStatus::Tripped { path, .. } => {
            tracing::error!(
                "redflag.md present at {:?} — aborting sync startup until removed",
                path
            );
            if let Ok(mut s) = tray.write() {
                s.set_redflag_tripped(true);
            }
            host.notify(
                "Vault Sync paused",
                "redflag.md detected at your vaults root — sync paused until removed.",
            );
            spawn_redflag_monitor(&sup, gate, tray.clone());
            return boot_aborted("redflag");
        }
        redflag::RedflagStatus::Clear => {
            if let Ok(mut s) = tray.write() {
                s.set_redflag_tripped(false);
            }
            spawn_redflag_monitor(&sup, gate.clone(), tray.clone());
        }
    }

    let token = match token_store::load(snapshot.top_sub()) {
        Ok(Some(t)) => t,
        Ok(None) => {
            tracing::error!(
                "no token in keyring or file fallback for subscriber_id={}; re-pair required",
                snapshot.top_sub()
            );
            host.notify(
                "Vault Sync needs re-pairing",
                "Open the wizard to re-pair this device.",
            );
            return boot_aborted("token");
        }
        Err(e) => {
            tracing::error!("token_store read failed: {e}");
            return boot_aborted("token");
        }
    };

    scan_obsidian_plugins(&roots);

    let api = match ApiClient::new(&cfg.nexus_url, &token) {
        Ok(a) => Arc::new(a),
        Err(e) => {
            tracing::error!("api client init failed: {e}");
            return boot_aborted("api_init");
        }
    };
    let health_snap = match api.health().await {
        Ok(s) => s,
        Err(e) => {
            tracing::error!("api health failed: {e}");
            return boot_aborted("api_health");
        }
    };
    match api.patch_self_version().await {
        Ok(_) => tracing::info!(
            version = api_client::daemon_version(),
            platform = api_client::daemon_platform(),
            "reported daemon_version + daemon_platform to /api/sync/subscribers/me"
        ),
        Err(e) => tracing::warn!("daemon_version self-report failed: {e}"),
    }

    let append_arms_enabled = materializer::append_arms_enabled_from_env_value(
        std::env::var("VAULT_SYNC_APPEND_ARMS").ok().as_deref(),
    );
    if !append_arms_enabled {
        tracing::warn!(
            "VAULT_SYNC_APPEND_ARMS is set to an off-word: append-aware conflict arms DISABLED (pre-fix stash floor for every R4/R5 conflict)"
        );
    }
    let materializer_cfg = materializer::MaterializerConfig {
        device_id: cfg.subscriber_id.clone(),
        conflict_policy: conflict_stash::ConflictPolicy::NewerWins,
        append_arms_enabled,
        // R2(a) (TKT-406f6cb3): flight-recorder conflict stashes go to the
        // app-data quarantine (beside logs/), never into the vault.
        // (BootEnv.workspace == resolve_workspace_root() in production; a
        // harness workspace keeps the quarantine inside its temp tree.)
        stash_quarantine_dir: Some(workspace.join(conflict_stash::StashQuarantine::DIR_NAME)),
        ..Default::default()
    };
    let echo_guard = Arc::new(echo_guard::EchoGuard::new());
    let sync_health = SyncHealth::new();
    sup.supervise_handle(
        TaskSpec::new("heartbeat", TaskClass::Optional),
        heartbeat::spawn(Arc::clone(&api), Arc::clone(&sync_health)),
    );

    // ---- B2 --------------------------------------------------------------
    let state_dir = match state_io::registry().get(snapshot.top_sub()) {
        Ok(sd) => sd,
        Err(e) => {
            tracing::error!("sync not started: {e}");
            host.notify(
                "Vault Sync push failed to start",
                &format!("state directory unavailable: {e}"),
            );
            return boot_aborted("not_bootstrapped");
        }
    };
    let vault_folders: Vec<String> = roots
        .iter()
        .filter_map(|r| r.path.file_name().map(|n| n.to_string_lossy().into_owned()))
        .collect();
    let shadow =
        sync_shadow::ShadowStore::load_with_vault_folders(&state_dir, vault_folders.clone());
    let base_seq =
        base_seq_store::BaseSeqStore::load_with_vault_folders(&state_dir, vault_folders.clone());
    let read_receipt =
        read_receipt::ReadReceiptStore::load_with_vault_folders(&state_dir, vault_folders);
    sup.supervise_handle(
        TaskSpec::new("flush:shadow", TaskClass::Required),
        sync_shadow::ShadowStore::spawn_periodic_flush(Arc::clone(&shadow)),
    );
    sup.supervise_handle(
        TaskSpec::new("flush:base_seq", TaskClass::Required),
        base_seq_store::BaseSeqStore::spawn_periodic_flush(Arc::clone(&base_seq)),
    );
    sup.supervise_handle(
        TaskSpec::new("flush:read_receipt", TaskClass::Required),
        read_receipt::ReadReceiptStore::spawn_periodic_flush(Arc::clone(&read_receipt)),
    );
    register_store_flush(&sup, &shadow, &base_seq, &read_receipt);

    // ---- B3: D9 shadow-wipe fast path (unchanged except the J8 wrap). ----
    if shadow.is_empty() {
        match ApiClient::new(&cfg.nexus_url, &token) {
            Ok(seed_api) => {
                let _ = reconciliation::seed_shadow_from_server_if_empty(
                    primary_root.clone(),
                    Arc::new(seed_api),
                    cfg.subscriber_id.clone(),
                    Arc::clone(&shadow),
                )
                .await;
            }
            Err(e) => {
                tracing::warn!("D9 shadow seed: api client init failed: {e}; skipping seed");
            }
        }
    }

    // ---- B4: one owner per distinct effective sub of a root. ------------
    let mut table = JournalTable::default();
    let mut root_handles = Vec::with_capacity(roots.len());
    let mut open_failures: Vec<(RootKey, PipelineStage, String)> = Vec::new();
    for root in &roots {
        match table.journal_for_root(root) {
            Ok(owner) => root_handles.push(RootHandles::new(root.clone(), Some(owner))),
            Err((stage, why)) => {
                open_failures.push((root.key, stage, why));
                root_handles.push(RootHandles::new(root.clone(), None));
            }
        }
    }

    // ---- B5: build and publish (every root not ready). ------------------
    let handles = Arc::new(SyncHandles::new(SyncHandlesParts {
        snapshot: Arc::clone(&snapshot),
        shadow: Arc::clone(&shadow),
        base_seq: Arc::clone(&base_seq),
        read_receipt: Arc::clone(&read_receipt),
        journals: table.into_owners(),
        roots: root_handles,
        live: sup.live_sender(),
    }));
    for (key, stage, why) in open_failures {
        tracing::error!(
            root = key,
            stage = stage.as_str(),
            "push pipeline: {why}; not starting push side"
        );
        host.notify(
            "Vault Sync push failed to start",
            &format!("journal open failed: {why}"),
        );
        fail_root(&handles, key, stage);
    }
    if !slot.publish(Arc::clone(&handles)) {
        tracing::error!("sync handles already published in this process; keeping the first");
    }

    // ---- B6: the CS-5 hook point (epoch_acquire goes here). Nothing in CS-3.

    // ---- B7: the materializer; its compensating journal is the journal
    // the primary root's pipeline drains (G2). ---------------------------
    let compensating = handles.journal(&roots[0].eff_sub);
    if compensating.is_none() {
        tracing::warn!("R1: compensating-push journal unavailable; anti-strip ARM 1 will preserve local but not enqueue (converges next reconcile pass)");
    }
    let mut materializer = materializer::Materializer::new(
        primary_root.clone(),
        health_snap.shadow_path.clone(),
        materializer::MaterializerMode::from_str(&health_snap.materializer_mode),
        workspace.clone(),
        cfg.subscriber_id.clone(),
        materializer_cfg,
    )
    .with_tray_state(tray.clone())
    .with_echo_guard(Arc::clone(&echo_guard))
    .with_shadow_store(Arc::clone(&shadow))
    .with_base_seq_store(Arc::clone(&base_seq));
    if let Some(j) = compensating {
        materializer = materializer.with_push_journal(j);
    }
    {
        let m = materializer.clone();
        sup.supervise(
            TaskSpec::new("conflict_refresh", TaskClass::Optional),
            async move {
                let mut tick =
                    tokio::time::interval(crate::supervise::tick(Duration::from_secs(60)));
                tick.tick().await;
                loop {
                    tick.tick().await;
                    m.refresh_conflict_count_into_tray();
                }
            },
        );
    }

    // ---- B8: the per-root pipelines. ------------------------------------
    let ctx = PipelineCtx {
        sup: Arc::clone(&sup),
        host: Arc::clone(&host),
        cfg: Arc::clone(&cfg),
        handles: Arc::clone(&handles),
        scope_roots: health_snap.scope_roots.clone(),
        scope_excludes: health_snap.scope_excludes.clone(),
        tray: tray.clone(),
        echo_guard,
        materializer: materializer.clone(),
        shadow: Arc::clone(&shadow),
        base_seq: Arc::clone(&base_seq),
        read_receipt: Arc::clone(&read_receipt),
        sync_health,
        knobs: knobs.clone(),
        stash_quarantine: workspace.join(conflict_stash::StashQuarantine::DIR_NAME),
        // WS-A A3: config-scope (beside reconcile-retry-ledger.json), loaded
        // once for the process; in-memory if the config dir is not bootstrapped.
        rejected_ledger: Arc::new(match crate::state_io::registry().config_dir() {
            Ok(sd) => rejected_ledger::RejectedLedger::load(&sd),
            Err(e) => {
                tracing::warn!("rejected ledger not persisted: {e}");
                rejected_ledger::RejectedLedger::in_memory()
            }
        }),
    };
    let watch_handles: Vec<WatchHandle> = roots
        .iter()
        .filter_map(|r| spawn_push_pipeline(&ctx, r.key))
        .collect();

    // ---- B9: stranded-journal diagnosis (diagnose only, G-6). -----------
    diagnose_stranded(&snapshot);

    // ---- B10: the SSE consumer (Required), awaited here. -----------------
    let resumed_id = sse::SseConsumer::load_last_event_id(&state_dir).or(cfg.last_event_id.clone());
    if let Some(rid) = &resumed_id {
        tracing::info!(
            last_event_id = %rid,
            file = StateFile::LastEventId.name(),
            "resuming SSE from persisted last_event_id"
        );
    }
    let sse_spec = TaskSpec::new("sse_consumer", TaskClass::Required);
    let consumer = match primary_sse_consumer(
        cfg.nexus_url.clone(),
        token,
        health_snap.scope_roots,
        health_snap.scope_excludes,
        materializer,
        &roots,
    ) {
        Ok(c) => {
            let h = Arc::clone(&handles);
            c.with_tray_state(tray)
                .with_state_dir(&state_dir)
                .with_on_connected(Arc::new(move || h.mark_sse_connected()))
        }
        Err(e) => {
            tracing::error!("sse consumer init failed: {e}");
            sup.task_ended(&sse_spec, Ok(TaskEnd::Returned));
            drop(watch_handles);
            return TaskEnd::Classified;
        }
    };
    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    tracing::info!(
        "starting SSE consumer for subscriber_id={}",
        snapshot.top_sub()
    );
    let res = std::panic::AssertUnwindSafe(consumer.run(resumed_id, shutdown_rx))
        .catch_unwind()
        .await;
    let end = match res {
        Err(p) => Err(panic_text(&p)),
        Ok(Ok(())) => Ok(TaskEnd::Returned),
        Ok(Err(e)) => match sse::auth_failure_status(&e) {
            Some(status) => Ok(sse_auth_terminal(snapshot.top_sub(), status)),
            None => {
                tracing::error!("SSE consumer exited with error: {e}");
                Ok(TaskEnd::Returned)
            }
        },
    };
    // Classify first, so the watcher ends below are cascade.
    sup.task_ended(&sse_spec, end);
    drop(watch_handles);
    TaskEnd::Classified
}

fn panic_text(p: &Box<dyn std::any::Any + Send>) -> String {
    p.downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| p.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "panic".to_string())
}

/// Gate C6 (No Silent Holds): the SSE 401/403 Terminal stop and its one
/// dedicated notice: what stopped, why, and the one action that lifts it.
pub(crate) fn sse_auth_terminal(sub: &str, status: u16) -> TaskEnd {
    TaskEnd::Terminal {
        reason: "sse_auth_failed".to_string(),
        event: Some(StateHealthEvent::SseAuthFailed {
            sub: sub.to_string(),
            status,
        }),
        notice: Some(Notice {
            title: "Vault Sync stopped: device token rejected".to_string(),
            body: format!(
                "What stopped: pull and push for subscriber {sub} on this host. \
                 Why: the server rejected this device's token (HTTP {status}). \
                 To resume: re-pair this device in the Vault Sync wizard."
            ),
        }),
    }
}

/// Gate E2: the kill-switched storm fence's one dedicated notice.
fn fence_notice(root: &Path) -> Notice {
    Notice {
        title: "Vault Sync stopped detecting local edits".to_string(),
        body: format!(
            "What stopped: local-edit detection for root {}. \
             Why: the storm circuit-breaker tripped on a push journal at capacity. \
             To resume: restart the daemon after the backlog clears.",
            root.display()
        ),
    }
}

/// v0.1.4/v0.1.7: disable conflicting Obsidian plugins in every known vault
/// and every sync root before SSE starts materializing.
fn scan_obsidian_plugins(roots: &[RootSpec]) {
    let mut vaults_to_scan: Vec<PathBuf> = obsidian_install_detect::find_known_vaults();
    for root in roots {
        let root_path = &root.path;
        if root_path.join(".obsidian").is_dir() && !vaults_to_scan.iter().any(|q| q == root_path) {
            vaults_to_scan.push(root_path.clone());
        }
        if let Ok(entries) = std::fs::read_dir(root_path) {
            for entry in entries.flatten() {
                let p = entry.path();
                if p.is_dir()
                    && p.join(".obsidian").is_dir()
                    && !vaults_to_scan.iter().any(|q| q == &p)
                {
                    vaults_to_scan.push(p);
                }
            }
        }
    }
    tracing::info!(
        "scanning {} obsidian vault(s) for conflicting plugins",
        vaults_to_scan.len()
    );
    for vault in &vaults_to_scan {
        let detect = obsidian_plugin_detect::detect_and_disable(vault);
        if let Some(line) = obsidian_plugin_detect::summary_line(&detect) {
            tracing::info!("[{}] {line}", vault.display());
        }
    }
}

/// The 60 s redflag monitor (Optional).
fn spawn_redflag_monitor(sup: &Arc<Supervisor>, gate: redflag::RedflagGate, tray: SharedTrayState) {
    sup.supervise(
        TaskSpec::new("redflag_monitor", TaskClass::Optional),
        async move {
            let mut tick = tokio::time::interval(crate::supervise::tick(Duration::from_secs(60)));
            loop {
                tick.tick().await;
                crate::redflag_tick(&gate, &tray);
            }
        },
    );
}

/// What every root's pipeline shares.
struct PipelineCtx {
    sup: Arc<Supervisor>,
    host: Arc<dyn Host>,
    cfg: Arc<Config>,
    handles: Arc<SyncHandles>,
    scope_roots: Vec<String>,
    scope_excludes: Vec<String>,
    tray: SharedTrayState,
    echo_guard: Arc<echo_guard::EchoGuard>,
    materializer: materializer::Materializer,
    shadow: Arc<sync_shadow::ShadowStore>,
    base_seq: Arc<base_seq_store::BaseSeqStore>,
    read_receipt: Arc<read_receipt::ReadReceiptStore>,
    sync_health: Arc<SyncHealth>,
    knobs: BootKnobs,
    /// R2(a) (TKT-406f6cb3): `<workspace>/conflict-quarantine`.
    stash_quarantine: std::path::PathBuf,
    /// WS-A A3 (TKT-17fc22e5): ONE rejected ledger shared by every root's
    /// push client and reconciliation backstop (one file, one writer set).
    rejected_ledger: Arc<rejected_ledger::RejectedLedger>,
}

/// TKT-35f75318 wiring (B10): build the SSE consumer. It materializes into
/// the PRIMARY root (`roots[0]`), so its defensive scope check must use that
/// root's route to match the route-prefixed STORAGE key. `run_boot` calls
/// this to make its consumer, so the wiring test in `cs3_tests::boot_wiring` covers
/// the real path. Empty `roots` = the bare vault route.
pub(crate) fn primary_sse_consumer(
    nexus_url: String,
    token: String,
    scope_roots: Vec<String>,
    scope_excludes: Vec<String>,
    materializer: crate::materializer::Materializer,
    roots: &[RootSpec],
) -> anyhow::Result<sse::SseConsumer> {
    let primary_route = roots.first().map(|r| r.route.as_str()).unwrap_or("");
    sse::SseConsumer::new(nexus_url, token, scope_roots, scope_excludes, materializer)
        .map(|c| c.with_route(primary_route))
}

/// TKT-35f75318 wiring (push pipeline): the file watcher's config for one
/// root. `scope_roots`/`scope_excludes` from the server are STORAGE-key
/// prefixes (`<route>/...`); the watcher gets the root's route so its scope
/// gate matches the same key form. Empty route (vault) = `None`.
/// `spawn_push_pipeline` calls this to build its watcher config, so the
/// wiring test in `cs3_tests::boot_wiring` covers the real path.
pub(crate) fn push_watcher_config(
    spec: &RootSpec,
    scope_roots: Vec<String>,
    scope_excludes: Vec<String>,
    managed: bool,
) -> file_watcher::FileWatcherConfig {
    file_watcher::FileWatcherConfig {
        allowed_extensions: vec!["md".into(), "canvas".into()],
        scope_roots,
        scope_excludes,
        debounce_ms: 500,
        managed_payload_excludes: managed,
        storage_prefix: if spec.route.is_empty() {
            None
        } else {
            Some(spec.route.clone())
        },
    }
}

/// One root's push pipeline (B8): the drain loop (Required), the stall
/// watchdog (Optional), the reconcile backstop (RootDegrading), the pull
/// backfill (Optional) and the watcher (Required). Every failure sets the
/// root's `failed` stage; the partial-run behaviour of today is kept (a
/// root whose watcher failed keeps its drain loop running). Returns the
/// `WatchHandle`, which the boot task keeps alive.
fn spawn_push_pipeline(ctx: &PipelineCtx, key: RootKey) -> Option<WatchHandle> {
    let root = ctx.handles.root(key)?;
    let spec = root.spec().clone();
    let subscriber_id = spec.eff_sub.clone();
    let host = &ctx.host;

    // R7 (TKT-166e1c07): PARK the push side when the shadow store loaded
    // suspect (empty vault_folders but vault-prefixed keys).
    #[cfg(test)]
    let forced_park = crate::faults::armed(&format!("pipeline.r7_park.{key}"));
    #[cfg(not(test))]
    let forced_park = false;
    if forced_park || ctx.shadow.vault_scope_suspect() {
        tracing::error!(
            "push pipeline: PARKED (R7) - vault_folders resolved EMPTY but the shadow store holds \
             vault-prefixed keys (config vault_name is likely missing). Refusing to start the push \
             side to avoid a mass re-push (2026-07-18 trinity incident). Fix config vault_name and restart."
        );
        host.notify(
            "Vault Sync parked (config error)",
            "vault_folders resolved empty but prior sync history is present. Push is refused to \
             avoid a mass re-push. Set vault_name in the config and restart.",
        );
        fail_root(&ctx.handles, key, PipelineStage::R7Park);
        return None;
    }

    if root.failed().is_some() {
        // B4 already recorded journal_open / not_bootstrapped.
        return None;
    }
    let journal = root.journal()?.clone();

    let token = match token_store::load(&subscriber_id) {
        Ok(Some(t)) => t,
        _ => {
            tracing::error!("push pipeline: token unavailable; not starting push side");
            host.notify(
                "Vault Sync push failed to start",
                "Authentication token unavailable; not starting push side.",
            );
            fail_root(&ctx.handles, key, PipelineStage::Token);
            return None;
        }
    };

    let sd = match state_io::registry().get(&subscriber_id) {
        Ok(sd) => sd,
        Err(e) => {
            tracing::error!("push pipeline: {e}; not starting push side");
            host.notify(
                "Vault Sync push failed to start",
                &format!("state directory unavailable: {e}"),
            );
            fail_root(&ctx.handles, key, PipelineStage::NotBootstrapped);
            return None;
        }
    };
    let delete_ledger = crate::delete_ledger::DeleteLedger::new(&sd);
    let delete_ledger_for_backfill = delete_ledger.clone();
    let vault_root = spec.path.clone();

    // The pipeline's own client, from its own token (gate C7): V&R borrows
    // exactly this client; nothing substitutes another sub's token.
    let api_for_push = match ApiClient::new(&ctx.cfg.nexus_url, &token) {
        Ok(a) => Arc::new(a),
        Err(e) => {
            tracing::error!("push pipeline: api client init failed: {e}; not starting push side");
            host.notify(
                "Vault Sync push failed to start",
                &format!("api client init failed: {e}"),
            );
            fail_root(&ctx.handles, key, PipelineStage::ApiInit);
            return None;
        }
    };
    root.set_api(Arc::clone(&api_for_push));

    let managed = managed_instance::is_managed_mode();
    let mut push_cfg = push_client::PushClientConfig {
        allowed_extensions: vec![".md".into(), ".canvas".into()],
        strip_frontmatter_fields_for_diff: vec!["updated".into()],
        max_retry_attempts: 5,
        initial_backoff_ms: 500,
        max_backoff_ms: 60_000,
        managed_payload_excludes: managed,
        ..Default::default()
    };
    if let Some(ms) = ctx.knobs.push_loop_interval_ms {
        push_cfg.loop_interval_ms = ms;
    }
    let enqueued_hashes: Arc<std::sync::Mutex<std::collections::HashMap<String, String>>> =
        Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
    let drain_mark = {
        let h = Arc::clone(&ctx.handles);
        Arc::new(move || {
            if let Some(r) = h.root(key) {
                r.mark(FIRST_DRAIN_DONE);
            }
        }) as Arc<dyn Fn() + Send + Sync>
    };
    let push_client = push_client::PushClient::new(
        api_for_push,
        Arc::clone(&journal),
        subscriber_id.clone(),
        push_cfg,
        vault_root.clone(),
    )
    .with_tray_state(ctx.tray.clone())
    .with_shadow_store(Arc::clone(&ctx.shadow))
    .with_stash_quarantine(ctx.stash_quarantine.clone())
    .with_base_seq_store(Arc::clone(&ctx.base_seq))
    .with_read_receipt_store(Arc::clone(&ctx.read_receipt))
    .with_sync_health(Arc::clone(&ctx.sync_health))
    .with_materializer(ctx.materializer.clone())
    .with_enqueued_hashes(Arc::clone(&enqueued_hashes))
    .with_drain_hook(drain_mark)
    .with_rejected_ledger(Arc::clone(&ctx.rejected_ledger));

    tracing::info!(
        "starting push_client drain loop for subscriber_id={}",
        subscriber_id
    );
    ctx.sup.supervise(
        TaskSpec::new(format!("drain_loop:{key}"), TaskClass::Required),
        async move {
            // Never-fired shutdown channel: the loop runs for the process
            // lifetime; the sender is held here so `changed()` never errors.
            let (_hold_tx, rx) = tokio::sync::watch::channel(false);
            push_client.run_loop(rx).await;
            TaskEnd::Returned
        },
    );

    let env = reconciliation::ProcessEnv;
    let recovery_disabled = sync_health::is_recovery_disabled(&env);
    let threshold = ctx
        .knobs
        .stall_threshold
        .unwrap_or_else(|| sync_health::read_threshold(&env));
    spawn_stall_watchdog(
        &ctx.sup,
        Arc::clone(&ctx.sync_health),
        Arc::clone(&journal),
        subscriber_id.clone(),
        ctx.knobs.watchdog_tick,
        threshold,
        recovery_disabled,
    );

    match ApiClient::new(&ctx.cfg.nexus_url, &token) {
        Ok(recon_api) => ctx.sup.supervise_handle(
            TaskSpec::root_degrading(format!("reconcile_backstop:{key}"), root.alive_flag()),
            reconciliation::spawn_reconciliation_task(
                vault_root.clone(),
                Arc::new(recon_api),
                Arc::clone(&journal),
                subscriber_id.clone(),
                ctx.tray.clone(),
                ctx.materializer.clone(),
                Arc::clone(&ctx.shadow),
                Arc::clone(&ctx.rejected_ledger),
            ),
        ),
        Err(e) => {
            tracing::warn!("reconciliation: api client init failed: {e}; backstop not spawned");
        }
    }

    match ApiClient::new(&ctx.cfg.nexus_url, &token) {
        Ok(backfill_api) => ctx.sup.supervise_handle(
            TaskSpec::new(format!("pull_backfill:{key}"), TaskClass::Optional),
            pull_backfill::spawn_pull_backfill_task_guarded(
                Arc::new(backfill_api),
                ctx.materializer.clone(),
                Some(delete_ledger_for_backfill),
            ),
        ),
        Err(e) => {
            tracing::warn!("pull_backfill: api client init failed: {e}; backfill not spawned");
        }
    }

    // --- file_watcher: local FS edits → the SAME journal owner (J5). ---
    let burst = Arc::new(std::sync::Mutex::new(
        redflag::DeleteBurstDetector::new(20, Duration::from_secs(30)).with_pause_marker(&sd),
    ));
    redflag::register_delete_burst_handle(Arc::clone(&burst));
    let watcher_cfg = push_watcher_config(
        &spec,
        ctx.scope_roots.clone(),
        ctx.scope_excludes.clone(),
        managed,
    );
    let watcher = match file_watcher::FileWatcher::new(
        vault_root.clone(),
        Arc::clone(&journal),
        burst,
        watcher_cfg,
        subscriber_id.clone(),
    ) {
        Ok(w) => w
            .with_tray_state(ctx.tray.clone())
            .with_echo_guard(Arc::clone(&ctx.echo_guard))
            .with_shadow_store(Arc::clone(&ctx.shadow))
            .with_sync_health(Arc::clone(&ctx.sync_health))
            .with_enqueued_hashes(enqueued_hashes)
            .with_delete_ledger(delete_ledger)
            .with_storm_threshold(ctx.knobs.storm_threshold),
        Err(e) => {
            tracing::error!("push pipeline: file_watcher init failed: {e}; push_client running but no local-edit detection");
            host.notify(
                "Vault Sync push failed to start",
                &format!("file_watcher init failed: {e}"),
            );
            fail_root(&ctx.handles, key, PipelineStage::WatcherInit);
            return None;
        }
    };
    #[cfg(test)]
    let started = if crate::faults::armed(&format!("watcher_start.inotify.{key}")) {
        Err(file_watcher::FileWatcherError::InotifyLimitExceeded { current: 0 })
    } else {
        watcher.start()
    };
    #[cfg(not(test))]
    let started = watcher.start();
    match started {
        Ok(mut handle) => {
            tracing::info!("file_watcher started for subscriber_id={}", subscriber_id);
            if let Some(task) = handle.take_task() {
                supervise_watcher(
                    &ctx.sup,
                    &ctx.handles,
                    key,
                    &vault_root,
                    recovery_disabled,
                    task,
                );
            }
            root.mark(WATCHER_STARTED);
            Some(handle)
        }
        Err(file_watcher::FileWatcherError::InotifyLimitExceeded { current }) => {
            tracing::error!(
                "push pipeline: inotify watch limit exceeded (current={current}); raise fs.inotify.max_user_watches"
            );
            host.emit("inotify_limit_exceeded", serde_json::json!(current));
            host.notify(
                "Vault Sync push failed to start",
                &format!(
                    "Linux inotify watch limit exceeded (current={current}). \
                     Run: sudo sysctl -w fs.inotify.max_user_watches=524288"
                ),
            );
            fail_root(&ctx.handles, key, PipelineStage::WatcherStart);
            None
        }
        Err(e) => {
            tracing::error!("push pipeline: file_watcher start failed: {e}; push_client running but no local-edit detection");
            host.notify(
                "Vault Sync push failed to start",
                &format!("file_watcher start failed: {e}"),
            );
            fail_root(&ctx.handles, key, PipelineStage::WatcherStart);
            None
        }
    }
}

/// The watcher task (Required). Its storm-fence break is a designed
/// restart (gate C4): exit 70 through `supervise`, the one restart path; or,
/// with the watchdog's recovery kill switch set, a Terminal stop with
/// exactly one fence notice (gate E2).
pub(crate) fn supervise_watcher(
    sup: &Arc<Supervisor>,
    handles: &Arc<SyncHandles>,
    key: RootKey,
    root_path: &Path,
    recovery_disabled: bool,
    task: tokio::task::JoinHandle<WatcherEnd>,
) {
    let h = Arc::clone(handles);
    let root_path = root_path.to_path_buf();
    sup.supervise(
        TaskSpec::new(format!("watcher:{key}"), TaskClass::Required),
        async move {
            match task.await {
                Ok(WatcherEnd::Fenced) => {
                    fail_root(&h, key, PipelineStage::WatcherFenced);
                    if recovery_disabled {
                        TaskEnd::Terminal {
                            reason: "watcher_fenced".to_string(),
                            event: None,
                            notice: Some(fence_notice(&root_path)),
                        }
                    } else {
                        TaskEnd::Restart {
                            reason: "watcher_fenced".to_string(),
                            cause: "storm_fence".to_string(),
                        }
                    }
                }
                Ok(WatcherEnd::Shutdown) | Ok(WatcherEnd::ChannelClosed) => TaskEnd::Returned,
                Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
                Err(_) => TaskEnd::Returned,
            }
        },
    );
}

/// The stall watchdog (Optional) and its recovery closure. `PushStall` and
/// `WatcherStale` keep their restart, routed through `exit_once` (the
/// watchdog does nothing when an exit or restart is already claimed; C9
/// re-check). `WatcherFenced` neither restarts nor notifies: the supervisor
/// owns the fence (gate C4, E2).
pub(crate) fn spawn_stall_watchdog(
    sup: &Arc<Supervisor>,
    health: Arc<SyncHealth>,
    journal: Arc<JournalOwner>,
    subscriber_id: String,
    tick: Duration,
    threshold: Duration,
    recovery_disabled: bool,
) {
    let host = Arc::clone(sup.host());
    let sup2 = Arc::clone(sup);
    let handle = sync_health::spawn_progress_stall_watchdog(
        health,
        tick,
        threshold,
        recovery_disabled,
        move || {
            let j = Arc::clone(&journal);
            async move {
                #[cfg(test)]
                if crate::faults::armed("stall_watchdog.pending") {
                    return 1;
                }
                j.len()
            }
        },
        move |evt: StallEvent| {
            if evt.trigger == RecoveryTrigger::WatcherFenced {
                tracing::info!(
                    subscriber_id = %subscriber_id,
                    "sync_health: watcher fenced; the supervisor owns its one restart or notice (CS-3 gate C4)"
                );
                return;
            }
            tracing::error!(
                subscriber_id = %subscriber_id,
                pending = evt.pending,
                secs_since_progress = evt.secs_since_progress,
                "sync_health: push pipeline STALLED; recovering"
            );
            host.emit(
                "sync_stalled",
                serde_json::json!({
                    "pending": evt.pending,
                    "secs_since_progress": evt.secs_since_progress,
                    "subscriber_id": subscriber_id,
                }),
            );
            host.notify(
                "Vault Sync stalled; restarting",
                &format!(
                    "Push pipeline silent for {}m with {} pending edits. Restarting daemon to recover.",
                    evt.secs_since_progress / 60,
                    evt.pending
                ),
            );
            if evt.recovery_will_restart {
                sup2.restart_once("stall_watchdog");
            }
        },
    );
    sup.supervise_handle(TaskSpec::new("stall_watchdog", TaskClass::Optional), handle);
}

/// B9 (§3.6, G-6 DIAGNOSE ONLY): for each sub in S with no root, count the
/// stranded journal's lines by action. `fstatat` first; if non-empty, one
/// `O_RDONLY|O_NOFOLLOW` read on the dirfd. No write open, no lock-file
/// open, no claim; the file is left byte-identical.
pub(crate) fn diagnose_stranded(snapshot: &OwnershipSnapshot) {
    for sub in snapshot.subs() {
        if snapshot.roots().iter().any(|r| &r.eff_sub == sub) {
            continue;
        }
        let Ok(sd) = state_io::registry().get(sub) else {
            continue;
        };
        let bytes = match sd.len(StateFile::PushJournal) {
            Ok(Some(n)) if n > 0 => n,
            _ => continue,
        };
        let data = match sd.read(StateFile::PushJournal) {
            Ok(Some(d)) => d,
            Ok(None) => continue,
            Err(e) => {
                tracing::warn!(sub = %sub, error = %e, "stranded journal: read failed; not diagnosed");
                continue;
            }
        };
        let c = count_lines_by_action(&data);
        tracing::warn!(
            sub = %sub,
            journal = %sd.display(Some(StateFile::PushJournal)),
            bytes,
            create = c.create,
            modify = c.modify,
            delete = c.delete,
            unparsed = c.unparsed,
            "stranded journal: subscriber {sub} has no sync root, so nothing drains its {}; left untouched (diagnose only, G-6)",
            StateFile::PushJournal.name()
        );
        crate::sync_health::raise_state_event(StateHealthEvent::StrandedJournal {
            sub: sub.clone(),
            bytes,
            create: c.create,
            modify: c.modify,
            delete: c.delete,
            unparsed: c.unparsed,
        });
    }
}
