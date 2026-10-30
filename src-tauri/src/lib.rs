// CS-2 (TKT-23091e72) gate 1: deny path-based writers crate-wide, so a local
// `cargo clippy` without `-D warnings` also fails (see clippy.toml).
#![deny(clippy::disallowed_methods)]
// CS-3 T-CS3-11: no std MutexGuard may be held across an `.await`.
#![deny(clippy::await_holding_lock)]
pub mod api_client;
pub mod append_merge;
pub mod auth_backoff;
pub mod base_seq_store;
pub mod boot;
pub mod canonical_form;
pub mod commands;
pub mod commands_vaults;
pub mod config;
pub mod conflict_stash;
pub mod delete_ledger;
pub mod durable_file;
pub mod echo_guard;
pub mod fault_inject;
#[cfg(test)]
pub(crate) mod faults;
pub mod file_watcher;
pub mod heartbeat;
pub mod integrity_check;
pub mod journal_owner;
pub mod keyring;
pub mod managed_instance;
pub mod materializer;
pub mod obsidian_install_detect;
pub mod obsidian_plugin_detect;
pub mod ownership;
pub mod pairing;
pub mod path_policy;
pub mod pull_backfill;
pub mod push_client;
pub mod push_journal;
pub mod rasp_fence;
pub mod read_receipt;
pub mod reconciliation;
pub mod redflag;
pub mod rejected_ledger;
pub mod retry_ledger;
pub mod scope;
pub mod sse;
pub mod state_io;
pub mod supervise;
pub mod sync_handles;
pub mod sync_health;
pub mod sync_shadow;
pub mod token_store;
pub mod tray;
pub mod tray_state;
pub mod verify_repair;

use tauri::Manager;
use tauri_plugin_notification::NotificationExt;
use tauri_plugin_updater::UpdaterExt;

/// S477 §3.3 (v0.3.7): single entry point for sending OS-level notifications
/// (NSUserNotification on macOS, toast on Windows, libnotify on Linux) via
/// `tauri-plugin-notification`. Used for the four key user-visible events:
/// first successful pair, redflag tripped, push pipeline init failure, and
/// re-pair-needed (no token).
///
/// Failures (no permission, daemon backend down) are logged at WARN and
/// swallowed -- a missing notification must NEVER take the daemon down.
pub fn notify_user(app: &tauri::AppHandle, title: &str, body: &str) {
    if let Err(e) = app.notification().builder().title(title).body(body).show() {
        tracing::warn!("notification failed (title={title:?}): {e}");
    }
}

/// v0.4.29 control-plane P0 (Change 2): what the launch gate decided.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LaunchDecision {
    /// Spawn the SSE consumer + push pipelines.
    pub start_sync: bool,
    /// Show + focus the pairing wizard window.
    pub show_wizard: bool,
}

/// v0.4.29 control-plane P0 (Change 2): pure, NON-PROMPTING launch gate.
///
/// Inputs:
/// - `has_subscriber_config` — config.toml loads and carries a non-empty
///   `subscriber_id`.
/// - `has_persisted_token` — `token_store::has_persisted_token()` (process
///   cache / token FILE only; by construction it can never invoke the OS
///   keyring, so evaluating this gate can never hang the main thread on a
///   macOS Keychain prompt — the pre-0.4.29 fatal-unattended failure).
///
/// Decision:
/// - paired (config + token file) → headless: start sync, no wizard.
/// - config but no token file → LEGACY (pre-0.4.29 keyring-only) install:
///   still start sync — the async-path `token_store::load()` performs the
///   one-time keyring→file migration OFF the main thread (worst case one
///   final prompt, then never again). Not paired by the non-prompting
///   predicate, so the wizard also shows (same as the old not-paired UX).
///   Refusing to start sync here would strand every pre-0.4.29 host: the
///   migration shim only runs when load() runs.
/// - no config → pairing wizard only.
pub fn launch_decision(has_subscriber_config: bool, has_persisted_token: bool) -> LaunchDecision {
    LaunchDecision {
        start_sync: has_subscriber_config,
        show_wizard: !(has_subscriber_config && has_persisted_token),
    }
}

/// S477 §3.2: enumerate immediate subdirectories of `vaults_root` for the
/// wizard's Paired panel detected-vaults list. Pure stdlib + platform-agnostic.
#[tauri::command]
fn list_vault_folders(vaults_root: String) -> Vec<commands_vaults::VaultFolderInfo> {
    commands_vaults::list_vault_folders_impl(std::path::Path::new(&vaults_root))
}

/// TKT-31dc4efa wiring: build the tray ICON only when the plugin policy says
/// so (vault mode). `run()` routes its single `tray::build_tray` call through
/// here, so the wiring test covers the real gate. Returns whether `build` ran.
fn build_tray_if_enabled<E>(
    policy: &managed_instance::PluginPolicy,
    build: impl FnOnce() -> Result<(), E>,
) -> Result<bool, E> {
    if policy.tray {
        build()?;
        Ok(true)
    } else {
        tracing::info!("managed-instance mode: tray icon suppressed (TKT-31dc4efa)");
        Ok(false)
    }
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    // Managed-instance mode (spec §5, 2026-08-08): resolve ONCE, up front.
    // `managed_config_dir()` canonicalizes (and creates) the dir — or panics
    // if the env is set but unresolvable (fail closed; never fall back to the
    // vault instance's config).
    let managed_dir = managed_instance::managed_config_dir();
    let policy = managed_instance::plugin_policy(managed_dir.is_some());

    // Exclusive advisory lock, fail-closed: one daemon per canonical config
    // dir, acquired BEFORE any pipeline starts and held for the process
    // lifetime (leaked on purpose — the daemon never re-enters run()). A
    // second process against the same dir, still refused after the 10 s
    // retry window, exits 0 with the distinct lock-held line (TKT-a6e5c227:
    // a supervisor must not relaunch a duplicate); an I/O error exits 1.
    if let Some(dir) = &managed_dir {
        let lock = managed_instance::acquire_daemon_lock_or_exit(dir);
        tracing::info!(
            lock = %lock.path.display(),
            config_dir = %dir.display(),
            "managed-instance mode: config dir lock acquired"
        );
        Box::leak(Box::new(lock));
    }

    let mut builder = tauri::Builder::default();
    // R12: tauri_plugin_single_instance is app-global (keyed by app identity,
    // not config dir) — in managed mode it would wrongly couple the vault
    // instance and every managed instance of the same binary. The per-config-
    // dir daemon.lock above is the managed-mode exclusivity mechanism.
    if policy.single_instance {
        builder = builder.plugin(tauri_plugin_single_instance::init(|app, argv, _| {
            // S477 §3.3 (v0.3.7): second-launch "find-me" path -- raise the
            // wizard if it's hidden or minimized, then focus it. Without
            // .show() + .unminimize() the user clicking the dock/.app a
            // second time sees nothing happen.
            //
            // S489: but a `--silent` second launch is a BACKGROUND respawn
            // (login autostart, or a launchd KeepAlive agent relaunching the
            // daemon after a quit/crash) -- NOT a user asking to see the
            // window. Raising the wizard on those turns any external respawn
            // into a popup loop: quit -> respawn(--silent) -> show() -> quit...
            // Only a genuine user re-launch (no --silent) should find-me the
            // window. The daemon is tray-resident; background relaunches must
            // stay silent.
            if argv.iter().any(|a| a == "--silent") {
                tracing::info!("single_instance: --silent relaunch — not raising window");
                return;
            }
            if let Some(w) = app.get_webview_window("main") {
                let _ = w.show();
                let _ = w.unminimize();
                let _ = w.set_focus();
            }
        }));
    }
    // Autostart: login-item registration is app-global; a managed instance's
    // lifecycle belongs to the unit manager, so it never touches login items.
    if policy.autostart {
        builder = builder.plugin(tauri_plugin_autostart::init(
            tauri_plugin_autostart::MacosLauncher::LaunchAgent,
            Some(vec!["--silent"]),
        ));
    }
    // Updater: the vault instance is the sole update leader for the shared
    // binary; managed instances never self-update (the plugin is not even
    // registered, and spawn_updater_check below is policy-gated to match).
    if policy.updater {
        builder = builder.plugin(tauri_plugin_updater::Builder::new().build());
    }
    builder
        .plugin(tauri_plugin_store::Builder::default().build())
        .plugin(tauri_plugin_shell::init())
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_notification::init())
        .invoke_handler(tauri::generate_handler![
            pairing::pair,
            pairing::patch_self_subscriber,
            pairing::load_current_config,
            pairing::load_current_token,
            commands::verify_repair_run,
            commands::list_conflicts,
            list_vault_folders
        ])
        .setup(move |app| {
            // v0.1.6: set_activation_policy returns () on macOS (infallible).
            // The static LSUIElement=true in Info.plist is the canonical
            // path; this is defense-in-depth.
            #[cfg(target_os = "macos")]
            {
                use tauri::ActivationPolicy;
                app.set_activation_policy(ActivationPolicy::Accessory);
                tracing::info!("set_activation_policy(Accessory) invoked");
            }

            let cfg_path = config::default_config_path();
            // CS-3 §3.1 / §3.5 steps 2-4: the ONE config read of this process
            // (CS-1 startup maintenance first). The tray state, the launch
            // gate, the CS-2 bootstrap set S, the pipelines and V&R all read
            // this one immutable snapshot (replaces loads L1-L4).
            let boot_cfg = ownership::BootConfig::load_at_startup(&cfg_path);
            let boot::StartupPlan {
                snapshot,
                tray: shared_state,
                decision,
            } = boot::plan_startup(&boot_cfg);

            // TKT-31dc4efa: a managed instance is a second copy of the same
            // app — building its tray put a second, identical Nexus Sync icon
            // in the macOS menu bar. Skip ONLY the icon UI; `shared_state` is
            // still created above and handed to every pipeline below. Every
            // tray-UI path (2s refresher, icon animation, menu handlers with
            // their `tray_by_id` lookups, TrayMenuHandles) lives inside
            // build_tray, so none of it exists without the icon.
            build_tray_if_enabled(&policy, || {
                tray::build_tray(app.handle(), shared_state.clone())
            })?;

            // CS-3 D6: the handles slot is managed ALWAYS (even unpaired), so
            // V&R can refuse with NotPublished instead of failing to resolve.
            let slot = sync_handles::SyncHandlesSlot::default();
            app.manage(slot.clone());
            // CS-3 §3.5: the process supervisor (liveness, exit_once).
            let sup = supervise::Supervisor::process(std::sync::Arc::new(boot::TauriHost(
                app.handle().clone(),
            )));
            // TKT-1c728ac4 follow-up 2: the tray's click-to-update reaches the
            // supervisor here, so it restarts through `restart_once`.
            app.manage(sup.clone());

            // S484: silent auto-update — checks on startup AND every 5 min,
            // using the pubkey + endpoints in tauri.conf.json. On a staged
            // update it restarts the daemon when idle, through `exit_once`
            // (CS-3 r2.1: the same class of non-74 leaver as the watchdog).
            // Managed-instance mode: NOT spawned (the vault instance is the
            // sole update leader).
            if policy.updater {
                spawn_updater_check(&sup, app.handle().clone(), shared_state.clone());
            }

            // v0.3: app has no meaning without a valid pairing; the wizard
            // opens when config or token is missing (decision from the
            // snapshot, above).
            if decision.start_sync {
                if let Some(snapshot) = snapshot {
                    // CS-3 §3.5 step 6: CS-2 bootstrap with the snapshot's S
                    // (synchronous, before any store load or pipeline), the
                    // supervised custody tick and config_watch, then the boot
                    // task, which loads nothing.
                    boot::start_sync(boot::BootEnv {
                        sup,
                        snapshot,
                        slot,
                        tray: shared_state,
                        workspace: commands::resolve_workspace_root(),
                        knobs: boot::BootKnobs::default(),
                    });
                }
            }
            // Managed-instance mode implies --silent semantics: NEVER raise
            // the wizard/GUI (policy.allow_wizard=false). An unpaired managed
            // instance logs and stays headless — pairing it is the launcher/
            // provisioning flow's job, not an interactive window's.
            if decision.show_wizard && policy.allow_wizard {
                tracing::info!("not paired (no persisted token file) — opening pairing wizard");
                if let Some(w) = app.get_webview_window("main") {
                    let _ = w.show();
                    let _ = w.set_focus();
                }
            } else if decision.show_wizard {
                tracing::warn!(
                    "not paired, but managed-instance mode is silent — wizard suppressed; \
                     pair headlessly (`vault-sync-daemon pair …`) with NEXUS_SYNC_CONFIG_DIR set"
                );
            }

            // v0.3.3: intercept the main window's close button so it HIDES
            // instead of destroying the window. Default Tauri behavior on
            // Windows is to terminate the whole app when the last window
            // closes -- which killed the daemon every time Cyril hit the X
            // on the wizard. The daemon is tray-resident; window lifecycle
            // must not control daemon lifecycle. Cyril S476 verbatim:
            //     "closing the settings window closed the app"
            if let Some(win) = app.get_webview_window("main") {
                let win_for_handler = win.clone();
                win.on_window_event(move |event| {
                    if let tauri::WindowEvent::CloseRequested { api, .. } = event {
                        api.prevent_close();
                        let _ = win_for_handler.hide();
                    }
                });
            }

            Ok(())
        })
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}

/// Pure decision: should the daemon restart NOW to apply a staged update?
///
/// `update_staged` — an update has been downloaded+installed and awaits restart.
/// `secs_since_staged` — seconds since it was staged (drives the max-defer ceiling).
/// `secs_since_activity` — seconds since the last SSE/FS event (None = never).
/// Idle = no pending uploads, not verifying, not reconciling, and quiescent for
/// `quiescent_secs`. The redflag/halt state is intentionally NOT an input — a
/// wedged daemon must still be able to take an update. The max-defer ceiling
/// forces a restart after `max_defer_secs` regardless of busyness so an
/// always-busy host can't starve (load-bearing on macOS/Linux; on Windows the
/// NSIS update force-exits + relaunches on its own).
#[allow(clippy::too_many_arguments)]
pub fn should_restart_now(
    update_staged: bool,
    secs_since_staged: u64,
    secs_since_activity: Option<u64>,
    uploads_pending: u64,
    verify_in_progress: bool,
    recon_in_progress: bool,
    quiescent_secs: u64,
    max_defer_secs: u64,
) -> bool {
    if !update_staged {
        return false;
    }
    if secs_since_staged >= max_defer_secs {
        return true;
    }
    if uploads_pending > 0 || verify_in_progress || recon_in_progress {
        return false;
    }
    match secs_since_activity {
        Some(s) => s >= quiescent_secs,
        None => true,
    }
}

/// Check for a daemon update from the Nexus-served release endpoint. If
/// available, download + install in the background, then restart the daemon
/// once it is idle (or the max-defer ceiling is hit). v0.1.4 shipped a fire-
/// once check; S484 (v0.4.2) adds a 6h periodic loop + restart-when-idle.
fn spawn_updater_check(
    sup: &std::sync::Arc<supervise::Supervisor>,
    app: tauri::AppHandle,
    tray_state: tray_state::SharedTrayState,
) {
    use std::time::{Duration, Instant};
    // v0.4.12: poll every 5 min (was 6h) so a PUSHED release is detected and
    // surfaced on the tray within minutes — auto-update must react to a push,
    // not wait for the next restart. The check is a tiny GET; 5 min is cheap.
    const CHECK_INTERVAL: Duration = Duration::from_secs(300);
    const POLL_WHEN_STAGED: Duration = Duration::from_secs(60);
    const QUIESCENT_SECS: u64 = 300;
    const MAX_DEFER_SECS: u64 = 24 * 3600;
    let leaver = std::sync::Arc::clone(sup);
    sup.supervise(
        supervise::TaskSpec::new("updater", supervise::TaskClass::Optional),
        async move {
            let mut staged_at: Option<Instant> = None;
            loop {
                // Only check for a new update while none is staged (else the running
                // binary's version stays old and we'd re-download every cycle).
                if staged_at.is_none() {
                    match app.updater() {
                        Ok(updater) => match updater.check().await {
                            Ok(Some(update)) => {
                                tracing::info!(
                                    "update available: {} -> {}; downloading",
                                    env!("CARGO_PKG_VERSION"),
                                    update.version
                                );
                                // v0.4.12: light up the tray indicator the moment we
                                // detect the pushed release — obvious + persistent
                                // until the restart applies it (the user can click
                                // the tray item to apply now, or it auto-applies on
                                // idle below).
                                if let Ok(mut s) = tray_state.write() {
                                    s.set_update_available(Some(update.version.clone()));
                                }
                                match update.download_and_install(|_, _| {}, || {}).await {
                                    Ok(()) => {
                                        staged_at = Some(Instant::now());
                                        tracing::info!("update staged; restart when idle");
                                    }
                                    Err(e) => tracing::warn!("download_and_install failed: {e}"),
                                }
                            }
                            Ok(None) => tracing::debug!("no update available"),
                            Err(e) => tracing::warn!("update check failed: {e}"),
                        },
                        Err(e) => tracing::warn!("updater init failed: {e}"),
                    }
                }
                if let Some(since) = staged_at {
                    let (secs_activity, pending, verify, recon) = match tray_state.read() {
                        Ok(s) => (
                            s.last_event_at
                                .and_then(|t| t.elapsed().ok())
                                .map(|d| d.as_secs()),
                            s.uploads_pending as u64,
                            s.verify_in_progress,
                            s.recon_in_progress,
                        ),
                        Err(_) => (None, 0, false, false),
                    };
                    if should_restart_now(
                        true,
                        since.elapsed().as_secs(),
                        secs_activity,
                        pending,
                        verify,
                        recon,
                        QUIESCENT_SECS,
                        MAX_DEFER_SECS,
                    ) {
                        tracing::info!("applying staged update — restarting daemon");
                        // CS-3 r2.1 (lib.rs:421@23ef189d): through exit_once with
                        // the C9 re-check; nothing happens when an exit or restart
                        // is already claimed.
                        leaver.restart_once("updater");
                    }
                }
                let nap = if staged_at.is_some() {
                    POLL_WHEN_STAGED
                } else {
                    CHECK_INTERVAL
                };
                tokio::time::sleep(nap).await;
            }
        },
    );
}

/// B2 (Nexus Sync): pure helper that extracts the ordered list of
/// `(watch_root, route)` pairs from `cfg.sync_roots`.
///
/// Callers iterate this list and spawn one push+watch pipeline per entry.
/// The returned `PathBuf` is the directory passed verbatim as the watcher
/// root; pushed paths will be computed RELATIVE to that directory by
/// `FileWatcher`. No vault-name / first-segment prefix is added here —
/// path namespacing is handled server-side via the `route` field.
///
/// Returns an empty Vec only when `cfg.sync_roots` is empty (which the B1
/// back-compat logic prevents in practice — at minimum one entry is
/// synthesised from `vaults_root`).
pub fn roots_to_watch(cfg: &config::Config) -> Vec<(std::path::PathBuf, String)> {
    cfg.sync_roots
        .iter()
        .map(|sr| (sr.path.clone(), sr.route.clone()))
        .collect()
}

/// B2b (Nexus Sync): resolve the effective subscriber_id for a single
/// sync root.
///
/// Priority rule:
/// 1. If `root.subscriber_id` is non-empty, use it — the root has its own
///    server-registered subscriber (assigned at pairing).
/// 2. Otherwise fall back to `cfg.subscriber_id` — the top-level (vault-
///    root) subscriber, which is the only subscriber on existing installs
///    and is always set by `from_toml_back_compat` on the synthesised entry.
///
/// This ensures the vault root keeps pushing under the existing subscriber
/// after a B2b upgrade (back-compat) while new roots added via
/// `[[sync_roots]]` blocks without a `subscriber_id` yet fall back to the
/// same top-level subscriber until they are properly paired.
pub fn effective_subscriber_id(root: &config::SyncRoot, cfg: &config::Config) -> String {
    if !root.subscriber_id.is_empty() {
        root.subscriber_id.clone()
    } else {
        cfg.subscriber_id.clone()
    }
}

/// One iteration of the redflag monitor. Extracted as a `pub fn` so it can
/// be unit-tested directly without spawning a tokio task.
///
/// Semantics:
/// - If the gate now reports `Tripped`, do nothing (the startup path is
///   responsible for the initial trip + log; subsequent re-trips during a
///   already-tripped session are no-ops).
/// - If the gate reports `Clear` AND the tray flag was previously true,
///   flip the tray flag back to false and log a recovery line. For v0.3.0
///   the daemon does NOT auto-restart sync on recovery — owner must
///   restart the daemon. Recovery just unblocks the tray UI.
pub fn redflag_tick(gate: &redflag::RedflagGate, tray_state: &tray_state::SharedTrayState) {
    match gate.check() {
        redflag::RedflagStatus::Tripped { .. } => { /* still tripped — no-op */ }
        redflag::RedflagStatus::Clear => {
            let was_tripped = tray_state
                .read()
                .map(|s| s.redflag_tripped)
                .unwrap_or(false);
            if was_tripped {
                tracing::info!("redflag.md removed; tray cleared. Restart daemon to resume sync.");
                if let Ok(mut s) = tray_state.write() {
                    s.set_redflag_tripped(false);
                }
            }
        }
    }
}

/// Unit CS-3 in-process tests (T-CS3-3..-22).
#[cfg(test)]
#[path = "cs3_tests.rs"]
mod cs3_tests;

/// Unit CS-3 daemon-subprocess harness and tests.
#[cfg(test)]
#[path = "cs3_harness.rs"]
mod cs3_harness;

#[cfg(test)]
#[allow(clippy::disallowed_methods)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::{Arc, RwLock};

    #[test]
    fn redflag_monitor_clears_tray_on_file_removal() {
        let dir = tempfile::TempDir::new().unwrap();
        let vault_path = dir.path().to_path_buf();
        let redflag_path = vault_path.join("redflag.md");

        // Seed the file present + tray flag true.
        std::fs::write(&redflag_path, b"halt").unwrap();
        let tray_state: tray_state::SharedTrayState =
            Arc::new(RwLock::new(tray_state::TrayState::new(
                "sub-test".to_string(),
                "https://example".to_string(),
                vault_path.clone(),
            )));
        tray_state.write().unwrap().set_redflag_tripped(true);

        let gate = redflag::RedflagGate::new(vault_path.clone());

        // Tick with file present — flag stays true.
        redflag_tick(&gate, &tray_state);
        assert!(
            tray_state.read().unwrap().redflag_tripped,
            "tray flag must stay true while redflag.md is present"
        );

        // Remove the file and tick again — flag flips to false.
        std::fs::remove_file(&redflag_path).unwrap();
        redflag_tick(&gate, &tray_state);
        assert!(
            !tray_state.read().unwrap().redflag_tripped,
            "tray flag must clear once redflag.md is removed"
        );

        // Suppress unused-import warning when no other test consumes it.
        let _ = PathBuf::new();
    }

    /// TKT-31dc4efa: in managed-instance mode NO tray icon is built, so every
    /// tray-STATE writer the headless pipelines drive (updater indicator,
    /// conflict counter, redflag monitor, recon backstop, verify flag,
    /// delete-burst valve) must work against a bare SharedTrayState with no
    /// TrayIcon / TrayMenuHandles anywhere — no panic, state still updates,
    /// and the tooltip text still renders (for logs) without a tray.
    /// Wiring test (TKT-31dc4efa): the gate `run()` uses around
    /// `tray::build_tray`. Managed policy must NOT invoke the builder; vault
    /// policy must invoke it exactly once and propagate its error.
    #[test]
    fn wiring_tray_gate_skips_builder_in_managed_mode() {
        let calls = std::cell::Cell::new(0u32);
        let managed = managed_instance::plugin_policy(true);
        let built = build_tray_if_enabled(&managed, || {
            calls.set(calls.get() + 1);
            Ok::<(), String>(())
        })
        .unwrap();
        assert!(!built, "managed mode must not build the tray icon");
        assert_eq!(calls.get(), 0, "managed mode must not call build_tray");

        let vault = managed_instance::plugin_policy(false);
        let built = build_tray_if_enabled(&vault, || {
            calls.set(calls.get() + 1);
            Ok::<(), String>(())
        })
        .unwrap();
        assert!(built, "vault mode must build the tray icon");
        assert_eq!(calls.get(), 1, "vault mode calls build_tray exactly once");

        let err = build_tray_if_enabled(&vault, || Err::<(), String>("boom".into()));
        assert_eq!(err, Err("boom".to_string()), "builder error must propagate");
    }

    #[test]
    fn tray_state_writers_are_safe_with_no_tray_icon_built() {
        let dir = tempfile::TempDir::new().unwrap();
        let vault_path = dir.path().to_path_buf();
        let policy = managed_instance::plugin_policy(true);
        assert!(!policy.tray, "precondition: managed mode builds no tray");

        let tray_state: tray_state::SharedTrayState =
            Arc::new(RwLock::new(tray_state::TrayState::new(
                "sub-managed".to_string(),
                "https://example".to_string(),
                vault_path.clone(),
            )));
        {
            let mut s = tray_state.write().unwrap();
            s.set_update_available(Some("9.9.9".to_string()));
            s.set_conflict_unresolved(2);
            s.set_verify_in_progress(true);
            s.set_verify_in_progress(false);
            s.set_recon_in_progress(true);
            s.note_recon_pass(3, 4);
            s.set_delete_burst_paused(true);
            s.set_delete_burst_paused(false);
            s.set_redflag_tripped(true);
        }
        // redflag monitor tick: file absent → clears the flag, no tray needed.
        let gate = redflag::RedflagGate::new(vault_path);
        redflag_tick(&gate, &tray_state);

        let s = tray_state.read().unwrap();
        assert_eq!(s.update_available.as_deref(), Some("9.9.9"));
        assert_eq!(s.conflict_unresolved, 2);
        assert!(!s.redflag_tripped);
        assert!(!tray::build_tooltip(&s).is_empty());
    }

    // CS-2: `push_journal_path_identical_from_both_call_sites` was removed
    // with `commands::push_journal_path`. The push pipeline and
    // verify_repair now both resolve the journal as
    // `state_io::registry().get(sub)` + `StateFile::PushJournal`: one held
    // directory, one fixed name, no path to compare.

    // -------------------------------------------------------------------------
    // B2: roots_to_watch helper tests
    // -------------------------------------------------------------------------

    fn make_config_with_sync_roots(sync_roots: Vec<config::SyncRoot>) -> config::Config {
        config::Config {
            nexus_url: "https://nexus.example.com".into(),
            subscriber_id: "sub-b2-test".into(),
            vaults_root: PathBuf::from("/Users/test/Vaults"),
            daemon_version: "0.4.0".into(),
            daemon_platform: "test-platform".into(),
            last_event_id: None,
            sync_roots,
        }
    }

    /// roots_to_watch returns one entry per sync_root entry — verifies the
    /// iteration contract that drives the per-root watch+push pipeline spawn.
    #[test]
    fn roots_to_watch_returns_one_entry_per_sync_root() {
        let cfg = make_config_with_sync_roots(vec![
            config::SyncRoot {
                path: PathBuf::from("/Vaults/Mainframe"),
                route: String::new(),
                subscriber_id: String::new(),
            },
            config::SyncRoot {
                path: PathBuf::from("/Vaults/Dev"),
                route: "dev".into(),
                subscriber_id: String::new(),
            },
        ]);
        let result = roots_to_watch(&cfg);
        assert_eq!(result.len(), 2, "expected 2 entries for a 2-root config");
        assert_eq!(result[0].0, PathBuf::from("/Vaults/Mainframe"));
        assert_eq!(result[0].1, "");
        assert_eq!(result[1].0, PathBuf::from("/Vaults/Dev"));
        assert_eq!(result[1].1, "dev");
    }

    /// roots_to_watch returns exactly 1 entry for a back-compat single-root
    /// config (the B1 synthesised case: vaults_root with no sync_roots in TOML
    /// produces exactly one SyncRoot entry).
    #[test]
    fn roots_to_watch_returns_one_entry_for_back_compat_single_root() {
        let cfg = make_config_with_sync_roots(vec![config::SyncRoot {
            path: PathBuf::from("/Vaults/Mainframe"),
            route: String::new(),
            subscriber_id: String::new(),
        }]);
        let result = roots_to_watch(&cfg);
        assert_eq!(
            result.len(),
            1,
            "expected 1 entry for a back-compat single-root config"
        );
        assert_eq!(result[0].0, PathBuf::from("/Vaults/Mainframe"));
        assert_eq!(result[0].1, "");
    }

    /// roots_to_watch returns an empty Vec when sync_roots is empty.
    /// This is the degenerate case that triggers the "warn and skip" path
    /// in spawn_sse_consumer.
    #[test]
    fn roots_to_watch_returns_empty_for_empty_sync_roots() {
        let cfg = make_config_with_sync_roots(vec![]);
        let result = roots_to_watch(&cfg);
        assert!(result.is_empty(), "expected empty Vec for empty sync_roots");
    }

    /// roots_to_watch preserves order — the first entry returned is the
    /// primary root used for the redflag gate and the SSE materializer.
    #[test]
    fn roots_to_watch_preserves_sync_roots_order() {
        let paths = [
            PathBuf::from("/alpha"),
            PathBuf::from("/beta"),
            PathBuf::from("/gamma"),
        ];
        let sync_roots = paths
            .iter()
            .enumerate()
            .map(|(i, p)| config::SyncRoot {
                path: p.clone(),
                route: format!("route-{i}"),
                subscriber_id: String::new(),
            })
            .collect();
        let cfg = make_config_with_sync_roots(sync_roots);
        let result = roots_to_watch(&cfg);
        assert_eq!(result.len(), 3);
        for (i, expected_path) in paths.iter().enumerate() {
            assert_eq!(
                &result[i].0, expected_path,
                "entry {i} path must match sync_roots[{i}].path"
            );
            assert_eq!(
                result[i].1,
                format!("route-{i}"),
                "entry {i} route must match sync_roots[{i}].route"
            );
        }
    }

    // -------------------------------------------------------------------------
    // B2b: effective_subscriber_id resolver tests
    // -------------------------------------------------------------------------

    fn make_cfg_with_top_level_sub(sub: &str) -> config::Config {
        config::Config {
            nexus_url: "https://nexus.example.com".into(),
            subscriber_id: sub.into(),
            vaults_root: PathBuf::from("/Users/test/Vaults"),
            daemon_version: "0.4.0".into(),
            daemon_platform: "test-platform".into(),
            last_event_id: None,
            sync_roots: vec![],
        }
    }

    /// When a root has its own non-empty subscriber_id, that value is used —
    /// NOT the top-level cfg.subscriber_id.
    #[test]
    fn effective_subscriber_id_uses_root_then_falls_back() {
        let cfg = make_cfg_with_top_level_sub("sub-top-level");

        // Root with its own subscriber_id → root wins.
        let root_with_sub = config::SyncRoot {
            path: PathBuf::from("/Vaults/Dev"),
            route: "dev".into(),
            subscriber_id: "sub-dev".into(),
        };
        assert_eq!(
            effective_subscriber_id(&root_with_sub, &cfg),
            "sub-dev",
            "non-empty root.subscriber_id must be preferred over cfg.subscriber_id"
        );

        // Root with empty subscriber_id → falls back to cfg.
        let root_without_sub = config::SyncRoot {
            path: PathBuf::from("/Vaults/Mainframe"),
            route: String::new(),
            subscriber_id: String::new(),
        };
        assert_eq!(
            effective_subscriber_id(&root_without_sub, &cfg),
            "sub-top-level",
            "empty root.subscriber_id must fall back to cfg.subscriber_id"
        );
    }
}

#[cfg(test)]
#[allow(clippy::disallowed_methods)]
mod launch_gate_tests {
    use super::launch_decision;

    /// v0.4.29 Change 2: paired (config subscriber_id + persisted token file)
    /// → fully headless: sync starts, wizard stays hidden. This is the
    /// unattended-auto-update invariant.
    #[test]
    fn paired_host_is_headless() {
        let d = launch_decision(true, true);
        assert!(d.start_sync, "paired host must start sync");
        assert!(!d.show_wizard, "paired host must NOT show the wizard");
    }

    /// Legacy (pre-0.4.29) install: config exists but the token file hasn't
    /// been migrated yet. Sync MUST still start — the async-path
    /// token_store::load() performs the one-time keyring→file migration off
    /// the main thread. Not paired by the non-prompting predicate → wizard
    /// shows (matches the old not-paired UX; never a main-thread prompt).
    #[test]
    fn legacy_config_without_token_file_still_starts_sync() {
        let d = launch_decision(true, false);
        assert!(
            d.start_sync,
            "legacy keyring-only install must still start sync (migration path)"
        );
        assert!(
            d.show_wizard,
            "not paired by the non-prompting check → wizard"
        );
    }

    /// Never paired: no subscriber_id config at all → wizard only.
    #[test]
    fn unpaired_host_shows_wizard_only() {
        let d = launch_decision(false, false);
        assert!(!d.start_sync, "no config → nothing to sync");
        assert!(d.show_wizard, "no config → pairing wizard");
    }

    /// Degenerate: token file but no config (config deleted by hand). No
    /// subscriber to sync under → wizard.
    #[test]
    fn token_without_config_shows_wizard() {
        let d = launch_decision(false, true);
        assert!(!d.start_sync);
        assert!(d.show_wizard);
    }
}

#[cfg(test)]
#[allow(clippy::disallowed_methods)]
mod restart_gate_tests {
    use super::should_restart_now;

    #[test]
    fn restart_gate_logic() {
        let q = 300u64; // quiescent secs
        let md = 86_400u64; // max-defer secs
                            // No update staged → never restart.
        assert!(!should_restart_now(false, 0, None, 0, false, false, q, md));
        // Staged + never-any-activity → restart.
        assert!(should_restart_now(true, 10, None, 0, false, false, q, md));
        // Staged + uploads pending → wait.
        assert!(!should_restart_now(true, 10, None, 3, false, false, q, md));
        // Staged + verify in progress → wait.
        assert!(!should_restart_now(
            true,
            10,
            Some(999),
            0,
            true,
            false,
            q,
            md
        ));
        // Staged + recent activity (< quiescent) → wait.
        assert!(!should_restart_now(
            true,
            10,
            Some(60),
            0,
            false,
            false,
            q,
            md
        ));
        // Staged + quiescent elapsed → restart.
        assert!(should_restart_now(
            true,
            10,
            Some(400),
            0,
            false,
            false,
            q,
            md
        ));
        // Max-defer elapsed overrides busyness → restart even if busy.
        assert!(should_restart_now(
            true,
            90_000,
            Some(1),
            5,
            true,
            true,
            q,
            md
        ));
    }
}
