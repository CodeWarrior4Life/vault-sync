pub mod api_client;
pub mod commands;
pub mod commands_vaults;
pub mod config;
pub mod conflict_stash;
pub mod echo_guard;
pub mod file_watcher;
pub mod integrity_check;
pub mod keyring;
pub mod materializer;
pub mod obsidian_install_detect;
pub mod obsidian_plugin_detect;
pub mod pairing;
pub mod pull_backfill;
pub mod push_client;
pub mod push_journal;
pub mod rasp_fence;
pub mod reconciliation;
pub mod redflag;
pub mod scope;
pub mod sse;
pub mod sync_health;
pub mod sync_shadow;
pub mod token_store;
pub mod tray;
pub mod tray_state;
pub mod verify_repair;

use tauri::{Emitter, Manager};
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

/// S477 §3.2: enumerate immediate subdirectories of `vaults_root` for the
/// wizard's Paired panel detected-vaults list. Pure stdlib + platform-agnostic.
#[tauri::command]
fn list_vault_folders(vaults_root: String) -> Vec<commands_vaults::VaultFolderInfo> {
    commands_vaults::list_vault_folders_impl(std::path::Path::new(&vaults_root))
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_single_instance::init(|app, argv, _| {
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
        }))
        .plugin(tauri_plugin_autostart::init(
            tauri_plugin_autostart::MacosLauncher::LaunchAgent,
            Some(vec!["--silent"]),
        ))
        .plugin(tauri_plugin_updater::Builder::new().build())
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
        .setup(|app| {
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
            let shared_state = {
                // S477: display the configured vaults_root verbatim (the
                // watch root), NOT vaults_root + vault_name. The daemon
                // watches the entire vaults_root and the wizard surfaces
                // that scope so users can see what's actually being
                // synced.
                let (sub, url, vault_dir) = match config::Config::load_from(&cfg_path) {
                    Ok(c) => (c.subscriber_id, c.nexus_url, c.vaults_root.clone()),
                    Err(_) => (String::new(), String::new(), std::path::PathBuf::new()),
                };
                std::sync::Arc::new(std::sync::RwLock::new(tray_state::TrayState::new(
                    sub, url, vault_dir,
                )))
            };

            tray::build_tray(app.handle(), shared_state.clone())?;

            // S484: silent auto-update — checks on startup AND every 6h, using
            // the pubkey + endpoints in tauri.conf.json. Runs detached so a slow
            // /admin/api/vault-sync/releases/<platform>/latest never blocks SSE
            // startup; on a staged update it restarts the daemon when idle.
            // Failures log only — never block the daemon.
            spawn_updater_check(app.handle().clone(), shared_state.clone());

            // v0.3: app has no meaning without a valid pairing. Open the
            // wizard window automatically if config OR token is missing.
            // Cyril S473: *"the dialog to enter the key should be the first
            // thing that comes up [...] since the app has no meaning without
            // a valid connection to the server"*.
            let has_token = cfg_path.exists()
                && config::Config::load_from(&cfg_path)
                    .ok()
                    .and_then(|c| token_store::load(&c.subscriber_id).ok().flatten())
                    .is_some();
            if has_token {
                spawn_sse_consumer(app.handle().clone(), cfg_path, shared_state);
            } else {
                tracing::info!("no token persisted — opening pairing wizard");
                if let Some(w) = app.get_webview_window("main") {
                    let _ = w.show();
                    let _ = w.set_focus();
                }
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
fn spawn_updater_check(app: tauri::AppHandle, tray_state: tray_state::SharedTrayState) {
    use std::time::{Duration, Instant};
    // v0.4.12: poll every 5 min (was 6h) so a PUSHED release is detected and
    // surfaced on the tray within minutes — auto-update must react to a push,
    // not wait for the next restart. The check is a tiny GET; 5 min is cheap.
    const CHECK_INTERVAL: Duration = Duration::from_secs(300);
    const POLL_WHEN_STAGED: Duration = Duration::from_secs(60);
    const QUIESCENT_SECS: u64 = 300;
    const MAX_DEFER_SECS: u64 = 24 * 3600;
    tauri::async_runtime::spawn(async move {
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
                    app.restart();
                }
            }
            let nap = if staged_at.is_some() {
                POLL_WHEN_STAGED
            } else {
                CHECK_INTERVAL
            };
            tokio::time::sleep(nap).await;
        }
    });
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

/// Boot the SSE consumer from a saved config + keyring token. Runs on the
/// Tauri async runtime so it doesn't block app startup.
fn spawn_sse_consumer(
    app: tauri::AppHandle,
    cfg_path: std::path::PathBuf,
    tray_state: tray_state::SharedTrayState,
) {
    tauri::async_runtime::spawn(async move {
        let cfg = match config::Config::load_from(&cfg_path) {
            Ok(c) => c,
            Err(e) => {
                tracing::error!("config load failed: {e}");
                return;
            }
        };

        // B2: derive the watch roots from sync_roots. Use the first root as
        // the redflag gate anchor (covers the primary vault). A future task
        // can extend this to check all roots.
        let watch_roots = roots_to_watch(&cfg);
        if watch_roots.is_empty() {
            tracing::warn!(
                "sync_roots is empty — no watch pipeline will start. \
                 Check config: at least one [[sync_roots]] entry is required."
            );
            return;
        }
        // Redflag gate: checked against the first sync root (primary vault).
        // Future: extend to all roots (deferred — multi-root redflag is a
        // follow-up task).
        let primary_root = watch_roots[0].0.clone();
        let gate = redflag::RedflagGate::new(primary_root.clone());
        match gate.check() {
            redflag::RedflagStatus::Tripped { path, .. } => {
                tracing::error!(
                    "redflag.md present at {:?} — aborting sync startup until removed",
                    path
                );
                if let Ok(mut s) = tray_state.write() {
                    s.set_redflag_tripped(true);
                }
                // S477 §3.3 (v0.3.7): surface to the user via OS notification
                // so they understand sync is paused (the tray-only daemon
                // would otherwise be silent).
                notify_user(
                    &app,
                    "Vault Sync paused",
                    "redflag.md detected at your vaults root — sync paused until removed.",
                );
                // Do NOT start SSE consumer or push pipeline. The 60s
                // monitor task will clear the flag if/when the file is
                // removed (recovery requires a daemon restart in v0.3.0).
                spawn_redflag_monitor(gate, tray_state.clone());
                return;
            }
            redflag::RedflagStatus::Clear => {
                if let Ok(mut s) = tray_state.write() {
                    s.set_redflag_tripped(false);
                }
                // Start the monitor on the Clear path too — if the file
                // appears mid-session, the monitor catches it on the next
                // tick and updates the tray. v0.3.0 does NOT auto-pause
                // the pipeline mid-session; that's a v0.3.1 follow-up.
                spawn_redflag_monitor(gate.clone(), tray_state.clone());
            }
        }

        let token = match token_store::load(&cfg.subscriber_id) {
            Ok(Some(t)) => t,
            Ok(None) => {
                tracing::error!(
                    "no token in keyring or file fallback for subscriber_id={}; re-pair required",
                    cfg.subscriber_id
                );
                // S477 §3.3 (v0.3.7): surface to the user -- the daemon
                // cannot start sync until the user re-pairs via the wizard.
                notify_user(
                    &app,
                    "Vault Sync needs re-pairing",
                    "Open the wizard to re-pair this device.",
                );
                return;
            }
            Err(e) => {
                tracing::error!("token_store read failed: {e}");
                return;
            }
        };

        // v0.1.4: scan vault for conflicting Obsidian plugins and disable
        // them in community-plugins.json before SSE starts materializing.
        // v0.1.7: scan ALL known Obsidian vaults (not just the configured
        // one) — Cyril's setup has Mainframe at D:\Vaults\Mainframe with
        // nexus-sync enabled there but other vaults under D:\Vaults\ may
        // also have the conflict. Discover via Obsidian's obsidian.json
        // registry.
        {
            let mut vaults_to_scan: Vec<std::path::PathBuf> =
                obsidian_install_detect::find_known_vaults();
            // B2: scan each sync_root.path directly (it IS the vault root
            // now, not a parent container). Also scan any immediate
            // subdirectories that look like Obsidian vaults in case a
            // sync_root is a parent container.
            for (root_path, _route) in &watch_roots {
                // Add the root itself if it has an .obsidian dir.
                if root_path.join(".obsidian").is_dir()
                    && !vaults_to_scan.iter().any(|q| q == root_path)
                {
                    vaults_to_scan.push(root_path.clone());
                }
                // Also enumerate immediate subdirectories (back-compat: in
                // case sync_root is a parent container rather than the
                // vault itself).
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
        let api = match api_client::ApiClient::new(&cfg.nexus_url, &token) {
            Ok(a) => a,
            Err(e) => {
                tracing::error!("api client init failed: {e}");
                return;
            }
        };
        let snap = match api.health().await {
            Ok(s) => s,
            Err(e) => {
                tracing::error!("api health failed: {e}");
                return;
            }
        };

        // S477 v0.3.8 (C): self-report daemon_version + daemon_platform to
        // the server on every startup. Pairs with (B) User-Agent stamping
        // so server logs + admin DB query both agree on "what version is
        // this host running?". Fire-and-forget — failure is non-fatal.
        match api.patch_self_version().await {
            Ok(_) => tracing::info!(
                version = api_client::daemon_version(),
                platform = api_client::daemon_platform(),
                "reported daemon_version + daemon_platform to /api/sync/subscribers/me"
            ),
            Err(e) => tracing::warn!("daemon_version self-report failed: {e}"),
        }
        // v0.3: daemon state OUT of vault (mandate §1 row 13). Workspace
        // root defaults to LocalAppData/Nexus (or platform equivalent);
        // falls back to home dir / temp_dir if data_local_dir() is None.
        let workspace_root = dirs::data_local_dir()
            .unwrap_or_else(|| dirs::home_dir().unwrap_or_else(std::env::temp_dir))
            .join("Nexus");
        let materializer_cfg = materializer::MaterializerConfig {
            device_id: cfg.subscriber_id.clone(),
            ..Default::default()
        };
        // S492 echo guard: shared between the materializer (records its writes)
        // and every file_watcher (skips the resulting echo events). One guard
        // for the daemon so the SSE->materialize->watcher->push loop is broken
        // across all sync_roots.
        let echo_guard = std::sync::Arc::new(echo_guard::EchoGuard::new());
        // opfix-vaultsync-dormancy: shared progress-tracking handle. The
        // PushClient stamps it on every drain that processed >= 1 event; the
        // per-sync_root watchdog reads it to detect "pending diffs but no
        // push attempts for N minutes" (R1+R3) and recovers via app.restart()
        // (R2). One Arc per daemon; all sync_roots share progress liveness so
        // a stall on ANY root trips the watchdog (and the restarted process
        // re-arms a fresh watchdog covering all roots).
        let sync_health = sync_health::SyncHealth::new();
        // fix/reconcile-server-wins-shadow: persistent per-file "last-synced
        // server hash" marker. Shared (Arc) across the materializer (records on
        // every pull), the push_client (records on every accepted push), and the
        // reconcile backstop (reads to decide push-vs-pull on drift). Lives under
        // the per-subscriber sync-state runtime dir, beside last_event_id.
        let shadow = sync_shadow::ShadowStore::load(
            commands::resolve_workspace_root()
                .join(".lattice-runtime")
                .join(&cfg.subscriber_id)
                .join("sync-state")
                .join("shadow_hashes.json"),
        );
        sync_shadow::ShadowStore::spawn_periodic_flush(shadow.clone());
        // B2: materializer uses the primary sync_root.path as vault root.
        // Multi-root materialization is a deferred task (one materializer
        // per sync_root); for now the SSE consumer materializes into the
        // first root only (back-compat with single-vault setups).
        let materializer = materializer::Materializer::new(
            primary_root.clone(),
            snap.shadow_path.clone(),
            materializer::MaterializerMode::from_str(&snap.materializer_mode),
            workspace_root,
            cfg.subscriber_id.clone(),
            materializer_cfg,
        )
        .with_tray_state(tray_state.clone())
        .with_echo_guard(echo_guard.clone())
        .with_shadow_store(shadow.clone());

        // Wave 4: spawn a 60s periodic task that refreshes the tray's
        // `conflict_unresolved` counter from the on-disk stash siblings.
        // `Materializer: Clone` is shallow — PathBuf/String fields + an
        // Arc'd debounce timer — so we can hand a clone to the timer task
        // while the primary instance moves into the SSE consumer.
        let materializer_for_refresh = materializer.clone();
        tauri::async_runtime::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(60));
            // First `tick.tick().await` fires immediately; consume it so we
            // don't double-scan at startup (sse.run() will also drive
            // write-path activity).
            tick.tick().await;
            loop {
                tick.tick().await;
                materializer_for_refresh.refresh_conflict_count_into_tray();
            }
        });

        // B2 (Nexus Sync): push side — iterate sync_roots, spawning one
        // watch+push pipeline per sync_root, each rooted at sync_root.path.
        // Pushed paths are RELATIVE to that root (no vault-name first-segment
        // prepend — the server handles routing via the route field, which is
        // wired per-push in a later task; for now all pipelines share the
        // single cfg.subscriber_id).
        //
        // DEFERRED (multi-route/per-subscriber plumbing): each sync_root
        // carries a `route` string intended for server-side subscriber scope
        // selection. Sending the correct route on each push request and
        // registering per-sync_root subscriber IDs requires controller changes
        // that are out of B2 scope. For now, all roots share one subscriber_id
        // and the route is unused on the push wire. Tracked for a later task.
        //
        // `snap.scope_roots` / `snap.scope_excludes` are consumed by the SSE
        // consumer below, so clone them for each watcher first.
        //
        // The returned `_watch_handles` Vec MUST stay alive for the daemon's
        // lifetime: dropping any handle stops the corresponding OS watcher.
        // We bind the Vec into this async scope so it lives across the
        // forever-awaiting `consumer.run().await` at the tail of this task.
        let watch_scope_roots = snap.scope_roots.clone();
        let watch_scope_excludes = snap.scope_excludes.clone();
        // Resolve the journal workspace root via the SAME helper verify_repair
        // uses (`commands::resolve_workspace_root`), guaranteeing both call
        // sites open the identical push_journal.jsonl file.
        let workspace_root_for_journal = commands::resolve_workspace_root();
        // B2b: iterate sync_roots directly so we have the full SyncRoot struct
        // (including subscriber_id). effective_subscriber_id resolves the
        // per-root subscriber (root's own if set, else top-level fallback).
        let _watch_handles: Vec<_> = cfg
            .sync_roots
            .iter()
            .map(|root| {
                let eff_sub = effective_subscriber_id(root, &cfg);
                // fix/reconcile-server-wins-shadow: hand the reconcile backstop
                // its own materializer clone (to EXECUTE server-wins pulls) +
                // the shared shadow store (to decide push-vs-pull). Clone here,
                // BEFORE the primary materializer moves into the SSE consumer.
                spawn_push_pipeline(
                    &app,
                    &cfg,
                    eff_sub,
                    root.path.clone(),
                    workspace_root_for_journal.clone(),
                    watch_scope_roots.clone(),
                    watch_scope_excludes.clone(),
                    tray_state.clone(),
                    echo_guard.clone(),
                    materializer.clone(),
                    shadow.clone(),
                    sync_health.clone(),
                )
            })
            .collect();

        // S477 v0.3.8 (A): per-subscriber on-disk path for last_event_id
        // persistence. Lives alongside the push_journal under the workspace
        // runtime dir, namespaced by subscriber_id so multi-subscriber
        // hosts don't collide.
        let last_event_id_path = commands::resolve_workspace_root()
            .join(".lattice-runtime")
            .join(&cfg.subscriber_id)
            .join("sync-state")
            .join("last_event_id");
        // Load on startup so catchup-on-reconnect actually fires server-side.
        // The Config.last_event_id field is preserved as a back-compat
        // fallback (older daemons may have populated it via the legacy code
        // path); disk-persisted value takes precedence.
        let resumed_id =
            sse::SseConsumer::load_last_event_id(&last_event_id_path).or(cfg.last_event_id.clone());
        if let Some(rid) = &resumed_id {
            tracing::info!(
                last_event_id = %rid,
                path = %last_event_id_path.display(),
                "resuming SSE from persisted last_event_id"
            );
        }

        let consumer = match sse::SseConsumer::new(
            cfg.nexus_url.clone(),
            token,
            snap.scope_roots,
            snap.scope_excludes,
            materializer,
        ) {
            Ok(c) => c
                .with_tray_state(tray_state)
                .with_last_event_id_path(last_event_id_path),
            Err(e) => {
                tracing::error!("sse consumer init failed: {e}");
                return;
            }
        };
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        tracing::info!(
            "starting SSE consumer for subscriber_id={}",
            cfg.subscriber_id
        );
        // `consumer.run()` awaits forever (it loops reconnecting on the SSE
        // long-poll). `_watch_handle` is held across this await, so the OS
        // watcher + its background task stay alive for the daemon's lifetime.
        if let Err(e) = consumer.run(resumed_id, shutdown_rx).await {
            tracing::error!("SSE consumer exited with error: {e}");
        }
        // Keep all watch handles alive until the task actually unwinds (only
        // reached if the SSE consumer returns, which it normally never does).
        drop(_watch_handles);
    });
}

/// v0.3 push pipeline wire-up. Opens the SHARED push journal (same path
/// verify_repair resolves via `commands::push_journal_path`), starts the
/// file_watcher (local FS edits → journal), and spawns the push_client drain
/// loop (journal → `POST /api/sync/push`). Returns the [`file_watcher::WatchHandle`]
/// which the caller MUST keep alive — dropping it stops the OS watcher.
///
/// ## B2 (Nexus Sync): per-sync_root watch_root
/// `watch_root` is the directory the `FileWatcher` is rooted at — passed
/// directly from `sync_root.path`. Pushed paths are computed RELATIVE to
/// `watch_root` by `FileWatcher`, so NO vault-name / first-segment prefix
/// is added by this function. The old `cfg.vaults_root` field is no longer
/// used in this path.
///
/// ## ApiClient sharing
/// `ApiClient::new` is cheap (a reqwest client + base URL + token header).
/// Rather than thread the SSE-side `api` (built only for the one `health()`
/// call) through an extra `Arc`, we build a FRESH `Arc<ApiClient>` here for the
/// push_client. Both hold the same bearer token; there is no shared mutable
/// state, so a second instance is semantically identical and avoids invasive
/// Arc-threading at the SSE call site.
///
/// ## Journal-path consistency with verify_repair
/// Both call sites resolve the path via `commands::resolve_workspace_root()` +
/// `commands::push_journal_path(&workspace_root, &subscriber_id)` — the single
/// source of truth in `commands.rs`. They therefore open THE SAME jsonl file.
///
/// ## Dual-handle race disposition
/// verify_repair opens its own `PushJournal` handle on the same path while the
/// drain loop here holds another. `PushJournal::open` re-reads the file, and
/// appends are line-atomic, so verify_repair's appends are picked up by the
/// drain loop on its next 5s poll tick. This dual-handle pattern is the
/// documented v0.3.0 limitation (a shared-handle refactor — threading this
/// `Arc<Mutex<PushJournal>>` into verify_repair_run — is deferred to keep this
/// wire-up low-risk). The drain loop never loses appends; worst case is a
/// one-poll-interval (≤5s) latency before verify_repair's enqueued pushes
/// upload.
/// B2b (Nexus Sync): `subscriber_id` is now passed in as the *effective*
/// per-root subscriber, resolved by the caller via
/// `effective_subscriber_id(&root, &cfg)`.  Every internal use of a
/// subscriber ID inside this function (token load, journal path, push
/// client, reconciler, file watcher) now uses this parameter, so each
/// sync root truly pushes under its own registered subscriber.
#[allow(clippy::too_many_arguments)]
fn spawn_push_pipeline(
    app: &tauri::AppHandle,
    cfg: &config::Config,
    subscriber_id: String,
    watch_root: std::path::PathBuf,
    workspace_root: std::path::PathBuf,
    scope_roots: Vec<String>,
    scope_excludes: Vec<String>,
    tray_state: tray_state::SharedTrayState,
    echo_guard: std::sync::Arc<echo_guard::EchoGuard>,
    materializer: materializer::Materializer,
    shadow: std::sync::Arc<sync_shadow::ShadowStore>,
    sync_health: std::sync::Arc<sync_health::SyncHealth>,
) -> Option<file_watcher::WatchHandle> {
    use std::sync::Arc;

    let token = match token_store::load(&subscriber_id) {
        Ok(Some(t)) => t,
        _ => {
            tracing::error!("push pipeline: token unavailable; not starting push side");
            notify_user(
                app,
                "Vault Sync push failed to start",
                "Authentication token unavailable; not starting push side.",
            );
            return None;
        }
    };

    // Shared journal — SAME path verify_repair uses (commands.rs SoT).
    let journal_path = commands::push_journal_path(&workspace_root, &subscriber_id);
    if let Some(parent) = journal_path.parent() {
        if let Err(e) = std::fs::create_dir_all(parent) {
            tracing::error!(
                "push pipeline: create_dir_all({}) failed: {e}; not starting push side",
                parent.display()
            );
            notify_user(
                app,
                "Vault Sync push failed to start",
                &format!("create_dir_all({}) failed: {e}", parent.display()),
            );
            return None;
        }
    }
    let journal = match push_journal::PushJournal::open(&journal_path) {
        Ok(j) => Arc::new(tokio::sync::Mutex::new(j)),
        Err(e) => {
            tracing::error!("push pipeline: journal open failed: {e}; not starting push side");
            notify_user(
                app,
                "Vault Sync push failed to start",
                &format!("journal open failed: {e}"),
            );
            return None;
        }
    };

    // B2: watch_root is passed in directly from sync_root.path —
    // paths will be relative to watch_root (no first-segment prefix).
    let vault_root = watch_root;

    // --- push_client: journal → server drain loop ---
    let api_for_push = match api_client::ApiClient::new(&cfg.nexus_url, &token) {
        Ok(a) => Arc::new(a),
        Err(e) => {
            tracing::error!("push pipeline: api client init failed: {e}; not starting push side");
            notify_user(
                app,
                "Vault Sync push failed to start",
                &format!("api client init failed: {e}"),
            );
            return None;
        }
    };
    // PushClientConfig.allowed_extensions carry a LEADING DOT (".md").
    let push_cfg = push_client::PushClientConfig {
        allowed_extensions: vec![".md".into(), ".canvas".into()],
        strip_frontmatter_fields_for_diff: vec!["updated".into()],
        max_retry_attempts: 5,
        initial_backoff_ms: 500,
        max_backoff_ms: 60_000,
        ..Default::default()
    };
    let push_client = push_client::PushClient::new(
        api_for_push,
        journal.clone(),
        subscriber_id.clone(),
        push_cfg,
        vault_root.clone(),
    )
    .with_tray_state(tray_state.clone())
    .with_shadow_store(shadow.clone())
    .with_sync_health(sync_health.clone());

    // Never-fired shutdown channel — the push loop runs for the daemon's
    // lifetime. We hold the sender in the spawned task so it isn't dropped
    // (a dropped sender would make `changed()` error → loop exit).
    let (push_shutdown_tx, push_shutdown_rx) = tokio::sync::watch::channel(false);
    tracing::info!(
        "starting push_client drain loop for subscriber_id={}",
        subscriber_id
    );
    tauri::async_runtime::spawn(async move {
        let _hold_tx = push_shutdown_tx; // keep sender alive for the loop's lifetime
        push_client.run_loop(push_shutdown_rx).await;
        tracing::warn!("push_client.run_loop returned (unexpected for a forever loop)");
    });

    // opfix-vaultsync-dormancy (R1+R2+R3): spawn the progress-stall watchdog
    // alongside the push loop. It polls the journal depth every 60s; on a
    // stall it fires the recovery closure which notifies the owner AND
    // restarts the daemon (the only reliable way to revive a panicked async
    // task: `tauri::async_runtime::spawn` swallows panics into a JoinHandle
    // nobody awaits, which is precisely how the engine went quiet while the
    // updater kept ticking in incident 2026-06-13).
    {
        let env = reconciliation::ProcessEnv;
        let threshold = sync_health::read_threshold(&env);
        let recovery_disabled = sync_health::is_recovery_disabled(&env);
        let journal_for_watchdog = journal.clone();
        let app_handle = app.clone();
        let subscriber_for_log = subscriber_id.clone();
        let _watchdog = sync_health::spawn_progress_stall_watchdog(
            sync_health.clone(),
            std::time::Duration::from_secs(60),
            threshold,
            recovery_disabled,
            move || {
                // Async closure: the journal is behind a tokio::sync::Mutex,
                // which CANNOT be locked via blocking_lock from a tokio
                // worker thread (panics). Hand the watchdog an async path so
                // it awaits the lock cleanly.
                let j = journal_for_watchdog.clone();
                async move {
                    let g = j.lock().await;
                    g.len()
                }
            },
            move |evt| {
                tracing::error!(
                    subscriber_id = %subscriber_for_log,
                    pending = evt.pending,
                    secs_since_progress = evt.secs_since_progress,
                    "sync_health: push pipeline STALLED; recovering"
                );
                if let Err(e) = app_handle.emit(
                    "sync_stalled",
                    &serde_json::json!({
                        "pending": evt.pending,
                        "secs_since_progress": evt.secs_since_progress,
                        "subscriber_id": subscriber_for_log,
                    }),
                ) {
                    tracing::warn!("failed to emit sync_stalled event: {e}");
                }
                notify_user(
                    &app_handle,
                    "Vault Sync stalled; restarting",
                    &format!(
                        "Push pipeline silent for {}m with {} pending edits. Restarting daemon to recover.",
                        evt.secs_since_progress / 60,
                        evt.pending
                    ),
                );
                if evt.recovery_will_restart {
                    // Hard recovery: re-init all sync tasks via a fresh
                    // process. Matches what should_restart_now plus
                    // app.restart already does on staged-update apply.
                    app_handle.restart();
                }
            },
        );
    }

    // --- S477 v0.3.8 (D) reconciliation backstop ---
    // Periodic background sweep that diffs local vs server (via the same
    // VerifyRepair machinery the owner-invoked tray menu uses) and
    // queues pushes / logs pulls to close any systemic drift. Honors
    // VAULT_SYNC_DISABLE_RECON kill switch + VAULT_SYNC_RECON_INTERVAL_SECS
    // cadence override. Shares the push_client's journal handle so any
    // pushes it queues are picked up by the drain loop above.
    // Non-fatal on api-client init failure — just skip the backstop;
    // push pipeline + file_watcher carry on.
    match api_client::ApiClient::new(&cfg.nexus_url, &token) {
        Ok(recon_api) => {
            // fix/reconcile-server-wins-shadow: pass the materializer (to
            // execute server-wins pulls) + shadow store (to decide direction).
            let _recon_task = reconciliation::spawn_reconciliation_task(
                vault_root.clone(),
                Arc::new(recon_api),
                journal.clone(),
                subscriber_id.clone(),
                tray_state.clone(),
                materializer.clone(),
                shadow.clone(),
            );
        }
        Err(e) => {
            tracing::warn!("reconciliation: api client init failed: {e}; backstop not spawned");
        }
    }

    // R6 pull-backfill: full server→local completeness pass. The reconciliation
    // backstop above only reconciles paths that ALREADY exist locally (it sends
    // its local manifest to /reconcile-batch); a note that exists only on the
    // server is never in that manifest and is never pulled. This pass closes
    // that gap by paging GET /changes?since=0 (the full canonical enumeration)
    // and materializing every locally-missing, non-substrate note — the exact
    // create-write the materializer was always capable of but was never asked
    // to do. Honors VAULT_SYNC_DISABLE_BACKFILL + VAULT_SYNC_BACKFILL_INTERVAL_SECS.
    // Non-fatal on api-client init failure.
    match api_client::ApiClient::new(&cfg.nexus_url, &token) {
        Ok(backfill_api) => {
            let _backfill_task = pull_backfill::spawn_pull_backfill_task(
                Arc::new(backfill_api),
                materializer.clone(),
            );
        }
        Err(e) => {
            tracing::warn!("pull_backfill: api client init failed: {e}; backfill not spawned");
        }
    }

    // --- file_watcher: local FS edits → journal ---
    // SUBSTRATE NOTE: FileWatcher requires `Arc<std::sync::Mutex<PushJournal>>`
    // (its append path is synchronous, inside the notify task), whereas
    // PushClient + verify_repair require `Arc<tokio::sync::Mutex<PushJournal>>`
    // (async drain). These mutex types are NOT interchangeable, so the watcher
    // CANNOT share the `journal` handle above. We open a SEPARATE PushJournal
    // handle on the SAME file path. This widens the documented dual-handle
    // pattern to three handles (watcher std-mutex, push_client tokio-mutex,
    // verify_repair tokio-mutex) — all on one jsonl file. Appends are
    // line-atomic and the drain loop re-reads, so no append is lost; worst
    // case is ≤5s poll latency. A unified single-mutex journal is deferred.
    let watcher_journal = match push_journal::PushJournal::open(&journal_path) {
        Ok(j) => Arc::new(std::sync::Mutex::new(j)),
        Err(e) => {
            tracing::error!(
                "push pipeline: watcher journal open failed: {e}; push_client running but no local-edit detection"
            );
            notify_user(
                app,
                "Vault Sync push failed to start",
                &format!("watcher journal open failed: {e}"),
            );
            return None;
        }
    };
    // FileWatcherConfig.allowed_extensions are WITHOUT a leading dot ("md").
    let burst = Arc::new(std::sync::Mutex::new(redflag::DeleteBurstDetector::new(
        20,
        std::time::Duration::from_secs(30),
    )));
    // S484: expose the valve so the tray "Resume delete propagation" action
    // can reset it in place (no daemon restart). Register before `burst` is
    // moved into the file watcher below.
    redflag::register_delete_burst_handle(burst.clone());
    let watcher_cfg = file_watcher::FileWatcherConfig {
        allowed_extensions: vec!["md".into(), "canvas".into()],
        scope_roots,
        scope_excludes,
        debounce_ms: 500,
    };
    let watcher = match file_watcher::FileWatcher::new(
        vault_root,
        watcher_journal,
        burst,
        watcher_cfg,
        subscriber_id.clone(),
    ) {
        Ok(w) => w.with_tray_state(tray_state).with_echo_guard(echo_guard),
        Err(e) => {
            tracing::error!("push pipeline: file_watcher init failed: {e}; push_client running but no local-edit detection");
            notify_user(
                app,
                "Vault Sync push failed to start",
                &format!("file_watcher init failed: {e}"),
            );
            return None;
        }
    };
    match watcher.start() {
        Ok(handle) => {
            tracing::info!("file_watcher started for subscriber_id={}", subscriber_id);
            Some(handle)
        }
        Err(file_watcher::FileWatcherError::InotifyLimitExceeded { current }) => {
            // S477 §3.5 (v0.3.7): Linux inotify watch-limit exhaustion. Surface
            // a structured Tauri event to the wizard so it can render the
            // sysctl-one-liner banner. Also log + OS-notify so the user sees
            // the failure even if the wizard window is closed.
            tracing::error!(
                "push pipeline: inotify watch limit exceeded (current={current}); raise fs.inotify.max_user_watches"
            );
            if let Err(emit_err) = app.emit("inotify_limit_exceeded", current) {
                tracing::warn!("failed to emit inotify_limit_exceeded event: {emit_err}");
            }
            notify_user(
                app,
                "Vault Sync push failed to start",
                &format!(
                    "Linux inotify watch limit exceeded (current={current}). \
                     Run: sudo sysctl -w fs.inotify.max_user_watches=524288"
                ),
            );
            None
        }
        Err(e) => {
            tracing::error!("push pipeline: file_watcher start failed: {e}; push_client running but no local-edit detection");
            notify_user(
                app,
                "Vault Sync push failed to start",
                &format!("file_watcher start failed: {e}"),
            );
            None
        }
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

/// Periodic 60s check that re-evaluates redflag.md. If the file disappears
/// after a Tripped state, clears the tray flag and logs a recovery line.
/// (For v0.3.0, daemon does NOT auto-restart sync on recovery — owner must
/// restart the daemon. Recovery just unblocks the path back to clean.)
fn spawn_redflag_monitor(gate: redflag::RedflagGate, tray_state: tray_state::SharedTrayState) {
    tauri::async_runtime::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(60));
        loop {
            tick.tick().await;
            redflag_tick(&gate, &tray_state);
        }
    });
}

#[cfg(test)]
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

    /// The push pipeline (spawn_push_pipeline) and verify_repair
    /// (commands::build_verify_repair) MUST resolve the identical journal file.
    /// Both go through `commands::resolve_workspace_root` +
    /// `commands::push_journal_path`. This test pins that both call sites
    /// produce byte-identical paths for the same subscriber_id, so the
    /// verify-repair appends and the drain loop operate on one file.
    #[test]
    fn push_journal_path_identical_from_both_call_sites() {
        // lib.rs spawn_push_pipeline resolution.
        let ws_spawn = commands::resolve_workspace_root();
        let spawn_side = commands::push_journal_path(&ws_spawn, "sub-xyz");

        // verify_repair_run resolution (commands.rs).
        let ws_vr = commands::resolve_workspace_root();
        let vr_side = commands::push_journal_path(&ws_vr, "sub-xyz");

        assert_eq!(
            spawn_side, vr_side,
            "push journal path must match between push pipeline and verify_repair"
        );
    }

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
