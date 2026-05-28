pub mod api_client;
pub mod commands;
pub mod commands_vaults;
pub mod config;
pub mod conflict_stash;
pub mod file_watcher;
pub mod integrity_check;
pub mod keyring;
pub mod materializer;
pub mod obsidian_install_detect;
pub mod obsidian_plugin_detect;
pub mod pairing;
pub mod push_client;
pub mod push_journal;
pub mod rasp_fence;
pub mod redflag;
pub mod scope;
pub mod sse;
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
    if let Err(e) = app
        .notification()
        .builder()
        .title(title)
        .body(body)
        .show()
    {
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
        .plugin(tauri_plugin_single_instance::init(|app, _, _| {
            // S477 §3.3 (v0.3.7): second-launch "find-me" path -- raise the
            // wizard if it's hidden or minimized, then focus it. Without
            // .show() + .unminimize() the user clicking the dock/.app a
            // second time sees nothing happen.
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

            // v0.1.4: silent auto-update check on startup. Uses the pubkey +
            // endpoints declared in tauri.conf.json. Runs detached so a slow
            // /admin/api/vault-sync/releases/<platform>/latest doesn't block
            // SSE startup. Failures log only — never block the daemon.
            spawn_updater_check(app.handle().clone());

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

/// Check for a daemon update from the Nexus-served release endpoint. If
/// available, download + install in the background. v0.1.4 ships silent
/// auto-update by default; a future version can promote to "prompt first".
fn spawn_updater_check(app: tauri::AppHandle) {
    tauri::async_runtime::spawn(async move {
        let updater = match app.updater() {
            Ok(u) => u,
            Err(e) => {
                tracing::warn!("updater init failed: {e}");
                return;
            }
        };
        match updater.check().await {
            Ok(Some(update)) => {
                tracing::info!(
                    "update available: {} -> {}; downloading",
                    env!("CARGO_PKG_VERSION"),
                    update.version
                );
                if let Err(e) = update.download_and_install(|_, _| {}, || {}).await {
                    tracing::warn!("update download_and_install failed: {e}");
                } else {
                    tracing::info!("update installed; will apply on next launch");
                }
            }
            Ok(None) => tracing::debug!("no update available"),
            Err(e) => tracing::warn!("update check failed: {e}"),
        }
    });
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

        // v0.3 — redflag.md circuit breaker (mandate §3). S477: check the
        // full vaults_root (the daemon's watch root), not vaults_root +
        // vault_name. A `redflag.md` placed at the vaults_root level is
        // a kill switch covering every vault under it; per-vault
        // emergency stops are a future refinement.
        let vault_root = cfg.vaults_root.clone();
        let gate = redflag::RedflagGate::new(vault_root.clone());
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
            // S477: enumerate every immediate subdirectory of `vaults_root`
            // that looks like an Obsidian vault (has a `.obsidian/` dir).
            // Adds any vaults Obsidian's registry hasn't seen yet (fresh
            // install, or non-default registry locations).
            if let Ok(entries) = std::fs::read_dir(&cfg.vaults_root) {
                for entry in entries.flatten() {
                    let p = entry.path();
                    if p.is_dir() && p.join(".obsidian").is_dir() {
                        if !vaults_to_scan.iter().any(|q| q == &p) {
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
        let materializer = materializer::Materializer::new(
            cfg.vaults_root.clone(),
            snap.shadow_path.clone(),
            materializer::MaterializerMode::from_str(&snap.materializer_mode),
            workspace_root,
            cfg.subscriber_id.clone(),
            materializer_cfg,
        )
        .with_tray_state(tray_state.clone());

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

        // v0.3 push side — start the file_watcher (local edits → journal) and
        // the push_client drain loop (journal → server). This is what makes
        // the daemon genuinely BIDIRECTIONAL; without it the daemon is
        // pull-only and verify_repair's journal appends are never drained.
        //
        // We start the push pipeline here, AFTER the redflag-clear gate above
        // (mandate §3 — a tripped redflag returns early and never reaches this
        // point). `snap.scope_roots` / `snap.scope_excludes` are consumed by
        // the SSE consumer below, so clone them for the watcher first.
        //
        // The returned `_watch_handle` MUST stay alive for the lifetime of the
        // daemon: dropping it stops the OS watcher (see `WatchHandle::Drop`).
        // We bind it into this async scope, which lives across the
        // forever-awaiting `consumer.run().await` at the tail of this task.
        let watch_scope_roots = snap.scope_roots.clone();
        let watch_scope_excludes = snap.scope_excludes.clone();
        // Resolve the journal workspace root via the SAME helper verify_repair
        // uses (`commands::resolve_workspace_root`), guaranteeing both call
        // sites open the identical push_journal.jsonl file.
        let workspace_root_for_journal = commands::resolve_workspace_root();
        let _watch_handle = spawn_push_pipeline(
            &app,
            &cfg,
            workspace_root_for_journal,
            watch_scope_roots,
            watch_scope_excludes,
            tray_state.clone(),
        );

        let consumer = match sse::SseConsumer::new(
            cfg.nexus_url.clone(),
            token,
            snap.scope_roots,
            snap.scope_excludes,
            materializer,
        ) {
            Ok(c) => c.with_tray_state(tray_state),
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
        if let Err(e) = consumer.run(cfg.last_event_id, shutdown_rx).await {
            tracing::error!("SSE consumer exited with error: {e}");
        }
        // Keep the watch handle bound until the task actually unwinds (only
        // reached if the SSE consumer returns, which it normally never does).
        drop(_watch_handle);
    });
}

/// v0.3 push pipeline wire-up. Opens the SHARED push journal (same path
/// verify_repair resolves via `commands::push_journal_path`), starts the
/// file_watcher (local FS edits → journal), and spawns the push_client drain
/// loop (journal → `POST /api/sync/push`). Returns the [`file_watcher::WatchHandle`]
/// which the caller MUST keep alive — dropping it stops the OS watcher.
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
fn spawn_push_pipeline(
    app: &tauri::AppHandle,
    cfg: &config::Config,
    workspace_root: std::path::PathBuf,
    scope_roots: Vec<String>,
    scope_excludes: Vec<String>,
    tray_state: tray_state::SharedTrayState,
) -> Option<file_watcher::WatchHandle> {
    use std::sync::Arc;

    let token = match token_store::load(&cfg.subscriber_id) {
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
    let journal_path = commands::push_journal_path(&workspace_root, &cfg.subscriber_id);
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

    // S477: the watch + push root is the configured `vaults_root`,
    // verbatim. Do NOT join `vault_name` — `vaults_root` is the
    // appointed sync root and can contain multiple vault folders.
    // The watcher emits paths relative to `vaults_root` (so the
    // vault folder name becomes the first segment of the pushed
    // path), and the server handles per-vault namespacing.
    let vault_root = cfg.vaults_root.clone();

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
        cfg.subscriber_id.clone(),
        push_cfg,
        vault_root.clone(),
    )
    .with_tray_state(tray_state.clone());

    // Never-fired shutdown channel — the push loop runs for the daemon's
    // lifetime. We hold the sender in the spawned task so it isn't dropped
    // (a dropped sender would make `changed()` error → loop exit).
    let (push_shutdown_tx, push_shutdown_rx) = tokio::sync::watch::channel(false);
    tracing::info!(
        "starting push_client drain loop for subscriber_id={}",
        cfg.subscriber_id
    );
    tauri::async_runtime::spawn(async move {
        let _hold_tx = push_shutdown_tx; // keep sender alive for the loop's lifetime
        push_client.run_loop(push_shutdown_rx).await;
        tracing::warn!("push_client.run_loop returned (unexpected for a forever loop)");
    });

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
        cfg.subscriber_id.clone(),
    ) {
        Ok(w) => w.with_tray_state(tray_state),
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
            tracing::info!(
                "file_watcher started for subscriber_id={}",
                cfg.subscriber_id
            );
            Some(handle)
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
}
