pub mod api_client;
pub mod config;
pub mod keyring;
pub mod materializer;
pub mod obsidian_install_detect;
pub mod obsidian_plugin_detect;
pub mod pairing;
pub mod scope;
pub mod sse;
pub mod token_store;
pub mod tray;
pub mod tray_state;

use tauri::Manager;
use tauri_plugin_updater::UpdaterExt;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_single_instance::init(|app, _, _| {
            let _ = app
                .get_webview_window("main")
                .map(|w: tauri::WebviewWindow| w.set_focus());
        }))
        .plugin(tauri_plugin_autostart::init(
            tauri_plugin_autostart::MacosLauncher::LaunchAgent,
            Some(vec!["--silent"]),
        ))
        .plugin(tauri_plugin_updater::Builder::new().build())
        .plugin(tauri_plugin_store::Builder::default().build())
        .plugin(tauri_plugin_shell::init())
        .plugin(tauri_plugin_dialog::init())
        .invoke_handler(tauri::generate_handler![
            pairing::pair,
            pairing::load_current_config
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
                let (sub, url, root) = match config::Config::load_from(&cfg_path) {
                    Ok(c) => (c.subscriber_id, c.nexus_url, c.vault_root),
                    Err(_) => (String::new(), String::new(), std::path::PathBuf::new()),
                };
                std::sync::Arc::new(std::sync::RwLock::new(tray_state::TrayState::new(
                    sub, url, root,
                )))
            };

            tray::build_tray(app.handle(), shared_state.clone())?;

            // v0.1.4: silent auto-update check on startup. Uses the pubkey +
            // endpoints declared in tauri.conf.json. Runs detached so a slow
            // /admin/api/vault-sync/releases/<platform>/latest doesn't block
            // SSE startup. Failures log only — never block the daemon.
            spawn_updater_check(app.handle().clone());

            if cfg_path.exists() {
                spawn_sse_consumer(app.handle().clone(), cfg_path, shared_state);
            } else {
                if let Some(w) = app.get_webview_window("main") {
                    let _ = w.show();
                    let _ = w.set_focus();
                }
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
    let _ = app;
    tauri::async_runtime::spawn(async move {
        let cfg = match config::Config::load_from(&cfg_path) {
            Ok(c) => c,
            Err(e) => {
                tracing::error!("config load failed: {e}");
                return;
            }
        };
        let token = match token_store::load(&cfg.subscriber_id) {
            Ok(Some(t)) => t,
            Ok(None) => {
                tracing::error!(
                    "no token in keyring or file fallback for subscriber_id={}; re-pair required",
                    cfg.subscriber_id
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
            // The configured vault may not be in obsidian.json yet (fresh
            // install scenario) — include it explicitly.
            if !vaults_to_scan.iter().any(|p| p == &cfg.vault_root) {
                vaults_to_scan.push(cfg.vault_root.clone());
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
        let materializer = materializer::Materializer::new(
            cfg.vault_root.clone(),
            snap.shadow_path.clone(),
            materializer::MaterializerMode::from_str(&snap.materializer_mode),
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
        if let Err(e) = consumer.run(cfg.last_event_id, shutdown_rx).await {
            tracing::error!("SSE consumer exited with error: {e}");
        }
    });
}
