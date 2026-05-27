pub mod api_client;
pub mod config;
pub mod keyring;
pub mod materializer;
pub mod pairing;
pub mod scope;
pub mod sse;
pub mod tray;

use tauri::Manager;

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
        .invoke_handler(tauri::generate_handler![pairing::pair])
        .setup(|app| {
            // S471 v0.1.2 fix: on macOS, set Accessory BEFORE registering the
            // tray icon. Reversed order (build_tray then Accessory) orphans
            // the status item — tray slot stays present but renders nothing.
            #[cfg(target_os = "macos")]
            {
                use tauri::ActivationPolicy;
                let _ = app.set_activation_policy(ActivationPolicy::Accessory);
            }

            tray::build_tray(app.handle())?;

            let cfg_path = config::default_config_path();
            if cfg_path.exists() {
                // S471 fix: actually spawn the SSE consumer when paired.
                // v0.1.0 left this as a TODO; daemon launched but never synced.
                spawn_sse_consumer(app.handle().clone(), cfg_path);
            } else {
                // S471 fix: actually SHOW the pair-wizard window on first run.
                // The window is created `visible: false` in tauri.conf.json so
                // without an explicit show() the user sees nothing.
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

/// Boot the SSE consumer from a saved config + keyring token. Runs on the
/// Tauri async runtime so it doesn't block app startup.
fn spawn_sse_consumer(app: tauri::AppHandle, cfg_path: std::path::PathBuf) {
    let _ = app; // reserved for future tray-status signaling
    tauri::async_runtime::spawn(async move {
        let cfg = match config::Config::load_from(&cfg_path) {
            Ok(c) => c,
            Err(e) => {
                tracing::error!("config load failed: {e}");
                return;
            }
        };
        let token = match keyring::get_token(&cfg.subscriber_id) {
            Ok(Some(t)) => t,
            Ok(None) => {
                tracing::error!(
                    "no keyring token for subscriber_id={}; re-pair required",
                    cfg.subscriber_id
                );
                return;
            }
            Err(e) => {
                tracing::error!("keyring read failed: {e}");
                return;
            }
        };
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
            Ok(c) => c,
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
