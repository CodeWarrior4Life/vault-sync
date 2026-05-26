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
            tray::build_tray(app.handle())?;
            // If config exists → start SSE consumer; else → show pairing window
            if config::default_config_path().exists() {
                // TODO T29: spawn sse consumer task
            } else {
                // show wizard window (already configured in tauri.conf.json)
            }
            Ok(())
        })
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
