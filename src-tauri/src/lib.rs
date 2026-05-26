pub mod api_client;
pub mod config;
pub mod keyring;
pub mod materializer;
pub mod pairing;
pub mod scope;
pub mod sse;
pub mod tray;

// Learn more about Tauri commands at https://tauri.app/develop/calling-rust/
#[tauri::command]
fn greet(name: &str) -> String {
    format!("Hello, {}! You've been greeted from Rust!", name)
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .invoke_handler(tauri::generate_handler![greet, pairing::pair])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
