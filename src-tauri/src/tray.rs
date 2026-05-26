use tauri::{
    menu::{MenuBuilder, MenuItemBuilder},
    tray::TrayIconBuilder,
    AppHandle,
};

pub fn build_tray(app: &AppHandle) -> tauri::Result<()> {
    let status = MenuItemBuilder::with_id("status", "Status: connecting...")
        .enabled(false)
        .build(app)?;
    let open_vault = MenuItemBuilder::with_id("open-vault", "Open Vault").build(app)?;
    let pause = MenuItemBuilder::with_id("pause", "Pause Sync").build(app)?;
    let resync = MenuItemBuilder::with_id("resync", "Force Resync").build(app)?;
    let open_admin = MenuItemBuilder::with_id("open-admin", "Open Admin in Browser").build(app)?;
    let about = MenuItemBuilder::with_id("about", "About...").build(app)?;
    let quit = MenuItemBuilder::with_id("quit", "Quit").build(app)?;
    let menu = MenuBuilder::new(app)
        .items(&[
            &status,
            &open_vault,
            &pause,
            &resync,
            &open_admin,
            &about,
            &quit,
        ])
        .build()?;
    TrayIconBuilder::new()
        .menu(&menu)
        .icon(app.default_window_icon().unwrap().clone())
        .on_menu_event(|app, event| match event.id.as_ref() {
            "open-vault" => { /* tauri::shell::open vault_root */ }
            "pause" => { /* signal sse consumer to pause */ }
            "resync" => { /* clear last_event_id + reconnect */ }
            "open-admin" => { /* open nexus_url/admin/vault-sync */ }
            "quit" => {
                app.exit(0);
            }
            _ => {}
        })
        .build(app)?;
    Ok(())
}
