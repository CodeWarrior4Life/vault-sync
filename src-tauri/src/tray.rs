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
    let icon = app
        .default_window_icon()
        .cloned()
        .ok_or_else(|| tauri::Error::AssetNotFound("default window icon (used for tray)".into()))?;

    let mut builder = TrayIconBuilder::new().menu(&menu).icon(icon);
    // S471 v0.1.2 fix: on macOS, tell the tray to treat the icon as a
    // template image (auto-inverts for dark/light mode + renders crisp at
    // status-bar height). Without this the colored .icns can render
    // invisible against the menu bar.
    #[cfg(target_os = "macos")]
    {
        builder = builder.icon_as_template(true);
    }
    builder = builder.on_menu_event(|app, event| match event.id.as_ref() {
        "open-vault" => { /* tauri::shell::open vault_root */ }
        "pause" => { /* signal sse consumer to pause */ }
        "resync" => { /* clear last_event_id + reconnect */ }
        "open-admin" => { /* open nexus_url/admin/vault-sync */ }
        "quit" => {
            app.exit(0);
        }
        _ => {}
    });
    builder.build(app)?;
    Ok(())
}
