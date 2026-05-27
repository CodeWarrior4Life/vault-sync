use crate::tray_state::SharedTrayState;
use std::time::Duration;
use tauri::{
    menu::{Menu, MenuBuilder, MenuItem, MenuItemBuilder},
    tray::TrayIconBuilder,
    AppHandle, Manager, Wry,
};
use tauri_plugin_shell::ShellExt;

/// Build the tray icon + menu, wire handlers to actual functionality, and
/// spawn a background task that refreshes the visible status line every 2 s
/// from the SharedTrayState that the SSE consumer writes to.
pub fn build_tray(app: &AppHandle, state: SharedTrayState) -> tauri::Result<()> {
    let status_item = MenuItemBuilder::with_id("status", "Status: starting…")
        .enabled(false)
        .build(app)?;
    let activity_item = MenuItemBuilder::with_id("activity", "0 events received")
        .enabled(false)
        .build(app)?;
    let last_error_item = MenuItemBuilder::with_id("last_error", "")
        .enabled(false)
        .build(app)?;

    let open_vault = MenuItemBuilder::with_id("open-vault", "Open Vault Folder").build(app)?;
    let open_admin = MenuItemBuilder::with_id("open-admin", "Open Admin in Browser").build(app)?;
    let pause = MenuItemBuilder::with_id("pause", "Pause Sync (coming v0.1.4)")
        .enabled(false)
        .build(app)?;
    let resync = MenuItemBuilder::with_id("resync", "Force Resync (coming v0.1.4)")
        .enabled(false)
        .build(app)?;
    let about = MenuItemBuilder::with_id("about", "About…").build(app)?;
    let quit = MenuItemBuilder::with_id("quit", "Quit").build(app)?;

    let menu: Menu<Wry> = MenuBuilder::new(app)
        .items(&[&status_item, &activity_item, &last_error_item])
        .separator()
        .items(&[&open_vault, &open_admin, &pause, &resync])
        .separator()
        .items(&[&about, &quit])
        .build()?;

    // Hold a clone of the state for the handlers + the refresh task.
    let handler_state = state.clone();
    let refresh_state = state.clone();

    let icon = app
        .default_window_icon()
        .cloned()
        .ok_or_else(|| tauri::Error::AssetNotFound("default window icon (used for tray)".into()))?;

    let mut builder = TrayIconBuilder::new()
        .menu(&menu)
        .icon(icon)
        .tooltip("Nexus Vault Sync");
    #[cfg(target_os = "macos")]
    {
        builder = builder.icon_as_template(true);
    }
    builder = builder.on_menu_event(move |app, event| {
        let st = handler_state.clone();
        match event.id.as_ref() {
            "open-vault" => {
                if let Ok(s) = st.read() {
                    let path = s.vault_root.clone();
                    drop(s);
                    // tauri-plugin-shell's open() is deprecated in favor of
                    // tauri-plugin-opener (v0.1.4 swap), but functionally
                    // identical for this use case.
                    #[allow(deprecated)]
                    let _ = app.shell().open(path.to_string_lossy().to_string(), None);
                }
            }
            "open-admin" => {
                if let Ok(s) = st.read() {
                    let url = format!("{}/admin/vault-sync", s.nexus_url.trim_end_matches('/'));
                    drop(s);
                    #[allow(deprecated)]
                    let _ = app.shell().open(url, None);
                }
            }
            "about" => {
                if let Some(w) = app.get_webview_window("main") {
                    let _ = w.show();
                    let _ = w.set_focus();
                }
            }
            "quit" => {
                app.exit(0);
            }
            _ => {}
        }
    });
    builder.build(app)?;

    // Background refresher: poll SharedTrayState every 2s and update the
    // top three (status / activity / last_error) menu items in place.
    // Keeps the hover-menu live without a heavyweight reactive system.
    let app_handle = app.clone();
    tauri::async_runtime::spawn(async move {
        let mut prev_status_line = String::new();
        let mut prev_activity = String::new();
        let mut prev_error = String::new();
        loop {
            tokio::time::sleep(Duration::from_secs(2)).await;
            let (status_line, activity_line, error_line) = match refresh_state.read() {
                Ok(s) => {
                    let activity = if let Some(t) = s.last_event_at {
                        if let Ok(elapsed) = t.elapsed() {
                            format!(
                                "{} events · last {}",
                                s.events_received,
                                format_staleness(elapsed)
                            )
                        } else {
                            format!("{} events", s.events_received)
                        }
                    } else if s.events_received > 0 {
                        format!("{} events", s.events_received)
                    } else {
                        "0 events received".to_string()
                    };
                    let err = s.last_error.clone().unwrap_or_default();
                    (s.status.label().to_string(), activity, err)
                }
                Err(_) => continue,
            };
            // Only call set_text if changed — avoids triggering macOS menu
            // animation refresh churn.
            if status_line != prev_status_line {
                if let Some(item) = app_handle
                    .try_state::<TrayMenuHandles>()
                    .as_deref()
                    .and_then(|h| h.status.as_ref())
                {
                    let _ = item.set_text(format!("Status: {status_line}"));
                }
                prev_status_line = status_line;
            }
            if activity_line != prev_activity {
                if let Some(item) = app_handle
                    .try_state::<TrayMenuHandles>()
                    .as_deref()
                    .and_then(|h| h.activity.as_ref())
                {
                    let _ = item.set_text(&activity_line);
                }
                prev_activity = activity_line;
            }
            if error_line != prev_error {
                if let Some(item) = app_handle
                    .try_state::<TrayMenuHandles>()
                    .as_deref()
                    .and_then(|h| h.last_error.as_ref())
                {
                    if error_line.is_empty() {
                        let _ = item.set_text("");
                    } else {
                        let _ = item.set_text(format!("⚠ {error_line}"));
                    }
                }
                prev_error = error_line;
            }
        }
    });

    // Stash the menu item handles in Tauri's State so the refresher above
    // can update them. Using try_state means the refresher gracefully no-ops
    // if the handles aren't registered (shouldn't happen but safer than panic).
    app.manage(TrayMenuHandles {
        status: Some(status_item),
        activity: Some(activity_item),
        last_error: Some(last_error_item),
    });

    Ok(())
}

/// Tauri State container for the live menu items that the refresher updates.
struct TrayMenuHandles {
    status: Option<MenuItem<Wry>>,
    activity: Option<MenuItem<Wry>>,
    last_error: Option<MenuItem<Wry>>,
}

fn format_staleness(d: Duration) -> String {
    let s = d.as_secs();
    match s {
        0..=59 => format!("{s}s ago"),
        60..=3599 => format!("{}m ago", s / 60),
        3600..=86399 => format!("{}h ago", s / 3600),
        _ => format!("{}d ago", s / 86400),
    }
}
