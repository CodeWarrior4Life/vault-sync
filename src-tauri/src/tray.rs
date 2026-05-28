use crate::commands;
use crate::tray_state::{SharedTrayState, TrayState};
use std::sync::Arc;
use std::time::Duration;
use tauri::{
    image::Image,
    menu::{Menu, MenuBuilder, MenuItem, MenuItemBuilder},
    tray::{TrayIcon, TrayIconBuilder},
    AppHandle, Emitter, Manager, Wry,
};
use tauri_plugin_dialog::{DialogExt, MessageDialogKind};
use tauri_plugin_shell::ShellExt;
use tracing::{info, warn};

/// 32×32 RGBA PNG embedded at compile time so the tray icon doesn't depend
/// on `default_window_icon()` (which resolves to the large .icns and renders
/// as a near-invisible smudge once macOS template-scales it down to status-
/// bar height). Source: src-tauri/icons/32x32.png.
///
/// S477 §3.3 (v0.3.7): macOS gets a TIGHTER 22×22 variant — macOS scales the
/// 32×32 down to fit the menu-bar height (~22px on notched displays), eating
/// nearly half the icon's effective pixels to transparent padding. The 22×22
/// variant renders at native resolution with no scale-down padding loss.
/// Other platforms keep the 32×32 (Windows + Linux render at native size).
#[cfg(target_os = "macos")]
const TRAY_ICON_BYTES: &[u8] = include_bytes!("../icons/tray-macos-tight.png");

#[cfg(not(target_os = "macos"))]
const TRAY_ICON_BYTES: &[u8] = include_bytes!("../icons/32x32.png");
/// v0.3.3: 8-frame "bright band sweeps top -> bottom" animation. The icon
/// is a yellow-to-blue gradient; the bright band moves along the gradient
/// so the result reads as a wave undulating from yellow to blue (and
/// implicitly wraps back to the top via the cyclic frame index). Generated
/// programmatically from 32x32.png by halving alpha proportional to the
/// distance from each frame's band center.
const TRAY_ICON_ANIM_BYTES: [&[u8]; 8] = [
    include_bytes!("../icons/32x32-anim-0.png"),
    include_bytes!("../icons/32x32-anim-1.png"),
    include_bytes!("../icons/32x32-anim-2.png"),
    include_bytes!("../icons/32x32-anim-3.png"),
    include_bytes!("../icons/32x32-anim-4.png"),
    include_bytes!("../icons/32x32-anim-5.png"),
    include_bytes!("../icons/32x32-anim-6.png"),
    include_bytes!("../icons/32x32-anim-7.png"),
];

/// Stable id for the tray icon so handlers (e.g. the verify-repair arm) can
/// look it up via `app.tray_by_id` and flip the tooltip synchronously without
/// waiting for the 2s refresh poller.
const TRAY_ICON_ID: &str = "main-tray";

/// Build the tray icon + menu, wire handlers to actual functionality, and
/// spawn a background task that refreshes the visible status line every 2 s
/// from the SharedTrayState that the SSE consumer writes to.
///
/// v0.3 (mandate §4.1 + §9 AG5+AG13): adds pending-uploads / conflicts /
/// verify-repair / redflag / delete-burst menu items and a live tooltip
/// (build_tooltip) updated within 2 s of every state change.
pub fn build_tray(app: &AppHandle, state: SharedTrayState) -> tauri::Result<()> {
    info!("build_tray: entry");
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
    let settings = MenuItemBuilder::with_id("settings", "Settings…").build(app)?;
    let pause = MenuItemBuilder::with_id("pause", "Pause Sync (coming v0.1.8)")
        .enabled(false)
        .build(app)?;
    let resync = MenuItemBuilder::with_id("resync", "Force Resync (coming v0.1.8)")
        .enabled(false)
        .build(app)?;

    // v0.3 — telemetry / safety menu items.
    let pending_item = MenuItemBuilder::with_id("pending-uploads", "Pending uploads: 0")
        .enabled(false)
        .build(app)?;
    let conflicts_item =
        MenuItemBuilder::with_id("conflicts", "Conflicts unresolved: 0").build(app)?;
    let verify_repair_item =
        MenuItemBuilder::with_id("verify-repair", "Verify and repair all files…").build(app)?;
    // Conditional safety-valve items — present in the menu but blank-text
    // when the underlying state is clear. (Tauri's menu doesn't support
    // clean post-build append/remove, so we render presence via the label.)
    let redflag_item = MenuItemBuilder::with_id("redflag-status", "")
        .enabled(false)
        .build(app)?;
    let delete_burst_item = MenuItemBuilder::with_id("delete-burst-status", "")
        .enabled(false)
        .build(app)?;

    let about = MenuItemBuilder::with_id("about", "About…").build(app)?;
    let quit = MenuItemBuilder::with_id("quit", "Quit").build(app)?;

    let menu: Menu<Wry> = MenuBuilder::new(app)
        .items(&[&status_item, &activity_item, &last_error_item])
        .separator()
        .items(&[&pending_item, &conflicts_item, &verify_repair_item])
        .separator()
        .items(&[&redflag_item, &delete_burst_item])
        .separator()
        .items(&[&open_vault, &open_admin, &settings, &pause, &resync])
        .separator()
        .items(&[&about, &quit])
        .build()?;

    // Hold a clone of the state for the handlers + the refresh task.
    let handler_state = state.clone();
    let refresh_state = state.clone();

    let icon = match Image::from_bytes(TRAY_ICON_BYTES) {
        Ok(i) => {
            info!(
                "build_tray: tray icon loaded from embedded PNG ({} bytes)",
                TRAY_ICON_BYTES.len()
            );
            i
        }
        Err(e) => {
            warn!(
                "build_tray: embedded PNG load failed ({e}); falling back to default_window_icon"
            );
            app.default_window_icon().cloned().ok_or_else(|| {
                tauri::Error::AssetNotFound("default window icon (tray fallback)".into())
            })?
        }
    };

    // v0.1.5: dropping icon_as_template(true) — the dedicated 32×32 colored
    // PNG renders as a small color icon in the menu bar (visible) instead of
    // the previous template-scaled-down-from-.icns which Cyril reported as
    // entirely invisible on macOS 26.4.
    let mut builder = TrayIconBuilder::with_id(TRAY_ICON_ID)
        .menu(&menu)
        .icon(icon)
        .tooltip("Nexus Vault Sync");
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
            "settings" => {
                // v0.1.7: re-open the pair-wizard window for re-config
                // (change vault path / token / nexus URL). Same window,
                // just shown on demand instead of only-on-first-run.
                if let Some(w) = app.get_webview_window("main") {
                    let _ = w.show();
                    let _ = w.set_focus();
                }
            }
            "about" => {
                if let Some(w) = app.get_webview_window("main") {
                    let _ = w.show();
                    let _ = w.set_focus();
                }
            }
            "conflicts" => {
                // v0.3.0 Wave 4: native list dialog showing first 20
                // unresolved `*.conflict-from-*.md` siblings + a "open vault
                // to resolve manually" hint. Full interactive resolver UI
                // is v0.3.1+; the paired `list_conflicts` Tauri command
                // already returns the structured data for that next step.
                tracing::info!("tray: conflicts clicked — invoking list_conflicts");
                let app_handle = app.clone();
                tauri::async_runtime::spawn(async move {
                    match commands::list_conflicts().await {
                        Ok(entries) => {
                            let count = entries.len();
                            let sample: Vec<String> = entries
                                .iter()
                                .take(20)
                                .map(|e| {
                                    format!(
                                        "• {} (from {}, lsn {})",
                                        e.original_path, e.from_device, e.from_lsn
                                    )
                                })
                                .collect();
                            let body = if count == 0 {
                                "No unresolved conflicts.".to_string()
                            } else {
                                format!(
                                    "{} unresolved conflict(s){}:\n\n{}\n\n{}",
                                    count,
                                    if count > 20 { " (showing first 20)" } else { "" },
                                    sample.join("\n"),
                                    "Open the vault to review and merge manually. v0.3.1 will add an in-app resolver."
                                )
                            };
                            app_handle
                                .dialog()
                                .message(body)
                                .title("Conflicts unresolved")
                                .show(|_| {});
                        }
                        Err(e) => {
                            tracing::error!("list_conflicts failed: {e}");
                            app_handle
                                .dialog()
                                .message(format!("Could not list conflicts: {e}"))
                                .title("Conflicts unresolved")
                                .kind(MessageDialogKind::Error)
                                .show(|_| {});
                        }
                    }
                });
            }
            "verify-repair" => {
                // v0.3 alpha3: show the webview window IMMEDIATELY with a
                // "Verifying…" view (Cyril: *"I want the dialogue to pop up
                // immediately and then give me a waiting"*). A native modal
                // dialog can't do this — it blocks + can't update in place.
                // So we drive a webview panel via events: emit verify-progress
                // NOW (frontend shows spinner instantly), run the sweep async,
                // then emit verify-result / verify-error to fill the panel.
                tracing::info!("tray: verify-repair clicked — opening progress window");

                if let Some(w) = app.get_webview_window("main") {
                    let _ = w.show();
                    let _ = w.set_focus();
                }
                let _ = app.emit("verify-progress", ());

                if let Ok(mut s) = st.write() {
                    s.set_verify_in_progress(true);
                }
                if let Some(tray) = app.tray_by_id(TRAY_ICON_ID) {
                    let _ = tray.set_tooltip(Some("⟳ Verifying vault… scanning files"));
                }

                let app_handle = app.clone();
                let st_done = st.clone();
                tauri::async_runtime::spawn(async move {
                    let result = commands::verify_repair_run().await;
                    if let Ok(mut s) = st_done.write() {
                        s.set_verify_in_progress(false);
                    }
                    match result {
                        Ok(report) => {
                            tracing::info!(?report, "verify_repair completed");
                            let _ = app_handle.emit("verify-result", &report);
                        }
                        Err(e) => {
                            tracing::error!("verify_repair failed: {e}");
                            let _ = app_handle.emit("verify-error", e.to_string());
                        }
                    }
                });
            }
            "redflag-status" => {
                // Open vault folder so user can locate + delete redflag.md.
                if let Ok(s) = st.read() {
                    let path = s.vault_root.clone();
                    drop(s);
                    #[allow(deprecated)]
                    let _ = app.shell().open(path.to_string_lossy().to_string(), None);
                }
            }
            "quit" => {
                app.exit(0);
            }
            _ => {}
        }
    });
    let tray = builder.build(app)?;
    info!("build_tray: TrayIcon registered + menu attached");

    // Wrap the TrayIcon so the refresher task can set_tooltip on it.
    let tray_arc: Arc<TrayIcon<Wry>> = Arc::new(tray);
    let tray_for_refresh = tray_arc.clone();
    let tray_for_anim = tray_arc.clone();
    let anim_state = state.clone();

    // Background refresher: poll SharedTrayState every 2s and update menu
    // items + tray tooltip. Mandate §9 AG13 requires the surface updates
    // within 2 seconds — this matches the cadence.
    let app_handle = app.clone();
    tauri::async_runtime::spawn(async move {
        let mut prev_status_line = String::new();
        let mut prev_activity = String::new();
        let mut prev_error = String::new();
        let mut prev_pending = String::new();
        let mut prev_conflicts = String::new();
        let mut prev_redflag = String::new();
        let mut prev_burst = String::new();
        let mut prev_tooltip = String::new();
        loop {
            tokio::time::sleep(Duration::from_secs(2)).await;
            let snapshot = match refresh_state.read() {
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
                    let pending = format!("Pending uploads: {}", s.uploads_pending);
                    let conflicts = format!("Conflicts unresolved: {}", s.conflict_unresolved);
                    let redflag = if s.redflag_tripped {
                        "redflag.md PRESENT — sync HALTED".to_string()
                    } else {
                        String::new()
                    };
                    let burst = if s.delete_burst_paused {
                        "Delete-burst paused — review".to_string()
                    } else {
                        String::new()
                    };
                    let tooltip = build_tooltip(&s);
                    Some((
                        s.status.label().to_string(),
                        activity,
                        err,
                        pending,
                        conflicts,
                        redflag,
                        burst,
                        tooltip,
                    ))
                }
                Err(_) => None,
            };
            let Some((
                status_line,
                activity_line,
                error_line,
                pending_line,
                conflicts_line,
                redflag_line,
                burst_line,
                tooltip,
            )) = snapshot
            else {
                continue;
            };

            let handles = app_handle.try_state::<TrayMenuHandles>();
            // Only call set_text if changed — avoids macOS menu refresh churn.
            if status_line != prev_status_line {
                if let Some(item) = handles.as_deref().and_then(|h| h.status.as_ref()) {
                    let _ = item.set_text(format!("Status: {status_line}"));
                }
                prev_status_line = status_line;
            }
            if activity_line != prev_activity {
                if let Some(item) = handles.as_deref().and_then(|h| h.activity.as_ref()) {
                    let _ = item.set_text(&activity_line);
                }
                prev_activity = activity_line;
            }
            if error_line != prev_error {
                if let Some(item) = handles.as_deref().and_then(|h| h.last_error.as_ref()) {
                    if error_line.is_empty() {
                        let _ = item.set_text("");
                    } else {
                        let _ = item.set_text(format!("⚠ {error_line}"));
                    }
                }
                prev_error = error_line;
            }
            if pending_line != prev_pending {
                if let Some(item) = handles.as_deref().and_then(|h| h.pending.as_ref()) {
                    let _ = item.set_text(&pending_line);
                }
                prev_pending = pending_line;
            }
            if conflicts_line != prev_conflicts {
                if let Some(item) = handles.as_deref().and_then(|h| h.conflicts.as_ref()) {
                    let _ = item.set_text(&conflicts_line);
                }
                prev_conflicts = conflicts_line;
            }
            if redflag_line != prev_redflag {
                if let Some(item) = handles.as_deref().and_then(|h| h.redflag.as_ref()) {
                    let _ = item.set_text(&redflag_line);
                }
                prev_redflag = redflag_line;
            }
            if burst_line != prev_burst {
                if let Some(item) = handles.as_deref().and_then(|h| h.delete_burst.as_ref()) {
                    let _ = item.set_text(&burst_line);
                }
                prev_burst = burst_line;
            }
            if tooltip != prev_tooltip {
                let _ = tray_for_refresh.set_tooltip(Some(&tooltip));
                prev_tooltip = tooltip;
            }
        }
    });

    // v0.3.5: gentle icon undulation while the daemon is "busy" -- this
    // covers ANY active sync work, not just verify/connection states.
    // Triggers:
    //   - verify_in_progress (owner-invoked Verify & Repair sweep)
    //   - connection states Starting / Connecting / Reconnecting
    //   - uploads_pending > 0 (push direction has queued work)
    //   - last_event_at within the last 3 seconds (pull direction just
    //     received a fanout event; animation reads as "syncing in")
    // Cycles 8 frames at 150ms so the bright band sweeps top -> bottom
    // along the icon's yellow -> blue gradient and wraps. Cyril S476:
    //     "the animation should start/stop whenever sync'ing is
    //      occuring too"
    tauri::async_runtime::spawn(async move {
        use crate::tray_state::ConnectionStatus;
        let normal_icon = match Image::from_bytes(TRAY_ICON_BYTES) {
            Ok(i) => i,
            Err(e) => {
                warn!("tray anim: failed to load normal icon ({e}); animation disabled");
                return;
            }
        };
        let frames: Vec<Image> = TRAY_ICON_ANIM_BYTES
            .iter()
            .filter_map(|b| Image::from_bytes(b).ok())
            .collect();
        if frames.len() != TRAY_ICON_ANIM_BYTES.len() {
            warn!(
                "tray anim: only loaded {}/{} frames; animation will be choppy",
                frames.len(),
                TRAY_ICON_ANIM_BYTES.len()
            );
        }
        if frames.is_empty() {
            warn!("tray anim: no frames loaded; animation disabled");
            return;
        }
        const RECENT_EVENT_WINDOW: std::time::Duration = std::time::Duration::from_secs(3);
        let mut frame_idx: usize = 0;
        let mut was_animating = false;
        loop {
            tokio::time::sleep(Duration::from_millis(150)).await;
            let animate = match anim_state.read() {
                Ok(s) => {
                    let connecting = matches!(
                        s.status,
                        ConnectionStatus::Starting
                            | ConnectionStatus::Connecting
                            | ConnectionStatus::Reconnecting
                    );
                    let pushing = s.uploads_pending > 0;
                    let pulling = s
                        .last_event_at
                        .and_then(|t| t.elapsed().ok())
                        .map(|e| e < RECENT_EVENT_WINDOW)
                        .unwrap_or(false);
                    s.verify_in_progress || connecting || pushing || pulling
                }
                Err(_) => false,
            };
            if animate {
                let next = frames[frame_idx % frames.len()].clone();
                let _ = tray_for_anim.set_icon(Some(next));
                frame_idx = frame_idx.wrapping_add(1);
                was_animating = true;
            } else if was_animating {
                // Settled -- restore the full-strength icon exactly once.
                let _ = tray_for_anim.set_icon(Some(normal_icon.clone()));
                frame_idx = 0;
                was_animating = false;
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
        pending: Some(pending_item),
        conflicts: Some(conflicts_item),
        redflag: Some(redflag_item),
        delete_burst: Some(delete_burst_item),
    });
    // Hold the verify_repair item alive (the menu already retains it but
    // we keep this binding to make the intent explicit — Tauri's MenuItem
    // is reference-counted internally).
    let _ = verify_repair_item;

    // v0.3.1 wave 3-J: `verify-repair` is now wired to commands::verify_repair_run.
    // TODO(v0.3.1): conflict sweep modal — present unresolved
    // *.conflict-from-*.md and let the owner pick + resolve.

    Ok(())
}

/// Tauri State container for the live menu items that the refresher updates.
struct TrayMenuHandles {
    status: Option<MenuItem<Wry>>,
    activity: Option<MenuItem<Wry>>,
    last_error: Option<MenuItem<Wry>>,
    pending: Option<MenuItem<Wry>>,
    conflicts: Option<MenuItem<Wry>>,
    redflag: Option<MenuItem<Wry>>,
    delete_burst: Option<MenuItem<Wry>>,
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

/// Build the single-line tray tooltip from the current TrayState.
///
/// Priority order (highest wins): Redflag, then Disconnected, then Issue,
/// then Uploading, then Synced. Mandate §9 AG5 — owner can hover the tray
/// icon and instantly see whether sync is healthy, busy, or stuck.
///
/// Cyril S471 wanted hover-tooltip showing live sync status: whether notes
/// are uploading, downloading, or sync has been achieved.
pub fn build_tooltip(state: &TrayState) -> String {
    use crate::tray_state::ConnectionStatus;

    // (1) Redflag dominates everything.
    if state.redflag_tripped {
        return "🛑 redflag.md present — sync HALTED".to_string();
    }

    // (1.5) Verify-and-repair in progress — owner-invoked sweep is walking +
    //       hashing the whole vault. Sits just below redflag so the owner gets
    //       instant "we're working on it" feedback the moment they click the
    //       menu item, instead of a stale tooltip for the ~16s the scan takes.
    if state.verify_in_progress {
        return "⟳ Verifying vault… scanning files".to_string();
    }

    // (2) Disconnected — only hard SSE failure states. Connecting /
    //     Reconnecting / Starting are intermediate and fall through to the
    //     Synced/Uploading branches with a stale `last` timestamp instead.
    if matches!(
        state.status,
        ConnectionStatus::AuthFailed | ConnectionStatus::Error
    ) {
        let err = state.last_error.as_deref().unwrap_or("unknown");
        return format!("❌ Disconnected • {err}");
    }

    // (3) Issue — integrity failures / unresolved conflicts / delete-burst pause.
    if state.integrity_failures > 0 || state.conflict_unresolved > 0 || state.delete_burst_paused {
        let burst = if state.delete_burst_paused {
            "paused"
        } else {
            ""
        };
        return format!(
            "⚠ {} integrity / {} conflicts / {}",
            state.integrity_failures, state.conflict_unresolved, burst
        );
    }

    // (4) Uploading — pending push events queued.
    let last_err = state.last_error.as_deref().unwrap_or("none");
    if state.uploads_pending > 0 {
        return format!(
            "Syncing ⟳ • {} pending • last error: {}",
            state.uploads_pending, last_err
        );
    }

    // (5) Synced (healthy default).
    let last = state
        .last_event_at
        .and_then(|t| t.elapsed().ok())
        .map(format_staleness)
        .unwrap_or_else(|| "never".to_string());
    format!(
        "Synced ✓ • {} ↓ {} ↑ last {}",
        state.events_received, state.uploads_sent, last
    )
}

// ---------------------------------------------------------------------------
// Tests — `build_tooltip` is the pure-function surface. The Tauri runtime
// paths (set_tooltip / menu set_text) only compile-check; integration uses
// a live App and lives in Wave 4 manual QA.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tray_state::{ConnectionStatus, TrayState};
    use std::path::PathBuf;
    use std::time::SystemTime;

    fn fresh() -> TrayState {
        let mut s = TrayState::new(
            "sub".into(),
            "https://example".into(),
            PathBuf::from("/vault"),
        );
        s.status = ConnectionStatus::Connected;
        s
    }

    #[test]
    fn tooltip_synced_branch() {
        let mut s = fresh();
        s.events_received = 42;
        s.uploads_sent = 7;
        s.last_event_at = Some(SystemTime::now());
        let t = build_tooltip(&s);
        assert!(t.starts_with("Synced ✓"), "got: {t}");
        assert!(t.contains("42 ↓"), "got: {t}");
        assert!(t.contains("7 ↑"), "got: {t}");
        assert!(t.contains("last "), "got: {t}");
    }

    #[test]
    fn tooltip_synced_with_no_events_yet() {
        let s = fresh();
        let t = build_tooltip(&s);
        assert!(t.starts_with("Synced ✓"), "got: {t}");
        assert!(t.contains("never"), "got: {t}");
    }

    #[test]
    fn tooltip_uploading_branch() {
        let mut s = fresh();
        s.uploads_pending = 5;
        let t = build_tooltip(&s);
        assert!(t.starts_with("Syncing ⟳"), "got: {t}");
        assert!(t.contains("5 pending"), "got: {t}");
        assert!(t.contains("last error: none"), "got: {t}");
    }

    #[test]
    fn tooltip_uploading_includes_last_error() {
        let mut s = fresh();
        s.uploads_pending = 2;
        s.last_error = Some("503 backend".into());
        let t = build_tooltip(&s);
        assert!(t.contains("last error: 503 backend"), "got: {t}");
    }

    #[test]
    fn tooltip_issue_branch_with_integrity() {
        let mut s = fresh();
        s.integrity_failures = 3;
        let t = build_tooltip(&s);
        assert!(t.starts_with("⚠"), "got: {t}");
        assert!(t.contains("3 integrity"), "got: {t}");
        assert!(t.contains("0 conflicts"), "got: {t}");
    }

    #[test]
    fn tooltip_issue_branch_with_conflicts() {
        let mut s = fresh();
        s.conflict_unresolved = 4;
        let t = build_tooltip(&s);
        assert!(t.contains("4 conflicts"), "got: {t}");
    }

    #[test]
    fn tooltip_issue_branch_with_delete_burst_paused() {
        let mut s = fresh();
        s.delete_burst_paused = true;
        let t = build_tooltip(&s);
        assert!(t.starts_with("⚠"), "got: {t}");
        assert!(t.contains("paused"), "got: {t}");
    }

    #[test]
    fn tooltip_redflag_overrides_all() {
        let mut s = fresh();
        // Set every other signal — redflag must still win.
        s.redflag_tripped = true;
        s.integrity_failures = 99;
        s.conflict_unresolved = 99;
        s.uploads_pending = 99;
        s.delete_burst_paused = true;
        s.status = ConnectionStatus::AuthFailed;
        s.last_error = Some("ignored".into());
        let t = build_tooltip(&s);
        assert!(t.contains("redflag.md"), "got: {t}");
        assert!(t.contains("HALTED"), "got: {t}");
        assert!(!t.contains("Disconnected"), "got: {t}");
        assert!(!t.contains("Syncing"), "got: {t}");
    }

    #[test]
    fn tooltip_verify_in_progress_branch() {
        let mut s = fresh();
        s.verify_in_progress = true;
        let t = build_tooltip(&s);
        assert!(t.starts_with("⟳ Verifying vault"), "got: {t}");
        assert!(t.contains("scanning files"), "got: {t}");
    }

    #[test]
    fn tooltip_redflag_beats_verify_in_progress() {
        let mut s = fresh();
        s.redflag_tripped = true;
        s.verify_in_progress = true;
        let t = build_tooltip(&s);
        assert!(t.contains("redflag.md"), "got: {t}");
        assert!(!t.contains("Verifying"), "got: {t}");
    }

    #[test]
    fn tooltip_verify_in_progress_beats_uploading_and_issues() {
        let mut s = fresh();
        s.verify_in_progress = true;
        s.uploads_pending = 9;
        s.integrity_failures = 3;
        s.conflict_unresolved = 2;
        let t = build_tooltip(&s);
        assert!(t.starts_with("⟳ Verifying vault"), "got: {t}");
        assert!(!t.contains("Syncing"), "got: {t}");
        assert!(!t.starts_with("⚠"), "got: {t}");
    }

    #[test]
    fn tooltip_disconnected_branch() {
        let mut s = fresh();
        s.status = ConnectionStatus::AuthFailed;
        s.last_error = Some("token revoked".into());
        let t = build_tooltip(&s);
        assert!(t.starts_with("❌ Disconnected"), "got: {t}");
        assert!(t.contains("token revoked"), "got: {t}");
    }

    #[test]
    fn tooltip_priority_redflag_beats_disconnected() {
        let mut s = fresh();
        s.redflag_tripped = true;
        s.status = ConnectionStatus::AuthFailed;
        let t = build_tooltip(&s);
        assert!(t.contains("redflag.md"));
        assert!(!t.contains("Disconnected"));
    }

    #[test]
    fn tooltip_priority_disconnected_beats_uploading() {
        let mut s = fresh();
        s.status = ConnectionStatus::AuthFailed;
        s.last_error = Some("403".into());
        s.uploads_pending = 5;
        let t = build_tooltip(&s);
        assert!(t.starts_with("❌ Disconnected"), "got: {t}");
        assert!(!t.contains("Syncing"));
    }

    #[test]
    fn tooltip_priority_issue_beats_uploading() {
        let mut s = fresh();
        s.uploads_pending = 5;
        s.integrity_failures = 1;
        let t = build_tooltip(&s);
        assert!(t.starts_with("⚠"), "got: {t}");
        assert!(!t.contains("Syncing"), "got: {t}");
    }

    #[test]
    fn last_sync_relative_seconds_formatted() {
        let mut s = fresh();
        s.events_received = 1;
        s.last_event_at = Some(SystemTime::now() - Duration::from_secs(5));
        let t = build_tooltip(&s);
        assert!(t.contains("last "), "got: {t}");
        // 5 seconds elapsed → "5s ago" via format_staleness.
        assert!(t.contains("s ago") || t.contains("m ago"), "got: {t}");
    }
}
