// Prevents additional console window on Windows in release, DO NOT REMOVE!!
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use std::path::PathBuf;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// v0.3.4: daily-rolling file appender at
/// `<data_local_dir>/Nexus/logs/daemon.log.YYYY-MM-DD`. Without this the
/// daemon ran on Windows GUI subsystem (no console), stderr went to the
/// void, and every silent error (envelope-parse rejections, materializer
/// write failures, etc.) was invisible. S476 root-cause hunt for the
/// "shadow materializer doesn't write" bug burned hours guessing because
/// these errors weren't observable.
fn main() {
    let log_dir: PathBuf = dirs::data_local_dir()
        .unwrap_or_else(|| dirs::home_dir().unwrap_or_else(std::env::temp_dir))
        .join("Nexus")
        .join("logs");
    let _ = std::fs::create_dir_all(&log_dir);

    let file_appender = tracing_appender::rolling::daily(&log_dir, "daemon.log");
    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);
    // Leak the guard — non-blocking writer needs it alive for the whole
    // process lifetime, and main() has no natural place to hand it off.
    // Leaking on a process-singleton is fine; daemon never re-enters main().
    Box::leak(Box::new(guard));

    let file_layer = tracing_subscriber::fmt::layer()
        .with_writer(non_blocking)
        .with_ansi(false)
        .with_target(true)
        .with_thread_ids(false)
        .with_line_number(true);

    let stderr_layer = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr)
        .with_ansi(false);

    let filter = EnvFilter::from_default_env()
        .add_directive("vault_sync_daemon=debug".parse().unwrap())
        .add_directive("eventsource_client=info".parse().unwrap());

    tracing_subscriber::registry()
        .with(filter)
        .with(file_layer)
        .with(stderr_layer)
        .try_init()
        .ok();

    tracing::info!(
        log_dir = %log_dir.display(),
        version = env!("CARGO_PKG_VERSION"),
        "vault-sync-daemon starting; file logging active"
    );

    vault_sync_daemon::run()
}
