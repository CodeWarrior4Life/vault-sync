// Prevents additional console window on Windows in release, DO NOT REMOVE!!
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use std::path::PathBuf;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// Usage banner for the headless CLI (S534). Printed on `--help` and on any
/// argument-parse error.
const USAGE: &str = "\
vault-sync-daemon — headless configuration + daemon

USAGE:
  vault-sync-daemon                      Launch the GUI/daemon (default, unchanged)
  vault-sync-daemon pair    --url <URL> --root <VAULTS_ROOT> --mode <live|shadow|disabled> [--token <TOKEN>]
  vault-sync-daemon set-mode <live|shadow|disabled>
  vault-sync-daemon --help

pair       Pair this host WITHOUT the GUI: verify the token against <URL>, persist
           local config, and PATCH the server-side subscriber to <mode>. Prints the
           PairingSuccess as JSON on success. The token is best supplied via the
           NEXUS_SYNC_TOKEN env var or stdin (so it never lands in argv/the process
           list); --token is accepted but discouraged.
set-mode   Change the materializer mode of an ALREADY-PAIRED host: loads the stored
           config + token and PATCHes the server-side subscriber. The running daemon
           picks the new mode up on its next start/health cycle.
";

/// v0.3.4: daily-rolling file appender at
/// `<data_local_dir>/Nexus/logs/daemon.log.YYYY-MM-DD`. Without this the
/// daemon ran on Windows GUI subsystem (no console), stderr went to the
/// void, and every silent error (envelope-parse rejections, materializer
/// write failures, etc.) was invisible. S476 root-cause hunt for the
/// "shadow materializer doesn't write" bug burned hours guessing because
/// these errors weren't observable.
/// R7 / F-A6 (TKT-989ad5f2): the crate's tracing directive. DEBUG only when
/// opted in; INFO otherwise. PURE + unit-tested.
fn resolve_log_directive(debug: bool) -> &'static str {
    if debug {
        "vault_sync_daemon=debug"
    } else {
        "vault_sync_daemon=info"
    }
}

/// R7 / F-A6: DEBUG-logging opt-in via the `VAULT_SYNC_LOG_DEBUG` env var.
/// Truthy = `1`/`true`/`yes`/`on`/`debug` (case-insensitive). Default (unset or
/// anything else) is INFO. Logging inits before the daemon config is loaded, so
/// the env var is the config knob available at this point.
fn debug_logging_opt_in() -> bool {
    std::env::var("VAULT_SYNC_LOG_DEBUG")
        .ok()
        .map(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on" | "debug"
            )
        })
        .unwrap_or(false)
}

fn init_logging() {
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

    // R7 / F-A6 (TKT-989ad5f2): default the daemon log level to INFO, with
    // DEBUG opt-in. Pre-fix this hardcoded `vault_sync_daemon=debug`, forcing
    // DEBUG for the crate regardless of env — 1.4GB/day on trinity. Logging
    // initializes before the daemon config is loaded and a tracing subscriber
    // cannot be re-inited, so the opt-in is the `VAULT_SYNC_LOG_DEBUG` env
    // (operators set it in the service unit / launch config). `RUST_LOG` still
    // overrides via `from_default_env` for ad-hoc debugging.
    let filter = EnvFilter::from_default_env()
        .add_directive(resolve_log_directive(debug_logging_opt_in()).parse().unwrap())
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
}

fn main() {
    init_logging();

    // S534: headless CLI. args() minus argv[0]. No subcommand ⇒ launch the GUI
    // exactly as before. `pair` / `set-mode` configure the daemon without the
    // GUI window (operator ask: "start this on my own with the settings
    // desired"). Help flags print usage to stdout and exit 0.
    let args: Vec<String> = std::env::args().skip(1).collect();
    if matches!(
        args.first().map(String::as_str),
        Some("--help" | "-h" | "help")
    ) {
        print!("{USAGE}");
        std::process::exit(0);
    }

    match parse_cli(&args) {
        Ok(CliCommand::Gui) => vault_sync_daemon::run(),
        Ok(cmd) => run_cli(cmd),
        Err(msg) => {
            eprintln!("error: {msg}\n\n{USAGE}");
            std::process::exit(2);
        }
    }
}

/// Parsed headless command. `Gui` means "no subcommand — behave as today".
#[derive(Debug, PartialEq, Eq)]
enum CliCommand {
    Gui,
    Pair {
        url: String,
        /// `None` ⇒ resolve from `NEXUS_SYNC_TOKEN` env then stdin at run time.
        token: Option<String>,
        root: String,
        mode: String,
    },
    SetMode {
        mode: String,
    },
}

/// PURE argument parser (no I/O, no env, no stdin) so it is exhaustively unit
/// testable. Token resolution (env/stdin) is deliberately deferred to run time.
fn parse_cli(args: &[String]) -> Result<CliCommand, String> {
    match args.first().map(String::as_str) {
        None => Ok(CliCommand::Gui),
        Some("pair") => {
            let rest = &args[1..];
            let url = extract_flag(rest, "--url")?.ok_or("pair: --url <URL> is required")?;
            let token = extract_flag(rest, "--token")?;
            let root =
                extract_flag(rest, "--root")?.ok_or("pair: --root <VAULTS_ROOT> is required")?;
            let mode = extract_flag(rest, "--mode")?
                .ok_or("pair: --mode <live|shadow|disabled> is required")?;
            validate_mode(&mode)?;
            Ok(CliCommand::Pair {
                url,
                token,
                root,
                mode,
            })
        }
        Some("set-mode") => {
            let mode = args
                .get(1)
                .cloned()
                .ok_or("set-mode: <live|shadow|disabled> is required")?;
            validate_mode(&mode)?;
            Ok(CliCommand::SetMode { mode })
        }
        // A leading FLAG (e.g. `--silent`, passed by the LaunchAgent / `open
        // -a … --args --silent` / the updater relaunch) is NOT a headless
        // subcommand — fall through to the normal GUI/daemon launch, which owns
        // its own flag handling. Only a bare non-subcommand word is a typo and
        // errors. (v0.4.31: v0.4.30 regressed here — `--silent` hit the error
        // arm, so the daemon refused to start on every normal launch.)
        Some(other) if other.starts_with('-') => Ok(CliCommand::Gui),
        Some(other) => Err(format!("unknown subcommand: {other}")),
    }
}

/// Extract `--name <value>` or `--name=<value>` from a flag list. Returns
/// `Ok(None)` when absent, `Err` when present but missing its value.
fn extract_flag(args: &[String], name: &str) -> Result<Option<String>, String> {
    let eq_prefix = format!("{name}=");
    let mut i = 0;
    while i < args.len() {
        let a = &args[i];
        if a == name {
            return args
                .get(i + 1)
                .cloned()
                .map(Some)
                .ok_or_else(|| format!("{name} requires a value"));
        }
        if let Some(v) = a.strip_prefix(&eq_prefix) {
            return Ok(Some(v.to_string()));
        }
        i += 1;
    }
    Ok(None)
}

fn validate_mode(mode: &str) -> Result<(), String> {
    match mode {
        "live" | "shadow" | "disabled" => Ok(()),
        other => Err(format!(
            "invalid mode '{other}' (expected live|shadow|disabled)"
        )),
    }
}

/// Run a headless CLI command to completion and exit the process. `pair` and
/// `set-mode` are async (they hit the Nexus API), so we spin a dedicated tokio
/// runtime here — this runs BEFORE any Tauri async context exists.
fn run_cli(cmd: CliCommand) -> ! {
    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("error: failed to start async runtime: {e}");
            std::process::exit(1);
        }
    };
    let result: Result<(), String> = rt.block_on(async move {
        match cmd {
            CliCommand::Pair {
                url,
                token,
                root,
                mode,
            } => cli_pair(url, token, root, mode).await,
            CliCommand::SetMode { mode } => cli_set_mode(mode).await,
            CliCommand::Gui => unreachable!("Gui is dispatched by main, not run_cli"),
        }
    });
    match result {
        Ok(()) => std::process::exit(0),
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(1);
        }
    }
}

/// Resolve the pairing token WITHOUT leaking it into argv: prefer the explicit
/// `--token`, then `NEXUS_SYNC_TOKEN`, then a single read from stdin.
fn resolve_token(flag: Option<String>) -> Result<String, String> {
    if let Some(t) = flag {
        if !t.is_empty() {
            return Ok(t);
        }
    }
    if let Ok(t) = std::env::var("NEXUS_SYNC_TOKEN") {
        if !t.is_empty() {
            return Ok(t);
        }
    }
    use std::io::Read;
    let mut buf = String::new();
    std::io::stdin()
        .read_to_string(&mut buf)
        .map_err(|e| format!("failed to read token from stdin: {e}"))?;
    let t = buf.trim().to_string();
    if t.is_empty() {
        return Err(
            "no token provided (pass --token, set NEXUS_SYNC_TOKEN, or pipe it on stdin)".into(),
        );
    }
    Ok(t)
}

async fn cli_pair(
    url: String,
    token: Option<String>,
    root: String,
    mode: String,
) -> Result<(), String> {
    let token = resolve_token(token)?;
    let input = vault_sync_daemon::pairing::PairingInput {
        nexus_url: url,
        token,
        vaults_root: PathBuf::from(root),
        materializer_mode: Some(mode),
    };
    let success = vault_sync_daemon::pairing::pair_inner(
        input,
        vault_sync_daemon::config::default_config_path(),
    )
    .await
    .map_err(|e| e.to_string())?;
    let json = serde_json::to_string_pretty(&success).map_err(|e| e.to_string())?;
    println!("{json}");
    Ok(())
}

async fn cli_set_mode(mode: String) -> Result<(), String> {
    let config_path = vault_sync_daemon::config::default_config_path();
    let cfg = vault_sync_daemon::config::Config::load_from(&config_path).map_err(|e| {
        format!(
            "cannot load config ({}): {e} — pair this host first",
            config_path.display()
        )
    })?;
    let token = vault_sync_daemon::token_store::load(&cfg.subscriber_id)
        .map_err(|e| format!("cannot load stored token: {e}"))?
        .ok_or("no stored token for this subscriber — pair this host first")?;
    let client = vault_sync_daemon::api_client::ApiClient::new(&cfg.nexus_url, &token)
        .map_err(|e| e.to_string())?;
    let state = client
        .patch_self_subscriber(Some(&mode))
        .await
        .map_err(|e| e.to_string())?;
    let json = serde_json::to_string_pretty(&state).map_err(|e| e.to_string())?;
    println!("{json}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn no_args_is_gui() {
        assert_eq!(parse_cli(&[]), Ok(CliCommand::Gui));
    }

    #[test]
    fn pair_parses_all_flags() {
        let args = v(&[
            "pair",
            "--url",
            "https://nexus.example",
            "--token",
            "vsk_abc",
            "--root",
            "/Vaults/Mainframe",
            "--mode",
            "live",
        ]);
        assert_eq!(
            parse_cli(&args),
            Ok(CliCommand::Pair {
                url: "https://nexus.example".into(),
                token: Some("vsk_abc".into()),
                root: "/Vaults/Mainframe".into(),
                mode: "live".into(),
            })
        );
    }

    #[test]
    fn pair_token_optional_defaults_none() {
        let args = v(&[
            "pair",
            "--url",
            "https://n",
            "--root",
            "/v",
            "--mode",
            "shadow",
        ]);
        assert_eq!(
            parse_cli(&args),
            Ok(CliCommand::Pair {
                url: "https://n".into(),
                token: None,
                root: "/v".into(),
                mode: "shadow".into(),
            })
        );
    }

    #[test]
    fn pair_flag_order_independent_and_eq_form() {
        // Flags in a different order, and the `--flag=value` form.
        let args = v(&["pair", "--mode=disabled", "--root=/v", "--url=https://n"]);
        assert_eq!(
            parse_cli(&args),
            Ok(CliCommand::Pair {
                url: "https://n".into(),
                token: None,
                root: "/v".into(),
                mode: "disabled".into(),
            })
        );
    }

    #[test]
    fn pair_missing_required_flag_errors() {
        let args = v(&["pair", "--url", "https://n", "--mode", "live"]); // no --root
        assert!(parse_cli(&args).is_err());
    }

    #[test]
    fn pair_flag_without_value_errors() {
        let args = v(&["pair", "--url"]); // dangling flag
        assert!(parse_cli(&args).is_err());
    }

    #[test]
    fn pair_invalid_mode_errors() {
        let args = v(&[
            "pair",
            "--url",
            "https://n",
            "--root",
            "/v",
            "--mode",
            "bogus",
        ]);
        assert!(parse_cli(&args).is_err());
    }

    #[test]
    fn set_mode_parses() {
        assert_eq!(
            parse_cli(&v(&["set-mode", "live"])),
            Ok(CliCommand::SetMode {
                mode: "live".into()
            })
        );
    }

    #[test]
    fn set_mode_requires_mode() {
        assert!(parse_cli(&v(&["set-mode"])).is_err());
    }

    #[test]
    fn set_mode_invalid_errors() {
        assert!(parse_cli(&v(&["set-mode", "nonsense"])).is_err());
    }

    #[test]
    fn unknown_subcommand_errors() {
        assert!(parse_cli(&v(&["frobnicate"])).is_err());
    }

    #[test]
    fn leading_flags_fall_through_to_gui() {
        // v0.4.31 regression guard: the normal launch passes --silent; it must
        // NOT be treated as a subcommand (that broke daemon startup in v0.4.30).
        assert_eq!(parse_cli(&v(&["--silent"])).unwrap(), CliCommand::Gui);
        assert_eq!(
            parse_cli(&v(&["--silent", "--foo"])).unwrap(),
            CliCommand::Gui
        );
        assert_eq!(parse_cli(&v(&[])).unwrap(), CliCommand::Gui);
    }

    #[test]
    fn extract_flag_space_and_eq_and_absent() {
        let args = v(&["--a", "1", "--b=2"]);
        assert_eq!(extract_flag(&args, "--a").unwrap(), Some("1".into()));
        assert_eq!(extract_flag(&args, "--b").unwrap(), Some("2".into()));
        assert_eq!(extract_flag(&args, "--c").unwrap(), None);
        assert!(extract_flag(&v(&["--a"]), "--a").is_err());
    }

    /// R7 / F-A6 (TKT-989ad5f2): the default daemon log level is INFO; DEBUG is
    /// opt-in only. This pins that the hardcoded `=debug` directive is gone.
    #[test]
    fn log_directive_defaults_to_info_debug_is_opt_in() {
        assert_eq!(resolve_log_directive(false), "vault_sync_daemon=info");
        assert_eq!(resolve_log_directive(true), "vault_sync_daemon=debug");
    }
}
