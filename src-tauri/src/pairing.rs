use crate::api_client::{ApiClient, ApiError, HealthSnapshot};
use crate::config::{
    apply_enrollment_bytes, default_config_path, Config, ConfigError, EnrollmentFields,
};
use crate::durable_file::Commit;
use crate::keyring::KeyringError;
use crate::token_store::{self, TokenStoreError, TokenStored};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PairingError {
    #[error("keyring unavailable: {0}")]
    KeyringUnavailable(#[from] KeyringError),
    #[error("token persistence failed: {0}")]
    TokenStore(#[from] TokenStoreError),
    #[error("api error: {0}")]
    Api(#[from] ApiError),
    #[error("config error: {0}")]
    Config(#[from] ConfigError),
    /// CS-1 (C2): after a config write whose durability was uncertain, the
    /// bytes read back differ from the bytes written. Never retried.
    #[error(
        "config read-back mismatch at {path}: the file on disk is not what pairing wrote \
         (durability was uncertain); not retrying — re-run pairing"
    )]
    ReadBackMismatch { path: String },
    /// CS-1 (C2): the read-back after an uncertain config write failed.
    #[error("config read-back failed after an uncertain write: {0}")]
    ReadBack(std::io::Error),
}

/// CS-1: durability of the pairing's persisted state (config + token file),
/// the WEAKER of the two replace outcomes. A read-back never upgrades it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Durability {
    Durable,
    Uncertain,
}

impl From<Commit> for Durability {
    fn from(c: Commit) -> Self {
        match c {
            Commit::Durable => Durability::Durable,
            Commit::CommittedDurabilityUncertain => Durability::Uncertain,
        }
    }
}

/// CS-1: the one-line warning the CLI prints and the wizard shows when the
/// pairing's durability is `Uncertain`.
pub const UNCERTAIN_DURABILITY_WARNING: &str = "Paired, but the system could not confirm the \
    settings were flushed to disk; if this machine loses power soon, pair again.";

#[derive(Debug, Deserialize)]
pub struct PairingInput {
    pub nexus_url: String,
    pub token: String,
    /// v0.2.0: PARENT directory of one-or-more Obsidian vaults
    /// (e.g. `D:\Vaults`). Aliased to legacy `vault_root` for back-compat
    /// with v0.1.x clients still POSTing the old field name.
    #[serde(alias = "vault_root")]
    pub vaults_root: PathBuf,
    /// v0.3.2: optional mode override. If `Some(...)`, the wizard called
    /// the picker -- daemon PATCHes `/api/sync/subscribers/me` after the
    /// legacy pair flow to flip the server-side row. `None` leaves the
    /// existing server-side mode untouched (back-compat).
    #[serde(default)]
    pub materializer_mode: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct PairingSuccess {
    pub subscriber_id: String,
    pub scope_roots: Vec<String>,
    pub materializer_mode: String,
    /// CS-1: the weaker of the config and token-file replace outcomes.
    pub durability: Durability,
    /// CS-1: [`UNCERTAIN_DURABILITY_WARNING`] when `durability` is
    /// `Uncertain`, for the wizard to display; `None` otherwise.
    pub durability_warning: Option<String>,
}

/// Extracted for unit testability — no Tauri runtime context required.
pub async fn pair_inner(
    input: PairingInput,
    config_path: PathBuf,
) -> Result<PairingSuccess, PairingError> {
    pair_inner_with(input, config_path, token_store::store).await
}

/// [`pair_inner`] with the token persistence injected (tests pass a writer
/// rooted in a temp dir so they never touch the real config dir).
pub(crate) async fn pair_inner_with<F>(
    input: PairingInput,
    config_path: PathBuf,
    store_token: F,
) -> Result<PairingSuccess, PairingError>
where
    F: Fn(&str, &str) -> Result<TokenStored, TokenStoreError>,
{
    // v0.1.4: skip keyring preflight here — token_store::store handles
    // keyring failure transparently by falling back to a 0600 file. Pairing
    // succeeds on Linux-no-secret-service and SSH-installed-Mac alike.
    let client = ApiClient::new(&input.nexus_url, &input.token)?;
    let snap: HealthSnapshot = client.health().await?;
    let token = store_token(&snap.subscriber_id, &input.token)?;
    tracing::info!("token persisted via {} backend", token.backend);
    // P0 2026-08-05: MERGE — never overwrite — the on-disk config.
    // Enrollment owns exactly the fields in `EnrollmentFields` (plus the
    // last_event_id cursor); everything else (`vault_name`, `[[sync_roots]]`,
    // unknown/future keys) must survive a re-pair. The old path serialized a
    // typed `Config` (which has no `vault_name` field) over the file,
    // silently dropping the field and mis-rooting the daemon at
    // `vaults_root` itself on the next load.
    //
    // CS-1: exactly ONE atomic replace. On `Uncertain` the bytes are read
    // back and compared; a mismatch is an error and is NEVER retried, and an
    // equal read-back does NOT upgrade the outcome (C2).
    let (config_commit, written) = apply_enrollment_bytes(
        &config_path,
        &EnrollmentFields {
            nexus_url: input.nexus_url,
            subscriber_id: snap.subscriber_id.clone(),
            vaults_root: input.vaults_root,
            daemon_version: env!("CARGO_PKG_VERSION").to_string(),
            daemon_platform: detect_platform(),
        },
    )?;
    if config_commit == Commit::CommittedDurabilityUncertain {
        let back = std::fs::read(&config_path).map_err(PairingError::ReadBack)?;
        if back != written {
            return Err(PairingError::ReadBackMismatch {
                path: config_path.display().to_string(),
            });
        }
    }
    let durability = Durability::from(config_commit.weaker(token.commit));
    if durability == Durability::Uncertain {
        tracing::warn!("{UNCERTAIN_DURABILITY_WARNING}");
    }

    // v0.3.2: if the wizard sent a mode preference, push it to the server
    // via the subscriber self-PATCH endpoint. Failure here is non-fatal --
    // the pair itself succeeded; the user just doesn't get the mode they
    // asked for and a warning surfaces in the result.
    let final_mode = if let Some(requested) = input.materializer_mode.as_deref() {
        match client.patch_self_subscriber(Some(requested)).await {
            Ok(state) => state.materializer_mode,
            Err(e) => {
                tracing::warn!("patch_self_subscriber failed: {e} -- server mode unchanged");
                snap.materializer_mode
            }
        }
    } else {
        snap.materializer_mode
    };

    Ok(PairingSuccess {
        subscriber_id: snap.subscriber_id,
        scope_roots: snap.scope_roots,
        materializer_mode: final_mode,
        durability,
        durability_warning: (durability == Durability::Uncertain)
            .then(|| UNCERTAIN_DURABILITY_WARNING.to_string()),
    })
}

#[tauri::command]
pub async fn pair(app: tauri::AppHandle, input: PairingInput) -> Result<PairingSuccess, String> {
    let result = pair_inner(input, default_config_path())
        .await
        .map_err(|e| e.to_string())?;
    // S477 §3.3 (v0.3.7): notify on first successful pair so the user knows
    // the tray-only daemon is running -- the wizard window will hide on
    // close and they need to be able to find the tray icon.
    crate::notify_user(
        &app,
        "Vault Sync is running",
        "Look for the icon in your menu bar near the notch.",
    );
    Ok(result)
}

#[derive(Debug, Serialize)]
pub struct CurrentConfig {
    pub nexus_url: String,
    pub vaults_root: String,
    pub subscriber_id: String,
}

#[tauri::command]
pub fn load_current_config() -> Option<CurrentConfig> {
    let path = default_config_path();
    let cfg = Config::load_from(&path).ok()?;
    Some(CurrentConfig {
        nexus_url: cfg.nexus_url,
        vaults_root: cfg.vaults_root.to_string_lossy().to_string(),
        subscriber_id: cfg.subscriber_id,
    })
}

/// S477 §3.2 (Phase B): Tauri-exposed PATCH for the Paired-page "Edit
/// Settings" flow when the user re-submits the form WITHOUT rotating the
/// token. The frontend calls this with the new vaults_root + new mode.
///
/// **STUB**: full implementation (load token + ApiClient + PATCH
/// `/api/sync/subscribers/me` with vaults_root + materializer_mode + persist
/// the updated config locally) is parked for Phase F coordination. The
/// server-side PATCH endpoint currently only accepts `materializer_mode`,
/// so plumbing `vaults_root` through requires a coordinated server change.
/// Until that lands, surface a structured error to the user so the wizard
/// can show "Edit Settings not yet wired — re-pair with token to change
/// vault root or mode."
///
/// TODO(s477-phase-f): wire ApiClient::patch_self_subscriber, extend server
/// PATCH schema to accept vaults_root, persist updated Config to disk, emit
/// a 're-pair' equivalent event so the daemon picks up the new root.
#[tauri::command]
pub async fn patch_self_subscriber(
    nexus_url: String,
    new_vaults_root: String,
    new_mode: String,
) -> Result<(), String> {
    let _ = (nexus_url, new_vaults_root, new_mode);
    Err("PATCH not yet implemented (S477 Phase F coordination pending — re-pair with a token to change settings).".to_string())
}

/// v0.3: return the currently-stored bearer token (keyring → file fallback)
/// so the Settings UI can pre-fill the field. Cyril verbatim S473:
/// *"its not truly persisted unless it shows up as filled in the UI"*.
/// Returns None if no token is found for the configured subscriber_id.
#[tauri::command]
pub fn load_current_token() -> Option<String> {
    let path = default_config_path();
    let cfg = Config::load_from(&path).ok()?;
    token_store::load(&cfg.subscriber_id).ok().flatten()
}

fn detect_platform() -> String {
    match (std::env::consts::OS, std::env::consts::ARCH) {
        ("windows", "x86_64") => "windows-x86_64",
        ("macos", "x86_64") => "macos-x86_64",
        ("macos", "aarch64") => "macos-aarch64",
        ("linux", "x86_64") => "linux-x86_64",
        (o, a) => Box::leak(format!("{o}-{a}").into_boxed_str()),
    }
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::durable_file::seams;
    use crate::token_store::KeyringOps;
    use std::path::Path;

    struct NoKeyring;
    impl KeyringOps for NoKeyring {
        fn get(&self, _s: &str) -> Result<Option<String>, KeyringError> {
            panic!("keyring must not be touched")
        }
        fn set(&self, _s: &str, _t: &str) -> Result<(), KeyringError> {
            panic!("keyring must not be touched")
        }
    }

    async fn health_server(sub: &str) -> mockito::ServerGuard {
        let mut srv = mockito::Server::new_async().await;
        srv.mock("GET", "/api/sync/health")
            .with_status(200)
            .with_body(format!(
                r#"{{"subscriber_id":"{sub}","scope_roots":["notes/"],"scope_excludes":[],"materializer_mode":"shadow","shadow_path":null}}"#
            ))
            .create_async()
            .await;
        srv
    }

    /// Run `pair_inner_with`, writing the token under `tok_dir` (file-only).
    async fn pair_in(
        url: &str,
        cfg_path: &Path,
        tok_dir: &Path,
    ) -> Result<PairingSuccess, PairingError> {
        let input = PairingInput {
            nexus_url: url.to_string(),
            token: "vsk_cs1".to_string(),
            vaults_root: PathBuf::from("/tmp/vaults-cs1"),
            materializer_mode: None,
        };
        let tok_dir = tok_dir.to_path_buf();
        pair_inner_with(input, cfg_path.to_path_buf(), move |sub, tok| {
            let p = tok_dir.join(format!("token-{sub}.bin"));
            token_store::store_impl(sub, tok, &p, &NoKeyring, true)
        })
        .await
    }

    fn config_calls() -> usize {
        seams::calls()
            .iter()
            .filter(|c| *c == "settings.toml")
            .count()
    }

    /// Positive control: with no injected fault the pairing is `Durable` on
    /// unix (and `Uncertain` on Windows, which cannot prove durability).
    #[tokio::test]
    async fn pair_without_faults_reports_platform_durability() {
        seams::reset();
        let srv = health_server("sub-cs1-ok").await;
        let d = tempfile::TempDir::new().unwrap();
        let cfg = d.path().join("cfg").join("settings.toml");
        let r = pair_in(&srv.url(), &cfg, &d.path().join("tok"))
            .await
            .unwrap();
        #[cfg(unix)]
        {
            assert_eq!(r.durability, Durability::Durable);
            assert!(r.durability_warning.is_none());
        }
        #[cfg(not(unix))]
        assert_eq!(r.durability, Durability::Uncertain);
        assert_eq!(config_calls(), 1);
    }

    /// T-CS1-8: an injected `CommittedDurabilityUncertain` on the config write
    /// gives `durability: Uncertain` after a byte-equal read-back (no upgrade)
    /// and exactly ONE config `replace_atomic` (no retry).
    /// Mutation: retry the write on `Uncertain`.
    #[tokio::test]
    async fn t_cs1_8_uncertain_config_write_is_reported_and_never_retried() {
        seams::reset();
        let srv = health_server("sub-cs1-8").await;
        let d = tempfile::TempDir::new().unwrap();
        let cfg_dir = d.path().join("cfg");
        let cfg = cfg_dir.join("settings.toml");
        seams::fail_dir_fsync(Some(cfg_dir.clone()), 5); // EIO on the config dir only
        let r = pair_in(&srv.url(), &cfg, &d.path().join("tok"))
            .await
            .unwrap();
        assert_eq!(r.durability, Durability::Uncertain);
        assert_eq!(
            r.durability_warning.as_deref(),
            Some(UNCERTAIN_DURABILITY_WARNING)
        );
        assert_eq!(
            config_calls(),
            1,
            "exactly one config replace: {:?}",
            seams::calls()
        );
        Config::load_from(&cfg).expect("committed config loads");
    }

    /// T-CS1-8 (C5 half): an uncertain TOKEN write makes the pairing
    /// `Uncertain` even when the config write is durable (weaker of the two).
    #[cfg(unix)]
    #[tokio::test]
    async fn uncertain_token_write_makes_pairing_uncertain() {
        seams::reset();
        let srv = health_server("sub-cs1-tok").await;
        let d = tempfile::TempDir::new().unwrap();
        let tok_dir = d.path().join("tok");
        seams::fail_dir_fsync(Some(tok_dir.clone()), 5);
        let r = pair_in(
            &srv.url(),
            &d.path().join("cfg").join("settings.toml"),
            &tok_dir,
        )
        .await
        .unwrap();
        assert_eq!(r.durability, Durability::Uncertain);
    }

    /// T-CS1-14 (C2): a MISMATCHED read-back after an uncertain config write
    /// returns an error after exactly ONE write.
    /// Mutation: upgrade to `Durable`, or retry, on mismatch.
    #[tokio::test]
    async fn t_cs1_14_mismatched_read_back_is_an_error_after_one_write() {
        seams::reset();
        let srv = health_server("sub-cs1-14").await;
        let d = tempfile::TempDir::new().unwrap();
        let cfg_dir = d.path().join("cfg");
        let cfg = cfg_dir.join("settings.toml");
        seams::fail_dir_fsync(Some(cfg_dir.clone()), 5);
        seams::set_hook(Box::new(|p, path| {
            if p == seams::Point::AfterRename
                && path.file_name().and_then(|n| n.to_str()) == Some("settings.toml")
            {
                std::fs::write(path, b"tampered = true\n").unwrap();
            }
        }));
        let r = pair_in(&srv.url(), &cfg, &d.path().join("tok")).await;
        assert!(
            matches!(r, Err(PairingError::ReadBackMismatch { .. })),
            "mismatch must be an error, got {r:?}"
        );
        assert_eq!(
            config_calls(),
            1,
            "no retry on mismatch: {:?}",
            seams::calls()
        );
    }
}
