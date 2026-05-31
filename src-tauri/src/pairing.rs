use crate::api_client::{ApiClient, ApiError, HealthSnapshot};
use crate::config::{default_config_path, Config, ConfigError};
use crate::keyring::KeyringError;
use crate::token_store::{self, TokenStoreError};
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
}

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
}

/// Extracted for unit testability — no Tauri runtime context required.
pub async fn pair_inner(
    input: PairingInput,
    config_path: PathBuf,
) -> Result<PairingSuccess, PairingError> {
    // v0.1.4: skip keyring preflight here — token_store::store handles
    // keyring failure transparently by falling back to a 0600 file. Pairing
    // succeeds on Linux-no-secret-service and SSH-installed-Mac alike.
    let client = ApiClient::new(&input.nexus_url, &input.token)?;
    let snap: HealthSnapshot = client.health().await?;
    let backend = token_store::store(&snap.subscriber_id, &input.token)?;
    tracing::info!("token persisted via {backend} backend");
    let cfg = Config {
        nexus_url: input.nexus_url,
        subscriber_id: snap.subscriber_id.clone(),
        vaults_root: input.vaults_root,
        daemon_version: env!("CARGO_PKG_VERSION").to_string(),
        daemon_platform: detect_platform(),
        last_event_id: None,
        // TODO(B2): populate sync_roots from pairing wizard input once
        // the watch loop iterates sync_roots instead of vaults_root.
        sync_roots: vec![],
    };
    cfg.save_to(&config_path)?;

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
