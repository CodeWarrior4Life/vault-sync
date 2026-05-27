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
        // v0.2.0: vault_name hardcoded to "Mainframe" — only vault Nexus
        // currently knows server-side. Multi-vault routing comes when
        // events carry their own vault_id.
        vault_name: "Mainframe".to_string(),
        daemon_version: env!("CARGO_PKG_VERSION").to_string(),
        daemon_platform: detect_platform(),
        last_event_id: None,
    };
    cfg.save_to(&config_path)?;
    Ok(PairingSuccess {
        subscriber_id: snap.subscriber_id,
        scope_roots: snap.scope_roots,
        materializer_mode: snap.materializer_mode,
    })
}

#[tauri::command]
pub async fn pair(input: PairingInput) -> Result<PairingSuccess, String> {
    pair_inner(input, default_config_path())
        .await
        .map_err(|e| e.to_string())
}

#[derive(Debug, Serialize)]
pub struct CurrentConfig {
    pub nexus_url: String,
    pub vaults_root: String,
    pub vault_name: String,
    pub subscriber_id: String,
}

#[tauri::command]
pub fn load_current_config() -> Option<CurrentConfig> {
    let path = default_config_path();
    let cfg = Config::load_from(&path).ok()?;
    Some(CurrentConfig {
        nexus_url: cfg.nexus_url,
        vaults_root: cfg.vaults_root.to_string_lossy().to_string(),
        vault_name: cfg.vault_name,
        subscriber_id: cfg.subscriber_id,
    })
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
