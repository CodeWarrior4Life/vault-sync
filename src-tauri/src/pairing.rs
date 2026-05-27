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
    pub vault_root: PathBuf,
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
        vault_root: input.vault_root,
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
