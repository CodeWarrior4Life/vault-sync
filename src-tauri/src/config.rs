use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("config file not found")]
    NotFound,
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("parse error: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("serialize error: {0}")]
    Serialize(#[from] toml::ser::Error),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Config {
    pub nexus_url: String,
    pub subscriber_id: String,
    /// v0.2.0: PARENT directory holding one or more Obsidian vaults (e.g.
    /// `D:\Vaults`). Materializer routes to `<vaults_root>/<vault_name>/.../`.
    /// Back-compat: if `vaults_root` is missing but legacy `vault_root` is
    /// present in the on-disk file, the deserializer accepts the legacy
    /// field and the daemon derives `vaults_root` = parent + `vault_name` =
    /// basename at load time.
    #[serde(alias = "vault_root")]
    pub vaults_root: PathBuf,
    /// v0.2.0: name of THIS subscriber's vault under `vaults_root`. Today
    /// hardcoded to "Mainframe" since that's the only vault Nexus knows
    /// about server-side. Future multi-vault server-side will let each
    /// subscriber/event carry its own vault_id and the daemon will route
    /// per-vault.
    #[serde(default = "default_vault_name")]
    pub vault_name: String,
    pub daemon_version: String,
    pub daemon_platform: String,
    #[serde(default)]
    pub last_event_id: Option<String>,
}

fn default_vault_name() -> String {
    "Mainframe".to_string()
}

impl Config {
    pub fn load_from(path: &Path) -> Result<Self, ConfigError> {
        if !path.exists() {
            return Err(ConfigError::NotFound);
        }
        let s = fs::read_to_string(path)?;
        Ok(toml::from_str(&s)?)
    }

    pub fn save_to(&self, path: &Path) -> Result<(), ConfigError> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let s = toml::to_string_pretty(self)?;
        fs::write(path, s)?;
        Ok(())
    }
}

/// Returns the OS-appropriate config path:
/// - Windows: `%APPDATA%\Nexus\vault-sync\config.toml`
/// - macOS:   `~/Library/Application Support/Nexus/vault-sync/config.toml`
/// - Linux:   `$XDG_CONFIG_HOME/nexus-vault-sync/config.toml` (default `~/.config/nexus-vault-sync/config.toml`)
pub fn default_config_path() -> PathBuf {
    let base = dirs::config_dir().expect("config dir resolvable");
    #[cfg(target_os = "linux")]
    return base.join("nexus-vault-sync").join("config.toml");
    #[cfg(not(target_os = "linux"))]
    return base.join("Nexus").join("vault-sync").join("config.toml");
}
