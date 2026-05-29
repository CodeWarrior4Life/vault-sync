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
    /// `D:\Vaults`). v0.3.9 (Option A namespace unification) re-introduces
    /// the per-config `vault_name` below; the daemon's effective watch +
    /// materialize + push + verify-repair root is `vaults_root.join(vault_name)`.
    ///
    /// Back-compat: if `vaults_root` is missing but legacy `vault_root` is
    /// present in the on-disk file, the deserializer accepts the legacy
    /// field via the alias below.
    #[serde(alias = "vault_root")]
    pub vaults_root: PathBuf,
    /// v0.3.9 (Option A namespace unification): the single vault folder name
    /// under `vaults_root` (e.g. `"Mainframe"`). The daemon's effective
    /// watch + materialize + push + verify-repair root is
    /// `vaults_root.join(vault_name)`, so every payload path is BARE
    /// (vault-relative) — matching the server canonical. `#[serde(default)]`
    /// yields `""` for pre-v0.3.9 configs; an empty value signals
    /// "needs migration / re-pair" and is gated at startup (lib.rs).
    #[serde(default)]
    pub vault_name: String,
    pub daemon_version: String,
    pub daemon_platform: String,
    #[serde(default)]
    pub last_event_id: Option<String>,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_loads_vault_name_field() {
        let toml_str = r#"
nexus_url = "https://example.com"
subscriber_id = "abc-123"
vaults_root = "/Users/test/Vaults"
vault_name = "Mainframe"
daemon_version = "0.3.9"
daemon_platform = "macos-aarch64"
"#;
        let cfg: Config = toml::from_str(toml_str).expect("config must load");
        assert_eq!(cfg.vaults_root, PathBuf::from("/Users/test/Vaults"));
        assert_eq!(cfg.vault_name, "Mainframe");
    }

    #[test]
    fn config_missing_vault_name_defaults_empty() {
        // v0.3.8 on-disk configs lack vault_name; serde-default yields "".
        let toml_str = r#"
nexus_url = "https://example.com"
subscriber_id = "abc-123"
vaults_root = "/Users/test/Vaults"
daemon_version = "0.3.8"
daemon_platform = "macos-aarch64"
"#;
        let cfg: Config = toml::from_str(toml_str).expect("legacy config must still load");
        assert_eq!(cfg.vault_name, "", "missing vault_name must default to empty");
    }

    #[test]
    fn config_save_includes_vault_name_field() {
        let cfg = Config {
            nexus_url: "https://x".into(),
            subscriber_id: "s".into(),
            vaults_root: PathBuf::from("/v"),
            vault_name: "Mainframe".into(),
            daemon_version: "0.3.9".into(),
            daemon_platform: "macos-aarch64".into(),
            last_event_id: None,
        };
        let serialized = toml::to_string(&cfg).expect("serialize");
        assert!(
            serialized.contains("vault_name = \"Mainframe\""),
            "vault_name must appear in saved config; got: {serialized}"
        );
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
