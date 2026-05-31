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

/// B1 (Nexus Sync): an independent sync root — one directory tree whose
/// changes are pushed to / materialised from Nexus.
///
/// `route` is a short, lower-case identifier used to select the SSE
/// subscriber scope on the server side.  An empty string (`""`) is the
/// canonical Mainframe vault (bare storage).  Examples: `""`, `"dev"`,
/// `"archive"`.
///
/// `subscriber_id` (B2b): the subscriber ID this root pushes under.  The
/// server maps subscriber → its registered route → storage.  For the vault
/// root (back-compat path), this is copied from the top-level
/// `Config.subscriber_id` by `from_toml_back_compat`.  For extra roots
/// added via `[[sync_roots]]` blocks that omit this field, it defaults to
/// `""` — filled in at pairing time (a later task).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct SyncRoot {
    pub path: std::path::PathBuf,
    #[serde(default)]
    pub route: String,
    /// B2b: per-root subscriber ID. Defaults to `""` when omitted from TOML
    /// (filled at pairing). The back-compat synthesis path assigns the
    /// top-level `Config.subscriber_id` here automatically.
    #[serde(default)]
    pub subscriber_id: String,
}

/// Intermediate deserialisation target that tolerates the legacy
/// `vaults_root` / `vault_name` fields so we can synthesise `sync_roots`
/// in `Config::from_toml_back_compat`.
#[derive(Debug, Deserialize)]
struct RawConfig {
    pub nexus_url: String,
    pub subscriber_id: String,
    #[serde(alias = "vault_root")]
    pub vaults_root: PathBuf,
    pub daemon_version: String,
    pub daemon_platform: String,
    #[serde(default)]
    pub last_event_id: Option<String>,
    /// Legacy field — present on v0.2.0 – v0.3.6 on-disk configs.
    /// Used by `from_toml_back_compat` to synthesise `sync_roots`.
    #[serde(default)]
    pub vault_name: Option<String>,
    /// B1 (Nexus Sync): new multi-root list.
    #[serde(default)]
    pub sync_roots: Vec<SyncRoot>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Config {
    pub nexus_url: String,
    pub subscriber_id: String,
    /// v0.2.0: PARENT directory holding one or more Obsidian vaults (e.g.
    /// `D:\Vaults`). Post-S477, this IS the daemon's watch + materialize
    /// root; the vault folder name becomes the first segment of every
    /// payload path (no per-config vault_name needed).
    ///
    /// Back-compat: if `vaults_root` is missing but legacy `vault_root` is
    /// present in the on-disk file, the deserializer accepts the legacy
    /// field via the alias below.
    ///
    /// Legacy `vault_name` field (v0.2.0 – v0.3.6) is silently ignored on
    /// load — serde tolerates unknown TOML keys by default, so existing
    /// configs continue to deserialize without manual migration.
    ///
    /// TODO(B2): once the watch loop is rewired to iterate `sync_roots`,
    /// `vaults_root` can be removed and `sync_roots[0].path` used instead.
    #[serde(alias = "vault_root")]
    pub vaults_root: PathBuf,
    pub daemon_version: String,
    pub daemon_platform: String,
    #[serde(default)]
    pub last_event_id: Option<String>,
    /// B1 (Nexus Sync): ordered list of independent sync roots.
    ///
    /// On fresh configs this is populated explicitly.  On legacy on-disk
    /// configs it is synthesised by `from_toml_back_compat` from the
    /// `vaults_root` (+ optional `vault_name`) fields so that call sites
    /// can migrate to iterating `sync_roots` incrementally.
    ///
    /// `#[serde(default)]` keeps deserialization of configs that pre-date
    /// B1 working — the field will simply be an empty Vec.
    #[serde(default)]
    pub sync_roots: Vec<SyncRoot>,
}

impl Config {
    /// Parse TOML and synthesise `sync_roots` from legacy fields when the
    /// new `[[sync_roots]]` block is absent or empty.
    ///
    /// Rules (applied in order):
    /// 1. If `sync_roots` is non-empty, use it as-is.
    /// 2. Else if `vaults_root` is present:
    ///    a. If `vault_name` is non-empty, synthesise
    ///       `SyncRoot { path: vaults_root.join(vault_name), route: "" }`.
    ///    b. Otherwise synthesise
    ///       `SyncRoot { path: vaults_root, route: "" }`.
    #[allow(clippy::doc_overindented_list_items)]
    pub fn from_toml_back_compat(s: &str) -> Result<Self, ConfigError> {
        let raw: RawConfig = toml::from_str(s)?;

        let sync_roots = if !raw.sync_roots.is_empty() {
            // Explicit [[sync_roots]] blocks: use as-is. Their `subscriber_id`
            // defaults to "" via #[serde(default)] when omitted from TOML.
            raw.sync_roots
        } else {
            // Legacy path: synthesise from vaults_root + optional vault_name.
            // B2b: the synthesised vault root inherits the top-level
            // subscriber_id so existing installs keep pushing under the same
            // subscriber they always have.
            let path = match raw.vault_name.as_deref() {
                Some(name) if !name.is_empty() => raw.vaults_root.join(name),
                _ => raw.vaults_root.clone(),
            };
            vec![SyncRoot {
                path,
                route: String::new(),
                subscriber_id: raw.subscriber_id.clone(),
            }]
        };

        Ok(Config {
            nexus_url: raw.nexus_url,
            subscriber_id: raw.subscriber_id,
            vaults_root: raw.vaults_root,
            daemon_version: raw.daemon_version,
            daemon_platform: raw.daemon_platform,
            last_event_id: raw.last_event_id,
            sync_roots,
        })
    }

    pub fn load_from(path: &Path) -> Result<Self, ConfigError> {
        if !path.exists() {
            return Err(ConfigError::NotFound);
        }
        let s = fs::read_to_string(path)?;
        Self::from_toml_back_compat(&s)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_loads_legacy_vault_name_field_without_error() {
        let toml_str = r#"
nexus_url = "https://example.com"
subscriber_id = "abc-123"
vaults_root = "/Users/test/Vaults"
vault_name = "Mainframe"
daemon_version = "0.3.6"
daemon_platform = "macos-aarch64"
"#;
        let cfg: Config = toml::from_str(toml_str).expect("legacy config must load");
        assert_eq!(cfg.vaults_root, PathBuf::from("/Users/test/Vaults"));
    }

    #[test]
    fn config_save_omits_vault_name_field() {
        let cfg = Config {
            nexus_url: "https://x".into(),
            subscriber_id: "s".into(),
            vaults_root: PathBuf::from("/v"),
            daemon_version: "0.3.7".into(),
            daemon_platform: "macos-aarch64".into(),
            last_event_id: None,
            sync_roots: vec![],
        };
        let serialized = toml::to_string(&cfg).expect("serialize");
        assert!(
            !serialized.contains("vault_name"),
            "vault_name must not appear in saved config; got: {serialized}"
        );
    }

    // --- B1: new sync_roots tests ---

    #[test]
    fn sync_roots_parse_new_shape() {
        let toml_str = r#"
nexus_url = "https://nexus.example.com"
subscriber_id = "sub-001"
vaults_root = "/Users/test/Vaults"
daemon_version = "0.4.0"
daemon_platform = "macos-aarch64"

[[sync_roots]]
path = "/Users/test/Vaults/Mainframe"
route = ""

[[sync_roots]]
path = "/Users/test/Vaults/Dev"
route = "dev"
"#;
        let cfg = Config::from_toml_back_compat(toml_str).expect("new-shape parse must succeed");
        assert_eq!(cfg.sync_roots.len(), 2, "expected 2 sync_roots");
        assert_eq!(cfg.sync_roots[0].route, "");
        assert_eq!(cfg.sync_roots[1].route, "dev");
    }

    #[test]
    fn back_compat_legacy_vaults_root_vault_name() {
        // Legacy TOML: has vaults_root + vault_name but NO sync_roots block.
        // back-compat should synthesize one SyncRoot with path = vaults_root/vault_name.
        let toml_str = r#"
nexus_url = "https://nexus.example.com"
subscriber_id = "sub-002"
vaults_root = "/Users/test/Vaults"
vault_name = "Mainframe"
daemon_version = "0.3.8"
daemon_platform = "macos-aarch64"
"#;
        let cfg = Config::from_toml_back_compat(toml_str)
            .expect("legacy vaults_root+vault_name must synthesize sync_roots");
        assert_eq!(
            cfg.sync_roots.len(),
            1,
            "expected exactly 1 synthesized sync_root"
        );
        assert_eq!(cfg.sync_roots[0].route, "");
        assert!(
            cfg.sync_roots[0].path.ends_with("Mainframe"),
            "synthesized path must end with vault_name; got: {:?}",
            cfg.sync_roots[0].path
        );
    }

    #[test]
    fn back_compat_vaults_root_only() {
        // Legacy TOML: vaults_root present but vault_name absent → use vaults_root itself.
        let toml_str = r#"
nexus_url = "https://nexus.example.com"
subscriber_id = "sub-003"
vaults_root = "/Users/test/Vaults"
daemon_version = "0.3.8"
daemon_platform = "macos-aarch64"
"#;
        let cfg = Config::from_toml_back_compat(toml_str)
            .expect("legacy vaults_root-only must synthesize sync_roots");
        assert_eq!(
            cfg.sync_roots.len(),
            1,
            "expected exactly 1 synthesized sync_root"
        );
        assert_eq!(cfg.sync_roots[0].route, "");
        assert_eq!(
            cfg.sync_roots[0].path,
            PathBuf::from("/Users/test/Vaults"),
            "path must equal vaults_root when vault_name absent"
        );
    }

    #[test]
    fn sync_root_round_trips_serde() {
        let original = Config {
            nexus_url: "https://nexus.example.com".into(),
            subscriber_id: "sub-rt".into(),
            vaults_root: PathBuf::from("/Users/test/Vaults"),
            daemon_version: "0.4.0".into(),
            daemon_platform: "macos-aarch64".into(),
            last_event_id: Some("evt-42".into()),
            sync_roots: vec![
                SyncRoot {
                    path: PathBuf::from("/Users/test/Vaults/Mainframe"),
                    route: String::new(),
                    subscriber_id: "sub-rt".into(),
                },
                SyncRoot {
                    path: PathBuf::from("/Users/test/DevVaults/Work"),
                    route: "work".into(),
                    subscriber_id: String::new(),
                },
            ],
        };
        let serialized = toml::to_string_pretty(&original).expect("serialize");
        let deserialized: Config = toml::from_str(&serialized).expect("deserialize");
        assert_eq!(
            original, deserialized,
            "round-trip must produce identical Config"
        );
    }

    // --- B2b: per-root subscriber_id tests ---

    /// A `[[sync_roots]]` block with an explicit `subscriber_id` value must
    /// surface that value on the parsed `SyncRoot`.
    #[test]
    fn sync_root_carries_subscriber_id() {
        let toml_str = r#"
nexus_url = "https://nexus.example.com"
subscriber_id = "sub-vault"
vaults_root = "/Users/test/Vaults"
daemon_version = "0.4.0"
daemon_platform = "macos-aarch64"

[[sync_roots]]
path = "/Users/test/Vaults/Mainframe"
route = ""
subscriber_id = "sub-dev"
"#;
        let cfg = Config::from_toml_back_compat(toml_str)
            .expect("[[sync_roots]] with subscriber_id must parse");
        assert_eq!(cfg.sync_roots.len(), 1);
        assert_eq!(
            cfg.sync_roots[0].subscriber_id, "sub-dev",
            "subscriber_id from [[sync_roots]] block must be preserved"
        );
    }

    /// When a `[[sync_roots]]` block OMITS `subscriber_id`, the field must
    /// default to `""` (to be filled at pairing).
    #[test]
    fn empty_sync_root_subscriber_defaults_blank() {
        let toml_str = r#"
nexus_url = "https://nexus.example.com"
subscriber_id = "sub-vault"
vaults_root = "/Users/test/Vaults"
daemon_version = "0.4.0"
daemon_platform = "macos-aarch64"

[[sync_roots]]
path = "/Users/test/Vaults/Dev"
route = "dev"
"#;
        let cfg = Config::from_toml_back_compat(toml_str)
            .expect("[[sync_roots]] without subscriber_id must parse");
        assert_eq!(cfg.sync_roots.len(), 1);
        assert_eq!(
            cfg.sync_roots[0].subscriber_id, "",
            "subscriber_id must default to empty string when omitted"
        );
    }

    /// Legacy on-disk config (no `[[sync_roots]]` block): the synthesised
    /// `SyncRoot` must inherit the top-level `subscriber_id` so existing
    /// installs keep pushing under the same subscriber they always have.
    #[test]
    fn back_compat_assigns_top_level_subscriber_id_to_vault_root() {
        let toml_str = r#"
nexus_url = "https://nexus.example.com"
subscriber_id = "sub-legacy-123"
vaults_root = "/Users/test/Vaults"
vault_name = "Mainframe"
daemon_version = "0.3.8"
daemon_platform = "macos-aarch64"
"#;
        let cfg = Config::from_toml_back_compat(toml_str)
            .expect("legacy back-compat must synthesise sync_root with subscriber_id");
        assert_eq!(
            cfg.sync_roots.len(),
            1,
            "expected exactly 1 synthesised sync_root"
        );
        assert_eq!(
            cfg.sync_roots[0].subscriber_id, "sub-legacy-123",
            "synthesised vault sync_root must carry the top-level subscriber_id"
        );
    }
}
