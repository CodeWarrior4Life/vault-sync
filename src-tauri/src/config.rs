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
    /// P0 2026-08-05 mis-root guard: legacy root synthesis (rule 2b) was
    /// about to root the daemon at `vaults_root` ITSELF, but that directory
    /// visibly contains a vault folder — syncing the parent tree would
    /// mis-key every path, orphan the shadow baseline, and echo-loop against
    /// the server. Fail closed and make the operator pin the root.
    #[error(
        "config is mis-rooted (refusing to sync — fail closed): the config has no `vault_name` \
         and no `[[sync_roots]]`, so the daemon would sync vaults_root `{root}` itself, but that \
         directory contains the vault folder `{child}`. Rooting at the parent mis-keys every \
         sync path (P0 2026-08-05 echo loop). Pin the root explicitly in config.toml:\n\n\
         [[sync_roots]]\npath = \"{root}/{child}\"\nroute = \"\"\n\n\
         (or restore `vault_name = \"{child}\"`), then restart the daemon."
    )]
    MisRooted { root: String, child: String },
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
    /// Legacy `vault_name` field (v0.2.0 – v0.3.6) is NOT modelled here —
    /// it is parsed by `RawConfig` and consumed by `from_toml_back_compat`
    /// to synthesise `sync_roots`. Because `Config` cannot represent it,
    /// serializing a `Config` over a user's config file DROPS `vault_name`
    /// (and any other unmodelled key) — the root cause of the P0 2026-08-05
    /// echo loop. Rewrites of the on-disk file must therefore go through
    /// `apply_enrollment` (untyped `toml::Table` merge), never `save_to`.
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
    ///       `SyncRoot { path: vaults_root, route: "" }` — UNLESS
    ///       `vaults_root` visibly contains a vault child directory (one
    ///       holding `.obsidian/`, or one named after the bare vault name
    ///       when the config carries it), in which case return
    ///       `ConfigError::MisRooted` instead of silently syncing the whole
    ///       parent tree (P0 2026-08-05 fail-closed guard).
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
                _ => {
                    // Rule 2b would root the daemon at vaults_root ITSELF.
                    // If vaults_root demonstrably contains a vault child,
                    // that is the mis-rooted shape that caused the
                    // 2026-08-05 P0 (vault_name dropped on re-enroll) —
                    // refuse instead of silently syncing the parent tree.
                    if let Some(child) =
                        find_vault_child_of(&raw.vaults_root, raw.vault_name.as_deref())
                    {
                        return Err(ConfigError::MisRooted {
                            root: raw.vaults_root.display().to_string(),
                            child,
                        });
                    }
                    raw.vaults_root.clone()
                }
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

    /// Serialize ONLY the typed fields of `Config` to `path`.
    ///
    /// WARNING (P0 2026-08-05): this drops every key `Config` does not
    /// model — `vault_name`, unknown/future fields. Never use it to rewrite
    /// a user's existing on-disk config; enrollment goes through
    /// `apply_enrollment`, which merges into the existing file.
    pub fn save_to(&self, path: &Path) -> Result<(), ConfigError> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let s = toml::to_string_pretty(self)?;
        fs::write(path, s)?;
        Ok(())
    }
}

/// P0 2026-08-05 mis-root guard helper: when legacy root synthesis (rule
/// 2b) is about to root the daemon at `vaults_root` ITSELF, look for
/// evidence that `vaults_root` is actually the PARENT of a vault — a child
/// directory that (a) contains `.obsidian/`, or (b) is named exactly after
/// the server's bare vault name when the config carries one. Returns the
/// first matching child name (sorted, for deterministic errors).
///
/// A non-existent or unreadable `vaults_root` yields `None`: fresh installs
/// pair before the directory exists, and the guard must not brick them —
/// it only fires on positive evidence of mis-rooting.
fn find_vault_child_of(vaults_root: &Path, bare_vault_name: Option<&str>) -> Option<String> {
    let entries = fs::read_dir(vaults_root).ok()?;
    let mut matches: Vec<String> = entries
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir())
        .filter_map(|e| {
            let name = e.file_name().to_string_lossy().into_owned();
            let named_after_vault = bare_vault_name.is_some_and(|v| !v.is_empty() && name == v);
            if named_after_vault || e.path().join(".obsidian").is_dir() {
                Some(name)
            } else {
                None
            }
        })
        .collect();
    matches.sort();
    matches.into_iter().next()
}

/// The fields the enrollment (pairing) flow OWNS on the on-disk config.
///
/// Invariant (P0 2026-08-05): enrollment may only ADD or UPDATE the fields
/// listed here (plus the `last_event_id` cursor it also owns); it must never
/// remove or alter anything else in the file — in particular `vault_name`,
/// `[[sync_roots]]`, and any unknown/future keys must survive a re-pair.
#[derive(Debug, Clone)]
pub struct EnrollmentFields {
    pub nexus_url: String,
    pub subscriber_id: String,
    pub vaults_root: PathBuf,
    pub daemon_version: String,
    pub daemon_platform: String,
}

/// Merge the enrollment-owned fields into the on-disk config file,
/// preserving every other key verbatim (`vault_name`, `[[sync_roots]]`,
/// unknown/future fields).
///
/// The previous implementation constructed a typed `Config` and serialized
/// it over the file. `Config` has no `vault_name` field, so every
/// enroll/re-enroll silently dropped it; root-synthesis rule 2b then rooted
/// the daemon at `vaults_root` itself, mis-keying every path (P0 2026-08-05
/// echo loop). This path goes through an untyped `toml::Table` instead, so
/// the save is structurally incapable of touching keys enrollment does not
/// own.
pub fn apply_enrollment(path: &Path, fields: &EnrollmentFields) -> Result<(), ConfigError> {
    let mut table: toml::Table = match fs::read_to_string(path) {
        Ok(s) => toml::from_str(&s)?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => toml::Table::new(),
        Err(e) => return Err(ConfigError::Io(e)),
    };

    // Re-enrolling under a DIFFERENT subscriber invalidates the (legacy,
    // in-config) SSE cursor; a same-subscriber re-pair keeps it so resume
    // semantics are unchanged. The live cursor is the per-subscriber
    // `last_event_id` sidecar file (lib.rs), which re-scopes itself by
    // subscriber_id automatically.
    let same_subscriber =
        table.get("subscriber_id").and_then(|v| v.as_str()) == Some(fields.subscriber_id.as_str());
    if !same_subscriber {
        table.remove("last_event_id");
    }

    // `vaults_root` supersedes the legacy `vault_root` alias; leaving both
    // keys in the file would make the next load fail as a duplicate field.
    table.remove("vault_root");

    table.insert("nexus_url".into(), fields.nexus_url.clone().into());
    table.insert("subscriber_id".into(), fields.subscriber_id.clone().into());
    table.insert(
        "vaults_root".into(),
        fields.vaults_root.to_string_lossy().into_owned().into(),
    );
    table.insert(
        "daemon_version".into(),
        fields.daemon_version.clone().into(),
    );
    table.insert(
        "daemon_platform".into(),
        fields.daemon_platform.clone().into(),
    );

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let s = toml::to_string_pretty(&table)?;
    fs::write(path, s)?;
    Ok(())
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

    // --- P0 2026-08-05: enrollment round-trip ---

    /// The core round-trip invariant: a config carrying `vault_name` plus an
    /// unknown/future key survives an enrollment save byte-meaningfully —
    /// vault_name still present, unknown key still present, only the
    /// enrollment-owned fields updated.
    #[test]
    fn apply_enrollment_preserves_vault_name_and_unknown_keys() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("config.toml");
        fs::write(
            &path,
            r#"
nexus_url = "https://stale.example.com"
subscriber_id = "sub-old"
vaults_root = "/Users/test/Vaults"
vault_name = "Mainframe"
daemon_version = "0.4.32"
daemon_platform = "linux-x86_64"
future_unknown_key = "must-survive"
"#,
        )
        .unwrap();

        apply_enrollment(
            &path,
            &EnrollmentFields {
                nexus_url: "https://new.example.com".into(),
                subscriber_id: "sub-new".into(),
                vaults_root: PathBuf::from("/Users/test/Vaults"),
                daemon_version: "0.4.37".into(),
                daemon_platform: "linux-x86_64".into(),
            },
        )
        .unwrap();

        let saved = fs::read_to_string(&path).unwrap();
        let table: toml::Table = toml::from_str(&saved).unwrap();
        assert_eq!(
            table.get("vault_name").and_then(|v| v.as_str()),
            Some("Mainframe"),
            "vault_name dropped by enrollment save; got:\n{saved}"
        );
        assert_eq!(
            table.get("future_unknown_key").and_then(|v| v.as_str()),
            Some("must-survive"),
            "unknown key dropped by enrollment save; got:\n{saved}"
        );
        assert_eq!(
            table.get("nexus_url").and_then(|v| v.as_str()),
            Some("https://new.example.com")
        );
        assert_eq!(
            table.get("subscriber_id").and_then(|v| v.as_str()),
            Some("sub-new")
        );
        assert_eq!(
            table.get("daemon_version").and_then(|v| v.as_str()),
            Some("0.4.37")
        );

        // And the daemon parse still roots at vaults_root/vault_name (2a).
        let cfg = Config::from_toml_back_compat(&saved).expect("saved config must parse");
        assert!(
            cfg.sync_roots[0].path.ends_with("Mainframe"),
            "post-enrollment root must still be the vault, got {:?}",
            cfg.sync_roots[0].path
        );
    }

    /// First pair on a fresh machine: no existing file — enrollment writes a
    /// loadable config from scratch.
    #[test]
    fn apply_enrollment_creates_fresh_config() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("nested").join("config.toml");
        apply_enrollment(
            &path,
            &EnrollmentFields {
                nexus_url: "https://nexus.example.com".into(),
                subscriber_id: "sub-fresh".into(),
                vaults_root: PathBuf::from("/Users/test/Vaults"),
                daemon_version: "0.4.37".into(),
                daemon_platform: "macos-aarch64".into(),
            },
        )
        .unwrap();
        let cfg = Config::load_from(&path).expect("fresh enrollment config must load");
        assert_eq!(cfg.subscriber_id, "sub-fresh");
        assert_eq!(cfg.vaults_root, PathBuf::from("/Users/test/Vaults"));
    }

    /// Same-subscriber re-pair keeps the legacy in-config cursor; enrolling
    /// under a different subscriber clears it (stale cursor for a different
    /// identity).
    #[test]
    fn apply_enrollment_last_event_id_scoped_to_subscriber() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("config.toml");
        let base = r#"
nexus_url = "https://nexus.example.com"
subscriber_id = "sub-same"
vaults_root = "/v"
vault_name = "Mainframe"
daemon_version = "0.4.32"
daemon_platform = "linux-x86_64"
last_event_id = "evt-99"
"#;
        let fields = |sub: &str| EnrollmentFields {
            nexus_url: "https://nexus.example.com".into(),
            subscriber_id: sub.into(),
            vaults_root: PathBuf::from("/v"),
            daemon_version: "0.4.37".into(),
            daemon_platform: "linux-x86_64".into(),
        };

        fs::write(&path, base).unwrap();
        apply_enrollment(&path, &fields("sub-same")).unwrap();
        let table: toml::Table = toml::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(
            table.get("last_event_id").and_then(|v| v.as_str()),
            Some("evt-99"),
            "same-subscriber re-pair must keep the cursor"
        );

        fs::write(&path, base).unwrap();
        apply_enrollment(&path, &fields("sub-DIFFERENT")).unwrap();
        let table: toml::Table = toml::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        assert!(
            table.get("last_event_id").is_none(),
            "subscriber change must clear the stale cursor"
        );
    }

    /// A legacy file using the `vault_root` alias must not end up with BOTH
    /// keys after enrollment (serde would reject the duplicate field).
    #[test]
    fn apply_enrollment_migrates_legacy_vault_root_alias() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("config.toml");
        fs::write(
            &path,
            r#"
nexus_url = "https://nexus.example.com"
subscriber_id = "sub-legacy"
vault_root = "/old/Vaults"
vault_name = "Mainframe"
daemon_version = "0.1.9"
daemon_platform = "linux-x86_64"
"#,
        )
        .unwrap();
        apply_enrollment(
            &path,
            &EnrollmentFields {
                nexus_url: "https://nexus.example.com".into(),
                subscriber_id: "sub-legacy".into(),
                vaults_root: PathBuf::from("/new/Vaults"),
                daemon_version: "0.4.37".into(),
                daemon_platform: "linux-x86_64".into(),
            },
        )
        .unwrap();
        let saved = fs::read_to_string(&path).unwrap();
        let table: toml::Table = toml::from_str(&saved).unwrap();
        assert!(table.get("vault_root").is_none(), "alias must be migrated");
        assert_eq!(
            table.get("vaults_root").and_then(|v| v.as_str()),
            Some("/new/Vaults")
        );
        assert_eq!(
            table.get("vault_name").and_then(|v| v.as_str()),
            Some("Mainframe")
        );
        Config::load_from(&path).expect("migrated config must load");
    }

    // --- P0 2026-08-05: mis-root refusal guard (rule 2b) ---

    /// vaults_root containing a `Mainframe/.obsidian` child + no vault_name
    /// ⇒ from_toml_back_compat must fail closed instead of rooting the
    /// daemon at the parent tree.
    #[test]
    fn misroot_guard_refuses_rule_2b_when_vault_child_present() {
        let dir = tempfile::TempDir::new().unwrap();
        fs::create_dir_all(dir.path().join("Mainframe").join(".obsidian")).unwrap();
        let toml_str = format!(
            r#"
nexus_url = "https://nexus.example.com"
subscriber_id = "sub-p0"
vaults_root = '{}'
daemon_version = "0.4.37"
daemon_platform = "linux-x86_64"
"#,
            dir.path().display()
        );
        let err = Config::from_toml_back_compat(&toml_str)
            .expect_err("must refuse to root at a vaults_root containing a vault child");
        match &err {
            ConfigError::MisRooted { child, .. } => {
                assert_eq!(child, "Mainframe", "guard must name the vault child")
            }
            other => panic!("expected MisRooted, got: {other:?}"),
        }
        let msg = err.to_string();
        assert!(
            msg.contains("[[sync_roots]]") && msg.contains("Mainframe"),
            "error must be actionable (mention [[sync_roots]] + the child); got: {msg}"
        );
    }

    /// A child directory WITHOUT `.obsidian/` is not vault evidence — rule
    /// 2b still roots at vaults_root itself (existing behavior preserved).
    #[test]
    fn misroot_guard_ignores_non_vault_children() {
        let dir = tempfile::TempDir::new().unwrap();
        fs::create_dir_all(dir.path().join("just-some-folder")).unwrap();
        let toml_str = format!(
            r#"
nexus_url = "https://nexus.example.com"
subscriber_id = "sub-ok"
vaults_root = '{}'
daemon_version = "0.4.37"
daemon_platform = "linux-x86_64"
"#,
            dir.path().display()
        );
        let cfg = Config::from_toml_back_compat(&toml_str)
            .expect("non-vault children must not trip the guard");
        assert_eq!(cfg.sync_roots[0].path, dir.path().to_path_buf());
    }

    /// vault_name present ⇒ rule 2a applies and the guard is never
    /// consulted, even with a real vault child on disk (synthesis works).
    #[test]
    fn misroot_guard_not_consulted_when_vault_name_present() {
        let dir = tempfile::TempDir::new().unwrap();
        fs::create_dir_all(dir.path().join("Mainframe").join(".obsidian")).unwrap();
        let toml_str = format!(
            r#"
nexus_url = "https://nexus.example.com"
subscriber_id = "sub-2a"
vaults_root = '{}'
vault_name = "Mainframe"
daemon_version = "0.4.37"
daemon_platform = "linux-x86_64"
"#,
            dir.path().display()
        );
        let cfg = Config::from_toml_back_compat(&toml_str)
            .expect("vault_name synthesis must keep working");
        assert_eq!(cfg.sync_roots[0].path, dir.path().join("Mainframe"));
    }

    /// Explicit [[sync_roots]] ⇒ rule 1 applies and the guard is never
    /// consulted, even with a real vault child on disk (existing behavior
    /// preserved — the operator's pinned roots always win).
    #[test]
    fn misroot_guard_not_consulted_with_explicit_sync_roots() {
        let dir = tempfile::TempDir::new().unwrap();
        fs::create_dir_all(dir.path().join("Mainframe").join(".obsidian")).unwrap();
        let toml_str = format!(
            r#"
nexus_url = "https://nexus.example.com"
subscriber_id = "sub-pinned"
vaults_root = '{root}'
daemon_version = "0.4.37"
daemon_platform = "linux-x86_64"

[[sync_roots]]
path = '{root}/Mainframe'
route = ""
"#,
            root = dir.path().display()
        );
        let cfg = Config::from_toml_back_compat(&toml_str)
            .expect("explicit [[sync_roots]] must parse unchanged");
        assert_eq!(cfg.sync_roots.len(), 1);
        assert!(cfg.sync_roots[0].path.ends_with("Mainframe"));
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
