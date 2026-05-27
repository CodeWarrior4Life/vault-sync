//! Detect + auto-disable Obsidian plugins that would conflict with the
//! vault-sync daemon (legacy lattice-sync, vault-sync-mobile, etc.) so two
//! sync engines don't fight over the same vault.
//!
//! We don't DELETE anything — just edit `<vault>/.obsidian/community-plugins.json`
//! to remove the plugin id from the enabled list. The plugin folder stays on
//! disk; Cyril can manually re-enable via Obsidian's settings if he really
//! wants both running.

use std::fs;
use std::path::Path;
use tracing::{info, warn};

/// Known plugin IDs that conflict with the vault-sync daemon. Add to this
/// list when we ship a new conflicting plugin or rename an existing one.
const CONFLICTING_PLUGIN_IDS: &[&str] = &[
    "lattice-sync",
    "vault-sync",
    "obsidian-nexus-sync",
    "nexus-vault-sync",
    "obsidian-vault-sync",
];

#[derive(Debug, Default)]
pub struct DetectResult {
    pub disabled: Vec<String>,
    pub already_disabled: Vec<String>,
    pub not_found: bool,
}

/// Scan the vault's `.obsidian/plugins/` dir + community-plugins.json. For
/// each conflicting plugin currently enabled, remove it from the enabled
/// list. Returns the list of plugins it disabled so the caller can surface
/// a tray notification.
pub fn detect_and_disable(vault_root: &Path) -> DetectResult {
    let mut result = DetectResult::default();
    let obsidian_dir = vault_root.join(".obsidian");
    if !obsidian_dir.exists() {
        result.not_found = true;
        return result;
    }
    let plugins_dir = obsidian_dir.join("plugins");
    let mut on_disk: Vec<String> = Vec::new();
    if plugins_dir.exists() {
        if let Ok(entries) = fs::read_dir(&plugins_dir) {
            for entry in entries.flatten() {
                if entry.path().is_dir() {
                    if let Some(name) = entry.file_name().to_str() {
                        on_disk.push(name.to_string());
                    }
                }
            }
        }
    }
    let community_plugins_path = obsidian_dir.join("community-plugins.json");
    let enabled: Vec<String> = if community_plugins_path.exists() {
        match fs::read_to_string(&community_plugins_path) {
            Ok(s) => serde_json::from_str(&s).unwrap_or_default(),
            Err(e) => {
                warn!("failed to read community-plugins.json: {e}");
                Vec::new()
            }
        }
    } else {
        Vec::new()
    };

    let mut to_disable: Vec<String> = Vec::new();
    for id in CONFLICTING_PLUGIN_IDS {
        if enabled.iter().any(|e| e == id) {
            to_disable.push((*id).to_string());
        } else if on_disk.iter().any(|d| d == id) {
            result.already_disabled.push((*id).to_string());
        }
    }
    if to_disable.is_empty() {
        return result;
    }

    let new_enabled: Vec<&String> = enabled.iter().filter(|e| !to_disable.contains(e)).collect();
    let serialized = match serde_json::to_string_pretty(&new_enabled) {
        Ok(s) => s,
        Err(e) => {
            warn!("failed to serialize new community-plugins.json: {e}");
            return result;
        }
    };
    if let Err(e) = fs::write(&community_plugins_path, serialized) {
        warn!("failed to write community-plugins.json: {e}");
        return result;
    }
    for id in &to_disable {
        info!("disabled conflicting Obsidian plugin: {id}");
    }
    result.disabled = to_disable;
    result
}

/// Convert into a one-line human-readable summary, suitable for tray
/// notification or log line.
pub fn summary_line(r: &DetectResult) -> Option<String> {
    if r.disabled.is_empty() {
        return None;
    }
    Some(format!(
        "Disabled conflicting Obsidian plugin{}: {}",
        if r.disabled.len() == 1 { "" } else { "s" },
        r.disabled.join(", ")
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn write_enabled(dir: &Path, enabled: &[&str]) {
        let obsidian = dir.join(".obsidian");
        fs::create_dir_all(&obsidian).unwrap();
        let j = serde_json::to_string_pretty(&enabled.to_vec()).unwrap();
        fs::write(obsidian.join("community-plugins.json"), j).unwrap();
    }

    fn read_enabled(dir: &Path) -> Vec<String> {
        let p = dir.join(".obsidian/community-plugins.json");
        let s = fs::read_to_string(p).unwrap();
        serde_json::from_str(&s).unwrap()
    }

    #[test]
    fn no_dot_obsidian_marks_not_found() {
        let tmp = TempDir::new().unwrap();
        let r = detect_and_disable(tmp.path());
        assert!(r.not_found);
        assert!(r.disabled.is_empty());
    }

    #[test]
    fn empty_enabled_list_yields_no_changes() {
        let tmp = TempDir::new().unwrap();
        write_enabled(tmp.path(), &[]);
        let r = detect_and_disable(tmp.path());
        assert!(!r.not_found);
        assert!(r.disabled.is_empty());
    }

    #[test]
    fn no_conflicting_plugins_means_no_changes() {
        let tmp = TempDir::new().unwrap();
        write_enabled(tmp.path(), &["dataview", "templater", "kepano-defuddle"]);
        let r = detect_and_disable(tmp.path());
        assert!(r.disabled.is_empty());
        let after = read_enabled(tmp.path());
        assert_eq!(after, vec!["dataview", "templater", "kepano-defuddle"]);
    }

    #[test]
    fn disables_conflicting_plugin_and_leaves_others() {
        let tmp = TempDir::new().unwrap();
        write_enabled(tmp.path(), &["dataview", "lattice-sync", "templater"]);
        let r = detect_and_disable(tmp.path());
        assert_eq!(r.disabled, vec!["lattice-sync"]);
        let after = read_enabled(tmp.path());
        assert_eq!(after, vec!["dataview", "templater"]);
    }

    #[test]
    fn disables_multiple_conflicting() {
        let tmp = TempDir::new().unwrap();
        write_enabled(
            tmp.path(),
            &["lattice-sync", "dataview", "vault-sync", "templater"],
        );
        let r = detect_and_disable(tmp.path());
        // order matches CONFLICTING_PLUGIN_IDS order
        assert!(r.disabled.contains(&"lattice-sync".to_string()));
        assert!(r.disabled.contains(&"vault-sync".to_string()));
        let after = read_enabled(tmp.path());
        assert_eq!(after, vec!["dataview", "templater"]);
    }

    #[test]
    fn summary_line_format() {
        let r = DetectResult {
            disabled: vec!["lattice-sync".into()],
            ..Default::default()
        };
        assert_eq!(
            summary_line(&r).unwrap(),
            "Disabled conflicting Obsidian plugin: lattice-sync"
        );
        let r = DetectResult {
            disabled: vec!["lattice-sync".into(), "vault-sync".into()],
            ..Default::default()
        };
        assert_eq!(
            summary_line(&r).unwrap(),
            "Disabled conflicting Obsidian plugins: lattice-sync, vault-sync"
        );
    }
}
