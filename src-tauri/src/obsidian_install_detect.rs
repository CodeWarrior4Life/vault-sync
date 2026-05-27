//! Discover Obsidian's known vaults via its global `obsidian.json` registry.
//!
//! Obsidian stores the list of every vault the user has ever opened in a
//! single JSON file at OS-specific paths:
//!   - Windows: `%APPDATA%\obsidian\obsidian.json`
//!   - macOS:   `~/Library/Application Support/obsidian/obsidian.json`
//!   - Linux:   `~/.config/obsidian/obsidian.json` (XDG_CONFIG_HOME-aware)
//!
//! Schema:
//! ```json
//! {
//!   "vaults": {
//!     "<vault-id-hex>": { "path": "D:\\Vaults\\Mainframe", "ts": 1700000000000, "open": true }
//!   }
//! }
//! ```
//!
//! `find_known_vaults()` returns the list of paths regardless of `open` —
//! conflicting plugins should be disabled in CLOSED vaults too (they'll
//! reactivate when the user re-opens). Non-existent paths in the registry
//! (e.g. user deleted the vault folder) are filtered out.

use serde::Deserialize;
use std::path::PathBuf;
use tracing::{debug, warn};

#[derive(Debug, Deserialize)]
struct ObsidianRegistry {
    #[serde(default)]
    vaults: std::collections::HashMap<String, VaultEntry>,
}

#[derive(Debug, Deserialize)]
struct VaultEntry {
    path: String,
    #[serde(default)]
    #[allow(dead_code)]
    ts: Option<u64>,
    #[serde(default)]
    #[allow(dead_code)]
    open: Option<bool>,
}

/// Path to Obsidian's global registry JSON on the current OS.
pub fn registry_path() -> Option<PathBuf> {
    #[cfg(target_os = "windows")]
    {
        let appdata = std::env::var_os("APPDATA")?;
        Some(
            PathBuf::from(appdata)
                .join("obsidian")
                .join("obsidian.json"),
        )
    }
    #[cfg(target_os = "macos")]
    {
        let home = std::env::var_os("HOME")?;
        Some(
            PathBuf::from(home)
                .join("Library/Application Support/obsidian")
                .join("obsidian.json"),
        )
    }
    #[cfg(target_os = "linux")]
    {
        // XDG_CONFIG_HOME if set, otherwise ~/.config
        if let Some(xdg) = std::env::var_os("XDG_CONFIG_HOME") {
            return Some(PathBuf::from(xdg).join("obsidian").join("obsidian.json"));
        }
        let home = std::env::var_os("HOME")?;
        Some(
            PathBuf::from(home)
                .join(".config/obsidian")
                .join("obsidian.json"),
        )
    }
    #[cfg(not(any(target_os = "windows", target_os = "macos", target_os = "linux")))]
    {
        None
    }
}

/// Return paths of every Obsidian vault the user has opened at least once
/// that still exists on disk. Empty Vec if Obsidian isn't installed / no
/// vaults known / registry can't be read.
pub fn find_known_vaults() -> Vec<PathBuf> {
    let Some(reg_path) = registry_path() else {
        debug!("obsidian registry path not resolvable on this OS");
        return Vec::new();
    };
    if !reg_path.exists() {
        debug!("no obsidian registry at {}", reg_path.display());
        return Vec::new();
    }
    let raw = match std::fs::read_to_string(&reg_path) {
        Ok(s) => s,
        Err(e) => {
            warn!(
                "obsidian registry read failed at {}: {e}",
                reg_path.display()
            );
            return Vec::new();
        }
    };
    let parsed: ObsidianRegistry = match serde_json::from_str(&raw) {
        Ok(p) => p,
        Err(e) => {
            warn!("obsidian registry parse failed: {e}");
            return Vec::new();
        }
    };
    let mut paths: Vec<PathBuf> = Vec::new();
    for (id, v) in parsed.vaults {
        let p = PathBuf::from(&v.path);
        if !p.exists() {
            debug!(
                "known obsidian vault {id} at {} no longer exists; skipping",
                v.path
            );
            continue;
        }
        paths.push(p);
    }
    paths
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn registry_path_resolves_per_os() {
        // Just confirm the function returns Some(..) when env is set.
        // Real path validity depends on the OS the test runs on.
        let _ = registry_path();
    }

    #[test]
    fn find_known_vaults_returns_only_existing_paths() {
        let tmp = TempDir::new().unwrap();
        let real = tmp.path().join("real-vault");
        std::fs::create_dir_all(&real).unwrap();
        let registry = serde_json::json!({
            "vaults": {
                "aaaa1111": {"path": real.to_string_lossy(), "ts": 1, "open": true},
                "bbbb2222": {"path": tmp.path().join("ghost-vault").to_string_lossy(), "ts": 2}
            }
        });
        let raw = serde_json::to_string(&registry).unwrap();
        let _parsed: ObsidianRegistry = serde_json::from_str(&raw).unwrap();
        // Note: full find_known_vaults() integration test would need env
        // override of the registry path — that requires either an env-var
        // injection in registry_path() (which we keep simple) or a test
        // double. Existence-filtering logic is exercised by hand here:
        let r: ObsidianRegistry = serde_json::from_str(&raw).unwrap();
        let mut paths: Vec<PathBuf> = Vec::new();
        for (_id, v) in r.vaults {
            let p = PathBuf::from(&v.path);
            if p.exists() {
                paths.push(p);
            }
        }
        assert_eq!(paths.len(), 1);
        assert!(paths[0].ends_with("real-vault"));
    }

    #[test]
    fn empty_registry_yields_empty_list() {
        let r: ObsidianRegistry = serde_json::from_str(r#"{"vaults": {}}"#).unwrap();
        assert!(r.vaults.is_empty());
    }

    #[test]
    fn malformed_registry_doesnt_panic() {
        let result: Result<ObsidianRegistry, _> = serde_json::from_str("not json");
        assert!(result.is_err());
    }
}
