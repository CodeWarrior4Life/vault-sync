//! Tauri command surface for wizard-driven vault discovery (S477 §3.2).

use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VaultFolderInfo {
    pub name: String,
    pub has_obsidian: bool,
}

/// Enumerate immediate subdirectories of `vaults_root`. For each, report its
/// folder name + whether it contains a `.obsidian/` subdirectory (Obsidian
/// vault marker). Hidden directories (leading dot) are excluded.
pub fn list_vault_folders_impl(vaults_root: &Path) -> Vec<VaultFolderInfo> {
    let mut out = Vec::new();
    let entries = match std::fs::read_dir(vaults_root) {
        Ok(e) => e,
        Err(_) => return out,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let name = match path.file_name().and_then(|n| n.to_str()) {
            Some(n) => n.to_string(),
            None => continue,
        };
        if name.starts_with('.') {
            continue;
        }
        let has_obsidian = path.join(".obsidian").is_dir();
        out.push(VaultFolderInfo { name, has_obsidian });
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn empty_root_returns_empty_list() {
        let tmp = TempDir::new().unwrap();
        let result = list_vault_folders_impl(tmp.path());
        assert_eq!(result, vec![]);
    }

    #[test]
    fn detects_obsidian_vaults_under_root() {
        let tmp = TempDir::new().unwrap();
        std::fs::create_dir_all(tmp.path().join("Mainframe/.obsidian")).unwrap();
        std::fs::create_dir_all(tmp.path().join("OtherVault/.obsidian")).unwrap();
        std::fs::create_dir_all(tmp.path().join("NotAVault")).unwrap();
        let result = list_vault_folders_impl(tmp.path());
        assert_eq!(result.len(), 3);
        assert_eq!(
            result[0],
            VaultFolderInfo {
                name: "Mainframe".into(),
                has_obsidian: true
            }
        );
        assert_eq!(
            result[1],
            VaultFolderInfo {
                name: "NotAVault".into(),
                has_obsidian: false
            }
        );
        assert_eq!(
            result[2],
            VaultFolderInfo {
                name: "OtherVault".into(),
                has_obsidian: true
            }
        );
    }

    #[test]
    fn excludes_dot_prefixed_dirs() {
        let tmp = TempDir::new().unwrap();
        std::fs::create_dir_all(tmp.path().join(".trash")).unwrap();
        std::fs::create_dir_all(tmp.path().join(".obsidian")).unwrap();
        std::fs::create_dir_all(tmp.path().join("Mainframe/.obsidian")).unwrap();
        let result = list_vault_folders_impl(tmp.path());
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "Mainframe");
    }
}
