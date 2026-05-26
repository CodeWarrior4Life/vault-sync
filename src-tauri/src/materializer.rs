use crate::api_client::NotePayload;
use crate::scope::is_safe_path;
use sha2::{Digest, Sha256};
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use tempfile::NamedTempFile;
use thiserror::Error;
use tracing::{info, warn};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaterializerMode {
    Shadow,
    Live,
    Disabled,
}

impl MaterializerMode {
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Self {
        match s {
            "live" => Self::Live,
            "disabled" => Self::Disabled,
            _ => Self::Shadow,
        }
    }
}

#[derive(Debug, Error)]
pub enum MaterializerError {
    #[error("path traversal rejected: {0}")]
    PathTraversal(String),
    #[error("materializer_mode=live not yet implemented in E2 (F adds atomic-write to live tree)")]
    NotYetImplemented,
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("sha mismatch: expected {expected}, got {actual}")]
    ShaMismatch { expected: String, actual: String },
}

pub struct Materializer {
    vault_root: PathBuf,
    shadow_subdir: String,
    mode: MaterializerMode,
}

impl Materializer {
    pub fn new(vault_root: PathBuf, shadow_path: Option<String>, mode: MaterializerMode) -> Self {
        let shadow_subdir = shadow_path.unwrap_or_else(|| ".lattice-sync/shadow/".to_string());
        Self { vault_root, shadow_subdir, mode }
    }

    pub fn write(&self, payload: &NotePayload) -> Result<(), MaterializerError> {
        match self.mode {
            MaterializerMode::Live => return Err(MaterializerError::NotYetImplemented),
            MaterializerMode::Disabled => {
                info!("materializer_mode=disabled; skipping write for {}", payload.path);
                return Ok(());
            }
            MaterializerMode::Shadow => {}
        }
        if !is_safe_path(&payload.path) {
            return Err(MaterializerError::PathTraversal(payload.path.clone()));
        }
        let target = self.vault_root.join(&self.shadow_subdir).join(&payload.path);
        // Path-traversal final canonicalization check
        let canonical_vault = self.vault_root.canonicalize().unwrap_or_else(|_| self.vault_root.clone());
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent)?;
            let canonical_parent = parent.canonicalize().unwrap_or_else(|_| parent.to_path_buf());
            if !canonical_parent.starts_with(&canonical_vault) {
                return Err(MaterializerError::PathTraversal(payload.path.clone()));
            }
        }
        let content = serialize_with_frontmatter(payload);
        let actual_sha = hex::encode(Sha256::digest(content.as_bytes()));
        // Atomic write
        let dir = target.parent().unwrap();
        let mut tmp = NamedTempFile::new_in(dir)?;
        tmp.write_all(content.as_bytes())?;
        tmp.flush()?;
        tmp.persist(&target).map_err(|e| e.error)?;
        // SHA verify (after write — soft check, logged not errored to match spec §6.6)
        if actual_sha != payload.sha256 {
            warn!(
                expected = %payload.sha256,
                actual = %actual_sha,
                path = %payload.path,
                "materializer SHA mismatch — file written but does not match server hash"
            );
        }
        Ok(())
    }

    pub fn soft_delete(&self, path: &str) -> Result<(), MaterializerError> {
        if !is_safe_path(path) {
            return Err(MaterializerError::PathTraversal(path.into()));
        }
        if matches!(self.mode, MaterializerMode::Disabled) {
            info!("materializer disabled; skipping delete for {}", path);
            return Ok(());
        }
        let target = self.vault_root.join(&self.shadow_subdir).join(path);
        if !target.exists() {
            info!("soft_delete: nothing to delete at {}", path);
            return Ok(());
        }
        let ts = chrono::Utc::now().format("%Y%m%dT%H%M%SZ");
        let renamed = target.with_file_name(
            format!("{}.deleted-{ts}", target.file_name().unwrap().to_string_lossy())
        );
        fs::rename(&target, &renamed)?;
        info!(from = %target.display(), to = %renamed.display(), "soft_delete done");
        Ok(())
    }
}

fn serialize_with_frontmatter(payload: &NotePayload) -> String {
    let fm_yaml = serde_yaml::to_string(&payload.frontmatter).unwrap_or_default();
    format!("---\n{fm_yaml}---\n\n{}", payload.body)
}
