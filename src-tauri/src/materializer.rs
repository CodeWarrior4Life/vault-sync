use crate::api_client::NotePayload;
use crate::rasp_fence::is_substrate_path;
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
    #[error("RASP substrate path refused (read-only by daemon): {0}")]
    SubstrateRefuse(String),
    #[error("materializer_mode=live not yet implemented in E2 (F adds atomic-write to live tree)")]
    NotYetImplemented,
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("sha mismatch: expected {expected}, got {actual}")]
    ShaMismatch { expected: String, actual: String },
}

/// v0.2.0: materializer holds `vaults_root` (parent dir, e.g. `D:\Vaults`)
/// and the specific `vault_name` it writes to (e.g. `"Mainframe"`). Target
/// path for an event with vault-relative `payload.path` becomes
/// `<vaults_root>/<vault_name>/<shadow_subdir>/<payload.path>`. Per-vault
/// boundary is enforced via canonical-path check against
/// `<vaults_root>/<vault_name>`.
pub struct Materializer {
    vaults_root: PathBuf,
    vault_name: String,
    shadow_subdir: String,
    mode: MaterializerMode,
}

impl Materializer {
    pub fn new(
        vaults_root: PathBuf,
        vault_name: String,
        shadow_path: Option<String>,
        mode: MaterializerMode,
    ) -> Self {
        let shadow_subdir = shadow_path.unwrap_or_else(|| ".lattice-sync/shadow/".to_string());
        Self {
            vaults_root,
            vault_name,
            shadow_subdir,
            mode,
        }
    }

    /// `<vaults_root>/<vault_name>` — the per-vault tree this materializer writes within.
    fn vault_dir(&self) -> PathBuf {
        self.vaults_root.join(&self.vault_name)
    }

    pub fn write(&self, payload: &NotePayload) -> Result<(), MaterializerError> {
        match self.mode {
            MaterializerMode::Live => return Err(MaterializerError::NotYetImplemented),
            MaterializerMode::Disabled => {
                info!(
                    "materializer_mode=disabled; skipping write for {}",
                    payload.path
                );
                return Ok(());
            }
            MaterializerMode::Shadow => {}
        }
        if !is_safe_path(&payload.path) {
            return Err(MaterializerError::PathTraversal(payload.path.clone()));
        }
        // v0.2.0: RASP substrate fence — refuse to materialize any path that
        // matches a substrate-layer pattern (00_VAULT.md / Family.md /
        // Mission.md / 02_Projects/Protocols/** / _project/** /
        // _rapport/people/**). Logged + Err, no file written.
        if is_substrate_path(&payload.path) {
            return Err(MaterializerError::SubstrateRefuse(payload.path.clone()));
        }
        let target = self
            .vault_dir()
            .join(&self.shadow_subdir)
            .join(&payload.path);
        // Path-traversal final canonicalization check
        let vault_dir = self.vault_dir();
        let canonical_vault = vault_dir.canonicalize().unwrap_or(vault_dir);
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent)?;
            let canonical_parent = parent
                .canonicalize()
                .unwrap_or_else(|_| parent.to_path_buf());
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
        if is_substrate_path(path) {
            return Err(MaterializerError::SubstrateRefuse(path.into()));
        }
        if matches!(self.mode, MaterializerMode::Disabled) {
            info!("materializer disabled; skipping delete for {}", path);
            return Ok(());
        }
        let target = self.vault_dir().join(&self.shadow_subdir).join(path);
        if !target.exists() {
            info!("soft_delete: nothing to delete at {}", path);
            return Ok(());
        }
        let ts = chrono::Utc::now().format("%Y%m%dT%H%M%SZ");
        let renamed = target.with_file_name(format!(
            "{}.deleted-{ts}",
            target.file_name().unwrap().to_string_lossy()
        ));
        fs::rename(&target, &renamed)?;
        info!(from = %target.display(), to = %renamed.display(), "soft_delete done");
        Ok(())
    }
}

fn serialize_with_frontmatter(payload: &NotePayload) -> String {
    let fm_yaml = serde_yaml::to_string(&payload.frontmatter).unwrap_or_default();
    format!("---\n{fm_yaml}---\n\n{}", payload.body)
}
