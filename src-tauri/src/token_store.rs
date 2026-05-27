//! Token persistence with graceful fallback.
//!
//! Order of preference:
//! 1. OS keyring (macOS Keychain / Windows Credential Manager / Linux Secret
//!    Service). Most secure; preferred when available.
//! 2. File fallback at `<config_dir>/token.bin`, mode 0600 on unix. Used
//!    automatically when the keyring backend is unavailable (headless
//!    Linux without gnome-keyring, SSH-session installs that can't unlock
//!    login.keychain, etc.). NOT a downgrade attack vector — the file is
//!    chmod'd 600 and lives in the same user-scoped config dir the keyring
//!    crate already trusts for its own metadata.
//!
//! Net effect: pairing wizard always succeeds. End user sees one paste-URL
//! + paste-token + click-Validate flow, regardless of OS keychain quirks.

use crate::config::default_config_path;
use crate::keyring;
use std::fs;
use std::path::PathBuf;
use thiserror::Error;
use tracing::{debug, warn};

#[derive(Debug, Error)]
pub enum TokenStoreError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("keyring error: {0}")]
    Keyring(#[from] keyring::KeyringError),
}

fn token_file_path(subscriber_id: &str) -> PathBuf {
    // Sibling of config.toml — same directory, so cleanup is one rm -rf.
    let mut p = default_config_path();
    p.set_file_name(format!("token-{subscriber_id}.bin"));
    p
}

/// Persist a token. Tries keyring first; on failure falls back to a 0600 file
/// next to config.toml. Returns the storage path actually used so the caller
/// can log / tray-status which backend won.
pub fn store(subscriber_id: &str, token: &str) -> Result<&'static str, TokenStoreError> {
    if keyring::set_token(subscriber_id, token).is_ok() {
        debug!("token persisted to OS keyring");
        return Ok("keyring");
    }
    warn!("OS keyring write failed; falling back to file at config dir");
    let path = token_file_path(subscriber_id);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&path, token.as_bytes())?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&path)?.permissions();
        perms.set_mode(0o600);
        fs::set_permissions(&path, perms)?;
    }
    Ok("file")
}

/// Load a token. Tries keyring first; falls back to the file. Returns None
/// only when both backends report the entry doesn't exist.
pub fn load(subscriber_id: &str) -> Result<Option<String>, TokenStoreError> {
    match keyring::get_token(subscriber_id) {
        Ok(Some(t)) => return Ok(Some(t)),
        Ok(None) => debug!("keyring returned None; trying file fallback"),
        Err(e) => warn!("keyring read failed ({e}); trying file fallback"),
    }
    let path = token_file_path(subscriber_id);
    if path.exists() {
        let bytes = fs::read(&path)?;
        let token = String::from_utf8(bytes)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        return Ok(Some(token.trim_end_matches('\n').to_string()));
    }
    Ok(None)
}

/// Remove a token from both backends. Best-effort on each; errors are logged
/// but not propagated (this is called from the Unpair flow, not the hot path).
pub fn delete(subscriber_id: &str) {
    if let Err(e) = keyring::delete_token(subscriber_id) {
        warn!("keyring delete failed for {subscriber_id}: {e}");
    }
    let path = token_file_path(subscriber_id);
    if path.exists() {
        if let Err(e) = fs::remove_file(&path) {
            warn!("token file delete failed for {subscriber_id}: {e}");
        }
    }
}
