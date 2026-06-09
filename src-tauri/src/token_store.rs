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
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};
use thiserror::Error;
use tracing::{debug, warn};

/// Process-level token cache, keyed by subscriber_id.
///
/// On macOS an unsigned / ad-hoc-signed binary triggers a Keychain access
/// prompt on EVERY `keyring::get_token`. The daemon loads the token from
/// several independent startup paths (SSE consumer, push pipeline, reconciler /
/// status reporter), so without a cache the user is prompted ONCE PER LOAD —
/// the "enter the password 3 times at every startup" report. Caching the first
/// successful load means the keyring backend is hit AT MOST ONCE per subscriber
/// per process, so the user is prompted at most once (and with a signed release
/// + "Always Allow", zero times after the initial grant).
fn token_cache() -> &'static Mutex<HashMap<String, String>> {
    static CACHE: OnceLock<Mutex<HashMap<String, String>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn cache_get(subscriber_id: &str) -> Option<String> {
    token_cache()
        .lock()
        .ok()
        .and_then(|m| m.get(subscriber_id).cloned())
}

fn cache_put(subscriber_id: &str, token: &str) {
    if let Ok(mut m) = token_cache().lock() {
        m.insert(subscriber_id.to_string(), token.to_string());
    }
}

fn cache_clear(subscriber_id: &str) {
    if let Ok(mut m) = token_cache().lock() {
        m.remove(subscriber_id);
    }
}

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
    // Keep the process cache in step with the new token regardless of backend,
    // so an in-process load() after a re-pair returns the fresh value.
    cache_put(subscriber_id, token);
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
    // Cache hit -> never touch the keyring (no repeat macOS Keychain prompt).
    if let Some(t) = cache_get(subscriber_id) {
        return Ok(Some(t));
    }
    match keyring::get_token(subscriber_id) {
        Ok(Some(t)) => {
            cache_put(subscriber_id, &t);
            return Ok(Some(t));
        }
        Ok(None) => debug!("keyring returned None; trying file fallback"),
        Err(e) => warn!("keyring read failed ({e}); trying file fallback"),
    }
    let path = token_file_path(subscriber_id);
    if path.exists() {
        let bytes = fs::read(&path)?;
        let token = String::from_utf8(bytes)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        let token = token.trim_end_matches('\n').to_string();
        cache_put(subscriber_id, &token);
        return Ok(Some(token));
    }
    Ok(None)
}

/// Remove a token from both backends. Best-effort on each; errors are logged
/// but not propagated (this is called from the Unpair flow, not the hot path).
pub fn delete(subscriber_id: &str) {
    cache_clear(subscriber_id);
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

#[cfg(test)]
mod tests {
    use super::{cache_clear, cache_get, cache_put};

    #[test]
    fn cache_put_then_get_returns_value() {
        // Unique key so the shared process cache can't collide across tests.
        let sub = "tok-cache-test-put-get";
        assert_eq!(cache_get(sub), None);
        cache_put(sub, "bearer-123");
        assert_eq!(cache_get(sub).as_deref(), Some("bearer-123"));
        cache_clear(sub);
        assert_eq!(cache_get(sub), None);
    }

    #[test]
    fn cache_put_overwrites_on_rotation() {
        let sub = "tok-cache-test-rotate";
        cache_put(sub, "old");
        cache_put(sub, "new");
        assert_eq!(cache_get(sub).as_deref(), Some("new"));
        cache_clear(sub);
    }
}
