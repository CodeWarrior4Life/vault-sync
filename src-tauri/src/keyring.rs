use keyring::Entry;
use std::sync::atomic::{AtomicU64, Ordering};
use thiserror::Error;

const SERVICE: &str = "nexus-vault-sync";

/// Per-process monotonic counter so each `preflight()` call uses a unique
/// probe key. Avoids parallel-test races on macOS keychain where concurrent
/// `set_password()` writes to the same key serialise on app-approval and
/// time out under headless CI (S471 substrate finding).
static PREFLIGHT_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Error)]
pub enum KeyringError {
    #[error("keyring backend unavailable: {0}")]
    Unavailable(String),
    #[error("keyring operation failed: {0}")]
    OperationFailed(#[from] keyring::Error),
}

fn entry(subscriber_id: &str) -> Result<Entry, KeyringError> {
    Entry::new(SERVICE, &format!("bearer.{subscriber_id}")).map_err(KeyringError::from)
}

pub fn set_token(subscriber_id: &str, token: &str) -> Result<(), KeyringError> {
    entry(subscriber_id)?.set_password(token)?;
    Ok(())
}

pub fn get_token(subscriber_id: &str) -> Result<Option<String>, KeyringError> {
    match entry(subscriber_id)?.get_password() {
        Ok(t) => Ok(Some(t)),
        Err(keyring::Error::NoEntry) => Ok(None),
        Err(e) => Err(KeyringError::from(e)),
    }
}

pub fn delete_token(subscriber_id: &str) -> Result<(), KeyringError> {
    match entry(subscriber_id)?.delete_credential() {
        Ok(()) => Ok(()),
        Err(keyring::Error::NoEntry) => Ok(()),
        Err(e) => Err(KeyringError::from(e)),
    }
}

/// C1: Pre-flight check — verifies keyring backend is functional. On Linux
/// without libsecret/Secret Service, returns an error with actionable guidance
/// for the pairing wizard.
pub fn preflight() -> Result<(), KeyringError> {
    let n = PREFLIGHT_COUNTER.fetch_add(1, Ordering::Relaxed);
    let probe_owned = format!("preflight.probe.{}.{}", std::process::id(), n);
    let probe = probe_owned.as_str();
    let value = "ok";
    let e = entry(probe)?;
    e.set_password(value).map_err(|err| {
        #[cfg(target_os = "linux")]
        {
            KeyringError::Unavailable(format!(
                "Linux Secret Service not available. Install with: sudo apt install libsecret-1-dev gnome-keyring, \
                 then ensure your desktop session has gnome-keyring-daemon running. Underlying error: {err}"
            ))
        }
        #[cfg(not(target_os = "linux"))]
        {
            KeyringError::from(err)
        }
    })?;
    let _ = e.delete_credential();
    Ok(())
}
