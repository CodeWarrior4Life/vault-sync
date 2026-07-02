//! Token persistence — FILE-FIRST (v0.4.29 control-plane P0).
//!
//! Read order: process cache → 0600 file at `<config_dir>/token-<sub>.bin`
//! → OS keyring (legacy fallback only). Write: the file is ALWAYS written;
//! the keyring write is best-effort defense-in-depth.
//!
//! WHY file-first (v0.4.29): after a tauri auto-update the NEW binary's
//! code signature is not in the macOS Keychain ACL for the existing item,
//! so the first `keyring::get_token` throws a blocking TouchID/password
//! prompt — fatal on an unattended host. Pre-0.4.29 code tried the keyring
//! FIRST on load and wrote the file ONLY when the keyring write failed, so
//! on a healthy Mac the token lived exclusively in the Keychain and every
//! auto-update re-armed the prompt. Now:
//! - `store()` writes the 0600 file unconditionally (keyring best-effort);
//! - `load()` reads the file before ever touching the keyring;
//! - a legacy keyring-only token is migrated to the file on first load
//!   (one last prompt at most, then never again);
//! - `VAULT_SYNC_TOKEN_BACKEND=file` skips the keyring entirely (both
//!   directions) for hosts where any Keychain touch is unacceptable.
//!
//! The 0600 file is NOT a downgrade attack vector — it is chmod'd 600 and
//! lives in the same user-scoped config dir the keyring crate already
//! trusts for its own metadata.
//!
//! Net effect: pairing wizard always succeeds, and a paired host survives
//! auto-updates with ZERO Keychain prompts.

use crate::config::default_config_path;
use crate::keyring;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use thiserror::Error;
use tracing::{debug, warn};

/// v0.4.29: opt-out of the OS keyring entirely. When set to `file`, `store()`
/// never writes the keyring and `load()` never reads it (a missing file is
/// simply `Ok(None)`, which drives re-pair). For hosts where ANY Keychain
/// touch is unacceptable (unattended Macs on auto-update).
pub const TOKEN_BACKEND_ENV: &str = "VAULT_SYNC_TOKEN_BACKEND";

/// Pure parser for [`TOKEN_BACKEND_ENV`] — split out so tests never mutate
/// process env (env mutation is racy under the parallel test runner).
fn backend_is_file_only(value: Option<&str>) -> bool {
    matches!(value.map(str::trim), Some(v) if v.eq_ignore_ascii_case("file"))
}

fn file_only_backend() -> bool {
    backend_is_file_only(std::env::var(TOKEN_BACKEND_ENV).ok().as_deref())
}

/// Thin seam over the OS keyring so unit tests can substitute a fake — a
/// real `keyring::Entry` touch from a test throws a macOS Keychain prompt
/// locally and hangs headless CI (see keyring.rs preflight S471 note).
pub(crate) trait KeyringOps {
    fn get(&self, subscriber_id: &str) -> Result<Option<String>, keyring::KeyringError>;
    fn set(&self, subscriber_id: &str, token: &str) -> Result<(), keyring::KeyringError>;
}

/// Production backend: delegates to the real keyring module.
struct OsKeyring;

impl KeyringOps for OsKeyring {
    fn get(&self, subscriber_id: &str) -> Result<Option<String>, keyring::KeyringError> {
        keyring::get_token(subscriber_id)
    }
    fn set(&self, subscriber_id: &str, token: &str) -> Result<(), keyring::KeyringError> {
        keyring::set_token(subscriber_id, token)
    }
}

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

/// Write the token file with 0600 perms (unix), creating parent dirs.
fn write_token_file(path: &Path, token: &str) -> Result<(), TokenStoreError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, token.as_bytes())?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(path)?.permissions();
        perms.set_mode(0o600);
        fs::set_permissions(path, perms)?;
    }
    Ok(())
}

/// Persist a token. v0.4.29: the 0600 file is ALWAYS written (it is the
/// backend `load()` trusts first — the only one that survives an auto-update
/// without a Keychain prompt); the keyring write is best-effort
/// defense-in-depth, skipped entirely under `VAULT_SYNC_TOKEN_BACKEND=file`.
/// Returns the backend(s) that persisted the token, for log/tray-status.
pub fn store(subscriber_id: &str, token: &str) -> Result<&'static str, TokenStoreError> {
    store_impl(
        subscriber_id,
        token,
        &token_file_path(subscriber_id),
        &OsKeyring,
        file_only_backend(),
    )
}

fn store_impl(
    subscriber_id: &str,
    token: &str,
    file_path: &Path,
    keyring: &dyn KeyringOps,
    file_only: bool,
) -> Result<&'static str, TokenStoreError> {
    // Keep the process cache in step with the new token regardless of backend,
    // so an in-process load() after a re-pair returns the fresh value.
    cache_put(subscriber_id, token);
    // The file write is UNCONDITIONAL and authoritative: if it fails, store()
    // fails (a keyring-only token would re-arm the auto-update prompt).
    write_token_file(file_path, token)?;
    if file_only {
        debug!("token persisted to file (keyring skipped: {TOKEN_BACKEND_ENV}=file)");
        return Ok("file");
    }
    match keyring.set(subscriber_id, token) {
        Ok(()) => {
            debug!("token persisted to file + OS keyring");
            Ok("file+keyring")
        }
        Err(e) => {
            warn!("OS keyring write failed ({e}); token persisted to file only");
            Ok("file")
        }
    }
}

/// Load a token. v0.4.29 order: cache → FILE → keyring. The keyring is a
/// legacy-only fallback (pre-0.4.29 installs stored keyring-only); when it
/// still holds the token, the migration shim writes the file so the NEXT
/// process never touches the keyring again. Under
/// `VAULT_SYNC_TOKEN_BACKEND=file` the keyring is never consulted — a
/// missing file is `Ok(None)`, which drives re-pair.
pub fn load(subscriber_id: &str) -> Result<Option<String>, TokenStoreError> {
    load_impl(
        subscriber_id,
        &token_file_path(subscriber_id),
        &OsKeyring,
        file_only_backend(),
    )
}

fn load_impl(
    subscriber_id: &str,
    file_path: &Path,
    keyring: &dyn KeyringOps,
    file_only: bool,
) -> Result<Option<String>, TokenStoreError> {
    // Cache hit -> never touch any backend (at most one backend touch per
    // subscriber per process).
    if let Some(t) = cache_get(subscriber_id) {
        return Ok(Some(t));
    }
    // FILE first: never triggers a Keychain prompt, survives auto-updates.
    if file_path.exists() {
        let bytes = fs::read(file_path)?;
        let token = String::from_utf8(bytes)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        let token = token.trim_end_matches('\n').to_string();
        cache_put(subscriber_id, &token);
        return Ok(Some(token));
    }
    if file_only {
        debug!("no token file and {TOKEN_BACKEND_ENV}=file — not consulting keyring");
        return Ok(None);
    }
    // Legacy fallback (pre-0.4.29 keyring-only installs). May prompt ONCE on
    // macOS after an auto-update; the migration shim below makes it the last
    // time this host ever touches the keyring for this subscriber.
    match keyring.get(subscriber_id) {
        Ok(Some(t)) => {
            cache_put(subscriber_id, &t);
            // Migration shim: persist to the file so the next process reads
            // file-first. Best-effort — a shim failure must not break a
            // successful load.
            match write_token_file(file_path, &t) {
                Ok(()) => debug!("migrated legacy keyring token to file backend"),
                Err(e) => warn!("keyring→file migration shim failed ({e}); load still Ok"),
            }
            Ok(Some(t))
        }
        Ok(None) => {
            debug!("no token in file or keyring");
            Ok(None)
        }
        Err(e) => {
            warn!("keyring read failed ({e}) and no token file; treating as no token");
            Ok(None)
        }
    }
}

/// v0.4.29 (Change 2): NON-PROMPTING paired check for the launch gate.
/// Consults ONLY the process cache and the token file — by construction it
/// cannot invoke the keyring, so it can never hang the main thread on a
/// macOS Keychain prompt. A legacy keyring-only token is invisible here
/// until the first `load()` migrates it to the file.
pub fn has_persisted_token(subscriber_id: &str) -> bool {
    has_persisted_token_at(subscriber_id, &token_file_path(subscriber_id))
}

fn has_persisted_token_at(subscriber_id: &str, file_path: &Path) -> bool {
    cache_get(subscriber_id).is_some() || file_path.exists()
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
    use super::{
        backend_is_file_only, cache_clear, cache_get, cache_put, has_persisted_token_at, load_impl,
        store_impl, KeyringOps,
    };
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Fake keyring backend: scripted token + failure mode, counts every call.
    /// Follows the keyring.rs Entry/preflight seam pattern — no OS keychain is
    /// ever touched from unit tests (a real touch would throw a macOS prompt
    /// in local runs and hang headless CI).
    struct FakeKeyring {
        token: Option<String>,
        fail_set: bool,
        gets: AtomicUsize,
        sets: AtomicUsize,
    }

    impl FakeKeyring {
        fn with_token(token: &str) -> Self {
            Self {
                token: Some(token.to_string()),
                fail_set: false,
                gets: AtomicUsize::new(0),
                sets: AtomicUsize::new(0),
            }
        }
        fn empty() -> Self {
            Self {
                token: None,
                fail_set: false,
                gets: AtomicUsize::new(0),
                sets: AtomicUsize::new(0),
            }
        }
        fn failing_set() -> Self {
            Self {
                token: None,
                fail_set: true,
                gets: AtomicUsize::new(0),
                sets: AtomicUsize::new(0),
            }
        }
    }

    impl KeyringOps for FakeKeyring {
        fn get(
            &self,
            _subscriber_id: &str,
        ) -> Result<Option<String>, crate::keyring::KeyringError> {
            self.gets.fetch_add(1, Ordering::SeqCst);
            Ok(self.token.clone())
        }
        fn set(
            &self,
            _subscriber_id: &str,
            _token: &str,
        ) -> Result<(), crate::keyring::KeyringError> {
            self.sets.fetch_add(1, Ordering::SeqCst);
            if self.fail_set {
                return Err(crate::keyring::KeyringError::Unavailable(
                    "fake keyring: set disabled".into(),
                ));
            }
            Ok(())
        }
    }

    /// Keyring that PANICS on any call — proves a code path never touches the
    /// keyring backend (the v0.4.29 no-prompt invariant).
    struct PanicKeyring;
    impl KeyringOps for PanicKeyring {
        fn get(&self, _s: &str) -> Result<Option<String>, crate::keyring::KeyringError> {
            panic!("keyring::get_token must not be called on this path");
        }
        fn set(&self, _s: &str, _t: &str) -> Result<(), crate::keyring::KeyringError> {
            panic!("keyring::set_token must not be called on this path");
        }
    }

    fn tmp_token_path(dir: &tempfile::TempDir, sub: &str) -> PathBuf {
        dir.path().join(format!("token-{sub}.bin"))
    }

    #[cfg(unix)]
    fn assert_mode_0600(path: &Path) {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "token file must be chmod 0600");
    }
    #[cfg(not(unix))]
    fn assert_mode_0600(_path: &Path) {}

    // ---------------------------------------------------------------------
    // v0.4.29 Change 1: store() ALWAYS writes the 0600 file
    // ---------------------------------------------------------------------

    /// The critical regression the old code carried: on a healthy Mac the
    /// keyring write succeeded so the file was NEVER written — after an
    /// auto-update the token was only reachable through a Keychain prompt.
    /// store must now write the file UNCONDITIONALLY.
    #[test]
    fn store_always_writes_file_even_when_keyring_succeeds() {
        let dir = tempfile::TempDir::new().unwrap();
        let sub = "tok-store-file-always";
        let path = tmp_token_path(&dir, sub);
        let kr = FakeKeyring::empty();
        let backend = store_impl(sub, "bearer-abc", &path, &kr, false).unwrap();
        assert!(
            path.exists(),
            "file must be written even on keyring success"
        );
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "bearer-abc");
        assert_mode_0600(&path);
        assert_eq!(
            kr.sets.load(Ordering::SeqCst),
            1,
            "keyring write stays best-effort"
        );
        assert_eq!(backend, "file+keyring");
        cache_clear(sub);
    }

    #[test]
    fn store_keyring_failure_is_nonfatal_file_still_written() {
        let dir = tempfile::TempDir::new().unwrap();
        let sub = "tok-store-keyring-fails";
        let path = tmp_token_path(&dir, sub);
        let kr = FakeKeyring::failing_set();
        let backend = store_impl(sub, "bearer-def", &path, &kr, false).unwrap();
        assert!(path.exists());
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "bearer-def");
        assert_eq!(backend, "file");
        cache_clear(sub);
    }

    /// VAULT_SYNC_TOKEN_BACKEND=file → the keyring is never touched on store.
    #[test]
    fn store_file_only_flag_skips_keyring_entirely() {
        let dir = tempfile::TempDir::new().unwrap();
        let sub = "tok-store-file-only";
        let path = tmp_token_path(&dir, sub);
        let backend = store_impl(sub, "bearer-ghi", &path, &PanicKeyring, true).unwrap();
        assert!(path.exists());
        assert_mode_0600(&path);
        assert_eq!(backend, "file");
        cache_clear(sub);
    }

    // ---------------------------------------------------------------------
    // v0.4.29 Change 1: load() order = cache → FILE → keyring
    // ---------------------------------------------------------------------

    /// File-first ordering: when the file exists the keyring must NEVER be
    /// consulted (PanicKeyring proves it). This is the post-auto-update
    /// no-prompt invariant.
    #[test]
    fn load_reads_file_first_without_touching_keyring() {
        let dir = tempfile::TempDir::new().unwrap();
        let sub = "tok-load-file-first";
        let path = tmp_token_path(&dir, sub);
        std::fs::write(&path, "bearer-file\n").unwrap();
        let got = load_impl(sub, &path, &PanicKeyring, false).unwrap();
        assert_eq!(
            got.as_deref(),
            Some("bearer-file"),
            "trailing newline trimmed"
        );
        cache_clear(sub);
    }

    /// Under the file-only flag a missing file is Ok(None) — drives re-pair —
    /// and the keyring must never be called.
    #[test]
    fn load_file_only_flag_missing_file_is_none_never_keyring() {
        let dir = tempfile::TempDir::new().unwrap();
        let sub = "tok-load-file-only-none";
        let path = tmp_token_path(&dir, sub);
        let got = load_impl(sub, &path, &PanicKeyring, true).unwrap();
        assert_eq!(got, None);
        cache_clear(sub);
    }

    /// Legacy-Mac migration shim: file absent + keyring holds the token →
    /// load returns it AND writes the 0600 file so the NEXT process never
    /// touches the keyring again.
    #[test]
    fn load_migrates_keyring_token_to_file() {
        let dir = tempfile::TempDir::new().unwrap();
        let sub = "tok-load-migrate";
        let path = tmp_token_path(&dir, sub);
        let kr = FakeKeyring::with_token("bearer-legacy");
        let got = load_impl(sub, &path, &kr, false).unwrap();
        assert_eq!(got.as_deref(), Some("bearer-legacy"));
        assert_eq!(kr.gets.load(Ordering::SeqCst), 1);
        assert!(
            path.exists(),
            "migration shim must write the file on first load"
        );
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "bearer-legacy");
        assert_mode_0600(&path);

        // Second load in a FRESH process (cache cleared): file wins, keyring
        // is never touched.
        cache_clear(sub);
        let again = load_impl(sub, &path, &PanicKeyring, false).unwrap();
        assert_eq!(again.as_deref(), Some("bearer-legacy"));
        cache_clear(sub);
    }

    #[test]
    fn load_no_file_no_keyring_entry_is_none() {
        let dir = tempfile::TempDir::new().unwrap();
        let sub = "tok-load-none";
        let path = tmp_token_path(&dir, sub);
        let kr = FakeKeyring::empty();
        assert_eq!(load_impl(sub, &path, &kr, false).unwrap(), None);
        assert_eq!(kr.gets.load(Ordering::SeqCst), 1);
        cache_clear(sub);
    }

    /// token_cache() semantics preserved: after one load the backends are
    /// never touched again in-process (at most one backend touch per
    /// subscriber per process).
    #[test]
    fn load_cache_hit_touches_no_backend() {
        let dir = tempfile::TempDir::new().unwrap();
        let sub = "tok-load-cache-hit";
        let path = tmp_token_path(&dir, sub);
        std::fs::write(&path, "bearer-cached").unwrap();
        assert_eq!(
            load_impl(sub, &path, &PanicKeyring, false)
                .unwrap()
                .as_deref(),
            Some("bearer-cached")
        );
        // Delete the file: only the cache can answer now — and the keyring
        // still must not be touched.
        std::fs::remove_file(&path).unwrap();
        assert_eq!(
            load_impl(sub, &path, &PanicKeyring, false)
                .unwrap()
                .as_deref(),
            Some("bearer-cached")
        );
        cache_clear(sub);
    }

    // ---------------------------------------------------------------------
    // v0.4.29 Change 2: has_persisted_token — cache/file only, NO keyring
    // ---------------------------------------------------------------------

    /// Structural no-prompt guarantee: has_persisted_token_at takes NO keyring
    /// backend at all — it cannot invoke the keyring by construction. These
    /// assertions pin the cache/file semantics.
    #[test]
    fn has_persisted_token_checks_file_and_cache_never_keyring() {
        let dir = tempfile::TempDir::new().unwrap();
        let sub = "tok-has-persisted";
        let path = tmp_token_path(&dir, sub);
        assert!(
            !has_persisted_token_at(sub, &path),
            "no cache, no file → false"
        );
        std::fs::write(&path, "bearer-x").unwrap();
        assert!(has_persisted_token_at(sub, &path), "file present → true");
        std::fs::remove_file(&path).unwrap();
        cache_put(sub, "bearer-x");
        assert!(has_persisted_token_at(sub, &path), "cache hit → true");
        cache_clear(sub);
    }

    // ---------------------------------------------------------------------
    // VAULT_SYNC_TOKEN_BACKEND parsing (pure — no env mutation in tests)
    // ---------------------------------------------------------------------

    #[test]
    fn backend_flag_parsing() {
        assert!(!backend_is_file_only(None));
        assert!(!backend_is_file_only(Some("")));
        assert!(!backend_is_file_only(Some("keyring")));
        assert!(backend_is_file_only(Some("file")));
        assert!(backend_is_file_only(Some("FILE")));
        assert!(backend_is_file_only(Some(" file ")));
    }

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
