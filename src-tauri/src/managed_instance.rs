//! Managed-instance mode (spec §5, "Memory Parity — Sync-Owned Architecture +
//! Tiered Guards", 2026-08-08 R7).
//!
//! When the `NEXUS_SYNC_CONFIG_DIR` environment variable is set, the daemon
//! runs as a MANAGED INSTANCE: an additional, unit-manager-owned copy of the
//! binary whose entire config surface (config.toml, token files, reconcile
//! retry ledger, daemon.lock) lives under that directory instead of the OS
//! default config dir. The vault instance (env unset) is byte-identical to
//! pre-0.4.38 behavior — nothing in this module may run for it.
//!
//! Invariants owned here:
//! - **Canonical config dir**: the env value is realpath-canonicalized
//!   (created first if absent, THEN canonicalized) before ANY use, so two
//!   spellings of one directory (symlink vs real path, trailing junk) can
//!   never yield two config paths or two lock files.
//! - **Fail-closed resolution**: if the env is set but the directory cannot
//!   be created or canonicalized, the daemon PANICS. It must never silently
//!   fall back to the vault instance's default config path — that would point
//!   a managed instance at the vault's config (the exact mis-target the mode
//!   exists to prevent).
//! - **Exclusive advisory lock**: one daemon per canonical config dir,
//!   enforced by a non-blocking exclusive lock on `<dir>/daemon.lock` held
//!   for the process lifetime. A second process exits non-zero with a
//!   distinct error message.
//! - **Plugin policy**: managed instances register NO single-instance plugin
//!   (R12 — the lock above replaces it, keyed by config dir instead of
//!   app identity), NO updater (the vault instance is the sole update leader
//!   for the shared binary), NO autostart (login-item registration is
//!   app-global), and never raise the wizard/GUI (implied `--silent`).

use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};

/// The environment variable that arms managed-instance mode.
pub const ENV_CONFIG_DIR: &str = "NEXUS_SYNC_CONFIG_DIR";

/// Basename of the per-config-dir exclusive lock file.
pub const LOCK_FILENAME: &str = "daemon.lock";

/// True iff this process runs in managed-instance mode (env set + non-empty).
pub fn is_managed_mode() -> bool {
    std::env::var_os(ENV_CONFIG_DIR).is_some_and(|v| !v.is_empty())
}

/// Resolve a RAW config-dir spelling to its canonical form: create the
/// directory if absent, THEN realpath-canonicalize it. Pure seam over the
/// filesystem (no env read) so tests can drive it with explicit inputs.
///
/// `dunce::canonicalize` is used so Windows does not grow a `\\?\` prefix
/// (no-op on non-Windows) — consistent with the daemon's other path handling.
pub fn resolve_config_dir(raw: &Path) -> io::Result<PathBuf> {
    std::fs::create_dir_all(raw)?;
    dunce::canonicalize(raw)
}

/// Managed-instance config dir from the environment, canonicalized.
///
/// - Env unset/empty → `None` (vault mode; the caller falls through to the
///   OS default path — pre-0.4.38 behavior, byte-identical).
/// - Env set but unresolvable → PANIC (fail closed; see module docs).
pub fn managed_config_dir() -> Option<PathBuf> {
    let raw = std::env::var_os(ENV_CONFIG_DIR)?;
    if raw.is_empty() {
        return None;
    }
    match resolve_config_dir(Path::new(&raw)) {
        Ok(dir) => Some(dir),
        Err(e) => panic!(
            "managed-instance mode: {ENV_CONFIG_DIR}={raw:?} could not be created/canonicalized \
             ({e}); refusing to fall back to the default config path"
        ),
    }
}

/// A held exclusive advisory lock on `<config-dir>/daemon.lock`. Keep this
/// alive for the process lifetime (the daemon `Box::leak`s it); dropping it
/// releases the lock.
#[derive(Debug)]
pub struct DaemonLock {
    /// The open, locked file. The OS lock is tied to this handle.
    _file: File,
    /// Where the lock lives (for logging).
    pub path: PathBuf,
}

/// Why the daemon lock could not be acquired.
#[derive(Debug, thiserror::Error)]
pub enum LockError {
    /// DISTINCT second-process refusal: another daemon holds the lock for
    /// this canonical config dir. The caller must exit non-zero.
    #[error("managed-instance lock held by another process for {dir}")]
    Held { dir: PathBuf },
    #[error("managed-instance lock at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
}

/// Acquire the NON-BLOCKING exclusive advisory lock on
/// `<canonical_config_dir>/daemon.lock`. `canonical_config_dir` MUST already
/// be the output of [`resolve_config_dir`] / [`managed_config_dir`] — locking
/// a non-canonical spelling would defeat the one-lock-per-dir invariant.
///
/// Uses std's file locking (flock(2) on Unix, LockFileEx on Windows — stable
/// since Rust 1.89), so no new dependency is required.
pub fn acquire_daemon_lock(canonical_config_dir: &Path) -> Result<DaemonLock, LockError> {
    let path = canonical_config_dir.join(LOCK_FILENAME);
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(&path)
        .map_err(|source| LockError::Io {
            path: path.clone(),
            source,
        })?;
    match file.try_lock() {
        Ok(()) => Ok(DaemonLock { _file: file, path }),
        Err(std::fs::TryLockError::WouldBlock) => Err(LockError::Held {
            dir: canonical_config_dir.to_path_buf(),
        }),
        Err(std::fs::TryLockError::Error(source)) => Err(LockError::Io { path, source }),
    }
}

/// What the Tauri builder registers, as a PURE function of the mode — the
/// compile-time seam the plugins-absent tests assert against. `run()` in
/// lib.rs consults exactly this (no other plugin gating exists).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PluginPolicy {
    /// `tauri_plugin_single_instance` (R12): vault mode only. Managed
    /// instances rely on the per-config-dir daemon.lock instead — the plugin
    /// is keyed app-globally and would wrongly couple the vault + managed
    /// instances of the same binary.
    pub single_instance: bool,
    /// `tauri_plugin_updater` + the spawn_updater_check task: vault mode
    /// only. The vault instance is the sole update leader for the shared
    /// binary; managed instances never self-update.
    pub updater: bool,
    /// `tauri_plugin_autostart`: vault mode only. Login-item registration is
    /// app-global; a managed instance's lifecycle belongs to the unit manager.
    pub autostart: bool,
    /// May the pairing wizard window ever be shown/raised? Managed mode
    /// implies `--silent` semantics: never.
    pub allow_wizard: bool,
}

/// The single source of truth for per-mode plugin registration.
pub fn plugin_policy(managed: bool) -> PluginPolicy {
    PluginPolicy {
        single_instance: !managed,
        updater: !managed,
        autostart: !managed,
        allow_wizard: !managed,
    }
}

#[cfg(test)]
pub(crate) mod test_env {
    //! Process-wide env-var serialization for tests. `std::env::set_var`
    //! mutates process state; any test touching `NEXUS_SYNC_CONFIG_DIR` MUST
    //! hold this mutex so parallel tests (in any module) can't observe a
    //! half-set env. Non-env tests never call `default_config_path()`-style
    //! env readers, so they need no guard.
    use std::sync::{Mutex, MutexGuard};

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    pub fn lock() -> MutexGuard<'static, ()> {
        ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// RAII: sets `NEXUS_SYNC_CONFIG_DIR` for the guard's lifetime, restores
    /// the previous state (set or unset) on drop.
    pub struct ScopedConfigDirEnv {
        _guard: MutexGuard<'static, ()>,
        prev: Option<std::ffi::OsString>,
    }

    impl ScopedConfigDirEnv {
        pub fn set(value: &std::path::Path) -> Self {
            let guard = lock();
            let prev = std::env::var_os(super::ENV_CONFIG_DIR);
            std::env::set_var(super::ENV_CONFIG_DIR, value);
            Self {
                _guard: guard,
                prev,
            }
        }

        pub fn unset() -> Self {
            let guard = lock();
            let prev = std::env::var_os(super::ENV_CONFIG_DIR);
            std::env::remove_var(super::ENV_CONFIG_DIR);
            Self {
                _guard: guard,
                prev,
            }
        }
    }

    impl Drop for ScopedConfigDirEnv {
        fn drop(&mut self) {
            match self.prev.take() {
                Some(v) => std::env::set_var(super::ENV_CONFIG_DIR, v),
                None => std::env::remove_var(super::ENV_CONFIG_DIR),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn resolve_creates_missing_dir_then_canonicalizes() {
        let base = TempDir::new().unwrap();
        let target = base.path().join("does").join("not").join("exist-yet");
        assert!(!target.exists());
        let resolved = resolve_config_dir(&target).expect("must create then canonicalize");
        assert!(target.is_dir(), "dir must have been created");
        // Canonical form of a fresh real dir == canonicalize of the raw path.
        assert_eq!(resolved, dunce::canonicalize(&target).unwrap());
    }

    /// Two spellings of ONE directory (real path vs a symlinked alias) must
    /// resolve to the SAME canonical dir — hence the same config.toml and the
    /// same daemon.lock. This is the two-locks-from-two-spellings fence.
    #[cfg(unix)]
    #[test]
    fn symlinked_spelling_resolves_to_same_canonical_dir_and_lock() {
        let base = TempDir::new().unwrap();
        let real = base.path().join("real-config-dir");
        std::fs::create_dir_all(&real).unwrap();
        let alias = base.path().join("alias");
        std::os::unix::fs::symlink(&real, &alias).unwrap();

        let via_real = resolve_config_dir(&real).unwrap();
        let via_alias = resolve_config_dir(&alias).unwrap();
        assert_eq!(
            via_real, via_alias,
            "symlink spelling must canonicalize to the identical dir"
        );
        assert_eq!(
            via_real.join(LOCK_FILENAME),
            via_alias.join(LOCK_FILENAME),
            "both spellings must map to ONE lock file"
        );

        // And the lock actually excludes across the two spellings.
        let _held = acquire_daemon_lock(&via_real).expect("first acquisition succeeds");
        match acquire_daemon_lock(&via_alias) {
            Err(LockError::Held { dir }) => assert_eq!(dir, via_real),
            other => panic!("expected Held via symlink spelling, got {other:?}"),
        }
    }

    #[test]
    fn lock_is_exclusive_and_second_acquire_is_refused_distinctly() {
        let dir = TempDir::new().unwrap();
        let canon = resolve_config_dir(dir.path()).unwrap();

        let first = acquire_daemon_lock(&canon).expect("first lock must succeed");
        let second = acquire_daemon_lock(&canon);
        match &second {
            Err(LockError::Held { dir: held_dir }) => {
                assert_eq!(held_dir, &canon);
                let msg = second.as_ref().unwrap_err().to_string();
                assert!(
                    msg.contains("managed-instance lock held by another process"),
                    "refusal message must be DISTINCT; got: {msg}"
                );
                assert!(
                    msg.contains(&canon.display().to_string()),
                    "refusal message must name the dir; got: {msg}"
                );
            }
            other => panic!("second acquisition must be refused as Held, got {other:?}"),
        }

        // Releasing the first lock frees the dir for a fresh acquisition.
        drop(first);
        acquire_daemon_lock(&canon).expect("lock must be re-acquirable after release");
    }

    #[test]
    fn managed_mode_detection_follows_env() {
        {
            let _env = test_env::ScopedConfigDirEnv::unset();
            assert!(!is_managed_mode(), "unset env must NOT arm managed mode");
            assert_eq!(
                managed_config_dir(),
                None,
                "unset env must resolve to None (vault mode)"
            );
        }
        {
            let dir = TempDir::new().unwrap();
            let _env = test_env::ScopedConfigDirEnv::set(dir.path());
            assert!(is_managed_mode());
            let resolved = managed_config_dir().expect("set env must resolve");
            assert_eq!(resolved, dunce::canonicalize(dir.path()).unwrap());
        }
    }

    /// The plugin policy is the single seam run() consults: managed mode
    /// registers NONE of single-instance/updater/autostart and never raises
    /// the wizard; vault mode keeps all of them (pre-0.4.38 behavior).
    #[test]
    fn plugin_policy_managed_drops_lifecycle_plugins_vault_keeps_them() {
        assert_eq!(
            plugin_policy(true),
            PluginPolicy {
                single_instance: false,
                updater: false,
                autostart: false,
                allow_wizard: false,
            }
        );
        assert_eq!(
            plugin_policy(false),
            PluginPolicy {
                single_instance: true,
                updater: true,
                autostart: true,
                allow_wizard: true,
            }
        );
    }
}
