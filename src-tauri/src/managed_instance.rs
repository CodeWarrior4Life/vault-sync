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
//!   enforced by an exclusive lock on `<dir>/daemon.lock` held for the
//!   process lifetime. A held lock is retried for a bounded window (10 s,
//!   Unit 1d) so a self-restart's successor outlives the predecessor's
//!   teardown; after the window a second process exits non-zero with a
//!   distinct error message.
//! - **Plugin policy**: managed instances register NO single-instance plugin
//!   (R12 — the lock above replaces it, keyed by config dir instead of
//!   app identity), NO updater (the vault instance is the sole update leader
//!   for the shared binary), NO autostart (login-item registration is
//!   app-global), and never raise the wizard/GUI (implied `--silent`).

use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// The environment variable that arms managed-instance mode.
pub const ENV_CONFIG_DIR: &str = "NEXUS_SYNC_CONFIG_DIR";

/// Basename of the per-config-dir exclusive lock file.
pub const LOCK_FILENAME: &str = "daemon.lock";

/// Total time [`acquire_daemon_lock`] keeps retrying a held lock before it
/// gives up with [`LockError::Held`] (Unit 1d, TKT-3f0e423e). A self-restart
/// (Tauri `restart`) spawns the successor BEFORE the predecessor exits, so the
/// successor briefly meets the still-held lock; 10 s covers a slow teardown
/// and stays well below the supervisor's 30 s `RestartSec`.
pub const LOCK_RETRY_WINDOW: Duration = Duration::from_secs(10);

/// Sleep between lock attempts while the lock is held.
pub const LOCK_RETRY_POLL: Duration = Duration::from_millis(250);

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
#[allow(clippy::disallowed_methods)] // CS-2 gate 2: listed in state_io_exemptions.toml
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

/// Acquire the exclusive advisory lock on `<canonical_config_dir>/daemon.lock`,
/// retrying a HELD lock for [`LOCK_RETRY_WINDOW`] (polling every
/// [`LOCK_RETRY_POLL`]) before refusing with [`LockError::Held`].
/// `canonical_config_dir` MUST already be the output of [`resolve_config_dir`]
/// / [`managed_config_dir`] — locking a non-canonical spelling would defeat
/// the one-lock-per-dir invariant.
///
/// Uses std's file locking (flock(2) on Unix, LockFileEx on Windows — stable
/// since Rust 1.89), so no new dependency is required.
pub fn acquire_daemon_lock(canonical_config_dir: &Path) -> Result<DaemonLock, LockError> {
    acquire_daemon_lock_with(canonical_config_dir, LOCK_RETRY_WINDOW, LOCK_RETRY_POLL)
}

/// [`acquire_daemon_lock`] with an explicit retry `window` and `poll`
/// interval (the seam tests drive with small values).
///
/// - Lock held by another process (`TryLockError::WouldBlock`): sleep `poll`
///   and retry until `window` has elapsed on the monotonic clock, then return
///   [`LockError::Held`] (one final attempt is made at the deadline).
/// - Open failure or `TryLockError::Error` (an I/O error): return
///   [`LockError::Io`] IMMEDIATELY, with no retry.
#[allow(clippy::disallowed_methods)] // CS-2 gate 2: listed in state_io_exemptions.toml
pub fn acquire_daemon_lock_with(
    canonical_config_dir: &Path,
    window: Duration,
    poll: Duration,
) -> Result<DaemonLock, LockError> {
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
    match retry_try_lock(|| file.try_lock(), window, poll) {
        Ok(()) => Ok(DaemonLock { _file: file, path }),
        Err(RetryFail::Held) => Err(LockError::Held {
            dir: canonical_config_dir.to_path_buf(),
        }),
        Err(RetryFail::Io(source)) => Err(LockError::Io { path, source }),
    }
}

/// Why [`retry_try_lock`] gave up.
#[derive(Debug)]
enum RetryFail {
    /// Still `WouldBlock` when the window elapsed.
    Held,
    /// A real lock error: returned on the FIRST occurrence, never retried.
    Io(io::Error),
}

/// The bounded retry, separated from the file so a test can inject a real
/// `TryLockError::Error` (which cannot be provoked reliably on a real file).
/// Retries ONLY on `WouldBlock`, for at most `window` (monotonic clock).
fn retry_try_lock<F>(mut try_lock: F, window: Duration, poll: Duration) -> Result<(), RetryFail>
where
    F: FnMut() -> Result<(), std::fs::TryLockError>,
{
    let started = Instant::now();
    loop {
        match try_lock() {
            Ok(()) => return Ok(()),
            Err(std::fs::TryLockError::WouldBlock) => {
                let elapsed = started.elapsed();
                if elapsed >= window {
                    return Err(RetryFail::Held);
                }
                std::thread::sleep(poll.min(window - elapsed));
            }
            Err(std::fs::TryLockError::Error(source)) => return Err(RetryFail::Io(source)),
        }
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
#[allow(clippy::disallowed_methods)]
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
#[allow(clippy::disallowed_methods)]
mod tests {

    /// T-1d-3b (gate C1, kills M3): a genuine `TryLockError::Error` is returned on
    /// the FIRST call and never retried, even with a long window.
    #[test]
    fn t_1d_3b_try_lock_error_is_never_retried() {
        let mut calls = 0u32;
        let started = Instant::now();
        let r = retry_try_lock(
            || {
                calls += 1;
                Err(std::fs::TryLockError::Error(io::Error::other(
                    "injected lock error",
                )))
            },
            Duration::from_secs(5),
            Duration::from_millis(50),
        );
        assert!(
            matches!(r, Err(RetryFail::Io(_))),
            "a real lock error must surface as Io"
        );
        assert_eq!(calls, 1, "a real lock error must not be retried");
        assert!(
            started.elapsed() < Duration::from_millis(100),
            "no sleep on a real lock error"
        );
    }

    /// The same seam still retries WouldBlock and then succeeds.
    #[test]
    fn t_1d_3c_wouldblock_then_ok_via_seam() {
        let mut calls = 0u32;
        let r = retry_try_lock(
            || {
                calls += 1;
                if calls < 3 {
                    Err(std::fs::TryLockError::WouldBlock)
                } else {
                    Ok(())
                }
            },
            Duration::from_secs(2),
            Duration::from_millis(10),
        );
        assert!(r.is_ok());
        assert_eq!(calls, 3);
    }
    use super::*;
    use std::sync::mpsc;
    use tempfile::TempDir;

    /// Small retry window for tests that expect a `Held` refusal.
    const SHORT_WINDOW: Duration = Duration::from_millis(100);
    const SHORT_POLL: Duration = Duration::from_millis(20);

    /// Take the lock on `<dir>/daemon.lock` with an independent open +
    /// `try_lock` (NOT through the code under test).
    fn hold_lock_directly(dir: &Path) -> File {
        let f = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(dir.join(LOCK_FILENAME))
            .unwrap();
        f.try_lock().expect("holder must take the free lock");
        f
    }

    /// Run `acquire_daemon_lock_with` on a worker thread so a mutation that
    /// never returns turns the test RED (timeout) instead of hanging it.
    /// Returns (outcome, elapsed).
    fn acquire_on_thread(
        dir: &Path,
        window: Duration,
        poll: Duration,
        bound: Duration,
    ) -> (Result<DaemonLock, LockError>, Duration) {
        let (tx, rx) = mpsc::channel();
        let dir = dir.to_path_buf();
        std::thread::spawn(move || {
            let t0 = Instant::now();
            let r = acquire_daemon_lock_with(&dir, window, poll);
            let _ = tx.send((r, t0.elapsed()));
        });
        rx.recv_timeout(bound)
            .unwrap_or_else(|_| panic!("acquire did not return within {bound:?}"))
    }

    /// T-1d-1: a holder releases after 600 ms; the retrying acquire waits it
    /// out and succeeds. Mutation that turns it red: window 0 (no retry).
    #[test]
    fn t1d1_retry_acquires_after_holder_releases() {
        let dir = TempDir::new().unwrap();
        let canon = resolve_config_dir(dir.path()).unwrap();
        let held = hold_lock_directly(&canon);
        let holder = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(600));
            drop(held);
        });
        let t0 = Instant::now();
        let got =
            acquire_daemon_lock_with(&canon, Duration::from_secs(3), Duration::from_millis(50));
        let elapsed = t0.elapsed();
        holder.join().unwrap();
        let lock = got.expect("retry must acquire once the holder releases");
        assert_eq!(lock.path, canon.join(LOCK_FILENAME));
        assert!(
            elapsed >= Duration::from_millis(600),
            "must have waited for the holder; elapsed {elapsed:?}"
        );
    }

    /// T-1d-2: a holder that never releases yields `Held` once the window has
    /// elapsed, and not much later. Mutation: retry forever (no deadline).
    #[test]
    fn t1d2_window_elapses_to_held_bounded() {
        let dir = TempDir::new().unwrap();
        let canon = resolve_config_dir(dir.path()).unwrap();
        let _held = hold_lock_directly(&canon);
        let (got, elapsed) = acquire_on_thread(
            &canon,
            Duration::from_millis(500),
            Duration::from_millis(50),
            Duration::from_secs(2),
        );
        match got {
            Err(LockError::Held { dir: d }) => assert_eq!(d, canon),
            other => panic!("expected Held after the window, got {other:?}"),
        }
        assert!(
            elapsed >= Duration::from_millis(500),
            "must retry for the whole window; elapsed {elapsed:?}"
        );
        assert!(
            elapsed < Duration::from_secs(2),
            "must stop at the window; elapsed {elapsed:?}"
        );
    }

    /// T-1d-3: an I/O error (the lock path is a directory, so the open fails)
    /// returns `Io` immediately with no retry. Mutation: retry on every error.
    #[test]
    fn t1d3_io_error_returns_immediately_without_retry() {
        let dir = TempDir::new().unwrap();
        let canon = resolve_config_dir(dir.path()).unwrap();
        std::fs::create_dir(canon.join(LOCK_FILENAME)).unwrap();
        let (got, elapsed) = acquire_on_thread(
            &canon,
            Duration::from_secs(3),
            Duration::from_millis(50),
            Duration::from_secs(5),
        );
        match got {
            Err(LockError::Io { path, .. }) => assert_eq!(path, canon.join(LOCK_FILENAME)),
            other => panic!("expected Io for an unopenable lock path, got {other:?}"),
        }
        assert!(
            elapsed < Duration::from_millis(100),
            "an I/O error must not be retried; elapsed {elapsed:?}"
        );
    }

    const T1D5_CHILD_DIR_ENV: &str = "MI_T1D5_CHILD_DIR";
    const T1D5_CHILD_READY_ENV: &str = "MI_T1D5_CHILD_READY";

    /// T-1d-5 helper, NOT a test on its own: the predecessor process. The
    /// parent re-executes this test binary filtered to this test with the
    /// config dir in the environment; without it this returns immediately.
    #[test]
    #[ignore = "T-1d-5 second-process helper; spawned by t1d5_real_process_restart_overlap_is_waited_out"]
    fn t1d5_child_process_holder() {
        let Ok(dir) = std::env::var(T1D5_CHILD_DIR_ENV) else {
            return;
        };
        let ready = std::env::var(T1D5_CHILD_READY_ENV).unwrap();
        let lock = acquire_daemon_lock_with(Path::new(&dir), Duration::ZERO, SHORT_POLL)
            .expect("child must take the free lock");
        std::fs::write(&ready, b"held").unwrap();
        std::thread::sleep(Duration::from_secs(1));
        // Exit while still holding, like a restarting daemon's `exit(0)`:
        // the OS releases the lock at process exit.
        std::mem::forget(lock);
        std::process::exit(0);
    }

    /// T-1d-5: integration with a REAL second process. The child holds the
    /// lock and exits after 1 s; the parent's `acquire_daemon_lock` (default
    /// window) succeeds — the Tauri restart overlap. Mutation: window 0.
    #[test]
    fn t1d5_real_process_restart_overlap_is_waited_out() {
        let dir = TempDir::new().unwrap();
        let canon = resolve_config_dir(dir.path()).unwrap();
        let ready = dir.path().join("child-ready");
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "managed_instance::tests::t1d5_child_process_holder",
                "--ignored",
                "--test-threads=1",
                "--quiet",
            ])
            .env(T1D5_CHILD_DIR_ENV, &canon)
            .env(T1D5_CHILD_READY_ENV, &ready)
            .stdout(std::process::Stdio::null())
            .spawn()
            .expect("spawn second process");

        let wait_start = Instant::now();
        while !ready.exists() {
            if wait_start.elapsed() > Duration::from_secs(30) {
                let _ = child.kill();
                panic!("child never signalled that it holds the lock");
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        // Positive control: the child really holds the lock right now.
        match acquire_daemon_lock_with(&canon, Duration::ZERO, SHORT_POLL) {
            Err(LockError::Held { .. }) => {}
            other => panic!("child must hold the lock before the parent's acquire, got {other:?}"),
        }

        let t0 = Instant::now();
        let got = acquire_daemon_lock(&canon);
        let elapsed = t0.elapsed();
        let status = child.wait().expect("child wait");
        assert!(status.success(), "child must exit cleanly: {status:?}");
        got.expect("parent must acquire once the predecessor process exits");
        assert!(
            elapsed < LOCK_RETRY_WINDOW,
            "must acquire inside the window; elapsed {elapsed:?}"
        );
    }

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
        match acquire_daemon_lock_with(&via_alias, SHORT_WINDOW, SHORT_POLL) {
            Err(LockError::Held { dir }) => assert_eq!(dir, via_real),
            other => panic!("expected Held via symlink spelling, got {other:?}"),
        }
    }

    #[test]
    fn lock_is_exclusive_and_second_acquire_is_refused_distinctly() {
        let dir = TempDir::new().unwrap();
        let canon = resolve_config_dir(dir.path()).unwrap();

        let first = acquire_daemon_lock(&canon).expect("first lock must succeed");
        // T-1d-4: a small retry window keeps the suite fast; the refusal is
        // still the distinct `Held` once the window elapses.
        let second = acquire_daemon_lock_with(&canon, SHORT_WINDOW, SHORT_POLL);
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
