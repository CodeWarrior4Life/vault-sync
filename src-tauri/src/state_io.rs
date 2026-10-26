//! Unit CS-2 (TKT-23091e72): `StateDir`, the sole state-I/O capability.
//!
//! [`StateDir`] is the ONLY production code that may create, write, rename,
//! remove or read sync-state bytes. Every operation is relative to a held
//! directory fd (`openat`/`renameat`/`unlinkat`/`fstatat`) and is addressed by
//! the closed [`StateFile`] enum. No production code outside this module keeps
//! a path to a state file: owners hold `(Arc<StateDir>, StateFile)`.
//!
//! In CS-2 the `StateDir` is UNLEASED: an fd on the existing v1 directory
//! `<workspace>/.lattice-runtime/<sub>/sync-state/`, with no `flock` (the lease
//! is CS-4). What CS-2 guarantees:
//!
//! 1. Writes cannot reach a replacement tree: after a move plus recreate, an
//!    `rm -rf` or a `RENAME_EXCHANGE`, no writer builds or fills a new dir.
//! 2. Custody detection: a holder whose canonical path no longer resolves to
//!    its held inode commits nothing further, sends no push and exits 74.
//! 3. No write-time directory creation: [`bootstrap`] is the only creator.
//!
//! This module is exempt from gate 1 (clippy `disallowed-methods`) as a whole;
//! the exemption is listed in `state_io_exemptions.toml` (gate 2).
#![allow(clippy::disallowed_methods)]

use std::collections::HashMap;
use std::fmt;
use std::fs::File;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use crate::durable_file::{self, sealed::Sealed, Anchor, MutexKey};
pub use crate::durable_file::{Commit, NotCommitted};
use crate::sync_health::{raise_state_event, StateHealthEvent};

/// Layout literals. `.lattice-runtime` is exposed as one const because four
/// non-state modules need it (exclusion lists and the shadow-mode vault tree,
/// D8); the `sync-state` leaf stays private, so rebuilding a state path
/// outside this module needs a literal gate 3 catches.
pub mod layout {
    /// The per-workspace runtime root directory name.
    pub const RUNTIME_DIR_NAME: &str = ".lattice-runtime";
}

const SYNC_STATE_DIR_NAME: &str = "sync-state";
/// The CS-4 v2 layout root. Reserved: CS-2 never creates it; the run-time
/// belt refuses writes under it.
const STATE_V2_DIR_NAME: &str = "state-v2";

/// Exit status on lost custody (EX_IOERR).
pub const EXIT_OWNERSHIP_LOST: i32 = 74;

/// Which directory a [`StateFile`] lives in.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Scope {
    /// `<workspace>/.lattice-runtime/<sub>/sync-state/`.
    SyncState,
    /// The daemon config dir (W13; relocation is CS-4, Q-r2-4).
    ConfigDir,
}

/// The closed set of state files. CS-2 registers exactly the files that have
/// writers today (D2); later units add members together with their writers
/// and their T-CS2-1 triggers (the exhaustive `match` there makes it
/// compulsory).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum StateFile {
    PushJournal,
    PushJournalLock,
    Shadow,
    BaseSeq,
    ReadReceipt,
    LastEventId,
    DeleteLedger,
    PauseMarker,
    ReconcileRetryLedger,
}

impl StateFile {
    /// Every member, in declaration order.
    pub const ALL: &'static [StateFile] = &[
        StateFile::PushJournal,
        StateFile::PushJournalLock,
        StateFile::Shadow,
        StateFile::BaseSeq,
        StateFile::ReadReceipt,
        StateFile::LastEventId,
        StateFile::DeleteLedger,
        StateFile::PauseMarker,
        StateFile::ReconcileRetryLedger,
    ];

    /// Fixed relative name: no `/`, no `..`, byte-identical to v1.
    pub const fn name(self) -> &'static str {
        match self {
            StateFile::PushJournal => "push_journal.jsonl",
            StateFile::PushJournalLock => "push_journal.lock",
            StateFile::Shadow => "shadow_hashes.json",
            StateFile::BaseSeq => "base_seq.json",
            StateFile::ReadReceipt => "read_receipt.json",
            StateFile::LastEventId => "last_event_id",
            StateFile::DeleteLedger => "delete-burst-SUPPRESSED",
            StateFile::PauseMarker => "delete-burst-PAUSED",
            StateFile::ReconcileRetryLedger => "reconcile-retry-ledger.json",
        }
    }

    pub const fn scope(self) -> Scope {
        match self {
            StateFile::ReconcileRetryLedger => Scope::ConfigDir,
            _ => Scope::SyncState,
        }
    }
}

/// Why custody was lost.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LossReason {
    /// The canonical path no longer resolves (ENOENT / ENOTDIR).
    CanonicalAbsent,
    /// The canonical path resolves to a different inode.
    Replaced { observed: (u64, u64) },
    /// `stat` of the canonical path failed with this errno (fail closed).
    StatError(i32),
    /// Bootstrap could not hold the inode at the canonical path (§3.2 step 3).
    BootstrapRace,
    /// The process latch is already set; no syscall was issued.
    Latched,
}

impl LossReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            LossReason::CanonicalAbsent => "canonical_absent",
            LossReason::Replaced { .. } => "replaced",
            LossReason::StatError(_) => "stat_error",
            LossReason::BootstrapRace => "bootstrap_race",
            LossReason::Latched => "latched",
        }
    }
}

/// Custody of a state dir is lost.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("state-dir ownership lost ({})", reason.as_str())]
pub struct OwnershipLost {
    pub reason: LossReason,
}

/// True iff `e` carries an [`OwnershipLost`].
pub fn is_ownership_lost(e: &io::Error) -> bool {
    e.get_ref()
        .and_then(|i| i.downcast_ref::<OwnershipLost>())
        .is_some()
}

fn lost(reason: LossReason) -> NotCommitted {
    NotCommitted(io::Error::other(OwnershipLost { reason }))
}

/// Operator-facing text for a state dir or file. Implements `Display` ONLY:
/// it is not `AsRef<Path>`, not `Into<PathBuf>` and not `Deref` (T-CS2-21),
/// and every call site outside `state_io` is listed (gate C7).
#[derive(Debug, Clone)]
pub struct DisplayPath(String);

impl fmt::Display for DisplayPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// The canonical path of a held directory. Private; no conversions.
struct CanonPath(PathBuf);

/// A subscriber id was not bootstrapped (or was rejected).
#[derive(Debug, Clone, thiserror::Error)]
#[error("state dir not bootstrapped for {sub:?}: {why}")]
pub struct NotBootstrapped {
    pub sub: String,
    pub why: String,
}

/// Bootstrap failure for the whole registry (per-sub failures are recorded
/// in the registry instead, so valid subs still bootstrap).
#[derive(Debug, thiserror::Error)]
pub enum BootstrapError {
    #[error("workspace root unusable: {0}")]
    Workspace(io::Error),
}

// ---------------------------------------------------------------------------
// Process latch and exit 74
// ---------------------------------------------------------------------------

/// The `OWNERSHIP_LOST` latch (§3.3). One per process in production; a test
/// `StateDir` gets its own so parallel tests stay independent.
pub(crate) struct Latch {
    set: AtomicBool,
    errors_logged: AtomicUsize,
    #[cfg(test)]
    pub(crate) exits: Mutex<Vec<(i32, LossReason)>>,
}

impl Latch {
    fn new() -> Self {
        Latch {
            set: AtomicBool::new(false),
            errors_logged: AtomicUsize::new(0),
            #[cfg(test)]
            exits: Mutex::new(Vec::new()),
        }
    }
    pub(crate) fn is_set(&self) -> bool {
        self.set.load(Ordering::SeqCst)
    }
    #[cfg(test)]
    #[cfg_attr(not(unix), allow(dead_code))] // used by unix-only tests
    pub(crate) fn errors_logged(&self) -> usize {
        self.errors_logged.load(Ordering::SeqCst)
    }
}

fn process_latch() -> Arc<Latch> {
    static L: OnceLock<Arc<Latch>> = OnceLock::new();
    L.get_or_init(|| Arc::new(Latch::new())).clone()
}

type LogFlush = Box<dyn FnOnce() + Send>;

fn log_flush_slot() -> &'static Mutex<Option<LogFlush>> {
    static S: OnceLock<Mutex<Option<LogFlush>>> = OnceLock::new();
    S.get_or_init(|| Mutex::new(None))
}

/// Register the closure that flushes the tracing non-blocking writer (it
/// drops the appender's `WorkerGuard`). Called once by `main::init_logging`.
pub fn register_log_flush(f: Box<dyn FnOnce() + Send>) {
    *log_flush_slot().lock().unwrap_or_else(|p| p.into_inner()) = Some(f);
}

/// Flush logs, waiting at most 250 ms (§3.3 step 4).
fn flush_logs_bounded() {
    let f = log_flush_slot()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .take();
    if let Some(f) = f {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            f();
            let _ = tx.send(());
        });
        let _ = rx.recv_timeout(Duration::from_millis(250));
    }
}

/// §3.3: latch, log once, raise SyncHealth, flush logs, `exit(74)`. Only the
/// first caller proceeds; the others return at once.
fn ownership_lost(latch: &Latch, shown: &DisplayPath, id: (u64, u64), reason: &LossReason) {
    if latch
        .set
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        return;
    }
    latch.errors_logged.fetch_add(1, Ordering::SeqCst);
    tracing::error!(
        state_dir = %shown,
        held_dev = id.0,
        held_ino = id.1,
        observation = ?reason,
        reason = reason.as_str(),
        "state_io: OWNERSHIP LOST: the canonical state path no longer resolves to the held directory; committing nothing further, sending no push, exiting 74"
    );
    raise_state_event(StateHealthEvent::OwnershipLost {
        reason: reason.as_str().to_string(),
    });
    flush_logs_bounded();
    exit_74(latch, reason);
}

#[cfg(not(test))]
fn exit_74(_latch: &Latch, _reason: &LossReason) {
    std::process::exit(EXIT_OWNERSHIP_LOST);
}

/// Tests record the exit instead of killing the test runner, unless a child
/// process asks for the real exit (`STATE_IO_REAL_EXIT=1`).
#[cfg(test)]
fn exit_74(latch: &Latch, reason: &LossReason) {
    if std::env::var_os("STATE_IO_REAL_EXIT").is_some() {
        std::process::exit(EXIT_OWNERSHIP_LOST);
    }
    latch
        .exits
        .lock()
        .unwrap()
        .push((EXIT_OWNERSHIP_LOST, reason.clone()));
}

// ---------------------------------------------------------------------------
// Test seams
// ---------------------------------------------------------------------------

/// Test-only per-`StateDir` seams: custody switch and a syscall recorder.
#[cfg(test)]
#[derive(Default)]
pub(crate) struct DirSeams {
    /// When set, custody is not checked (T-CS2-2, -5, -15, -16, -31A).
    pub(crate) custody_disabled: AtomicBool,
    /// Every syscall-level operation, as `"<op>:<name>"`.
    pub(crate) recorder: Mutex<Vec<String>>,
}

#[cfg(test)]
impl DirSeams {
    fn rec(&self, op: &str, name: &str) {
        self.recorder.lock().unwrap().push(format!("{op}:{name}"));
    }
}

#[cfg(test)]
#[allow(clippy::disallowed_methods)]
#[cfg_attr(not(unix), allow(dead_code))] // used by unix-only tests
pub(crate) mod seams {
    use std::cell::RefCell;
    use std::path::Path;

    /// Filesystem probe override for `fstatfs` (T-CS2-17).
    #[derive(Clone, Debug)]
    pub(crate) enum FsProbe {
        Magic(u64),
        Name(String),
    }

    pub(crate) type LeafHook = Box<dyn FnMut(&Path)>;

    #[derive(Default)]
    struct State {
        bootstrap_rec: Vec<String>,
        force_fs: Option<FsProbe>,
        after_leaf_open: Option<LeafHook>,
        belt_ids: Option<Vec<(u64, u64)>>,
    }

    thread_local! {
        static S: RefCell<State> = RefCell::new(State::default());
    }

    pub(crate) fn reset() {
        S.with(|s| *s.borrow_mut() = State::default());
    }
    pub(crate) fn bootstrap_rec(op: &str, name: &str) {
        S.with(|s| s.borrow_mut().bootstrap_rec.push(format!("{op}:{name}")));
    }
    pub(crate) fn bootstrap_calls() -> Vec<String> {
        S.with(|s| s.borrow().bootstrap_rec.clone())
    }
    pub(crate) fn force_fs(p: Option<FsProbe>) {
        S.with(|s| s.borrow_mut().force_fs = p);
    }
    /// T-CS2-11: the belt's registered id set for this thread (the process
    /// registry stays empty in unit tests).
    pub(crate) fn set_belt_ids(ids: Option<Vec<(u64, u64)>>) {
        S.with(|s| s.borrow_mut().belt_ids = ids);
    }
    pub(crate) fn belt_ids() -> Option<Vec<(u64, u64)>> {
        S.with(|s| s.borrow().belt_ids.clone())
    }
    pub(crate) fn forced_fs() -> Option<FsProbe> {
        S.with(|s| s.borrow().force_fs.clone())
    }
    /// Runs between the leaf `openat` and the post-open identity check
    /// (T-CS2-14), with the canonical leaf path.
    pub(crate) fn set_after_leaf_open(h: Option<LeafHook>) {
        S.with(|s| s.borrow_mut().after_leaf_open = h);
    }
    pub(crate) fn run_after_leaf_open(p: &Path) {
        let h = S.with(|s| s.borrow_mut().after_leaf_open.take());
        if let Some(mut h) = h {
            h(p);
            S.with(|s| {
                let mut st = s.borrow_mut();
                if st.after_leaf_open.is_none() {
                    st.after_leaf_open = Some(h);
                }
            });
        }
    }
}

// ---------------------------------------------------------------------------
// unix syscalls
// ---------------------------------------------------------------------------

#[cfg(unix)]
mod sys {
    use std::ffi::{CStr, CString};
    use std::fs::File;
    use std::io;
    use std::os::unix::io::{FromRawFd, RawFd};

    pub(super) const O_FILE_READ: libc::c_int =
        libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK;
    pub(super) const O_DIR: libc::c_int = libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC;

    pub(super) fn cstr(name: &str) -> io::Result<CString> {
        CString::new(name).map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "NUL in name"))
    }

    pub(super) fn errno_is(e: &io::Error, code: libc::c_int) -> bool {
        e.raw_os_error() == Some(code)
    }

    /// `openat` with EINTR retry. `mode` is only read with `O_CREAT`.
    pub(super) fn openat(
        dir: RawFd,
        name: &str,
        flags: libc::c_int,
        mode: libc::c_uint,
    ) -> io::Result<File> {
        let c = cstr(name)?;
        loop {
            // SAFETY: `c` is NUL-terminated and outlives the call; `dir` is a
            // live fd owned by the caller for the duration of the call.
            let fd = unsafe { libc::openat(dir, c.as_ptr(), flags, mode) };
            if fd >= 0 {
                // SAFETY: `fd` was just returned by openat and is owned here.
                return Ok(unsafe { File::from_raw_fd(fd) });
            }
            let e = io::Error::last_os_error();
            if e.kind() != io::ErrorKind::Interrupted {
                return Err(e);
            }
        }
    }

    /// `fstatat(dir, name, AT_SYMLINK_NOFOLLOW)`; `None` on ENOENT.
    pub(super) fn fstatat_nofollow(dir: RawFd, name: &str) -> io::Result<Option<libc::stat>> {
        let c = cstr(name)?;
        let mut st = std::mem::MaybeUninit::<libc::stat>::uninit();
        // SAFETY: valid C string, live fd, `st` is a writable stat buffer.
        let rc =
            unsafe { libc::fstatat(dir, c.as_ptr(), st.as_mut_ptr(), libc::AT_SYMLINK_NOFOLLOW) };
        if rc == 0 {
            // SAFETY: fstatat returned 0, so it initialized `st`.
            return Ok(Some(unsafe { st.assume_init() }));
        }
        let e = io::Error::last_os_error();
        if errno_is(&e, libc::ENOENT) {
            Ok(None)
        } else {
            Err(e)
        }
    }

    pub(super) fn renameat(dir: RawFd, from: &str, to: &str) -> io::Result<()> {
        let (f, t) = (cstr(from)?, cstr(to)?);
        // SAFETY: valid C strings, live fd.
        if unsafe { libc::renameat(dir, f.as_ptr(), dir, t.as_ptr()) } == -1 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    pub(super) fn unlinkat(dir: RawFd, name: &str) -> io::Result<()> {
        let c = cstr(name)?;
        // SAFETY: valid C string, live fd.
        if unsafe { libc::unlinkat(dir, c.as_ptr(), 0) } == -1 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// `mkdirat`; `Ok(true)` if created, `Ok(false)` on EEXIST.
    pub(super) fn mkdirat(dir: RawFd, name: &str, mode: libc::mode_t) -> io::Result<bool> {
        let c = cstr(name)?;
        // SAFETY: valid C string, live fd.
        if unsafe { libc::mkdirat(dir, c.as_ptr(), mode) } == 0 {
            return Ok(true);
        }
        let e = io::Error::last_os_error();
        if errno_is(&e, libc::EEXIST) {
            Ok(false)
        } else {
            Err(e)
        }
    }

    pub(super) fn is_reg(st: &libc::stat) -> bool {
        (st.st_mode & libc::S_IFMT) == libc::S_IFREG
    }

    pub(super) fn is_lnk(st: &libc::stat) -> bool {
        (st.st_mode & libc::S_IFMT) == libc::S_IFLNK
    }

    /// Does the parent of the held directory (`openat(dir, "..")`) still
    /// hold the held directory under `leaf`? `fstatat(parent, leaf,
    /// AT_SYMLINK_NOFOLLOW)` and compare `(st_dev, st_ino)` with `id`. A stat
    /// BY NAME crosses into a mount, so a bind-mounted state dir matches
    /// (a `readdir` `d_ino` is the underlying mountpoint inode and would
    /// not). Read-only. ENOENT or a different identity is `Ok(false)`.
    pub(super) fn parent_holds_leaf(dir: RawFd, leaf: &str, id: (u64, u64)) -> io::Result<bool> {
        use std::os::unix::io::AsRawFd;
        let parent = openat(dir, "..", O_DIR, 0)?;
        match fstatat_nofollow(parent.as_raw_fd(), leaf)? {
            None => Ok(false),
            #[allow(clippy::unnecessary_cast)]
            Some(st) => Ok((st.st_dev as u64, st.st_ino as u64) == id),
        }
    }

    /// Entry names of an open directory, from `readdir` on a fresh
    /// description (`openat(dir, ".")`): no file inside is opened.
    pub(super) fn read_dir_names(dir: RawFd) -> io::Result<Vec<String>> {
        let d = openat(dir, ".", O_DIR, 0)?;
        let fd = std::os::unix::io::IntoRawFd::into_raw_fd(d);
        // SAFETY: fdopendir takes ownership of `fd` (closed by closedir).
        let dp = unsafe { libc::fdopendir(fd) };
        if dp.is_null() {
            let e = io::Error::last_os_error();
            // SAFETY: fdopendir failed, so `fd` is still ours to close.
            unsafe { libc::close(fd) };
            return Err(e);
        }
        let mut out = Vec::new();
        loop {
            // SAFETY: `dp` is a live DIR*; readdir returns NULL at the end.
            let ent = unsafe { libc::readdir(dp) };
            if ent.is_null() {
                break;
            }
            // SAFETY: `ent` is non-null and d_name is NUL-terminated.
            let name = unsafe { CStr::from_ptr((*ent).d_name.as_ptr()) };
            let name = name.to_string_lossy().into_owned();
            if name != "." && name != ".." {
                out.push(name);
            }
        }
        // SAFETY: `dp` came from fdopendir and is closed exactly once.
        unsafe { libc::closedir(dp) };
        Ok(out)
    }
}

// ---------------------------------------------------------------------------
// Filesystem observation (T-S9, observe-only in CS-2)
// ---------------------------------------------------------------------------

/// Classify a Linux `statfs.f_type` magic. `Ok(name)` for the supported set
/// (btrfs, ext4, xfs; tmpfs under `cfg(test)`), `Err(label)` otherwise.
// Read by observe_fs on Linux only; tests use it everywhere.
#[cfg_attr(not(any(target_os = "linux", target_os = "android")), allow(dead_code))]
pub(crate) fn classify_fs_magic(magic: u64) -> Result<&'static str, String> {
    match magic {
        0x9123_683E => Ok("btrfs"),
        0xEF53 => Ok("ext4"),
        0x5846_5342 => Ok("xfs"),
        #[cfg(test)]
        0x0102_1994 => Ok("tmpfs"),
        0x6573_5546 => Err("fuse".to_string()),
        0x6969 => Err("nfs".to_string()),
        0x517B => Err("smb".to_string()),
        0xFE53_4D42 => Err("smb2".to_string()),
        0xFF53_4D42 => Err("cifs".to_string()),
        other => Err(format!("0x{other:x}")),
    }
}

/// Classify an Apple `statfs.f_fstypename`.
// Read by observe_fs on Apple only; tests use it everywhere.
#[cfg_attr(not(any(target_vendor = "apple", test)), allow(dead_code))]
#[cfg_attr(all(test, not(unix)), allow(dead_code))]
pub(crate) fn classify_fs_name(name: &str) -> Result<&'static str, String> {
    match name {
        "apfs" => Ok("apfs"),
        #[cfg(test)]
        "tmpfs" => Ok("tmpfs"),
        other => Err(other.to_string()),
    }
}

#[cfg(unix)]
fn observe_fs(dir: &File) -> Result<&'static str, String> {
    #[cfg(test)]
    if let Some(p) = seams::forced_fs() {
        return match p {
            seams::FsProbe::Magic(m) => classify_fs_magic(m),
            seams::FsProbe::Name(n) => classify_fs_name(&n),
        };
    }
    use std::os::unix::io::AsRawFd;
    let mut s = std::mem::MaybeUninit::<libc::statfs>::uninit();
    // SAFETY: live fd, writable statfs buffer.
    if unsafe { libc::fstatfs(dir.as_raw_fd(), s.as_mut_ptr()) } != 0 {
        return Err(format!("fstatfs: {}", io::Error::last_os_error()));
    }
    // SAFETY: fstatfs returned 0, so it initialized `s`.
    let s = unsafe { s.assume_init() };
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        #[allow(clippy::unnecessary_cast)]
        let magic = s.f_type as u64;
        classify_fs_magic(magic)
    }
    #[cfg(target_vendor = "apple")]
    {
        // SAFETY: f_fstypename is a NUL-terminated C char array.
        let n = unsafe { std::ffi::CStr::from_ptr(s.f_fstypename.as_ptr()) };
        classify_fs_name(&n.to_string_lossy())
    }
    #[cfg(not(any(target_os = "linux", target_os = "android", target_vendor = "apple")))]
    {
        let _ = s;
        Err("unknown".to_string())
    }
}

// ---------------------------------------------------------------------------
// StateDir
// ---------------------------------------------------------------------------

/// The held state directory. Not `Clone`; shared as `Arc<StateDir>`.
pub struct StateDir {
    /// `O_RDONLY|O_DIRECTORY|O_CLOEXEC`: the held inode.
    #[cfg(unix)]
    dir: File,
    /// Windows (not a release target, NG-3): pathname I/O behind the same API.
    #[cfg(not(unix))]
    path: PathBuf,
    /// `(st_dev, st_ino)` from `fstat` at bootstrap.
    id: (u64, u64),
    canon: CanonPath,
    scope: Scope,
    /// This boot created the leaf directory.
    created: bool,
    dir_sync_pending: AtomicBool,
    latch: Arc<Latch>,
    #[cfg(test)]
    pub(crate) seams: DirSeams,
}

impl fmt::Debug for StateDir {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StateDir")
            .field("dir", &self.display(None).0)
            .field("id", &self.id)
            .field("scope", &self.scope)
            .finish()
    }
}

/// A file opened for appending through a [`StateDir`]: fd-bound, no path.
/// Every write checks custody first.
pub struct StateAppender<'a> {
    sd: &'a StateDir,
    file: File,
    name: &'static str,
    created: bool,
}

impl StateAppender<'_> {
    /// Current length of the open file (`fstat` on the fd).
    pub fn len(&self) -> io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    /// True iff the open file is empty.
    pub fn is_empty(&self) -> io::Result<bool> {
        Ok(self.len()? == 0)
    }

    /// Write the whole buffer (custody first).
    pub fn write_all(&mut self, bytes: &[u8]) -> Result<(), NotCommitted> {
        self.sd.guard_mutation()?;
        #[cfg(test)]
        self.sd.seams.rec("write", self.name);
        self.file.write_all(bytes).map_err(NotCommitted)
    }

    /// `sync_data` the file; a file this appender created also syncs the
    /// directory, and a dir-sync error gives `CommittedDurabilityUncertain`.
    pub fn sync_data(&mut self) -> Result<Commit, NotCommitted> {
        #[cfg(test)]
        self.sd.seams.rec("sync_data", self.name);
        self.file.sync_data().map_err(NotCommitted)?;
        if self.created {
            self.created = false;
            return Ok(self.sd.sync_dir_outcome(self.name));
        }
        Ok(Commit::Durable)
    }
}

impl StateDir {
    fn check_scope(&self, f: StateFile) -> Result<(), NotCommitted> {
        if f.scope() != self.scope {
            // A programming error. The debug assert is compiled out of test
            // builds so T-CS2-27 can observe the InvalidInput mapping.
            #[cfg(not(test))]
            debug_assert!(false, "StateFile {f:?} used on a {:?} StateDir", self.scope);
            return Err(NotCommitted(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "state file {} has scope {:?}; this StateDir is {:?}",
                    f.name(),
                    f.scope(),
                    self.scope
                ),
            )));
        }
        Ok(())
    }

    fn check_scope_io(&self, f: StateFile) -> io::Result<()> {
        self.check_scope(f).map_err(|n| n.0)
    }

    /// Custody (§3.3): `stat(canon)` FOLLOWING symlinks (D1), compared on
    /// `(st_dev, st_ino)`, never on path strings. Every non-Ok outcome is
    /// [`OwnershipLost`] (fail closed). Windows: always `Ok` (documented).
    pub fn custody(&self) -> Result<(), OwnershipLost> {
        if self.latch.is_set() {
            return Err(OwnershipLost {
                reason: LossReason::Latched,
            });
        }
        #[cfg(test)]
        if self.seams.custody_disabled.load(Ordering::SeqCst) {
            return Ok(());
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let mut tries = 0;
            loop {
                tries += 1;
                match std::fs::metadata(&self.canon.0) {
                    Ok(m) if (m.dev(), m.ino()) == self.id => return Ok(()),
                    Ok(m) => {
                        return Err(OwnershipLost {
                            reason: LossReason::Replaced {
                                observed: (m.dev(), m.ino()),
                            },
                        })
                    }
                    Err(e) if e.kind() == io::ErrorKind::Interrupted && tries < 2 => continue,
                    Err(e)
                        if sys::errno_is(&e, libc::ENOENT) || sys::errno_is(&e, libc::ENOTDIR) =>
                    {
                        return Err(OwnershipLost {
                            reason: LossReason::CanonicalAbsent,
                        })
                    }
                    Err(e) => {
                        return Err(OwnershipLost {
                            reason: LossReason::StatError(e.raw_os_error().unwrap_or(-1)),
                        })
                    }
                }
            }
        }
        #[cfg(not(unix))]
        {
            Ok(())
        }
    }

    /// Entry to every mutating op: latch first (no syscall while latched),
    /// then custody; a failed custody check takes the exit-74 path.
    fn guard_mutation(&self) -> Result<(), NotCommitted> {
        if self.latch.is_set() {
            return Err(lost(LossReason::Latched));
        }
        match self.custody() {
            Ok(()) => Ok(()),
            Err(l) => {
                ownership_lost(&self.latch, &self.display(None), self.id, &l.reason);
                Err(lost(l.reason))
            }
        }
    }

    /// Custody for a read-side decision (the delete valve, §3.3a, and the
    /// push gate): the same check, and a failure also takes the exit-74 path.
    pub fn custody_or_lost(&self) -> Result<(), OwnershipLost> {
        self.guard_mutation().map_err(|n| {
            n.0.get_ref()
                .and_then(|i| i.downcast_ref::<OwnershipLost>())
                .cloned()
                .unwrap_or(OwnershipLost {
                    reason: LossReason::Latched,
                })
        })
    }

    /// The held directory has not been unlinked (§3.3a step 3):
    /// `fstat(dirfd).st_nlink > 0` (fast path) AND its parent, reached
    /// through the held fd's `..`, still holds it under its leaf name
    /// (`fstatat(parent, leaf, NOFOLLOW)` equals the held `(dev, ino)`). The
    /// second half exists because APFS keeps `st_nlink > 0` on a removed
    /// directory that is still open (MEASURED on the macOS CI legs); it
    /// stats by NAME so a bind-mounted state dir matches (a `readdir` d_ino
    /// is the underlying mountpoint inode, MEASURED on link). The leaf name
    /// comes from the held canonical path; nothing is re-resolved. Any error
    /// is returned (the caller keeps the valve latched). Windows: `true`.
    pub fn held_dir_linked(&self) -> io::Result<bool> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            use std::os::unix::io::AsRawFd;
            if self.dir.metadata()?.nlink() == 0 {
                return Ok(false);
            }
            let leaf = self
                .canon
                .0
                .file_name()
                .and_then(|n| n.to_str())
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "held dir has no leaf name")
                })?;
            sys::parent_holds_leaf(self.dir.as_raw_fd(), leaf, self.id)
        }
        #[cfg(not(unix))]
        {
            Ok(true)
        }
    }

    /// Operator text ONLY (gate C7 lists every call site outside state_io).
    pub fn display(&self, f: Option<StateFile>) -> DisplayPath {
        match f {
            Some(f) => DisplayPath(self.canon.0.join(f.name()).display().to_string()),
            None => DisplayPath(self.canon.0.display().to_string()),
        }
    }

    /// This boot created the leaf directory.
    pub fn created_this_boot(&self) -> bool {
        self.created
    }

    /// The scope this dir serves.
    pub fn scope(&self) -> Scope {
        self.scope
    }

    /// A directory sync is owed after an uncertain commit (§3.4).
    pub fn dir_sync_pending(&self) -> bool {
        self.dir_sync_pending.load(Ordering::SeqCst)
    }

    /// Sync the held dir after a commit; `Uncertain` sets `dir_sync_pending`
    /// and raises `durable_write_uncertain{file}` (rate-limited).
    fn sync_dir_outcome(&self, name: &str) -> Commit {
        let c = match StateAnchor(self).sync_dir() {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(state_dir = %self.display(None), error = %e, "state_io: directory sync failed after commit; durability uncertain");
                Commit::CommittedDurabilityUncertain
            }
        };
        if c == Commit::CommittedDurabilityUncertain {
            self.note_uncertain(name);
        }
        c
    }

    fn note_uncertain(&self, name: &str) {
        self.dir_sync_pending.store(true, Ordering::SeqCst);
        raise_state_event(StateHealthEvent::DurableWriteUncertain {
            file: name.to_string(),
        });
    }

    /// Re-sync the held directory if a sync is pending (the custody tick).
    pub fn resync_if_pending(&self) {
        if !self.dir_sync_pending.load(Ordering::SeqCst) {
            return;
        }
        if let Ok(Commit::Durable) = StateAnchor(self).sync_dir() {
            self.dir_sync_pending.store(false, Ordering::SeqCst);
        }
    }

    // ----- reads (no custody: a read through the dirfd reads the held inode)

    /// Read a whole state file; `None` when absent. `O_NOFOLLOW|O_NONBLOCK`,
    /// then `S_ISREG` (C2): a symlink, FIFO, socket, device or directory at
    /// the name is an error, never followed and never blocking.
    pub fn read(&self, f: StateFile) -> io::Result<Option<Vec<u8>>> {
        self.check_scope_io(f)?;
        match self.open_read_raw(f.name())? {
            Some(mut file) => {
                let mut buf = Vec::new();
                file.read_to_end(&mut buf)?;
                Ok(Some(buf))
            }
            None => Ok(None),
        }
    }

    /// Open a state file for streaming reads (same flags as [`Self::read`]).
    pub fn open_read(&self, f: StateFile) -> io::Result<Option<File>> {
        self.check_scope_io(f)?;
        self.open_read_raw(f.name())
    }

    fn open_read_raw(&self, name: &str) -> io::Result<Option<File>> {
        #[cfg(test)]
        self.seams.rec("openat", name);
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            let file = match sys::openat(self.dir.as_raw_fd(), name, sys::O_FILE_READ, 0) {
                Ok(f) => f,
                Err(e) if sys::errno_is(&e, libc::ENOENT) => return Ok(None),
                Err(e) => return Err(e),
            };
            ensure_regular(&file, name)?;
            Ok(Some(file))
        }
        #[cfg(not(unix))]
        {
            match std::fs::symlink_metadata(self.path.join(name)) {
                Ok(m) if !m.file_type().is_file() => {
                    return Err(not_regular(name));
                }
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
                Err(e) => return Err(e),
            }
            match File::open(self.path.join(name)) {
                Ok(f) => Ok(Some(f)),
                Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
                Err(e) => Err(e),
            }
        }
    }

    /// `fstatat(AT_SYMLINK_NOFOLLOW)`: ANY directory entry under the name is
    /// "present", a symlink included (C2), so a planted link never reads as
    /// absent.
    pub fn exists(&self, f: StateFile) -> io::Result<bool> {
        self.check_scope_io(f)?;
        #[cfg(test)]
        self.seams.rec("fstatat", f.name());
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            Ok(sys::fstatat_nofollow(self.dir.as_raw_fd(), f.name())?.is_some())
        }
        #[cfg(not(unix))]
        {
            match std::fs::symlink_metadata(self.path.join(f.name())) {
                Ok(_) => Ok(true),
                Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
                Err(e) => Err(e),
            }
        }
    }

    /// Size of the entry under the name (`fstatat`, NOFOLLOW); `None` when
    /// absent.
    pub fn len(&self, f: StateFile) -> io::Result<Option<u64>> {
        self.check_scope_io(f)?;
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            #[allow(clippy::unnecessary_cast)]
            Ok(sys::fstatat_nofollow(self.dir.as_raw_fd(), f.name())?
                .map(|st| st.st_size.max(0) as u64))
        }
        #[cfg(not(unix))]
        {
            match std::fs::symlink_metadata(self.path.join(f.name())) {
                Ok(m) => Ok(Some(m.len())),
                Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
                Err(e) => Err(e),
            }
        }
    }

    // ----- writes (custody first, before any syscall that writes)

    /// Append `bytes` as ONE `write_all`, then `sync_data` (custody first).
    pub fn append(&self, f: StateFile, bytes: &[u8]) -> Result<Commit, NotCommitted> {
        let mut a = self.open_append(f)?;
        a.write_all(bytes)?;
        a.sync_data()
    }

    /// Open a state file for appending. Opens WITHOUT `O_CREAT` first; on
    /// ENOENT creates it with `O_CREAT|O_EXCL` (0600); EEXIST retries the
    /// plain open. Always `O_NOFOLLOW|O_CLOEXEC`, then `S_ISREG`.
    pub fn open_append(&self, f: StateFile) -> Result<StateAppender<'_>, NotCommitted> {
        self.check_scope(f)?;
        self.guard_mutation()?;
        let (file, created) = self.open_or_create(f.name(), false)?;
        Ok(StateAppender {
            sd: self,
            file,
            name: f.name(),
            created,
        })
    }

    /// Create a state file if absent (`O_CREAT|O_EXCL|O_NOFOLLOW`, 0600) and
    /// report whether this call created it (W1 journal touch).
    pub fn ensure_file(&self, f: StateFile) -> Result<bool, NotCommitted> {
        self.check_scope(f)?;
        self.guard_mutation()?;
        let (_file, created) = self.open_or_create(f.name(), false)?;
        if created {
            let _ = self.sync_dir_outcome(f.name());
        }
        Ok(created)
    }

    /// The push-journal lock (1a C3/C6): `O_RDWR|O_CREAT|O_NOFOLLOW|O_CLOEXEC`
    /// 0600, a fresh description per call (never `try_clone`d).
    pub fn open_lock(&self, f: StateFile) -> io::Result<File> {
        if f != StateFile::PushJournalLock {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("open_lock is for PushJournalLock only, not {}", f.name()),
            ));
        }
        self.check_scope_io(f)?;
        self.guard_mutation().map_err(|n| n.0)?;
        Ok(self.open_or_create(f.name(), true).map_err(|n| n.0)?.0)
    }

    fn open_or_create(&self, name: &str, rdwr: bool) -> Result<(File, bool), NotCommitted> {
        #[cfg(test)]
        self.seams.rec("openat", name);
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            let fd = self.dir.as_raw_fd();
            let base = if rdwr {
                libc::O_RDWR
            } else {
                libc::O_WRONLY | libc::O_APPEND
            } | libc::O_NOFOLLOW
                | libc::O_CLOEXEC;
            for _ in 0..3 {
                match sys::openat(fd, name, base, 0) {
                    Ok(file) => {
                        ensure_regular(&file, name).map_err(NotCommitted)?;
                        return Ok((file, false));
                    }
                    Err(e) if sys::errno_is(&e, libc::ENOENT) => {}
                    Err(e) => return Err(NotCommitted(e)),
                }
                match sys::openat(fd, name, base | libc::O_CREAT | libc::O_EXCL, 0o600) {
                    Ok(file) => return Ok((file, true)),
                    Err(e) if sys::errno_is(&e, libc::EEXIST) => continue,
                    Err(e) => return Err(NotCommitted(e)),
                }
            }
            Err(NotCommitted(io::Error::other(format!(
                "open_or_create({name}) raced repeatedly"
            ))))
        }
        #[cfg(not(unix))]
        {
            let p = self.path.join(name);
            if let Ok(m) = std::fs::symlink_metadata(&p) {
                if !m.file_type().is_file() {
                    return Err(NotCommitted(not_regular(name)));
                }
            }
            let mut o = std::fs::OpenOptions::new();
            if rdwr {
                o.read(true).write(true);
            } else {
                o.append(true);
            }
            match o.open(&p) {
                Ok(f) => Ok((f, false)),
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    o.create_new(true);
                    o.open(&p).map(|f| (f, true)).map_err(NotCommitted)
                }
                Err(e) => Err(NotCommitted(e)),
            }
        }
    }

    /// Atomic replace through CS-1 `durable_file` over the private
    /// [`StateAnchor`] (custody at entry and again before `renameat`).
    pub fn replace_atomic(&self, f: StateFile, bytes: &[u8]) -> Result<Commit, NotCommitted> {
        self.check_scope(f)?;
        self.guard_mutation()?;
        let c = durable_file::replace_atomic(&StateAnchor(self), f.name(), bytes)?;
        if c == Commit::CommittedDurabilityUncertain {
            self.note_uncertain(f.name());
        }
        Ok(c)
    }

    /// `unlinkat`; ENOENT is `Ok(Durable)`.
    pub fn remove(&self, f: StateFile) -> Result<Commit, NotCommitted> {
        self.check_scope(f)?;
        self.guard_mutation()?;
        match StateAnchor(self).unlink(f.name()) {
            Ok(()) => Ok(self.sync_dir_outcome(f.name())),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(Commit::Durable),
            Err(e) => Err(NotCommitted(e)),
        }
    }

    /// Test constructor: hold `dir` (which must exist) as a `SyncState` dir
    /// with its own latch.
    #[cfg(test)]
    pub fn for_test(dir: &Path) -> Arc<StateDir> {
        Self::for_test_scoped(dir, Scope::SyncState)
    }

    /// Test helper for code that used to take a state-file PATH: hold the
    /// directory containing `p` (created if missing). The file name in `p`
    /// is ignored; the `StateFile` name is fixed.
    #[cfg(test)]
    pub fn for_test_file(p: &Path) -> Arc<StateDir> {
        let d = p
            .parent()
            .filter(|d| !d.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        std::fs::create_dir_all(d).expect("for_test_file: create dir");
        Self::for_test(d)
    }

    /// Test constructor with an explicit scope.
    #[cfg(test)]
    pub fn for_test_scoped(dir: &Path, scope: Scope) -> Arc<StateDir> {
        let canon = dunce::canonicalize(dir).expect("for_test: dir must exist");
        let (held, id) = open_held(&canon).expect("for_test: open dir");
        Arc::new(StateDir::new_held(
            held,
            id,
            canon,
            scope,
            false,
            Arc::new(Latch::new()),
        ))
    }

    /// Test access to this dir's identity.
    #[cfg(test)]
    #[cfg_attr(not(unix), allow(dead_code))] // used by unix-only tests
    pub(crate) fn test_id(&self) -> (u64, u64) {
        self.id
    }

    /// Test access to this dir's latch.
    #[cfg(test)]
    #[cfg_attr(not(unix), allow(dead_code))] // used by unix-only tests
    pub(crate) fn test_latch(&self) -> &Latch {
        &self.latch
    }

    fn new_held(
        held: Held,
        id: (u64, u64),
        canon: PathBuf,
        scope: Scope,
        created: bool,
        latch: Arc<Latch>,
    ) -> StateDir {
        StateDir {
            #[cfg(unix)]
            dir: held,
            #[cfg(not(unix))]
            path: held,
            id,
            canon: CanonPath(canon),
            scope,
            created,
            dir_sync_pending: AtomicBool::new(false),
            latch,
            #[cfg(test)]
            seams: DirSeams::default(),
        }
    }
}

#[cfg(unix)]
type Held = File;
#[cfg(not(unix))]
type Held = PathBuf;

/// Open an existing directory to hold (follows symlinks: the held directory
/// is the one place they are followed, D1) and read its identity.
#[cfg(unix)]
fn open_held(p: &Path) -> io::Result<(Held, (u64, u64))> {
    use std::os::unix::fs::MetadataExt;
    let f = File::open(p)?;
    let m = f.metadata()?;
    if !m.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "state dir is not a directory",
        ));
    }
    Ok((f, (m.dev(), m.ino())))
}

#[cfg(not(unix))]
fn open_held(p: &Path) -> io::Result<(Held, (u64, u64))> {
    if !std::fs::metadata(p)?.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "state dir is not a directory",
        ));
    }
    Ok((p.to_path_buf(), (0, 0)))
}

fn not_regular(name: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("state file {name} is not a regular file (refused, C2)"),
    )
}

#[cfg(unix)]
fn ensure_regular(file: &File, name: &str) -> io::Result<()> {
    if !file.metadata()?.file_type().is_file() {
        return Err(not_regular(name));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// StateAnchor: the CS-1 anchor over a StateDir, private to this module
// ---------------------------------------------------------------------------

/// The only way `durable_file` reaches a `StateDir` (C8): private, so code
/// outside `state_io` cannot hand a `StateDir` to `durable_file` with an
/// arbitrary name.
struct StateAnchor<'a>(&'a StateDir);

impl Sealed for StateAnchor<'_> {}

impl Anchor for StateAnchor<'_> {
    fn create_tmp_excl(&self, tmp_name: &str, mode: u32) -> io::Result<File> {
        #[cfg(test)]
        self.0.seams.rec("openat", tmp_name);
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            sys::openat(
                self.0.dir.as_raw_fd(),
                tmp_name,
                libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                mode,
            )
        }
        #[cfg(not(unix))]
        {
            let _ = mode;
            std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(self.0.path.join(tmp_name))
        }
    }

    fn rename(&self, from: &str, to: &str) -> io::Result<()> {
        #[cfg(test)]
        self.0.seams.rec("renameat", to);
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            sys::renameat(self.0.dir.as_raw_fd(), from, to)
        }
        #[cfg(not(unix))]
        {
            durable_file::commit_rename(&self.0.path.join(from), &self.0.path.join(to))
        }
    }

    fn unlink(&self, name: &str) -> io::Result<()> {
        #[cfg(test)]
        self.0.seams.rec("unlinkat", name);
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            sys::unlinkat(self.0.dir.as_raw_fd(), name)
        }
        #[cfg(not(unix))]
        {
            std::fs::remove_file(self.0.path.join(name))
        }
    }

    fn is_symlink(&self, name: &str) -> io::Result<bool> {
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            Ok(sys::fstatat_nofollow(self.0.dir.as_raw_fd(), name)?
                .map(|st| sys::is_lnk(&st))
                .unwrap_or(false))
        }
        #[cfg(not(unix))]
        {
            match std::fs::symlink_metadata(self.0.path.join(name)) {
                Ok(m) => Ok(m.file_type().is_symlink()),
                Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
                Err(e) => Err(e),
            }
        }
    }

    fn open_read(&self, name: &str) -> io::Result<Option<File>> {
        self.0.open_read_raw(name)
    }

    fn sync_dir(&self) -> io::Result<Commit> {
        #[cfg(test)]
        self.0.seams.rec("sync_dir", "");
        #[cfg(unix)]
        {
            durable_file::dir_file_durability(&self.0.dir, Path::new(""))
        }
        #[cfg(not(unix))]
        {
            Ok(Commit::CommittedDurabilityUncertain)
        }
    }

    fn mutex_key(&self) -> io::Result<MutexKey> {
        Ok(MutexKey::DevIno(self.0.id.0, self.0.id.1))
    }

    fn pre_commit(&self) -> io::Result<()> {
        self.0.guard_mutation().map_err(|n| n.0)
    }

    #[cfg(test)]
    fn test_path(&self, name: &str) -> PathBuf {
        self.0.canon.0.join(name)
    }
}

// ---------------------------------------------------------------------------
// Bootstrap and registry
// ---------------------------------------------------------------------------

/// The process registry of bootstrapped `StateDir`s, keyed by
/// `(scope, sub)`. [`Registry::get`] never creates anything.
#[derive(Default)]
pub struct Registry {
    sync: HashMap<String, Arc<StateDir>>,
    config: Option<Arc<StateDir>>,
    rejected: HashMap<String, String>,
}

impl Registry {
    /// The sync-state dir of `sub`; `NotBootstrapped` for any sub outside the
    /// bootstrap set S or rejected by validation.
    pub fn get(&self, sub: &str) -> Result<Arc<StateDir>, NotBootstrapped> {
        if let Some(d) = self.sync.get(sub) {
            return Ok(d.clone());
        }
        Err(NotBootstrapped {
            sub: sub.to_string(),
            why: self
                .rejected
                .get(sub)
                .cloned()
                .unwrap_or_else(|| "not in the bootstrap set".to_string()),
        })
    }

    /// The config-scope dir (W13).
    pub fn config_dir(&self) -> Result<Arc<StateDir>, NotBootstrapped> {
        self.config.clone().ok_or_else(|| NotBootstrapped {
            sub: "<config>".to_string(),
            why: self
                .rejected
                .get("<config>")
                .cloned()
                .unwrap_or_else(|| "config dir not bootstrapped".to_string()),
        })
    }

    /// Subscriber ids that bootstrap rejected, with the reason.
    pub fn rejected(&self) -> Vec<(String, String)> {
        let mut v: Vec<_> = self
            .rejected
            .iter()
            .map(|(a, b)| (a.clone(), b.clone()))
            .collect();
        v.sort();
        v
    }

    /// Every registered dir (sync-state dirs and the config dir).
    pub fn all(&self) -> Vec<Arc<StateDir>> {
        let mut v: Vec<_> = self.sync.values().cloned().collect();
        if let Some(c) = &self.config {
            v.push(c.clone());
        }
        v
    }

    /// `(dev, ino)` of every registered dir (the run-time belt's id set).
    fn ids(&self) -> Vec<(u64, u64)> {
        self.all().iter().map(|d| d.id).collect()
    }
}

static REGISTRY: OnceLock<Registry> = OnceLock::new();
static WORKSPACE: OnceLock<PathBuf> = OnceLock::new();

/// Install the bootstrapped registry for the process. A second install is
/// refused (the first one stands) and logged.
pub fn install(reg: Registry, workspace: &Path) -> &'static Registry {
    if REGISTRY.set(reg).is_err() {
        tracing::error!("state_io: registry already installed; keeping the first");
    }
    let _ = WORKSPACE.set(workspace.to_path_buf());
    registry()
}

/// The process registry; empty until [`install`]. Never creates anything.
pub fn registry() -> &'static Registry {
    static EMPTY: OnceLock<Registry> = OnceLock::new();
    REGISTRY
        .get()
        .unwrap_or_else(|| EMPTY.get_or_init(Registry::default))
}

/// C1: a subscriber id must be non-empty, not `.`/`..`, and only
/// `[A-Za-z0-9._-]` (so no `/`, `\`, NUL or `:`).
pub fn validate_sub(sub: &str) -> Result<(), String> {
    if sub.is_empty() {
        return Err("empty subscriber id".to_string());
    }
    if sub == "." || sub == ".." {
        return Err(format!("subscriber id {sub:?} is a dot name"));
    }
    if let Some(b) = sub
        .bytes()
        .find(|b| !(b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-')))
    {
        return Err(format!(
            "subscriber id {sub:?} contains a forbidden byte 0x{b:02x}"
        ));
    }
    Ok(())
}

/// The ONLY place a state directory is created (§3.2).
///
/// Runs synchronously in `.setup()` before any store load or pipeline. For
/// each sub in `subs`: validate (C1), walk `.lattice-runtime/<sub>/sync-state`
/// with `mkdirat(0700)` + `openat(O_DIRECTORY)` (an existing dir keeps its
/// mode), verify the held inode against `stat(canonical)` (3 tries, then
/// exit 74 `bootstrap_race`), observe the filesystem (observe-only), sweep
/// dead-pid CS-1 temps, register. The config dir is opened, never created.
pub fn bootstrap(
    workspace: &Path,
    subs: &[String],
    config_dir: &Path,
) -> Result<Registry, BootstrapError> {
    let latch = process_latch();
    bootstrap_with_latch(workspace, subs, config_dir, latch)
}

fn bootstrap_with_latch(
    workspace: &Path,
    subs: &[String],
    config_dir: &Path,
    latch: Arc<Latch>,
) -> Result<Registry, BootstrapError> {
    // The workspace root itself (above the runtime root) is created if this
    // is a first run: bootstrap is the only creator (it replaces W14's
    // create_dir_all of the journal parent).
    if !workspace.is_dir() {
        let mut b = std::fs::DirBuilder::new();
        b.recursive(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            b.mode(0o700);
        }
        b.create(workspace).map_err(BootstrapError::Workspace)?;
    }
    // Symlinks ABOVE the runtime root are resolved and allowed.
    let ws = dunce::canonicalize(workspace).map_err(BootstrapError::Workspace)?;

    let mut reg = Registry::default();
    let mut seen: Vec<&String> = Vec::new();
    for sub in subs {
        if seen.contains(&sub) {
            continue;
        }
        seen.push(sub);
        if let Err(why) = validate_sub(sub) {
            tracing::error!(subscriber_id = ?sub, reason = %why, "state_io: REJECTED subscriber id; no state dir, no pipeline for it");
            reg.rejected.insert(sub.clone(), why);
            continue;
        }
        match open_sync_state(&ws, sub, &latch) {
            Ok(sd) => {
                if sd.created {
                    raise_state_event(StateHealthEvent::StateDirCreated { sub: sub.clone() });
                }
                sd.sweep_stale_temps();
                reg.sync.insert(sub.clone(), Arc::new(sd));
            }
            Err(e) => {
                tracing::error!(subscriber_id = %sub, error = %e, "state_io: bootstrap failed for subscriber; its pipeline will not start");
                reg.rejected.insert(sub.clone(), e.to_string());
            }
        }
    }

    // Config scope: opened, never created (§3.2 step 5).
    match dunce::canonicalize(config_dir).and_then(|c| open_held(&c).map(|h| (c, h))) {
        Ok((canon, (held, id))) => {
            let sd = StateDir::new_held(held, id, canon, Scope::ConfigDir, false, latch.clone());
            sd.sweep_stale_temps();
            reg.config = Some(Arc::new(sd));
        }
        Err(e) => {
            tracing::warn!(error = %e, "state_io: config dir not available; the reconcile retry ledger will not persist");
            reg.rejected.insert("<config>".to_string(), e.to_string());
        }
    }
    Ok(reg)
}

#[cfg(unix)]
const BOOTSTRAP_TRIES: usize = 3;

#[cfg(unix)]
fn open_sync_state(ws: &Path, sub: &str, latch: &Arc<Latch>) -> io::Result<StateDir> {
    use std::os::unix::fs::MetadataExt;
    use std::os::unix::io::AsRawFd;
    let canon = ws
        .join(layout::RUNTIME_DIR_NAME)
        .join(sub)
        .join(SYNC_STATE_DIR_NAME);
    for _ in 0..BOOTSTRAP_TRIES {
        let mut cur = File::open(ws)?;
        let mut created = false;
        for comp in [layout::RUNTIME_DIR_NAME, sub, SYNC_STATE_DIR_NAME] {
            #[cfg(test)]
            seams::bootstrap_rec("mkdirat", comp);
            created = sys::mkdirat(cur.as_raw_fd(), comp, 0o700)?;
            #[cfg(test)]
            seams::bootstrap_rec("openat", comp);
            cur = sys::openat(cur.as_raw_fd(), comp, sys::O_DIR, 0)?;
        }
        let m = cur.metadata()?;
        let id = (m.dev(), m.ino());
        #[cfg(test)]
        seams::run_after_leaf_open(&canon);
        if let Ok(cm) = std::fs::metadata(&canon) {
            if (cm.dev(), cm.ino()) == id {
                let sd =
                    StateDir::new_held(cur, id, canon, Scope::SyncState, created, latch.clone());
                if let Err(fs_type) = observe_fs(&sd.dir) {
                    tracing::warn!(state_dir = %sd.display(None), fs_type = %fs_type, "state_io: state dir is on an unsupported filesystem (observe-only in CS-2; CS-4 enforces)");
                    raise_state_event(StateHealthEvent::StateFsUnsupported { fs_type });
                }
                return Ok(sd);
            }
        }
        tracing::warn!(subscriber_id = %sub, "state_io: bootstrap identity mismatch (rename race); retrying");
    }
    let shown = DisplayPath(canon.display().to_string());
    ownership_lost(latch, &shown, (0, 0), &LossReason::BootstrapRace);
    Err(io::Error::other(OwnershipLost {
        reason: LossReason::BootstrapRace,
    }))
}

#[cfg(not(unix))]
fn open_sync_state(ws: &Path, sub: &str, latch: &Arc<Latch>) -> io::Result<StateDir> {
    let canon = ws
        .join(layout::RUNTIME_DIR_NAME)
        .join(sub)
        .join(SYNC_STATE_DIR_NAME);
    let created = !canon.is_dir();
    if created {
        std::fs::DirBuilder::new().recursive(true).create(&canon)?;
    }
    let (held, id) = open_held(&canon)?;
    Ok(StateDir::new_held(
        held,
        id,
        canon,
        Scope::SyncState,
        created,
        latch.clone(),
    ))
}

impl StateDir {
    /// Startup stale-temp sweep (§3.2 step 6, CS-1 rule): unlink
    /// `<name>.tmp-<pid>-<nanos>-<seq>` only when the pid is dead and not
    /// ours; keep live-pid temps; leave unparsable names alone. Legacy temp
    /// leftovers are COUNTED and never deleted. Works from `readdir` names
    /// and `fstatat`/`unlinkat` only: it NEVER opens a swept file (C11).
    pub(crate) fn sweep_stale_temps(&self) {
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            let fd = self.dir.as_raw_fd();
            let names = match sys::read_dir_names(fd) {
                Ok(n) => n,
                Err(e) => {
                    tracing::warn!(state_dir = %self.display(None), error = %e, "state_io: stale-temp sweep could not read the dir");
                    return;
                }
            };
            let me = std::process::id();
            let (mut removed, mut kept_live, mut legacy) = (0usize, 0usize, 0usize);
            for name in names {
                if is_legacy_temp(&name) {
                    legacy += 1;
                    continue;
                }
                if !name.contains(".tmp-") {
                    continue;
                }
                let Some(pid) = durable_file::parse_temp_pid(&name) else {
                    legacy += 1;
                    continue;
                };
                if pid == me || durable_file::pid_alive(pid) != Some(false) {
                    kept_live += 1;
                    continue;
                }
                match sys::fstatat_nofollow(fd, &name) {
                    Ok(Some(st)) if sys::is_reg(&st) => {}
                    _ => continue,
                }
                #[cfg(test)]
                self.seams.rec("unlinkat", &name);
                match sys::unlinkat(fd, &name) {
                    Ok(()) => removed += 1,
                    Err(e) => {
                        tracing::warn!(file = %name, error = %e, "state_io: stale-temp unlink failed")
                    }
                }
            }
            if removed + kept_live + legacy > 0 {
                tracing::info!(
                    state_dir = %self.display(None),
                    removed,
                    kept_live,
                    legacy_temps_kept = legacy,
                    "state_io: stale-temp sweep"
                );
            }
        }
    }
}

/// Pre-CS-2 temp shapes (counted, never deleted by CS-2): the 1a journal
/// temp `push_journal.jsonl.tmp-<pid>-<nanos>` (two numeric parts), the SSE
/// `.last_event_id.tmp.<pid>`, the retry ledger `*.json.tmp`, and
/// `NamedTempFile` `.tmp*`.
#[cfg_attr(not(unix), allow(dead_code))]
fn is_legacy_temp(name: &str) -> bool {
    if name.starts_with(".last_event_id.tmp.") || name.ends_with(".json.tmp") {
        return true;
    }
    if name.starts_with(".tmp") {
        return true;
    }
    if let Some(rest) = name.strip_prefix("push_journal.jsonl.tmp-") {
        let parts: Vec<&str> = rest.split('-').collect();
        return parts.len() == 2 && parts.iter().all(|p| p.bytes().all(|b| b.is_ascii_digit()));
    }
    false
}

// ---------------------------------------------------------------------------
// Push gate and custody tick
// ---------------------------------------------------------------------------

/// The push gate (§3.3): custody of EVERY registered dir, immediately before
/// `self.api.push(&req)`. While the latch is set it fails without a syscall.
pub fn custody_all() -> Result<(), OwnershipLost> {
    #[cfg(test)]
    test_hooks::run_before_push_gate();
    if process_latch().is_set() {
        return Err(OwnershipLost {
            reason: LossReason::Latched,
        });
    }
    for d in registry().all() {
        d.custody_or_lost()?;
    }
    Ok(())
}

/// One custody tick over `dirs`: custody (exit 74 on loss), then re-sync any
/// dir whose `dir_sync_pending` is set.
pub fn custody_tick(dirs: &[Arc<StateDir>]) {
    for d in dirs {
        if d.custody_or_lost().is_err() {
            return;
        }
        d.resync_if_pending();
    }
}

/// Spawn the 5 s custody tick over the process registry.
pub fn spawn_custody_tick() {
    tauri::async_runtime::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(5));
        loop {
            tick.tick().await;
            custody_tick(&registry().all());
        }
    });
}

/// Test hooks reachable from other modules' tests (T-CS2-3).
#[cfg(test)]
#[allow(clippy::disallowed_methods)]
pub(crate) mod test_hooks {
    use std::sync::Mutex;

    type Hook = Box<dyn FnMut() + Send>;
    static BEFORE_PUSH_GATE: Mutex<Option<Hook>> = Mutex::new(None);

    #[allow(dead_code)] // used by T-CS2-3 (CS-2 commit c, push gate)
    pub(crate) fn set_before_push_gate(h: Option<Hook>) {
        *BEFORE_PUSH_GATE.lock().unwrap() = h;
    }
    pub(super) fn run_before_push_gate() {
        if let Some(h) = BEFORE_PUSH_GATE.lock().unwrap().as_mut() {
            h();
        }
    }
}

// ---------------------------------------------------------------------------
// Gate 4: the run-time belt
// ---------------------------------------------------------------------------

/// A vault writer was asked to write into state.
#[derive(Debug, thiserror::Error)]
#[error("refused: {target} resolves into a registered state directory (CS-2 gate 4)")]
pub struct ForeignStateWrite {
    pub target: String,
}

/// Gate 4 (§3.5): called by every exempt vault writer before it creates
/// directories or writes. Walks the target's ancestors EXACTLY AS GIVEN (no
/// lexical normalization first, C9), `stat`s each one that exists (following
/// symlinks), and refuses if any `(dev, ino)` equals a registered `StateDir`
/// id (the config dir included). Also refuses a lexical prefix of
/// `<workspace>/state-v2/`. It does NOT refuse `.lattice-runtime/<sub>/shadow/`.
pub fn refuse_state_target(target: &Path) -> Result<(), ForeignStateWrite> {
    #[cfg(test)]
    if let Some(ids) = seams::belt_ids() {
        return refuse_state_target_in(target, &ids, None);
    }
    refuse_state_target_in(
        target,
        &registry().ids(),
        WORKSPACE.get().map(|p| p.as_path()),
    )
}

pub(crate) fn refuse_state_target_in(
    target: &Path,
    ids: &[(u64, u64)],
    workspace: Option<&Path>,
) -> Result<(), ForeignStateWrite> {
    let refuse = || ForeignStateWrite {
        target: target.display().to_string(),
    };
    if let Some(ws) = workspace {
        if target.starts_with(ws.join(STATE_V2_DIR_NAME)) {
            return Err(refuse());
        }
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        // `Path::ancestors` is purely lexical over the components as given:
        // `a/../sync-state/x` yields `a/../sync-state`, which `stat` resolves
        // through the real `..` (no normalization).
        for anc in target.ancestors() {
            if anc.as_os_str().is_empty() {
                continue;
            }
            if let Ok(m) = std::fs::metadata(anc) {
                if ids.contains(&(m.dev(), m.ino())) {
                    return Err(refuse());
                }
            }
        }
    }
    #[cfg(not(unix))]
    let _ = ids;
    Ok(())
}

#[cfg(test)]
#[path = "state_io_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "state_io_owner_tests.rs"]
mod owner_tests;
