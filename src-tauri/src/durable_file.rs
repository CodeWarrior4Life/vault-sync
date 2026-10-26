//! Unit CS-1 (TKT-23091e72): durable small-file writes with TYPED outcomes.
//!
//! Every small-file writer goes through this module. The outcome type keeps
//! two facts apart that a plain `io::Result<()>` conflates:
//!
//! - `Ok(Commit::Durable)`: the NEW bytes are visible at the target and the
//!   containing directory was fsynced (or the filesystem refuses directory
//!   fsync, which is tolerated).
//! - `Ok(Commit::CommittedDurabilityUncertain)`: the NEW bytes are visible
//!   (the rename, which is the commit point, succeeded), but durability across
//!   power loss could not be established.
//! - `Err(NotCommitted)`: the previous file is byte-identical; nothing at the
//!   target changed and the temp has been unlinked.
//!
//! Replace steps: a temp `<name>.tmp-<pid>-<nanos>-<seq>` opened with
//! `create_new` (mode 0600 on unix, in the same open); write; `sync_all`;
//! `rename` (the commit point); directory fsync. The target itself is never
//! opened, and a target that is a symlink is refused.
//!
//! The framed format (`v1\n<sha256-hex(body)>\n<body>`) is used only for
//! files this design creates. It is NEVER used for `config.toml`, which
//! `Config::load_from` reads as raw TOML.

use sha2::{Digest, Sha256};
use std::collections::{BTreeSet, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use thiserror::Error;

/// Successful replace: the NEW bytes are visible at the target.
#[must_use = "a Commit says whether the write is durable; inspect it"]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Commit {
    /// The new bytes are visible and the directory entry was fsynced (or the
    /// filesystem does not support directory fsync, EINVAL/ENOTSUP).
    Durable,
    /// The new bytes are visible but durability across power loss is not
    /// established (directory fsync failed, or the platform cannot prove it).
    CommittedDurabilityUncertain,
}

impl Commit {
    /// The weaker of two outcomes: `Durable` only if both are `Durable`.
    pub fn weaker(self, other: Commit) -> Commit {
        if self == Commit::Durable && other == Commit::Durable {
            Commit::Durable
        } else {
            Commit::CommittedDurabilityUncertain
        }
    }
}

/// Failed replace: the previous file at the target is byte-identical.
#[must_use = "a NotCommitted means the write did not happen"]
#[derive(Debug, Error)]
#[error("not committed (previous file untouched): {0}")]
pub struct NotCommitted(pub io::Error);

/// A directory that small files are written into. CS-1 has [`PathAnchor`];
/// CS-2 adds `StateDir`.
pub trait Anchor {
    fn dir(&self) -> &Path;
}

/// An anchor that is just a directory path.
#[derive(Debug, Clone)]
pub struct PathAnchor(PathBuf);

impl PathAnchor {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        PathAnchor(dir.into())
    }

    /// Split a file path into its anchor (parent dir, `.` if none) and name.
    pub fn split(path: &Path) -> io::Result<(PathAnchor, String)> {
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("not a writable file path: {}", path.display()),
                )
            })?
            .to_string();
        let dir = match path.parent() {
            Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
            _ => PathBuf::from("."),
        };
        Ok((PathAnchor(dir), name))
    }
}

impl Anchor for PathAnchor {
    fn dir(&self) -> &Path {
        &self.0
    }
}

/// Error reading a framed file.
#[derive(Debug, Error)]
pub enum FramedReadError {
    #[error("io error reading framed file: {0}")]
    Io(io::Error),
    #[error("framed file is corrupt: {0}")]
    Corrupt(CorruptKind),
}

/// Why a framed file was judged corrupt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CorruptKind {
    /// Header or hash line missing or malformed.
    BadFrame,
    /// The body does not hash to the recorded digest.
    BadHash,
}

impl std::fmt::Display for CorruptKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CorruptKind::BadFrame => f.write_str("bad frame"),
            CorruptKind::BadHash => f.write_str("bad hash"),
        }
    }
}

/// Error from [`replace_monotone_set`].
#[derive(Debug, Error)]
pub enum MonotoneError {
    /// The existing file is corrupt; the write was refused and the file is
    /// byte-identical.
    #[error("monotone set refused: existing file is corrupt ({0})")]
    Corrupt(CorruptKind),
    /// An element is empty or contains a newline (it would break the format).
    #[error("monotone set element is empty or contains a newline")]
    InvalidElement,
    /// Reading, locking or canonicalizing failed; nothing was written.
    #[error("monotone set io error: {0}")]
    Io(io::Error),
    #[error(transparent)]
    NotCommitted(#[from] NotCommitted),
}

const TEMP_MARKER: &str = ".tmp-";
const TEMP_OPEN_TRIES: usize = 3;
static TEMP_SEQ: AtomicU64 = AtomicU64::new(0);

fn temp_name(name: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let seq = TEMP_SEQ.fetch_add(1, Ordering::Relaxed);
    format!("{name}{TEMP_MARKER}{}-{nanos}-{seq}", std::process::id())
}

fn validate_name(name: &str) -> io::Result<()> {
    if name.is_empty()
        || name == "."
        || name == ".."
        || name.contains('/')
        || name.contains('\\')
        || name.contains('\0')
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid durable-file name: {name:?}"),
        ));
    }
    Ok(())
}

/// Create the anchor dir if it is missing; a directory created here is 0700
/// on unix (C4).
fn ensure_dir(dir: &Path) -> io::Result<()> {
    if dir.is_dir() {
        return Ok(());
    }
    let mut b = fs::DirBuilder::new();
    b.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        b.mode(0o700);
    }
    b.create(dir)
}

/// Open a fresh temp with `create_new` (never re-opening an existing path)
/// and mode 0600 in the same open on unix. EEXIST retries with a fresh name
/// up to [`TEMP_OPEN_TRIES`] times (C6).
fn open_temp(dir: &Path, name: &str) -> io::Result<(File, PathBuf)> {
    let mut last_err = None;
    for _ in 0..TEMP_OPEN_TRIES {
        #[cfg(test)]
        let candidate = seams::next_forced_temp_name().unwrap_or_else(|| temp_name(name));
        #[cfg(not(test))]
        let candidate = temp_name(name);
        let path = dir.join(&candidate);
        let mut o = OpenOptions::new();
        o.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            o.mode(0o600);
        }
        match o.open(&path) {
            Ok(f) => return Ok((f, path)),
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                tracing::debug!(temp = %path.display(), "temp name exists; retrying with a fresh name");
                last_err = Some(e);
            }
            Err(e) => return Err(e),
        }
    }
    Err(last_err.unwrap_or_else(|| io::Error::other("temp open retries exhausted")))
}

/// Windows `ERROR_SHARING_VIOLATION`.
const ERROR_SHARING_VIOLATION: i32 = 32;
const SHARING_RETRIES: usize = 5;
const SHARING_BACKOFF: std::time::Duration = std::time::Duration::from_millis(50);

/// C3: run a rename, retrying a sharing violation up to 5 times 50 ms apart.
/// Every error that survives is returned as an error (the caller maps it to
/// `NotCommitted`); a sharing violation is NEVER treated as success.
#[cfg_attr(not(any(windows, test)), allow(dead_code))]
fn rename_retrying_sharing_violation(
    mut op: impl FnMut() -> io::Result<()>,
    mut sleep: impl FnMut(std::time::Duration),
) -> io::Result<()> {
    let mut attempt = 0;
    loop {
        attempt += 1;
        match op() {
            Ok(()) => return Ok(()),
            Err(e) if e.raw_os_error() == Some(ERROR_SHARING_VIOLATION) => {
                if attempt >= SHARING_RETRIES {
                    return Err(e);
                }
                sleep(SHARING_BACKOFF);
            }
            Err(e) => return Err(e),
        }
    }
}

fn commit_rename(from: &Path, to: &Path) -> io::Result<()> {
    #[cfg(test)]
    if let Some(e) = seams::take_rename_failure() {
        return Err(e);
    }
    #[cfg(windows)]
    {
        rename_retrying_sharing_violation(|| fs::rename(from, to), std::thread::sleep)
    }
    #[cfg(not(windows))]
    {
        fs::rename(from, to)
    }
}

fn sync_temp(f: &File) -> io::Result<()> {
    #[cfg(test)]
    if let Some(e) = seams::take_sync_all_failure() {
        return Err(e);
    }
    f.sync_all()
}

/// Apple targets get the §4b directory-durability rule. Unix only: every
/// reader sits in a `cfg(unix)` block, so Windows would flag it dead.
#[cfg(unix)]
const APPLE: bool = cfg!(target_vendor = "apple");

/// Apple errno values for "F_FULLFSYNC is not supported on this fd" (§4b):
/// EINVAL, ENOTTY, ENOTSUP, EOPNOTSUPP. Spelled as numbers so the pure
/// classifier (and its test) compiles and runs on every OS.
const APPLE_FULLFSYNC_UNSUPPORTED: &[i32] = &[22, 25, 45, 102];

/// Which branch a directory sync took (recorded by tests; T-CS1-17).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DirSyncBranch {
    /// Non-Apple unix: `fsync(dirfd)` succeeded.
    Synced,
    /// Non-Apple unix: EINVAL/ENOTSUP, tolerated as `Durable` (unchanged 1a rule).
    ToleratedUnsupported,
    /// Apple: `F_FULLFSYNC` on the directory fd succeeded.
    AppleFullFsync,
    /// Apple: `F_FULLFSYNC` unsupported; fell back to plain `fsync(dirfd)`,
    /// which does not flush the drive cache, so durability is uncertain.
    AppleFallbackFsync,
    /// Any other error.
    Failed,
}

/// Pure §4b classification of a directory-sync attempt. `first` is the
/// result of the primary sync (`F_FULLFSYNC` on Apple, `fsync` elsewhere);
/// `fallback` is plain `fsync(dirfd)`, run only on the Apple-unsupported path.
#[cfg_attr(not(any(unix, test)), allow(dead_code))]
fn classify_dir_sync(
    apple: bool,
    first: io::Result<()>,
    fallback: impl FnOnce() -> io::Result<()>,
) -> (DirSyncBranch, io::Result<Commit>) {
    match first {
        Ok(()) if apple => (DirSyncBranch::AppleFullFsync, Ok(Commit::Durable)),
        Ok(()) => (DirSyncBranch::Synced, Ok(Commit::Durable)),
        Err(e)
            if apple
                && e.raw_os_error()
                    .is_some_and(|c| APPLE_FULLFSYNC_UNSUPPORTED.contains(&c)) =>
        {
            if let Err(fe) = fallback() {
                tracing::debug!(error = %fe, "fallback fsync(dirfd) after unsupported F_FULLFSYNC failed");
            }
            (
                DirSyncBranch::AppleFallbackFsync,
                Ok(Commit::CommittedDurabilityUncertain),
            )
        }
        Err(e) if !apple && dir_fsync_tolerable(&e) => {
            tracing::debug!(error = %e, "directory fsync unsupported here; tolerated");
            (DirSyncBranch::ToleratedUnsupported, Ok(Commit::Durable))
        }
        Err(e) => (DirSyncBranch::Failed, Err(e)),
    }
}

/// The primary directory sync: `F_FULLFSYNC` on Apple, `fsync` elsewhere.
#[cfg(unix)]
fn primary_dir_sync(f: &File) -> io::Result<()> {
    #[cfg(target_vendor = "apple")]
    {
        use std::os::unix::io::AsRawFd;
        // SAFETY: fcntl on an fd we own for the duration of the call.
        let rc = unsafe { libc::fcntl(f.as_raw_fd(), libc::F_FULLFSYNC) };
        if rc == -1 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
    #[cfg(not(target_vendor = "apple"))]
    {
        f.sync_all()
    }
}

/// Plain `fsync(dirfd)`: the Apple fallback (§4b).
#[cfg(unix)]
fn plain_fsync(f: &File) -> io::Result<()> {
    use std::os::unix::io::AsRawFd;
    // SAFETY: fsync on an fd we own for the duration of the call.
    if unsafe { libc::fsync(f.as_raw_fd()) } == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Sync a directory so a rename that just completed inside it is durable, and
/// say how durable (§4b). Unix only: on Windows opening a directory as a file
/// fails with access denied, so it reports `CommittedDurabilityUncertain`.
fn dir_durability(dir: &Path) -> io::Result<Commit> {
    #[cfg(unix)]
    {
        let d = if dir.as_os_str().is_empty() {
            Path::new(".")
        } else {
            dir
        };
        let (branch, out) = match File::open(d) {
            Ok(f) => {
                #[cfg(test)]
                let first = match seams::take_dir_fsync_failure(d) {
                    Some(e) => Err(e),
                    None => primary_dir_sync(&f),
                };
                #[cfg(not(test))]
                let first = primary_dir_sync(&f);
                classify_dir_sync(APPLE, first, || plain_fsync(&f))
            }
            Err(e) => classify_dir_sync(APPLE, Err(e), || Ok(())),
        };
        #[cfg(test)]
        seams::record_event(format!("dir_sync_branch:{branch:?}"));
        let _ = branch;
        out
    }
    #[cfg(not(unix))]
    {
        let _ = dir;
        Ok(Commit::CommittedDurabilityUncertain)
    }
}

/// Fsync a directory after a rename inside it. The ONE directory-fsync
/// implementation in the tree (C8; absorbed from Unit 1a's
/// `push_journal::fsync_dir`), kept for 1a's callers (the push journal and
/// `flush_durable`, which move onto `durable_file` in CS-2). Tolerated
/// outcomes (non-Apple EINVAL/ENOTSUP, the Apple fallback, non-unix) are
/// `Ok`; any other error is returned.
pub(crate) fn fsync_dir(dir: &Path) -> io::Result<()> {
    dir_durability(dir).map(|_| ())
}

#[cfg_attr(not(any(unix, test)), allow(dead_code))]
fn dir_fsync_tolerable(e: &io::Error) -> bool {
    // EINVAL maps to InvalidInput. ENOTSUP / EOPNOTSUPP are matched by raw
    // value because std does not give them a stable ErrorKind.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    const ENOTSUP: &[i32] = &[95];
    #[cfg(any(
        target_os = "macos",
        target_os = "ios",
        target_os = "freebsd",
        target_os = "openbsd",
        target_os = "netbsd"
    ))]
    const ENOTSUP: &[i32] = &[45, 102];
    #[cfg(not(any(
        target_os = "linux",
        target_os = "android",
        target_os = "macos",
        target_os = "ios",
        target_os = "freebsd",
        target_os = "openbsd",
        target_os = "netbsd"
    )))]
    const ENOTSUP: &[i32] = &[];
    matches!(
        e.kind(),
        io::ErrorKind::InvalidInput | io::ErrorKind::Unsupported
    ) || e.raw_os_error().is_some_and(|c| ENOTSUP.contains(&c))
}

/// Atomically replace `<anchor>/<name>` with exactly `bytes`.
///
/// `Err(NotCommitted)` guarantees the previous file is byte-identical and the
/// temp is gone. `Ok` means the new bytes are visible; see [`Commit`].
pub fn replace_atomic(a: &impl Anchor, name: &str, bytes: &[u8]) -> Result<Commit, NotCommitted> {
    #[cfg(test)]
    seams::record_call(name);
    let dir = a.dir();
    validate_name(name).map_err(NotCommitted)?;
    ensure_dir(dir).map_err(NotCommitted)?;
    let target = dir.join(name);
    // C4: refuse a symlinked target. `rename` would replace the link itself,
    // but a link at a path we own is a planted redirection; refuse it.
    match fs::symlink_metadata(&target) {
        Ok(m) if m.file_type().is_symlink() => {
            return Err(NotCommitted(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("refusing to replace a symlink: {}", target.display()),
            )));
        }
        Ok(_) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(NotCommitted(e)),
    }

    let (mut f, temp) = open_temp(dir, name).map_err(NotCommitted)?;
    let abandon = |e: io::Error, temp: &Path| -> NotCommitted {
        if let Err(rm) = fs::remove_file(temp) {
            tracing::warn!(temp = %temp.display(), error = %rm, "could not unlink temp after a failed replace");
        }
        NotCommitted(e)
    };
    #[cfg(test)]
    if let Some(e) = seams::at(seams::Point::AfterOpen, &temp) {
        return Err(e);
    }

    let (first, second) = bytes.split_at(bytes.len() / 2);
    if let Err(e) = f.write_all(first) {
        return Err(abandon(e, &temp));
    }
    #[cfg(test)]
    if let Some(e) = seams::at(seams::Point::MidWrite, &temp) {
        return Err(e);
    }
    if let Err(e) = f.write_all(second) {
        return Err(abandon(e, &temp));
    }
    #[cfg(test)]
    if let Some(e) = seams::at(seams::Point::AfterWrite, &temp) {
        return Err(e);
    }
    if let Err(e) = sync_temp(&f) {
        drop(f);
        return Err(abandon(e, &temp));
    }
    drop(f);
    #[cfg(test)]
    if let Some(e) = seams::at(seams::Point::AfterSync, &temp) {
        return Err(e);
    }
    // The rename is the commit point.
    if let Err(e) = commit_rename(&temp, &target) {
        return Err(abandon(e, &temp));
    }
    #[cfg(test)]
    if let Some(e) = seams::at(seams::Point::AfterRename, &target) {
        return Err(e);
    }
    Ok(post_commit_durability(dir))
}

/// After the rename: the new bytes are visible. The directory sync decides
/// `Durable` vs `CommittedDurabilityUncertain` (§3, §4b); any error after the
/// commit point is `CommittedDurabilityUncertain`, never `NotCommitted`.
/// Windows has no write-through rename available here (no `windows-sys`
/// dependency), so it reports `CommittedDurabilityUncertain` (C3).
fn post_commit_durability(dir: &Path) -> Commit {
    match dir_durability(dir) {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(dir = %dir.display(), error = %e, "directory fsync failed after commit; durability uncertain");
            Commit::CommittedDurabilityUncertain
        }
    }
}

const FRAME_HEADER: &str = "v1";

fn frame(body: &[u8]) -> Vec<u8> {
    let digest = hex::encode(Sha256::digest(body));
    let mut out = Vec::with_capacity(body.len() + 70);
    out.extend_from_slice(FRAME_HEADER.as_bytes());
    out.push(b'\n');
    out.extend_from_slice(digest.as_bytes());
    out.push(b'\n');
    out.extend_from_slice(body);
    out
}

fn unframe(raw: &[u8]) -> Result<Vec<u8>, CorruptKind> {
    let nl1 = raw
        .iter()
        .position(|&b| b == b'\n')
        .ok_or(CorruptKind::BadFrame)?;
    if &raw[..nl1] != FRAME_HEADER.as_bytes() {
        return Err(CorruptKind::BadFrame);
    }
    let rest = &raw[nl1 + 1..];
    let nl2 = rest
        .iter()
        .position(|&b| b == b'\n')
        .ok_or(CorruptKind::BadFrame)?;
    let hash = &rest[..nl2];
    if hash.len() != 64 || !hash.iter().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
        return Err(CorruptKind::BadFrame);
    }
    let body = &rest[nl2 + 1..];
    if hex::encode(Sha256::digest(body)).as_bytes() != hash {
        return Err(CorruptKind::BadHash);
    }
    Ok(body.to_vec())
}

/// Atomically replace `<anchor>/<name>` with the framed `body`.
pub fn replace_framed(a: &impl Anchor, name: &str, body: &[u8]) -> Result<Commit, NotCommitted> {
    replace_atomic(a, name, &frame(body))
}

/// Read a framed file. `Ok(None)` when it does not exist; `Corrupt` on a bad
/// frame or hash (NEVER `None`, which would read as "absent").
pub fn read_framed(a: &impl Anchor, name: &str) -> Result<Option<Vec<u8>>, FramedReadError> {
    validate_name(name).map_err(FramedReadError::Io)?;
    let raw = match fs::read(a.dir().join(name)) {
        Ok(r) => r,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(FramedReadError::Io(e)),
    };
    unframe(&raw).map(Some).map_err(FramedReadError::Corrupt)
}

fn monotone_lock(key: PathBuf) -> Arc<Mutex<()>> {
    static LOCKS: OnceLock<Mutex<HashMap<PathBuf, Arc<Mutex<()>>>>> = OnceLock::new();
    let map = LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut g = map.lock().unwrap_or_else(|p| p.into_inner());
    g.entry(key).or_default().clone()
}

/// Union `add` into the framed newline-joined set at `<anchor>/<name>`.
///
/// SINGLE-PROCESS ONLY UNTIL THE CS-4 LEASE (C7). Exclusion is a per
/// (canonical dir, name) in-process mutex held across read, union and
/// replace; there is NO cross-process exclusion here. Any caller added
/// before CS-4 needs a gate review. A `Corrupt` existing file refuses the
/// write and is left byte-identical.
#[cfg_attr(not(test), allow(dead_code))] // no caller until CS-2/CS-4 (C7)
pub(crate) fn replace_monotone_set(
    a: &impl Anchor,
    name: &str,
    add: &BTreeSet<String>,
) -> Result<Commit, MonotoneError> {
    if add.iter().any(|s| s.is_empty() || s.contains('\n')) {
        return Err(MonotoneError::InvalidElement);
    }
    validate_name(name).map_err(MonotoneError::Io)?;
    ensure_dir(a.dir()).map_err(MonotoneError::Io)?;
    // C6: canonicalize failure is an error, never a raw-path fallback.
    let key = fs::canonicalize(a.dir())
        .map_err(MonotoneError::Io)?
        .join(name);
    let lock = monotone_lock(key);
    let _held = lock.lock().unwrap_or_else(|p| p.into_inner());

    let existing = match read_framed(a, name) {
        Ok(Some(body)) => body,
        Ok(None) => Vec::new(),
        Err(FramedReadError::Corrupt(k)) => return Err(MonotoneError::Corrupt(k)),
        Err(FramedReadError::Io(e)) => return Err(MonotoneError::Io(e)),
    };
    #[cfg(test)]
    if let Some(e) = seams::at(seams::Point::MonotoneAfterRead, &a.dir().join(name)) {
        return Err(MonotoneError::Io(e.0));
    }
    let text =
        String::from_utf8(existing).map_err(|_| MonotoneError::Corrupt(CorruptKind::BadFrame))?;
    let mut set: BTreeSet<String> = text
        .split('\n')
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect();
    set.extend(add.iter().cloned());
    let body = set.into_iter().collect::<Vec<_>>().join("\n");
    Ok(replace_framed(a, name, body.as_bytes())?)
}

/// Parse the pid out of a temp name `<name>.tmp-<pid>-<nanos>-<seq>`.
/// `None` when the name does not have exactly that shape.
fn parse_temp_pid(file_name: &str) -> Option<u32> {
    let idx = file_name.rfind(TEMP_MARKER)?;
    if idx == 0 {
        return None;
    }
    let tail = &file_name[idx + TEMP_MARKER.len()..];
    let parts: Vec<&str> = tail.split('-').collect();
    if parts.len() != 3
        || parts
            .iter()
            .any(|p| p.is_empty() || !p.bytes().all(|b| b.is_ascii_digit()))
    {
        return None;
    }
    let pid: u32 = parts[0].parse().ok()?;
    parts[1].parse::<u128>().ok()?;
    parts[2].parse::<u64>().ok()?;
    (pid > 0).then_some(pid)
}

/// Is `pid` a live process? `None` when liveness cannot be determined, which
/// the sweep treats as alive.
fn pid_alive(pid: u32) -> Option<bool> {
    #[cfg(unix)]
    {
        let p = i32::try_from(pid).ok()?;
        if p <= 0 {
            return None;
        }
        // SAFETY: kill(pid, 0) sends no signal; it only checks existence and
        // permission. No memory is touched.
        let rc = unsafe { libc::kill(p, 0) };
        if rc == 0 {
            return Some(true);
        }
        match io::Error::last_os_error().raw_os_error() {
            Some(libc::ESRCH) => Some(false),
            Some(libc::EPERM) => Some(true),
            _ => None,
        }
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
        None
    }
}

/// Startup stale-temp sweep for one anchor dir: unlink `*.tmp-<pid>-*` temps
/// whose pid is NOT alive and is not this process. A live (or undeterminable)
/// pid's temp is never touched; an unparsable temp-like name is kept and
/// logged at WARN.
pub fn sweep_stale_temps(dir: &Path) {
    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return,
        Err(e) => {
            tracing::warn!(dir = %dir.display(), error = %e, "stale-temp sweep: cannot read dir");
            return;
        }
    };
    let me = std::process::id();
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if !name.contains(TEMP_MARKER) {
            continue;
        }
        let Some(pid) = parse_temp_pid(name) else {
            tracing::warn!(dir = %dir.display(), file = name, "stale-temp sweep: unparsable temp name; left alone");
            continue;
        };
        if pid == me || pid_alive(pid) != Some(false) {
            continue;
        }
        let path = entry.path();
        match fs::symlink_metadata(&path) {
            Ok(m) if m.file_type().is_file() => {}
            _ => continue,
        }
        match fs::remove_file(&path) {
            Ok(()) => {
                tracing::info!(file = %path.display(), pid, "stale-temp sweep: removed dead-pid temp")
            }
            Err(e) => {
                tracing::warn!(file = %path.display(), error = %e, "stale-temp sweep: unlink failed")
            }
        }
    }
}

/// Best-effort startup maintenance for the anchor dirs this unit writes:
/// sweep dead-pid temps, then fsync the dir (unix; same tolerance). This only
/// narrows the window after an `Uncertain` write; it does not prove
/// durability. Duplicate dirs are visited once.
pub fn startup_maintenance(dirs: &[&Path]) {
    let mut seen: Vec<&Path> = Vec::new();
    for &d in dirs {
        if seen.contains(&d) {
            continue;
        }
        seen.push(d);
        if !d.is_dir() {
            continue;
        }
        sweep_stale_temps(d);
        if let Err(e) = fsync_dir(d) {
            tracing::warn!(dir = %d.display(), error = %e, "startup directory fsync failed (best effort)");
        }
        #[cfg(test)]
        seams::record_event(format!("startup_fsync:{}", d.display()));
    }
}

/// Test-only fault injection and observation points. All state is
/// THREAD-LOCAL so parallel tests never see each other's faults.
#[cfg(test)]
pub(crate) mod seams {
    use super::NotCommitted;
    use std::cell::RefCell;
    use std::collections::VecDeque;
    use std::io;
    use std::path::{Path, PathBuf};

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) enum Point {
        AfterOpen,
        MidWrite,
        AfterWrite,
        AfterSync,
        AfterRename,
        MonotoneAfterRead,
    }

    pub(crate) type Hook = Box<dyn FnMut(Point, &Path)>;

    #[derive(Default)]
    struct State {
        sync_all_failure: Option<i32>,
        rename_failure: Option<i32>,
        /// (dir file-name suffix match, raw os error)
        dir_fsync_failure: Option<(Option<PathBuf>, i32)>,
        forced_temp_names: VecDeque<String>,
        crash_at: Option<Point>,
        hook: Option<Hook>,
        calls: Vec<String>,
        events: Vec<String>,
    }

    thread_local! {
        static STATE: RefCell<State> = RefCell::new(State::default());
    }

    pub(crate) fn reset() {
        STATE.with(|s| *s.borrow_mut() = State::default());
    }
    pub(crate) fn fail_sync_all(raw: i32) {
        STATE.with(|s| s.borrow_mut().sync_all_failure = Some(raw));
    }
    pub(crate) fn fail_rename(raw: i32) {
        STATE.with(|s| s.borrow_mut().rename_failure = Some(raw));
    }
    /// Fail the next directory fsync (of `dir`, or any dir if `None`).
    pub(crate) fn fail_dir_fsync(dir: Option<PathBuf>, raw: i32) {
        STATE.with(|s| s.borrow_mut().dir_fsync_failure = Some((dir, raw)));
    }
    pub(crate) fn force_temp_names(names: &[&str]) {
        STATE.with(|s| {
            s.borrow_mut()
                .forced_temp_names
                .extend(names.iter().map(|n| n.to_string()))
        });
    }
    /// Simulate the process stopping at `p`: return early WITHOUT cleanup.
    pub(crate) fn crash_at(p: Point) {
        STATE.with(|s| s.borrow_mut().crash_at = Some(p));
    }
    pub(crate) fn set_hook(h: Hook) {
        STATE.with(|s| s.borrow_mut().hook = Some(h));
    }
    pub(crate) fn calls() -> Vec<String> {
        STATE.with(|s| s.borrow().calls.clone())
    }
    pub(crate) fn events() -> Vec<String> {
        STATE.with(|s| s.borrow().events.clone())
    }
    pub(crate) fn record_event(e: String) {
        STATE.with(|s| s.borrow_mut().events.push(e));
    }

    pub(super) fn record_call(name: &str) {
        STATE.with(|s| s.borrow_mut().calls.push(name.to_string()));
    }
    pub(super) fn next_forced_temp_name() -> Option<String> {
        STATE.with(|s| s.borrow_mut().forced_temp_names.pop_front())
    }
    pub(super) fn take_sync_all_failure() -> Option<io::Error> {
        STATE.with(|s| {
            s.borrow_mut()
                .sync_all_failure
                .take()
                .map(io::Error::from_raw_os_error)
        })
    }
    pub(super) fn take_rename_failure() -> Option<io::Error> {
        STATE.with(|s| {
            s.borrow_mut()
                .rename_failure
                .take()
                .map(io::Error::from_raw_os_error)
        })
    }
    #[cfg_attr(not(unix), allow(dead_code))]
    pub(super) fn take_dir_fsync_failure(dir: &Path) -> Option<io::Error> {
        STATE.with(|s| {
            let mut st = s.borrow_mut();
            match &st.dir_fsync_failure {
                Some((None, _)) => st
                    .dir_fsync_failure
                    .take()
                    .map(|(_, raw)| io::Error::from_raw_os_error(raw)),
                Some((Some(want), _)) if want == dir => st
                    .dir_fsync_failure
                    .take()
                    .map(|(_, raw)| io::Error::from_raw_os_error(raw)),
                _ => None,
            }
        })
    }

    /// Run the hook at `p`; return a simulated-crash error if `crash_at(p)`.
    pub(super) fn at(p: Point, path: &Path) -> Option<NotCommitted> {
        let hook = STATE.with(|s| s.borrow_mut().hook.take());
        if let Some(mut h) = hook {
            h(p, path);
            STATE.with(|s| {
                let mut st = s.borrow_mut();
                if st.hook.is_none() {
                    st.hook = Some(h);
                }
            });
        }
        let crash = STATE.with(|s| s.borrow().crash_at == Some(p));
        crash.then(|| NotCommitted(io::Error::other(format!("simulated process stop at {p:?}"))))
    }
}

#[cfg(test)]
#[path = "durable_file_tests.rs"]
mod tests;
