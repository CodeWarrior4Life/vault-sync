//! Unit CS-1 tests (spec §5). Each test names the mutation that turns it red.

use super::seams::{self, Point};
use super::*;
use std::sync::mpsc;
use std::time::Duration;

fn temps_in(dir: &Path) -> Vec<String> {
    fs::read_dir(dir)
        .unwrap()
        .flatten()
        .filter_map(|e| e.file_name().to_str().map(str::to_string))
        .filter(|n| n.contains(TEMP_MARKER))
        .collect()
}

fn setup_old(dir: &Path, name: &str, old: &[u8]) -> PathAnchor {
    fs::write(dir.join(name), old).unwrap();
    PathAnchor::new(dir)
}

#[cfg(unix)]
fn mode_of(p: &Path) -> u32 {
    use std::os::unix::fs::PermissionsExt;
    fs::metadata(p).unwrap().permissions().mode() & 0o777
}

/// T-CS1-1: injected `sync_all` failure returns `NotCommitted`; the target is
/// byte-identical and the temp is unlinked.
/// Mutation: ignore the `sync_all` error.
#[test]
fn t_cs1_1_sync_all_failure_is_not_committed_and_target_untouched() {
    seams::reset();
    let d = tempfile::TempDir::new().unwrap();
    let a = setup_old(d.path(), "f", b"OLD-BYTES");
    seams::fail_sync_all(5); // EIO
    let r = replace_atomic(&a, "f", b"NEW-BYTES-LONGER");
    assert!(
        r.is_err(),
        "sync_all failure must be NotCommitted, got {r:?}"
    );
    assert_eq!(fs::read(d.path().join("f")).unwrap(), b"OLD-BYTES");
    assert!(temps_in(d.path()).is_empty(), "temp must be unlinked");
}

/// T-CS1-2 (also runs on the Windows CI leg): injected `rename` failure
/// returns `NotCommitted`; the target is byte-identical and the temp is
/// unlinked. Mutation: return `Ok` after a failed rename.
#[test]
fn t_cs1_2_rename_failure_is_not_committed_and_target_untouched() {
    seams::reset();
    let d = tempfile::TempDir::new().unwrap();
    let a = setup_old(d.path(), "f", b"OLD-BYTES");
    seams::fail_rename(5);
    let r = replace_atomic(&a, "f", b"NEW-BYTES");
    assert!(r.is_err(), "rename failure must be NotCommitted, got {r:?}");
    assert_eq!(fs::read(d.path().join("f")).unwrap(), b"OLD-BYTES");
    assert!(temps_in(d.path()).is_empty(), "temp must be unlinked");
}

/// T-CS1-3: an injected directory-fsync failure other than EINVAL/ENOTSUP
/// returns `CommittedDurabilityUncertain` with the NEW bytes at the target;
/// EINVAL/ENOTSUP give `Durable` on non-Apple unix and
/// `CommittedDurabilityUncertain` on Apple (§4b; see T-CS1-17).
/// Mutation: return `NotCommitted` after the rename.
#[cfg(unix)]
#[test]
fn t_cs1_3_dir_fsync_failure_is_committed_uncertain() {
    seams::reset();
    let d = tempfile::TempDir::new().unwrap();
    let a = setup_old(d.path(), "f", b"OLD");
    seams::fail_dir_fsync(None, 5); // EIO: not tolerable
    let r = replace_atomic(&a, "f", b"NEW");
    assert_eq!(
        r.expect("a post-rename failure is never NotCommitted"),
        Commit::CommittedDurabilityUncertain
    );
    assert_eq!(fs::read(d.path().join("f")).unwrap(), b"NEW");

    #[cfg(any(target_os = "linux", target_os = "android"))]
    let enotsup = 95;
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    let enotsup = 45;
    for tolerable in [22 /* EINVAL */, enotsup] {
        seams::fail_dir_fsync(None, tolerable);
        let r = replace_atomic(&a, "f", b"NEWER");
        let want = if APPLE {
            Commit::CommittedDurabilityUncertain
        } else {
            Commit::Durable
        };
        assert_eq!(r.unwrap(), want, "errno {tolerable} (apple={APPLE})");
        assert_eq!(fs::read(d.path().join("f")).unwrap(), b"NEWER");
    }
}

/// T-CS1-4 (byte half; the config cold-load half is in config.rs):
/// `replace_atomic` writes EXACTLY the given bytes, unframed.
/// Mutation: frame the bytes.
#[test]
fn t_cs1_4_replace_atomic_writes_exact_bytes() {
    seams::reset();
    let d = tempfile::TempDir::new().unwrap();
    let a = PathAnchor::new(d.path());
    for bytes in [
        &b""[..],
        b"x",
        b"nexus_url = \"https://n\"\n",
        &[0u8, 255, 10, 13][..],
    ] {
        let _c = replace_atomic(&a, "f", bytes).unwrap();
        assert_eq!(fs::read(d.path().join("f")).unwrap(), bytes);
    }
}

/// T-CS1-5: `read_framed` returns `Corrupt` on a flipped body byte, a bad hash
/// line and a missing header, and `None` when absent.
/// Mutation: return `None` on a parse error.
#[test]
fn t_cs1_5_read_framed_corrupt_cases() {
    seams::reset();
    let d = tempfile::TempDir::new().unwrap();
    let a = PathAnchor::new(d.path());
    assert!(matches!(read_framed(&a, "absent"), Ok(None)));

    let _c = replace_framed(&a, "f", b"alpha\nbeta").unwrap();
    assert_eq!(read_framed(&a, "f").unwrap().unwrap(), b"alpha\nbeta");
    let good = fs::read(d.path().join("f")).unwrap();
    assert!(good.starts_with(b"v1\n"));

    // Flipped body byte.
    let mut flipped = good.clone();
    *flipped.last_mut().unwrap() ^= 0x01;
    fs::write(d.path().join("f"), &flipped).unwrap();
    assert!(matches!(
        read_framed(&a, "f"),
        Err(FramedReadError::Corrupt(CorruptKind::BadHash))
    ));

    // Bad hash line (not 64 lowercase hex).
    fs::write(d.path().join("f"), b"v1\nnot-a-hash\nalpha\nbeta").unwrap();
    assert!(matches!(
        read_framed(&a, "f"),
        Err(FramedReadError::Corrupt(CorruptKind::BadFrame))
    ));

    // Well-formed hash of the wrong body.
    let wrong = format!("v1\n{}\nalpha\nbeta", "0".repeat(64));
    fs::write(d.path().join("f"), wrong).unwrap();
    assert!(matches!(
        read_framed(&a, "f"),
        Err(FramedReadError::Corrupt(CorruptKind::BadHash))
    ));

    // Missing header.
    fs::write(d.path().join("f"), b"alpha\nbeta").unwrap();
    assert!(matches!(
        read_framed(&a, "f"),
        Err(FramedReadError::Corrupt(CorruptKind::BadFrame))
    ));
    // Empty file: no header.
    fs::write(d.path().join("f"), b"").unwrap();
    assert!(matches!(
        read_framed(&a, "f"),
        Err(FramedReadError::Corrupt(CorruptKind::BadFrame))
    ));
}

fn set_of(items: &[&str]) -> BTreeSet<String> {
    items.iter().map(|s| s.to_string()).collect()
}

fn read_set(a: &PathAnchor, name: &str) -> BTreeSet<String> {
    let body = read_framed(a, name).unwrap().unwrap();
    String::from_utf8(body)
        .unwrap()
        .split('\n')
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

/// T-CS1-6: two threads adding {b} and {c} to {a}, interleaved at the read,
/// give {a,b,c}; a `Corrupt` file is refused and left byte-identical.
/// Mutation: drop the per-path mutex, or overwrite a corrupt file.
#[test]
fn t_cs1_6_monotone_set_interleaved_union_and_corrupt_refusal() {
    seams::reset();
    let d = tempfile::TempDir::new().unwrap();
    let dir = d.path().to_path_buf();
    let _c = replace_monotone_set(&PathAnchor::new(&dir), "set", &set_of(&["a"])).unwrap();

    let (a_read_tx, a_read_rx) = mpsc::channel::<()>();
    let (b_read_tx, b_read_rx) = mpsc::channel::<()>();

    let dir_a = dir.clone();
    let ta = std::thread::spawn(move || {
        seams::set_hook(Box::new(move |p, _| {
            if p == Point::MonotoneAfterRead {
                a_read_tx.send(()).unwrap();
                // Without the mutex B reads now (and signals); with it B is
                // blocked on the lock, so this times out and A writes first.
                let _ = b_read_rx.recv_timeout(Duration::from_millis(400));
            }
        }));
        replace_monotone_set(&PathAnchor::new(&dir_a), "set", &set_of(&["b"])).unwrap()
    });
    let dir_b = dir.clone();
    let tb = std::thread::spawn(move || {
        a_read_rx.recv().unwrap();
        seams::set_hook(Box::new(move |p, _| {
            if p == Point::MonotoneAfterRead {
                let _ = b_read_tx.send(());
            }
        }));
        replace_monotone_set(&PathAnchor::new(&dir_b), "set", &set_of(&["c"])).unwrap()
    });
    let _ = ta.join().unwrap();
    let _ = tb.join().unwrap();
    assert_eq!(
        read_set(&PathAnchor::new(&dir), "set"),
        set_of(&["a", "b", "c"])
    );

    // Corrupt existing file: refused, byte-identical.
    let corrupt = b"v1\nzzzz\nwhatever".to_vec();
    fs::write(dir.join("set"), &corrupt).unwrap();
    let r = replace_monotone_set(&PathAnchor::new(&dir), "set", &set_of(&["d"]));
    assert!(matches!(r, Err(MonotoneError::Corrupt(_))), "got {r:?}");
    assert_eq!(fs::read(dir.join("set")).unwrap(), corrupt);

    // Invalid element refused before any I/O.
    assert!(matches!(
        replace_monotone_set(&PathAnchor::new(&dir), "set2", &set_of(&["x\ny"])),
        Err(MonotoneError::InvalidElement)
    ));
}

/// T-CS1-7 (process-crash torn-file, NOT power loss): stopping at each crash
/// point (mid temp write, after the temp write, after `sync_all`, after the
/// rename = before the dir fsync) and re-reading leaves a WHOLE old or new
/// file, never a partial one.
/// Mutation: write the target in place instead of through a temp.
#[test]
fn t_cs1_7_process_crash_points_leave_whole_old_or_new() {
    let old = b"OLD-OLD-OLD-OLD-OLD-OLD".to_vec();
    let new = b"NEW-new-NEW-new-NEW-new-NEW-new".to_vec();
    for (p, expect_new) in [
        (Point::MidWrite, false),
        (Point::AfterWrite, false),
        (Point::AfterSync, false),
        (Point::AfterRename, true),
    ] {
        seams::reset();
        let d = tempfile::TempDir::new().unwrap();
        let a = setup_old(d.path(), "f", &old);
        seams::crash_at(p);
        let _ = replace_atomic(&a, "f", &new);
        let got = fs::read(d.path().join("f")).unwrap();
        assert!(got == old || got == new, "torn file at {p:?}: {got:?}");
        assert_eq!(
            got == new,
            expect_new,
            "wrong side of the commit point at {p:?}"
        );
    }
}

/// T-CS1-10 (C4): the temp is mode 0600 from the open (checked BEFORE the
/// write), the final file is 0600, and a created anchor dir is 0700.
/// Mutation: open with the default mode then chmod.
#[cfg(unix)]
#[test]
fn t_cs1_10_temp_is_0600_from_open_and_created_dir_is_0700() {
    seams::reset();
    let d = tempfile::TempDir::new().unwrap();
    let dir = d.path().join("fresh").join("anchor");
    let seen = std::rc::Rc::new(std::cell::Cell::new(false));
    let seen2 = seen.clone();
    seams::set_hook(Box::new(move |p, path| {
        if p == Point::AfterOpen {
            assert_eq!(
                fs::metadata(path).unwrap().len(),
                0,
                "checked before the write"
            );
            assert_eq!(mode_of(path), 0o600, "temp must be 0600 from the open");
            seen2.set(true);
        }
    }));
    let _c = replace_atomic(&PathAnchor::new(&dir), "f", b"secret").unwrap();
    assert!(seen.get(), "AfterOpen hook must have run");
    assert_eq!(mode_of(&dir.join("f")), 0o600);
    assert_eq!(mode_of(&dir), 0o700, "a created anchor dir is 0700");
}

/// T-CS1-11 (C4): a symlink planted at the temp name, and a symlinked target,
/// are both refused with the target (and the symlink's victim) unchanged.
/// Mutation: follow symlinks.
#[cfg(unix)]
#[test]
fn t_cs1_11_symlinks_at_temp_and_target_are_refused() {
    use std::os::unix::fs::symlink;
    seams::reset();
    let d = tempfile::TempDir::new().unwrap();
    let outside = tempfile::TempDir::new().unwrap();
    let victim = outside.path().join("victim");
    fs::write(&victim, b"VICTIM").unwrap();
    let a = setup_old(d.path(), "f", b"OLD");

    // (a) symlink at every temp name the writer tries.
    symlink(&victim, d.path().join("f.tmp-planted")).unwrap();
    seams::force_temp_names(&["f.tmp-planted", "f.tmp-planted", "f.tmp-planted"]);
    let r = replace_atomic(&a, "f", b"NEW");
    assert!(r.is_err(), "planted temp symlink must be refused: {r:?}");
    assert_eq!(fs::read(&victim).unwrap(), b"VICTIM");
    assert_eq!(fs::read(d.path().join("f")).unwrap(), b"OLD");
    assert!(fs::symlink_metadata(d.path().join("f.tmp-planted"))
        .unwrap()
        .file_type()
        .is_symlink());

    // (b) the target itself is a symlink.
    seams::reset();
    let t = d.path().join("linked");
    symlink(&victim, &t).unwrap();
    let r = replace_atomic(&a, "linked", b"NEW");
    assert!(r.is_err(), "symlinked target must be refused: {r:?}");
    assert!(fs::symlink_metadata(&t).unwrap().file_type().is_symlink());
    assert_eq!(fs::read(&victim).unwrap(), b"VICTIM");
}

/// T-CS1-12 (C6): a forced EEXIST at the temp name retries with a fresh name
/// and never opens the existing temp; three collisions give `NotCommitted`.
/// Mutation: open without `create_new`.
#[test]
fn t_cs1_12_eexist_retries_fresh_name_never_reopens() {
    seams::reset();
    let d = tempfile::TempDir::new().unwrap();
    let a = setup_old(d.path(), "f", b"OLD");
    let taken = d.path().join("f.tmp-taken");
    fs::write(&taken, b"SENTINEL").unwrap();

    seams::force_temp_names(&["f.tmp-taken"]);
    let _c = replace_atomic(&a, "f", b"NEW").expect("retry with a fresh name succeeds");
    assert_eq!(fs::read(d.path().join("f")).unwrap(), b"NEW");
    assert_eq!(
        fs::read(&taken).unwrap(),
        b"SENTINEL",
        "existing temp never opened"
    );

    seams::force_temp_names(&["f.tmp-taken", "f.tmp-taken", "f.tmp-taken"]);
    let r = replace_atomic(&a, "f", b"NEWER");
    assert!(r.is_err(), "three collisions must be NotCommitted");
    assert_eq!(fs::read(d.path().join("f")).unwrap(), b"NEW");
    assert_eq!(fs::read(&taken).unwrap(), b"SENTINEL");
}

/// T-CS1-12 support (C6): temp names are unique within a process (seq).
#[test]
fn temp_names_are_unique_and_parse_back_to_this_pid() {
    let n1 = temp_name("state.toml");
    let n2 = temp_name("state.toml");
    assert_ne!(n1, n2);
    assert_eq!(parse_temp_pid(&n1), Some(std::process::id()));
}

/// T-CS1-13 (C3): real-filesystem outcome per OS: `Durable` on non-Apple
/// unix; on Apple `Durable` iff `F_FULLFSYNC` succeeded (§4b, T-CS1-17);
/// `CommittedDurabilityUncertain` on Windows (no write-through rename).
#[test]
fn t_cs1_13_real_filesystem_outcome_per_os() {
    seams::reset();
    let d = tempfile::TempDir::new().unwrap();
    let a = PathAnchor::new(d.path());
    let c = replace_atomic(&a, "f", b"bytes").unwrap();
    #[cfg(all(unix, not(target_vendor = "apple")))]
    assert_eq!(c, Commit::Durable);
    #[cfg(target_vendor = "apple")]
    {
        let fallback = seams::events()
            .iter()
            .any(|e| e == "dir_sync_branch:AppleFallbackFsync");
        let want = if fallback {
            Commit::CommittedDurabilityUncertain
        } else {
            Commit::Durable
        };
        assert_eq!(c, want);
    }
    #[cfg(windows)]
    assert_eq!(c, Commit::CommittedDurabilityUncertain);
    assert_eq!(fs::read(d.path().join("f")).unwrap(), b"bytes");
}

/// T-CS1-13 (C3, logic half, runs on every OS): a sharing violation is
/// retried 5 times 50 ms apart and then returned as an ERROR; other errors
/// return at once. Mutation: map a Windows sharing violation to `Ok`.
#[test]
fn t_cs1_13_sharing_violation_retry_is_bounded_and_never_ok() {
    let mut attempts = 0;
    let mut sleeps = Vec::new();
    let r = rename_retrying_sharing_violation(
        || {
            attempts += 1;
            Err(io::Error::from_raw_os_error(ERROR_SHARING_VIOLATION))
        },
        |d| sleeps.push(d),
    );
    assert!(r.is_err(), "a persistent sharing violation is an error");
    assert_eq!(attempts, 5);
    assert_eq!(sleeps, vec![Duration::from_millis(50); 4]);

    let mut attempts = 0;
    let r = rename_retrying_sharing_violation(
        || {
            attempts += 1;
            if attempts < 3 {
                Err(io::Error::from_raw_os_error(ERROR_SHARING_VIOLATION))
            } else {
                Ok(())
            }
        },
        |_| {},
    );
    assert!(r.is_ok());
    assert_eq!(attempts, 3);

    let mut attempts = 0;
    let r = rename_retrying_sharing_violation(
        || {
            attempts += 1;
            Err(io::Error::from_raw_os_error(5))
        },
        |_| {},
    );
    assert!(r.is_err());
    assert_eq!(attempts, 1, "non-sharing errors are not retried");
}

/// T-CS1-13 (C3, Windows real-filesystem): a target held open with no
/// sharing is NotCommitted with the target unchanged and no temp left.
#[cfg(windows)]
#[test]
fn t_cs1_13_windows_locked_target_is_not_committed() {
    use std::os::windows::fs::OpenOptionsExt;
    seams::reset();
    let d = tempfile::TempDir::new().unwrap();
    let a = setup_old(d.path(), "f", b"OLD");
    let _hold = OpenOptions::new()
        .read(true)
        .share_mode(0)
        .open(d.path().join("f"))
        .unwrap();
    let r = replace_atomic(&a, "f", b"NEW");
    assert!(
        r.is_err(),
        "a locked target must not report a commit: {r:?}"
    );
    drop(_hold);
    assert_eq!(fs::read(d.path().join("f")).unwrap(), b"OLD");
    assert!(temps_in(d.path()).is_empty());
}

/// T-CS1-16 (sweep): a dead-pid temp is removed; a live-pid temp (a spawned
/// sleeper), this process's own temp, an unparsable temp-like name and a
/// regular file are kept. Mutation: sweep by name pattern only.
#[cfg(unix)]
#[test]
fn t_cs1_16_startup_sweep_removes_only_dead_pid_temps() {
    seams::reset();
    let d = tempfile::TempDir::new().unwrap();
    let dead_pid = {
        let mut c = std::process::Command::new("true").spawn().unwrap();
        let pid = c.id();
        c.wait().unwrap();
        pid
    };
    let mut sleeper = std::process::Command::new("sleep")
        .arg("30")
        .spawn()
        .unwrap();
    let live_pid = sleeper.id();
    let me = std::process::id();

    let dead = format!("state.toml.tmp-{dead_pid}-123-0");
    let live = format!("state.toml.tmp-{live_pid}-123-0");
    let mine = format!("state.toml.tmp-{me}-123-0");
    let unparsable = "state.toml.tmp-notapid-x".to_string();
    for n in [&dead, &live, &mine, &unparsable] {
        fs::write(d.path().join(n), b"partial").unwrap();
    }
    fs::write(d.path().join("state.toml"), b"real").unwrap();

    startup_maintenance(&[d.path(), d.path()]);

    let _ = sleeper.kill();
    let _ = sleeper.wait();
    assert!(
        !d.path().join(&dead).exists(),
        "dead-pid temp must be removed"
    );
    assert!(d.path().join(&live).exists(), "live-pid temp must be kept");
    assert!(d.path().join(&mine).exists(), "own temp must be kept");
    assert!(
        d.path().join(&unparsable).exists(),
        "unparsable name must be kept"
    );
    assert_eq!(fs::read(d.path().join("state.toml")).unwrap(), b"real");
}

#[test]
fn parse_temp_pid_shapes() {
    assert_eq!(parse_temp_pid("a.tmp-12-34-5"), Some(12));
    assert_eq!(parse_temp_pid("a.tmp-12-34"), None);
    assert_eq!(parse_temp_pid("a.tmp-12-34-5-6"), None);
    assert_eq!(parse_temp_pid("a.tmp-x-34-5"), None);
    assert_eq!(parse_temp_pid("a.tmp-0-34-5"), None);
    assert_eq!(parse_temp_pid(".tmp-12-34-5"), None);
}

#[test]
fn invalid_names_are_not_committed() {
    seams::reset();
    let d = tempfile::TempDir::new().unwrap();
    let a = PathAnchor::new(d.path());
    for bad in ["", ".", "..", "a/b", "a\\b"] {
        assert!(
            replace_atomic(&a, bad, b"x").is_err(),
            "{bad:?} must be refused"
        );
    }
}

#[test]
fn commit_weaker() {
    use Commit::*;
    assert_eq!(Durable.weaker(Durable), Durable);
    assert_eq!(
        Durable.weaker(CommittedDurabilityUncertain),
        CommittedDurabilityUncertain
    );
    assert_eq!(
        CommittedDurabilityUncertain.weaker(Durable),
        CommittedDurabilityUncertain
    );
}

/// T-CS1-17 (§4b, pure half, runs on EVERY OS): on Apple, a directory
/// `F_FULLFSYNC` that fails with EINVAL/ENOTTY/ENOTSUP/EOPNOTSUPP falls back to
/// plain `fsync(dirfd)` and gives `CommittedDurabilityUncertain`; only a
/// successful `F_FULLFSYNC` is `Durable`; any other error is an error.
/// Non-Apple unix keeps EINVAL/ENOTSUP as `Durable`.
/// Mutation: map an Apple ENOTSUP to `Durable`.
#[test]
fn t_cs1_17_apple_dir_durability_rule() {
    for errno in [
        45,  /* ENOTSUP */
        102, /* EOPNOTSUPP */
        22,  /* EINVAL */
        25,  /* ENOTTY */
    ] {
        let mut fallback_calls = 0;
        let (branch, out) =
            classify_dir_sync(true, Err(io::Error::from_raw_os_error(errno)), || {
                fallback_calls += 1;
                Ok(())
            });
        assert_eq!(branch, DirSyncBranch::AppleFallbackFsync, "errno {errno}");
        assert_eq!(
            out.unwrap(),
            Commit::CommittedDurabilityUncertain,
            "apple errno {errno} must NOT be Durable"
        );
        assert_eq!(fallback_calls, 1, "plain fsync(dirfd) fallback runs once");
    }
    // A fallback that itself fails is still Uncertain (never Durable).
    let (_, out) = classify_dir_sync(true, Err(io::Error::from_raw_os_error(45)), || {
        Err(io::Error::from_raw_os_error(5))
    });
    assert_eq!(out.unwrap(), Commit::CommittedDurabilityUncertain);
    // F_FULLFSYNC success is the only Apple Durable.
    let (branch, out) = classify_dir_sync(true, Ok(()), || panic!("no fallback on success"));
    assert_eq!(branch, DirSyncBranch::AppleFullFsync);
    assert_eq!(out.unwrap(), Commit::Durable);
    // Any other Apple error is an error (replace_atomic maps it to Uncertain).
    let (branch, out) = classify_dir_sync(true, Err(io::Error::from_raw_os_error(5)), || {
        panic!("no fallback on EIO")
    });
    assert_eq!(branch, DirSyncBranch::Failed);
    assert!(out.is_err());
    // Non-Apple unix: EINVAL stays tolerated as Durable (unchanged rule).
    // Unix only: raw 22 is EINVAL there, but a different Windows error code,
    // and Windows never reaches this classifier (dir_durability is cfg(unix)).
    #[cfg(unix)]
    {
        let (branch, out) = classify_dir_sync(false, Err(io::Error::from_raw_os_error(22)), || {
            panic!("no fallback off Apple")
        });
        assert_eq!(branch, DirSyncBranch::ToleratedUnsupported);
        assert_eq!(out.unwrap(), Commit::Durable);
    }
}

/// T-CS1-17 (§4b, seam half): a fault seam forces the ENOTSUP branch through
/// the real `replace_atomic` path; the recorded branch and outcome follow the
/// rule for this OS.
#[cfg(unix)]
#[test]
fn t_cs1_17_forced_enotsup_through_replace_atomic() {
    seams::reset();
    let d = tempfile::TempDir::new().unwrap();
    let a = PathAnchor::new(d.path());
    let enotsup = if cfg!(any(target_os = "linux", target_os = "android")) {
        95
    } else {
        45
    };
    seams::fail_dir_fsync(None, enotsup);
    let c = replace_atomic(&a, "f", b"x").unwrap();
    let ev = seams::events();
    if APPLE {
        assert!(
            ev.contains(&"dir_sync_branch:AppleFallbackFsync".to_string()),
            "{ev:?}"
        );
        assert_eq!(c, Commit::CommittedDurabilityUncertain);
    } else {
        assert!(
            ev.contains(&"dir_sync_branch:ToleratedUnsupported".to_string()),
            "{ev:?}"
        );
        assert_eq!(c, Commit::Durable);
    }
}

/// T-CS1-17 (§4b, real filesystem): records WHICH branch fired on this host
/// (printed; the macOS CI legs run this with --nocapture) and asserts the
/// outcome matches the rule for that branch.
#[cfg(unix)]
#[test]
fn t_cs1_17_real_filesystem_branch_is_recorded() {
    seams::reset();
    let d = tempfile::TempDir::new().unwrap();
    let c = replace_atomic(&PathAnchor::new(d.path()), "f", b"x").unwrap();
    let ev = seams::events();
    let branch = ev
        .iter()
        .find_map(|e| e.strip_prefix("dir_sync_branch:"))
        .expect("a branch is always recorded")
        .to_string();
    eprintln!(
        "T-CS1-17 {}/{}: directory sync branch = {branch}, outcome = {c:?}",
        std::env::consts::OS,
        std::env::consts::ARCH
    );
    let want = match branch.as_str() {
        "AppleFullFsync" | "Synced" | "ToleratedUnsupported" => Commit::Durable,
        "AppleFallbackFsync" | "Failed" => Commit::CommittedDurabilityUncertain,
        other => panic!("unexpected branch {other}"),
    };
    assert_eq!(c, want);
    if APPLE {
        assert!(
            branch.starts_with("Apple"),
            "Apple must take an Apple branch: {branch}"
        );
    } else {
        assert!(!branch.starts_with("Apple"), "non-Apple must not: {branch}");
    }
}
