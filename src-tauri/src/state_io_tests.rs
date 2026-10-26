//! Unit CS-2 (TKT-23091e72) tests for the `state_io` core. Owner-level tests
//! (journal, stores, ledger, marker, SSE, retry ledger) live beside their
//! owners and in `state_io_owner_tests.rs`.
#![allow(clippy::disallowed_methods)]

use super::*;
use std::sync::atomic::Ordering;
use tempfile::TempDir;

/// Bootstrap `subs` under a fresh workspace with a PRIVATE latch, so a
/// recorded exit 74 never leaks into other tests.
pub(crate) fn boot(subs: &[&str]) -> (TempDir, TempDir, Registry, Arc<Latch>) {
    let ws = TempDir::new().unwrap();
    let cfg = TempDir::new().unwrap();
    let latch = Arc::new(Latch::new());
    let subs: Vec<String> = subs.iter().map(|s| s.to_string()).collect();
    let reg = bootstrap_with_latch(ws.path(), &subs, cfg.path(), latch.clone()).unwrap();
    (ws, cfg, reg, latch)
}

pub(crate) fn leaf(ws: &Path, sub: &str) -> PathBuf {
    dunce::canonicalize(ws)
        .unwrap()
        .join(layout::RUNTIME_DIR_NAME)
        .join(sub)
        .join(SYNC_STATE_DIR_NAME)
}

pub(crate) fn names(dir: &Path) -> Vec<String> {
    let mut v: Vec<String> = std::fs::read_dir(dir)
        .map(|rd| {
            rd.map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
                .collect()
        })
        .unwrap_or_default();
    v.sort();
    v
}

fn exits(l: &Latch) -> Vec<(i32, LossReason)> {
    l.exits.lock().unwrap().clone()
}

/// T-CS2-21: no path accessor on `StateDir` or `DisplayPath`.
#[test]
fn t_cs2_21_no_path_accessor() {
    static_assertions::assert_not_impl_any!(StateDir: AsRef<Path>, Into<PathBuf>, Clone);
    static_assertions::assert_not_impl_any!(
        DisplayPath: AsRef<Path>,
        Into<PathBuf>,
        std::ops::Deref<Target = str>
    );
}

/// T-CS2-12 (state_io half): with the leaf present and the journal absent,
/// `append` creates the FILE in the held dir; an unknown sub is
/// `NotBootstrapped` and nothing is created.
#[test]
fn t_cs2_12_append_creates_file_registry_never_creates() {
    let (ws, _cfg, reg, _l) = boot(&["sub-a"]);
    let d = leaf(ws.path(), "sub-a");
    let sd = reg.get("sub-a").unwrap();
    assert!(!d.join("push_journal.jsonl").exists());
    let _c = sd.append(StateFile::PushJournal, b"x\n").unwrap();
    assert_eq!(std::fs::read(d.join("push_journal.jsonl")).unwrap(), b"x\n");
    let before = names(&ws.path().join(layout::RUNTIME_DIR_NAME));
    assert!(reg.get("sub-unknown").is_err());
    assert!(registry().get("sub-never-installed").is_err());
    assert_eq!(names(&ws.path().join(layout::RUNTIME_DIR_NAME)), before);
}

/// T-CS2-13: a fresh workspace gives `created=true`, `state_dir_created`
/// and 0700 components; an existing 0755 tree gives `created=false` with its
/// mode unchanged.
#[cfg(unix)]
#[test]
fn t_cs2_13_bootstrap_created_and_modes() {
    use std::os::unix::fs::PermissionsExt;
    let mode = |p: &Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
    let (ws, _cfg, reg, _l) = boot(&["sub-t13-fresh"]);
    let sd = reg.get("sub-t13-fresh").unwrap();
    assert!(sd.created_this_boot());
    assert!(
        crate::sync_health::state_events().contains(&StateHealthEvent::StateDirCreated {
            sub: "sub-t13-fresh".into()
        })
    );
    let rt = dunce::canonicalize(ws.path())
        .unwrap()
        .join(layout::RUNTIME_DIR_NAME);
    for p in [
        rt.clone(),
        rt.join("sub-t13-fresh"),
        rt.join("sub-t13-fresh").join(SYNC_STATE_DIR_NAME),
    ] {
        assert_eq!(mode(&p), 0o700, "{}", p.display());
    }

    // Existing 0755 tree: mode unchanged, created=false.
    let ws2 = TempDir::new().unwrap();
    let l2 = ws2
        .path()
        .join(layout::RUNTIME_DIR_NAME)
        .join("sub-t13-old")
        .join(SYNC_STATE_DIR_NAME);
    std::fs::create_dir_all(&l2).unwrap();
    for p in [l2.clone(), l2.parent().unwrap().to_path_buf()] {
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    let cfg = TempDir::new().unwrap();
    let reg2 = bootstrap_with_latch(
        ws2.path(),
        &["sub-t13-old".into()],
        cfg.path(),
        Arc::new(Latch::new()),
    )
    .unwrap();
    assert!(!reg2.get("sub-t13-old").unwrap().created_this_boot());
    assert_eq!(mode(&l2), 0o755);
    assert_eq!(mode(l2.parent().unwrap()), 0o755);
}

/// T-CS2-14: a seam renames the leaf between `openat` and the post-open
/// `stat`: bootstrap retries and ends holding the inode now at the canonical
/// path; after 3 forced mismatches it exits 74 with `bootstrap_race`.
#[cfg(unix)]
#[test]
fn t_cs2_14_verify_after_open() {
    use std::os::unix::fs::MetadataExt;
    seams::reset();
    let ws = TempDir::new().unwrap();
    let cfg = TempDir::new().unwrap();
    let n = std::rc::Rc::new(std::cell::Cell::new(0u32));
    let n2 = n.clone();
    seams::set_after_leaf_open(Some(Box::new(move |p: &Path| {
        if n2.get() == 0 {
            std::fs::rename(p, p.with_file_name("moved-away")).unwrap();
        }
        n2.set(n2.get() + 1);
    })));
    let latch = Arc::new(Latch::new());
    let reg =
        bootstrap_with_latch(ws.path(), &["sub14".into()], cfg.path(), latch.clone()).unwrap();
    seams::set_after_leaf_open(None);
    let sd = reg.get("sub14").expect("retry must succeed");
    assert_eq!(n.get(), 2, "one mismatch then one clean open");
    let at_path = std::fs::metadata(leaf(ws.path(), "sub14")).unwrap();
    assert_eq!(sd.id, (at_path.dev(), at_path.ino()));
    assert!(exits(&latch).is_empty());

    // Three forced mismatches: exit 74 bootstrap_race.
    let ws = TempDir::new().unwrap();
    seams::set_after_leaf_open(Some(Box::new(move |p: &Path| {
        let _ = std::fs::remove_dir_all(p.with_file_name("gone"));
        std::fs::rename(p, p.with_file_name("gone")).unwrap();
    })));
    let latch = Arc::new(Latch::new());
    let reg =
        bootstrap_with_latch(ws.path(), &["sub14b".into()], cfg.path(), latch.clone()).unwrap();
    seams::set_after_leaf_open(None);
    assert!(reg.get("sub14b").is_err());
    assert_eq!(
        exits(&latch),
        vec![(EXIT_OWNERSHIP_LOST, LossReason::BootstrapRace)]
    );
}

/// T-CS2-17: injected FUSE, NFS and SMB magics raise
/// `state_fs_unsupported{type}` and bootstrap continues with no writes
/// outside the state dir.
#[cfg(unix)]
#[test]
fn t_cs2_17_fs_observation_is_observe_only() {
    for (magic, label) in [(0x6573_5546u64, "fuse"), (0x6969, "nfs"), (0x517B, "smb")] {
        seams::reset();
        seams::force_fs(Some(seams::FsProbe::Magic(magic)));
        let before = crate::sync_health::state_events()
            .iter()
            .filter(|e| {
                **e == StateHealthEvent::StateFsUnsupported {
                    fs_type: label.into(),
                }
            })
            .count();
        let (ws, _cfg, reg, l) = boot(&["sub17"]);
        seams::force_fs(None);
        assert!(reg.get("sub17").is_ok(), "bootstrap continues");
        assert!(exits(&l).is_empty());
        let after = crate::sync_health::state_events()
            .iter()
            .filter(|e| {
                **e == StateHealthEvent::StateFsUnsupported {
                    fs_type: label.into(),
                }
            })
            .count();
        assert!(after > before, "{label}: state_fs_unsupported raised");
        assert_eq!(names(ws.path()), vec![layout::RUNTIME_DIR_NAME.to_string()]);
        assert!(names(&leaf(ws.path(), "sub17")).is_empty());
    }
    // The Apple probe path (f_fstypename) classifies the same way.
    seams::force_fs(Some(seams::FsProbe::Name("smbfs".into())));
    let (_ws, _cfg, reg, _l) = boot(&["sub17n"]);
    seams::force_fs(None);
    assert!(reg.get("sub17n").is_ok());
    assert!(
        crate::sync_health::state_events().contains(&StateHealthEvent::StateFsUnsupported {
            fs_type: "smbfs".into()
        })
    );
    assert_eq!(classify_fs_magic(0x9123_683E), Ok("btrfs"));
    assert_eq!(classify_fs_name("apfs"), Ok("apfs"));
}

/// T-CS2-29 (C1): invalid subscriber ids are rejected with zero `mkdirat`
/// calls for them, while the valid id bootstraps normally.
#[test]
fn t_cs2_29_subscriber_id_validation() {
    seams::reset();
    let bad = ["", ".", "..", "../x", "a/b", "a\\b", "a:b", "a\0b", "é"];
    let mut subs: Vec<&str> = bad.to_vec();
    subs.push("good-sub_1.x");
    let (ws, _cfg, reg, _l) = boot(&subs);
    for b in bad {
        let e = reg.get(b).unwrap_err();
        assert!(!e.why.is_empty(), "{b:?}");
        assert!(reg.rejected().iter().any(|(s, _)| s == b), "{b:?}");
    }
    assert!(reg.get("good-sub_1.x").is_ok());
    #[cfg(unix)]
    {
        let calls = seams::bootstrap_calls();
        let mk: Vec<&String> = calls.iter().filter(|c| c.starts_with("mkdirat:")).collect();
        assert_eq!(
            mk,
            vec![
                &format!("mkdirat:{}", layout::RUNTIME_DIR_NAME),
                &"mkdirat:good-sub_1.x".to_string(),
                &format!("mkdirat:{SYNC_STATE_DIR_NAME}"),
            ],
            "zero mkdirat for any rejected id"
        );
    }
    assert_eq!(
        names(&ws.path().join(layout::RUNTIME_DIR_NAME)),
        vec!["good-sub_1.x".to_string()]
    );
}

/// T-CS2-27: a ConfigDir member on a SyncState dir is
/// `NotCommitted(InvalidInput)` and writes nothing.
#[test]
fn t_cs2_27_scope_guard() {
    let d = TempDir::new().unwrap();
    let sd = StateDir::for_test(d.path());
    let e = sd
        .replace_atomic(StateFile::ReconcileRetryLedger, b"{}")
        .unwrap_err();
    assert_eq!(e.0.kind(), io::ErrorKind::InvalidInput);
    let e = sd
        .append(StateFile::ReconcileRetryLedger, b"x")
        .unwrap_err();
    assert_eq!(e.0.kind(), io::ErrorKind::InvalidInput);
    assert!(sd.read(StateFile::ReconcileRetryLedger).is_err());
    let c = StateDir::for_test_scoped(d.path(), Scope::ConfigDir);
    assert!(c.replace_atomic(StateFile::Shadow, b"{}").is_err());
    assert!(names(d.path()).is_empty(), "nothing written");
}

/// Move the held dir away and recreate an empty one at the path.
#[cfg(unix)]
fn move_and_recreate(d: &Path) -> PathBuf {
    let moved = d.with_file_name("sync-state.moved");
    std::fs::rename(d, &moved).unwrap();
    std::fs::create_dir(d).unwrap();
    moved
}

/// T-CS2-5 (T-S6, RENAME_EXCHANGE): swapping the held A with an empty B
/// makes the next commit `OwnershipLost`; with custody bypassed for one
/// write, that write lands in the held inode (now named B) and A stays
/// empty. Custody compares `(dev, ino)`, never path strings.
#[cfg(any(target_os = "linux", target_vendor = "apple"))]
#[test]
fn t_cs2_5_rename_exchange() {
    let (ws, _cfg, reg, latch) = boot(&["sub5"]);
    let a = leaf(ws.path(), "sub5");
    let b = a.with_file_name("B");
    std::fs::create_dir(&b).unwrap();
    let sd = reg.get("sub5").unwrap();
    let (ca, cb) = (sys_cstr(&a), sys_cstr(&b));
    #[cfg(target_os = "linux")]
    // SAFETY: valid C strings; AT_FDCWD.
    let rc = unsafe {
        libc::renameat2(
            libc::AT_FDCWD,
            ca.as_ptr(),
            libc::AT_FDCWD,
            cb.as_ptr(),
            libc::RENAME_EXCHANGE,
        )
    };
    #[cfg(target_vendor = "apple")]
    // SAFETY: valid C strings.
    let rc = unsafe { libc::renamex_np(ca.as_ptr(), cb.as_ptr(), libc::RENAME_SWAP) };
    assert_eq!(rc, 0, "exchange: {}", io::Error::last_os_error());

    // Seam-bypassed write lands in the held inode, now named B.
    sd.seams.custody_disabled.store(true, Ordering::SeqCst);
    let _c = sd.append(StateFile::DeleteLedger, b"x\n").unwrap();
    sd.seams.custody_disabled.store(false, Ordering::SeqCst);
    assert_eq!(names(&b), vec!["delete-burst-SUPPRESSED".to_string()]);
    assert!(names(&a).is_empty(), "A stays empty");

    let e = sd.append(StateFile::DeleteLedger, b"y\n").unwrap_err();
    assert!(is_ownership_lost(&e.0));
    assert!(matches!(
        exits(&latch).as_slice(),
        [(74, LossReason::Replaced { .. })]
    ));
    assert!(names(&a).is_empty());
}

#[cfg(unix)]
fn sys_cstr(p: &Path) -> std::ffi::CString {
    use std::os::unix::ffi::OsStrExt;
    std::ffi::CString::new(p.as_os_str().as_bytes()).unwrap()
}

/// T-CS2-25: a symlinked layout keeps custody `Ok` (intermediate AND leaf
/// symlinks); retargeting the symlink makes the next commit `Replaced` and
/// the process exits 74. Custody FOLLOWS symlinks (D1).
#[cfg(unix)]
#[test]
fn t_cs2_25_symlinked_layout() {
    // Intermediate symlink: <ws>/.lattice-runtime -> <real>.
    let ws = TempDir::new().unwrap();
    let real = TempDir::new().unwrap();
    std::os::unix::fs::symlink(real.path(), ws.path().join(layout::RUNTIME_DIR_NAME)).unwrap();
    let cfg = TempDir::new().unwrap();
    let latch = Arc::new(Latch::new());
    let reg = bootstrap_with_latch(ws.path(), &["s25".into()], cfg.path(), latch.clone()).unwrap();
    let sd = reg.get("s25").unwrap();
    for _ in 0..100 {
        assert!(sd.custody().is_ok());
    }
    let _c = sd.append(StateFile::DeleteLedger, b"a\n").unwrap();
    assert!(real
        .path()
        .join("s25/sync-state/delete-burst-SUPPRESSED")
        .exists());

    // Leaf symlink: <ws2>/.lattice-runtime/s25/sync-state -> <t1>.
    let ws2 = TempDir::new().unwrap();
    let t1 = TempDir::new().unwrap();
    let t2 = TempDir::new().unwrap();
    let parent = ws2.path().join(layout::RUNTIME_DIR_NAME).join("s25");
    std::fs::create_dir_all(&parent).unwrap();
    let link = parent.join(SYNC_STATE_DIR_NAME);
    std::os::unix::fs::symlink(t1.path(), &link).unwrap();
    let latch2 = Arc::new(Latch::new());
    let reg2 =
        bootstrap_with_latch(ws2.path(), &["s25".into()], cfg.path(), latch2.clone()).unwrap();
    let sd2 = reg2.get("s25").expect("leaf symlink layout bootstraps");
    for _ in 0..100 {
        assert!(
            sd2.custody().is_ok(),
            "symlinked leaf must not false-positive"
        );
    }
    let _c = sd2.append(StateFile::DeleteLedger, b"a\n").unwrap();
    assert!(exits(&latch2).is_empty(), "no exit while the link holds");
    assert!(t1.path().join("delete-burst-SUPPRESSED").exists());

    // Retarget the link: the next commit is Replaced, exit 74.
    std::fs::remove_file(&link).unwrap();
    std::os::unix::fs::symlink(t2.path(), &link).unwrap();
    let e = sd2.append(StateFile::DeleteLedger, b"b\n").unwrap_err();
    assert!(is_ownership_lost(&e.0));
    assert!(matches!(
        exits(&latch2).as_slice(),
        [(74, LossReason::Replaced { .. })]
    ));
    assert!(names(t2.path()).is_empty());
}

/// T-CS2-4 (T-S3, rm -rf, all four CI OS): after `rm -rf` of the state dir
/// every writer returns `NotCommitted` (or a benign ENOENT for `remove`),
/// the directory and its parents are not recreated, and on unix the process
/// exits 74.
#[test]
fn t_cs2_4_rm_rf_no_resurrection() {
    let (ws, _cfg, reg, latch) = boot(&["sub4"]);
    let d = leaf(ws.path(), "sub4");
    let sd = reg.get("sub4").unwrap();
    std::fs::remove_dir_all(ws.path().join(layout::RUNTIME_DIR_NAME)).unwrap();
    assert!(sd.append(StateFile::PushJournal, b"x\n").is_err());
    assert!(sd.replace_atomic(StateFile::Shadow, b"{}").is_err());
    assert!(sd.open_append(StateFile::DeleteLedger).is_err());
    assert!(sd.open_lock(StateFile::PushJournalLock).is_err());
    let _ = sd.remove(StateFile::PauseMarker);
    assert!(!d.exists(), "state dir must not be recreated");
    assert!(
        !ws.path().join(layout::RUNTIME_DIR_NAME).exists(),
        "parents must not be recreated"
    );
    #[cfg(unix)]
    assert_eq!(
        exits(&latch),
        vec![(EXIT_OWNERSHIP_LOST, LossReason::CanonicalAbsent)]
    );
    #[cfg(not(unix))]
    let _ = latch;
}

/// T-CS2-26 (state_io half): after the first `OwnershipLost`, concurrent
/// mutating ops all return `NotCommitted` with ZERO further syscalls seen by
/// the recorder, and exactly one ERROR is logged.
#[cfg(unix)]
#[test]
fn t_cs2_26_latch_blocks_without_syscalls() {
    let (ws, _cfg, reg, latch) = boot(&["sub26"]);
    let d = leaf(ws.path(), "sub26");
    let sd = reg.get("sub26").unwrap();
    let moved = move_and_recreate(&d);
    assert!(sd.append(StateFile::PushJournal, b"x\n").is_err());
    assert!(latch.is_set());
    sd.seams.recorder.lock().unwrap().clear();
    let mut hs = Vec::new();
    for i in 0..8 {
        let sd = sd.clone();
        hs.push(std::thread::spawn(move || {
            for _ in 0..50 {
                let e = match i % 4 {
                    0 => sd.append(StateFile::PushJournal, b"x\n").unwrap_err(),
                    1 => sd.replace_atomic(StateFile::Shadow, b"{}").unwrap_err(),
                    2 => sd.append(StateFile::DeleteLedger, b"p\n").unwrap_err(),
                    _ => sd.remove(StateFile::PauseMarker).unwrap_err(),
                };
                assert!(is_ownership_lost(&e.0));
            }
        }));
    }
    for h in hs {
        h.join().unwrap();
    }
    assert!(
        sd.seams.recorder.lock().unwrap().is_empty(),
        "zero syscalls while latched: {:?}",
        sd.seams.recorder.lock().unwrap()
    );
    assert_eq!(latch.errors_logged(), 1, "exactly one ERROR");
    assert_eq!(exits(&latch).len(), 1);
    assert!(names(&d).is_empty());
    assert!(names(&moved).is_empty());
}

/// T-CS2-24 (C11): the bootstrap sweep unlinks a dead-pid CS-1 temp, keeps a
/// live-pid temp and the legacy leftovers, and never `openat`s a swept name.
#[cfg(unix)]
#[test]
fn t_cs2_24_stale_temp_sweep() {
    let dead_pid = {
        let mut c = std::process::Command::new("true").spawn().unwrap();
        let pid = c.id();
        c.wait().unwrap();
        pid
    };
    let mut live = std::process::Command::new("sleep")
        .arg("30")
        .spawn()
        .unwrap();
    let d = TempDir::new().unwrap();
    let dead = format!("shadow_hashes.json.tmp-{dead_pid}-1-0");
    let alive = format!("base_seq.json.tmp-{}-1-0", live.id());
    let legacy = [
        ".last_event_id.tmp.4242".to_string(),
        "reconcile-retry-ledger.json.tmp".to_string(),
        "push_journal.jsonl.tmp-4242-99".to_string(),
        ".tmpAbC123".to_string(),
    ];
    for n in [&dead, &alive].into_iter().chain(legacy.iter()) {
        std::fs::write(d.path().join(n), b"t").unwrap();
    }
    let sd = StateDir::for_test(d.path());
    sd.sweep_stale_temps();
    let left = names(d.path());
    assert!(!left.contains(&dead), "dead-pid temp removed");
    assert!(left.contains(&alive), "live-pid temp kept");
    for l in &legacy {
        assert!(left.contains(l), "legacy kept: {l}");
    }
    let rec = sd.seams.recorder.lock().unwrap().clone();
    for n in [&dead, &alive].into_iter().chain(legacy.iter()) {
        assert!(
            !rec.contains(&format!("openat:{n}")),
            "swept name opened: {n}"
        );
    }
    assert!(rec.contains(&format!("unlinkat:{dead}")));
    let _ = live.kill();
    let _ = live.wait();
}

/// T-CS2-30 (C2, state_io half): for every `StateFile`, a planted symlink to
/// an outside file is never followed by any op, and `exists()` reports it;
/// a planted FIFO makes `read` error within 1 s instead of blocking.
#[cfg(unix)]
#[test]
fn t_cs2_30_no_follow_and_regular_file() {
    let outside = TempDir::new().unwrap();
    let victim = outside.path().join("victim");
    std::fs::write(&victim, b"ORIGINAL").unwrap();
    for &f in StateFile::ALL {
        let d = TempDir::new().unwrap();
        let sd = StateDir::for_test_scoped(d.path(), f.scope());
        std::os::unix::fs::symlink(&victim, d.path().join(f.name())).unwrap();
        assert!(sd.exists(f).unwrap(), "{f:?}: a symlink is present");
        assert!(sd.read(f).is_err(), "{f:?}: read refuses the link");
        let _ = sd.append(f, b"EVIL");
        let _ = sd.open_append(f).map(|mut a| a.write_all(b"EVIL"));
        if f == StateFile::PushJournalLock {
            assert!(sd.open_lock(f).is_err(), "lock open refuses the link");
        }
        assert!(
            sd.replace_atomic(f, b"EVIL").is_err(),
            "{f:?}: replace refuses"
        );
        assert_eq!(std::fs::read(&victim).unwrap(), b"ORIGINAL", "{f:?}");
    }

    let d = TempDir::new().unwrap();
    let sd = StateDir::for_test(d.path());
    let fifo = sys_cstr(&d.path().join(StateFile::LastEventId.name()));
    // SAFETY: valid C string.
    assert_eq!(unsafe { libc::mkfifo(fifo.as_ptr(), 0o600) }, 0);
    let (tx, rx) = std::sync::mpsc::channel();
    let sd2 = sd.clone();
    std::thread::spawn(move || {
        let _ = tx.send(sd2.read(StateFile::LastEventId).is_err());
    });
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(1)),
        Ok(true),
        "a FIFO read must error promptly, not block"
    );
}

/// T-CS2-34 support: the StateAnchor seals through `durable_file::sealed`.
#[test]
fn state_anchor_round_trip_through_durable_file() {
    let d = TempDir::new().unwrap();
    let sd = StateDir::for_test(d.path());
    let c = sd.replace_atomic(StateFile::Shadow, b"{\"a\":1}").unwrap();
    #[cfg(unix)]
    assert_eq!(c, Commit::Durable);
    #[cfg(not(unix))]
    let _ = c;
    assert_eq!(
        sd.read(StateFile::Shadow).unwrap().unwrap(),
        b"{\"a\":1}".to_vec()
    );
    assert_eq!(names(d.path()), vec!["shadow_hashes.json".to_string()]);
    let _c = sd.remove(StateFile::Shadow).unwrap();
    assert_eq!(sd.read(StateFile::Shadow).unwrap(), None);
}
