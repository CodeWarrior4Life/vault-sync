//! Unit CS-2 (TKT-23091e72) owner-level tests: every registered writer and
//! reader, driven through its real owner (journal, stores, ledger, marker,
//! SSE cursor, retry ledger) against a held `StateDir`.
#![allow(clippy::disallowed_methods)]

use super::*;
use crate::delete_ledger::DeleteLedger;
use crate::push_journal::{new_event_id, PushAction, PushBase, PushEvent, PushJournal};
use crate::redflag::DeleteBurstDetector;
use std::sync::atomic::Ordering;
use tempfile::TempDir;

fn evt(path: &str) -> PushEvent {
    PushEvent {
        schema_version: crate::push_journal::CURRENT_SCHEMA,
        id: new_event_id(),
        path: path.to_string(),
        action: PushAction::Modify,
        base_hash: PushBase::Unknown,
        content_sha: String::new(),
        content_bytes: None,
        queued_at: chrono::Utc::now(),
        device_id: "dev".into(),
    }
}

fn names(dir: &Path) -> Vec<String> {
    super::tests::names(dir)
}

/// A held SyncState dir `D` under a scratch workspace, plus `D.moved`.
struct Fixture {
    _ws: TempDir,
    d: PathBuf,
    moved: PathBuf,
    sd: Arc<StateDir>,
}

fn fixture() -> Fixture {
    let ws = TempDir::new().unwrap();
    let d = ws.path().join("sync-state");
    std::fs::create_dir(&d).unwrap();
    let sd = StateDir::for_test(&d);
    let moved = ws.path().join("sync-state.moved");
    Fixture {
        _ws: ws,
        d,
        moved,
        sd,
    }
}

impl Fixture {
    /// Rename the held dir to `D.moved` and recreate an empty `D`.
    fn move_and_recreate(&self) {
        std::fs::rename(&self.d, &self.moved).unwrap();
        std::fs::create_dir(&self.d).unwrap();
    }
    fn exits(&self) -> Vec<(i32, LossReason)> {
        self.sd.test_latch().exits.lock().unwrap().clone()
    }
}

fn is_lost(e: &io::Error) -> bool {
    is_ownership_lost(e)
}

fn journal_err_lost(e: &crate::push_journal::JournalError) -> bool {
    matches!(e, crate::push_journal::JournalError::Io(io) if is_lost(io))
}

fn sse_consumer() -> crate::sse::SseConsumer {
    let vaults = Box::leak(Box::new(TempDir::new().unwrap()));
    let ws = Box::leak(Box::new(TempDir::new().unwrap()));
    let mat = crate::materializer::Materializer::new(
        vaults.path().to_path_buf(),
        Some("shadow/".to_string()),
        crate::materializer::MaterializerMode::Disabled,
        ws.path().to_path_buf(),
        "cs2".to_string(),
        crate::materializer::MaterializerConfig {
            device_id: "cs2".into(),
            ..Default::default()
        },
    );
    crate::sse::SseConsumer::new("http://127.0.0.1:1".into(), "t".into(), vec![], vec![], mat)
        .unwrap()
}

/// Fire every writer of `f` after `fx.move_and_recreate()` (prepared state is
/// built BEFORE the move by `prepare`). Returns how many trigger results
/// reported a lost custody (or, with custody disabled, succeeded).
fn fire_writers(fx: &Fixture, f: StateFile, custody_on: bool) {
    // Exhaustive, no wildcard arm: a new StateFile member fails to compile
    // until it gets a trigger here (T-CS2-1).
    match f {
        StateFile::PushJournal => {
            let mut j = PushJournal::open(&fx.sd).unwrap();
            j.append(evt("a.md")).unwrap();
            j.append(evt("a.md")).unwrap();
            let cur = j.drain(1).unwrap().remove(0).1;
            fx.move_and_recreate();
            let r = [
                j.append(evt("b.md")).err(),
                j.append_batch(vec![evt("c.md")]).err(),
                j.ack(cur).err(),
                j.compact().err(),
                PushJournal::open(&fx.sd).err(),
            ];
            if custody_on {
                for e in r {
                    assert!(
                        e.as_ref().is_some_and(journal_err_lost),
                        "journal writer must lose custody: {e:?}"
                    );
                }
            }
        }
        StateFile::PushJournalLock => {
            fx.move_and_recreate();
            let r1 = fx.sd.open_lock(StateFile::PushJournalLock);
            let r2 = PushJournal::open(&fx.sd);
            if custody_on {
                assert!(r1.as_ref().is_err_and(is_lost));
                assert!(r2.as_ref().err().is_some_and(journal_err_lost));
            }
        }
        StateFile::Shadow => {
            let s = crate::sync_shadow::ShadowStore::load(&fx.sd);
            s.record("a.md", "h");
            fx.move_and_recreate();
            let r = s.flush();
            if custody_on {
                assert!(r.as_ref().is_err_and(|e| is_lost(&e.0)), "{r:?}");
            }
        }
        StateFile::BaseSeq => {
            let s = crate::base_seq_store::BaseSeqStore::load(&fx.sd);
            s.record_adopted("a.md", 7);
            fx.move_and_recreate();
            let r = s.flush();
            if custody_on {
                assert!(r.as_ref().is_err_and(|e| is_lost(&e.0)), "{r:?}");
            }
        }
        StateFile::ReadReceipt => {
            let s = crate::read_receipt::ReadReceiptStore::load(&fx.sd);
            s.record_verified(
                "a.md",
                crate::read_receipt::Receipt {
                    revision_seq: 3,
                    body_sha: "bodysha".into(),
                },
            );
            fx.move_and_recreate();
            let r = s.flush();
            if custody_on {
                assert!(r.as_ref().is_err_and(|e| is_lost(&e.0)), "{r:?}");
            }
        }
        StateFile::LastEventId => {
            let c = sse_consumer().with_state_dir(&fx.sd);
            fx.move_and_recreate();
            c.persist_last_event_id("9-9");
        }
        StateFile::DeleteLedger => {
            let l = DeleteLedger::new(&fx.sd);
            fx.move_and_recreate();
            let ok = l.record("gone.md");
            l.clear();
            if custody_on {
                assert!(!ok, "ledger record must fail");
            }
        }
        StateFile::PauseMarker => {
            let mut det = DeleteBurstDetector::new(1, std::time::Duration::from_secs(30))
                .with_pause_marker(&fx.sd);
            fx.move_and_recreate();
            det.record_delete(); // trips: writes the marker
            det.reset(); // clears the marker
        }
        StateFile::ReconcileRetryLedger => {
            // Config scope: covered by `t_cs2_1_retry_ledger_config_scope`.
            fx.move_and_recreate();
        }
    }
}

/// T-CS2-1 (T-S1 over every registered writer): after move + recreate,
/// every writer of every member leaves `D` empty and its commit is
/// `OwnershipLost` (the holder exits 74).
#[cfg(unix)]
#[test]
fn t_cs2_1_every_writer_never_fills_the_replacement() {
    for &f in StateFile::ALL {
        if f.scope() != Scope::SyncState {
            continue;
        }
        let fx = fixture();
        fire_writers(&fx, f, true);
        assert!(
            names(&fx.d).is_empty(),
            "{f:?}: a writer reached the replacement dir: {:?}",
            names(&fx.d)
        );
        assert!(
            matches!(fx.exits().as_slice(), [(74, LossReason::Replaced { .. })]),
            "{f:?}: exit 74 Replaced expected, got {:?}",
            fx.exits()
        );
    }
}

/// T-CS2-1, config-scope member (the retry ledger in the config dir).
#[cfg(unix)]
#[test]
fn t_cs2_1_retry_ledger_config_scope() {
    let ws = TempDir::new().unwrap();
    let d = ws.path().join("cfg");
    std::fs::create_dir(&d).unwrap();
    let sd = StateDir::for_test_scoped(&d, Scope::ConfigDir);
    let l = crate::retry_ledger::RetryLedger::load(&sd);
    std::fs::rename(&d, ws.path().join("cfg.moved")).unwrap();
    std::fs::create_dir(&d).unwrap();
    l.record_failure("x.md", "boom");
    assert!(names(&d).is_empty(), "{:?}", names(&d));
    assert_eq!(sd.test_latch().exits.lock().unwrap().len(), 1);
    // T-CS2-2 half: with custody disabled the write lands in the held inode.
    let sd2 = StateDir::for_test_scoped(&d, Scope::ConfigDir);
    let l2 = crate::retry_ledger::RetryLedger::load(&sd2);
    sd2.seams.custody_disabled.store(true, Ordering::SeqCst);
    std::fs::rename(&d, ws.path().join("cfg.moved2")).unwrap();
    std::fs::create_dir(&d).unwrap();
    l2.record_failure("y.md", "boom");
    assert!(names(&d).is_empty());
    assert_eq!(
        names(&ws.path().join("cfg.moved2")),
        vec!["reconcile-retry-ledger.json".to_string()]
    );
}

/// T-CS2-2 (the dirfd layer alone): with custody DISABLED, every write lands
/// in `D.moved` and `D` stays empty, so confinement does not depend on the
/// custody check.
#[cfg(unix)]
#[test]
fn t_cs2_2_dirfd_confines_without_custody() {
    for &f in StateFile::ALL {
        if f.scope() != Scope::SyncState {
            continue;
        }
        let fx = fixture();
        fx.sd.seams.custody_disabled.store(true, Ordering::SeqCst);
        fire_writers(&fx, f, false);
        assert!(
            names(&fx.d).is_empty(),
            "{f:?}: write reached the replacement: {:?}",
            names(&fx.d)
        );
        assert!(fx.exits().is_empty());
        let landed = names(&fx.moved);
        let expect = match f {
            // The marker is written then cleared; the ledger is cleared.
            StateFile::PauseMarker | StateFile::DeleteLedger => None,
            _ => Some(f.name().to_string()),
        };
        if let Some(n) = expect {
            assert!(landed.contains(&n), "{f:?}: {n} not in D.moved: {landed:?}");
        }
    }
}

fn latched_detector(sd: &Arc<StateDir>) -> DeleteBurstDetector {
    let mut det =
        DeleteBurstDetector::new(1, std::time::Duration::from_secs(30)).with_pause_marker(sd);
    det.record_delete();
    assert!(det.is_paused());
    assert!(sd.exists(StateFile::PauseMarker).unwrap());
    det
}

/// T-CS2-15 (marker reader): latched, marker present; the dir is moved and
/// recreated with custody disabled: `poll_marker_resume` returns false and
/// the valve stays latched (the marker is still in the held inode).
#[cfg(unix)]
#[test]
fn t_cs2_15_marker_reader_reads_the_held_inode() {
    let fx = fixture();
    let mut det = latched_detector(&fx.sd);
    fx.sd.seams.custody_disabled.store(true, Ordering::SeqCst);
    fx.move_and_recreate();
    assert!(!det.poll_marker_resume(), "must not unlatch");
    assert!(det.is_paused());
    assert_eq!(det.grace_remaining(), 0, "no grace window");
}

/// T-CS2-31 (gate C3, delete valve): `rm -rf` of the held dir with the valve
/// latched, the marker present and two suppressed paths: no unlatch, no
/// replay, no `ledger.clear()`, no grace, and exit 74 under custody.
#[cfg(unix)]
#[test]
fn t_cs2_31_delete_valve_unlatch_order() {
    // Main case: custody ON.
    let fx = fixture();
    let ledger = DeleteLedger::new(&fx.sd);
    assert!(ledger.record("p1.md") && ledger.record("p2.md"));
    let mut det = latched_detector(&fx.sd);
    std::fs::remove_dir_all(&fx.d).unwrap();
    assert!(!det.poll_marker_resume(), "rm -rf must not unlatch");
    assert!(det.is_paused());
    assert_eq!(det.grace_remaining(), 0);
    assert!(
        matches!(fx.exits().as_slice(), [(74, LossReason::CanonicalAbsent)]),
        "{:?}",
        fx.exits()
    );

    // Case A: custody DISABLED, rm -rf: only st_nlink == 0 can hold the latch.
    let fx = fixture();
    let mut det = latched_detector(&fx.sd);
    fx.sd.seams.custody_disabled.store(true, Ordering::SeqCst);
    std::fs::remove_dir_all(&fx.d).unwrap();
    assert!(!fx.sd.exists(StateFile::PauseMarker).unwrap());
    assert!(
        !det.poll_marker_resume(),
        "case A: st_nlink must hold the latch"
    );
    assert!(det.is_paused());
    assert_eq!(det.grace_remaining(), 0);

    // Case B: unlink the marker INSIDE the held dir, rename the held dir away
    // and recreate an empty one: exists=Ok(false), st_nlink>0, so only
    // custody (Replaced) can hold the latch, and it exits 74.
    let fx = fixture();
    let mut det = latched_detector(&fx.sd);
    std::fs::remove_file(fx.d.join(StateFile::PauseMarker.name())).unwrap();
    fx.move_and_recreate();
    assert!(!fx.sd.exists(StateFile::PauseMarker).unwrap());
    assert!(fx.sd.held_dir_linked().unwrap());
    assert!(
        !det.poll_marker_resume(),
        "case B: custody must hold the latch"
    );
    assert!(det.is_paused());
    assert!(
        matches!(fx.exits().as_slice(), [(74, LossReason::Replaced { .. })]),
        "{:?}",
        fx.exits()
    );

    // Positive control: the operator removes the marker for real (custody
    // and the held dir intact): the valve unlatches with a grace window.
    let fx = fixture();
    let mut det = latched_detector(&fx.sd);
    std::fs::remove_file(fx.d.join(StateFile::PauseMarker.name())).unwrap();
    assert!(det.poll_marker_resume(), "control: a real resume unlatches");
    assert!(!det.is_paused());
    assert!(fx.exits().is_empty());
}

/// T-CS2-16 (ledger reader): with suppressed path A in the ledger, the dir
/// is moved and recreated with custody disabled, then one pull_backfill pass
/// over a page listing A runs with `created == 0` and A absent. Positive
/// control: the same pass with an empty ledger creates A.
#[cfg(unix)]
#[tokio::test]
async fn t_cs2_16_ledger_reader_guards_backfill_after_move() {
    use sha2::{Digest, Sha256};
    let rel = "notes/suppressed.md";
    let body = "hello\n";
    let sha = hex::encode(Sha256::digest(body.as_bytes()));
    let mut srv = mockito::Server::new_async().await;
    let _changes = srv
        .mock("GET", "/api/sync/changes")
        .match_query(mockito::Matcher::Any)
        .with_status(200)
        .with_body(format!(
            r#"{{"changes":[{{"path":"{rel}","lsn":5}}],"next_lsn":5}}"#
        ))
        .create_async()
        .await;
    let _empty = srv
        .mock("GET", "/api/sync/changes")
        .match_query(mockito::Matcher::UrlEncoded("since".into(), "5".into()))
        .with_status(200)
        .with_body(r#"{"changes":[],"next_lsn":5}"#)
        .create_async()
        .await;
    let _note = srv
        .mock("GET", "/api/sync/note")
        .match_query(mockito::Matcher::Any)
        .with_status(200)
        .with_body(format!(
            r#"{{"path":"{rel}","frontmatter":{{}},"body":{b},"sha256":"{sha}","modified":"2026-07-01T00:00:00Z","enriched_body":{b}}}"#,
            b = serde_json::to_string(body).unwrap()
        ))
        .create_async()
        .await;
    let api = crate::api_client::ApiClient::new(&srv.url(), "t").unwrap();

    let run = |vault: PathBuf, ledger: Option<DeleteLedger>| {
        let api = &api;
        async move {
            let ws = TempDir::new().unwrap();
            let mat = crate::materializer::Materializer::new(
                vault,
                None,
                crate::materializer::MaterializerMode::Live,
                ws.path().to_path_buf(),
                "cs2".into(),
                crate::materializer::MaterializerConfig::default(),
            );
            crate::pull_backfill::run_pull_backfill_guarded(api, &mat, 100, 2, ledger.as_ref())
                .await
        }
    };

    // Positive control: no ledger entry, A is created.
    let v1 = TempDir::new().unwrap();
    let ctl = run(v1.path().to_path_buf(), None).await;
    assert_eq!(ctl.created, 1, "control must create A: {ctl:?}");
    assert!(v1.path().join(rel).exists());

    // A suppressed in the ledger; dir moved + recreated, custody disabled.
    let fx = fixture();
    let ledger = DeleteLedger::new(&fx.sd);
    assert!(ledger.record(rel));
    fx.sd.seams.custody_disabled.store(true, Ordering::SeqCst);
    fx.move_and_recreate();
    let v2 = TempDir::new().unwrap();
    let stats = run(v2.path().to_path_buf(), Some(ledger)).await;
    assert_eq!(stats.created, 0, "{stats:?}");
    assert!(!v2.path().join(rel).exists(), "A must stay absent");
}

/// T-CS2-26 (owner half): after the first `OwnershipLost`, concurrent
/// journal appends, store flushes and ledger records all fail with ZERO
/// write syscalls seen by the recorder, and exactly one ERROR is logged.
#[cfg(unix)]
#[test]
fn t_cs2_26_latch_blocks_owners() {
    let fx = fixture();
    let j = Arc::new(std::sync::Mutex::new(PushJournal::open(&fx.sd).unwrap()));
    let shadow = crate::sync_shadow::ShadowStore::load(&fx.sd);
    let ledger = DeleteLedger::new(&fx.sd);
    fx.move_and_recreate();
    assert!(j.lock().unwrap().append(evt("first.md")).is_err());
    assert!(fx.sd.test_latch().is_set());
    fx.sd.seams.recorder.lock().unwrap().clear();
    let mut hs = Vec::new();
    for i in 0..6 {
        let (j, shadow, ledger) = (j.clone(), shadow.clone(), ledger.clone());
        hs.push(std::thread::spawn(move || {
            for k in 0..30 {
                match i % 3 {
                    0 => assert!(j.lock().unwrap().append(evt("x.md")).is_err()),
                    1 => {
                        shadow.record(&format!("k{k}.md"), "h");
                        assert!(shadow.flush().is_err());
                    }
                    _ => assert!(!ledger.record("y.md")),
                }
            }
        }));
    }
    for h in hs {
        h.join().unwrap();
    }
    let rec = fx.sd.seams.recorder.lock().unwrap().clone();
    let writes: Vec<&String> = rec
        .iter()
        .filter(|r| {
            ["write:", "renameat:", "unlinkat:", "sync_data:", "openat:"]
                .iter()
                .any(|p| r.starts_with(p))
        })
        .collect();
    assert!(writes.is_empty(), "syscalls while latched: {writes:?}");
    assert_eq!(fx.sd.test_latch().errors_logged(), 1, "exactly one ERROR");
    assert!(names(&fx.d).is_empty());
}

/// T-CS2-30 (C2, owner half): for every `StateFile`, a planted symlink at
/// its name pointing to an outside file leaves that file byte-identical
/// after every owner's writers AND readers run.
#[cfg(unix)]
#[test]
fn t_cs2_30_owners_never_follow_planted_symlinks() {
    let outside = TempDir::new().unwrap();
    let victim = outside.path().join("victim");
    std::fs::write(&victim, b"ORIGINAL").unwrap();
    for &f in StateFile::ALL {
        let d = TempDir::new().unwrap();
        let sd = StateDir::for_test_scoped(d.path(), f.scope());
        std::os::unix::fs::symlink(&victim, d.path().join(f.name())).unwrap();
        match f {
            StateFile::PushJournal | StateFile::PushJournalLock => {
                assert!(PushJournal::open(&sd).is_err(), "{f:?}");
            }
            StateFile::Shadow => {
                let s = crate::sync_shadow::ShadowStore::load(&sd);
                s.record("a", "b");
                assert!(s.flush().is_err());
            }
            StateFile::BaseSeq => {
                let s = crate::base_seq_store::BaseSeqStore::load(&sd);
                s.record_adopted("a", 1);
                assert!(s.flush().is_err());
            }
            StateFile::ReadReceipt => {
                let s = crate::read_receipt::ReadReceiptStore::load(&sd);
                s.record_verified(
                    "a",
                    crate::read_receipt::Receipt {
                        revision_seq: 1,
                        body_sha: "x".into(),
                    },
                );
                assert!(s.flush().is_err());
            }
            StateFile::LastEventId => {
                assert_eq!(crate::sse::SseConsumer::load_last_event_id(&sd), None);
                sse_consumer()
                    .with_state_dir(&sd)
                    .persist_last_event_id("1");
            }
            StateFile::DeleteLedger => {
                let l = DeleteLedger::new(&sd);
                assert!(l.paths().is_empty());
                assert!(!l.record("evil.md"));
            }
            StateFile::PauseMarker => {
                let mut det = DeleteBurstDetector::new(1, std::time::Duration::from_secs(30))
                    .with_pause_marker(&sd);
                det.record_delete();
                assert!(
                    !det.poll_marker_resume(),
                    "a planted symlink reads as PRESENT: the valve stays latched"
                );
            }
            StateFile::ReconcileRetryLedger => {
                let l = crate::retry_ledger::RetryLedger::load(&sd);
                assert!(l.is_empty());
                l.record_failure("a", "b");
            }
        }
        assert_eq!(std::fs::read(&victim).unwrap(), b"ORIGINAL", "{f:?}");
    }
}
