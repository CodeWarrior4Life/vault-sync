//! Unit CS-2 (TKT-23091e72) owner-level tests: every registered writer and
//! reader, driven through its real owner (journal, stores, ledger, marker,
//! SSE cursor, retry ledger) against a held `StateDir`.
#![allow(clippy::disallowed_methods)]
// Most of these tests are unix-only; their helpers are unused on Windows.
#![cfg_attr(not(unix), allow(dead_code, unused_imports))]

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

    // Case B: unlink the marker INSIDE the held dir, then rename the held
    // dir's PARENT away and recreate an empty parent/leaf at the canonical
    // path. The held dir stays linked under its leaf name in the moved
    // parent, so exists=Ok(false) and step 3 holds (st_nlink>0, the parent
    // still holds the leaf): only custody (Replaced) can keep the latch,
    // and it exits 74.
    let base = TempDir::new().unwrap();
    let parent = base.path().join("p");
    let leafdir = parent.join("sync-state");
    std::fs::create_dir_all(&leafdir).unwrap();
    let sd = StateDir::for_test(&leafdir);
    let mut det = latched_detector(&sd);
    std::fs::remove_file(leafdir.join(StateFile::PauseMarker.name())).unwrap();
    std::fs::rename(&parent, base.path().join("p.moved")).unwrap();
    std::fs::create_dir_all(&leafdir).unwrap();
    assert!(!sd.exists(StateFile::PauseMarker).unwrap());
    assert!(sd.held_dir_linked().unwrap(), "step 3 must hold in case B");
    assert!(
        !det.poll_marker_resume(),
        "case B: custody must hold the latch"
    );
    assert!(det.is_paused());
    let exits = sd.test_latch().exits.lock().unwrap().clone();
    assert!(
        matches!(exits.as_slice(), [(74, LossReason::Replaced { .. })]),
        "{exits:?}"
    );

    // Case C (step 3, by-name half): rename the held LEAF away and recreate
    // it, with custody disabled: the parent no longer holds the held inode
    // under its leaf name, so step 3 alone keeps the latch.
    let fx = fixture();
    let mut det = latched_detector(&fx.sd);
    fx.sd.seams.custody_disabled.store(true, Ordering::SeqCst);
    std::fs::remove_file(fx.d.join(StateFile::PauseMarker.name())).unwrap();
    fx.move_and_recreate();
    assert!(!fx.sd.held_dir_linked().unwrap());
    assert!(
        !det.poll_marker_resume(),
        "case C: step 3 must hold the latch"
    );
    assert!(det.is_paused());

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

/// T-CS2-11 (gate 4, the run-time belt): an exempt vault writer asked to
/// write under the registered sync-state dir (directly, through a vault
/// symlink into it, through the shadow-mode target `../sync-state/x` without
/// lexical normalization (C9), or under the config-scope dir) is refused and
/// writes nothing, while a shadow-mode materialize under
/// `.lattice-runtime/<sub>/shadow/` succeeds.
#[cfg(unix)]
#[test]
fn t_cs2_11_run_time_belt() {
    use crate::materializer::{
        Materializer, MaterializerConfig, MaterializerError, MaterializerMode,
    };
    let ws = TempDir::new().unwrap();
    let vaults = TempDir::new().unwrap();
    let cfg = TempDir::new().unwrap();
    let rt = ws.path().join(layout::RUNTIME_DIR_NAME).join("sub11");
    let state = rt.join("sync-state");
    let shadow = rt.join("shadow");
    std::fs::create_dir_all(&state).unwrap();
    std::fs::create_dir_all(&shadow).unwrap();
    std::fs::create_dir_all(vaults.path().join("Mainframe")).unwrap();
    let sd = StateDir::for_test(&state);
    let cd = StateDir::for_test_scoped(cfg.path(), Scope::ConfigDir);
    seams::set_belt_ids(Some(vec![sd.test_id(), cd.test_id()]));

    // Primitive cases.
    assert!(refuse_state_target(&state.join("x")).is_err(), "direct");
    let link = vaults.path().join("Mainframe").join("link");
    std::os::unix::fs::symlink(&state, &link).unwrap();
    assert!(
        refuse_state_target(&link.join("evil.md")).is_err(),
        "vault symlink"
    );
    assert!(
        refuse_state_target(&shadow.join("../sync-state/x")).is_err(),
        "shadow ../sync-state without lexical normalization"
    );
    assert!(
        refuse_state_target(&cfg.path().join("x.json")).is_err(),
        "config scope"
    );
    assert!(
        refuse_state_target(&shadow.join("Mainframe/a.md")).is_ok(),
        "shadow tree"
    );
    assert!(
        refuse_state_target(&vaults.path().join("Mainframe/a.md")).is_ok(),
        "vault"
    );

    // Through a real exempt writer: Live materialize via the vault symlink.
    let live = Materializer::new(
        vaults.path().to_path_buf(),
        Some("shadow/".to_string()),
        MaterializerMode::Live,
        ws.path().to_path_buf(),
        "sub11".to_string(),
        MaterializerConfig {
            device_id: "d".into(),
            ..Default::default()
        },
    );
    let body = "evil\n";
    let p = crate::materializer::test_payload("Mainframe/link/evil.md", body);
    let r = live.write(&p);
    assert!(
        matches!(r, Err(MaterializerError::ForeignStateWrite(_))),
        "the belt must refuse the symlinked state target: {r:?}"
    );
    assert!(names(&state).is_empty(), "nothing written into state");

    // Positive control: a shadow-mode materialize under .lattice-runtime/<sub>/shadow.
    let sh = Materializer::new(
        vaults.path().to_path_buf(),
        Some("shadow/".to_string()),
        MaterializerMode::Shadow,
        ws.path().to_path_buf(),
        "sub11".to_string(),
        MaterializerConfig {
            device_id: "d".into(),
            ..Default::default()
        },
    );
    let r = sh.write(&crate::materializer::test_payload(
        "Mainframe/ok.md",
        "ok\n",
    ));
    seams::set_belt_ids(None);
    assert!(r.is_ok(), "shadow-mode write must pass the belt: {r:?}");
    assert!(shadow.join("Mainframe/ok.md").exists());
}

/// T-CS2-18 (T-D2 for the StateDir): the recorder shows the temp `sync_all`
/// (F_FULLFSYNC on Apple, VENDOR-DOC) and the held-dirfd sync for
/// `replace_atomic`, and `sync_data` on the file for `append`; with the
/// dirfd sync forced to "unsupported", Apple falls back to `fsync(dirfd)` and
/// reports `CommittedDurabilityUncertain` (CS-1 §4b) while Linux keeps the
/// tolerated `Durable` rule.
#[cfg(unix)]
#[test]
fn t_cs2_18_statedir_durability_contract() {
    use crate::durable_file::seams as dseams;
    let fx = fixture();
    dseams::reset();
    fx.sd.seams.recorder.lock().unwrap().clear();
    let c = fx.sd.replace_atomic(StateFile::Shadow, b"{}").unwrap();
    let rec = fx.sd.seams.recorder.lock().unwrap().clone();
    let ev = dseams::events();
    assert!(
        rec.iter()
            .any(|r| r.starts_with("openat:shadow_hashes.json.tmp-")),
        "{rec:?}"
    );
    assert!(
        rec.contains(&"renameat:shadow_hashes.json".to_string()),
        "{rec:?}"
    );
    assert!(rec.contains(&"sync_dir:".to_string()), "{rec:?}");
    assert!(ev.contains(&"sync_all:temp".to_string()), "{ev:?}");
    let apple = cfg!(target_vendor = "apple");
    let want = if apple { "AppleFullFsync" } else { "Synced" };
    assert!(
        ev.iter().any(|e| e == &format!("dir_sync_branch:{want}")),
        "{ev:?}"
    );
    assert_eq!(c, Commit::Durable);

    // append: sync_data on the file.
    fx.sd.seams.recorder.lock().unwrap().clear();
    let _c = fx.sd.append(StateFile::DeleteLedger, b"a\n").unwrap();
    assert!(fx
        .sd
        .seams
        .recorder
        .lock()
        .unwrap()
        .contains(&"sync_data:delete-burst-SUPPRESSED".to_string()));

    // Forced "F_FULLFSYNC unsupported" on the dirfd.
    dseams::reset();
    let enotsup = if apple { 45 } else { 95 };
    dseams::fail_dir_fsync(None, enotsup);
    let c = fx
        .sd
        .replace_atomic(StateFile::Shadow, b"{\"x\":1}")
        .unwrap();
    let ev = dseams::events();
    if apple {
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

/// T-CS2-23 (no behaviour change while custody holds; modes stated per C10):
/// v1 names and raw formats are unchanged (raw stores reload; no framing),
/// and the file modes are exactly: 0600 for files written through
/// `replace_atomic` or created by `openat(O_CREAT, 0600)` (journal on first
/// create and after every rewrite, pause marker, last_event_id, retry
/// ledger, a newly created ledger), and unchanged 0600 for the three stores
/// and the lock file. An EXISTING ledger keeps its mode (appends do not
/// chmod).
#[cfg(unix)]
#[test]
fn t_cs2_23_formats_and_modes() {
    use std::os::unix::fs::PermissionsExt;
    let mode = |p: &Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
    let fx = fixture();
    let mut j = PushJournal::open(&fx.sd).unwrap();
    assert_eq!(
        mode(&fx.d.join("push_journal.jsonl")),
        0o600,
        "journal create"
    );
    j.append(evt("a.md")).unwrap();
    j.append(evt("a.md")).unwrap();
    j.compact().unwrap();
    assert_eq!(
        mode(&fx.d.join("push_journal.jsonl")),
        0o600,
        "journal rewrite"
    );
    assert_eq!(mode(&fx.d.join("push_journal.lock")), 0o600, "lock");
    let line = std::fs::read_to_string(fx.d.join("push_journal.jsonl")).unwrap();
    assert!(
        line.starts_with('{') && line.ends_with('\n'),
        "raw jsonl: {line:?}"
    );

    let shadow = crate::sync_shadow::ShadowStore::load(&fx.sd);
    shadow.record("n.md", "h");
    let _c = shadow.flush().unwrap();
    let bs = crate::base_seq_store::BaseSeqStore::load(&fx.sd);
    bs.record_adopted("n.md", 4);
    let _c = bs.flush().unwrap();
    let rr = crate::read_receipt::ReadReceiptStore::load(&fx.sd);
    rr.record_verified(
        "n.md",
        crate::read_receipt::Receipt {
            revision_seq: 4,
            body_sha: "s".into(),
        },
    );
    let _c = rr.flush().unwrap();
    for n in ["shadow_hashes.json", "base_seq.json", "read_receipt.json"] {
        assert_eq!(mode(&fx.d.join(n)), 0o600, "{n}");
        let raw = std::fs::read(fx.d.join(n)).unwrap();
        assert_eq!(raw.first(), Some(&b'{'), "{n}: raw JSON, never framed");
    }
    // The raw stores reload (a framed file would load EMPTY).
    assert_eq!(
        crate::sync_shadow::ShadowStore::load(&fx.sd)
            .get("n.md")
            .as_deref(),
        Some("h")
    );
    assert_eq!(
        crate::base_seq_store::BaseSeqStore::load(&fx.sd).get("n.md"),
        Some(4)
    );

    let mut det =
        DeleteBurstDetector::new(1, std::time::Duration::from_secs(30)).with_pause_marker(&fx.sd);
    det.record_delete();
    assert_eq!(mode(&fx.d.join("delete-burst-PAUSED")), 0o600, "marker");
    sse_consumer()
        .with_state_dir(&fx.sd)
        .persist_last_event_id("7-1");
    assert_eq!(mode(&fx.d.join("last_event_id")), 0o600, "last_event_id");
    assert_eq!(
        std::fs::read(fx.d.join("last_event_id")).unwrap(),
        b"7-1",
        "last_event_id stays raw"
    );
    assert!(DeleteLedger::new(&fx.sd).record("x.md"));
    assert_eq!(
        mode(&fx.d.join("delete-burst-SUPPRESSED")),
        0o600,
        "new ledger"
    );

    // An existing ledger keeps its mode.
    let fx2 = fixture();
    std::fs::write(fx2.d.join("delete-burst-SUPPRESSED"), b"").unwrap();
    std::fs::set_permissions(
        fx2.d.join("delete-burst-SUPPRESSED"),
        std::fs::Permissions::from_mode(0o644),
    )
    .unwrap();
    assert!(DeleteLedger::new(&fx2.sd).record("y.md"));
    assert_eq!(mode(&fx2.d.join("delete-burst-SUPPRESSED")), 0o644);

    let cfg = TempDir::new().unwrap();
    let cd = StateDir::for_test_scoped(cfg.path(), Scope::ConfigDir);
    crate::retry_ledger::RetryLedger::load(&cd).record_failure("z.md", "e");
    assert_eq!(mode(&cfg.path().join("reconcile-retry-ledger.json")), 0o600);
    // Exactly the v1 file names, nothing else.
    let mut got = names(&fx.d);
    got.sort();
    assert_eq!(
        got,
        vec![
            "base_seq.json",
            "delete-burst-PAUSED",
            "delete-burst-SUPPRESSED",
            "last_event_id",
            "push_journal.jsonl",
            "push_journal.lock",
            "read_receipt.json",
            "shadow_hashes.json",
        ]
    );
}

/// Run `test` (an ignored child test of this module) inside
/// `unshare -rm`; `None` (SKIPPED-LOUD) when unshare is unavailable.
#[cfg(target_os = "linux")]
fn run_in_unshare(test: &str) -> Option<std::process::Output> {
    let probe = std::process::Command::new("unshare")
        .args([
            "-rm",
            "sh",
            "-c",
            "d=$(mktemp -d) && e=$(mktemp -d) && mount --bind \"$d\" \"$e\" && umount \"$e\"",
        ])
        .output();
    match probe {
        Ok(o) if o.status.success() => {}
        Ok(o) => {
            eprintln!(
                "SKIPPED-LOUD {test}: `unshare -rm` + bind mount not permitted here ({}). REQUIRED on link and icarus at rollout (CS-2 §7).",
                String::from_utf8_lossy(&o.stderr).trim()
            );
            return None;
        }
        Err(e) => {
            eprintln!(
                "SKIPPED-LOUD {test}: unshare unavailable ({e}). REQUIRED on link and icarus at rollout (CS-2 §7)."
            );
            return None;
        }
    }
    Some(
        std::process::Command::new("unshare")
            .arg("-rm")
            .arg(std::env::current_exe().unwrap())
            .args([
                "--exact",
                &format!("state_io::owner_tests::{test}"),
                "--ignored",
                "--test-threads=1",
                "--nocapture",
            ])
            .env("CS2_UNSHARE_CHILD", "1")
            .output()
            .unwrap(),
    )
}

#[cfg(target_os = "linux")]
fn bind(src: &Path, dst: &Path) {
    let o = std::process::Command::new("mount")
        .arg("--bind")
        .arg(src)
        .arg(dst)
        .output()
        .unwrap();
    assert!(o.status.success(), "mount --bind: {o:?}");
}

/// T-CS2-6 child (runs inside `unshare -rm`).
#[cfg(target_os = "linux")]
#[test]
#[ignore = "T-CS2-6 child; spawned by t_cs2_6_bind_alias_no_false_positive"]
fn t_cs2_6_child() {
    if std::env::var_os("CS2_UNSHARE_CHILD").is_none() {
        return;
    }
    let tmp = TempDir::new().unwrap();
    let a = tmp.path().join("A");
    let b = tmp.path().join("B");
    std::fs::create_dir_all(&a).unwrap();
    std::fs::create_dir_all(&b).unwrap();
    bind(&a, &b);
    let (reg, latch) = {
        let latch = Arc::new(Latch::new());
        let cfg = tmp.path().join("cfg");
        std::fs::create_dir_all(&cfg).unwrap();
        (
            bootstrap_with_latch(&a, &["s6".into()], &cfg, latch.clone()).unwrap(),
            latch,
        )
    };
    let sd = reg.get("s6").unwrap();
    for _ in 0..1000 {
        assert!(sd.custody().is_ok(), "bind alias must not false-positive");
    }
    assert!(DeleteLedger::new(&sd).record("seen-at-b.md"));
    let at_b = b
        .join(layout::RUNTIME_DIR_NAME)
        .join("s6")
        .join("sync-state")
        .join("delete-burst-SUPPRESSED");
    assert!(at_b.exists(), "the StateDir write is visible at B");
    assert!(latch.exits.lock().unwrap().is_empty(), "no exit");
    println!("T6_OK");
}

/// T-CS2-6 (T-S7 control, bind alias): in `unshare -rm`, bind A at B,
/// bootstrap via A: custody stays Ok across 1000 checks, a StateDir write is
/// visible at B, and no exit happens. SKIPPED-LOUD where unshare is
/// unavailable (required on link and icarus at rollout).
#[cfg(target_os = "linux")]
#[test]
fn t_cs2_6_bind_alias_no_false_positive() {
    let Some(out) = run_in_unshare("t_cs2_6_child") else {
        return;
    };
    assert!(out.status.success(), "{out:?}");
    assert!(
        String::from_utf8_lossy(&out.stdout).contains("T6_OK"),
        "{out:?}"
    );
}

/// T-CS2-7 child (runs inside `unshare -rm`).
#[cfg(target_os = "linux")]
#[test]
#[ignore = "T-CS2-7 child; spawned by t_cs2_7_over_mount_exits_74"]
fn t_cs2_7_child() {
    if std::env::var_os("CS2_UNSHARE_CHILD").is_none() {
        return;
    }
    let tmp = TempDir::new().unwrap();
    let ws = tmp.path().join("ws");
    let e = tmp.path().join("E");
    std::fs::create_dir_all(&ws).unwrap();
    std::fs::create_dir_all(&e).unwrap();
    let latch = Arc::new(Latch::new());
    let cfg = tmp.path().join("cfg");
    std::fs::create_dir_all(&cfg).unwrap();
    let reg = bootstrap_with_latch(&ws, &["s7".into()], &cfg, latch.clone()).unwrap();
    let sd = reg.get("s7").unwrap();
    let leaf = super::tests::leaf(&ws, "s7");
    bind(&e, &leaf);
    // Seam-bypassed write lands in the held inode, not in E.
    sd.seams.custody_disabled.store(true, Ordering::SeqCst);
    assert!(DeleteLedger::new(&sd).record("held.md"));
    sd.seams.custody_disabled.store(false, Ordering::SeqCst);
    assert!(names(&e).is_empty(), "nothing in E");
    // Custody: Replaced, exit 74.
    assert!(matches!(
        sd.custody(),
        Err(OwnershipLost {
            reason: LossReason::Replaced { .. }
        })
    ));
    let e2 = sd.append(StateFile::DeleteLedger, b"x\n").unwrap_err();
    assert!(is_ownership_lost(&e2.0));
    assert!(matches!(
        latch.exits.lock().unwrap().as_slice(),
        [(74, LossReason::Replaced { .. })]
    ));
    // Unmount: the held inode has the seam-bypassed write.
    let o = std::process::Command::new("umount")
        .arg(&leaf)
        .output()
        .unwrap();
    assert!(o.status.success(), "{o:?}");
    assert!(leaf.join("delete-burst-SUPPRESSED").exists());
    println!("T7_OK");
}

/// T-CS2-7 (T-S8, over-mount): bind an empty E over the held canonical
/// path: custody returns Replaced, the process exits 74, and a seam-bypassed
/// write lands in the held inode and not in E. SKIPPED-LOUD where unshare is
/// unavailable (required on link and icarus at rollout).
#[cfg(target_os = "linux")]
#[test]
fn t_cs2_7_over_mount_exits_74() {
    let Some(out) = run_in_unshare("t_cs2_7_child") else {
        return;
    };
    assert!(out.status.success(), "{out:?}");
    assert!(
        String::from_utf8_lossy(&out.stdout).contains("T7_OK"),
        "{out:?}"
    );
}

/// Bind-mounted state dir child (runs inside `unshare -rm`).
#[cfg(target_os = "linux")]
#[test]
#[ignore = "bind-mount valve child; spawned by t_cs2_31_bind_mounted_state_dir_unlatches"]
fn t_cs2_31_bind_child() {
    if std::env::var_os("CS2_UNSHARE_CHILD").is_none() {
        return;
    }
    let tmp = TempDir::new().unwrap();
    let src = tmp.path().join("src");
    let ws = tmp.path().join("ws");
    let state = ws.join("state");
    std::fs::create_dir_all(&src).unwrap();
    std::fs::create_dir_all(&state).unwrap();
    bind(&src, &state);
    let sd = StateDir::for_test(&state);
    assert!(
        sd.held_dir_linked().unwrap(),
        "a bind-mounted dir is linked"
    );
    let mut det = latched_detector(&sd);
    std::fs::remove_file(state.join(StateFile::PauseMarker.name())).unwrap();
    assert!(det.poll_marker_resume(), "clearing the marker must resume");
    assert!(!det.is_paused());
    assert!(sd.test_latch().exits.lock().unwrap().is_empty());
    println!("BIND_OK");
}

/// §3.3a step 3 on a bind-mounted state dir: with the valve latched,
/// removing the marker unlatches it (a `readdir` d_ino of the parent is the
/// underlying mountpoint inode, MEASURED on link, so a d_ino match would
/// latch it permanently). SKIPPED-LOUD where unshare is unavailable.
#[cfg(target_os = "linux")]
#[test]
fn t_cs2_31_bind_mounted_state_dir_unlatches() {
    let Some(out) = run_in_unshare("t_cs2_31_bind_child") else {
        return;
    };
    assert!(out.status.success(), "{out:?}");
    assert!(
        String::from_utf8_lossy(&out.stdout).contains("BIND_OK"),
        "{out:?}"
    );
}
