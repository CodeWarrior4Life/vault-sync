//! Unit CS-3 (TKT-b6d8c4ba) in-process tests. The daemon-subprocess tests
//! live in `cs3_harness.rs`. Each test names the mutation that must turn it
//! red (spec section 6); the mutation table is in the PR body.
#![allow(clippy::disallowed_methods)]

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tempfile::TempDir;

use crate::api_client::ApiClient;
use crate::journal_owner::{JournalOwner, OffloadPath};
use crate::ownership::{BootConfig, ConfigWatch, OwnershipSnapshot};
use crate::push_journal::{
    new_event_id, JournalError, PushAction, PushBase, PushEvent, PushJournal, CURRENT_SCHEMA,
};
use crate::state_io::{ExitGate, StateDir, StateFile};
use crate::supervise::{Host, Liveness, Supervisor, TaskClass, TaskEnd, TaskSpec};
use crate::sync_handles::{
    BorrowRefused, PipelineStage, RootHandles, SyncHandles, SyncHandlesParts, SyncHandlesSlot,
    FIRST_DRAIN_DONE, WATCHER_STARTED,
};
use crate::sync_health::StateHealthEvent;
use crate::sync_shadow::ShadowStore;

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// A [`Host`] that records instead of acting.
#[derive(Default)]
pub(crate) struct RecordingHost {
    pub notices: Mutex<Vec<(String, String)>>,
    pub emits: Mutex<Vec<String>>,
    pub exits: Mutex<Vec<i32>>,
    pub restarts: AtomicUsize,
}

impl Host for RecordingHost {
    fn notify(&self, title: &str, body: &str) {
        self.notices
            .lock()
            .unwrap()
            .push((title.to_string(), body.to_string()));
    }
    fn emit(&self, event: &str, _payload: serde_json::Value) {
        self.emits.lock().unwrap().push(event.to_string());
    }
    fn exit(&self, code: i32) {
        self.exits.lock().unwrap().push(code);
    }
    fn restart(&self) {
        self.restarts.fetch_add(1, Ordering::SeqCst);
    }
}

impl RecordingHost {
    fn exits(&self) -> Vec<i32> {
        self.exits.lock().unwrap().clone()
    }
    fn notices_titled(&self, title: &str) -> usize {
        self.notices
            .lock()
            .unwrap()
            .iter()
            .filter(|(t, _)| t == title)
            .count()
    }
}

fn evt(path: &str) -> PushEvent {
    PushEvent {
        schema_version: CURRENT_SCHEMA,
        id: new_event_id(),
        path: path.to_string(),
        action: PushAction::Modify,
        base_hash: PushBase::KnownBase("0".repeat(64)),
        content_sha: "1".repeat(64),
        content_bytes: Some(b"x".to_vec()),
        queued_at: chrono::Utc::now(),
        device_id: "dev-cs3".into(),
    }
}

fn sha256_hex(b: &[u8]) -> String {
    use sha2::Digest;
    hex::encode(sha2::Sha256::digest(b))
}

/// One config root for [`HandlesFx`].
struct RootDef {
    /// The root dir's basename (its vault folder).
    folder: &'static str,
    /// The root's own subscriber ("" = the top-level one).
    sub: &'static str,
}

/// A published `SyncHandles` built in-process: a temp config file, one
/// test `StateDir` per subscriber, a journal owner per root sub, the
/// daemon's shadow from the top-level dir with the real `vault_folders`.
struct HandlesFx {
    _dirs: Vec<TempDir>,
    cfg_path: PathBuf,
    vaults: PathBuf,
    snapshot: Arc<OwnershipSnapshot>,
    handles: Arc<SyncHandles>,
    slot: SyncHandlesSlot,
    sup: Arc<Supervisor>,
    host: Arc<RecordingHost>,
    sds: std::collections::BTreeMap<String, Arc<StateDir>>,
}

fn config_toml(url: &str, top: &str, vaults: &Path, roots: &[RootDef]) -> String {
    let mut s = format!(
        "nexus_url = \"{url}\"\nsubscriber_id = \"{top}\"\nvaults_root = \"{}\"\ndaemon_version = \"0.0.0-cs3\"\ndaemon_platform = \"test\"\n",
        vaults.display()
    );
    for r in roots {
        s.push_str(&format!(
            "\n[[sync_roots]]\npath = \"{}\"\nroute = \"\"\nsubscriber_id = \"{}\"\n",
            vaults.join(r.folder).display(),
            r.sub
        ));
    }
    s
}

/// Build the fixture. `pre_shadow` is written into the top-level dir's
/// shadow file BEFORE the store loads (legacy-key tests).
fn handles_fx(
    url: &str,
    top: &str,
    roots: &[RootDef],
    pre_shadow: Option<&str>,
    token_for: impl Fn(&str) -> String,
) -> HandlesFx {
    let base = TempDir::new().unwrap();
    let cfg_dir = base.path().join("cfg");
    let vaults = base.path().join("vaults");
    std::fs::create_dir_all(&cfg_dir).unwrap();
    for r in roots {
        std::fs::create_dir_all(vaults.join(r.folder)).unwrap();
    }
    let cfg_path = cfg_dir.join(crate::config::default_config_path().file_name().unwrap());
    std::fs::write(&cfg_path, config_toml(url, top, &vaults, roots)).unwrap();
    let snapshot = match BootConfig::load_at_startup_with(&cfg_path, &cfg_dir) {
        BootConfig::Loaded(s) => s,
        other => panic!("fixture config did not load: {other:?}"),
    };
    let mut sds = std::collections::BTreeMap::new();
    for sub in snapshot.subs() {
        let d = base.path().join("state").join(sub);
        std::fs::create_dir_all(&d).unwrap();
        sds.insert(sub.clone(), StateDir::for_test(&d));
    }
    let top_sd = sds[snapshot.top_sub()].clone();
    if let Some(json) = pre_shadow {
        std::fs::write(
            base.path()
                .join("state")
                .join(top)
                .join(StateFile::Shadow.name()),
            json,
        )
        .unwrap();
    }
    let folders: Vec<String> = roots.iter().map(|r| r.folder.to_string()).collect();
    let shadow = ShadowStore::load_with_vault_folders(&top_sd, folders.clone());
    let base_seq =
        crate::base_seq_store::BaseSeqStore::load_with_vault_folders(&top_sd, folders.clone());
    let read_receipt =
        crate::read_receipt::ReadReceiptStore::load_with_vault_folders(&top_sd, folders);
    let mut journals = std::collections::BTreeMap::new();
    let mut root_handles = Vec::new();
    for spec in snapshot.roots() {
        let owner = journals
            .entry(spec.eff_sub.clone())
            .or_insert_with(|| JournalOwner::open(&sds[&spec.eff_sub], &spec.eff_sub).unwrap())
            .clone();
        let rh = RootHandles::new(spec.clone(), Some(owner));
        rh.set_api(Arc::new(
            ApiClient::new(url, &token_for(&spec.eff_sub)).unwrap(),
        ));
        rh.mark(WATCHER_STARTED);
        rh.mark(FIRST_DRAIN_DONE);
        root_handles.push(rh);
    }
    let host = Arc::new(RecordingHost::default());
    let sup = Supervisor::with_gate(host.clone(), ExitGate::detached());
    let handles = Arc::new(SyncHandles::new(SyncHandlesParts {
        snapshot: snapshot.clone(),
        shadow,
        base_seq,
        read_receipt,
        journals,
        roots: root_handles,
        live: sup.live_sender(),
    }));
    handles.mark_sse_connected();
    let slot = SyncHandlesSlot::default();
    assert!(slot.publish(handles.clone()));
    HandlesFx {
        _dirs: vec![base],
        cfg_path,
        vaults,
        snapshot,
        handles,
        slot,
        sup,
        host,
        sds,
    }
}

fn one_root(url: &str) -> HandlesFx {
    handles_fx(
        url,
        "sub-y",
        &[RootDef {
            folder: "Mainframe",
            sub: "",
        }],
        None,
        |s| format!("tok-{s}"),
    )
}

/// Replace `p` with same-size content and restore its mtime.
fn same_size_rewrite(p: &Path) {
    let before = std::fs::metadata(p).unwrap();
    let mtime = before.modified().unwrap();
    let mut bytes = std::fs::read(p).unwrap();
    let i = bytes
        .iter()
        .position(|b| *b == b'0')
        .expect("a digit to flip");
    bytes[i] = b'9';
    std::fs::write(p, &bytes).unwrap();
    let f = std::fs::OpenOptions::new().write(true).open(p).unwrap();
    f.set_modified(mtime).unwrap();
    let after = std::fs::metadata(p).unwrap();
    assert_eq!(after.len(), before.len());
    assert_eq!(after.modified().unwrap(), mtime);
}

fn pending_restart_events(snapshot_sha8: &str) -> Vec<StateHealthEvent> {
    crate::sync_health::state_events()
        .into_iter()
        .filter(|e| match e {
            StateHealthEvent::ConfigPendingRestart {
                snapshot_sha8: s, ..
            }
            | StateHealthEvent::ConfigPendingRestartCleared { snapshot_sha8: s } => {
                s == snapshot_sha8
            }
            _ => false,
        })
        .collect()
}

// ---------------------------------------------------------------------------
// T-CS3-3, T-CS3-4: config change surfaced, never reloaded
// ---------------------------------------------------------------------------

/// T-CS3-3 (AR-1C-005): a same-size replacement with its mtime restored is
/// raised within 2 ticks under paused time, and a V&R borrow made BEFORE the
/// first tick is refused with `ConfigChanged`.
/// Mutations: gate the hash on an mtime or size change; let the borrow
/// trust the tick.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn t_cs3_3_same_size_change_raised_and_borrow_refused_before_tick() {
    let fx = one_root("http://127.0.0.1:1");
    let sha8 = fx.snapshot.sha8();
    same_size_rewrite(&fx.cfg_path);

    // The borrow re-hashes at click time: refused before any tick.
    let r = crate::commands::verify_repair_preflight(Some(&fx.slot));
    match r {
        Err(BorrowRefused::ConfigChanged {
            snapshot_sha8,
            disk_sha8,
        }) => {
            assert_eq!(snapshot_sha8, sha8);
            assert_ne!(disk_sha8, sha8);
        }
        Err(other) => panic!("expected ConfigChanged, got {other:?}"),
        Ok(_) => panic!("expected ConfigChanged, the borrow succeeded"),
    }

    let tray = Arc::new(std::sync::RwLock::new(crate::tray_state::TrayState::new(
        String::new(),
        String::new(),
        PathBuf::new(),
    )));
    let period = Duration::from_secs(60);
    let task = tokio::spawn(crate::ownership::run_config_watch(
        ConfigWatch::new(fx.snapshot.clone(), Some(tray.clone())),
        period,
    ));
    let mut raised_at = None;
    for tick in 1..=2 {
        tokio::time::advance(period).await;
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        if tray.read().unwrap().config_pending_restart {
            raised_at = Some(tick);
            break;
        }
    }
    task.abort();
    assert!(
        raised_at.is_some(),
        "config_pending_restart not raised within 2 ticks of a same-size, same-mtime change"
    );
    assert_eq!(
        pending_restart_events(&sha8)
            .iter()
            .filter(|e| matches!(e, StateHealthEvent::ConfigPendingRestart { .. }))
            .count(),
        1
    );
}

/// T-CS3-4 (surface, never reload; in-process half): across 10 ticks the
/// event is raised once per distinct disk hash, a second distinct change
/// raises once more, a missing file raises with `disk_sha8 = "absent"`, and
/// restoring the original bytes raises `config_pending_restart_cleared`.
/// The live half (an edit still drains and pushes; no task restarts; no
/// exit) is `cs3_harness::t_cs3_4_config_change_keeps_syncing`.
/// Mutations: raise the event every tick; re-run bootstrap on change.
#[test]
fn t_cs3_4_raised_once_per_distinct_hash_and_cleared() {
    let fx = one_root("http://127.0.0.1:1");
    let original = std::fs::read(&fx.cfg_path).unwrap();
    let mut w = ConfigWatch::new(fx.snapshot.clone(), None);
    for _ in 0..3 {
        w.tick();
    }
    assert!(
        w.raised().is_empty(),
        "unchanged config raised {:?}",
        w.raised()
    );
    same_size_rewrite(&fx.cfg_path);
    for _ in 0..10 {
        w.tick();
    }
    assert_eq!(w.raised().len(), 1, "{:?}", w.raised());
    std::fs::write(&fx.cfg_path, b"a different config\n").unwrap();
    for _ in 0..10 {
        w.tick();
    }
    assert_eq!(w.raised().len(), 2, "{:?}", w.raised());
    std::fs::remove_file(&fx.cfg_path).unwrap();
    for _ in 0..3 {
        w.tick();
    }
    assert!(matches!(
        w.raised().last(),
        Some(StateHealthEvent::ConfigPendingRestart { disk_sha8, .. }) if disk_sha8 == "absent"
    ));
    std::fs::write(&fx.cfg_path, &original).unwrap();
    for _ in 0..10 {
        w.tick();
    }
    assert_eq!(w.raised().len(), 4, "{:?}", w.raised());
    assert!(matches!(
        w.raised().last(),
        Some(StateHealthEvent::ConfigPendingRestartCleared { .. })
    ));
    // Never reloaded: the snapshot the handles hold is unchanged.
    assert!(Arc::ptr_eq(fx.handles.snapshot(), &fx.snapshot));
}

// ---------------------------------------------------------------------------
// T-CS3-5: one owner per journal file per process, never released
// ---------------------------------------------------------------------------

/// T-CS3-5 (I-OWN-4, gate C8): a second `JournalOwner::open` on the same
/// `StateDir` returns `AlreadyOwned` and opens no second lock description;
/// after every owner `Arc` is dropped a third open STILL returns
/// `AlreadyOwned`. (The gate half is in tests/state_io_gate.rs.)
/// Mutations: drop the claim check; clear the claim bit on drop.
#[test]
fn t_cs3_5_one_claim_never_released() {
    let d = TempDir::new().unwrap();
    let sd = StateDir::for_test(d.path());
    let lock_opens = |sd: &StateDir| {
        sd.seams
            .recorder
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.as_str() == "openat:push_journal.lock")
            .count()
    };
    let first = JournalOwner::open(&sd, "sub-a").unwrap();
    assert!(sd.is_claimed(StateFile::PushJournal));
    assert_eq!(lock_opens(&sd), 1);
    match JournalOwner::open(&sd, "sub-a") {
        Err(JournalError::AlreadyOwned(_)) => {}
        other => panic!("second open: expected AlreadyOwned, got {other:?}"),
    }
    assert_eq!(
        lock_opens(&sd),
        1,
        "the refused open opened a lock description"
    );
    let clone = Arc::clone(&first);
    drop(first);
    drop(clone);
    match JournalOwner::open(&sd, "sub-a") {
        Err(JournalError::AlreadyOwned(_)) => {}
        other => panic!("re-open after drop: expected AlreadyOwned, got {other:?}"),
    }
    assert!(sd.is_claimed(StateFile::PushJournal));
    assert_eq!(sd.seams.journal_constructs.load(Ordering::SeqCst), 1);
    // A claim taken on one StateDir does not open another.
    let d2 = TempDir::new().unwrap();
    let sd2 = StateDir::for_test(d2.path());
    let claim = sd2.claim(StateFile::PushJournal).unwrap();
    assert!(matches!(
        PushJournal::open_claimed(&sd, claim),
        Err(JournalError::AlreadyOwned(_))
    ));
}

// ---------------------------------------------------------------------------
// T-CS3-6: the AR-1C-018 race through ONE owner
// ---------------------------------------------------------------------------

/// T-CS3-6 (T-J1): on a multi-thread runtime, watcher `append` ‖ V&R
/// `append_batch` (a real borrow through `verify_repair_with`) ‖ `drain` ‖
/// `ack_batch` ‖ `compact` ‖ compensating `append`, ~10k operations through
/// ONE owner: every appended id is drained-and-acked or still live (0 lost),
/// no acked id reappears, and the `PushJournal` construction count for the
/// `StateDir` is 1.
/// Mutation: V&R opens its own `PushJournal` (count 2).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t_cs3_6_race_through_one_owner() {
    let mut srv = mockito::Server::new_async().await;
    let fx = one_root(&srv.url());
    let root = fx.vaults.join("Mainframe");
    // V&R: each pass finds 3 missing-on-server notes and appends them.
    for i in 0..3 {
        std::fs::write(root.join(format!("vr{i}.md")), format!("body {i}\n")).unwrap();
    }
    let deltas: Vec<serde_json::Value> = (0..3)
        .map(|i| serde_json::json!({"path": format!("vr{i}.md"), "state": "missing-on-server"}))
        .collect();
    let _m = srv
        .mock("POST", "/api/sync/reconcile-batch")
        .with_status(200)
        .with_body(serde_json::json!({ "deltas": deltas }).to_string())
        .expect_at_least(1)
        .create_async()
        .await;
    let owner = fx.handles.journal("sub-y").unwrap();
    let sd = owner.state_dir().unwrap();
    assert!(Arc::ptr_eq(&sd, &fx.sds["sub-y"]));
    let appended = Arc::new(Mutex::new(std::collections::HashSet::<String>::new()));
    let acked = Arc::new(Mutex::new(std::collections::HashSet::<String>::new()));
    let reappeared = Arc::new(AtomicUsize::new(0));
    let done = Arc::new(AtomicBool::new(false));
    const N: usize = 2500;

    let mut writers = Vec::new();
    for tag in ["watcher", "compensating"] {
        let (o, a) = (owner.clone(), appended.clone());
        writers.push(tokio::spawn(async move {
            for i in 0..N {
                let e = evt(&format!("{tag}/{i}.md"));
                let id = e.id.clone();
                o.append(e).unwrap();
                a.lock().unwrap().insert(id);
                if i % 64 == 0 {
                    tokio::task::yield_now().await;
                }
            }
        }));
    }
    let vr = {
        let slot = fx.slot.clone();
        tokio::spawn(async move {
            let mut ok = 0;
            for _ in 0..20 {
                if crate::commands::verify_repair_with(Some(&slot))
                    .await
                    .is_ok()
                {
                    ok += 1;
                }
            }
            ok
        })
    };
    let drainer = {
        let (o, ak, re, dn) = (
            owner.clone(),
            acked.clone(),
            reappeared.clone(),
            done.clone(),
        );
        tokio::spawn(async move {
            loop {
                let batch = o.drain(64).unwrap();
                if batch.is_empty() && dn.load(Ordering::SeqCst) {
                    break;
                }
                let mut cursors = Vec::new();
                {
                    let mut g = ak.lock().unwrap();
                    for (e, c) in batch {
                        if !g.insert(e.id.clone()) {
                            re.fetch_add(1, Ordering::SeqCst);
                        }
                        cursors.push(c);
                    }
                }
                o.ack_batch(cursors).unwrap();
                tokio::task::yield_now().await;
            }
        })
    };
    let compactor = {
        let (o, dn) = (owner.clone(), done.clone());
        tokio::spawn(async move {
            while !dn.load(Ordering::SeqCst) {
                o.compact().unwrap();
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
    };
    for w in writers {
        w.await.unwrap();
    }
    let vr_ok = vr.await.unwrap();
    assert!(vr_ok > 0, "no V&R pass ran");
    done.store(true, Ordering::SeqCst);
    compactor.await.unwrap();
    drainer.await.unwrap();

    // V&R's appended ids are whatever the drainer saw beyond the writers'.
    let appended = appended.lock().unwrap().clone();
    let acked = acked.lock().unwrap().clone();
    let live: std::collections::HashSet<String> = owner
        .drain(usize::MAX)
        .unwrap()
        .into_iter()
        .map(|(e, _)| e.id)
        .collect();
    let lost: Vec<&String> = appended
        .iter()
        .filter(|id| !acked.contains(*id) && !live.contains(*id))
        .collect();
    // compact() collapses one-per-path; the writers use distinct paths, so
    // nothing they appended may be collapsed away.
    assert!(lost.is_empty(), "{} appended ids lost", lost.len());
    assert_eq!(
        reappeared.load(Ordering::SeqCst),
        0,
        "an acked id reappeared"
    );
    assert_eq!(
        sd.seams.journal_constructs.load(Ordering::SeqCst),
        1,
        "the StateDir's PushJournal was constructed more than once"
    );
}

// ---------------------------------------------------------------------------
// T-CS3-10: the offload matrix (gate C3)
// ---------------------------------------------------------------------------

/// Run `append` + `drain` on `owner` and report the offload path taken.
fn journal_ops(owner: &JournalOwner) -> Option<OffloadPath> {
    owner.append(evt("matrix.md")).unwrap();
    let p = crate::journal_owner::last_offload_path();
    let _ = owner.drain(8).unwrap();
    p
}

/// Measure the worst gap of a 10 ms ticker spawned on `rt` while `work`
/// runs; returns (work result, worst gap).
fn with_sibling_ticker<R>(rt: &tokio::runtime::Runtime, work: impl FnOnce() -> R) -> (R, Duration) {
    let stop = Arc::new(AtomicBool::new(false));
    let worst = Arc::new(Mutex::new(Duration::ZERO));
    let (s2, w2) = (stop.clone(), worst.clone());
    let ticker = rt.spawn(async move {
        let mut last = Instant::now();
        while !s2.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(10)).await;
            let gap = last.elapsed();
            last = Instant::now();
            let mut w = w2.lock().unwrap();
            if gap > *w {
                *w = gap;
            }
        }
    });
    std::thread::sleep(Duration::from_millis(50));
    *worst.lock().unwrap() = Duration::ZERO;
    let r = work();
    stop.store(true, Ordering::SeqCst);
    let _ = rt.block_on(ticker);
    let w = *worst.lock().unwrap();
    (r, w)
}

/// T-CS3-10 (T-J3, gate C3 matrix): `append` + `drain` from each of the five
/// contexts completes without panic within 2 s; on the multi-thread contexts
/// a sibling task on the same runtime keeps ticking every 10 ms with no
/// stall above 100 ms while each journal op holds the lock 150 ms; the
/// `IN_OFFLOAD` assertion holds everywhere; inline is observed only on the
/// plain thread and the current-thread runtime.
/// Mutations: call `self.j.append(e)` directly; inline whenever `try_current`
/// is `Ok`; `block_in_place` on a current-thread runtime.
#[test]
fn t_cs3_10_offload_matrix() {
    let d = TempDir::new().unwrap();
    let sd = StateDir::for_test(d.path());
    let owner = JournalOwner::open(&sd, "sub-matrix").unwrap();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    owner.inner().set_hold_ms(150);

    // 1. A multi-thread runtime worker (the drain loop, the watcher task).
    let o = owner.clone();
    let started = Instant::now();
    let (path, worst) = with_sibling_ticker(&rt, || {
        rt.block_on(rt.spawn(async move { journal_ops(&o) }))
            .unwrap()
    });
    assert!(started.elapsed() < Duration::from_secs(2));
    assert_eq!(path, Some(OffloadPath::BlockInPlace), "worker");
    assert!(
        worst < Duration::from_millis(100),
        "worker: sibling stalled {worst:?}"
    );

    // 2. A spawn_blocking thread (V&R's hashing pool).
    let o = owner.clone();
    let (path, worst) = with_sibling_ticker(&rt, || {
        rt.block_on(rt.spawn_blocking(move || journal_ops(&o)))
            .unwrap()
    });
    assert_eq!(path, Some(OffloadPath::BlockInPlace), "spawn_blocking");
    assert!(
        worst < Duration::from_millis(100),
        "spawn_blocking: sibling stalled {worst:?}"
    );

    // 3. A thread inside a multi-thread runtime's block_on (the CLI).
    let o = owner.clone();
    let h = rt.handle().clone();
    let (path, worst) = with_sibling_ticker(&rt, || {
        std::thread::spawn(move || h.block_on(async move { journal_ops(&o) }))
            .join()
            .unwrap()
    });
    assert_eq!(path, Some(OffloadPath::BlockInPlace), "block_on thread");
    assert!(
        worst < Duration::from_millis(100),
        "block_on: sibling stalled {worst:?}"
    );

    // 4. A plain std::thread: inline.
    let o = owner.clone();
    let path = std::thread::spawn(move || journal_ops(&o)).join().unwrap();
    assert_eq!(path, Some(OffloadPath::Inline), "plain thread");

    // 5. A current-thread runtime (#[tokio::test] default): inline, no
    //    panic (block_in_place would panic here). The design runs in place,
    //    so this runtime's one thread is busy for the op's duration.
    let ct = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let o = owner.clone();
    let path = ct.block_on(async move { journal_ops(&o) });
    assert_eq!(path, Some(OffloadPath::Inline), "current-thread runtime");
    owner.inner().set_hold_ms(0);
}

// ---------------------------------------------------------------------------
// T-CS3-12: the recorded lock order
// ---------------------------------------------------------------------------

/// T-CS3-12 (D16): path lock then journal (the compensating path) passes;
/// a planted inversion that takes a path lock inside the journal lock
/// panics with the `JOURNAL_HELD` message.
/// Mutation: remove the debug assertion.
#[test]
fn t_cs3_12_lock_order_inversion_panics() {
    let d = TempDir::new().unwrap();
    let owner = JournalOwner::open(&StateDir::for_test(d.path()), "sub-lo").unwrap();
    let v = TempDir::new().unwrap();
    let mat = crate::materializer::Materializer::new(
        v.path().to_path_buf(),
        None,
        crate::materializer::MaterializerMode::Live,
        v.path().to_path_buf(),
        "sub-lo".into(),
        crate::materializer::MaterializerConfig::default(),
    );
    // The recorded order: path lock, then the journal.
    {
        let lock = mat.path_lock_for_test("a.md");
        let _g = lock.lock().unwrap();
        owner.append(evt("a.md")).unwrap();
    }
    // The planted inversion.
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        owner.inner().run_holding_lock(|| {
            let _ = mat.path_lock_for_test("b.md");
        })
    }));
    let msg = match r {
        Err(p) => p
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| p.downcast_ref::<&str>().map(|s| s.to_string()))
            .unwrap_or_default(),
        Ok(()) => panic!("taking a path lock inside the journal lock did not panic"),
    };
    assert!(msg.contains("JOURNAL_HELD"), "{msg}");
}

// ---------------------------------------------------------------------------
// T-CS3-13, -14, -15: the V&R borrow
// ---------------------------------------------------------------------------

fn split_fx(url: &str, pre_shadow: Option<&str>) -> HandlesFx {
    handles_fx(
        url,
        "sub-y",
        &[RootDef {
            folder: "Mainframe",
            sub: "sub-x",
        }],
        pre_shadow,
        |s| format!("tok-{s}"),
    )
}

/// T-CS3-13 (T-V1, TKT-b6d8c4ba facets 1 and 2): split config, shadow entry
/// stored under the legacy-prefixed key `Mainframe/notes/a.md` in the
/// top-level dir. The borrowed shadow is `Arc::ptr_eq` with the handles'
/// shadow, and V&R classifies a genuine local edit as PUSH (the pre-CS-3
/// per-root `ShadowStore::load` reads an empty baseline and says PULL).
/// Mutation: `ShadowStore::load(sd)` in V&R.
#[tokio::test]
async fn t_cs3_13_vr_uses_the_daemon_shadow() {
    let server_body = b"server version\n";
    let server_hash = sha256_hex(server_body);
    let legacy = serde_json::json!({ "Mainframe/notes/a.md": server_hash }).to_string();
    let mut srv = mockito::Server::new_async().await;
    let fx = split_fx(&srv.url(), Some(&legacy));
    let root = fx.vaults.join("Mainframe");
    std::fs::create_dir_all(root.join("notes")).unwrap();
    std::fs::write(root.join("notes/a.md"), b"a genuine local edit\n").unwrap();
    let _m = srv
        .mock("POST", "/api/sync/reconcile-batch")
        .with_status(200)
        .with_body(
            serde_json::json!({"deltas": [{"path": "notes/a.md", "state": "drift", "server_hash": server_hash}]})
                .to_string(),
        )
        .expect(1)
        .create_async()
        .await;

    let borrows = crate::commands::verify_repair_preflight(Some(&fx.slot)).unwrap();
    assert!(Arc::ptr_eq(&borrows[0].shadow, fx.handles.shadow()));
    assert_eq!(
        fx.handles.shadow().get("notes/a.md").as_deref(),
        Some(server_hash.as_str())
    );
    drop(borrows);

    let report = crate::commands::verify_repair_with(Some(&fx.slot))
        .await
        .unwrap();
    assert_eq!(
        report.modify_count, 1,
        "a genuine local edit must PUSH: {report:?}"
    );
    assert_eq!(report.add_count, 0, "classified as PULL: {report:?}");
    let owner = fx.handles.journal("sub-x").unwrap();
    assert_eq!(owner.len(), 1, "the push was appended to X's owner");
}

/// T-CS3-14 (facet 3, gate C7; in-process half): split config with distinct
/// tokens. V&R's reconcile request for root X carries X's bearer; with the
/// root's `api` slot unset the borrow refuses `RootNotReady{api}`, the mock
/// sees zero requests and no token load happens. (The pipeline-token half,
/// `RootFailed{token}`, is `cs3_harness::t_cs3_14_root_failed_token`.)
/// Mutation: fall back to `token_store::load(&cfg.subscriber_id)` when the
/// slot is empty.
#[tokio::test]
async fn t_cs3_14_vr_uses_the_root_token() {
    let mut srv = mockito::Server::new_async().await;
    let fx = split_fx(&srv.url(), None);
    std::fs::write(fx.vaults.join("Mainframe/a.md"), b"x\n").unwrap();
    let as_x = srv
        .mock("POST", "/api/sync/reconcile-batch")
        .match_header("authorization", "Bearer tok-sub-x")
        .with_status(200)
        .with_body(r#"{"deltas":[]}"#)
        .expect(1)
        .create_async()
        .await;
    crate::commands::verify_repair_with(Some(&fx.slot))
        .await
        .unwrap();
    as_x.assert_async().await;

    // A root whose api slot is empty (no pipeline client yet).
    let mut srv2 = mockito::Server::new_async().await;
    let none = srv2
        .mock("POST", "/api/sync/reconcile-batch")
        .expect(0)
        .create_async()
        .await;
    let fx2 = split_fx(&srv2.url(), None);
    let spec = fx2.snapshot.roots()[0].clone();
    let bare = RootHandles::new(spec, fx2.handles.journal("sub-x"));
    bare.mark(WATCHER_STARTED);
    bare.mark(FIRST_DRAIN_DONE);
    let handles = Arc::new(SyncHandles::new(SyncHandlesParts {
        snapshot: fx2.snapshot.clone(),
        shadow: fx2.handles.shadow().clone(),
        base_seq: fx2.handles.base_seq().clone(),
        read_receipt: fx2.handles.read_receipt().clone(),
        journals: fx2.handles.journals().clone(),
        roots: vec![bare],
        live: fx2.sup.live_sender(),
    }));
    handles.mark_sse_connected();
    let slot = SyncHandlesSlot::default();
    slot.publish(handles);
    let before = crate::token_store::LOADS.lock().unwrap().len();
    let r = crate::commands::verify_repair_preflight(Some(&slot));
    assert!(
        matches!(&r, Err(BorrowRefused::RootNotReady { missing, .. }) if missing.contains(&"api")),
        "{:?}",
        r.as_ref().err()
    );
    let e = crate::commands::verify_repair_with(Some(&slot))
        .await
        .unwrap_err();
    assert!(e.contains("not ready"), "{e}");
    let loads: Vec<String> = crate::token_store::LOADS.lock().unwrap()[before..].to_vec();
    assert!(
        !loads.iter().any(|s| s == "sub-y" || s == "sub-x"),
        "the borrow loaded a token: {loads:?}"
    );
    none.assert_async().await;
}

/// T-CS3-15 (T-V2 readiness): before publication `NotPublished`; before the
/// first drain `RootNotReady{first_drain}`; before SSE `Connected`
/// `RootNotReady{sse}`; a second root failed at `watcher_start` refuses the
/// whole V&R with `RootFailed{watcher_start}` and ZERO reconcile requests
/// for the healthy first root (preflight); after full readiness it runs.
/// Mutations: `borrow` ignores the readiness bits; V&R borrows lazily inside
/// the root loop (root 1 runs).
#[tokio::test]
async fn t_cs3_15_readiness_and_preflight() {
    // Not published.
    let empty = SyncHandlesSlot::default();
    assert!(matches!(
        crate::commands::verify_repair_preflight(Some(&empty)),
        Err(BorrowRefused::NotPublished { .. })
    ));

    let mut srv = mockito::Server::new_async().await;
    let fx = handles_fx(
        &srv.url(),
        "sub-y",
        &[
            RootDef {
                folder: "Mainframe",
                sub: "",
            },
            RootDef {
                folder: "Second",
                sub: "",
            },
        ],
        None,
        |s| format!("tok-{s}"),
    );
    std::fs::write(fx.vaults.join("Mainframe/a.md"), b"x\n").unwrap();
    let mk = |bits: u8, sse: bool, fail2: Option<PipelineStage>| {
        let roots: Vec<RootHandles> = fx
            .snapshot
            .roots()
            .iter()
            .map(|spec| {
                let r = RootHandles::new(spec.clone(), fx.handles.journal("sub-y"));
                r.set_api(fx.handles.root(spec.key).unwrap().api().unwrap().clone());
                r.mark(bits);
                if spec.key == 1 {
                    if let Some(st) = fail2 {
                        r.fail(st);
                    }
                }
                r
            })
            .collect();
        let h = Arc::new(SyncHandles::new(SyncHandlesParts {
            snapshot: fx.snapshot.clone(),
            shadow: fx.handles.shadow().clone(),
            base_seq: fx.handles.base_seq().clone(),
            read_receipt: fx.handles.read_receipt().clone(),
            journals: fx.handles.journals().clone(),
            roots,
            live: fx.sup.live_sender(),
        }));
        if sse {
            h.mark_sse_connected();
        }
        let slot = SyncHandlesSlot::default();
        slot.publish(h);
        slot
    };
    let none = srv
        .mock("POST", "/api/sync/reconcile-batch")
        .expect(0)
        .create_async()
        .await;
    let s = mk(WATCHER_STARTED, true, None);
    assert!(matches!(
        crate::commands::verify_repair_preflight(Some(&s)),
        Err(BorrowRefused::RootNotReady { root: 0, ref missing }) if missing == &vec!["first_drain"]
    ));
    let s = mk(WATCHER_STARTED | FIRST_DRAIN_DONE, false, None);
    assert!(matches!(
        crate::commands::verify_repair_preflight(Some(&s)),
        Err(BorrowRefused::RootNotReady { root: 0, ref missing }) if missing == &vec!["sse"]
    ));
    let s = mk(
        WATCHER_STARTED | FIRST_DRAIN_DONE,
        true,
        Some(PipelineStage::WatcherStart),
    );
    let e = crate::commands::verify_repair_with(Some(&s))
        .await
        .unwrap_err();
    assert!(e.contains("failed to start at stage watcher_start"), "{e}");
    assert!(matches!(
        crate::commands::verify_repair_preflight(Some(&s)),
        Err(BorrowRefused::RootFailed {
            root: 1,
            stage: PipelineStage::WatcherStart
        })
    ));
    none.assert_async().await;
    none.remove_async().await;

    let ran = srv
        .mock("POST", "/api/sync/reconcile-batch")
        .with_status(200)
        .with_body(r#"{"deltas":[]}"#)
        .expect(2)
        .create_async()
        .await;
    let s = mk(WATCHER_STARTED | FIRST_DRAIN_DONE, true, None);
    let report = crate::commands::verify_repair_with(Some(&s)).await.unwrap();
    assert_eq!(report.files_scanned, 1);
    ran.assert_async().await;
}

/// T-CS3-16 (in-process half): a borrowed V&R whose liveness goes `Stopped`
/// mid-run returns "daemon stopping" and appends nothing; a root whose
/// reconcile backstop ended is refused `RootNotAlive`. (The per-task panic
/// injection with the exit-70 check is `cs3_harness::t_cs3_16_*`.)
/// Mutation: V&R ignores its `live` receiver.
#[tokio::test]
async fn t_cs3_16_vr_stops_on_liveness() {
    let mut srv = mockito::Server::new_async().await;
    let fx = one_root(&srv.url());
    let root = fx.vaults.join("Mainframe");
    std::fs::write(root.join("new.md"), b"new\n").unwrap();
    let _m = srv
        .mock("POST", "/api/sync/reconcile-batch")
        .with_status(200)
        .with_body(r#"{"deltas":[{"path":"new.md","state":"missing-on-server"}]}"#)
        .create_async()
        .await;
    fx.sup.stop("task_ended:drain_loop:0");
    let e = crate::commands::verify_repair_with(Some(&fx.slot))
        .await
        .unwrap_err();
    assert!(
        e.contains("daemon stopping") || e.contains("stopping"),
        "{e}"
    );
    assert_eq!(
        fx.handles.journal("sub-y").unwrap().len(),
        0,
        "appended after the stop"
    );

    // A borrowed V&R that sees the stop after it borrowed (mid-run).
    let mut srv2 = mockito::Server::new_async().await;
    let fx2 = one_root(&srv2.url());
    std::fs::write(fx2.vaults.join("Mainframe/new.md"), b"new\n").unwrap();
    let _m2 = srv2
        .mock("POST", "/api/sync/reconcile-batch")
        .with_status(200)
        .with_body(r#"{"deltas":[{"path":"new.md","state":"missing-on-server"}]}"#)
        .create_async()
        .await;
    let b = crate::commands::verify_repair_preflight(Some(&fx2.slot))
        .unwrap()
        .remove(0);
    fx2.sup.stop("task_ended:watcher:0");
    let vr = crate::verify_repair::VerifyRepair::new(
        b.root.path.clone(),
        b.api,
        b.journal.clone(),
        b.root.eff_sub.clone(),
        crate::verify_repair::VerifyRepairConfig::default(),
    )
    .with_shadow(b.shadow)
    .with_liveness(b.live);
    assert!(matches!(
        vr.run().await,
        Err(crate::verify_repair::VerifyRepairError::Stopped(_))
    ));
    assert_eq!(b.journal.len(), 0, "appended after the stop");

    // RootDegrading: the backstop's end clears the root's alive flag.
    let fx3 = one_root("http://127.0.0.1:1");
    let alive = fx3.handles.root(0).unwrap().alive_flag();
    fx3.sup.task_ended(
        &TaskSpec::root_degrading("reconcile_backstop:0", alive),
        Err("injected".into()),
    );
    assert!(matches!(
        crate::commands::verify_repair_preflight(Some(&fx3.slot)),
        Err(BorrowRefused::RootNotAlive { root: 0, .. })
    ));
    assert!(fx3.host.exits().is_empty(), "a RootDegrading end exited");
    assert!(fx3.sup.is_live());
}

// ---------------------------------------------------------------------------
// T-CS3-17: supervise (in-process half)
// ---------------------------------------------------------------------------

async fn wait_until(mut f: impl FnMut() -> bool, max: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < max {
        if f() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    f()
}

/// T-CS3-17 (VENDOR-DOC claim measured): a panicking task's
/// `tauri::async_runtime::JoinHandle` reports the panic as `Err`; a Required
/// panic and a Required `Ok` return each exit 70 exactly once; an Optional
/// panic raises `task_ended` with no exit; a kill switch raises
/// `task_disabled`; a later end after the stop is cascade (no second exit).
/// Mutation: `supervise` spawns without awaiting the `JoinHandle`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn t_cs3_17_supervise_classes() {
    let r = tauri::async_runtime::spawn(async {
        panic!("CS-3 join-handle probe");
    })
    .await;
    assert!(
        r.is_err(),
        "a panicking task's JoinHandle did not report Err"
    );

    let host = Arc::new(RecordingHost::default());
    let sup = Supervisor::with_gate(host.clone(), ExitGate::detached());
    sup.supervise(TaskSpec::new("opt_probe", TaskClass::Optional), async {
        panic!("optional");
    });
    sup.supervise(
        TaskSpec::new("disabled_probe", TaskClass::RootDegrading),
        async { TaskEnd::Disabled },
    );
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(host.exits().is_empty(), "an Optional end exited");
    assert!(sup.is_live());
    let ev = crate::sync_health::state_events();
    assert!(ev.iter().any(|e| matches!(e, StateHealthEvent::TaskEnded { name, cause } if name == "opt_probe" && cause == "panic")));
    assert!(ev
        .iter()
        .any(|e| matches!(e, StateHealthEvent::TaskDisabled { name } if name == "disabled_probe")));

    sup.supervise(TaskSpec::new("req_probe", TaskClass::Required), async {
        panic!("required");
    });
    assert!(
        wait_until(|| host.exits() == vec![70], Duration::from_secs(1)).await,
        "a Required panic did not exit 70 within 1 s: {:?}",
        host.exits()
    );
    assert_eq!(
        sup.liveness(),
        Liveness::Stopped {
            reason: "task_ended:req_probe".into()
        }
    );
    // Cascade: a later Required end after the stop does not exit again.
    sup.supervise(TaskSpec::new("req_probe_2", TaskClass::Required), async {
        TaskEnd::Returned
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(host.exits(), vec![70]);

    // An Ok return of a Required task exits 70 too.
    let host2 = Arc::new(RecordingHost::default());
    let sup2 = Supervisor::with_gate(host2.clone(), ExitGate::detached());
    sup2.supervise(TaskSpec::new("req_return", TaskClass::Required), async {
        TaskEnd::Returned
    });
    assert!(wait_until(|| host2.exits() == vec![70], Duration::from_secs(1)).await);
}

/// T-CS3-18 (in-process half): the SSE auth Terminal stop sets
/// `Stopped{sse_auth_failed}`, raises `sse_auth_failed{sub}` once, sends ONE
/// dedicated notice naming what stopped, why and the one action, and never
/// exits; repeated and cascade ends send no second notice.
/// Mutations: classify the 401 return as an unexpected end; emit the notice
/// on each end; omit the notice.
#[tokio::test]
async fn t_cs3_18_sse_auth_terminal_one_notice() {
    let host = Arc::new(RecordingHost::default());
    let sup = Supervisor::with_gate(host.clone(), ExitGate::detached());
    let spec = TaskSpec::new("sse_consumer", TaskClass::Required);
    sup.task_ended(&spec, Ok(crate::boot::sse_auth_terminal("sub-cs3-18", 401)));
    sup.task_ended(&spec, Ok(crate::boot::sse_auth_terminal("sub-cs3-18", 401)));
    sup.task_ended(
        &TaskSpec::new("watcher:0", TaskClass::Required),
        Ok(TaskEnd::Returned),
    );
    assert!(host.exits().is_empty(), "the SSE auth stop exited");
    assert_eq!(
        sup.liveness(),
        Liveness::Stopped {
            reason: "sse_auth_failed".into()
        }
    );
    let notices = host.notices.lock().unwrap().clone();
    assert_eq!(notices.len(), 1, "{notices:?}");
    let (title, body) = &notices[0];
    assert!(title.contains("token rejected"), "{title}");
    assert!(
        body.contains("pull and push for subscriber sub-cs3-18"),
        "{body}"
    );
    assert!(body.contains("HTTP 401"), "{body}");
    assert!(body.contains("re-pair this device"), "{body}");
    let n = crate::sync_health::state_events()
        .iter()
        .filter(|e| matches!(e, StateHealthEvent::SseAuthFailed { sub, .. } if sub == "sub-cs3-18"))
        .count();
    assert_eq!(n, 1);
}

// ---------------------------------------------------------------------------
// T-CS3-17 storm fence (Linux: needs real inotify events)
// ---------------------------------------------------------------------------

#[cfg(target_os = "linux")]
async fn storm_fence(recovery_disabled: bool) -> (Arc<RecordingHost>, Arc<Supervisor>) {
    let fx = one_root("http://127.0.0.1:1");
    let owner = fx.handles.journal("sub-y").unwrap();
    owner.inner().set_force_capacity(true);
    let health = crate::sync_health::SyncHealth::new();
    let root = fx.vaults.join("Mainframe");
    let burst = Arc::new(Mutex::new(crate::redflag::DeleteBurstDetector::new(
        20,
        Duration::from_secs(30),
    )));
    let w = crate::file_watcher::FileWatcher::new(
        root.clone(),
        owner.clone(),
        burst,
        crate::file_watcher::FileWatcherConfig {
            allowed_extensions: vec!["md".into()],
            scope_roots: vec![],
            scope_excludes: vec![],
            debounce_ms: 50,
            managed_payload_excludes: false,
        },
        "sub-y",
    )
    .unwrap()
    .with_sync_health(health.clone())
    .with_storm_threshold(Some(5));
    let mut handle = w.start().unwrap();
    let task = handle.take_task().unwrap();
    crate::boot::supervise_watcher(&fx.sup, &fx.handles, 0, &root, recovery_disabled, task);
    crate::boot::spawn_stall_watchdog(
        &fx.sup,
        health,
        owner,
        "sub-y".into(),
        Duration::from_millis(50),
        Duration::from_secs(3600),
        recovery_disabled,
    );
    tokio::time::sleep(Duration::from_millis(200)).await;
    for i in 0..8 {
        std::fs::write(root.join(format!("storm{i}.md")), format!("{i}\n")).unwrap();
    }
    let (host, sup) = (fx.host.clone(), fx.sup.clone());
    let fenced = wait_until(|| !sup.is_live(), Duration::from_secs(5)).await;
    assert!(fenced, "the storm fence did not trip");
    // Three more watchdog ticks.
    tokio::time::sleep(Duration::from_millis(200)).await;
    drop(handle);
    drop(fx);
    (host, sup)
}

/// T-CS3-17 (gate C4): a storm-fence break gives exactly ONE restart path:
/// one exit 70 from `supervise` and zero restarts from the stall watchdog
/// over the next 3 watchdog ticks.
/// Mutation: keep the watchdog's `WatcherFenced` restart (two paths).
#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn t_cs3_17_storm_fence_one_restart_path() {
    let (host, sup) = storm_fence(false).await;
    assert_eq!(host.exits(), vec![70], "one exit 70 from supervise");
    assert_eq!(
        host.restarts.load(Ordering::SeqCst),
        0,
        "the watchdog restarted too"
    );
    assert_eq!(
        sup.liveness(),
        Liveness::Stopped {
            reason: "watcher_fenced".into()
        }
    );
    assert!(crate::sync_health::state_events()
        .iter()
        .any(|e| matches!(e, StateHealthEvent::TaskEnded { cause, .. } if cause == "storm_fence")));
}

/// T-CS3-17 (gate E2): with the watchdog's recovery kill switch set, the
/// fence gives no exit, `live` = `Stopped{watcher_fenced}`, and exactly ONE
/// fence notice across the next 3 watchdog ticks and the cascade ends.
/// Mutation: the watchdog's `WatcherFenced` trigger notifies on each tick.
#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn t_cs3_17_storm_fence_kill_switch_one_notice() {
    let (host, sup) = storm_fence(true).await;
    assert!(host.exits().is_empty(), "the kill-switched fence exited");
    assert_eq!(host.restarts.load(Ordering::SeqCst), 0);
    assert_eq!(
        sup.liveness(),
        Liveness::Stopped {
            reason: "watcher_fenced".into()
        }
    );
    let notices = host.notices.lock().unwrap().clone();
    assert_eq!(notices.len(), 1, "exactly one notice: {notices:?}");
    assert_eq!(
        host.notices_titled("Vault Sync stopped detecting local edits"),
        1
    );
    assert!(notices[0]
        .1
        .contains("restart the daemon after the backlog clears"));
}

// ---------------------------------------------------------------------------
// T-CS3-21, T-CS3-22
// ---------------------------------------------------------------------------

/// T-CS3-21 (gate C10, run-time half): `wrap_scratch` of a Dir-backed
/// journal returns `Err`; of the scratch sink it succeeds. (The gate half and
/// T-CS2-22 cover the single caller and no temp file.)
#[test]
fn t_cs3_21_wrap_scratch_refuses_a_real_journal() {
    let d = TempDir::new().unwrap();
    let real = PushJournal::open(&StateDir::for_test(d.path())).unwrap();
    assert!(JournalOwner::wrap_scratch(real).is_err());
    assert!(JournalOwner::wrap_scratch(PushJournal::scratch()).is_ok());
}

/// T-CS3-22 (G-7, in-process half; the T-CS3-19 order-B setup): the 70
/// path claims `exit_once`, custody is lost INSIDE its pre-exit flush, so
/// the flush issues zero write syscalls, nothing lands in the replaced dir,
/// the held file stays byte-identical, and the leaver exits 74, not 70.
/// (The custody-intact half, an entry surviving in `shadow_hashes.json`
/// after exit 70, is `cs3_harness::t_cs3_22_pre_exit_flush_persists`.)
/// Mutation: write the flush through a path that bypasses `guard_mutation`.
#[test]
fn t_cs3_22_latched_flush_writes_nothing() {
    let base = TempDir::new().unwrap();
    let d = base.path().join("sync-dir");
    std::fs::create_dir_all(&d).unwrap();
    let sd = StateDir::for_test(&d);
    let shadow = ShadowStore::load(&sd);
    let base_seq = crate::base_seq_store::BaseSeqStore::load(&sd);
    let rr = crate::read_receipt::ReadReceiptStore::load(&sd);
    shadow.record("a.md", &"a".repeat(64));
    let _ = shadow.flush().unwrap();
    let file = StateFile::Shadow.name();
    let before = std::fs::read(d.join(file)).unwrap();
    shadow.record("b.md", &"b".repeat(64));

    let host = Arc::new(RecordingHost::default());
    let sup = Supervisor::with_gate(host.clone(), ExitGate::for_dir(&sd));
    crate::boot::register_store_flush(&sup, &shadow, &base_seq, &rr);
    let (d2, sd2) = (d.clone(), sd.clone());
    sup.set_in_pre_exit_flush(Some(Box::new(move || {
        std::fs::rename(&d2, d2.with_extension("moved")).unwrap();
        std::fs::create_dir_all(&d2).unwrap();
        sd2.seams.recorder.lock().unwrap().clear();
    })));
    sup.exit_70("t_cs3_22");
    assert!(
        sd.test_latch().is_set(),
        "custody loss did not latch mutations"
    );
    let writes: Vec<String> = sd
        .seams
        .recorder
        .lock()
        .unwrap()
        .iter()
        .filter(|r| {
            [
                "write:",
                "renameat:",
                "unlinkat:",
                "sync_data:",
                "open_w:",
                "openat:",
            ]
            .iter()
            .any(|p| r.starts_with(p))
        })
        .cloned()
        .collect();
    assert!(
        writes.is_empty(),
        "flush issued syscalls after the loss: {writes:?}"
    );
    assert!(
        !d.join(file).exists(),
        "a flush write landed in the replaced directory"
    );
    assert_eq!(
        std::fs::read(d.with_extension("moved").join(file)).unwrap(),
        before,
        "the held shadow file changed after the loss"
    );
    assert_eq!(
        host.exits(),
        vec![74],
        "custody lost: the 70 path must exit 74"
    );
}
