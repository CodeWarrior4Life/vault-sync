//! Unit CS-3 (TKT-b6d8c4ba) daemon-subprocess harness (the CS-2 T-CS2-3
//! pattern): the parent test writes a paired config, token files and a vault
//! into a temp tree, starts a mockito server, and re-runs this test binary as
//! a CHILD (`cs3_child_daemon`, `#[ignore]`d) with `CS3_CHILD=<scenario>`.
//! The child runs the real `.setup()` path minus Tauri: `BootConfig`,
//! `plan_startup`, `Supervisor::process`, `boot::start_sync` (the CS-2
//! bootstrap, the supervised custody tick, `config_watch` and the boot task
//! with every pipeline), then acts out the scenario and prints `KEY=value`
//! lines. Real exits are real (`STATE_IO_REAL_EXIT=1`; the child host exits
//! the process; a restart prints `RESTART_CALLED` and exits 99).
#![allow(clippy::disallowed_methods)]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
#[cfg(unix)]
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::ownership::BootConfig;
use crate::push_journal::{new_event_id, PushAction, PushBase, PushEvent, CURRENT_SCHEMA};
use crate::supervise::{Host, Liveness, Supervisor};
use crate::sync_handles::{BorrowRefused, SyncHandles, SyncHandlesSlot};
use crate::sync_health::StateHealthEvent;

// ===========================================================================
// Child side
// ===========================================================================

struct ChildHost;

impl Host for ChildHost {
    fn notify(&self, title: &str, body: &str) {
        println!("NOTICE|{title}|{body}");
    }
    fn emit(&self, event: &str, _payload: serde_json::Value) {
        println!("EMIT|{event}");
    }
    fn exit(&self, code: i32) {
        use std::io::Write;
        println!("HOST_EXIT={code}");
        let _ = std::io::stdout().flush();
        std::process::exit(code);
    }
    fn restart(&self) {
        use std::io::Write;
        println!("RESTART_CALLED");
        let _ = std::io::stdout().flush();
        std::process::exit(99);
    }
}

static VR_DONE: AtomicBool = AtomicBool::new(false);

fn env_ms(k: &str) -> Option<Duration> {
    std::env::var(k)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_millis)
}

fn child_knobs() -> crate::boot::BootKnobs {
    let mut k = crate::boot::BootKnobs::default();
    if let Some(d) = env_ms("CS3_WATCHDOG_MS") {
        k.watchdog_tick = d;
    }
    if let Some(d) = env_ms("CS3_CONFIG_WATCH_MS") {
        k.config_watch_period = d;
    }
    if std::env::var("CS3_STALL_ZERO").is_ok() {
        k.stall_threshold = Some(Duration::ZERO);
    }
    k.push_loop_interval_ms = Some(
        std::env::var("CS3_PUSH_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(50),
    );
    k
}

struct Child {
    sup: Arc<Supervisor>,
    slot: SyncHandlesSlot,
    snapshot: Arc<crate::ownership::OwnershipSnapshot>,
    tray: crate::tray_state::SharedTrayState,
    ws: PathBuf,
}

fn child_boot() -> Child {
    let host: Arc<dyn Host> = Arc::new(ChildHost);
    let sup = Supervisor::process(host);
    let cfg_path = crate::config::default_config_path();
    let boot_cfg = BootConfig::load_at_startup(&cfg_path);
    let plan = crate::boot::plan_startup(&boot_cfg);
    assert!(plan.decision.start_sync, "child config is not paired");
    let snapshot = plan.snapshot.expect("snapshot");
    let slot = SyncHandlesSlot::default();
    let ws = PathBuf::from(std::env::var("CS3_WS").unwrap());
    crate::boot::start_sync(crate::boot::BootEnv {
        sup: sup.clone(),
        snapshot: snapshot.clone(),
        slot: slot.clone(),
        tray: plan.tray.clone(),
        workspace: ws.clone(),
        knobs: child_knobs(),
    });
    Child {
        sup,
        slot,
        snapshot,
        tray: plan.tray,
        ws,
    }
}

async fn wait_for(mut f: impl FnMut() -> bool, max: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < max {
        if f() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    f()
}

/// Wait until handles are published and every root in `keys` borrows.
async fn wait_ready(c: &Child, keys: &[usize]) -> Arc<SyncHandles> {
    let ok = wait_for(
        || {
            c.slot
                .get()
                .is_some_and(|h| keys.iter().all(|k| h.borrow(*k).is_ok()))
        },
        Duration::from_secs(30),
    )
    .await;
    let h = c.slot.get();
    if !ok {
        let why: Vec<String> = match &h {
            None => vec!["not published".into()],
            Some(h) => keys
                .iter()
                .map(|k| format!("{:?}", h.borrow(*k).err()))
                .collect(),
        };
        println!("NOT_READY={why:?}");
        std::process::exit(3);
    }
    println!("READY");
    h.unwrap()
}

fn state_dir_of(ws: &Path, sub: &str) -> PathBuf {
    ws.join(".lattice-runtime").join(sub).join("sync-state")
}

fn file_hashes(dir: &Path) -> BTreeMap<String, String> {
    let mut m = BTreeMap::new();
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            if e.path().is_file() {
                use sha2::Digest;
                let b = std::fs::read(e.path()).unwrap_or_default();
                m.insert(
                    e.file_name().to_string_lossy().into_owned(),
                    hex::encode(sha2::Sha256::digest(&b)),
                );
            }
        }
    }
    m
}

/// Move the held sync-state dir of `sub` away (custody lost) and record the
/// moved dir's file hashes in `<ws>/moved-hashes.json`.
fn lose_custody(ws: &Path, sub: &str) {
    let d = state_dir_of(ws, sub);
    let moved = d.with_file_name("sync-state.moved");
    std::fs::rename(&d, &moved).unwrap();
    let hashes = file_hashes(&moved);
    std::fs::write(
        ws.join("moved-hashes.json"),
        serde_json::to_string(&hashes).unwrap(),
    )
    .unwrap();
    println!("CUSTODY_LOST");
}

fn print_events(filter: impl Fn(&StateHealthEvent) -> bool) {
    for e in crate::sync_health::state_events() {
        if filter(&e) {
            println!("EVENT={e:?}");
        }
    }
}

fn vr_spawn(slot: SyncHandlesSlot) {
    tauri::async_runtime::spawn(async move {
        let r = crate::commands::verify_repair_with(Some(&slot)).await;
        match r {
            Ok(rep) => println!(
                "VR_RESULT=ok modify={} add={}",
                rep.modify_count, rep.add_count
            ),
            Err(e) => println!("VR_RESULT=err {e}"),
        }
        VR_DONE.store(true, Ordering::SeqCst);
    });
}

async fn child_main(scenario: String) {
    let c = child_boot();
    match scenario.as_str() {
        // T-CS3-1: one config load across setup, bootstrap and the boot task.
        "boot_once" => {
            let h = wait_ready(&c, &[0]).await;
            println!(
                "CONFIG_LOADS={}",
                crate::config::CONFIG_LOADS.load(Ordering::SeqCst)
            );
            println!("TRAY_SHA8={}", c.tray.read().unwrap().config_sha8);
            println!("HANDLES_SHA8={}", h.snapshot().sha8());
            println!("SNAPSHOT_SHA8={}", c.snapshot.sha8());
            let s_ok = c
                .snapshot
                .subs()
                .iter()
                .all(|s| crate::state_io::registry().get(s).is_ok());
            println!("S_BOOTSTRAPPED={s_ok}");
            println!("ROOTS_STARTED={}", h.roots().len());
        }
        // T-CS3-2: the config changes between the bootstrap and the boot
        // task (the seam is armed before start_sync in `cs3_child_daemon`).
        "toctou" => {
            let h = wait_ready(&c, &[0]).await;
            println!("ROOTS={}", h.roots().len());
            let sha8 = c.snapshot.sha8();
            let raised = wait_for(
                || {
                    crate::sync_health::state_events().iter().any(|e| {
                        matches!(e, StateHealthEvent::ConfigPendingRestart { snapshot_sha8, .. } if *snapshot_sha8 == sha8)
                    })
                },
                Duration::from_secs(3),
            )
            .await;
            println!("PENDING_RAISED={raised}");
            println!(
                "Z_BOOTSTRAPPED={}",
                crate::state_io::registry().get("sub-z").is_ok()
            );
        }
        // T-CS3-4 (live half): after a config change an edit still drains
        // and pushes; no task restarts; no exit; once per distinct hash.
        "config_change" => {
            let h = wait_ready(&c, &[0]).await;
            let cfg = c.snapshot.cfg_path().to_path_buf();
            let original = std::fs::read(&cfg).unwrap();
            let mut changed = original.clone();
            changed.extend_from_slice(b"# operator edit\n");
            std::fs::write(&cfg, &changed).unwrap();
            tokio::time::sleep(Duration::from_millis(2_000)).await;
            let root = h.snapshot().roots()[0].path.clone();
            std::fs::write(
                root.join("after-change.md"),
                b"edited after the config change\n",
            )
            .unwrap();
            let sent = wait_for(
                || c.tray.read().unwrap().uploads_sent >= 1,
                Duration::from_secs(15),
            )
            .await;
            println!("PUSHED={sent}");
            std::fs::write(&cfg, &original).unwrap();
            tokio::time::sleep(Duration::from_millis(1_500)).await;
            let sha8 = c.snapshot.sha8();
            let mine = |e: &StateHealthEvent| match e {
                StateHealthEvent::ConfigPendingRestart { snapshot_sha8, .. }
                | StateHealthEvent::ConfigPendingRestartCleared { snapshot_sha8 } => {
                    *snapshot_sha8 == sha8
                }
                _ => false,
            };
            let ev: Vec<StateHealthEvent> = crate::sync_health::state_events()
                .into_iter()
                .filter(|e| mine(e))
                .collect();
            println!(
                "PENDING_COUNT={}",
                ev.iter()
                    .filter(|e| matches!(e, StateHealthEvent::ConfigPendingRestart { .. }))
                    .count()
            );
            println!(
                "CLEARED_COUNT={}",
                ev.iter()
                    .filter(|e| matches!(e, StateHealthEvent::ConfigPendingRestartCleared { .. }))
                    .count()
            );
            println!("LIVE={}", c.sup.is_live());
        }
        // T-CS3-7: the construction census in a split config.
        "split_census" => {
            let _h = wait_ready(&c, &[0]).await;
            let census = |tag: &str| {
                for sub in ["sub-x", "sub-y"] {
                    let sd = crate::state_io::registry().get(sub).unwrap();
                    println!(
                        "{tag}_{sub}_CONSTRUCTS={}",
                        sd.seams.journal_constructs.load(Ordering::SeqCst)
                    );
                    println!(
                        "{tag}_{sub}_CLAIMED={}",
                        sd.is_claimed(crate::state_io::StateFile::PushJournal)
                    );
                }
            };
            census("BOOT");
            let r = crate::commands::verify_repair_with(Some(&c.slot)).await;
            println!("VR_RESULT={}", if r.is_ok() { "ok" } else { "err" });
            census("CLICK");
        }
        // T-CS3-8: the anti-strip compensating push lands in X's journal and
        // X's drain loop pushes it (the SSE event is served by the mock).
        "split_compensating" => {
            let h = wait_ready(&c, &[0]).await;
            let pushed = wait_for(
                || std::fs::read_to_string(c.ws.join("push-log.txt")).is_ok_and(|s| !s.is_empty()),
                Duration::from_secs(15),
            )
            .await;
            tokio::time::sleep(Duration::from_millis(500)).await;
            println!("PUSH_SEEN={pushed}");
            println!(
                "Y_JOURNAL_EXISTS={}",
                state_dir_of(&c.ws, "sub-y")
                    .join(crate::state_io::StateFile::PushJournal.name())
                    .exists()
            );
            println!("X_LEN={}", h.journal("sub-x").unwrap().len());
        }
        // T-CS3-9: the stranded journal of the rootless top-level sub.
        "stranded" => {
            let _h = wait_ready(&c, &[0]).await;
            tokio::time::sleep(Duration::from_millis(500)).await;
            print_events(|e| matches!(e, StateHealthEvent::StrandedJournal { .. }));
            let sd = crate::state_io::registry().get("sub-y").unwrap();
            println!(
                "Y_CLAIMED={}",
                sd.is_claimed(crate::state_io::StateFile::PushJournal)
            );
            for r in sd.seams.recorder.lock().unwrap().iter() {
                println!("Y_REC={r}");
            }
        }
        // T-CS3-14: X has no token, so X's pipeline fails at token load.
        "split_no_x_token" => {
            let ok = wait_for(
                || {
                    c.slot.get().is_some_and(|h| {
                        h.root(0).and_then(|r| r.failed())
                            == Some(crate::sync_handles::PipelineStage::Token)
                    })
                },
                Duration::from_secs(20),
            )
            .await;
            println!("X_FAILED_TOKEN={ok}");
            let before = crate::token_store::LOADS.lock().unwrap().len();
            let r = crate::commands::verify_repair_preflight(Some(&c.slot));
            println!(
                "PREFLIGHT={:?}",
                r.as_ref().err().unwrap_or(&BorrowRefused::NotLive {
                    reason: "borrowed".into()
                })
            );
            let e = crate::commands::verify_repair_with(Some(&c.slot)).await;
            println!("VR_RESULT={:?}", e.err());
            let loads: Vec<String> = crate::token_store::LOADS.lock().unwrap()[before..].to_vec();
            println!("LOADS_DURING_CLICK={loads:?}");
        }
        // T-CS3-16: V&R parked between the manifest walk and the reconcile
        // request; then the named Required task panics.
        "vr_panic" => {
            let h = wait_ready(&c, &[0]).await;
            let owner = h.journal(h.snapshot().top_sub()).unwrap();
            let o2 = owner.clone();
            c.sup.set_before_recheck(Some(Box::new(move |who| {
                let start = Instant::now();
                while !VR_DONE.load(Ordering::SeqCst) && start.elapsed() < Duration::from_secs(5) {
                    std::thread::sleep(Duration::from_millis(10));
                }
                println!("LEAVER={who}");
                println!("JOURNAL_LEN={}", o2.len());
            })));
            crate::faults::arm("vr.park_until_stopped");
            vr_spawn(c.slot.clone());
            let parked = wait_for(
                || crate::verify_repair::VR_PARKED.load(Ordering::SeqCst),
                Duration::from_secs(15),
            )
            .await;
            println!("VR_PARKED={parked}");
            let site = std::env::var("CS3_PANIC_SITE").unwrap();
            crate::faults::arm(&format!("{site}.panic"));
            println!("ARMED={site}");
            tokio::time::sleep(Duration::from_secs(10)).await;
            println!("NO_EXIT");
        }
        // T-CS3-16 (RootDegrading): the backstop panicked at start.
        "backstop_panic" => {
            let ok = wait_for(
                || {
                    c.slot.get().is_some_and(|h| {
                        matches!(h.borrow(0), Err(BorrowRefused::RootNotAlive { .. }))
                    })
                },
                Duration::from_secs(20),
            )
            .await;
            println!("ROOT_NOT_ALIVE={ok}");
            let e = crate::commands::verify_repair_with(Some(&c.slot)).await;
            println!("VR_RESULT={:?}", e.err());
            tokio::time::sleep(Duration::from_millis(500)).await;
            println!("LIVE={}", c.sup.is_live());
        }
        // T-CS3-17: one supervised end, armed after readiness.
        "supervise" => {
            let _h = wait_ready(&c, &[0]).await;
            for f in std::env::var("CS3_ARM").unwrap_or_default().split(',') {
                if !f.is_empty() {
                    crate::faults::arm(f);
                }
            }
            println!(
                "ARMED_MS={}",
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis()
            );
            tokio::time::sleep(Duration::from_millis(1_500)).await;
            if let Some(h) = c.slot.get() {
                println!("ROOT0_ALIVE={}", h.root(0).is_some_and(|r| r.is_alive()));
                println!("BORROW0={:?}", h.borrow(0).err());
            }
            println!("LIVE={}", c.sup.is_live());
            println!("NO_EXIT");
        }
        // T-CS3-18: the mock SSE answers 401.
        "sse_401" => {
            let stopped = wait_for(
                || {
                    c.slot.get().is_some_and(|h| {
                        h.liveness()
                            == Liveness::Stopped {
                                reason: "sse_auth_failed".into(),
                            }
                    })
                },
                Duration::from_secs(20),
            )
            .await;
            println!("STOPPED_SSE_AUTH={stopped}");
            let e = crate::commands::verify_repair_with(Some(&c.slot)).await;
            println!("VR_RESULT={:?}", e.err());
            tokio::time::sleep(Duration::from_secs(3)).await;
            print_events(|e| matches!(e, StateHealthEvent::SseAuthFailed { .. }));
            println!("NO_EXIT");
        }
        // T-CS3-19: exit precedence, orders A, B and C.
        "exit_order" => {
            let h = wait_ready(&c, &[0]).await;
            let sub = h.snapshot().top_sub().to_string();
            let ws = c.ws.clone();
            match std::env::var("CS3_ORDER").unwrap().as_str() {
                "A" => {
                    crate::state_io::test_hooks::set_before_push_gate(Some(Box::new(move || {
                        lose_custody(&ws, &sub);
                    })));
                    h.journal(h.snapshot().top_sub())
                        .unwrap()
                        .append(order_event("order-a.md"))
                        .unwrap();
                }
                "B" => {
                    c.sup.set_in_pre_exit_flush(Some(Box::new(move || {
                        lose_custody(&ws, &sub);
                    })));
                    crate::faults::arm("drain_loop.panic");
                }
                "C" => {
                    c.sup.set_before_recheck(Some(Box::new(move |who| {
                        if who == "stall_watchdog" {
                            lose_custody(&ws, &sub);
                        }
                    })));
                    crate::faults::arm("stall_watchdog.pending");
                }
                o => panic!("order {o}"),
            }
            tokio::time::sleep(Duration::from_secs(10)).await;
            println!("NO_EXIT");
        }
        // T-CS3-20: a watcher start failure on root 1; an R7 park on root 0.
        "partial" => {
            let ok = wait_for(
                || {
                    c.slot.get().is_some_and(|h| {
                        let last = h.roots().len() - 1;
                        h.sse_connected_once()
                            && h.root(last).is_some_and(|r| {
                                r.failed().is_some()
                                    && (r.failed()
                                        == Some(crate::sync_handles::PipelineStage::R7Park)
                                        || r.ready_bits() & crate::sync_handles::FIRST_DRAIN_DONE
                                            != 0)
                            })
                    })
                },
                Duration::from_secs(20),
            )
            .await;
            let h = c.slot.get().unwrap();
            for k in 0..h.roots().len() {
                let r = h.root(k).unwrap();
                println!("ROOT{k}_FAILED={:?}", r.failed());
                println!("ROOT{k}_MISSING={:?}", r.missing());
                println!(
                    "ROOT{k}_DRAINED={}",
                    r.ready_bits() & crate::sync_handles::FIRST_DRAIN_DONE != 0
                );
            }
            println!("WAITED={ok}");
            let e = crate::commands::verify_repair_with(Some(&c.slot)).await;
            println!("VR_RESULT={:?}", e.err());
            print_events(|e| matches!(e, StateHealthEvent::PipelineFailed { .. }));
        }
        // T-CS3-22: a dirty shadow entry survives a drain-loop panic.
        "flush_persists" => {
            let h = wait_ready(&c, &[0]).await;
            h.shadow().record("cs3-22-dirty.md", &"d".repeat(64));
            crate::faults::arm("drain_loop.panic");
            tokio::time::sleep(Duration::from_secs(10)).await;
            println!("NO_EXIT");
        }
        o => panic!("unknown CS-3 child scenario {o}"),
    }
}

fn order_event(path: &str) -> PushEvent {
    PushEvent {
        schema_version: CURRENT_SCHEMA,
        id: new_event_id(),
        path: path.to_string(),
        action: PushAction::Modify,
        base_hash: PushBase::NoRow,
        content_sha: "1".repeat(64),
        content_bytes: Some(b"order\n".to_vec()),
        queued_at: chrono::Utc::now(),
        device_id: "dev-cs3".into(),
    }
}

/// The CHILD: not a test on its own. Without `CS3_CHILD` it returns at once.
#[test]
#[ignore = "CS-3 subprocess helper; spawned by the cs3_harness tests"]
fn cs3_child_daemon() {
    let Ok(scenario) = std::env::var("CS3_CHILD") else {
        return;
    };
    // libtest prints "test ... " without a newline before the first output.
    println!("\nCHILD_START");
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .with_env_filter(tracing_subscriber::EnvFilter::new("info"))
        .try_init();
    crate::faults::arm_from_env();
    if scenario == "toctou" {
        // T-CS3-2: rewrite config.toml after the bootstrap and before the
        // boot task, adding a root with a NEW sub Z.
        crate::boot::test_hooks::set_after_bootstrap(Some(Box::new(|| {
            let p = crate::config::default_config_path();
            let mut s = std::fs::read_to_string(&p).unwrap();
            let vaults = PathBuf::from(std::env::var("CS3_VAULTS").unwrap());
            std::fs::create_dir_all(vaults.join("Zed")).unwrap();
            s.push_str(&format!(
                "\n[[sync_roots]]\npath = '{}'\nroute = \"\"\nsubscriber_id = \"sub-z\"\n",
                vaults.join("Zed").display()
            ));
            std::fs::write(&p, s).unwrap();
            println!("CONFIG_REWRITTEN");
        })));
    }
    tauri::async_runtime::block_on(child_main(scenario));
}

// ===========================================================================
// Parent side
// ===========================================================================

#[cfg(unix)]
mod parent {
    use super::*;
    use std::io::{BufRead, BufReader};
    use std::process::{Command, Stdio};

    pub(super) struct Env {
        pub _base: tempfile::TempDir,
        pub cfg_dir: PathBuf,
        pub ws: PathBuf,
        pub vaults: PathBuf,
        pub home: PathBuf,
    }

    pub(super) struct Root<'a> {
        pub folder: &'a str,
        pub sub: &'a str,
    }

    impl Env {
        pub fn new(url: &str, top: &str, roots: &[Root], tokens: &[&str]) -> Env {
            let base = tempfile::TempDir::new().unwrap();
            let cfg_dir = base.path().join("cfg");
            let ws = base.path().join("ws");
            let vaults = base.path().join("vaults");
            let home = base.path().join("home");
            for d in [&cfg_dir, &ws, &vaults, &home] {
                std::fs::create_dir_all(d).unwrap();
            }
            let mut s = format!(
                "nexus_url = \"{url}\"\nsubscriber_id = \"{top}\"\nvaults_root = '{}'\ndaemon_version = \"0.0.0-cs3\"\ndaemon_platform = \"test\"\n",
                vaults.display()
            );
            for r in roots {
                std::fs::create_dir_all(vaults.join(r.folder)).unwrap();
                s.push_str(&format!(
                    "\n[[sync_roots]]\npath = '{}'\nroute = \"\"\nsubscriber_id = \"{}\"\n",
                    vaults.join(r.folder).display(),
                    r.sub
                ));
            }
            std::fs::write(
                cfg_dir.join(crate::config::default_config_path().file_name().unwrap()),
                s,
            )
            .unwrap();
            for t in tokens {
                std::fs::write(cfg_dir.join(format!("token-{t}.bin")), format!("tok-{t}")).unwrap();
            }
            Env {
                _base: base,
                cfg_dir,
                ws,
                vaults,
                home,
            }
        }
        pub fn state_dir(&self, sub: &str) -> PathBuf {
            state_dir_of(&self.ws, sub)
        }
        pub fn config_sha8(&self) -> String {
            use sha2::Digest;
            hex::encode(
                &sha2::Sha256::digest(
                    std::fs::read(
                        self.cfg_dir
                            .join(crate::config::default_config_path().file_name().unwrap()),
                    )
                    .unwrap(),
                )[..4],
            )
        }
    }

    pub(super) struct Out {
        pub code: Option<i32>,
        pub stdout: String,
        pub stderr: String,
        /// Unix ms when the child exited.
        pub exited_ms: u128,
    }

    impl Out {
        pub fn kv(&self, key: &str) -> Option<String> {
            let p = format!("{key}=");
            self.stdout
                .lines()
                .find_map(|l| l.strip_prefix(&p).map(|s| s.to_string()))
        }
        pub fn has(&self, line: &str) -> bool {
            self.stdout.lines().any(|l| l == line)
        }
        pub fn dump(&self) -> String {
            format!(
                "exit={:?}\n--- stdout ---\n{}\n--- stderr (tail) ---\n{}",
                self.code,
                self.stdout,
                self.stderr
                    .lines()
                    .rev()
                    .take(60)
                    .collect::<Vec<_>>()
                    .into_iter()
                    .rev()
                    .collect::<Vec<_>>()
                    .join("\n")
            )
        }
    }

    pub(super) fn run_child(env: &Env, scenario: &str, url: &str, extra: &[(&str, &str)]) -> Out {
        let mut cmd = Command::new(std::env::current_exe().unwrap());
        cmd.args([
            "--exact",
            "cs3_harness::cs3_child_daemon",
            "--ignored",
            "--test-threads=1",
            "--nocapture",
        ])
        .env("CS3_CHILD", scenario)
        .env("CS3_URL", url)
        .env("CS3_WS", &env.ws)
        .env("CS3_VAULTS", &env.vaults)
        .env("NEXUS_SYNC_CONFIG_DIR", &env.cfg_dir)
        .env("VAULT_SYNC_TOKEN_BACKEND", "file")
        .env("HOME", &env.home)
        .env("XDG_CONFIG_HOME", env.home.join(".config"))
        .env("XDG_DATA_HOME", env.home.join(".local/share"))
        .env("STATE_IO_REAL_EXIT", "1")
        .env("VAULT_SYNC_DISABLE_BACKFILL", "1")
        .env("RUST_BACKTRACE", "0")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
        if !extra
            .iter()
            .any(|(k, _)| *k == "VAULT_SYNC_RECON_INTERVAL_SECS")
        {
            cmd.env("VAULT_SYNC_DISABLE_RECON", "1");
        }
        for (k, v) in extra {
            cmd.env(k, v);
        }
        let mut child = cmd.spawn().unwrap();
        let so = child.stdout.take().unwrap();
        let se = child.stderr.take().unwrap();
        let t_out = std::thread::spawn(move || {
            let mut s = String::new();
            for l in BufReader::new(so).lines().map_while(Result::ok) {
                s.push_str(&l);
                s.push('\n');
            }
            s
        });
        let t_err = std::thread::spawn(move || {
            let mut s = String::new();
            for l in BufReader::new(se).lines().map_while(Result::ok) {
                s.push_str(&l);
                s.push('\n');
            }
            s
        });
        let deadline = Instant::now() + Duration::from_secs(90);
        let status = loop {
            if let Some(st) = child.try_wait().unwrap() {
                break Some(st);
            }
            if Instant::now() > deadline {
                let _ = child.kill();
                break child.wait().ok();
            }
            std::thread::sleep(Duration::from_millis(5));
        };
        let exited_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        Out {
            code: status.and_then(|s| s.code()),
            stdout: t_out.join().unwrap(),
            stderr: t_err.join().unwrap(),
            exited_ms,
        }
    }

    /// What the mock push endpoint saw.
    #[derive(Clone, Debug)]
    pub(super) struct PushSeen {
        pub bearer: String,
        pub path: String,
        pub base_hash: String,
    }

    pub(super) struct Mock {
        pub srv: mockito::ServerGuard,
        pub pushes: Arc<Mutex<Vec<PushSeen>>>,
        pub reconciles: Arc<Mutex<usize>>,
    }

    pub(super) struct MockOpts {
        pub sse_401: bool,
        /// Extra SSE frames sent once at the start of each stream.
        pub sse_prelude: String,
        /// Deltas the reconcile endpoint returns.
        pub reconcile_deltas: serde_json::Value,
        /// When set, the push endpoint is a CAS row: accept iff base ==
        /// row, else 409 with `expected_hash` = row.
        pub cas_row: Option<Arc<Mutex<String>>>,
        /// A file the push handler appends one line per push to.
        pub push_log: Option<PathBuf>,
    }

    impl Default for MockOpts {
        fn default() -> Self {
            Self {
                sse_401: false,
                sse_prelude: String::new(),
                reconcile_deltas: serde_json::json!([]),
                cas_row: None,
                push_log: None,
            }
        }
    }

    fn sha_hex(b: &[u8]) -> String {
        use sha2::Digest;
        hex::encode(sha2::Sha256::digest(b))
    }

    fn push_fields(req: &mockito::Request) -> (String, String, String, String) {
        let bearer = req
            .header("authorization")
            .first()
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_string();
        let body: serde_json::Value =
            serde_json::from_slice(req.body().map(|b| b.as_slice()).unwrap_or(b"{}"))
                .unwrap_or_default();
        let path = body["path"].as_str().unwrap_or_default().to_string();
        let base = body["base_hash"].as_str().unwrap_or_default().to_string();
        use base64::Engine;
        let content = base64::engine::general_purpose::STANDARD
            .decode(body["content"].as_str().unwrap_or_default())
            .unwrap_or_default();
        (bearer, path, base, sha_hex(&content))
    }

    pub(super) async fn mock(opts: MockOpts) -> Mock {
        let mut srv = mockito::Server::new_async().await;
        srv.mock("GET", "/api/sync/health")
            .with_status(200)
            .with_body(
                r#"{"subscriber_id":"x","scope_roots":[],"scope_excludes":[],"materializer_mode":"live","shadow_path":null}"#,
            )
            .create_async()
            .await;
        srv.mock("PATCH", "/api/sync/subscribers/me")
            .with_status(200)
            .with_body(
                r#"{"ok":true,"subscriber_id":"x","materializer_mode":"live","shadow_path":null,"scope_roots":[],"scope_excludes":[]}"#,
            )
            .create_async()
            .await;
        if opts.sse_401 {
            srv.mock("GET", "/api/sync/events")
                .with_status(401)
                .with_body("unauthorized")
                .create_async()
                .await;
        } else {
            let prelude = opts.sse_prelude.clone();
            let sent = Arc::new(AtomicBool::new(false));
            srv.mock("GET", "/api/sync/events")
                .with_status(200)
                .with_header("content-type", "text/event-stream")
                .with_chunked_body(move |w| {
                    if !sent.swap(true, Ordering::SeqCst) && !prelude.is_empty() {
                        w.write_all(prelude.as_bytes())?;
                    }
                    let start = Instant::now();
                    while start.elapsed() < Duration::from_secs(80) {
                        w.write_all(b": keep-alive\n\n")?;
                        std::thread::sleep(Duration::from_millis(50));
                    }
                    Ok(())
                })
                .create_async()
                .await;
        }
        let reconciles = Arc::new(Mutex::new(0usize));
        let rc = reconciles.clone();
        let deltas = opts.reconcile_deltas.clone();
        srv.mock("POST", "/api/sync/reconcile-batch")
            .with_status(200)
            .with_body_from_request(move |_| {
                *rc.lock().unwrap() += 1;
                serde_json::json!({ "deltas": deltas })
                    .to_string()
                    .into_bytes()
            })
            .create_async()
            .await;
        let pushes = Arc::new(Mutex::new(Vec::new()));
        let (p1, row1, log1) = (pushes.clone(), opts.cas_row.clone(), opts.push_log.clone());
        let row2 = opts.cas_row.clone();
        srv.mock("POST", "/api/sync/push")
            .with_status_code_from_request(move |req| {
                let (bearer, path, base, content_sha) = push_fields(req);
                p1.lock().unwrap().push(PushSeen {
                    bearer,
                    path: path.clone(),
                    base_hash: base.clone(),
                });
                if let Some(log) = &log1 {
                    use std::io::Write;
                    if let Ok(mut f) = std::fs::OpenOptions::new().create(true).append(true).open(log) {
                        let _ = writeln!(f, "{path}");
                    }
                }
                match &row1 {
                    Some(row) => {
                        let mut r = row.lock().unwrap();
                        if base == *r {
                            *r = content_sha;
                            200
                        } else {
                            409
                        }
                    }
                    None => 200,
                }
            })
            .with_body_from_request(move |req| {
                let (_, _, base, content_sha) = push_fields(req);
                if let Some(row) = &row2 {
                    let r = row.lock().unwrap();
                    if *r != content_sha && base != *r {
                        return serde_json::json!({ "expected_hash": *r }).to_string().into_bytes();
                    }
                }
                format!(
                    r#"{{"status":"accepted","seq":1,"content_hash":"{content_sha}","server_hash":"{content_sha}","server_seq":1,"merged_content":null,"message":null}}"#
                )
                .into_bytes()
            })
            .create_async()
            .await;
        Mock {
            srv,
            pushes,
            reconciles,
        }
    }

    pub(super) fn one_root_env(url: &str) -> Env {
        Env::new(
            url,
            "sub-a",
            &[Root {
                folder: "Mainframe",
                sub: "",
            }],
            &["sub-a"],
        )
    }

    pub(super) fn split_env(url: &str, tokens: &[&str]) -> Env {
        Env::new(
            url,
            "sub-y",
            &[Root {
                folder: "Mainframe",
                sub: "sub-x",
            }],
            tokens,
        )
    }

    pub(super) fn count(hay: &str, needle: &str) -> usize {
        hay.lines().filter(|l| l.contains(needle)).count()
    }

    /// The moved dir's files are byte-identical to the hashes recorded at the
    /// instant custody was lost (no state write after the loss).
    pub(super) fn assert_untouched_since_loss(env: &Env, sub: &str, out: &Out) {
        let rec: BTreeMap<String, String> = serde_json::from_str(
            &std::fs::read_to_string(env.ws.join("moved-hashes.json"))
                .unwrap_or_else(|_| panic!("custody was never lost\n{}", out.dump())),
        )
        .unwrap();
        let moved = env.state_dir(sub).with_file_name("sync-state.moved");
        assert_eq!(
            file_hashes(&moved),
            rec,
            "state files changed after the custody loss\n{}",
            out.dump()
        );
        assert!(
            !env.state_dir(sub).exists(),
            "something recreated the state dir after the loss\n{}",
            out.dump()
        );
    }
}

#[cfg(unix)]
use parent::*;

// ---------------------------------------------------------------------------
// The tests
// ---------------------------------------------------------------------------

/// T-CS3-1 (F3, one load): a full boot records exactly ONE config load
/// across `.setup()`, `bootstrap_state_dirs` and the boot task, and the tray
/// state, the bootstrap S and the started roots all carry the same snapshot
/// (`config_sha256` of the file's bytes).
/// Mutation: restore `Config::load_from` inside `bootstrap_state_dirs`.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn t_cs3_1_one_config_load() {
    let m = mock(MockOpts::default()).await;
    let env = one_root_env(&m.srv.url());
    let out = run_child(&env, "boot_once", &m.srv.url(), &[]);
    assert_eq!(out.code, Some(0), "{}", out.dump());
    assert_eq!(
        out.kv("CONFIG_LOADS").as_deref(),
        Some("1"),
        "{}",
        out.dump()
    );
    let sha8 = env.config_sha8();
    for k in ["TRAY_SHA8", "HANDLES_SHA8", "SNAPSHOT_SHA8"] {
        assert_eq!(
            out.kv(k).as_deref(),
            Some(sha8.as_str()),
            "{k}\n{}",
            out.dump()
        );
    }
    assert_eq!(out.kv("S_BOOTSTRAPPED").as_deref(), Some("true"));
    assert_eq!(out.kv("ROOTS_STARTED").as_deref(), Some("1"));
}

/// T-CS3-2 (F3 TOCTOU): `config.toml` gains a root with a new sub Z between
/// the bootstrap and the boot task: exactly the snapshot's roots start, no
/// pipeline or notice appears for Z, Z is not bootstrapped, and
/// `config_pending_restart` is raised within one `config_watch` tick.
/// Mutation: the boot task re-loads config.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn t_cs3_2_toctou_snapshot_only() {
    let m = mock(MockOpts::default()).await;
    let env = one_root_env(&m.srv.url());
    let out = run_child(
        &env,
        "toctou",
        &m.srv.url(),
        &[("CS3_CONFIG_WATCH_MS", "300")],
    );
    assert_eq!(out.code, Some(0), "{}", out.dump());
    assert!(out.has("CONFIG_REWRITTEN"), "{}", out.dump());
    assert_eq!(out.kv("ROOTS").as_deref(), Some("1"), "{}", out.dump());
    assert_eq!(
        out.kv("PENDING_RAISED").as_deref(),
        Some("true"),
        "{}",
        out.dump()
    );
    assert_eq!(out.kv("Z_BOOTSTRAPPED").as_deref(), Some("false"));
    assert_eq!(
        count(&out.stdout, "sub-z"),
        0,
        "a notice named Z\n{}",
        out.dump()
    );
    assert_eq!(
        count(&out.stderr, "subscriber_id=sub-z"),
        0,
        "a pipeline started for Z\n{}",
        out.dump()
    );
}

/// T-CS3-4 (live half): after a config change an edit under the original
/// root still drains and pushes, nothing restarts or exits, the event is
/// raised once for the changed bytes across ~10 ticks, and restoring the
/// original bytes raises `config_pending_restart_cleared`; nothing re-runs
/// the bootstrap.
/// Mutations: re-run bootstrap on change; raise the event every tick.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn t_cs3_4_config_change_keeps_syncing() {
    let m = mock(MockOpts::default()).await;
    let env = one_root_env(&m.srv.url());
    let out = run_child(
        &env,
        "config_change",
        &m.srv.url(),
        &[("CS3_CONFIG_WATCH_MS", "200")],
    );
    assert_eq!(out.code, Some(0), "{}", out.dump());
    assert_eq!(out.kv("PUSHED").as_deref(), Some("true"), "{}", out.dump());
    assert!(
        m.pushes
            .lock()
            .unwrap()
            .iter()
            .any(|p| p.path == "after-change.md"),
        "{}",
        out.dump()
    );
    assert_eq!(
        out.kv("PENDING_COUNT").as_deref(),
        Some("1"),
        "{}",
        out.dump()
    );
    assert_eq!(
        out.kv("CLEARED_COUNT").as_deref(),
        Some("1"),
        "{}",
        out.dump()
    );
    assert_eq!(out.kv("LIVE").as_deref(), Some("true"));
    assert_eq!(count(&out.stderr, "task_ended"), 0, "{}", out.dump());
    assert_eq!(
        count(&out.stderr, "registry already installed"),
        0,
        "{}",
        out.dump()
    );
}

/// T-CS3-7 (gate C2, run-time census): a split config (root X, top-level Y)
/// plus one V&R click constructs exactly one journal for X and none for Y,
/// Y's claim bit stays unset, and the click adds no construction.
/// Mutation: open the compensating journal on `cfg.subscriber_id`.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn t_cs3_7_split_census() {
    let m = mock(MockOpts::default()).await;
    let env = split_env(&m.srv.url(), &["sub-x", "sub-y"]);
    let out = run_child(&env, "split_census", &m.srv.url(), &[]);
    assert_eq!(out.code, Some(0), "{}", out.dump());
    for tag in ["BOOT", "CLICK"] {
        assert_eq!(
            out.kv(&format!("{tag}_sub-x_CONSTRUCTS")).as_deref(),
            Some("1"),
            "{}",
            out.dump()
        );
        assert_eq!(
            out.kv(&format!("{tag}_sub-y_CONSTRUCTS")).as_deref(),
            Some("0"),
            "{}",
            out.dump()
        );
        assert_eq!(
            out.kv(&format!("{tag}_sub-y_CLAIMED")).as_deref(),
            Some("false"),
            "{}",
            out.dump()
        );
    }
    assert_eq!(out.kv("VR_RESULT").as_deref(), Some("ok"), "{}", out.dump());
}

/// The anti-strip setup: local has frontmatter, the daemon shadow (top-level
/// Y dir) records the local hash, the server serves the stripped body.
#[cfg(unix)]
fn anti_strip_setup(env: &Env) -> (String, String) {
    let local = "---\ntitle: Keep Me\n---\nSHARED BODY\n";
    let server_body = "SHARED BODY\n";
    use sha2::Digest;
    let local_sha = hex::encode(sha2::Sha256::digest(local.as_bytes()));
    let server_sha = hex::encode(sha2::Sha256::digest(server_body.as_bytes()));
    let root = env.vaults.join("Mainframe");
    std::fs::create_dir_all(root.join("notes")).unwrap();
    std::fs::write(root.join("notes/strip.md"), local).unwrap();
    let ysd = env.state_dir("sub-y");
    std::fs::create_dir_all(&ysd).unwrap();
    std::fs::write(
        ysd.join(crate::state_io::StateFile::Shadow.name()),
        serde_json::json!({ "notes/strip.md": local_sha }).to_string(),
    )
    .unwrap();
    (local.to_string(), server_sha)
}

#[cfg(unix)]
async fn anti_strip_mock(server_sha: &str, row: Arc<Mutex<String>>, log: PathBuf) -> Mock {
    let prelude = "event: enrichment_complete\ndata: {\"op\":\"UPDATE\",\"path\":\"notes/strip.md\",\"phase\":\"enrichment_complete\",\"lsn\":7}\nid: lsn-7\n\n".to_string();
    let mut m = mock(MockOpts {
        sse_prelude: prelude,
        cas_row: Some(row),
        push_log: Some(log),
        ..Default::default()
    })
    .await;
    m.srv
        .mock("GET", "/api/sync/note")
        .match_query(mockito::Matcher::Any)
        .with_status(200)
        .with_body(
            serde_json::json!({
                "path": "notes/strip.md",
                "frontmatter": {},
                "body": "SHARED BODY\n",
                "sha256": server_sha,
                "modified": null,
                "file_mtime": null,
                "enriched_body": "SHARED BODY\n",
            })
            .to_string(),
        )
        .create_async()
        .await;
    m
}

/// T-CS3-8 (T-J2, G2, gate C2): split config, root X with its own token,
/// top-level Y. (Positive) an anti-strip pure-strip pull at the primary root
/// enqueues a compensating `Modify` with `KnownBase(server_hash)` that X's
/// drain loop pushes: the mock sees one push, authenticated as X, with that
/// base, and Y's journal file is never created. (Negative, R-CS3-7) X's row
/// is at another revision: the push gets 409, X's row and the local file are
/// byte-unchanged, and no push forges the row's hash as its base.
/// Mutations: the compensating journal on Y's dir (positive goes red); the
/// mock accepts a stale `KnownBase` (negative goes red).
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn t_cs3_8_compensating_push_lands_in_x() {
    // Positive.
    let probe = mock(MockOpts::default()).await;
    let env = split_env(&probe.srv.url(), &["sub-x", "sub-y"]);
    let (local, server_sha) = anti_strip_setup(&env);
    let row = Arc::new(Mutex::new(server_sha.clone()));
    let m = anti_strip_mock(&server_sha, row.clone(), env.ws.join("push-log.txt")).await;
    // The config was written with the probe URL; point it at this mock.
    let cfg = env
        .cfg_dir
        .join(crate::config::default_config_path().file_name().unwrap());
    let s = std::fs::read_to_string(&cfg)
        .unwrap()
        .replace(&probe.srv.url(), &m.srv.url());
    std::fs::write(&cfg, s).unwrap();
    let out = run_child(&env, "split_compensating", &m.srv.url(), &[]);
    assert_eq!(out.code, Some(0), "{}", out.dump());
    assert_eq!(
        out.kv("PUSH_SEEN").as_deref(),
        Some("true"),
        "{}",
        out.dump()
    );
    let pushes = m.pushes.lock().unwrap().clone();
    let strip: Vec<&PushSeen> = pushes
        .iter()
        .filter(|p| p.path == "notes/strip.md")
        .collect();
    assert_eq!(strip.len(), 1, "{pushes:?}\n{}", out.dump());
    assert_eq!(strip[0].bearer, "Bearer tok-sub-x", "{}", out.dump());
    assert_eq!(strip[0].base_hash, server_sha);
    assert_eq!(
        out.kv("Y_JOURNAL_EXISTS").as_deref(),
        Some("false"),
        "{}",
        out.dump()
    );
    assert_eq!(
        std::fs::read_to_string(env.vaults.join("Mainframe/notes/strip.md")).unwrap(),
        local
    );

    // Negative: X's row is at another revision.
    let probe2 = mock(MockOpts::default()).await;
    let env2 = split_env(&probe2.srv.url(), &["sub-x", "sub-y"]);
    let (local2, server_sha2) = anti_strip_setup(&env2);
    let other = "f".repeat(64);
    let row2 = Arc::new(Mutex::new(other.clone()));
    let m2 = anti_strip_mock(&server_sha2, row2.clone(), env2.ws.join("push-log.txt")).await;
    let cfg2 = env2
        .cfg_dir
        .join(crate::config::default_config_path().file_name().unwrap());
    let s2 = std::fs::read_to_string(&cfg2)
        .unwrap()
        .replace(&probe2.srv.url(), &m2.srv.url());
    std::fs::write(&cfg2, s2).unwrap();
    let out2 = run_child(&env2, "split_compensating", &m2.srv.url(), &[]);
    assert_eq!(out2.code, Some(0), "{}", out2.dump());
    let pushes2 = m2.pushes.lock().unwrap().clone();
    assert!(
        pushes2.iter().any(|p| p.path == "notes/strip.md"),
        "no push attempted\n{}",
        out2.dump()
    );
    assert_eq!(
        *row2.lock().unwrap(),
        other,
        "X's server row was overwritten\n{}",
        out2.dump()
    );
    assert!(
        !pushes2.iter().any(|p| p.base_hash == other),
        "a push forged the row hash as its base: {pushes2:?}"
    );
    assert_eq!(
        std::fs::read_to_string(env2.vaults.join("Mainframe/notes/strip.md")).unwrap(),
        local2,
        "the local file changed"
    );
}

/// T-CS3-9 (G-6, gate C2): Y's journal pre-seeded with one compensating
/// `Modify`, one replayed `Delete` with `PushBase::Unknown` and a torn final
/// line: a split boot raises `stranded_journal{sub: Y, create 0, modify 1,
/// delete 1, unparsed 0}` exactly once, Y's journal is byte-identical, the
/// recorder sees only read opens on it and no lock-file open, Y's claim bit
/// stays unset, and no push is sent for either line.
/// Mutations: open Y through `JournalOwner::open`; count unterminated lines;
/// migrate the lines into X's owner.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn t_cs3_9_stranded_diagnosed_not_touched() {
    let m = mock(MockOpts::default()).await;
    let env = split_env(&m.srv.url(), &["sub-x", "sub-y"]);
    let ysd = env.state_dir("sub-y");
    std::fs::create_dir_all(&ysd).unwrap();
    let modify = PushEvent {
        base_hash: PushBase::KnownBase("a".repeat(64)),
        ..order_event("stranded/mod.md")
    };
    let delete = PushEvent {
        action: PushAction::Delete,
        base_hash: PushBase::Unknown,
        content_sha: String::new(),
        content_bytes: None,
        ..order_event("stranded/del.md")
    };
    let mut bytes = Vec::new();
    for e in [&modify, &delete] {
        bytes.extend_from_slice(serde_json::to_string(e).unwrap().as_bytes());
        bytes.push(b'\n');
    }
    bytes.extend_from_slice(br#"{"schema_version":1,"id":"torn","path":"x"#);
    let jpath = ysd.join(crate::state_io::StateFile::PushJournal.name());
    std::fs::write(&jpath, &bytes).unwrap();
    let out = run_child(&env, "stranded", &m.srv.url(), &[]);
    assert_eq!(out.code, Some(0), "{}", out.dump());
    let events: Vec<&str> = out
        .stdout
        .lines()
        .filter(|l| l.starts_with("EVENT=StrandedJournal"))
        .collect();
    assert_eq!(events.len(), 1, "{}", out.dump());
    let e = events[0];
    for f in [
        "sub: \"sub-y\"",
        "create: 0",
        "modify: 1",
        "delete: 1",
        "unparsed: 0",
    ] {
        assert!(e.contains(f), "{f} missing from {e}");
    }
    assert_eq!(std::fs::read(&jpath).unwrap(), bytes, "Y's journal changed");
    assert_eq!(
        out.kv("Y_CLAIMED").as_deref(),
        Some("false"),
        "{}",
        out.dump()
    );
    let recs: Vec<&str> = out
        .stdout
        .lines()
        .filter_map(|l| l.strip_prefix("Y_REC="))
        .collect();
    assert!(
        !recs.iter().any(|r| *r == "openat:push_journal.lock"
            || *r == "open_w:push_journal.jsonl"
            || r.starts_with("write:push_journal")
            || r.starts_with("renameat:push_journal")),
        "a write-capable or lock open on Y's journal: {recs:?}"
    );
    assert!(
        recs.contains(&"openat:push_journal.jsonl"),
        "the read open was not seen: {recs:?}"
    );
    assert!(
        !m.pushes
            .lock()
            .unwrap()
            .iter()
            .any(|p| p.path.starts_with("stranded/")),
        "a stranded line was pushed"
    );
}

/// T-CS3-14 (gate C7, pipeline-token half): split config with only Y's
/// token: X's pipeline fails at `token_store::load(eff_sub)` and V&R refuses
/// with `RootFailed{token}`; the mock sees zero reconcile requests and the
/// click loads no token (Y's included).
/// Mutation: fall back to the top-level token when the slot is empty.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn t_cs3_14_root_failed_token() {
    let m = mock(MockOpts::default()).await;
    // An empty vault: the D9 seed sends no reconcile request, so every
    // request the mock could see would be V&R's.
    let env = split_env(&m.srv.url(), &["sub-y"]);
    let out = run_child(&env, "split_no_x_token", &m.srv.url(), &[]);
    assert_eq!(out.code, Some(0), "{}", out.dump());
    assert_eq!(
        out.kv("X_FAILED_TOKEN").as_deref(),
        Some("true"),
        "{}",
        out.dump()
    );
    let pre = out.kv("PREFLIGHT").unwrap_or_default();
    assert!(
        pre.contains("RootFailed") && pre.contains("Token"),
        "{}",
        out.dump()
    );
    let vr = out.kv("VR_RESULT").unwrap_or_default();
    assert!(
        vr.contains("failed to start at stage token"),
        "{}",
        out.dump()
    );
    assert_eq!(
        out.kv("LOADS_DURING_CLICK").as_deref(),
        Some("[]"),
        "{}",
        out.dump()
    );
    assert_eq!(*m.reconciles.lock().unwrap(), 0);
}

#[cfg(unix)]
fn vr_panic_run(site: &str) -> (Out, Mock) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let m = rt.block_on(mock(MockOpts {
        reconcile_deltas: serde_json::json!([{"path": "vr.md", "state": "missing-on-server"}]),
        ..Default::default()
    }));
    let env = one_root_env(&m.srv.url());
    std::fs::write(env.vaults.join("Mainframe/vr.md"), b"vr\n").unwrap();
    let out = run_child(
        &env,
        "vr_panic",
        &m.srv.url(),
        &[("CS3_PANIC_SITE", site), ("CS3_FAST_TICK_MS", "50")],
    );
    drop(env);
    (out, m)
}

/// T-CS3-16 (T-V2 liveness): with V&R parked between the manifest walk and
/// `append_batch`, each Required task in turn panics: V&R returns "daemon
/// stopping", the journal shows 0 appends after the stop, and the process
/// exits 70.
/// Mutation: V&R ignores its `live` receiver.
#[cfg(unix)]
#[test]
fn t_cs3_16_required_panic_stops_vr_and_exits_70() {
    for site in [
        "drain_loop",
        "watcher",
        "custody_tick",
        "flush_shadow",
        "flush_base_seq",
        "flush_read_receipt",
        "sse_consumer",
    ] {
        let (out, _m) = vr_panic_run(site);
        assert_eq!(
            out.code,
            Some(70),
            "{site}: exit 70 expected\n{}",
            out.dump()
        );
        assert_eq!(
            out.kv("VR_PARKED").as_deref(),
            Some("true"),
            "{site}\n{}",
            out.dump()
        );
        let vr = out.kv("VR_RESULT").unwrap_or_default();
        assert!(
            vr.starts_with("err daemon stopping"),
            "{site}: V&R did not stop: {vr}\n{}",
            out.dump()
        );
        assert_eq!(
            out.kv("JOURNAL_LEN").as_deref(),
            Some("0"),
            "{site}: appended after the stop\n{}",
            out.dump()
        );
    }
}

/// T-CS3-16 (RootDegrading): the reconcile backstop panics; that root's
/// `alive` is false, V&R refuses it with `RootNotAlive`, and the process does
/// not exit.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn t_cs3_16_backstop_panic_degrades_root() {
    let m = mock(MockOpts::default()).await;
    let env = one_root_env(&m.srv.url());
    let out = run_child(
        &env,
        "backstop_panic",
        &m.srv.url(),
        &[
            ("CS3_FAULTS", "reconcile_backstop.panic"),
            ("VAULT_SYNC_RECON_INTERVAL_SECS", "3600"),
        ],
    );
    assert_eq!(out.code, Some(0), "{}", out.dump());
    assert_eq!(
        out.kv("ROOT_NOT_ALIVE").as_deref(),
        Some("true"),
        "{}",
        out.dump()
    );
    assert!(
        out.kv("VR_RESULT")
            .unwrap_or_default()
            .contains("is degraded"),
        "{}",
        out.dump()
    );
    assert_eq!(out.kv("LIVE").as_deref(), Some("true"));
}

#[cfg(unix)]
fn supervise_run(arm: &str, extra: &[(&str, &str)]) -> Out {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let m = rt.block_on(mock(MockOpts::default()));
    let env = one_root_env(&m.srv.url());
    let mut e = vec![("CS3_ARM", arm), ("CS3_FAST_TICK_MS", "50")];
    e.extend_from_slice(extra);
    run_child(&env, "supervise", &m.srv.url(), &e)
}

/// T-CS3-17 (supervise, gate C4; subprocess half): a drain-loop panic exits
/// 70 within 1 s with one ERROR and one `task_ended{cause: panic}`; an `Ok`
/// return of the drain loop exits 70; a heartbeat panic and a
/// `config_watch` panic each raise `task_ended` with no exit; the backstop
/// kill switch raises `task_disabled` and the root stays alive. (The storm
/// fence and the JoinHandle measurement are in `cs3_tests`.)
/// Mutations: `supervise` without awaiting the `JoinHandle`; classify
/// `config_watch` as Required.
#[cfg(unix)]
#[test]
fn t_cs3_17_supervised_ends() {
    // "Within 1 s": one retry absorbs a loaded CI runner; a supervisor that
    // never exits (the fire-and-forget mutant) fails both attempts.
    let mut out = supervise_run("drain_loop.panic", &[]);
    for attempt in 0..2 {
        assert_eq!(out.code, Some(70), "{}", out.dump());
        let armed: u128 = out.kv("ARMED_MS").unwrap().parse().unwrap();
        let took = out.exited_ms.saturating_sub(armed);
        if took < 1_000 {
            break;
        }
        assert!(attempt == 0, "exit 70 took {took} ms twice\n{}", out.dump());
        out = supervise_run("drain_loop.panic", &[]);
    }
    assert_eq!(
        count(&out.stderr, "REQUIRED task ended unexpectedly"),
        1,
        "{}",
        out.dump()
    );
    let ended: Vec<&str> = out
        .stderr
        .lines()
        .filter(|l| l.contains("sync_health: task_ended"))
        .collect();
    assert_eq!(ended.len(), 1, "{}", out.dump());
    assert!(
        ended[0].contains("cause=panic") && ended[0].contains("drain_loop:0"),
        "{ended:?}"
    );

    let out = supervise_run("drain_loop.return", &[]);
    assert_eq!(out.code, Some(70), "{}", out.dump());
    assert!(count(&out.stderr, "cause=returned") >= 1, "{}", out.dump());

    let out = supervise_run(
        "heartbeat.panic,config_watch.panic",
        &[("CS3_CONFIG_WATCH_MS", "100")],
    );
    assert_eq!(
        out.code,
        Some(0),
        "an Optional panic exited\n{}",
        out.dump()
    );
    assert!(out.has("NO_EXIT"), "{}", out.dump());
    assert_eq!(out.kv("LIVE").as_deref(), Some("true"));
    for task in ["task=heartbeat", "task=config_watch"] {
        assert!(
            out.stderr
                .lines()
                .any(|l| l.contains("sync_health: task_ended") && l.contains(task)),
            "{task}\n{}",
            out.dump()
        );
    }

    let out = supervise_run("", &[("VAULT_SYNC_DISABLE_RECON", "1")]);
    assert_eq!(out.code, Some(0), "{}", out.dump());
    assert!(
        out.stderr
            .lines()
            .any(|l| l.contains("task_disabled") && l.contains("reconcile_backstop:0")),
        "{}",
        out.dump()
    );
    assert_eq!(out.kv("ROOT0_ALIVE").as_deref(), Some("true"));
    assert_eq!(out.kv("BORROW0").as_deref(), Some("None"), "{}", out.dump());
}

/// T-CS3-18 (gate C6): the mock SSE answers 401: no exit (3 s observed), `live`
/// reads `Stopped{sse_auth_failed}`, `sse_auth_failed{sub}` is raised once,
/// exactly ONE notice names what stopped, why and the one action, V&R refuses
/// with `NotLive`, and the watcher ends after the boot task drops their
/// handles are logged as cascade with no exit 70 and no second notice.
/// Mutations: classify the 401 as an unexpected end (exit 70); notice on
/// each cascade end; omit the notice.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn t_cs3_18_sse_401_terminal() {
    let m = mock(MockOpts {
        sse_401: true,
        ..Default::default()
    })
    .await;
    let env = one_root_env(&m.srv.url());
    let out = run_child(&env, "sse_401", &m.srv.url(), &[]);
    assert_eq!(out.code, Some(0), "{}", out.dump());
    assert!(out.has("NO_EXIT"), "{}", out.dump());
    assert_eq!(
        out.kv("STOPPED_SSE_AUTH").as_deref(),
        Some("true"),
        "{}",
        out.dump()
    );
    let notices: Vec<&str> = out
        .stdout
        .lines()
        .filter(|l| l.starts_with("NOTICE|"))
        .collect();
    assert_eq!(notices.len(), 1, "{}", out.dump());
    let n = notices[0];
    assert!(n.contains("pull and push for subscriber sub-a"), "{n}");
    assert!(n.contains("HTTP 401"), "{n}");
    assert!(n.contains("re-pair this device"), "{n}");
    assert_eq!(
        out.stdout
            .lines()
            .filter(|l| l.starts_with("EVENT=SseAuthFailed"))
            .count(),
        1,
        "{}",
        out.dump()
    );
    let vr = out.kv("VR_RESULT").unwrap_or_default();
    assert!(
        vr.contains("the daemon is stopping (sse_auth_failed)"),
        "{}",
        out.dump()
    );
    assert!(
        out.stderr
            .lines()
            .any(|l| l.contains("cascade") && l.contains("watcher:0")),
        "the watcher end was not logged as cascade\n{}",
        out.dump()
    );
    assert_eq!(count(&out.stderr, "exiting 70"), 0, "{}", out.dump());
}

#[cfg(unix)]
fn exit_order_run(order: &str, extra: &[(&str, &str)]) -> (Out, Env, Mock) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let m = rt.block_on(mock(MockOpts::default()));
    let env = one_root_env(&m.srv.url());
    let mut e = vec![("CS3_ORDER", order)];
    e.extend_from_slice(extra);
    let out = run_child(&env, "exit_order", &m.srv.url(), &e);
    (out, env, m)
}

/// T-CS3-19 (gates C1 and C9, three orders): A, 74 first (custody lost at
/// the push gate while a drain is mid-commit); B, 70 first (a drain-loop
/// panic; custody lost inside its pre-exit store flush); C, watchdog first
/// (a forced PushStall claims `exit_once`; custody lost between the claim and
/// `app.restart()`). In ALL three the process exits 74, no restart is called
/// and no successor is spawned, `ownership_lost` set the mutation latch even
/// where `exit_once` was already claimed (B, C), and no state byte changed
/// after the loss.
/// Mutations: `ownership_lost` latches only when it wins `exit_once`; drop
/// the 70 path's re-check (B exits 70); drop the watchdog's re-check (C
/// relaunches).
#[cfg(unix)]
#[test]
fn t_cs3_19_exit_precedence() {
    let (out, env, m) = exit_order_run("A", &[]);
    assert_eq!(out.code, Some(74), "A\n{}", out.dump());
    assert!(!out.has("RESTART_CALLED"), "A\n{}", out.dump());
    assert_eq!(m.pushes.lock().unwrap().len(), 0, "A pushed after the loss");
    assert_untouched_since_loss(&env, "sub-a", &out);

    let (out, env, _m) = exit_order_run("B", &[]);
    assert_eq!(out.code, Some(74), "B\n{}", out.dump());
    assert!(!out.has("RESTART_CALLED"), "B\n{}", out.dump());
    let line = out
        .stderr
        .lines()
        .find(|l| l.contains("exiting 74 instead"))
        .unwrap_or_else(|| panic!("B: no C9 line\n{}", out.dump()));
    assert!(line.contains("mutation_latch=true"), "B: {line}");
    assert!(line.contains("drain_loop:0"), "B: {line}");
    assert_untouched_since_loss(&env, "sub-a", &out);

    let (out, env, _m) =
        exit_order_run("C", &[("CS3_STALL_ZERO", "1"), ("CS3_WATCHDOG_MS", "100")]);
    assert_eq!(out.code, Some(74), "C\n{}", out.dump());
    assert!(!out.has("RESTART_CALLED"), "C relaunched\n{}", out.dump());
    let line = out
        .stderr
        .lines()
        .find(|l| l.contains("exiting 74 instead"))
        .unwrap_or_else(|| panic!("C: no C9 line\n{}", out.dump()));
    assert!(line.contains("mutation_latch=true"), "C: {line}");
    assert!(line.contains("stall_watchdog"), "C: {line}");
    assert_untouched_since_loss(&env, "sub-a", &out);
}

/// T-CS3-20 (partial pipeline): an injected watcher start failure on root 1
/// leaves it `failed = watcher_start` with `pipeline_failed{root, stage}`,
/// its drain loop still draining (status quo), its readiness missing the
/// watcher, and V&R refusing it; an R7 park on root 0 gives
/// `failed = r7_park`.
/// Mutation: set the root ready on the drain bit alone.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn t_cs3_20_partial_pipeline() {
    let m = mock(MockOpts::default()).await;
    let env = Env::new(
        &m.srv.url(),
        "sub-a",
        &[
            Root {
                folder: "Mainframe",
                sub: "",
            },
            Root {
                folder: "Second",
                sub: "",
            },
        ],
        &["sub-a"],
    );
    let out = run_child(
        &env,
        "partial",
        &m.srv.url(),
        &[("CS3_FAULTS", "watcher_start.inotify.1")],
    );
    assert_eq!(out.code, Some(0), "{}", out.dump());
    assert_eq!(
        out.kv("ROOT1_FAILED").as_deref(),
        Some("Some(WatcherStart)"),
        "{}",
        out.dump()
    );
    assert_eq!(
        out.kv("ROOT1_DRAINED").as_deref(),
        Some("true"),
        "{}",
        out.dump()
    );
    let missing = out.kv("ROOT1_MISSING").unwrap_or_default();
    assert!(
        missing.contains("watcher"),
        "root 1 reads ready without its watcher: {missing}"
    );
    assert!(
        out.stdout
            .lines()
            .any(|l| l.starts_with("EVENT=PipelineFailed")
                && l.contains("root: 1")
                && l.contains("watcher_start")),
        "{}",
        out.dump()
    );
    assert!(
        out.kv("VR_RESULT")
            .unwrap_or_default()
            .contains("stage watcher_start"),
        "{}",
        out.dump()
    );

    let m2 = mock(MockOpts::default()).await;
    let env2 = one_root_env(&m2.srv.url());
    let out2 = run_child(
        &env2,
        "partial",
        &m2.srv.url(),
        &[("CS3_FAULTS", "pipeline.r7_park.0")],
    );
    assert_eq!(
        out2.kv("ROOT0_FAILED").as_deref(),
        Some("Some(R7Park)"),
        "{}",
        out2.dump()
    );
}

/// T-CS3-22 (bounded pre-exit flush, G-7; custody-intact half): with a dirty
/// shadow entry, a drain-loop panic leaves the entry in `shadow_hashes.json`
/// after exit 70 (the periodic flush is 30 s away; only the pre-exit flush
/// can have written it).
/// Mutation: skip the pre-exit flush (entry lost).
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn t_cs3_22_pre_exit_flush_persists() {
    let m = mock(MockOpts::default()).await;
    let env = one_root_env(&m.srv.url());
    let out = run_child(&env, "flush_persists", &m.srv.url(), &[]);
    assert_eq!(out.code, Some(70), "{}", out.dump());
    let shadow = std::fs::read_to_string(
        env.state_dir("sub-a")
            .join(crate::state_io::StateFile::Shadow.name()),
    )
    .unwrap_or_default();
    assert!(
        shadow.contains("cs3-22-dirty.md"),
        "the dirty entry was lost: {shadow}\n{}",
        out.dump()
    );
}
