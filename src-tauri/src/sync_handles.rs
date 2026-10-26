//! Unit CS-3 (TKT-b6d8c4ba) §3.3-3.4: one set of handles, published once,
//! and the Verify and Repair borrow (AR-1C-003, AR-1C-017, AR-027).
//!
//! [`SyncHandles`] holds the snapshot, the three stores, the journal owners,
//! per-root readiness and the process liveness signal. It is built by the
//! boot task and published into the [`SyncHandlesSlot`] exactly once
//! (`OnceLock::set`). V&R borrows from it ([`SyncHandles::borrow`]) and never
//! loads config, opens a journal, loads a shadow or loads a token.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::watch;

use crate::api_client::ApiClient;
use crate::base_seq_store::BaseSeqStore;
use crate::journal_owner::JournalOwner;
use crate::ownership::{OwnershipSnapshot, RootKey, RootSpec};
use crate::read_receipt::ReadReceiptStore;
use crate::supervise::Liveness;
use crate::sync_shadow::ShadowStore;

/// The managed slot (D6): `app.manage()`d in `.setup()` always, even
/// unpaired. Empty until the boot task publishes.
#[derive(Clone, Default)]
pub struct SyncHandlesSlot(Arc<OnceLock<Arc<SyncHandles>>>);

impl SyncHandlesSlot {
    /// Publish `h`. A second publication in one process is refused (the
    /// first stands) and returns false.
    pub fn publish(&self, h: Arc<SyncHandles>) -> bool {
        self.0.set(h).is_ok()
    }

    pub fn get(&self) -> Option<Arc<SyncHandles>> {
        self.0.get().cloned()
    }
}

/// One publication per process (D17): `(pid, boot unix-nanos)`. CS-5 binds
/// its epoch and `boot_seq` to it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InstanceId {
    pub pid: u32,
    pub boot_nanos: u128,
}

impl InstanceId {
    pub fn now() -> Self {
        Self {
            pid: std::process::id(),
            boot_nanos: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or_default(),
        }
    }
}

/// Where a root's pipeline failed (§3.3). Any failed stage keeps the root
/// not-ready for the life of the process.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PipelineStage {
    R7Park,
    Token,
    NotBootstrapped,
    JournalOpen,
    ApiInit,
    WatcherInit,
    WatcherStart,
    WatcherFenced,
}

impl PipelineStage {
    pub fn as_str(self) -> &'static str {
        match self {
            PipelineStage::R7Park => "r7_park",
            PipelineStage::Token => "token",
            PipelineStage::NotBootstrapped => "not_bootstrapped",
            PipelineStage::JournalOpen => "journal_open",
            PipelineStage::ApiInit => "api_init",
            PipelineStage::WatcherInit => "watcher_init",
            PipelineStage::WatcherStart => "watcher_start",
            PipelineStage::WatcherFenced => "watcher_fenced",
        }
    }
}

/// Readiness bit: the root's watcher started.
pub const WATCHER_STARTED: u8 = 1;
/// Readiness bit: the root's drain loop finished one `drain_once`.
pub const FIRST_DRAIN_DONE: u8 = 2;

/// Per-root handles and state.
pub struct RootHandles {
    spec: RootSpec,
    /// `None` if its sub's journal failed to open (stage `journal_open`).
    journal: Option<Arc<JournalOwner>>,
    /// Set ONLY from the root pipeline's own client, built from
    /// `token_store::load(eff_sub)` (gate C7). No fallback exists.
    api: OnceLock<Arc<ApiClient>>,
    ready: AtomicU8,
    alive: Arc<AtomicBool>,
    failed: OnceLock<PipelineStage>,
}

impl RootHandles {
    pub fn new(spec: RootSpec, journal: Option<Arc<JournalOwner>>) -> Self {
        Self {
            spec,
            journal,
            api: OnceLock::new(),
            ready: AtomicU8::new(0),
            alive: Arc::new(AtomicBool::new(true)),
            failed: OnceLock::new(),
        }
    }

    pub fn spec(&self) -> &RootSpec {
        &self.spec
    }
    pub fn journal(&self) -> Option<&Arc<JournalOwner>> {
        self.journal.as_ref()
    }
    /// Gate C7: called only by the root's pipeline with the client it built
    /// from its own token.
    pub fn set_api(&self, api: Arc<ApiClient>) {
        let _ = self.api.set(api);
    }
    pub fn api(&self) -> Option<&Arc<ApiClient>> {
        self.api.get()
    }
    pub fn mark(&self, bit: u8) {
        self.ready.fetch_or(bit, Ordering::SeqCst);
    }
    pub fn ready_bits(&self) -> u8 {
        self.ready.load(Ordering::SeqCst)
    }
    /// The RootDegrading flag (the reconcile backstop's supervisor clears it).
    pub fn alive_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.alive)
    }
    pub fn is_alive(&self) -> bool {
        self.alive.load(Ordering::SeqCst)
    }
    /// Record the first failed stage (later ones are ignored).
    pub fn fail(&self, stage: PipelineStage) -> bool {
        self.failed.set(stage).is_ok()
    }
    pub fn failed(&self) -> Option<PipelineStage> {
        self.failed.get().copied()
    }
}

/// The process's sync handles (§3.3).
pub struct SyncHandles {
    instance: InstanceId,
    snapshot: Arc<OwnershipSnapshot>,
    shadow: Arc<ShadowStore>,
    base_seq: Arc<BaseSeqStore>,
    read_receipt: Arc<ReadReceiptStore>,
    journals: BTreeMap<String, Arc<JournalOwner>>,
    roots: Vec<RootHandles>,
    live: watch::Sender<Liveness>,
    sse_connected_once: AtomicBool,
}

/// What V&R gets for one root.
pub struct LiveBorrow {
    pub root: RootSpec,
    pub journal: Arc<JournalOwner>,
    pub shadow: Arc<ShadowStore>,
    pub api: Arc<ApiClient>,
    pub live: watch::Receiver<Liveness>,
    pub instance: InstanceId,
}

/// Why V&R refused (§3.4).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BorrowRefused {
    NotPublished {
        why: String,
    },
    RootNotReady {
        root: RootKey,
        missing: Vec<&'static str>,
    },
    RootFailed {
        root: RootKey,
        stage: PipelineStage,
    },
    RootNotAlive {
        root: RootKey,
        task: &'static str,
    },
    NotLive {
        reason: String,
    },
    ConfigChanged {
        snapshot_sha8: String,
        disk_sha8: String,
    },
    ConfigUnreadable {
        why: String,
    },
}

impl std::fmt::Display for BorrowRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BorrowRefused::NotPublished { why } => write!(f, "sync is not running ({why})"),
            BorrowRefused::RootNotReady { root, missing } => {
                write!(f, "root {root} is not ready yet (waiting for: {})", missing.join(", "))
            }
            BorrowRefused::RootFailed { root, stage } => {
                write!(f, "root {root} failed to start at stage {}", stage.as_str())
            }
            BorrowRefused::RootNotAlive { root, task } => {
                write!(f, "root {root} is degraded: its {task} task ended")
            }
            BorrowRefused::NotLive { reason } => write!(f, "the daemon is stopping ({reason})"),
            BorrowRefused::ConfigChanged {
                snapshot_sha8,
                disk_sha8,
            } => write!(
                f,
                "config.toml changed on disk ({snapshot_sha8} -> {disk_sha8}); restart the daemon to apply it"
            ),
            BorrowRefused::ConfigUnreadable { why } => write!(f, "config.toml unreadable: {why}"),
        }
    }
}

/// The parts the boot task assembles into [`SyncHandles`].
pub struct SyncHandlesParts {
    pub snapshot: Arc<OwnershipSnapshot>,
    pub shadow: Arc<ShadowStore>,
    pub base_seq: Arc<BaseSeqStore>,
    pub read_receipt: Arc<ReadReceiptStore>,
    pub journals: BTreeMap<String, Arc<JournalOwner>>,
    pub roots: Vec<RootHandles>,
    pub live: watch::Sender<Liveness>,
}

impl SyncHandles {
    /// Build (every root not ready). The stores and owners are moved in and
    /// handed out only as `Arc` clones.
    pub fn new(p: SyncHandlesParts) -> Self {
        Self {
            instance: InstanceId::now(),
            snapshot: p.snapshot,
            shadow: p.shadow,
            base_seq: p.base_seq,
            read_receipt: p.read_receipt,
            journals: p.journals,
            roots: p.roots,
            live: p.live,
            sse_connected_once: AtomicBool::new(false),
        }
    }

    pub fn instance(&self) -> InstanceId {
        self.instance
    }
    pub fn snapshot(&self) -> &Arc<OwnershipSnapshot> {
        &self.snapshot
    }
    pub fn shadow(&self) -> &Arc<ShadowStore> {
        &self.shadow
    }
    pub fn base_seq(&self) -> &Arc<BaseSeqStore> {
        &self.base_seq
    }
    pub fn read_receipt(&self) -> &Arc<ReadReceiptStore> {
        &self.read_receipt
    }
    /// The journal owner of `sub`, if it opened.
    pub fn journal(&self, sub: &str) -> Option<Arc<JournalOwner>> {
        self.journals.get(sub).cloned()
    }
    pub fn journals(&self) -> &BTreeMap<String, Arc<JournalOwner>> {
        &self.journals
    }
    pub fn roots(&self) -> &[RootHandles] {
        &self.roots
    }
    pub fn root(&self, key: RootKey) -> Option<&RootHandles> {
        self.roots.get(key)
    }
    pub fn liveness(&self) -> Liveness {
        self.live.borrow().clone()
    }
    /// The SSE consumer reached `Connected` (a latch: a reconnect never
    /// clears it).
    pub fn mark_sse_connected(&self) {
        self.sse_connected_once.store(true, Ordering::SeqCst);
    }
    pub fn sse_connected_once(&self) -> bool {
        self.sse_connected_once.load(Ordering::SeqCst)
    }

    /// Borrow root `key` for V&R (§3.4). Refuses unless the process is live,
    /// the root has not failed, is alive, and is ready (watcher started,
    /// first drain done, SSE connected once, pipeline client set).
    pub fn borrow(&self, key: RootKey) -> Result<LiveBorrow, BorrowRefused> {
        if let Liveness::Stopped { reason } = self.liveness() {
            return Err(BorrowRefused::NotLive { reason });
        }
        let Some(r) = self.roots.get(key) else {
            return Err(BorrowRefused::RootNotReady {
                root: key,
                missing: vec!["root"],
            });
        };
        if let Some(stage) = r.failed() {
            return Err(BorrowRefused::RootFailed { root: key, stage });
        }
        if !r.is_alive() {
            return Err(BorrowRefused::RootNotAlive {
                root: key,
                task: "reconcile_backstop",
            });
        }
        let bits = r.ready_bits();
        let mut missing = Vec::new();
        if bits & WATCHER_STARTED == 0 {
            missing.push("watcher");
        }
        if bits & FIRST_DRAIN_DONE == 0 {
            missing.push("first_drain");
        }
        if !self.sse_connected_once() {
            missing.push("sse");
        }
        let api = r.api().cloned();
        if api.is_none() {
            missing.push("api");
        }
        let journal = r.journal().cloned();
        if journal.is_none() {
            missing.push("journal");
        }
        if !missing.is_empty() {
            return Err(BorrowRefused::RootNotReady { root: key, missing });
        }
        Ok(LiveBorrow {
            root: r.spec().clone(),
            journal: journal.expect("checked"),
            shadow: Arc::clone(&self.shadow),
            api: api.expect("checked"),
            live: self.live.subscribe(),
            instance: self.instance,
        })
    }
}
