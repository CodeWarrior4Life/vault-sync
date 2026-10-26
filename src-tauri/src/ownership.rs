//! Unit CS-3 (TKT-b6d8c4ba) §3.1: one config snapshot per process (F3,
//! AR-1C-005).
//!
//! The daemon reads `config.toml` once per process, in `.setup()`. The tray
//! state, the launch gate, the CS-2 bootstrap set S, the pipelines and Verify
//! and Repair all read that one immutable [`OwnershipSnapshot`]. The hash and
//! the parsed config come from the same buffer, so they cannot disagree.
//!
//! A changed file on disk is surfaced (`config_watch`, every 60 s, full
//! sha256, no mtime or size shortcut) and never reloaded: the daemon keeps
//! running on its snapshot until a restart. V&R re-hashes the file at click
//! time (D10), so it never runs under a stale snapshot.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use sha2::{Digest, Sha256};

use crate::config::{Config, ConfigError};
use crate::supervise::TaskEnd;
use crate::sync_health::{raise_state_event, StateHealthEvent};
use crate::tray_state::SharedTrayState;

/// A root's index in `cfg.sync_roots` order.
pub type RootKey = usize;

/// One sync root as the snapshot resolved it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RootSpec {
    pub key: RootKey,
    pub path: PathBuf,
    pub route: String,
    /// `effective_subscriber_id(root)`: the root's own sub, else the
    /// top-level sub.
    pub eff_sub: String,
}

/// The process's one view of its configuration.
#[derive(Debug)]
pub struct OwnershipSnapshot {
    cfg_path: PathBuf,
    cfg: Arc<Config>,
    config_sha256: [u8; 32],
    top_sub: String,
    subs: Vec<String>,
    roots: Vec<RootSpec>,
    loaded_at: SystemTime,
}

/// Lowercase hex of the first 4 bytes (operator text).
pub fn sha8(h: &[u8; 32]) -> String {
    hex::encode(&h[..4])
}

impl OwnershipSnapshot {
    /// Parse `bytes` (the exact bytes read from `cfg_path`) and hash them.
    pub fn from_bytes(cfg_path: &Path, bytes: &[u8]) -> Result<Self, ConfigError> {
        let text = std::str::from_utf8(bytes)
            .map_err(|e| ConfigError::Io(io::Error::new(io::ErrorKind::InvalidData, e)))?;
        let cfg = Config::from_toml_back_compat(text)?;
        let config_sha256: [u8; 32] = Sha256::digest(bytes).into();
        let top_sub = cfg.subscriber_id.clone();
        // S keeps today's order and dedup (lib.rs:489-494@23ef189d): the
        // top-level sub first, then each new effective sub in roots order.
        let mut subs = vec![top_sub.clone()];
        let mut roots = Vec::with_capacity(cfg.sync_roots.len());
        for (key, r) in cfg.sync_roots.iter().enumerate() {
            let eff_sub = crate::effective_subscriber_id(r, &cfg);
            if !subs.contains(&eff_sub) {
                subs.push(eff_sub.clone());
            }
            roots.push(RootSpec {
                key,
                path: r.path.clone(),
                route: r.route.clone(),
                eff_sub,
            });
        }
        Ok(Self {
            cfg_path: cfg_path.to_path_buf(),
            cfg: Arc::new(cfg),
            config_sha256,
            top_sub,
            subs,
            roots,
            loaded_at: SystemTime::now(),
        })
    }

    pub fn cfg_path(&self) -> &Path {
        &self.cfg_path
    }
    pub fn cfg(&self) -> &Arc<Config> {
        &self.cfg
    }
    pub fn config_sha256(&self) -> &[u8; 32] {
        &self.config_sha256
    }
    pub fn sha8(&self) -> String {
        sha8(&self.config_sha256)
    }
    pub fn top_sub(&self) -> &str {
        &self.top_sub
    }
    /// The CS-2 bootstrap set S.
    pub fn subs(&self) -> &[String] {
        &self.subs
    }
    pub fn roots(&self) -> &[RootSpec] {
        &self.roots
    }
    pub fn loaded_at(&self) -> SystemTime {
        self.loaded_at
    }
}

/// The outcome of the one startup read.
#[derive(Debug)]
pub enum BootConfig {
    Absent,
    Unreadable(String),
    Loaded(Arc<OwnershipSnapshot>),
}

impl BootConfig {
    /// CS-1 startup maintenance (dead-pid temp sweep and dir fsync on the
    /// config and token dirs), then ONE read of the file's bytes, the sha256
    /// of those bytes, and `Config::from_toml_back_compat` of the same bytes.
    /// Replaces the four startup loads L1-L4.
    pub fn load_at_startup(cfg_path: &Path) -> BootConfig {
        let token_dir = crate::token_store::token_dir();
        Self::load_at_startup_with(cfg_path, &token_dir)
    }

    pub(crate) fn load_at_startup_with(cfg_path: &Path, token_dir: &Path) -> BootConfig {
        let config_dir = cfg_path.parent().unwrap_or_else(|| Path::new("."));
        crate::durable_file::startup_maintenance(&[config_dir, token_dir]);
        // T-CS3-1: the same `config_load:` seam event `Config::load_from`
        // records, so a boot shows exactly one.
        #[cfg(test)]
        crate::durable_file::seams::record_event(format!("config_load:{}", cfg_path.display()));
        #[cfg(test)]
        crate::config::CONFIG_LOADS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        match read_config_bytes(cfg_path) {
            Ok(None) => BootConfig::Absent,
            Err(e) => BootConfig::Unreadable(e.to_string()),
            Ok(Some(bytes)) => match OwnershipSnapshot::from_bytes(cfg_path, &bytes) {
                Ok(s) => BootConfig::Loaded(Arc::new(s)),
                Err(e) => BootConfig::Unreadable(e.to_string()),
            },
        }
    }

    pub fn snapshot(&self) -> Option<&Arc<OwnershipSnapshot>> {
        match self {
            BootConfig::Loaded(s) => Some(s),
            _ => None,
        }
    }
}

/// Read the config file's bytes (gate 1 permits reads). `None` if absent.
fn read_config_bytes(p: &Path) -> io::Result<Option<Vec<u8>>> {
    match std::fs::read(p) {
        Ok(b) => Ok(Some(b)),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

/// What the file on disk hashes to right now.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DiskConfig {
    Hash([u8; 32]),
    Absent,
    Unreadable(String),
}

impl DiskConfig {
    pub fn sha8(&self) -> String {
        match self {
            DiskConfig::Hash(h) => sha8(h),
            DiskConfig::Absent => "absent".to_string(),
            DiskConfig::Unreadable(_) => "unreadable".to_string(),
        }
    }
}

/// A fresh full-file sha256 of `p` (no parse, no mtime or size shortcut).
pub fn hash_config_file(p: &Path) -> DiskConfig {
    match read_config_bytes(p) {
        Ok(Some(b)) => DiskConfig::Hash(Sha256::digest(&b).into()),
        Ok(None) => DiskConfig::Absent,
        Err(e) => DiskConfig::Unreadable(e.to_string()),
    }
}

/// The 60 s config change notice (§3.1). Surfaces, never reloads.
pub struct ConfigWatch {
    snapshot: Arc<OwnershipSnapshot>,
    tray: Option<SharedTrayState>,
    /// The disk sha8 last raised (None = disk matches the snapshot).
    raised_for: Option<String>,
    /// Events this watch raised, in order (observation).
    raised: Vec<StateHealthEvent>,
    /// The previous tick read an EMPTY file (TKT-e9f45912): a truncate-then-write
    /// writer is briefly empty, so one empty read is unsettled, not a change.
    empty_seen: bool,
}

impl ConfigWatch {
    pub fn new(snapshot: Arc<OwnershipSnapshot>, tray: Option<SharedTrayState>) -> Self {
        Self {
            snapshot,
            tray,
            raised_for: None,
            raised: Vec::new(),
            empty_seen: false,
        }
    }

    /// One tick: hash the file; raise `config_pending_restart` once per
    /// distinct disk hash, and `config_pending_restart_cleared` when the
    /// bytes return to the snapshot's hash.
    pub fn tick(&mut self) {
        let disk = hash_config_file(self.snapshot.cfg_path());
        let same = matches!(&disk, DiskConfig::Hash(h) if h == self.snapshot.config_sha256());
        let snapshot_sha8 = self.snapshot.sha8();
        // TKT-e9f45912: an EMPTY read that does not match the snapshot is
        // unsettled the first time (a non-atomic writer mid-write: macOS CI
        // saw disk_sha8=e3b0c442, the sha256 of no bytes, for ~200 ms). Raise
        // nothing and re-read next tick; a file still empty then is real.
        let empty: [u8; 32] = Sha256::digest(b"").into();
        let is_empty = matches!(&disk, DiskConfig::Hash(h) if *h == empty);
        if !same && is_empty && !self.empty_seen {
            self.empty_seen = true;
            return;
        }
        self.empty_seen = is_empty;
        if same {
            if self.raised_for.take().is_some() {
                self.set_tray(false);
                self.raise(StateHealthEvent::ConfigPendingRestartCleared { snapshot_sha8 });
            }
            return;
        }
        let disk_sha8 = disk.sha8();
        if self.raised_for.as_deref() == Some(disk_sha8.as_str()) {
            return;
        }
        self.raised_for = Some(disk_sha8.clone());
        self.set_tray(true);
        self.raise(StateHealthEvent::ConfigPendingRestart {
            snapshot_sha8,
            disk_sha8,
        });
    }

    fn set_tray(&self, pending: bool) {
        if let Some(t) = &self.tray {
            if let Ok(mut w) = t.write() {
                w.set_config_pending_restart(pending);
            }
        }
    }

    fn raise(&mut self, e: StateHealthEvent) {
        raise_state_event(e.clone());
        self.raised.push(e);
    }

    /// Events raised so far (tests).
    pub fn raised(&self) -> &[StateHealthEvent] {
        &self.raised
    }
}

/// The supervised `config_watch` task (class Optional, gate C4): tick every
/// `period` forever.
pub async fn run_config_watch(mut w: ConfigWatch, period: Duration) -> TaskEnd {
    let mut tick = tokio::time::interval(period);
    tick.tick().await; // the immediate first tick: the snapshot was just read
    loop {
        tick.tick().await;
        #[cfg(test)]
        crate::faults::point("config_watch");
        w.tick();
    }
}
