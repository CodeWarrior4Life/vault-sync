//! Persistent per-file shadow-hash store — the missing "last-synced server
//! hash" marker that lets the reconcile backstop tell a genuine local user
//! edit (→ push) apart from a STALE prior-materialization (→ pull, server-wins).
//!
//! ## Why this exists
//!
//! The reconcile backstop (`verify_repair::run`) compares each local file's
//! SHA against the server `fs_hash`. When they differ it has historically
//! ALWAYS enqueued a PUSH. For a host that mirrors the server, that is wrong:
//! if our local copy is a stale materialization (the server moved on after we
//! last synced), pushing it overwrites the newer server bytes → 409/overwrite
//! churn (the "storm"). The daemon can't distinguish the two cases because it
//! keeps NO record of the server hash it last synced each file to.
//!
//! This store IS that record. On every successful materialize (pull) and every
//! accepted push, we `record(path, server_canonical_hash)`. At reconcile time
//! the direction decision (`verify_repair::decide_direction`) reads it:
//!
//! * shadow == current server hash  ⇒ server hasn't moved since we synced ⇒ a
//!   local≠server diff can only be a genuine local edit ⇒ PUSH.
//! * shadow absent, or shadow ≠ current server hash ⇒ the server moved since we
//!   synced ⇒ our local is stale ⇒ PULL (server-wins overwrite).
//!
//! ## Persistence
//!
//! Backed by a flat JSON `HashMap<path, server_hash>` on disk. Writes are
//! dirty-gated (no I/O when nothing changed) and atomic (tmp+rename). A missing
//! OR corrupt file loads as EMPTY — never a panic; a corrupt shadow simply
//! degrades to "no marker" (pull-on-drift), which is the safe default for a
//! mirror host. A periodic 30s flush keeps the on-disk copy fresh without
//! coupling to any single write path.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tracing::warn;

use crate::state_io::{Commit, NotCommitted, StateDir, StateFile};
use unicode_normalization::UnicodeNormalization;

/// Periodic flush cadence for [`ShadowStore::spawn_periodic_flush`].
const FLUSH_INTERVAL: Duration = Duration::from_secs(30);

/// D8 (S511, TKT-2dc9a17e): canonicalize a sync path to ONE fleet-wide form.
///
/// The data-loss amplifier: macOS persists filenames in NFD (decomposed),
/// while ext4/NTFS store the bytes verbatim and the server's canonical form is
/// NFC (precomposed). A non-ASCII note name therefore keys the shadow store
/// differently across hosts, so `shadow.get()` misses, the reconcile backstop
/// reads "shadow absent", treats the local copy as stale, and pulls-over a
/// genuine local edit. Routing every KEY (shadow record/get, the reconcile
/// manifest path, the push wire path) through this function makes the lookup
/// normalization-invariant. We deliberately key in NFC but still WRITE to the
/// platform-native on-disk path (NFD on macOS) so we never create a duplicate
/// decomposed/precomposed file on disk; only the in-memory/wire KEY is NFC.
///
/// Also folds backslash to forward-slash so a Windows-origin path and a
/// Unix-origin path for the same note collapse to one key. No em-dashes here
/// or anywhere (house rule).
pub fn canonical_sync_path(s: &str) -> String {
    s.nfc().collect::<String>().replace('\\', "/")
}

/// Unit 1a S1/S2 (TKT-b25fce06): generation-tracked durable flush, shared
/// identically by [`ShadowStore`], `BaseSeqStore` and `ReadReceiptStore`.
///
/// * `mut_gen` is bumped by every mutation AFTER the map change and while the
///   map mutex is still held (call [`FlushGen::bump`] inside the lock).
/// * `flushed_gen` is the generation last durably written.
/// * `flush_lock` is taken BEFORE the snapshot and held through write, temp
///   `sync_all`, persist (rename), directory fsync and `fetch_max` (C2), so an
///   older snapshot can never finish last and rename over a newer one.
///   Mutations never take it, so they are not blocked by flush I/O.
pub(crate) struct FlushGen {
    mut_gen: AtomicU64,
    flushed_gen: AtomicU64,
    flush_lock: Mutex<()>,
    #[cfg(test)]
    pub(crate) hooks: FlushHooks,
}

/// Test-only fault and interleaving points for [`FlushGen::flush_durable`].
#[cfg(test)]
#[derive(Default)]
pub(crate) struct FlushHooks {
    /// One-shot callback run right after the snapshot is taken (T-S1, T-S3).
    #[allow(clippy::type_complexity)]
    pub(crate) after_snapshot: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    /// Make the temp-file `sync_all` fail (T-S2).
    pub(crate) fail_sync: AtomicBool,
}

impl FlushGen {
    /// `migrated` = the load-time migration changed the map, which counts as
    /// one mutation (it used to set `dirty`).
    pub(crate) fn new(migrated: bool) -> Self {
        FlushGen {
            mut_gen: AtomicU64::new(u64::from(migrated)),
            flushed_gen: AtomicU64::new(0),
            flush_lock: Mutex::new(()),
            #[cfg(test)]
            hooks: FlushHooks::default(),
        }
    }

    /// Record one mutation. MUST be called with the map mutex held, after the
    /// map change.
    pub(crate) fn bump(&self) {
        self.mut_gen.fetch_add(1, Ordering::SeqCst);
    }

    /// Current mutation generation. Read it under the map mutex when taking
    /// a snapshot so the generation and the snapshot agree.
    pub(crate) fn current(&self) -> u64 {
        self.mut_gen.load(Ordering::SeqCst)
    }

    /// True iff a mutation has not yet been durably flushed.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn is_dirty(&self) -> bool {
        self.mut_gen.load(Ordering::SeqCst) != self.flushed_gen.load(Ordering::SeqCst)
    }

    #[cfg(test)]
    pub(crate) fn flushed(&self) -> u64 {
        self.flushed_gen.load(Ordering::SeqCst)
    }

    /// Durably persist a snapshot as `file` in the held state dir (W5-W7,
    /// CS-2): `StateDir::replace_atomic`, i.e. CS-1 `durable_file` over the
    /// StateDir anchor (unique 0600 temp, `sync_all`, `renameat`, dir sync).
    /// `snapshot` must lock the map, read [`Self::current`] and clone the map
    /// under that lock, and return both. No-op (`Ok(Durable)`) when nothing
    /// changed since the last durable flush.
    ///
    /// Outcome mapping (CS-2 §3.4): `Durable` and
    /// `CommittedDurabilityUncertain` both mean the bytes are visible, so
    /// both advance `flushed_gen` (the StateDir sets `dir_sync_pending` and
    /// raises `durable_write_uncertain` on the latter); `NotCommitted` keeps
    /// the 1a behaviour (generation stays, the next tick retries).
    pub(crate) fn flush_durable<T, F>(
        &self,
        sd: &StateDir,
        file: StateFile,
        snapshot: F,
    ) -> Result<Commit, NotCommitted>
    where
        T: serde::Serialize,
        F: FnOnce() -> (u64, T),
    {
        // Fast path: nothing unflushed. A mutation racing this read lands
        // after this call and is picked up by the next flush.
        if !self.is_dirty() {
            return Ok(Commit::Durable);
        }
        // C2: held from BEFORE the snapshot through the fetch_max below.
        let _flush = self.flush_lock.lock().unwrap_or_else(|p| p.into_inner());
        let (g, snap) = snapshot();
        #[cfg(test)]
        self.hooks.run_after_snapshot();
        if g == self.flushed_gen.load(Ordering::SeqCst) {
            return Ok(Commit::Durable);
        }
        let bytes =
            serde_json::to_vec(&snap).map_err(|e| NotCommitted(std::io::Error::other(e)))?;
        // T-S2 fault point: the temp `sync_all` now runs inside durable_file.
        #[cfg(test)]
        if self.hooks.fail_sync.load(Ordering::SeqCst) {
            crate::durable_file::seams::fail_sync_all(5);
        }
        let c = sd.replace_atomic(file, &bytes)?;
        // S2: only the generation that was actually written is marked
        // flushed. A mutation after the snapshot has mut_gen > g, so the next
        // flush still writes it.
        self.flushed_gen.fetch_max(g, Ordering::SeqCst);
        Ok(c)
    }
}

#[cfg(test)]
impl FlushHooks {
    fn run_after_snapshot(&self) {
        let hook = self.after_snapshot.lock().unwrap().take();
        if let Some(h) = hook {
            h();
        }
    }
}

/// Shared T-S1..T-S3 checks (Unit 1a §3), run against each of the three
/// stores from their own test modules.
#[cfg(test)]
#[allow(clippy::disallowed_methods)]
pub(crate) mod flush_gen_checks {
    use super::FlushGen;
    use std::path::{Path, PathBuf};
    use std::sync::{mpsc, Arc, Mutex};
    use std::thread::JoinHandle;
    use std::time::Duration;
    use tempfile::TempDir;

    /// A store under test. `put(i)` records entry `i`; `has(i)` checks it.
    pub(crate) trait GenStore: Send + Sync + 'static {
        fn load_at(path: PathBuf) -> Arc<Self>;
        fn put(&self, i: u32);
        fn has(&self, i: u32) -> bool;
        fn flush_now(&self) -> Result<crate::state_io::Commit, crate::state_io::NotCommitted>;
        fn gen(&self) -> &FlushGen;
        /// The store's fixed state file (CS-2).
        const FILE: crate::state_io::StateFile;
    }

    fn dir_entries(dir: &Path) -> Vec<String> {
        let mut v: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        v.sort();
        v
    }

    /// T-S1: a mutation landing between the snapshot and the generation
    /// update is persisted by the NEXT flush.
    pub(crate) fn t_s1<S: GenStore>() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(S::FILE.name());
        let s = S::load_at(path.clone());
        s.put(1);
        let s2 = Arc::clone(&s);
        *s.gen().hooks.after_snapshot.lock().unwrap() = Some(Box::new(move || s2.put(2)));
        let _c = s.flush_now().unwrap();
        assert!(
            s.gen().is_dirty(),
            "the post-snapshot mutation must leave the store dirty"
        );
        let _c = s.flush_now().unwrap();
        let reloaded = S::load_at(path);
        assert!(reloaded.has(1), "entry 1 persisted");
        assert!(
            reloaded.has(2),
            "the mutation injected after the snapshot must be persisted by the next flush"
        );
    }

    /// T-S2: an injected temp `sync_all` error propagates, writes nothing,
    /// leaves no temp file and leaves the generation unflushed.
    pub(crate) fn t_s2<S: GenStore>() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(S::FILE.name());
        let s = S::load_at(path.clone());
        s.put(1);
        s.gen()
            .hooks
            .fail_sync
            .store(true, std::sync::atomic::Ordering::SeqCst);
        assert!(s.flush_now().is_err(), "sync_all failure must propagate");
        assert!(s.gen().is_dirty(), "generation must stay unflushed");
        assert_eq!(s.gen().flushed(), 0);
        assert!(!path.exists(), "nothing may be persisted");
        assert!(
            dir_entries(dir.path()).is_empty(),
            "temp file must be removed: {:?}",
            dir_entries(dir.path())
        );
        s.gen()
            .hooks
            .fail_sync
            .store(false, std::sync::atomic::Ordering::SeqCst);
        let _c = s.flush_now().unwrap();
        assert!(S::load_at(path).has(1));
    }

    /// T-S3 (C2): flush A snapshots, a mutation lands, flush B is started and
    /// given every chance to snapshot and finish before A continues. The
    /// NEWER snapshot must end up on disk and `flushed_gen` must equal B's
    /// generation.
    pub(crate) fn t_s3<S: GenStore>() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(S::FILE.name());
        let s = S::load_at(path.clone());
        s.put(1);
        let s2 = Arc::clone(&s);
        type FlushJoin = JoinHandle<Result<(), crate::state_io::NotCommitted>>;
        let b_slot: Arc<Mutex<Option<FlushJoin>>> = Arc::new(Mutex::new(None));
        let b_slot2 = Arc::clone(&b_slot);
        *s.gen().hooks.after_snapshot.lock().unwrap() = Some(Box::new(move || {
            // A has snapshotted. A mutation lands, then flush B runs.
            s2.put(2);
            let (tx, rx) = mpsc::channel();
            let s3 = Arc::clone(&s2);
            let h = std::thread::spawn(move || {
                let r = s3.flush_now().map(|_| ());
                let _ = tx.send(());
                r
            });
            // If flush_lock does not span A's snapshot..commit, B snapshots,
            // writes and finishes inside this window, before A resumes.
            let _ = rx.recv_timeout(Duration::from_millis(500));
            *b_slot2.lock().unwrap() = Some(h);
        }));
        let _c = s.flush_now().unwrap(); // flush A
        let b = b_slot.lock().unwrap().take().expect("flush B was started");
        b.join().unwrap().unwrap();
        let reloaded = S::load_at(path);
        assert!(reloaded.has(1));
        assert!(
            reloaded.has(2),
            "the NEWER snapshot (with entry 2) must be on disk, not A's older one"
        );
        assert_eq!(
            s.gen().flushed(),
            s.gen().current(),
            "flushed_gen must equal B's generation"
        );
        assert!(!s.gen().is_dirty());
    }
}

/// Persistent per-file shadow-hash store: path → last-synced server hash.
pub struct ShadowStore {
    inner: Mutex<HashMap<String, String>>,
    /// CS-2: the held state dir; the file is `StateFile::Shadow`.
    sd: Arc<StateDir>,
    /// Unit 1a S1/S2: mutation / durable-flush generations (replaces `dirty`).
    gen: FlushGen,
    /// B2' (TKT-86ae42a3, 2026-07-18 conflict storm): the vault folder names
    /// (sync-root basenames, NFC) whose prefix must be STRIPPED off keys.
    /// Before v0.4.28 every pipeline keyed this store with vaults-root-relative
    /// paths (`Mainframe/01_Notes/x.md`); B2 (v0.4.28) moved every pipeline to
    /// sync-root-relative paths (`01_Notes/x.md`) but never migrated the keys,
    /// so the reconcile leg read `shadow absent` for the ENTIRE pre-B2 sync
    /// history and R5-stashed a conflict copy per divergent path (2,395 mints
    /// on link on 07-18 alone). Canonical key form is sync-root-relative.
    vault_folders: Vec<String>,
    /// R7 (TKT-166e1c07, 2026-07-18 trinity incident): set at load when
    /// `vault_folders` resolved EMPTY but the store holds vault-prefixed-looking
    /// keys. In that state the prefix strip is a silent no-op and a push/migration
    /// would mass-mis-key and re-push the vault. Consumers (the push pipeline)
    /// read this and PARK rather than proceed. See [`detect_vault_scope_suspect`].
    vault_scope_suspect: AtomicBool,
    /// R2 (TKT-406f6cb3): the RECEIVED-HASH LEDGER. In-memory ONLY (never
    /// flushed, never read by reconcile). `path -> sha` is present iff this
    /// process KNOWS the local file holds exactly the bytes the server holds:
    /// set by the materializer right after it wrote (or verified identical)
    /// the server's bytes, cleared the moment anything may make that untrue
    /// (a push leaving the host, any delete, a soft-delete). The push client's
    /// drain gate refuses to send bytes equal to it - an echo of what was
    /// just received. Deliberately NOT the shadow: the shadow is kept stale
    /// on purpose on several paths (failed ack-align rewrite, non-accepted
    /// delete) and lags the server, so a shadow-keyed gate swallows genuine
    /// reverts. Lost on restart => the gate fails OPEN (an idempotent push).
    received: Mutex<HashMap<String, ReceivedMark>>,
}

/// R2 (TKT-406f6cb3): one received-ledger entry. `gen` is bumped by every
/// clear and every materializer note (review M-B).
#[derive(Debug, Default)]
struct ReceivedMark {
    sha: Option<String>,
    gen: u64,
}

/// R7 (TKT-166e1c07, 2026-07-18 trinity incident): detect the misconfiguration
/// where `vault_folders` resolves EMPTY (e.g. `config.toml` missing `vault_name`,
/// collapsing the sync root to `vaults_root` with no vault segment) while the
/// shadow store still holds vault-prefixed-looking keys. In that state the
/// prefix-strip is a silent no-op, so a migration/push mis-keys EN MASSE and the
/// B2 migration no-ops -> the 2,249-note mass push. Returns the count of
/// prefixed-looking keys when the scope is suspect, else `None`.
///
/// Definition: suspect iff `vault_folders` is empty AND at least one key looks
/// vault-prefixed (carries a leading path segment, i.e. contains `/`). In normal
/// operation `vault_folders` is NEVER empty (there is always a sync-root
/// basename), so this fires ONLY in the misconfiguration state; a genuinely flat
/// vault (keys with no `/`) never trips it.
pub fn detect_vault_scope_suspect<'a>(
    vault_folders: &[String],
    keys: impl Iterator<Item = &'a str>,
) -> Option<usize> {
    if !vault_folders.is_empty() {
        return None;
    }
    let n = keys.filter(|k| k.contains('/')).count();
    if n > 0 {
        Some(n)
    } else {
        None
    }
}

impl ShadowStore {
    /// Load the store from `path` with NO vault-folder awareness (tests /
    /// callers that already pass canonical sync-root-relative keys).
    pub fn load(sd: &Arc<StateDir>) -> Arc<ShadowStore> {
        Self::load_with_vault_folders(sd, Vec::new())
    }

    /// Canonicalize a key: NFC + slash-fold (D8), then strip a leading
    /// `<vault_folder>/` segment if it names a known vault folder (B2').
    /// Makes record/get shape-invariant: a legacy prefixed caller and a
    /// current sync-root-relative caller hit the SAME entry.
    ///
    /// Known limitation (documented, safe): a note whose vault-relative path
    /// genuinely starts with a segment equal to the vault folder name
    /// (`<vault>/Mainframe/x.md`) aliases with `x.md`. A wrong alias degrades
    /// to the always-stash floor (one conflict copy), never silent loss.
    fn canon_key(&self, path: &str) -> String {
        let k = canonical_sync_path(path);
        if let Some((first, rest)) = k.split_once('/') {
            if !rest.is_empty() && self.vault_folders.iter().any(|f| f == first) {
                return rest.to_string();
            }
        }
        k
    }

    /// Load the store from `path`. A missing OR corrupt file starts EMPTY and
    /// logs a `warn!` — NEVER panics. The returned store is `dirty == false`
    /// (nothing to flush until something is recorded or a migration re-keyed).
    ///
    /// `vault_folders` are the sync-root basenames (e.g. `["Mainframe"]`).
    /// Legacy pre-v0.4.28 keys carrying that prefix are migrated to the
    /// canonical sync-root-relative form ON LOAD (one-time, B2'), exactly like
    /// the D8 NFC migration below — without it, the B2 path-shape cutover
    /// orphans the entire prior sync history and every dormant-but-divergent
    /// note falls to R5 conflict (the 07-15..07-18 conflict storm).
    pub fn load_with_vault_folders(
        sd: &Arc<StateDir>,
        vault_folders: Vec<String>,
    ) -> Arc<ShadowStore> {
        let raw = match sd.read(StateFile::Shadow) {
            Ok(Some(bytes)) => match serde_json::from_slice::<HashMap<String, String>>(&bytes) {
                Ok(m) => m,
                Err(e) => {
                    warn!(
                        file = StateFile::Shadow.name(),
                        error = %e,
                        "shadow store: corrupt JSON, starting EMPTY (degrades to pull-on-drift)"
                    );
                    HashMap::new()
                }
            },
            Ok(None) => HashMap::new(),
            Err(e) => {
                warn!(
                    file = StateFile::Shadow.name(),
                    error = %e,
                    "shadow store: read failed, starting EMPTY"
                );
                HashMap::new()
            }
        };
        // D8 (S511): one-time NFC key migration. Existing keys may be NFD
        // (macOS-origin) or backslash-form (Windows-origin). Without this, the
        // NFC cutover would make every non-ASCII key MISS on the first lookup,
        // mass-Pull, and trigger the exact data-loss event. We re-key on load
        // so the cutover is safe. dirty is set iff anything actually changed,
        // so a clean ASCII store does not gratuitously rewrite to disk. On a
        // key collision after normalization (an NFD and an NFC key for the same
        // note both present) we keep the existing value, leaving the residual
        // to converge via the always-stash path.
        let vault_folders: Vec<String> = vault_folders
            .into_iter()
            .map(|f| canonical_sync_path(&f))
            .filter(|f| !f.is_empty())
            .collect();
        // Two-phase migration so the collision policy is deterministic and
        // CURRENT-era values always win:
        //   1. Keys already in canonical form (NFC + no vault prefix) insert
        //      first — these were written by current (post-B2) code.
        //   2. Keys that re-key under migration (NFD → NFC, and/or a legacy
        //      `<vault>/` prefix stripped) fill remaining gaps via or_insert —
        //      a legacy value NEVER overwrites a current one. A stale shadow
        //      value can only degrade to the always-stash floor (one conflict
        //      copy), never to a silent overwrite, so gap-fill is safe.
        let strip = |k: &str| -> String {
            if let Some((first, rest)) = k.split_once('/') {
                if !rest.is_empty() && vault_folders.iter().any(|f| f == first) {
                    return rest.to_string();
                }
            }
            k.to_string()
        };
        let mut map: HashMap<String, String> = HashMap::with_capacity(raw.len());
        let mut legacy: Vec<(String, String)> = Vec::new();
        let mut migrated = false;
        for (k, v) in raw.into_iter() {
            let nk = strip(&canonical_sync_path(&k));
            if nk != k {
                migrated = true;
                legacy.push((nk, v));
            } else {
                map.insert(nk, v);
            }
        }
        let legacy_count = legacy.len();
        for (nk, v) in legacy {
            map.entry(nk).or_insert(v);
        }
        if migrated {
            warn!(
                file = StateFile::Shadow.name(),
                legacy_count,
                "shadow store: migrated keys to canonical form (NFC, S511 D8 + vault-prefix strip, B2' TKT-86ae42a3)"
            );
        }
        // R7 (TKT-166e1c07): flag the empty-vault_folders + prefixed-keys state so
        // the push pipeline parks instead of mass-re-pushing (2026-07-18 trinity
        // incident). WARN loudly at load; the actual refusal is at the push side.
        let suspect = detect_vault_scope_suspect(&vault_folders, map.keys().map(|s| s.as_str()));
        if let Some(prefixed_keys) = suspect {
            warn!(
                file = StateFile::Shadow.name(),
                prefixed_keys,
                "shadow store: vault_folders resolved EMPTY but the store holds vault-prefixed keys \
                 (config vault_name is likely missing). REFUSING migrations/pushes downstream (park) \
                 to avoid a mass re-push. Fix the config vault_name and restart (R7, 2026-07-18 trinity incident)."
            );
        }
        Arc::new(ShadowStore {
            inner: Mutex::new(map),
            sd: Arc::clone(sd),
            gen: FlushGen::new(migrated),
            vault_folders,
            vault_scope_suspect: AtomicBool::new(suspect.is_some()),
            received: Mutex::new(HashMap::new()),
        })
    }

    /// Upsert `path -> server_hash`. No I/O, bumps the mutation generation so the next
    /// `flush()` persists it. D8 (S511): the key is normalized to NFC canonical
    /// form so record/get are normalization-invariant, regardless of which OS
    /// (NFD macOS vs verbatim ext4/NTFS) produced the path.
    pub fn record(&self, path: &str, server_hash: &str) {
        let key = self.canon_key(path);
        if let Ok(mut m) = self.inner.lock() {
            m.insert(key, server_hash.to_string());
            self.gen.bump();
        }
    }

    /// R5 / F-B3.3 (TKT-989ad5f2): drop the recorded hash for `path`. Used on a
    /// terminal 409+404 (the server has no row) so the stale shadow does not
    /// keep re-arming a CAS-conflict push against a row that no longer exists.
    /// Bumps the mutation generation so the next `flush()` persists the removal. No-op if
    /// the key was absent. Key normalized to NFC like `record`/`get`.
    pub fn remove(&self, path: &str) {
        let key = self.canon_key(path);
        if let Ok(mut m) = self.inner.lock() {
            if m.remove(&key).is_some() {
                self.gen.bump();
            }
        }
    }

    /// R2 (TKT-406f6cb3): mark that the local file at `path` now holds exactly
    /// the server's bytes `sha` (see the `received` field). In-memory only.
    ///
    /// Bumps the path's generation: a materializer write supersedes whatever
    /// an in-flight push was about to conclude (review M-B).
    pub fn note_received(&self, path: &str, sha: &str) {
        let key = self.canon_key(path);
        if let Ok(mut m) = self.received.lock() {
            let e = m.entry(key).or_default();
            e.gen = e.gen.wrapping_add(1);
            e.sha = Some(sha.to_string());
        }
    }

    /// R2: forget the received mark for `path` (anything that may make "local
    /// == server" untrue: a push leaving the host, any delete). Bumps and
    /// returns the path's generation; a push keeps it to prove, when its
    /// response arrives, that nothing (a soft-delete, a materialize) touched
    /// the path in between (review M-B). A poisoned lock returns `None`.
    pub fn forget_received(&self, path: &str) -> Option<u64> {
        let key = self.canon_key(path);
        let mut m = self.received.lock().ok()?;
        let e = m.entry(key).or_default();
        e.gen = e.gen.wrapping_add(1);
        e.sha = None;
        Some(e.gen)
    }

    /// R2 review M-B: note `sha` as received ONLY if the path's generation is
    /// still `gen` (the value the caller's own `forget_received` returned).
    /// An intervening clear or materialize (e.g. an SSE soft_delete of the
    /// note racing our push's HTTP response) bumps the generation, so a stale
    /// response can never re-arm the gate for a file that is gone. Returns
    /// whether it noted.
    pub fn note_received_if_unchanged(&self, path: &str, sha: &str, gen: u64) -> bool {
        let key = self.canon_key(path);
        let Ok(mut m) = self.received.lock() else {
            return false;
        };
        match m.get_mut(&key) {
            Some(e) if e.gen == gen => {
                e.sha = Some(sha.to_string());
                true
            }
            _ => false,
        }
    }

    /// R2: the sha the local file is known to share with the server, if any.
    /// A poisoned lock reads as `None` (gate fails open).
    pub fn received(&self, path: &str) -> Option<String> {
        let key = self.canon_key(path);
        self.received
            .lock()
            .ok()
            .and_then(|m| m.get(&key).and_then(|e| e.sha.clone()))
    }

    /// The last-synced server hash recorded for `path`, if any. D8 (S511): the
    /// lookup key is normalized to NFC so a get always hits the record() that
    /// stored it, even across an NFD/NFC OS boundary.
    pub fn get(&self, path: &str) -> Option<String> {
        let key = self.canon_key(path);
        self.inner.lock().ok().and_then(|m| m.get(&key).cloned())
    }

    /// Number of recorded entries. D9 (S511): the startup shadow-wipe fast-path
    /// uses this to detect an empty/wiped shadow (fresh install, corrupt-load
    /// reset, manual delete) so it can seed shadow=server BEFORE the first
    /// reconcile decision and avoid a conflict-copy avalanche.
    pub fn len(&self) -> usize {
        self.inner.lock().map(|m| m.len()).unwrap_or(0)
    }

    /// True iff the store has no recorded entries (see [`Self::len`]).
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// R7 (TKT-166e1c07): true when this store loaded in the suspect state
    /// (`vault_folders` empty but vault-prefixed keys present). The push
    /// pipeline reads this and PARKS (refuses to drain/push) so a config with a
    /// missing `vault_name` cannot mass-re-push the vault (2026-07-18 trinity
    /// incident). See [`detect_vault_scope_suspect`].
    pub fn vault_scope_suspect(&self) -> bool {
        self.vault_scope_suspect.load(Ordering::Relaxed)
    }

    /// Persist the full map to `self.path` durably (tmp in the destination
    /// dir, `sync_all`, rename, dir fsync). No-op (and `Ok`) when nothing
    /// changed since the last durable flush. Creates parent dirs as needed.
    /// A mutation that lands after the snapshot stays unflushed and is written
    /// by the next flush (Unit 1a S2); see [`FlushGen`].
    pub fn flush(&self) -> Result<Commit, NotCommitted> {
        self.gen.flush_durable(&self.sd, StateFile::Shadow, || {
            let m = match self.inner.lock() {
                Ok(m) => m,
                Err(poisoned) => poisoned.into_inner(),
            };
            (self.gen.current(), m.clone())
        })
    }

    /// Test access to the flush generations (T-CS2-19).
    #[cfg(test)]
    #[cfg_attr(not(unix), allow(dead_code))] // used by unix-only tests
    pub(crate) fn test_gen(&self) -> &FlushGen {
        &self.gen
    }

    /// Spawn a background loop that flushes every 30s for the process lifetime.
    /// The immediate first `interval` tick is consumed so we don't flush at t=0
    /// (nothing recorded yet). A flush error is logged and the loop continues.
    /// CS-3: returns its `JoinHandle` for the supervisor (class Required).
    pub fn spawn_periodic_flush(
        self: Arc<Self>,
    ) -> tauri::async_runtime::JoinHandle<crate::supervise::TaskEnd> {
        tauri::async_runtime::spawn(async move {
            let mut tick = tokio::time::interval(crate::supervise::tick(FLUSH_INTERVAL));
            tick.tick().await; // consume the immediate first tick
            loop {
                tick.tick().await;
                #[cfg(test)]
                crate::faults::point("flush_shadow");
                if let Err(e) = self.flush() {
                    warn!(error = %e, "shadow store: periodic flush failed (will retry)");
                }
            }
        })
    }
}

#[cfg(test)]
#[allow(clippy::disallowed_methods)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use tempfile::TempDir;

    #[test]
    fn load_empty_when_file_missing() {
        let dir = TempDir::new().unwrap();
        let p = dir.path().join("does-not-exist.json");
        let store = ShadowStore::load(&crate::state_io::StateDir::for_test_file(&p));
        assert_eq!(store.get("anything.md"), None);
    }

    #[test]
    fn record_then_get() {
        let dir = TempDir::new().unwrap();
        let store = ShadowStore::load(&crate::state_io::StateDir::for_test_file(
            &dir.path().join("shadow_hashes.json"),
        ));
        store.record("notes/a.md", "hash-aaa");
        assert_eq!(store.get("notes/a.md"), Some("hash-aaa".to_string()));
        // upsert overwrites
        store.record("notes/a.md", "hash-bbb");
        assert_eq!(store.get("notes/a.md"), Some("hash-bbb".to_string()));
        assert_eq!(store.get("notes/missing.md"), None);
    }

    #[test]
    fn persist_then_reload_round_trip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("sub").join("shadow_hashes.json"); // parent dir created
        {
            let store = ShadowStore::load(&crate::state_io::StateDir::for_test_file(&path.clone()));
            store.record("notes/a.md", "h-a");
            store.record("notes/b.md", "h-b");
            let _c = store.flush().unwrap();
        }
        // Fresh load sees the persisted entries.
        let reloaded = ShadowStore::load(&crate::state_io::StateDir::for_test_file(&path));
        assert_eq!(reloaded.get("notes/a.md"), Some("h-a".to_string()));
        assert_eq!(reloaded.get("notes/b.md"), Some("h-b".to_string()));
    }

    #[test]
    fn corrupt_file_loads_empty_no_panic() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("shadow_hashes.json");
        std::fs::write(&path, b"{{not json").unwrap();
        let store = ShadowStore::load(&crate::state_io::StateDir::for_test_file(&path));
        assert_eq!(store.get("notes/a.md"), None);
        // still usable after a corrupt load
        store.record("notes/a.md", "recovered");
        assert_eq!(store.get("notes/a.md"), Some("recovered".to_string()));
    }

    #[test]
    fn flush_is_noop_when_not_dirty() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("shadow_hashes.json");
        let store = ShadowStore::load(&crate::state_io::StateDir::for_test_file(&path.clone()));
        // Fresh load is not dirty → flush writes nothing and is Ok.
        assert!(store.flush().is_ok());
        assert!(!path.exists(), "non-dirty flush must NOT create the file");
    }

    #[test]
    fn flush_clears_dirty_then_second_flush_is_noop() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("shadow_hashes.json");
        let store = ShadowStore::load(&crate::state_io::StateDir::for_test_file(&path.clone()));
        store.record("a.md", "h");
        let _c = store.flush().unwrap();
        assert!(path.exists());
        // Remove the file; a second (non-dirty) flush must NOT recreate it.
        std::fs::remove_file(&path).unwrap();
        let _c = store.flush().unwrap();
        assert!(
            !path.exists(),
            "second flush after a clean flush must be a no-op"
        );
    }

    // ---- D8 (S511): NFC canonicalization + key migration ----

    /// canonical_sync_path collapses NFD (decomposed) to NFC (precomposed) and
    /// folds backslashes to forward slashes. The cafe-with-accent case is the
    /// canonical cross-OS amplifier: macOS writes "e + combining acute"; the
    /// server/ext4/NTFS store "e-acute" precomposed. Both MUST map to one key.
    #[test]
    fn canonical_sync_path_nfc_round_trip() {
        let nfc = "Notes/Cafe\u{0301}.md".nfc().collect::<String>(); // precomposed cafe-acute
        let nfd = "Notes/Cafe\u{0301}.md".nfd().collect::<String>(); // decomposed
        assert_ne!(
            nfc, nfd,
            "NFD and NFC byte forms must differ for the test to matter"
        );
        assert_eq!(
            canonical_sync_path(&nfc),
            canonical_sync_path(&nfd),
            "NFD input must canonicalize to the same key as NFC input"
        );
    }

    #[test]
    fn canonical_sync_path_folds_backslashes() {
        assert_eq!(canonical_sync_path("Notes\\sub\\x.md"), "Notes/sub/x.md");
    }

    /// record(NFD key) then get(NFC key) must hit: the store is
    /// normalization-invariant on both write and read.
    #[test]
    fn shadow_record_get_normalization_invariant() {
        let dir = TempDir::new().unwrap();
        let store = ShadowStore::load(&crate::state_io::StateDir::for_test_file(
            &dir.path().join("shadow_hashes.json"),
        ));
        let nfd = "Notes/Cafe\u{0301}.md".nfd().collect::<String>();
        let nfc = "Notes/Cafe\u{0301}.md".nfc().collect::<String>();
        store.record(&nfd, "hash-nfd");
        // Reading the precomposed (NFC) form must hit the NFD-recorded entry.
        assert_eq!(store.get(&nfc), Some("hash-nfd".to_string()));
        // And re-recording under the NFC key overwrites the SAME entry.
        store.record(&nfc, "hash-nfc");
        assert_eq!(store.get(&nfd), Some("hash-nfc".to_string()));
    }

    // ---- B2' (TKT-86ae42a3): vault-prefix key migration + shape invariance ----

    /// THE 07-18 conflict-storm regression. A pre-v0.4.28 store keyed
    /// `Mainframe/01_Notes/x.md`; post-B2 pipelines look up `01_Notes/x.md`.
    /// Without the prefix migration the lookup misses, the reconcile leg reads
    /// "shadow absent", and R5 mints a conflict copy per divergent path.
    /// This test FAILS on pre-fix code (get returns None).
    #[test]
    fn load_migrates_vault_prefixed_keys_to_sync_root_relative() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("shadow_hashes.json");
        let mut m = HashMap::new();
        m.insert(
            "Mainframe/01_Notes/legacy.md".to_string(),
            "h-legacy".to_string(),
        );
        m.insert("01_Notes/current.md".to_string(), "h-current".to_string());
        std::fs::write(&path, serde_json::to_vec(&m).unwrap()).unwrap();

        let store = ShadowStore::load_with_vault_folders(
            &crate::state_io::StateDir::for_test_file(&path),
            vec!["Mainframe".into()],
        );
        assert_eq!(
            store.get("01_Notes/legacy.md"),
            Some("h-legacy".to_string()),
            "pre-B2 prefixed key must be readable via the current sync-root-relative shape"
        );
        assert_eq!(
            store.get("01_Notes/current.md"),
            Some("h-current".to_string())
        );
    }

    /// Collision policy: when BOTH namespaces hold the same note, the
    /// current-era (unprefixed) value wins — legacy gap-fills only.
    #[test]
    fn vault_prefix_migration_current_value_wins_on_collision() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("shadow_hashes.json");
        let mut m = HashMap::new();
        m.insert("Mainframe/x.md".to_string(), "h-legacy".to_string());
        m.insert("x.md".to_string(), "h-current".to_string());
        std::fs::write(&path, serde_json::to_vec(&m).unwrap()).unwrap();

        let store = ShadowStore::load_with_vault_folders(
            &crate::state_io::StateDir::for_test_file(&path),
            vec!["Mainframe".into()],
        );
        assert_eq!(store.get("x.md"), Some("h-current".to_string()));
        assert_eq!(
            store.get("Mainframe/x.md"),
            Some("h-current".to_string()),
            "prefixed lookup must alias to the same canonical entry"
        );
    }

    /// Runtime shape-invariance: record under one shape, get under the other.
    #[test]
    fn record_get_invariant_across_vault_prefix_shapes() {
        let dir = TempDir::new().unwrap();
        let store = ShadowStore::load_with_vault_folders(
            &crate::state_io::StateDir::for_test_file(&dir.path().join("shadow_hashes.json")),
            vec!["Mainframe".into()],
        );
        store.record("Mainframe/01_Notes/y.md", "h1");
        assert_eq!(store.get("01_Notes/y.md"), Some("h1".to_string()));
        store.record("01_Notes/y.md", "h2");
        assert_eq!(store.get("Mainframe/01_Notes/y.md"), Some("h2".to_string()));
    }

    /// Without vault_folders (plain load), behavior is unchanged: prefixed
    /// keys stay prefixed (no accidental stripping when folders are unknown).
    #[test]
    fn plain_load_leaves_prefixed_keys_untouched() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("shadow_hashes.json");
        let mut m = HashMap::new();
        m.insert("Mainframe/x.md".to_string(), "h".to_string());
        std::fs::write(&path, serde_json::to_vec(&m).unwrap()).unwrap();
        let store = ShadowStore::load(&crate::state_io::StateDir::for_test_file(&path));
        assert_eq!(store.get("Mainframe/x.md"), Some("h".to_string()));
        assert_eq!(store.get("x.md"), None);
    }

    /// On load, an existing NFD key is migrated to NFC so the cutover does not
    /// mass-miss. The migrated store reads dirty (so the re-keyed map persists).
    #[test]
    fn load_migrates_nfd_keys_to_nfc() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("shadow_hashes.json");
        let nfd = "Notes/Cafe\u{0301}.md".nfd().collect::<String>();
        let nfc = "Notes/Cafe\u{0301}.md".nfc().collect::<String>();
        // Hand-write a store file with a decomposed key (simulating a
        // macOS-origin shadow from an older daemon).
        let mut m = HashMap::new();
        m.insert(nfd.clone(), "h".to_string());
        std::fs::write(&path, serde_json::to_vec(&m).unwrap()).unwrap();

        let store = ShadowStore::load(&crate::state_io::StateDir::for_test_file(&path));
        // The NFC lookup hits the migrated key.
        assert_eq!(store.get(&nfc), Some("h".to_string()));
    }

    // --- R7 empty-vault_folders guard (TKT-166e1c07, 2026-07-18 trinity) ---

    /// R7: the pure detector fires ONLY when vault_folders is empty AND the
    /// store holds vault-prefixed-looking keys; a flat vault or a configured
    /// vault_folders never trips it.
    #[test]
    fn detect_vault_scope_suspect_matrix() {
        let prefixed = ["Mainframe/01_Notes/x.md", "Mainframe/y.md"];
        let flat = ["x.md", "y.md"];
        // Empty folders + prefixed keys -> suspect (count of prefixed keys).
        assert_eq!(
            detect_vault_scope_suspect(&[], prefixed.iter().copied()),
            Some(2)
        );
        // Empty folders + flat keys -> not suspect (a genuinely flat vault).
        assert_eq!(detect_vault_scope_suspect(&[], flat.iter().copied()), None);
        // Configured folders -> never suspect (normal operation).
        assert_eq!(
            detect_vault_scope_suspect(&["Mainframe".to_string()], prefixed.iter().copied()),
            None
        );
        // Empty folders + empty store -> not suspect (nothing to protect).
        assert_eq!(
            detect_vault_scope_suspect(&[], std::iter::empty::<&str>()),
            None
        );
    }

    /// R7: a shadow store that loads with EMPTY vault_folders but holds
    /// vault-prefixed keys reports vault_scope_suspect() == true (so the push
    /// pipeline parks); the same store with vault_folders configured does not.
    #[test]
    fn shadow_store_flags_and_clears_suspect_scope() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("shadow_hashes.json");
        let mut m = HashMap::new();
        m.insert("Mainframe/01_Notes/x.md".to_string(), "h".to_string());
        std::fs::write(&path, serde_json::to_vec(&m).unwrap()).unwrap();

        // Empty vault_folders + prefixed key -> suspect.
        let suspect = ShadowStore::load_with_vault_folders(
            &crate::state_io::StateDir::for_test_file(&path.clone()),
            vec![],
        );
        assert!(suspect.vault_scope_suspect());

        // Same file, vault_folders configured -> prefix strips, not suspect.
        let ok = ShadowStore::load_with_vault_folders(
            &crate::state_io::StateDir::for_test_file(&path),
            vec!["Mainframe".into()],
        );
        assert!(!ok.vault_scope_suspect());
    }

    // ---- Unit 1a (TKT-b25fce06) §3: T-S1..T-S3 for ShadowStore ----

    impl super::flush_gen_checks::GenStore for ShadowStore {
        fn load_at(path: PathBuf) -> Arc<Self> {
            ShadowStore::load(&crate::state_io::StateDir::for_test_file(&path))
        }
        fn put(&self, i: u32) {
            self.record(&format!("n/{i}.md"), &format!("h{i}"));
        }
        fn has(&self, i: u32) -> bool {
            self.get(&format!("n/{i}.md")) == Some(format!("h{i}"))
        }
        const FILE: crate::state_io::StateFile = crate::state_io::StateFile::Shadow;
        fn flush_now(&self) -> Result<crate::state_io::Commit, crate::state_io::NotCommitted> {
            self.flush()
        }
        fn gen(&self) -> &FlushGen {
            &self.gen
        }
    }

    #[test]
    fn ts1_shadow_mutation_after_snapshot_is_persisted_by_next_flush() {
        super::flush_gen_checks::t_s1::<ShadowStore>();
    }

    #[test]
    fn ts2_shadow_sync_error_propagates_and_stays_unflushed() {
        super::flush_gen_checks::t_s2::<ShadowStore>();
    }

    #[test]
    fn ts3_shadow_concurrent_flushes_leave_newer_snapshot() {
        super::flush_gen_checks::t_s3::<ShadowStore>();
    }

    /// The load-time migration counts as a mutation (it used to set dirty).
    #[test]
    fn shadow_migration_bumps_generation() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("shadow_hashes.json");
        let mut m = HashMap::new();
        m.insert("Mainframe/x.md".to_string(), "h".to_string());
        std::fs::write(&path, serde_json::to_vec(&m).unwrap()).unwrap();
        let store = ShadowStore::load_with_vault_folders(
            &crate::state_io::StateDir::for_test_file(&path.clone()),
            vec!["Mainframe".into()],
        );
        assert!(store.gen.is_dirty());
        let _c = store.flush().unwrap();
        assert!(!store.gen.is_dirty());
        let raw: HashMap<String, String> =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert!(raw.contains_key("x.md"));
    }
}
