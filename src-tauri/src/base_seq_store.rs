//! Persistent per-file base_seq store: the daemon leg of the R7b causal gate
//! (THESEUS AR-002, TKT-166e1c07). Records, per note, the `change_seq` of the
//! server version this daemon last OBSERVED (byte-verified materialized or
//! pushed-and-accepted). That token is the proof-of-observation the daemon
//! declares on every push/delete so the server causal gate
//! (`causal_gate.py::causal_gate_decision`) can fail closed on an
//! unknown/stale/forged lineage instead of trusting a `base_hash` a stale
//! client can forge.
//!
//! ## Why a SEPARATE store (not a field on ShadowStore)
//!
//! The shadow store (`sync_shadow::ShadowStore`) is a flat
//! `HashMap<path, server_hash>` whose on-disk JSON format and storm-fix
//! migration logic are load-bearing and heavily tested. Rather than widen that
//! value type (a blast-radius change across every reconcile/pull/push site),
//! this store is an ADDITIVE parallel map `HashMap<path, i64>` persisted to a
//! sibling file. It reuses the SAME key canonicalization
//! (`sync_shadow::canonical_sync_path` + vault-folder-prefix strip) so a
//! base_seq entry and its shadow-hash twin always key identically.
//!
//! ## Fail-closed by construction
//!
//! `get()` returns `None` when nothing is recorded. `None` is the honest
//! "unknown/empty lineage" signal: the push path sends `base_seq: null`, the
//! server (flag on) fails the causal gate closed (409), and the daemon takes
//! the refetch/merge path (R2/R4). We NEVER fabricate or default a seq.
//!
//! ## Persistence
//!
//! Backed by a flat JSON `HashMap<path, i64>` on disk, dirty-gated + atomic
//! (tmp+rename), exactly like the shadow store. A missing OR corrupt file loads
//! as EMPTY (never a panic) which simply means "no lineage known yet" for every
//! note: fail-closed, refetch/merge on the first push under the flag.

use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tempfile::NamedTempFile;
use tracing::warn;

use crate::sync_shadow::canonical_sync_path;

/// Periodic flush cadence, matching the shadow store.
const FLUSH_INTERVAL: Duration = Duration::from_secs(30);

/// Persistent per-file observed-`change_seq` store: path -> last-observed
/// server `change_seq` (proof-of-observation for the R7b causal gate).
pub struct BaseSeqStore {
    inner: Mutex<HashMap<String, i64>>,
    path: PathBuf,
    dirty: AtomicBool,
    /// Sync-root basenames whose leading `<vault_folder>/` prefix is stripped
    /// off keys, identical to `ShadowStore` so the two stores key in lockstep.
    vault_folders: Vec<String>,
}

impl BaseSeqStore {
    /// Load with NO vault-folder awareness (tests / callers passing canonical
    /// sync-root-relative keys).
    pub fn load(path: PathBuf) -> Arc<BaseSeqStore> {
        Self::load_with_vault_folders(path, Vec::new())
    }

    /// Canonicalize a key: NFC + slash-fold, then strip a leading
    /// `<vault_folder>/` segment if it names a known vault folder. Keeps
    /// record/get shape-invariant and identical to the shadow store's keying.
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
    /// logs a `warn!` (never panics). An empty store means "no lineage known"
    /// for every note, which is the fail-closed default (refetch/merge on the
    /// first push under the flag). Legacy prefixed keys are migrated to the
    /// canonical sync-root-relative form on load, mirroring the shadow store.
    pub fn load_with_vault_folders(path: PathBuf, vault_folders: Vec<String>) -> Arc<BaseSeqStore> {
        let raw = match std::fs::read(&path) {
            Ok(bytes) => match serde_json::from_slice::<HashMap<String, i64>>(&bytes) {
                Ok(m) => m,
                Err(e) => {
                    warn!(
                        path = %path.display(),
                        error = %e,
                        "base_seq store: corrupt JSON, starting EMPTY (fail-closed: refetch/merge)"
                    );
                    HashMap::new()
                }
            },
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => HashMap::new(),
            Err(e) => {
                warn!(
                    path = %path.display(),
                    error = %e,
                    "base_seq store: read failed, starting EMPTY (fail-closed)"
                );
                HashMap::new()
            }
        };
        let vault_folders: Vec<String> = vault_folders
            .into_iter()
            .map(|f| canonical_sync_path(&f))
            .filter(|f| !f.is_empty())
            .collect();
        let strip = |k: &str| -> String {
            if let Some((first, rest)) = k.split_once('/') {
                if !rest.is_empty() && vault_folders.iter().any(|f| f == first) {
                    return rest.to_string();
                }
            }
            k.to_string()
        };
        let mut map: HashMap<String, i64> = HashMap::with_capacity(raw.len());
        let mut legacy: Vec<(String, i64)> = Vec::new();
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
        for (nk, v) in legacy {
            map.entry(nk).or_insert(v);
        }
        if migrated {
            warn!(
                path = %path.display(),
                "base_seq store: migrated keys to canonical form (NFC + vault-prefix strip)"
            );
        }
        Arc::new(BaseSeqStore {
            inner: Mutex::new(map),
            path,
            dirty: AtomicBool::new(migrated),
            vault_folders,
        })
    }

    /// Upsert `path -> seq`. No I/O; sets the dirty flag. The seq MUST come
    /// from a server response (push `server_seq` / note `change_seq`), NEVER a
    /// local assumption, and MUST be recorded only AFTER the corresponding
    /// bytes are byte-verified on the local FS (R3). Callers enforce both.
    pub fn record(&self, path: &str, seq: i64) {
        let key = self.canon_key(path);
        if let Ok(mut m) = self.inner.lock() {
            m.insert(key, seq);
            self.dirty.store(true, Ordering::Relaxed);
        }
    }

    /// The last-observed server `change_seq` for `path`, if any. `None` is the
    /// fail-closed "unknown/empty lineage" signal (R4): the caller sends
    /// `base_seq: null` and takes the refetch/merge path on the server's 409.
    pub fn get(&self, path: &str) -> Option<i64> {
        let key = self.canon_key(path);
        self.inner.lock().ok().and_then(|m| m.get(&key).copied())
    }

    /// Drop the lineage for `path` (e.g. after a confirmed delete tombstone so
    /// a later re-create starts from unknown lineage rather than a stale seq).
    pub fn remove(&self, path: &str) {
        let key = self.canon_key(path);
        if let Ok(mut m) = self.inner.lock() {
            if m.remove(&key).is_some() {
                self.dirty.store(true, Ordering::Relaxed);
            }
        }
    }

    /// Number of recorded entries.
    pub fn len(&self) -> usize {
        self.inner.lock().map(|m| m.len()).unwrap_or(0)
    }

    /// True iff the store has no recorded entries.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Persist the full map via atomic tmp+rename. No-op (and `Ok`) when not
    /// dirty. Clears the dirty flag only after a successful persist.
    pub fn flush(&self) -> std::io::Result<()> {
        if !self.dirty.load(Ordering::Relaxed) {
            return Ok(());
        }
        let snapshot: HashMap<String, i64> = match self.inner.lock() {
            Ok(m) => m.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        };
        let bytes = serde_json::to_vec(&snapshot).map_err(std::io::Error::other)?;

        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let parent = self
            .path
            .parent()
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| PathBuf::from("."));
        let mut tmp = NamedTempFile::new_in(&parent)?;
        tmp.write_all(&bytes)?;
        tmp.flush()?;
        tmp.persist(&self.path).map_err(|e| e.error)?;

        self.dirty.store(false, Ordering::Relaxed);
        Ok(())
    }

    /// Spawn a background loop that flushes every 30s for the process lifetime.
    /// The immediate first tick is consumed so we don't flush at t=0.
    pub fn spawn_periodic_flush(store: Arc<BaseSeqStore>) {
        tauri::async_runtime::spawn(async move {
            let mut tick = tokio::time::interval(FLUSH_INTERVAL);
            tick.tick().await;
            loop {
                tick.tick().await;
                if let Err(e) = store.flush() {
                    warn!(error = %e, "base_seq store: periodic flush failed");
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_path(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("baseseq_test_{}_{}.json", std::process::id(), name));
        let _ = std::fs::remove_file(&p);
        p
    }

    #[test]
    fn get_returns_none_for_unknown_lineage() {
        // R4: an unrecorded note yields None (fail-closed), never a fabricated
        // or defaulted seq.
        let s = BaseSeqStore::load(tmp_path("unknown"));
        assert_eq!(s.get("01_Notes/x.md"), None);
    }

    #[test]
    fn record_then_get_roundtrips_and_persists() {
        let path = tmp_path("roundtrip");
        {
            let s = BaseSeqStore::load(path.clone());
            s.record("01_Notes/x.md", 4242);
            assert_eq!(s.get("01_Notes/x.md"), Some(4242));
            s.flush().unwrap();
        }
        // Reload from disk: the observed seq survives a restart.
        let s2 = BaseSeqStore::load(path.clone());
        assert_eq!(s2.get("01_Notes/x.md"), Some(4242));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn keys_are_vault_prefix_invariant() {
        // A legacy `<vault>/`-prefixed key and a sync-root-relative key hit the
        // SAME entry, identical to the shadow store's keying.
        let s = BaseSeqStore::load_with_vault_folders(
            tmp_path("prefix"),
            vec!["Mainframe".to_string()],
        );
        s.record("Mainframe/01_Notes/x.md", 7);
        assert_eq!(s.get("01_Notes/x.md"), Some(7));
    }

    #[test]
    fn corrupt_file_loads_empty_not_panic() {
        let path = tmp_path("corrupt");
        std::fs::write(&path, b"{not valid json").unwrap();
        let s = BaseSeqStore::load(path.clone());
        assert!(s.is_empty());
        assert_eq!(s.get("anything.md"), None);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn remove_drops_lineage() {
        let s = BaseSeqStore::load(tmp_path("remove"));
        s.record("a.md", 9);
        assert_eq!(s.get("a.md"), Some(9));
        s.remove("a.md");
        assert_eq!(s.get("a.md"), None);
    }
}
