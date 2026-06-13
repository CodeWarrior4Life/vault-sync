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
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tempfile::NamedTempFile;
use tracing::warn;

/// Periodic flush cadence for [`ShadowStore::spawn_periodic_flush`].
const FLUSH_INTERVAL: Duration = Duration::from_secs(30);

/// Persistent per-file shadow-hash store: path → last-synced server hash.
pub struct ShadowStore {
    inner: Mutex<HashMap<String, String>>,
    path: PathBuf,
    dirty: AtomicBool,
}

impl ShadowStore {
    /// Load the store from `path`. A missing OR corrupt file starts EMPTY and
    /// logs a `warn!` — NEVER panics. The returned store is `dirty == false`
    /// (nothing to flush until something is recorded).
    pub fn load(path: PathBuf) -> Arc<ShadowStore> {
        let map = match std::fs::read(&path) {
            Ok(bytes) => match serde_json::from_slice::<HashMap<String, String>>(&bytes) {
                Ok(m) => m,
                Err(e) => {
                    warn!(
                        path = %path.display(),
                        error = %e,
                        "shadow store: corrupt JSON — starting EMPTY (degrades to pull-on-drift)"
                    );
                    HashMap::new()
                }
            },
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => HashMap::new(),
            Err(e) => {
                warn!(
                    path = %path.display(),
                    error = %e,
                    "shadow store: read failed — starting EMPTY"
                );
                HashMap::new()
            }
        };
        Arc::new(ShadowStore {
            inner: Mutex::new(map),
            path,
            dirty: AtomicBool::new(false),
        })
    }

    /// Upsert `path → server_hash`. No I/O — sets the dirty flag so the next
    /// `flush()` persists it.
    pub fn record(&self, path: &str, server_hash: &str) {
        if let Ok(mut m) = self.inner.lock() {
            m.insert(path.to_string(), server_hash.to_string());
            self.dirty.store(true, Ordering::Relaxed);
        }
    }

    /// The last-synced server hash recorded for `path`, if any.
    pub fn get(&self, path: &str) -> Option<String> {
        self.inner.lock().ok().and_then(|m| m.get(path).cloned())
    }

    /// Persist the full map to `self.path` via atomic tmp+rename. No-op (and
    /// `Ok`) when not dirty. Creates parent dirs as needed. Clears the dirty
    /// flag on success.
    pub fn flush(&self) -> std::io::Result<()> {
        if !self.dirty.load(Ordering::Relaxed) {
            return Ok(());
        }
        // Snapshot under the lock, then write outside it.
        let snapshot: HashMap<String, String> = match self.inner.lock() {
            Ok(m) => m.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        };
        let bytes = serde_json::to_vec(&snapshot).map_err(std::io::Error::other)?;

        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        // Atomic tmp+rename, anchored in the destination dir so the rename is
        // same-filesystem (no cross-device EXDEV).
        let parent = self
            .path
            .parent()
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| PathBuf::from("."));
        let mut tmp = NamedTempFile::new_in(&parent)?;
        tmp.write_all(&bytes)?;
        tmp.flush()?;
        tmp.persist(&self.path).map_err(|e| e.error)?;

        // Only clear dirty AFTER a successful persist. A concurrent record()
        // between the snapshot and here re-sets dirty (we read it again next
        // flush), so we never lose a write.
        self.dirty.store(false, Ordering::Relaxed);
        Ok(())
    }

    /// Spawn a background loop that flushes every 30s for the process lifetime.
    /// The immediate first `interval` tick is consumed so we don't flush at t=0
    /// (nothing recorded yet). A flush error is logged and the loop continues.
    pub fn spawn_periodic_flush(self: Arc<Self>) {
        tauri::async_runtime::spawn(async move {
            let mut tick = tokio::time::interval(FLUSH_INTERVAL);
            tick.tick().await; // consume the immediate first tick
            loop {
                tick.tick().await;
                if let Err(e) = self.flush() {
                    warn!(error = %e, "shadow store: periodic flush failed (will retry)");
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn load_empty_when_file_missing() {
        let dir = TempDir::new().unwrap();
        let p = dir.path().join("does-not-exist.json");
        let store = ShadowStore::load(p);
        assert_eq!(store.get("anything.md"), None);
    }

    #[test]
    fn record_then_get() {
        let dir = TempDir::new().unwrap();
        let store = ShadowStore::load(dir.path().join("shadow.json"));
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
        let path = dir.path().join("sub").join("shadow.json"); // parent dir created
        {
            let store = ShadowStore::load(path.clone());
            store.record("notes/a.md", "h-a");
            store.record("notes/b.md", "h-b");
            store.flush().unwrap();
        }
        // Fresh load sees the persisted entries.
        let reloaded = ShadowStore::load(path);
        assert_eq!(reloaded.get("notes/a.md"), Some("h-a".to_string()));
        assert_eq!(reloaded.get("notes/b.md"), Some("h-b".to_string()));
    }

    #[test]
    fn corrupt_file_loads_empty_no_panic() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("shadow.json");
        std::fs::write(&path, b"{{not json").unwrap();
        let store = ShadowStore::load(path);
        assert_eq!(store.get("notes/a.md"), None);
        // still usable after a corrupt load
        store.record("notes/a.md", "recovered");
        assert_eq!(store.get("notes/a.md"), Some("recovered".to_string()));
    }

    #[test]
    fn flush_is_noop_when_not_dirty() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("shadow.json");
        let store = ShadowStore::load(path.clone());
        // Fresh load is not dirty → flush writes nothing and is Ok.
        assert!(store.flush().is_ok());
        assert!(!path.exists(), "non-dirty flush must NOT create the file");
    }

    #[test]
    fn flush_clears_dirty_then_second_flush_is_noop() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("shadow.json");
        let store = ShadowStore::load(path.clone());
        store.record("a.md", "h");
        store.flush().unwrap();
        assert!(path.exists());
        // Remove the file; a second (non-dirty) flush must NOT recreate it.
        std::fs::remove_file(&path).unwrap();
        store.flush().unwrap();
        assert!(
            !path.exists(),
            "second flush after a clean flush must be a no-op"
        );
    }
}
