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
//! ## Provenance (TKT-372e31b2, PR #11 review Finding 1)
//!
//! Since the verified read-receipt landed (TKT-f74edf99), a seq can enter this
//! store two causally DIFFERENT ways, and consumers now care which:
//!
//! * **Adopted** (`record_adopted`) — the local FS was byte-verified to hold
//!   EXACTLY this server version's bytes at record time (post-integrity
//!   materialize, ack-align rewrite, or an accepted push whose bytes were
//!   already canonical). Local bytes that later differ from this version are
//!   therefore a write layered ON TOP of it — proof of DESCENT. Only this
//!   provenance may enable the materializer's causal-preserve arm.
//! * **Observed** (`record_observed`) — a verified read-receipt proved we SAW
//!   this server version (hash-verified body), but the local file was never
//!   confirmed to hold its bytes. It authorises the WIRE `base_seq` on the
//!   retry push (the whole point of the receipt), but it proves NOTHING about
//!   the ancestry of the local bytes, so it must NEVER enable the
//!   causal-preserve arm: on a 409 refetch of a genuinely divergent note the
//!   receipt records the server head seq, and the immediately following
//!   materialize of that same head would otherwise see `incoming == observed`
//!   and swallow every true conflict as "local is newer".
//!
//! `get()` is provenance-blind (wire declaration, stash naming);
//! `get_adopted()` is the causal-preserve gate.
//!
//! ## Persistence
//!
//! Backed by a flat JSON map on disk, dirty-gated + atomic (tmp+rename),
//! exactly like the shadow store. A missing OR corrupt file loads as EMPTY
//! (never a panic) which simply means "no lineage known yet" for every note:
//! fail-closed, refetch/merge on the first push under the flag.
//!
//! Values are `{"seq": N, "prov": "adopted"|"observed"}`. LEGACY entries (a
//! bare `N` from a pre-provenance daemon) load as **Observed** — the SAFE
//! default. A v0.4.36 store already contains receipt-recorded seqs that are
//! indistinguishable from adopted ones, so mapping legacy to Adopted would
//! enable the preserve arm on exactly the entries it must not trust. Mapping
//! legacy to Observed keeps the wire declaration intact (no re-409 storm) and
//! merely makes the causal arm stand down to the always-stash floor (both
//! byte-sets preserved — safe, at worst one extra fork) until the entry
//! re-earns Adopted on its next byte-verified materialize / accepted push.

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

/// HOW a recorded seq was earned (TKT-372e31b2, Finding 1). See module docs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SeqProvenance {
    /// Byte-verified adoption: the local FS held exactly this version's bytes
    /// at record time. Proof of DESCENT for later local edits — the ONLY
    /// provenance that may enable the causal-preserve arm.
    Adopted,
    /// Verified read-receipt observation (TKT-f74edf99): we provably SAW this
    /// version, but the local file was never confirmed to hold it. Authorises
    /// the wire `base_seq`; never the causal-preserve arm.
    Observed,
}

/// One recorded lineage entry: the seq plus how it was earned.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct SeqEntry {
    seq: i64,
    prov: SeqProvenance,
}

/// On-disk value: either the legacy bare seq (pre-provenance daemons) or the
/// tagged entry. Legacy maps to `Observed` — the safe, non-preserve-enabling
/// default (module docs, "Persistence").
#[derive(serde::Deserialize)]
#[serde(untagged)]
enum RawSeqEntry {
    Tagged(SeqEntry),
    Legacy(i64),
}

/// Persistent per-file observed-`change_seq` store: path -> last-observed
/// server `change_seq` (proof-of-observation for the R7b causal gate), tagged
/// with the provenance that earned it.
pub struct BaseSeqStore {
    inner: Mutex<HashMap<String, SeqEntry>>,
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
    /// Legacy BARE-SEQ values (pre-provenance daemons) load as `Observed` —
    /// the safe, non-preserve-enabling default (module docs, "Persistence") —
    /// and are persisted in the tagged form on the next flush.
    pub fn load_with_vault_folders(path: PathBuf, vault_folders: Vec<String>) -> Arc<BaseSeqStore> {
        let mut value_migrated = false;
        let raw: HashMap<String, SeqEntry> = match std::fs::read(&path) {
            Ok(bytes) => match serde_json::from_slice::<HashMap<String, RawSeqEntry>>(&bytes) {
                Ok(m) => m
                    .into_iter()
                    .map(|(k, v)| {
                        let entry = match v {
                            RawSeqEntry::Tagged(e) => e,
                            RawSeqEntry::Legacy(seq) => {
                                value_migrated = true;
                                SeqEntry {
                                    seq,
                                    prov: SeqProvenance::Observed,
                                }
                            }
                        };
                        (k, entry)
                    })
                    .collect(),
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
        let mut map: HashMap<String, SeqEntry> = HashMap::with_capacity(raw.len());
        let mut legacy: Vec<(String, SeqEntry)> = Vec::new();
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
        if value_migrated {
            warn!(
                path = %path.display(),
                "base_seq store: legacy bare-seq entries loaded as prov=observed (safe default: wire declaration intact, causal-preserve arm disabled until re-earned as adopted)"
            );
        }
        Arc::new(BaseSeqStore {
            inner: Mutex::new(map),
            path,
            dirty: AtomicBool::new(migrated || value_migrated),
            vault_folders,
        })
    }

    /// Upsert `path -> seq` with ADOPTED provenance. No I/O; sets the dirty
    /// flag. The seq MUST come from a server response (push `server_seq` /
    /// note `change_seq`), NEVER a local assumption, and MUST be recorded only
    /// AFTER the corresponding bytes are byte-verified on the local FS (R3).
    /// Callers enforce both — this is what makes Adopted a proof of descent.
    pub fn record_adopted(&self, path: &str, seq: i64) {
        self.record_with(path, seq, SeqProvenance::Adopted);
    }

    /// Upsert `path -> seq` with OBSERVED provenance (verified read-receipt,
    /// TKT-f74edf99). Authorises the wire `base_seq` retry declaration but
    /// never the causal-preserve arm. When the entry already holds the SAME
    /// seq as Adopted, the stronger proof is kept (an observation of a version
    /// we already byte-verified adds nothing and must not weaken it).
    pub fn record_observed(&self, path: &str, seq: i64) {
        let key = self.canon_key(path);
        if let Ok(mut m) = self.inner.lock() {
            if let Some(existing) = m.get(&key) {
                if existing.prov == SeqProvenance::Adopted && existing.seq == seq {
                    return;
                }
            }
            m.insert(
                key,
                SeqEntry {
                    seq,
                    prov: SeqProvenance::Observed,
                },
            );
            self.dirty.store(true, Ordering::Relaxed);
        }
    }

    fn record_with(&self, path: &str, seq: i64, prov: SeqProvenance) {
        let key = self.canon_key(path);
        if let Ok(mut m) = self.inner.lock() {
            m.insert(key, SeqEntry { seq, prov });
            self.dirty.store(true, Ordering::Relaxed);
        }
    }

    /// The last-observed server `change_seq` for `path`, if any — PROVENANCE-
    /// BLIND (wire declaration, stash naming). `None` is the fail-closed
    /// "unknown/empty lineage" signal (R4): the caller sends `base_seq: null`
    /// and takes the refetch/merge path on the server's 409.
    pub fn get(&self, path: &str) -> Option<i64> {
        let key = self.canon_key(path);
        self.inner
            .lock()
            .ok()
            .and_then(|m| m.get(&key).map(|e| e.seq))
    }

    /// The last ADOPTED server `change_seq` for `path` — the causal-preserve
    /// gate (TKT-372e31b2, Finding 1). Returns `None` for Observed/legacy
    /// entries: a receipt-earned or unknown-provenance seq proves nothing
    /// about the ancestry of the local bytes, so the preserve arm must stand
    /// down to the always-stash floor.
    pub fn get_adopted(&self, path: &str) -> Option<i64> {
        let key = self.canon_key(path);
        self.inner.lock().ok().and_then(|m| {
            m.get(&key)
                .filter(|e| e.prov == SeqProvenance::Adopted)
                .map(|e| e.seq)
        })
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
        let snapshot: HashMap<String, SeqEntry> = match self.inner.lock() {
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
            s.record_adopted("01_Notes/x.md", 4242);
            assert_eq!(s.get("01_Notes/x.md"), Some(4242));
            s.flush().unwrap();
        }
        // Reload from disk: the seq AND its provenance survive a restart.
        let s2 = BaseSeqStore::load(path.clone());
        assert_eq!(s2.get("01_Notes/x.md"), Some(4242));
        assert_eq!(s2.get_adopted("01_Notes/x.md"), Some(4242));
        let _ = std::fs::remove_file(&path);
    }

    /// Finding 1 (TKT-372e31b2, PR #11 review): an Observed (receipt-earned)
    /// entry authorises the wire declaration (`get`) but NEVER the
    /// causal-preserve gate (`get_adopted`).
    #[test]
    fn observed_provenance_feeds_wire_but_not_causal_gate() {
        let path = tmp_path("observed");
        {
            let s = BaseSeqStore::load(path.clone());
            s.record_observed("01_Notes/x.md", 77);
            assert_eq!(s.get("01_Notes/x.md"), Some(77), "wire declaration intact");
            assert_eq!(
                s.get_adopted("01_Notes/x.md"),
                None,
                "a receipt-earned seq must not enable the preserve arm"
            );
            s.flush().unwrap();
        }
        // Provenance survives a restart too.
        let s2 = BaseSeqStore::load(path.clone());
        assert_eq!(s2.get("01_Notes/x.md"), Some(77));
        assert_eq!(s2.get_adopted("01_Notes/x.md"), None);
        let _ = std::fs::remove_file(&path);
    }

    /// Finding 1 migration: a LEGACY store (bare `path -> seq` values, written
    /// by a pre-provenance daemon — including v0.4.36 stores that already mix
    /// receipt-recorded and adopted seqs indistinguishably) loads with every
    /// entry as Observed: `get` intact (no re-409 storm), `get_adopted` None
    /// (legacy entries never NEWLY enable the preserve arm). The upgraded
    /// tagged form is persisted on the next flush.
    #[test]
    fn legacy_bare_seq_entries_load_as_observed_safe_default() {
        let path = tmp_path("legacy");
        std::fs::write(&path, br#"{"01_Notes/x.md": 91, "01_Notes/y.md": 12}"#).unwrap();
        {
            let s = BaseSeqStore::load(path.clone());
            assert_eq!(s.get("01_Notes/x.md"), Some(91));
            assert_eq!(s.get("01_Notes/y.md"), Some(12));
            assert_eq!(s.get_adopted("01_Notes/x.md"), None);
            assert_eq!(s.get_adopted("01_Notes/y.md"), None);
            // The load marked the store dirty; flush writes the tagged form.
            s.flush().unwrap();
        }
        let bytes = std::fs::read(&path).unwrap();
        let txt = String::from_utf8(bytes).unwrap();
        assert!(
            txt.contains("\"prov\":\"observed\""),
            "flush must persist the tagged form, got: {txt}"
        );
        let s2 = BaseSeqStore::load(path.clone());
        assert_eq!(s2.get("01_Notes/x.md"), Some(91));
        assert_eq!(s2.get_adopted("01_Notes/x.md"), None);
        let _ = std::fs::remove_file(&path);
    }

    /// An observation of a version we already byte-verified ADOPTED must not
    /// weaken the stronger proof; a DIFFERENT observed seq replaces it (the
    /// adoption proves nothing about the newer version's ancestry).
    #[test]
    fn observed_same_seq_keeps_adopted_different_seq_replaces() {
        let s = BaseSeqStore::load(tmp_path("keep_adopted"));
        s.record_adopted("a.md", 100);
        s.record_observed("a.md", 100);
        assert_eq!(
            s.get_adopted("a.md"),
            Some(100),
            "same-seq observation must not downgrade an adoption"
        );
        s.record_observed("a.md", 105);
        assert_eq!(s.get("a.md"), Some(105), "newer observation wins the wire");
        assert_eq!(
            s.get_adopted("a.md"),
            None,
            "the newer version was never adopted"
        );
    }

    #[test]
    fn keys_are_vault_prefix_invariant() {
        // A legacy `<vault>/`-prefixed key and a sync-root-relative key hit the
        // SAME entry, identical to the shadow store's keying.
        let s = BaseSeqStore::load_with_vault_folders(
            tmp_path("prefix"),
            vec!["Mainframe".to_string()],
        );
        s.record_adopted("Mainframe/01_Notes/x.md", 7);
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
        s.record_adopted("a.md", 9);
        assert_eq!(s.get("a.md"), Some(9));
        s.remove("a.md");
        assert_eq!(s.get("a.md"), None);
    }
}
