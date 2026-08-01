//! Durable READ-RECEIPT store: the client leg of the Vault Sync Convergence
//! P2b recovery protocol (TKT-f74edf99, R1). A receipt is the daemon's PROOF
//! that it fetched a specific server revision body AND byte-verified its hash
//! before trusting the revision's `change_seq` as a push baseline.
//!
//! ## Why a receipt and not "just record the number" (R1, binding)
//!
//! GPT-5.6's 2026-07-29 review refuted the naive "on a 409, read the server's
//! current_seq and record it" fix as "the same forgery hazard relocated": a
//! bare number recorded without the body is indistinguishable from a fabricated
//! baseline, and the server causal gate exists precisely to reject fabricated
//! lineage. So a baseline may be authorised ONLY by a receipt whose body hash
//! was verified against the exact revision the server named. Recording a
//! baseline from a number alone is forbidden; this store makes that structural:
//! the ONLY writer is [`ReadReceiptStore::record_verified`], and callers reach
//! it only through [`verify_receipt`], which returns `None` on any hash
//! mismatch (fail-closed).
//!
//! ## What a receipt binds
//!
//! Each receipt ties together `(revision_seq, body_sha)` for a path. In this
//! system the server `change_seq` IS the revision identifier (the note's
//! monotonic version token), so `revision_seq` doubles as the revision_id the
//! requirement names; `body_sha` is the hex sha256 of the exact revision body
//! the daemon fetched and verified. A recorded receipt is the evidence that
//! authorises the daemon to declare `base_seq = revision_seq` on its retry.
//!
//! ## Relationship to the base_seq store
//!
//! [`base_seq_store::BaseSeqStore`](crate::base_seq_store) is what goes ON THE
//! WIRE (`PushRequest.base_seq`). This store is the durable AUDIT of how each
//! baseline was earned: a `path -> Receipt` map persisted to a sibling file.
//! The refetch/merge path records BOTH (the receipt here, the number on the
//! wire) from one verified fetch, so the wire baseline is never fed from an
//! unverified source.
//!
//! ## Persistence / fail-closed
//!
//! Backed by a flat JSON `HashMap<path, Receipt>` on disk, dirty-gated + atomic
//! (tmp+rename), exactly like the shadow / base_seq stores. A missing OR corrupt
//! file loads as EMPTY (never a panic): no receipts means no authorised
//! baselines, which is the correct fail-closed default (the push declares
//! `base_seq: null` and takes the refetch/merge path on the server's 409).

use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tempfile::NamedTempFile;
use tracing::warn;

use crate::sync_shadow::canonical_sync_path;

/// Periodic flush cadence, matching the shadow / base_seq stores.
const FLUSH_INTERVAL: Duration = Duration::from_secs(30);

/// A verified read-receipt for one note: proof the daemon fetched revision
/// `revision_seq` and confirmed its body hashed to `body_sha`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Receipt {
    /// The server `change_seq` (revision identifier) of the fetched body.
    pub revision_seq: i64,
    /// Hex sha256 of the exact revision body the daemon fetched and verified.
    pub body_sha: String,
}

/// Verify a fetched revision body against the hash the server declared for it,
/// and (if present) the hash the 409 named as the expected head. Returns a
/// [`Receipt`] ONLY when the freshly-computed sha256 of `body` matches
/// `declared_sha` AND (when `Some`) `expected_head_sha`. Any mismatch, or a
/// missing revision seq, returns `None` — the caller MUST NOT record a baseline
/// on `None` (fail-closed, R1).
///
/// PURE (no I/O): the single verification choke point, exhaustively testable.
pub fn verify_receipt(
    body: &[u8],
    declared_sha: &str,
    expected_head_sha: Option<&str>,
    revision_seq: Option<i64>,
) -> Option<Receipt> {
    // A pre-R7b server that omits the revision seq cannot authorise a baseline:
    // there is no revision token to declare on retry. Fail closed.
    let seq = revision_seq?;
    let actual = hex::encode(Sha256::digest(body));
    if actual != declared_sha {
        return None;
    }
    if let Some(head) = expected_head_sha {
        if actual != head {
            return None;
        }
    }
    Some(Receipt {
        revision_seq: seq,
        body_sha: actual,
    })
}

/// Durable per-note verified-receipt store: `path -> Receipt`.
pub struct ReadReceiptStore {
    inner: Mutex<HashMap<String, Receipt>>,
    path: PathBuf,
    dirty: AtomicBool,
    /// Sync-root basenames whose leading `<vault_folder>/` prefix is stripped
    /// off keys, identical to `ShadowStore`/`BaseSeqStore` so all stores key in
    /// lockstep.
    vault_folders: Vec<String>,
}

impl ReadReceiptStore {
    /// Load with NO vault-folder awareness (tests / callers passing canonical
    /// sync-root-relative keys).
    pub fn load(path: PathBuf) -> Arc<ReadReceiptStore> {
        Self::load_with_vault_folders(path, Vec::new())
    }

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
    /// logs a `warn!` (never panics). Empty means "no authorised baselines yet"
    /// for every note, the fail-closed default.
    pub fn load_with_vault_folders(
        path: PathBuf,
        vault_folders: Vec<String>,
    ) -> Arc<ReadReceiptStore> {
        let raw = match std::fs::read(&path) {
            Ok(bytes) => match serde_json::from_slice::<HashMap<String, Receipt>>(&bytes) {
                Ok(m) => m,
                Err(e) => {
                    warn!(
                        path = %path.display(),
                        error = %e,
                        "read-receipt store: corrupt JSON, starting EMPTY (fail-closed: refetch/merge)"
                    );
                    HashMap::new()
                }
            },
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => HashMap::new(),
            Err(e) => {
                warn!(
                    path = %path.display(),
                    error = %e,
                    "read-receipt store: read failed, starting EMPTY (fail-closed)"
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
        let mut map: HashMap<String, Receipt> = HashMap::with_capacity(raw.len());
        let mut migrated = false;
        for (k, v) in raw.into_iter() {
            let nk = strip(&canonical_sync_path(&k));
            if nk != k {
                migrated = true;
            }
            map.entry(nk).or_insert(v);
        }
        if migrated {
            warn!(
                path = %path.display(),
                "read-receipt store: migrated keys to canonical form (NFC + vault-prefix strip)"
            );
        }
        Arc::new(ReadReceiptStore {
            inner: Mutex::new(map),
            path,
            dirty: AtomicBool::new(migrated),
            vault_folders,
        })
    }

    /// Record a VERIFIED receipt for `path`. This is the ONLY writer, and it
    /// takes a [`Receipt`] value — which callers can obtain ONLY from
    /// [`verify_receipt`] returning `Some`. There is deliberately no
    /// `record(path, seq)` that takes a bare number: recording a baseline from
    /// a number alone is forbidden (R1). No I/O; sets the dirty flag.
    pub fn record_verified(&self, path: &str, receipt: Receipt) {
        let key = self.canon_key(path);
        if let Ok(mut m) = self.inner.lock() {
            m.insert(key, receipt);
            self.dirty.store(true, Ordering::Relaxed);
        }
    }

    /// The verified receipt for `path`, if any. `None` = no authorised baseline.
    pub fn get(&self, path: &str) -> Option<Receipt> {
        let key = self.canon_key(path);
        self.inner.lock().ok().and_then(|m| m.get(&key).cloned())
    }

    /// Drop the receipt for `path` (e.g. after a confirmed delete tombstone).
    pub fn remove(&self, path: &str) {
        let key = self.canon_key(path);
        if let Ok(mut m) = self.inner.lock() {
            if m.remove(&key).is_some() {
                self.dirty.store(true, Ordering::Relaxed);
            }
        }
    }

    /// Number of recorded receipts.
    pub fn len(&self) -> usize {
        self.inner.lock().map(|m| m.len()).unwrap_or(0)
    }

    /// True iff no receipts are recorded.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Persist the full map via atomic tmp+rename. No-op (and `Ok`) when not
    /// dirty. Clears the dirty flag only after a successful persist.
    pub fn flush(&self) -> std::io::Result<()> {
        if !self.dirty.load(Ordering::Relaxed) {
            return Ok(());
        }
        let snapshot: HashMap<String, Receipt> = match self.inner.lock() {
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
    pub fn spawn_periodic_flush(store: Arc<ReadReceiptStore>) {
        tauri::async_runtime::spawn(async move {
            let mut tick = tokio::time::interval(FLUSH_INTERVAL);
            tick.tick().await;
            loop {
                tick.tick().await;
                if let Err(e) = store.flush() {
                    warn!(error = %e, "read-receipt store: periodic flush failed");
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
        p.push(format!("receipt_test_{}_{}.json", std::process::id(), name));
        let _ = std::fs::remove_file(&p);
        p
    }

    fn sha_of(s: &str) -> String {
        hex::encode(Sha256::digest(s.as_bytes()))
    }

    #[test]
    fn verify_receipt_accepts_matching_body_and_head() {
        // R1: a body whose hash matches BOTH the server-declared sha and the
        // 409's expected head sha yields a receipt bound to (seq, verified sha).
        let body = b"server head bytes";
        let sha = sha_of("server head bytes");
        let r = verify_receipt(body, &sha, Some(&sha), Some(42)).expect("should verify");
        assert_eq!(r.revision_seq, 42);
        assert_eq!(r.body_sha, sha);
    }

    #[test]
    fn verify_receipt_rejects_hash_mismatch_forgery_hazard() {
        // R1 (binding): a body that does NOT hash to the declared sha must be
        // refused — this is the exact "record a number without the body" forgery
        // hazard GPT-5.6 named. None => caller records no baseline (fail-closed).
        let body = b"tampered or wrong body";
        let declared = sha_of("the real server body");
        assert_eq!(verify_receipt(body, &declared, None, Some(42)), None);
    }

    #[test]
    fn verify_receipt_rejects_head_mismatch() {
        // Body matches the note's own declared sha, but the 409 named a
        // DIFFERENT head — the server moved under us; refuse (fail-closed).
        let body = b"a body";
        let sha = sha_of("a body");
        let other = sha_of("some other head");
        assert_eq!(verify_receipt(body, &sha, Some(&other), Some(7)), None);
    }

    #[test]
    fn verify_receipt_rejects_missing_revision_seq() {
        // A pre-R7b server that omits change_seq cannot authorise a baseline.
        let body = b"a body";
        let sha = sha_of("a body");
        assert_eq!(verify_receipt(body, &sha, None, None), None);
    }

    #[test]
    fn record_verified_roundtrips_and_persists() {
        let path = tmp_path("roundtrip");
        let sha = sha_of("body");
        {
            let s = ReadReceiptStore::load(path.clone());
            let r = verify_receipt(b"body", &sha, None, Some(99)).unwrap();
            s.record_verified("01_Notes/x.md", r);
            assert_eq!(s.get("01_Notes/x.md").unwrap().revision_seq, 99);
            s.flush().unwrap();
        }
        let s2 = ReadReceiptStore::load(path.clone());
        let got = s2.get("01_Notes/x.md").expect("survives restart");
        assert_eq!(got.revision_seq, 99);
        assert_eq!(got.body_sha, sha);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn keys_are_vault_prefix_invariant() {
        let s =
            ReadReceiptStore::load_with_vault_folders(tmp_path("prefix"), vec!["Mainframe".into()]);
        let sha = sha_of("b");
        s.record_verified(
            "Mainframe/01_Notes/x.md",
            verify_receipt(b"b", &sha, None, Some(3)).unwrap(),
        );
        assert_eq!(s.get("01_Notes/x.md").unwrap().revision_seq, 3);
    }

    #[test]
    fn corrupt_file_loads_empty_not_panic() {
        let path = tmp_path("corrupt");
        std::fs::write(&path, b"{not valid json").unwrap();
        let s = ReadReceiptStore::load(path.clone());
        assert!(s.is_empty());
        assert_eq!(s.get("anything.md"), None);
        let _ = std::fs::remove_file(&path);
    }
}
