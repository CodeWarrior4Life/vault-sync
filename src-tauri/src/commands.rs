//! Tauri command surface invoked from the JS front-end + the tray menu.
//!
//! Hosts `verify_repair_run`, the owner-invokable "Verify and repair all
//! files…" sweep wired to `tray.rs`. CS-3 §3.4: it borrows the daemon's own
//! handles (journal owner, shadow, per-root API client) from the published
//! `SyncHandles` and delegates to `verify_repair::VerifyRepair::run`.

use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::Serialize;
use tauri::Manager;

use crate::config;
use crate::conflict_stash::parse_conflict_filename;
use crate::ownership::{hash_config_file, DiskConfig};
use crate::sync_handles::{BorrowRefused, LiveBorrow, SyncHandlesSlot};
use crate::verify_repair::{
    VerifyRepair, VerifyRepairConfig, VerifyRepairError, VerifyRepairReport,
};

/// v0.3 mandate §3 — `<workspace_root>/.lattice-runtime/<subscriber_slug>/`
/// is the canonical "daemon state OUT of vault" anchor. Mirrors the resolver
/// in `lib.rs::spawn_sse_consumer` so the journal path the verify-repair
/// command opens is the SAME jsonl file the push_client drain-loop reads.
/// Falls back through `data_local_dir → home_dir → temp_dir` so the daemon
/// never crashes on an exotic platform; the final fallback is good enough
/// for the verify-repair single-shot path.
pub(crate) fn resolve_workspace_root() -> PathBuf {
    dirs::data_local_dir()
        .unwrap_or_else(|| dirs::home_dir().unwrap_or_else(std::env::temp_dir))
        .join("Nexus")
}

/// Owner-invokable Tauri command, also called directly by the tray
/// (`tray.rs`). CS-3 §3.4 (D6, P10): reads the managed
/// [`SyncHandlesSlot`] and borrows; it never loads config, loads a token,
/// opens a journal or loads a shadow.
#[tauri::command]
pub async fn verify_repair_run(app: tauri::AppHandle) -> Result<VerifyRepairReport, String> {
    let slot = app
        .try_state::<SyncHandlesSlot>()
        .map(|s| s.inner().clone());
    verify_repair_with(slot.as_ref()).await
}

fn refused(r: BorrowRefused) -> String {
    format!("Verify and repair refused: {r}")
}

/// CS-3 §3.4 preflight, all or nothing (D9, D10): a fresh hash of the
/// config file compared with the snapshot's, then a borrow of EVERY root in
/// snapshot order. Any refusal returns before any manifest walk or server
/// request.
pub fn verify_repair_preflight(
    slot: Option<&SyncHandlesSlot>,
) -> Result<Vec<LiveBorrow>, BorrowRefused> {
    let Some(handles) = slot.and_then(|s| s.get()) else {
        return Err(BorrowRefused::NotPublished {
            why: "unpaired, booting, or boot aborted".to_string(),
        });
    };
    let snap = handles.snapshot();
    match hash_config_file(snap.cfg_path()) {
        DiskConfig::Hash(h) if &h == snap.config_sha256() => {}
        DiskConfig::Unreadable(why) => return Err(BorrowRefused::ConfigUnreadable { why }),
        other => {
            return Err(BorrowRefused::ConfigChanged {
                snapshot_sha8: snap.sha8(),
                disk_sha8: other.sha8(),
            })
        }
    }
    snap.roots().iter().map(|r| handles.borrow(r.key)).collect()
}

/// The V&R body: preflight, then one `VerifyRepair` per root with the
/// root's borrowed journal owner, the daemon's own top-level shadow (the
/// real `vault_folders` keying) and the root pipeline's own `ApiClient`
/// (closes TKT-b6d8c4ba facets 1-3). Reports merge into one.
pub async fn verify_repair_with(
    slot: Option<&SyncHandlesSlot>,
) -> Result<VerifyRepairReport, String> {
    let borrows = verify_repair_preflight(slot).map_err(refused)?;
    let mut combined = VerifyRepairReport::default();
    let roots = borrows.len();
    for (done, b) in borrows.into_iter().enumerate() {
        let vr = VerifyRepair::new(
            b.root.path.clone(),
            b.api,
            b.journal,
            b.root.eff_sub.clone(),
            VerifyRepairConfig::default(),
        )
        .with_shadow(b.shadow)
        .with_liveness(b.live);
        let report = match vr.run().await {
            Ok(r) => r,
            Err(VerifyRepairError::Stopped(_)) => {
                return Err(format!("daemon stopping: no changes after {done} roots"));
            }
            Err(e) => return Err(e.to_string()),
        };
        tracing::info!(
            root = %b.root.path.display(),
            subscriber_id = %b.root.eff_sub,
            files_scanned = report.files_scanned,
            modify_count = report.modify_count,
            add_count = report.add_count,
            substrate_refused_count = report.substrate_refused_count,
            elapsed_ms = report.elapsed_ms,
            "verify_repair_run: per-root complete"
        );
        combined.files_scanned += report.files_scanned;
        combined.files_in_sync += report.files_in_sync;
        combined.modify_count += report.modify_count;
        combined
            .modify_paths_sample
            .extend(report.modify_paths_sample);
        combined.add_count += report.add_count;
        combined.add_paths_sample.extend(report.add_paths_sample);
        combined.delete_count += report.delete_count;
        combined
            .delete_paths_sample
            .extend(report.delete_paths_sample);
        combined.substrate_refused_count += report.substrate_refused_count;
        combined.extension_filtered_count += report.extension_filtered_count;
        combined.errors.extend(report.errors);
        combined.elapsed_ms += report.elapsed_ms;
    }
    tracing::info!(
        roots_scanned = roots,
        files_scanned = combined.files_scanned,
        modify_count = combined.modify_count,
        add_count = combined.add_count,
        substrate_refused_count = combined.substrate_refused_count,
        elapsed_ms = combined.elapsed_ms,
        "verify_repair_run: all roots complete"
    );
    Ok(combined)
}

// ---------------------------------------------------------------------------
// list_conflicts — Wave 4 tray surface (Agent L)
// ---------------------------------------------------------------------------

/// Local serde adapter for `Option<DateTime<Utc>>` — we don't pull in the
/// `chrono/serde` feature (same convention as `push_journal::ts`). Stores as
/// RFC3339 string when Some, null when None.
mod opt_ts {
    use chrono::{DateTime, Utc};
    use serde::{Serialize, Serializer};
    pub fn serialize<S: Serializer>(v: &Option<DateTime<Utc>>, s: S) -> Result<S::Ok, S::Error> {
        match v {
            Some(dt) => dt.to_rfc3339().serialize(s),
            None => s.serialize_none(),
        }
    }
}

/// One unresolved `*.conflict-from-*.md` sibling file as surfaced to the UI.
#[derive(Debug, Clone, Serialize)]
pub struct ConflictEntry {
    /// Forward-slash relative path of the stash file inside the vault root.
    pub path: String,
    /// Originating device id parsed from the filename.
    pub from_device: String,
    /// Server lsn at which this revision was stashed.
    pub from_lsn: u64,
    /// Forward-slash relative path of the original note this stash pairs with
    /// (`<stem>.md` in the same directory).
    pub original_path: String,
    pub size_bytes: u64,
    #[serde(with = "opt_ts")]
    pub mtime: Option<DateTime<Utc>>,
}

/// List every `*.conflict-from-*.md` stash file under the configured vault.
///
/// Returns an empty list when there are no conflicts. Returns `Err` when the
/// daemon config can't be loaded (no pairing yet, file corrupt, etc.) so the
/// caller can surface a meaningful message rather than "no conflicts".
#[tauri::command]
pub async fn list_conflicts() -> Result<Vec<ConflictEntry>, String> {
    let cfg_path = config::default_config_path();
    let cfg =
        config::Config::load_from(&cfg_path).map_err(|e| format!("config load failed: {e}"))?;
    // S477: list_conflicts scans the entire `vaults_root` — the vault folder
    // is the first segment of each ConflictEntry.path.
    let vault_root = cfg.vaults_root.clone();
    list_conflicts_in(&vault_root)
}

/// Pure-function inner — same scan logic, vault root passed in. Extracted so
/// unit tests can drive it against a tempdir without needing the platform
/// config file in place. Mirrors the `pair`/`pair_inner` split convention.
pub fn list_conflicts_in(vault_root: &Path) -> Result<Vec<ConflictEntry>, String> {
    let mut entries: Vec<ConflictEntry> = Vec::new();
    walk_conflicts(vault_root, &mut |abs_path: &Path| {
        let Some(name) = abs_path.file_name().and_then(|n| n.to_str()) else {
            return;
        };
        let Some(parsed) = parse_conflict_filename(name) else {
            return;
        };
        let rel = match abs_path.strip_prefix(vault_root) {
            Ok(r) => r.to_path_buf(),
            Err(_) => return,
        };
        let rel_str = rel.to_string_lossy().replace('\\', "/");
        let parent_rel = rel.parent().unwrap_or(Path::new("")).to_path_buf();
        let original_rel = parent_rel.join(format!("{}.md", parsed.stem));
        let original_str = original_rel.to_string_lossy().replace('\\', "/");

        let (size_bytes, mtime) = match std::fs::metadata(abs_path) {
            Ok(m) => {
                let mtime = m.modified().ok().map(DateTime::<Utc>::from);
                (m.len(), mtime)
            }
            Err(_) => (0, None),
        };

        entries.push(ConflictEntry {
            path: rel_str,
            from_device: parsed.device,
            from_lsn: parsed.lsn,
            original_path: original_str,
            size_bytes,
            mtime,
        });
    });
    entries.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(entries)
}

/// Std-only recursive walker scoped to this module so it stays decoupled
/// from `conflict_stash::walk_dir` (which is private to that module).
fn walk_conflicts(root: &Path, visit: &mut dyn FnMut(&Path)) {
    if !root.exists() {
        return;
    }
    let mut stack: Vec<PathBuf> = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let rd = match std::fs::read_dir(&dir) {
            Ok(r) => r,
            Err(_) => continue,
        };
        for entry in rd.flatten() {
            let p = entry.path();
            let ft = match entry.file_type() {
                Ok(t) => t,
                Err(_) => continue,
            };
            if ft.is_dir() {
                stack.push(p);
            } else if ft.is_file() {
                visit(&p);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::disallowed_methods)]
mod tests {
    use super::*;
    use crate::config::Config;
    use mockito::Server;
    use std::fs;
    use std::path::PathBuf;
    use tempfile::TempDir;

    fn make_config(server_url: &str, vaults_root: PathBuf, _vault_name: &str) -> Config {
        // _vault_name retained as a parameter so call sites stay readable
        // (they pass the vault folder name they just created on disk) but
        // is no longer stored on Config post-S477.
        Config {
            nexus_url: server_url.to_string(),
            subscriber_id: "sub-test".to_string(),
            vaults_root,
            daemon_version: "0.3.0-test".to_string(),
            daemon_platform: "test".to_string(),
            last_event_id: None,
            // TODO(B2): populate sync_roots once watch loop iterates them.
            sync_roots: vec![],
        }
    }

    /// T-CS2-12 (Verify-and-repair half), CS-3 form: with no published
    /// handles V&R refuses `NotPublished` and touches no state dir (it never
    /// resolves a sub's dir itself any more).
    #[tokio::test]
    async fn t_cs2_12_verify_repair_unpublished_refuses() {
        let e = verify_repair_with(None).await.unwrap_err();
        assert!(e.contains("Verify and repair refused"), "{e}");
        assert!(e.contains("sync is not running"), "{e}");
        let empty = SyncHandlesSlot::default();
        let e = verify_repair_with(Some(&empty)).await.unwrap_err();
        assert!(e.contains("sync is not running"), "{e}");
    }

    fn sd_for(ws: &TempDir, sub: &str) -> std::sync::Arc<crate::state_io::StateDir> {
        let d = ws.path().join(sub);
        fs::create_dir_all(&d).unwrap();
        crate::state_io::StateDir::for_test(&d)
    }

    /// The pre-CS-3 per-root V&R construction, kept as a test helper so the
    /// per-root walk and aggregate tests keep exercising `VerifyRepair`.
    fn build_verify_repair(
        cfg: &Config,
        token: &str,
        sd: &std::sync::Arc<crate::state_io::StateDir>,
        vault_root: PathBuf,
        subscriber_id: &str,
    ) -> Result<
        (
            VerifyRepair,
            std::sync::Arc<crate::journal_owner::JournalOwner>,
        ),
        String,
    > {
        let api = std::sync::Arc::new(
            crate::api_client::ApiClient::new(&cfg.nexus_url, token).map_err(|e| e.to_string())?,
        );
        let journal = crate::journal_owner::test_owner(sd);
        let shadow = crate::sync_shadow::ShadowStore::load(sd);
        let vr = VerifyRepair::new(
            vault_root,
            api,
            journal.clone(),
            subscriber_id.to_string(),
            VerifyRepairConfig::default(),
        )
        .with_shadow(shadow);
        Ok((vr, journal))
    }

    #[test]
    fn resolve_workspace_root_does_not_panic() {
        // Smoke: at minimum we get *some* path ending in "Nexus".
        let p = resolve_workspace_root();
        assert!(p
            .components()
            .next_back()
            .and_then(|c| c.as_os_str().to_str())
            .map(|s| s == "Nexus")
            .unwrap_or(false));
    }

    #[tokio::test]
    async fn build_verify_repair_opens_journal_in_held_dir_and_returns_runnable() {
        // Tempdir for the vault parent + tempdir for the workspace; assert
        // the journal path's parent is created on disk and the returned vr
        // is well-formed enough to invoke build_local_manifest.
        let vault_parent = TempDir::new().unwrap();
        let workspace = TempDir::new().unwrap();
        // The vault dir itself (vaults_root/vault_name) must exist for the
        // manifest walk to enumerate anything.
        let vault_name = "TestVault";
        fs::create_dir_all(vault_parent.path().join(vault_name)).unwrap();
        fs::write(vault_parent.path().join(vault_name).join("a.md"), b"hello").unwrap();

        let srv = Server::new_async().await;
        let cfg = make_config(&srv.url(), vault_parent.path().to_path_buf(), vault_name);

        // B4: pass vault_root explicitly (the vault folder, not the parent).
        // The manifest entry for TestVault/a.md is "a.md" when rooted at the
        // vault folder directly (post-B4 per-root semantics).
        let vault_root = vault_parent.path().join(vault_name);
        let (vr, _journal) = build_verify_repair(
            &cfg,
            "vsk_test",
            &sd_for(&workspace, &cfg.subscriber_id),
            vault_root,
            &cfg.subscriber_id,
        )
        .unwrap();
        let m = vr.build_local_manifest().unwrap();
        assert_eq!(m.len(), 1);
        // B4: vault_root = vaults_root/vault_name → manifest path is just "a.md"
        // (no vault folder prefix, since vault_root IS the vault folder).
        assert_eq!(m[0].path, "a.md");

        // CS-2: the file is created by `PushJournal::open` in the HELD dir;
        // no directory is created at verify time.
        assert!(workspace
            .path()
            .join(&cfg.subscriber_id)
            .join("push_journal.jsonl")
            .exists());
    }

    #[tokio::test]
    async fn build_verify_repair_runs_against_mock_server() {
        let vault_parent = TempDir::new().unwrap();
        let workspace = TempDir::new().unwrap();
        let vault_name = "TestVault";
        fs::create_dir_all(vault_parent.path().join(vault_name)).unwrap();
        fs::write(vault_parent.path().join(vault_name).join("a.md"), b"hello").unwrap();

        let mut srv = Server::new_async().await;
        let _m = srv
            .mock("POST", "/api/sync/reconcile-batch")
            .with_status(200)
            .with_body(r#"{"deltas":[]}"#)
            .create_async()
            .await;

        let cfg = make_config(&srv.url(), vault_parent.path().to_path_buf(), vault_name);
        // B4: pass the per-root vault_root (the vault folder itself).
        let vault_root = vault_parent.path().join(vault_name);
        let (vr, _journal) = build_verify_repair(
            &cfg,
            "vsk_test",
            &sd_for(&workspace, &cfg.subscriber_id),
            vault_root,
            &cfg.subscriber_id,
        )
        .unwrap();
        let report = vr.run().await.unwrap();
        assert_eq!(report.files_scanned, 1);
        assert_eq!(report.files_in_sync, 1);
        assert_eq!(report.modify_count, 0);
        assert_eq!(report.add_count, 0);
        assert_eq!(report.delete_count, 0);
    }

    #[tokio::test]
    async fn verify_repair_run_returns_err_when_no_config() {
        // We can't fully mock `default_config_path()` (it reads OS dirs),
        // so the deterministic-no-config probe goes through build_verify_repair
        // with a config the loader can't find. The public command's no-config
        // branch is exercised by attempting load_from on a non-existent path.
        let nope =
            std::env::temp_dir().join(format!("vault-sync-no-such-config-{}.toml", uuid_like()));
        // Belt-and-suspenders: ensure the path REALLY doesn't exist.
        let _ = std::fs::remove_file(&nope);
        let result = config::Config::load_from(&nope);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn report_is_serializable_to_json() {
        // Round-trip a populated report through serde_json. Locks in the
        // Serialize derive — without it, the Tauri command can't return
        // VerifyRepairReport directly.
        let vault_parent = TempDir::new().unwrap();
        let workspace = TempDir::new().unwrap();
        let vault_name = "RoundTripVault";
        fs::create_dir_all(vault_parent.path().join(vault_name)).unwrap();
        fs::write(vault_parent.path().join(vault_name).join("a.md"), b"hello").unwrap();
        let mut srv = Server::new_async().await;
        let _m = srv
            .mock("POST", "/api/sync/reconcile-batch")
            .with_status(200)
            .with_body(r#"{"deltas":[]}"#)
            .create_async()
            .await;
        let cfg = make_config(&srv.url(), vault_parent.path().to_path_buf(), vault_name);
        // B4: pass per-root vault_root explicitly.
        let vault_root = vault_parent.path().join(vault_name);
        let (vr, _journal) = build_verify_repair(
            &cfg,
            "vsk_test",
            &sd_for(&workspace, &cfg.subscriber_id),
            vault_root,
            &cfg.subscriber_id,
        )
        .unwrap();
        let report = vr.run().await.unwrap();

        let json = serde_json::to_string(&report).expect("serialize");
        assert!(json.contains("\"files_scanned\":1"));
        let back: VerifyRepairReport = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back.files_scanned, report.files_scanned);
        assert_eq!(back.modify_count, report.modify_count);
        assert_eq!(back.elapsed_ms, report.elapsed_ms);
    }

    // Local helper: avoids pulling in `uuid` as a dev-dep just for a unique
    // path component. Uses the system time + thread id.
    fn uuid_like() -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        format!("{nanos:x}")
    }

    // -------------------------------------------------------------------
    // list_conflicts tests (Agent L) — drive `list_conflicts_in` against a
    // tempdir so we don't need a platform config file.
    // -------------------------------------------------------------------

    #[test]
    fn list_conflicts_empty_vault_returns_empty() {
        let tmp = TempDir::new().unwrap();
        let out = list_conflicts_in(tmp.path()).unwrap();
        assert!(out.is_empty(), "expected empty list, got: {out:?}");
    }

    #[test]
    fn list_conflicts_lists_one_conflict() {
        let tmp = TempDir::new().unwrap();
        fs::write(
            tmp.path().join("note.conflict-from-morpheus-1.md"),
            b"hello world",
        )
        .unwrap();
        let out = list_conflicts_in(tmp.path()).unwrap();
        assert_eq!(out.len(), 1, "got: {out:?}");
        let e = &out[0];
        assert_eq!(e.path, "note.conflict-from-morpheus-1.md");
        assert_eq!(e.from_device, "morpheus");
        assert_eq!(e.from_lsn, 1);
        assert_eq!(e.original_path, "note.md");
        assert_eq!(e.size_bytes, b"hello world".len() as u64);
    }

    #[test]
    fn list_conflicts_lists_multiple_recursively() {
        let tmp = TempDir::new().unwrap();
        fs::create_dir_all(tmp.path().join("a/b/c")).unwrap();
        fs::write(tmp.path().join("root.conflict-from-morpheus-1.md"), b"r").unwrap();
        fs::write(tmp.path().join("a/mid.conflict-from-trinity-22.md"), b"mm").unwrap();
        fs::write(
            tmp.path().join("a/b/c/deep.conflict-from-switch-99.md"),
            b"ddd",
        )
        .unwrap();
        // Non-matches
        fs::write(tmp.path().join("regular.md"), b"x").unwrap();
        fs::write(tmp.path().join("a/old.conflict-2024-01-01.md"), b"x").unwrap();
        fs::write(tmp.path().join("a/b/wrong.conflict-from-bar-1.txt"), b"x").unwrap();

        let out = list_conflicts_in(tmp.path()).unwrap();
        assert_eq!(out.len(), 3, "got: {out:?}");
        let paths: Vec<&str> = out.iter().map(|e| e.path.as_str()).collect();
        assert_eq!(
            paths,
            vec![
                "a/b/c/deep.conflict-from-switch-99.md",
                "a/mid.conflict-from-trinity-22.md",
                "root.conflict-from-morpheus-1.md",
            ]
        );
        let deep = out
            .iter()
            .find(|e| e.path.ends_with("deep.conflict-from-switch-99.md"))
            .unwrap();
        assert_eq!(deep.from_device, "switch");
        assert_eq!(deep.from_lsn, 99);
        assert_eq!(deep.original_path, "a/b/c/deep.md");
    }

    #[test]
    fn list_conflicts_ignores_non_conflict_md_files() {
        let tmp = TempDir::new().unwrap();
        fs::write(tmp.path().join("a.md"), b"x").unwrap();
        fs::write(tmp.path().join("b.md"), b"x").unwrap();
        fs::write(tmp.path().join("c.txt"), b"x").unwrap();
        let out = list_conflicts_in(tmp.path()).unwrap();
        assert!(out.is_empty(), "got: {out:?}");
    }

    #[test]
    fn list_conflicts_populates_size_and_mtime() {
        let tmp = TempDir::new().unwrap();
        let payload = b"some content of known length";
        fs::write(tmp.path().join("n.conflict-from-dev-1.md"), payload).unwrap();
        let out = list_conflicts_in(tmp.path()).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].size_bytes, payload.len() as u64);
        assert!(out[0].mtime.is_some(), "mtime should be populated");
    }

    #[test]
    fn list_conflicts_collision_suffix_files_listed_with_inner_lsn() {
        // `note.conflict-from-morpheus-1-2.md` is a re-stash — parser returns
        // lsn=1, and `list_conflicts_in` reports it as a separate entry.
        let tmp = TempDir::new().unwrap();
        fs::write(tmp.path().join("note.conflict-from-morpheus-1.md"), b"a").unwrap();
        fs::write(tmp.path().join("note.conflict-from-morpheus-1-2.md"), b"b").unwrap();
        let out = list_conflicts_in(tmp.path()).unwrap();
        assert_eq!(out.len(), 2, "got: {out:?}");
        for e in &out {
            assert_eq!(e.from_device, "morpheus");
            assert_eq!(e.from_lsn, 1);
            assert_eq!(e.original_path, "note.md");
        }
    }

    #[test]
    fn list_conflicts_missing_vault_root_returns_empty() {
        let out = list_conflicts_in(Path::new("/definitely/does/not/exist/vault-xyz-123")).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn list_conflicts_returns_err_when_no_config() {
        // The outer `list_conflicts` tauri::command relies on
        // `Config::load_from(default_config_path())` — proxy that here.
        let nope = std::env::temp_dir().join(format!("vs-no-conflict-config-{}.toml", uuid_like()));
        let _ = std::fs::remove_file(&nope);
        let result = config::Config::load_from(&nope);
        assert!(result.is_err());
    }

    // ─── B4: per-sync_root build_verify_repair tests ─────────────────────

    /// B4 core: `build_verify_repair` accepts an explicit per-root `vault_root`
    /// and `subscriber_id`. Two independently created VerifyRepair instances
    /// (one per sync_root) must each walk only their own directory.
    #[tokio::test]
    async fn build_verify_repair_per_root_walks_own_directory_only() {
        let root_a = TempDir::new().unwrap();
        let root_b = TempDir::new().unwrap();
        let workspace = TempDir::new().unwrap();

        fs::write(root_a.path().join("note_a.md"), b"in-root-a").unwrap();
        fs::write(root_b.path().join("note_b.md"), b"in-root-b").unwrap();

        let srv = Server::new_async().await;
        // Use the same nexus_url for both; subscriber_ids differ.
        let cfg = Config {
            nexus_url: srv.url(),
            subscriber_id: "sub-fallback".to_string(),
            vaults_root: root_a.path().to_path_buf(), // not used by build_verify_repair post-B4
            daemon_version: "0.4.0-test".to_string(),
            daemon_platform: "test".to_string(),
            last_event_id: None,
            sync_roots: vec![],
        };

        let (vr_a, _j_a) = build_verify_repair(
            &cfg,
            "vsk_test",
            &sd_for(&workspace, "sub-a"),
            root_a.path().to_path_buf(),
            "sub-a",
        )
        .unwrap();

        let (vr_b, _j_b) = build_verify_repair(
            &cfg,
            "vsk_test",
            &sd_for(&workspace, "sub-b"),
            root_b.path().to_path_buf(),
            "sub-b",
        )
        .unwrap();

        let manifest_a = vr_a.build_local_manifest().unwrap();
        let manifest_b = vr_b.build_local_manifest().unwrap();

        let paths_a: Vec<&str> = manifest_a.iter().map(|e| e.path.as_str()).collect();
        let paths_b: Vec<&str> = manifest_b.iter().map(|e| e.path.as_str()).collect();

        assert_eq!(
            paths_a,
            vec!["note_a.md"],
            "root_a must only see its own note; got {paths_a:?}"
        );
        assert_eq!(
            paths_b,
            vec!["note_b.md"],
            "root_b must only see its own note; got {paths_b:?}"
        );

        // No cross-contamination.
        assert!(
            !paths_a.contains(&"note_b.md"),
            "root_a manifest must not include root_b files"
        );
        assert!(
            !paths_b.contains(&"note_a.md"),
            "root_b manifest must not include root_a files"
        );
    }

    /// B4: verify_repair_run aggregates over all sync_roots. Build the VR
    /// for a 2-root config using the helper and confirm both roots are covered.
    /// Uses `build_verify_repair` directly (the Tauri command itself can't be
    /// invoked in unit tests without a running Tauri runtime).
    #[tokio::test]
    async fn build_verify_repair_two_root_aggregate() {
        let root_a = TempDir::new().unwrap();
        let root_b = TempDir::new().unwrap();
        let workspace = TempDir::new().unwrap();

        // root_a: 2 files. root_b: 1 file.
        fs::write(root_a.path().join("alpha.md"), b"a1").unwrap();
        fs::write(root_a.path().join("beta.md"), b"a2").unwrap();
        fs::write(root_b.path().join("gamma.md"), b"b1").unwrap();

        let mut srv = Server::new_async().await;
        let _mock = srv
            .mock("POST", "/api/sync/reconcile-batch")
            .with_status(200)
            .with_body(r#"{"deltas":[]}"#)
            .expect_at_least(2) // once per sync_root
            .create_async()
            .await;

        let cfg = Config {
            nexus_url: srv.url(),
            subscriber_id: "sub-top".to_string(),
            vaults_root: root_a.path().to_path_buf(),
            daemon_version: "0.4.0-test".to_string(),
            daemon_platform: "test".to_string(),
            last_event_id: None,
            sync_roots: vec![
                crate::config::SyncRoot {
                    path: root_a.path().to_path_buf(),
                    route: String::new(),
                    subscriber_id: "sub-a".to_string(),
                },
                crate::config::SyncRoot {
                    path: root_b.path().to_path_buf(),
                    route: "dev".to_string(),
                    subscriber_id: String::new(), // → falls back to sub-top
                },
            ],
        };

        // Derive pairs as verify_repair_run does internally.
        let pairs =
            crate::verify_repair::roots_to_reconcile_pairs(&cfg.sync_roots, &cfg.subscriber_id);
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0].1, "sub-a");
        assert_eq!(pairs[1].1, "sub-top"); // fallback

        // Run each root and aggregate (mirrors verify_repair_run logic).
        let mut total_scanned = 0usize;
        for (vault_root, subscriber_id) in &pairs {
            let (vr, _j) = build_verify_repair(
                &cfg,
                "vsk_test",
                &sd_for(&workspace, subscriber_id),
                vault_root.clone(),
                subscriber_id,
            )
            .unwrap();
            let report = vr.run().await.unwrap();
            total_scanned += report.files_scanned;
        }
        // root_a has 2 files, root_b has 1 → total = 3.
        assert_eq!(total_scanned, 3, "aggregate must cover all sync_roots");
        _mock.assert_async().await;
    }
}
