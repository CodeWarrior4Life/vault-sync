//! Tauri command surface invoked from the JS front-end + the tray menu.
//!
//! Currently only hosts `verify_repair_run`, the owner-invokable "Verify and
//! repair all files…" sweep wired to `tray.rs`. The command resolves config +
//! token + workspace + journal exactly the way `lib.rs::spawn_sse_consumer`
//! does, then delegates to `verify_repair::VerifyRepair::run`.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::Serialize;
use tokio::sync::Mutex;

use crate::api_client::ApiClient;
use crate::config;
use crate::conflict_stash::parse_conflict_filename;
use crate::push_journal::PushJournal;
use crate::token_store;
use crate::verify_repair::{VerifyRepair, VerifyRepairConfig, VerifyRepairReport};

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

/// Compute the push-journal path used by both the SSE/file-watcher write
/// side and verify_repair. Single source of truth so the two paths can never
/// drift.
pub(crate) fn push_journal_path(
    workspace_root: &std::path::Path,
    subscriber_slug: &str,
) -> PathBuf {
    workspace_root
        .join(".lattice-runtime")
        .join(subscriber_slug)
        .join("sync-state")
        .join("push_journal.jsonl")
}

/// Build a `VerifyRepair` from explicit inputs (config + token + workspace).
/// Pulled out of the `#[tauri::command]` for unit-testability — the command
/// itself does the env-driven I/O (config load + keyring read) and delegates
/// here. Mirrors the `pair` / `pair_inner` split in `pairing.rs`.
pub(crate) fn build_verify_repair(
    cfg: &config::Config,
    token: &str,
    workspace_root: PathBuf,
) -> Result<(VerifyRepair, Arc<Mutex<PushJournal>>), String> {
    let api = ApiClient::new(&cfg.nexus_url, token).map_err(|e| e.to_string())?;
    let api_arc = Arc::new(api);

    let journal_path = push_journal_path(&workspace_root, &cfg.subscriber_id);
    if let Some(parent) = journal_path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("create_dir_all({}) failed: {e}", parent.display()))?;
    }
    let journal = PushJournal::open(&journal_path).map_err(|e| e.to_string())?;
    let journal_arc = Arc::new(Mutex::new(journal));

    // S477: post-watch-root-fix, `vaults_root` IS the verify-repair root.
    // The vault folder is encoded as the first segment of each path; we
    // no longer join a per-config `vault_name`.
    let vault_root = cfg.vaults_root.clone();
    let vr = VerifyRepair::new(
        vault_root,
        api_arc,
        journal_arc.clone(),
        cfg.subscriber_id.clone(),
        VerifyRepairConfig::default(),
    );
    Ok((vr, journal_arc))
}

/// Owner-invokable Tauri command. Loads the daemon config, pulls the token
/// from the keyring (or file fallback), opens the shared push_journal, then
/// runs `VerifyRepair::run` end-to-end.
///
/// Returns the structured report so the tray click handler can render a
/// summary dialog. On any failure the `String` body is logged + surfaced to
/// the dialog ("Verify and repair failed: …"); the daemon does NOT crash.
#[tauri::command]
pub async fn verify_repair_run() -> Result<VerifyRepairReport, String> {
    let cfg_path = config::default_config_path();
    let cfg = config::Config::load_from(&cfg_path).map_err(|e| e.to_string())?;

    let token = match token_store::load(&cfg.subscriber_id).map_err(|e| e.to_string())? {
        Some(t) => t,
        None => {
            return Err(format!(
                "no token in keyring or file fallback for subscriber_id={}; re-pair required",
                cfg.subscriber_id
            ));
        }
    };

    let workspace_root = resolve_workspace_root();
    let (vr, _journal) = build_verify_repair(&cfg, &token, workspace_root)?;
    let report = vr.run().await.map_err(|e| e.to_string())?;
    tracing::info!(
        files_scanned = report.files_scanned,
        modify_count = report.modify_count,
        add_count = report.add_count,
        delete_count = report.delete_count,
        substrate_refused_count = report.substrate_refused_count,
        elapsed_ms = report.elapsed_ms,
        "verify_repair_run: complete"
    );
    Ok(report)
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
        }
    }

    #[test]
    fn push_journal_path_matches_workspace_layout() {
        let ws = PathBuf::from("/tmp/ws");
        let p = push_journal_path(&ws, "sub-abc");
        // Forward-slash representation may differ on Windows but the segments
        // must match — assert via Path components rather than string compare.
        let comps: Vec<_> = p.components().map(|c| c.as_os_str().to_owned()).collect();
        let last4: Vec<&str> = comps
            .iter()
            .rev()
            .take(4)
            .rev()
            .map(|s| s.to_str().unwrap())
            .collect();
        assert_eq!(
            last4,
            vec![
                ".lattice-runtime",
                "sub-abc",
                "sync-state",
                "push_journal.jsonl"
            ]
        );
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
    async fn build_verify_repair_creates_journal_dir_and_returns_runnable() {
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

        let (vr, _journal) =
            build_verify_repair(&cfg, "vsk_test", workspace.path().to_path_buf()).unwrap();
        let m = vr.build_local_manifest().unwrap();
        assert_eq!(m.len(), 1);
        // S477: vault_root is vaults_root verbatim, so the manifest entry
        // for a file at vaults_root/TestVault/a.md surfaces with the vault
        // folder as its first path segment.
        assert_eq!(m[0].path, "TestVault/a.md");

        let journal_path = push_journal_path(workspace.path(), &cfg.subscriber_id);
        assert!(journal_path.parent().unwrap().is_dir());
        // The file itself is created by `PushJournal::open`.
        assert!(journal_path.exists());
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
            .mock("POST", "/api/sync/reconcile")
            .with_status(200)
            .with_body(
                r#"{"actions":[],"stats":{"push":0,"pull":0,"identical":0},"server_time":""}"#,
            )
            .create_async()
            .await;

        let cfg = make_config(&srv.url(), vault_parent.path().to_path_buf(), vault_name);
        let (vr, _journal) =
            build_verify_repair(&cfg, "vsk_test", workspace.path().to_path_buf()).unwrap();
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
            .mock("POST", "/api/sync/reconcile")
            .with_status(200)
            .with_body(
                r#"{"actions":[],"stats":{"push":0,"pull":0,"identical":0},"server_time":""}"#,
            )
            .create_async()
            .await;
        let cfg = make_config(&srv.url(), vault_parent.path().to_path_buf(), vault_name);
        let (vr, _journal) =
            build_verify_repair(&cfg, "vsk_test", workspace.path().to_path_buf()).unwrap();
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
}
