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

/// Build a `VerifyRepair` from explicit inputs (config + token + workspace +
/// vault_root). Pulled out of the `#[tauri::command]` for unit-testability
/// — the command itself does the env-driven I/O (config load + keyring read)
/// and delegates here. Mirrors the `pair` / `pair_inner` split in `pairing.rs`.
///
/// B4 (Nexus Sync): `vault_root` is now a separate parameter so callers can
/// pass each `sync_root.path` independently, running one VerifyRepair per
/// root instead of a single global `cfg.vaults_root` sweep.
///
/// `subscriber_id` is also separated from `cfg.subscriber_id` so the caller
/// can supply the *effective* per-root subscriber (resolved via
/// `effective_subscriber_id(&root, &cfg)` in `lib.rs`).
pub(crate) fn build_verify_repair(
    cfg: &config::Config,
    token: &str,
    workspace_root: PathBuf,
    vault_root: PathBuf,
    subscriber_id: &str,
) -> Result<(VerifyRepair, Arc<Mutex<PushJournal>>), String> {
    let api = ApiClient::new(&cfg.nexus_url, token).map_err(|e| e.to_string())?;
    let api_arc = Arc::new(api);

    let journal_path = push_journal_path(&workspace_root, subscriber_id);
    if let Some(parent) = journal_path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("create_dir_all({}) failed: {e}", parent.display()))?;
    }
    let journal = PushJournal::open(&journal_path).map_err(|e| e.to_string())?;
    let journal_arc = Arc::new(Mutex::new(journal));

    // fix/reconcile-server-wins-shadow: wire the (read-only) shadow store so the
    // owner-invoked "Verify and repair" makes the SAME push-vs-pull direction
    // call the periodic backstop does. Loads the SAME on-disk shadow file the
    // daemon maintains (path → last-synced server hash); reads the persisted
    // (last-flushed, ≤30s old) state. With shadow but no materializer, the manual
    // button correctly PUSHES genuine local edits (shadow == server) and DETECTS
    // (counts) stale locals — their server-wins PULL is executed by the periodic
    // backstop (which is wired with a materializer). This avoids constructing a
    // materializer here without the server health snapshot (mode/shadow_path),
    // which could write to the wrong place.
    let shadow_path = workspace_root
        .join(".lattice-runtime")
        .join(subscriber_id)
        .join("sync-state")
        .join("shadow_hashes.json");
    let shadow = crate::sync_shadow::ShadowStore::load(shadow_path);

    let vr = VerifyRepair::new(
        vault_root,
        api_arc,
        journal_arc.clone(),
        subscriber_id.to_string(),
        VerifyRepairConfig::default(),
    )
    .with_shadow(shadow);
    Ok((vr, journal_arc))
}

/// Owner-invokable Tauri command. Loads the daemon config, pulls the token
/// from the keyring (or file fallback), then runs `VerifyRepair::run`
/// end-to-end for EACH sync_root (B4 per-root semantics).
///
/// Reports from individual roots are merged into a single aggregate
/// `VerifyRepairReport` so the tray click handler can render a combined
/// summary dialog. On any failure the `String` body is logged + surfaced to
/// the dialog ("Verify and repair failed: …"); the daemon does NOT crash.
///
/// B4 (Nexus Sync): iterates `cfg.sync_roots` so every configured root gets
/// its own manifest walk + reconcile call. Each root uses its effective
/// subscriber_id (root's own when set, else top-level fallback) as the
/// `device_id` passed to the server reconcile endpoint, ensuring the server
/// maps the manifest to the correct subscriber namespace.
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

    // B4: iterate sync_roots, running one VerifyRepair per root.
    // Derive the per-root (vault_root, subscriber_id) pairs using the
    // same priority logic as lib::effective_subscriber_id.
    let pairs = crate::verify_repair::roots_to_reconcile_pairs(&cfg.sync_roots, &cfg.subscriber_id);

    // If sync_roots is empty (should not happen post-B1 back-compat, but
    // handle gracefully), fall back to the legacy single-root path using
    // cfg.vaults_root + cfg.subscriber_id.
    let pairs = if pairs.is_empty() {
        vec![(cfg.vaults_root.clone(), cfg.subscriber_id.clone())]
    } else {
        pairs
    };

    // Aggregate all per-root reports into one combined report.
    let mut combined = VerifyRepairReport::default();
    for (vault_root, subscriber_id) in &pairs {
        let (vr, _journal) = build_verify_repair(
            &cfg,
            &token,
            workspace_root.clone(),
            vault_root.clone(),
            subscriber_id,
        )?;
        let report = vr.run().await.map_err(|e| e.to_string())?;
        tracing::info!(
            root = %vault_root.display(),
            subscriber_id = %subscriber_id,
            files_scanned = report.files_scanned,
            modify_count = report.modify_count,
            add_count = report.add_count,
            substrate_refused_count = report.substrate_refused_count,
            elapsed_ms = report.elapsed_ms,
            "verify_repair_run: per-root complete"
        );
        // Accumulate into combined report.
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
        roots_scanned = pairs.len(),
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

        // B4: pass vault_root explicitly (the vault folder, not the parent).
        // The manifest entry for TestVault/a.md is "a.md" when rooted at the
        // vault folder directly (post-B4 per-root semantics).
        let vault_root = vault_parent.path().join(vault_name);
        let (vr, _journal) = build_verify_repair(
            &cfg,
            "vsk_test",
            workspace.path().to_path_buf(),
            vault_root,
            &cfg.subscriber_id,
        )
        .unwrap();
        let m = vr.build_local_manifest().unwrap();
        assert_eq!(m.len(), 1);
        // B4: vault_root = vaults_root/vault_name → manifest path is just "a.md"
        // (no vault folder prefix, since vault_root IS the vault folder).
        assert_eq!(m[0].path, "a.md");

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
            workspace.path().to_path_buf(),
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
            workspace.path().to_path_buf(),
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
            workspace.path().to_path_buf(),
            root_a.path().to_path_buf(),
            "sub-a",
        )
        .unwrap();

        let (vr_b, _j_b) = build_verify_repair(
            &cfg,
            "vsk_test",
            workspace.path().to_path_buf(),
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
                workspace.path().to_path_buf(),
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
