//! D1 (Piece 1, v0.4.28): byte-strict R1 "alignment pull".
//!
//! The v0.4.27 R1 basis is NORMALIZED (frontmatter + CRLF/BOM), so a local
//! CRLF file whose content normalized-equals the server LF canonical is a
//! permanent Noop on the pull side while every byte-strict comparer (server
//! CAS, reconcile-batch fs_hash) keeps seeing drift - the B1' alternation.
//! v0.4.28 splits R1: raw-equal -> Noop (unchanged); normalized-equal but
//! raw-unequal -> rewrite local to the server's exact canonical bytes through
//! the existing persist machinery (echo-guarded, locked, atomic), no stash.

use std::sync::Arc;
use tempfile::TempDir;
use vault_sync_daemon::api_client::NotePayload;
use vault_sync_daemon::echo_guard::EchoGuard;
use vault_sync_daemon::materializer::{
    MaterializeOutcome, Materializer, MaterializerConfig, MaterializerMode, SkipReason,
};
use vault_sync_daemon::sync_shadow::ShadowStore;

const VAULT: &str = "Mainframe";

fn sha256_hex(b: &[u8]) -> String {
    use sha2::Digest;
    hex::encode(sha2::Sha256::digest(b))
}

struct Fixture {
    _vaults: TempDir,
    _ws: TempDir,
    _shadow_dir: TempDir,
    mat: Materializer,
    shadow: Arc<ShadowStore>,
    echo: Arc<EchoGuard>,
    vault_root: std::path::PathBuf,
}

fn fixture() -> Fixture {
    let vaults = TempDir::new().unwrap();
    let ws = TempDir::new().unwrap();
    let shadow_dir = TempDir::new().unwrap();
    std::fs::create_dir_all(vaults.path().join(VAULT)).unwrap();
    let shadow = ShadowStore::load(shadow_dir.path().join("shadow.json"));
    let echo = Arc::new(EchoGuard::new());
    let mat = Materializer::new(
        vaults.path().to_path_buf(),
        Some("shadow/".into()),
        MaterializerMode::Live,
        ws.path().to_path_buf(),
        "subscriber-itest".to_string(),
        MaterializerConfig {
            device_id: "test-host".into(),
            ..Default::default()
        },
    )
    .with_shadow_store(shadow.clone())
    .with_echo_guard(echo.clone());
    let vault_root = vaults.path().to_path_buf();
    Fixture {
        _vaults: vaults,
        _ws: ws,
        _shadow_dir: shadow_dir,
        mat,
        shadow,
        echo,
        vault_root,
    }
}

fn payload(rel: &str, canonical: &str) -> NotePayload {
    NotePayload {
        path: rel.to_string(),
        frontmatter: serde_json::Value::Null,
        body: canonical.to_string(),
        sha256: sha256_hex(canonical.as_bytes()),
        modified: "2026-07-01T00:00:00Z".to_string(),
        file_mtime: None,
        enriched_body: Some(canonical.to_string()),
        created: None,
    }
}

fn conflict_stashes_under(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    fn walk(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
        if let Ok(rd) = std::fs::read_dir(dir) {
            for e in rd.flatten() {
                let p = e.path();
                if p.is_dir() {
                    walk(&p, out);
                } else if p
                    .file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.contains(".conflict-from-"))
                    .unwrap_or(false)
                {
                    out.push(p);
                }
            }
        }
    }
    let mut out = Vec::new();
    walk(root, &mut out);
    out
}

/// D1 core: local CRLF, server LF (normalized-equal, raw-unequal) -> the local
/// file is rewritten byte-equal to the server canonical, shadow = server hash,
/// NO stash, and the write is echo-guarded (no push enqueued from the event).
#[test]
fn test_r1_alignment_pull_rewrites_crlf_local() {
    let fx = fixture();
    let rel = format!("{VAULT}/notes/a.md");
    let canonical = "line one\nline two\n";
    let local_crlf = "line one\r\nline two\r\n";
    let abs = fx.vault_root.join(&rel);
    std::fs::create_dir_all(abs.parent().unwrap()).unwrap();
    std::fs::write(&abs, local_crlf).unwrap();
    // B1' precondition state: the old daemon recorded the LOCAL raw sha on an
    // idempotent accept, so shadow != server.
    fx.shadow.record(&rel, &sha256_hex(local_crlf.as_bytes()));

    let p = payload(&rel, canonical);
    let outcome = fx.mat.write(&p).unwrap();

    assert_eq!(
        outcome,
        MaterializeOutcome::AlignedToCanonical { path: abs.clone() },
        "normalized-equal but raw-unequal must be an ALIGNMENT PULL"
    );
    assert_eq!(
        std::fs::read(&abs).unwrap(),
        canonical.as_bytes(),
        "local must now be byte-equal to the server canonical"
    );
    assert_eq!(
        fx.shadow.get(&rel).as_deref(),
        Some(p.sha256.as_str()),
        "shadow must record the server hash"
    );
    assert!(
        conflict_stashes_under(&fx.vault_root).is_empty(),
        "alignment pull must NOT stash (zero content difference by construction)"
    );
    // Echo suppression: the guard holds the (path, canonical sha) entry, so
    // the file_watcher event from this write is recognized as an echo and no
    // push is enqueued.
    assert!(
        fx.echo.is_echo(&rel, &p.sha256),
        "the aligned write must be echo-guarded (no push from the FS event)"
    );
}

/// Byte-identical local stays a NO-WRITE Noop: no mtime churn, same outcome
/// as v0.4.27.
#[test]
fn test_r1_raw_equal_still_noop() {
    let fx = fixture();
    let rel = format!("{VAULT}/notes/b.md");
    let canonical = "same bytes\n";
    let abs = fx.vault_root.join(&rel);
    std::fs::create_dir_all(abs.parent().unwrap()).unwrap();
    std::fs::write(&abs, canonical).unwrap();
    let mtime_before = std::fs::metadata(&abs).unwrap().modified().unwrap();

    let p = payload(&rel, canonical);
    let outcome = fx.mat.write(&p).unwrap();

    assert_eq!(
        outcome,
        MaterializeOutcome::Skipped(SkipReason::IdenticalToLocal),
        "byte-identical must stay a Noop skip"
    );
    assert_eq!(
        std::fs::metadata(&abs).unwrap().modified().unwrap(),
        mtime_before,
        "a raw-equal Noop must not touch the file (no mtime churn)"
    );
    assert_eq!(fx.shadow.get(&rel).as_deref(), Some(p.sha256.as_str()));
}

/// Anti-strip interplay (spec D1 acceptance): an alignment pull can never
/// strip frontmatter BY CONSTRUCTION - a local file WITH frontmatter and a
/// server body WITHOUT it are not normalized-equal, so the flow lands in the
/// S513 anti-strip guard (PreserveLocalEdit), never in the alignment rewrite.
#[test]
fn test_alignment_pull_respects_anti_strip() {
    let fx = fixture();
    let rel = format!("{VAULT}/notes/c.md");
    let local = "---\ntitle: keep me\n---\nbody\r\n"; // frontmatter + CRLF
    let server = "body\n"; // frontmatter-stripped server copy
    let abs = fx.vault_root.join(&rel);
    std::fs::create_dir_all(abs.parent().unwrap()).unwrap();
    std::fs::write(&abs, local).unwrap();
    // No shadow entry: raw decision would be R5 Conflict, and the pull would
    // strip -> the guard downgrades to PreserveLocalEdit.

    let p = payload(&rel, server);
    let outcome = fx.mat.write(&p).unwrap();

    assert_eq!(
        outcome,
        MaterializeOutcome::Skipped(SkipReason::LocalEditPreserved),
        "a frontmatter-stripping server copy must be refused, never 'aligned'"
    );
    assert_eq!(
        std::fs::read(&abs).unwrap(),
        local.as_bytes(),
        "local file must be untouched"
    );
}
