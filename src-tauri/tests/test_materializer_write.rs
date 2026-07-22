//! Integration test surface for the v0.3 materializer. The bulk of unit
//! coverage now lives in `src/materializer.rs::tests`; this file keeps a
//! cross-crate-boundary smoke test against the public API.

use tempfile::TempDir;
use vault_sync_daemon::api_client::NotePayload;
use vault_sync_daemon::materializer::{
    MaterializeOutcome, Materializer, MaterializerConfig, MaterializerError, MaterializerMode,
};

const VAULT: &str = "Mainframe";
const SLUG: &str = "subscriber-itest";

fn mk(vaults_tmp: &TempDir, workspace_tmp: &TempDir, mode: MaterializerMode) -> Materializer {
    std::fs::create_dir_all(vaults_tmp.path().join(VAULT)).unwrap();
    Materializer::new(
        vaults_tmp.path().to_path_buf(),
        Some("shadow/".into()),
        mode,
        workspace_tmp.path().to_path_buf(),
        SLUG.to_string(),
        MaterializerConfig {
            device_id: "morpheus".into(),
            ..Default::default()
        },
    )
}

fn sha256_hex(s: &str) -> String {
    use sha2::Digest;
    hex::encode(sha2::Sha256::digest(s.as_bytes()))
}

fn payload(path: &str, body: &str) -> NotePayload {
    let fm = serde_json::json!({"title": "Test", "tags": ["a", "b"]});
    let fm_yaml = serde_yaml::to_string(&fm).unwrap();
    let serialized = format!("---\n{fm_yaml}---\n\n{body}");
    NotePayload {
        path: path.into(),
        frontmatter: fm,
        body: body.into(),
        sha256: sha256_hex(&serialized),
        modified: Some("2026-05-27T00:00:00Z".into()),
        file_mtime: None,
        created: None,
        change_seq: None,
        enriched_body: Some(serialized),
    }
}

#[test]
fn shadow_write_creates_file_with_frontmatter_under_workspace_runtime() {
    let v = TempDir::new().unwrap();
    let w = TempDir::new().unwrap();
    let m = mk(&v, &w, MaterializerMode::Shadow);
    let out = m.write(&payload("01_Inbox/foo.md", "hello")).unwrap();
    let expected = w
        .path()
        .join(".lattice-runtime")
        .join(SLUG)
        .join("shadow/01_Inbox/foo.md");
    assert_eq!(
        out,
        MaterializeOutcome::Wrote {
            path: expected.clone()
        }
    );
    let written = std::fs::read_to_string(&expected).unwrap();
    assert!(written.contains("title: Test"));
    assert!(written.contains("hello"));
}

#[test]
fn live_write_lands_in_vault_tree() {
    let v = TempDir::new().unwrap();
    let w = TempDir::new().unwrap();
    let m = mk(&v, &w, MaterializerMode::Live);
    // Wire paths carry the vault folder as their first segment (sync_roots
    // model); live mode joins straight onto vaults_root.
    let out = m
        .write(&payload(&format!("{VAULT}/01_Inbox/foo.md"), "hello"))
        .unwrap();
    let expected = v.path().join(VAULT).join("01_Inbox/foo.md");
    assert_eq!(
        out,
        MaterializeOutcome::Wrote {
            path: expected.clone()
        }
    );
    assert!(expected.exists());
}

#[test]
fn write_rejects_path_traversal() {
    let v = TempDir::new().unwrap();
    let w = TempDir::new().unwrap();
    let m = mk(&v, &w, MaterializerMode::Shadow);
    let np = payload("../escape.md", "x");
    assert!(matches!(
        m.write(&np),
        Err(MaterializerError::PathTraversal(_))
    ));
}

#[test]
fn write_materializes_former_substrate_paths_as_content() {
    // "substrate must sync" (2026-06-20): the substrate fence is lifted. Paths
    // that used to be refused now materialize byte-faithfully like any note,
    // protected by the same conflict-stash / anti-strip machinery.
    let v = TempDir::new().unwrap();
    let w = TempDir::new().unwrap();
    let m = mk(&v, &w, MaterializerMode::Shadow);
    for substrate in &[
        "00_VAULT.md",
        "02_Projects/Nexus/00_VAULT.md",
        "02_Projects/Foo/Family.md",
        "02_Projects/Foo/Mission.md",
        "02_Projects/Protocols/foo.md",
        "_project/x.md",
        "_rapport/people/cyril.md",
    ] {
        let out = m.write(&payload(substrate, "should land now")).unwrap();
        let expected = w
            .path()
            .join(".lattice-runtime")
            .join(SLUG)
            .join(format!("shadow/{substrate}"));
        assert_eq!(
            out,
            MaterializeOutcome::Wrote {
                path: expected.clone()
            },
            "expected Wrote on {substrate}, got {out:?}"
        );
        let written = std::fs::read_to_string(&expected).unwrap();
        assert!(
            written.contains("should land now"),
            "{substrate} body must be written"
        );
    }
}

#[test]
fn delete_renames_to_deleted_ts() {
    let v = TempDir::new().unwrap();
    let w = TempDir::new().unwrap();
    let m = mk(&v, &w, MaterializerMode::Shadow);
    m.write(&payload("01_Inbox/foo.md", "x")).unwrap();
    m.soft_delete("01_Inbox/foo.md").unwrap();
    let shadow_dir = w
        .path()
        .join(".lattice-runtime")
        .join(SLUG)
        .join("shadow/01_Inbox/");
    assert!(!shadow_dir.join("foo.md").exists());
    let entries: Vec<_> = std::fs::read_dir(&shadow_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_string_lossy()
                .starts_with("foo.md.deleted-")
        })
        .collect();
    assert_eq!(entries.len(), 1, "expected one .deleted-* file");
}

#[test]
fn delete_nothing_to_delete_is_not_error() {
    let v = TempDir::new().unwrap();
    let w = TempDir::new().unwrap();
    let m = mk(&v, &w, MaterializerMode::Shadow);
    assert!(m.soft_delete("01_Inbox/never-existed.md").is_ok());
}

#[test]
fn delete_former_substrate_path_now_soft_deletes() {
    // "substrate must sync": soft_delete no longer refuses substrate. After a
    // write, the substrate path soft-deletes to a `.deleted-*` sibling like
    // any content note.
    let v = TempDir::new().unwrap();
    let w = TempDir::new().unwrap();
    let m = mk(&v, &w, MaterializerMode::Shadow);
    m.write(&payload("00_VAULT.md", "x")).unwrap();
    m.soft_delete("00_VAULT.md").unwrap();
    let shadow_dir = w.path().join(".lattice-runtime").join(SLUG).join("shadow/");
    assert!(!shadow_dir.join("00_VAULT.md").exists());
    let deleted: Vec<_> = std::fs::read_dir(&shadow_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_string_lossy()
                .starts_with("00_VAULT.md.deleted-")
        })
        .collect();
    assert_eq!(
        deleted.len(),
        1,
        "expected one .deleted-* file for substrate"
    );
}
