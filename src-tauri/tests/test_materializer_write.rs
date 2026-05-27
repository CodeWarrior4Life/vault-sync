use tempfile::TempDir;
use vault_sync_daemon::api_client::NotePayload;
use vault_sync_daemon::materializer::{Materializer, MaterializerError, MaterializerMode};

const VAULT: &str = "Mainframe";

fn mk(tmp: &TempDir, mode: MaterializerMode) -> Materializer {
    // v0.2.0: ensure <vaults_root>/<vault_name> exists so canonicalize works.
    std::fs::create_dir_all(tmp.path().join(VAULT)).unwrap();
    Materializer::new(
        tmp.path().to_path_buf(),
        VAULT.to_string(),
        Some(".lattice-sync/shadow/".into()),
        mode,
    )
}

fn sha256_hex(s: &str) -> String {
    use sha2::Digest;
    hex::encode(sha2::Sha256::digest(s.as_bytes()))
}

fn payload(path: &str, body: &str) -> NotePayload {
    NotePayload {
        path: path.into(),
        frontmatter: serde_json::json!({"title": "Test", "tags": ["a", "b"]}),
        body: body.into(),
        sha256: sha256_hex(&format!(
            "---\ntitle: Test\ntags:\n  - a\n  - b\n---\n\n{body}"
        )),
        modified: "2026-05-25T00:00:00Z".into(),
        file_mtime: None,
    }
}

#[test]
fn write_creates_file_with_frontmatter() {
    let tmp = TempDir::new().unwrap();
    let m = mk(&tmp, MaterializerMode::Shadow);
    m.write(&payload("01_Inbox/foo.md", "hello")).unwrap();
    let written = std::fs::read_to_string(
        tmp.path()
            .join(VAULT)
            .join(".lattice-sync/shadow/01_Inbox/foo.md"),
    )
    .unwrap();
    assert!(written.contains("title: Test"));
    assert!(written.contains("hello"));
}

#[test]
fn write_atomic_no_partial_file_on_serialize_failure() {
    // Placeholder retained from prior test surface.
}

#[test]
fn write_rejects_path_traversal() {
    let tmp = TempDir::new().unwrap();
    let m = mk(&tmp, MaterializerMode::Shadow);
    let np = payload("../escape.md", "x");
    assert!(matches!(
        m.write(&np),
        Err(MaterializerError::PathTraversal(_))
    ));
}

#[test]
fn write_refuses_live_mode_in_e2() {
    let tmp = TempDir::new().unwrap();
    let m = mk(&tmp, MaterializerMode::Live);
    assert!(matches!(
        m.write(&payload("foo.md", "x")),
        Err(MaterializerError::NotYetImplemented)
    ));
}

#[test]
fn write_refuses_rasp_substrate_path() {
    let tmp = TempDir::new().unwrap();
    let m = mk(&tmp, MaterializerMode::Shadow);
    for substrate in &[
        "00_VAULT.md",
        "02_Projects/Nexus/00_VAULT.md",
        "Family.md",
        "Mission.md",
        "02_Projects/Protocols/foo.md",
        "_project/x.md",
        "_rapport/people/cyril.md",
    ] {
        assert!(
            matches!(
                m.write(&payload(substrate, "should never land")),
                Err(MaterializerError::SubstrateRefuse(_))
            ),
            "expected RASP refuse on {substrate}"
        );
    }
}

#[test]
fn delete_renames_to_deleted_ts() {
    let tmp = TempDir::new().unwrap();
    let m = mk(&tmp, MaterializerMode::Shadow);
    m.write(&payload("01_Inbox/foo.md", "x")).unwrap();
    m.soft_delete("01_Inbox/foo.md").unwrap();
    let shadow_dir = tmp.path().join(VAULT).join(".lattice-sync/shadow/01_Inbox/");
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
    let tmp = TempDir::new().unwrap();
    let m = mk(&tmp, MaterializerMode::Shadow);
    assert!(m.soft_delete("01_Inbox/never-existed.md").is_ok());
}

#[test]
fn delete_refuses_rasp_substrate_path() {
    let tmp = TempDir::new().unwrap();
    let m = mk(&tmp, MaterializerMode::Shadow);
    assert!(matches!(
        m.soft_delete("00_VAULT.md"),
        Err(MaterializerError::SubstrateRefuse(_))
    ));
}
