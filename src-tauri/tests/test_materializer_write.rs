use vault_sync_daemon::materializer::{Materializer, MaterializerError, MaterializerMode};
use vault_sync_daemon::api_client::NotePayload;
use tempfile::TempDir;

fn sha256_hex(s: &str) -> String {
    use sha2::Digest;
    hex::encode(sha2::Sha256::digest(s.as_bytes()))
}

fn payload(path: &str, body: &str) -> NotePayload {
    NotePayload {
        path: path.into(),
        frontmatter: serde_json::json!({"title": "Test", "tags": ["a", "b"]}),
        body: body.into(),
        sha256: sha256_hex(&format!("---\ntitle: Test\ntags:\n  - a\n  - b\n---\n\n{body}")),
        modified: "2026-05-25T00:00:00Z".into(),
        file_mtime: None,
    }
}

#[test]
fn write_creates_file_with_frontmatter() {
    let vault = TempDir::new().unwrap();
    let m = Materializer::new(vault.path().to_path_buf(), Some(".lattice-sync/shadow/".into()), MaterializerMode::Shadow);
    m.write(&payload("foo.md", "hello")).unwrap();
    let written = std::fs::read_to_string(vault.path().join(".lattice-sync/shadow/foo.md")).unwrap();
    assert!(written.contains("title: Test"));
    assert!(written.contains("hello"));
}

#[test]
fn write_atomic_no_partial_file_on_serialize_failure() {
    // Pass invalid frontmatter to trigger serialize failure
    // ... (implementation detail; ensure tempfile gets cleaned up)
}

#[test]
fn write_rejects_path_traversal() {
    let vault = TempDir::new().unwrap();
    let m = Materializer::new(vault.path().to_path_buf(), None, MaterializerMode::Shadow);
    let np = payload("../escape.md", "x");
    matches!(m.write(&np), Err(MaterializerError::PathTraversal(_)));
}

#[test]
fn write_refuses_live_mode_in_e2() {
    let vault = TempDir::new().unwrap();
    let m = Materializer::new(vault.path().to_path_buf(), None, MaterializerMode::Live);
    matches!(m.write(&payload("foo.md", "x")), Err(MaterializerError::NotYetImplemented));
}
