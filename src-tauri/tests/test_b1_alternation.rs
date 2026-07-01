//! B1' alternation regression (Piece 1 spec D1 acceptance:
//! test_b1_alternation_regression).
//!
//! The v0.4.27 livelock, per pass, forever: local CRLF + shadow=local-raw-sha
//! (recorded by the old Accepted arm) + server LF canonical ->
//!   pass N:   reconcile classifies drift -> PUSH -> idempotent accept ->
//!             shadow = local raw sha (old D3 bug)
//!   pass N+1: drift -> PULL -> R1 normalized-equal -> Noop, NO rewrite,
//!             shadow = server -> GOTO pass N.
//! One full-body push + one pull per CRLF file per pass - 18k files on
//! Trinity = a permanent low-grade storm.
//!
//! With v0.4.28 (D1 + D3) the loop must die in ONE pull pass: the alignment
//! rewrite converges the bytes, and the second pass is zero drift, zero
//! pushes, zero pulls.

use std::sync::Arc;
use tempfile::TempDir;
use vault_sync_daemon::api_client::NotePayload;
use vault_sync_daemon::echo_guard::EchoGuard;
use vault_sync_daemon::materializer::{
    MaterializeOutcome, Materializer, MaterializerConfig, MaterializerMode, SkipReason,
};
use vault_sync_daemon::sync_shadow::ShadowStore;
use vault_sync_daemon::verify_repair::{decide_direction, Direction};

const VAULT: &str = "Mainframe";

fn sha256_hex(b: &[u8]) -> String {
    use sha2::Digest;
    hex::encode(sha2::Sha256::digest(b))
}

#[test]
fn test_b1_alternation_regression() {
    // ---- setup: the exact B1' state ----
    let vaults = TempDir::new().unwrap();
    let ws = TempDir::new().unwrap();
    let sdir = TempDir::new().unwrap();
    std::fs::create_dir_all(vaults.path().join(VAULT)).unwrap();
    let shadow = ShadowStore::load(sdir.path().join("shadow.json"));
    let echo = Arc::new(EchoGuard::new());
    let mat = Materializer::new(
        vaults.path().to_path_buf(),
        Some("shadow/".into()),
        MaterializerMode::Live,
        ws.path().to_path_buf(),
        "sub-b1".to_string(),
        MaterializerConfig {
            device_id: "trinity-sim".into(),
            ..Default::default()
        },
    )
    .with_shadow_store(shadow.clone())
    .with_echo_guard(echo.clone());

    let rel = format!("{VAULT}/notes/storm.md");
    let local_crlf = "storm line\r\nsecond line\r\n";
    let server_lf = "storm line\nsecond line\n";
    let local_raw_sha = sha256_hex(local_crlf.as_bytes());
    let server_sha = sha256_hex(server_lf.as_bytes());
    let abs = vaults.path().join(&rel);
    std::fs::create_dir_all(abs.parent().unwrap()).unwrap();
    std::fs::write(&abs, local_crlf).unwrap();
    // Old-daemon residue: shadow holds the LOCAL raw sha (v0.4.27 Accepted arm).
    shadow.record(&rel, &local_raw_sha);

    // ---- pass 1: reconcile classify -> PULL -> alignment rewrite ----
    // Server sees fs_hash=local_raw_sha != server_sha -> state "drift".
    // shadow(local_raw) != server -> Direction::Pull (verify_repair table).
    let dir1 = decide_direction("drift", &local_raw_sha, Some(&server_sha), Some(&local_raw_sha));
    assert_eq!(dir1, Direction::Pull, "pass 1 must classify PULL, not push");

    let payload = NotePayload {
        path: rel.clone(),
        frontmatter: serde_json::Value::Null,
        body: server_lf.to_string(),
        sha256: server_sha.clone(),
        modified: "2026-07-01T00:00:00Z".to_string(),
        file_mtime: None,
        enriched_body: Some(server_lf.to_string()),
        created: None,
    };
    let outcome1 = mat.write(&payload).unwrap();
    assert_eq!(
        outcome1,
        MaterializeOutcome::AlignedToCanonical { path: abs.clone() },
        "pass 1 pull must be the D1 alignment rewrite"
    );
    assert_eq!(std::fs::read(&abs).unwrap(), server_lf.as_bytes());
    assert_eq!(shadow.get(&rel).as_deref(), Some(server_sha.as_str()));
    // ZERO pushes from the rewrite: the FS event it caused is an echo.
    assert!(
        echo.is_echo(&rel, &server_sha),
        "the alignment write must be echo-suppressed (zero pushes)"
    );

    // ---- pass 2: zero drift, zero pushes, zero pulls ----
    // Local fs_hash now == server fs_hash -> the server answers "match".
    let local_sha_2 = sha256_hex(&std::fs::read(&abs).unwrap());
    assert_eq!(local_sha_2, server_sha, "pass 2: byte-converged");
    let dir2 = decide_direction(
        "match",
        &local_sha_2,
        Some(&server_sha),
        shadow.get(&rel).as_deref(),
    );
    assert_eq!(dir2, Direction::Noop, "pass 2 must be zero ops");
    // Even a redundant pull delivery is now a raw-equal NO-WRITE noop.
    let mtime_before = std::fs::metadata(&abs).unwrap().modified().unwrap();
    let outcome2 = mat.write(&payload).unwrap();
    assert_eq!(
        outcome2,
        MaterializeOutcome::Skipped(SkipReason::IdenticalToLocal),
        "pass 2 redundant delivery must be a no-write noop"
    );
    assert_eq!(
        std::fs::metadata(&abs).unwrap().modified().unwrap(),
        mtime_before,
        "pass 2 must not touch the file (the eternal-alternation trace is dead)"
    );
}
