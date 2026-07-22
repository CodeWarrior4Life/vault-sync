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
//! With v0.4.28's D1 alignment rewrite, the loop must die in ONE pull pass:
//! the rewrite converges the bytes, and the second pass is zero drift, zero
//! pushes, zero pulls.
//!
//! This test is a D1-ONLY tripwire, not a D3 regression gate: the D3-bug
//! shadow state (`shadow.record(&rel, &local_raw_sha)` below) is HAND-SEEDED
//! to reproduce the v0.4.27 symptom, not derived by driving the actual D3
//! `shadow_hash_for_ack` code path. It proves D1's alignment rewrite kills
//! the alternation once that state exists, but says nothing about whether D3
//! still produces that state today. The D3 gate — that `shadow_hash_for_ack`
//! now records `server_hash` on Accepted instead of the local raw sha — is
//! `push_client::tests::test_ack_materialize_rewrite_then_shadow`.

use mockito::Server;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::sync::Mutex;
use vault_sync_daemon::api_client::{ApiClient, NotePayload};
use vault_sync_daemon::echo_guard::EchoGuard;
use vault_sync_daemon::materializer::{
    MaterializeOutcome, Materializer, MaterializerConfig, MaterializerMode, SkipReason,
};
use vault_sync_daemon::push_journal::PushJournal;
use vault_sync_daemon::sync_shadow::ShadowStore;
use vault_sync_daemon::verify_repair::{
    decide_direction, Direction, VerifyRepair, VerifyRepairConfig,
};

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
    let dir1 = decide_direction(
        "drift",
        &local_raw_sha,
        Some(&server_sha),
        Some(&local_raw_sha),
    );
    assert_eq!(dir1, Direction::Pull, "pass 1 must classify PULL, not push");

    let payload = NotePayload {
        path: rel.clone(),
        frontmatter: serde_json::Value::Null,
        body: server_lf.to_string(),
        sha256: server_sha.clone(),
        modified: Some("2026-07-01T00:00:00Z".to_string()),
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

/// Stronger B1' variant: drives the REAL `VerifyRepair::run` pull loop
/// end-to-end against a mocked server, instead of hand-calling
/// `decide_direction` + `Materializer::write` as the test above does.
///
/// Same D1-only caveat as `test_b1_alternation_regression`: the D3-bug
/// shadow entry is still hand-seeded (`shadow.record`), not produced by
/// driving `shadow_hash_for_ack`. What THIS test adds is proof that the
/// production call path — `reconcile-batch` classify -> `decide_direction`
/// -> pull -> `/api/sync/note` fetch -> `Materializer::write` (the alignment
/// rewrite) -- converges with ZERO pushes, by mounting a push route with
/// `expect(0)` (mockito fails the test if it's ever hit) alongside the
/// reconcile-batch and note mocks that `VerifyRepair::run` actually calls.
#[tokio::test]
async fn test_b1_alternation_verify_repair_run_zero_pushes() {
    let vault = TempDir::new().unwrap();
    let ws = TempDir::new().unwrap();
    let sdir = TempDir::new().unwrap();
    let jdir = TempDir::new().unwrap();

    let rel = "notes/storm.md";
    let local_crlf = "storm line\r\nsecond line\r\n";
    let server_lf = "storm line\nsecond line\n";
    let local_raw_sha = sha256_hex(local_crlf.as_bytes());
    let server_sha = sha256_hex(server_lf.as_bytes());

    std::fs::create_dir_all(vault.path().join("notes")).unwrap();
    std::fs::write(vault.path().join(rel), local_crlf).unwrap();

    // Same hand-seeded D3-bug residue as the test above: shadow holds the
    // LOCAL raw sha, matching what the old (pre-D3-fix) Accepted arm used to
    // record. This is what makes decide_direction choose Pull over Push for
    // a "drift" delta instead of Noop.
    let shadow = ShadowStore::load(sdir.path().join("shadow.json"));
    shadow.record(rel, &local_raw_sha);

    let mut srv = Server::new_async().await;
    // reconcile-batch reports the CRLF-drifted note as drift, echoing the
    // server's canonical hash.
    let m_reconcile = srv
        .mock("POST", "/api/sync/reconcile-batch")
        .with_status(200)
        .with_body(format!(
            r#"{{"deltas":[{{"path":"{rel}","state":"drift","server_hash":"{server_sha}"}}]}}"#
        ))
        .create_async()
        .await;
    // /api/sync/note serves the canonical payload the pull fetches and
    // materializes.
    let note_body = format!(
        r#"{{"path":"{rel}","frontmatter":{{}},"body":{body},"sha256":"{server_sha}","modified":"2026-07-01T00:00:00Z","enriched_body":{body}}}"#,
        body = serde_json::to_string(server_lf).unwrap()
    );
    let m_note = srv
        .mock("GET", "/api/sync/note")
        .match_query(mockito::Matcher::UrlEncoded("path".into(), rel.into()))
        .with_status(200)
        .with_body(note_body)
        .create_async()
        .await;
    // The whole point of D1: this must NEVER be hit. mockito fails the test
    // if a request lands here.
    let m_push = srv
        .mock("POST", "/api/sync/push")
        .expect(0)
        .create_async()
        .await;

    let api = Arc::new(ApiClient::new(&srv.url(), "vsk_test").unwrap());
    let journal_path = jdir.path().join("push_journal.jsonl");
    let journal = Arc::new(Mutex::new(PushJournal::open(&journal_path).unwrap()));
    let mat = Materializer::new(
        vault.path().to_path_buf(),
        Some("shadow/".into()),
        MaterializerMode::Live,
        ws.path().to_path_buf(),
        "sub-b1-vr".to_string(),
        MaterializerConfig::default(),
    )
    .with_shadow_store(shadow.clone());

    let vr = VerifyRepair::new(
        vault.path().to_path_buf(),
        api,
        journal.clone(),
        "trinity-sim".into(),
        VerifyRepairConfig::default(),
    )
    .with_materializer(mat)
    .with_shadow(shadow.clone());

    let report = vr.run().await.unwrap();

    assert_eq!(report.modify_count, 0, "drift-but-Pull must NOT push");
    assert_eq!(report.add_count, 1, "the drift must resolve as one pull");
    let j = journal.lock().await;
    assert_eq!(
        j.len(),
        0,
        "no push journaled — the alignment rewrite never enqueues a push"
    );
    drop(j);

    // Local file converged to the server's canonical bytes, and the shadow
    // now holds the server hash (no more residual D3-bug state).
    assert_eq!(
        std::fs::read(vault.path().join(rel)).unwrap(),
        server_lf.as_bytes(),
        "file must converge to canonical bytes via the real pull loop"
    );
    assert_eq!(shadow.get(rel).as_deref(), Some(server_sha.as_str()));

    m_reconcile.assert_async().await;
    m_note.assert_async().await;
    m_push.assert_async().await;
}
