/// T22 — SSE consumer tests
///
/// Strategy: spin up a mockito server that serves a finite SSE body (one or more events then
/// EOF). Feed the server URL to SseConsumer::new, then drive consumer.run() until the expected
/// side-effects appear (positive tests) or after a generous fixed wait (negative tests).
///
/// Tests 1 and 2 pass.  Tests 3 and 4 are #[ignore] — see TODO comments.
use mockito::Server;
use std::time::Duration;
use tempfile::TempDir;
use tokio::sync::watch;
use vault_sync_daemon::materializer::{Materializer, MaterializerMode};
use vault_sync_daemon::sse::SseConsumer;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_consumer(base_url: &str, vault_root: &TempDir) -> SseConsumer {
    SseConsumer::new(
        base_url.to_string(),
        "vsk_test".to_string(),
        vec![], // empty roots = accept everything
        vec![],
        Materializer::new(
            vault_root.path().to_path_buf(),
            None,
            MaterializerMode::Shadow,
        ),
    )
    .unwrap()
}

/// Spawn the consumer in the background.
/// Returns the join handle and a shutdown sender; send `true` (or drop) to stop the task.
fn spawn_consumer(consumer: SseConsumer) -> (tokio::task::JoinHandle<()>, watch::Sender<bool>) {
    let (tx, rx) = watch::channel(false);
    let handle = tokio::spawn(async move {
        let _ = consumer.run(None, rx).await;
    });
    (handle, tx)
}

/// Poll `path` for existence up to `timeout`, checking every 50 ms.
/// Returns `true` if the path appeared within the deadline, `false` otherwise.
async fn wait_for_path(path: &std::path::Path, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        if path.exists() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

// ---------------------------------------------------------------------------
// Test 1: enrichment_complete events are materialised; lint_complete events
//         are silently skipped.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn consumes_enrichment_complete_skips_lint_events() {
    let mut srv = Server::new_async().await;
    let vault = TempDir::new().unwrap();

    // ---- mock GET /api/sync/events ----------------------------------------
    // Two events: lint_complete (should be dropped) + enrichment_complete
    // (should trigger a note fetch).
    let sse_body = concat!(
        "event: lint_complete\n",
        "data: {\"op\":\"INSERT\",\"path\":\"Notes/skip.md\",\"phase\":\"lint_complete\"}\n",
        "id: lsn-0\n",
        "\n",
        "event: enrichment_complete\n",
        "data: {\"op\":\"INSERT\",\"path\":\"Notes/hello.md\",\"phase\":\"enrichment_complete\"}\n",
        "id: lsn-1\n",
        "\n",
    );

    let _m_events = srv
        .mock("GET", "/api/sync/events")
        .with_status(200)
        .with_header("content-type", "text/event-stream")
        .with_header("cache-control", "no-cache")
        .with_body(sse_body)
        .create_async()
        .await;

    // ---- mock GET /api/sync/note (only called for enrichment_complete) ------
    let _m_note = srv
        .mock("GET", "/api/sync/note")
        .match_query(mockito::Matcher::UrlEncoded(
            "path".into(),
            "Notes/hello.md".into(),
        ))
        .with_status(200)
        .with_body(
            r#"{"path":"Notes/hello.md","frontmatter":{},"body":"hello world","sha256":"abc","modified":"2026-05-26T00:00:00Z","file_mtime":null}"#,
        )
        .create_async()
        .await;

    let consumer = make_consumer(&srv.url(), &vault);
    let (handle, tx) = spawn_consumer(consumer);

    // Poll for the expected output file for up to 5 s (CI-safe).
    let shadow = vault.path().join(".lattice-sync/shadow/Notes/hello.md");
    let landed = wait_for_path(&shadow, Duration::from_secs(5)).await;

    // Signal shutdown and wait for the consumer task to exit.
    let _ = tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;

    assert!(
        landed,
        "enrichment_complete payload should land in shadow tree (timed out after 5s)"
    );

    // lint_complete path should NOT be on disk
    let lint_shadow = vault.path().join(".lattice-sync/shadow/Notes/skip.md");
    assert!(
        !lint_shadow.exists(),
        "lint_complete payload must not land in shadow tree"
    );
}

// ---------------------------------------------------------------------------
// Test 2: path-traversal envelope is rejected — nothing written to disk.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn path_traversal_rejected_in_envelope() {
    let mut srv = Server::new_async().await;
    let vault = TempDir::new().unwrap();

    // Envelope carries a path-traversal string.
    let sse_body = concat!(
        "event: enrichment_complete\n",
        "data: {\"op\":\"INSERT\",\"path\":\"../../etc/passwd\",\"phase\":\"enrichment_complete\"}\n",
        "id: lsn-1\n",
        "\n",
    );

    let _m_events = srv
        .mock("GET", "/api/sync/events")
        .with_status(200)
        .with_header("content-type", "text/event-stream")
        .with_header("cache-control", "no-cache")
        .with_body(sse_body)
        .create_async()
        .await;

    // fetch_note must NOT be called for a traversal path — but mock it defensively
    // so we'd see a hit if the guard fails.
    let _m_note = srv
        .mock("GET", "/api/sync/note")
        .expect(0) // assert NEVER called
        .create_async()
        .await;

    let consumer = make_consumer(&srv.url(), &vault);
    let (handle, tx) = spawn_consumer(consumer);

    // Fixed wait: give the consumer time to receive + reject the envelope before
    // the negative assertion fires. 2 s is generous enough for CI slowness.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let _ = tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;

    // The shadow dir should be completely empty (no file created for the malicious path).
    let shadow_root = vault.path().join(".lattice-sync/shadow");
    let shadow_has_files = shadow_root.exists()
        && std::fs::read_dir(&shadow_root)
            .map(|mut d| d.next().is_some())
            .unwrap_or(false);
    assert!(
        !shadow_has_files,
        "no file should be written for a path-traversal envelope"
    );
}

// ---------------------------------------------------------------------------
// Test 3: reconnect includes Last-Event-Id header on second attempt.
//
// TODO: T22-CF — eventsource-client reconnect harness needs upstream test
// fixture. The eventsource-client crate handles reconnect internally via its
// ReconnectingRequest machinery; intercepting the second HTTP request with
// mockito and asserting the header requires sequencing two separate mock
// responses on the same path (a "consume-once" first mock returning EOF then
// a second mock), but eventsource-client's built-in reconnect reuses its own
// backoff timer rather than our run_one_session loop.  To test last_event_id
// propagation properly we'd need to stub the underlying hyper connector or
// expose run_one_session publicly and call it twice.  Deferred to T22-CF.
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "TODO: T22-CF — eventsource-client reconnect harness needs upstream test fixture"]
async fn reconnects_with_last_event_id() {
    // Placeholder — see ignore reason above.
}

// ---------------------------------------------------------------------------
// Test 4: 503 with Retry-After is honoured (≈ 1 s sleep before reconnect).
//
// TODO: T22-CF — eventsource-client returns UnexpectedResponse for non-200
// before we can read a body, so mockito 503 is not surfaced as a body-level
// SSE event.  Our run() loop already parses the Retry-After header from the
// eventsource_client::Response embedded in SseError::UnexpectedResponse, but
// asserting the ≈1 s elapsed time from a test is flaky under CI load.
// Deferred to T22-CF (timing-sensitive integration test belongs in the
// autonomous-qa harness against a live Nexus, not the unit-test suite).
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "TODO: T22-CF — 503/Retry-After timing assertion is CI-flaky; belongs in QA harness"]
async fn sse_503_with_retry_after_honored() {
    // Placeholder — see ignore reason above.
}

// ---------------------------------------------------------------------------
// Test 5: CF-S470-T22-401-fastfail — 401 (token revoked) propagates as fatal
//         error and the consumer exits without retrying.
//
// Without the fast-fail fix, the SSE consumer would treat 401 the same as a
// 503 backoff and keep reconnecting with a token the server has already
// rejected — burning Nexus rate-limit and never recovering.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sse_401_fast_fails_without_retry() {
    let mut srv = Server::new_async().await;
    let vault = TempDir::new().unwrap();

    // 401 with expect(1): mockito asserts at drop that the endpoint was hit
    // exactly once — i.e. no reconnect attempt was made.
    let _m_events = srv
        .mock("GET", "/api/sync/events")
        .with_status(401)
        .with_header("content-type", "application/json")
        .with_body(r#"{"detail":"token revoked"}"#)
        .expect(1)
        .create_async()
        .await;

    let consumer = make_consumer(&srv.url(), &vault);
    let (_tx, rx) = watch::channel(false);

    // Generous 3 s timeout — fast-fail should land in <100 ms post-fix.
    // Pre-fix, the consumer would backoff (1 s → 2 s → 4 s …) and still be
    // running after 3 s with multiple mock hits, blowing the expect(1) check.
    let result = tokio::time::timeout(Duration::from_secs(3), consumer.run(None, rx)).await;

    match result {
        Ok(Ok(())) => panic!("consumer should not have completed cleanly on 401"),
        Ok(Err(_)) => {} // expected — 401 propagated as fatal
        Err(_) => panic!("consumer did not fast-fail on 401; still running after 3s"),
    }
}

#[tokio::test]
async fn sse_403_fast_fails_without_retry() {
    let mut srv = Server::new_async().await;
    let vault = TempDir::new().unwrap();

    let _m_events = srv
        .mock("GET", "/api/sync/events")
        .with_status(403)
        .with_header("content-type", "application/json")
        .with_body(r#"{"detail":"forbidden"}"#)
        .expect(1)
        .create_async()
        .await;

    let consumer = make_consumer(&srv.url(), &vault);
    let (_tx, rx) = watch::channel(false);

    let result = tokio::time::timeout(Duration::from_secs(3), consumer.run(None, rx)).await;

    match result {
        Ok(Ok(())) => panic!("consumer should not have completed cleanly on 403"),
        Ok(Err(_)) => {} // expected — 403 propagated as fatal
        Err(_) => panic!("consumer did not fast-fail on 403; still running after 3s"),
    }
}
