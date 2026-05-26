/// T22 — SSE consumer tests
///
/// Strategy: spin up a mockito server that serves a finite SSE body (one or more events then
/// EOF). Feed the server URL to SseConsumer::new, then drive consumer.run() for a short window
/// (300 ms) before sending a shutdown signal. Assert materializer side-effects (or absence of
/// side-effects) on the temp-directory shadow tree.
///
/// Tests 1 and 2 pass.  Tests 3 and 4 are #[ignore] — see TODO comments.
use mockito::Server;
use std::time::Duration;
use tempfile::TempDir;
use tokio::sync::watch;
use tokio::time::timeout;
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

/// Run the consumer until shutdown or 400 ms, whichever comes first.
/// Sends shutdown before returning.
async fn run_consumer_briefly(consumer: &SseConsumer) {
    let (tx, rx) = watch::channel(false);
    let _ = timeout(Duration::from_millis(400), consumer.run(None, rx)).await;
    // Signal shutdown in case run() is still looping (reconnect backoff).
    let _ = tx.send(true);
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
    run_consumer_briefly(&consumer).await;

    // enrichment_complete path should be on disk
    let shadow = vault.path().join(".lattice-sync/shadow/Notes/hello.md");
    assert!(
        shadow.exists(),
        "enrichment_complete payload should land in shadow tree"
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
    run_consumer_briefly(&consumer).await;

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
