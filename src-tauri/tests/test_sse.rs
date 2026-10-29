// CS-2 gate 2: integration tests may use path-based fs freely.
#![allow(clippy::disallowed_methods)]
/// T22 — SSE consumer tests
///
/// Strategy: spin up a mockito server that serves a finite SSE body (one or more events then
/// EOF). Feed the server URL to SseConsumer::new, then drive consumer.run() until the expected
/// side-effects appear (positive tests) or after a generous fixed wait (negative tests).
///
/// Tests 1 and 2 pass.  Tests 3 and 4 are #[ignore] — see TODO comments.
use mockito::Server;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use tempfile::TempDir;
use tokio::sync::watch;
use vault_sync_daemon::materializer::{Materializer, MaterializerConfig, MaterializerMode};
use vault_sync_daemon::sse::SseConsumer;
use vault_sync_daemon::tray_state::{ConnectionStatus, SharedTrayState, TrayState};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_consumer(base_url: &str, vaults_root: &TempDir) -> SseConsumer {
    std::fs::create_dir_all(vaults_root.path().join("Mainframe")).unwrap();
    SseConsumer::new(
        base_url.to_string(),
        "vsk_test".to_string(),
        vec![], // empty roots = accept everything
        vec![],
        Materializer::new(
            vaults_root.path().to_path_buf(),
            None,
            MaterializerMode::Shadow,
            vaults_root.path().to_path_buf(),
            "test-subscriber".to_string(),
            MaterializerConfig::default(),
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
    // S477 v0.3.7: materializer writes shadow-mode notes under
    // <workspace_root>/.lattice-runtime/<subscriber>/shadow/<rel>; here the
    // workspace_root is the same tempdir as vaults_root for test convenience.
    let shadow = vault
        .path()
        .join(".lattice-runtime/test-subscriber/shadow/Notes/hello.md");
    let landed = wait_for_path(&shadow, Duration::from_secs(5)).await;

    // Signal shutdown and wait for the consumer task to exit.
    let _ = tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;

    assert!(
        landed,
        "enrichment_complete payload should land in shadow tree (timed out after 5s)"
    );

    // lint_complete path should NOT be on disk
    let lint_shadow = vault
        .path()
        .join(".lattice-runtime/test-subscriber/shadow/Notes/skip.md");
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
    let shadow_root = vault.path().join(".lattice-runtime/test-subscriber/shadow");
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
// Test 5: TKT-8fc0b2a4 (WS-A4) — an auth failure (401/403) does NOT end
//         delivery. The consumer enters AuthFailed, keeps the cursor, retries
//         on the bounded schedule, and when the server accepts again the
//         reconnect carries the ORIGINAL Last-Event-ID and the status returns
//         to Connected.
//
// Pre-fix (CF-S470-T22-401-fastfail, 2026-05) the loop returned Err on the
// first 401 and delivery stayed dead for 3 days on Trinity (2026-07-07)
// while every liveness instrument read alive. The previous tests here
// (`sse_401_fast_fails_without_retry`, `sse_403_...`) asserted that old
// contract and were replaced by this one.
// ---------------------------------------------------------------------------

/// Minimal SSE origin: answers the first `n_reject` connects with `reject`
/// (401 or 403) and every later connect with a 200 event-stream held open by
/// keep-alives. Records each request's `Last-Event-ID` header (None when the
/// header is absent) so the test can prove the cursor survived the failure.
async fn spawn_auth_flapping_sse_server(
    n_reject: usize,
    reject: u16,
    seen: Arc<Mutex<Vec<Option<String>>>>,
) -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut n = 0usize;
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                return;
            };
            let mut buf = vec![0u8; 8192];
            let read = sock.read(&mut buf).await.unwrap_or(0);
            let head = String::from_utf8_lossy(&buf[..read]).to_string();
            let lei = head.lines().find_map(|l| {
                let (k, v) = l.split_once(':')?;
                k.trim()
                    .eq_ignore_ascii_case("last-event-id")
                    .then(|| v.trim().to_string())
            });
            seen.lock().unwrap().push(lei);
            n += 1;
            if n <= n_reject {
                let body = r#"{"detail":"token revoked"}"#;
                let resp = format!(
                    "HTTP/1.1 {reject} Rejected\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = sock.write_all(resp.as_bytes()).await;
                let _ = sock.shutdown().await;
                continue;
            }
            let resp = "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\n\r\n: keep-alive\n\n";
            if sock.write_all(resp.as_bytes()).await.is_err() {
                continue;
            }
            // Hold the accepted session open for the rest of the test.
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    if sock.write_all(b": keep-alive\n\n").await.is_err() {
                        return;
                    }
                }
            });
        }
    });
    addr
}

async fn wait_for_tray(
    tray: &SharedTrayState,
    what: &str,
    timeout: Duration,
    pred: impl Fn(&TrayState) -> bool,
) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        {
            let t = tray.read().unwrap();
            if pred(&t) {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for {what}; status={:?} auth_failed_since={:?} last_error={:?}",
                t.status,
                t.auth_failed_since,
                t.last_error
            );
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

async fn auth_failure_retries_and_resumes(reject: u16) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let addr = spawn_auth_flapping_sse_server(2, reject, seen.clone()).await;
    let base_url = format!("http://{addr}");
    let vault = TempDir::new().unwrap();
    let tray: SharedTrayState = Arc::new(RwLock::new(TrayState::new(
        "sub-test".to_string(),
        base_url.clone(),
        vault.path().to_path_buf(),
    )));
    let transitions = Arc::new(AtomicU32::new(0));
    let t2 = Arc::clone(&transitions);
    let consumer = make_consumer(&base_url, &vault)
        .with_tray_state(tray.clone())
        // Tiny schedule so the real loop runs in milliseconds (production:
        // 30 s doubling to 10 min; the schedule itself is unit-tested in
        // auth_backoff.rs).
        .with_auth_backoff(Duration::from_millis(60), Duration::from_millis(200))
        .with_on_auth_failed(Arc::new(move |_status| {
            t2.fetch_add(1, Ordering::SeqCst);
        }));
    let (tx, rx) = watch::channel(false);
    let handle = tokio::spawn(async move { consumer.run(Some("lsn-41".to_string()), rx).await });

    // The first rejection is VISIBLE: AuthFailed + auth_failed_since stamped.
    wait_for_tray(&tray, "AuthFailed", Duration::from_secs(5), |t| {
        t.status == ConnectionStatus::AuthFailed && t.auth_failed_since.is_some()
    })
    .await;

    // Then the server accepts: the loop reconnects and reports Connected
    // without ever having returned.
    wait_for_tray(&tray, "Connected", Duration::from_secs(10), |t| {
        t.status == ConnectionStatus::Connected
    })
    .await;
    assert!(
        !handle.is_finished(),
        "run() must not have returned on HTTP {reject}"
    );
    {
        let t = tray.read().unwrap();
        assert_eq!(
            t.auth_failed_since, None,
            "recovery clears auth_failed_since"
        );
        assert_eq!(t.last_error, None, "Connected clears last_error");
    }
    assert_eq!(
        transitions.load(Ordering::SeqCst),
        1,
        "the on_auth_failed hook fires ONCE per episode, not per attempt"
    );

    // The cursor: every connect, including the one that succeeded after two
    // rejections, carried the ORIGINAL Last-Event-ID.
    let seen = seen.lock().unwrap().clone();
    assert_eq!(
        seen.len(),
        3,
        "two rejected connects then the accepted one: {seen:?}"
    );
    for (i, lei) in seen.iter().enumerate() {
        assert_eq!(
            lei.as_deref(),
            Some("lsn-41"),
            "connect #{i} must carry the original cursor: {seen:?}"
        );
    }

    let _ = tx.send(true);
    handle.abort();
}

#[tokio::test]
async fn sse_401_retries_with_backoff_and_resumes_from_original_cursor() {
    auth_failure_retries_and_resumes(401).await;
}

#[tokio::test]
async fn sse_403_retries_with_backoff_and_resumes_from_original_cursor() {
    auth_failure_retries_and_resumes(403).await;
}
