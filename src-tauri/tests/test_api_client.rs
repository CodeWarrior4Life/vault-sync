// CS-2 gate 2: integration tests may use path-based fs freely.
#![allow(clippy::disallowed_methods)]
use mockito::Server;
use vault_sync_daemon::api_client::{ApiClient, ApiError};

#[tokio::test]
async fn health_ok_returns_dispatcher_snapshot() {
    let mut srv = Server::new_async().await;
    let _m = srv.mock("GET", "/api/sync/health")
        .with_status(200)
        .with_body(r#"{"subscriber_id":"test","scope_roots":["a/"],"scope_excludes":[],"materializer_mode":"shadow"}"#)
        .match_header("authorization", "Bearer vsk_test")
        .create_async().await;
    let client = ApiClient::new(&srv.url(), "vsk_test").unwrap();
    let snap = client.health().await.unwrap();
    assert_eq!(snap.subscriber_id, "test");
}

#[tokio::test]
async fn health_401_returns_auth_error() {
    let mut srv = Server::new_async().await;
    let _m = srv
        .mock("GET", "/api/sync/health")
        .with_status(401)
        .create_async()
        .await;
    let client = ApiClient::new(&srv.url(), "vsk_bad").unwrap();
    assert!(matches!(client.health().await, Err(ApiError::Unauthorized)));
}

#[tokio::test]
async fn body_fetch_returns_envelope() {
    // Real server wire shape (nexus sync_routes_p1.py get_note_body): file_mtime
    // is a unix-ts FLOAT (BUG 1, c7d17a2) and the envelope carries `enriched_body`
    // = the exact bytes the `sha256` is computed over (BUG 2, S486). The daemon
    // materializes enriched_body verbatim, so the pull path must decode it.
    let mut srv = Server::new_async().await;
    let _m = srv.mock("GET", "/api/sync/note?path=foo.md")
        .with_status(200)
        .with_body(r#"{"path":"foo.md","frontmatter":{"title":"X"},"body":"hello","sha256":"abc","modified":"2026-05-25T00:00:00Z","file_mtime":1779300968.264,"enriched_body":"---\ntitle: X\n---\nhello","content_hash":"abc","updated_at":"2026-05-25T00:00:00Z"}"#)
        .create_async().await;
    let client = ApiClient::new(&srv.url(), "vsk_test").unwrap();
    let np = client.fetch_note("foo.md").await.unwrap();
    assert_eq!(np.body, "hello");
    assert_eq!(np.file_mtime, Some(1779300968.264));
    assert_eq!(
        np.enriched_body.as_deref(),
        Some("---\ntitle: X\n---\nhello")
    );
}

// ---- AR-009 (TKT-c41c2225) regressions -------------------------------------

/// The EXACT live-journal failing note: `01_Periodic/Daily/2026-07-04...md`.
/// The live server (verified read-only GET) returns HTTP 200 application/json
/// with `modified: null` (and file_mtime/created/updated_at also null). On
/// v0.4.32 `NotePayload.modified` was a non-optional `String`, so serde rejected
/// `null` and `fetch_note` failed every reconcile cycle with the opaque
/// `error decoding response body`. This asserts the note now decodes.
///
/// RED ON OLD CODE: with `modified: String`, `fetch_note` returns
/// `Err(ApiError::Network(...))` and this `.unwrap()` panics.
#[tokio::test]
async fn regression_ar009_daily_note_null_modified_decodes() {
    let path = "01_Periodic/Daily/2026-07-04-Saturday - Quiet holiday inbox noise, no meetings or plans on July 4th.md";
    let mut srv = Server::new_async().await;
    // Body mirrors the live wire shape captured from the server: modified,
    // file_mtime, created, updated_at ALL null; sha256 == content_hash.
    let body = format!(
        r#"{{"path":{p},"frontmatter":{{}},"body":"quiet day","sha256":"0df143d0","modified":null,"file_mtime":null,"created":null,"enriched_body":"quiet day","content_hash":"0df143d0","updated_at":null}}"#,
        p = serde_json::to_string(path).unwrap()
    );
    let _m = srv
        .mock("GET", mockito::Matcher::Any)
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(body)
        .create_async()
        .await;
    let client = ApiClient::new(&srv.url(), "vsk_test").unwrap();
    let np = client
        .fetch_note(path)
        .await
        .expect("AR-009: null `modified` must decode, not error");
    assert_eq!(np.body, "quiet day");
    assert!(np.modified.is_none(), "null modified deserializes to None");
    assert!(np.file_mtime.is_none());
}

/// A JSON structural mismatch (200 OK, application/json, but a field is the
/// wrong type / missing) must now surface as `ApiError::Decode` carrying
/// status + content-type + body length + serde detail, and MUST NOT attach a
/// raw body sample (the body is the note; no content leak).
///
/// RED ON OLD CODE: `ApiError::Decode` does not exist pre-fix, so this file
/// fails to compile against v0.4.32 (structural red).
#[tokio::test]
async fn ar009_json_decode_failure_is_diagnosable_and_leak_free() {
    let mut srv = Server::new_async().await;
    // `body` (required) is an int, not a string -> structural decode failure.
    let _m = srv
        .mock("GET", mockito::Matcher::Any)
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_header("x-request-id", "req-abc-123")
        .with_body(r#"{"path":"x.md","frontmatter":{},"body":12345,"sha256":"h"}"#)
        .create_async()
        .await;
    let client = ApiClient::new(&srv.url(), "vsk_test").unwrap();
    let err = client.fetch_note("x.md").await.unwrap_err();
    match err {
        ApiError::Decode(d) => {
            assert_eq!(d.context, "fetch_note");
            assert_eq!(d.status, 200);
            assert!(d.content_type.starts_with("application/json"));
            assert!(d.body_len > 0);
            assert_eq!(d.request_id.as_deref(), Some("x-request-id=req-abc-123"));
            assert!(!d.serde_error.is_empty(), "serde detail must be captured");
            assert!(
                d.body_sample.is_none(),
                "JSON body must NOT be sampled (no content leak); got {:?}",
                d.body_sample
            );
        }
        other => panic!("expected ApiError::Decode, got {other:?}"),
    }
}

/// A non-JSON body (e.g. an HTML/proxy error page returned with 200) attaches a
/// bounded body sample, since it is diagnostic and is not note content.
#[tokio::test]
async fn ar009_non_json_decode_attaches_bounded_sample() {
    let mut srv = Server::new_async().await;
    let html = format!("<html><body>{}</body></html>", "x".repeat(1000));
    let _m = srv
        .mock("GET", mockito::Matcher::Any)
        .with_status(200)
        .with_header("content-type", "text/html; charset=utf-8")
        .with_body(&html)
        .create_async()
        .await;
    let client = ApiClient::new(&srv.url(), "vsk_test").unwrap();
    let err = client.fetch_note("x.md").await.unwrap_err();
    match err {
        ApiError::Decode(d) => {
            assert!(d.content_type.starts_with("text/html"));
            let s = d.body_sample.expect("non-JSON body must attach a sample");
            assert!(
                s.len() <= 256,
                "sample must be bounded (<=256), got {}",
                s.len()
            );
            assert!(s.starts_with("<html>"));
        }
        other => panic!("expected ApiError::Decode, got {other:?}"),
    }
}

#[tokio::test]
async fn health_503_returns_rate_limited() {
    let mut srv = Server::new_async().await;
    let _m = srv
        .mock("GET", "/api/sync/health")
        .with_status(503)
        .with_header("retry-after", "30")
        .create_async()
        .await;
    let client = ApiClient::new(&srv.url(), "vsk_test").unwrap();
    let err = client.health().await.unwrap_err();
    assert!(matches!(
        err,
        ApiError::RateLimited {
            retry_after_secs: 30
        }
    ));
}

// ---- WS-A A1 (TKT-17fc22e5 / TKT-8fc0b2a4): carry the rejection reason ----

fn push_req<'a>(path: &'a str, b64: &'a str) -> vault_sync_daemon::api_client::PushRequest<'a> {
    vault_sync_daemon::api_client::PushRequest {
        device_id: "dev-test",
        path,
        content_b64: b64,
        base_hash: "",
        base_seq: None,
        action: vault_sync_daemon::api_client::PushApiAction::Create,
    }
}

/// A FastAPI 400 carries `{"detail": "<reason>"}`; the daemon logged the
/// status only (215 lines/day on Trinity with no cause). The `detail` field
/// must survive to the caller as `ApiError::Rejected { status, detail }`.
#[tokio::test]
async fn push_400_carries_json_detail_as_rejected() {
    let mut srv = Server::new_async().await;
    let _m = srv
        .mock("POST", "/api/sync/push")
        .with_status(400)
        .with_header("content-type", "application/json")
        .with_body(r#"{"detail":"invalid path"}"#)
        .create_async()
        .await;
    let client = ApiClient::new(&srv.url(), "vsk_test").unwrap();
    let err = client.push(&push_req("a.md", "aGk=")).await.unwrap_err();
    match err {
        ApiError::Rejected { status, detail } => {
            assert_eq!(status, 400);
            assert_eq!(detail, "invalid path");
        }
        other => panic!("expected ApiError::Rejected, got {other:?}"),
    }
}

/// 422 is the same permanent class; a non-JSON body is carried raw.
#[tokio::test]
async fn push_422_carries_raw_body_when_not_json() {
    let mut srv = Server::new_async().await;
    let _m = srv
        .mock("POST", "/api/sync/push")
        .with_status(422)
        .with_body("content contains NUL bytes; not storable")
        .create_async()
        .await;
    let client = ApiClient::new(&srv.url(), "vsk_test").unwrap();
    let err = client.push(&push_req("a.md", "aGk=")).await.unwrap_err();
    match err {
        ApiError::Rejected { status, detail } => {
            assert_eq!(status, 422);
            assert_eq!(detail, "content contains NUL bytes; not storable");
        }
        other => panic!("expected ApiError::Rejected, got {other:?}"),
    }
}

/// The detail is bounded (512 chars) so a pathological body cannot bloat a
/// log line or the rejected ledger.
#[tokio::test]
async fn push_400_detail_is_capped_at_512_chars() {
    let mut srv = Server::new_async().await;
    let long = "x".repeat(5000);
    let _m = srv
        .mock("POST", "/api/sync/push")
        .with_status(400)
        .with_body(format!(r#"{{"detail":"{long}"}}"#))
        .create_async()
        .await;
    let client = ApiClient::new(&srv.url(), "vsk_test").unwrap();
    let err = client.push(&push_req("a.md", "aGk=")).await.unwrap_err();
    match err {
        ApiError::Rejected { detail, .. } => {
            assert!(
                detail.chars().count() <= 512,
                "detail must be capped, got {} chars",
                detail.chars().count()
            );
        }
        other => panic!("expected ApiError::Rejected, got {other:?}"),
    }
}

/// A 500 stays the generic (retriable) `Server` class: only 400/422 are
/// promoted to the permanent `Rejected` variant.
#[tokio::test]
async fn push_500_stays_server_error() {
    let mut srv = Server::new_async().await;
    let _m = srv
        .mock("POST", "/api/sync/push")
        .with_status(500)
        .with_body(r#"{"detail":"boom"}"#)
        .create_async()
        .await;
    let client = ApiClient::new(&srv.url(), "vsk_test").unwrap();
    let err = client.push(&push_req("a.md", "aGk=")).await.unwrap_err();
    assert!(matches!(err, ApiError::Server(500)), "got {err:?}");
}
