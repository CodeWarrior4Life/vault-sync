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
    let mut srv = Server::new_async().await;
    let _m = srv.mock("GET", "/api/sync/note?path=foo.md")
        .with_status(200)
        .with_body(r#"{"path":"foo.md","frontmatter":{"title":"X"},"body":"hello","sha256":"abc","modified":"2026-05-25T00:00:00Z","file_mtime":"2026-05-25T00:00:00Z"}"#)
        .create_async().await;
    let client = ApiClient::new(&srv.url(), "vsk_test").unwrap();
    let np = client.fetch_note("foo.md").await.unwrap();
    assert_eq!(np.body, "hello");
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
