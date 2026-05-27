use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ApiError {
    #[error("unauthorized — token rejected")]
    Unauthorized,
    #[error("forbidden — subscriber revoked")]
    Forbidden,
    #[error("not found: {0}")]
    NotFound(String),
    #[error("server error: HTTP {0}")]
    Server(u16),
    #[error("rate limited; retry after {retry_after_secs} seconds")]
    RateLimited { retry_after_secs: u64 },
    #[error("network error: {0}")]
    Network(#[from] reqwest::Error),
    /// 409 Conflict — base_hash CAS mismatch on push. Server returns the
    /// hash it expected (current server-side content_hash) so the client
    /// can fetch+merge+replay. Per R2 + mandate §5 push contract.
    #[error("conflict — base_hash mismatch (expected={expected_hash:?})")]
    Conflict { expected_hash: Option<String> },
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HealthSnapshot {
    pub subscriber_id: String,
    pub scope_roots: Vec<String>,
    pub scope_excludes: Vec<String>,
    pub materializer_mode: String,
    pub shadow_path: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NotePayload {
    pub path: String,
    pub frontmatter: serde_json::Value,
    pub body: String,
    pub sha256: String,
    pub modified: String,
    pub file_mtime: Option<String>,
}

/// 4-state push outcome envelope (mandate §5, post-S473 amendments).
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PushStatus {
    /// Server accepted the push and emitted a new SSE event.
    Accepted,
    /// Server detected a non-overlapping concurrent edit and merged it for
    /// the client. The merged content is returned in `merged_content`.
    Merged,
    /// Server detected an overlapping concurrent edit and produced a
    /// content stream containing `<<<<<<<` style markers. The marked-up
    /// content is returned in `merged_content` for the user to resolve.
    ConflictMarkers,
    /// Server hit a recoverable error (rate-limit, transient backend
    /// failure). `message` carries the reason.
    Error,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushApiAction {
    Create,
    Modify,
    Delete,
}

impl Serialize for PushApiAction {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let v = match self {
            PushApiAction::Create => "create",
            PushApiAction::Modify => "modify",
            PushApiAction::Delete => "delete",
        };
        s.serialize_str(v)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct PushRequest<'a> {
    pub device_id: &'a str,
    pub path: &'a str,
    /// Server `SyncPushRequest` (nexus/api/sync_routes.py) names this field
    /// `content`, not `content_b64`. Mismatch → HTTP 422. Verified S473.
    /// Caller is responsible for b64-encoding raw bytes.
    #[serde(rename = "content")]
    pub content_b64: &'a str,
    /// Server requires `base_hash` as a NON-optional string (`Field(...)`).
    /// Sending `null` → HTTP 422. When the client has no known base (create,
    /// or a modify where it never pulled the server copy), send empty string;
    /// the server treats "" as "no known base" and either accepts (server has
    /// no row) or returns 409 conflict with its current hash. Verified S473.
    pub base_hash: &'a str,
    pub action: PushApiAction,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PushResponse {
    pub status: PushStatus,
    pub seq: Option<u64>,
    pub content_hash: Option<String>,
    pub server_hash: Option<String>,
    pub server_seq: Option<u64>,
    pub merged_content: Option<String>,
    pub message: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct ConflictBody {
    expected_hash: Option<String>,
}

/// One entry in the local-manifest payload sent to `/api/sync/reconcile`.
/// Mandate §3 "Verify and repair all files" / §5 reconcile contract.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReconcileEntry {
    pub path: String,
    pub content_hash: String,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReconcileRequest<'a> {
    pub device_id: &'a str,
    // Server's ReconcileRequest (nexus/core/sync/models.py) names this field
    // `manifest`, not `entries`. Mismatch → HTTP 422. Verified S473.
    #[serde(rename = "manifest")]
    pub entries: &'a [ReconcileEntry],
}

/// Action the server tells the client to perform. Matches the live server
/// vocabulary (nexus/core/sync) — verified S473.
/// `Push` — client should upload its local canonical (reason=client_only or
///          hash_mismatch).
/// `Pull` — server has it, client missing (reason=server_only). The SSE
///          consumer materializes it; verify_repair does NOT write the FS.
/// `Skip` — in sync (reason=identical); no-op.
/// There is NO "delete" action — the server never asks the client to delete.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ReconcileAction {
    Push,
    Pull,
    Skip,
}

/// One entry in the server's reconcile `actions` array.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct ReconcileActionEntry {
    pub path: String,
    pub action: ReconcileAction,
    /// Server reason vocabulary: "client_only", "server_only",
    /// "hash_mismatch", "identical".
    pub reason: String,
    /// Server-side canonical content_hash. Useful for the client to compare
    /// or to skip if it already matches local (defense-in-depth).
    #[serde(default)]
    pub server_hash: Option<String>,
    #[serde(default)]
    pub server_size: Option<i64>,
}

#[derive(Debug, Clone, Deserialize, Default, PartialEq, Eq)]
pub struct ReconcileStats {
    #[serde(default)]
    pub push: u64,
    #[serde(default)]
    pub pull: u64,
    #[serde(default)]
    pub identical: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ReconcileResponse {
    pub actions: Vec<ReconcileActionEntry>,
    #[serde(default)]
    pub stats: ReconcileStats,
    #[serde(default)]
    pub server_time: String,
}

pub struct ApiClient {
    base_url: String,
    token: String,
    http: Client,
}

impl ApiClient {
    pub fn new(base_url: &str, token: &str) -> Result<Self, ApiError> {
        let http = Client::builder()
            .timeout(Duration::from_secs(30))
            .pool_max_idle_per_host(4)
            .user_agent("lattice-vault-sync/0.3.0")
            .build()?;
        Ok(Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            token: token.to_string(),
            http,
        })
    }

    pub async fn health(&self) -> Result<HealthSnapshot, ApiError> {
        let resp = self
            .http
            .get(format!("{}/api/sync/health", self.base_url))
            .bearer_auth(&self.token)
            .send()
            .await?;
        match resp.status() {
            StatusCode::OK => Ok(resp.json().await?),
            StatusCode::UNAUTHORIZED => Err(ApiError::Unauthorized),
            StatusCode::FORBIDDEN => Err(ApiError::Forbidden),
            StatusCode::SERVICE_UNAVAILABLE => {
                let retry = resp
                    .headers()
                    .get("retry-after")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(60);
                Err(ApiError::RateLimited {
                    retry_after_secs: retry,
                })
            }
            s => Err(ApiError::Server(s.as_u16())),
        }
    }

    pub async fn fetch_note(&self, path: &str) -> Result<NotePayload, ApiError> {
        let resp = self
            .http
            .get(format!("{}/api/sync/note", self.base_url))
            .query(&[("path", path)])
            .bearer_auth(&self.token)
            .send()
            .await?;
        match resp.status() {
            StatusCode::OK => Ok(resp.json().await?),
            StatusCode::UNAUTHORIZED => Err(ApiError::Unauthorized),
            StatusCode::FORBIDDEN => Err(ApiError::Forbidden),
            StatusCode::NOT_FOUND => Err(ApiError::NotFound(path.to_string())),
            StatusCode::SERVICE_UNAVAILABLE => {
                let retry = resp
                    .headers()
                    .get("retry-after")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(60);
                Err(ApiError::RateLimited {
                    retry_after_secs: retry,
                })
            }
            s => Err(ApiError::Server(s.as_u16())),
        }
    }

    /// POST a local edit to the server. Mandate §5 push contract. See module
    /// docs / `PushStatus` for the 4-state response envelope. Maps 409 to
    /// `ApiError::Conflict { expected_hash }` so the caller (conflict
    /// resolver) can re-fetch+merge+replay per R2.
    /// POST a local manifest to the server's reconcile endpoint. The server
    /// returns an `actions` list (Push / Pull / Skip) plus `stats` and
    /// `server_time`. Per mandate §3 + §5 reconcile contract. Used by
    /// `verify_repair::VerifyRepair::run` for the owner-invoked full-vault
    /// rescan.
    pub async fn reconcile(
        &self,
        req: &ReconcileRequest<'_>,
    ) -> Result<ReconcileResponse, ApiError> {
        // v0.3.1: per-call timeout override. The client default (30s) is
        // sized for small request/response pairs (health, single push) and
        // blew up on the first real reconcile against a 6k+ note vault:
        // the SERVER returned a 28k-entry plan (Reconciliation plan: 28152
        // push, 0 pull, 0 identical -- container log 2026-05-27 16:37:44),
        // but reqwest's 30s wall-clock fired while reading the response
        // body, so the daemon surfaced "network error: error sending request"
        // even though the server logged 200 OK. 300s gives initial-pair on
        // a multi-tens-of-thousands-of-files vault plenty of room without
        // hanging forever on a truly stuck request.
        let resp = self
            .http
            .post(format!("{}/api/sync/reconcile", self.base_url))
            .bearer_auth(&self.token)
            .json(req)
            .timeout(Duration::from_secs(300))
            .send()
            .await?;
        match resp.status() {
            StatusCode::OK => Ok(resp.json().await?),
            StatusCode::UNAUTHORIZED => Err(ApiError::Unauthorized),
            StatusCode::FORBIDDEN => Err(ApiError::Forbidden),
            StatusCode::SERVICE_UNAVAILABLE => {
                let retry = resp
                    .headers()
                    .get("retry-after")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(60);
                Err(ApiError::RateLimited {
                    retry_after_secs: retry,
                })
            }
            s => Err(ApiError::Server(s.as_u16())),
        }
    }

    pub async fn push(&self, req: &PushRequest<'_>) -> Result<PushResponse, ApiError> {
        let resp = self
            .http
            .post(format!("{}/api/sync/push", self.base_url))
            .bearer_auth(&self.token)
            .json(req)
            .send()
            .await?;
        let status = resp.status();
        match status {
            StatusCode::OK | StatusCode::CREATED => Ok(resp.json().await?),
            StatusCode::CONFLICT => {
                let body: ConflictBody = resp.json().await.unwrap_or(ConflictBody {
                    expected_hash: None,
                });
                Err(ApiError::Conflict {
                    expected_hash: body.expected_hash,
                })
            }
            StatusCode::UNAUTHORIZED => Err(ApiError::Unauthorized),
            StatusCode::FORBIDDEN => Err(ApiError::Forbidden),
            StatusCode::NOT_FOUND => Err(ApiError::NotFound(req.path.to_string())),
            StatusCode::SERVICE_UNAVAILABLE => {
                let retry = resp
                    .headers()
                    .get("retry-after")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(60);
                Err(ApiError::RateLimited {
                    retry_after_secs: retry,
                })
            }
            s => Err(ApiError::Server(s.as_u16())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Regression guard for the S473 server/client contract mismatch: the
    /// daemon used to expect `{"diff":[...]}` but the live server returns
    /// `{"actions":[...],"stats":{...},"server_time":"..."}`. Deserialize the
    /// EXACT verified-live server response body and assert it parses + every
    /// field populates with the correct vocabulary.
    #[test]
    fn reconcile_response_deserializes_live_server_contract() {
        let body = r#"{
            "actions": [
                {"path": "test/probe.md", "action": "push", "reason": "client_only", "server_hash": null, "server_size": null}
            ],
            "stats": {"push": 1, "pull": 0, "identical": 0},
            "server_time": "2026-05-27T17:04:04.112114+00:00"
        }"#;

        let resp: ReconcileResponse = serde_json::from_str(body).expect("must parse live contract");

        assert_eq!(resp.actions.len(), 1);
        let a = &resp.actions[0];
        assert_eq!(a.path, "test/probe.md");
        assert_eq!(a.action, ReconcileAction::Push);
        assert_eq!(a.reason, "client_only");
        assert_eq!(a.server_hash, None);
        assert_eq!(a.server_size, None);

        assert_eq!(resp.stats.push, 1);
        assert_eq!(resp.stats.pull, 0);
        assert_eq!(resp.stats.identical, 0);
        assert_eq!(resp.server_time, "2026-05-27T17:04:04.112114+00:00");
    }

    /// All three action variants + reasons round-trip from the wire vocabulary.
    #[test]
    fn reconcile_action_vocabulary_parses() {
        let body = r#"{
            "actions": [
                {"path": "a.md", "action": "push", "reason": "hash_mismatch", "server_hash": "deadbeef", "server_size": 11},
                {"path": "b.md", "action": "pull", "reason": "server_only", "server_hash": "abc", "server_size": 42},
                {"path": "c.md", "action": "skip", "reason": "identical"}
            ],
            "stats": {"push": 1, "pull": 1, "identical": 1},
            "server_time": ""
        }"#;
        let resp: ReconcileResponse = serde_json::from_str(body).unwrap();
        assert_eq!(resp.actions[0].action, ReconcileAction::Push);
        assert_eq!(resp.actions[1].action, ReconcileAction::Pull);
        assert_eq!(resp.actions[2].action, ReconcileAction::Skip);
        // Missing optional fields default cleanly.
        assert_eq!(resp.actions[2].server_hash, None);
        assert_eq!(resp.actions[2].server_size, None);
    }
}
