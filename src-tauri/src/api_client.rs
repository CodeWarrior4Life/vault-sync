use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use thiserror::Error;

/// S477 v0.3.8 (B): User-Agent + (C): daemon_version reporting helpers.
///
/// Single source of truth for "what version + platform am I" — used by both
/// the HTTP `User-Agent` header on every API call (so server logs stamp
/// every request with daemon version + host platform) AND by the daemon's
/// startup self-PATCH to `/api/sync/subscribers/me` (so the
/// `vault_subscribers` row carries the same values for admin observability).
pub fn daemon_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

pub fn daemon_platform() -> &'static str {
    match (std::env::consts::OS, std::env::consts::ARCH) {
        ("windows", "x86_64") => "windows-x86_64",
        ("macos", "x86_64") => "macos-x86_64",
        ("macos", "aarch64") => "macos-aarch64",
        ("linux", "x86_64") => "linux-x86_64",
        ("linux", "aarch64") => "linux-aarch64",
        _ => "unknown",
    }
}

pub fn user_agent_string() -> String {
    format!(
        "lattice-vault-sync/{}/{}",
        daemon_version(),
        daemon_platform()
    )
}

/// AR-009 (TKT-c41c2225): deserialize a `200 OK` body into `T`, and on failure
/// produce a fully diagnosable `ApiError::Decode` instead of reqwest's opaque
/// "error decoding response body".
///
/// Forensics captured (no note-content leak): HTTP status, content-type, body
/// length, a request-id header if present, and the structural serde error
/// (field position + expected/found type). A bounded raw `body_sample` is
/// attached ONLY when the content-type is not JSON -- an HTML/proxy error page
/// is diagnostic and is not user content; a JSON structural mismatch means the
/// body IS the note, so it is never sampled.
pub(crate) async fn decode_json<T: serde::de::DeserializeOwned>(
    resp: reqwest::Response,
    context: &'static str,
) -> Result<T, ApiError> {
    let status = resp.status().as_u16();
    let content_type = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    // request-id, in preference order (server, then generic, then cloudflare).
    let request_id = ["x-request-id", "x-correlation-id", "cf-ray"]
        .iter()
        .find_map(|h| {
            resp.headers()
                .get(*h)
                .and_then(|v| v.to_str().ok())
                .map(|s| format!("{h}={s}"))
        });
    // Read the raw bytes ourselves so a decode failure keeps the forensics
    // (reqwest's `.json()` consumes the body and discards them).
    let bytes = resp.bytes().await?;
    let body_len = bytes.len();
    match serde_json::from_slice::<T>(&bytes) {
        Ok(v) => Ok(v),
        Err(e) => {
            let is_json = content_type.starts_with("application/json");
            let body_sample = if is_json {
                // The body is the (structurally-bad) note payload -- do NOT
                // sample it; the serde position/type carries the signal.
                None
            } else {
                // Non-JSON (HTML/proxy error page) -- a bounded prefix is safe
                // and diagnostic. Cap at 256 bytes, lossy-decoded.
                const SAMPLE_CAP: usize = 256;
                let end = bytes.len().min(SAMPLE_CAP);
                Some(String::from_utf8_lossy(&bytes[..end]).into_owned())
            };
            Err(ApiError::Decode(Box::new(DecodeDetail {
                context,
                status,
                content_type,
                body_len,
                request_id,
                serde_error: e.to_string(),
                body_sample,
            })))
        }
    }
}

/// AR-009 (TKT-c41c2225): forensics for a decode failure. Boxed inside
/// `ApiError::Decode` so the common-case `ApiError` stays small (clippy
/// `result_large_err`). See [`decode_json`] for the no-content-leak rules.
#[derive(Debug, Clone)]
pub struct DecodeDetail {
    pub context: &'static str,
    pub status: u16,
    pub content_type: String,
    pub body_len: usize,
    pub request_id: Option<String>,
    pub serde_error: String,
    pub body_sample: Option<String>,
}

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
    /// AR-009 (TKT-c41c2225): the transport succeeded (HTTP 200) but the body
    /// could not be deserialized into the expected type. The bare
    /// `reqwest::Error` for this case Displays only as "error decoding response
    /// body", which is undiagnosable in a log. This variant captures the
    /// forensics WITHOUT leaking note content: HTTP status, content-type,
    /// body length, a request-id if the server/proxy set one, and the
    /// structural serde error (which names the field position + the
    /// expected-vs-found type, e.g. `invalid type: null, expected a string at
    /// line 1 column 4523` -- a position, never the note body). A bounded raw
    /// `body_sample` is attached ONLY when the content-type is NOT JSON (an
    /// HTML/proxy error page is diagnostic and is not user content); for a
    /// JSON structural mismatch the body IS the note, so no sample is taken.
    #[error(
        "decode error in {}: HTTP {} content-type={} body_len={} request_id={:?} serde=({}){}",
        .0.context, .0.status, .0.content_type, .0.body_len, .0.request_id, .0.serde_error,
        .0.body_sample.as_ref().map(|s| format!(" sample={s:?}")).unwrap_or_default()
    )]
    Decode(Box<DecodeDetail>),
    /// 409 Conflict — base_hash CAS mismatch on push. Server returns the
    /// hash it expected (current server-side content_hash) so the client
    /// can fetch+merge+replay. Per R2 + mandate §5 push contract.
    #[error("conflict — base_hash mismatch (expected={expected_hash:?})")]
    Conflict { expected_hash: Option<String> },
    /// HTTP 426 Upgrade Required — the server's Piece 1 min-daemon-version
    /// gate (`NEXUS_SYNC_MIN_DAEMON_VERSION`, spec S5) rejected this daemon as
    /// too old. PERMANENT until the binary is upgraded: the caller must fail
    /// loudly and back off, NEVER retry-loop. `detail` is the raw response
    /// body (the server names the required version in it).
    #[error("upgrade required - server min-daemon-version gate rejected this daemon: {detail}")]
    UpgradeRequired { detail: String },
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HealthSnapshot {
    pub subscriber_id: String,
    pub scope_roots: Vec<String>,
    pub scope_excludes: Vec<String>,
    pub materializer_mode: String,
    pub shadow_path: Option<String>,
}

/// v0.3.2: response shape from `PATCH /api/sync/subscribers/me`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SubscriberSelfState {
    pub ok: bool,
    pub subscriber_id: String,
    pub materializer_mode: String,
    pub shadow_path: Option<String>,
    pub scope_roots: Vec<String>,
    pub scope_excludes: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NotePayload {
    pub path: String,
    pub frontmatter: serde_json::Value,
    pub body: String,
    pub sha256: String,
    // AR-009 (TKT-c41c2225): the server returns `modified: null` for notes whose
    // vault_notes.modified column is NULL (observed live on the 2026-07-04 Daily
    // note). Typed as a non-optional `String`, serde rejected `null` and every
    // fetch of such a note failed with the opaque "error decoding response body",
    // failing the server-wins pull every reconcile cycle. `Option` + serde(default)
    // matches the already-optional `file_mtime`/`created` and is back-compatible
    // with servers that send a real string. No code reads this field (it is
    // metadata only; the materializer restores times from `file_mtime`/`created`).
    #[serde(default)]
    pub modified: Option<String>,
    // Server returns file_mtime as a unix-timestamp float (vault_notes.file_mtime
    // is double precision), e.g. 1779300968.264 — NOT a string. Typing this as
    // Option<String> made every /api/sync/note body-fetch fail serde decode
    // ("error decoding response body"), breaking the entire pull path (S485 e2e).
    pub file_mtime: Option<f64>,
    // S486 BUG 2 fix: the server returns the EXACT bytes it hashed for `sha256`
    // as `enriched_body` (server cache_writer computes sha256(enriched_body);
    // on a cache miss enriched_body == body_raw == the sha256 basis). The
    // pull-path materializer writes THIS verbatim instead of re-serializing
    // frontmatter (serde_yaml can never reproduce the original bytes), so the
    // strict integrity check passes by construction and the note stays
    // byte-faithful. `serde(default)` keeps back-compat with older servers that
    // omit the field (materializer falls back to frontmatter reconstruction).
    #[serde(default)]
    pub enriched_body: Option<String>,
    // Canonical note creation time as a unix-timestamp float (vault_notes.created),
    // e.g. 1709825021.0. The materializer restores this onto the written file's
    // birthtime (macOS) so re-materialization no longer resets it to "now" — the
    // ctime-clobber that reordered the operator's "Created"-sorted note list
    // (2026-06-05). serde(default) keeps back-compat with servers that omit it.
    #[serde(default)]
    pub created: Option<f64>,
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

/// One entry in the `/api/sync/reconcile-batch` request. `fs_hash` is the
/// raw-file-bytes SHA-256 the server keeps in `vault_reconcile_state.fs_hash`
/// — the same hash `verify_repair` already computes during its manifest walk.
///
/// v0.4.10: migrated off the dead legacy `/api/sync/reconcile` endpoint, which
/// gated on the SQLite `sync_devices` table the v0.3+ subscriber daemon never
/// registers in (it registers as a `vault_subscribers` row via
/// `PATCH /subscribers/me`, never `POST /register`). Every legacy reconcile
/// call 404'd "Device not registered", so the backstop never functioned.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ReconcileBatchItem {
    pub path: String,
    pub fs_hash: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReconcileBatchRequest {
    pub paths: Vec<ReconcileBatchItem>,
}

/// One per-path delta in the `/api/sync/reconcile-batch` response.
/// `state` is the server vocabulary:
/// * `"match"`             — local fs_hash == server fs_hash; in sync, skip.
/// * `"drift"`             — local differs from server; client should push.
/// * `"missing-on-server"` — server has no row for this path; client should push.
///
/// The server only returns deltas for paths the client SENT, so there is no
/// "pull" outcome here — server-only files are surfaced by the SSE/changes
/// feed, not by reconcile-batch.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct ReconcileDelta {
    pub path: String,
    pub state: String,
    #[serde(default)]
    pub server_hash: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct ReconcileBatchResponse {
    #[serde(default)]
    pub deltas: Vec<ReconcileDelta>,
}

/// One row from `GET /api/sync/changes` — a canonical note known to the
/// server, in `change_seq` order. The server returns `{path, file_mtime,
/// modified, indexed_at, lsn}` (NO content hash — clients compute their own FS
/// hash and use `/reconcile-batch` for server-side comparison). R6 uses only
/// `path` (to detect locally-missing canonical notes) and `lsn` (to page).
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct ChangeRow {
    pub path: String,
    #[serde(default)]
    pub file_mtime: f64,
    #[serde(default)]
    pub modified: String,
    #[serde(default)]
    pub indexed_at: String,
    pub lsn: i64,
}

/// Response envelope for `GET /api/sync/changes`. `next_lsn` is the cursor to
/// pass as `since` on the next page; it advances past skipped (cross-route)
/// rows so a full enumeration always terminates.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct ChangesResponse {
    #[serde(default)]
    pub changes: Vec<ChangeRow>,
    #[serde(default)]
    pub next_lsn: i64,
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
            .user_agent(user_agent_string())
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
            // AR-009: decode via the diagnosable helper, not `.json().await?`.
            // A structural mismatch now yields ApiError::Decode with forensics
            // instead of the opaque "error decoding response body".
            StatusCode::OK => decode_json(resp, "fetch_note").await,
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

    /// `GET /api/sync/changes?since=<lsn>&limit=<n>` — one page of canonical
    /// notes whose `change_seq > since`, in ascending `lsn` order. Paging from
    /// `since=0` enumerates the ENTIRE server-side canonical set for this
    /// subscriber's route, which is what the R6 pull-backfill walks to discover
    /// notes that exist on the server but were never materialized locally
    /// (created elsewhere / while this daemon was down beyond the SSE replay
    /// window). The SSE feed only ever delivers notes that get a *fresh*
    /// enrichment event, so it can never surface these — hence the dedicated
    /// full enumeration here.
    pub async fn get_changes(&self, since: i64, limit: u32) -> Result<ChangesResponse, ApiError> {
        let resp = self
            .http
            .get(format!("{}/api/sync/changes", self.base_url))
            .query(&[("since", since.to_string()), ("limit", limit.to_string())])
            .bearer_auth(&self.token)
            // A page is metadata-only (no note bodies), but a 5000-row page on a
            // large vault is still a non-trivial pair; give it the same 120s
            // room the reconcile manifest pair gets rather than the 30s default.
            .timeout(Duration::from_secs(120))
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

    /// POST a local edit to the server. Mandate §5 push contract. See module
    /// docs / `PushStatus` for the 4-state response envelope. Maps 409 to
    /// `ApiError::Conflict { expected_hash }` so the caller (conflict
    /// resolver) can re-fetch+merge+replay per R2.
    /// POST the local manifest to the server's reconcile endpoint and get back
    /// a per-path delta list (`match` / `drift` / `missing-on-server`).
    ///
    /// v0.4.10: targets `/api/sync/reconcile-batch` (the Postgres
    /// `vault_reconcile_state` contract, subscriber-bearer auth) — NOT the
    /// legacy `/api/sync/reconcile` (SQLite `sync_devices`), which 404'd
    /// "Device not registered" for every v0.3+ subscriber. Used by
    /// `verify_repair::VerifyRepair::run` for the reconciliation backstop.
    pub async fn reconcile_batch(
        &self,
        req: &ReconcileBatchRequest,
    ) -> Result<ReconcileBatchResponse, ApiError> {
        // 300s per-call timeout override. The client default (30s) is sized for
        // small request/response pairs (health, single push); a full-vault
        // manifest (tens of thousands of paths) produces a large pair that the
        // 30s wall-clock fired on mid-body-read, surfacing a spurious "network
        // error" even though the server logged 200 (verified S473 against a
        // 28k-entry plan). 300s gives the initial-pair on a huge vault room.
        let resp = self
            .http
            .post(format!("{}/api/sync/reconcile-batch", self.base_url))
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
            StatusCode::UPGRADE_REQUIRED => {
                let detail: String = resp
                    .text()
                    .await
                    .unwrap_or_default()
                    .chars()
                    .take(300)
                    .collect();
                Err(ApiError::UpgradeRequired { detail })
            }
            s => Err(ApiError::Server(s.as_u16())),
        }
    }

    /// v0.3.2: PATCH /api/sync/subscribers/me to mutate the caller's OWN
    /// materializer_mode (or other self-configurable fields the server
    /// allows). Per-subscriber bearer auth; only mutates the row tied to
    /// the bearer presented. Used by the pairing wizard's mode picker.
    pub async fn patch_self_subscriber(
        &self,
        materializer_mode: Option<&str>,
    ) -> Result<SubscriberSelfState, ApiError> {
        let body = serde_json::json!({
            "materializer_mode": materializer_mode,
        });
        let resp = self
            .http
            .patch(format!("{}/api/sync/subscribers/me", self.base_url))
            .bearer_auth(&self.token)
            .json(&body)
            .timeout(Duration::from_secs(30))
            .send()
            .await?;
        match resp.status() {
            StatusCode::OK => Ok(resp.json().await?),
            StatusCode::BAD_REQUEST => Err(ApiError::Server(400)),
            StatusCode::UNAUTHORIZED => Err(ApiError::Unauthorized),
            StatusCode::FORBIDDEN => Err(ApiError::Forbidden),
            s => Err(ApiError::Server(s.as_u16())),
        }
    }

    /// S477 v0.3.8 (C): PATCH /api/sync/subscribers/me with the running
    /// daemon's version + platform. Fires on every daemon startup so the
    /// `vault_subscribers.daemon_version` + `daemon_platform` columns
    /// always reflect what's actually running -- not whatever was set at
    /// first-pair time and never updated. Pairs with (B): User-Agent on
    /// every API call also stamps version + platform, so server logs +
    /// admin DB query both agree on the answer to "what version is this
    /// host running?". Fire-and-forget contract: failure is non-fatal
    /// (caller logs + continues).
    pub async fn patch_self_version(&self) -> Result<SubscriberSelfState, ApiError> {
        let body = serde_json::json!({
            "daemon_version": daemon_version(),
            "daemon_platform": daemon_platform(),
        });
        let resp = self
            .http
            .patch(format!("{}/api/sync/subscribers/me", self.base_url))
            .bearer_auth(&self.token)
            .json(&body)
            .timeout(Duration::from_secs(15))
            .send()
            .await?;
        match resp.status() {
            StatusCode::OK => Ok(resp.json().await?),
            StatusCode::BAD_REQUEST => Err(ApiError::Server(400)),
            StatusCode::UNAUTHORIZED => Err(ApiError::Unauthorized),
            StatusCode::FORBIDDEN => Err(ApiError::Forbidden),
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
            StatusCode::UPGRADE_REQUIRED => {
                let detail: String = resp
                    .text()
                    .await
                    .unwrap_or_default()
                    .chars()
                    .take(300)
                    .collect();
                Err(ApiError::UpgradeRequired { detail })
            }
            s => Err(ApiError::Server(s.as_u16())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// v0.4.30 ship gate: the self-reported daemon_version (User-Agent + the
    /// startup PATCH /subscribers/me the server's S5 min-version gate reads)
    /// must be the control-plane P0 version. Guards against shipping under a
    /// version string the fleet would mistake for an older release. Bumped for
    /// the TKT-86ae42a3 conflict-storm fix (B2' shadow-key migration +
    /// conflict-storm circuit breaker).
    #[test]
    fn daemon_version_is_0_4_32() {
        assert_eq!(daemon_version(), "0.4.32");
        assert!(user_agent_string().starts_with("lattice-vault-sync/0.4.32/"));
    }

    /// v0.4.10 contract guard: deserialize the EXACT `/api/sync/reconcile-batch`
    /// response body (`{"deltas":[{path,state,server_hash?}]}`) and assert every
    /// state-vocabulary variant parses, including the optional `server_hash`.
    #[test]
    fn reconcile_batch_response_deserializes_live_server_contract() {
        let body = r#"{
            "deltas": [
                {"path": "a.md", "state": "drift", "server_hash": "deadbeef"},
                {"path": "b.md", "state": "missing-on-server"},
                {"path": "c.md", "state": "match", "server_hash": "abc123"}
            ]
        }"#;

        let resp: ReconcileBatchResponse =
            serde_json::from_str(body).expect("must parse live reconcile-batch contract");

        assert_eq!(resp.deltas.len(), 3);
        assert_eq!(resp.deltas[0].path, "a.md");
        assert_eq!(resp.deltas[0].state, "drift");
        assert_eq!(resp.deltas[0].server_hash.as_deref(), Some("deadbeef"));
        // `missing-on-server` carries no server_hash → defaults to None.
        assert_eq!(resp.deltas[1].state, "missing-on-server");
        assert_eq!(resp.deltas[1].server_hash, None);
        assert_eq!(resp.deltas[2].state, "match");
        assert_eq!(resp.deltas[2].server_hash.as_deref(), Some("abc123"));
    }

    /// An empty / absent `deltas` array deserializes to an empty Vec (the
    /// in-sync case where every sent path matched but the server returned none).
    #[test]
    fn reconcile_batch_empty_deltas_parses() {
        let resp: ReconcileBatchResponse = serde_json::from_str(r#"{"deltas": []}"#).unwrap();
        assert!(resp.deltas.is_empty());
        let resp2: ReconcileBatchResponse = serde_json::from_str(r#"{}"#).unwrap();
        assert!(resp2.deltas.is_empty());
    }

    /// Request serializes to the server-expected `{"paths":[{path,fs_hash}]}`.
    #[test]
    fn reconcile_batch_request_serializes_paths_fs_hash() {
        let req = ReconcileBatchRequest {
            paths: vec![ReconcileBatchItem {
                path: "notes/a.md".into(),
                fs_hash: "abc".into(),
            }],
        };
        let j = serde_json::to_string(&req).unwrap();
        assert!(j.contains("\"paths\""));
        assert!(j.contains("\"path\":\"notes/a.md\""));
        assert!(j.contains("\"fs_hash\":\"abc\""));
        // No device_id / manifest leftovers from the legacy contract.
        assert!(!j.contains("device_id"));
        assert!(!j.contains("manifest"));
    }

    /// S5 client half (v0.4.28): HTTP 426 from the server's min-daemon-version
    /// gate must map to the DISTINCT ApiError::UpgradeRequired carrying the
    /// server's detail body, not the generic Server(426) (which the push
    /// client would treat as transient and retry-loop against a gate that can
    /// never pass without an upgrade).
    #[tokio::test]
    async fn push_maps_426_to_upgrade_required() {
        let mut srv = mockito::Server::new_async().await;
        let _m = srv
            .mock("POST", "/api/sync/push")
            .with_status(426)
            // The FROZEN server body shape (server plan Task 9). The mapping
            // must tolerate any body, so the assertion is contains(), not parse.
            .with_body(
                r#"{"detail":{"error":"daemon_version_below_minimum","minimum":"0.4.28","reported":"0.4.27"}}"#,
            )
            .create_async()
            .await;
        let api = ApiClient::new(&srv.url(), "vsk_test").unwrap();
        let req = PushRequest {
            device_id: "dev",
            path: "a.md",
            content_b64: "eA==",
            base_hash: "",
            action: PushApiAction::Modify,
        };
        match api.push(&req).await {
            Err(ApiError::UpgradeRequired { detail }) => {
                assert!(
                    detail.contains("0.4.28"),
                    "detail must carry the server body naming the required version, got: {detail}"
                );
            }
            other => panic!("expected UpgradeRequired, got {other:?}"),
        }
    }

    /// Same mapping on the reconcile endpoint (the gate covers mutating +
    /// reconcile endpoints, spec S5).
    #[tokio::test]
    async fn reconcile_batch_maps_426_to_upgrade_required() {
        let mut srv = mockito::Server::new_async().await;
        let _m = srv
            .mock("POST", "/api/sync/reconcile-batch")
            .with_status(426)
            // reported:null exercises the fail-closed never-reported case.
            .with_body(
                r#"{"detail":{"error":"daemon_version_below_minimum","minimum":"0.4.28","reported":null}}"#,
            )
            .create_async()
            .await;
        let api = ApiClient::new(&srv.url(), "vsk_test").unwrap();
        let req = ReconcileBatchRequest { paths: vec![] };
        match api.reconcile_batch(&req).await {
            Err(ApiError::UpgradeRequired { detail }) => {
                assert!(detail.contains("0.4.28"), "got: {detail}");
            }
            other => panic!("expected UpgradeRequired, got {other:?}"),
        }
    }
}
