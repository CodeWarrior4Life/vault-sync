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
            .build()?;
        Ok(Self { base_url: base_url.trim_end_matches('/').to_string(), token: token.to_string(), http })
    }

    pub async fn health(&self) -> Result<HealthSnapshot, ApiError> {
        let resp = self.http
            .get(format!("{}/api/sync/health", self.base_url))
            .bearer_auth(&self.token)
            .send().await?;
        match resp.status() {
            StatusCode::OK => Ok(resp.json().await?),
            StatusCode::UNAUTHORIZED => Err(ApiError::Unauthorized),
            StatusCode::FORBIDDEN => Err(ApiError::Forbidden),
            s if s.is_server_error() => Err(ApiError::Server(s.as_u16())),
            s => Err(ApiError::Server(s.as_u16())),
        }
    }

    pub async fn fetch_note(&self, path: &str) -> Result<NotePayload, ApiError> {
        let resp = self.http
            .get(format!("{}/api/sync/note", self.base_url))
            .query(&[("path", path)])
            .bearer_auth(&self.token)
            .send().await?;
        match resp.status() {
            StatusCode::OK => Ok(resp.json().await?),
            StatusCode::UNAUTHORIZED => Err(ApiError::Unauthorized),
            StatusCode::FORBIDDEN => Err(ApiError::Forbidden),
            StatusCode::NOT_FOUND => Err(ApiError::NotFound(path.to_string())),
            StatusCode::SERVICE_UNAVAILABLE => {
                let retry = resp.headers().get("retry-after")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(60);
                Err(ApiError::RateLimited { retry_after_secs: retry })
            }
            s => Err(ApiError::Server(s.as_u16())),
        }
    }
}
