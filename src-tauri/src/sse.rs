use crate::api_client::ApiClient;
use crate::materializer::Materializer;
use crate::scope::{is_safe_path, path_in_scope};
use eventsource_client::{Client, Error as SseError, SSE};
use futures::TryStreamExt;
use serde::Deserialize;
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::sleep;
use tracing::{debug, error, warn};

#[derive(Debug, Deserialize)]
struct Envelope {
    op: String, // INSERT | UPDATE | DELETE
    path: String,
    #[allow(dead_code)]
    phase: String, // lint_pending | lint_complete | enrichment_complete
    #[serde(default)]
    #[allow(dead_code)]
    lsn: Option<String>,
}

pub struct SseConsumer {
    nexus_url: String,
    token: String,
    scope_roots: Vec<String>,
    scope_excludes: Vec<String>,
    api: ApiClient,
    materializer: Materializer,
}

impl SseConsumer {
    pub fn new(
        nexus_url: String,
        token: String,
        scope_roots: Vec<String>,
        scope_excludes: Vec<String>,
        materializer: Materializer,
    ) -> anyhow::Result<Self> {
        let api = ApiClient::new(&nexus_url, &token)?;
        Ok(Self {
            nexus_url,
            token,
            scope_roots,
            scope_excludes,
            api,
            materializer,
        })
    }

    pub async fn run(
        &self,
        mut last_event_id: Option<String>,
        mut shutdown: watch::Receiver<bool>,
    ) -> anyhow::Result<()> {
        let mut backoff = Duration::from_secs(1);
        loop {
            if *shutdown.borrow() {
                break;
            }
            match self.run_one_session(&mut last_event_id).await {
                Ok(()) => {
                    backoff = Duration::from_secs(1);
                }
                Err(SseError::StreamClosed)
                | Err(SseError::Eof)
                | Err(SseError::UnexpectedEof)
                | Err(SseError::TimedOut)
                | Err(SseError::HttpStream(_)) => {
                    warn!("SSE disconnected; reconnecting in {:?}", backoff);
                    // Respect shutdown during backoff sleep
                    tokio::select! {
                        _ = sleep(backoff) => {}
                        _ = shutdown.changed() => { break; }
                    }
                    backoff = (backoff * 2).min(Duration::from_secs(60));
                }
                Err(SseError::UnexpectedResponse(resp, body)) => {
                    let status = resp.status();
                    // CF-S470-T22-401-fastfail: auth-failures are not transient —
                    // retrying with the same token cannot recover. Propagate as fatal
                    // so the daemon surfaces the failure (tray red + user re-pair).
                    if status == 401 || status == 403 {
                        error!(status = status, "SSE auth failure; not retrying");
                        return Err(SseError::UnexpectedResponse(resp, body).into());
                    }
                    // Otherwise (5xx etc.) treat as transient and back off.
                    let retry_secs = resp
                        .get_header_value("retry-after")
                        .ok()
                        .flatten()
                        .and_then(|s| s.parse::<u64>().ok())
                        .unwrap_or(backoff.as_secs());
                    warn!(
                        status = status,
                        retry_after = retry_secs,
                        "SSE server error; backing off"
                    );
                    let wait = Duration::from_secs(retry_secs);
                    tokio::select! {
                        _ = sleep(wait) => {}
                        _ = shutdown.changed() => { break; }
                    }
                    backoff = (backoff * 2).min(Duration::from_secs(60));
                }
                Err(e) => {
                    error!("SSE fatal: {e}");
                    return Err(e.into());
                }
            }
        }
        Ok(())
    }

    async fn run_one_session(&self, last_event_id: &mut Option<String>) -> Result<(), SseError> {
        let url = format!("{}/api/sync/events", self.nexus_url);
        let mut builder = eventsource_client::ClientBuilder::for_url(&url)?
            .header("Authorization", &format!("Bearer {}", self.token))?;
        if let Some(id) = last_event_id.as_deref() {
            builder = builder.last_event_id(id.to_owned());
        }
        let client = builder.build();
        let mut stream = client.stream();
        while let Some(event) = stream.try_next().await? {
            match event {
                SSE::Connected(_) => {
                    debug!("SSE connected");
                }
                SSE::Event(ev) => {
                    if ev.event_type != "enrichment_complete" {
                        debug!("intermediate event observed: {}", ev.event_type);
                        continue;
                    }
                    let env: Envelope = match serde_json::from_str(&ev.data) {
                        Ok(e) => e,
                        Err(e) => {
                            error!("envelope parse failed: {e}");
                            continue;
                        }
                    };
                    if !is_safe_path(&env.path) {
                        error!("path traversal rejected at SSE: {}", env.path);
                        continue;
                    }
                    if !path_in_scope(&env.path, &self.scope_roots, &self.scope_excludes) {
                        debug!("defensive scope drop: {}", env.path);
                        continue;
                    }
                    if env.op == "DELETE" {
                        if let Err(e) = self.materializer.soft_delete(&env.path) {
                            error!("materializer soft_delete failed: {e}");
                        }
                    } else {
                        match self.api.fetch_note(&env.path).await {
                            Ok(payload) => {
                                if let Err(e) = self.materializer.write(&payload) {
                                    error!("materializer write failed: {e}");
                                }
                            }
                            Err(e) => error!("body fetch failed for {}: {e}", env.path),
                        }
                    }
                    if let Some(id) = ev.id {
                        if !id.is_empty() {
                            *last_event_id = Some(id);
                        }
                    }
                }
                SSE::Comment(_) => {} // heartbeat
            }
        }
        Ok(())
    }
}
