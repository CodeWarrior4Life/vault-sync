use crate::api_client::ApiClient;
use crate::materializer::Materializer;
use crate::scope::{is_safe_path, path_in_scope};
use crate::tray_state::{ConnectionStatus, SharedTrayState};
use eventsource_client::{Client, Error as SseError, SSE};
use futures::TryStreamExt;
use serde::Deserialize;
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::sleep;
use tracing::{debug, error, warn};

fn default_op() -> String {
    "UPSERT".to_string()
}

#[derive(Debug, Deserialize)]
struct Envelope {
    /// INSERT | UPDATE | DELETE | (server's catchup path omits this field —
    /// default UPSERT so the daemon doesn't silently drop catchup envelopes
    /// when reconnecting after a network blip. Root cause of S476's
    /// "shadow materializer never writes" symptom: serde was rejecting
    /// every catchup payload with `missing field op` before reaching the
    /// materializer, and stderr logs go to /dev/null on Windows GUI subsystem
    /// so the failures were invisible.)
    #[serde(default = "default_op")]
    op: String,
    path: String,
    #[allow(dead_code)]
    phase: String, // lint_pending | lint_complete | enrichment_complete
    /// v0.3.5: accept lsn as either int OR string. Server cache_writer now
    /// stringifies via str(...) but the PG trigger function (notify_vault_
    /// note_change) emits via `txid_current()` which is BIGINT, and any
    /// other future emitter could choose either format. `Value` accepts
    /// anything serde-deserializable; we don't actually use lsn anywhere
    /// in the daemon's hot path, just thread it through.
    #[serde(default)]
    #[allow(dead_code)]
    lsn: Option<serde_json::Value>,
}

pub struct SseConsumer {
    nexus_url: String,
    token: String,
    scope_roots: Vec<String>,
    scope_excludes: Vec<String>,
    api: ApiClient,
    materializer: Materializer,
    tray_state: Option<SharedTrayState>,
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
            tray_state: None,
        })
    }

    pub fn with_tray_state(mut self, state: SharedTrayState) -> Self {
        self.tray_state = Some(state);
        self
    }

    fn ts_set(&self, status: ConnectionStatus) {
        if let Some(s) = &self.tray_state {
            if let Ok(mut st) = s.write() {
                st.set_status(status);
            }
        }
    }

    fn ts_event(&self) {
        if let Some(s) = &self.tray_state {
            if let Ok(mut st) = s.write() {
                st.record_event();
            }
        }
    }

    fn ts_err(&self, status: ConnectionStatus, msg: String) {
        if let Some(s) = &self.tray_state {
            if let Ok(mut st) = s.write() {
                st.set_error(status, msg);
            }
        }
    }

    pub async fn run(
        &self,
        mut last_event_id: Option<String>,
        mut shutdown: watch::Receiver<bool>,
    ) -> anyhow::Result<()> {
        let mut backoff = Duration::from_secs(1);
        self.ts_set(ConnectionStatus::Connecting);
        loop {
            if *shutdown.borrow() {
                break;
            }
            match self.run_one_session(&mut last_event_id).await {
                Ok(()) => {
                    backoff = Duration::from_secs(1);
                    self.ts_set(ConnectionStatus::Reconnecting);
                }
                Err(SseError::StreamClosed)
                | Err(SseError::Eof)
                | Err(SseError::UnexpectedEof)
                | Err(SseError::TimedOut)
                | Err(SseError::HttpStream(_)) => {
                    warn!("SSE disconnected; reconnecting in {:?}", backoff);
                    self.ts_set(ConnectionStatus::Reconnecting);
                    tokio::select! {
                        _ = sleep(backoff) => {}
                        _ = shutdown.changed() => { break; }
                    }
                    backoff = (backoff * 2).min(Duration::from_secs(60));
                }
                Err(SseError::UnexpectedResponse(resp, body)) => {
                    let status = resp.status();
                    if status == 401 || status == 403 {
                        error!(status = status, "SSE auth failure; not retrying");
                        self.ts_err(
                            ConnectionStatus::AuthFailed,
                            format!("token rejected (HTTP {status})"),
                        );
                        return Err(SseError::UnexpectedResponse(resp, body).into());
                    }
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
                    self.ts_err(
                        ConnectionStatus::Reconnecting,
                        format!("HTTP {status}, retry in {retry_secs}s"),
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
                    self.ts_err(ConnectionStatus::Error, e.to_string());
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
                    self.ts_set(ConnectionStatus::Connected);
                }
                SSE::Event(ev) => {
                    if ev.event_type != "enrichment_complete" {
                        debug!("intermediate event observed: {}", ev.event_type);
                        continue;
                    }
                    self.ts_event();
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
