use crate::api_client::ApiClient;
use crate::materializer::{MaterializeOutcome, Materializer, SkipReason};
use crate::scope::{is_safe_path, path_in_scope};
use crate::tray_state::{ConnectionStatus, SharedTrayState};
use eventsource_client::{Client, Error as SseError, SSE};
use futures::TryStreamExt;
use serde::Deserialize;
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::sleep;
use tracing::{debug, error, warn};

/// D2 (S511, TKT-2dc9a17e): extract a `u64` change_seq from the SSE envelope's
/// `lsn`, which the server may send as an integer OR a stringified integer
/// (the cache_writer stringifies via `str(...)`, the PG trigger emits BIGINT).
/// Used to deterministically name any conflict stash by server change_seq so
/// concurrent fleet writers converge on ONE filename instead of N copies.
/// Returns 0 when absent or unparseable (a `0`-suffixed stash is still valid).
fn change_seq_from_lsn(lsn: &Option<serde_json::Value>) -> u64 {
    match lsn {
        Some(serde_json::Value::Number(n)) => n.as_u64().unwrap_or(0),
        Some(serde_json::Value::String(s)) => s.parse::<u64>().unwrap_or(0),
        _ => 0,
    }
}

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
    /// anything serde-deserializable. S511 D2: we now USE it as the server
    /// change_seq to deterministically name conflict stashes.
    #[serde(default)]
    lsn: Option<serde_json::Value>,
}

/// TKT-35f75318: the SSE consumer's defensive scope decision, factored out as
/// a pure fn so it is unit-testable. `wire_path` is the route-RELATIVE path the
/// server sends (it strips `<route>/` in `_map_event_to_wire`), while
/// `scope_roots` / `scope_excludes` are STORAGE-key prefixes (the server
/// matches them on the canonical key before `to_wire`). `route` `None`/empty =
/// the bare vault route. Returns `true` only when the event may be applied;
/// the caller DROPS on `false`.
pub fn sse_event_in_scope(
    wire_path: &str,
    route: Option<&str>,
    scope_roots: &[String],
    scope_excludes: &[String],
) -> bool {
    let key = crate::file_watcher::storage_key_for(route, wire_path);
    path_in_scope(&key, scope_roots, scope_excludes)
}

pub struct SseConsumer {
    nexus_url: String,
    token: String,
    scope_roots: Vec<String>,
    scope_excludes: Vec<String>,
    /// TKT-35f75318: the materialized root's route (`None` = bare vault),
    /// used only to build the storage key for the defensive scope check.
    route: Option<String>,
    api: ApiClient,
    materializer: Materializer,
    tray_state: Option<SharedTrayState>,
    /// S477 v0.3.8 (A): where `last_event_id` is persisted after every
    /// successful SSE event. Loaded on daemon startup; written on each event
    /// with non-empty `ev.id`. Closes the catchup-on-restart gap that
    /// silently drops every event emitted while the daemon was down.
    /// CS-2 (W12): the held state dir (`StateFile::LastEventId`), written
    /// with an atomic replace. `None` => no persistence (unit tests).
    state_dir: Option<std::sync::Arc<crate::state_io::StateDir>>,
    /// TKT-fc62ea8a: max silence tolerated on the stream before the session
    /// is force-reconnected. See [`LIVENESS_WINDOW`].
    liveness_window: std::time::Duration,
    /// CS-3 §3.3 readiness: called on every `Connected` (the handles latch
    /// `sse_connected_once`).
    on_connected: Option<std::sync::Arc<dyn Fn() + Send + Sync>>,
}

/// CS-3 §3.5 (P13): the HTTP status of an SSE auth failure (401/403) that
/// made [`SseConsumer::run`] stop by design, if `e` is one.
pub fn auth_failure_status(e: &anyhow::Error) -> Option<u16> {
    match e.downcast_ref::<SseError>() {
        Some(SseError::UnexpectedResponse(resp, _)) => {
            let s = resp.status();
            (s == 401 || s == 403).then_some(s)
        }
        _ => None,
    }
}

/// TKT-fc62ea8a: event-arrival liveness window. The server emits a
/// `: keep-alive` SSE comment every 30s (sync_routes_p1 `heartbeat_interval`),
/// so a healthy stream ALWAYS delivers a frame — event or comment — within
/// that period. If NOTHING arrives for this window (3 missed keep-alives),
/// the transport is dead even though the socket may still look ESTABLISHED:
/// the 08-26/08-28 incident class was an origin restart behind the Cloudflare
/// tunnel leaving the edge TCP half-open, so `stream.try_next()` blocked
/// forever, the daemon never reconnected, and a host ran days on the ~30min
/// reconcile pull alone. On expiry the session returns `SseError::TimedOut`,
/// which the `run()` loop already handles: reconnect with backoff, resuming
/// from the persisted `last_event_id` so the server replays the gap.
const LIVENESS_WINDOW: std::time::Duration = std::time::Duration::from_secs(90);

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
            route: None,
            api,
            materializer,
            tray_state: None,
            state_dir: None,
            liveness_window: LIVENESS_WINDOW,
            on_connected: None,
        })
    }

    /// CS-3 §3.3: run `f` on every `Connected`.
    pub fn with_on_connected(mut self, f: std::sync::Arc<dyn Fn() + Send + Sync>) -> Self {
        self.on_connected = Some(f);
        self
    }

    /// TKT-fc62ea8a: override the liveness window (tests use sub-second
    /// windows; production keeps [`LIVENESS_WINDOW`]).
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn with_liveness_window(mut self, w: std::time::Duration) -> Self {
        self.liveness_window = w;
        self
    }

    /// TKT-35f75318: this consumer's defensive scope decision for one
    /// route-RELATIVE wire path (its own route, scope roots and excludes).
    /// The event loop calls this, so tests that drive it cover the real check.
    pub(crate) fn event_in_scope(&self, wire_path: &str) -> bool {
        sse_event_in_scope(
            wire_path,
            self.route.as_deref(),
            &self.scope_roots,
            &self.scope_excludes,
        )
    }

    /// TKT-35f75318: set the route of the root this consumer materializes into
    /// (the primary sync_root). Empty string = bare vault = `None`.
    pub fn with_route(mut self, route: &str) -> Self {
        self.route = if route.is_empty() {
            None
        } else {
            Some(route.to_string())
        };
        self
    }

    pub fn with_tray_state(mut self, state: SharedTrayState) -> Self {
        self.tray_state = Some(state);
        self
    }

    /// S477 v0.3.8 (A) / CS-2: persist last_event_id in the subscriber's
    /// held state dir (namespaced per subscriber by the bootstrap).
    pub fn with_state_dir(mut self, sd: &std::sync::Arc<crate::state_io::StateDir>) -> Self {
        self.state_dir = Some(std::sync::Arc::clone(sd));
        self
    }

    /// W12 (CS-2): atomic replace through the held dir (CS-1 unique temp,
    /// no create_dir_all). Failure is logged and swallowed — losing one
    /// event's persistence is not worth crashing the SSE loop. The next
    /// event will re-persist over the same file.
    pub(crate) fn persist_last_event_id(&self, id: &str) {
        let Some(sd) = &self.state_dir else {
            return;
        };
        if let Err(e) = sd.replace_atomic(crate::state_io::StateFile::LastEventId, id.as_bytes()) {
            warn!(error = %e, "last_event_id: persist failed");
        }
    }

    /// S477 v0.3.8 (A): read the persisted last_event_id (through the held
    /// dir fd). Caller uses this on daemon startup to resume from the right
    /// point so the server's catchup-on-reconnect replays only events
    /// emitted while the daemon was down (per sync_routes_p1.py
    /// event_stream catchup). Returns None if the file is absent, empty, or
    /// unreadable — any of which is "first run" semantics.
    pub fn load_last_event_id(sd: &crate::state_io::StateDir) -> Option<String> {
        let bytes = sd.read(crate::state_io::StateFile::LastEventId).ok()??;
        let s = String::from_utf8(bytes).ok()?;
        let trimmed = s.trim().to_string();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed)
        }
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

    // clippy::result_large_err — `SseError` is an ALIAS for the foreign
    // `eventsource_client::Error` (152 bytes), so its variants cannot be boxed
    // here, and `Box<SseError>` would only move the cost to a cold path: this is
    // a long-lived session loop whose Err is returned once per disconnect, then
    // reconnected with backoff. Surfaced 2026-08-26 when CI's stable moved to
    // Rust 1.98 and the lint began firing on pre-existing code (the 0.4.38 CI
    // run on 2026-08-08 was green on the same source), turning main red for any
    // commit pushed that day. Allowed deliberately rather than restructuring a
    // foreign error type under time pressure.
    #[allow(clippy::result_large_err)]
    async fn run_one_session(&self, last_event_id: &mut Option<String>) -> Result<(), SseError> {
        let url = format!("{}/api/sync/events", self.nexus_url);
        let mut builder = eventsource_client::ClientBuilder::for_url(&url)?
            .header("Authorization", &format!("Bearer {}", self.token))?;
        if let Some(id) = last_event_id.as_deref() {
            builder = builder.last_event_id(id.to_owned());
        }
        let client = builder.build();
        let mut stream = client.stream();
        loop {
            // TKT-fc62ea8a event-arrival liveness watchdog: a healthy stream
            // delivers a frame (event OR the server's 30s keep-alive comment)
            // well inside the window. Total silence means a half-open
            // transport (CF-tunnel edge kept ESTABLISHED across an origin
            // restart) or a dead origin generator — either way the only
            // recovery is a reconnect, which also triggers server-side
            // catch-up from the persisted last_event_id.
            #[cfg(test)]
            crate::faults::point("sse_consumer");
            let event = match tokio::time::timeout(self.liveness_window, stream.try_next()).await {
                Ok(next) => match next? {
                    Some(ev) => ev,
                    None => break,
                },
                Err(_elapsed) => {
                    warn!(
                        window_secs = self.liveness_window.as_secs(),
                        "SSE liveness watchdog: no frames (events or keep-alives) within the window — stream is half-open or the origin stopped emitting; forcing reconnect"
                    );
                    self.ts_err(
                        ConnectionStatus::Reconnecting,
                        format!(
                            "no SSE frames in {}s; forcing reconnect",
                            self.liveness_window.as_secs()
                        ),
                    );
                    return Err(SseError::TimedOut);
                }
            };
            match event {
                SSE::Connected(_) => {
                    debug!("SSE connected");
                    self.ts_set(ConnectionStatus::Connected);
                    if let Some(f) = &self.on_connected {
                        f();
                    }
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
                    // TKT-a3e3ceb1: a chunk pseudo-row is never a note. Refuse before
                    // fetch/write/delete; no per-path log (storm), debug only.
                    if crate::path_policy::is_chunk_pseudo_path(&env.path) {
                        debug!("sse: chunk pseudo-path refused (TKT-a3e3ceb1)");
                        continue;
                    }
                    if !self.event_in_scope(&env.path) {
                        debug!("defensive scope drop: {}", env.path);
                        continue;
                    }
                    if env.op == "DELETE" {
                        // D13 (S511): delete-vs-modify is resolved inside
                        // soft_delete/the materializer; the consumer just
                        // forwards the delete intent. Modification-beats-deletion
                        // and the resurrection guard live below the daemon edge.
                        if let Err(e) = self.materializer.soft_delete(&env.path) {
                            error!("materializer soft_delete failed: {e}");
                        }
                    } else {
                        // D2 (S511, TKT-2dc9a17e): thread the server change_seq
                        // (from the SSE envelope lsn) into write() so a conflict
                        // stash is named deterministically, and HONOR the rich
                        // outcome instead of blindly trusting write() to overwrite.
                        let change_seq = change_seq_from_lsn(&env.lsn);
                        match self.api.fetch_note(&env.path).await {
                            Ok(payload) => {
                                match self
                                    .materializer
                                    .write_with_change_seq(&payload, change_seq)
                                {
                                    Ok(MaterializeOutcome::Skipped(
                                        SkipReason::LocalEditPreserved,
                                    )) => {
                                        // R2: a genuine local edit. Do NOTHING here,
                                        // the file_watcher/push pipeline carries the
                                        // edit UP. NEVER overwrite it with the older
                                        // server copy (the silent-revert bug).
                                        warn!(
                                            path = %env.path,
                                            "sse: local edit preserved (R2), not overwritten; push pipeline will carry it up"
                                        );
                                    }
                                    Ok(MaterializeOutcome::Stashed { stash_path }) => {
                                        warn!(
                                            path = %env.path,
                                            stash = %stash_path.display(),
                                            "sse: CONFLICT, stashed local revision then materialized server winner"
                                        );
                                    }
                                    // TKT-ddff1877: append-aware arms. Local
                                    // superseded (or merged past) the server
                                    // head without a fork; the compensating
                                    // push carries the result up.
                                    Ok(MaterializeOutcome::Skipped(
                                        SkipReason::AppendPreservedPushUp { enqueued_push },
                                    )) => {
                                        warn!(
                                            path = %env.path,
                                            enqueued_push,
                                            "sse: APPEND ARM A - server head is a line-prefix of local; local preserved (no fork), compensating push enqueued"
                                        );
                                    }
                                    Ok(MaterializeOutcome::Merged { enqueued_push, .. }) => {
                                        warn!(
                                            path = %env.path,
                                            enqueued_push,
                                            "sse: APPEND ARM B - both sides appended after the verified base; merged locally (no fork), compensating push enqueued"
                                        );
                                    }
                                    Ok(MaterializeOutcome::Wrote { .. })
                                    | Ok(MaterializeOutcome::AlignedToCanonical { .. })
                                    | Ok(MaterializeOutcome::Skipped(_)) => {
                                        debug!(path = %env.path, "sse: materialized server note");
                                    }
                                    Ok(MaterializeOutcome::IntegrityFailed {
                                        expected_sha,
                                        actual_sha,
                                        ..
                                    }) => {
                                        error!(
                                            path = %env.path,
                                            expected = %expected_sha,
                                            actual = %actual_sha,
                                            "sse: materializer integrity check FAILED"
                                        );
                                    }
                                    Err(e) => {
                                        error!("materializer write failed: {e}");
                                    }
                                }
                            }
                            Err(e) => error!("body fetch failed for {}: {e}", env.path),
                        }
                    }
                    if let Some(id) = ev.id {
                        if !id.is_empty() {
                            // S477 v0.3.8 (A): persist to disk BEFORE updating
                            // the in-memory copy so a crash between persist and
                            // assign re-replays one event (idempotent — better
                            // than skipping one).
                            self.persist_last_event_id(&id);
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

#[cfg(test)]
#[allow(clippy::disallowed_methods)]
mod tests {
    use super::*;
    use crate::materializer::{Materializer, MaterializerConfig, MaterializerMode};
    use std::time::Duration;

    /// Minimal SSE origin that sends valid headers + `frames`, then goes
    /// SILENT while keeping the socket open — the client-visible shape of a
    /// half-open transport (CF edge ESTABLISHED across an origin restart) or
    /// a dead origin generator: no frames, no FIN, forever.
    async fn spawn_silent_sse_server(
        frames: &'static str,
    ) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 4096];
            let _ = sock.read(&mut buf).await;
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\n\r\n{frames}"
            );
            sock.write_all(resp.as_bytes()).await.unwrap();
            sock.flush().await.unwrap();
            tokio::time::sleep(Duration::from_secs(3600)).await;
        });
        (addr, handle)
    }

    // ---------- TKT-35f75318: defensive scope check on the storage key ----------

    fn v(xs: &[&str]) -> Vec<String> {
        xs.iter().map(|s| s.to_string()).collect()
    }

    /// (a) routed, in scope: wire `t4-canary.md` on route harness-memory/Nexus is
    /// storage key `harness-memory/Nexus/t4-canary.md` -> applied. Red pre-fix.
    #[test]
    fn sse_routed_in_scope_is_applied() {
        assert!(sse_event_in_scope(
            "t4-canary.md",
            Some("harness-memory/Nexus"),
            &v(&["harness-memory/"]),
            &[],
        ));
    }

    /// (b) routed, out of scope -> dropped (unrelated root and a sibling route).
    #[test]
    fn sse_routed_out_of_scope_is_dropped() {
        for roots in [&["other/"][..], &["harness-memory/Cutlist/"][..]] {
            assert!(!sse_event_in_scope(
                "t4-canary.md",
                Some("harness-memory/Nexus"),
                &v(roots),
                &[],
            ));
        }
        // Storage-form exclude still drops on a routed root.
        assert!(!sse_event_in_scope(
            "private/x.md",
            Some("harness-memory/Nexus"),
            &v(&["harness-memory/"]),
            &v(&["harness-memory/Nexus/private/"]),
        ));
    }

    /// (c) bare/empty route: unchanged (wire path == storage key).
    #[test]
    fn sse_bare_route_unchanged() {
        for route in [None, Some("")] {
            let roots = v(&["02_Projects/"]);
            assert!(sse_event_in_scope("02_Projects/x.md", route, &roots, &[]));
            assert!(!sse_event_in_scope("05_Personal/y.md", route, &roots, &[]));
            assert!(!sse_event_in_scope(
                "harness-memory/Nexus/a.md",
                route,
                &[],
                &v(&["harness-memory/"]),
            ));
        }
    }

    /// The builder maps empty route to None (bare) and keeps a named route.
    #[test]
    fn sse_with_route_maps_empty_to_bare() {
        let mat = || {
            Materializer::new(
                std::env::temp_dir(),
                None,
                MaterializerMode::Disabled,
                std::env::temp_dir(),
                "sse-test".to_string(),
                MaterializerConfig::default(),
            )
        };
        let c = SseConsumer::new(
            "http://127.0.0.1:9".into(),
            "t".into(),
            vec![],
            vec![],
            mat(),
        )
        .unwrap()
        .with_route("");
        assert_eq!(c.route, None);
        let c = SseConsumer::new(
            "http://127.0.0.1:9".into(),
            "t".into(),
            vec![],
            vec![],
            mat(),
        )
        .unwrap()
        .with_route("harness-memory/Nexus");
        assert_eq!(c.route.as_deref(), Some("harness-memory/Nexus"));
    }

    fn test_consumer(
        addr: std::net::SocketAddr,
        window: Duration,
    ) -> (tempfile::TempDir, tempfile::TempDir, SseConsumer) {
        let vaults = tempfile::TempDir::new().unwrap();
        let ws = tempfile::TempDir::new().unwrap();
        let mat = Materializer::new(
            vaults.path().to_path_buf(),
            Some("shadow/".to_string()),
            MaterializerMode::Disabled,
            ws.path().to_path_buf(),
            "sse-test".to_string(),
            MaterializerConfig {
                device_id: "sse-test".into(),
                ..Default::default()
            },
        );
        let c = SseConsumer::new(
            format!("http://{addr}"),
            "test-token".to_string(),
            vec![],
            vec![],
            mat,
        )
        .unwrap()
        .with_liveness_window(window);
        (vaults, ws, c)
    }

    /// TKT-fc62ea8a regression: a stream that goes totally silent (no events,
    /// no keep-alives, socket held open) must be abandoned within the liveness
    /// window with `SseError::TimedOut` so the run() loop reconnects. PRE-FIX
    /// BEHAVIOR: `stream.try_next()` blocked forever — this test only fails
    /// via its outer 10s timeout, which is exactly the daemon running days on
    /// a half-open stream scaled down.
    #[tokio::test]
    async fn liveness_watchdog_abandons_a_silent_half_open_stream() {
        let (addr, _srv) = spawn_silent_sse_server(": keep-alive\n\n").await;
        let (_v, _w, c) = test_consumer(addr, Duration::from_millis(300));
        let mut lei: Option<String> = None;
        let out = tokio::time::timeout(Duration::from_secs(10), c.run_one_session(&mut lei))
            .await
            .expect(
                "PRE-FIX failure mode: run_one_session hung forever on a silent half-open stream",
            );
        assert!(
            matches!(out, Err(SseError::TimedOut)),
            "expected liveness TimedOut, got {out:?}"
        );
    }

    /// The watchdog measures per-frame gaps, not total session length: a
    /// stream delivering keep-alive comments faster than the window must NOT
    /// be killed by the watchdog even after many windows have elapsed.
    #[tokio::test]
    async fn keep_alives_inside_the_window_hold_the_session_open() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let _srv = tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 4096];
            let _ = sock.read(&mut buf).await;
            sock.write_all(
                b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\n\r\n",
            )
            .await
            .unwrap();
            // 8 keep-alives at 100ms — total 800ms, well past the 300ms
            // window, but every GAP is inside it.
            for _ in 0..8 {
                tokio::time::sleep(Duration::from_millis(100)).await;
                if sock.write_all(b": keep-alive\n\n").await.is_err() {
                    return;
                }
            }
            // Then clean EOF: the session must end via stream end, not TimedOut.
        });
        let (_v, _w, c) = test_consumer(addr, Duration::from_millis(300));
        let mut lei: Option<String> = None;
        let out = tokio::time::timeout(Duration::from_secs(10), c.run_one_session(&mut lei))
            .await
            .expect("session should complete promptly");
        assert!(
            !matches!(out, Err(SseError::TimedOut)),
            "keep-alives inside the window must not trip the watchdog, got {out:?}"
        );
    }

    /// CS-2 (W12): last_event_id persists through the held state dir with an
    /// atomic replace (0600, no fixed-name temp left behind) and loads back.
    #[tokio::test]
    async fn last_event_id_round_trips_through_state_dir() {
        let (addr, _srv) = spawn_silent_sse_server("").await;
        let (_v, _w, c) = test_consumer(addr, Duration::from_millis(300));
        let d = tempfile::TempDir::new().unwrap();
        let sd = crate::state_io::StateDir::for_test(d.path());
        let c = c.with_state_dir(&sd);
        assert_eq!(SseConsumer::load_last_event_id(&sd), None);
        c.persist_last_event_id("42-7");
        c.persist_last_event_id("43-1");
        assert_eq!(
            SseConsumer::load_last_event_id(&sd).as_deref(),
            Some("43-1")
        );
        let names: Vec<String> = std::fs::read_dir(d.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(names, vec!["last_event_id".to_string()]);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let m = std::fs::metadata(d.path().join("last_event_id")).unwrap();
            assert_eq!(m.permissions().mode() & 0o777, 0o600);
        }
    }
}
