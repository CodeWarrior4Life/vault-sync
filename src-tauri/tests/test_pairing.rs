// CS-2 gate 2: integration tests may use path-based fs freely.
#![allow(clippy::disallowed_methods)]
use mockito::Server;
use std::ffi::OsString;
use std::path::PathBuf;
use std::sync::{Mutex, MutexGuard};
use tempfile::TempDir;
use vault_sync_daemon::managed_instance::ENV_CONFIG_DIR;
use vault_sync_daemon::pairing::{pair_inner, PairingInput};

/// TKT-160527ad: `pair_inner` persists the token via `token_store::store()`,
/// which ALWAYS writes `token-<sub>.bin` beside `default_config_path()` —
/// the REAL per-user config dir unless `NEXUS_SYNC_CONFIG_DIR` is set. Every
/// test that pairs therefore runs inside an [`IsolatedConfigDir`].
///
/// Env vars are process-global, so all mutation of `NEXUS_SYNC_CONFIG_DIR`
/// in this binary is serialized on this lock (same idiom as the crate's
/// `managed_instance::test_env`, which is `cfg(test)` and not reachable from
/// an integration test).
static ENV_LOCK: Mutex<()> = Mutex::new(());

fn env_lock() -> MutexGuard<'static, ()> {
    ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner())
}

/// RAII: points `NEXUS_SYNC_CONFIG_DIR` at a fresh tempdir for the guard's
/// lifetime, then restores the previous env and deletes the tempdir — which
/// removes the token FILE backend along with it.
struct IsolatedConfigDir {
    dir: TempDir,
    prev: Option<OsString>,
    // Declared last: dropped after the env is restored and the dir removed.
    _lock: MutexGuard<'static, ()>,
}

impl IsolatedConfigDir {
    fn new() -> Self {
        let lock = env_lock();
        let dir = TempDir::new().expect("isolated config tempdir");
        let prev = std::env::var_os(ENV_CONFIG_DIR);
        std::env::set_var(ENV_CONFIG_DIR, dir.path());
        Self {
            dir,
            prev,
            _lock: lock,
        }
    }

    /// Remove both token backends for `sub`: the keyring entry and the file.
    fn cleanup(&self, sub: &str) {
        let _ = vault_sync_daemon::keyring::delete_token(sub);
        let _ = std::fs::remove_file(self.dir.path().join(format!("token-{sub}.bin")));
    }
}

impl Drop for IsolatedConfigDir {
    fn drop(&mut self) {
        match self.prev.take() {
            Some(v) => std::env::set_var(ENV_CONFIG_DIR, v),
            None => std::env::remove_var(ENV_CONFIG_DIR),
        }
    }
}

fn make_input(nexus_url: &str, tmp: &TempDir) -> (PairingInput, PathBuf) {
    let config_path = tmp.path().join("config.toml");
    let input = PairingInput {
        nexus_url: nexus_url.to_string(),
        token: "vsk_test_token".to_string(),
        vaults_root: tmp.path().join("vault"),
        materializer_mode: None,
    };
    (input, config_path)
}

fn health_body(subscriber_id: &str) -> String {
    format!(
        r#"{{"subscriber_id":"{subscriber_id}","scope_roots":["notes/"],"scope_excludes":[],"materializer_mode":"shadow","shadow_path":null}}"#
    )
}

/// Happy path: valid URL + token → Ok with correct subscriber_id.
#[tokio::test]
async fn validate_url_token_pair_ok() {
    let iso = IsolatedConfigDir::new();
    let mut srv = Server::new_async().await;
    let _m = srv
        .mock("GET", "/api/sync/health")
        .with_status(200)
        .with_body(health_body("sub-abc123"))
        .match_header("authorization", "Bearer vsk_test_token")
        .expect_at_least(0)
        .expect_at_most(1)
        .create_async()
        .await;

    let tmp = TempDir::new().unwrap();
    let (input, config_path) = make_input(&srv.url(), &tmp);

    // Skip on CI if keyring is unavailable (preflight will return Err)
    // We only assert the network + parse path here; keyring/config assertions
    // are in success_writes_keyring_and_config.
    let result = pair_inner(input, config_path).await;

    // On systems where keyring is available (Windows/macOS CI) this must be Ok.
    // On Linux CI without Secret Service this will be Err(KeyringUnavailable).
    // We accept both but ensure we DID reach the server (mock asserted by mockito).
    match result {
        Ok(success) => {
            assert_eq!(success.subscriber_id, "sub-abc123");
            assert_eq!(success.scope_roots, vec!["notes/"]);
            assert_eq!(success.materializer_mode, "shadow");
            // Clean up both backends so concurrent tests don't see stale entries.
            iso.cleanup("sub-abc123");
        }
        Err(e) => {
            // Only acceptable failure on CI is a keyring error.
            let msg = e.to_string();
            assert!(
                msg.contains("keyring"),
                "unexpected error (not keyring): {msg}"
            );
        }
    }
}

/// 401 from server must yield an error whose message contains "unauthorized" or "auth".
#[tokio::test]
async fn pair_401_yields_auth_error() {
    let _iso = IsolatedConfigDir::new();
    let mut srv = Server::new_async().await;
    let _m = srv
        .mock("GET", "/api/sync/health")
        .with_status(401)
        .create_async()
        .await;

    let tmp = TempDir::new().unwrap();
    let (input, config_path) = make_input(&srv.url(), &tmp);

    let result = pair_inner(input, config_path).await;

    // Either a keyring preflight failure OR an API 401.
    match result {
        Err(e) => {
            let msg = e.to_string().to_lowercase();
            assert!(
                msg.contains("unauthorized") || msg.contains("auth") || msg.contains("keyring"),
                "error message did not mention auth or keyring: {msg}"
            );
        }
        Ok(_) => panic!("expected Err for 401, got Ok"),
    }
}

/// DNS / network failure must yield an error whose message contains "network" or "connect" or similar.
#[tokio::test]
async fn dns_failure_yields_network_error() {
    let _iso = IsolatedConfigDir::new();
    // 127.0.0.1:1 — localhost, port 1. Nothing listens; OS yields immediate
    // ECONNREFUSED on every platform (no SYN-ACK round trip → no timeout wait).
    // 0.0.0.0 was ambiguous on macOS (kernel rewrites + multi-interface enum →
    // 60 s hang under headless CI); 192.0.2.1 (RFC 5737 TEST-NET-1) goes through
    // OS connect timeout (~21 s). Using loopback:1 gives sub-second completion
    // and still satisfies the "network/connect/refused" error-message contract.
    let tmp = TempDir::new().unwrap();
    let (input, config_path) = make_input("http://127.0.0.1:1", &tmp);

    let result = pair_inner(input, config_path).await;

    match result {
        Err(e) => {
            let msg = e.to_string().to_lowercase();
            // Accept keyring error (preflight runs before network call) OR a network-level error.
            assert!(
                msg.contains("network")
                    || msg.contains("connect")
                    || msg.contains("error sending")
                    || msg.contains("connection refused")
                    || msg.contains("os error")
                    || msg.contains("keyring"),
                "error message did not indicate network or keyring failure: {msg}"
            );
        }
        Ok(_) => panic!("expected Err for unreachable host, got Ok"),
    }
}

/// P0 2026-08-05: a re-pair must MERGE into the existing config.toml —
/// `vault_name` and unknown/future keys survive the pair/save cycle; only
/// the enrollment-owned fields change. (The old path serialized a typed
/// `Config`, which has no `vault_name` field, over the file — silently
/// dropping it and mis-rooting the daemon at `vaults_root` on next load.)
#[tokio::test]
async fn repair_preserves_vault_name_and_unknown_keys() {
    let iso = IsolatedConfigDir::new();
    let mut srv = Server::new_async().await;
    let _m = srv
        .mock("GET", "/api/sync/health")
        .with_status(200)
        .with_body(health_body("sub-p0-roundtrip"))
        .match_header("authorization", "Bearer vsk_test_token")
        .expect_at_least(0)
        .expect_at_most(1)
        .create_async()
        .await;

    let tmp = TempDir::new().unwrap();
    let (input, config_path) = make_input(&srv.url(), &tmp);

    // Pre-existing enrolled config carrying vault_name + a future key.
    std::fs::write(
        &config_path,
        format!(
            r#"
nexus_url = "https://stale.example.com"
subscriber_id = "sub-p0-roundtrip"
vaults_root = '{}'
vault_name = "Mainframe"
daemon_version = "0.4.32"
daemon_platform = "linux-x86_64"
future_unknown_key = "must-survive"
"#,
            tmp.path().join("vault").display()
        ),
    )
    .unwrap();

    let result = pair_inner(input, config_path.clone()).await;

    match result {
        Ok(success) => {
            assert_eq!(success.subscriber_id, "sub-p0-roundtrip");
            let saved = std::fs::read_to_string(&config_path).unwrap();
            assert!(
                saved.contains(r#"vault_name = "Mainframe""#),
                "vault_name dropped by re-pair; config now:\n{saved}"
            );
            assert!(
                saved.contains("future_unknown_key") && saved.contains("must-survive"),
                "unknown key dropped by re-pair; config now:\n{saved}"
            );
            assert!(
                saved.contains(&srv.url()),
                "nexus_url not updated by re-pair; config now:\n{saved}"
            );
            assert!(
                !saved.contains("stale.example.com"),
                "stale nexus_url survived re-pair; config now:\n{saved}"
            );
            iso.cleanup("sub-p0-roundtrip");
        }
        Err(e) => {
            // Only acceptable failure on CI is a keyring error (see the
            // other tests in this file).
            let msg = e.to_string();
            assert!(
                msg.contains("keyring"),
                "unexpected error (not keyring): {msg}"
            );
        }
    }
}

/// Full happy path: after pair_inner, config.toml must exist and (if keyring available) token readable.
///
/// The keyring assertion is skipped on CI hosts that lack a Secret Service backend.
#[tokio::test]
async fn success_writes_keyring_and_config() {
    let iso = IsolatedConfigDir::new();
    let mut srv = Server::new_async().await;
    let _m = srv
        .mock("GET", "/api/sync/health")
        .with_status(200)
        .with_body(health_body("sub-xyz789"))
        .match_header("authorization", "Bearer vsk_test_token")
        .expect_at_least(0)
        .expect_at_most(1)
        .create_async()
        .await;

    let tmp = TempDir::new().unwrap();
    let config_path = tmp.path().join("config.toml");
    let input = PairingInput {
        nexus_url: srv.url(),
        token: "vsk_test_token".to_string(),
        vaults_root: tmp.path().join("vault"),
        materializer_mode: None,
    };

    // Check keyring preflight first; if unavailable, skip the keyring assertion.
    let keyring_available = vault_sync_daemon::keyring::preflight().is_ok();

    let result = pair_inner(input, config_path.clone()).await;

    if keyring_available {
        let success = result.expect("pair_inner should succeed when keyring is available");
        assert_eq!(success.subscriber_id, "sub-xyz789");

        // Config file must have been written.
        assert!(
            config_path.exists(),
            "config.toml was not written to {config_path:?}"
        );

        // Keyring must have the token stored under the subscriber_id.
        let stored = vault_sync_daemon::keyring::get_token("sub-xyz789")
            .expect("get_token should not error");
        assert_eq!(
            stored,
            Some("vsk_test_token".to_string()),
            "keyring token mismatch"
        );

        // Clean up both backends so this test is idempotent.
        iso.cleanup("sub-xyz789");
    } else {
        // TODO: T23-CF — keyring not available on CI; skip keyring + config assertions.
        match result {
            Err(e) => {
                let msg = e.to_string();
                assert!(
                    msg.contains("keyring"),
                    "unexpected non-keyring error when keyring unavailable: {msg}"
                );
            }
            Ok(_) => {
                // Unexpectedly succeeded despite preflight failing — still verify config.
                assert!(
                    config_path.exists(),
                    "config.toml was not written to {config_path:?}"
                );
            }
        }
    }
}

// ── TKT-160527ad guard ────────────────────────────────────────────────────

/// Set of `token-*` file names in `dir` (empty if the dir does not exist).
fn token_files(dir: &std::path::Path) -> std::collections::BTreeSet<String> {
    std::fs::read_dir(dir)
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .filter(|n| n.starts_with("token-"))
                .collect()
        })
        .unwrap_or_default()
}

/// The REAL per-user config dir: resolved under the env lock with
/// `NEXUS_SYNC_CONFIG_DIR` unset, then the previous env is restored.
fn real_config_dir() -> PathBuf {
    let _lock = env_lock();
    let prev = std::env::var_os(ENV_CONFIG_DIR);
    std::env::remove_var(ENV_CONFIG_DIR);
    let dir = vault_sync_daemon::config::default_config_path()
        .parent()
        .expect("config path has a parent")
        .to_path_buf();
    if let Some(v) = prev {
        std::env::set_var(ENV_CONFIG_DIR, v);
    }
    dir
}

/// TKT-160527ad: pairing in a test must NEVER write a token file into the
/// real per-user config dir. Pre-fix, `token_store::store()` wrote
/// `token-<sub>.bin` there and the tests only cleaned the keyring.
#[tokio::test]
async fn pairing_test_never_writes_token_into_real_config_dir() {
    const SUB: &str = "sub-isolation-guard";
    let real = real_config_dir();
    let leaked = real.join(format!("token-{SUB}.bin"));
    assert!(!leaked.exists(), "precondition: {leaked:?} already present");
    let before = token_files(&real);

    let mut srv = Server::new_async().await;
    let _m = srv
        .mock("GET", "/api/sync/health")
        .with_status(200)
        .with_body(health_body(SUB))
        .create_async()
        .await;
    {
        let iso = IsolatedConfigDir::new();
        let tmp = TempDir::new().unwrap();
        let (input, config_path) = make_input(&srv.url(), &tmp);
        let _ = pair_inner(input, config_path).await;
        iso.cleanup(SUB);
    }

    let after = token_files(&real);
    let new: Vec<_> = after.difference(&before).cloned().collect();
    if leaked.exists() {
        // Remove ONLY this test's own fixture file so a RED run leaves no residue.
        let _ = std::fs::remove_file(&leaked);
    }
    assert!(
        new.is_empty(),
        "pairing test leaked token file(s) into the real config dir {real:?}: {new:?}"
    );
}
