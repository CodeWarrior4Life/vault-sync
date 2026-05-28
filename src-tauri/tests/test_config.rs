use std::path::PathBuf;
use tempfile::TempDir;
use vault_sync_daemon::config::{Config, ConfigError};

#[test]
fn save_then_load_round_trips() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("config.toml");
    let cfg = Config {
        nexus_url: "https://nexus.obsidian-inc.com".to_string(),
        subscriber_id: "test-sid".to_string(),
        vaults_root: PathBuf::from("/home/user/vault"),
        daemon_version: env!("CARGO_PKG_VERSION").to_string(),
        daemon_platform: "linux-x86_64".to_string(),
        last_event_id: None,
    };
    cfg.save_to(&path).unwrap();
    let loaded = Config::load_from(&path).unwrap();
    assert_eq!(loaded, cfg);
}

#[test]
fn load_missing_returns_error() {
    let dir = TempDir::new().unwrap();
    let result = Config::load_from(&dir.path().join("missing.toml"));
    assert!(matches!(result, Err(ConfigError::NotFound)));
}

#[test]
fn path_handles_unicode_filenames() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("cfg.toml");
    let cfg = Config {
        nexus_url: "https://nexus.obsidian-inc.com".to_string(),
        subscriber_id: "sid-😀".to_string(),
        vaults_root: PathBuf::from("/home/usér/váult"),
        daemon_version: "0.1.0".to_string(),
        daemon_platform: "linux-x86_64".to_string(),
        last_event_id: None,
    };
    cfg.save_to(&path).unwrap();
    let loaded = Config::load_from(&path).unwrap();
    assert_eq!(loaded.subscriber_id, "sid-😀");
}
