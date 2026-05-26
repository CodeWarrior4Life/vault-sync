use vault_sync_daemon::keyring::{delete_token, get_token, preflight, set_token};

#[test]
fn set_then_get_round_trips() {
    let sid = "test-subscriber-1";
    set_token(sid, "vsk_test_abc").unwrap();
    assert_eq!(get_token(sid).unwrap(), Some("vsk_test_abc".to_string()));
    delete_token(sid).unwrap();
}

#[test]
fn get_missing_returns_none() {
    assert_eq!(get_token("nonexistent-sid").unwrap(), None);
}

#[test]
fn preflight_returns_ok_or_specific_error() {
    match preflight() {
        Ok(()) => {}
        #[allow(unused_variables)]
        Err(e) => {
            #[cfg(target_os = "linux")]
            assert!(
                e.to_string().to_lowercase().contains("secret"),
                "Linux preflight error must mention libsecret/Secret Service: {}",
                e
            );
        }
    }
}
