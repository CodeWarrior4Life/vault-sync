//! TKT-9d927317 (icarus enrollment) R3 guard tests: config -> sync-root
//! synthesis, the "vault_name footgun".
//!
//! Enrollment-critical invariant. The live link + trinity mass-push incidents
//! (TKT-86ae42a3, TKT-8a70148c) traced back to a config whose sync target
//! resolved to the BARE PARENT `vaults_root` instead of `vaults_root/Mainframe`.
//! When the daemon watches the bare parent it treats the whole parent tree as
//! the vault and can push a divergent view of every file up to PG.
//!
//! `from_toml_back_compat` (config.rs:116-148) is the single resolution point.
//! These tests lock its three branches so an icarus config that would sync the
//! wrong root can never ship silently, and so a future refactor cannot quietly
//! change which directory becomes the sync root.

use vault_sync_daemon::config::Config;

const BASE: &str = r#"
nexus_url = "https://nexus.obsidian-inc.com"
subscriber_id = "icarus-sid-0000"
vaults_root = "/var/home/cyril/vaults"
daemon_version = "0.4.32"
daemon_platform = "linux-x86_64"
"#;

/// CORRECT icarus shape (mirrors link's known-good config): empty/absent
/// `sync_roots` + `vault_name = "Mainframe"` -> the sync root is the vault
/// SUBDIR, not the parent.
#[test]
fn vault_name_present_targets_vault_subdir() {
    let toml = format!("{BASE}\nvault_name = \"Mainframe\"\n");
    let cfg = Config::from_toml_back_compat(&toml).expect("parse");
    assert_eq!(cfg.sync_roots.len(), 1, "exactly one synthesized root");
    assert_eq!(
        cfg.sync_roots[0].path,
        std::path::PathBuf::from("/var/home/cyril/vaults/Mainframe"),
        "vault_name must extend vaults_root into the Mainframe subdir"
    );
    // The synthesized root must inherit the top-level subscriber (B2b), else it
    // pushes under an empty subscriber.
    assert_eq!(cfg.sync_roots[0].subscriber_id, "icarus-sid-0000");
}

/// THE FOOTGUN, locked as a regression guard: absent `sync_roots` AND absent
/// `vault_name` -> the sync root collapses to the BARE PARENT `vaults_root`.
/// This is the mass-push shape. This test does not endorse the behavior; it
/// pins it so the icarus runbook's "vault_name is mandatory" step is provably
/// load-bearing and any change to this branch is caught.
#[test]
fn no_vault_name_collapses_to_bare_parent_footgun() {
    let cfg = Config::from_toml_back_compat(BASE).expect("parse");
    assert_eq!(cfg.sync_roots.len(), 1);
    assert_eq!(
        cfg.sync_roots[0].path,
        std::path::PathBuf::from("/var/home/cyril/vaults"),
        "without vault_name the root collapses to the bare parent (mass-push risk)"
    );
}

/// Explicit `[[sync_roots]]` blocks win outright; `vault_name` is ignored when
/// they are present. Guards the "use as-is" branch (config.rs:119-122).
#[test]
fn explicit_sync_roots_take_precedence() {
    let toml = format!(
        "{BASE}\nvault_name = \"Ignored\"\n\n[[sync_roots]]\npath = \"/var/home/cyril/vaults/Mainframe\"\nroute = \"\"\nsubscriber_id = \"icarus-sid-0000\"\n"
    );
    let cfg = Config::from_toml_back_compat(&toml).expect("parse");
    assert_eq!(cfg.sync_roots.len(), 1);
    assert_eq!(
        cfg.sync_roots[0].path,
        std::path::PathBuf::from("/var/home/cyril/vaults/Mainframe")
    );
}
