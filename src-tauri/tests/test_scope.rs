use vault_sync_daemon::scope::path_in_scope;

#[test]
fn includes_when_no_roots_no_excludes() {
    assert!(path_in_scope("foo/bar.md", &[], &[]));
}

#[test]
fn includes_when_under_root() {
    assert!(path_in_scope("02_Projects/Nexus/foo.md", &["02_Projects/Nexus/".into()], &[]));
}

#[test]
fn excludes_out_of_root() {
    assert!(!path_in_scope("02_Projects/Lattice/foo.md", &["02_Projects/Nexus/".into()], &[]));
}

#[test]
fn excludes_when_in_exclude_list() {
    assert!(!path_in_scope(
        "02_Projects/Nexus/_resources/img.png",
        &["02_Projects/Nexus/".into()],
        &["02_Projects/Nexus/_resources/".into()],
    ));
}

#[test]
fn includes_root_itself_when_listed_with_slash() {
    assert!(path_in_scope("02_Projects/Nexus/", &["02_Projects/Nexus/".into()], &[]));
}
