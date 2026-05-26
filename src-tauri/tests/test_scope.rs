use vault_sync_daemon::scope::{is_safe_path, path_in_scope};

#[test]
fn includes_when_no_roots_no_excludes() {
    assert!(path_in_scope("foo/bar.md", &[], &[]));
}

#[test]
fn includes_when_under_root() {
    assert!(path_in_scope(
        "02_Projects/Nexus/foo.md",
        &["02_Projects/Nexus/".into()],
        &[]
    ));
}

#[test]
fn excludes_out_of_root() {
    assert!(!path_in_scope(
        "02_Projects/Lattice/foo.md",
        &["02_Projects/Nexus/".into()],
        &[]
    ));
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
    assert!(path_in_scope(
        "02_Projects/Nexus/",
        &["02_Projects/Nexus/".into()],
        &[]
    ));
}

// ---------------------------------------------------------------------------
// is_safe_path — CF-S470-T19: direct unit coverage
// ---------------------------------------------------------------------------

#[test]
fn safe_path_accepts_plain_relative() {
    assert!(is_safe_path("Notes/hello.md"));
    assert!(is_safe_path("02_Projects/Nexus/foo.md"));
    assert!(is_safe_path("a.md"));
}

#[test]
fn safe_path_rejects_parent_traversal() {
    assert!(!is_safe_path("../etc/passwd"));
    assert!(!is_safe_path("../../secrets"));
}

#[test]
fn safe_path_rejects_embedded_parent_segments() {
    assert!(!is_safe_path("Notes/../../etc"));
    assert!(!is_safe_path("a/../b"));
}

#[test]
fn safe_path_rejects_unix_absolute() {
    assert!(!is_safe_path("/etc/passwd"));
    assert!(!is_safe_path("/"));
}

#[test]
fn safe_path_rejects_windows_backslash_root() {
    assert!(!is_safe_path("\\Windows\\System32"));
    assert!(!is_safe_path("\\"));
}

#[test]
fn safe_path_rejects_windows_drive_prefix() {
    assert!(!is_safe_path("C:\\Users\\admin"));
    assert!(!is_safe_path("D:/Vaults/secrets"));
    assert!(!is_safe_path("Z:\\anywhere"));
}

#[test]
fn safe_path_accepts_dot_files_and_dot_in_name() {
    // Single dots and dotfiles are fine — only `..` is dangerous.
    assert!(is_safe_path(".gitignore"));
    assert!(is_safe_path("Notes/.hidden"));
    assert!(is_safe_path("file.with.dots.md"));
}

#[test]
fn safe_path_accepts_empty() {
    // Empty string trivially passes all four checks (no chars to match traversal/abs/drive).
    // Documents current behaviour — caller is expected to reject empties earlier in the pipeline.
    assert!(is_safe_path(""));
}
