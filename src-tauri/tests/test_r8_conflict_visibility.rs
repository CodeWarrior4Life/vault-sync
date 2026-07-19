//! TKT-9d927317 (icarus enrollment) R4/R8 tests: where conflict material lands.
//!
//! R4 (operator S66 binding): "any conflict/temp/quarantine material the daemon
//! writes on icarus must live under dot-prefixed dirs invisible to Obsidian."
//!
//! Current v0.4.32 behavior (conflict_stash.rs:264-278) writes the losing
//! revision as a SIBLING `.md` file next to the original:
//!   <vault_root>/<dir>/<stem>.conflict-from-<device>-<lsn>.md
//! That file is a normal `.md` in a normal directory, so Obsidian indexes and
//! renders it. The THESEUS adversarial review (2026-07-19) independently
//! flagged exactly these `CLAUDE.conflict-from-*` artifacts as active pollution.
//!
//! `characterize_*` pins the current (R4-NONCOMPLIANT) behavior so the gap is
//! executable, not just prose. `r4_*` is the DESIRED invariant; it FAILS on the
//! current tree and is `#[ignore]`d so CI stays green until the owner decides
//! whether to redirect the stash under a dot-prefixed dir (that change is
//! fleet-wide and owner-gated; it is NOT made in this burn).

use std::path::{Component, PathBuf};
use vault_sync_daemon::conflict_stash::{ConflictPolicy, ConflictStash};

fn stash_path_for(original: &str) -> (PathBuf, PathBuf) {
    let vault_root = PathBuf::from("/var/home/cyril/vaults/Mainframe");
    let stash = ConflictStash::new(vault_root.clone(), ConflictPolicy::Manual);
    let p = stash.compute_stash_path_public(original, "icarus", 42);
    let rel = p
        .strip_prefix(&vault_root)
        .expect("stash under vault_root")
        .to_path_buf();
    (p, rel)
}

/// Characterization: the stash is a visible sibling `.md` in the note's own
/// directory. NO path component is dot-prefixed. (Passes on current code.)
#[test]
fn characterize_conflict_stash_is_visible_sibling() {
    let (full, rel) = stash_path_for("02_Projects/Foo/Note.md");
    assert_eq!(
        full,
        PathBuf::from(
            "/var/home/cyril/vaults/Mainframe/02_Projects/Foo/Note.conflict-from-icarus-42.md"
        )
    );
    let has_dot_component = rel.components().any(|c| match c {
        Component::Normal(s) => s.to_string_lossy().starts_with('.'),
        _ => false,
    });
    assert!(
        !has_dot_component,
        "current behavior: conflict copy is VISIBLE to Obsidian (no dot-dir); rel={rel:?}"
    );
    assert!(rel
        .file_name()
        .unwrap()
        .to_string_lossy()
        .ends_with(".conflict-from-icarus-42.md"));
}

/// R4 desired-state spec. Some component of the stash path (relative to the
/// vault root) must be dot-prefixed so Obsidian never indexes conflict copies.
/// FAILS on the current tree by design (executable record of the gap).
#[test]
#[ignore = "R4 gap: v0.4.32 writes visible sibling conflict copies; fix is fleet-wide + owner-gated (see BURN_REPORT)"]
fn r4_conflict_material_must_live_under_dot_prefixed_dir() {
    let (_full, rel) = stash_path_for("02_Projects/Foo/Note.md");
    let has_dot_component = rel.components().any(|c| match c {
        Component::Normal(s) => s.to_string_lossy().starts_with('.'),
        _ => false,
    });
    assert!(
        has_dot_component,
        "R4: conflict material must live under a dot-prefixed dir invisible to Obsidian; got {rel:?}"
    );
}
