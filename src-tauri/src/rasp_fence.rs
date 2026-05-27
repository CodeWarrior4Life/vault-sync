//! Runtime-Agnostic Substrate Principle (RASP) refuse-to-write fence.
//!
//! Per [[D:/Vaults/Mainframe/02_Projects/Protocols/Runtime-Agnostic Substrate Principle.md]]
//! — Substrate Layer Inviolability corollary: enrichment workers, indexers,
//! embedders, and any content-mutating process MAY NEVER mutate substrate-
//! layer content. The sync daemon is one such content-mutating process.
//!
//! Today the canonical substrate path list is hardcoded here (the same list
//! the principle doc enumerates). Future iteration: read each vault's
//! `substrate_layer_paths` from its `00_VAULT.md` rulebook so the substrate
//! definition lives in the substrate itself (the deeper RASP form).
//!
//! Even in shadow-write mode the fence is ON for defense-in-depth — if Phase
//! F flips the daemon to live-write, the same guard prevents the daemon from
//! ever clobbering a substrate file.

/// Substrate path patterns, expressed as a closed enumeration so the
/// matcher is exact-prefix or exact-equals — no regex, no glob library.
/// Patterns ending in `/` are prefix-matched; equal-exact-match for
/// everything else. Order matters only insofar as the first hit wins.
const SUBSTRATE_PATH_RULES: &[SubstrateRule] = &[
    SubstrateRule::ExactSuffix("00_VAULT.md"),
    SubstrateRule::ExactSuffix("Family.md"),
    SubstrateRule::ExactSuffix("Mission.md"),
    SubstrateRule::PathPrefix("02_Projects/Protocols/"),
    SubstrateRule::PathPrefix("_project/"),
    SubstrateRule::PathPrefix("_rapport/people/"),
];

#[derive(Debug, Clone, Copy)]
enum SubstrateRule {
    /// Path equals or ends with `/<literal>` (matches both root-level
    /// `00_VAULT.md` AND nested `02_Projects/Foo/00_VAULT.md`).
    ExactSuffix(&'static str),
    /// Path starts with the literal (prefix match, includes trailing `/`).
    PathPrefix(&'static str),
}

/// Return true iff the given path (vault-relative, forward-slashed) is a
/// RASP-protected substrate path that the daemon MUST NOT materialize.
pub fn is_substrate_path(path: &str) -> bool {
    for rule in SUBSTRATE_PATH_RULES {
        match rule {
            SubstrateRule::ExactSuffix(s) => {
                if path == *s {
                    return true;
                }
                if path.ends_with(&format!("/{s}")) {
                    return true;
                }
            }
            SubstrateRule::PathPrefix(p) => {
                if path.starts_with(p) {
                    return true;
                }
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn root_00_vault_is_substrate() {
        assert!(is_substrate_path("00_VAULT.md"));
    }

    #[test]
    fn nested_00_vault_is_substrate() {
        assert!(is_substrate_path("02_Projects/Nexus/00_VAULT.md"));
        assert!(is_substrate_path("01_Inbox/00_VAULT.md"));
    }

    #[test]
    fn family_md_is_substrate() {
        assert!(is_substrate_path("Family.md"));
        assert!(is_substrate_path("02_Projects/Grosse/Family.md"));
    }

    #[test]
    fn mission_md_is_substrate() {
        assert!(is_substrate_path("Mission.md"));
        assert!(is_substrate_path("02_Projects/Nexus/Mission.md"));
    }

    #[test]
    fn protocols_dir_is_substrate() {
        assert!(is_substrate_path("02_Projects/Protocols/foo.md"));
        assert!(is_substrate_path("02_Projects/Protocols/sub/bar.md"));
    }

    #[test]
    fn project_dir_is_substrate() {
        assert!(is_substrate_path("_project/anything.md"));
    }

    #[test]
    fn rapport_people_dir_is_substrate() {
        assert!(is_substrate_path("_rapport/people/cyril.md"));
        assert!(is_substrate_path("_rapport/people/sub/x.md"));
    }

    #[test]
    fn rapport_non_people_subdirs_not_substrate() {
        // Only people/ is fenced under _rapport — other rapport dirs are content.
        assert!(!is_substrate_path("_rapport/cards/foo.md"));
        assert!(!is_substrate_path("_rapport/conversations/x.md"));
    }

    #[test]
    fn ordinary_content_not_substrate() {
        assert!(!is_substrate_path("02_Projects/Nexus/Specifications/foo.md"));
        assert!(!is_substrate_path("01_Inbox/quick-note.md"));
        assert!(!is_substrate_path("03_Areas/Health/journal.md"));
        assert!(!is_substrate_path("Daily/2026-05-27.md"));
    }

    #[test]
    fn name_collisions_not_caught() {
        // 'Mission Statement.md' is NOT 'Mission.md'.
        assert!(!is_substrate_path("Mission Statement.md"));
        // 'Family History.md' is NOT 'Family.md'.
        assert!(!is_substrate_path("Family History.md"));
        // 'My 00_VAULT.md notes.md' is NOT 00_VAULT.md.
        assert!(!is_substrate_path("My 00_VAULT.md notes.md"));
    }
}
