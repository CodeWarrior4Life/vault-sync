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
///
/// Rule kinds:
/// - `ExactSuffix(s)` — matches `path == s` or `path.ends_with("/" + s)`. For
///   the well-known pointer files (00_VAULT.md, CLAUDE.md, GEMINI.md,
///   AGENTS.md) the basename comparison is case-insensitive; for everything
///   else case-sensitive.
/// - `PathPrefix(p)` — prefix match (literal, case-sensitive).
/// - `ScopedSuffix(prefix, filename)` — fences `<prefix>**/<filename>` only.
///   Matches IFF the path begins with `prefix` AND the basename equals
///   `filename` (case-insensitive for the basename, since Family.md /
///   Mission.md are pointer-class). The path may or may not have more
///   intermediate segments between `prefix` and the file.
///
/// Order matters only insofar as the first hit wins (we short-circuit).
const SUBSTRATE_PATH_RULES: &[SubstrateRule] = &[
    // Vault-wide pointer files (any depth, case-insensitive basename)
    SubstrateRule::ExactSuffix("00_VAULT.md"),
    SubstrateRule::ExactSuffix("CLAUDE.md"),
    SubstrateRule::ExactSuffix("GEMINI.md"),
    SubstrateRule::ExactSuffix("AGENTS.md"),
    // Scoped pointer files — fence ONLY under 02_Projects/**
    SubstrateRule::ScopedSuffix("02_Projects/", "Family.md"),
    SubstrateRule::ScopedSuffix("02_Projects/", "Mission.md"),
    // Substrate prefixes
    SubstrateRule::PathPrefix("02_Projects/Protocols/"),
    SubstrateRule::PathPrefix("_project/"),
    SubstrateRule::PathPrefix("_rapport/people/"),
    SubstrateRule::PathPrefix("_rapport/groups/"),
    SubstrateRule::PathPrefix("_rapport/triage/"),
];

#[derive(Debug, Clone, Copy)]
enum SubstrateRule {
    /// Path equals or ends with `/<literal>` (matches both root-level
    /// and nested occurrences). Basename comparison is case-insensitive
    /// because all current ExactSuffix rules are pointer-class files
    /// (00_VAULT.md, CLAUDE.md, GEMINI.md, AGENTS.md) where lowercase
    /// variants like `claude.md` must also be caught.
    ExactSuffix(&'static str),
    /// Path starts with the literal (prefix match, includes trailing `/`).
    /// Case-sensitive.
    PathPrefix(&'static str),
    /// Path begins with `prefix` AND basename equals `filename`
    /// (case-insensitive for filename). Use for fencing pointer-class
    /// files only under a specific scope, e.g. `Family.md` under
    /// `02_Projects/**` but NOT at vault root.
    ScopedSuffix(&'static str, &'static str),
}

impl SubstrateRule {
    /// Stable string identifier for logging / tray counters.
    fn label(&self) -> &'static str {
        match self {
            SubstrateRule::ExactSuffix(s) => s,
            SubstrateRule::PathPrefix(p) => p,
            // ScopedSuffix uses the filename as label (the discriminating
            // half); prefix is implied context.
            SubstrateRule::ScopedSuffix(_, f) => f,
        }
    }
}

/// Outcome of classifying a path against the RASP fence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathClassification {
    /// Path is regular content — daemon may materialize.
    Content,
    /// Path is RASP-protected substrate. `rule` is the static label of the
    /// matching rule (suitable for structured log fields + tray counters).
    Substrate { rule: &'static str },
}

impl PathClassification {
    pub fn is_substrate(&self) -> bool {
        matches!(self, PathClassification::Substrate { .. })
    }
}

/// Normalize a path for matching: convert Windows backslashes to forward
/// slashes. We do NOT lowercase the whole path here — only individual rule
/// comparisons that need case-insensitivity (basename of pointer files) do
/// that locally, because prefix matches like `_project/` are case-sensitive
/// by spec.
fn normalize(path: &str) -> String {
    path.replace('\\', "/")
}

/// Extract basename (last path segment) of a forward-slashed path.
fn basename(path: &str) -> &str {
    match path.rfind('/') {
        Some(i) => &path[i + 1..],
        None => path,
    }
}

/// Classify the given vault-relative path against the RASP fence.
pub fn classify_path(path: &str) -> PathClassification {
    let normalized = normalize(path);
    for rule in SUBSTRATE_PATH_RULES {
        match rule {
            SubstrateRule::ExactSuffix(s) => {
                // Case-insensitive basename compare for pointer-class files.
                let bn = basename(&normalized);
                if bn.eq_ignore_ascii_case(s) {
                    return PathClassification::Substrate { rule: rule.label() };
                }
            }
            SubstrateRule::PathPrefix(p) => {
                if normalized.starts_with(p) {
                    return PathClassification::Substrate { rule: rule.label() };
                }
            }
            SubstrateRule::ScopedSuffix(prefix, filename) => {
                if normalized.starts_with(prefix)
                    && basename(&normalized).eq_ignore_ascii_case(filename)
                {
                    return PathClassification::Substrate { rule: rule.label() };
                }
            }
        }
    }
    PathClassification::Content
}

/// Back-compat boolean wrapper — return true iff the given path (vault-
/// relative) is a RASP-protected substrate path that the daemon MUST NOT
/// materialize. Prefer [`classify_path`] when the matching rule is needed
/// (for logging or tray-counter attribution).
pub fn is_substrate_path(path: &str) -> bool {
    classify_path(path).is_substrate()
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Preserved tests from v0.2.0 ---

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
    fn mission_md_is_substrate_under_projects() {
        // Mission.md is now SCOPED to 02_Projects/** (no longer matches at root).
        assert!(is_substrate_path("02_Projects/Nexus/Mission.md"));
    }

    #[test]
    fn family_md_is_substrate_under_projects() {
        // Same scoping change as Mission.md.
        assert!(is_substrate_path("02_Projects/Grosse/Family.md"));
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
    fn rapport_non_fenced_subdirs_not_substrate() {
        // people/groups/triage are fenced — other rapport dirs are content.
        assert!(!is_substrate_path("_rapport/cards/foo.md"));
        assert!(!is_substrate_path("_rapport/conversations/x.md"));
    }

    #[test]
    fn ordinary_content_not_substrate() {
        assert!(!is_substrate_path(
            "02_Projects/Nexus/Specifications/foo.md"
        ));
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

    // --- New tests for v0.3 behavior ---

    #[test]
    fn claude_md_at_root_is_substrate() {
        assert!(is_substrate_path("CLAUDE.md"));
    }

    #[test]
    fn claude_md_nested_is_substrate() {
        assert!(is_substrate_path("02_Projects/Foo/CLAUDE.md"));
    }

    #[test]
    fn claude_md_lowercase_is_substrate() {
        // Case-insensitive basename for pointer-class files.
        assert!(is_substrate_path("claude.md"));
    }

    #[test]
    fn gemini_md_is_substrate() {
        assert!(is_substrate_path("GEMINI.md"));
        assert!(is_substrate_path("02_Projects/Bar/GEMINI.md"));
        assert!(is_substrate_path("gemini.md"));
    }

    #[test]
    fn agents_md_is_substrate() {
        assert!(is_substrate_path("AGENTS.md"));
        assert!(is_substrate_path("02_Projects/Baz/AGENTS.md"));
        assert!(is_substrate_path("agents.md"));
    }

    #[test]
    fn rapport_groups_is_substrate() {
        assert!(is_substrate_path("_rapport/groups/dev-team.md"));
    }

    #[test]
    fn rapport_triage_is_substrate() {
        assert!(is_substrate_path("_rapport/triage/inbox.md"));
    }

    #[test]
    fn family_md_at_root_is_content_not_substrate() {
        // NEW behavior — v0.2.0 fenced this; v0.3 scopes to 02_Projects/**.
        assert!(!is_substrate_path("Family.md"));
    }

    #[test]
    fn family_md_direct_under_projects_is_substrate() {
        // 02_Projects/Family.md — direct child, still under prefix.
        assert!(is_substrate_path("02_Projects/Family.md"));
    }

    #[test]
    fn mission_md_at_root_is_content() {
        // NEW behavior — root Mission.md no longer fenced.
        assert!(!is_substrate_path("Mission.md"));
    }

    #[test]
    fn mission_md_under_inbox_is_content() {
        // Outside 02_Projects/** — content.
        assert!(!is_substrate_path("00_Inbox/Mission.md"));
    }

    #[test]
    fn mission_md_under_projects_subdir_is_substrate() {
        assert!(is_substrate_path("02_Projects/Bar/Mission.md"));
    }

    #[test]
    fn windows_backslash_paths_normalized() {
        // Backslash → forward slash before matching.
        assert!(is_substrate_path("02_Projects\\Foo\\Family.md"));
        assert!(is_substrate_path("02_Projects\\Protocols\\foo.md"));
        assert!(is_substrate_path("_rapport\\groups\\dev.md"));
    }

    #[test]
    fn family_collision_not_caught() {
        // 'family-tree.md' is NOT 'Family.md'.
        assert!(!is_substrate_path("family-tree.md"));
        assert!(!is_substrate_path("02_Projects/Foo/family-tree.md"));
    }

    #[test]
    fn protocols_anything_is_substrate() {
        assert!(is_substrate_path("02_Projects/Protocols/anything.md"));
    }

    #[test]
    fn project_underscore_dir_is_substrate() {
        assert!(is_substrate_path("_project/test.md"));
    }

    #[test]
    fn rapport_people_alice_is_substrate() {
        assert!(is_substrate_path("_rapport/people/alice.md"));
    }

    // --- classify_path rule-attribution tests ---

    #[test]
    fn classify_returns_content_for_ordinary_paths() {
        assert_eq!(
            classify_path("01_Inbox/note.md"),
            PathClassification::Content
        );
    }

    #[test]
    fn classify_returns_rule_label_for_substrate() {
        match classify_path("CLAUDE.md") {
            PathClassification::Substrate { rule } => assert_eq!(rule, "CLAUDE.md"),
            other => panic!("expected Substrate, got {:?}", other),
        }
        match classify_path("02_Projects/Foo/Family.md") {
            PathClassification::Substrate { rule } => assert_eq!(rule, "Family.md"),
            other => panic!("expected Substrate, got {:?}", other),
        }
        match classify_path("_rapport/triage/inbox.md") {
            PathClassification::Substrate { rule } => assert_eq!(rule, "_rapport/triage/"),
            other => panic!("expected Substrate, got {:?}", other),
        }
    }
}
