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
///
/// S018 / "substrate must sync" (2026-06-20): the rule list is now EMPTY.
///
/// The daemon previously fenced substrate paths OUT of sync in both
/// directions (never pushed, never materialized) to honor RASP's Substrate
/// Layer Inviolability corollary. That was a categorical error: it conflated
/// *transport* (faithfully replicating canonical bytes — exactly what RASP
/// WANTS, "the substrate is the singular source of truth everywhere") with
/// *mutation* (transforming content — what RASP actually forbids). Fencing
/// substrate out of transport made every host carry a divergent copy — the
/// precise divergence RASP exists to prevent.
///
/// Operator ruling: "substrate needs to move; the junk fence is important."
/// So substrate now transports as ordinary CONTENT, protected by the same
/// conflict-stash / newer-wins / anti-strip machinery as any note. The SERVER
/// already accepts substrate (its baseline excludes are junk-only), so the
/// fix is daemon-only and lives here: with no rules, `classify_path` always
/// returns `Content` and every downstream consumer uniformly stops refusing
/// substrate. `is_junk_path` (below) is UNCHANGED — the junk fence stays.
///
/// To revert (restore the substrate fence), repopulate this list with the
/// `SubstrateRule` entries that were here historically — no consumer changes
/// are needed because the `Substrate` classification path is still wired.
const SUBSTRATE_PATH_RULES: &[SubstrateRule] = &[];

/// Closed enumeration of substrate-path rule kinds. The runtime rule list
/// (`SUBSTRATE_PATH_RULES`) is EMPTY ("substrate must sync", 2026-06-20), so no
/// rule is ever constructed and substrate transports as content. The variants
/// are kept (with the matcher arms in `classify_path`) so the fence is restored
/// simply by repopulating the rule list — zero consumer-site changes. While the
/// list is empty the variants are statically unconstructed, hence the allow.
#[allow(dead_code)] // restored when SUBSTRATE_PATH_RULES is repopulated
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
    /// Path is regular content — daemon may materialize. Since the substrate
    /// fence was lifted ("substrate must sync"), `classify_path` ALWAYS returns
    /// this; substrate transports as content.
    Content,
    /// Path is RASP-protected substrate. `rule` is the static label of the
    /// matching rule (suitable for structured log fields + tray counters).
    /// Constructed only when `SUBSTRATE_PATH_RULES` has entries (currently
    /// empty), so at runtime it never occurs — but it is still referenced by
    /// `classify_path`'s matcher and every consumer's match arms, so the fence
    /// is restored by repopulating the rule list without touching any consumer.
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

/// Strip the first path segment if there are at least two segments. This
/// is the S477 "after optional vault folder prefix" view: incoming paths
/// now include the vault folder as their first segment
/// (`Mainframe/02_Projects/Protocols/foo.md`), so substrate rules anchored
/// at the vault root (`02_Projects/Protocols/`) must be matched against
/// both the path as-given and the path with its first segment removed.
fn after_first_segment(path: &str) -> Option<&str> {
    path.find('/').map(|i| &path[i + 1..])
}

/// Classify the given vault-relative-or-vaults-root-relative path against
/// the RASP fence. Substrate rules are anchored at the vault root; with
/// S477 the path may carry an extra vault folder as its leading segment
/// (`Mainframe/...`), so each rule is checked against the path as-given
/// AND against the path with its first segment stripped.
pub fn classify_path(path: &str) -> PathClassification {
    let normalized = normalize(path);
    let stripped = after_first_segment(&normalized);
    let candidates: &[&str] = match stripped {
        Some(s) => &[normalized.as_str(), s],
        None => &[normalized.as_str()],
    };
    for rule in SUBSTRATE_PATH_RULES {
        for candidate in candidates {
            match rule {
                SubstrateRule::ExactSuffix(s) => {
                    // Case-insensitive basename compare for pointer-class
                    // files. Basename-match is prefix-invariant so the
                    // stripped variant is redundant here but kept for
                    // uniformity (cheap).
                    let bn = basename(candidate);
                    if bn.eq_ignore_ascii_case(s) {
                        return PathClassification::Substrate { rule: rule.label() };
                    }
                }
                SubstrateRule::PathPrefix(p) => {
                    if candidate.starts_with(p) {
                        return PathClassification::Substrate { rule: rule.label() };
                    }
                }
                SubstrateRule::ScopedSuffix(prefix, filename) => {
                    if candidate.starts_with(prefix)
                        && basename(candidate).eq_ignore_ascii_case(filename)
                    {
                        return PathClassification::Substrate { rule: rule.label() };
                    }
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

/// Returns `true` iff the path contains a junk segment that must be excluded
/// from sync regardless of vault configuration.
///
/// Junk classes (checked per path segment after normalization):
/// - **AppleDouble files** — any basename that starts with `._` (e.g.
///   `._note.md`, `dir/._x.md`). These are macOS extended-attribute sidecar
///   files that cause duplicate messes on non-HFS+ filesystems.
/// - **`.DS_Store` / `Thumbs.db`** — OS folder metadata, exact basename match.
/// - **V9 substrate dirs** — `.obsidian`, `.trash`, `.git`, `node_modules`,
///   `.cody-tmp`, `__pycache__`, `.lattice-sync`, `.lattice-runtime`, matched
///   as an exact segment at any depth (the server rejects these with HTTP 400).
/// - **Junk extensions** — `*.pyc`, `*.swp`, `*.tmp`.
///
/// Carve-out — `.nx-*` machine-namespace segments (e.g. `.nx-trinity/`,
/// `.nx-morpheus/`) are NOT junk. The `._` prefix requires a literal
/// underscore after the dot, so `.nx-` (dot + 'n') is structurally
/// distinct and is never matched by the AppleDouble check. This is verified
/// by the `includes_nx_host_namespace` test.
pub fn is_junk_path(path: &str) -> bool {
    // V9 baseline substrate dirs the server rejects (HTTP 400) and that must
    // never be pushed. Exact segment match at ANY depth (e.g. the nested
    // `02_Projects/.obsidian/workspace.json`). S481: these regressed off the
    // push walk, causing a 400 retry-storm.
    const JUNK_DIRS: &[&str] = &[
        ".obsidian",
        ".trash",
        ".git",
        "node_modules",
        ".cody-tmp",
        "__pycache__",
        ".lattice-sync",
        ".lattice-runtime",
    ];
    // Junk basenames (exact, case-sensitive).
    const JUNK_BASENAMES: &[&str] = &[".DS_Store", "Thumbs.db"];
    // Junk file extensions (suffix match on the segment).
    const JUNK_EXTS: &[&str] = &[".pyc", ".swp", ".tmp"];

    let normalized = normalize(path);
    for segment in normalized.split('/') {
        if segment.is_empty() {
            continue;
        }
        // AppleDouble: basename starts with `._`
        if segment.starts_with("._") {
            return true;
        }
        // exact junk basenames (.DS_Store, Thumbs.db)
        if JUNK_BASENAMES.contains(&segment) {
            return true;
        }
        // V9 substrate dirs — exact segment match, so `.nx-*` machine
        // namespaces are unaffected (only literal substrate dir names match).
        if JUNK_DIRS.contains(&segment) {
            return true;
        }
        // junk extensions (*.pyc / *.swp / *.tmp)
        if JUNK_EXTS.iter().any(|e| segment.ends_with(e)) {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- "substrate must sync" inversion (2026-06-20) ---
    //
    // The substrate fence is lifted: `SUBSTRATE_PATH_RULES` is empty, so EVERY
    // path classifies as `Content` and `is_substrate_path` is always false.
    // The former substrate paths (00_VAULT.md, CLAUDE.md, Protocols/, _project/,
    // _rapport/{people,groups,triage}/, scoped Mission.md/Family.md) now
    // transport as ordinary content. The tests below pin that inversion: each
    // former-substrate path is now NOT substrate / IS content.

    #[test]
    fn root_00_vault_is_content_now() {
        assert!(!is_substrate_path("00_VAULT.md"));
    }

    #[test]
    fn nested_00_vault_is_content_now() {
        assert!(!is_substrate_path("02_Projects/Nexus/00_VAULT.md"));
        assert!(!is_substrate_path("01_Inbox/00_VAULT.md"));
    }

    #[test]
    fn mission_md_under_projects_is_content_now() {
        assert!(!is_substrate_path("02_Projects/Nexus/Mission.md"));
        assert!(!is_substrate_path("02_Projects/Bar/Mission.md"));
        assert!(!is_substrate_path("02_Projects/Foo/CLAUDE.md"));
    }

    #[test]
    fn family_md_under_projects_is_content_now() {
        assert!(!is_substrate_path("02_Projects/Grosse/Family.md"));
        assert!(!is_substrate_path("02_Projects/Family.md"));
    }

    #[test]
    fn protocols_dir_is_content_now() {
        assert!(!is_substrate_path("02_Projects/Protocols/foo.md"));
        assert!(!is_substrate_path("02_Projects/Protocols/sub/bar.md"));
        assert!(!is_substrate_path("02_Projects/Protocols/anything.md"));
    }

    #[test]
    fn project_dir_is_content_now() {
        assert!(!is_substrate_path("_project/anything.md"));
        assert!(!is_substrate_path("_project/test.md"));
    }

    #[test]
    fn rapport_people_dir_is_content_now() {
        assert!(!is_substrate_path("_rapport/people/cyril.md"));
        assert!(!is_substrate_path("_rapport/people/sub/x.md"));
        assert!(!is_substrate_path("_rapport/people/alice.md"));
    }

    #[test]
    fn rapport_groups_is_content_now() {
        assert!(!is_substrate_path("_rapport/groups/dev-team.md"));
    }

    #[test]
    fn rapport_triage_is_content_now() {
        assert!(!is_substrate_path("_rapport/triage/inbox.md"));
    }

    #[test]
    fn rapport_non_fenced_subdirs_still_content() {
        // These were always content and remain content.
        assert!(!is_substrate_path("_rapport/cards/foo.md"));
        assert!(!is_substrate_path("_rapport/conversations/x.md"));
    }

    #[test]
    fn pointer_files_are_content_now() {
        // 00_VAULT/CLAUDE/GEMINI/AGENTS at any depth + any case: all content.
        for p in [
            "CLAUDE.md",
            "02_Projects/Foo/CLAUDE.md",
            "claude.md",
            "GEMINI.md",
            "02_Projects/Bar/GEMINI.md",
            "gemini.md",
            "AGENTS.md",
            "02_Projects/Baz/AGENTS.md",
            "agents.md",
        ] {
            assert!(!is_substrate_path(p), "{p} must be content now");
        }
    }

    #[test]
    fn ordinary_content_still_content() {
        assert!(!is_substrate_path(
            "02_Projects/Nexus/Specifications/foo.md"
        ));
        assert!(!is_substrate_path("01_Inbox/quick-note.md"));
        assert!(!is_substrate_path("03_Areas/Health/journal.md"));
        assert!(!is_substrate_path("Daily/2026-05-27.md"));
        // Root Mission.md / Family.md were already content; still content.
        assert!(!is_substrate_path("Mission.md"));
        assert!(!is_substrate_path("Family.md"));
        assert!(!is_substrate_path("00_Inbox/Mission.md"));
    }

    #[test]
    fn windows_backslash_paths_are_content_now() {
        // Former-substrate paths with backslashes are now content too.
        assert!(!is_substrate_path("02_Projects\\Foo\\Family.md"));
        assert!(!is_substrate_path("02_Projects\\Protocols\\foo.md"));
        assert!(!is_substrate_path("_rapport\\groups\\dev.md"));
    }

    // --- classify_path now always returns Content ---

    #[test]
    fn classify_returns_content_for_ordinary_paths() {
        assert_eq!(
            classify_path("01_Inbox/note.md"),
            PathClassification::Content
        );
    }

    #[test]
    fn classify_returns_content_for_former_substrate_paths() {
        // The inversion: paths that USED to classify as Substrate now classify
        // as Content on BOTH the push and pull sides (classify_path is the
        // single source consumed by file_watcher push + materializer pull).
        for p in [
            "02_Projects/Protocols/foo.md",
            "CLAUDE.md",
            "02_Projects/Foo/Family.md",
            "_rapport/triage/inbox.md",
            "_project/x.md",
            "00_VAULT.md",
            // S477 vault-folder-prefixed variant:
            "Mainframe/02_Projects/Protocols/foo.md",
        ] {
            assert_eq!(
                classify_path(p),
                PathClassification::Content,
                "{p} must classify as Content (substrate fence lifted)"
            );
            assert!(!is_substrate_path(p), "{p} must not be substrate");
        }
    }

    // --- B3: AppleDouble / DS_Store junk exclusion + .nx-* carve-out ---

    #[test]
    fn excludes_appledouble() {
        // Root-level AppleDouble sidecar
        assert!(is_junk_path("._note.md"), "._note.md should be junk");
        // Nested AppleDouble sidecar
        assert!(is_junk_path("dir/._x.md"), "dir/._x.md should be junk");
        // Any basename starting with ._
        assert!(is_junk_path("._anything"), "._anything should be junk");
        // Deep nesting
        assert!(
            is_junk_path("02_Projects/Foo/._hidden.md"),
            "deeply nested ._hidden.md should be junk"
        );
        // Windows backslash path normalized
        assert!(
            is_junk_path("02_Projects\\Foo\\._bar.md"),
            "backslash path with ._bar.md should be junk"
        );
    }

    #[test]
    fn excludes_dsstore() {
        assert!(
            is_junk_path(".DS_Store"),
            ".DS_Store at root should be junk"
        );
        assert!(
            is_junk_path("dir/.DS_Store"),
            ".DS_Store nested should be junk"
        );
        assert!(
            is_junk_path("02_Projects/Nexus/.DS_Store"),
            ".DS_Store deep nested should be junk"
        );
    }

    #[test]
    fn excludes_v9_substrate_dirs_at_any_depth() {
        // S481: server rejects these with HTTP 400; daemon must exclude them.
        for p in [
            ".obsidian/workspace.json",
            "02_Projects/.obsidian/x.json",
            ".obsidian/plugins/foo/main.js",
            ".trash/old.md",
            "02_Projects/Nexus/.trash/x.md",
            ".git/config",
            "node_modules/pkg/index.js",
            ".cody-tmp/scratch.md",
            "sub/__pycache__/x.pyc",
            ".lattice-sync/shadow/x.md",
            ".lattice-runtime/uuid/state",
            "dir/Thumbs.db",
            "x.pyc",
            "note.md.swp",
            "build.tmp",
        ] {
            assert!(is_junk_path(p), "{p} should be junk (V9 substrate)");
        }
        // Real notes + .nx-* machine namespaces must still sync.
        for p in [
            "02_Projects/Nexus/note.md",
            "01_Notes/x.md",
            ".nx-trinity/build/out.bin",
            "02_Projects/.nx-morpheus/build/x.md",
        ] {
            assert!(!is_junk_path(p), "{p} must NOT be junk (should sync)");
        }
    }

    #[test]
    fn includes_nx_host_namespace() {
        // .nx-<host> machine-namespace dirs must NOT be treated as junk.
        assert!(
            !is_junk_path(".nx-trinity/build/out.bin"),
            ".nx-trinity/ should NOT be junk"
        );
        assert!(
            !is_junk_path("dir/.nx-morpheus/x.md"),
            ".nx-morpheus/ nested should NOT be junk"
        );
        assert!(
            !is_junk_path(".nx-neo/y"),
            ".nx-neo/ top-level should NOT be junk"
        );
        // The ._* rule requires underscore immediately after dot — .nx- has 'n',
        // so it is structurally distinct. Belt-and-suspenders check:
        assert!(
            !is_junk_path(".nx-morpheus/build/x"),
            ".nx-morpheus/build/x should NOT be junk (no ._ segment)"
        );
    }

    #[test]
    fn junk_fence_unaffected_by_substrate_inversion() {
        // The junk fence and the (now-lifted) substrate fence are orthogonal.
        // Former-substrate paths are NOT junk — they sync as content. The junk
        // fence itself (.obsidian/.trash/.git/etc.) is untouched by the
        // "substrate must sync" change.
        for p in [
            "_rapport/people/x",
            "02_Projects/Protocols/foo.md",
            "_project/anything.md",
        ] {
            assert!(!is_junk_path(p), "{p} is content, not junk — must sync");
            assert!(
                !is_substrate_path(p),
                "{p} is no longer substrate (fence lifted)"
            );
        }
    }

    #[test]
    fn normal_note_not_excluded() {
        assert!(
            !is_junk_path("02_Projects/foo.md"),
            "ordinary note should not be junk"
        );
        assert!(!is_junk_path("Daily/2026-05-29.md"), "daily note not junk");
        assert!(!is_junk_path("01_Inbox/quick.md"), "inbox note not junk");
    }
}
