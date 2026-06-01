/// Mirrors `server/nexus/api/scope_filter_match.py::path_in_scope`.
/// Semantics:
///   - empty roots → include everything (no positive filter)
///   - non-empty roots → path must start with one of them
///   - excludes (non-empty) → path must NOT start with any of them; excludes override roots
pub fn path_in_scope(path: &str, scope_roots: &[String], scope_excludes: &[String]) -> bool {
    // Excludes first
    for ex in scope_excludes {
        if path.starts_with(ex.as_str()) {
            return false;
        }
    }
    // Roots
    if scope_roots.is_empty() {
        return true;
    }
    for root in scope_roots {
        if path.starts_with(root.as_str()) {
            return true;
        }
    }
    false
}

/// True if any `/`- or `\`-delimited segment is exactly `..` — a real
/// path-traversal escape. A filename that merely *contains* `..` as a
/// substring (e.g. `Anysa says....md`, a title ending in `...`) is a
/// legitimate name, NOT a traversal.
///
/// S490: the prior `path.contains("..")` substring check black-holed ~96 real
/// `01_Notes/` notes whose titles end in `...` (three ASCII dots) — they never
/// materialized. Ellipsis `…` (U+2026, single char) slipped through because it
/// has no `..`; the three-dot ASCII form did not. Segment equality fixes both.
pub fn has_dotdot_segment(path: &str) -> bool {
    path.split(['/', '\\']).any(|seg| seg == "..")
}

/// Path-traversal guard — rejects paths that escape vault root.
pub fn is_safe_path(path: &str) -> bool {
    !has_dotdot_segment(path)
        && !path.starts_with('/')
        && !path.starts_with('\\')
        && !path.contains(":\\")  // Windows drive
        && !path.contains(":/")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_real_traversal_segments() {
        assert!(!is_safe_path("../escape.md"));
        assert!(!is_safe_path("01_Notes/../../etc/passwd"));
        assert!(!is_safe_path("a/../b.md"));
        assert!(!is_safe_path("foo/..")); // trailing segment
        assert!(!is_safe_path("..\\windows\\esc.md")); // backslash sep
    }

    #[test]
    fn allows_dots_inside_filenames() {
        // S490 regression: titles ending in `...` (three ASCII dots) contain
        // `..` as a substring but are NOT traversals.
        assert!(is_safe_path("01_Notes/Anysa says....md"));
        assert!(is_safe_path(
            "01_Notes/And that's the bottom line because....md"
        ));
        assert!(is_safe_path("01_Notes/A file named ... .md"));
        assert!(is_safe_path("01_Notes/...md")); // leading dots in a name
        assert!(is_safe_path("01_Notes/a..b.md")); // double dot mid-name
                                                   // ellipsis (U+2026) always passed; keep it green.
        assert!(is_safe_path("01_Notes/Anysa says….md"));
    }

    #[test]
    fn rejects_absolute_and_drive_paths() {
        assert!(!is_safe_path("/etc/passwd"));
        assert!(!is_safe_path("\\\\server\\share"));
        assert!(!is_safe_path("C:\\Windows"));
        assert!(!is_safe_path("file://x"));
    }

    #[test]
    fn dotdot_segment_detection() {
        assert!(has_dotdot_segment("a/../b"));
        assert!(has_dotdot_segment(".."));
        assert!(!has_dotdot_segment("a...b"));
        assert!(!has_dotdot_segment("..."));
        assert!(!has_dotdot_segment("name....md"));
    }
}
