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

/// Path-traversal guard — rejects paths that escape vault root.
pub fn is_safe_path(path: &str) -> bool {
    !path.contains("..")
        && !path.starts_with('/')
        && !path.starts_with('\\')
        && !path.contains(":\\")  // Windows drive
        && !path.contains(":/")
}
