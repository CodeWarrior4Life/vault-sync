//! Shared path policy: paths that are never notes.
//!
//! TKT-a3e3ceb1: canonical `vault_notes` carries ~2.72M search-index
//! pseudo-rows whose path is `<name>.md#chunk_<N>` (suffix added by the S363
//! backfill). They are slices of a parent note, NOT notes. Materializing them
//! produced 248,957 real files on link. The daemon refuses them by
//! construction at every site where a server-advertised or local path enters
//! a create/write/hash/push/delete decision.

use std::sync::atomic::{AtomicUsize, Ordering};

const CHUNK_MARKER: &str = "#chunk_";

/// True iff the FINAL path component ends in `#chunk_<digits+>` (one or more
/// ASCII digits, nothing after). Handles `/` and `\` separators.
///
/// `Foo.md#chunk_12` -> true. `chunk_notes.md`, `#chunk_x/Foo.md`,
/// `Foo.md#chunk_` and `Foo.md#chunk_1a` -> false.
pub fn is_chunk_pseudo_path(rel: &str) -> bool {
    let name = rel.rsplit(['/', '\\']).next().unwrap_or(rel);
    match name.rfind(CHUNK_MARKER) {
        Some(i) => {
            let digits = &name[i + CHUNK_MARKER.len()..];
            !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit())
        }
        None => false,
    }
}

/// Per-pass refusal tally: count every refusal, log ONE summary line.
#[derive(Debug, Default)]
pub struct ChunkRefusals(AtomicUsize);

impl ChunkRefusals {
    pub fn new() -> Self {
        Self::default()
    }
    /// Returns true (and counts) if `rel` is a chunk pseudo-path.
    pub fn refuse(&self, rel: &str) -> bool {
        if is_chunk_pseudo_path(rel) {
            self.0.fetch_add(1, Ordering::Relaxed);
            true
        } else {
            false
        }
    }
    pub fn count(&self) -> usize {
        self.0.load(Ordering::Relaxed)
    }
    /// Emit the single per-pass summary line (only when N > 0).
    pub fn log_summary(&self, site: &str) {
        let n = self.count();
        if n > 0 {
            tracing::info!(site, "chunk pseudo-paths refused: {n}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn positives() {
        assert!(is_chunk_pseudo_path("Foo.md#chunk_12"));
        assert!(is_chunk_pseudo_path("01_Notes/Foo.md#chunk_0"));
        assert!(is_chunk_pseudo_path("a/b/c/Deep Note.md#chunk_2719999"));
        assert!(is_chunk_pseudo_path("a\\b\\Foo.md#chunk_3"));
        assert!(is_chunk_pseudo_path("Foo.md#chunk_1#chunk_2"));
    }

    #[test]
    fn negatives() {
        assert!(!is_chunk_pseudo_path("chunk_notes.md"));
        assert!(!is_chunk_pseudo_path("01_Notes/chunk_notes.md"));
        assert!(!is_chunk_pseudo_path("#chunk_x/Foo.md"));
        assert!(!is_chunk_pseudo_path("#chunk_5/Foo.md"));
        assert!(!is_chunk_pseudo_path("Foo.md"));
        assert!(!is_chunk_pseudo_path(""));
    }

    #[test]
    fn edge_marker_without_digits_or_with_trailing_junk() {
        assert!(!is_chunk_pseudo_path("Foo.md#chunk_"));
        assert!(!is_chunk_pseudo_path("Foo.md#chunk_x"));
        assert!(!is_chunk_pseudo_path("Foo.md#chunk_1a"));
        assert!(!is_chunk_pseudo_path("Foo.md#chunk_1.md"));
        assert!(!is_chunk_pseudo_path("Foo.md#chunk_ 1"));
        assert!(!is_chunk_pseudo_path("Foo.md#chunk_١")); // non-ASCII digit
    }

    #[test]
    fn tally_counts_only_chunks() {
        let t = ChunkRefusals::new();
        assert!(t.refuse("a.md#chunk_1"));
        assert!(!t.refuse("a.md"));
        assert!(t.refuse("b/c.md#chunk_2"));
        assert_eq!(t.count(), 2);
    }
}
