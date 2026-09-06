//! Append-aware conflict resolution (TKT-ddff1877, regression of the
//! 2026-05-25 append-concatenation P0).
//!
//! `materializer::decide()` is content-relational: when local, the last-synced
//! shadow and the server head are three distinct versions it calls the pair a
//! conflict and the always-stash floor forks the WHOLE file
//! (`<stem>.conflict-from-<own device>-<seq>.md`) and overwrites local with the
//! server head. For an append-only file — a flight recorder — that outcome is
//! provably wrong in two shapes the fleet measured on link and Trinity
//! (2026-09-01 .. 2026-09-05, 25 forks, 31+34 MB, 78 appended lines dropped
//! from the live file):
//!
//! * **Stale head.** The server head is an OLDER snapshot of the same file (a
//!   peer re-pushed a stale copy): every server line is already in local, in
//!   order, at the front. Materializing the head moves the file BACKWARD and
//!   sidelines the newer appended lines into a fork nobody contested.
//! * **Two appenders.** Both sides appended after the same last-synced base:
//!   `local = base + L`, `server = base + S`. The lossless resolution is
//!   `base + S + L`, one converged file, not a fork.
//!
//! Both are decided here with PURE functions and NO heuristics:
//!
//! * a strict line-prefix test for the stale-head shape, and
//! * a VERIFIED common base for the two-appender shape — the common byte
//!   prefix of local and server, cut at a line boundary, whose sha256 equals
//!   the shadow store's last-synced server hash. Only a cut point that hashes
//!   to the exact version this daemon last synced is accepted as the base, so
//!   an edit INSIDE the shared history (a modified or deleted line) never
//!   passes and falls back to the existing stash floor.
//!
//! Everything else is `Unresolved` and the caller keeps its pre-existing
//! behavior. Nothing here writes, records or logs; the materializer owns the
//! side effects (echo-guard, atomic write, observed-lineage record, the
//! compensating push).

use sha2::{Digest, Sha256};

/// How many candidate line-boundary cut points below the raw common-prefix end
/// are hashed while searching for the verified base. Two appenders whose
/// appended bytes happen to share a leading run (e.g. both start with "\n- **")
/// push the raw common prefix a little PAST the true base; this bounds the walk
/// back. Generous for any real recorder append (a handful of lines).
pub const MAX_BASE_BACKTRACK_CANDIDATES: usize = 128;

/// Outcome of [`resolve`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AppendResolution {
    /// Local strictly supersedes the server head: every server byte is at the
    /// front of local (line-aligned) and local has more. Preserve local, push
    /// it up; nothing on the server is lost.
    LocalSupersedesServer,
    /// The server head strictly supersedes local (local is a line-prefix of
    /// the head, or both appended and the server already holds local's
    /// append plus more). A clean pull converges without loss; no stash.
    ServerSupersedesLocal,
    /// Both sides appended after a VERIFIED common base. `merged` is
    /// `base + server_tail + local_tail`, `base_len` the verified cut.
    Merged { base_len: usize, merged: Vec<u8> },
    /// No lossless append relation; the caller keeps its existing behavior.
    Unresolved,
}

/// True iff `prefix` is a strict, line-aligned prefix of `whole`: `whole`
/// starts with `prefix`, is longer, `prefix` is non-empty, and the boundary
/// falls on a line end (`prefix` ends with `\n`, or the next byte of `whole`
/// is `\n`). A mid-line cut is NOT an append and is refused.
pub fn is_strict_line_prefix(prefix: &[u8], whole: &[u8]) -> bool {
    if prefix.is_empty() || whole.len() <= prefix.len() || !whole.starts_with(prefix) {
        return false;
    }
    prefix[prefix.len() - 1] == b'\n' || whole[prefix.len()] == b'\n'
}

/// Length of the common byte prefix of `a` and `b`.
fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count()
}

fn decode_sha_hex(hex_sha: &str) -> Option<[u8; 32]> {
    let bytes = hex::decode(hex_sha).ok()?;
    bytes.try_into().ok()
}

/// Find the VERIFIED common base of `local` and `server`: the longest cut point
/// `p` within their common byte prefix such that `p` is a line boundary and
/// `sha256(local[..p]) == shadow_sha_hex` (the last-synced server hash this
/// daemon recorded). Returns `None` when no cut hashes to the shadow, when the
/// shadow is not a valid sha256 hex, or when either side has nothing after the
/// base (those are the R2/R3 shapes, not a two-appender merge).
///
/// Cost: one linear pass over the common prefix (an incremental hasher cloned
/// at each candidate), bounded by [`MAX_BASE_BACKTRACK_CANDIDATES`].
pub fn verified_common_base(local: &[u8], server: &[u8], shadow_sha_hex: &str) -> Option<usize> {
    let want = decode_sha_hex(shadow_sha_hex)?;
    let n = common_prefix_len(local, server);
    if n == 0 {
        return None;
    }
    // Candidate cut points, ascending: every position p <= n where the byte
    // before p is '\n' (the base ended with a newline) or the byte AT p is '\n'
    // in both inputs (the base's last line was unterminated and both appenders
    // supplied the newline), plus n itself. Keep only the highest
    // MAX_BASE_BACKTRACK_CANDIDATES.
    let mut candidates: Vec<usize> = Vec::new();
    for p in (1..=n).rev() {
        let line_end_before = local[p - 1] == b'\n';
        let newline_at =
            p < local.len() && p < server.len() && local[p] == b'\n' && server[p] == b'\n';
        if p == n || line_end_before || newline_at {
            candidates.push(p);
            if candidates.len() >= MAX_BASE_BACKTRACK_CANDIDATES {
                break;
            }
        }
    }
    candidates.sort_unstable();
    candidates.dedup();
    let mut hasher = Sha256::new();
    let mut fed = 0usize;
    for &p in &candidates {
        hasher.update(&local[fed..p]);
        fed = p;
        let digest = hasher.clone().finalize();
        if digest[..] == want[..] {
            // Both sides must have appended something past the base; a side
            // that did not is the R2/R3 shape and not ours.
            if p < local.len() && p < server.len() {
                return Some(p);
            }
            return None;
        }
    }
    None
}

/// Largest `k <= min(len)` such that `a[..k] == b[..k]` and `k` is a line
/// boundary in both (`k == len` of a side, or the byte before `k` is `\n`).
fn common_line_prefix_len(a: &[u8], b: &[u8]) -> usize {
    let n = common_prefix_len(a, b);
    if n == a.len() || n == b.len() {
        return n;
    }
    // Walk back to the last '\n' inside the common run.
    a[..n]
        .iter()
        .rposition(|&c| c == b'\n')
        .map_or(0, |i| i + 1)
}

/// Build the lossless two-appender merge given a verified `base_len`:
/// `base + shared + server_rest + local_rest`, where `shared` is the
/// line-aligned run both tails begin with (lines both sides appended
/// identically — kept ONCE, never duplicated). When one tail is entirely
/// contained in the other, the longer side is returned verbatim. A newline is
/// inserted between the tails only when needed so two entries never join into
/// one line, and a duplicated separator newline is dropped when the base was
/// unterminated and both appenders supplied one.
pub fn merge_appends(base_len: usize, local: &[u8], server: &[u8]) -> Vec<u8> {
    let base = &local[..base_len];
    let lx = &local[base_len..];
    let ly = &server[base_len..];
    let k = common_line_prefix_len(lx, ly);
    let (shared, lx_rest, ly_rest) = (&lx[..k], &lx[k..], &ly[k..]);
    if ly_rest.is_empty() {
        return local.to_vec();
    }
    if lx_rest.is_empty() {
        return server.to_vec();
    }
    let mut out = Vec::with_capacity(local.len() + ly_rest.len() + 1);
    out.extend_from_slice(base);
    out.extend_from_slice(shared);
    out.extend_from_slice(ly_rest);
    let head_terminated = out.last() == Some(&b'\n');
    let before_ly_terminated = base
        .iter()
        .chain(shared.iter())
        .next_back()
        .is_some_and(|&c| c == b'\n');
    let lx_leading_nl = lx_rest.first() == Some(&b'\n');
    let mut lx_emit = lx_rest;
    if !before_ly_terminated && head_terminated && lx_leading_nl {
        // Both appenders supplied the separator the unterminated base lacked;
        // the server's copy is already in place.
        lx_emit = &lx_rest[1..];
    } else if !head_terminated && !lx_leading_nl {
        out.push(b'\n');
    }
    out.extend_from_slice(lx_emit);
    out
}

/// Decide the append relation between `local` (the bytes on this host's disk)
/// and `server` (the head being materialized), given the last-synced shadow
/// hash when known. Pure: no I/O, no logging.
pub fn resolve(local: &[u8], server: &[u8], shadow_sha_hex: Option<&str>) -> AppendResolution {
    if is_strict_line_prefix(server, local) {
        return AppendResolution::LocalSupersedesServer;
    }
    if is_strict_line_prefix(local, server) {
        return AppendResolution::ServerSupersedesLocal;
    }
    let Some(shadow) = shadow_sha_hex else {
        return AppendResolution::Unresolved;
    };
    let Some(base_len) = verified_common_base(local, server, shadow) else {
        return AppendResolution::Unresolved;
    };
    let merged = merge_appends(base_len, local, server);
    if merged == local {
        return AppendResolution::LocalSupersedesServer;
    }
    if merged == server {
        return AppendResolution::ServerSupersedesLocal;
    }
    AppendResolution::Merged { base_len, merged }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sha(b: &[u8]) -> String {
        hex::encode(Sha256::digest(b))
    }

    const BASE: &[u8] = b"# recorder\n\n## SESSION LOG\n- **09:00** opened. Files: log\n- **09:05** step one. Files: a\n";

    #[test]
    fn strict_line_prefix_accepts_line_aligned_append_only() {
        let whole = [BASE, b"- **09:10** appended\n"].concat();
        assert!(is_strict_line_prefix(BASE, &whole));
        // Equal is not strict.
        assert!(!is_strict_line_prefix(BASE, BASE));
        // Empty prefix never qualifies.
        assert!(!is_strict_line_prefix(b"", &whole));
        // Mid-line cut is refused.
        let cut = &whole[..whole.len() - 4];
        assert!(!is_strict_line_prefix(cut, &whole));
        // Unterminated base followed by "\n..." is line-aligned.
        let unterminated = b"line one\nline two";
        let grown = b"line one\nline two\nline three\n";
        assert!(is_strict_line_prefix(unterminated, grown));
    }

    #[test]
    fn verified_base_is_found_only_when_a_line_cut_hashes_to_the_shadow() {
        let local = [BASE, b"- **09:10** link\n"].concat();
        let server = [BASE, b"- **09:11** trinity\n"].concat();
        assert_eq!(
            verified_common_base(&local, &server, &sha(BASE)),
            Some(BASE.len())
        );
        // A shadow that names some other version: no base.
        assert_eq!(verified_common_base(&local, &server, &sha(b"other")), None);
        // Garbage shadow hex: no base, no panic.
        assert_eq!(verified_common_base(&local, &server, "zz"), None);
    }

    #[test]
    fn verified_base_backtracks_past_a_shared_append_prefix() {
        // Both appenders start their entry with the same 12 bytes, so the raw
        // common prefix runs PAST the base. The verified cut must still land
        // exactly on the base.
        let local = [BASE, b"- **09:10** link did X\n"].concat();
        let server = [BASE, b"- **09:10** trinity did Y\n"].concat();
        assert!(common_prefix_len(&local, &server) > BASE.len());
        assert_eq!(
            verified_common_base(&local, &server, &sha(BASE)),
            Some(BASE.len())
        );
    }

    #[test]
    fn verified_base_handles_an_unterminated_base() {
        let base = b"# rec\n- **09:00** last line without newline";
        let local = [&base[..], b"\n- **09:10** link\n"].concat();
        let server = [&base[..], b"\n- **09:11** trinity\n"].concat();
        assert_eq!(
            verified_common_base(&local, &server, &sha(base)),
            Some(base.len())
        );
        let merged = merge_appends(base.len(), &local, &server);
        assert_eq!(
            merged,
            [&base[..], b"\n- **09:11** trinity\n- **09:10** link\n"].concat(),
            "one separator, not two"
        );
    }

    #[test]
    fn verified_base_refuses_when_one_side_did_not_append() {
        // local == base exactly: that is R3 (clean pull), not a merge.
        let server = [BASE, b"- **09:11** trinity\n"].concat();
        assert_eq!(verified_common_base(BASE, &server, &sha(BASE)), None);
    }

    #[test]
    fn merge_keeps_every_line_exactly_once_server_tail_first() {
        let local = [BASE, b"- **09:10** link\n"].concat();
        let server = [BASE, b"- **09:11** trinity\n"].concat();
        let merged = merge_appends(BASE.len(), &local, &server);
        assert_eq!(
            merged,
            [BASE, b"- **09:11** trinity\n- **09:10** link\n"].concat()
        );
        let text = String::from_utf8(merged).unwrap();
        assert_eq!(text.matches("- **09:10** link").count(), 1);
        assert_eq!(text.matches("- **09:11** trinity").count(), 1);
    }

    #[test]
    fn merge_inserts_a_newline_when_the_server_tail_is_unterminated() {
        let local = [BASE, b"- **09:10** link\n"].concat();
        let server = [BASE, b"- **09:11** trinity"].concat();
        let merged = merge_appends(BASE.len(), &local, &server);
        assert_eq!(
            merged,
            [BASE, b"- **09:11** trinity\n- **09:10** link\n"].concat()
        );
    }

    #[test]
    fn merge_dedupes_identical_and_nested_appends() {
        let same = [BASE, b"- **09:10** same\n"].concat();
        assert_eq!(merge_appends(BASE.len(), &same, &same), same);
        let longer = [BASE, b"- **09:10** same\n- **09:12** more\n"].concat();
        // local has server's append plus more -> local verbatim
        assert_eq!(merge_appends(BASE.len(), &longer, &same), longer);
        // server has local's append plus more -> server verbatim
        assert_eq!(merge_appends(BASE.len(), &same, &longer), longer);
    }

    #[test]
    fn merge_keeps_a_shared_appended_run_once_then_both_tails() {
        // Both sides appended the same two lines first (e.g. a salvage
        // re-append landed on both hosts), then diverged. The shared run must
        // appear ONCE, followed by the server's rest, then local's rest.
        let shared = b"- **09:10** shared one\n- **09:11** shared two\n";
        let local = [BASE, shared, b"- **09:12** link\n"].concat();
        let server = [BASE, shared, b"- **09:13** trinity\n"].concat();
        let merged = merge_appends(BASE.len(), &local, &server);
        assert_eq!(
            merged,
            [BASE, shared, b"- **09:13** trinity\n- **09:12** link\n"].concat()
        );
        let text = String::from_utf8(merged).unwrap();
        assert_eq!(text.matches("shared one").count(), 1);
        assert_eq!(text.matches("shared two").count(), 1);
    }

    #[test]
    fn resolve_stale_head_is_local_supersedes() {
        // The measured link shape: the head is an older snapshot of the file.
        let local = [BASE, b"- **09:10** a\n- **09:12** b\n- **09:15** c\n"].concat();
        let stale_head = &local[..BASE.len() + "- **09:10** a\n".len()];
        assert_eq!(
            resolve(&local, stale_head, Some(&sha(b"whatever the shadow says"))),
            AppendResolution::LocalSupersedesServer
        );
        // ...and it needs no shadow at all.
        assert_eq!(
            resolve(&local, stale_head, None),
            AppendResolution::LocalSupersedesServer
        );
    }

    #[test]
    fn resolve_two_appenders_merges_with_verified_base() {
        let local = [BASE, b"- **09:10** link\n"].concat();
        let server = [BASE, b"- **09:11** trinity\n"].concat();
        match resolve(&local, &server, Some(&sha(BASE))) {
            AppendResolution::Merged { base_len, merged } => {
                assert_eq!(base_len, BASE.len());
                assert_eq!(
                    merged,
                    [BASE, b"- **09:11** trinity\n- **09:10** link\n"].concat()
                );
            }
            other => panic!("expected Merged, got {other:?}"),
        }
    }

    #[test]
    fn resolve_local_prefix_of_server_is_server_supersedes() {
        let local = [BASE, b"- **09:10** a\n"].concat();
        let server = [BASE, b"- **09:10** a\n- **09:12** b\n"].concat();
        assert_eq!(
            resolve(&local, &server, None),
            AppendResolution::ServerSupersedesLocal
        );
        // Also via the merge path when both are past the base and the server's
        // tail begins with local's.
        assert_eq!(
            resolve(&local, &server, Some(&sha(BASE))),
            AppendResolution::ServerSupersedesLocal
        );
    }

    #[test]
    fn resolve_is_unresolved_when_the_shared_history_was_edited() {
        // The server changed a line INSIDE the base (not an append) and also
        // appended; local appended. No cut hashes to the shadow -> stash floor.
        let mut edited = BASE.to_vec();
        let pos = edited.windows(8).position(|w| w == b"step one").unwrap();
        edited[pos..pos + 8].copy_from_slice(b"STEP ONE");
        let server = [&edited[..], b"- **09:11** trinity\n"].concat();
        let local = [BASE, b"- **09:10** link\n"].concat();
        assert_eq!(
            resolve(&local, &server, Some(&sha(BASE))),
            AppendResolution::Unresolved
        );
    }

    #[test]
    fn resolve_is_unresolved_without_a_shadow_for_two_appenders() {
        let local = [BASE, b"- **09:10** link\n"].concat();
        let server = [BASE, b"- **09:11** trinity\n"].concat();
        assert_eq!(resolve(&local, &server, None), AppendResolution::Unresolved);
    }

    #[test]
    fn resolve_is_unresolved_for_a_mid_line_stale_head() {
        let local = [BASE, b"- **09:10** link did a thing\n"].concat();
        let cut = &local[..local.len() - 6];
        assert_eq!(
            resolve(&local, cut, Some(&sha(b"nope"))),
            AppendResolution::Unresolved
        );
    }

    #[test]
    fn resolve_is_unresolved_for_unrelated_content() {
        assert_eq!(
            resolve(b"alpha\nbeta\n", b"gamma\ndelta\n", Some(&sha(b"alpha\n"))),
            AppendResolution::Unresolved
        );
    }
}
