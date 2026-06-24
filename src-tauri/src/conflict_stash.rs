//! Conflict stash — v0.3 LWW (last-writer-wins) divergence handler.
//!
//! When the daemon detects a local revision that diverges from the server's
//! canonical (server-authoritative `lsn`), this module decides — per the
//! configured `ConflictPolicy` + a path-based class A/B/C/D heuristic —
//! whether to preserve the losing revision as a sibling file
//! `<stem>.conflict-from-<device_id>-<lsn>.md`.
//!
//! This is DISTINCT from the orphan `.conflict-<UTC>.md` files written by a
//! retired older sync tool. Those were unscoped + timestamp-based; ours are
//! device+lsn tagged and only written when policy says to stash.
//!
//! See mandate §3 ("Conflict model — EXPLICIT CHOICE") + §4.1.
//!
//! NOTE on classification: the v0.3 plan deliberately keeps
//! `ConflictClassifier::classify` as a simple path-based heuristic. True
//! content-based A (identical / trivial-fm) vs B (canonical superset) vs C
//! (unique-content) detection is the materializer's job and lands in a
//! later phase. For now `classify` returns D for known-sensitive paths and
//! C for everything else. A and B are reserved variants the materializer
//! will populate once content-diff is wired.

use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use tempfile::NamedTempFile;
use thiserror::Error;

/// Parsed result of a `<stem>.conflict-from-<device>-<lsn>[-<n>].md` filename.
///
/// Pure data — no I/O. Used by the tray "Conflicts unresolved" list-dialog
/// surface (and any v0.3.1 webview resolver) to display the original note
/// name, originating device, and stash lsn without re-parsing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedConflict {
    /// The original note stem (filename without `.md`), e.g. `note` for
    /// `note.conflict-from-morpheus-1234.md`.
    pub stem: String,
    /// Originating device id. May contain hyphens (e.g. `cody-trinity`).
    pub device: String,
    /// Server lsn at which the divergent revision was stashed.
    pub lsn: u64,
}

/// Parse a `*.conflict-from-*.md` filename into its components.
///
/// Format: `<stem>.conflict-from-<device>-<lsn>[-<n>].md`
///   * `<stem>` is the original note name (may contain dots/dashes, but
///     cannot contain `.conflict-from-` since that marker is what we split on).
///   * `<device>` may contain hyphens (e.g. `cody-trinity`) — only the
///     trailing `-<lsn>` portion is structurally parsed.
///   * `<lsn>` is an unsigned integer.
///   * Optional collision suffix `-<n>` (n=2,3,...) appended by
///     `ConflictStash::resolve_collision`. When present we still parse the
///     ORIGINAL lsn (the n is ignored).
///
/// Returns `None` if the filename doesn't match the structural pattern.
pub fn parse_conflict_filename(name: &str) -> Option<ParsedConflict> {
    // Must be a .md file
    let inner = name.strip_suffix(".md")?;
    // Must contain the marker. Split on FIRST occurrence — the stem cannot
    // itself contain `.conflict-from-` (legal Obsidian filenames don't).
    let (stem, after) = inner.split_once(".conflict-from-")?;
    if stem.is_empty() || after.is_empty() {
        return None;
    }
    // `after` is `<device>-<lsn>` or `<device>-<lsn>-<n>`. We need the
    // trailing numeric component(s). Try the easy case first: rsplitn(2, '-')
    // gives us (last_token, prefix). If last_token parses as u64, AND prefix
    // contains at least one more hyphen (so device is non-empty), check
    // whether prefix itself ends in `-<another_u64>` — in that case the
    // last_token is the collision suffix and the REAL lsn is one hop in.
    let (prefix, last) = after.rsplit_once('-')?;
    let last_num: u64 = last.parse().ok()?;
    if prefix.is_empty() {
        return None;
    }
    // Detect collision suffix: prefix = `<device>-<lsn>` where <lsn> is a u64.
    if let Some((device, lsn_str)) = prefix.rsplit_once('-') {
        if !device.is_empty() {
            if let Ok(lsn_inner) = lsn_str.parse::<u64>() {
                // Collision form: `<device>-<lsn>-<n>`. Use inner lsn, ignore n.
                return Some(ParsedConflict {
                    stem: stem.to_string(),
                    device: device.to_string(),
                    lsn: lsn_inner,
                });
            }
        }
    }
    // No collision suffix: prefix IS the device, last IS the lsn.
    Some(ParsedConflict {
        stem: stem.to_string(),
        device: prefix.to_string(),
        lsn: last_num,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConflictPolicy {
    ServerWins,
    NewerWins,
    Manual,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConflictClass {
    /// identical-content or trivial-frontmatter-only diff — auto-resolve, no stash
    A,
    /// canonical is strict superset — pull canonical, no stash
    B,
    /// unique content on both sides — stash per policy
    C,
    /// security-sensitive path — ALWAYS stash regardless of policy
    D,
}

pub struct ConflictClassifier;

impl ConflictClassifier {
    /// Path-based heuristic. Returns D for known security-sensitive paths,
    /// C otherwise. A/B are content-derived and assigned by the materializer
    /// when content-diff lands (post-v0.3).
    pub fn classify(path: &str) -> ConflictClass {
        let norm: String = path.replace('\\', "/");
        let lower = norm.to_ascii_lowercase();

        // 1. Any `Credentials.md` file anywhere
        // 2. Anything under a top-level or nested `Credentials/` dir
        // 3. `02_Projects/Protocols/Infrastructure*`
        // 4. `02_Projects/Protocols/Bootstrap*`
        if is_credentials_md(&norm)
            || lower.contains("/credentials/")
            || lower.starts_with("credentials/")
            || matches_protocol_prefix(&norm, "Infrastructure")
            || matches_protocol_prefix(&norm, "Bootstrap")
        {
            return ConflictClass::D;
        }
        ConflictClass::C
    }
}

fn is_credentials_md(path: &str) -> bool {
    // Match any segment that is exactly `Credentials.md`
    path.rsplit('/').next() == Some("Credentials.md")
}

fn matches_protocol_prefix(path: &str, prefix: &str) -> bool {
    // Find "02_Projects/Protocols/" + prefix at any path position.
    let needle = format!("02_Projects/Protocols/{prefix}");
    path.contains(&needle)
}

#[derive(Debug, Error)]
pub enum StashError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("invalid path: {0}")]
    InvalidPath(String),
}

#[derive(Debug, Clone)]
pub struct StashDecision {
    pub stash: bool,
    pub stash_path: Option<PathBuf>,
    pub reason: String,
}

pub struct ConflictStash {
    vault_root: PathBuf,
    policy: ConflictPolicy,
}

impl ConflictStash {
    pub fn new(vault_root: PathBuf, policy: ConflictPolicy) -> Self {
        Self { vault_root, policy }
    }

    /// Decide whether the divergent local revision should be stashed.
    ///
    /// `local_lsn` is `None` when we have a local file but no recorded server
    /// `lsn` for it (e.g. file existed before daemon ever saw it). In that
    /// case `NewerWins` treats local as "older" (we don't know its lsn) and
    /// stashes it.
    pub fn decide(
        &self,
        path: &str,
        local_lsn: Option<u64>,
        server_lsn: u64,
        device_id: &str,
    ) -> StashDecision {
        let class = ConflictClassifier::classify(path);

        // Class D — always stash regardless of policy
        if class == ConflictClass::D {
            let stash_path = self.compute_stash_path(path, device_id, local_lsn.unwrap_or(0));
            return StashDecision {
                stash: true,
                stash_path: Some(stash_path),
                reason: format!(
                    "class D security-sensitive path always stashed (policy={:?})",
                    self.policy
                ),
            };
        }

        match self.policy {
            // D3 (S511, TKT-2dc9a17e): there is NO safe no-stash cell for
            // divergent content. The old `ServerWins => stash:false` arm encoded
            // the silent overwrite the operator forbids (I-83 NEVER-SILENT-
            // OVERWRITE). ServerWins now behaves like always-stash: a Class-C
            // divergence is preserved, not silently reverted. (The live write()
            // path is now divergence-driven and no longer routes through this
            // policy at all; this keeps the standalone API safe too.)
            ConflictPolicy::ServerWins => {
                let stash_path = self.compute_stash_path(path, device_id, local_lsn.unwrap_or(0));
                StashDecision {
                    stash: true,
                    stash_path: Some(stash_path),
                    reason: "divergent local revision stashed before overwrite (no silent server-wins; S511 D3)"
                        .to_string(),
                }
            }
            ConflictPolicy::Manual => {
                let stash_path = self.compute_stash_path(path, device_id, local_lsn.unwrap_or(0));
                StashDecision {
                    stash: true,
                    stash_path: Some(stash_path),
                    reason: "manual policy: always stash divergent revision".to_string(),
                }
            }
            ConflictPolicy::NewerWins => {
                let local_is_older = match local_lsn {
                    Some(l) => l < server_lsn,
                    None => true, // unknown local lsn = treat as older
                };
                if local_is_older {
                    let stash_path =
                        self.compute_stash_path(path, device_id, local_lsn.unwrap_or(0));
                    StashDecision {
                        stash: true,
                        stash_path: Some(stash_path),
                        reason: format!(
                            "newer-wins: local_lsn={:?} < server_lsn={server_lsn}, stashing local",
                            local_lsn
                        ),
                    }
                } else {
                    StashDecision {
                        stash: false,
                        stash_path: None,
                        reason: format!(
                            "newer-wins: local_lsn={:?} >= server_lsn={server_lsn}, local wins",
                            local_lsn
                        ),
                    }
                }
            }
        }
    }

    /// Compute the canonical stash sibling path.
    /// `<vault_root>/<original_dir>/<original_stem>.conflict-from-<device_id>-<lsn>.md`
    fn compute_stash_path(&self, original_path: &str, device_id: &str, lsn: u64) -> PathBuf {
        let rel = original_path.replace('\\', "/");
        let rel_path = Path::new(&rel);

        let parent = rel_path.parent().unwrap_or(Path::new(""));
        let stem = rel_path
            .file_stem()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_else(|| "untitled".to_string());

        let filename = format!("{stem}.conflict-from-{device_id}-{lsn}.md");
        self.vault_root.join(parent).join(filename)
    }

    /// D5 (S511): public accessor for the would-be stash path (pre-collision).
    /// The materializer needs this to record the stash in the echo_guard BEFORE
    /// `write_stash` is called, so the file_watcher recognizes the conflict-copy
    /// write as an echo and never enqueues it as a push. The actual on-disk name
    /// may gain a `-2`/`-3` collision suffix; for echo-keying the base name is
    /// what the watcher first observes, and a suffixed copy is independently
    /// excluded by the `*.conflict-from-*.md` name filter.
    pub fn compute_stash_path_public(
        &self,
        original_path: &str,
        device_id: &str,
        lsn: u64,
    ) -> PathBuf {
        self.compute_stash_path(original_path, device_id, lsn)
    }

    /// Write the stashed (losing) revision to disk atomically.
    ///
    /// - Atomic tmp+rename (tempfile::NamedTempFile + persist).
    /// - Never overwrite an existing stash file — append `-2`, `-3`, ...
    /// - Path safety: parent dir's canonical path must stay within vault_root.
    pub fn write_stash(
        &self,
        original_path: &str,
        local_content: &[u8],
        device_id: &str,
        local_lsn: u64,
    ) -> Result<PathBuf, StashError> {
        let base_path = self.compute_stash_path(original_path, device_id, local_lsn);

        let parent = base_path
            .parent()
            .ok_or_else(|| StashError::InvalidPath(format!("{base_path:?} has no parent")))?;
        fs::create_dir_all(parent)?;

        // Path-safety: ensure the resolved parent stays within vault_root.
        let canonical_root = self
            .vault_root
            .canonicalize()
            .unwrap_or_else(|_| self.vault_root.clone());
        let canonical_parent = parent
            .canonicalize()
            .unwrap_or_else(|_| parent.to_path_buf());
        if !canonical_parent.starts_with(&canonical_root) {
            return Err(StashError::InvalidPath(format!(
                "resolved stash path {canonical_parent:?} escapes vault_root {canonical_root:?}"
            )));
        }

        // Idempotency (S514, TKT-d1a41f94): if a `*.conflict-from-*` sibling for
        // this original already holds byte-identical content, return it instead
        // of writing another. Without this, the same divergence recurring every
        // reconcile cycle appended -2/-3/... endlessly (the 209-file storm).
        if let Some(existing) = self.find_identical_stash(&base_path, local_content) {
            return Ok(existing);
        }

        // Collision-avoid: if base_path exists, try -2, -3, ...
        let final_path = self.resolve_collision(&base_path);

        let mut tmp = NamedTempFile::new_in(parent)?;
        tmp.write_all(local_content)?;
        tmp.flush()?;
        tmp.persist(&final_path)
            .map_err(|e| StashError::Io(e.error))?;

        Ok(final_path)
    }

    /// Idempotency helper (S514, TKT-d1a41f94): return an existing
    /// `<orig>.conflict-from-*` sibling whose bytes equal `content`, if any.
    /// Keys off the original-note prefix (the part before `.conflict-from-`) so
    /// it matches regardless of device/lsn/collision-suffix — the same losing
    /// content is preserved exactly once, not re-stashed every reconcile pass.
    fn find_identical_stash(&self, base_path: &Path, content: &[u8]) -> Option<PathBuf> {
        let parent = base_path.parent()?;
        let base_name = base_path.file_name()?.to_str()?;
        let orig_prefix = base_name.split(".conflict-from-").next()?;
        let needle = format!("{orig_prefix}.conflict-from-");
        for entry in fs::read_dir(parent).ok()?.flatten() {
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if !name.starts_with(&needle) || !name.ends_with(".md") {
                continue;
            }
            if let Ok(bytes) = fs::read(entry.path()) {
                if bytes == content {
                    return Some(entry.path());
                }
            }
        }
        None
    }

    fn resolve_collision(&self, base: &Path) -> PathBuf {
        if !base.exists() {
            return base.to_path_buf();
        }
        let parent = base.parent().unwrap_or(Path::new(""));
        let stem = base
            .file_stem()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default();
        let ext = base
            .extension()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_else(|| "md".to_string());
        for n in 2u32..u32::MAX {
            let candidate = parent.join(format!("{stem}-{n}.{ext}"));
            if !candidate.exists() {
                return candidate;
            }
        }
        // Fallback (functionally unreachable)
        base.to_path_buf()
    }

    /// Recursively scan vault_root for files matching `*.conflict-from-*.md`.
    /// Used by tray surface to report unresolved-stash count.
    pub fn unresolved_count(&self) -> Result<usize, StashError> {
        let mut count = 0usize;
        walk_dir(&self.vault_root, &mut |entry_path| {
            if let Some(name) = entry_path.file_name().and_then(|n| n.to_str()) {
                if name.contains(".conflict-from-") && name.ends_with(".md") {
                    count += 1;
                }
            }
        })?;
        Ok(count)
    }
}

/// Hand-rolled recursive walker — std-only (no walkdir/glob dep added).
fn walk_dir(root: &Path, visit: &mut dyn FnMut(&Path)) -> Result<(), io::Error> {
    if !root.exists() {
        return Ok(());
    }
    let mut stack: Vec<PathBuf> = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let rd = match fs::read_dir(&dir) {
            Ok(r) => r,
            Err(e) if e.kind() == io::ErrorKind::PermissionDenied => continue,
            Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e),
        };
        for entry in rd.flatten() {
            let p = entry.path();
            let ft = match entry.file_type() {
                Ok(t) => t,
                Err(_) => continue,
            };
            if ft.is_dir() {
                stack.push(p);
            } else if ft.is_file() {
                visit(&p);
            }
            // symlinks ignored — we don't follow them
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    // ----- classifier tests -----

    #[test]
    fn classify_credentials_is_class_d() {
        assert_eq!(
            ConflictClassifier::classify("Credentials.md"),
            ConflictClass::D
        );
        assert_eq!(
            ConflictClassifier::classify("02_Projects/Foo/Credentials.md"),
            ConflictClass::D
        );
        assert_eq!(
            ConflictClassifier::classify("_rapport/people/X/Credentials.md"),
            ConflictClass::D
        );
        // Credentials/ folder treatment
        assert_eq!(
            ConflictClassifier::classify("Credentials/api-keys.md"),
            ConflictClass::D
        );
        assert_eq!(
            ConflictClassifier::classify("02_Projects/Foo/Credentials/sub.md"),
            ConflictClass::D
        );
    }

    #[test]
    fn classify_infrastructure_is_class_d() {
        assert_eq!(
            ConflictClassifier::classify("02_Projects/Protocols/Infrastructure Reference.md"),
            ConflictClass::D
        );
        assert_eq!(
            ConflictClassifier::classify("02_Projects/Protocols/Infrastructure/inner.md"),
            ConflictClass::D
        );
    }

    #[test]
    fn classify_bootstrap_is_class_d() {
        assert_eq!(
            ConflictClassifier::classify("02_Projects/Protocols/Bootstrap Config.md"),
            ConflictClass::D
        );
        assert_eq!(
            ConflictClassifier::classify("02_Projects/Protocols/Bootstrap/policy.md"),
            ConflictClass::D
        );
    }

    #[test]
    fn classify_normal_is_class_c() {
        assert_eq!(
            ConflictClassifier::classify("02_Projects/Foo/normal.md"),
            ConflictClass::C
        );
        assert_eq!(
            ConflictClassifier::classify("Inbox/2026-05-27 note.md"),
            ConflictClass::C
        );
        assert_eq!(
            ConflictClassifier::classify("02_Projects/Protocols/Some Other Protocol.md"),
            ConflictClass::C
        );
    }

    // ----- decide() tests -----

    fn cs(policy: ConflictPolicy) -> ConflictStash {
        // Use a relative dummy path; decide() doesn't touch the filesystem.
        ConflictStash::new(PathBuf::from("vault"), policy)
    }

    /// D3 (S511, TKT-2dc9a17e): ServerWins no longer has a silent-overwrite
    /// cell. A Class-C divergence under ServerWins now STASHES (preserves the
    /// loser) rather than dropping it. This flips the pre-S511 assertion.
    #[test]
    fn decide_server_wins_class_c_now_stashes() {
        let s = cs(ConflictPolicy::ServerWins);
        let d = s.decide("02_Projects/Foo/normal.md", Some(10), 20, "morpheus");
        assert!(
            d.stash,
            "S511: server-wins+C MUST now stash (no silent overwrite); reason={}",
            d.reason
        );
        let sp = d
            .stash_path
            .expect("class C under server-wins must produce a stash_path now");
        let name = sp.file_name().unwrap().to_string_lossy().into_owned();
        assert_eq!(name, "normal.conflict-from-morpheus-10.md");
        assert!(
            !d.reason.contains("silently overwritten"),
            "the misleading silent-overwrite reason text must be gone"
        );
    }

    #[test]
    fn decide_server_wins_class_d_stashes() {
        let s = cs(ConflictPolicy::ServerWins);
        let d = s.decide("Credentials.md", Some(10), 20, "morpheus");
        assert!(d.stash, "server-wins+D MUST stash; reason={}", d.reason);
        let sp = d.stash_path.expect("class D must produce stash_path");
        let name = sp.file_name().unwrap().to_string_lossy().into_owned();
        assert_eq!(name, "Credentials.conflict-from-morpheus-10.md");
    }

    #[test]
    fn decide_manual_always_stashes() {
        let s = cs(ConflictPolicy::Manual);
        let d = s.decide("02_Projects/Foo/normal.md", Some(10), 20, "trinity");
        assert!(
            d.stash,
            "manual policy MUST always stash; reason={}",
            d.reason
        );
        let sp = d.stash_path.expect("manual must produce stash_path");
        let name = sp.file_name().unwrap().to_string_lossy().into_owned();
        assert_eq!(name, "normal.conflict-from-trinity-10.md");
    }

    #[test]
    fn decide_newer_wins_older_loses_stashes() {
        let s = cs(ConflictPolicy::NewerWins);
        // local=5 < server=10 → local is older → stash local
        let d = s.decide("02_Projects/Foo/normal.md", Some(5), 10, "morpheus");
        assert!(
            d.stash,
            "newer-wins older-local MUST stash; reason={}",
            d.reason
        );
        let sp = d.stash_path.unwrap();
        let name = sp.file_name().unwrap().to_string_lossy().into_owned();
        assert_eq!(name, "normal.conflict-from-morpheus-5.md");
    }

    #[test]
    fn decide_newer_wins_already_newest_no_stash() {
        let s = cs(ConflictPolicy::NewerWins);
        // local=20 >= server=10 → local wins → no stash
        let d = s.decide("02_Projects/Foo/normal.md", Some(20), 10, "morpheus");
        assert!(
            !d.stash,
            "newer-wins newest-local must NOT stash; reason={}",
            d.reason
        );
        assert!(d.stash_path.is_none());
    }

    #[test]
    fn decide_newer_wins_unknown_local_lsn_stashes() {
        let s = cs(ConflictPolicy::NewerWins);
        // local lsn unknown → treat as older → stash
        let d = s.decide("02_Projects/Foo/normal.md", None, 10, "morpheus");
        assert!(d.stash, "unknown local_lsn must stash under newer-wins");
    }

    // ----- write_stash() tests -----

    #[test]
    fn write_stash_creates_sibling_with_device_lsn_in_filename() {
        let tmp = tempdir().unwrap();
        let stash = ConflictStash::new(tmp.path().to_path_buf(), ConflictPolicy::Manual);
        // Pretend original lives at <root>/02_Projects/Foo/note.md
        fs::create_dir_all(tmp.path().join("02_Projects/Foo")).unwrap();
        fs::write(
            tmp.path().join("02_Projects/Foo/note.md"),
            b"original canonical",
        )
        .unwrap();

        let result = stash
            .write_stash(
                "02_Projects/Foo/note.md",
                b"local divergent content",
                "morpheus",
                1234,
            )
            .unwrap();

        let expected = tmp
            .path()
            .join("02_Projects/Foo/note.conflict-from-morpheus-1234.md");
        assert_eq!(result, expected);
        assert!(result.exists(), "stash file should exist at {result:?}");
        let body = fs::read(&result).unwrap();
        assert_eq!(body, b"local divergent content");
        // Original untouched
        let orig = fs::read(tmp.path().join("02_Projects/Foo/note.md")).unwrap();
        assert_eq!(orig, b"original canonical");
    }

    #[test]
    fn write_stash_collision_appends_suffix() {
        let tmp = tempdir().unwrap();
        let stash = ConflictStash::new(tmp.path().to_path_buf(), ConflictPolicy::Manual);
        fs::create_dir_all(tmp.path().join("notes")).unwrap();

        // First write — gets the base name
        let p1 = stash
            .write_stash("notes/x.md", b"v1", "morpheus", 1)
            .unwrap();
        assert!(p1
            .file_name()
            .unwrap()
            .to_string_lossy()
            .ends_with("conflict-from-morpheus-1.md"));

        // Second write with SAME device+lsn — collision, should get -2 suffix
        let p2 = stash
            .write_stash("notes/x.md", b"v2", "morpheus", 1)
            .unwrap();
        assert_ne!(p1, p2);
        assert!(p2
            .file_name()
            .unwrap()
            .to_string_lossy()
            .contains("conflict-from-morpheus-1-2.md"));

        // Third — should get -3
        let p3 = stash
            .write_stash("notes/x.md", b"v3", "morpheus", 1)
            .unwrap();
        assert!(p3
            .file_name()
            .unwrap()
            .to_string_lossy()
            .contains("conflict-from-morpheus-1-3.md"));

        // Confirm all three coexist
        assert!(p1.exists() && p2.exists() && p3.exists());
        assert_eq!(fs::read(&p1).unwrap(), b"v1");
        assert_eq!(fs::read(&p2).unwrap(), b"v2");
        assert_eq!(fs::read(&p3).unwrap(), b"v3");
    }

    #[test]
    fn write_stash_idempotent_for_identical_content() {
        // S514 (TKT-d1a41f94): the same losing content recurring every reconcile
        // cycle must be stashed ONCE, not piled into -2/-3/... (the 209-file storm).
        let tmp = tempdir().unwrap();
        let stash = ConflictStash::new(tmp.path().to_path_buf(), ConflictPolicy::Manual);
        fs::create_dir_all(tmp.path().join("notes")).unwrap();

        let p1 = stash.write_stash("notes/x.md", b"loser", "morpheus", 1).unwrap();
        // Same content, different device+lsn → reuse existing stash, no new file.
        let p2 = stash.write_stash("notes/x.md", b"loser", "trinity", 99).unwrap();
        assert_eq!(p1, p2, "identical content must reuse the existing stash");

        let n = fs::read_dir(tmp.path().join("notes"))
            .unwrap()
            .flatten()
            .filter(|e| e.file_name().to_string_lossy().contains(".conflict-from-"))
            .count();
        assert_eq!(n, 1, "exactly one stash file for identical content");

        // Different content still gets its own file (genuine divergence preserved).
        let p3 = stash
            .write_stash("notes/x.md", b"different", "trinity", 100)
            .unwrap();
        assert_ne!(p1, p3);
    }

    #[test]
    fn write_stash_atomic_via_tmp_rename() {
        // We can't directly observe "atomic" without injecting a fault; instead
        // verify that no `*.tmp*` files linger in the target dir after a
        // successful write — tempfile::persist removes the tmp inode.
        let tmp = tempdir().unwrap();
        let stash = ConflictStash::new(tmp.path().to_path_buf(), ConflictPolicy::Manual);
        fs::create_dir_all(tmp.path().join("d")).unwrap();
        let _ = stash
            .write_stash("d/n.md", b"payload", "trinity", 42)
            .unwrap();

        // Scan target dir — only the final stash file, no leftover tmp.
        let mut entries: Vec<String> = fs::read_dir(tmp.path().join("d"))
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        entries.sort();
        // Should be exactly one file, the persisted stash.
        assert_eq!(entries.len(), 1, "expected 1 file, got: {entries:?}");
        assert_eq!(entries[0], "n.conflict-from-trinity-42.md");
    }

    #[test]
    fn write_stash_rejects_symlink_escape() {
        // Best-effort: on platforms without symlink permission this becomes a
        // no-op; the test still passes the non-escape path.
        let outer = tempdir().unwrap();
        let vault = outer.path().join("vault");
        let outside = outer.path().join("outside");
        fs::create_dir_all(&vault).unwrap();
        fs::create_dir_all(&outside).unwrap();

        // Try to create a symlink INSIDE the vault that points outside. On
        // Windows this requires Developer Mode or admin — skip if it errors.
        let link_dir = vault.join("escape");
        #[cfg(unix)]
        let symlink_ok = std::os::unix::fs::symlink(&outside, &link_dir).is_ok();
        #[cfg(windows)]
        let symlink_ok = std::os::windows::fs::symlink_dir(&outside, &link_dir).is_ok();

        let stash = ConflictStash::new(vault.clone(), ConflictPolicy::Manual);

        if symlink_ok {
            // Attempting to stash UNDER the symlinked dir should be rejected
            // because canonical parent resolves outside vault_root.
            let r = stash.write_stash("escape/note.md", b"x", "dev", 1);
            assert!(
                matches!(r, Err(StashError::InvalidPath(_))),
                "expected InvalidPath escape rejection, got: {r:?}"
            );
        } else {
            // Symlink unavailable — at minimum confirm a normal write inside
            // vault still works (regression check that path-safety isn't
            // over-rejecting legitimate writes).
            let r = stash.write_stash("safe/note.md", b"x", "dev", 1);
            assert!(r.is_ok(), "non-escape write should succeed, got: {r:?}");
        }
    }

    #[test]
    fn unresolved_count_scans_recursive() {
        let tmp = tempdir().unwrap();
        let stash = ConflictStash::new(tmp.path().to_path_buf(), ConflictPolicy::Manual);

        // 0 to start
        assert_eq!(stash.unresolved_count().unwrap(), 0);

        // Plant some stash files at various depths + some non-matches
        fs::create_dir_all(tmp.path().join("a/b/c")).unwrap();
        fs::write(tmp.path().join("note.conflict-from-morpheus-1.md"), b"x").unwrap();
        fs::write(tmp.path().join("a/note.conflict-from-trinity-2.md"), b"x").unwrap();
        fs::write(
            tmp.path().join("a/b/c/deep.conflict-from-switch-99.md"),
            b"x",
        )
        .unwrap();
        // non-matches:
        fs::write(tmp.path().join("a/regular.md"), b"x").unwrap();
        fs::write(tmp.path().join("a/old.conflict-2024-01-01.md"), b"x").unwrap(); // legacy retired-tool style
        fs::write(tmp.path().join("a/b/foo.conflict-from-bar-1.txt"), b"x").unwrap(); // wrong ext

        assert_eq!(stash.unresolved_count().unwrap(), 3);
    }

    // ----- parse_conflict_filename tests -----

    #[test]
    fn parses_simple_name() {
        let p = parse_conflict_filename("note.conflict-from-morpheus-1234.md").unwrap();
        assert_eq!(p.stem, "note");
        assert_eq!(p.device, "morpheus");
        assert_eq!(p.lsn, 1234);
    }

    #[test]
    fn parses_collision_suffix() {
        // `<stem>.conflict-from-<device>-<lsn>-<n>.md` — n is ignored, lsn returned.
        let p = parse_conflict_filename("note.conflict-from-morpheus-1234-2.md").unwrap();
        assert_eq!(p.stem, "note");
        assert_eq!(p.device, "morpheus");
        assert_eq!(p.lsn, 1234);

        let p3 = parse_conflict_filename("note.conflict-from-morpheus-1234-3.md").unwrap();
        assert_eq!(p3.lsn, 1234);
    }

    #[test]
    fn parses_device_with_hyphens() {
        let p = parse_conflict_filename("note.conflict-from-cody-trinity-1234.md").unwrap();
        assert_eq!(p.stem, "note");
        assert_eq!(p.device, "cody-trinity");
        assert_eq!(p.lsn, 1234);
    }

    #[test]
    fn parses_device_with_hyphens_and_collision() {
        let p = parse_conflict_filename("note.conflict-from-cody-trinity-1234-2.md").unwrap();
        assert_eq!(p.stem, "note");
        assert_eq!(p.device, "cody-trinity");
        assert_eq!(p.lsn, 1234);
    }

    #[test]
    fn rejects_normal_filename() {
        assert!(parse_conflict_filename("note.md").is_none());
        assert!(parse_conflict_filename("regular.md").is_none());
    }

    #[test]
    fn rejects_malformed() {
        // Single token after marker — no lsn separator
        assert!(parse_conflict_filename("note.conflict-from-X.md").is_none());
        // Empty stem
        assert!(parse_conflict_filename(".conflict-from-X-1.md").is_none());
        // No trailing number
        assert!(parse_conflict_filename("note.conflict-from-X-Y.md").is_none());
    }

    #[test]
    fn rejects_wrong_ext() {
        assert!(parse_conflict_filename("note.conflict-from-X-1.txt").is_none());
        assert!(parse_conflict_filename("note.conflict-from-X-1").is_none());
    }

    #[test]
    fn rejects_legacy_orphan_conflict() {
        // Retired tool format: `note.conflict-2024-01-01.md` — no `from-` marker.
        assert!(parse_conflict_filename("note.conflict-2024-01-01.md").is_none());
    }

    #[test]
    fn unresolved_count_handles_missing_root() {
        let stash = ConflictStash::new(
            PathBuf::from("/definitely/does/not/exist/vault-xyz-123"),
            ConflictPolicy::Manual,
        );
        // Missing root must not error
        assert_eq!(stash.unresolved_count().unwrap(), 0);
    }
}
