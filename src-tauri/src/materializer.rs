//! Materializer — server→client downloads via atomic tmp+rename.
//!
//! v0.3 (Wave 3): promotes Live mode from a `NotYetImplemented` error to a
//! real atomic-write into the live vault tree.  Shadow mode now writes to
//! the per-host **workspace runtime** dir (`<workspace_root>/.lattice-runtime/
//! <slug>/shadow/<path>`) — NOT into the vault — per mandate §1 row 13.
//!
//! Every successful write is followed by an `IntegrityChecker::verify(...)`
//! pass (mandate §1 row 5 + T8).  Mismatches yield an
//! `MaterializeOutcome::IntegrityFailed`; the bad write is *not* deleted so
//! the owner can inspect.
//!
//! Before overwriting a live-mode target the materializer applies a
//! pull-side idempotency + conflict-stash hook mirroring `push_client`'s
//! frontmatter-normalized SHA check (mandate §1 row 4 + R16, §3 conflict
//! model).  Class-D paths (Credentials.md etc.) always stash regardless of
//! policy.
//!
//! Shadow mode preserves the v0.2 behavior with one path change: state
//! lives in the workspace runtime dir, not in `<vault>/.lattice-sync/`.

use crate::api_client::NotePayload;
use crate::conflict_stash::{
    ConflictClass, ConflictClassifier, ConflictPolicy, ConflictStash, StashError,
};
use crate::integrity_check::{
    ByteLevelResult, ExpectedIntegrity, IntegrityChecker, IntegrityError, IntegrityResult,
};
use crate::rasp_fence::{classify_path, PathClassification};
use crate::scope::is_safe_path;
use crate::tray_state::SharedTrayState;
use sha2::{Digest, Sha256};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use tempfile::NamedTempFile;
use thiserror::Error;
use tracing::{info, warn};

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaterializerMode {
    Shadow,
    Live,
    Disabled,
}

impl MaterializerMode {
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Self {
        match s {
            "live" => Self::Live,
            "disabled" => Self::Disabled,
            _ => Self::Shadow,
        }
    }
}

#[derive(Debug, Error)]
pub enum MaterializerError {
    #[error("path traversal rejected: {0}")]
    PathTraversal(String),
    #[error("RASP substrate path refused (read-only by daemon): {0}")]
    SubstrateRefuse(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("sha mismatch: expected {expected}, got {actual}")]
    ShaMismatch { expected: String, actual: String },
    #[error("conflict-stash error: {0}")]
    Stash(#[from] StashError),
    #[error("integrity-check error: {0}")]
    Integrity(String),
}

impl From<IntegrityError> for MaterializerError {
    fn from(e: IntegrityError) -> Self {
        MaterializerError::Integrity(format!("{e:?}"))
    }
}

/// Why a write was skipped (no I/O happened beyond classification).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SkipReason {
    /// RASP substrate fence refused the path. `rule` is the static label of
    /// the matching rule, e.g. `"00_VAULT.md"` or `"_rapport/people/"`.
    SubstrateRefused { rule: &'static str },
    /// Local content already matches the server's canonical SHA after
    /// frontmatter normalization. No write needed.
    IdenticalToLocal,
    /// Materializer is configured in `Disabled` mode.
    DisabledMode,
}

/// Outcome of a single `write()` call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MaterializeOutcome {
    /// File was written to disk at `path` (atomic tmp+rename succeeded AND
    /// post-write integrity check passed).
    Wrote { path: PathBuf },
    /// No write happened.  See `SkipReason`.
    Skipped(SkipReason),
    /// A local divergent revision was stashed before the canonical was
    /// written.  `stash_path` is the sibling stash file.  The canonical was
    /// also written to its final path.
    Stashed { stash_path: PathBuf },
    /// Write completed but the post-write integrity check failed.  The file
    /// is intentionally NOT deleted — the owner can inspect both the bad
    /// write and the resulting ticket.
    IntegrityFailed {
        path: PathBuf,
        expected_sha: String,
        actual_sha: String,
    },
}

// ---------------------------------------------------------------------------
// Materializer
// ---------------------------------------------------------------------------

/// Materializer config — opt-in feature flags.  Defaults align with
/// mandate §1 (integrity ON, ServerWins conflict default per §3).
#[derive(Debug, Clone)]
pub struct MaterializerConfig {
    /// Post-write integrity verification (mandate §1 row 5 + T8). Default ON.
    pub enable_integrity_check: bool,
    /// Pull-side conflict policy. Default `ServerWins` — silently overwrite
    /// non-class-D local divergent revisions.  Class D always stashes.
    pub conflict_policy: ConflictPolicy,
    /// Frontmatter fields stripped before computing the normalized
    /// idempotency SHA (mandate §1 row 10 / R16). Mirrors
    /// `PushClientConfig::strip_frontmatter_fields_for_diff` so push and
    /// pull use the same canonical-hash basis.
    pub strip_frontmatter_fields_for_diff: Vec<String>,
    /// Device identifier used when writing stash files
    /// (`<stem>.conflict-from-<device_id>-<lsn>.md`).
    pub device_id: String,
}

impl Default for MaterializerConfig {
    fn default() -> Self {
        Self {
            enable_integrity_check: true,
            conflict_policy: ConflictPolicy::ServerWins,
            strip_frontmatter_fields_for_diff: vec!["updated".into()],
            device_id: "unknown-device".to_string(),
        }
    }
}

/// v0.3.0 materializer.  Holds the runtime fields needed to write notes
/// into either live or shadow mode:
///
/// Note (S477): the daemon treats `vaults_root` as the actual watch +
/// materialize root. Incoming payloads carry the vault folder as the
/// first segment of their relative path, so live mode writes to
/// `<vaults_root>/<rel>` directly, allowing multiple vaults to coexist
/// under one `vaults_root`. The v0.2.0 `vault_name` field is gone as of
/// v0.3.7 — see config.rs for the legacy-tolerant load path.
///
/// - `workspace_root` — the per-host daemon state dir
///   (e.g. `%LocalAppData%\Nexus`). Shadow-mode writes go under
///   `<workspace_root>/.lattice-runtime/<subscriber_slug>/shadow/<path>`,
///   never into the vault tree.
/// - `subscriber_slug` — used to namespace the runtime dir (one host can
///   pair multiple subscribers without colliding).
/// - `config` — feature flags (integrity, conflict policy, ...).
pub struct Materializer {
    vaults_root: PathBuf,
    shadow_subdir: String,
    mode: MaterializerMode,
    workspace_root: PathBuf,
    subscriber_slug: String,
    config: MaterializerConfig,
    /// Optional tray telemetry sink (mandate §9 AG13 — Wave 4 wire-up). If
    /// set, integrity-check failures bump `tray.integrity_failures`, and
    /// `refresh_conflict_count_into_tray()` may be called by a background
    /// timer to refresh `tray.conflict_unresolved`.
    tray_state: Option<SharedTrayState>,
    /// Epoch-millis of the last `refresh_conflict_count_into_tray()` call.
    /// Wrapped in `Arc<AtomicI64>` so a cloned materializer (used by the
    /// 60s background refresh task in `lib::spawn_sse_consumer`) shares
    /// the debounce window with the primary write-path instance.
    last_conflict_refresh_ms: Arc<AtomicI64>,
}

impl Clone for Materializer {
    fn clone(&self) -> Self {
        Self {
            vaults_root: self.vaults_root.clone(),
            shadow_subdir: self.shadow_subdir.clone(),
            mode: self.mode,
            workspace_root: self.workspace_root.clone(),
            subscriber_slug: self.subscriber_slug.clone(),
            config: self.config.clone(),
            tray_state: self.tray_state.clone(),
            last_conflict_refresh_ms: self.last_conflict_refresh_ms.clone(),
        }
    }
}

/// Debounce window for `refresh_conflict_count_into_tray()` — skip a refresh
/// if the last one ran less than this many milliseconds ago.
const CONFLICT_REFRESH_DEBOUNCE_MS: i64 = 30_000;

impl Materializer {
    /// New v0.3 constructor.  See `MaterializerConfig::default` for the
    /// recommended defaults (integrity ON, ServerWins).
    pub fn new(
        vaults_root: PathBuf,
        shadow_path: Option<String>,
        mode: MaterializerMode,
        workspace_root: PathBuf,
        subscriber_slug: String,
        config: MaterializerConfig,
    ) -> Self {
        let shadow_subdir = shadow_path.unwrap_or_else(|| "shadow/".to_string());
        Self {
            vaults_root,
            shadow_subdir,
            mode,
            workspace_root,
            subscriber_slug,
            config,
            tray_state: None,
            last_conflict_refresh_ms: Arc::new(AtomicI64::new(0)),
        }
    }

    /// Builder-style: attach a `SharedTrayState`. After this, integrity-check
    /// failures bump `tray.integrity_failures`, and the caller may invoke
    /// `refresh_conflict_count_into_tray()` on a timer to refresh
    /// `tray.conflict_unresolved`. Backwards-compatible — pre-Wave-4
    /// constructors keep working with `tray_state = None`.
    pub fn with_tray_state(mut self, state: SharedTrayState) -> Self {
        self.tray_state = Some(state);
        self
    }

    /// Scan the live-vault tree for `*.conflict-from-*.md` stash siblings and
    /// publish the count to the tray (if a tray is attached). Debounced:
    /// returns early without scanning if a refresh ran less than
    /// `CONFLICT_REFRESH_DEBOUNCE_MS` ago. Caller-driven (mandate §4.1 — kept
    /// off the `write()` hot path).
    ///
    /// No-op when `tray_state` is None.
    pub fn refresh_conflict_count_into_tray(&self) {
        let Some(tray) = self.tray_state.as_ref() else {
            return;
        };

        // Debounce — skip if we ran recently.
        let now_ms = chrono::Utc::now().timestamp_millis();
        let last = self.last_conflict_refresh_ms.load(Ordering::Relaxed);
        if last != 0 && now_ms.saturating_sub(last) < CONFLICT_REFRESH_DEBOUNCE_MS {
            return;
        }
        self.last_conflict_refresh_ms
            .store(now_ms, Ordering::Relaxed);

        // Stash scan-root mirrors `write()`: live-mode uses the configured
        // vaults_root (which can contain multiple vaults — all scanned),
        // shadow-mode uses the shadow tree.
        let scan_root = match self.mode {
            MaterializerMode::Live => self.vaults_root.clone(),
            _ => self.shadow_root(),
        };
        let stasher = ConflictStash::new(scan_root, self.config.conflict_policy);
        match stasher.unresolved_count() {
            Ok(n) => {
                if let Ok(mut w) = tray.write() {
                    w.set_conflict_unresolved(n);
                }
            }
            Err(e) => {
                warn!(error = ?e, "refresh_conflict_count_into_tray: stash scan failed");
            }
        }
    }

    /// `<workspace_root>/.lattice-runtime/<subscriber_slug>/shadow/` — the
    /// per-subscriber shadow tree (mandate §1 row 13: daemon state OUT of
    /// vault).
    fn shadow_root(&self) -> PathBuf {
        // Allow callers to override the trailing folder name via
        // shadow_subdir, but anchor it under <workspace>/.lattice-runtime/<slug>.
        self.workspace_root
            .join(".lattice-runtime")
            .join(&self.subscriber_slug)
            .join(&self.shadow_subdir)
    }

    /// Target path for a payload, depending on mode. `rel` is expected to
    /// be relative to `vaults_root` (i.e. the vault folder is its first
    /// segment), so live mode joins straight onto `vaults_root` and
    /// shadow mode onto the per-subscriber shadow tree.
    fn target_for(&self, rel: &str) -> PathBuf {
        match self.mode {
            MaterializerMode::Live => self.vaults_root.join(rel),
            MaterializerMode::Shadow => self.shadow_root().join(rel),
            // Disabled: target unused, but provide a sensible placeholder.
            MaterializerMode::Disabled => self.shadow_root().join(rel),
        }
    }

    /// Convenience: live-vault path for a relative file (used by callers
    /// who need to compute the live target before write — e.g. tests).
    pub fn live_path_for(&self, rel: &str) -> PathBuf {
        self.vaults_root.join(rel)
    }

    /// Public main entry — writes a payload into vault (live) or shadow tree.
    pub fn write(&self, payload: &NotePayload) -> Result<MaterializeOutcome, MaterializerError> {
        // 1. Mode gate.
        if matches!(self.mode, MaterializerMode::Disabled) {
            info!(
                "materializer_mode=disabled; skipping write for {}",
                payload.path
            );
            return Ok(MaterializeOutcome::Skipped(SkipReason::DisabledMode));
        }

        // 2. Path safety.
        if !is_safe_path(&payload.path) {
            return Err(MaterializerError::PathTraversal(payload.path.clone()));
        }

        // 3. RASP substrate fence — refuse with rule label.
        if let PathClassification::Substrate { rule } = classify_path(&payload.path) {
            warn!(
                rule = rule,
                path = %payload.path,
                "materializer refusing substrate path"
            );
            return Ok(MaterializeOutcome::Skipped(SkipReason::SubstrateRefused {
                rule,
            }));
        }

        // 4. Resolve canonical content + content_sha.
        //    BUG 2 (S486): the server's `sha256` is computed over the EXACT
        //    bytes it returns as `enriched_body` (server cache_writer hashes
        //    enriched_body; on a cache miss enriched_body == body_raw == the
        //    sha256 basis). Materialize those bytes verbatim so the strict
        //    integrity check passes by construction AND the note stays
        //    byte-faithful — re-serializing frontmatter through serde_yaml uses
        //    a different YAML rendering + `\n\n` separator and could never
        //    reproduce the original bytes, which failed integrity on every
        //    fronted note. Fall back to reconstruction only for older servers
        //    that don't send the field.
        let content = match &payload.enriched_body {
            Some(raw) => raw.clone(),
            None => serialize_with_frontmatter(payload),
        };
        let content_bytes = content.as_bytes();
        let actual_sha = hex::encode(Sha256::digest(content_bytes));

        // 5. Compute target.
        let target = self.target_for(&payload.path);

        // 6. Idempotency + conflict-stash (only meaningful if target exists).
        let mut stash_path: Option<PathBuf> = None;
        if target.exists() {
            match self.compare_local_to_canonical(&target, content_bytes)? {
                LocalCompare::Identical => {
                    info!(
                        path = %payload.path,
                        "materializer skip — local already identical to canonical"
                    );
                    return Ok(MaterializeOutcome::Skipped(SkipReason::IdenticalToLocal));
                }
                LocalCompare::Diverges { local_bytes } => {
                    // Consult conflict-stash policy.
                    let class = ConflictClassifier::classify(&payload.path);
                    let should_stash = class == ConflictClass::D
                        || self.config.conflict_policy != ConflictPolicy::ServerWins;
                    if should_stash {
                        // Stash root: live-mode uses vaults_root (the watch
                        // root) so stashes sit next to the canonical file
                        // regardless of which vault under vaults_root holds
                        // it; shadow-mode uses shadow_root.
                        let stash_root = match self.mode {
                            MaterializerMode::Live => self.vaults_root.clone(),
                            _ => self.shadow_root(),
                        };
                        let stasher = ConflictStash::new(stash_root, self.config.conflict_policy);
                        let written = stasher.write_stash(
                            &payload.path,
                            &local_bytes,
                            &self.config.device_id,
                            0, // local_lsn unknown — use 0 placeholder in filename
                        )?;
                        warn!(
                            path = %payload.path,
                            stash = %written.display(),
                            class = ?class,
                            policy = ?self.config.conflict_policy,
                            "materializer stashed local divergent revision"
                        );
                        stash_path = Some(written);
                    } else {
                        info!(
                            path = %payload.path,
                            class = ?class,
                            "materializer server-wins: overwriting non-class-D divergent local"
                        );
                    }
                }
            }
        }

        // 7. Path-safety + parent dir.
        let canonical_root = self.canonical_root_for_mode();
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent)?;
            let canonical_parent = parent
                .canonicalize()
                .unwrap_or_else(|_| parent.to_path_buf());
            if !canonical_parent.starts_with(&canonical_root) {
                return Err(MaterializerError::PathTraversal(payload.path.clone()));
            }
        }

        // 8. Atomic tmp+rename. Tmp file must be on the same FS as target,
        //    so we anchor it at target.parent() (same dir).
        let parent = target
            .parent()
            .expect("target has parent after create_dir_all");
        let mut tmp = NamedTempFile::new_in(parent)?;
        tmp.write_all(content_bytes)?;
        tmp.flush()?;
        tmp.persist(&target).map_err(|e| e.error)?;

        // 9. Post-write integrity check.
        if self.config.enable_integrity_check {
            let expected = ExpectedIntegrity {
                sha256_hex: payload.sha256.clone(),
                size_bytes: content_bytes.len() as u64,
            };
            let checker = IntegrityChecker::new(false);
            let result: IntegrityResult = checker.verify(&target, &expected)?;
            if !result.is_ok() {
                let actual_hex = match &result.byte_level {
                    ByteLevelResult::ShaMismatch { actual_prefix, .. } => actual_prefix.clone(),
                    _ => actual_sha.clone(),
                };
                warn!(
                    expected = %payload.sha256,
                    actual = %actual_sha,
                    path = %target.display(),
                    "materializer integrity check FAILED — file kept on disk for inspection"
                );
                // Wave 4: surface the failure to the tray dashboard.
                if let Some(tray) = &self.tray_state {
                    if let Ok(mut w) = tray.write() {
                        w.inc_integrity_failures();
                    }
                }
                return Ok(MaterializeOutcome::IntegrityFailed {
                    path: target,
                    expected_sha: payload.sha256.clone(),
                    actual_sha: actual_hex,
                });
            }
        } else if actual_sha != payload.sha256 {
            // Legacy soft SHA check — log only, don't fail.
            warn!(
                expected = %payload.sha256,
                actual = %actual_sha,
                path = %payload.path,
                "materializer SHA mismatch (integrity-check disabled) — file written but does not match server hash"
            );
        }

        if let Some(stash) = stash_path {
            Ok(MaterializeOutcome::Stashed { stash_path: stash })
        } else {
            Ok(MaterializeOutcome::Wrote { path: target })
        }
    }

    /// Pick the canonical-root directory for the active mode.  Used by the
    /// path-traversal sanity check.
    fn canonical_root_for_mode(&self) -> PathBuf {
        let raw_root = match self.mode {
            MaterializerMode::Live => self.vaults_root.clone(),
            _ => self.shadow_root(),
        };
        // Ensure the root exists so canonicalize() succeeds.
        let _ = fs::create_dir_all(&raw_root);
        raw_root.canonicalize().unwrap_or(raw_root)
    }

    /// Compare the on-disk file to incoming canonical bytes, normalized for
    /// frontmatter idempotency (R16).
    fn compare_local_to_canonical(
        &self,
        target: &Path,
        canonical_bytes: &[u8],
    ) -> Result<LocalCompare, MaterializerError> {
        let local_bytes = fs::read(target)?;
        let local_norm =
            normalize_for_diff(&local_bytes, &self.config.strip_frontmatter_fields_for_diff);
        let canonical_norm = normalize_for_diff(
            canonical_bytes,
            &self.config.strip_frontmatter_fields_for_diff,
        );
        let local_hash = hex::encode(Sha256::digest(&local_norm));
        let canonical_hash = hex::encode(Sha256::digest(&canonical_norm));
        if local_hash == canonical_hash {
            Ok(LocalCompare::Identical)
        } else {
            Ok(LocalCompare::Diverges { local_bytes })
        }
    }

    /// Soft-delete preserves the v0.2 contract (move to `<name>.deleted-<ts>`).
    /// In live mode it operates on the vault tree; in shadow mode on the
    /// runtime tree.  Disabled mode no-ops.
    pub fn soft_delete(&self, path: &str) -> Result<(), MaterializerError> {
        if !is_safe_path(path) {
            return Err(MaterializerError::PathTraversal(path.into()));
        }
        if let PathClassification::Substrate { rule: _ } = classify_path(path) {
            return Err(MaterializerError::SubstrateRefuse(path.into()));
        }
        if matches!(self.mode, MaterializerMode::Disabled) {
            info!("materializer disabled; skipping delete for {}", path);
            return Ok(());
        }
        let target = self.target_for(path);
        if !target.exists() {
            info!("soft_delete: nothing to delete at {}", path);
            return Ok(());
        }
        let ts = chrono::Utc::now().format("%Y%m%dT%H%M%SZ");
        let renamed = target.with_file_name(format!(
            "{}.deleted-{ts}",
            target.file_name().unwrap().to_string_lossy()
        ));
        fs::rename(&target, &renamed)?;
        info!(from = %target.display(), to = %renamed.display(), "soft_delete done");
        Ok(())
    }
}

enum LocalCompare {
    Identical,
    Diverges { local_bytes: Vec<u8> },
}

// ---------------------------------------------------------------------------
// Frontmatter normalization (mirrors push_client::normalize_for_diff exactly)
// ---------------------------------------------------------------------------

fn normalize_for_diff(content: &[u8], strip_fields: &[String]) -> Vec<u8> {
    let s = match std::str::from_utf8(content) {
        Ok(s) => s,
        Err(_) => return content.to_vec(),
    };
    if !s.starts_with("---\n") && !s.starts_with("---\r\n") {
        return content.to_vec();
    }
    let body_start = match find_frontmatter_end(s) {
        Some(i) => i,
        None => return content.to_vec(),
    };
    let after_open = if s.starts_with("---\r\n") { 5 } else { 4 };
    let fm_block = &s[after_open..body_start.fm_inner_end];
    let body = &s[body_start.body_start..];

    let stripped_fm = strip_yaml_fields(fm_block, strip_fields);
    let mut out = String::with_capacity(content.len());
    out.push_str("---\n");
    out.push_str(&stripped_fm);
    if !stripped_fm.ends_with('\n') {
        out.push('\n');
    }
    out.push_str("---\n");
    out.push_str(body);
    out.into_bytes()
}

struct FrontmatterEnd {
    fm_inner_end: usize,
    body_start: usize,
}

fn find_frontmatter_end(s: &str) -> Option<FrontmatterEnd> {
    let after_open = if s.starts_with("---\r\n") { 5 } else { 4 };
    let mut cursor = after_open;
    let bytes = s.as_bytes();
    while cursor < bytes.len() {
        let line_end = match bytes[cursor..].iter().position(|&b| b == b'\n') {
            Some(p) => cursor + p,
            None => return None,
        };
        let mut line = &s[cursor..line_end];
        if line.ends_with('\r') {
            line = &line[..line.len() - 1];
        }
        if line == "---" {
            return Some(FrontmatterEnd {
                fm_inner_end: cursor,
                body_start: line_end + 1,
            });
        }
        cursor = line_end + 1;
    }
    None
}

fn strip_yaml_fields(fm_block: &str, fields: &[String]) -> String {
    if fields.is_empty() {
        return fm_block.to_string();
    }
    let mut out = String::with_capacity(fm_block.len());
    let mut skipping = false;
    for line in fm_block.lines() {
        let is_top_level = !line.starts_with(' ') && !line.starts_with('\t');
        if is_top_level {
            let key = line.split_once(':').map(|(k, _)| k.trim()).unwrap_or("");
            if fields.iter().any(|f| f == key) {
                skipping = true;
                continue;
            }
            skipping = false;
        }
        if skipping {
            continue;
        }
        out.push_str(line);
        out.push('\n');
    }
    out
}

fn serialize_with_frontmatter(payload: &NotePayload) -> String {
    // S476 v0.3.5: omit the `---\n...\n---\n` block when frontmatter is
    // missing or empty. Before this fix every shadow file got a useless
    // `---\n{}\n---\n` preamble (the server returns `frontmatter: {}` for
    // notes without YAML front-matter, and serde_yaml renders that as
    // `{}\n` -> wrapped in fences it became junk-frontmatter noise at the
    // top of every file).
    let is_empty = match &payload.frontmatter {
        serde_json::Value::Null => true,
        serde_json::Value::Object(m) => m.is_empty(),
        _ => false,
    };
    if is_empty {
        return payload.body.clone();
    }
    let fm_yaml = serde_yaml::to_string(&payload.frontmatter).unwrap_or_default();
    format!("---\n{fm_yaml}---\n\n{}", payload.body)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    const VAULT: &str = "Mainframe";
    const SLUG: &str = "subscriber-test";

    /// (vaults_root_tmp, workspace_tmp, materializer)
    fn mk(mode: MaterializerMode, cfg: MaterializerConfig) -> (TempDir, TempDir, Materializer) {
        let vaults_tmp = TempDir::new().unwrap();
        let ws_tmp = TempDir::new().unwrap();
        std::fs::create_dir_all(vaults_tmp.path().join(VAULT)).unwrap();
        let m = Materializer::new(
            vaults_tmp.path().to_path_buf(),
            Some("shadow/".to_string()),
            mode,
            ws_tmp.path().to_path_buf(),
            SLUG.to_string(),
            cfg,
        );
        (vaults_tmp, ws_tmp, m)
    }

    fn default_cfg() -> MaterializerConfig {
        MaterializerConfig {
            device_id: "morpheus".into(),
            ..Default::default()
        }
    }

    fn sha256_hex(s: &str) -> String {
        hex::encode(Sha256::digest(s.as_bytes()))
    }

    /// Test helper: builds a NotePayload with the path namespaced under
    /// the test VAULT folder. Per S477, NotePayload.path is relative to
    /// `vaults_root`, so the vault folder is the first segment. Callers
    /// keep passing intra-vault relatives ("01_Inbox/foo.md") and this
    /// helper prepends VAULT exactly once. Paths starting with "../"
    /// (traversal-attempt tests) are passed through unmodified so the
    /// path-safety check sees the raw escape attempt.
    fn payload(path: &str, body: &str) -> NotePayload {
        let prefixed = if path.starts_with("../") || path.starts_with(&format!("{VAULT}/")) {
            path.to_string()
        } else {
            format!("{VAULT}/{path}")
        };
        let fm = serde_json::json!({"title": "Test", "tags": ["a", "b"]});
        let fm_yaml = serde_yaml::to_string(&fm).unwrap_or_default();
        let serialized = format!("---\n{fm_yaml}---\n\n{body}");
        NotePayload {
            path: prefixed,
            frontmatter: fm,
            body: body.into(),
            sha256: sha256_hex(&serialized),
            modified: "2026-05-27T00:00:00Z".into(),
            file_mtime: None,
            // Mirror the real server: enriched_body is the exact content the
            // sha256 is computed over (S486).
            enriched_body: Some(serialized),
        }
    }

    fn payload_with_bad_sha(path: &str, body: &str) -> NotePayload {
        let mut p = payload(path, body);
        p.sha256 = "0".repeat(64);
        p
    }

    // ---- BUG 2 (S486): pull-path integrity over enriched_body -------------

    /// Real-server shape: `sha256` is computed over the EXACT bytes the server
    /// returns as `enriched_body` (server cache_writer hashes enriched_body;
    /// cache-miss path sets enriched_body == body_raw == the sha256 basis).
    /// The daemon must materialize `enriched_body` verbatim — NOT a serde_yaml
    /// reconstruction, which uses different frontmatter serialization + a
    /// `\n\n` separator and could never byte-match, so the strict integrity
    /// check failed on every fronted note (S485 e2e blocker). With the field
    /// present and integrity ENABLED, the write must succeed and reproduce the
    /// server bytes exactly.
    #[test]
    fn pull_path_materializes_server_enriched_body_verbatim_integrity_ok() {
        let mut cfg = default_cfg();
        cfg.enable_integrity_check = true;
        let (vaults, _ws, m) = mk(MaterializerMode::Live, cfg);

        // The server's faithful bytes use a SINGLE-newline frontmatter
        // separator; serde_yaml reconstruction emits `---\n{yaml}---\n\n{body}`
        // (double newline) — guaranteeing the two differ.
        let original = "---\ntitle: Real\n---\nSingle-newline body, server-faithful.\n";
        let p = NotePayload {
            path: format!("{VAULT}/01_Inbox/faithful.md"),
            frontmatter: serde_json::json!({"title": "Real"}),
            body: "Single-newline body, server-faithful.\n".into(),
            sha256: sha256_hex(original),
            modified: "2026-05-31T00:00:00Z".into(),
            file_mtime: None,
            enriched_body: Some(original.to_string()),
        };

        // Guard: if reconstruction happened to equal the server bytes this
        // test wouldn't exercise the bug.
        assert_ne!(
            serialize_with_frontmatter(&p),
            original,
            "reconstruction must differ from server bytes for this regression to be meaningful"
        );

        let out = m.write(&p).unwrap();
        assert!(
            matches!(out, MaterializeOutcome::Wrote { .. }),
            "strict integrity must PASS by materializing enriched_body verbatim, got {out:?}"
        );
        let on_disk =
            std::fs::read_to_string(vaults.path().join(VAULT).join("01_Inbox/faithful.md"))
                .unwrap();
        assert_eq!(
            on_disk, original,
            "must write the server's exact hashed bytes (byte-faithful)"
        );
    }

    /// Back-compat: an older server that omits `enriched_body` (field defaults
    /// to None) still materializes via frontmatter reconstruction.
    #[test]
    fn pull_path_falls_back_to_reconstruction_when_enriched_body_absent() {
        let (vaults, _ws, m) = mk(MaterializerMode::Live, default_cfg());
        let fm = serde_json::json!({"title": "Legacy"});
        let fm_yaml = serde_yaml::to_string(&fm).unwrap();
        let serialized = format!("---\n{fm_yaml}---\n\nlegacy body");
        let p = NotePayload {
            path: format!("{VAULT}/01_Inbox/legacy.md"),
            frontmatter: fm,
            body: "legacy body".into(),
            sha256: sha256_hex(&serialized),
            modified: "2026-05-31T00:00:00Z".into(),
            file_mtime: None,
            enriched_body: None,
        };
        let out = m.write(&p).unwrap();
        assert!(
            matches!(out, MaterializeOutcome::Wrote { .. }),
            "got {out:?}"
        );
        let on_disk =
            std::fs::read_to_string(vaults.path().join(VAULT).join("01_Inbox/legacy.md")).unwrap();
        assert_eq!(on_disk, serialized);
    }

    // ---- mode-routing -----------------------------------------------------

    #[test]
    fn live_mode_writes_to_vault_path_not_shadow() {
        let (vaults, ws, m) = mk(MaterializerMode::Live, default_cfg());
        let out = m.write(&payload("01_Inbox/foo.md", "hello")).unwrap();
        let expected = vaults.path().join(VAULT).join("01_Inbox/foo.md");
        match out {
            MaterializeOutcome::Wrote { path } => assert_eq!(path, expected),
            other => panic!("expected Wrote, got {other:?}"),
        }
        assert!(expected.exists());
        let shadow_target = ws
            .path()
            .join(".lattice-runtime")
            .join(SLUG)
            .join("shadow/01_Inbox/foo.md");
        assert!(!shadow_target.exists());
    }

    #[test]
    fn shadow_mode_writes_to_workspace_runtime_not_vault() {
        let (vaults, ws, m) = mk(MaterializerMode::Shadow, default_cfg());
        let out = m.write(&payload("01_Inbox/foo.md", "hello")).unwrap();
        // S477: payload paths now include the vault folder as the first
        // segment, so the shadow tree mirrors that prefix.
        let expected = ws
            .path()
            .join(".lattice-runtime")
            .join(SLUG)
            .join("shadow")
            .join(VAULT)
            .join("01_Inbox/foo.md");
        match out {
            MaterializeOutcome::Wrote { path } => assert_eq!(path, expected),
            other => panic!("expected Wrote, got {other:?}"),
        }
        assert!(expected.exists());
        let vault_target = vaults.path().join(VAULT).join("01_Inbox/foo.md");
        assert!(!vault_target.exists());
    }

    #[test]
    fn shadow_mode_path_outside_vault() {
        let (vaults, _ws, m) = mk(MaterializerMode::Shadow, default_cfg());
        m.write(&payload("01_Inbox/foo.md", "x")).unwrap();
        let shadow_root_canonical = m.shadow_root().canonicalize().unwrap();
        let vault_root_canonical = vaults.path().join(VAULT).canonicalize().unwrap();
        assert!(
            !shadow_root_canonical.starts_with(&vault_root_canonical),
            "shadow={} should not be inside vault={}",
            shadow_root_canonical.display(),
            vault_root_canonical.display()
        );
    }

    #[test]
    fn disabled_mode_writes_nothing_returns_skipped() {
        let (vaults, ws, m) = mk(MaterializerMode::Disabled, default_cfg());
        let out = m.write(&payload("01_Inbox/foo.md", "x")).unwrap();
        assert_eq!(out, MaterializeOutcome::Skipped(SkipReason::DisabledMode));
        assert!(!vaults.path().join(VAULT).join("01_Inbox/foo.md").exists());
        assert!(!ws
            .path()
            .join(".lattice-runtime")
            .join(SLUG)
            .join("shadow/01_Inbox/foo.md")
            .exists());
    }

    // ---- substrate refusal -----------------------------------------------

    #[test]
    fn substrate_refusal_returns_skipped_with_rule_label() {
        let (_v, _w, m) = mk(MaterializerMode::Live, default_cfg());
        let out = m.write(&payload("00_VAULT.md", "x")).unwrap();
        match out {
            MaterializeOutcome::Skipped(SkipReason::SubstrateRefused { rule }) => {
                assert_eq!(rule, "00_VAULT.md");
            }
            other => panic!("expected SubstrateRefused, got {other:?}"),
        }
    }

    #[test]
    fn substrate_refusal_protocols_returns_rule() {
        let (_v, _w, m) = mk(MaterializerMode::Live, default_cfg());
        let out = m
            .write(&payload("02_Projects/Protocols/foo.md", "x"))
            .unwrap();
        match out {
            MaterializeOutcome::Skipped(SkipReason::SubstrateRefused { rule }) => {
                assert_eq!(rule, "02_Projects/Protocols/");
            }
            other => panic!("expected SubstrateRefused, got {other:?}"),
        }
    }

    // ---- idempotency + frontmatter normalization -------------------------

    #[test]
    fn identical_local_skips_no_write() {
        let (vaults, _ws, m) = mk(MaterializerMode::Live, default_cfg());
        let p = payload("01_Inbox/foo.md", "hello");
        m.write(&p).unwrap();
        let target = vaults.path().join(VAULT).join("01_Inbox/foo.md");
        let mtime_before = std::fs::metadata(&target).unwrap().modified().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(50));
        let out = m.write(&p).unwrap();
        assert_eq!(
            out,
            MaterializeOutcome::Skipped(SkipReason::IdenticalToLocal)
        );
        let mtime_after = std::fs::metadata(&target).unwrap().modified().unwrap();
        assert_eq!(
            mtime_before, mtime_after,
            "mtime should not advance on skip"
        );
    }

    #[test]
    fn frontmatter_only_rewrite_treated_as_identical() {
        let (vaults, _ws, m) = mk(MaterializerMode::Live, default_cfg());
        let target = vaults.path().join(VAULT).join("01_Inbox/n.md");
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();
        // Local file: same key set as canonical except `updated: 2026-05-01`.
        // To make this test order-stable across serde_yaml versions, build
        // the local pre-existing file using the SAME serializer the
        // materializer will use for the canonical payload (just with the
        // older `updated` value). The normalize_for_diff strip will remove
        // `updated:` from both before hashing, leaving identical content.
        let local_fm =
            serde_json::json!({"title": "Test", "updated": "2026-05-01", "tags": ["a", "b"]});
        let local_fm_yaml = serde_yaml::to_string(&local_fm).unwrap();
        let local_content = format!("---\n{local_fm_yaml}---\n\nbody-text");
        std::fs::write(&target, local_content).unwrap();

        // Canonical from server: same fields, newer `updated:`.
        let fm = serde_json::json!({"title": "Test", "updated": "2026-05-27", "tags": ["a", "b"]});
        let fm_yaml = serde_yaml::to_string(&fm).unwrap();
        let serialized = format!("---\n{fm_yaml}---\n\nbody-text");
        let p = NotePayload {
            // S477: payload path is vaults-root-relative (vault folder first).
            path: format!("{VAULT}/01_Inbox/n.md"),
            frontmatter: fm,
            body: "body-text".into(),
            sha256: sha256_hex(&serialized),
            modified: "2026-05-27T00:00:00Z".into(),
            file_mtime: None,
            enriched_body: Some(serialized),
        };
        let out = m.write(&p).unwrap();
        assert_eq!(
            out,
            MaterializeOutcome::Skipped(SkipReason::IdenticalToLocal)
        );
    }

    // ---- conflict stash ---------------------------------------------------

    #[test]
    fn stash_written_for_conflict_class_d() {
        let (vaults, _ws, m) = mk(MaterializerMode::Live, default_cfg());
        let target = vaults.path().join(VAULT).join("02_Projects/Credentials.md");
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();
        std::fs::write(&target, "local-secrets-version").unwrap();
        let p = payload("02_Projects/Credentials.md", "server-canonical-secrets");
        let out = m.write(&p).unwrap();
        match out {
            MaterializeOutcome::Stashed { stash_path } => {
                assert!(stash_path.exists(), "stash file should exist");
                let stash_content = std::fs::read_to_string(&stash_path).unwrap();
                assert_eq!(stash_content, "local-secrets-version");
                let cur = std::fs::read_to_string(&target).unwrap();
                assert!(cur.contains("server-canonical-secrets"));
            }
            other => panic!("expected Stashed, got {other:?}"),
        }
    }

    #[test]
    fn stash_not_written_for_class_c_under_server_wins() {
        let (vaults, _ws, m) = mk(MaterializerMode::Live, default_cfg());
        let target = vaults.path().join(VAULT).join("02_Projects/Foo/normal.md");
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();
        std::fs::write(&target, "old-local").unwrap();
        let p = payload("02_Projects/Foo/normal.md", "server-canonical");
        let out = m.write(&p).unwrap();
        match out {
            MaterializeOutcome::Wrote { path } => {
                assert_eq!(path, target);
                let cur = std::fs::read_to_string(&target).unwrap();
                assert!(cur.contains("server-canonical"));
            }
            other => panic!("expected Wrote (no stash), got {other:?}"),
        }
        let dir = target.parent().unwrap();
        for entry in std::fs::read_dir(dir).unwrap().flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            assert!(
                !name.contains(".conflict-from-"),
                "unexpected stash file: {name}"
            );
        }
    }

    // ---- integrity check --------------------------------------------------

    #[test]
    fn integrity_check_failure_yields_outcome() {
        let (vaults, _ws, m) = mk(MaterializerMode::Live, default_cfg());
        let p = payload_with_bad_sha("01_Inbox/foo.md", "hello");
        let out = m.write(&p).unwrap();
        match out {
            MaterializeOutcome::IntegrityFailed {
                path, expected_sha, ..
            } => {
                assert_eq!(path, vaults.path().join(VAULT).join("01_Inbox/foo.md"));
                assert_eq!(expected_sha, p.sha256);
                assert!(path.exists(), "integrity-failed file must remain on disk");
            }
            other => panic!("expected IntegrityFailed, got {other:?}"),
        }
    }

    #[test]
    fn integrity_check_disabled_writes_anyway() {
        let cfg = MaterializerConfig {
            enable_integrity_check: false,
            ..default_cfg()
        };
        let (vaults, _ws, m) = mk(MaterializerMode::Live, cfg);
        let p = payload_with_bad_sha("01_Inbox/foo.md", "hello");
        let out = m.write(&p).unwrap();
        match out {
            MaterializeOutcome::Wrote { path } => {
                assert_eq!(path, vaults.path().join(VAULT).join("01_Inbox/foo.md"));
                assert!(path.exists());
            }
            other => panic!("expected Wrote (integrity disabled), got {other:?}"),
        }
    }

    // ---- atomic + parent dirs --------------------------------------------

    #[test]
    fn parent_dirs_created() {
        let (vaults, _ws, m) = mk(MaterializerMode::Live, default_cfg());
        let out = m.write(&payload("a/b/c/d.md", "deep")).unwrap();
        let expected = vaults.path().join(VAULT).join("a/b/c/d.md");
        assert_eq!(
            out,
            MaterializeOutcome::Wrote {
                path: expected.clone()
            }
        );
        assert!(expected.exists());
    }

    #[test]
    fn existing_atomic_persist_preserved_no_tmp_leftover() {
        let (_vaults, _ws, m) = mk(MaterializerMode::Live, default_cfg());
        m.write(&payload("01_Inbox/foo.md", "hello")).unwrap();
        // S477: live_path_for takes a vaults-root-relative path (vault
        // folder first segment), matching the materializer's contract.
        let dir = m.live_path_for(&format!("{VAULT}/01_Inbox/foo.md"));
        let parent = dir.parent().unwrap();
        let entries: Vec<String> = std::fs::read_dir(parent)
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            entries.len(),
            1,
            "expected only the final file, got: {entries:?}"
        );
        assert_eq!(entries[0], "foo.md");
    }

    #[test]
    fn atomic_write_no_partial_visible() {
        let (vaults, _ws, m) = mk(MaterializerMode::Live, default_cfg());
        let target = vaults.path().join(VAULT).join("loop/x.md");
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();
        for i in 0..100 {
            let body = format!("iteration-{i}");
            let p = payload("loop/x.md", &body);
            m.write(&p).unwrap();
            let read = std::fs::read_to_string(&target).unwrap();
            assert!(
                read.starts_with("---\n"),
                "iter {i} non-atomic? got: {read:?}"
            );
            assert!(
                read.contains("iteration-"),
                "iter {i} missing body: got: {read:?}"
            );
        }
    }

    // ---- preserved v0.2 surface ------------------------------------------

    #[test]
    fn write_creates_file_with_frontmatter() {
        let (_v, ws, m) = mk(MaterializerMode::Shadow, default_cfg());
        m.write(&payload("01_Inbox/foo.md", "hello")).unwrap();
        // S477: shadow tree mirrors the vault-folder-first path shape.
        let written = std::fs::read_to_string(
            ws.path()
                .join(".lattice-runtime")
                .join(SLUG)
                .join("shadow")
                .join(VAULT)
                .join("01_Inbox/foo.md"),
        )
        .unwrap();
        assert!(written.contains("title: Test"));
        assert!(written.contains("hello"));
    }

    #[test]
    fn write_rejects_path_traversal() {
        let (_v, _w, m) = mk(MaterializerMode::Shadow, default_cfg());
        let np = payload("../escape.md", "x");
        assert!(matches!(
            m.write(&np),
            Err(MaterializerError::PathTraversal(_))
        ));
    }

    #[test]
    fn write_allows_trailing_dots_in_name() {
        // S490 regression: a note whose title ends in `...` (three ASCII dots)
        // contains `..` as a substring but is NOT a traversal — it must
        // materialize, not get black-holed.
        let (_v, _w, m) = mk(MaterializerMode::Shadow, default_cfg());
        let out = m.write(&payload("01_Notes/Anysa says....md", "x"));
        assert!(
            out.is_ok(),
            "trailing-dots name should write, got {:?}",
            out
        );
    }

    #[test]
    fn delete_renames_to_deleted_ts() {
        let (_v, ws, m) = mk(MaterializerMode::Shadow, default_cfg());
        m.write(&payload("01_Inbox/foo.md", "x")).unwrap();
        // S477: soft_delete takes vaults-root-relative paths, same as write().
        m.soft_delete(&format!("{VAULT}/01_Inbox/foo.md")).unwrap();
        let shadow_dir = ws
            .path()
            .join(".lattice-runtime")
            .join(SLUG)
            .join("shadow")
            .join(VAULT)
            .join("01_Inbox");
        assert!(!shadow_dir.join("foo.md").exists());
        let entries: Vec<_> = std::fs::read_dir(&shadow_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.file_name()
                    .to_string_lossy()
                    .starts_with("foo.md.deleted-")
            })
            .collect();
        assert_eq!(entries.len(), 1, "expected one .deleted-* file");
    }

    #[test]
    fn delete_nothing_to_delete_is_not_error() {
        let (_v, _w, m) = mk(MaterializerMode::Shadow, default_cfg());
        assert!(m.soft_delete("01_Inbox/never-existed.md").is_ok());
    }

    #[test]
    fn delete_refuses_rasp_substrate_path() {
        let (_v, _w, m) = mk(MaterializerMode::Shadow, default_cfg());
        assert!(matches!(
            m.soft_delete("00_VAULT.md"),
            Err(MaterializerError::SubstrateRefuse(_))
        ));
    }

    // ---- Wave 4: tray-state wire-up ---------------------------------------

    fn make_shared_tray() -> SharedTrayState {
        std::sync::Arc::new(std::sync::RwLock::new(crate::tray_state::TrayState::new(
            "sub".into(),
            "https://x".into(),
            PathBuf::from("/v"),
        )))
    }

    #[test]
    fn integrity_failure_increments_tray_counter() {
        let (_vaults, _ws, m_base) = mk(MaterializerMode::Live, default_cfg());
        let tray = make_shared_tray();
        let m = m_base.with_tray_state(tray.clone());
        let p = payload_with_bad_sha("01_Inbox/foo.md", "hello");
        let out = m.write(&p).unwrap();
        assert!(matches!(out, MaterializeOutcome::IntegrityFailed { .. }));
        let s = tray.read().unwrap();
        assert_eq!(s.integrity_failures, 1);
    }

    #[test]
    fn successful_write_does_not_increment_integrity_failures() {
        let (_vaults, _ws, m_base) = mk(MaterializerMode::Live, default_cfg());
        let tray = make_shared_tray();
        let m = m_base.with_tray_state(tray.clone());
        let out = m.write(&payload("01_Inbox/foo.md", "hello")).unwrap();
        assert!(matches!(out, MaterializeOutcome::Wrote { .. }));
        let s = tray.read().unwrap();
        assert_eq!(s.integrity_failures, 0);
    }

    #[test]
    fn with_tray_state_is_idempotent_back_compat() {
        // Materializer without tray_state must still work — no panic, no
        // surprises, integrity-failed outcome still surfaced via return value.
        let (_vaults, _ws, m) = mk(MaterializerMode::Live, default_cfg());
        let p = payload_with_bad_sha("01_Inbox/foo.md", "hello");
        let out = m.write(&p).unwrap();
        assert!(matches!(out, MaterializeOutcome::IntegrityFailed { .. }));
        // And a successful write also fine.
        let ok = m.write(&payload("01_Inbox/bar.md", "world")).unwrap();
        assert!(matches!(ok, MaterializeOutcome::Wrote { .. }));
    }

    #[test]
    fn refresh_conflict_count_into_tray_scans_and_sets() {
        let (vaults, _ws, m_base) = mk(MaterializerMode::Live, default_cfg());
        let tray = make_shared_tray();
        let m = m_base.with_tray_state(tray.clone());
        let vault_dir = vaults.path().join(VAULT);
        std::fs::create_dir_all(vault_dir.join("01_Inbox")).unwrap();
        // Three conflict-stash siblings, varied subpaths.
        std::fs::write(
            vault_dir.join("01_Inbox/a.conflict-from-dev1-1.md"),
            "stash-a",
        )
        .unwrap();
        std::fs::write(
            vault_dir.join("01_Inbox/b.conflict-from-dev2-7.md"),
            "stash-b",
        )
        .unwrap();
        std::fs::write(vault_dir.join("c.conflict-from-dev3-12.md"), "stash-c").unwrap();
        m.refresh_conflict_count_into_tray();
        let s = tray.read().unwrap();
        assert_eq!(s.conflict_unresolved, 3);
    }

    #[test]
    fn refresh_with_no_tray_state_is_noop() {
        let (vaults, _ws, m) = mk(MaterializerMode::Live, default_cfg());
        let vault_dir = vaults.path().join(VAULT);
        std::fs::create_dir_all(&vault_dir).unwrap();
        std::fs::write(vault_dir.join("a.conflict-from-d-1.md"), "x").unwrap();
        // Must not panic, must not touch any tray (there is none).
        m.refresh_conflict_count_into_tray();
        m.refresh_conflict_count_into_tray();
    }

    // ---- B4: per-sync_root materializer tests --------------------------------

    /// B4 core: each sync_root gets its own Materializer constructed with
    /// `sync_root.path` as `vaults_root`. Writes must land at
    /// `<sync_root.path>/<wire_path>`, NOT at some global vaults container.
    ///
    /// Simulates the two-root scenario:
    ///   root_a → /tmp/.../RootA/
    ///   root_b → /tmp/.../RootB/
    ///
    /// A Materializer constructed for root_a writes `notes/x.md` to
    /// `RootA/notes/x.md`; one for root_b writes the same wire_path to
    /// `RootB/notes/x.md`. They must NOT cross-contaminate.
    #[test]
    fn per_root_materializer_writes_to_sync_root_path_join_wire_path() {
        // Two completely separate sync roots (two vault directories).
        let ws_tmp = TempDir::new().unwrap();

        let root_a = TempDir::new().unwrap();
        let root_b = TempDir::new().unwrap();

        let mk_for_root = |root_path: &std::path::Path| {
            Materializer::new(
                root_path.to_path_buf(),
                Some("shadow/".to_string()),
                MaterializerMode::Live,
                ws_tmp.path().to_path_buf(),
                "sub-test".to_string(),
                default_cfg(),
            )
        };

        let mat_a = mk_for_root(root_a.path());
        let mat_b = mk_for_root(root_b.path());

        // Build payloads with the SAME wire path (relative to their respective root).
        let wire_path = "notes/x.md";
        let make_payload = |body: &str| {
            let fm = serde_json::json!({"title": "T"});
            let fm_yaml = serde_yaml::to_string(&fm).unwrap();
            let serialized = format!("---\n{fm_yaml}---\n\n{body}");
            NotePayload {
                path: wire_path.to_string(),
                frontmatter: fm,
                body: body.into(),
                sha256: hex::encode(Sha256::digest(serialized.as_bytes())),
                modified: "2026-05-29T00:00:00Z".into(),
                file_mtime: None,
                enriched_body: Some(serialized),
            }
        };

        let out_a = mat_a.write(&make_payload("body-a")).unwrap();
        let out_b = mat_b.write(&make_payload("body-b")).unwrap();

        // Materializer A must write to root_a/<wire_path>.
        let expected_a = root_a.path().join(wire_path);
        match out_a {
            MaterializeOutcome::Wrote { path } => {
                assert_eq!(path, expected_a, "root_a target mismatch")
            }
            other => panic!("expected Wrote for root_a, got {other:?}"),
        }
        assert!(expected_a.exists());
        let content_a = std::fs::read_to_string(&expected_a).unwrap();
        assert!(
            content_a.contains("body-a"),
            "root_a content wrong: {content_a:?}"
        );

        // Materializer B must write to root_b/<wire_path>.
        let expected_b = root_b.path().join(wire_path);
        match out_b {
            MaterializeOutcome::Wrote { path } => {
                assert_eq!(path, expected_b, "root_b target mismatch")
            }
            other => panic!("expected Wrote for root_b, got {other:?}"),
        }
        assert!(expected_b.exists());
        let content_b = std::fs::read_to_string(&expected_b).unwrap();
        assert!(
            content_b.contains("body-b"),
            "root_b content wrong: {content_b:?}"
        );

        // No cross-contamination: root_a must NOT contain root_b's file.
        let cross_a = root_a.path().join(wire_path);
        let cross_b = root_b.path().join(wire_path);
        let read_cross_a = std::fs::read_to_string(&cross_a).unwrap();
        let read_cross_b = std::fs::read_to_string(&cross_b).unwrap();
        assert!(
            !read_cross_a.contains("body-b"),
            "root_a must not contain root_b content"
        );
        assert!(
            !read_cross_b.contains("body-a"),
            "root_b must not contain root_a content"
        );
    }

    /// B4: `live_path_for(wire_path)` returns `<sync_root.path>/<wire_path>`.
    /// The caller uses this to locate the file before write (e.g. conflict detection).
    #[test]
    fn live_path_for_returns_sync_root_join_wire_path() {
        let sync_root = TempDir::new().unwrap();
        let ws = TempDir::new().unwrap();
        let mat = Materializer::new(
            sync_root.path().to_path_buf(),
            None,
            MaterializerMode::Live,
            ws.path().to_path_buf(),
            "sub".to_string(),
            default_cfg(),
        );
        let wire = "01_Inbox/note.md";
        let result = mat.live_path_for(wire);
        assert_eq!(result, sync_root.path().join(wire));
    }

    /// Ported from main v0.3.9 (S479 E1, commit e816439) into the sync_roots
    /// line. The S479 duplicate-filename bug (`…`→`ΓÇª`, `'`→`ΓÇÖ`, `🚨`→`≡ƒÜ¿`)
    /// came from a shared Windows ingest-layer writer decoding UTF-8 bytes as
    /// the CP437 OEM console codepage. The daemon's materializer was AUDITED
    /// CLEAN (it uses `std::fs`/`OsStr`, UTF-16/UTF-8 native on Windows), so
    /// there is no boundary to fix — this test PINS that property under the
    /// per-root (B4) materialize path: a note whose name carries non-ASCII
    /// punctuation + an emoji materializes to disk with a byte-identical UTF-8
    /// filename, never CP437-mangled, so any future OEM-decode regression fails
    /// loudly.
    #[test]
    fn materialize_preserves_unicode_filename_bytes_not_cp437() {
        let (vaults, _ws, m) = mk(MaterializerMode::Live, default_cfg());
        // Non-ASCII punctuation (… ' – " ") + emoji (🚨) — the exact mojibake
        // class from the S479 worklist.
        let name = "Probe … 'q' – \u{201C}d\u{201D} 🚨.md";
        let rel = format!("01_Inbox/{name}");
        let out = m.write(&payload(&rel, "hello")).unwrap();
        assert!(
            matches!(out, MaterializeOutcome::Wrote { .. }),
            "expected Wrote, got {out:?}"
        );
        // Per-root convention: Live writes under <vaults_root>/<VAULT>/...
        let dir = vaults.path().join(VAULT).join("01_Inbox");
        let names: Vec<String> = std::fs::read_dir(&dir)
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert!(
            names.iter().any(|n| n == name),
            "on-disk filename must be byte-identical UTF-8; got {names:?}"
        );
        assert!(
            !names.iter().any(|n| n.contains("ΓÇ") || n.contains("≡ƒ")),
            "CP437 mojibake detected on disk: {names:?}"
        );
    }
}
