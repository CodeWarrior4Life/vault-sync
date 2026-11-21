//! TKT-dfbd1580, Amendment A-04 section 9 (AR5-009): the daemon side of the
//! non-md effective filter, failing CLOSED.
//!
//! What this module is, and is not, in this build:
//!
//! * **Lane split (A04-9.6, A02-8.2 step 2).** [`is_text_lane`] is the one
//!   question every enqueue path asks before the note pipeline: is this path a
//!   text-lane path (`.md` / `.canvas`)? Anything else is a non-text-lane path
//!   and, until the blob-client PR routes it, is dropped before the note push
//!   journal and before delete classification, counted `nonmd_unrouted`. A
//!   configured extension list can only NARROW the text lane, never widen it.
//! * **Valid effective filter (A04-9.1, A04-9.1a).** [`evaluate`] is the pure
//!   check of the two server representations (metadata object and resolved
//!   bytes). Every failure is `failed_closed`; representation failures carry
//!   the `filter_representation_mismatch{check}` label. Fetching is not here
//!   (the server routes do not exist yet, A04-12.5); the checks are testable
//!   with stubbed replies.
//! * **Process state (A04-9.2, A04-9.3).** [`BlobFilterGate`] starts
//!   `failed_closed` in every process and can only become valid through a
//!   successful [`evaluate`] in that process. There is no constructor from
//!   persisted bytes.
//! * **First-PR gate (A04-9.6).** No path is routed to the blob lane by this
//!   build, whatever the gate says: [`BlobFilterGate::routes_blob_lane`] is
//!   `false`.
//!
//! Nothing here panics on server input: every malformed reply is a value.

use std::collections::{BTreeMap, HashSet};
use std::fmt;

use serde::de::{self, Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};
use serde::Serialize;
use sha2::{Digest, Sha256};

/// A04-9.6 / A02-8.2 step 2: the text-lane extensions (lowercase, no dot).
/// The match is case-insensitive, as the shipped watcher gate is
/// (`file_watcher.rs` extension gate, `eq_ignore_ascii_case`).
pub const TEXT_LANE_EXTENSIONS: &[&str] = &["md", "canvas"];

/// A04-9.1a (f): a version race is refetched at most this many times, then
/// the daemon stays `failed_closed` until the next cycle.
pub const MAX_REPRESENTATION_REFETCH: u32 = 3;

/// A04-9.1 (b): the metadata object's top-level keys, exactly.
const METADATA_KEYS: &[&str] = &["per_rule_drop_counters", "rules", "sha", "state", "version"];

/// A04-9.1a (b): the resolved body's top-level keys, exactly.
const RESOLVED_KEYS: &[&str] = &["grammar_version", "rules"];

/// A04-9.1a (d): the exact 200 `Content-Type`.
const RESOLVED_CONTENT_TYPE: &str = "application/json";

/// A04-9.1a (d): the policy-version header.
const VERSION_HEADER: &str = "x-nexus-filter-version";

/// The error text the strict parser raises on a repeated object key, so the
/// caller can tell a duplicate (A04-9.1a (f)) from any other syntax error.
const DUP_KEY_MARKER: &str = "nexus-sync-duplicate-object-key";

// ---------------------------------------------------------------------------
// Lane split
// ---------------------------------------------------------------------------

/// A04-9.6: true iff `path` (vault-relative, forward slash) is a text-lane
/// path. The extension is taken from the basename only; a dotfile with no
/// further dot (`.md`) and a basename ending in a dot have no extension and
/// are NOT text lane. Anything not positively text lane is non-text lane.
pub fn is_text_lane(path: &str) -> bool {
    let ext = basename_extension(path);
    TEXT_LANE_EXTENSIONS.iter().any(|t| *t == ext)
}

/// The lowercase extension of the basename of `path`, or `""` when it has
/// none (no dot, a leading-dot-only basename, or a trailing dot).
pub fn basename_extension(path: &str) -> String {
    let base = path.rsplit('/').next().unwrap_or("");
    match base.rfind('.') {
        Some(0) | None => String::new(),
        Some(i) => base[i + 1..].to_ascii_lowercase(),
    }
}

// ---------------------------------------------------------------------------
// Strict JSON (duplicate-detecting) and the canonical rendering
// ---------------------------------------------------------------------------

/// A parsed JSON value that keeps object entries in received order and was
/// produced by a parser that REJECTS duplicate keys (A04-9.1a (f): "the
/// daemon's parser MUST detect duplicates rather than keep the last").
#[derive(Debug, Clone)]
pub enum Json {
    Null,
    Bool(bool),
    Int(i128),
    Float(f64),
    Str(String),
    Arr(Vec<Json>),
    Obj(Vec<(String, Json)>),
}

/// Deep equality "as parsed JSON values": objects compare as key sets
/// (order-insensitive), arrays element-wise in order.
impl PartialEq for Json {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Json::Null, Json::Null) => true,
            (Json::Bool(a), Json::Bool(b)) => a == b,
            (Json::Int(a), Json::Int(b)) => a == b,
            (Json::Float(a), Json::Float(b)) => a == b,
            (Json::Str(a), Json::Str(b)) => a == b,
            (Json::Arr(a), Json::Arr(b)) => a == b,
            // Keys are unique (the strict parser refuses duplicates), so equal
            // length plus "every key of `a` is in `b` with an equal value" is
            // set equality.
            (Json::Obj(a), Json::Obj(b)) => {
                a.len() == b.len()
                    && a.iter().all(|(ka, va)| {
                        b.iter()
                            .find(|(kb, _)| kb == ka)
                            .is_some_and(|(_, vb)| va == vb)
                    })
            }
            _ => false,
        }
    }
}

struct JsonVisitor;

impl<'de> Visitor<'de> for JsonVisitor {
    type Value = Json;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("any JSON value")
    }

    fn visit_unit<E: de::Error>(self) -> Result<Json, E> {
        Ok(Json::Null)
    }

    fn visit_none<E: de::Error>(self) -> Result<Json, E> {
        Ok(Json::Null)
    }

    fn visit_bool<E: de::Error>(self, v: bool) -> Result<Json, E> {
        Ok(Json::Bool(v))
    }

    fn visit_i64<E: de::Error>(self, v: i64) -> Result<Json, E> {
        Ok(Json::Int(i128::from(v)))
    }

    fn visit_u64<E: de::Error>(self, v: u64) -> Result<Json, E> {
        Ok(Json::Int(i128::from(v)))
    }

    fn visit_f64<E: de::Error>(self, v: f64) -> Result<Json, E> {
        Ok(Json::Float(v))
    }

    fn visit_str<E: de::Error>(self, v: &str) -> Result<Json, E> {
        Ok(Json::Str(v.to_owned()))
    }

    fn visit_string<E: de::Error>(self, v: String) -> Result<Json, E> {
        Ok(Json::Str(v))
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Json, A::Error> {
        let mut out = Vec::new();
        while let Some(v) = seq.next_element::<Json>()? {
            out.push(v);
        }
        Ok(Json::Arr(out))
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Json, A::Error> {
        let mut out: Vec<(String, Json)> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        while let Some(k) = map.next_key::<String>()? {
            if !seen.insert(k.clone()) {
                return Err(de::Error::custom(DUP_KEY_MARKER));
            }
            let v = map.next_value::<Json>()?;
            out.push((k, v));
        }
        Ok(Json::Obj(out))
    }
}

impl<'de> Deserialize<'de> for Json {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        d.deserialize_any(JsonVisitor)
    }
}

/// Why a byte string did not parse as strict JSON.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParseFail {
    DuplicateKey,
    Syntax,
}

/// Parse `bytes` as one JSON value, rejecting duplicate object keys.
pub fn parse_strict(bytes: &[u8]) -> Result<Json, ParseFail> {
    serde_json::from_slice::<Json>(bytes).map_err(|e| {
        if e.to_string().contains(DUP_KEY_MARKER) {
            ParseFail::DuplicateKey
        } else {
            ParseFail::Syntax
        }
    })
}

/// A04-9.1a (c): render `v` canonically. `None` when `v` holds a value the
/// canonical form cannot express (any non-integer number).
pub fn render_canonical(v: &Json) -> Option<String> {
    let mut out = String::new();
    render_into(v, &mut out)?;
    Some(out)
}

fn render_into(v: &Json, out: &mut String) -> Option<()> {
    match v {
        Json::Null => out.push_str("null"),
        Json::Bool(true) => out.push_str("true"),
        Json::Bool(false) => out.push_str("false"),
        Json::Int(i) => out.push_str(&i.to_string()),
        Json::Float(_) => return None,
        Json::Str(s) => render_str(s, out),
        Json::Arr(items) => {
            out.push('[');
            for (n, item) in items.iter().enumerate() {
                if n > 0 {
                    out.push(',');
                }
                render_into(item, out)?;
            }
            out.push(']');
        }
        Json::Obj(entries) => {
            let mut sorted: Vec<&(String, Json)> = entries.iter().collect();
            // Ascending order of the keys' UTF-8 bytes (`str` orders by bytes).
            sorted.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
            out.push('{');
            for (n, (k, val)) in sorted.into_iter().enumerate() {
                if n > 0 {
                    out.push(',');
                }
                render_str(k, out);
                out.push(':');
                render_into(val, out)?;
            }
            out.push('}');
        }
    }
    Some(())
}

/// Escape only `"`, `\` and code points below U+0020 (as lowercase
/// `\u00xx`); everything else, non-ASCII included, is written raw.
fn render_str(s: &str, out: &mut String) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
}

// ---------------------------------------------------------------------------
// Evaluation of the two representations (A04-9.1, A04-9.1a)
// ---------------------------------------------------------------------------

/// One HTTP reply, as the fetch layer received it. Header names are matched
/// case-insensitively; a header present more than once never matches.
#[derive(Debug, Clone, Copy)]
pub struct HttpReply<'a> {
    pub status: u16,
    pub headers: &'a [(&'a str, &'a str)],
    pub body: &'a [u8],
}

/// The rule grammar (WP-2 "C1 grammar", A02-8.14) a daemon build
/// implements. A04-9.1 (e): rules parse under the grammar version the daemon
/// implements, and any unknown key or rule class is invalid. This build
/// implements none ([`IMPLEMENTED_GRAMMAR`]), so no filter can be valid in
/// production until the grammar lands.
pub trait RuleGrammar {
    fn grammar_version(&self) -> i128;
    /// `Err(reason)` on any unknown key, unknown rule class or malformed rule.
    fn check_rules(&self, rules: &[Json]) -> Result<(), String>;
}

/// The grammar this build implements: none (WP-2 has not shipped).
pub const IMPLEMENTED_GRAMMAR: Option<&'static dyn RuleGrammar> = None;

/// The check that failed. [`Check::label`] is the stable counter label.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Check {
    /// A04-9.3: nothing has been validated in this process yet.
    NotFetchedThisProcess,
    /// A04-9.1 (a): the metadata route answered other than 200.
    MetadataStatus,
    /// A04-9.1 (b): the metadata body is not one strict JSON value.
    MetadataParse,
    /// A04-9.1 (b): not an object whose top-level keys are exactly the five.
    MetadataKeys,
    /// A04-9.1 (c): `version` is not an integer of at least 1.
    MetadataVersion,
    /// A04-9.1 (c): `sha` is not 64 lowercase hex.
    MetadataSha,
    /// A04-9.1 (c): `state` is not exactly `"active"`.
    MetadataState,
    /// A04-9.1 (f): the server has reported no policy version in this process.
    PolicyVersionUnknown,
    /// A04-9.1a (e): the resolved route answered other than 200.
    ResolvedStatus,
    /// A04-9.1a (d)/(g): `Content-Type` is not exactly `application/json`.
    ContentType,
    /// A04-9.1a (d)/(f): `ETag` is not the unquoted metadata sha.
    Etag,
    /// A04-9.1a (d)/(f): `X-Nexus-Filter-Version` is not the metadata version.
    VersionHeader,
    /// A04-9.1 (d) / A04-9.1a (f): `sha256(body)` is not the metadata sha.
    BodySha,
    /// A04-9.1a (f): the body repeats an object key.
    DuplicateKey,
    /// A04-9.1a (c): the body is not JSON at all.
    Parse,
    /// A04-9.1a (c)/(f): the body re-rendered canonically is not the body.
    CanonicalRender,
    /// A04-9.1a (b): the body is not exactly `{grammar_version, rules}`.
    ResolvedShape,
    /// A04-9.1a (f): the body's `rules` differ from the metadata `rules`.
    RulesEquality,
    /// A04-9.1 (e): the daemon implements no grammar of this version.
    GrammarUnsupported,
    /// A04-9.1 (e): a rule has an unknown key or class.
    RuleInvalid,
}

impl Check {
    /// The stable label used for counters and logs.
    pub fn label(self) -> &'static str {
        match self {
            Check::NotFetchedThisProcess => "not_fetched_this_process",
            Check::MetadataStatus => "metadata_status",
            Check::MetadataParse => "metadata_parse",
            Check::MetadataKeys => "metadata_keys",
            Check::MetadataVersion => "metadata_version",
            Check::MetadataSha => "metadata_sha",
            Check::MetadataState => "metadata_state",
            Check::PolicyVersionUnknown => "policy_version_unknown",
            Check::ResolvedStatus => "resolved_status",
            Check::ContentType => "content_type",
            Check::Etag => "etag",
            Check::VersionHeader => "version_header",
            Check::BodySha => "body_sha",
            Check::DuplicateKey => "duplicate_key",
            Check::Parse => "parse",
            Check::CanonicalRender => "canonical_render",
            Check::ResolvedShape => "resolved_shape",
            Check::RulesEquality => "rules_equality",
            Check::GrammarUnsupported => "grammar_unsupported",
            Check::RuleInvalid => "rule_invalid",
        }
    }

    /// True iff this failure counts as `filter_representation_mismatch{check}`
    /// (A04-9.1a): the resolved route's status, headers or bytes disagree
    /// with the metadata or with the canonical form.
    pub fn is_representation_mismatch(self) -> bool {
        matches!(
            self,
            Check::ResolvedStatus
                | Check::ContentType
                | Check::Etag
                | Check::VersionHeader
                | Check::BodySha
                | Check::DuplicateKey
                | Check::Parse
                | Check::CanonicalRender
                | Check::ResolvedShape
                | Check::RulesEquality
        )
    }
}

/// A04-9.1 (f): a version disagreement that is NOT a failure by itself and
/// does not page; the caller re-reads and evaluates again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Retry {
    /// Metadata `version` is newer than the newest reported policy version:
    /// re-read `GET /api/sync/rollout/state` first.
    RereadRolloutState,
    /// Metadata `version` is older: the metadata is stale, refetch it.
    RefetchMetadata,
}

/// A filter that passed every check of A04-9.1 and A04-9.1a.
#[derive(Debug, Clone, PartialEq)]
pub struct ValidFilter {
    pub version: u64,
    pub sha: String,
    pub grammar_version: i128,
    /// Parsed from the received bytes, never re-serialized (A04-9.1 (d)).
    pub rules: Vec<Json>,
}

/// The outcome of one evaluation.
#[derive(Debug, Clone, PartialEq)]
pub enum Evaluation {
    Valid(ValidFilter),
    FailedClosed(Check),
    Retry(Retry),
}

/// A04-9.1 and A04-9.1a, pure. `newest_policy_version` is the newest active
/// policy version the server has reported to this daemon in this process
/// (register snapshot or any later rollout-state read, whichever is newer).
/// `grammar` is the grammar this build implements ([`IMPLEMENTED_GRAMMAR`]
/// in production).
pub fn evaluate(
    metadata: &HttpReply<'_>,
    resolved: &HttpReply<'_>,
    newest_policy_version: Option<u64>,
    grammar: Option<&dyn RuleGrammar>,
) -> Evaluation {
    match evaluate_inner(metadata, resolved, newest_policy_version, grammar) {
        Ok(Ok(v)) => Evaluation::Valid(v),
        Ok(Err(r)) => Evaluation::Retry(r),
        Err(c) => Evaluation::FailedClosed(c),
    }
}

type Outcome = Result<Result<ValidFilter, Retry>, Check>;

fn evaluate_inner(
    metadata: &HttpReply<'_>,
    resolved: &HttpReply<'_>,
    newest_policy_version: Option<u64>,
    grammar: Option<&dyn RuleGrammar>,
) -> Outcome {
    // A04-9.1 (a): own id, 200.
    if metadata.status != 200 {
        return Err(Check::MetadataStatus);
    }
    // A04-9.1 (b): one strict JSON object, keys exactly the five.
    let meta = parse_strict(metadata.body).map_err(|_| Check::MetadataParse)?;
    let Json::Obj(meta) = meta else {
        return Err(Check::MetadataKeys);
    };
    if !keys_exactly(&meta, METADATA_KEYS) {
        return Err(Check::MetadataKeys);
    }
    // A04-9.1 (c).
    let version = match field(&meta, "version") {
        Some(Json::Int(v)) if *v >= 1 => u64::try_from(*v).map_err(|_| Check::MetadataVersion)?,
        _ => return Err(Check::MetadataVersion),
    };
    let sha = match field(&meta, "sha") {
        Some(Json::Str(s)) if is_lower_hex_64(s) => s.clone(),
        _ => return Err(Check::MetadataSha),
    };
    match field(&meta, "state") {
        Some(Json::Str(s)) if s == "active" => {}
        _ => return Err(Check::MetadataState),
    }
    // A04-9.1 (f): newest reported policy version, in this process.
    let Some(newest) = newest_policy_version else {
        return Err(Check::PolicyVersionUnknown);
    };
    if version > newest {
        return Ok(Err(Retry::RereadRolloutState));
    }
    if version < newest {
        return Ok(Err(Retry::RefetchMetadata));
    }

    // A04-9.1a (e): any status other than 200 is "no valid filter".
    if resolved.status != 200 {
        return Err(Check::ResolvedStatus);
    }
    // A04-9.1a (d), (g): exact headers.
    if single_header(resolved, "content-type") != Some(RESOLVED_CONTENT_TYPE) {
        return Err(Check::ContentType);
    }
    match single_header(resolved, "etag") {
        Some(e) if is_lower_hex_64(e) && e == sha => {}
        _ => return Err(Check::Etag),
    }
    match single_header(resolved, VERSION_HEADER).and_then(parse_decimal) {
        Some(v) if v == version => {}
        _ => return Err(Check::VersionHeader),
    }
    // A04-9.1 (d) / A04-9.1a (f): sha256(body) = ETag = metadata.sha.
    if hex::encode(Sha256::digest(resolved.body)) != sha {
        return Err(Check::BodySha);
    }
    // A04-9.1a (f): duplicate keys detected by the parser, then the
    // re-render must be byte-identical to what was received.
    let body = parse_strict(resolved.body).map_err(|e| match e {
        ParseFail::DuplicateKey => Check::DuplicateKey,
        ParseFail::Syntax => Check::Parse,
    })?;
    match render_canonical(&body) {
        Some(r) if r.as_bytes() == resolved.body => {}
        _ => return Err(Check::CanonicalRender),
    }
    // A04-9.1a (b): exactly {grammar_version >= 1, rules: array}.
    let Json::Obj(body) = body else {
        return Err(Check::ResolvedShape);
    };
    if !keys_exactly(&body, RESOLVED_KEYS) {
        return Err(Check::ResolvedShape);
    }
    let grammar_version = match field(&body, "grammar_version") {
        Some(Json::Int(g)) if *g >= 1 => *g,
        _ => return Err(Check::ResolvedShape),
    };
    let rules = match field(&body, "rules") {
        Some(Json::Arr(r)) => r.clone(),
        _ => return Err(Check::ResolvedShape),
    };
    // A04-9.1a (f): body rules equal metadata rules as parsed values.
    match field(&meta, "rules") {
        Some(Json::Arr(m)) if *m == rules => {}
        _ => return Err(Check::RulesEquality),
    }
    // A04-9.1 (e): the grammar this build implements.
    let Some(g) = grammar else {
        return Err(Check::GrammarUnsupported);
    };
    if g.grammar_version() != grammar_version {
        return Err(Check::GrammarUnsupported);
    }
    if g.check_rules(&rules).is_err() {
        return Err(Check::RuleInvalid);
    }
    Ok(Ok(ValidFilter {
        version,
        sha,
        grammar_version,
        rules,
    }))
}

fn field<'a>(obj: &'a [(String, Json)], key: &str) -> Option<&'a Json> {
    obj.iter().find(|(k, _)| k == key).map(|(_, v)| v)
}

/// The object's keys are exactly `want` (no more, no fewer; keys are unique).
fn keys_exactly(obj: &[(String, Json)], want: &[&str]) -> bool {
    obj.len() == want.len() && want.iter().all(|w| field(obj, w).is_some())
}

fn is_lower_hex_64(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

/// A header value, iff the header (case-insensitive name) is present exactly
/// once.
fn single_header<'a>(reply: &HttpReply<'a>, name: &str) -> Option<&'a str> {
    let mut found = reply
        .headers
        .iter()
        .filter(|(n, _)| n.eq_ignore_ascii_case(name))
        .map(|(_, v)| *v);
    let first = found.next()?;
    if found.next().is_some() {
        return None;
    }
    Some(first)
}

/// Minimal decimal form only: ASCII digits, no sign, no leading zero.
fn parse_decimal(s: &str) -> Option<u64> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    if s.len() > 1 && s.starts_with('0') {
        return None;
    }
    s.parse().ok()
}

// ---------------------------------------------------------------------------
// Process state (A04-9.2, A04-9.3)
// ---------------------------------------------------------------------------

/// The serializable state reported on the status surface (A04-9.2).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BlobFilterStatus {
    #[default]
    FailedClosed,
    Valid,
}

/// Counters for the filter evaluation (A04-9.1a (f)).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct FilterCounters {
    /// Evaluations that ended `failed_closed`.
    pub failed_closed: u64,
    /// `filter_representation_mismatch{check}`.
    pub filter_representation_mismatch: BTreeMap<&'static str, u64>,
}

/// The per-process filter state. Starts `failed_closed`
/// ([`Check::NotFetchedThisProcess`]); becomes valid only through
/// [`BlobFilterGate::apply`] of an [`Evaluation::Valid`].
#[derive(Debug, Clone)]
pub struct BlobFilterGate {
    current: Option<ValidFilter>,
    last_failure: Option<Check>,
    counters: FilterCounters,
}

impl Default for BlobFilterGate {
    fn default() -> Self {
        Self::new()
    }
}

impl BlobFilterGate {
    /// A04-9.3: a new process is `failed_closed` until A04-9.1 holds again.
    pub fn new() -> Self {
        Self {
            current: None,
            last_failure: Some(Check::NotFetchedThisProcess),
            counters: FilterCounters::default(),
        }
    }

    /// Fold one evaluation into the state. A failure drops any previously
    /// valid filter (A04-9.3: a sha mismatch is `failed_closed`); a
    /// [`Retry`] changes nothing (A04-9.1 (f): neither case alone pages).
    pub fn apply(&mut self, eval: Evaluation) -> BlobFilterStatus {
        match eval {
            Evaluation::Valid(f) => {
                self.current = Some(f);
                self.last_failure = None;
            }
            Evaluation::FailedClosed(check) => {
                self.current = None;
                self.last_failure = Some(check);
                self.counters.failed_closed = self.counters.failed_closed.saturating_add(1);
                if check.is_representation_mismatch() {
                    let n = self
                        .counters
                        .filter_representation_mismatch
                        .entry(check.label())
                        .or_insert(0);
                    *n = n.saturating_add(1);
                }
                tracing::warn!(
                    target: "vault_sync_daemon::blob_filter",
                    check = check.label(),
                    "blob_filter: effective filter FAILED CLOSED (no blob-lane action; text lane unaffected)"
                );
            }
            Evaluation::Retry(_) => {}
        }
        self.status()
    }

    pub fn status(&self) -> BlobFilterStatus {
        if self.current.is_some() {
            BlobFilterStatus::Valid
        } else {
            BlobFilterStatus::FailedClosed
        }
    }

    pub fn last_failure(&self) -> Option<Check> {
        self.last_failure
    }

    pub fn valid_filter(&self) -> Option<&ValidFilter> {
        self.current.as_ref()
    }

    pub fn counters(&self) -> &FilterCounters {
        &self.counters
    }

    /// A04-9.6: this build has no blob client, so no path is routed to the
    /// blob lane whatever the filter state.
    pub fn routes_blob_lane(&self) -> bool {
        false
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::disallowed_methods)]
mod tests {
    use super::*;

    // ---- A04-9.6: the lane split -------------------------------------------

    #[test]
    fn a04_9_6_text_lane_is_md_and_canvas_only() {
        for p in [
            "a.md",
            "dir/b.canvas",
            "Notes/C.MD",
            "x/y/Z.Canvas",
            "dir.with.dots/n.md",
            "...md.md",
        ] {
            assert!(is_text_lane(p), "{p} must be text lane");
        }
        for p in [
            "a.png",
            "a",
            ".md",
            "dir/.canvas",
            "a.md.png",
            "dir.md/x",
            "dir.md/x.png",
            "a.",
            "a.mdx",
            "a.MDX",
            "a.md ",
            "a.md#chunk_1",
            "",
            "/",
            "dir/",
        ] {
            assert!(!is_text_lane(p), "{p:?} must NOT be text lane");
        }
    }

    #[test]
    fn a04_9_6_basename_extension_reads_the_basename_only() {
        assert_eq!(basename_extension("a/b.PNG"), "png");
        assert_eq!(basename_extension("a.md/b"), "");
        assert_eq!(basename_extension(".md"), "");
        assert_eq!(basename_extension("a."), "");
        assert_eq!(basename_extension("a.tar.gz"), "gz");
        assert_eq!(basename_extension(""), "");
    }

    // ---- strict parse and canonical render (A04-9.1a (c), (f)) -------------

    #[test]
    fn a04_9_1a_parser_detects_duplicate_keys_at_any_depth() {
        assert_eq!(
            parse_strict(br#"{"a":1,"a":2}"#),
            Err(ParseFail::DuplicateKey)
        );
        assert_eq!(
            parse_strict(br#"{"r":[{"k":1,"k":1}]}"#),
            Err(ParseFail::DuplicateKey)
        );
        assert_eq!(parse_strict(b"{"), Err(ParseFail::Syntax));
        assert_eq!(parse_strict(b"\xEF\xBB\xBF{}"), Err(ParseFail::Syntax));
        assert!(parse_strict(br#"{"a":1,"b":{"a":2}}"#).is_ok());
    }

    #[test]
    fn a04_9_1a_canonical_render_is_byte_exact() {
        // Canonical: sorted keys, no whitespace, only `"`, `\` and control
        // characters escaped, non-ASCII raw.
        let canon = "{\"a\":[1,-2,0,true,false,null],\"b\":\"x\\\"y\\\\z\\u001f/\u{e9}\"}";
        let v = parse_strict(canon.as_bytes()).unwrap();
        assert_eq!(render_canonical(&v).unwrap(), canon);
        // Keys are sorted by UTF-8 bytes on render.
        let v = parse_strict(br#"{"b":1,"a":2}"#).unwrap();
        assert_eq!(render_canonical(&v).unwrap(), r#"{"a":2,"b":1}"#);
        // Control characters render as lowercase \u00xx.
        let v = parse_strict(br#""\u001F\n""#).unwrap();
        assert_eq!(render_canonical(&v).unwrap(), r#""\u001f\u000a""#);
        // `\/` parses to `/`, which renders unescaped; an escaped non-ASCII
        // character renders raw.
        let v = parse_strict(br#""a\/b\u00e9""#).unwrap();
        assert_eq!(render_canonical(&v).unwrap(), "\"a/b\u{e9}\"");
        // Any non-integer number has no canonical form.
        let v = parse_strict(b"1.0").unwrap();
        assert!(render_canonical(&v).is_none());
        let v = parse_strict(b"1e2").unwrap();
        assert!(render_canonical(&v).is_none());
    }

    #[test]
    fn a04_9_1a_json_equality_is_value_equality() {
        let a = parse_strict(br#"{"x":1,"y":[1,2]}"#).unwrap();
        let b = parse_strict(br#"{"y":[1,2],"x":1}"#).unwrap();
        assert_eq!(a, b, "object key order is not significant");
        let c = parse_strict(br#"{"y":[2,1],"x":1}"#).unwrap();
        assert_ne!(a, c, "array order is significant");
        let d = parse_strict(br#"{"x":1}"#).unwrap();
        assert_ne!(a, d);
        assert_ne!(d, a);
    }

    // ---- fixtures -----------------------------------------------------------

    const RULES: &str = r#"[{"class":"junk","pattern":".sync-tmp/**"},{"class":"junk","pattern":"*.conflict-from-*"}]"#;

    fn canonical_body() -> String {
        format!(r#"{{"grammar_version":1,"rules":{RULES}}}"#)
    }

    fn sha_hex(b: &[u8]) -> String {
        hex::encode(Sha256::digest(b))
    }

    /// A test-only grammar v1: each rule is an object with exactly the keys
    /// `class` and `pattern`, `class` one of `junk`.
    struct TestGrammar;
    impl RuleGrammar for TestGrammar {
        fn grammar_version(&self) -> i128 {
            1
        }
        fn check_rules(&self, rules: &[Json]) -> Result<(), String> {
            for r in rules {
                let Json::Obj(entries) = r else {
                    return Err("rule is not an object".into());
                };
                let mut keys: Vec<&str> = entries.iter().map(|(k, _)| k.as_str()).collect();
                keys.sort_unstable();
                if keys != ["class", "pattern"] {
                    return Err(format!("unknown keys {keys:?}"));
                }
                for (k, v) in entries {
                    match (k.as_str(), v) {
                        ("class", Json::Str(c)) if c == "junk" => {}
                        ("pattern", Json::Str(_)) => {}
                        _ => return Err(format!("bad {k}")),
                    }
                }
            }
            Ok(())
        }
    }

    /// Everything needed to evaluate one case; each field is consistent with
    /// `body` unless a test overrides it.
    struct Case {
        meta_status: u16,
        meta: String,
        res_status: u16,
        content_type: String,
        etag: String,
        version_header: String,
        body: Vec<u8>,
        newest: Option<u64>,
    }

    impl Case {
        /// A fully consistent case for `body` at policy `version`.
        fn for_body(body: &[u8], version: u64) -> Self {
            let sha = sha_hex(body);
            Case {
                meta_status: 200,
                meta: format!(
                    r#"{{"version":{version},"sha":"{sha}","state":"active","rules":{RULES},"per_rule_drop_counters":{{}}}}"#
                ),
                res_status: 200,
                content_type: "application/json".into(),
                etag: sha,
                version_header: version.to_string(),
                body: body.to_vec(),
                newest: Some(version),
            }
        }

        fn run(&self, grammar: Option<&dyn RuleGrammar>) -> Evaluation {
            let headers = [
                ("Content-Type", self.content_type.as_str()),
                ("ETag", self.etag.as_str()),
                ("X-Nexus-Filter-Version", self.version_header.as_str()),
                ("Cache-Control", "no-store"),
            ];
            let meta = HttpReply {
                status: self.meta_status,
                headers: &[],
                body: self.meta.as_bytes(),
            };
            let res = HttpReply {
                status: self.res_status,
                headers: &headers,
                body: &self.body,
            };
            evaluate(&meta, &res, self.newest, grammar)
        }
    }

    fn good() -> Case {
        Case::for_body(canonical_body().as_bytes(), 1)
    }

    /// The body is changed AND the metadata sha and ETag follow it, so the
    /// failure lands on the byte-form check, not on the sha.
    fn with_body(body: &str) -> Case {
        Case::for_body(body.as_bytes(), 1)
    }

    fn failed(e: Evaluation) -> Check {
        match e {
            Evaluation::FailedClosed(c) => c,
            other => panic!("expected failed_closed, got {other:?}"),
        }
    }

    // ---- positive control ----------------------------------------------------

    #[test]
    fn a04_9_1_consistent_representations_are_valid_under_an_implemented_grammar() {
        match good().run(Some(&TestGrammar)) {
            Evaluation::Valid(v) => {
                assert_eq!(v.version, 1);
                assert_eq!(v.sha, sha_hex(canonical_body().as_bytes()));
                assert_eq!(v.grammar_version, 1);
                assert_eq!(v.rules.len(), 2);
            }
            other => panic!("expected valid, got {other:?}"),
        }
    }

    #[test]
    fn a04_9_1_e_this_build_implements_no_grammar_so_nothing_is_valid() {
        assert!(IMPLEMENTED_GRAMMAR.is_none());
        assert_eq!(
            failed(good().run(IMPLEMENTED_GRAMMAR)),
            Check::GrammarUnsupported
        );
    }

    // ---- A04-9 negative tests ------------------------------------------------

    #[test]
    fn a04_9_neg_effective_filter_404() {
        let mut c = good();
        c.meta_status = 404;
        assert_eq!(failed(c.run(Some(&TestGrammar))), Check::MetadataStatus);
    }

    #[test]
    fn a04_9_1_b_neg_extra_top_level_key() {
        let mut c = good();
        c.meta = c.meta.replacen('{', r#"{"extra":1,"#, 1);
        assert_eq!(failed(c.run(Some(&TestGrammar))), Check::MetadataKeys);
    }

    #[test]
    fn a04_9_1_b_neg_missing_top_level_key() {
        let mut c = good();
        c.meta = c.meta.replace(r#","per_rule_drop_counters":{}"#, "");
        assert_eq!(failed(c.run(Some(&TestGrammar))), Check::MetadataKeys);
    }

    #[test]
    fn a04_9_1_b_neg_metadata_not_an_object_or_duplicate_key() {
        let mut c = good();
        c.meta = "[]".into();
        assert_eq!(failed(c.run(Some(&TestGrammar))), Check::MetadataKeys);
        let mut c = good();
        c.meta = c.meta.replacen('{', r#"{"state":"active","#, 1);
        assert_eq!(failed(c.run(Some(&TestGrammar))), Check::MetadataParse);
        let mut c = good();
        c.meta = "not json".into();
        assert_eq!(failed(c.run(Some(&TestGrammar))), Check::MetadataParse);
    }

    #[test]
    fn a04_9_1_c_neg_state_candidate() {
        let mut c = good();
        c.meta = c.meta.replace(r#""active""#, r#""candidate""#);
        assert_eq!(failed(c.run(Some(&TestGrammar))), Check::MetadataState);
    }

    #[test]
    fn a04_9_1_c_neg_version_and_sha_forms() {
        for bad in [
            r#""version":0"#,
            r#""version":"1""#,
            r#""version":1.0"#,
            r#""version":-1"#,
        ] {
            let mut c = good();
            c.meta = c.meta.replace(r#""version":1"#, bad);
            assert_eq!(
                failed(c.run(Some(&TestGrammar))),
                Check::MetadataVersion,
                "{bad}"
            );
        }
        let mut c = good();
        let upper = c.etag.to_ascii_uppercase();
        c.meta = c.meta.replace(&c.etag, &upper);
        assert_eq!(failed(c.run(Some(&TestGrammar))), Check::MetadataSha);
        let mut c = good();
        let short = c.etag[..63].to_string();
        c.meta = c.meta.replace(&c.etag, &short);
        assert_eq!(failed(c.run(Some(&TestGrammar))), Check::MetadataSha);
    }

    #[test]
    fn a04_9_1_d_neg_body_sha_mismatch() {
        // Metadata sha and ETag name the canonical body; the bytes served are
        // a different (still canonical) body.
        let mut c = good();
        c.body = br#"{"grammar_version":1,"rules":[]}"#.to_vec();
        assert_eq!(failed(c.run(Some(&TestGrammar))), Check::BodySha);
    }

    #[test]
    fn a04_9_1_e_neg_unknown_rule_class_and_unknown_key() {
        let body = canonical_body().replacen(r#""junk""#, r#""mystery""#, 1);
        let mut c = with_body(&body);
        c.meta = c.meta.replacen(r#""junk""#, r#""mystery""#, 1);
        assert_eq!(failed(c.run(Some(&TestGrammar))), Check::RuleInvalid);

        let body =
            canonical_body().replacen(r#"{"class":"junk","#, r#"{"class":"junk","extra":1,"#, 1);
        let mut c = with_body(&body);
        c.meta = c
            .meta
            .replacen(r#"{"class":"junk","#, r#"{"class":"junk","extra":1,"#, 1);
        assert_eq!(failed(c.run(Some(&TestGrammar))), Check::RuleInvalid);
    }

    #[test]
    fn a04_9_1_e_neg_grammar_version_not_implemented() {
        let body = canonical_body().replace(r#""grammar_version":1"#, r#""grammar_version":2"#);
        let c = with_body(&body);
        assert_eq!(failed(c.run(Some(&TestGrammar))), Check::GrammarUnsupported);
    }

    // ---- A04-9.1 (f): version freshness, promotion (AR7-007) -----------------

    #[test]
    fn a04_9_1_f_metadata_newer_than_reported_rereads_rollout_state() {
        let mut c = Case::for_body(canonical_body().as_bytes(), 2);
        c.newest = Some(1);
        assert_eq!(
            c.run(Some(&TestGrammar)),
            Evaluation::Retry(Retry::RereadRolloutState)
        );
    }

    #[test]
    fn a04_9_1_f_metadata_older_than_reported_is_refetched() {
        let mut c = good();
        c.newest = Some(2);
        assert_eq!(
            c.run(Some(&TestGrammar)),
            Evaluation::Retry(Retry::RefetchMetadata)
        );
    }

    #[test]
    fn a04_9_1_f_no_reported_policy_version_is_failed_closed() {
        let mut c = good();
        c.newest = None;
        assert_eq!(
            failed(c.run(Some(&TestGrammar))),
            Check::PolicyVersionUnknown
        );
    }

    /// Promotion test (AR7-007), daemon half: valid at V; V+1 promoted; the
    /// first evaluation against V+1 metadata asks for a rollout re-read and
    /// does NOT drop the gate to failed_closed; after the re-read reports
    /// V+1, the refetched pair is valid at V+1. Mutation control: pinning
    /// (f) to the register snapshot (newest stays V) never reaches V+1.
    #[test]
    fn a04_9_1_f_promotion_is_valid_at_v_plus_1_without_failed_closed() {
        let mut gate = BlobFilterGate::new();
        assert_eq!(
            gate.apply(good().run(Some(&TestGrammar))),
            BlobFilterStatus::Valid
        );
        let mut next = Case::for_body(canonical_body().as_bytes(), 2);
        next.newest = Some(1);
        assert_eq!(
            gate.apply(next.run(Some(&TestGrammar))),
            BlobFilterStatus::Valid,
            "a newer metadata version alone must not fail closed"
        );
        assert_eq!(gate.counters().failed_closed, 0);
        next.newest = Some(2);
        assert_eq!(
            gate.apply(next.run(Some(&TestGrammar))),
            BlobFilterStatus::Valid
        );
        assert_eq!(gate.valid_filter().unwrap().version, 2);
        assert_eq!(gate.counters().failed_closed, 0);
    }

    // ---- A04-9.1a wire-form tests (AR6-007) -----------------------------------

    fn wire_case(c: Case, want: Check) {
        let mut gate = BlobFilterGate::new();
        let status = gate.apply(c.run(Some(&TestGrammar)));
        assert_eq!(status, BlobFilterStatus::FailedClosed, "{want:?}");
        assert_eq!(gate.last_failure(), Some(want));
        assert!(want.is_representation_mismatch(), "{want:?}");
        assert_eq!(
            gate.counters()
                .filter_representation_mismatch
                .get(want.label())
                .copied(),
            Some(1),
            "filter_representation_mismatch{{check={}}}",
            want.label()
        );
        assert_eq!(gate.counters().failed_closed, 1);
        assert!(!gate.routes_blob_lane());
    }

    #[test]
    fn a04_9_1a_wire_one_added_space() {
        let body = canonical_body().replacen(r#""rules":"#, r#""rules": "#, 1);
        wire_case(with_body(&body), Check::CanonicalRender);
    }

    #[test]
    fn a04_9_1a_wire_two_keys_swapped() {
        let body = format!(r#"{{"rules":{RULES},"grammar_version":1}}"#);
        wire_case(with_body(&body), Check::CanonicalRender);
    }

    #[test]
    fn a04_9_1a_wire_duplicated_key() {
        let body = canonical_body().replacen(
            r#"{"grammar_version":1,"#,
            r#"{"grammar_version":1,"grammar_version":1,"#,
            1,
        );
        wire_case(with_body(&body), Check::DuplicateKey);
    }

    #[test]
    fn a04_9_1a_wire_escaped_solidus() {
        let body = canonical_body().replacen(".sync-tmp/**", r".sync-tmp\/**", 1);
        wire_case(with_body(&body), Check::CanonicalRender);
    }

    #[test]
    fn a04_9_1a_wire_float_one_point_zero() {
        let body = canonical_body().replace(r#""grammar_version":1"#, r#""grammar_version":1.0"#);
        wire_case(with_body(&body), Check::CanonicalRender);
    }

    #[test]
    fn a04_9_1a_wire_trailing_newline() {
        let body = format!("{}\n", canonical_body());
        wire_case(with_body(&body), Check::CanonicalRender);
    }

    #[test]
    fn a04_9_1a_wire_content_type_text_plain_or_with_parameters() {
        let mut c = good();
        c.content_type = "text/plain".into();
        wire_case(c, Check::ContentType);
        let mut c = good();
        c.content_type = "application/json; charset=utf-8".into();
        wire_case(c, Check::ContentType);
    }

    #[test]
    fn a04_9_1a_wire_etag_in_quotes() {
        let mut c = good();
        c.etag = format!("\"{}\"", c.etag);
        wire_case(c, Check::Etag);
    }

    /// Mutation control: body and metadata.sha agree here, so only the ETag
    /// comparison can catch it.
    #[test]
    fn a04_9_1a_wire_etag_differs_from_metadata_sha() {
        let mut c = good();
        c.etag = sha_hex(b"something else");
        wire_case(c, Check::Etag);
    }

    /// Mutation control: removing the header-version comparison makes this
    /// case valid.
    #[test]
    fn a04_9_1a_wire_version_header_differs() {
        let mut c = good();
        c.version_header = "2".into();
        wire_case(c, Check::VersionHeader);
        let mut c = good();
        c.version_header = "01".into();
        wire_case(c, Check::VersionHeader);
    }

    /// Mutation control: removing the metadata-`rules` equality check makes
    /// this case valid.
    #[test]
    fn a04_9_1a_wire_metadata_rules_differ_in_one_element() {
        let mut c = good();
        c.meta = c.meta.replacen(".sync-tmp/**", ".nexus-trash/**", 1);
        wire_case(c, Check::RulesEquality);
    }

    #[test]
    fn a04_9_1a_wire_503_filter_failed_closed() {
        let mut c = good();
        c.res_status = 503;
        c.body = br#"{"detail":{"error":"filter_failed_closed"}}"#.to_vec();
        wire_case(c, Check::ResolvedStatus);
    }

    #[test]
    fn a04_9_1a_wire_body_shape() {
        let body = format!(r#"{{"grammar_version":1,"rules":{RULES},"version":1}}"#);
        wire_case(with_body(&body), Check::ResolvedShape);
        let body = r#"{"grammar_version":0,"rules":[]}"#;
        wire_case(with_body(body), Check::ResolvedShape);
        let body = r#"{"grammar_version":1,"rules":{}}"#;
        wire_case(with_body(body), Check::ResolvedShape);
    }

    #[test]
    fn a04_9_1a_wire_duplicate_header_never_matches() {
        let c = good();
        let headers = [
            ("Content-Type", "application/json"),
            ("ETag", c.etag.as_str()),
            ("etag", c.etag.as_str()),
            ("X-Nexus-Filter-Version", "1"),
        ];
        let meta = HttpReply {
            status: 200,
            headers: &[],
            body: c.meta.as_bytes(),
        };
        let res = HttpReply {
            status: 200,
            headers: &headers,
            body: &c.body,
        };
        assert_eq!(
            failed(evaluate(&meta, &res, Some(1), Some(&TestGrammar))),
            Check::Etag
        );
    }

    #[test]
    fn a04_9_1a_header_names_match_case_insensitively() {
        let c = good();
        let headers = [
            ("content-type", "application/json"),
            ("etag", c.etag.as_str()),
            ("x-nexus-filter-version", "1"),
        ];
        let meta = HttpReply {
            status: 200,
            headers: &[],
            body: c.meta.as_bytes(),
        };
        let res = HttpReply {
            status: 200,
            headers: &headers,
            body: &c.body,
        };
        assert!(matches!(
            evaluate(&meta, &res, Some(1), Some(&TestGrammar)),
            Evaluation::Valid(_)
        ));
    }

    // ---- A04-9.2, A04-9.3: process state -------------------------------------

    /// A04-9.3 "restart with only a cached filter": a new process has no
    /// way to adopt cached bytes; it is failed_closed until 9.1 holds.
    #[test]
    fn a04_9_3_a_new_process_is_failed_closed_until_9_1_holds() {
        let gate = BlobFilterGate::new();
        assert_eq!(gate.status(), BlobFilterStatus::FailedClosed);
        assert_eq!(gate.last_failure(), Some(Check::NotFetchedThisProcess));
        assert!(gate.valid_filter().is_none());
        assert!(!gate.routes_blob_lane());
        assert_eq!(
            BlobFilterGate::default().status(),
            BlobFilterStatus::FailedClosed
        );
    }

    #[test]
    fn a04_9_3_a_later_mismatch_drops_a_valid_filter() {
        let mut gate = BlobFilterGate::new();
        gate.apply(good().run(Some(&TestGrammar)));
        assert_eq!(gate.status(), BlobFilterStatus::Valid);
        let mut c = good();
        c.body = br#"{"grammar_version":1,"rules":[]}"#.to_vec();
        assert_eq!(
            gate.apply(c.run(Some(&TestGrammar))),
            BlobFilterStatus::FailedClosed
        );
        assert!(gate.valid_filter().is_none());
        assert_eq!(gate.last_failure(), Some(Check::BodySha));
        assert_eq!(gate.counters().failed_closed, 1);
    }

    #[test]
    fn a04_9_6_a_valid_filter_still_routes_nothing_to_the_blob_lane() {
        let mut gate = BlobFilterGate::new();
        assert_eq!(
            gate.apply(good().run(Some(&TestGrammar))),
            BlobFilterStatus::Valid
        );
        assert!(!gate.routes_blob_lane());
    }

    #[test]
    fn a04_9_2_status_serializes_as_failed_closed() {
        assert_eq!(
            serde_json::to_string(&BlobFilterStatus::FailedClosed).unwrap(),
            r#""failed_closed""#
        );
        assert_eq!(
            serde_json::to_string(&BlobFilterStatus::Valid).unwrap(),
            r#""valid""#
        );
    }

    #[test]
    fn a04_9_check_labels_are_unique() {
        let all = [
            Check::NotFetchedThisProcess,
            Check::MetadataStatus,
            Check::MetadataParse,
            Check::MetadataKeys,
            Check::MetadataVersion,
            Check::MetadataSha,
            Check::MetadataState,
            Check::PolicyVersionUnknown,
            Check::ResolvedStatus,
            Check::ContentType,
            Check::Etag,
            Check::VersionHeader,
            Check::BodySha,
            Check::DuplicateKey,
            Check::Parse,
            Check::CanonicalRender,
            Check::ResolvedShape,
            Check::RulesEquality,
            Check::GrammarUnsupported,
            Check::RuleInvalid,
        ];
        let labels: HashSet<&str> = all.iter().map(|c| c.label()).collect();
        assert_eq!(labels.len(), all.len());
        for c in all {
            let expect = matches!(
                c,
                Check::ResolvedStatus
                    | Check::ContentType
                    | Check::Etag
                    | Check::VersionHeader
                    | Check::BodySha
                    | Check::DuplicateKey
                    | Check::Parse
                    | Check::CanonicalRender
                    | Check::ResolvedShape
                    | Check::RulesEquality
            );
            assert_eq!(c.is_representation_mismatch(), expect, "{c:?}");
        }
    }

    /// No panic on any malformed input: a sweep of hostile bodies through
    /// both routes ends in a value every time.
    #[test]
    fn a04_9_no_panic_on_hostile_input() {
        let bodies: [&[u8]; 9] = [
            b"",
            b"\xff\xfe",
            b"null",
            b"{\"a\":",
            b"[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[",
            b"{\"grammar_version\":170141183460469231731687303715884105728,\"rules\":[]}",
            b"\"\\ud800\"",
            b"{\"grammar_version\":1,\"rules\":[1e400]}",
            b"{\"version\":18446744073709551616,\"sha\":\"\",\"state\":\"active\",\"rules\":[],\"per_rule_drop_counters\":{}}",
        ];
        for b in bodies {
            let meta = HttpReply {
                status: 200,
                headers: &[],
                body: b,
            };
            let res = HttpReply {
                status: 200,
                headers: &[("Content-Type", "application/json")],
                body: b,
            };
            let e = evaluate(&meta, &res, Some(1), Some(&TestGrammar));
            assert!(matches!(e, Evaluation::FailedClosed(_)), "{e:?}");
            let mut c = good();
            c.body = b.to_vec();
            let e = c.run(Some(&TestGrammar));
            assert!(matches!(e, Evaluation::FailedClosed(_)), "{e:?}");
        }
    }
}
