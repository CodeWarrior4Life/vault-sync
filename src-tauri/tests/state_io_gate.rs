//! Unit CS-2 (TKT-23091e72) gates 2 and 3, as a test (runs in
//! `cargo test --all-targets` on every CI leg).
//!
//! Gate 2 (exemptions): an `allow`/`expect`/`warn` of
//! `clippy::disallowed_methods` on any item, impl item, nested module, statement
//! or file (inner attribute) must sit on an item path listed in
//! `state_io_exemptions.toml`, and every listed path must still exist. Any
//! suppression of `clippy::style`, `clippy::all` or `warnings` (every attribute
//! form, including `cfg_attr`) is refused outside listed items and exactly
//! `#[cfg(test)]` code (C4). The direct dependency set must equal the
//! `[reviewed_deps]` table (C6).
//!
//! Gate 3 (layout confinement): outside `state_io` and `#[cfg(test)]`, no string
//! literal (including literals inside macro token streams) may contain
//! `sync-state`, `state-v2` or `.lattice-runtime`, or name one of the nine
//! state files; no `with_extension("lock")`; and every `.display(<one arg>)`
//! call site is listed (C7, keyed on CALL SHAPE, not on the receiver, G2).
//!
//! Structural invariants: exactly two `impl Sealed for` sites, PathAnchor in
//! durable_file and StateAnchor in state_io, and none inside a macro body (C8);
//! exactly one definition each of the temp-name format (`temp_name`) and the
//! outcome mapping (`classify_dir_sync`), both in durable_file (T-CS2-34); no
//! `fn fsync_dir` outside durable_file (T-CS2-20); exactly one caller of
//! `PushJournal::scratch()`, the D9 seed, and `scratch` is not `pub` (C10).
//!
//! `STATE_IO_GATE_ROOT` (a src-tauri dir) points the tree checks at another
//! tree; the negcontrol script uses it on planted scratch copies.
#![allow(clippy::disallowed_methods)]

use proc_macro2::{Delimiter, TokenStream, TokenTree};
use quote_free::tokens_to_string;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use syn::visit::Visit;

/// `quote` is not a dependency; stringify tokens with proc-macro2's Display.
mod quote_free {
    pub fn tokens_to_string(t: &impl std::fmt::Display) -> String {
        t.to_string()
            .chars()
            .filter(|c| !c.is_whitespace())
            .collect()
    }
}

const STATE_FILE_NAMES: &[&str] = &[
    "push_journal.jsonl",
    "push_journal.lock",
    "shadow_hashes.json",
    "base_seq.json",
    "read_receipt.json",
    "last_event_id",
    "delete-burst-SUPPRESSED",
    "delete-burst-PAUSED",
    "reconcile-retry-ledger.json",
];

const DISALLOWED_LINTS: &[&str] = &["clippy::disallowed_methods", "clippy::disallowed_method"];
const GROUP_LINTS: &[&str] = &["clippy::style", "clippy::all", "warnings"];

// ---------------------------------------------------------------------------
// Exemptions
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Clone)]
struct Exemptions {
    items: BTreeMap<String, String>,
    display_sites: BTreeMap<String, String>,
    allowed_literals: BTreeMap<String, String>,
    reviewed_deps: BTreeSet<String>,
}

fn parse_exemptions(text: &str) -> Exemptions {
    let v: toml::Value = toml::from_str(text).expect("state_io_exemptions.toml parses");
    let table = |k: &str| -> BTreeMap<String, toml::Value> {
        v.get(k)
            .and_then(|t| t.as_table())
            .map(|t| t.iter().map(|(a, b)| (a.clone(), b.clone())).collect())
            .unwrap_or_default()
    };
    let strs = |k: &str| -> BTreeMap<String, String> {
        table(k)
            .into_iter()
            .map(|(a, b)| {
                let r = b.as_str().unwrap_or_default().to_string();
                assert!(!r.trim().is_empty(), "[{k}] {a}: a reason is required");
                (a, r)
            })
            .collect()
    };
    Exemptions {
        items: strs("exemptions"),
        display_sites: strs("display_sites"),
        allowed_literals: table("allowed_literals")
            .into_iter()
            .map(|(a, b)| {
                let lit = b
                    .get("literal")
                    .and_then(|l| l.as_str())
                    .unwrap()
                    .to_string();
                assert!(b.get("reason").and_then(|r| r.as_str()).is_some());
                (a, lit)
            })
            .collect(),
        reviewed_deps: table("reviewed_deps").into_keys().collect(),
    }
}

// ---------------------------------------------------------------------------
// The scanner
// ---------------------------------------------------------------------------

/// What the scan saw, for the whole-tree invariants.
#[derive(Debug, Default)]
struct Facts {
    seen_items: BTreeSet<String>,
    sealed_impls: Vec<(String, String)>,
    fn_defs: Vec<(String, String)>,
    scratch_callers: Vec<String>,
    violations: Vec<String>,
}

struct Scanner<'a> {
    ex: &'a Exemptions,
    facts: &'a mut Facts,
    /// Module path segments (file module first).
    stack: Vec<String>,
    /// Depth of `#[cfg(test)]` ancestry.
    in_test: usize,
    file: String,
}

fn is_cfg_test(attrs: &[syn::Attribute]) -> bool {
    attrs
        .iter()
        .any(|a| tokens_to_string(&ToTokensDisplay(a)) == "#[cfg(test)]")
}

/// Display adapter so an attribute can be stringified without `quote`.
struct ToTokensDisplay<'a>(&'a syn::Attribute);
impl std::fmt::Display for ToTokensDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", quote::ToTokens::to_token_stream(self.0))
    }
}

/// The lint names suppressed by an attribute (`allow`/`expect`/`warn`,
/// directly or anywhere inside `cfg_attr`).
fn suppressed_lints(a: &syn::Attribute) -> Vec<String> {
    let s = tokens_to_string(&ToTokensDisplay(a));
    let mut out = Vec::new();
    for kw in ["allow(", "expect(", "warn("] {
        let mut rest = s.as_str();
        while let Some(i) = rest.find(kw) {
            // Word boundary: the char before must not be an ident char.
            let prev_ok = i == 0
                || !rest[..i]
                    .chars()
                    .last()
                    .is_some_and(|c| c.is_alphanumeric() || c == '_');
            let after = &rest[i + kw.len()..];
            if prev_ok {
                let mut depth = 0;
                let mut end = after.len();
                for (j, c) in after.char_indices() {
                    match c {
                        '(' => depth += 1,
                        ')' if depth == 0 => {
                            end = j;
                            break;
                        }
                        ')' => depth -= 1,
                        _ => {}
                    }
                }
                out.extend(after[..end].split(',').map(|x| x.to_string()));
            }
            rest = after;
        }
    }
    out
}

impl Scanner<'_> {
    fn path(&self) -> String {
        self.stack.join("::")
    }
    fn module_root(&self) -> &str {
        self.stack.first().map(|s| s.as_str()).unwrap_or("")
    }
    fn in_state_io(&self) -> bool {
        self.module_root() == "state_io"
    }
    fn violation(&mut self, what: String) {
        self.facts
            .violations
            .push(format!("{}: {} [{}]", self.file, what, self.path()));
    }

    fn with_item<F: FnOnce(&mut Self)>(&mut self, name: String, attrs: &[syn::Attribute], f: F) {
        let test = is_cfg_test(attrs);
        self.stack.push(name);
        if test {
            self.in_test += 1;
        }
        if self.in_test == 0 {
            self.facts.seen_items.insert(self.path());
        }
        f(self);
        if test {
            self.in_test -= 1;
        }
        self.stack.pop();
    }

    /// The listed item governing the current position: the innermost
    /// ancestor path that is listed.
    fn listed(&self, map: &BTreeMap<String, String>) -> bool {
        (1..=self.stack.len()).any(|n| map.contains_key(&self.stack[..n].join("::")))
    }

    fn check_literal(&mut self, lit: &str) {
        if self.in_test > 0 || self.in_state_io() {
            return;
        }
        let item = self.path();
        let allowed = self
            .ex
            .allowed_literals
            .iter()
            .any(|(p, l)| l == lit && item.starts_with(p.as_str()));
        if lit.contains("sync-state") || lit.contains("state-v2") {
            self.violation(format!("layout literal {lit:?} outside state_io (gate 3)"));
        }
        if lit.contains(".lattice-runtime") {
            self.violation(format!(
                "{lit:?}: use state_io::layout::RUNTIME_DIR_NAME (gate 3)"
            ));
        }
        for n in STATE_FILE_NAMES {
            let hit =
                lit == *n || lit.contains(&format!("/{n}")) || lit.contains(&format!("\\{n}"));
            if hit && !allowed {
                self.violation(format!(
                    "state file name literal {lit:?} outside state_io (C7)"
                ));
            }
        }
    }

    fn check_display_call(&mut self) {
        if self.in_test > 0 || self.in_state_io() {
            return;
        }
        if !self.listed(&self.ex.display_sites.clone()) {
            self.violation("unlisted .display(<arg>) call site (C7)".into());
        }
    }

    /// Walk a macro body: string literals, one-argument `.display(..)` calls,
    /// `with_extension("lock")`, `impl .. Sealed for` and scratch callers.
    fn scan_tokens(&mut self, ts: TokenStream) {
        let toks: Vec<TokenTree> = ts.into_iter().collect();
        for (i, t) in toks.iter().enumerate() {
            match t {
                TokenTree::Group(g) => self.scan_tokens(g.stream()),
                TokenTree::Literal(l) => {
                    if let Ok(s) = syn::parse_str::<syn::LitStr>(&l.to_string()) {
                        self.check_literal(&s.value());
                    }
                }
                TokenTree::Ident(id) => {
                    let name = id.to_string();
                    let after_dot =
                        i > 0 && matches!(&toks[i - 1], TokenTree::Punct(p) if p.as_char() == '.');
                    let group = match toks.get(i + 1) {
                        Some(TokenTree::Group(g)) if g.delimiter() == Delimiter::Parenthesis => {
                            Some(g.stream())
                        }
                        _ => None,
                    };
                    if after_dot && name == "display" {
                        if let Some(args) = &group {
                            if one_arg(args) {
                                self.check_display_call();
                            }
                        }
                    }
                    if after_dot && name == "with_extension" {
                        if let Some(args) = &group {
                            if tokens_to_string(args) == "\"lock\"" {
                                self.with_extension_lock();
                            }
                        }
                    }
                    if name == "impl" && self.in_test == 0 {
                        let rest: Vec<String> = toks[i + 1..]
                            .iter()
                            .take_while(|t| !matches!(t, TokenTree::Group(g) if g.delimiter() == Delimiter::Brace))
                            .map(|t| t.to_string())
                            .collect();
                        if rest.iter().any(|t| t == "Sealed") && rest.iter().any(|t| t == "for") {
                            self.violation("`impl Sealed for` inside a macro body (C8)".into());
                        }
                    }
                    if name == "scratch"
                        && i >= 3
                        && toks[i - 3].to_string() == "PushJournal"
                        && self.in_test == 0
                    {
                        let p = self.path();
                        self.facts.scratch_callers.push(p);
                    }
                }
                TokenTree::Punct(_) => {}
            }
        }
    }

    fn with_extension_lock(&mut self) {
        if self.in_test == 0 && !self.in_state_io() {
            self.violation("with_extension(\"lock\") outside state_io (C7)".into());
        }
    }
}

fn one_arg(args: &TokenStream) -> bool {
    let toks: Vec<TokenTree> = args.clone().into_iter().collect();
    !toks.is_empty()
        && !toks
            .iter()
            .any(|t| matches!(t, TokenTree::Punct(p) if p.as_char() == ','))
}

fn type_name(ty: &syn::Type) -> String {
    match ty {
        syn::Type::Path(p) => p
            .path
            .segments
            .last()
            .map(|s| s.ident.to_string())
            .unwrap_or_default(),
        syn::Type::Reference(r) => type_name(&r.elem),
        other => tokens_to_string(&TypeDisplay(other)),
    }
}

struct TypeDisplay<'a>(&'a syn::Type);
impl std::fmt::Display for TypeDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", quote::ToTokens::to_token_stream(self.0))
    }
}

impl<'ast> Visit<'ast> for Scanner<'_> {
    fn visit_attribute(&mut self, a: &'ast syn::Attribute) {
        // Doc attributes are skipped (gate 3); suppressions are checked.
        if a.path().is_ident("doc") {
            return;
        }
        let lints = suppressed_lints(a);
        if self.in_test == 0 {
            let listed = self.listed(&self.ex.items.clone());
            if lints.iter().any(|l| DISALLOWED_LINTS.contains(&l.as_str())) && !listed {
                self.violation(format!(
                    "unlisted suppression of clippy::disallowed_methods (gate 2): {}",
                    tokens_to_string(&ToTokensDisplay(a))
                ));
            }
            if lints.iter().any(|l| GROUP_LINTS.contains(&l.as_str())) && !listed {
                self.violation(format!(
                    "lint-group suppression silences disallowed_methods (C4): {}",
                    tokens_to_string(&ToTokensDisplay(a))
                ));
            }
        }
        syn::visit::visit_attribute(self, a);
    }

    fn visit_item_fn(&mut self, n: &'ast syn::ItemFn) {
        let name = n.sig.ident.to_string();
        let module = self.path();
        if self.in_test == 0 && !is_cfg_test(&n.attrs) {
            self.facts.fn_defs.push((module.clone(), name.clone()));
            if name == "scratch" && matches!(n.vis, syn::Visibility::Public(_)) {
                self.violation("PushJournal::scratch must not be `pub` (C10)".into());
            }
        }
        self.with_item(name, &n.attrs, |s| syn::visit::visit_item_fn(s, n));
    }

    fn visit_impl_item_fn(&mut self, n: &'ast syn::ImplItemFn) {
        let name = n.sig.ident.to_string();
        if self.in_test == 0 && !is_cfg_test(&n.attrs) {
            let owner = self.path();
            self.facts.fn_defs.push((owner, name.clone()));
            if name == "scratch" && matches!(n.vis, syn::Visibility::Public(_)) {
                self.violation("PushJournal::scratch must not be `pub` (C10)".into());
            }
        }
        self.with_item(name, &n.attrs, |s| syn::visit::visit_impl_item_fn(s, n));
    }

    fn visit_item_impl(&mut self, n: &'ast syn::ItemImpl) {
        let ty = type_name(&n.self_ty);
        if let Some((_, tr, _)) = &n.trait_ {
            if tr.segments.last().is_some_and(|s| s.ident == "Sealed")
                && self.in_test == 0
                && !is_cfg_test(&n.attrs)
            {
                let m = self.path();
                self.facts.sealed_impls.push((m, ty.clone()));
            }
        }
        // An impl block does not add an item of its own; its methods are
        // `<module>::<Type>::<method>`.
        let test = is_cfg_test(&n.attrs);
        if test {
            self.in_test += 1;
        }
        self.stack.push(ty);
        syn::visit::visit_item_impl(self, n);
        self.stack.pop();
        if test {
            self.in_test -= 1;
        }
    }

    fn visit_item_mod(&mut self, n: &'ast syn::ItemMod) {
        self.with_item(n.ident.to_string(), &n.attrs, |s| {
            syn::visit::visit_item_mod(s, n)
        });
    }

    fn visit_item_struct(&mut self, n: &'ast syn::ItemStruct) {
        self.with_item(n.ident.to_string(), &n.attrs, |s| {
            syn::visit::visit_item_struct(s, n)
        });
    }
    fn visit_item_enum(&mut self, n: &'ast syn::ItemEnum) {
        self.with_item(n.ident.to_string(), &n.attrs, |s| {
            syn::visit::visit_item_enum(s, n)
        });
    }
    fn visit_item_const(&mut self, n: &'ast syn::ItemConst) {
        self.with_item(n.ident.to_string(), &n.attrs, |s| {
            syn::visit::visit_item_const(s, n)
        });
    }
    fn visit_item_static(&mut self, n: &'ast syn::ItemStatic) {
        self.with_item(n.ident.to_string(), &n.attrs, |s| {
            syn::visit::visit_item_static(s, n)
        });
    }
    fn visit_item_trait(&mut self, n: &'ast syn::ItemTrait) {
        self.with_item(n.ident.to_string(), &n.attrs, |s| {
            syn::visit::visit_item_trait(s, n)
        });
    }

    fn visit_lit_str(&mut self, l: &'ast syn::LitStr) {
        self.check_literal(&l.value());
    }

    fn visit_macro(&mut self, m: &'ast syn::Macro) {
        self.scan_tokens(m.tokens.clone());
        syn::visit::visit_macro(self, m);
    }

    fn visit_expr_method_call(&mut self, n: &'ast syn::ExprMethodCall) {
        if n.method == "display" && n.args.len() == 1 {
            self.check_display_call();
        }
        if n.method == "with_extension" && n.args.len() == 1 {
            if let Some(syn::Expr::Lit(syn::ExprLit {
                lit: syn::Lit::Str(s),
                ..
            })) = n.args.first()
            {
                if s.value() == "lock" {
                    self.with_extension_lock();
                }
            }
        }
        syn::visit::visit_expr_method_call(self, n);
    }

    fn visit_expr_call(&mut self, n: &'ast syn::ExprCall) {
        if let syn::Expr::Path(p) = &*n.func {
            let segs: Vec<String> = p
                .path
                .segments
                .iter()
                .map(|s| s.ident.to_string())
                .collect();
            if segs.ends_with(&["PushJournal".to_string(), "scratch".to_string()])
                && self.in_test == 0
            {
                let path = self.path();
                self.facts.scratch_callers.push(path);
            }
        }
        syn::visit::visit_expr_call(self, n);
    }
}

/// Scan one parsed file as module `module`.
fn scan_file(module: &str, file_label: &str, text: &str, ex: &Exemptions, facts: &mut Facts) {
    let f = match syn::parse_file(text) {
        Ok(f) => f,
        Err(e) => {
            facts
                .violations
                .push(format!("{file_label}: does not parse: {e}"));
            return;
        }
    };
    let mut s = Scanner {
        ex,
        facts,
        stack: vec![module.to_string()],
        in_test: 0,
        file: file_label.to_string(),
    };
    s.facts.seen_items.insert(module.to_string());
    s.visit_file(&f);
}

/// Files included as `#[cfg(test)] #[path = "x.rs"] mod ...;` are test code.
fn test_only_files(text: &str) -> Vec<String> {
    let Ok(f) = syn::parse_file(text) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for item in f.items {
        if let syn::Item::Mod(m) = item {
            if m.content.is_none() && is_cfg_test(&m.attrs) {
                for a in &m.attrs {
                    if a.path().is_ident("path") {
                        if let syn::Meta::NameValue(nv) = &a.meta {
                            if let syn::Expr::Lit(syn::ExprLit {
                                lit: syn::Lit::Str(s),
                                ..
                            }) = &nv.value
                            {
                                out.push(s.value());
                            }
                        }
                    }
                }
            }
        }
    }
    out
}

fn root() -> PathBuf {
    std::env::var_os("STATE_IO_GATE_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")))
}

/// `(module, label, text)` for every non-test source under `<root>/src`.
fn tree_sources(root: &Path) -> Vec<(String, String, String)> {
    let src = root.join("src");
    let mut files: Vec<PathBuf> = walk(&src);
    files.sort();
    let mut test_files: BTreeSet<PathBuf> = BTreeSet::new();
    for f in &files {
        let text = std::fs::read_to_string(f).unwrap();
        for t in test_only_files(&text) {
            test_files.insert(f.parent().unwrap().join(t));
        }
    }
    files
        .into_iter()
        .filter(|f| !test_files.contains(f))
        .map(|f| {
            let rel = f.strip_prefix(&src).unwrap().with_extension("");
            let module = rel
                .components()
                .map(|c| c.as_os_str().to_string_lossy().into_owned())
                .collect::<Vec<_>>()
                .join("::");
            let text = std::fs::read_to_string(&f).unwrap();
            (module, f.display().to_string(), text)
        })
        .collect()
}

fn walk(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    for e in std::fs::read_dir(dir).unwrap() {
        let p = e.unwrap().path();
        if p.is_dir() {
            out.extend(walk(&p));
        } else if p.extension().is_some_and(|x| x == "rs") {
            out.push(p);
        }
    }
    out
}

fn scan_sources(srcs: &[(String, String, String)], ex: &Exemptions) -> Facts {
    let mut facts = Facts::default();
    for (m, l, t) in srcs {
        scan_file(m, l, t, ex, &mut facts);
    }
    facts
}

/// Stale listings and the whole-tree structural invariants.
fn tree_invariants(facts: &Facts, ex: &Exemptions) -> Vec<String> {
    let mut v = Vec::new();
    for p in ex.items.keys().chain(ex.display_sites.keys()) {
        if !facts.seen_items.contains(p) {
            v.push(format!(
                "stale exemption: listed item {p:?} no longer exists"
            ));
        }
    }
    for p in ex.allowed_literals.keys() {
        if !facts.seen_items.contains(p) {
            v.push(format!(
                "stale allowed literal: item {p:?} no longer exists"
            ));
        }
    }
    let mut sealed: Vec<(String, String)> = facts.sealed_impls.clone();
    sealed.sort();
    let want = vec![
        ("durable_file".to_string(), "PathAnchor".to_string()),
        ("state_io".to_string(), "StateAnchor".to_string()),
    ];
    if sealed != want {
        v.push(format!(
            "C8: exactly two `impl Sealed for` sites (PathAnchor in durable_file, StateAnchor in state_io) expected, found {sealed:?}"
        ));
    }
    for (name, home) in [
        ("temp_name", "durable_file"),
        ("classify_dir_sync", "durable_file"),
    ] {
        let defs: Vec<&(String, String)> =
            facts.fn_defs.iter().filter(|(_, n)| n == name).collect();
        if defs.len() != 1 || defs[0].0 != home {
            v.push(format!(
                "T-CS2-34: exactly one `fn {name}` expected, in {home}; found {defs:?}"
            ));
        }
    }
    for (m, n) in &facts.fn_defs {
        if n == "fsync_dir" && m != "durable_file" {
            v.push(format!(
                "T-CS2-20: `fn fsync_dir` defined outside durable_file, in {m}"
            ));
        }
    }
    if facts.scratch_callers != vec!["reconciliation::seed_shadow_from_server_if_empty".to_string()]
    {
        v.push(format!(
            "C10: PushJournal::scratch() must have exactly one caller, the D9 seed; found {:?}",
            facts.scratch_callers
        ));
    }
    v
}

/// C6: the direct dependency set equals the `[reviewed_deps]` table.
fn reviewed_deps_violations(cargo_toml: &str, ex: &Exemptions) -> Vec<String> {
    let v: toml::Value = toml::from_str(cargo_toml).unwrap();
    let mut deps: BTreeSet<String> = v
        .get("dependencies")
        .and_then(|d| d.as_table())
        .map(|t| t.keys().cloned().collect())
        .unwrap_or_default();
    if let Some(targets) = v.get("target").and_then(|t| t.as_table()) {
        for (_, tv) in targets {
            if let Some(d) = tv.get("dependencies").and_then(|d| d.as_table()) {
                deps.extend(d.keys().cloned());
            }
        }
    }
    let mut out = Vec::new();
    for d in deps.difference(&ex.reviewed_deps) {
        out.push(format!(
            "C6: dependency {d:?} has no [reviewed_deps] line (does it write files?)"
        ));
    }
    for d in ex.reviewed_deps.difference(&deps) {
        out.push(format!(
            "C6: [reviewed_deps] {d:?} is not a direct dependency"
        ));
    }
    out
}

fn tree_exemptions(root: &Path) -> Exemptions {
    parse_exemptions(&std::fs::read_to_string(root.join("state_io_exemptions.toml")).unwrap())
}

// ---------------------------------------------------------------------------
// The gate on THIS tree
// ---------------------------------------------------------------------------

#[test]
fn gate_holds_on_this_tree() {
    let root = root();
    let ex = tree_exemptions(&root);
    let facts = scan_sources(&tree_sources(&root), &ex);
    let mut v = facts.violations.clone();
    v.extend(tree_invariants(&facts, &ex));
    v.extend(reviewed_deps_violations(
        &std::fs::read_to_string(root.join("Cargo.toml")).unwrap(),
        &ex,
    ));
    assert!(
        v.is_empty(),
        "CS-2 state_io gate violations ({}):\n{}",
        v.len(),
        v.join("\n")
    );
}

// ---------------------------------------------------------------------------
// Fixture tests: each planted violation must be caught
// ---------------------------------------------------------------------------

fn ex_fixture() -> Exemptions {
    parse_exemptions(
        r#"
[exemptions]
"listed::ok_fn" = "fixture"
[display_sites]
"listed::shows" = "fixture"
[allowed_literals]
[reviewed_deps]
serde = "x"
"#,
    )
}

fn scan_one(module: &str, text: &str) -> Vec<String> {
    let ex = ex_fixture();
    scan_sources(&[(module.into(), format!("{module}.rs"), text.into())], &ex).violations
}

/// T-CS2-9 (gate 2): an unlisted allow on a non-test item fails, on an impl
/// method and inside a nested module too; a stale listed item fails; and
/// (C4) every lint-group/attribute form fails while the same attribute under
/// exactly `#[cfg(test)]` passes.
#[test]
fn t_cs2_9_exemption_gate() {
    let unlisted = [
        "#[allow(clippy::disallowed_methods)] fn f() {}",
        "struct S; impl S { #[allow(clippy::disallowed_methods)] fn m(&self) {} }",
        "mod inner { #[allow(clippy::disallowed_methods)] fn f() {} }",
        "fn f() { #[allow(clippy::disallowed_methods)] let _x = 1; }",
        "#[allow(clippy::style)] fn f() {}",
        "#[allow(clippy::all)] fn f() {}",
        "#[allow(warnings)] fn f() {}",
        "#[expect(clippy::disallowed_methods)] fn f() {}",
        "#[warn(clippy::disallowed_methods)] fn f() {}",
        "#![allow(clippy::all)] fn f() {}",
        "#[cfg_attr(unix, allow(clippy::disallowed_methods))] fn f() {}",
    ];
    for plant in unlisted {
        assert!(
            !scan_one("lib", plant).is_empty(),
            "planted suppression not caught: {plant}"
        );
    }
    for ok in [
        "#[cfg(test)] #[allow(clippy::disallowed_methods)] fn f() {}",
        "#[cfg(test)] mod tests { #![allow(clippy::all)] #[allow(warnings)] fn f() {} }",
        "#[cfg(test)] #[cfg_attr(unix, allow(clippy::disallowed_methods))] fn f() {}",
    ] {
        assert!(scan_one("lib", ok).is_empty(), "false positive: {ok}");
    }
    // A listed item passes; listing a removed item is stale.
    let ex = ex_fixture();
    let facts = scan_sources(
        &[(
            "listed".into(),
            "listed.rs".into(),
            "#[allow(clippy::disallowed_methods)] fn ok_fn() {} fn shows() {}".into(),
        )],
        &ex,
    );
    assert!(facts.violations.is_empty(), "{:?}", facts.violations);
    let facts = scan_sources(
        &[("listed".into(), "listed.rs".into(), "fn shows() {}".into())],
        &ex,
    );
    assert!(tree_invariants(&facts, &ex)
        .iter()
        .any(|v| v.contains("stale exemption") && v.contains("listed::ok_fn")));
}

/// T-CS2-10 (gate 3): `"sync-state"` in lib.rs and the same inside a macro
/// token stream fail; a doc comment mentioning sync-state passes.
#[test]
fn t_cs2_10_layout_literals() {
    assert!(!scan_one("lib", r#"fn f() -> &'static str { "sync-state" }"#).is_empty());
    assert!(!scan_one(
        "lib",
        r#"fn f(ws: &str) -> String { format!("{}/sync-state", ws) }"#
    )
    .is_empty());
    assert!(!scan_one("lib", r#"fn f() { tracing::info!("x {}", "state-v2"); }"#).is_empty());
    assert!(!scan_one("lib", r#"const X: &str = ".lattice-runtime";"#).is_empty());
    assert!(scan_one(
        "lib",
        "/// lives in sync-state/ beside the journal\nfn f() {}"
    )
    .is_empty());
    assert!(scan_one("state_io", r#"const S: &str = "sync-state";"#).is_empty());
}

/// T-CS2-33 (C7): a state file name literal, `with_extension("lock")`, an
/// unlisted one-argument `.display(..)` and the rename plant all fail; a
/// listed display site and a zero-argument `Path::display()` pass.
#[test]
fn t_cs2_33_literals_and_display_sites() {
    for plant in [
        r#"fn f() -> &'static str { "base_seq.json" }"#,
        r#"fn f(p: &std::path::Path) -> std::path::PathBuf { p.with_extension("lock") }"#,
        r#"fn f(sd: &S) -> String { sd.display(None).to_string() }"#,
        r#"fn f(sd: S) -> String { let x = sd; x.display(None).to_string() }"#,
        r#"fn f(sd: &S) { tracing::warn!(m = %sd.display(Some(F)), "x"); }"#,
        r#"fn f(w: &str) -> String { format!("{w}/push_journal.jsonl") }"#,
    ] {
        assert!(
            !scan_one("commands", plant).is_empty(),
            "not caught: {plant}"
        );
    }
    assert!(scan_one(
        "listed",
        r#"fn shows(sd: &S) -> String { sd.display(None).to_string() }"#
    )
    .is_empty());
    assert!(scan_one(
        "commands",
        r#"fn f(p: &std::path::Path) -> String { p.display().to_string() }"#
    )
    .is_empty());
    // A log string that merely mentions a name is not a path literal.
    assert!(scan_one(
        "sse",
        r#"fn f() { let _ = "last_event_id: persist failed"; }"#
    )
    .is_empty());
}

/// T-CS2-32 (C6): a new dependency without a `[reviewed_deps]` line fails,
/// and so does a reviewed line for a crate that is no longer a dependency.
#[test]
fn t_cs2_32_reviewed_deps() {
    let ex = ex_fixture();
    assert!(reviewed_deps_violations("[dependencies]\nserde = \"1\"\n", &ex).is_empty());
    let added = reviewed_deps_violations("[dependencies]\nserde = \"1\"\nnewcrate = \"1\"\n", &ex);
    assert!(added.iter().any(|v| v.contains("newcrate")), "{added:?}");
    let removed = reviewed_deps_violations("[dependencies]\n", &ex);
    assert!(removed.iter().any(|v| v.contains("serde")), "{removed:?}");
    let target = reviewed_deps_violations(
        "[dependencies]\nserde = \"1\"\n[target.'cfg(unix)'.dependencies]\nlibc = \"0.2\"\n",
        &ex,
    );
    assert!(target.iter().any(|v| v.contains("libc")), "{target:?}");
}

fn tree_with(extra: &[(&str, &str)]) -> (Facts, Exemptions) {
    let root = root();
    let ex = tree_exemptions(&root);
    let mut srcs = tree_sources(&root);
    for (m, t) in extra {
        srcs.push((m.to_string(), format!("{m}.rs (planted)"), t.to_string()));
    }
    (scan_sources(&srcs, &ex), ex)
}

/// T-CS2-34 (C8): a third `Anchor`/`Sealed` implementation fails, an
/// `impl Sealed for` inside a macro body fails, and a copy of the outcome
/// mapping outside durable_file fails.
#[test]
fn t_cs2_34_one_copy() {
    let (f, ex) = tree_with(&[(
        "plant",
        "struct Third; impl crate::durable_file::sealed::Sealed for Third {}",
    )]);
    assert!(tree_invariants(&f, &ex).iter().any(|v| v.contains("C8")));
    let (f, ex) = tree_with(&[(
        "plant",
        "fn classify_dir_sync() {} macro_rules! m { () => { impl Sealed for X {} } }",
    )]);
    let all: Vec<String> = f
        .violations
        .iter()
        .cloned()
        .chain(tree_invariants(&f, &ex))
        .collect();
    assert!(
        all.iter().any(|v| v.contains("classify_dir_sync")),
        "{all:?}"
    );
    assert!(all.iter().any(|v| v.contains("macro body")), "{all:?}");
}

/// T-CS2-20 (CS-1 C8): `fn fsync_dir` has no definition outside
/// durable_file; a planted local copy fails.
#[test]
fn t_cs2_20_one_fsync_dir() {
    let (f, ex) = tree_with(&[]);
    assert!(!tree_invariants(&f, &ex)
        .iter()
        .any(|v| v.contains("T-CS2-20")));
    assert!(f
        .fn_defs
        .iter()
        .any(|(m, n)| m == "durable_file" && n == "fsync_dir"));
    let (f, ex) = tree_with(&[("sync_shadow_plant", "fn fsync_dir(_d: &std::path::Path) {}")]);
    assert!(tree_invariants(&f, &ex)
        .iter()
        .any(|v| v.contains("T-CS2-20")));
}

/// T-CS2-35 (C10, structural half): exactly one caller of
/// `PushJournal::scratch()`, in the D9 seed; a second caller fails, and so
/// does making `scratch` `pub`.
#[test]
fn t_cs2_35_scratch_single_caller() {
    let (f, _) = tree_with(&[]);
    assert_eq!(
        f.scratch_callers,
        vec!["reconciliation::seed_shadow_from_server_if_empty".to_string()]
    );
    let (f, ex) = tree_with(&[(
        "commands_plant",
        "fn g() { let _j = crate::push_journal::PushJournal::scratch(); }",
    )]);
    assert!(tree_invariants(&f, &ex).iter().any(|v| v.contains("C10")));
    assert!(!scan_one(
        "push_journal",
        "impl PushJournal { pub fn scratch() -> Self { todo!() } }"
    )
    .is_empty());
}
