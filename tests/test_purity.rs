//! Fails when a pure module can reach an effect: `async`, the clock, an effectful part of `std`, a crate off the
//! allowlist, or a path that leaves `common`.

use std::path::{Path, PathBuf};

use proc_macro2::{Spacing, TokenStream, TokenTree};
use syn::visit::{self, Visit};

/// Crates a pure module may name, with `prop` for proptest's prelude alias; adding one is the review.
const PURE_CRATES: [&str; 4] = ["chrono", "chrono_tz", "proptest", "prop"];

/// The parts of `std` that reach outside the process's memory, `time` among them for its clocks.
const EFFECTFUL_STD_MODULES: [&str; 8] =
    ["env", "fs", "io", "net", "os", "process", "thread", "time"];

const PRIMITIVES: [&str; 17] = [
    "bool", "char", "str", "u8", "u16", "u32", "u64", "u128", "usize", "i8", "i16", "i32", "i64",
    "i128", "isize", "f32", "f64",
];

struct Checker {
    module: Vec<String>,
    /// The names each enclosing module binds itself, innermost last; a path's root resolves in the innermost.
    scopes: Vec<Vec<String>>,
    violations: Vec<String>,
}

impl Checker {
    fn check(&mut self, segments: &[String]) {
        let Some(root) = segments.first() else {
            return;
        };
        let second = segments.get(1).map(String::as_str);
        let outcome = match root.as_str() {
            "std" | "core" | "alloc" => match second {
                Some(module) if EFFECTFUL_STD_MODULES.contains(&module) => {
                    Err(format!("names `{root}::{module}`"))
                }
                _ => Ok(()),
            },
            "crate" => match second {
                Some("common") => Ok(()),
                _ => Err(format!("leaves common through `{}`", segments.join("::"))),
            },
            "super" => {
                let depth = segments
                    .iter()
                    .take_while(|segment| *segment == "super")
                    .count();
                if depth < self.module.len() {
                    Ok(())
                } else {
                    Err(format!("leaves common through `{}`", segments.join("::")))
                }
            }
            "self" | "Self" => Ok(()),
            name if name.starts_with(char::is_uppercase)
                || PURE_CRATES.contains(&name)
                || PRIMITIVES.contains(&name)
                || self
                    .scopes
                    .last()
                    .is_some_and(|scope| scope.iter().any(|scoped| scoped == name)) =>
            {
                Ok(())
            }
            name => Err(format!("names crate `{name}`")),
        };
        if let Err(violation) = outcome {
            self.violations.push(violation);
        }
        if segments.len() > 1 && segments.last().is_some_and(|last| last == "now") {
            self.violations
                .push(format!("reads the clock through `{}`", segments.join("::")));
        }
    }

    /// Syn leaves macro bodies unparsed, so their token trees are walked for paths and `async`/`await`.
    fn check_tokens(&mut self, tokens: TokenStream) {
        let trees: Vec<TokenTree> = tokens.into_iter().collect();
        let mut path: Vec<String> = Vec::new();
        let mut qualified = false;
        let mut after_separator = false;
        let mut after_generics = false;
        let mut index = 0;
        while index < trees.len() {
            let separator = matches!(
                (&trees[index], trees.get(index + 1)),
                (TokenTree::Punct(first), Some(TokenTree::Punct(second)))
                    if first.as_char() == ':' && first.spacing() == Spacing::Joint && second.as_char() == ':'
            );
            if separator {
                if path.is_empty() {
                    // `::` after a turbofish's `>` continues a type, not a root; otherwise it roots a path.
                    if after_generics {
                        path.push(String::new());
                    } else {
                        qualified = true;
                    }
                }
                after_separator = true;
                after_generics = false;
                index += 2;
                continue;
            }
            match &trees[index] {
                TokenTree::Ident(ident) => {
                    let word = ident.to_string();
                    if word == "async" || word == "await" {
                        self.violations.push(format!("uses `{word}` in a macro"));
                    }
                    if !after_separator {
                        self.flush(&mut path, &mut qualified);
                    }
                    path.push(word);
                    after_generics = false;
                }
                TokenTree::Group(group) => {
                    self.flush(&mut path, &mut qualified);
                    self.check_tokens(group.stream());
                    after_generics = false;
                }
                TokenTree::Punct(punct) => {
                    self.flush(&mut path, &mut qualified);
                    after_generics = punct.as_char() == '>';
                }
                TokenTree::Literal(_) => {
                    self.flush(&mut path, &mut qualified);
                    after_generics = false;
                }
            }
            after_separator = false;
            index += 1;
        }
        self.flush(&mut path, &mut qualified);
    }

    fn flush(&mut self, path: &mut Vec<String>, qualified: &mut bool) {
        let rooted = path.first().is_some_and(|root| !root.is_empty());
        if rooted && (path.len() > 1 || *qualified) {
            self.check(path);
        }
        path.clear();
        *qualified = false;
    }
}

impl Visit<'_> for Checker {
    fn visit_file(&mut self, file: &syn::File) {
        self.scopes.push(bindings(&file.items, None));
        visit::visit_file(self, file);
        self.scopes.pop();
    }

    fn visit_item_mod(&mut self, item: &syn::ItemMod) {
        self.module.push(item.ident.to_string());
        let scope = item
            .content
            .as_ref()
            .map(|(_, items)| bindings(items, self.scopes.last()));
        let entered = scope.is_some();
        self.scopes.extend(scope);
        visit::visit_item_mod(self, item);
        if entered {
            self.scopes.pop();
        }
        self.module.pop();
    }

    fn visit_item_extern_crate(&mut self, item: &syn::ItemExternCrate) {
        self.check(&[item.ident.to_string()]);
    }

    fn visit_item_use(&mut self, item: &syn::ItemUse) {
        for path in use_paths(&item.tree) {
            self.check(&path);
        }
    }

    fn visit_path(&mut self, path: &syn::Path) {
        if path.segments.len() > 1 || path.leading_colon.is_some() {
            let segments: Vec<String> = path
                .segments
                .iter()
                .map(|segment| segment.ident.to_string())
                .collect();
            self.check(&segments);
        }
        visit::visit_path(self, path);
    }

    fn visit_signature(&mut self, signature: &syn::Signature) {
        if signature.asyncness.is_some() {
            self.violations
                .push(format!("declares `async fn {}`", signature.ident));
        }
        visit::visit_signature(self, signature);
    }

    fn visit_expr_async(&mut self, expression: &syn::ExprAsync) {
        self.violations.push("uses an async block".to_string());
        visit::visit_expr_async(self, expression);
    }

    fn visit_expr_closure(&mut self, expression: &syn::ExprClosure) {
        if expression.asyncness.is_some() {
            self.violations.push("uses an async closure".to_string());
        }
        visit::visit_expr_closure(self, expression);
    }

    fn visit_expr_await(&mut self, expression: &syn::ExprAwait) {
        self.violations.push("uses `.await`".to_string());
        visit::visit_expr_await(self, expression);
    }

    fn visit_macro(&mut self, invocation: &syn::Macro) {
        if invocation
            .path
            .segments
            .last()
            .is_some_and(|segment| segment.ident == "include")
        {
            self.violations
                .push("includes unchecked source".to_string());
        }
        self.check_tokens(invocation.tokens.clone());
        visit::visit_macro(self, invocation);
    }
}

/// Every full path a `use` tree imports, groups expanded.
fn use_paths(tree: &syn::UseTree) -> Vec<Vec<String>> {
    match tree {
        syn::UseTree::Path(path) => use_paths(&path.tree)
            .into_iter()
            .map(|rest| [vec![path.ident.to_string()], rest].concat())
            .collect(),
        syn::UseTree::Name(name) => vec![vec![name.ident.to_string()]],
        syn::UseTree::Rename(rename) => vec![vec![rename.ident.to_string()]],
        syn::UseTree::Glob(_) => vec![vec![]],
        syn::UseTree::Group(group) => group.items.iter().flat_map(use_paths).collect(),
    }
}

/// The names a module's own items bind: child modules, imports as named or renamed, and extern crate aliases.
/// `use super::*` also binds everything the parent binds.
fn bindings(items: &[syn::Item], parent: Option<&Vec<String>>) -> Vec<String> {
    let mut names = Vec::new();
    for item in items {
        match item {
            syn::Item::Mod(module) => names.push(module.ident.to_string()),
            syn::Item::Use(import) => {
                if let syn::UseTree::Path(path) = &import.tree
                    && path.ident == "super"
                    && matches!(*path.tree, syn::UseTree::Glob(_))
                {
                    names.extend(parent.into_iter().flatten().cloned());
                }
                names.extend(use_bindings(&import.tree, None));
            }
            syn::Item::ExternCrate(extern_crate) => names.push(
                extern_crate
                    .rename
                    .as_ref()
                    .map_or(&extern_crate.ident, |(_, rename)| rename)
                    .to_string(),
            ),
            _ => {}
        }
    }
    names
}

/// The names a `use` tree binds, where `{self}` binds its parent.
fn use_bindings(tree: &syn::UseTree, parent: Option<&syn::Ident>) -> Vec<String> {
    match tree {
        syn::UseTree::Path(path) => use_bindings(&path.tree, Some(&path.ident)),
        syn::UseTree::Name(name) if name.ident == "self" => {
            parent.map(ToString::to_string).into_iter().collect()
        }
        syn::UseTree::Name(name) => vec![name.ident.to_string()],
        syn::UseTree::Rename(rename) => vec![rename.rename.to_string()],
        syn::UseTree::Glob(_) => Vec::new(),
        syn::UseTree::Group(group) => group
            .items
            .iter()
            .flat_map(|item| use_bindings(item, parent))
            .collect(),
    }
}

fn violations(source: &str, module: &[&str]) -> Vec<String> {
    let file = syn::parse_file(source).expect("pure module parses");
    let mut checker = Checker {
        module: module.iter().map(|segment| segment.to_string()).collect(),
        scopes: Vec::new(),
        violations: Vec::new(),
    };
    checker.visit_file(&file);
    checker.violations
}

fn rust_files(directory: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(directory).expect("directory reads") {
        let path = entry.expect("entry reads").path();
        if path.is_dir() {
            files.extend(rust_files(&path));
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            files.push(path);
        }
    }
    files
}

#[test]
fn test_pure_modules_reach_no_effect() {
    let source_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = rust_files(&source_root.join("common"));
    files.push(source_root.join("common.rs"));
    files.sort();
    let mut found = Vec::new();
    for file in &files {
        let relative = file.strip_prefix(&source_root).unwrap().with_extension("");
        let module: Vec<&str> = relative.iter().map(|part| part.to_str().unwrap()).collect();
        let source = std::fs::read_to_string(file).unwrap();
        for violation in violations(&source, &module) {
            found.push(format!("{}: {violation}", relative.display()));
        }
    }
    println!("checked {} files, {} violations", files.len(), found.len());
    assert!(files.len() >= 3, "only {} pure files found", files.len());
    assert_eq!(found, Vec::<String>::new());
}

#[test]
fn test_each_effect_is_named() {
    let cases = [
        ("use std::fs;", "names `std::fs`"),
        ("use std::{fmt, net::TcpStream};", "names `std::net`"),
        ("use std::time::Instant;", "names `std::time`"),
        ("use tokio::spawn;", "names crate `tokio`"),
        ("fn f() { ::reqwest::get(); }", "names crate `reqwest`"),
        (
            "use crate::archiver::Client;",
            "leaves common through `crate::archiver::Client`",
        ),
        (
            "use super::archiver;",
            "leaves common through `super::archiver`",
        ),
        (
            "fn f() { chrono::Utc::now(); }",
            "reads the clock through `chrono::Utc::now`",
        ),
        ("async fn f() {}", "declares `async fn f`"),
        ("fn f() { let _ = async {}; }", "uses an async block"),
        ("fn f() { let _ = async || 1; }", "uses an async closure"),
        ("fn f(g: G) { g.await; }", "uses `.await`"),
        (
            r#"fn f() { format!("{}", std::env::var("X")); }"#,
            "names `std::env`",
        ),
        (
            "fn f() { assert!(tokio::spawn(g)); }",
            "names crate `tokio`",
        ),
        (
            "fn f() { assert!(::tokio::spawn(g)); }",
            "names crate `tokio`",
        ),
        (
            r#"fn f() { assert!(c == '"', "{}", std::fs::read(x)); }"#,
            "names `std::fs`",
        ),
        (
            "mod nested { mod tokio {} } fn f() { tokio::spawn(); }",
            "names crate `tokio`",
        ),
        ("extern crate tokio as Runtime;", "names crate `tokio`"),
        (
            r#"fn f() { include!("../effectful.rs"); }"#,
            "includes unchecked source",
        ),
    ];
    for (source, expected) in cases {
        assert_eq!(violations(source, &["common"]), vec![expected], "{source}");
    }
}

#[test]
fn test_pure_paths_pass() {
    let source = r#"
        mod calendar { pub fn next() {} }
        use std::collections::BTreeMap;
        use chrono::{DateTime, Utc};
        impl std::fmt::Display for X {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { Ok(()) }
        }
        fn f() -> u32 {
            calendar::next();
            let _: Vec<u8> = Vec::<u8>::new().into_iter().collect::<Vec<_>>();
            assert_eq!(Self::now_or_never, "std::fs is only a string");
            u32::MAX
        }
        mod tests {
            use super::*;
            fn g() { calendar::next(); }
        }
    "#;
    let cases = [
        source,
        "use super::calendar;",
        r##"fn f() { concat!(r#"a "std::fs"#); }"##,
        r#"fn f() { assert!(c == '"' && Vec::<u8>::new().is_empty()); }"#,
        "use std::collections as maps; fn f() { maps::BTreeMap::<u8, u8>::new(); }",
        "use std::collections::{self}; fn f() { collections::BTreeMap::<u8, u8>::new(); }",
    ];
    for source in cases {
        assert_eq!(
            violations(source, &["common", "time"]),
            Vec::<String>::new(),
            "{source}"
        );
    }
}
