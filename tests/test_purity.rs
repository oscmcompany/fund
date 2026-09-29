//! Fails when a pure module can reach an effect: `async`, the clock, an effectful part of `std`, a crate off the
//! allowlist, or a path that leaves `common`. Capability generics show an effect in a signature but cannot forbid an
//! ambient call, so this check is what enforces purity.

use std::path::{Path, PathBuf};

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

/// Collects the lowercase names a file brings into scope itself: its modules and its imports.
#[derive(Default)]
struct Scope {
    names: Vec<String>,
}

impl Visit<'_> for Scope {
    fn visit_item_mod(&mut self, item: &syn::ItemMod) {
        self.names.push(item.ident.to_string());
        visit::visit_item_mod(self, item);
    }

    fn visit_item_use(&mut self, item: &syn::ItemUse) {
        for path in use_paths(&item.tree) {
            self.names.extend(path.last().cloned());
        }
    }
}

struct Checker {
    module: Vec<String>,
    scope: Vec<String>,
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
                || self.scope.iter().any(|scoped| scoped == name) =>
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

    /// Macro bodies are not parsed, so their tokens are scanned for paths and `async`/`await`.
    fn check_tokens(&mut self, tokens: &str) {
        for path in token_paths(tokens) {
            match path.as_slice() {
                [word] if word == "async" || word == "await" => {
                    self.violations.push(format!("uses `{word}` in a macro"))
                }
                [_] => {}
                _ => self.check(&path),
            }
        }
    }
}

impl Visit<'_> for Checker {
    fn visit_item_mod(&mut self, item: &syn::ItemMod) {
        self.module.push(item.ident.to_string());
        visit::visit_item_mod(self, item);
        self.module.pop();
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
        self.check_tokens(&invocation.tokens.to_string());
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

/// The `a::b::c` runs in a macro's printed tokens, skipping string literals and runs that follow `::`.
fn token_paths(tokens: &str) -> Vec<Vec<String>> {
    let mut paths = Vec::new();
    let mut current: Vec<String> = Vec::new();
    let mut after_separator = false;
    let mut characters = tokens.chars().peekable();
    while let Some(character) = characters.next() {
        if character.is_alphanumeric() || character == '_' {
            let mut word = character.to_string();
            while let Some(next) = characters.next_if(|next| next.is_alphanumeric() || *next == '_')
            {
                word.push(next);
            }
            if current.is_empty() || after_separator {
                current.push(word);
            } else {
                paths.push(std::mem::replace(&mut current, vec![word]));
            }
            after_separator = false;
        } else if character == ':' && characters.next_if_eq(&':').is_some() {
            if current.is_empty() {
                // A run after a turbofish (`Vec::<u8>::new`) is not a root.
                current.push(String::new());
            }
            after_separator = true;
        } else if !character.is_whitespace() {
            if character == '"' {
                while let Some(next) = characters.next() {
                    match next {
                        '\\' => {
                            characters.next();
                        }
                        '"' => break,
                        _ => {}
                    }
                }
            }
            paths.push(std::mem::take(&mut current));
            after_separator = false;
        }
    }
    paths.push(current);
    paths
        .into_iter()
        .filter(|path| !path.is_empty() && !path[0].is_empty())
        .collect()
}

fn violations(source: &str, module: &[&str]) -> Vec<String> {
    let file = syn::parse_file(source).expect("pure module parses");
    let mut scope = Scope::default();
    scope.visit_file(&file);
    let mut checker = Checker {
        module: module.iter().map(|segment| segment.to_string()).collect(),
        scope: scope.names,
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
    "#;
    assert_eq!(
        violations(source, &["common", "time"]),
        Vec::<String>::new()
    );
    assert_eq!(
        violations("use super::calendar;", &["common", "time"]),
        Vec::<String>::new()
    );
}
