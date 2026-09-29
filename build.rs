//! Stamps the commit the binary was built from into `FUND_COMMIT`, which the journal writer reads.

use std::path::Path;
use std::process::Command;

fn main() {
    // `src` is watched because an edit there changes the binary and must change the dirty stamp with it.
    println!("cargo:rerun-if-changed=src");
    println!("cargo:rerun-if-changed=.git/HEAD");
    if let Some(reference) = checked_out_reference() {
        let path = format!(".git/{reference}");
        if Path::new(&path).exists() {
            println!("cargo:rerun-if-changed={path}");
        }
    }
    // Nothing is emitted when git cannot answer, so the record says unmeasurable rather than naming a placeholder.
    if let Some(commit) = commit() {
        println!("cargo:rustc-env=FUND_COMMIT={commit}");
    }
}

/// The ref `HEAD` points at, or `None` on a detached head.
fn checked_out_reference() -> Option<String> {
    let head = std::fs::read_to_string(".git/HEAD").ok()?;
    Some(head.trim().strip_prefix("ref: ")?.to_string())
}

/// The commit, with `-dirty` when the working tree differs from it, so a record never names code that did not run.
fn commit() -> Option<String> {
    let commit = git(&["rev-parse", "HEAD"])?;
    let dirty = !git(&["status", "--porcelain"])?.is_empty();
    Some(if dirty {
        format!("{commit}-dirty")
    } else {
        commit
    })
}

fn git(arguments: &[&str]) -> Option<String> {
    let output = Command::new("git").args(arguments).output().ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8(output.stdout).ok())
        .flatten()
        .map(|text| text.trim().to_string())
}
