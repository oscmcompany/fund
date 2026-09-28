//! Stamps the commit the binary was built from into `FUND_REVISION`.
//!
//! Read through `fund::laboratory::journal::DatasetBuilt::new`, which is the only consumer.

use std::path::Path;
use std::process::Command;

fn main() {
    // Watched rather than left to cargo's default, which is "rerun when anything in the package
    // changes" only until the first `rerun-if-changed` is printed. `src` is watched because it is
    // what the dirty marker below is about: an edit there changes the binary and must change the
    // stamp with it.
    println!("cargo:rerun-if-changed=src");
    println!("cargo:rerun-if-changed=.git/HEAD");
    if let Some(reference) = checked_out_reference() {
        let path = format!(".git/{reference}");
        if Path::new(&path).exists() {
            println!("cargo:rerun-if-changed={path}");
        }
    }

    // Nothing emitted when git cannot answer, so `option_env!` reads `None` and the record says
    // unmeasurable. A placeholder string would be a revision nobody can look up.
    if let Some(revision) = revision() {
        println!("cargo:rustc-env=FUND_REVISION={revision}");
    }
}

/// The ref `HEAD` points at, or `None` on a detached head, where the sha is in `HEAD` itself.
fn checked_out_reference() -> Option<String> {
    let head = std::fs::read_to_string(".git/HEAD").ok()?;
    Some(head.trim().strip_prefix("ref: ")?.to_string())
}

/// The commit, with `-dirty` appended when the working tree differs from it.
///
/// The suffix is what keeps this honest on a laptop: without it a record would name a commit that
/// is not what ran, which is worse than naming nothing.
fn revision() -> Option<String> {
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
