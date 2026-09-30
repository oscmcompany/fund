//! Stamps the commit the binary was built from into `FUND_COMMIT`, which the journal writer reads.

use std::path::Path;
use std::process::Command;

fn main() {
    // `src` is watched because an edit there changes the binary and must change the dirty stamp with it.
    println!("cargo:rerun-if-changed=src");
    // Paths come from git so a worktree, a packed ref and the reflog are watched where git keeps them.
    let reference = git(&["symbolic-ref", "-q", "HEAD"]);
    for name in ["HEAD", "logs/HEAD", "packed-refs"]
        .map(str::to_string)
        .into_iter()
        .chain(reference)
    {
        if let Some(path) =
            git(&["rev-parse", "--git-path", &name]).filter(|path| Path::new(path).exists())
        {
            println!("cargo:rerun-if-changed={path}");
        }
    }
    // Nothing is emitted when git cannot answer, so the record says unmeasurable rather than naming a placeholder.
    if let Some(commit) = commit() {
        println!("cargo:rustc-env=FUND_COMMIT={commit}");
    }
}

/// The commit, with `-dirty` when the working tree differs from it, so a record never names code that did not run;
/// `None` for anything the journal's `Commit` would refuse.
fn commit() -> Option<String> {
    let commit = git(&["rev-parse", "HEAD"])?;
    // A SHA-256 repository prints 64 characters, which the journal's `Commit` refuses.
    let hexadecimal = |byte: u8| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte);
    if commit.len() != 40 || !commit.bytes().all(hexadecimal) {
        return None;
    }
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
