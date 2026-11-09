//! A study's own code as compiled into the binary that ran, so each experiment it journals names the source behind it.

use std::collections::BTreeMap;

use sha2::{Digest, Sha256};

use crate::common::laboratory::experiment::SourceDigest;

/// The files of a study binary by the path its `study!` named, as they were when it was compiled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Source {
    files: BTreeMap<String, String>,
}

impl Source {
    pub fn new(path: &str, text: &str) -> Self {
        Self {
            files: BTreeMap::from([(path.to_string(), text.to_string())]),
        }
    }

    /// Adds another file of the same binary, such as a module beside the file that opens the study.
    pub fn with(mut self, path: &str, text: &str) -> Self {
        self.files.insert(path.to_string(), text.to_string());
        self
    }

    /// The object stored under the digest: the files as one JSON object, ordered by path.
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(&self.files).expect("a map of strings always serializes")
    }

    pub fn digest(&self) -> SourceDigest {
        digest_of(&self.encode())
    }
}

/// The SHA-256 of `encoded` in lowercase hexadecimal, so stored bytes are keyed by what they hold rather than by what
/// names them.
pub fn digest_of(encoded: &[u8]) -> SourceDigest {
    let hash = Sha256::digest(encoded);
    let hexadecimal: String = hash.iter().map(|byte| format!("{byte:02x}")).collect();
    SourceDigest::new(&hexadecimal).expect("SHA-256 prints 64 lowercase hexadecimal digits")
}

/// Opens a `Study` that captures the calling file and each named sibling, given relative to the calling file. The
/// calling file is read relative to the crate's parent directory, so it serves a crate one level below the workspace
/// root, as `studies` is.
#[macro_export]
macro_rules! study {
    ($label:expr, $directory:expr $(, $sibling:literal)* $(,)?) => {
        $crate::laboratory::Study::open(
            $label,
            $directory,
            $crate::laboratory::source::Source::new(
                file!(),
                include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../", file!())),
            )
            $(.with($sibling, include_str!($sibling)))*,
        )
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_a_source_encodes_its_files_in_path_order() {
        let source = Source::new("studies/src/bin/b.rs", "fn main() {}\n").with("a.rs", "// a\n");
        assert_eq!(
            String::from_utf8(source.encode()).unwrap(),
            r#"{"a.rs":"// a\n","studies/src/bin/b.rs":"fn main() {}\n"}"#
        );
    }

    #[test]
    fn test_a_digest_is_the_sha256_of_the_encoded_files() {
        // `printf '{"main.rs":"fn main() {}"}' | shasum -a 256`
        assert_eq!(
            Source::new("main.rs", "fn main() {}").digest().as_str(),
            "bb656f2dc853fae5f4d0b5dd7d5ab3a9c82d2d45229fd429f730be8f4bf01571"
        );
    }

    #[test]
    fn test_the_digest_moves_with_any_file_and_not_with_the_order_files_are_added() {
        let base = Source::new("main.rs", "fn main() {}").with("model.rs", "pub fn fit() {}");
        assert_eq!(
            base.digest(),
            Source::new("model.rs", "pub fn fit() {}")
                .with("main.rs", "fn main() {}")
                .digest()
        );
        assert_ne!(
            base.digest(),
            Source::new("main.rs", "fn main() {}")
                .with("model.rs", "pub fn fit() { }")
                .digest()
        );
        assert_ne!(
            base.digest(),
            Source::new("main.rs", "fn main() {}").digest()
        );
    }
}
