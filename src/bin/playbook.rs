//! Uploads a playbook to the profile's records bucket, where the trader reads it at startup: `playbook upload <path>`.

use std::path::PathBuf;
use std::process::ExitCode;

use tracing_subscriber::EnvFilter;

use fund::archive::Archive;
use fund::common::playbook::Playbook;
use fund::common::storage::{ConfigurationKey, EntityTag, ObjectKey};
use fund::records::RefusedToStart;

const USAGE: &str = "playbook upload <path>";

/// What uploading a playbook does to the one held now.
#[derive(Debug, PartialEq, Eq)]
enum Upload<'a> {
    /// Nothing is held, so the write must still find the key empty.
    Create,
    /// The held playbook is byte for byte the one given.
    Unchanged,
    /// The write must still find the version this tag names, so a concurrent upload is refused rather than lost.
    Replace(&'a EntityTag),
}

fn upload<'a>(held: Option<&'a (Vec<u8>, EntityTag)>, contents: &[u8]) -> Upload<'a> {
    match held {
        None => Upload::Create,
        Some((bytes, _)) if bytes == contents => Upload::Unchanged,
        Some((_, tag)) => Upload::Replace(tag),
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    // An operator's command that ships nothing, so it logs to standard output alone rather than to a file.
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    let path = match arguments.as_slice() {
        [command, path] if command == "upload" => PathBuf::from(path),
        _ => {
            tracing::error!(?arguments, usage = USAGE, "Usage refused");
            return RefusedToStart.into();
        }
    };
    let contents = match std::fs::read_to_string(&path) {
        Ok(contents) => contents,
        Err(error) => {
            tracing::error!(path = %path.display(), %error, "Playbook not read");
            return RefusedToStart.into();
        }
    };
    // The trader refuses a playbook it cannot parse, so one is never uploaded.
    if let Err(refusal) = Playbook::parse(&contents) {
        tracing::error!(path = %path.display(), %refusal, "Playbook refused");
        return RefusedToStart.into();
    }
    let records = match Archive::records(&aws_config::load_from_env().await) {
        Ok(records) => records,
        Err(refusal) => {
            tracing::error!(%refusal, "Records configuration refused");
            return RefusedToStart.into();
        }
    };
    let key = ConfigurationKey::Playbook;
    let held = match records.get_tagged(&key).await {
        Ok(held) => held,
        Err(error) => {
            tracing::error!(%error, "Uploaded playbook not read");
            return ExitCode::FAILURE;
        }
    };
    let body = contents.into_bytes();
    let written = match upload(held.as_ref(), &body) {
        Upload::Unchanged => {
            tracing::info!(path = key.path(), "Playbook unchanged");
            return ExitCode::SUCCESS;
        }
        Upload::Create => records.create(&key, body).await,
        Upload::Replace(tag) => records.replace(&key, body, tag).await,
    };
    match written {
        Ok(()) => {
            tracing::info!(
                path = key.path(),
                replaced = held.is_some(),
                "Playbook uploaded"
            );
            ExitCode::SUCCESS
        }
        Err(error) => {
            tracing::error!(%error, "Playbook not uploaded");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_an_upload_creates_keeps_or_replaces_what_is_held() {
        let tag = EntityTag::new("\"abc\"");
        let held = (b"old".to_vec(), tag.clone());
        assert_eq!(upload(None, b"new"), Upload::Create);
        assert_eq!(upload(Some(&held), b"old"), Upload::Unchanged);
        assert_eq!(upload(Some(&held), b"new"), Upload::Replace(&tag));
    }
}
