//! The model registry on disk: a root holding `models/<name>/<version>/`, each version written once and opened only
//! through its pin.

use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};

use uuid::Uuid;

use crate::common::learning::registry::{
    Fitted, Inputs, MANIFEST_FILE, Manifest, Model, ModelPin, ModelRefusal, WEIGHTS_FILE, Weights,
};

/// Why a model was not written or opened.
#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
    #[error("{} could not be read or written: {cause}", .path.display())]
    Io {
        path: PathBuf,
        cause: std::io::Error,
    },
    /// A version is written once, so a refit is a new version rather than an overwrite.
    #[error("{} already holds a model", .path.display())]
    Exists { path: PathBuf },
    #[error(transparent)]
    Refused(#[from] ModelRefusal),
}

/// Writes `weights` and their `manifest` under `root`, refused unless the manifest names these weights, and returns
/// the pin that opens them.
pub fn write<M: Fitted>(
    root: &Path,
    manifest: &Manifest,
    weights: &Weights<M>,
) -> Result<ModelPin, RegistryError> {
    let (manifest_bytes, weights_bytes) = (manifest.encode(), weights.encode());
    let pin = manifest.pin();
    Model::<M>::open(&pin, weights.inputs(), &manifest_bytes, &weights_bytes)?;
    let directory = root.join(manifest.key().directory());
    // Every directory under the root is synced into its parent on each write, so a rerun finishes a failed sync.
    let synced: Vec<PathBuf> = directory
        .ancestors()
        .take_while(|ancestor| {
            (ancestor.starts_with(root) && *ancestor != root) || !ancestor.exists()
        })
        .map(Path::to_path_buf)
        .collect();
    std::fs::create_dir_all(&directory).map_err(|cause| RegistryError::Io {
        path: directory.clone(),
        cause,
    })?;
    for parent in synced.iter().filter_map(|created| created.parent()) {
        sync_directory(match parent.as_os_str().is_empty() {
            true => Path::new("."),
            false => parent,
        })?;
    }
    // Weights first, so a manifest on disk always has its weights beside it.
    create(&directory, WEIGHTS_FILE, &weights_bytes)?;
    create(&directory, MANIFEST_FILE, &manifest_bytes)?;
    Ok(pin)
}

/// Opens the model `pin` names under `root` for a caller whose rows hold `inputs` in order.
pub fn open<M: Fitted>(
    root: &Path,
    pin: &ModelPin,
    inputs: &Inputs,
) -> Result<Model<M>, RegistryError> {
    let directory = root.join(pin.key().directory());
    let manifest = read(&directory.join(MANIFEST_FILE))?;
    let weights = read(&directory.join(WEIGHTS_FILE))?;
    Ok(Model::open(pin, inputs, &manifest, &weights)?)
}

/// Creates `name` in `directory` holding `bytes` whole or not at all, and accepts a file already holding exactly
/// `bytes`, so a rerun after a partial write finishes it.
fn create(directory: &Path, name: &str, bytes: &[u8]) -> Result<(), RegistryError> {
    let path = &directory.join(name);
    let io = |path: &Path, cause| RegistryError::Io {
        path: path.to_path_buf(),
        cause,
    };
    let staged = directory.join(format!(".{name}.{}.partial", Uuid::new_v4()));
    let mut file = std::fs::File::create_new(&staged).map_err(|cause| io(&staged, cause))?;
    let written = file
        .write_all(bytes)
        .and_then(|()| file.sync_all())
        .map_err(|cause| io(&staged, cause));
    // A hard link never replaces an existing file, so two writers cannot both create the version.
    let linked = written.and_then(|()| match std::fs::hard_link(&staged, path) {
        Ok(()) => Ok(()),
        Err(cause) if cause.kind() == ErrorKind::AlreadyExists => match read(path)? == bytes {
            true => Ok(()),
            false => Err(RegistryError::Exists {
                path: path.to_path_buf(),
            }),
        },
        Err(cause) => Err(io(path, cause)),
    });
    // Syncing the directory makes the new name durable, so the manifest cannot outlive its weights on disk.
    let linked = linked.and_then(|()| sync_directory(directory));
    let removed = std::fs::remove_file(&staged).map_err(|cause| io(&staged, cause));
    linked.and(removed)
}

fn sync_directory(directory: &Path) -> Result<(), RegistryError> {
    std::fs::File::open(directory)
        .and_then(|handle| handle.sync_all())
        .map_err(|cause| RegistryError::Io {
            path: directory.to_path_buf(),
            cause,
        })
}

fn read(path: &Path) -> Result<Vec<u8>, RegistryError> {
    std::fs::read(path).map_err(|cause| RegistryError::Io {
        path: path.to_path_buf(),
        cause,
    })
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use chrono::{NaiveDate, TimeZone, Utc};

    use super::*;
    use crate::common::journal::{Commit, RunId};
    use crate::common::learning::registry::{ModelKey, ModelName, WeightsRefusal};
    use crate::common::learning::ridge::Ridge;
    use crate::common::time::{SessionDate, SessionRange};

    #[test]
    fn test_a_written_model_opens_by_its_pin_once_and_only_in_its_order() {
        let root = std::env::temp_dir().join(format!("registry-{}", Uuid::new_v4()));
        let inputs = Inputs::new(&["spread", "size"]).unwrap();
        let weights = Weights::<Ridge>::decode(
            br#"{"inputs":["spread","size"],"fitted":{"intercept":0.5,"coefficients":[1.0,2.0]}}"#,
            &inputs,
        )
        .unwrap();
        let session = SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 3, 6).unwrap());
        let manifest = Manifest::new(
            ModelKey::new(ModelName::new("tick_fill").unwrap(), NonZeroU32::MIN),
            Commit::new("0123456789abcdef0123456789abcdef01234567").unwrap(),
            RunId::new(Uuid::nil()),
            SessionRange::single(session),
            Utc.with_ymd_and_hms(2026, 3, 7, 1, 0, 0).unwrap(),
            &weights,
        )
        .unwrap();

        let pin = write(&root, &manifest, &weights).unwrap();
        let directory = root.join("models/tick_fill/1");
        assert!(directory.join("manifest.json").is_file());
        assert!(directory.join("model.weights.json").is_file());
        let model = open::<Ridge>(&root, &pin, &inputs).unwrap();
        assert_eq!(model.manifest(), &manifest);
        assert_eq!(model.fitted(), weights.fitted());
        // A rerun after the manifest was lost finishes the write, and a refit cannot take the version.
        std::fs::remove_file(directory.join("manifest.json")).unwrap();
        assert_eq!(write(&root, &manifest, &weights).unwrap(), pin);
        assert_eq!(
            open::<Ridge>(&root, &pin, &inputs).unwrap().manifest(),
            &manifest
        );
        let refit = Manifest::new(
            manifest.key().clone(),
            Commit::new("0123456789abcdef0123456789abcdef01234567").unwrap(),
            RunId::new(Uuid::from_u128(1)),
            SessionRange::single(session),
            Utc.with_ymd_and_hms(2026, 3, 7, 1, 0, 0).unwrap(),
            &weights,
        )
        .unwrap();
        assert!(matches!(
            write(&root, &refit, &weights),
            Err(RegistryError::Exists { path }) if path.ends_with("tick_fill/1/manifest.json")
        ));
        let mut listed: Vec<_> = std::fs::read_dir(&directory)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        listed.sort();
        assert_eq!(listed, ["manifest.json", "model.weights.json"]);
        let swapped = Inputs::new(&["size", "spread"]).unwrap();
        assert!(matches!(
            open::<Ridge>(&root, &pin, &swapped),
            Err(RegistryError::Refused(ModelRefusal::Weights(
                WeightsRefusal::Order { position: 0, .. }
            )))
        ));
        std::fs::remove_dir_all(&root).unwrap();
    }
}
