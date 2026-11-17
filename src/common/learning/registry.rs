//! The model registry: fitted weights stored with the names of the inputs they read, a manifest naming what fitted
//! them, and the pin a playbook holds, so a model opens only as the bytes it was pinned to and only for a caller
//! that builds its rows in the order it was fitted on.

use std::num::NonZeroU32;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use crate::common::journal::{Commit, RunId};
use crate::common::learning::boosting::ensemble::Booster;
use crate::common::learning::ridge::Ridge;
use crate::common::time::{SessionDate, SessionRange, SessionRangeRefusal};

/// Every model lives under this directory, one subdirectory per name and below it one per version.
pub const MODELS_ROOT: &str = "models";
pub const MANIFEST_FILE: &str = "manifest.json";
pub const WEIGHTS_FILE: &str = "model.weights.json";

/// A fitted model the registry stores, which reads rows of a fixed width; outside this crate only `Weights::decode`
/// reads one back, since reading one takes a `Sealed` that only this crate can build.
pub trait Fitted: Serialize + stored::Stored {
    fn width(&self) -> usize;
}

pub(crate) mod stored {
    /// Proof the caller is in this crate; a `Fitted` bound reaches `Stored::read` from outside, but cannot build this.
    pub struct Sealed(());

    /// Reads a fitted model from its stored form, without the input names its rows were fitted on.
    pub trait Stored: Sized {
        fn read<'de, D: serde::Deserializer<'de>>(
            sealed: Sealed,
            deserializer: D,
        ) -> Result<Self, D::Error>;
    }

    /// Reads a fitted model from its stored form, for `Weights::decode` and this crate's tests.
    pub(crate) fn read<'de, M: Stored, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<M, D::Error> {
        M::read(Sealed(()), deserializer)
    }
}

impl Fitted for Booster {
    fn width(&self) -> usize {
        Booster::width(self)
    }
}

impl Fitted for Ridge {
    fn width(&self) -> usize {
        self.coefficients().len()
    }
}

/// The names of a model's inputs in the order its rows hold them: at least one, none blank, none repeated.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "Vec<String>", into = "Vec<String>")]
pub struct Inputs(Vec<String>);

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InputsRefusal {
    #[error("a model reads at least one input")]
    Empty,
    #[error("input {position} has a blank name")]
    Blank { position: usize },
    #[error("`{name}` names inputs {first} and {second}")]
    Repeated {
        name: String,
        first: usize,
        second: usize,
    },
}

impl Inputs {
    pub fn new(names: &[&str]) -> Result<Self, InputsRefusal> {
        Self::try_from(
            names
                .iter()
                .map(|name| (*name).to_string())
                .collect::<Vec<_>>(),
        )
    }

    pub fn width(&self) -> usize {
        self.0.len()
    }

    pub fn names(&self) -> &[String] {
        &self.0
    }
}

impl TryFrom<Vec<String>> for Inputs {
    type Error = InputsRefusal;

    fn try_from(names: Vec<String>) -> Result<Self, Self::Error> {
        if names.is_empty() {
            return Err(InputsRefusal::Empty);
        }
        for (second, name) in names.iter().enumerate() {
            if name.trim().is_empty() {
                return Err(InputsRefusal::Blank { position: second });
            }
            if let Some(first) = names[..second].iter().position(|earlier| earlier == name) {
                return Err(InputsRefusal::Repeated {
                    name: name.clone(),
                    first,
                    second,
                });
            }
        }
        Ok(Self(names))
    }
}

impl From<Inputs> for Vec<String> {
    fn from(inputs: Inputs) -> Self {
        inputs.0
    }
}

/// A fitted model with the names of the inputs it reads, as many names as it reads inputs.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Weights<M> {
    inputs: Inputs,
    fitted: M,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, bound = "M: stored::Stored")]
struct WeightsFields<M> {
    inputs: Inputs,
    #[serde(deserialize_with = "stored::read")]
    fitted: M,
}

/// Why weights were refused, with the names or widths that refused them.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum WeightsRefusal {
    #[error("the weights are unreadable: {cause}")]
    Unreadable { cause: String },
    #[error("the weights name {inputs} inputs and the model reads {fitted}")]
    Width { inputs: usize, fitted: usize },
    /// The caller builds rows from a different input at `position` than the model was fitted on.
    #[error("input {position} is `{expected}` to the caller and `{stored}` to the weights")]
    Order {
        position: usize,
        expected: String,
        stored: String,
    },
    /// One list of names is the other's prefix, so they differ only in length.
    #[error("the caller builds rows {expected} wide and the weights read {stored}")]
    Count { expected: usize, stored: usize },
}

impl<M: Fitted> Weights<M> {
    pub fn new(inputs: Inputs, fitted: M) -> Result<Self, WeightsRefusal> {
        match inputs.width() == fitted.width() {
            true => Ok(Self { inputs, fitted }),
            false => Err(WeightsRefusal::Width {
                inputs: inputs.width(),
                fitted: fitted.width(),
            }),
        }
    }

    pub fn inputs(&self) -> &Inputs {
        &self.inputs
    }

    pub fn fitted(&self) -> &M {
        &self.fitted
    }

    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("fitted weights serialize to JSON")
    }

    /// Reads stored weights, refused unless their input names are `expected`, in the same order.
    pub fn decode(bytes: &[u8], expected: &Inputs) -> Result<Self, WeightsRefusal> {
        let fields: WeightsFields<M> =
            serde_json::from_slice(bytes).map_err(|error| WeightsRefusal::Unreadable {
                cause: error.to_string(),
            })?;
        let weights = Self::new(fields.inputs, fields.fitted)?;
        let (expected, stored) = (expected.names(), weights.inputs.names());
        match expected
            .iter()
            .zip(stored)
            .position(|(left, right)| left != right)
        {
            Some(position) => Err(WeightsRefusal::Order {
                position,
                expected: expected[position].clone(),
                stored: stored[position].clone(),
            }),
            None if expected.len() != stored.len() => Err(WeightsRefusal::Count {
                expected: expected.len(),
                stored: stored.len(),
            }),
            None => Ok(weights),
        }
    }
}

/// The SHA-256 of stored bytes as 64 lowercase hexadecimal digits.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Digest(String);

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DigestRefusal {
    #[error("`{raw}` is not 64 lowercase hexadecimal digits")]
    Malformed { raw: String },
}

impl Digest {
    pub fn of(bytes: &[u8]) -> Self {
        Self(
            Sha256::digest(bytes)
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect(),
        )
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for Digest {
    type Error = DigestRefusal;

    fn try_from(raw: String) -> Result<Self, Self::Error> {
        let hexadecimal = |byte: u8| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte);
        match raw.len() == 64 && raw.bytes().all(hexadecimal) {
            true => Ok(Self(raw)),
            false => Err(DigestRefusal::Malformed { raw }),
        }
    }
}

impl From<Digest> for String {
    fn from(digest: Digest) -> Self {
        digest.0
    }
}

/// A model's name: lowercase ASCII letters, digits and `_`, starting with a letter, so it is one path segment.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ModelName(String);

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ModelNameRefusal {
    #[error("`{raw}` is not a model name")]
    Malformed { raw: String },
}

impl ModelName {
    pub fn new(raw: &str) -> Result<Self, ModelNameRefusal> {
        Self::try_from(raw.to_string())
    }
}

impl TryFrom<String> for ModelName {
    type Error = ModelNameRefusal;

    fn try_from(raw: String) -> Result<Self, Self::Error> {
        let allowed = |character: char| {
            character.is_ascii_lowercase() || character.is_ascii_digit() || character == '_'
        };
        match raw.starts_with(|character: char| character.is_ascii_lowercase())
            && raw.chars().all(allowed)
        {
            true => Ok(Self(raw)),
            false => Err(ModelNameRefusal::Malformed { raw }),
        }
    }
}

impl From<ModelName> for String {
    fn from(name: ModelName) -> Self {
        name.0
    }
}

impl std::fmt::Display for ModelName {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// One version of one model, stored under `models/<name>/<version>/`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ModelKey {
    name: ModelName,
    version: NonZeroU32,
}

impl ModelKey {
    pub fn new(name: ModelName, version: NonZeroU32) -> Self {
        Self { name, version }
    }

    pub fn name(&self) -> &ModelName {
        &self.name
    }

    pub fn directory(&self) -> String {
        format!("{MODELS_ROOT}/{}/{}", self.name, self.version)
    }
}

/// What fitted a model: the commit of its architecture, the run that trained it, the sessions it trained on, the
/// instant by which every row it read was known, and the digest of its weights.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Manifest {
    key: ModelKey,
    architecture: Commit,
    training: RunId,
    window: SessionRange,
    known_at: DateTime<Utc>,
    weights: Digest,
}

/// A manifest as stored, checked on the way in.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestFields {
    name: ModelName,
    version: NonZeroU32,
    architecture: Commit,
    training: RunId,
    first_session: SessionDate,
    last_session: SessionDate,
    known_at: DateTime<Utc>,
    weights_sha256: Digest,
}

/// Why a manifest was refused, with the values that refused it.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ManifestRefusal {
    #[error("the manifest is unreadable: {cause}")]
    Unreadable { cause: String },
    /// A dirty tree names code no commit holds, so the architecture could not be rebuilt.
    #[error("the architecture commit {architecture} was built from a dirty tree")]
    Dirty { architecture: Commit },
    #[error("the sessions {first} to {last} end before they start")]
    Inverted {
        first: SessionDate,
        last: SessionDate,
    },
    /// The cutoff falls before the last session's close, so that session's rows were not yet all known.
    #[error("rows known by {known_at} cannot include the session {last}")]
    Cutoff {
        known_at: DateTime<Utc>,
        last: SessionDate,
    },
}

impl Manifest {
    pub fn new<M: Fitted>(
        key: ModelKey,
        architecture: Commit,
        training: RunId,
        window: SessionRange,
        known_at: DateTime<Utc>,
        weights: &Weights<M>,
    ) -> Result<Self, ManifestRefusal> {
        Self::checked(
            key,
            architecture,
            training,
            window,
            known_at,
            Digest::of(&weights.encode()),
        )
    }

    fn checked(
        key: ModelKey,
        architecture: Commit,
        training: RunId,
        window: SessionRange,
        known_at: DateTime<Utc>,
        weights: Digest,
    ) -> Result<Self, ManifestRefusal> {
        if architecture.is_dirty() {
            return Err(ManifestRefusal::Dirty { architecture });
        }
        if known_at < window.last().regular_close() {
            return Err(ManifestRefusal::Cutoff {
                known_at,
                last: window.last(),
            });
        }
        Ok(Self {
            key,
            architecture,
            training,
            window,
            known_at,
            weights,
        })
    }

    pub fn key(&self) -> &ModelKey {
        &self.key
    }

    pub fn window(&self) -> SessionRange {
        self.window
    }

    pub fn known_at(&self) -> DateTime<Utc> {
        self.known_at
    }

    pub fn encode(&self) -> Vec<u8> {
        let fields = ManifestFields {
            name: self.key.name.clone(),
            version: self.key.version,
            architecture: self.architecture.clone(),
            training: self.training,
            first_session: self.window.first(),
            last_session: self.window.last(),
            known_at: self.known_at,
            weights_sha256: self.weights.clone(),
        };
        serde_json::to_vec_pretty(&fields).expect("a manifest serializes to JSON")
    }

    /// The pin a playbook holds for this manifest's bytes.
    pub fn pin(&self) -> ModelPin {
        ModelPin {
            key: self.key.clone(),
            manifest: Digest::of(&self.encode()),
        }
    }

    fn decode(bytes: &[u8]) -> Result<Self, ManifestRefusal> {
        let fields: ManifestFields =
            serde_json::from_slice(bytes).map_err(|error| ManifestRefusal::Unreadable {
                cause: error.to_string(),
            })?;
        let window =
            SessionRange::new(fields.first_session, fields.last_session).map_err(|refusal| {
                match refusal {
                    SessionRangeRefusal::Inverted { first, last } => {
                        ManifestRefusal::Inverted { first, last }
                    }
                }
            })?;
        Self::checked(
            ModelKey::new(fields.name, fields.version),
            fields.architecture,
            fields.training,
            window,
            fields.known_at,
            fields.weights_sha256,
        )
    }
}

/// One model version and the digest of its manifest, which names the digest of its weights, as a playbook holds it.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(from = "PinFields")]
pub struct ModelPin {
    key: ModelKey,
    manifest: Digest,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PinFields {
    name: ModelName,
    version: NonZeroU32,
    manifest_sha256: Digest,
}

impl From<PinFields> for ModelPin {
    fn from(fields: PinFields) -> Self {
        Self {
            key: ModelKey::new(fields.name, fields.version),
            manifest: fields.manifest_sha256,
        }
    }
}

impl ModelPin {
    pub fn key(&self) -> &ModelKey {
        &self.key
    }

    pub fn manifest(&self) -> &Digest {
        &self.manifest
    }
}

/// A model opened from the bytes its pin names, for a caller whose rows hold its inputs in its order.
#[derive(Debug, Clone, PartialEq)]
pub struct Model<M> {
    manifest: Manifest,
    weights: Weights<M>,
}

/// Why a pinned model would not open, with the digests, keys or refusal that stopped it.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ModelRefusal {
    #[error("the manifest's digest is {}, and the pin holds {}", .actual.as_str(), .pinned.as_str())]
    ManifestDigest { pinned: Digest, actual: Digest },
    #[error(transparent)]
    Manifest(#[from] ManifestRefusal),
    /// The pinned bytes describe another model or version than the pin names.
    #[error("the pin names {} and the manifest {}", .pinned.directory(), .stored.directory())]
    Key { pinned: ModelKey, stored: ModelKey },
    #[error("the weights' digest is {}, and the manifest holds {}", .actual.as_str(), .stored.as_str())]
    WeightsDigest { stored: Digest, actual: Digest },
    #[error(transparent)]
    Weights(#[from] WeightsRefusal),
}

impl<M: Fitted> Model<M> {
    /// Opens the stored `manifest` and `weights`, refused unless both are the bytes `pin` names and the weights read
    /// `inputs` in order.
    pub fn open(
        pin: &ModelPin,
        inputs: &Inputs,
        manifest: &[u8],
        weights: &[u8],
    ) -> Result<Self, ModelRefusal> {
        let actual = Digest::of(manifest);
        if actual != pin.manifest {
            return Err(ModelRefusal::ManifestDigest {
                pinned: pin.manifest.clone(),
                actual,
            });
        }
        let manifest = Manifest::decode(manifest)?;
        if manifest.key != pin.key {
            return Err(ModelRefusal::Key {
                pinned: pin.key.clone(),
                stored: manifest.key,
            });
        }
        let actual = Digest::of(weights);
        if actual != manifest.weights {
            return Err(ModelRefusal::WeightsDigest {
                stored: manifest.weights,
                actual,
            });
        }
        let weights = Weights::decode(weights, inputs)?;
        Ok(Self { manifest, weights })
    }

    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    /// The input names in the order a row holds them, for building rows rather than restating the order.
    pub fn inputs(&self) -> &Inputs {
        self.weights.inputs()
    }

    pub fn fitted(&self) -> &M {
        self.weights.fitted()
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use chrono::{NaiveDate, TimeZone};
    use proptest::prelude::*;
    use serde::de::DeserializeOwned;
    use uuid::Uuid;

    use super::*;
    use crate::common::learning::boosting::ensemble::{Settings, Stopping};
    use crate::common::learning::boosting::{Fraction, Penalty};
    use crate::common::learning::loss::Loss;
    use crate::common::learning::matrix::{Matrix, Sample};
    use crate::common::monoid::Sequential;

    const SHA: &str = "0123456789abcdef0123456789abcdef01234567";

    fn ridge(coefficients: &[f64]) -> Ridge {
        stored::read::<Ridge, _>(serde_json::json!({
            "intercept": 0.5,
            "coefficients": coefficients,
        }))
        .unwrap()
    }

    fn session(day: u32) -> SessionDate {
        SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 3, day).unwrap())
    }

    fn manifest<M: Fitted>(weights: &Weights<M>) -> Manifest {
        Manifest::new(
            ModelKey::new(ModelName::new("tick_fill").unwrap(), NonZeroU32::MIN),
            Commit::new(SHA).unwrap(),
            RunId::new(Uuid::nil()),
            SessionRange::new(session(2), session(6)).unwrap(),
            Utc.with_ymd_and_hms(2026, 3, 7, 1, 0, 0).unwrap(),
            weights,
        )
        .unwrap()
    }

    fn stored(names: &[&str]) -> (ModelPin, Vec<u8>, Vec<u8>) {
        let coefficients: Vec<f64> = (0..names.len()).map(|index| index as f64).collect();
        let weights = Weights::new(Inputs::new(names).unwrap(), ridge(&coefficients)).unwrap();
        let manifest = manifest(&weights);
        (manifest.pin(), manifest.encode(), weights.encode())
    }

    fn pin(name: &str, manifest: &[u8]) -> ModelPin {
        toml::from_str(&format!(
            "name = \"{name}\"\nversion = 1\nmanifest_sha256 = \"{}\"\n",
            Digest::of(manifest).as_str()
        ))
        .unwrap()
    }

    #[test]
    fn test_inputs_in_another_order_than_the_fit_fail_to_load() {
        let (pin, manifest, weights) = stored(&["spread", "size", "imbalance"]);
        let swapped = Inputs::new(&["spread", "imbalance", "size"]).unwrap();
        assert_eq!(
            Model::<Ridge>::open(&pin, &swapped, &manifest, &weights),
            Err(ModelRefusal::Weights(WeightsRefusal::Order {
                position: 1,
                expected: "imbalance".to_string(),
                stored: "size".to_string(),
            }))
        );
        let same = Inputs::new(&["spread", "size", "imbalance"]).unwrap();
        let model = Model::<Ridge>::open(&pin, &same, &manifest, &weights).unwrap();
        assert_eq!(model.fitted().coefficients(), [0.0, 1.0, 2.0]);
        assert_eq!(model.inputs().names(), ["spread", "size", "imbalance"]);
    }

    /// Names one impl only while `T` has no public deserializer; with one, `_` is ambiguous and the crate fails to build.
    trait Undeserializable<Marker> {
        fn check() {}
    }
    impl<T> Undeserializable<()> for T {}
    impl<T: DeserializeOwned> Undeserializable<u8> for T {}

    #[test]
    fn test_fitted_models_load_only_through_their_weights() {
        <Booster as Undeserializable<_>>::check();
        <Ridge as Undeserializable<_>>::check();
    }

    #[test]
    fn test_a_booster_opens_only_in_its_fitted_order_and_predicts_as_fitted() {
        // The label is the first input alone, so the two inputs are not interchangeable.
        let values: Vec<f64> = (0..40)
            .flat_map(|row| [f64::from(row), f64::from(40 - row)])
            .collect();
        let labels: Vec<f64> = values.chunks(2).map(|row| row[0]).collect();
        let matrix = Matrix::new(&values, NonZeroUsize::new(2).unwrap()).unwrap();
        let settings = Settings {
            leaves: NonZeroUsize::new(4).unwrap(),
            learning_rate: Fraction::new(0.5).unwrap(),
            minimum_rows_per_leaf: NonZeroUsize::new(2).unwrap(),
            input_fraction: Fraction::new(1.0).unwrap(),
            row_fraction: Fraction::new(1.0).unwrap(),
            penalty: Penalty::new(1.0).unwrap(),
            seed: 7,
        };
        let booster = Booster::fit(
            &Sample::new(matrix, &labels, Loss::SquaredError).unwrap(),
            Stopping::Fixed { rounds: 5 },
            &settings,
            &Sequential,
        )
        .unwrap();
        let inputs = Inputs::new(&["first", "second"]).unwrap();
        let weights = Weights::new(inputs.clone(), booster.clone()).unwrap();
        let manifest = manifest(&weights);
        let (pin, manifest_bytes, weights_bytes) =
            (manifest.pin(), manifest.encode(), weights.encode());
        let reversed = Inputs::new(&["second", "first"]).unwrap();
        assert!(matches!(
            Model::<Booster>::open(&pin, &reversed, &manifest_bytes, &weights_bytes),
            Err(ModelRefusal::Weights(WeightsRefusal::Order {
                position: 0,
                ..
            }))
        ));
        let model = Model::<Booster>::open(&pin, &inputs, &manifest_bytes, &weights_bytes).unwrap();
        assert_eq!(model.fitted().predict(&matrix), booster.predict(&matrix));
        assert_eq!(model.manifest(), &manifest);
    }

    #[test]
    fn test_inputs_that_differ_only_in_length_fail_to_load() {
        let weights = Weights::new(Inputs::new(&["a", "b"]).unwrap(), ridge(&[1.0, 2.0])).unwrap();
        let bytes = weights.encode();
        assert_eq!(
            Weights::<Ridge>::decode(&bytes, &Inputs::new(&["a"]).unwrap()),
            Err(WeightsRefusal::Count {
                expected: 1,
                stored: 2
            })
        );
        assert_eq!(
            Weights::<Ridge>::decode(&bytes, &Inputs::new(&["a", "b", "c"]).unwrap()),
            Err(WeightsRefusal::Count {
                expected: 3,
                stored: 2
            })
        );
    }

    #[test]
    fn test_weights_naming_more_inputs_than_the_model_reads_are_refused() {
        assert_eq!(
            Weights::new(Inputs::new(&["a", "b"]).unwrap(), ridge(&[1.0])),
            Err(WeightsRefusal::Width {
                inputs: 2,
                fitted: 1
            })
        );
        let stored = br#"{"inputs":["a","b"],"fitted":{"intercept":0.5,"coefficients":[1.0]}}"#;
        assert_eq!(
            Weights::<Ridge>::decode(stored, &Inputs::new(&["a", "b"]).unwrap()),
            Err(WeightsRefusal::Width {
                inputs: 2,
                fitted: 1
            })
        );
    }

    #[test]
    fn test_input_names_must_be_present_distinct_and_not_blank() {
        assert_eq!(Inputs::new(&[]), Err(InputsRefusal::Empty));
        assert_eq!(
            Inputs::new(&["a", " "]),
            Err(InputsRefusal::Blank { position: 1 })
        );
        assert_eq!(
            Inputs::new(&["a", "b", "a"]),
            Err(InputsRefusal::Repeated {
                name: "a".to_string(),
                first: 0,
                second: 2
            })
        );
    }

    #[test]
    fn test_a_digest_is_the_sha256_of_the_bytes() {
        // `printf '' | shasum -a 256`
        assert_eq!(
            Digest::of(b"").as_str(),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert!(Digest::try_from("E3B0".repeat(16)).is_err());
        assert!(Digest::try_from("e3b0".repeat(15)).is_err());
    }

    #[test]
    fn test_model_names_are_one_lowercase_path_segment() {
        assert!(ModelName::new("tick_fill2").is_ok());
        for raw in ["", "2fill", "Tick", "tick/fill", "..", "tick-fill"] {
            assert!(ModelName::new(raw).is_err(), "{raw}");
        }
        let key = ModelKey::new(
            ModelName::new("tick_fill").unwrap(),
            NonZeroU32::new(3).unwrap(),
        );
        assert_eq!(key.directory(), "models/tick_fill/3");
    }

    #[test]
    fn test_a_tampered_file_or_a_wrong_pin_fails_to_open() {
        let names = ["spread", "size"];
        let inputs = Inputs::new(&names).unwrap();
        let (pinned, manifest, weights) = stored(&names);
        assert!(Model::<Ridge>::open(&pinned, &inputs, &manifest, &weights).is_ok());
        let mut tampered = weights.clone();
        let last = tampered.len() - 2;
        tampered[last] = b'7';
        assert!(matches!(
            Model::<Ridge>::open(&pinned, &inputs, &manifest, &tampered),
            Err(ModelRefusal::WeightsDigest { .. })
        ));
        let (other, ..) = stored(&["spread", "depth"]);
        assert!(matches!(
            Model::<Ridge>::open(&other, &inputs, &manifest, &weights),
            Err(ModelRefusal::ManifestDigest { .. })
        ));
        assert!(matches!(
            Model::<Ridge>::open(&pin("other", &manifest), &inputs, &manifest, &weights),
            Err(ModelRefusal::Key { .. })
        ));
    }

    #[test]
    fn test_a_manifest_refuses_a_dirty_commit_and_a_cutoff_before_its_last_close() {
        let weights = Weights::new(Inputs::new(&["a"]).unwrap(), ridge(&[1.0])).unwrap();
        let key = ModelKey::new(ModelName::new("tick_fill").unwrap(), NonZeroU32::MIN);
        let window = SessionRange::new(session(2), session(6)).unwrap();
        let build = |sha: &str, known_at| {
            Manifest::new(
                key.clone(),
                Commit::new(sha).unwrap(),
                RunId::new(Uuid::nil()),
                window,
                known_at,
                &weights,
            )
        };
        // 2026-03-06 16:00 Eastern is 21:00 UTC, the close of the last session.
        let closes = Utc.with_ymd_and_hms(2026, 3, 6, 21, 0, 0).unwrap();
        assert!(build(SHA, closes).is_ok());
        // A cutoff inside the last session, even one second before its close, has not seen all of it.
        let midnight = Utc.with_ymd_and_hms(2026, 3, 6, 5, 0, 0).unwrap();
        for before in [closes - chrono::TimeDelta::seconds(1), midnight] {
            assert_eq!(
                build(SHA, before),
                Err(ManifestRefusal::Cutoff {
                    known_at: before,
                    last: session(6)
                })
            );
        }
        assert!(matches!(
            build(&format!("{SHA}-dirty"), closes),
            Err(ManifestRefusal::Dirty { .. })
        ));
    }

    #[test]
    fn test_a_stored_manifest_with_inverted_sessions_is_refused() {
        let (_, manifest, weights) = stored(&["a"]);
        let text = String::from_utf8(manifest)
            .unwrap()
            .replace("\"2026-03-02\"", "\"2026-03-09\"");
        assert_eq!(
            Model::<Ridge>::open(
                &pin("tick_fill", text.as_bytes()),
                &Inputs::new(&["a"]).unwrap(),
                text.as_bytes(),
                &weights
            ),
            Err(ModelRefusal::Manifest(ManifestRefusal::Inverted {
                first: session(9),
                last: session(6)
            }))
        );
    }

    #[test]
    fn test_a_pin_written_in_toml_reads_as_the_manifest_pins_it() {
        let (pinned, manifest, _) = stored(&["a"]);
        assert_eq!(pin("tick_fill", &manifest), pinned);
        assert_eq!(pinned.manifest(), &Digest::of(&manifest));
        let text = format!(
            "name = \"tick_fill\"\nversion = 1\nmanifest_sha256 = \"{}\"\nextra = 1\n",
            Digest::of(&manifest).as_str()
        );
        assert!(toml::from_str::<ModelPin>(&text).is_err());
    }

    fn names(width: usize) -> Vec<String> {
        (0..width).map(|index| format!("input_{index}")).collect()
    }

    proptest! {
        #[test]
        fn test_weights_round_trip_through_their_bytes(
            coefficients in prop::collection::vec(-1e6_f64..1e6, 1..8),
        ) {
            let inputs = Inputs::try_from(names(coefficients.len())).unwrap();
            let weights = Weights::new(inputs.clone(), ridge(&coefficients)).unwrap();
            let bytes = weights.encode();
            let decoded = Weights::<Ridge>::decode(&bytes, &inputs).unwrap();
            prop_assert_eq!(&decoded, &weights);
            prop_assert_eq!(decoded.encode(), bytes);
        }

        #[test]
        fn test_only_the_fitted_order_opens(
            order in Just((0..6).collect::<Vec<usize>>()).prop_shuffle(),
        ) {
            let fitted = names(6);
            let weights = Weights::new(
                Inputs::try_from(fitted.clone()).unwrap(),
                ridge(&[1.0, 2.0, 3.0, 4.0, 5.0, 6.0]),
            )
            .unwrap();
            let permuted: Vec<String> = order.iter().map(|&index| fitted[index].clone()).collect();
            let decoded = Weights::<Ridge>::decode(
                &weights.encode(),
                &Inputs::try_from(permuted.clone()).unwrap(),
            );
            match order.iter().enumerate().position(|(position, &index)| position != index) {
                None => prop_assert_eq!(decoded, Ok(weights)),
                Some(position) => prop_assert_eq!(
                    decoded,
                    Err(WeightsRefusal::Order {
                        position,
                        expected: permuted[position].clone(),
                        stored: fitted[position].clone(),
                    })
                ),
            }
        }
    }
}
