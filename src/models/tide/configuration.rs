//! Model hyperparameters. Serialized into the artifact, so changing a default only affects new
//! training runs — existing artifacts keep the values they were trained with.

use serde::{Deserialize, Serialize};

/// TiDE model hyperparameters, persisted as `tide_parameters.json` in the training artifact and
/// reloaded at inference time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelParameters {
    input_size: usize,
    hidden_size: usize,
    encoder_layer_count: usize,
    decoder_layer_count: usize,
    output_length: usize,
    input_length: usize,
    dropout_rate: f64,
    quantiles: Vec<f64>,
    huber_delta: f64,
}

impl Default for ModelParameters {
    fn default() -> Self {
        Self {
            input_size: 0,
            hidden_size: 64,
            encoder_layer_count: 3,
            decoder_layer_count: 2,
            output_length: 1,
            input_length: 35,
            dropout_rate: 0.1,
            quantiles: vec![0.1, 0.5, 0.9],
            huber_delta: 0.5,
        }
    }
}

impl ModelParameters {
    /// Constructs parameters for the given data shape, applying the default
    /// architecture hyperparameters for everything else.
    pub fn new(input_size: usize, input_length: usize, output_length: usize) -> Self {
        Self {
            input_size,
            input_length,
            output_length,
            ..Self::default()
        }
    }

    /// Constructs parameters with every hyperparameter spelled out. Tests use
    /// this to build deliberately tiny architectures that train quickly.
    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub fn for_tests(
        input_size: usize,
        hidden_size: usize,
        encoder_layer_count: usize,
        decoder_layer_count: usize,
        output_length: usize,
        input_length: usize,
        dropout_rate: f64,
        quantiles: Vec<f64>,
        huber_delta: f64,
    ) -> Self {
        Self {
            input_size,
            hidden_size,
            encoder_layer_count,
            decoder_layer_count,
            output_length,
            input_length,
            dropout_rate,
            quantiles,
            huber_delta,
        }
    }

    /// Reads parameters from a training artifact, rejecting window lengths of zero.
    ///
    /// A zero `output_length` produces no horizon steps, so inference returns an empty set and a
    /// corrupt artifact is indistinguishable from a session with nothing to forecast. Refusing it
    /// here keeps the failure at the boundary where the untrusted file is read.
    pub fn load(path: &std::path::Path) -> Result<Self, Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(path)?;
        let parameters: Self = serde_json::from_str(&content)?;
        if parameters.output_length == 0 || parameters.input_length == 0 {
            return Err(format!(
                "Model parameters at {} have a zero window length (input_length {}, output_length {})",
                path.display(),
                parameters.input_length,
                parameters.output_length
            )
            .into());
        }
        parameters
            .validate_quantiles()
            .map_err(|reason| format!("Model parameters at {}: {reason}", path.display()))?;
        Ok(parameters)
    }

    /// Rejects a quantile list the rest of the pipeline cannot consume.
    ///
    /// Three consumers read this list and each responds to a bad one differently: `quantile_loss`
    /// pairs it positionally with tensor slices, `evaluate`'s index helpers unwrap a `partial_cmp`
    /// and so **panic on a non-finite value**, and the prediction path sizes its output by the
    /// count. Validating once here is what lets all three treat `quantiles()` as trustworthy
    /// instead of each inventing its own guard.
    ///
    /// Note what is deliberately *not* required: sorted order, and a `0.5` entry. Positions are
    /// located rather than assumed, so an artifact may list its quantiles in any order, and a list
    /// without an exact median resolves to the nearest one.
    fn validate_quantiles(&self) -> Result<(), String> {
        if self.quantiles.is_empty() {
            return Err("the quantile list is empty, so the model predicts nothing".to_string());
        }
        for quantile in &self.quantiles {
            if !quantile.is_finite() {
                return Err(format!("quantile {quantile} is not finite"));
            }
            if *quantile <= 0.0 || *quantile >= 1.0 {
                return Err(format!("quantile {quantile} is outside (0, 1)"));
            }
        }
        for (index, quantile) in self.quantiles.iter().enumerate() {
            if self.quantiles[..index].contains(quantile) {
                return Err(format!(
                    "quantile {quantile} appears more than once, so two output columns carry the \
                     same prediction"
                ));
            }
        }
        Ok(())
    }

    pub fn input_size(&self) -> usize {
        self.input_size
    }

    pub fn hidden_size(&self) -> usize {
        self.hidden_size
    }

    pub fn encoder_layer_count(&self) -> usize {
        self.encoder_layer_count
    }

    pub fn decoder_layer_count(&self) -> usize {
        self.decoder_layer_count
    }

    pub fn output_length(&self) -> usize {
        self.output_length
    }

    pub fn input_length(&self) -> usize {
        self.input_length
    }

    pub fn dropout_rate(&self) -> f64 {
        self.dropout_rate
    }

    pub fn quantiles(&self) -> &[f64] {
        &self.quantiles
    }

    pub fn huber_delta(&self) -> f64 {
        self.huber_delta
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_parameters() {
        let params = ModelParameters::default();
        assert_eq!(params.hidden_size(), 64);
        assert_eq!(params.output_length(), 1);
        assert_eq!(params.input_length(), 35);
        assert_eq!(params.quantiles(), [0.1, 0.5, 0.9]);
    }

    #[test]
    fn test_new_applies_defaults_for_architecture() {
        let params = ModelParameters::new(428, 35, 1);
        assert_eq!(params.input_size(), 428);
        assert_eq!(params.input_length(), 35);
        assert_eq!(params.output_length(), 1);
        assert_eq!(params.hidden_size(), 64);
        assert_eq!(params.encoder_layer_count(), 3);
        assert_eq!(params.decoder_layer_count(), 2);
        assert_eq!(params.dropout_rate(), 0.1);
        assert_eq!(params.quantiles(), [0.1, 0.5, 0.9]);
        assert_eq!(params.huber_delta(), 0.5);
    }

    #[test]
    fn test_deserialize_parameters() {
        let json = r#"{
            "input_size": 100,
            "hidden_size": 64,
            "encoder_layer_count": 3,
            "decoder_layer_count": 2,
            "output_length": 5,
            "input_length": 35,
            "dropout_rate": 0.1,
            "quantiles": [0.1, 0.5, 0.9],
            "huber_delta": 0.5
        }"#;
        let params: ModelParameters = serde_json::from_str(json).unwrap();
        assert_eq!(params.input_size(), 100);
        assert_eq!(params.hidden_size(), 64);
    }

    fn load_with_quantiles(quantiles: &str) -> Result<ModelParameters, String> {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("tide_parameters.json");
        std::fs::write(
            &path,
            format!(
                r#"{{"input_size": 100, "hidden_size": 64, "encoder_layer_count": 3,
                     "decoder_layer_count": 2, "output_length": 1, "input_length": 35,
                     "dropout_rate": 0.1, "quantiles": {quantiles}, "huber_delta": 0.5}}"#
            ),
        )
        .unwrap();
        ModelParameters::load(&path).map_err(|error| error.to_string())
    }

    /// Three consumers read this list and each responded to a bad one differently. The non-finite
    /// case is the sharp one: `evaluate`'s index helpers unwrap a `partial_cmp`, so a NaN quantile
    /// panicked on the pre-open inference path rather than reporting a bad artifact.
    #[test]
    fn test_load_rejects_a_quantile_list_the_pipeline_cannot_consume() {
        assert!(load_with_quantiles("[]").is_err(), "empty");
        assert!(load_with_quantiles("[0.1, 0.5, 0.5]").is_err(), "duplicate");
        assert!(load_with_quantiles("[0.0, 0.5, 0.9]").is_err(), "zero");
        assert!(load_with_quantiles("[0.1, 0.5, 1.0]").is_err(), "one");
        assert!(load_with_quantiles("[0.1, 0.5, 1.5]").is_err(), "above one");
        assert!(load_with_quantiles("[0.1, -0.5, 0.9]").is_err(), "negative");
    }

    /// serde_json cannot represent NaN, so a corrupt artifact expresses it as a literal that
    /// deserializes into one. Guarded because it is the value that panics rather than misbehaves.
    #[test]
    fn test_load_rejects_a_non_finite_quantile() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("tide_parameters.json");
        // 1e400 overflows f64 and deserializes to infinity.
        std::fs::write(
            &path,
            r#"{"input_size": 100, "hidden_size": 64, "encoder_layer_count": 3,
                "decoder_layer_count": 2, "output_length": 1, "input_length": 35,
                "dropout_rate": 0.1, "quantiles": [0.1, 0.5, 1e400], "huber_delta": 0.5}"#,
        )
        .unwrap();
        assert!(ModelParameters::load(&path).is_err());
    }

    /// Order is deliberately not a requirement: `evaluate` locates the lowest, highest, and nearest
    /// to the median rather than assuming positions, so an artifact may list them any way round.
    /// A list without an exact 0.5 is likewise fine.
    #[test]
    fn test_load_accepts_an_unsorted_list_and_one_without_an_exact_median() {
        assert!(load_with_quantiles("[0.9, 0.1, 0.5]").is_ok());
        assert!(load_with_quantiles("[0.2, 0.8]").is_ok());
    }

    #[test]
    fn test_load_rejects_a_zero_window_length() {
        // A zero output_length yields no horizon steps, so inference would return an empty set and
        // the corrupt artifact would read as a session with nothing to forecast.
        let directory = tempfile::tempdir().unwrap();
        let write = |name: &str, output_length: usize, input_length: usize| {
            let path = directory.path().join(name);
            std::fs::write(
                &path,
                format!(
                    r#"{{"input_size": 100, "hidden_size": 64, "encoder_layer_count": 3,
                         "decoder_layer_count": 2, "output_length": {output_length},
                         "input_length": {input_length}, "dropout_rate": 0.1,
                         "quantiles": [0.1, 0.5, 0.9], "huber_delta": 0.5}}"#
                ),
            )
            .unwrap();
            path
        };

        assert!(ModelParameters::load(&write("zero_output.json", 0, 35)).is_err());
        assert!(ModelParameters::load(&write("zero_input.json", 1, 0)).is_err());
        assert!(ModelParameters::load(&write("valid.json", 1, 35)).is_ok());
    }
}
