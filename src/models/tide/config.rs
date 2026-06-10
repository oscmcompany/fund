use serde::{Deserialize, Serialize};

/// TiDE model hyperparameters, persisted as `tide_parameters.json` in the
/// training artifact and reloaded at inference time.
///
/// Fields are private; construct via [`ModelParameters::new`] (which applies
/// the architecture defaults) or deserialize from a stored artifact.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelParameters {
    input_size: usize,
    hidden_size: usize,
    num_encoder_layers: usize,
    num_decoder_layers: usize,
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
            num_encoder_layers: 3,
            num_decoder_layers: 2,
            output_length: 5,
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
        num_encoder_layers: usize,
        num_decoder_layers: usize,
        output_length: usize,
        input_length: usize,
        dropout_rate: f64,
        quantiles: Vec<f64>,
        huber_delta: f64,
    ) -> Self {
        Self {
            input_size,
            hidden_size,
            num_encoder_layers,
            num_decoder_layers,
            output_length,
            input_length,
            dropout_rate,
            quantiles,
            huber_delta,
        }
    }

    pub fn load(path: &std::path::Path) -> Result<Self, Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(path)?;
        let params: Self = serde_json::from_str(&content)?;
        Ok(params)
    }

    pub fn input_size(&self) -> usize {
        self.input_size
    }

    pub fn hidden_size(&self) -> usize {
        self.hidden_size
    }

    pub fn num_encoder_layers(&self) -> usize {
        self.num_encoder_layers
    }

    pub fn num_decoder_layers(&self) -> usize {
        self.num_decoder_layers
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
        assert_eq!(params.output_length(), 5);
        assert_eq!(params.input_length(), 35);
        assert_eq!(params.quantiles(), [0.1, 0.5, 0.9]);
    }

    #[test]
    fn test_new_applies_defaults_for_architecture() {
        let params = ModelParameters::new(448, 35, 5);
        assert_eq!(params.input_size(), 448);
        assert_eq!(params.input_length(), 35);
        assert_eq!(params.output_length(), 5);
        assert_eq!(params.hidden_size(), 64);
        assert_eq!(params.num_encoder_layers(), 3);
        assert_eq!(params.num_decoder_layers(), 2);
        assert_eq!(params.dropout_rate(), 0.1);
        assert_eq!(params.quantiles(), [0.1, 0.5, 0.9]);
        assert_eq!(params.huber_delta(), 0.5);
    }

    #[test]
    fn test_deserialize_parameters() {
        let json = r#"{
            "input_size": 100,
            "hidden_size": 64,
            "num_encoder_layers": 3,
            "num_decoder_layers": 2,
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
}
