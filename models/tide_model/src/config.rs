use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelParameters {
    pub input_size: usize,
    pub hidden_size: usize,
    pub num_encoder_layers: usize,
    pub num_decoder_layers: usize,
    pub output_length: usize,
    pub input_length: usize,
    pub dropout_rate: f64,
    pub quantiles: Vec<f64>,
    pub huber_delta: f64,
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
    pub fn load(path: &std::path::Path) -> Result<Self, Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(path)?;
        let params: Self = serde_json::from_str(&content)?;
        Ok(params)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_parameters() {
        let params = ModelParameters::default();
        assert_eq!(params.hidden_size, 64);
        assert_eq!(params.output_length, 5);
        assert_eq!(params.input_length, 35);
        assert_eq!(params.quantiles, vec![0.1, 0.5, 0.9]);
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
        assert_eq!(params.input_size, 100);
        assert_eq!(params.hidden_size, 64);
    }
}
