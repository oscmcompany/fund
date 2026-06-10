use burn::backend::NdArray;
use burn::module::Module;
use burn::nn;
use burn::prelude::*;

#[derive(Module, Debug)]
pub struct ResidualBlock<B: Backend> {
    linear: nn::Linear<B>,
    layer_norm: nn::LayerNorm<B>,
    dropout: nn::Dropout,
}

impl<B: Backend> ResidualBlock<B> {
    pub fn new(device: &B::Device, hidden_size: usize, dropout_rate: f64) -> Self {
        let linear = nn::LinearConfig::new(hidden_size, hidden_size).init(device);
        let layer_norm = nn::LayerNormConfig::new(hidden_size).init(device);
        let dropout = nn::DropoutConfig::new(dropout_rate).init();
        Self {
            linear,
            layer_norm,
            dropout,
        }
    }

    pub fn forward(&self, input: Tensor<B, 2>) -> Tensor<B, 2> {
        let output = self.linear.forward(input.clone());
        let output = burn::tensor::activation::relu(output);
        let output = self.dropout.forward(output);
        let output = output + input;
        self.layer_norm.forward(output)
    }
}

#[derive(Module, Debug)]
pub struct TideModel<B: Backend> {
    feature_projection_1: nn::Linear<B>,
    feature_projection_2: nn::Linear<B>,
    encoder_blocks: Vec<ResidualBlock<B>>,
    decoder_blocks: Vec<ResidualBlock<B>>,
    output_projection: nn::Linear<B>,
    final_layer_norm: nn::LayerNorm<B>,
    output_length: usize,
    num_quantiles: usize,
}

impl TideModel<NdArray> {
    #[allow(clippy::too_many_arguments)]
    pub fn load(
        directory_path: &std::path::Path,
        input_size: usize,
        hidden_size: usize,
        num_encoder_layers: usize,
        num_decoder_layers: usize,
        output_length: usize,
        num_quantiles: usize,
        dropout_rate: f64,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let device = Default::default();
        let model = Self::new(
            &device,
            input_size,
            hidden_size,
            num_encoder_layers,
            num_decoder_layers,
            output_length,
            num_quantiles,
            dropout_rate,
        );

        let record_path = directory_path.join("tide_states");
        // A missing or corrupt record must fail loudly: silently returning the
        // randomly initialized model would let the service report a successful
        // load and serve arbitrary predictions.
        model
            .load_file(
                record_path,
                &burn::record::DefaultFileRecorder::<burn::record::FullPrecisionSettings>::new(),
                &device,
            )
            .map_err(|error| {
                format!(
                    "Failed to load model weights from {}: {error}",
                    directory_path.display()
                )
                .into()
            })
    }
}

impl<B: Backend> TideModel<B> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        device: &B::Device,
        input_size: usize,
        hidden_size: usize,
        num_encoder_layers: usize,
        num_decoder_layers: usize,
        output_length: usize,
        num_quantiles: usize,
        dropout_rate: f64,
    ) -> Self {
        let feature_projection_1 = nn::LinearConfig::new(input_size, hidden_size * 2).init(device);
        let feature_projection_2 = nn::LinearConfig::new(hidden_size * 2, hidden_size).init(device);

        let encoder_blocks = (0..num_encoder_layers)
            .map(|_| ResidualBlock::new(device, hidden_size, dropout_rate))
            .collect();

        let decoder_blocks = (0..num_decoder_layers)
            .map(|_| ResidualBlock::new(device, hidden_size, dropout_rate))
            .collect();

        let output_projection =
            nn::LinearConfig::new(hidden_size, output_length * num_quantiles).init(device);

        let final_layer_norm = nn::LayerNormConfig::new(hidden_size).init(device);

        Self {
            feature_projection_1,
            feature_projection_2,
            encoder_blocks,
            decoder_blocks,
            output_projection,
            final_layer_norm,
            output_length,
            num_quantiles,
        }
    }

    /// Persist the model weights as a Burn record at `directory_path/tide_states`,
    /// the exact stem [`TideModel::<NdArray>::load`] reads back. The on-disk file
    /// gets the recorder's own extension; the loader re-derives it from the stem.
    pub fn save(&self, directory_path: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
        std::fs::create_dir_all(directory_path)?;
        let record_path = directory_path.join("tide_states");
        self.clone().save_file(
            record_path,
            &burn::record::DefaultFileRecorder::<burn::record::FullPrecisionSettings>::new(),
        )?;
        Ok(())
    }

    pub fn forward(&self, input: Tensor<B, 2>) -> Tensor<B, 2> {
        let batch_size = input.dims()[0];

        let hidden = self.feature_projection_1.forward(input);
        let hidden = burn::tensor::activation::relu(hidden);
        let hidden = self.feature_projection_2.forward(hidden);
        let hidden = burn::tensor::activation::relu(hidden);

        let mut encoder_output = hidden;
        for block in &self.encoder_blocks {
            encoder_output = block.forward(encoder_output);
        }

        let mut decoder_output = encoder_output.clone();
        for block in &self.decoder_blocks {
            decoder_output = block.forward(decoder_output);
        }

        let combined = decoder_output + encoder_output;
        let combined = self.final_layer_norm.forward(combined);
        // The Python model projects relu(x): `output_projection(x.relu())`.
        let combined = burn::tensor::activation::relu(combined);

        let output = self.output_projection.forward(combined);
        output.reshape([batch_size, self.output_length * self.num_quantiles])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use burn::backend::NdArray;

    #[test]
    fn test_model_forward_shape() {
        let device = Default::default();
        let input_size = 100;
        let hidden_size = 32;
        let output_length = 5;
        let num_quantiles = 3;
        let batch_size = 4;

        let model: TideModel<NdArray> = TideModel::new(
            &device,
            input_size,
            hidden_size,
            2,
            2,
            output_length,
            num_quantiles,
            0.1,
        );

        let input = Tensor::<NdArray, 2>::zeros([batch_size, input_size], &device);
        let output = model.forward(input);

        assert_eq!(output.dims(), [batch_size, output_length * num_quantiles]);
    }

    #[test]
    fn test_residual_block() {
        let device = Default::default();
        let block: ResidualBlock<NdArray> = ResidualBlock::new(&device, 32, 0.1);
        let input = Tensor::<NdArray, 2>::zeros([2, 32], &device);
        let output = block.forward(input);
        assert_eq!(output.dims(), [2, 32]);
    }

    #[test]
    fn test_load_fails_loudly_when_record_is_missing() {
        // No tide_states record in the directory: load must error rather than
        // fall back to random weights and report success.
        let dir = tempfile::tempdir().unwrap();
        let result = TideModel::<NdArray>::load(dir.path(), 24, 16, 2, 1, 5, 3, 0.0);
        assert!(result.is_err());
        let message = result.err().unwrap().to_string();
        assert!(message.contains("Failed to load model weights"));
    }

    #[test]
    fn test_forward_applies_relu_before_output_projection() {
        let device = Default::default();
        let input_size = 12;
        let model: TideModel<NdArray> = TideModel::new(&device, input_size, 8, 1, 1, 2, 3, 0.0);

        let input = Tensor::<NdArray, 1>::from_floats(
            (0..(4 * input_size))
                .map(|i| (i as f32 * 0.37).sin())
                .collect::<Vec<_>>()
                .as_slice(),
            &device,
        )
        .reshape([4, input_size]);

        // Compose the expected output from the model's own components, applying
        // relu to the layer-normed combination before the output projection as
        // the Python trainer does (`output_projection(x.relu())`).
        let hidden =
            burn::tensor::activation::relu(model.feature_projection_1.forward(input.clone()));
        let hidden = burn::tensor::activation::relu(model.feature_projection_2.forward(hidden));
        let mut encoder_output = hidden;
        for block in &model.encoder_blocks {
            encoder_output = block.forward(encoder_output);
        }
        let mut decoder_output = encoder_output.clone();
        for block in &model.decoder_blocks {
            decoder_output = block.forward(decoder_output);
        }
        let combined = model
            .final_layer_norm
            .forward(decoder_output + encoder_output);
        let expected: Vec<f32> = model
            .output_projection
            .forward(burn::tensor::activation::relu(combined))
            .to_data()
            .to_vec()
            .unwrap();

        let actual: Vec<f32> = model.forward(input).to_data().to_vec().unwrap();
        assert_eq!(expected.len(), actual.len());
        for (e, a) in expected.iter().zip(actual.iter()) {
            assert!(
                (e - a).abs() < 1e-6,
                "forward must relu before the output projection: {e} vs {a}"
            );
        }
    }

    #[test]
    fn test_save_load_round_trip_preserves_forward() {
        let device = Default::default();
        let input_size = 24;
        let model: TideModel<NdArray> = TideModel::new(&device, input_size, 16, 2, 1, 5, 3, 0.0);

        // A non-trivial input so a random-weight fallback would differ.
        let input = Tensor::<NdArray, 1>::from_floats(
            (0..(2 * input_size))
                .map(|i| i as f32 * 0.01)
                .collect::<Vec<_>>()
                .as_slice(),
            &device,
        )
        .reshape([2, input_size]);
        let expected: Vec<f32> = model.forward(input.clone()).to_data().to_vec().unwrap();

        let dir = tempfile::tempdir().unwrap();
        model.save(dir.path()).unwrap();

        let loaded =
            TideModel::<NdArray>::load(dir.path(), input_size, 16, 2, 1, 5, 3, 0.0).unwrap();
        let actual: Vec<f32> = loaded.forward(input).to_data().to_vec().unwrap();

        assert_eq!(expected.len(), actual.len());
        for (e, a) in expected.iter().zip(actual.iter()) {
            assert!((e - a).abs() < 1e-6, "weights not preserved: {e} vs {a}");
        }
    }
}
