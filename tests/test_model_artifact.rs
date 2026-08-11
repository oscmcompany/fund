//! The train-to-serve contract: an artifact the trainer publishes must load into the inference path.
//!
//! Every other model test exercises one side. This one packages exactly what
//! `tide_model_trainer` stage four writes and loads it back exactly as the service does.

use burn::tensor::backend::Backend;

use fund::models::tide::artifact::{download_and_load_model, package_dir_to_tar_gz};
use fund::models::tide::configuration::ModelParameters;
use fund::models::tide::data::{input_feature_size, FeatureMappings, Scaler};
use fund::models::tide::fit::write_artifact_json;
use fund::models::tide::model::TiDEModel;
use fund::models::tide::train::TrainBackend;

const INPUT_LENGTH: usize = 35;
const OUTPUT_LENGTH: usize = 1;

/// An S3 client the local branch of `download_and_load_model` never calls.
///
/// The key handed to it below is a directory, which returns before any request is made. Building
/// one anyway is what lets the test reach the loader the service actually uses.
async fn unused_s3_client() -> aws_sdk_s3::Client {
    aws_sdk_s3::Client::new(
        &aws_config::SdkConfig::builder()
            .region(aws_config::Region::new("us-east-1"))
            .behavior_version(aws_config::BehaviorVersion::latest())
            .build(),
    )
}

fn scaler_over(columns: &[&str]) -> Scaler {
    let means = columns
        .iter()
        .map(|column| ((*column).to_string(), 0.0))
        .collect();
    let standard_deviations = columns
        .iter()
        .map(|column| ((*column).to_string(), 1.0))
        .collect();
    Scaler::new(means, standard_deviations).expect("a unit scaler must be valid")
}

/// Writes the four files stage four writes, tars them the way it tars them, and loads the result.
#[tokio::test]
async fn test_a_published_artifact_loads_into_the_inference_path() {
    let input_size = input_feature_size(INPUT_LENGTH, OUTPUT_LENGTH);
    let parameters = ModelParameters::new(input_size, INPUT_LENGTH, OUTPUT_LENGTH);

    let device = <TrainBackend as Backend>::Device::default();
    let model = TiDEModel::<TrainBackend>::new(
        &device,
        input_size,
        parameters.hidden_size(),
        parameters.encoder_layer_count(),
        parameters.decoder_layer_count(),
        parameters.output_length(),
        parameters.quantiles().len(),
        parameters.dropout_rate(),
    );

    let scaler = scaler_over(&[
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "volume",
        "volume_weighted_average_price",
        "daily_return",
    ]);
    let mut mappings = FeatureMappings::new();
    for column in ["ticker", "sector", "industry"] {
        mappings.insert(
            column.to_string(),
            [(format!("{column}-value"), 0)].into_iter().collect(),
        );
    }

    let staging = tempfile::tempdir().expect("a staging directory");
    write_artifact_json(staging.path(), &scaler, &mappings, &parameters)
        .expect("stage four must write the artifact JSON");
    burn::module::AutodiffModule::valid(&model)
        .save(staging.path())
        .expect("stage four must save the weights");

    // Packaged and unpacked rather than loaded from `staging` directly, so the test covers the
    // tarball the service downloads and not just the directory that produced it.
    let tarball = package_dir_to_tar_gz(staging.path()).expect("the artifact must package");
    let unpacked = tempfile::tempdir().expect("an unpack directory");
    tar::Archive::new(flate2::read::GzDecoder::new(tarball.as_slice()))
        .unpack(unpacked.path())
        .expect("the published tarball must unpack");

    let key = unpacked.path().to_string_lossy().to_string();
    let loaded = download_and_load_model(
        &unused_s3_client().await,
        "unused-bucket",
        &key,
        Some(unpacked.path()),
    )
    .await
    .expect("the inference path must load the artifact the trainer publishes");

    assert_eq!(loaded.parameters().input_size(), input_size);
    assert_eq!(loaded.parameters().input_length(), INPUT_LENGTH);
    assert_eq!(loaded.parameters().output_length(), OUTPUT_LENGTH);
    assert_eq!(
        loaded.parameters().quantiles().len(),
        parameters.quantiles().len()
    );
    assert_eq!(loaded.scaler().means().len(), scaler.means().len());
}

/// The loaded weights must produce a forward pass of the shape the prediction path indexes into.
///
/// Loading checks that the record deserializes; only a forward pass checks that it deserialized
/// into the architecture the parameters describe.
#[tokio::test]
async fn test_a_loaded_model_produces_one_row_of_quantiles_per_sample() {
    let input_size = input_feature_size(INPUT_LENGTH, OUTPUT_LENGTH);
    let parameters = ModelParameters::new(input_size, INPUT_LENGTH, OUTPUT_LENGTH);

    let device = <TrainBackend as Backend>::Device::default();
    let model = TiDEModel::<TrainBackend>::new(
        &device,
        input_size,
        parameters.hidden_size(),
        parameters.encoder_layer_count(),
        parameters.decoder_layer_count(),
        parameters.output_length(),
        parameters.quantiles().len(),
        parameters.dropout_rate(),
    );

    let staging = tempfile::tempdir().expect("a staging directory");
    burn::module::AutodiffModule::valid(&model)
        .save(staging.path())
        .expect("the weights must save");

    let reloaded = TiDEModel::<burn::backend::NdArray>::load(
        staging.path(),
        parameters.input_size(),
        parameters.hidden_size(),
        parameters.encoder_layer_count(),
        parameters.decoder_layer_count(),
        parameters.output_length(),
        parameters.quantiles().len(),
        parameters.dropout_rate(),
    )
    .expect("the weights must load back");

    let batch_size = 4;
    let input = burn::tensor::Tensor::<burn::backend::NdArray, 2>::zeros(
        [batch_size, input_size],
        &Default::default(),
    );
    let output = reloaded.forward(input);

    assert_eq!(
        output.dims(),
        [batch_size, OUTPUT_LENGTH * parameters.quantiles().len()]
    );
}
