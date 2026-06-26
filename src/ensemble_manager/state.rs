use std::sync::Arc;

use burn::backend::NdArray;
use sqlx::PgPool;
use tokio::sync::Mutex;

use crate::models::tide::config::ModelParameters;
use crate::models::tide::data::{FeatureMappings, Scaler};
use crate::models::tide::model::TideModel;

pub struct ModelState {
    model: TideModel<NdArray>,
    parameters: ModelParameters,
    scaler: Scaler,
    mappings: FeatureMappings,
    continuous_columns: Vec<String>,
    categorical_columns: Vec<String>,
    static_categorical_columns: Vec<String>,
    artifact_key: String,
    /// Training run id (the timestamp segment of the artifact key). Written to
    /// `equity_predictions.model_run_id` so predictions join `model_runs.run_id`.
    run_id: String,
    load_timestamp: i64,
}

impl ModelState {
    /// Constructs a `ModelState` from a fully loaded artifact.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        model: TideModel<NdArray>,
        parameters: ModelParameters,
        scaler: Scaler,
        mappings: FeatureMappings,
        continuous_columns: Vec<String>,
        categorical_columns: Vec<String>,
        static_categorical_columns: Vec<String>,
        artifact_key: String,
        run_id: String,
        load_timestamp: i64,
    ) -> Self {
        Self {
            model,
            parameters,
            scaler,
            mappings,
            continuous_columns,
            categorical_columns,
            static_categorical_columns,
            artifact_key,
            run_id,
            load_timestamp,
        }
    }

    pub fn model(&self) -> &TideModel<NdArray> {
        &self.model
    }

    pub fn parameters(&self) -> &ModelParameters {
        &self.parameters
    }

    pub fn scaler(&self) -> &Scaler {
        &self.scaler
    }

    pub fn mappings(&self) -> &FeatureMappings {
        &self.mappings
    }

    pub fn continuous_columns(&self) -> &[String] {
        &self.continuous_columns
    }

    pub fn categorical_columns(&self) -> &[String] {
        &self.categorical_columns
    }

    pub fn static_categorical_columns(&self) -> &[String] {
        &self.static_categorical_columns
    }

    pub fn artifact_key(&self) -> &str {
        &self.artifact_key
    }

    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    pub fn load_timestamp(&self) -> i64 {
        self.load_timestamp
    }
}

// SAFETY: TideModel<NdArray> is not Sync due to burn's Param<T> using an RwLock
// with a non-Sync FnOnce inside. We guard all access behind a Mutex, so this is safe.
unsafe impl Send for ModelState {}
unsafe impl Sync for ModelState {}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_s3_client() -> aws_sdk_s3::Client {
        let config = aws_sdk_s3::Config::builder()
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .build();
        aws_sdk_s3::Client::from_conf(config)
    }

    #[test]
    fn test_app_state_for_tests_accessors() {
        let state = AppState::for_tests(
            make_s3_client(),
            "test-bucket".to_string(),
            "models/tide/".to_string(),
            "http://data-manager:8080".to_string(),
            "latest".to_string(),
        );

        assert_eq!(state.artifact_bucket(), "test-bucket");
        assert_eq!(state.artifact_prefix(), "models/tide/");
        assert_eq!(state.data_manager_base_url(), "http://data-manager:8080");
        assert_eq!(state.model_version(), "latest");
        assert!(state.pool().is_none());
        assert!(state.local_artifact_dir().is_none());
    }

    #[tokio::test]
    async fn test_app_state_model_state_starts_empty() {
        let state = AppState::for_tests(
            make_s3_client(),
            "bucket".to_string(),
            "prefix/".to_string(),
            "http://localhost".to_string(),
            "1.0".to_string(),
        );

        let guard = state.model_state().lock().await;
        assert!(guard.is_none());
    }

    #[test]
    fn test_app_state_clone_shares_model_state_arc() {
        let state = AppState::for_tests(
            make_s3_client(),
            "bucket".to_string(),
            "prefix/".to_string(),
            "http://localhost".to_string(),
            "latest".to_string(),
        );
        let cloned = state.clone();
        // Both clones must point to the same Arc (same pointer address).
        assert!(std::ptr::eq(
            Arc::as_ptr(state.model_state()),
            Arc::as_ptr(cloned.model_state()),
        ));
    }

    #[test]
    fn test_app_state_s3_client_accessible() {
        let state = AppState::for_tests(
            make_s3_client(),
            "bucket".to_string(),
            "prefix/".to_string(),
            "http://localhost".to_string(),
            "latest".to_string(),
        );
        // Accessor must compile and return a reference without panicking.
        let _client = state.s3_client();
    }

    #[test]
    fn test_model_state_new_and_all_accessors() {
        use crate::models::tide::model::TideModel;
        use burn::backend::NdArray;
        use std::collections::HashMap;

        let device = Default::default();
        // input_size=32: matches make_tiny_dataset in evaluate tests (2*7+2*5+1*5+3=32).
        let model = TideModel::<NdArray>::new(&device, 32, 8, 1, 1, 1, 3, 0.0);
        let parameters = crate::models::tide::config::ModelParameters::for_tests(
            32,
            8,
            1,
            1,
            1,
            2,
            0.0,
            vec![0.1, 0.5, 0.9],
            0.5,
        );
        let scaler = Scaler {
            means: HashMap::new(),
            standard_deviations: HashMap::new(),
        };
        let mappings = FeatureMappings::new();
        let continuous_columns = vec!["close".to_string()];
        let categorical_columns = vec!["sector".to_string()];
        let static_categorical_columns = vec!["ticker".to_string()];
        let artifact_key = "models/tide/run-2026/output/model.tar.gz".to_string();
        let run_id = "run-2026".to_string();
        let load_timestamp = 1_000_000_i64;

        let state = ModelState::new(
            model,
            parameters,
            scaler,
            mappings,
            continuous_columns,
            categorical_columns,
            static_categorical_columns,
            artifact_key.clone(),
            run_id.clone(),
            load_timestamp,
        );

        // Verify all accessors return the values passed to new().
        let _model_ref = state.model();
        let _params_ref = state.parameters();
        let _scaler_ref = state.scaler();
        let _mappings_ref = state.mappings();
        assert_eq!(state.continuous_columns(), &["close"]);
        assert_eq!(state.categorical_columns(), &["sector"]);
        assert_eq!(state.static_categorical_columns(), &["ticker"]);
        assert_eq!(state.artifact_key(), artifact_key);
        assert_eq!(state.run_id(), run_id);
        assert_eq!(state.load_timestamp(), load_timestamp);
    }
}

#[derive(Clone)]
pub struct AppState {
    model_state: Arc<Mutex<Option<ModelState>>>,
    s3_client: aws_sdk_s3::Client,
    artifact_bucket: String,
    artifact_prefix: String,
    data_manager_base_url: String,
    model_version: String,
    local_artifact_dir: Option<std::path::PathBuf>,
    pool: Option<PgPool>,
}

impl AppState {
    pub fn model_state(&self) -> &Arc<Mutex<Option<ModelState>>> {
        &self.model_state
    }

    pub fn s3_client(&self) -> &aws_sdk_s3::Client {
        &self.s3_client
    }

    pub fn artifact_bucket(&self) -> &str {
        &self.artifact_bucket
    }

    pub fn artifact_prefix(&self) -> &str {
        &self.artifact_prefix
    }

    pub fn data_manager_base_url(&self) -> &str {
        &self.data_manager_base_url
    }

    pub fn model_version(&self) -> &str {
        &self.model_version
    }

    pub fn local_artifact_dir(&self) -> Option<&std::path::Path> {
        self.local_artifact_dir.as_deref()
    }

    pub fn pool(&self) -> Option<&PgPool> {
        self.pool.as_ref()
    }

    /// Constructs an `AppState` for tests, with no model loaded and no pool.
    #[cfg(test)]
    pub fn for_tests(
        s3_client: aws_sdk_s3::Client,
        artifact_bucket: String,
        artifact_prefix: String,
        data_manager_base_url: String,
        model_version: String,
    ) -> Self {
        AppState {
            model_state: Arc::new(Mutex::new(None)),
            s3_client,
            artifact_bucket,
            artifact_prefix,
            data_manager_base_url,
            model_version,
            local_artifact_dir: None,
            pool: None,
        }
    }

    pub async fn from_env() -> Self {
        let artifact_bucket = std::env::var("AWS_S3_MODEL_ARTIFACTS_BUCKET_NAME")
            .unwrap_or_else(|_| "fund-artifacts".to_string());
        let artifact_prefix = std::env::var("AWS_S3_MODEL_ARTIFACT_PATH")
            .unwrap_or_else(|_| "artifacts/tide/".to_string());
        let data_manager_base_url = std::env::var("FUND_DATA_MANAGER_BASE_URL")
            .unwrap_or_else(|_| "http://data-manager:8080".to_string());
        let model_version = std::env::var("MODEL_VERSION").unwrap_or_else(|_| "latest".to_string());
        let local_artifact_dir = std::env::var("FUND_LOCAL_ARTIFACT_DIR")
            .ok()
            .map(std::path::PathBuf::from);

        let s3_client = crate::common::aws::s3_client().await;

        let (pool, _) = crate::common::database::connect_optional_pool().await;

        AppState {
            model_state: Arc::new(Mutex::new(None)),
            s3_client,
            artifact_bucket,
            artifact_prefix,
            data_manager_base_url,
            model_version,
            local_artifact_dir,
            pool,
        }
    }
}
