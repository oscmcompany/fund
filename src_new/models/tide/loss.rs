//! Quantile (pinball) loss for the TiDE model, optionally Huber-smoothed.
//!
//! Ported from the Python trainer's `quantile_loss`. The training loss averages
//! the per-quantile loss over all quantiles and uses a strict `error > 0` split;
//! the evaluation CRPS (see [`crate::models::tide::evaluate`]) intentionally
//! differs (sum over quantiles, non-strict split).

use burn::prelude::*;

/// Compute the mean quantile loss.
///
/// - `predictions`: `[batch, output_length * num_quantiles]` (the raw model output).
/// - `targets`: `[batch, output_length]`.
/// - `quantiles`: the quantile levels, e.g. `[0.1, 0.5, 0.9]`.
/// - `huber_delta`: when `> 0`, applies Huber smoothing to the per-element error.
pub fn quantile_loss<B: Backend>(
    predictions: Tensor<B, 2>,
    targets: Tensor<B, 2>,
    quantiles: &[f64],
    huber_delta: f64,
    output_length: usize,
) -> Tensor<B, 1> {
    let [batch, _] = predictions.dims();
    let num_quantiles = quantiles.len();
    let device = predictions.device();
    let reshaped = predictions.reshape([batch, output_length, num_quantiles]);

    let mut total: Option<Tensor<B, 1>> = None;
    for (index, &quantile) in quantiles.iter().enumerate() {
        let prediction = reshaped
            .clone()
            .narrow(2, index, 1)
            .reshape([batch, output_length]);
        let error = targets.clone().sub(prediction);
        let positive = error.clone().greater_elem(0.0);

        let loss = if huber_delta > 0.0 {
            let absolute = error.clone().abs();
            let is_small = absolute.clone().lower_equal_elem(huber_delta);
            let huber_small = error.clone().powf_scalar(2.0).div_scalar(2.0 * huber_delta);
            let huber_large = absolute.sub_scalar(huber_delta / 2.0);
            let huber = huber_large.mask_where(is_small, huber_small);

            let weight_high =
                Tensor::<B, 2>::ones([batch, output_length], &device).mul_scalar(quantile);
            let weight_low =
                Tensor::<B, 2>::ones([batch, output_length], &device).mul_scalar(1.0 - quantile);
            let sign_weight = weight_low.mask_where(positive, weight_high);
            sign_weight.mul(huber).mean()
        } else {
            let above = error.clone().mul_scalar(quantile);
            let below = error.mul_scalar(quantile - 1.0);
            below.mask_where(positive, above).mean()
        };

        total = Some(match total {
            Some(accumulated) => accumulated + loss,
            None => loss,
        });
    }

    total
        .expect("quantiles must not be empty")
        .div_scalar(num_quantiles as f64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use burn::backend::NdArray;

    fn scalar(prediction: f32, target: f32, quantiles: &[f64], huber_delta: f64) -> f32 {
        let device = Default::default();
        // output_length = 1, so the prediction row holds one value per quantile.
        let predictions = Tensor::<NdArray, 1>::from_floats(
            vec![prediction; quantiles.len()].as_slice(),
            &device,
        )
        .reshape([1, quantiles.len()]);
        let targets =
            Tensor::<NdArray, 1>::from_floats([target].as_slice(), &device).reshape([1, 1]);
        quantile_loss(predictions, targets, quantiles, huber_delta, 1).into_scalar()
    }

    #[test]
    fn test_pinball_positive_error() {
        // target - prediction = 1.0 > 0 -> quantile * error = 0.9
        let loss = scalar(0.0, 1.0, &[0.9], 0.0);
        assert!((loss - 0.9).abs() < 1e-6, "got {loss}");
    }

    #[test]
    fn test_pinball_negative_error() {
        // error = -1.0 < 0 -> (quantile - 1) * error = (-0.1)*(-1) = 0.1
        let loss = scalar(1.0, 0.0, &[0.9], 0.0);
        assert!((loss - 0.1).abs() < 1e-6, "got {loss}");
    }

    #[test]
    fn test_pinball_median_is_half_abs() {
        let loss = scalar(0.0, 1.0, &[0.5], 0.0);
        assert!((loss - 0.5).abs() < 1e-6, "got {loss}");
    }

    #[test]
    fn test_huber_large_error() {
        // error = 1.0, delta = 0.5: abs > delta -> huber = abs - delta/2 = 0.75
        // sign weight (error>0) = q = 0.5 -> 0.375
        let loss = scalar(0.0, 1.0, &[0.5], 0.5);
        assert!((loss - 0.375).abs() < 1e-6, "got {loss}");
    }

    #[test]
    fn test_huber_small_error() {
        // error = 0.4, delta = 0.5: abs <= delta -> huber = error^2/(2*delta) = 0.16
        // sign weight (error>0) = q = 0.5 -> 0.08
        let loss = scalar(0.0, 0.4, &[0.5], 0.5);
        assert!((loss - 0.08).abs() < 1e-6, "got {loss}");
    }

    #[test]
    fn test_mean_over_quantiles() {
        // [0.1, 0.9] with error = 1.0: (0.1 + 0.9)/2 = 0.5
        let loss = scalar(0.0, 1.0, &[0.1, 0.9], 0.0);
        assert!((loss - 0.5).abs() < 1e-6, "got {loss}");
    }
}
