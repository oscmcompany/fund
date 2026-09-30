//! Sends a request through transient failures with capped exponential backoff.

use std::future::Future;
use std::time::Duration;

/// Attempts before a transient failure is returned.
const ATTEMPTS: u32 = 6;

/// Why a request produced no body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FetchError {
    /// A status retrying will not change, with the body the vendor sent.
    Refused { status: u16, body: String },
    /// Still transient after every attempt, with the last cause.
    Exhausted { attempts: u32, last: String },
    /// A body that did not parse as the documented payload.
    Malformed { reason: String },
}

impl std::fmt::Display for FetchError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Refused { status, body } => write!(formatter, "refused with {status}: {body}"),
            Self::Exhausted { attempts, last } => {
                write!(formatter, "still failing after {attempts} attempts: {last}")
            }
            Self::Malformed { reason } => write!(formatter, "malformed payload: {reason}"),
        }
    }
}

/// One attempt's result.
pub(crate) enum Outcome {
    Body(Vec<u8>),
    Transient(String),
    Refused { status: u16, body: String },
}

/// Sends one request: 429, 5xx and transport failures are transient, any other failure status is refused.
pub(crate) async fn send(request: reqwest::RequestBuilder) -> Outcome {
    let response = match request.send().await {
        Ok(response) => response,
        Err(error) => return Outcome::Transient(error.to_string()),
    };
    let status = response.status();
    let body = match response.bytes().await {
        Ok(body) => body.to_vec(),
        Err(error) => return Outcome::Transient(error.to_string()),
    };
    if status.is_success() {
        Outcome::Body(body)
    } else if status.as_u16() == 429 || status.is_server_error() {
        Outcome::Transient(format!("status {status}"))
    } else {
        Outcome::Refused {
            status: status.as_u16(),
            body: String::from_utf8_lossy(&body).into_owned(),
        }
    }
}

/// Runs `attempt` until it yields a body, waiting 250ms, 500ms, 1s, 2s, then 4s between transient failures.
pub(crate) async fn with_retries<Attempt, Pending>(
    mut attempt: Attempt,
) -> Result<Vec<u8>, FetchError>
where
    Attempt: FnMut() -> Pending,
    Pending: Future<Output = Outcome>,
{
    let mut last = String::new();
    for number in 0..ATTEMPTS {
        if number > 0 {
            tokio::time::sleep(Duration::from_millis(250 << (number - 1).min(4))).await;
        }
        match attempt().await {
            Outcome::Body(body) => return Ok(body),
            Outcome::Refused { status, body } => return Err(FetchError::Refused { status, body }),
            Outcome::Transient(cause) => last = cause,
        }
    }
    Err(FetchError::Exhausted {
        attempts: ATTEMPTS,
        last,
    })
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn test_transient_failures_are_retried_with_backoff() {
        let calls = Cell::new(0);
        let started = tokio::time::Instant::now();
        let body = with_retries(|| {
            calls.set(calls.get() + 1);
            let call = calls.get();
            async move {
                if call < 3 {
                    Outcome::Transient("status 503".to_string())
                } else {
                    Outcome::Body(b"ok".to_vec())
                }
            }
        })
        .await;
        assert_eq!(body, Ok(b"ok".to_vec()));
        assert_eq!(calls.get(), 3);
        assert_eq!(started.elapsed(), Duration::from_millis(750));
    }

    #[tokio::test(start_paused = true)]
    async fn test_a_refusal_is_not_retried() {
        let calls = Cell::new(0);
        let result = with_retries(|| {
            calls.set(calls.get() + 1);
            async {
                Outcome::Refused {
                    status: 400,
                    body: "invalid".to_string(),
                }
            }
        })
        .await;
        assert_eq!(
            result,
            Err(FetchError::Refused {
                status: 400,
                body: "invalid".to_string()
            })
        );
        assert_eq!(calls.get(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn test_a_failure_that_never_clears_is_exhausted() {
        let started = tokio::time::Instant::now();
        let result = with_retries(|| async { Outcome::Transient("status 429".to_string()) }).await;
        assert_eq!(
            result,
            Err(FetchError::Exhausted {
                attempts: 6,
                last: "status 429".to_string()
            })
        );
        // 250 + 500 + 1000 + 2000 + 4000: the fifth wait is capped at four seconds.
        assert_eq!(started.elapsed(), Duration::from_millis(7_750));
    }
}
