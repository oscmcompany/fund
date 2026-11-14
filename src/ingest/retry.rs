//! Sends a request through transient failures with exponential backoff.

use std::future::Future;
use std::io::Write;
use std::time::Duration;

use flate2::Compression;
use flate2::write::GzEncoder;

/// Attempts before a transient failure is returned.
const ATTEMPTS: u32 = 6;

/// Why a request produced no body.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FetchError {
    /// A status retrying will not change, with the body the vendor sent.
    #[error("refused with {status}: {body}")]
    Refused { status: u16, body: String },
    /// Still transient after every attempt, with the last cause.
    #[error("still failing after {attempts} attempts: {last}")]
    Exhausted { attempts: u32, last: String },
    /// A body that did not parse as the documented payload.
    #[error("malformed payload: {reason}")]
    Malformed { reason: String },
}

/// A request that has not answered in this long is abandoned and retried; the largest body, a full grouped daily,
/// is a few megabytes.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

/// How much of a refused response's body is kept, which Alpaca's `invalid symbol` message fits well inside.
const ERROR_BODY_LIMIT: usize = 4 * 1024;

/// One attempt's result.
pub(crate) enum Outcome {
    Body(Vec<u8>),
    Transient(String),
    Refused { status: u16, body: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Class {
    Success,
    Transient,
    Refused,
}

/// 429 and 5xx are transient; any other failure status is refused.
pub(crate) fn classify(status: reqwest::StatusCode) -> Class {
    if status.is_success() {
        Class::Success
    } else if status == reqwest::StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
        Class::Transient
    } else {
        Class::Refused
    }
}

/// Sends one request under a deadline, reading a body only once its status says it is wanted. Errors are kept
/// without their URL, so a credential in a query can never reach a log through one.
pub(crate) async fn send(request: reqwest::RequestBuilder) -> Outcome {
    let response = match request.timeout(REQUEST_TIMEOUT).send().await {
        Ok(response) => response,
        Err(error) => return Outcome::Transient(error.without_url().to_string()),
    };
    let status = response.status();
    match classify(status) {
        Class::Success => match response.bytes().await {
            Ok(body) => Outcome::Body(body.to_vec()),
            Err(error) => Outcome::Transient(error.without_url().to_string()),
        },
        Class::Transient => Outcome::Transient(format!("status {status}")),
        Class::Refused => refused(response).await,
    }
}

/// Sends one request like `send`, gzipping the body as it streams, so a body of gigabytes is never held uncompressed;
/// `timeout` bounds the whole transfer.
pub(crate) async fn send_gzipped(request: reqwest::RequestBuilder, timeout: Duration) -> Outcome {
    let mut response = match request.timeout(timeout).send().await {
        Ok(response) => response,
        Err(error) => return Outcome::Transient(error.without_url().to_string()),
    };
    let status = response.status();
    match classify(status) {
        Class::Success => {
            let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
            loop {
                let written = match response.chunk().await {
                    Ok(Some(chunk)) => encoder.write_all(&chunk),
                    Ok(None) => break,
                    Err(error) => return Outcome::Transient(error.without_url().to_string()),
                };
                if let Err(error) = written {
                    return Outcome::Transient(error.to_string());
                }
            }
            match encoder.finish() {
                Ok(body) => Outcome::Body(body),
                Err(error) => Outcome::Transient(error.to_string()),
            }
        }
        Class::Transient => Outcome::Transient(format!("status {status}")),
        Class::Refused => refused(response).await,
    }
}

/// A refused response with the start of the body the vendor sent.
async fn refused(mut response: reqwest::Response) -> Outcome {
    let status = response.status();
    let mut body = Vec::new();
    while body.len() < ERROR_BODY_LIMIT {
        match response.chunk().await {
            Ok(Some(chunk)) => body.extend_from_slice(&chunk),
            Ok(None) | Err(_) => break,
        }
    }
    body.truncate(ERROR_BODY_LIMIT);
    Outcome::Refused {
        status: status.as_u16(),
        body: String::from_utf8_lossy(&body).into_owned(),
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
            Outcome::Transient(cause) => {
                if number + 1 < ATTEMPTS {
                    tracing::warn!(attempt = number + 1, %cause, "Retrying a request");
                }
                last = cause;
            }
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

    #[test]
    fn test_only_rate_limits_and_server_errors_are_transient() {
        let classes: Vec<(u16, Class)> = [200, 204, 400, 401, 403, 404, 429, 500, 502, 503]
            .into_iter()
            .map(|code| (code, classify(reqwest::StatusCode::from_u16(code).unwrap())))
            .collect();
        assert_eq!(
            classes,
            [
                (200, Class::Success),
                (204, Class::Success),
                (400, Class::Refused),
                (401, Class::Refused),
                (403, Class::Refused),
                (404, Class::Refused),
                (429, Class::Transient),
                (500, Class::Transient),
                (502, Class::Transient),
                (503, Class::Transient),
            ]
        );
    }

    #[tokio::test]
    async fn test_a_transport_error_does_not_carry_its_url() {
        // Nothing listens on port 1, so the connection is refused locally.
        let request = reqwest::Client::new().get("http://127.0.0.1:1/bars?apiKey=hidden");
        match send(request).await {
            Outcome::Transient(cause) => assert!(!cause.contains("hidden"), "{cause}"),
            Outcome::Body(_) | Outcome::Refused { .. } => panic!("port 1 answered"),
        }
    }

    /// Answers one connection on a local port with `pieces`, flushed one at a time, then closes it.
    fn serve_once(pieces: Vec<&'static [u8]>) -> String {
        use std::io::{BufRead, BufReader};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut line = String::new();
            while reader.read_line(&mut line).unwrap() > 0 && line != "\r\n" {
                line.clear();
            }
            for piece in pieces {
                stream.write_all(piece).unwrap();
                stream.flush().unwrap();
                std::thread::sleep(Duration::from_millis(10));
            }
        });
        format!("http://{address}/rows")
    }

    #[tokio::test]
    async fn test_a_chunked_body_gzips_to_its_exact_bytes() {
        let url = serve_once(vec![
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n",
            b"6\r\nts,px\n\r\n",
            b"9\r\n1,100.25\n\r\n",
            b"9\r\n2,100.50\n\r\n",
            b"0\r\n\r\n",
        ]);
        let outcome = send_gzipped(reqwest::Client::new().get(url), Duration::from_secs(5)).await;
        let Outcome::Body(gzipped) = outcome else {
            panic!("a whole body was not returned");
        };
        let mut decompressed = String::new();
        std::io::Read::read_to_string(
            &mut flate2::read::GzDecoder::new(gzipped.as_slice()),
            &mut decompressed,
        )
        .unwrap();
        assert_eq!(decompressed, "ts,px\n1,100.25\n2,100.50\n");
    }

    #[tokio::test]
    async fn test_a_body_cut_short_is_transient_not_a_partial_file() {
        let url = serve_once(vec![
            b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\nConnection: close\r\n\r\n",
            b"ts,px\n1,100.25\n",
        ]);
        let outcome = send_gzipped(reqwest::Client::new().get(url), Duration::from_secs(5)).await;
        match outcome {
            Outcome::Transient(_) => {}
            Outcome::Body(_) | Outcome::Refused { .. } => panic!("a truncated body was accepted"),
        }
    }

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
        // 250 + 500 + 1000 + 2000 + 4000 between six attempts.
        assert_eq!(started.elapsed(), Duration::from_millis(7_750));
    }
}
