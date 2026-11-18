//! Alpaca's SIP stream: each frame read into the quote and trade outcomes its REST pages read into, and a connection
//! that authenticates, subscribes and then hands on each frame it receives.

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio_tungstenite::tungstenite::Message as Frame;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use super::{
    Alpaca, AlpacaQuote, AlpacaQuoteOutcome, AlpacaTrade, AlpacaTradeOutcome, quote_outcome,
    trade_outcome,
};
use crate::common::market::Symbol;
use crate::ingest::{RefusedRow, RowRefusal};

const SIP_STREAM_URL: &str = "wss://stream.data.alpaca.markets/v2/sip";
/// How long the connection, authentication and subscription together may take before the attempt is abandoned.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);

/// One message of a frame as the stream spells it; a frame is an array of them.
#[derive(Deserialize)]
#[serde(tag = "T")]
enum Wire {
    #[serde(rename = "success")]
    Success {
        #[serde(rename = "msg")]
        message: String,
    },
    #[serde(rename = "error")]
    Error {
        code: u16,
        #[serde(rename = "msg")]
        message: String,
    },
    #[serde(rename = "subscription")]
    Subscription {},
    #[serde(rename = "q")]
    Quote {
        #[serde(rename = "S")]
        ticker: String,
        #[serde(flatten)]
        row: AlpacaQuote,
    },
    #[serde(rename = "t")]
    Trade {
        #[serde(rename = "S")]
        ticker: String,
        #[serde(flatten)]
        row: AlpacaTrade,
    },
    /// A kind read nowhere here, such as a correction or a cancel, whose payload has not been probed.
    #[serde(other)]
    Unread,
}

/// What one message of a frame became.
#[derive(Debug, Clone, PartialEq)]
pub enum StreamMessage {
    /// The connection or the authentication accepted, as the stream words it.
    Success {
        message: String,
    },
    /// A refusal, such as failed authentication or a connection over the account's limit.
    Error {
        code: u16,
        message: String,
    },
    Subscription,
    Quote(AlpacaQuoteOutcome),
    Trade(AlpacaTradeOutcome),
    Unread,
}

/// Each message of a frame in the order sent; a frame that is not an array of messages is refused whole.
pub fn read_frame(text: &str) -> Result<Vec<StreamMessage>, StreamError> {
    let messages: Vec<Wire> =
        serde_json::from_str(text).map_err(|error| StreamError::Malformed {
            reason: error.to_string(),
        })?;
    Ok(messages
        .into_iter()
        .map(|message| match message {
            Wire::Success { message } => StreamMessage::Success { message },
            Wire::Error { code, message } => StreamMessage::Error { code, message },
            Wire::Subscription {} => StreamMessage::Subscription,
            Wire::Quote { ticker, row } => StreamMessage::Quote(match Symbol::new(&ticker) {
                Ok(symbol) => quote_outcome(&symbol, &row),
                Err(cause) => AlpacaQuoteOutcome::Refused(RefusedRow {
                    ticker,
                    cause: RowRefusal::Symbol(cause),
                }),
            }),
            Wire::Trade { ticker, row } => StreamMessage::Trade(match Symbol::new(&ticker) {
                Ok(symbol) => trade_outcome(&symbol, &row),
                Err(cause) => AlpacaTradeOutcome::Refused(RefusedRow {
                    ticker,
                    cause: RowRefusal::Symbol(cause),
                }),
            }),
            Wire::Unread => StreamMessage::Unread,
        })
        .collect())
}

/// Why the stream stopped or never started.
#[derive(Debug, thiserror::Error)]
pub enum StreamError {
    #[error("the socket failed: {0}")]
    Socket(Box<tokio_tungstenite::tungstenite::Error>),
    #[error("the stream refused with {code}: {message}")]
    Refused { code: u16, message: String },
    /// The handshake received a frame other than the one its step awaits.
    #[error("awaiting {awaiting}, received {received:?}")]
    Unexpected {
        awaiting: &'static str,
        received: Vec<StreamMessage>,
    },
    #[error("a frame did not read: {reason}")]
    Malformed { reason: String },
    #[error("the stream closed")]
    Closed,
    #[error("the handshake did not finish within {0:?}")]
    HandshakeTimedOut(Duration),
}

impl From<tokio_tungstenite::tungstenite::Error> for StreamError {
    fn from(error: tokio_tungstenite::tungstenite::Error) -> Self {
        Self::Socket(Box::new(error))
    }
}

/// An authenticated connection to the SIP stream, subscribed to its symbols.
pub struct SipStream {
    socket: WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
}

impl Alpaca {
    /// Connects to the SIP stream and subscribes to the quotes of `quotes` and the trades of `trades`, refused when the
    /// handshake takes longer than `HANDSHAKE_TIMEOUT`.
    pub async fn stream(
        &self,
        quotes: &[Symbol],
        trades: &[Symbol],
    ) -> Result<SipStream, StreamError> {
        tokio::time::timeout(HANDSHAKE_TIMEOUT, self.handshake(quotes, trades))
            .await
            .map_err(|_elapsed| StreamError::HandshakeTimedOut(HANDSHAKE_TIMEOUT))?
    }

    async fn handshake(
        &self,
        quotes: &[Symbol],
        trades: &[Symbol],
    ) -> Result<SipStream, StreamError> {
        let (socket, _) = tokio_tungstenite::connect_async(SIP_STREAM_URL).await?;
        let mut stream = SipStream { socket };
        stream.await_reply("the connection", |message| {
            matches!(message, StreamMessage::Success { message } if message == "connected")
        })
        .await?;
        let authenticate = serde_json::json!({
            "action": "auth",
            "key": self.credentials.key_id.expose(),
            "secret": self.credentials.secret.expose(),
        });
        stream.send(&authenticate).await?;
        stream
            .await_reply("the authentication", |message| {
                matches!(message, StreamMessage::Success { message } if message == "authenticated")
            })
            .await?;
        let tickers = |symbols: &[Symbol]| -> Vec<String> {
            symbols
                .iter()
                .map(|symbol| symbol.as_str().to_string())
                .collect()
        };
        let subscribe = serde_json::json!({
            "action": "subscribe",
            "quotes": tickers(quotes),
            "trades": tickers(trades),
        });
        stream.send(&subscribe).await?;
        stream
            .await_reply("the subscription", |message| {
                matches!(message, StreamMessage::Subscription)
            })
            .await?;
        Ok(stream)
    }
}

impl SipStream {
    /// The messages of the next frame carrying any; an error message from the stream is returned as its refusal.
    pub async fn next(&mut self) -> Result<Vec<StreamMessage>, StreamError> {
        loop {
            let text = match self.socket.next().await {
                None => return Err(StreamError::Closed),
                Some(frame) => match frame? {
                    Frame::Text(text) => text,
                    Frame::Close(_) => return Err(StreamError::Closed),
                    Frame::Binary(bytes) => {
                        return Err(StreamError::Malformed {
                            reason: format!("a binary frame of {} bytes", bytes.len()),
                        });
                    }
                    Frame::Ping(_) | Frame::Pong(_) | Frame::Frame(_) => continue,
                },
            };
            let messages = read_frame(text.as_str())?;
            let mut unread = false;
            for message in &messages {
                match message {
                    StreamMessage::Error { code, message } => {
                        return Err(StreamError::Refused {
                            code: *code,
                            message: message.clone(),
                        });
                    }
                    StreamMessage::Unread => unread = true,
                    StreamMessage::Success { .. }
                    | StreamMessage::Subscription
                    | StreamMessage::Quote(_)
                    | StreamMessage::Trade(_) => {}
                }
            }
            // Logged whole, so the first correction or cancel seen shows the payload a reader would be typed against.
            if unread {
                tracing::warn!(frame = text.as_str(), "Stream message unread");
            }
            return Ok(messages);
        }
    }

    async fn send(&mut self, message: &serde_json::Value) -> Result<(), StreamError> {
        Ok(self.socket.send(Frame::text(message.to_string())).await?)
    }

    /// Reads the next frame, refused unless it is the single message `expected` accepts.
    async fn await_reply(
        &mut self,
        awaiting: &'static str,
        expected: impl Fn(&StreamMessage) -> bool,
    ) -> Result<(), StreamError> {
        let received = self.next().await?;
        match received.as_slice() {
            [message] if expected(message) => Ok(()),
            _ => Err(StreamError::Unexpected { awaiting, received }),
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};

    use super::*;
    use crate::common::market::record::Quote;
    use crate::common::market::trade_bars::{ConditionLetter, Correction, Print, Tape};
    use crate::common::market::{Price, Shares};
    use crate::ingest::{PrintOrigin, QuoteOrigin};

    /// A synthetic frame shaped as the live stream sends one: a quote, a trade, a correction, a quote under a ticker
    /// that does not read and a crossed quote.
    const FRAME: &str = r#"[
        {"T":"q","S":"ABC","bx":"V","bp":12.29,"bs":280,"ax":"Q","ap":12.33,"as":40,"c":["R"],"z":"C","t":"2026-10-07T16:40:08.837526736Z"},
        {"T":"t","S":"ABC","i":304931,"x":"D","p":12.3,"s":7,"c":["@","I"],"z":"C","t":"2026-10-07T16:40:08.695744094Z"},
        {"T":"c","S":"ABC","x":"D","oi":304931,"op":12.3,"os":7,"oc":["@"],"ci":304932,"cp":12.31,"cs":7,"cc":["@"],"z":"C","t":"2026-10-07T16:40:09Z"},
        {"T":"q","S":"abc","bx":"V","bp":1,"bs":1,"ax":"V","ap":2,"as":1,"c":["R"],"z":"C","t":"2026-10-07T16:40:09Z"},
        {"T":"q","S":"XYZ","bx":"V","bp":3,"bs":1,"ax":"V","ap":2,"as":1,"c":["R"],"z":"A","t":"2026-10-07T16:40:09Z"}
    ]"#;

    fn instant(text: &str) -> DateTime<Utc> {
        text.parse().unwrap()
    }

    /// Each message reads in order into the outcome its REST row would, and a kind not read here is kept as unread.
    #[test]
    fn test_a_frame_reads_each_message_in_order() {
        let abc = Symbol::new("ABC").unwrap();
        let quote = Quote::new(
            abc.clone(),
            instant("2026-10-07T16:40:08.837526736Z"),
            Price::from_ticks(12_290_000).unwrap(),
            Price::from_ticks(12_330_000).unwrap(),
            Shares::whole(280).unwrap(),
            Shares::whole(40).unwrap(),
        )
        .unwrap();
        let messages = read_frame(FRAME).unwrap();
        assert_eq!(messages.len(), 5);
        assert_eq!(
            messages[0],
            StreamMessage::Quote(AlpacaQuoteOutcome::Quote {
                quote,
                origin: QuoteOrigin {
                    bid_exchange: Some("V".to_string()),
                    ask_exchange: Some("Q".to_string()),
                    tape: Some("C".to_string()),
                    conditions: vec!["R".to_string()],
                },
            })
        );
        assert_eq!(
            messages[1],
            StreamMessage::Trade(AlpacaTradeOutcome::Print {
                print: Print::new(
                    abc,
                    instant("2026-10-07T16:40:08.695744094Z"),
                    Price::from_ticks(12_300_000).unwrap(),
                    Shares::whole(7).unwrap()
                ),
                tape: Tape::UnlistedTrading,
                letters: vec![ConditionLetter::of('@'), ConditionLetter::of('I')],
                correction: Correction::Stands,
                origin: PrintOrigin {
                    exchange: Some("D".to_string()),
                    tape: "C".to_string(),
                    id: Some("304931".to_string()),
                },
            })
        );
        assert_eq!(messages[2], StreamMessage::Unread);
        assert!(matches!(
            &messages[3],
            StreamMessage::Quote(AlpacaQuoteOutcome::Refused(row))
                if matches!(row.cause(), RowRefusal::Symbol(_))
        ));
        assert!(matches!(
            &messages[4],
            StreamMessage::Quote(AlpacaQuoteOutcome::Refused(row))
                if matches!(row.cause(), RowRefusal::Quote(_))
        ));
    }

    /// The handshake's replies read as the live stream words them, a refusal with its code.
    #[test]
    fn test_the_handshake_replies_read_as_sent() {
        assert_eq!(
            read_frame(r#"[{"T":"success","msg":"authenticated"}]"#).unwrap(),
            [StreamMessage::Success {
                message: "authenticated".to_string()
            }]
        );
        assert_eq!(
            read_frame(r#"[{"T":"error","code":402,"msg":"auth failed"}]"#).unwrap(),
            [StreamMessage::Error {
                code: 402,
                message: "auth failed".to_string()
            }]
        );
        assert_eq!(
            read_frame(
                r#"[{"T":"subscription","trades":[],"quotes":["ABC"],"corrections":[],"cancelErrors":[]}]"#
            )
            .unwrap(),
            [StreamMessage::Subscription]
        );
    }

    /// A frame that is not an array of whole messages is refused whole rather than read in part.
    #[test]
    fn test_a_frame_that_is_not_an_array_is_malformed() {
        for frame in [r#"{"T":"q"}"#, "[{", r#"[{"T":"q","S":"ABC"}]"#] {
            assert!(
                matches!(read_frame(frame), Err(StreamError::Malformed { .. })),
                "{frame}"
            );
        }
    }

    /// Opens the SIP stream on the paper keys and reads quotes of a liquid name; places no order.
    #[tokio::test]
    #[ignore = "opens Alpaca's SIP stream during market hours; run deliberately under secretspec"]
    async fn live_the_sip_stream_sends_quotes() {
        let alpaca = Alpaca::from_environment(reqwest::Client::new()).unwrap();
        let spy = Symbol::new("SPY").unwrap();
        let mut stream = alpaca
            .stream(std::slice::from_ref(&spy), &[])
            .await
            .unwrap();
        let mut quotes = 0;
        while quotes < 10 {
            let messages = tokio::time::timeout(std::time::Duration::from_secs(30), stream.next())
                .await
                .unwrap()
                .unwrap();
            for message in messages {
                match message {
                    StreamMessage::Quote(AlpacaQuoteOutcome::Quote { quote, .. }) => {
                        assert_eq!(quote.symbol(), &spy);
                        quotes += 1;
                    }
                    other @ (StreamMessage::Quote(
                        AlpacaQuoteOutcome::OneSided { .. } | AlpacaQuoteOutcome::Refused(_),
                    )
                    | StreamMessage::Success { .. }
                    | StreamMessage::Error { .. }
                    | StreamMessage::Subscription
                    | StreamMessage::Trade(_)
                    | StreamMessage::Unread) => panic!("{other:?}"),
                }
            }
        }
    }
}
