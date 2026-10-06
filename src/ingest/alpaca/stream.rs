//! Alpaca's real-time SIP stream: trades, quotes and trading statuses for a universe, read through the same row
//! conversions as the REST history, so a live print or quote and an archived one become the same record.

use std::collections::VecDeque;

use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::Value;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::common::market::Symbol;
use crate::ingest::alpaca::{
    Alpaca, AlpacaQuote, AlpacaQuoteOutcome, AlpacaTrade, AlpacaTradeOutcome, quote_outcome,
    trade_outcome,
};
use crate::ingest::{RefusedRow, RowRefusal};

/// The SIP feed, the same for paper and live keys.
const STREAM_URL: &str = "wss://stream.data.alpaca.markets/v2/sip";

/// The tape's own identifier for a print, which a correction or cancel names and a REST read returns too.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize)]
#[serde(transparent)]
pub struct TradeId(u64);

/// One message off the stream, in the order Alpaca sent it.
#[derive(Debug, Clone, PartialEq)]
pub enum StreamMessage {
    Connected,
    Authenticated,
    /// What the stream now sends, as Alpaca confirmed it.
    Subscribed {
        trades: Vec<Symbol>,
        quotes: Vec<Symbol>,
        statuses: Vec<Symbol>,
    },
    Trade {
        id: Option<TradeId>,
        outcome: AlpacaTradeOutcome,
    },
    Quote(AlpacaQuoteOutcome),
    /// Alpaca's error message, such as an invalid request or a second connection past the plan's limit.
    Refused {
        code: u16,
        message: String,
    },
    /// A kind this client has not seen a real payload of, kept whole rather than guessed at.
    Unrecognized {
        kind: String,
        raw: String,
    },
}

/// Why the stream could not be opened or read.
#[derive(Debug)]
pub enum StreamError {
    Socket(String),
    Refused {
        code: u16,
        message: String,
    },
    Malformed {
        reason: String,
    },
    /// Alpaca confirmed a subscription missing these symbols from its trades, quotes or statuses.
    Unconfirmed {
        missing: Vec<Symbol>,
    },
    /// The socket closed before Alpaca confirmed what was asked of it.
    Closed {
        awaiting: &'static str,
    },
}

impl std::fmt::Display for StreamError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Socket(cause) => write!(formatter, "the stream's socket failed: {cause}"),
            Self::Refused { code, message } => {
                write!(formatter, "the stream refused with {code}: {message}")
            }
            Self::Malformed { reason } => {
                write!(formatter, "the stream sent an unreadable frame: {reason}")
            }
            Self::Unconfirmed { missing } => {
                let names: Vec<&str> = missing.iter().map(Symbol::as_str).collect();
                write!(
                    formatter,
                    "the stream left {} unsubscribed",
                    names.join(", ")
                )
            }
            Self::Closed { awaiting } => write!(formatter, "the stream closed awaiting {awaiting}"),
        }
    }
}

impl std::error::Error for StreamError {}

#[derive(Deserialize)]
struct SubscriptionPayload {
    #[serde(default)]
    trades: Vec<Symbol>,
    #[serde(default)]
    quotes: Vec<Symbol>,
    #[serde(default)]
    statuses: Vec<Symbol>,
}

#[derive(Deserialize)]
struct ErrorPayload {
    code: u16,
    msg: String,
}

/// An open SIP stream, authenticated and subscribed.
pub struct MarketStream {
    socket: WebSocketStream<MaybeTlsStream<TcpStream>>,
    pending: VecDeque<StreamMessage>,
}

impl MarketStream {
    /// Connects, authenticates and subscribes `symbols`' trades, quotes and statuses, returning once Alpaca confirms
    /// the subscription; corrections and cancels come with the trades unasked.
    pub async fn open(alpaca: &Alpaca, symbols: &[Symbol]) -> Result<Self, StreamError> {
        let (socket, _) = tokio_tungstenite::connect_async(STREAM_URL)
            .await
            .map_err(|error| StreamError::Socket(error.to_string()))?;
        let mut stream = Self {
            socket,
            pending: VecDeque::new(),
        };
        stream
            .awaiting("the connection", |message| {
                matches!(message, StreamMessage::Connected)
            })
            .await?;
        let authenticate =
            serde_json::json!({"action": "auth", "key": alpaca.key_id, "secret": alpaca.secret});
        stream.send(&authenticate).await?;
        stream
            .awaiting("authentication", |message| {
                matches!(message, StreamMessage::Authenticated)
            })
            .await?;
        let names: Vec<&str> = symbols.iter().map(Symbol::as_str).collect();
        let subscribe = serde_json::json!({"action": "subscribe", "trades": names, "quotes": names, "statuses": names});
        stream.send(&subscribe).await?;
        let confirmed = stream
            .awaiting("the subscription", |message| {
                matches!(message, StreamMessage::Subscribed { .. })
            })
            .await?;
        let missing = unconfirmed(symbols, &confirmed);
        if !missing.is_empty() {
            return Err(StreamError::Unconfirmed { missing });
        }
        Ok(stream)
    }

    /// The next message, `None` once the socket closes.
    pub async fn next(&mut self) -> Option<Result<StreamMessage, StreamError>> {
        loop {
            if let Some(message) = self.pending.pop_front() {
                return Some(Ok(message));
            }
            let text = match self.socket.next().await? {
                Ok(Message::Text(text)) => text,
                Ok(Message::Close(_)) => return None,
                // Pings are answered by the socket itself; nothing else carries messages.
                Ok(
                    Message::Ping(_) | Message::Pong(_) | Message::Binary(_) | Message::Frame(_),
                ) => continue,
                Err(error) => return Some(Err(StreamError::Socket(error.to_string()))),
            };
            match messages(&text) {
                Ok(messages) => self.pending.extend(messages),
                Err(error) => return Some(Err(error)),
            }
        }
    }

    async fn send(&mut self, request: &Value) -> Result<(), StreamError> {
        self.socket
            .send(Message::Text(request.to_string().into()))
            .await
            .map_err(|error| StreamError::Socket(error.to_string()))
    }

    /// Reads until `confirms` accepts a message, refusing on Alpaca's error and on a close.
    async fn awaiting(
        &mut self,
        what: &'static str,
        confirms: impl Fn(&StreamMessage) -> bool,
    ) -> Result<StreamMessage, StreamError> {
        loop {
            match self.next().await {
                None => return Err(StreamError::Closed { awaiting: what }),
                Some(Err(error)) => return Err(error),
                Some(Ok(StreamMessage::Refused { code, message })) => {
                    return Err(StreamError::Refused { code, message });
                }
                Some(Ok(message)) => {
                    if confirms(&message) {
                        return Ok(message);
                    }
                }
            }
        }
    }
}

/// The asked-for symbols a confirmation leaves out of any of its trades, quotes or statuses.
fn unconfirmed(asked: &[Symbol], confirmed: &StreamMessage) -> Vec<Symbol> {
    match confirmed {
        StreamMessage::Subscribed {
            trades,
            quotes,
            statuses,
        } => asked
            .iter()
            .filter(|symbol| {
                ![trades, quotes, statuses]
                    .iter()
                    .all(|listed| listed.contains(symbol))
            })
            .cloned()
            .collect(),
        StreamMessage::Connected
        | StreamMessage::Authenticated
        | StreamMessage::Trade { .. }
        | StreamMessage::Quote(_)
        | StreamMessage::Refused { .. }
        | StreamMessage::Unrecognized { .. } => asked.to_vec(),
    }
}

/// One frame, a JSON array of messages each tagged by its `T`; an element with no `T` makes the frame malformed.
fn messages(text: &str) -> Result<Vec<StreamMessage>, StreamError> {
    let malformed = |reason: String| StreamError::Malformed { reason };
    let elements: Vec<Value> =
        serde_json::from_str(text).map_err(|error| malformed(error.to_string()))?;
    elements
        .into_iter()
        .map(|element| {
            let kind = element
                .get("T")
                .and_then(Value::as_str)
                .ok_or_else(|| malformed(format!("no `T` in {element}")))?
                .to_string();
            let read = |element: Value| element.to_string();
            Ok(match kind.as_str() {
                "success" => match element.get("msg").and_then(Value::as_str) {
                    Some("connected") => StreamMessage::Connected,
                    Some("authenticated") => StreamMessage::Authenticated,
                    Some(_) | None => StreamMessage::Unrecognized {
                        raw: read(element),
                        kind,
                    },
                },
                "subscription" => {
                    let payload: SubscriptionPayload = serde_json::from_value(element)
                        .map_err(|error| malformed(error.to_string()))?;
                    StreamMessage::Subscribed {
                        trades: payload.trades,
                        quotes: payload.quotes,
                        statuses: payload.statuses,
                    }
                }
                "error" => {
                    let payload: ErrorPayload = serde_json::from_value(element)
                        .map_err(|error| malformed(error.to_string()))?;
                    StreamMessage::Refused {
                        code: payload.code,
                        message: payload.msg,
                    }
                }
                "t" => {
                    let id = element
                        .get("i")
                        .cloned()
                        .map(serde_json::from_value::<TradeId>)
                        .transpose()
                        .map_err(|error| malformed(error.to_string()))?;
                    let outcome = match symbol(&element) {
                        Ok(symbol) => {
                            let row: AlpacaTrade = serde_json::from_value(element)
                                .map_err(|error| malformed(error.to_string()))?;
                            trade_outcome(&symbol, &row)
                        }
                        Err(refused) => AlpacaTradeOutcome::Refused(refused),
                    };
                    StreamMessage::Trade { id, outcome }
                }
                "q" => StreamMessage::Quote(match symbol(&element) {
                    Ok(symbol) => {
                        let row: AlpacaQuote = serde_json::from_value(element)
                            .map_err(|error| malformed(error.to_string()))?;
                        quote_outcome(&symbol, &row)
                    }
                    Err(refused) => AlpacaQuoteOutcome::Refused(refused),
                }),
                _ => StreamMessage::Unrecognized {
                    raw: read(element),
                    kind,
                },
            })
        })
        .collect()
}

/// The message's `S`, refused as a row when it is no symbol this system reads.
fn symbol(element: &Value) -> Result<Symbol, RefusedRow> {
    let ticker = element.get("S").and_then(Value::as_str).unwrap_or_default();
    Symbol::new(ticker).map_err(|cause| RefusedRow {
        ticker: ticker.to_string(),
        cause: RowRefusal::Symbol(cause),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::market::record::Quote;
    use crate::common::market::trade_bars::{Print, Tape};
    use crate::common::market::{Price, Shares};

    /// The control frames as the SIP stream sent them on 2026-10-06, and its answer to a malformed request.
    #[test]
    fn test_control_frames_read_as_the_stream_sends_them() {
        assert_eq!(
            messages(r#"[{"T":"success","msg":"connected"}]"#).unwrap(),
            [StreamMessage::Connected]
        );
        assert_eq!(
            messages(r#"[{"T":"success","msg":"authenticated"}]"#).unwrap(),
            [StreamMessage::Authenticated]
        );
        assert_eq!(
            messages(r#"[{"T":"subscription","trades":["AAPL","SPY"],"quotes":["SPY"],"statuses":["AAPL","SPY"],"corrections":["AAPL","SPY"],"cancelErrors":["AAPL","SPY"]}]"#).unwrap(),
            [StreamMessage::Subscribed {
                trades: vec![Symbol::new("AAPL").unwrap(), Symbol::new("SPY").unwrap()],
                quotes: vec![Symbol::new("SPY").unwrap()],
                statuses: vec![Symbol::new("AAPL").unwrap(), Symbol::new("SPY").unwrap()],
            }]
        );
        assert_eq!(
            messages(r#"[{"T":"error","code":400,"msg":"invalid syntax"}]"#).unwrap(),
            [StreamMessage::Refused {
                code: 400,
                message: "invalid syntax".to_string()
            }]
        );
    }

    /// A trade and two quotes as the SIP stream sent them for SPY on 2026-10-06, in one frame, read through the same
    /// conversion as the REST history.
    #[test]
    fn test_trades_and_quotes_read_as_the_stream_sends_them() {
        let frame = r#"[{"T":"t","S":"SPY","i":52983625699126,"x":"P","p":779.87,"s":4,"c":[" ","F","T","I"],"z":"B","t":"2026-10-06T22:36:01.216274112Z"},{"T":"q","S":"SPY","bx":"K","bp":779.8,"bs":480,"ax":"P","ap":779.87,"as":1000,"c":["R"],"z":"B","t":"2026-10-06T22:35:56.179976636Z"},{"T":"q","S":"SPY","bx":"M","bp":779.81,"bs":280,"ax":"P","ap":779.87,"as":1000,"c":["R"],"z":"B","t":"2026-10-06T22:35:56.180096716Z"}]"#;
        let read = messages(frame).unwrap();
        let spy = Symbol::new("SPY").unwrap();
        let price = |dollars| Price::from_dollars(dollars).unwrap();
        let quote = |bid, bid_size, at: &str| {
            StreamMessage::Quote(AlpacaQuoteOutcome::Quote(
                Quote::new(
                    spy.clone(),
                    at.parse().unwrap(),
                    price(bid),
                    price(779.87),
                    Shares::whole(bid_size).unwrap(),
                    Shares::whole(1000).unwrap(),
                )
                .unwrap(),
            ))
        };
        assert_eq!(read.len(), 3);
        match &read[0] {
            StreamMessage::Trade {
                id,
                outcome:
                    AlpacaTradeOutcome::Print {
                        print,
                        tape,
                        letters,
                        corrected,
                    },
            } => {
                assert_eq!(*id, Some(TradeId(52_983_625_699_126)));
                assert_eq!(*tape, Tape::ConsolidatedTape);
                assert_eq!(*letters, [' ', 'F', 'T', 'I']);
                assert!(!corrected);
                match print {
                    Print::Trade(trade) => {
                        assert_eq!(
                            (trade.price(), trade.size()),
                            (price(779.87), Shares::whole(4).unwrap())
                        );
                        assert_eq!(
                            trade.timestamp(),
                            "2026-10-06T22:36:01.216274112Z"
                                .parse::<chrono::DateTime<chrono::Utc>>()
                                .unwrap()
                        );
                    }
                    Print::Unsized { .. } => panic!("a four-share print is sized"),
                }
            }
            other @ (StreamMessage::Trade {
                outcome: AlpacaTradeOutcome::Refused(_),
                ..
            }
            | StreamMessage::Connected
            | StreamMessage::Authenticated
            | StreamMessage::Subscribed { .. }
            | StreamMessage::Quote(_)
            | StreamMessage::Refused { .. }
            | StreamMessage::Unrecognized { .. }) => panic!("{other:?}"),
        }
        assert_eq!(read[1], quote(779.8, 480, "2026-10-06T22:35:56.179976636Z"));
        assert_eq!(
            read[2],
            quote(779.81, 280, "2026-10-06T22:35:56.180096716Z")
        );
    }

    /// A confirmation leaving a symbol out of any channel leaves it unconfirmed.
    #[test]
    fn test_a_partial_subscription_names_what_it_left_out() {
        let (aapl, spy) = (Symbol::new("AAPL").unwrap(), Symbol::new("SPY").unwrap());
        let confirmed = StreamMessage::Subscribed {
            trades: vec![aapl.clone(), spy.clone()],
            quotes: vec![spy.clone()],
            statuses: vec![aapl.clone(), spy.clone()],
        };
        let only_spy = std::slice::from_ref(&spy);
        assert_eq!(
            unconfirmed(&[aapl.clone(), spy.clone()], &confirmed),
            [aapl]
        );
        assert_eq!(unconfirmed(only_spy, &confirmed), Vec::<Symbol>::new());
        assert_eq!(
            unconfirmed(only_spy, &StreamMessage::Authenticated),
            only_spy
        );
    }

    /// A kind with no captured payload is kept whole, never dropped or guessed at; the kind here is invented.
    #[test]
    fn test_an_unseen_kind_is_kept_whole() {
        assert_eq!(
            messages(r#"[{"T":"zz","S":"SPY"}]"#).unwrap(),
            [StreamMessage::Unrecognized {
                kind: "zz".to_string(),
                raw: r#"{"S":"SPY","T":"zz"}"#.to_string()
            }]
        );
        assert!(matches!(
            messages("not json"),
            Err(StreamError::Malformed { .. })
        ));
        assert!(matches!(
            messages(r#"[{"S":"SPY"}]"#),
            Err(StreamError::Malformed { .. })
        ));
        assert!(matches!(
            messages(
                r#"[{"T":"t","S":"SPY","i":"x","p":1,"s":1,"z":"B","t":"2026-10-06T22:36:01Z"}]"#
            ),
            Err(StreamError::Malformed { .. })
        ));
        assert!(matches!(
            messages(r#"[{"T":"t","S":"spy!","p":1,"s":1,"z":"B","t":"2026-10-06T22:36:01Z"}]"#)
                .unwrap()
                .as_slice(),
            [StreamMessage::Trade {
                outcome: AlpacaTradeOutcome::Refused(_),
                ..
            }]
        ));
    }

    /// Opens the SIP stream for SPY and reads until a trade or quote arrives; run while SPY trades, regular or
    /// extended hours.
    #[tokio::test]
    #[ignore = "reads Alpaca's live SIP stream; run deliberately under secretspec while SPY trades"]
    async fn live_the_stream_opens_and_delivers_spy() {
        let alpaca = Alpaca::from_environment(reqwest::Client::new()).unwrap();
        let mut stream = MarketStream::open(&alpaca, &[Symbol::new("SPY").unwrap()])
            .await
            .unwrap();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            match tokio::time::timeout_at(deadline, stream.next())
                .await
                .expect("SPY printed within 30 seconds")
            {
                Some(Ok(StreamMessage::Trade {
                    outcome: AlpacaTradeOutcome::Print { .. },
                    ..
                }))
                | Some(Ok(StreamMessage::Quote(AlpacaQuoteOutcome::Quote(_)))) => break,
                Some(Ok(_)) => {}
                Some(Err(error)) => panic!("{error}"),
                None => panic!("the stream closed"),
            }
        }
    }
}
