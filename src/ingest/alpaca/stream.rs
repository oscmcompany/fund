//! Alpaca's real-time SIP stream: trades, quotes and trading statuses for a universe, read through the same row
//! conversions as the REST history, so a live print or quote and an archived one become the same record.

use std::collections::{BTreeMap, VecDeque};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::Value;
use strum::IntoEnumIterator;
use tokio::net::TcpStream;
use tokio::time::{Instant, timeout_at};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::common::market::Symbol;
use crate::ingest::alpaca::{
    Alpaca, AlpacaQuote, AlpacaQuoteOutcome, AlpacaTrade, AlpacaTradeOutcome, Credentials,
    quote_outcome, trade_outcome,
};
use crate::ingest::{RefusedRow, RowRefusal};

/// The SIP feed, the same for paper and live keys.
const STREAM_URL: &str = "wss://stream.data.alpaca.markets/v2/sip";

/// How long opening may take, from the connect to the confirmed subscription; reads after it wait on the market.
const OPENING_WINDOW: Duration = Duration::from_secs(30);

/// A channel a symbol is subscribed on, named as Alpaca's subscription names it.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    strum::Display,
    strum::EnumString,
    strum::IntoStaticStr,
    strum::EnumIter,
)]
#[strum(serialize_all = "snake_case")]
pub enum Channel {
    Trades,
    Quotes,
    Statuses,
}

/// A print's identity on the tape: the reporting exchange and its number there, which a correction or cancel names
/// and a REST read returns too; numbers repeat across exchanges, so the exchange is part of it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TradeId {
    exchange: char,
    number: u64,
}

impl TradeId {
    /// Orders before every real identity, so a range of seen prints can start at an instant.
    pub(crate) const LEAST: Self = Self {
        exchange: '\0',
        number: 0,
    };

    /// Reads `x` and `i` from a trade element, as the stream and the REST history both spell them.
    pub(crate) fn read(element: &Value) -> Result<Self, StreamElementRefusal> {
        let exchange = element
            .get("x")
            .and_then(Value::as_str)
            .ok_or(StreamElementRefusal::NoExchange)?;
        let mut letters = exchange.chars();
        let (Some(letter), None) = (letters.next(), letters.next()) else {
            return Err(StreamElementRefusal::ExchangeNotOneLetter {
                raw: exchange.to_string(),
            });
        };
        let number = element.get("i").ok_or(StreamElementRefusal::NoNumber)?;
        let number = number
            .as_u64()
            .ok_or_else(|| StreamElementRefusal::NumberNotUnsigned {
                raw: number.to_string(),
            })?;
        Ok(Self {
            exchange: letter,
            number,
        })
    }
}

/// Why one element of a frame, or one REST trade's identity, did not read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamElementRefusal {
    NoKind,
    NoExchange,
    ExchangeNotOneLetter {
        raw: String,
    },
    NoNumber,
    NumberNotUnsigned {
        raw: String,
    },
    /// A payload that did not parse as its kind's.
    Unreadable {
        reason: String,
    },
}

impl std::fmt::Display for StreamElementRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoKind => write!(formatter, "no `T`"),
            Self::NoExchange => write!(formatter, "a trade with no `x`"),
            Self::ExchangeNotOneLetter { raw } => {
                write!(formatter, "an exchange `{raw}` that is not one letter")
            }
            Self::NoNumber => write!(formatter, "a trade with no `i`"),
            Self::NumberNotUnsigned { raw } => {
                write!(formatter, "an `i` that is not an unsigned integer: {raw}")
            }
            Self::Unreadable { reason } => write!(formatter, "{reason}"),
        }
    }
}

impl std::error::Error for StreamElementRefusal {}

/// One message off the stream, in the order Alpaca sent it.
#[derive(Debug, Clone, PartialEq)]
pub enum StreamMessage {
    Connected,
    Authenticated,
    /// What the stream now sends on each channel, as Alpaca confirmed it.
    Subscribed(BTreeMap<Channel, Vec<Symbol>>),
    Trade {
        id: TradeId,
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
    /// One element of a frame that did not read, kept whole; the frame's other messages still arrive.
    Malformed {
        cause: StreamElementRefusal,
        raw: String,
    },
}

/// A step of opening the stream, named as a refusal awaiting it reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display)]
pub enum OpeningStep {
    #[strum(serialize = "the socket")]
    Socket,
    #[strum(serialize = "the connection")]
    Connection,
    #[strum(serialize = "authentication")]
    Authentication,
    #[strum(serialize = "the subscription")]
    Subscription,
}

impl OpeningStep {
    /// Whether `message` is Alpaca's confirmation of this step; the socket has none, as connecting confirms it.
    fn confirms(self, message: &StreamMessage) -> bool {
        match (self, message) {
            (Self::Connection, StreamMessage::Connected)
            | (Self::Authentication, StreamMessage::Authenticated)
            | (Self::Subscription, StreamMessage::Subscribed(_)) => true,
            (
                Self::Socket | Self::Connection | Self::Authentication | Self::Subscription,
                StreamMessage::Connected
                | StreamMessage::Authenticated
                | StreamMessage::Subscribed(_)
                | StreamMessage::Trade { .. }
                | StreamMessage::Quote(_)
                | StreamMessage::Refused { .. }
                | StreamMessage::Unrecognized { .. }
                | StreamMessage::Malformed { .. },
            ) => false,
        }
    }
}

/// Why the stream could not be opened or read.
#[derive(Debug, Clone, PartialEq)]
pub enum StreamError {
    Socket(String),
    Refused {
        code: u16,
        message: String,
    },
    Malformed {
        reason: String,
    },
    /// Alpaca confirmed a subscription leaving each of these symbols off the channel named beside it.
    Unconfirmed {
        missing: Vec<(Symbol, Channel)>,
    },
    /// Opening outlasted its window while awaiting this step.
    TimedOut {
        awaiting: OpeningStep,
    },
    /// The socket closed before Alpaca confirmed what was asked of it.
    Closed {
        awaiting: OpeningStep,
    },
    /// The open stream ended.
    Ended,
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
                let names: Vec<String> = missing
                    .iter()
                    .map(|(symbol, channel)| format!("{} {channel}", symbol.as_str()))
                    .collect();
                write!(formatter, "the stream did not confirm {}", names.join(", "))
            }
            Self::TimedOut { awaiting } => {
                write!(
                    formatter,
                    "the stream did not open in time awaiting {awaiting}"
                )
            }
            Self::Closed { awaiting } => write!(formatter, "the stream closed awaiting {awaiting}"),
            Self::Ended => write!(formatter, "the stream closed"),
        }
    }
}

impl std::error::Error for StreamError {}

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
    /// the subscription on every channel; corrections and cancels come with the trades unasked. Refused if opening
    /// outlasts `OPENING_WINDOW`.
    pub async fn open(alpaca: &Alpaca, symbols: &[Symbol]) -> Result<Self, StreamError> {
        Self::open_at(STREAM_URL, &alpaca.credentials, symbols, OPENING_WINDOW).await
    }

    async fn open_at(
        url: &str,
        credentials: &Credentials,
        symbols: &[Symbol],
        window: Duration,
    ) -> Result<Self, StreamError> {
        let deadline = Instant::now() + window;
        let (socket, _) = timeout_at(deadline, tokio_tungstenite::connect_async(url))
            .await
            .map_err(|_| StreamError::TimedOut {
                awaiting: OpeningStep::Socket,
            })?
            .map_err(|error| StreamError::Socket(error.to_string()))?;
        let mut stream = Self {
            socket,
            pending: VecDeque::new(),
        };
        stream.awaiting(deadline, OpeningStep::Connection).await?;
        stream
            .send(
                deadline,
                OpeningStep::Authentication,
                &credentials.authentication(),
            )
            .await?;
        stream
            .awaiting(deadline, OpeningStep::Authentication)
            .await?;
        let names: Vec<&str> = symbols.iter().map(Symbol::as_str).collect();
        let subscribe = serde_json::json!({"action": "subscribe", "trades": names, "quotes": names, "statuses": names});
        stream
            .send(deadline, OpeningStep::Subscription, &subscribe)
            .await?;
        let confirmed = stream.awaiting(deadline, OpeningStep::Subscription).await?;
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

    /// Sends `request` by `deadline`, naming the step it serves if it is late.
    async fn send(
        &mut self,
        deadline: Instant,
        step: OpeningStep,
        request: &Value,
    ) -> Result<(), StreamError> {
        timeout_at(
            deadline,
            self.socket.send(Message::Text(request.to_string().into())),
        )
        .await
        .map_err(|_| StreamError::TimedOut { awaiting: step })?
        .map_err(|error| StreamError::Socket(error.to_string()))
    }

    /// Reads until a message confirms `step`, refusing on Alpaca's error, on a close and at `deadline`.
    async fn awaiting(
        &mut self,
        deadline: Instant,
        step: OpeningStep,
    ) -> Result<StreamMessage, StreamError> {
        loop {
            let next = timeout_at(deadline, self.next())
                .await
                .map_err(|_| StreamError::TimedOut { awaiting: step })?;
            match next {
                None => return Err(StreamError::Closed { awaiting: step }),
                Some(Err(error)) => return Err(error),
                Some(Ok(StreamMessage::Refused { code, message })) => {
                    return Err(StreamError::Refused { code, message });
                }
                Some(Ok(message)) => {
                    if step.confirms(&message) {
                        return Ok(message);
                    }
                }
            }
        }
    }
}

/// Each asked-for symbol a confirmation leaves off a channel, with that channel.
fn unconfirmed(asked: &[Symbol], confirmed: &StreamMessage) -> Vec<(Symbol, Channel)> {
    let listed = match confirmed {
        StreamMessage::Subscribed(listed) => Some(listed),
        StreamMessage::Connected
        | StreamMessage::Authenticated
        | StreamMessage::Trade { .. }
        | StreamMessage::Quote(_)
        | StreamMessage::Refused { .. }
        | StreamMessage::Unrecognized { .. }
        | StreamMessage::Malformed { .. } => None,
    };
    let confirms = |symbol: &Symbol, channel: &Channel| {
        listed
            .and_then(|listed| listed.get(channel))
            .is_some_and(|symbols| symbols.contains(symbol))
    };
    asked
        .iter()
        .flat_map(|symbol| Channel::iter().map(move |channel| (symbol, channel)))
        .filter(|(symbol, channel)| !confirms(symbol, channel))
        .map(|(symbol, channel)| (symbol.clone(), channel))
        .collect()
}

/// One frame, a JSON array of messages each tagged by its `T`; only a frame that is no array is refused whole, so
/// one bad element never costs the others.
fn messages(text: &str) -> Result<Vec<StreamMessage>, StreamError> {
    let elements: Vec<Value> =
        serde_json::from_str(text).map_err(|error| StreamError::Malformed {
            reason: error.to_string(),
        })?;
    Ok(elements
        .into_iter()
        .map(|element| {
            message(&element).unwrap_or_else(|cause| StreamMessage::Malformed {
                cause,
                raw: element.to_string(),
            })
        })
        .collect())
}

/// One element as a message, or why it did not read.
fn message(element: &Value) -> Result<StreamMessage, StreamElementRefusal> {
    let kind = element
        .get("T")
        .and_then(Value::as_str)
        .ok_or(StreamElementRefusal::NoKind)?
        .to_string();
    let unreadable = |error: serde_json::Error| StreamElementRefusal::Unreadable {
        reason: error.to_string(),
    };
    Ok(match kind.as_str() {
        "success" => match element.get("msg").and_then(Value::as_str) {
            Some("connected") => StreamMessage::Connected,
            Some("authenticated") => StreamMessage::Authenticated,
            Some(_) | None => StreamMessage::Unrecognized {
                raw: element.to_string(),
                kind,
            },
        },
        "subscription" => StreamMessage::Subscribed(
            Channel::iter()
                .map(|channel| {
                    let listed = match element.get(<&str>::from(channel)) {
                        Some(listed) => Deserialize::deserialize(listed).map_err(unreadable)?,
                        None => Vec::new(),
                    };
                    Ok((channel, listed))
                })
                .collect::<Result<_, _>>()?,
        ),
        "error" => {
            let payload: ErrorPayload = Deserialize::deserialize(element).map_err(unreadable)?;
            StreamMessage::Refused {
                code: payload.code,
                message: payload.msg,
            }
        }
        "t" => {
            // A correction or cancel names its print by this id, so a trade without one cannot be followed.
            let id = TradeId::read(element)?;
            let outcome = match symbol(element) {
                Ok(symbol) => {
                    let row: AlpacaTrade = Deserialize::deserialize(element).map_err(unreadable)?;
                    trade_outcome(&symbol, &row)
                }
                Err(refused) => AlpacaTradeOutcome::Refused(refused),
            };
            StreamMessage::Trade { id, outcome }
        }
        "q" => StreamMessage::Quote(match symbol(element) {
            Ok(symbol) => {
                let row: AlpacaQuote = Deserialize::deserialize(element).map_err(unreadable)?;
                quote_outcome(&symbol, &row)
            }
            Err(refused) => AlpacaQuoteOutcome::Refused(refused),
        }),
        _ => StreamMessage::Unrecognized {
            raw: element.to_string(),
            kind,
        },
    })
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
    use crate::common::market::trade_bars::{Correction, Print, Tape};
    use crate::common::market::{Price, Shares};
    use crate::ingest::Secret;
    use crate::ingest::alpaca::{quote_page, trade_page};

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
            [StreamMessage::Subscribed(BTreeMap::from([
                (
                    Channel::Trades,
                    vec![Symbol::new("AAPL").unwrap(), Symbol::new("SPY").unwrap()]
                ),
                (Channel::Quotes, vec![Symbol::new("SPY").unwrap()]),
                (
                    Channel::Statuses,
                    vec![Symbol::new("AAPL").unwrap(), Symbol::new("SPY").unwrap()]
                ),
            ]))]
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
                        correction,
                    },
            } => {
                assert_eq!(
                    *id,
                    TradeId {
                        exchange: 'P',
                        number: 52_983_625_699_126
                    }
                );
                assert_eq!(*tape, Tape::ConsolidatedTape);
                assert_eq!(
                    letters
                        .iter()
                        .map(|letter| letter.get())
                        .collect::<Vec<char>>(),
                    [' ', 'F', 'T', 'I']
                );
                assert_eq!(*correction, Correction::Stands);
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
            | StreamMessage::Subscribed(_)
            | StreamMessage::Quote(_)
            | StreamMessage::Refused { .. }
            | StreamMessage::Unrecognized { .. }
            | StreamMessage::Malformed { .. }) => panic!("{other:?}"),
        }
        assert_eq!(read[1], quote(779.8, 480, "2026-10-06T22:35:56.179976636Z"));
        assert_eq!(
            read[2],
            quote(779.81, 280, "2026-10-06T22:35:56.180096716Z")
        );
    }

    proptest::proptest! {
        /// One trade element, read off the stream and off a REST page, becomes the same outcome, refusals included.
        #[test]
        fn property_a_trade_reads_the_same_live_and_archived(
            ticks in -1_000_i64..2_000_000_000,
            size in proptest::sample::select(vec![0.0, 0.5, 1.0, 4.0, 1_000.0, -1.0]),
            tape in proptest::sample::select(vec!["A", "B", "C", "E", "AB"]),
            conditions in proptest::collection::vec(
                proptest::sample::select(vec![" ", "@", "F", "T", "I", "XY", ""]),
                0..4,
            ),
            update in proptest::option::of(
                proptest::sample::select(vec!["incorrect", "corrected", "canceled", "unheard"]),
            ),
        ) {
            let element = serde_json::json!({
                "T": "t", "S": "SPY", "i": 7, "x": "P", "p": ticks as f64 / 1_000_000.0, "s": size,
                "c": conditions, "z": tape, "u": update, "t": "2026-10-06T22:36:01.216274112Z",
            });
            let live = match message(&element) {
                Ok(StreamMessage::Trade { outcome, .. }) => outcome,
                other => panic!("{other:?}"),
            };
            let page = serde_json::json!({"next_page_token": null, "trades": {"SPY": [element]}});
            let (archived, answered) =
                trade_page(&Symbol::new("SPY").unwrap(), page.to_string().as_bytes()).unwrap();
            proptest::prop_assert!(answered);
            proptest::prop_assert_eq!(archived, vec![live]);
        }

        /// One quote element, read off the stream and off a REST page, becomes the same outcome, one-sided included.
        #[test]
        fn property_a_quote_reads_the_same_live_and_archived(
            bid in proptest::sample::select(vec![0.0, 99.99, 100.0, -1.0]),
            ask in proptest::sample::select(vec![0.0, 100.0, 100.01]),
            bid_size in proptest::sample::select(vec![0.0, 1.0, 480.0, 0.5]),
            ask_size in proptest::sample::select(vec![1.0, 1_000.0]),
        ) {
            let element = serde_json::json!({
                "T": "q", "S": "SPY", "bx": "K", "bp": bid, "bs": bid_size, "ax": "P", "ap": ask,
                "as": ask_size, "c": ["R"], "z": "B", "t": "2026-10-06T22:35:56.179976636Z",
            });
            let live = match message(&element) {
                Ok(StreamMessage::Quote(outcome)) => outcome,
                other => panic!("{other:?}"),
            };
            let page = serde_json::json!({"next_page_token": null, "quotes": {"SPY": [element]}});
            let (archived, answered) =
                quote_page(&Symbol::new("SPY").unwrap(), page.to_string().as_bytes()).unwrap();
            proptest::prop_assert!(answered);
            proptest::prop_assert_eq!(archived, vec![live]);
        }
    }

    /// A confirmation leaving a symbol off a channel names the symbol and the channel.
    #[test]
    fn test_a_partial_subscription_names_what_it_left_out() {
        let (aapl, spy) = (Symbol::new("AAPL").unwrap(), Symbol::new("SPY").unwrap());
        let confirmed = StreamMessage::Subscribed(BTreeMap::from([
            (Channel::Trades, vec![aapl.clone(), spy.clone()]),
            (Channel::Quotes, vec![spy.clone()]),
            (Channel::Statuses, vec![aapl.clone(), spy.clone()]),
        ]));
        let only_spy = std::slice::from_ref(&spy);
        assert_eq!(
            unconfirmed(&[aapl.clone(), spy.clone()], &confirmed),
            [(aapl, Channel::Quotes)]
        );
        assert_eq!(unconfirmed(only_spy, &confirmed), []);
        assert_eq!(
            unconfirmed(only_spy, &StreamMessage::Authenticated),
            [
                (spy.clone(), Channel::Trades),
                (spy.clone(), Channel::Quotes),
                (spy, Channel::Statuses)
            ]
        );
    }

    /// Each opening step reads as its refusals named it before it was typed, and only its own message confirms it.
    #[test]
    fn test_each_opening_step_is_named_and_confirmed_by_its_own_message() {
        let steps = [
            OpeningStep::Socket,
            OpeningStep::Connection,
            OpeningStep::Authentication,
            OpeningStep::Subscription,
        ];
        assert_eq!(
            steps.map(|step| step.to_string()),
            [
                "the socket",
                "the connection",
                "authentication",
                "the subscription"
            ]
        );
        assert_eq!(
            StreamError::TimedOut {
                awaiting: OpeningStep::Socket
            }
            .to_string(),
            "the stream did not open in time awaiting the socket"
        );
        assert_eq!(
            StreamError::Closed {
                awaiting: OpeningStep::Subscription
            }
            .to_string(),
            "the stream closed awaiting the subscription"
        );
        let messages = [
            StreamMessage::Connected,
            StreamMessage::Authenticated,
            StreamMessage::Subscribed(BTreeMap::new()),
        ];
        assert_eq!(
            steps.map(|step| messages.clone().map(|message| step.confirms(&message))),
            [
                [false, false, false],
                [true, false, false],
                [false, true, false],
                [false, false, true],
            ]
        );
    }

    /// A kind with no captured payload is kept whole, never guessed at, and the kind here is invented; an element that
    /// does not read is kept whole as malformed while the rest of its frame still arrives.
    #[test]
    fn test_an_unseen_or_unreadable_element_is_kept_whole_beside_the_rest() {
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
        let frame = r#"[{"S":"SPY"},{"T":"t","S":"SPY","x":"P","i":"x","p":1,"s":1,"z":"B","t":"2026-10-06T22:36:01Z"},{"T":"t","S":"SPY","x":"P","p":1,"s":1,"z":"B","t":"2026-10-06T22:36:01Z"},{"T":"success","msg":"connected"}]"#;
        let read = messages(frame).unwrap();
        let causes: Vec<&StreamElementRefusal> = read
            .iter()
            .filter_map(|message| match message {
                StreamMessage::Malformed { cause, .. } => Some(cause),
                StreamMessage::Connected
                | StreamMessage::Authenticated
                | StreamMessage::Subscribed(_)
                | StreamMessage::Trade { .. }
                | StreamMessage::Quote(_)
                | StreamMessage::Refused { .. }
                | StreamMessage::Unrecognized { .. } => None,
            })
            .collect();
        assert_eq!(
            causes,
            [
                &StreamElementRefusal::NoKind,
                &StreamElementRefusal::NumberNotUnsigned {
                    raw: r#""x""#.to_string()
                },
                &StreamElementRefusal::NoNumber,
            ]
        );
        assert_eq!(read[3], StreamMessage::Connected);
        assert!(matches!(
            messages(
                r#"[{"T":"t","S":"spy!","x":"P","i":1,"p":1,"s":1,"z":"B","t":"2026-10-06T22:36:01Z"}]"#
            )
            .unwrap()
            .as_slice(),
            [StreamMessage::Trade {
                outcome: AlpacaTradeOutcome::Refused(_),
                ..
            }]
        ));
    }

    /// A server that accepts the socket and then says nothing times opening out at the first step it awaits.
    #[tokio::test]
    async fn test_a_silent_server_times_opening_out() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let _held = tokio_tungstenite::accept_async(socket).await.unwrap();
            tokio::time::sleep(Duration::from_secs(60)).await;
        });
        let opened = MarketStream::open_at(
            &format!("ws://{address}"),
            &Credentials {
                key_id: Secret::new("key".to_string()),
                secret: Secret::new("secret".to_string()),
            },
            &[Symbol::new("SPY").unwrap()],
            Duration::from_millis(300),
        )
        .await;
        assert!(matches!(
            opened,
            Err(StreamError::TimedOut {
                awaiting: OpeningStep::Connection
            })
        ));
        server.abort();
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
