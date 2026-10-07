//! The tape the trader reads: the SIP stream while it holds, and Alpaca's REST trades over any gap it leaves, each
//! print handed out once however many routes delivered it. Quotes are not backfilled; nothing that decides reads them.

use std::collections::{BTreeMap, BTreeSet, HashSet, VecDeque};
use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};
use serde::Deserialize;
use serde_json::Value;
use tokio::time::{Instant, timeout_at};

use crate::common::market::Symbol;
use crate::ingest::alpaca::stream::{MarketStream, StreamError, StreamMessage, TradeId};
use crate::ingest::alpaca::{
    Alpaca, AlpacaTrade, AlpacaTradeOutcome, PageTokens, TRADES_URL, trade_outcome,
};
use crate::ingest::retry::{FetchError, send, with_retries};
use crate::ingest::{RefusedRow, RowRefusal};

/// How far back a backfill reaches before the last print seen, so prints stamped beside it are not missed.
const BACKFILL_MARGIN: TimeDelta = TimeDelta::seconds(2);

/// How long behind the latest print a seen print is remembered; forgetting waits until a whole backfill is admitted,
/// since REST pages symbol by symbol and an early symbol's prints run ahead of a later one's.
const REMEMBERED: TimeDelta = TimeDelta::minutes(10);

/// The longest wait between attempts to reopen the stream.
const MOST_BACKOFF: Duration = Duration::from_secs(30);

/// A trade with its identity on the tape, as REST returns it.
pub type IdentifiedTrade = (TradeId, AlpacaTradeOutcome);

/// Where the tape's prints come from: a stream to open, and the REST trades since an instant to fill its gaps.
pub trait TapeSource {
    type Stream: TapeStream + Send;

    fn open(
        &self,
        symbols: &[Symbol],
    ) -> impl Future<Output = Result<Self::Stream, StreamError>> + Send;

    /// Every trade of `symbols` stamped at or after `since`, in time order per symbol.
    fn trades_since(
        &self,
        symbols: &[Symbol],
        since: DateTime<Utc>,
    ) -> impl Future<Output = Result<Vec<IdentifiedTrade>, FetchError>> + Send;
}

/// An open stream of messages, `None` once it closes.
pub trait TapeStream {
    fn next(&mut self) -> impl Future<Output = Option<Result<StreamMessage, StreamError>>> + Send;
}

impl TapeStream for MarketStream {
    async fn next(&mut self) -> Option<Result<StreamMessage, StreamError>> {
        MarketStream::next(self).await
    }
}

impl TapeSource for Alpaca {
    type Stream = MarketStream;

    async fn open(&self, symbols: &[Symbol]) -> Result<MarketStream, StreamError> {
        MarketStream::open(self, symbols).await
    }

    async fn trades_since(
        &self,
        symbols: &[Symbol],
        since: DateTime<Utc>,
    ) -> Result<Vec<IdentifiedTrade>, FetchError> {
        let names: Vec<&str> = symbols.iter().map(Symbol::as_str).collect();
        let (names, start) = (names.join(","), since.to_rfc3339());
        let mut trades = Vec::new();
        let mut tokens = PageTokens::default();
        let mut token: Option<String> = None;
        loop {
            let body = with_retries(|| {
                let mut query = vec![
                    ("symbols", names.as_str()),
                    ("start", start.as_str()),
                    ("feed", "sip"),
                    ("sort", "asc"),
                    ("limit", "10000"),
                ];
                if let Some(token) = token.as_deref() {
                    query.push(("page_token", token));
                }
                send(
                    self.http_client
                        .get(TRADES_URL)
                        .header("APCA-API-KEY-ID", &self.key_id)
                        .header("APCA-API-SECRET-KEY", &self.secret)
                        .query(&query),
                )
            })
            .await?;
            let next = tokens.next(&body)?;
            trades.extend(recent_trades(&body, symbols)?);
            match next {
                Some(next) => token = Some(next),
                None => return Ok(trades),
            }
        }
    }
}

#[derive(Deserialize)]
struct RecentTradesPage {
    /// `null` on a page with no trades.
    trades: Option<BTreeMap<String, Vec<Value>>>,
}

/// One page of trades for `requested`, a row filed under any other ticker refused; a row with no readable identity
/// makes the page malformed, since a backfilled print that cannot be matched to the stream's could be handed out twice.
fn recent_trades(body: &[u8], requested: &[Symbol]) -> Result<Vec<IdentifiedTrade>, FetchError> {
    let malformed = |reason: String| FetchError::Malformed { reason };
    let page: RecentTradesPage =
        serde_json::from_slice(body).map_err(|error| malformed(error.to_string()))?;
    let mut trades = Vec::new();
    for (ticker, rows) in page.trades.unwrap_or_default() {
        for row in rows {
            let id = TradeId::read(&row).map_err(malformed)?;
            let outcome = match Symbol::new(&ticker) {
                Ok(symbol) if !requested.contains(&symbol) => {
                    AlpacaTradeOutcome::Refused(RefusedRow {
                        ticker: ticker.clone(),
                        cause: RowRefusal::Unrequested,
                    })
                }
                Ok(symbol) => {
                    let row: AlpacaTrade = serde_json::from_value(row)
                        .map_err(|error| malformed(error.to_string()))?;
                    trade_outcome(&symbol, &row)
                }
                Err(cause) => AlpacaTradeOutcome::Refused(RefusedRow {
                    ticker: ticker.clone(),
                    cause: RowRefusal::Symbol(cause),
                }),
            };
            trades.push((id, outcome));
        }
    }
    Ok(trades)
}

/// A print across routes: its ticker and its identity on the tape, which numbers repeat across symbols on one exchange.
type Identity = (String, TradeId);

/// The prints already handed out, matched by identity alone and indexed by instant so the window behind the latest can
/// be forgotten.
#[derive(Debug, Default)]
struct Seen {
    identities: HashSet<Identity>,
    by_instant: BTreeSet<(DateTime<Utc>, Identity)>,
}

impl Seen {
    /// Whether the print is new, remembering it at `at` if so.
    fn admit(&mut self, at: DateTime<Utc>, identity: Identity) -> bool {
        let new = self.identities.insert(identity.clone());
        if new {
            self.by_instant.insert((at, identity));
        }
        new
    }

    /// Forgets every print remembered before `before`.
    fn forget_before(&mut self, before: DateTime<Utc>) {
        let kept = self
            .by_instant
            .split_off(&(before, (String::new(), TradeId::LEAST)));
        for (_, identity) in std::mem::replace(&mut self.by_instant, kept) {
            self.identities.remove(&identity);
        }
    }
}

/// What the feed hands out: each stream message, prints once, and the feed's own account of its gaps.
#[derive(Debug, Clone, PartialEq)]
pub enum FeedEvent {
    Message(StreamMessage),
    /// The stream ended or failed; prints come from REST until it reopens.
    Lost {
        cause: String,
    },
    /// The stream reopened, `attempts` tries since it last delivered a message.
    Reopened {
        attempts: u32,
    },
    /// REST trades since `since` were read and the `fresh` ones not already handed out precede this event.
    Backfilled {
        since: DateTime<Utc>,
        fresh: usize,
    },
    /// A backfill failed; the gap stays open and is retried with backoff, even while the stream is up.
    BackfillFailed {
        cause: String,
    },
}

/// The tape for `symbols`, reopened and backfilled whenever the stream drops.
pub struct Feed<Source: TapeSource> {
    source: Source,
    symbols: Vec<Symbol>,
    stream: Option<Source::Stream>,
    seen: Seen,
    /// Where the tape is wanted from, which anchors a loss before any print is handed out.
    from: DateTime<Utc>,
    /// The latest print handed out by either route.
    latest: Option<DateTime<Utc>>,
    /// The latest print the stream delivered, which bounds what it may yet repeat, so forgetting keys off it.
    latest_streamed: Option<DateTime<Utc>>,
    /// Where the stretch the stream has not covered begins, until a backfill closes it.
    gap: Option<DateTime<Utc>>,
    /// Opens since the stream last delivered a message, so one that opens and closes at once still backs off.
    attempts: u32,
    /// Backfills failed since the last that succeeded, and when the next may run.
    failed_backfills: u32,
    retry_at: Option<Instant>,
    pending: VecDeque<FeedEvent>,
}

impl<Source: TapeSource> Feed<Source> {
    /// A feed wanting the tape from `from` on; its first `next` opens the stream and backfills from `from`.
    pub fn new(source: Source, symbols: Vec<Symbol>, from: DateTime<Utc>) -> Self {
        Self {
            source,
            symbols,
            stream: None,
            seen: Seen::default(),
            from,
            latest: None,
            latest_streamed: None,
            gap: Some(from),
            attempts: 0,
            failed_backfills: 0,
            retry_at: None,
            pending: VecDeque::new(),
        }
    }

    /// The next event; the feed never ends. While the stream is down it keeps trying to reopen it, backfilling from
    /// REST between attempts, and while a gap stays open it retries the backfill between the stream's messages.
    pub async fn next(&mut self) -> FeedEvent {
        loop {
            if let Some(event) = self.pending.pop_front() {
                return event;
            }
            let Some(stream) = self.stream.as_mut() else {
                self.reopen().await;
                continue;
            };
            let read = match (self.gap, self.retry_at) {
                (Some(_), Some(retry_at)) => match timeout_at(retry_at, stream.next()).await {
                    Ok(read) => read,
                    Err(_) => {
                        self.backfill().await;
                        continue;
                    }
                },
                (None, Some(_) | None) | (Some(_), None) => stream.next().await,
            };
            match read {
                Some(Ok(message)) => {
                    self.attempts = 0;
                    let admitted = self.admitted(message, true);
                    self.forget();
                    if let Some(event) = admitted {
                        return event;
                    }
                }
                Some(Err(error)) => return self.lost(error.to_string()),
                None => return self.lost("the stream closed".to_string()),
            }
        }
    }

    /// The message to hand out, or `None` for a print or refusal already handed out.
    fn admitted(&mut self, message: StreamMessage, streamed: bool) -> Option<FeedEvent> {
        match &message {
            StreamMessage::Trade {
                id,
                outcome: AlpacaTradeOutcome::Print { print, .. },
            } => {
                let at = print.timestamp();
                if !self
                    .seen
                    .admit(at, (print.symbol().as_str().to_string(), *id))
                {
                    return None;
                }
                self.latest = Some(self.latest.map_or(at, |latest| latest.max(at)));
                if streamed {
                    self.latest_streamed =
                        Some(self.latest_streamed.map_or(at, |latest| latest.max(at)));
                }
            }
            StreamMessage::Trade {
                id,
                outcome: AlpacaTradeOutcome::Refused(row),
            } => {
                // A refusal carries no instant, so it is remembered from the latest print.
                let at = self.latest.unwrap_or(DateTime::<Utc>::MIN_UTC);
                if !self.seen.admit(at, (row.ticker().to_string(), *id)) {
                    return None;
                }
            }
            StreamMessage::Connected
            | StreamMessage::Authenticated
            | StreamMessage::Subscribed { .. }
            | StreamMessage::Quote(_)
            | StreamMessage::Refused { .. }
            | StreamMessage::Unrecognized { .. }
            | StreamMessage::Malformed { .. } => {}
        }
        Some(FeedEvent::Message(message))
    }

    /// Forgets prints well behind the latest the stream delivered; REST running ahead never moves this, so a print
    /// the reopened stream has yet to repeat is still remembered.
    fn forget(&mut self) {
        if let Some(latest) = self.latest_streamed {
            self.seen.forget_before(latest - REMEMBERED);
        }
    }

    fn lost(&mut self, cause: String) -> FeedEvent {
        self.stream = None;
        let start = self
            .latest
            .map_or(self.from, |latest| latest - BACKFILL_MARGIN);
        self.gap = Some(self.gap.map_or(start, |gap| gap.min(start)));
        FeedEvent::Lost { cause }
    }

    /// One attempt to reopen, then a backfill over any open gap whether or not it held: after a reopen it closes the
    /// gap, and while the stream stays down it carries the tape.
    async fn reopen(&mut self) {
        if self.attempts > 0 {
            tokio::time::sleep(backoff(self.attempts)).await;
        }
        self.attempts += 1;
        match self.source.open(&self.symbols).await {
            Ok(stream) => {
                self.stream = Some(stream);
                if self.attempts > 1 || self.latest.is_some() {
                    self.pending.push_back(FeedEvent::Reopened {
                        attempts: self.attempts,
                    });
                }
            }
            Err(error) => self.pending.push_back(FeedEvent::Lost {
                cause: error.to_string(),
            }),
        }
        self.backfill().await;
    }

    /// Reads REST trades from the gap's start and queues the ones not yet handed out. The gap closes once a backfill
    /// succeeds with the stream up; with it down, the gap moves up to the latest print and stays open.
    async fn backfill(&mut self) {
        let Some(since) = self.gap else {
            return;
        };
        match self.source.trades_since(&self.symbols, since).await {
            Ok(trades) => {
                let mut fresh = Vec::new();
                for (id, outcome) in trades {
                    if let Some(event) = self.admitted(StreamMessage::Trade { id, outcome }, false)
                    {
                        fresh.push(event);
                    }
                }
                self.forget();
                self.gap = match self.stream {
                    Some(_) => None,
                    None => Some(
                        self.latest
                            .map_or(since, |latest| (latest - BACKFILL_MARGIN).max(since)),
                    ),
                };
                (self.failed_backfills, self.retry_at) = (0, None);
                let count = fresh.len();
                self.pending.extend(fresh);
                self.pending.push_back(FeedEvent::Backfilled {
                    since,
                    fresh: count,
                });
            }
            Err(error) => {
                self.failed_backfills += 1;
                self.retry_at = Some(Instant::now() + backoff(self.failed_backfills));
                self.pending.push_back(FeedEvent::BackfillFailed {
                    cause: error.to_string(),
                });
            }
        }
    }
}

/// The wait before reopen attempt `attempts + 1`: one second, doubling, held to `MOST_BACKOFF`.
fn backoff(attempts: u32) -> Duration {
    Duration::from_secs(1u64 << attempts.saturating_sub(1).min(5)).min(MOST_BACKOFF)
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use crate::common::market::record::Trade;
    use crate::common::market::trade_bars::{Correction, Print, Tape};
    use crate::common::market::{Price, Shares};

    /// A page of AAPL trades as the REST history returned it for 2026-10-06 19:59:59, trimmed to three rows.
    const PAGE: &str = r#"{"next_page_token":"QUFQTHwxNzkxMzE2Nzk5MDAyNDE5OTE2fFF8MTE1MDk2","trades":{"AAPL":[{"c":["@","F"],"i":115093,"p":333.675,"s":40,"t":"2026-10-06T19:59:59.002268137Z","x":"Q","z":"C"},{"c":["@","F"],"i":115094,"p":333.69,"s":100,"t":"2026-10-06T19:59:59.002269693Z","x":"Q","z":"C"},{"c":["@","F"],"i":115095,"p":333.69,"s":60,"t":"2026-10-06T19:59:59.002418773Z","x":"Q","z":"C"}]}}"#;

    fn id(exchange: &str, number: u64) -> TradeId {
        TradeId::read(&serde_json::json!({"x": exchange, "i": number})).unwrap()
    }

    fn at(second: i64) -> DateTime<Utc> {
        "2026-10-07T14:00:00Z".parse::<DateTime<Utc>>().unwrap() + TimeDelta::seconds(second)
    }

    /// A one-share SPY print `second` seconds past 14:00, as the stream or REST would carry it.
    fn print(number: u64, second: i64) -> StreamMessage {
        print_at("SPY", number, at(second))
    }

    fn print_at(ticker: &str, number: u64, timestamp: DateTime<Utc>) -> StreamMessage {
        let trade = Trade::new(
            Symbol::new(ticker).unwrap(),
            timestamp,
            Price::from_ticks(700_000_000).unwrap(),
            Shares::whole(1).unwrap(),
        )
        .unwrap();
        StreamMessage::Trade {
            id: id("P", number),
            outcome: AlpacaTradeOutcome::Print {
                print: Print::Trade(trade),
                tape: Tape::ConsolidatedTape,
                letters: vec![' '],
                correction: Correction::Stands,
            },
        }
    }

    fn outcome(message: StreamMessage) -> (TradeId, AlpacaTradeOutcome) {
        match message {
            StreamMessage::Trade { id, outcome } => (id, outcome),
            other @ (StreamMessage::Connected
            | StreamMessage::Authenticated
            | StreamMessage::Subscribed { .. }
            | StreamMessage::Quote(_)
            | StreamMessage::Refused { .. }
            | StreamMessage::Unrecognized { .. }
            | StreamMessage::Malformed { .. }) => panic!("{other:?}"),
        }
    }

    /// The page's rows read with their identities, and a repeated page token is refused rather than followed forever.
    #[test]
    fn test_a_rest_page_reads_with_each_trades_identity() {
        let aapl = [Symbol::new("AAPL").unwrap()];
        let trades = recent_trades(PAGE.as_bytes(), &aapl).unwrap();
        let ids: Vec<TradeId> = trades.iter().map(|(id, _)| *id).collect();
        assert_eq!(ids, [id("Q", 115_093), id("Q", 115_094), id("Q", 115_095)]);
        assert!(
            trades
                .iter()
                .all(|(_, outcome)| matches!(outcome, AlpacaTradeOutcome::Print { .. }))
        );
        let unidentified = PAGE.replace(r#""i":115094,"#, "");
        assert!(matches!(
            recent_trades(unidentified.as_bytes(), &aapl),
            Err(FetchError::Malformed { .. })
        ));
        assert_eq!(
            recent_trades(br#"{"trades":null,"next_page_token":null}"#, &aapl).unwrap(),
            vec![]
        );
        let mut tokens = PageTokens::default();
        assert_eq!(
            tokens.next(PAGE.as_bytes()).unwrap().as_deref(),
            Some("QUFQTHwxNzkxMzE2Nzk5MDAyNDE5OTE2fFF8MTE1MDk2")
        );
        assert!(matches!(
            tokens.next(PAGE.as_bytes()),
            Err(FetchError::Malformed { .. })
        ));
    }

    /// The same number on two exchanges or two symbols is two prints, the same identity at another instant is one,
    /// and a forgotten print is admitted again.
    #[test]
    fn test_seen_prints_are_admitted_once_until_forgotten() {
        let spy = |exchange, number| ("SPY".to_string(), id(exchange, number));
        let mut seen = Seen::default();
        assert!(seen.admit(at(0), spy("P", 7)));
        assert!(!seen.admit(at(0), spy("P", 7)));
        assert!(!seen.admit(at(1), spy("P", 7)));
        assert!(seen.admit(at(0), spy("Q", 7)));
        assert!(seen.admit(at(0), ("AAPL".to_string(), id("P", 7))));
        assert!(seen.admit(at(5), spy("P", 8)));
        seen.forget_before(at(5));
        assert_eq!(seen.identities.len(), 1);
        assert!(seen.admit(at(0), spy("P", 7)));
        assert!(!seen.admit(at(5), spy("P", 8)));
    }

    #[test]
    fn test_the_backoff_doubles_from_a_second_to_thirty() {
        let waits: Vec<u64> = (1..=8)
            .map(|attempts| backoff(attempts).as_secs())
            .collect();
        assert_eq!(waits, [1, 2, 4, 8, 16, 30, 30, 30]);
    }

    /// What one scripted open does: fail, or hand out a stream that closes or stays open once its messages run out.
    enum ScriptedOpen {
        Fails(&'static str),
        Closes(Vec<StreamMessage>),
        StaysOpen(Vec<StreamMessage>),
    }

    /// Opens hand out scripted streams in turn, and each backfill the next scripted page.
    struct Scripted {
        opens: Mutex<VecDeque<ScriptedOpen>>,
        backfills: Mutex<VecDeque<Result<Vec<StreamMessage>, &'static str>>>,
        asked_since: Mutex<Vec<DateTime<Utc>>>,
    }

    struct ScriptedStream {
        messages: VecDeque<StreamMessage>,
        closes: bool,
    }

    impl TapeStream for ScriptedStream {
        async fn next(&mut self) -> Option<Result<StreamMessage, StreamError>> {
            match (self.messages.pop_front(), self.closes) {
                (Some(message), true | false) => Some(Ok(message)),
                (None, true) => None,
                (None, false) => std::future::pending().await,
            }
        }
    }

    impl TapeSource for Scripted {
        type Stream = ScriptedStream;

        async fn open(&self, _: &[Symbol]) -> Result<ScriptedStream, StreamError> {
            let open = self
                .opens
                .lock()
                .unwrap()
                .pop_front()
                .expect("the script has an open");
            match open {
                ScriptedOpen::Fails(cause) => Err(StreamError::Socket(cause.to_string())),
                ScriptedOpen::Closes(messages) => Ok(ScriptedStream {
                    messages: messages.into(),
                    closes: true,
                }),
                ScriptedOpen::StaysOpen(messages) => Ok(ScriptedStream {
                    messages: messages.into(),
                    closes: false,
                }),
            }
        }

        async fn trades_since(
            &self,
            _: &[Symbol],
            since: DateTime<Utc>,
        ) -> Result<Vec<IdentifiedTrade>, FetchError> {
            self.asked_since.lock().unwrap().push(since);
            match self
                .backfills
                .lock()
                .unwrap()
                .pop_front()
                .expect("the script has a backfill")
            {
                Ok(messages) => Ok(messages.into_iter().map(outcome).collect()),
                Err(cause) => Err(FetchError::Malformed {
                    reason: cause.to_string(),
                }),
            }
        }
    }

    /// A feed wanting the tape from a minute before 14:00.
    fn feed(
        opens: Vec<ScriptedOpen>,
        backfills: Vec<Result<Vec<StreamMessage>, &'static str>>,
    ) -> Feed<Scripted> {
        Feed::new(
            Scripted {
                opens: Mutex::new(opens.into()),
                backfills: Mutex::new(backfills.into()),
                asked_since: Mutex::new(Vec::new()),
            },
            vec![Symbol::new("SPY").unwrap()],
            at(-60),
        )
    }

    async fn take(feed: &mut Feed<Scripted>, count: usize) -> Vec<FeedEvent> {
        let mut events = Vec::new();
        for _ in 0..count {
            events.push(feed.next().await);
        }
        events
    }

    fn lost(cause: &str) -> FeedEvent {
        FeedEvent::Lost {
            cause: cause.to_string(),
        }
    }

    fn backfilled(since: DateTime<Utc>, fresh: usize) -> FeedEvent {
        FeedEvent::Backfilled { since, fresh }
    }

    /// The first open backfills from the instant the tape is wanted; the stream then drops after two prints, reopens,
    /// and the backfill from two seconds before the last print hands out only the print the stream never delivered.
    #[tokio::test(start_paused = true)]
    async fn test_a_dropped_stream_reopens_and_backfills_only_what_it_missed() {
        let mut feed = feed(
            vec![
                ScriptedOpen::Closes(vec![print(1, 0), print(2, 10)]),
                ScriptedOpen::StaysOpen(vec![print(4, 30)]),
            ],
            vec![Ok(vec![]), Ok(vec![print(2, 10), print(3, 20)])],
        );
        assert_eq!(
            take(&mut feed, 8).await,
            [
                backfilled(at(-60), 0),
                FeedEvent::Message(print(1, 0)),
                FeedEvent::Message(print(2, 10)),
                lost("the stream closed"),
                FeedEvent::Reopened { attempts: 1 },
                FeedEvent::Message(print(3, 20)),
                backfilled(at(8), 1),
                FeedEvent::Message(print(4, 30)),
            ]
        );
        assert_eq!(*feed.source.asked_since.lock().unwrap(), [at(-60), at(8)]);
    }

    /// Before the stream ever delivers, REST carries the tape from the instant it is wanted, and the gap moves up to
    /// the latest print while the stream stays down.
    #[tokio::test(start_paused = true)]
    async fn test_rest_carries_the_tape_from_the_start_while_the_stream_is_down() {
        let mut feed = feed(
            vec![
                ScriptedOpen::Fails("refused"),
                ScriptedOpen::StaysOpen(vec![print(1, 0)]),
            ],
            vec![Ok(vec![print(1, 0)]), Ok(vec![])],
        );
        let started = tokio::time::Instant::now();
        assert_eq!(
            take(&mut feed, 5).await,
            [
                lost("the stream's socket failed: refused"),
                FeedEvent::Message(print(1, 0)),
                backfilled(at(-60), 1),
                FeedEvent::Reopened { attempts: 2 },
                backfilled(at(-2), 0),
            ]
        );
        assert_eq!(started.elapsed(), Duration::from_secs(1));
        let repeated = tokio::time::timeout(Duration::from_secs(60), feed.next()).await;
        assert!(
            repeated.is_err(),
            "the reopened stream's copy of print 1 was handed out again"
        );
    }

    /// A backfill that fails after a reopen keeps the gap: it is retried with backoff while the stream is up, from the
    /// gap's start, not from prints the stream has delivered since.
    #[tokio::test(start_paused = true)]
    async fn test_a_failed_backfill_is_retried_from_the_gap_while_the_stream_is_up() {
        let mut feed = feed(
            vec![
                ScriptedOpen::Closes(vec![print(1, 0)]),
                ScriptedOpen::StaysOpen(vec![print(5, 300)]),
            ],
            vec![Ok(vec![]), Err("timed out"), Ok(vec![print(2, 100)])],
        );
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3600), take(&mut feed, 8))
                .await
                .expect("the failed gap is retried within the hour"),
            [
                backfilled(at(-60), 0),
                FeedEvent::Message(print(1, 0)),
                lost("the stream closed"),
                FeedEvent::Reopened { attempts: 1 },
                FeedEvent::BackfillFailed {
                    cause: "malformed payload: timed out".to_string()
                },
                FeedEvent::Message(print(5, 300)),
                FeedEvent::Message(print(2, 100)),
                backfilled(at(-2), 1),
            ]
        );
    }

    /// REST pages symbol by symbol and can run far ahead of the reopened stream, which may yet repeat what REST read;
    /// forgetting keys off the stream, so neither the SPY copy nor a refusal read twice is handed out again.
    #[tokio::test(start_paused = true)]
    async fn test_a_backfill_running_ahead_forgets_nothing_the_stream_may_repeat() {
        let refused = || StreamMessage::Trade {
            id: id("P", 9),
            outcome: AlpacaTradeOutcome::Refused(RefusedRow {
                ticker: "SPY".to_string(),
                cause: RowRefusal::Tape {
                    raw: "E".to_string(),
                },
            }),
        };
        let mut feed = feed(
            vec![
                ScriptedOpen::Closes(vec![print(1, 0), refused()]),
                ScriptedOpen::StaysOpen(vec![print(2, 5)]),
            ],
            vec![
                Ok(vec![]),
                Ok(vec![
                    print_at("AAPL", 1, at(1200)),
                    print(1, 0),
                    refused(),
                    print(2, 5),
                ]),
            ],
        );
        assert_eq!(
            take(&mut feed, 8).await,
            [
                backfilled(at(-60), 0),
                FeedEvent::Message(print(1, 0)),
                FeedEvent::Message(refused()),
                lost("the stream closed"),
                FeedEvent::Reopened { attempts: 1 },
                FeedEvent::Message(print_at("AAPL", 1, at(1200))),
                FeedEvent::Message(print(2, 5)),
                backfilled(at(-2), 2),
            ]
        );
        let repeated = tokio::time::timeout(Duration::from_secs(60), feed.next()).await;
        assert!(
            repeated.is_err(),
            "the stream's copy of print 2 was handed out again"
        );
    }

    /// A stream lost before any print anchors no gap of its own, so the reopen backfills again from the instant the
    /// tape is wanted rather than skipping the stretch it was down.
    #[tokio::test(start_paused = true)]
    async fn test_a_loss_before_any_print_backfills_from_the_start() {
        let mut feed = feed(
            vec![
                ScriptedOpen::Closes(vec![]),
                ScriptedOpen::StaysOpen(vec![]),
            ],
            vec![Ok(vec![]), Ok(vec![print(1, 0)])],
        );
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(60), take(&mut feed, 5))
                .await
                .expect("the reopen backfills"),
            [
                backfilled(at(-60), 0),
                lost("the stream closed"),
                FeedEvent::Reopened { attempts: 2 },
                FeedEvent::Message(print(1, 0)),
                backfilled(at(-60), 1),
            ]
        );
    }

    /// A stream that opens and closes before delivering anything still backs off between opens.
    #[tokio::test(start_paused = true)]
    async fn test_a_stream_that_closes_on_opening_backs_off() {
        let mut feed = feed(
            vec![
                ScriptedOpen::Closes(vec![print(1, 0)]),
                ScriptedOpen::Closes(vec![]),
                ScriptedOpen::Closes(vec![]),
                ScriptedOpen::StaysOpen(vec![print(2, 10)]),
            ],
            vec![Ok(vec![]), Ok(vec![]), Ok(vec![]), Ok(vec![])],
        );
        let started = tokio::time::Instant::now();
        let events = take(&mut feed, 12).await;
        let reopened: Vec<&FeedEvent> = events
            .iter()
            .filter(|event| matches!(event, FeedEvent::Reopened { .. }))
            .collect();
        assert_eq!(
            reopened,
            [
                &FeedEvent::Reopened { attempts: 1 },
                &FeedEvent::Reopened { attempts: 2 },
                &FeedEvent::Reopened { attempts: 3 }
            ]
        );
        assert_eq!(events[11], FeedEvent::Message(print(2, 10)));
        assert_eq!(started.elapsed(), Duration::from_secs(3));
    }

    /// A row filed under a ticker the request did not name is refused, never handed out as a print.
    #[test]
    fn test_a_row_for_an_unrequested_ticker_is_refused() {
        let trades = recent_trades(PAGE.as_bytes(), &[Symbol::new("SPY").unwrap()]).unwrap();
        assert_eq!(trades.len(), 3);
        assert!(trades.iter().all(|(_, outcome)| matches!(
            outcome,
            AlpacaTradeOutcome::Refused(row) if *row.cause() == RowRefusal::Unrequested
        )));
    }

    /// Reads the last ten minutes of SPY and AAPL trades from the REST history; every print has its own identity.
    #[tokio::test]
    #[ignore = "reads Alpaca's REST trade history; run deliberately under secretspec"]
    async fn live_recent_trades_read_with_distinct_identities() {
        let alpaca = Alpaca::from_environment(reqwest::Client::new()).unwrap();
        let symbols = [Symbol::new("SPY").unwrap(), Symbol::new("AAPL").unwrap()];
        let since = Utc::now() - TimeDelta::minutes(10);
        let trades = alpaca.trades_since(&symbols, since).await.unwrap();
        let identities: BTreeSet<(Symbol, TradeId)> = trades
            .iter()
            .map(|(id, outcome)| match outcome {
                AlpacaTradeOutcome::Print { print, .. } => (print.symbol().clone(), *id),
                AlpacaTradeOutcome::Refused(row) => panic!("{row:?}"),
            })
            .collect();
        println!("{} trades since {since}", trades.len());
        assert_eq!(identities.len(), trades.len());
    }
}
