//! Loads archive series for studies, each with the fingerprint of exactly what was read.

use std::collections::BTreeMap;

use crate::common::journal::RunId;
use crate::common::laboratory::dataset::{
    DatasetLeg, Fingerprint, FingerprintRefusal, PartitionRead, Symbols,
};
use crate::common::market::record::{BarInterval, Partition};
use crate::common::market::trade_bars::TradeBar;
use crate::common::storage::{Key, Provider, TradesKey};
use crate::common::time::calendar::TradingCalendar;
use crate::common::time::{SessionDate, SessionRange};
use crate::laboratory::Study;
use crate::storage::provenance::Provenance;
use crate::storage::{Bucket, BucketError, parquet, trade_bars};

/// Trade bars by session and the lineage of the partitions they came from.
#[derive(Debug)]
pub struct Dataset {
    bars: BTreeMap<SessionDate, Partition<TradeBar>>,
    lineage: Lineage,
}

/// What a dataset read: the fingerprint of its partitions and the run whose journal holds the read.
#[derive(Debug)]
pub struct Lineage {
    fingerprint: Fingerprint,
    run: RunId,
}

impl Lineage {
    pub fn fingerprint(&self) -> &Fingerprint {
        &self.fingerprint
    }

    pub fn run(&self) -> RunId {
        self.run
    }
}

impl Dataset {
    pub fn bars(&self) -> &BTreeMap<SessionDate, Partition<TradeBar>> {
        &self.bars
    }

    pub fn lineage(&self) -> &Lineage {
        &self.lineage
    }

    pub fn fingerprint(&self) -> &Fingerprint {
        self.lineage.fingerprint()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DatasetError {
    #[error("{0}")]
    Window(FingerprintRefusal),
    /// The read could not be journaled, so it is not returned: a study holds only cataloged data.
    #[error("the read could not be journaled: {0}")]
    Journal(std::io::Error),
    #[error("{0}")]
    Bucket(BucketError),
    #[error("the partition for {session} did not decode: {refusal:?}")]
    Decode {
        session: SessionDate,
        refusal: parquet::DecodeRefusal,
    },
}

/// Trade bars of `symbols` at `interval` for every trading session in `range`, each session from Massive where it
/// holds one and Alpaca otherwise, journaled to `study` before it is returned with the provider of each session and the
/// symbols kept in its fingerprint.
pub async fn trade_bars(
    archive: &Bucket,
    calendar: &TradingCalendar,
    range: SessionRange,
    interval: BarInterval,
    symbols: Symbols,
    study: &mut Study,
) -> Result<Dataset, DatasetError> {
    load(
        DatasetLeg::trade_bars(interval),
        calendar,
        range,
        symbols.clone(),
        study,
        async |session| {
            let found = preferred(trade_keys(interval, session), async |provider, key| {
                read(archive, provider, key, session, trade_bars::decode).await
            })
            .await?;
            // Kept as each session decodes, so a subset never holds more than one whole session at once.
            Ok(found.map(|(partition, read)| (kept(partition, &symbols), read)))
        },
    )
    .await
}

/// The rows of `partition` whose symbol `symbols` keeps, `None` when it keeps none of them.
fn kept(partition: Partition<TradeBar>, symbols: &Symbols) -> Option<Partition<TradeBar>> {
    match symbols {
        Symbols::Every => Some(partition),
        Symbols::Only(_) => Partition::try_from(
            partition
                .rows()
                .iter()
                .filter(|bar| symbols.keeps(bar.symbol()))
                .cloned()
                .collect::<Vec<_>>(),
        )
        .ok(),
    }
}

/// What `held` finds for the first of `candidates` it finds anything for, consulting none after that one; `None`
/// when it finds nothing for any.
async fn preferred<K, Found, Error>(
    candidates: impl IntoIterator<Item = (Provider, K)>,
    held: impl AsyncFn(Provider, K) -> Result<Option<Found>, Error>,
) -> Result<Option<Found>, Error> {
    for (provider, key) in candidates {
        if let Some(found) = held(provider, key).await? {
            return Ok(Some(found));
        }
    }
    Ok(None)
}

/// Each provider's trade-bar key at `interval` for `session`, in the order a session is preferred from.
fn trade_keys(interval: BarInterval, session: SessionDate) -> [(Provider, TradesKey); 2] {
    [Provider::Massive, Provider::Alpaca]
        .map(|provider| (provider, TradesKey::new(provider, interval, session)))
}

/// `leg`'s bars for every trading session in `range`, each read by `partition`, journaled to `study` before it is
/// returned.
async fn load(
    leg: DatasetLeg,
    calendar: &TradingCalendar,
    range: SessionRange,
    symbols: Symbols,
    study: &mut Study,
    partition: impl AsyncFn(
        SessionDate,
    ) -> Result<
        Option<(Option<Partition<TradeBar>>, PartitionRead)>,
        DatasetError,
    >,
) -> Result<Dataset, DatasetError> {
    // Taken empty first, so the window is checked before any read and its missing sessions are the ones to read.
    let owed =
        Fingerprint::new(leg, range, calendar, BTreeMap::new()).map_err(DatasetError::Window)?;
    let (mut bars, mut reads) = (BTreeMap::new(), BTreeMap::new());
    for session in owed.missing() {
        let Some((rows, read)) = partition(*session).await? else {
            continue;
        };
        if let Some(rows) = rows {
            bars.insert(*session, rows);
        }
        reads.insert(*session, read);
    }
    let fingerprint = Fingerprint::new(leg, range, calendar, reads)
        .map_err(DatasetError::Window)?
        .keeping(symbols);
    study.read(&fingerprint).map_err(DatasetError::Journal)?;
    Ok(Dataset {
        bars,
        lineage: Lineage {
            fingerprint,
            run: study.run_id(),
        },
    })
}

/// `provider`'s partition under `key` with the version read, or `None` when it has none.
async fn read<K: Copy + Into<Key>, B>(
    archive: &Bucket,
    provider: Provider,
    key: K,
    session: SessionDate,
    decode: impl FnOnce(&K, Vec<u8>) -> Result<(Partition<B>, Provenance), parquet::DecodeRefusal>,
) -> Result<Option<(Partition<B>, PartitionRead)>, DatasetError> {
    let Some((body, tag)) = archive
        .get_tagged(&key.into())
        .await
        .map_err(DatasetError::Bucket)?
    else {
        return Ok(None);
    };
    let (rows, _) =
        decode(&key, body).map_err(|refusal| DatasetError::Decode { session, refusal })?;
    Ok(Some((rows, PartitionRead::new(provider, tag))))
}

#[cfg(test)]
pub(crate) mod tests {
    use chrono::{DateTime, NaiveDate, Utc};

    use super::*;
    use crate::common::journal::{Observation, ReadLine, read};
    use crate::common::laboratory::estimate::{Estimate, summarize};
    use crate::common::laboratory::experiment::{Label, Outputs, Parameters};
    use crate::common::laboratory::series::Series;
    use crate::common::market::aggregate::TradeTotals;
    use crate::common::market::record::BarInterval;
    use crate::common::market::trade_bars::{HighLow, OpenClose, TradeBar, TradeSums};
    use crate::common::market::{DollarVolume, Price, Shares, StampedPrice, Symbol, TradeCount};
    use crate::common::storage::{EntityTag, Host, JournalKey};
    use crate::common::time::calendar::TradingSession;
    use crate::ingest::alpaca::Alpaca;
    use crate::laboratory::source::Source;

    fn session(day: i64) -> SessionDate {
        SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 3, 2).unwrap()).plus_calendar_days(day)
    }

    /// Sessions 0 to 3, each trading 09:30 to 16:00 Eastern.
    pub(crate) fn calendar() -> TradingCalendar {
        let (open, close) = (
            chrono::NaiveTime::from_hms_opt(9, 30, 0).unwrap(),
            chrono::NaiveTime::from_hms_opt(16, 0, 0).unwrap(),
        );
        TradingCalendar::new(
            (0..4)
                .map(|day| TradingSession::new(session(day), open, close).unwrap())
                .collect(),
            SessionRange::new(session(0), session(3)).unwrap(),
        )
        .unwrap()
    }

    /// AAPL's trade bar at `timestamp`, opening at its start and closing a second before its end, over 100 shares.
    fn trade_bar(interval: BarInterval, timestamp: DateTime<Utc>, prices: [f64; 4]) -> TradeBar {
        symbol_bar("AAPL", interval, timestamp, prices)
    }

    /// `symbol`'s trade bar at `timestamp`, as `trade_bar` builds AAPL's.
    fn symbol_bar(
        symbol: &str,
        interval: BarInterval,
        timestamp: DateTime<Utc>,
        [open, high, low, close]: [f64; 4],
    ) -> TradeBar {
        let price = |dollars: f64| Price::from_dollars(dollars).unwrap();
        let ends = interval.ends(timestamp);
        let opened = match interval {
            BarInterval::OneMinute | BarInterval::FiveMinute => timestamp,
            BarInterval::OneDay => ends - chrono::TimeDelta::minutes(390),
        };
        let shares = Shares::from_float(100.0).unwrap();
        TradeBar::new(
            Symbol::new(symbol).unwrap(),
            interval,
            timestamp,
            TradeSums::new(
                TradeTotals::new(
                    TradeCount::new(1),
                    shares,
                    DollarVolume::of(price(close), shares),
                ),
                Some(
                    OpenClose::new(
                        StampedPrice::new(opened, price(open)),
                        StampedPrice::new(ends - chrono::TimeDelta::seconds(1), price(close)),
                    )
                    .unwrap(),
                ),
                Some(HighLow::new(price(high), price(low)).unwrap()),
            ),
        )
        .unwrap()
    }

    /// `dataset` with each session read as two AAPL minute bars an hour before the close, opening at $10.
    pub(crate) fn minute_dataset(run: RunId) -> Dataset {
        let mut dataset = dataset(run);
        dataset.bars = dataset
            .bars
            .into_keys()
            .map(|day| {
                let minutes = [60, 59].map(|before| {
                    trade_bar(
                        BarInterval::OneMinute,
                        day.regular_close() - chrono::TimeDelta::minutes(before),
                        [10.0; 4],
                    )
                });
                (day, Partition::try_from(minutes.to_vec()).unwrap())
            })
            .collect();
        dataset
    }

    /// Sessions 0, 1 and 3 read by `run` with one, two and three bars, and session 2 missing.
    pub(crate) fn dataset(run: RunId) -> Dataset {
        let calendar = calendar();
        let bars = |day: i64, count: usize| {
            let bar = trade_bar(
                BarInterval::OneDay,
                session(day).regular_close(),
                [10.0, 12.0, 9.5, 11.0],
            );
            (session(day), Partition::try_from(vec![bar; count]).unwrap())
        };
        let read = [0, 1, 3]
            .map(|day| {
                (
                    session(day),
                    PartitionRead::new(
                        Provider::Massive,
                        EntityTag::new(&format!("\"tag-{day}\"")),
                    ),
                )
            })
            .into();
        Dataset {
            bars: [bars(0, 1), bars(1, 2), bars(3, 3)].into(),
            lineage: Lineage {
                fingerprint: Fingerprint::new(
                    DatasetLeg::DailyTradeBars,
                    SessionRange::new(session(0), session(3)).unwrap(),
                    &calendar,
                    read,
                )
                .unwrap(),
                run,
            },
        }
    }

    /// A session reads from Massive when both hold it, falls back to Alpaca when only Alpaca does, and is missing when
    /// neither does; no provider after the one found is consulted.
    #[tokio::test]
    async fn test_a_session_reads_from_the_first_provider_holding_it() {
        let candidates = [(Provider::Massive, "massive"), (Provider::Alpaca, "alpaca")];
        for (held, expected, consulted) in [
            (
                &["massive", "alpaca"][..],
                Some((Provider::Massive, "massive")),
                &["massive"][..],
            ),
            (
                &["alpaca"][..],
                Some((Provider::Alpaca, "alpaca")),
                &["massive", "alpaca"][..],
            ),
            (&[][..], None, &["massive", "alpaca"][..]),
        ] {
            let asked = std::sync::Mutex::new(Vec::new());
            let found = preferred(candidates, async |provider, key| {
                asked.lock().unwrap().push(key);
                Ok::<_, ()>(held.contains(&key).then_some((provider, key)))
            })
            .await
            .unwrap();
            assert_eq!(found, expected, "{held:?}");
            assert_eq!(*asked.lock().unwrap(), consulted, "{held:?}");
        }
    }

    /// A subset keeps only its symbols' rows, and a partition holding none of them keeps nothing.
    #[test]
    fn test_a_partition_keeps_only_the_symbols_named() {
        let at = session(0).regular_close();
        let partition = Partition::try_from(
            ["AAPL", "MSFT", "AAPL"]
                .map(|symbol| symbol_bar(symbol, BarInterval::OneDay, at, [10.0; 4]))
                .to_vec(),
        )
        .unwrap();
        let only = |symbols: &[&str]| {
            Symbols::only(
                symbols
                    .iter()
                    .map(|symbol| Symbol::new(symbol).unwrap())
                    .collect(),
            )
            .unwrap()
        };
        let symbols_of = |kept: Option<Partition<TradeBar>>| -> Option<Vec<String>> {
            kept.map(|partition| {
                partition
                    .rows()
                    .iter()
                    .map(|bar| bar.symbol().to_string())
                    .collect()
            })
        };
        assert_eq!(
            symbols_of(kept(partition.clone(), &Symbols::Every)),
            Some(vec!["AAPL".into(), "MSFT".into(), "AAPL".into()])
        );
        assert_eq!(
            symbols_of(kept(partition.clone(), &only(&["MSFT"]))),
            Some(vec!["MSFT".into()])
        );
        assert_eq!(symbols_of(kept(partition, &only(&["IBM"]))), None);
    }

    /// A session whose partition kept no symbol is still read, so it is neither missing nor given bars, and the
    /// fingerprint journaled names the symbols kept.
    #[tokio::test]
    async fn test_a_session_keeping_no_symbol_is_read_without_bars() {
        let directory = std::env::temp_dir().join(format!("fund-study-{}", uuid::Uuid::new_v4()));
        let mut study = Study::open(
            Label::new("subset").unwrap(),
            &directory,
            Source::new("test.rs", ""),
        )
        .unwrap();
        let symbols = Symbols::only([Symbol::new("AAPL").unwrap()].into()).unwrap();
        let dataset = load(
            DatasetLeg::DailyTradeBars,
            &calendar(),
            SessionRange::new(session(0), session(2)).unwrap(),
            symbols.clone(),
            &mut study,
            async |day| {
                let read = PartitionRead::new(Provider::Massive, EntityTag::new("\"tag\""));
                let bar = trade_bar(BarInterval::OneDay, day.regular_close(), [10.0; 4]);
                Ok(match day == session(0) {
                    true => Some((Some(Partition::try_from(vec![bar]).unwrap()), read)),
                    false if day == session(1) => Some((None, read)),
                    false => None,
                })
            },
        )
        .await
        .unwrap();
        assert_eq!(
            dataset.bars().keys().copied().collect::<Vec<_>>(),
            [session(0)]
        );
        assert_eq!(
            dataset
                .fingerprint()
                .partitions()
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            [session(0), session(1)]
        );
        assert_eq!(dataset.fingerprint().missing(), [session(2)]);
        assert_eq!(dataset.fingerprint().symbols(), &symbols);
        std::fs::remove_dir_all(&directory).unwrap();
    }

    /// A study's reads and experiments land in its journal in order, name exactly the data read, and survive the
    /// parquet encoding the profile bucket stores.
    #[test]
    fn test_a_study_catalogs_what_it_read_and_ran() {
        let directory = std::env::temp_dir().join(format!("fund-study-{}", uuid::Uuid::new_v4()));
        let mut study = Study::open(
            Label::new("bar counts").unwrap(),
            &directory,
            Source::new("test.rs", ""),
        )
        .unwrap();
        let dataset = dataset(study.run_id());
        study.read(dataset.fingerprint()).unwrap();
        let series = Series::new([
            (session(0), Some(1.0)),
            (session(1), Some(2.0)),
            (session(3), Some(3.0)),
        ])
        .unwrap();
        let estimate = Estimate::from_summary(summarize(&series)).unwrap();
        for variant in ["all", "none"] {
            study
                .experiment(
                    Parameters::new([("variant", variant)]).unwrap(),
                    &[dataset.lineage()],
                    Outputs::default()
                        .estimate("bars per session", estimate)
                        .unwrap()
                        .metric("sessions", 3.0)
                        .unwrap(),
                )
                .unwrap();
        }
        let file = std::fs::read_dir(&directory)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "jsonl")
            })
            .unwrap();
        let lines = read(&std::fs::read_to_string(&file).unwrap());
        let observations: Vec<Observation> = lines
            .iter()
            .map(|line| match line {
                ReadLine::Read(read) => read.record().observation().clone(),
                ReadLine::Unreadable { line, cause, .. } => panic!("line {line}: {cause:?}"),
            })
            .collect();
        assert_eq!(
            observations
                .iter()
                .map(Observation::event_type)
                .collect::<Vec<_>>(),
            ["dataset_read", "experiment_ran", "experiment_ran"]
        );
        let (first, second) = match (&observations[1], &observations[2]) {
            (Observation::ExperimentRan(first), Observation::ExperimentRan(second)) => {
                (first, second)
            }
            other => panic!("{other:?}"),
        };
        assert_eq!(first.fingerprints(), [dataset.fingerprint().clone()]);
        assert_eq!(first.parameters().settings()["variant"], "all");
        assert_eq!(first.estimates()["bars per session"], estimate);
        assert!(!first.machine().hostname().is_empty());
        assert_eq!(
            (
                first.machine().architecture(),
                first.machine().operating_system()
            ),
            (std::env::consts::ARCH, std::env::consts::OS)
        );
        assert!(first.since_opened() <= second.since_opened());
        let session = match &lines[0] {
            ReadLine::Read(read) => read.session(),
            ReadLine::Unreadable { .. } => {
                unreachable!("every line read above")
            }
        };
        let key = JournalKey::new(Host::Researcher, session);
        let encoded = crate::storage::journal::encode(&key, &lines).unwrap();
        assert_eq!(
            crate::storage::journal::decode(&key, encoded).unwrap(),
            lines
        );
        std::fs::remove_dir_all(&directory).unwrap();
    }

    /// A dataset another run read carries no `dataset_read` in this run's journal, so it is refused rather than
    /// recorded as this run's input.
    #[test]
    fn test_an_experiment_refuses_a_dataset_another_run_read() {
        let directory = std::env::temp_dir().join(format!("fund-study-{}", uuid::Uuid::new_v4()));
        let mut study = Study::open(
            Label::new("bar counts").unwrap(),
            &directory,
            Source::new("test.rs", ""),
        )
        .unwrap();
        let elsewhere = RunId::new(uuid::Uuid::new_v4());
        assert!(matches!(
            study.experiment(Parameters::default(), &[dataset(elsewhere).lineage()], Outputs::default()),
            Err(crate::laboratory::StudyError::ReadByAnotherRun { run }) if run == elsewhere
        ));
        assert!(!directory.read_dir().unwrap().any(|entry| {
            entry
                .unwrap()
                .path()
                .extension()
                .is_some_and(|extension| extension == "jsonl")
        }));
        std::fs::remove_dir_all(&directory).unwrap();
    }

    /// Read-only: minute trade bars over 2026-10-05 to 10-13 from the production archive, under secretspec. Each session
    /// read comes from Massive exactly when Massive holds it, and every other trading session is named missing.
    #[tokio::test]
    #[ignore = "reads the live archive and the Alpaca calendar; run deliberately under secretspec"]
    async fn live_trade_bars_read_each_session_from_its_preferred_provider() {
        let configuration = aws_config::load_from_env().await;
        let archive = Bucket::archive(&configuration).unwrap();
        let alpaca = Alpaca::from_environment(reqwest::Client::new()).unwrap();
        let session =
            |month, day| SessionDate::from_date(NaiveDate::from_ymd_opt(2026, month, day).unwrap());
        let range = SessionRange::new(session(10, 5), session(10, 13)).unwrap();
        let calendar = alpaca.calendar(range).await.unwrap();
        let directory = std::env::temp_dir().join(format!("fund-study-{}", uuid::Uuid::new_v4()));
        let mut study = Study::open(
            Label::new("live trade bar check").unwrap(),
            &directory,
            Source::new("test.rs", ""),
        )
        .unwrap();
        let dataset = trade_bars(
            &archive,
            &calendar,
            range,
            BarInterval::OneMinute,
            Symbols::Every,
            &mut study,
        )
        .await
        .unwrap();
        let fingerprint = dataset.fingerprint();
        assert!(!fingerprint.partitions().is_empty(), "{fingerprint:?}");
        let mut sessions: Vec<SessionDate> = fingerprint.partitions().keys().copied().collect();
        sessions.extend(fingerprint.missing());
        sessions.sort();
        assert_eq!(sessions, calendar.trading_days_in_range(range));
        for (session, read) in fingerprint.partitions() {
            let [(_, massive), (_, alpaca)] = trade_keys(BarInterval::OneMinute, *session);
            let held = archive.tag(&Key::from(massive)).await.unwrap().is_some();
            let expected = if held {
                Provider::Massive
            } else {
                Provider::Alpaca
            };
            assert_eq!(read.provider(), expected, "{session}");
            if !held {
                assert!(
                    archive.tag(&Key::from(alpaca)).await.unwrap().is_some(),
                    "{session}"
                );
            }
            let rows = dataset.bars()[session].rows().len();
            assert!(rows > 100_000, "{session}: {rows} minute trade bars");
            eprintln!("{session} {} {rows}", read.provider());
        }
        for session in fingerprint.missing() {
            for (provider, key) in trade_keys(BarInterval::OneMinute, *session) {
                let tag = archive.tag(&Key::from(key)).await.unwrap();
                assert_eq!(tag, None, "{session} is missing but {provider} holds it");
            }
        }
        eprintln!("missing {:?}", fingerprint.missing());
        std::fs::remove_dir_all(&directory).unwrap();
    }
}
