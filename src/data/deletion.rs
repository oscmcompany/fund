//! The one sanctioned way to delete an archived session partition.
//!
//! A delete names the route that would rebuild what it removes, and a partition with none is refused.

use aws_sdk_s3::Client as S3Client;
use chrono::{Months, NaiveDate};
use tracing::info;

use crate::common::aws::date_partitioned_key;
use crate::common::flatfiles::{raw_key, RawDataset};
use crate::common::provenance::{
    AlpacaPlan, MassivePlan, MassiveTransport, PartitionProvenance, Provenance,
};
use crate::common::types::{BarInterval, SessionDate};
use crate::data::archive::{alpaca_trades_faithful_from, ArchiveError, SessionFamily};

/// The last day Massive Stocks Advanced answers, which closes the flat-file route for quotes and trades.
///
/// A fact about the subscription rather than the vendor, so a renewal moves it; left stale it
/// refuses deletes that would have been safe, which is the direction a stale guard should fail in.
pub const MASSIVE_ADVANCED_LAPSES_ON: (i32, u32, u32) = (2026, 10, 11);

/// How far back Massive Stocks Starter answers, measured live 2026-09-04 to the day.
const STARTER_HISTORY_MONTHS: u32 = 60;

/// How long a route must stay open after the delete for the rebuild not to race its own closure.
///
/// A week covers a rebuild queued behind a nightly and a weekend; a route closing sooner than that
/// is treated as already closed.
const REBUILD_MARGIN_DAYS: i64 = 7;

/// One session partition of one family at one cadence: the unit a delete removes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionAddress {
    family: SessionFamily,
    interval: BarInterval,
    session: SessionDate,
}

impl PartitionAddress {
    pub const fn new(family: SessionFamily, interval: BarInterval, session: SessionDate) -> Self {
        Self {
            family,
            interval,
            session,
        }
    }

    /// The parquet object this address names.
    pub fn key(self) -> String {
        date_partitioned_key(&self.family.prefix(self.interval), self.session.date())
    }

    /// The vendor file this partition folds from, where the archive keeps one.
    ///
    /// Five-minute and daily bars have none: they came from Massive's REST route, which never
    /// arrives as a file.
    fn raw_dataset(self) -> Option<RawDataset> {
        match (self.family, self.interval) {
            (SessionFamily::Bars, BarInterval::OneMinute) => Some(RawDataset::MinuteAggregates),
            (SessionFamily::Bars, BarInterval::FiveMinute | BarInterval::OneDay) => None,
            (SessionFamily::Quotes, _) => Some(RawDataset::Quotes),
            (SessionFamily::Trades, _) => Some(RawDataset::Trades),
        }
    }

    /// The raw object's key, whether or not it was ever written.
    pub fn raw_key(self) -> Option<String> {
        self.raw_dataset()
            .map(|dataset| raw_key(dataset, self.session.date()))
    }
}

impl std::fmt::Display for PartitionAddress {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{} {} {}",
            self.family, self.interval, self.session
        )
    }
}

/// How a deleted partition would be rebuilt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RederivationRoute {
    /// The vendor's own bytes, kept in Deep Archive: a rebuild costs a restore and a fold.
    RawObject { key: String },
    /// A provider that still answers for the session and rebuilds it faithfully.
    Provider(Provenance),
}

impl std::fmt::Display for RederivationRoute {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RederivationRoute::RawObject { key } => write!(formatter, "raw object {key}"),
            RederivationRoute::Provider(provenance) => write!(
                formatter,
                "{} under {}",
                provenance.provider_name(),
                provenance.subscription_name()
            ),
        }
    }
}

/// Why one candidate route cannot rebuild a partition, carrying the date or key that closed it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteClosed {
    /// The archive never kept this session's vendor file.
    RawObjectAbsent { key: String },
    /// Massive Advanced lapses too soon for a rebuild to finish on it.
    AdvancedLapses { on: NaiveDate },
    /// The session is older than, or within the margin of, Starter's rolling window.
    OutsideStarterWindow { earliest: SessionDate },
    /// Alpaca's history differs from the archive's before this session, so its fold is a downgrade.
    AlpacaUnfaithful { faithful_from: SessionDate },
}

impl std::fmt::Display for RouteClosed {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RouteClosed::RawObjectAbsent { key } => write!(formatter, "no raw object at {key}"),
            RouteClosed::AdvancedLapses { on } => {
                write!(formatter, "Massive Advanced lapses {on}")
            }
            RouteClosed::OutsideStarterWindow { earliest } => {
                write!(formatter, "Starter answers from {earliest}")
            }
            RouteClosed::AlpacaUnfaithful { faithful_from } => {
                write!(formatter, "Alpaca is faithful from {faithful_from}")
            }
        }
    }
}

/// A partition, with the route that would rebuild it: the only thing [`delete_partition`] accepts.
///
/// Fields are private and [`rederivation_route`] is the only constructor, so holding one is proof the
/// check ran rather than a claim that it did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rederivable {
    address: PartitionAddress,
    route: RederivationRoute,
}

impl Rederivable {
    pub fn address(&self) -> PartitionAddress {
        self.address
    }

    pub fn route(&self) -> &RederivationRoute {
        &self.route
    }
}

/// Raised when nothing could rebuild a partition, naming every route that was tried and why it closed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("refusing to delete {address}: nothing could rebuild it ({})", format_closed(.closed))]
pub struct NoRederivationRoute {
    pub address: PartitionAddress,
    pub closed: Vec<RouteClosed>,
}

fn format_closed(closed: &[RouteClosed]) -> String {
    closed
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("; ")
}

/// The first route that would rebuild `address` if it were deleted on `today`, or every reason none would.
///
/// `raw_object_present` is the caller's `HEAD` of [`PartitionAddress::raw_key`]. Tried in cost order:
/// the kept bytes first because they need no subscription, then Massive, then Alpaca.
pub fn rederivation_route(
    address: PartitionAddress,
    today: SessionDate,
    raw_object_present: bool,
) -> Result<Rederivable, NoRederivationRoute> {
    let mut closed = Vec::new();
    let found = |route| Ok(Rederivable { address, route });

    if let Some(key) = address.raw_key() {
        match raw_object_present {
            true => return found(RederivationRoute::RawObject { key }),
            false => closed.push(RouteClosed::RawObjectAbsent { key }),
        }
    }

    let rebuilt_by = today.plus_calendar_days(REBUILD_MARGIN_DAYS);
    match address.family {
        SessionFamily::Bars => {
            let earliest = starter_earliest(rebuilt_by);
            match address.session >= earliest {
                true => {
                    return found(RederivationRoute::Provider(Provenance::massive(
                        MassivePlan::StocksStarter,
                        MassiveTransport::Rest,
                    )))
                }
                false => closed.push(RouteClosed::OutsideStarterWindow { earliest }),
            }
        }
        SessionFamily::Quotes | SessionFamily::Trades => {
            let lapses_on = advanced_lapses_on();
            match rebuilt_by.date() < lapses_on {
                true => {
                    return found(RederivationRoute::Provider(Provenance::massive(
                        MassivePlan::StocksAdvanced,
                        MassiveTransport::FlatFile,
                    )))
                }
                false => closed.push(RouteClosed::AdvancedLapses { on: lapses_on }),
            }
        }
    }

    match address.family {
        SessionFamily::Bars => {}
        SessionFamily::Quotes => {
            return found(RederivationRoute::Provider(Provenance::alpaca(
                AlpacaPlan::AlgoTraderPlus,
            )))
        }
        SessionFamily::Trades => {
            let faithful_from = alpaca_trades_faithful_from();
            match address.session >= faithful_from {
                true => {
                    return found(RederivationRoute::Provider(Provenance::alpaca(
                        AlpacaPlan::AlgoTraderPlus,
                    )))
                }
                false => closed.push(RouteClosed::AlpacaUnfaithful { faithful_from }),
            }
        }
    }

    Err(NoRederivationRoute { address, closed })
}

fn advanced_lapses_on() -> NaiveDate {
    let (year, month, day) = MASSIVE_ADVANCED_LAPSES_ON;
    NaiveDate::from_ymd_opt(year, month, day).expect("the lapse date is a real calendar date")
}

/// The earliest session Starter still serves on `date`.
fn starter_earliest(date: SessionDate) -> SessionDate {
    SessionDate::from_date(
        date.date()
            .checked_sub_months(Months::new(STARTER_HISTORY_MONTHS))
            .expect("five years before a session is a real calendar date"),
    )
}

/// Whether an object exists, answered by `HEAD` so a Deep Archive object needs no restore.
pub async fn object_exists(
    s3_client: &S3Client,
    bucket: &str,
    key: &str,
) -> Result<bool, ArchiveError> {
    match s3_client.head_object().bucket(bucket).key(key).send().await {
        Ok(_) => Ok(true),
        Err(error)
            if error
                .as_service_error()
                .is_some_and(|inner| inner.is_not_found()) =>
        {
            Ok(false)
        }
        Err(error) => Err(ArchiveError::Read {
            bucket: bucket.to_string(),
            key: key.to_string(),
            message: error.to_string(),
        }),
    }
}

/// What a delete removed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Deleted {
    /// The partition and its provenance sidecar.
    Partition,
    /// Nothing was stored at the address, so nothing was removed.
    Absent,
}

/// Deletes a partition and its sidecar, logging the route that would rebuild it.
///
/// The parquet goes first: a sidecar left behind by a failed second call describes nothing, while
/// a partition left without its sidecar is one the provenance sweep would report as unattributed.
pub async fn delete_partition(
    s3_client: &S3Client,
    bucket: &str,
    rederivable: &Rederivable,
) -> Result<Deleted, ArchiveError> {
    let key = rederivable.address.key();
    if !object_exists(s3_client, bucket, &key).await? {
        return Ok(Deleted::Absent);
    }
    for object in [key.clone(), PartitionProvenance::sidecar_key(&key)] {
        s3_client
            .delete_object()
            .bucket(bucket)
            .key(&object)
            .send()
            .await
            .map_err(|error| ArchiveError::Write {
                bucket: bucket.to_string(),
                key: object.clone(),
                message: error.to_string(),
            })?;
    }
    info!(
        partition = %rederivable.address,
        key = %key,
        route = %rederivable.route,
        "Deleted archive partition"
    );
    Ok(Deleted::Partition)
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use aws_smithy_http_client::test_util::infallible_client_fn;
    use aws_smithy_types::body::SdkBody;
    use percent_encoding::percent_decode_str;

    use super::*;

    fn session(year: i32, month: u32, day: u32) -> SessionDate {
        SessionDate::from_date(NaiveDate::from_ymd_opt(year, month, day).expect("a real date"))
    }

    fn address(family: SessionFamily, interval: BarInterval, on: SessionDate) -> PartitionAddress {
        PartitionAddress::new(family, interval, on)
    }

    fn alpaca() -> RederivationRoute {
        RederivationRoute::Provider(Provenance::alpaca(AlpacaPlan::AlgoTraderPlus))
    }

    #[test]
    fn test_a_kept_raw_object_is_the_route_whatever_the_date() {
        let target = address(
            SessionFamily::Trades,
            BarInterval::OneDay,
            session(2021, 9, 1),
        );
        let found = rederivation_route(target, session(2027, 1, 4), true)
            .expect("the kept bytes rebuild any session");
        assert_eq!(
            found.route(),
            &RederivationRoute::RawObject {
                key:
                    "data/raw/massive/equity/trades/schema=v1/year=2021/month=09/day=01/data.csv.gz"
                        .to_string()
            }
        );
    }

    #[test]
    fn test_an_early_trade_session_without_its_bytes_is_refused_after_the_lapse() {
        let target = address(
            SessionFamily::Trades,
            BarInterval::OneMinute,
            session(2022, 6, 14),
        );
        let refusal = rederivation_route(target, session(2026, 10, 12), false)
            .expect_err("Alpaca's early trades are a downgrade and the flat files are gone");
        assert_eq!(
            refusal.closed,
            vec![
                RouteClosed::RawObjectAbsent {
                    key: "data/raw/massive/equity/trades/schema=v1/year=2022/month=06/day=14/data.csv.gz"
                        .to_string()
                },
                RouteClosed::AdvancedLapses {
                    on: NaiveDate::from_ymd_opt(2026, 10, 11).expect("a real date")
                },
                RouteClosed::AlpacaUnfaithful {
                    faithful_from: session(2023, 7, 5)
                },
            ]
        );
    }

    #[test]
    fn test_the_flat_file_route_closes_a_week_before_the_lapse() {
        let target = address(
            SessionFamily::Trades,
            BarInterval::OneDay,
            session(2022, 6, 14),
        );
        let open = rederivation_route(target, session(2026, 10, 3), false)
            .expect("ten days out the flat files still answer");
        assert_eq!(
            open.route(),
            &RederivationRoute::Provider(Provenance::massive(
                MassivePlan::StocksAdvanced,
                MassiveTransport::FlatFile
            ))
        );
        assert!(rederivation_route(target, session(2026, 10, 4), false).is_err());
    }

    #[test]
    fn test_a_trade_session_alpaca_reproduces_falls_back_to_alpaca() {
        let faithful = address(
            SessionFamily::Trades,
            BarInterval::OneDay,
            session(2023, 7, 5),
        );
        let found = rederivation_route(faithful, session(2026, 12, 1), false)
            .expect("from 2023-07-05 Alpaca rebuilds trades faithfully");
        assert_eq!(found.route(), &alpaca());

        let one_before = address(
            SessionFamily::Trades,
            BarInterval::OneDay,
            session(2023, 7, 3),
        );
        assert!(rederivation_route(one_before, session(2026, 12, 1), false).is_err());
    }

    #[test]
    fn test_quotes_always_have_alpaca() {
        let target = address(
            SessionFamily::Quotes,
            BarInterval::OneMinute,
            session(2021, 8, 23),
        );
        let found = rederivation_route(target, session(2027, 1, 4), false)
            .expect("Alpaca serves quotes back to 2016");
        assert_eq!(found.route(), &alpaca());
    }

    #[test]
    fn test_daily_bars_are_refused_once_they_leave_starters_window() {
        let today = session(2026, 9, 25);
        let inside = address(
            SessionFamily::Bars,
            BarInterval::OneDay,
            session(2021, 10, 2),
        );
        assert_eq!(
            rederivation_route(inside, today, false)
                .expect("inside the window with a week to spare")
                .route(),
            &RederivationRoute::Provider(Provenance::massive(
                MassivePlan::StocksStarter,
                MassiveTransport::Rest
            ))
        );

        let edge = address(
            SessionFamily::Bars,
            BarInterval::OneDay,
            session(2021, 9, 30),
        );
        let refusal = rederivation_route(edge, today, false)
            .expect_err("inside the window today, outside it by the time a rebuild runs");
        assert_eq!(
            refusal.closed,
            vec![RouteClosed::OutsideStarterWindow {
                earliest: session(2021, 10, 2)
            }]
        );
    }

    #[test]
    fn test_five_minute_bars_have_no_raw_object_to_look_for() {
        let target = address(
            SessionFamily::Bars,
            BarInterval::FiveMinute,
            session(2024, 1, 25),
        );
        assert_eq!(target.raw_key(), None);
        let found = rederivation_route(target, session(2026, 9, 25), true)
            .expect("REST rebuilds a recent five-minute session");
        assert!(matches!(found.route(), RederivationRoute::Provider(_)));
    }

    #[test]
    fn test_the_refusal_names_every_closed_route() {
        let refusal = NoRederivationRoute {
            address: address(
                SessionFamily::Bars,
                BarInterval::OneDay,
                session(2021, 8, 23),
            ),
            closed: vec![RouteClosed::OutsideStarterWindow {
                earliest: session(2021, 10, 2),
            }],
        };
        assert_eq!(
            refusal.to_string(),
            "refusing to delete bars one_day 2021-08-23: nothing could rebuild it (Starter answers \
             from 2021-10-02)"
        );
    }

    fn scripted_s3_client(
        respond: impl Fn(&http::Method, &str) -> http::Response<SdkBody> + Send + Sync + 'static,
    ) -> S3Client {
        let http_client = infallible_client_fn(move |request| {
            let method = request.method().clone();
            let key = percent_decode_str(request.uri().path()).decode_utf8_lossy();
            respond(&method, &key)
        });
        S3Client::from_conf(
            aws_sdk_s3::Config::builder()
                .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
                .region(aws_sdk_s3::config::Region::new("us-east-1"))
                .credentials_provider(aws_sdk_s3::config::Credentials::new(
                    "test-key",
                    "test-secret",
                    None,
                    None,
                    "test",
                ))
                .http_client(http_client)
                .build(),
        )
    }

    fn status(code: u16) -> http::Response<SdkBody> {
        http::Response::builder()
            .status(code)
            .body(SdkBody::empty())
            .expect("a canned response must build")
    }

    fn recent_quotes() -> Rederivable {
        rederivation_route(
            address(
                SessionFamily::Quotes,
                BarInterval::OneDay,
                session(2026, 9, 18),
            ),
            session(2026, 9, 25),
            false,
        )
        .expect("recent quotes are rebuildable")
    }

    #[tokio::test]
    async fn test_a_delete_removes_the_partition_and_then_its_sidecar() {
        let deleted: Arc<Mutex<Vec<String>>> = Arc::default();
        let seen = Arc::clone(&deleted);
        let client = scripted_s3_client(move |method, key| match *method {
            http::Method::HEAD => status(200),
            http::Method::DELETE => {
                seen.lock().expect("unpoisoned").push(key.to_string());
                status(204)
            }
            _ => status(500),
        });

        let outcome = delete_partition(&client, "archive", &recent_quotes())
            .await
            .expect("the delete succeeds");

        assert_eq!(outcome, Deleted::Partition);
        let partition =
            "/data/derived/equity/quotes/interval=one_day/year=2026/month=09/day=18/data.parquet";
        assert_eq!(
            *deleted.lock().expect("unpoisoned"),
            vec![
                partition.to_string(),
                format!("{partition}.provenance.json")
            ]
        );
    }

    #[tokio::test]
    async fn test_a_partition_that_is_not_stored_deletes_nothing() {
        let deletes: Arc<Mutex<usize>> = Arc::default();
        let seen = Arc::clone(&deletes);
        let client = scripted_s3_client(move |method, _| match *method {
            http::Method::HEAD => status(404),
            _ => {
                *seen.lock().expect("unpoisoned") += 1;
                status(204)
            }
        });

        let outcome = delete_partition(&client, "archive", &recent_quotes())
            .await
            .expect("an absent partition is not an error");

        assert_eq!(outcome, Deleted::Absent);
        assert_eq!(*deletes.lock().expect("unpoisoned"), 0);
    }
}
