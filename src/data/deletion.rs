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
use crate::data::archive::{
    alpaca_trades_faithful_from, read_sidecar, ArchiveError, SessionFamily, SidecarRead,
};

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
    /// The route is open but rebuilds from a different source than the one that built the partition.
    NotTheSource { route: Provenance },
    /// The partition names more than one source, so no single route rebuilds all of its rows.
    MixedSources { routes: Vec<Provenance> },
    /// The partition has no readable provenance record, so which route built it is unknown.
    Unattributed,
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
            RouteClosed::NotTheSource { route } => write!(
                formatter,
                "{} under {} did not build it",
                route.provider_name(),
                route.subscription_name()
            ),
            RouteClosed::MixedSources { routes } => {
                let names: Vec<String> = routes
                    .iter()
                    .map(|route| {
                        format!(
                            "{} under {}",
                            route.provider_name(),
                            route.subscription_name()
                        )
                    })
                    .collect();
                write!(formatter, "built by {}", names.join(" and "))
            }
            RouteClosed::Unattributed => write!(formatter, "no readable provenance record"),
        }
    }
}

/// A partition's provenance record, as read, with the version a delete must still find.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredSidecar {
    routes: Vec<Provenance>,
    etag: String,
}

impl StoredSidecar {
    pub fn new(routes: Vec<Provenance>, etag: String) -> Self {
        Self { routes, etag }
    }
}

/// What the archive held at an address when it was inspected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredPartition {
    parquet_etag: String,
    /// `None` where the record was absent or did not parse, either of which leaves the source unknown.
    sidecar: Option<StoredSidecar>,
}

impl StoredPartition {
    pub fn new(parquet_etag: String, sidecar: Option<StoredSidecar>) -> Self {
        Self {
            parquet_etag,
            sidecar,
        }
    }
}

/// What is stored at an address.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Inspection {
    Stored(StoredPartition),
    /// No parquet. `orphaned_sidecar` is a record an earlier delete removed the parquet from and
    /// then failed to follow, carrying the version to delete.
    NotStored {
        orphaned_sidecar: Option<String>,
    },
}

/// Reads the parquet's version and the provenance record at `address`.
pub async fn inspect(
    s3_client: &S3Client,
    bucket: &str,
    address: PartitionAddress,
) -> Result<Inspection, ArchiveError> {
    let key = address.key();
    // The record is read first: a fold writes the parquet before its record, so reading in the same
    // order could pair a new parquet with the record it is about to replace.
    let (sidecar, orphaned_sidecar) =
        match read_sidecar(s3_client, bucket, &PartitionProvenance::sidecar_key(&key)).await? {
            SidecarRead::Found(record, etag) => (
                Some(StoredSidecar::new(record.routes, etag.clone())),
                Some(etag),
            ),
            SidecarRead::Unreadable | SidecarRead::Absent => (None, None),
        };
    match object_etag(s3_client, bucket, &key).await? {
        Some(parquet_etag) => Ok(Inspection::Stored(StoredPartition::new(
            parquet_etag,
            sidecar,
        ))),
        None => Ok(Inspection::NotStored { orphaned_sidecar }),
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
    parquet_etag: String,
    sidecar_etag: String,
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
/// A route counts only if it is the one source the partition's record names: any other writes
/// different rows, and a mixed partition's second source has no route back at all.
/// `raw_object_present` is the caller's `HEAD` of [`PartitionAddress::raw_key`]. Candidates are tried
/// in cost order: the kept bytes first because they need no subscription, then Massive, then Alpaca.
pub fn rederivation_route(
    address: PartitionAddress,
    today: SessionDate,
    stored: &StoredPartition,
    raw_object_present: bool,
) -> Result<Rederivable, NoRederivationRoute> {
    let refuse = |closed| Err(NoRederivationRoute { address, closed });
    let Some(sidecar) = &stored.sidecar else {
        return refuse(vec![RouteClosed::Unattributed]);
    };
    let source = match sidecar.routes.as_slice() {
        [source] => *source,
        [] => return refuse(vec![RouteClosed::Unattributed]),
        routes => {
            return refuse(vec![RouteClosed::MixedSources {
                routes: routes.to_vec(),
            }])
        }
    };

    let mut closed = Vec::new();
    for (route, provenance, shut) in candidates(address, today, raw_object_present) {
        match shut {
            Some(reason) => closed.push(reason),
            None if provenance != source => {
                closed.push(RouteClosed::NotTheSource { route: provenance })
            }
            None => {
                return Ok(Rederivable {
                    address,
                    route,
                    parquet_etag: stored.parquet_etag.clone(),
                    sidecar_etag: sidecar.etag.clone(),
                })
            }
        }
    }
    refuse(closed)
}

/// Every route that could rebuild `address`, in cost order, each with its source and what closes it.
fn candidates(
    address: PartitionAddress,
    today: SessionDate,
    raw_object_present: bool,
) -> Vec<(RederivationRoute, Provenance, Option<RouteClosed>)> {
    let mut candidates = Vec::new();
    if let Some(dataset) = address.raw_dataset() {
        let key = raw_key(dataset, address.session.date());
        let shut = (!raw_object_present).then(|| RouteClosed::RawObjectAbsent { key: key.clone() });
        candidates.push((
            RederivationRoute::RawObject { key },
            dataset.provenance(),
            shut,
        ));
    }

    let rebuilt_by = today.plus_calendar_days(REBUILD_MARGIN_DAYS);
    let provider = |provenance: Provenance, shut: Option<RouteClosed>| {
        (RederivationRoute::Provider(provenance), provenance, shut)
    };
    match address.family {
        SessionFamily::Bars => {
            let earliest = starter_earliest(rebuilt_by);
            candidates.push(provider(
                Provenance::massive(MassivePlan::StocksStarter, MassiveTransport::Rest),
                (address.session < earliest)
                    .then_some(RouteClosed::OutsideStarterWindow { earliest }),
            ));
        }
        SessionFamily::Quotes | SessionFamily::Trades => {
            let lapses_on = advanced_lapses_on();
            candidates.push(provider(
                Provenance::massive(MassivePlan::StocksAdvanced, MassiveTransport::FlatFile),
                (rebuilt_by.date() >= lapses_on)
                    .then_some(RouteClosed::AdvancedLapses { on: lapses_on }),
            ));
            let faithful_from = match address.family {
                SessionFamily::Trades => Some(alpaca_trades_faithful_from()),
                SessionFamily::Bars | SessionFamily::Quotes => None,
            };
            candidates.push(provider(
                Provenance::alpaca(AlpacaPlan::AlgoTraderPlus),
                faithful_from
                    .filter(|&floor| address.session < floor)
                    .map(|faithful_from| RouteClosed::AlpacaUnfaithful { faithful_from }),
            ));
        }
    }
    candidates
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

/// An object's ETag, or `None` where there is none, answered by `HEAD` so Deep Archive needs no restore.
pub async fn object_etag(
    s3_client: &S3Client,
    bucket: &str,
    key: &str,
) -> Result<Option<String>, ArchiveError> {
    match s3_client.head_object().bucket(bucket).key(key).send().await {
        Ok(head) => Ok(Some(head.e_tag().unwrap_or_default().to_string())),
        Err(error)
            if error
                .as_service_error()
                .is_some_and(|inner| inner.is_not_found()) =>
        {
            Ok(None)
        }
        Err(error) => Err(ArchiveError::Read {
            bucket: bucket.to_string(),
            key: key.to_string(),
            message: error.to_string(),
        }),
    }
}

/// Deletes one object only if it still carries `etag`.
///
/// Conditional because the archive's writers are: a fold landing after the inspection replaces the
/// object, and an unconditional delete would remove the new bytes the route check never saw.
async fn delete_if_unchanged(
    s3_client: &S3Client,
    bucket: &str,
    key: &str,
    etag: &str,
) -> Result<(), ArchiveError> {
    s3_client
        .delete_object()
        .bucket(bucket)
        .key(key)
        .if_match(etag)
        .send()
        .await
        .map(|_| ())
        .map_err(|error| ArchiveError::Write {
            bucket: bucket.to_string(),
            key: key.to_string(),
            message: match error
                .raw_response()
                .map(|response| response.status().as_u16())
            {
                Some(412) => "changed since it was inspected; inspect it again".to_string(),
                _ => error.to_string(),
            },
        })
}

/// Deletes a partition and then its sidecar, each only as it was inspected.
///
/// Parquet first: a sidecar a failed second call leaves behind describes nothing, and the next
/// inspection finds it as [`Inspection::NotStored`] for [`delete_orphaned_sidecar`].
pub async fn delete_partition(
    s3_client: &S3Client,
    bucket: &str,
    rederivable: &Rederivable,
) -> Result<(), ArchiveError> {
    let key = rederivable.address.key();
    delete_if_unchanged(s3_client, bucket, &key, &rederivable.parquet_etag).await?;
    delete_if_unchanged(
        s3_client,
        bucket,
        &PartitionProvenance::sidecar_key(&key),
        &rederivable.sidecar_etag,
    )
    .await?;
    info!(
        partition = %rederivable.address,
        key = %key,
        route = %rederivable.route,
        "Deleted archive partition"
    );
    Ok(())
}

/// Deletes a provenance record whose parquet is already gone, as it was inspected.
///
/// Needs no route: it describes rows that no longer exist, and left in place it would be read as the
/// record of whatever partition is written at the key next.
pub async fn delete_orphaned_sidecar(
    s3_client: &S3Client,
    bucket: &str,
    address: PartitionAddress,
    etag: &str,
) -> Result<(), ArchiveError> {
    let sidecar = PartitionProvenance::sidecar_key(&address.key());
    delete_if_unchanged(s3_client, bucket, &sidecar, etag).await?;
    info!(partition = %address, key = %sidecar, "Deleted orphaned provenance record");
    Ok(())
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

    fn alpaca() -> Provenance {
        Provenance::alpaca(AlpacaPlan::AlgoTraderPlus)
    }

    fn flat_file() -> Provenance {
        Provenance::massive(MassivePlan::StocksAdvanced, MassiveTransport::FlatFile)
    }

    fn starter() -> Provenance {
        Provenance::massive(MassivePlan::StocksStarter, MassiveTransport::Rest)
    }

    fn built_by(routes: &[Provenance]) -> StoredPartition {
        StoredPartition::new(
            "\"parquet\"".to_string(),
            Some(StoredSidecar::new(
                routes.to_vec(),
                "\"sidecar\"".to_string(),
            )),
        )
    }

    #[test]
    fn test_a_kept_raw_object_is_the_route_whatever_the_date() {
        let target = address(
            SessionFamily::Trades,
            BarInterval::OneDay,
            session(2021, 9, 1),
        );
        let found =
            rederivation_route(target, session(2027, 1, 4), &built_by(&[flat_file()]), true)
                .expect("the kept bytes rebuild any session they built");
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
        let refusal = rederivation_route(
            target,
            session(2026, 10, 12),
            &built_by(&[flat_file()]),
            false,
        )
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
        let stored = built_by(&[flat_file()]);
        let open = rederivation_route(target, session(2026, 10, 3), &stored, false)
            .expect("ten days out the flat files still answer");
        assert_eq!(open.route(), &RederivationRoute::Provider(flat_file()));
        assert!(rederivation_route(target, session(2026, 10, 4), &stored, false).is_err());
    }

    #[test]
    fn test_a_trade_session_alpaca_built_is_rebuilt_from_alpaca_from_its_floor() {
        let stored = built_by(&[alpaca()]);
        let faithful = address(
            SessionFamily::Trades,
            BarInterval::OneDay,
            session(2023, 7, 5),
        );
        let found = rederivation_route(faithful, session(2026, 12, 1), &stored, false)
            .expect("from 2023-07-05 Alpaca rebuilds trades faithfully");
        assert_eq!(found.route(), &RederivationRoute::Provider(alpaca()));

        let one_before = address(
            SessionFamily::Trades,
            BarInterval::OneDay,
            session(2023, 7, 3),
        );
        assert!(rederivation_route(one_before, session(2026, 12, 1), &stored, false).is_err());
    }

    #[test]
    fn test_a_partition_alpaca_built_is_not_rebuilt_from_massives_file() {
        let target = address(
            SessionFamily::Quotes,
            BarInterval::OneMinute,
            session(2026, 9, 22),
        );
        let found = rederivation_route(target, session(2026, 9, 25), &built_by(&[alpaca()]), true)
            .expect("Alpaca rebuilds what Alpaca built");
        assert_eq!(found.route(), &RederivationRoute::Provider(alpaca()));
    }

    #[test]
    fn test_a_partition_with_two_sources_is_refused_even_with_its_raw_file() {
        let target = address(
            SessionFamily::Quotes,
            BarInterval::OneMinute,
            session(2024, 1, 25),
        );
        let refusal = rederivation_route(
            target,
            session(2026, 9, 25),
            &built_by(&[flat_file(), alpaca()]),
            true,
        )
        .expect_err("the file cannot restore the repair's rows");
        assert_eq!(
            refusal.closed,
            vec![RouteClosed::MixedSources {
                routes: vec![flat_file(), alpaca()]
            }]
        );
    }

    #[test]
    fn test_a_partition_with_no_readable_record_is_refused() {
        let target = address(
            SessionFamily::Quotes,
            BarInterval::OneDay,
            session(2026, 9, 18),
        );
        let stored = StoredPartition::new("\"parquet\"".to_string(), None);
        let refusal = rederivation_route(target, session(2026, 9, 25), &stored, true)
            .expect_err("an unknown source has no known route back");
        assert_eq!(refusal.closed, vec![RouteClosed::Unattributed]);
    }

    #[test]
    fn test_daily_bars_are_refused_once_they_leave_starters_window() {
        let today = session(2026, 9, 25);
        let stored = built_by(&[starter()]);
        let inside = address(
            SessionFamily::Bars,
            BarInterval::OneDay,
            session(2021, 10, 2),
        );
        assert_eq!(
            rederivation_route(inside, today, &stored, false)
                .expect("inside the window with a week to spare")
                .route(),
            &RederivationRoute::Provider(starter())
        );

        let edge = address(
            SessionFamily::Bars,
            BarInterval::OneDay,
            session(2021, 9, 30),
        );
        let refusal = rederivation_route(edge, today, &stored, false)
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
        let found = rederivation_route(target, session(2026, 9, 25), &built_by(&[starter()]), true)
            .expect("REST rebuilds a recent five-minute session");
        assert_eq!(found.route(), &RederivationRoute::Provider(starter()));
    }

    #[test]
    fn test_the_refusal_names_every_closed_route() {
        let refusal = NoRederivationRoute {
            address: address(
                SessionFamily::Bars,
                BarInterval::OneDay,
                session(2021, 8, 23),
            ),
            closed: vec![
                RouteClosed::OutsideStarterWindow {
                    earliest: session(2021, 10, 2),
                },
                RouteClosed::NotTheSource { route: alpaca() },
            ],
        };
        assert_eq!(
            refusal.to_string(),
            "refusing to delete bars one_day 2021-08-23: nothing could rebuild it (Starter answers \
             from 2021-10-02; alpaca under algo_trader_plus did not build it)"
        );
    }

    /// Every request's method, decoded key and `If-Match`, answered by `respond`.
    fn scripted_s3_client(
        seen: Arc<Mutex<Vec<(String, String, Option<String>)>>>,
        respond: impl Fn(&http::Method, &str) -> http::Response<SdkBody> + Send + Sync + 'static,
    ) -> S3Client {
        let http_client = infallible_client_fn(move |request| {
            let method = request.method().clone();
            let key = percent_decode_str(request.uri().path())
                .decode_utf8_lossy()
                .to_string();
            let if_match = request
                .headers()
                .get("if-match")
                .and_then(|value| value.to_str().ok())
                .map(str::to_string);
            seen.lock()
                .expect("unpoisoned")
                .push((method.to_string(), key.clone(), if_match));
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
            &built_by(&[alpaca()]),
            false,
        )
        .expect("recent quotes are rebuildable")
    }

    const PARTITION: &str =
        "/data/derived/equity/quotes/interval=one_day/year=2026/month=09/day=18/data.parquet";

    #[tokio::test]
    async fn test_a_delete_removes_each_object_only_as_it_was_inspected() {
        let seen: Arc<Mutex<Vec<(String, String, Option<String>)>>> = Arc::default();
        let client = scripted_s3_client(Arc::clone(&seen), |_, _| status(204));

        delete_partition(&client, "archive", &recent_quotes())
            .await
            .expect("the delete succeeds");

        assert_eq!(
            *seen.lock().expect("unpoisoned"),
            vec![
                (
                    "DELETE".to_string(),
                    PARTITION.to_string(),
                    Some("\"parquet\"".to_string())
                ),
                (
                    "DELETE".to_string(),
                    format!("{PARTITION}.provenance.json"),
                    Some("\"sidecar\"".to_string())
                ),
            ]
        );
    }

    #[tokio::test]
    async fn test_a_partition_rewritten_since_inspection_is_left_with_its_record() {
        let seen: Arc<Mutex<Vec<(String, String, Option<String>)>>> = Arc::default();
        let client = scripted_s3_client(Arc::clone(&seen), |_, _| status(412));

        let error = delete_partition(&client, "archive", &recent_quotes())
            .await
            .expect_err("a fold replaced the parquet after it was inspected");

        assert!(error.to_string().contains("changed since it was inspected"));
        assert_eq!(
            seen.lock().expect("unpoisoned").len(),
            1,
            "the sidecar is not touched once the parquet refuses"
        );
    }
}
