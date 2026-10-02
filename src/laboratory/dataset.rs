//! Loads archive series for studies, each with the fingerprint of exactly what was read.

use std::collections::BTreeMap;

use crate::archive::bars::{DecodeRefusal, decode};
use crate::archive::{Archive, ArchiveError};
use crate::common::heal::Leg;
use crate::common::laboratory::dataset::{Fingerprint, FingerprintRefusal};
use crate::common::market::record::Bar;
use crate::common::time::SessionDate;
use crate::common::time::calendar::TradingCalendar;

/// Bars by session and the fingerprint of the partitions they came from.
#[derive(Debug)]
pub struct Dataset {
    bars: BTreeMap<SessionDate, Vec<Bar>>,
    fingerprint: Fingerprint,
}

impl Dataset {
    pub fn bars(&self) -> &BTreeMap<SessionDate, Vec<Bar>> {
        &self.bars
    }

    pub fn fingerprint(&self) -> &Fingerprint {
        &self.fingerprint
    }
}

#[derive(Debug)]
pub enum DatasetError {
    Window(FingerprintRefusal),
    Archive(ArchiveError),
    Decode {
        session: SessionDate,
        refusal: DecodeRefusal,
    },
}

impl std::fmt::Display for DatasetError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Window(refusal) => write!(formatter, "{refusal}"),
            Self::Archive(error) => write!(formatter, "{error}"),
            Self::Decode { session, refusal } => {
                write!(
                    formatter,
                    "the partition for {session} did not decode: {refusal:?}"
                )
            }
        }
    }
}

impl std::error::Error for DatasetError {}

/// Massive daily bars for every trading session from `first` to `last`; a session with no partition is recorded
/// missing in the fingerprint rather than refused.
pub async fn daily_bars(
    archive: &Archive,
    calendar: &TradingCalendar,
    first: SessionDate,
    last: SessionDate,
) -> Result<Dataset, DatasetError> {
    let leg = Leg::MassiveDailyBars;
    let series = leg.key(first).series();
    // Taken empty first, so the window is checked before any read and its missing sessions are the ones to read.
    let owed = Fingerprint::new(series.clone(), first, last, calendar, BTreeMap::new())
        .map_err(DatasetError::Window)?;
    let (mut bars, mut tags) = (BTreeMap::new(), BTreeMap::new());
    for session in owed.missing() {
        let key = leg.key(*session);
        let Some((body, tag)) = archive
            .get_tagged(&key)
            .await
            .map_err(DatasetError::Archive)?
        else {
            continue;
        };
        let (read, _) = decode(&key, body).map_err(|refusal| DatasetError::Decode {
            session: *session,
            refusal,
        })?;
        bars.insert(*session, read);
        tags.insert(*session, tag.as_str().to_string());
    }
    let fingerprint =
        Fingerprint::new(series, first, last, calendar, tags).map_err(DatasetError::Window)?;
    Ok(Dataset { bars, fingerprint })
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;

    use super::*;
    use crate::ingest::alpaca::Alpaca;

    /// Read-only: one week of the production archive, read twice, under secretspec. The week holds the layout's first
    /// sessions, 2026-09-28 and 09-29, so it always reads something.
    #[tokio::test]
    #[ignore = "reads the live archive and the Alpaca calendar; run deliberately under secretspec"]
    async fn live_a_week_of_daily_bars_is_read_whole_and_reproducibly() {
        let configuration = aws_config::load_from_env().await;
        let archive = Archive::market_data(&configuration).unwrap();
        let alpaca = Alpaca::from_environment(reqwest::Client::new()).unwrap();
        let session =
            |month, day| SessionDate::from_date(NaiveDate::from_ymd_opt(2026, month, day).unwrap());
        let (first, last) = (session(9, 28), session(10, 2));
        let calendar = alpaca.calendar(first, last).await.unwrap();
        let dataset = daily_bars(&archive, &calendar, first, last).await.unwrap();
        let fingerprint = dataset.fingerprint();
        assert!(
            fingerprint.partitions().contains_key(&session(9, 28)),
            "{fingerprint:?}"
        );
        let mut sessions: Vec<SessionDate> = fingerprint.partitions().keys().copied().collect();
        sessions.extend(fingerprint.missing());
        sessions.sort();
        assert_eq!(sessions, calendar.trading_days_in_range(first, last));
        assert_eq!(
            dataset.bars().keys().collect::<Vec<_>>(),
            fingerprint.partitions().keys().collect::<Vec<_>>()
        );
        for (session, bars) in dataset.bars() {
            assert!(bars.len() > 1000, "{session}: {} bars", bars.len());
        }
        let again = daily_bars(&archive, &calendar, first, last).await.unwrap();
        assert_eq!(again.fingerprint(), fingerprint);
        println!(
            "{} sessions read, {} missing, {} bars; {:?}",
            fingerprint.partitions().len(),
            fingerprint.missing().len(),
            dataset.bars().values().map(Vec::len).sum::<usize>(),
            fingerprint.partitions()
        );
    }
}
