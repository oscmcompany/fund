//! Standard errors that respect serial dependence: readings grouped into blocks that move together (by time, by name,
//! or both), each grouping's error taken from its block sums, and the largest well-supported one quoted.

use std::collections::BTreeMap;
use std::num::NonZeroU32;

use chrono::{DateTime, Datelike, Utc};
use serde::{Deserialize, Serialize};
use strum::IntoEnumIterator;

use crate::common::laboratory::READING_BOUND;
use crate::common::laboratory::estimate::share;
use crate::common::laboratory::haircut::DegreesOfFreedom;
use crate::common::laboratory::permutation::Generator;
use crate::common::market::Symbol;
use crate::common::time::SessionDate;

/// The fewest blocks a grouping's error is quoted from; below it the error's own error passes about 16%.
pub const QUOTED_BLOCKS: u64 = 20;

/// Readings stamped with the instant they were taken and the name they were taken on; `None` is unmeasurable.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Observations {
    readings: Vec<(DateTime<Utc>, Symbol, Option<f64>)>,
}

#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum BlocksRefusal {
    /// NaN, or beyond `READING_BOUND`, where sums and squares would stop being finite.
    #[error("the reading for {symbol} at {at} is out of range: {value}")]
    OutOfRange {
        at: DateTime<Utc>,
        symbol: Symbol,
        value: f64,
    },
    #[error("{measured} measured readings cannot carry an error")]
    TooFewMeasured { measured: u64 },
    /// Session and name together has no blocks of its own to relabel.
    #[error("{grouping} has no blocks of its own")]
    NotOneWay { grouping: Grouping },
    #[error("every reading fell in one {grouping} block")]
    OneBlock { grouping: Grouping },
}

impl Observations {
    pub fn new(
        readings: impl IntoIterator<Item = (DateTime<Utc>, Symbol, Option<f64>)>,
    ) -> Result<Self, BlocksRefusal> {
        let readings = readings
            .into_iter()
            .map(|(at, symbol, reading)| match reading {
                Some(value) if value.is_nan() || value.abs() > READING_BOUND => {
                    Err(BlocksRefusal::OutOfRange { at, symbol, value })
                }
                Some(_) | None => Ok((at, symbol, reading)),
            })
            .collect::<Result<_, _>>()?;
        Ok(Self { readings })
    }
}

/// What readings are grouped by: a time bucket, a name, a name within a session, or session and name at once.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    strum::Display,
    strum::EnumString,
    strum::IntoStaticStr,
    strum::EnumIter,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum Grouping {
    Minute,
    FiveMinutes,
    Hour,
    Session,
    /// Shown as a check on dependence across sessions, never quoted, since every hold ends by the close.
    Week,
    Name,
    NameSession,
    /// Session and name together (Cameron, Gelbach and Miller): the session and name variances less their
    /// intersection's.
    SessionAndName,
}

/// The block one reading falls in under a one-way grouping.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Block {
    Bucket(i64),
    Session(SessionDate),
    Week { year: i32, week: u32 },
    Name(Symbol),
    NameSession(Symbol, SessionDate),
}

/// One grouping's error of the coarsening's mean, from its block sums, with the blocks behind it.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "ClusteredFields")]
pub struct Clustered {
    grouping: Grouping,
    standard_error: f64,
    degrees_of_freedom: DegreesOfFreedom,
    blocks: u64,
}

#[derive(Deserialize)]
struct ClusteredFields {
    grouping: Grouping,
    standard_error: f64,
    degrees_of_freedom: DegreesOfFreedom,
    blocks: u64,
}

/// Why a stored grouping or coarsening was refused: it is not one `coarsen` could have produced.
#[derive(Debug, Clone, Copy, PartialEq, thiserror::Error)]
#[error("a stored coarsening is not one coarsen could produce")]
pub struct Inadmissible;

impl TryFrom<ClusteredFields> for Clustered {
    type Error = Inadmissible;

    fn try_from(fields: ClusteredFields) -> Result<Self, Self::Error> {
        let admitted = fields.standard_error.is_finite()
            && fields.standard_error >= 0.0
            && fields.blocks >= 2
            && fields.degrees_of_freedom.value() == (fields.blocks - 1) as f64;
        match admitted {
            true => Ok(Self {
                grouping: fields.grouping,
                standard_error: fields.standard_error,
                degrees_of_freedom: fields.degrees_of_freedom,
                blocks: fields.blocks,
            }),
            false => Err(Inadmissible),
        }
    }
}

impl Clustered {
    pub fn grouping(&self) -> Grouping {
        self.grouping
    }

    pub fn standard_error(&self) -> f64 {
        self.standard_error
    }

    pub fn degrees_of_freedom(&self) -> DegreesOfFreedom {
        self.degrees_of_freedom
    }

    /// For session and name together, the fewer of the two groupings' blocks.
    pub fn blocks(&self) -> u64 {
        self.blocks
    }
}

/// Why one grouping carries no error.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, thiserror::Error)]
#[serde(rename_all = "snake_case")]
pub enum GroupingRefusal {
    /// Every measured reading fell in one block, which leaves no variation between blocks to read.
    #[error("every reading fell in one block")]
    OneBlock,
}

/// Every grouping's error over the same readings, and the grouping the error is quoted at.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "CoarseningFields", into = "CoarseningFields")]
pub struct Coarsening {
    measured: u64,
    undefined: u64,
    mean: f64,
    /// The error as if every reading were independent, which effective sample size is read against.
    independent_standard_error: f64,
    groupings: BTreeMap<Grouping, Result<Clustered, GroupingRefusal>>,
}

#[derive(Serialize, Deserialize)]
struct CoarseningFields {
    measured: u64,
    undefined: u64,
    mean: f64,
    independent_standard_error: f64,
    groupings: BTreeMap<Grouping, GroupingReading>,
}

/// One grouping's reading as journaled, in the snake_case form the journal's other outcomes take.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum GroupingReading {
    Clustered(Clustered),
    Refused { cause: GroupingRefusal },
}

impl From<Coarsening> for CoarseningFields {
    fn from(coarsening: Coarsening) -> Self {
        Self {
            measured: coarsening.measured,
            undefined: coarsening.undefined,
            mean: coarsening.mean,
            independent_standard_error: coarsening.independent_standard_error,
            groupings: coarsening
                .groupings
                .into_iter()
                .map(|(grouping, reading)| {
                    let reading = match reading {
                        Ok(clustered) => GroupingReading::Clustered(clustered),
                        Err(cause) => GroupingReading::Refused { cause },
                    };
                    (grouping, reading)
                })
                .collect(),
        }
    }
}

impl TryFrom<CoarseningFields> for Coarsening {
    type Error = Inadmissible;

    fn try_from(fields: CoarseningFields) -> Result<Self, Self::Error> {
        let groupings: BTreeMap<Grouping, Result<Clustered, GroupingRefusal>> = fields
            .groupings
            .into_iter()
            .map(|(grouping, reading)| match reading {
                GroupingReading::Clustered(clustered) => (grouping, Ok(clustered)),
                GroupingReading::Refused { cause } => (grouping, Err(cause)),
            })
            .collect();
        let keyed = groupings.keys().copied().eq(Grouping::iter())
            && groupings.iter().all(|(grouping, reading)| match reading {
                Ok(clustered) => {
                    clustered.grouping == *grouping && clustered.blocks <= fields.measured
                }
                Err(GroupingRefusal::OneBlock) => true,
            });
        // Session and name stands only on all three of its parts, with the fewer of session's and name's blocks.
        let two_way = keyed
            && match (
                groupings[&Grouping::Session],
                groupings[&Grouping::Name],
                groupings[&Grouping::NameSession],
                groupings[&Grouping::SessionAndName],
            ) {
                (Ok(session), Ok(name), Ok(_), Ok(both)) => {
                    both.blocks == session.blocks.min(name.blocks)
                }
                (Ok(_), Ok(_), Ok(_), Err(_)) => false,
                (Err(_), ..) | (_, Err(_), ..) | (_, _, Err(_), _) => {
                    groupings[&Grouping::SessionAndName].is_err()
                }
            };
        let admitted = two_way
            && fields.measured >= 2
            && fields.mean.is_finite()
            && fields.independent_standard_error.is_finite()
            && fields.independent_standard_error >= 0.0;
        match admitted {
            true => Ok(Self {
                measured: fields.measured,
                undefined: fields.undefined,
                mean: fields.mean,
                independent_standard_error: fields.independent_standard_error,
                groupings,
            }),
            false => Err(Inadmissible),
        }
    }
}

impl Coarsening {
    pub fn measured(&self) -> u64 {
        self.measured
    }

    pub fn undefined(&self) -> u64 {
        self.undefined
    }

    pub fn mean(&self) -> f64 {
        self.mean
    }

    pub fn independent_standard_error(&self) -> f64 {
        self.independent_standard_error
    }

    pub fn groupings(&self) -> &BTreeMap<Grouping, Result<Clustered, GroupingRefusal>> {
        &self.groupings
    }

    /// The grouping with the largest error among those with at least `QUOTED_BLOCKS` blocks, week excepted.
    pub fn quoted(&self) -> Result<Clustered, QuotedRefusal> {
        let quotable = || {
            self.groupings
                .values()
                .filter_map(|clustered| clustered.as_ref().ok())
                .filter(|clustered| clustered.grouping != Grouping::Week)
        };
        quotable()
            .filter(|clustered| clustered.blocks >= QUOTED_BLOCKS)
            .max_by(|left, right| left.standard_error.total_cmp(&right.standard_error))
            .copied()
            .ok_or_else(|| QuotedRefusal::TooFewBlocks {
                // A refused grouping held every reading in one block.
                most_blocks: quotable()
                    .map(|clustered| clustered.blocks)
                    .max()
                    .unwrap_or(1),
            })
    }

    /// The independent readings the quoted error is worth, `measured` scaled by the independent variance over the
    /// quoted one.
    pub fn effective_sample_size(&self) -> Result<f64, EffectiveSampleSizeRefusal> {
        let quoted = self.quoted()?.standard_error;
        match quoted > 0.0 {
            true => Ok(self.measured as f64 * (self.independent_standard_error / quoted).powi(2)),
            false => Err(EffectiveSampleSizeRefusal::ZeroError),
        }
    }
}

/// Why no error is quoted.
#[derive(Debug, Clone, Copy, PartialEq, thiserror::Error)]
pub enum QuotedRefusal {
    #[error("no quotable grouping reaches {QUOTED_BLOCKS} blocks; the most is {most_blocks}")]
    TooFewBlocks { most_blocks: u64 },
}

/// Why no effective sample size is read.
#[derive(Debug, Clone, Copy, PartialEq, thiserror::Error)]
pub enum EffectiveSampleSizeRefusal {
    #[error(transparent)]
    Unquoted(#[from] QuotedRefusal),
    /// Every eligible grouping's block sums vanished, leaving nothing to scale against.
    #[error("the quoted error is zero")]
    ZeroError,
}

/// Every grouping's error over `observations`; refused below two measured readings.
pub fn coarsen(observations: &Observations) -> Result<Coarsening, BlocksRefusal> {
    let measured: Vec<(DateTime<Utc>, &Symbol, f64)> = observations
        .readings
        .iter()
        .filter_map(|(at, symbol, reading)| reading.map(|value| (*at, symbol, value)))
        .collect();
    let count = measured.len() as u64;
    if count < 2 {
        return Err(BlocksRefusal::TooFewMeasured { measured: count });
    }
    // A running mean, which stays exactly on a constant reading where a naive sum drifts and manufactures residuals.
    let mean = measured
        .iter()
        .enumerate()
        .fold(0.0, |mean, (index, (.., value))| {
            mean + (value - mean) / (index + 1) as f64
        });
    let squared_residuals: f64 = measured
        .iter()
        .map(|(.., value)| (value - mean).powi(2))
        .sum();
    let independent_standard_error = (squared_residuals / ((count - 1) * count) as f64).sqrt();
    let one_way = |grouping: Grouping| -> Result<(f64, u64), GroupingRefusal> {
        let mut sums: BTreeMap<Block, f64> = BTreeMap::new();
        for (at, symbol, value) in &measured {
            *sums.entry(block(grouping, *at, symbol)).or_insert(0.0) += value - mean;
        }
        let blocks = sums.len() as u64;
        match blocks {
            0 | 1 => Err(GroupingRefusal::OneBlock),
            _ => {
                let squared_sums: f64 = sums.values().map(|sum| sum * sum).sum();
                let variance =
                    blocks as f64 / (blocks - 1) as f64 * squared_sums / (count as f64).powi(2);
                Ok((variance, blocks))
            }
        }
    };
    let clustered = |grouping: Grouping, (variance, blocks): (f64, u64)| Clustered {
        grouping,
        standard_error: variance.sqrt(),
        degrees_of_freedom: DegreesOfFreedom::new((blocks - 1) as f64)
            .expect("two blocks or more leave at least one degree of freedom"),
        blocks,
    };
    let mut groupings = BTreeMap::new();
    let mut variances = BTreeMap::new();
    for grouping in [
        Grouping::Minute,
        Grouping::FiveMinutes,
        Grouping::Hour,
        Grouping::Session,
        Grouping::Week,
        Grouping::Name,
        Grouping::NameSession,
    ] {
        let reading = one_way(grouping);
        variances.insert(grouping, reading);
        groupings.insert(
            grouping,
            reading.map(|reading| clustered(grouping, reading)),
        );
    }
    let two_way = match (
        variances[&Grouping::Session],
        variances[&Grouping::Name],
        variances[&Grouping::NameSession],
    ) {
        (Ok((session, session_blocks)), Ok((name, name_blocks)), Ok((intersection, _))) => {
            let blocks = session_blocks.min(name_blocks);
            // A negative two-way variance is a small-sample artifact; the larger one-way variance stands in for it.
            let variance = match session + name - intersection {
                measured if measured >= 0.0 => measured,
                _ => session.max(name),
            };
            Ok(clustered(Grouping::SessionAndName, (variance, blocks)))
        }
        (Err(refusal), ..) | (_, Err(refusal), _) | (.., Err(refusal)) => Err(refusal),
    };
    groupings.insert(Grouping::SessionAndName, two_way);
    Ok(Coarsening {
        measured: count,
        undefined: observations.readings.len() as u64 - count,
        mean,
        independent_standard_error,
        groupings,
    })
}

/// The share of sign flips of `grouping`'s block sums whose total lies at least as far from zero as the readings'
/// does, counting the readings' own so it is never zero: the permutation null for a mean, relabelled by block so it
/// keeps the dependence within a block that flipping each reading would break.
pub fn flipped_block_share(
    observations: &Observations,
    grouping: Grouping,
    seed: u64,
    permutations: NonZeroU32,
) -> Result<f64, BlocksRefusal> {
    if grouping == Grouping::SessionAndName {
        return Err(BlocksRefusal::NotOneWay { grouping });
    }
    let mut sums: BTreeMap<Block, f64> = BTreeMap::new();
    let mut measured = 0;
    for (at, symbol, value) in observations
        .readings
        .iter()
        .filter_map(|(at, symbol, reading)| reading.map(|value| (at, symbol, value)))
    {
        *sums.entry(block(grouping, *at, symbol)).or_insert(0.0) += value;
        measured += 1;
    }
    if measured < 2 {
        return Err(BlocksRefusal::TooFewMeasured { measured });
    }
    if sums.len() < 2 {
        return Err(BlocksRefusal::OneBlock { grouping });
    }
    let sums: Vec<f64> = sums.into_values().collect();
    // Summed in block order, so a relabelling that reproduces the readings, or negates them, ties them exactly.
    let observed = sums.iter().sum::<f64>().abs();
    let mut generator = Generator::new(seed);
    let count = (0..permutations.get())
        .filter(|_| {
            let flipped: f64 = sums
                .iter()
                .map(|sum| match generator.coin() {
                    true => -sum,
                    false => *sum,
                })
                .sum();
            flipped.abs() >= observed
        })
        .count();
    Ok(share(count, permutations))
}

fn block(grouping: Grouping, at: DateTime<Utc>, symbol: &Symbol) -> Block {
    let bucket = |seconds: i64| Block::Bucket(at.timestamp().div_euclid(seconds));
    match grouping {
        Grouping::Minute => bucket(60),
        Grouping::FiveMinutes => bucket(300),
        Grouping::Hour => bucket(3600),
        Grouping::Session => Block::Session(SessionDate::at(at)),
        Grouping::Week => {
            let week = SessionDate::at(at).date().iso_week();
            Block::Week {
                year: week.year(),
                week: week.week(),
            }
        }
        Grouping::Name => Block::Name(symbol.clone()),
        Grouping::NameSession | Grouping::SessionAndName => {
            Block::NameSession(symbol.clone(), SessionDate::at(at))
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use proptest::prelude::*;
    use strum::IntoEnumIterator;

    use super::*;
    use crate::common::laboratory::estimate::{Estimate, summarize};
    use crate::common::laboratory::series::Series;

    /// 15:00 UTC on 2026-03-02 plus `minutes`, inside the regular session.
    fn at(minutes: i64) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 3, 2, 15, 0, 0).unwrap() + chrono::Duration::minutes(minutes)
    }

    fn symbol(index: usize) -> Symbol {
        let letters: String = format!("{index:03}")
            .bytes()
            .map(|digit| char::from(b'A' + digit - b'0'))
            .collect();
        Symbol::new(&letters).unwrap()
    }

    #[test]
    fn test_grouping_names_agree_between_display_and_parse() {
        for grouping in Grouping::iter() {
            assert_eq!(grouping.to_string().parse::<Grouping>(), Ok(grouping));
        }
        assert_eq!(Grouping::SessionAndName.to_string(), "session_and_name");
    }

    /// Two sessions of two names: hand-worked block sums for the session grouping, and a block that holds everything
    /// refused rather than read as zero.
    #[test]
    fn test_a_session_error_comes_from_its_block_sums() {
        let day = chrono::Duration::days(1);
        let observations = Observations::new([
            (at(0), symbol(0), Some(1.0)),
            (at(1), symbol(1), Some(3.0)),
            (at(0) + day, symbol(0), Some(5.0)),
            (at(1) + day, symbol(1), Some(7.0)),
            (at(2), symbol(1), None),
        ])
        .unwrap();
        let coarsening = coarsen(&observations).unwrap();
        assert_eq!((coarsening.measured(), coarsening.undefined()), (4, 1));
        assert_eq!(coarsening.mean(), 4.0);
        // Session residual sums -4 and +4: 2 / 1 * 32 / 16 = 4.
        let session = coarsening.groupings()[&Grouping::Session].unwrap();
        assert_eq!(
            (
                session.standard_error(),
                session.blocks(),
                session.degrees_of_freedom().value()
            ),
            (2.0, 2, 1.0)
        );
        // Name residual sums -2 and +2: 2 * 8 / 16 = 1.
        assert_eq!(
            coarsening.groupings()[&Grouping::Name]
                .unwrap()
                .standard_error(),
            1.0
        );
        assert_eq!(
            coarsening.groupings()[&Grouping::Week],
            Err(GroupingRefusal::OneBlock)
        );
        // Two blocks is far below the floor, so nothing is quoted and no effective size is claimed.
        assert_eq!(
            coarsening.quoted(),
            Err(QuotedRefusal::TooFewBlocks { most_blocks: 4 })
        );
        assert_eq!(
            coarsening.effective_sample_size(),
            Err(EffectiveSampleSizeRefusal::Unquoted(
                QuotedRefusal::TooFewBlocks { most_blocks: 4 }
            ))
        );
    }

    /// Sub-hour buckets split on UTC boundaries; a week is the ISO week of the Eastern session, not of the UTC date.
    #[test]
    fn test_blocks_split_at_their_boundaries() {
        let instant = |month, day, hour, minute, second| {
            Utc.with_ymd_and_hms(2026, month, day, hour, minute, second)
                .unwrap()
        };
        let name = symbol(0);
        let same =
            |grouping, left, right| block(grouping, left, &name) == block(grouping, right, &name);
        assert!(same(
            Grouping::Minute,
            instant(3, 2, 15, 0, 0),
            instant(3, 2, 15, 0, 59)
        ));
        assert!(!same(
            Grouping::Minute,
            instant(3, 2, 15, 0, 59),
            instant(3, 2, 15, 1, 0)
        ));
        assert!(same(
            Grouping::FiveMinutes,
            instant(3, 2, 15, 0, 0),
            instant(3, 2, 15, 4, 59)
        ));
        assert!(!same(
            Grouping::FiveMinutes,
            instant(3, 2, 15, 4, 59),
            instant(3, 2, 15, 5, 0)
        ));
        assert!(same(
            Grouping::Hour,
            instant(3, 2, 15, 0, 0),
            instant(3, 2, 15, 59, 59)
        ));
        assert!(!same(
            Grouping::Hour,
            instant(3, 2, 15, 59, 59),
            instant(3, 2, 16, 0, 0)
        ));
        // 04:59 UTC on 2026-03-03 is still the evening of 2026-03-02 in New York.
        assert!(same(
            Grouping::Session,
            instant(3, 2, 15, 0, 0),
            instant(3, 3, 4, 59, 0)
        ));
        assert_eq!(
            block(Grouping::Week, instant(3, 2, 3, 0, 0), &name),
            Block::Week {
                year: 2026,
                week: 9
            }
        );
        assert_eq!(
            block(Grouping::Week, instant(3, 2, 5, 0, 0), &name),
            Block::Week {
                year: 2026,
                week: 10
            }
        );
        assert_eq!(
            block(
                Grouping::Week,
                Utc.with_ymd_and_hms(2027, 1, 1, 15, 0, 0).unwrap(),
                &name
            ),
            Block::Week {
                year: 2026,
                week: 53
            }
        );
    }

    /// Two sessions of two names, hand-worked: session 4, name 1 and name-session 5/3 give a two-way variance of 10/3;
    /// and readings whose intersection outweighs both one-way variances fall back to the larger one, 1/4.
    #[test]
    fn test_two_way_combines_session_and_name_and_falls_back_when_negative() {
        let day = chrono::Duration::days(1);
        let grid = |values: [f64; 4]| {
            let observations = Observations::new([
                (at(0), symbol(0), Some(values[0])),
                (at(1), symbol(1), Some(values[1])),
                (at(0) + day, symbol(0), Some(values[2])),
                (at(1) + day, symbol(1), Some(values[3])),
            ])
            .unwrap();
            coarsen(&observations).unwrap().groupings()[&Grouping::SessionAndName].unwrap()
        };
        let combined = grid([1.0, 3.0, 5.0, 7.0]);
        assert!((combined.standard_error() - 1.825_741_858_350_553_8).abs() < 1e-12);
        assert_eq!(
            (combined.blocks(), combined.degrees_of_freedom().value()),
            (2, 1.0)
        );
        // Session sums 0 and 0, name sums 1 and -1, cells 4, 4, 1 and 1: 0 + 1/4 - 5/6 is negative.
        assert_eq!(grid([2.0, -2.0, -1.0, 1.0]).standard_error(), 0.5);
    }

    /// 300 readings of 0.1 over 30 sessions: a naive sum lands the mean off 0.1 and manufactures residuals, where
    /// identical readings must give every grouping a zero error.
    #[test]
    fn test_constant_readings_give_zero_error() {
        let day = chrono::Duration::days(1);
        let observations = Observations::new((0..300).map(|index| {
            (
                at(index as i64 % 10) + day * (index as i32 / 10),
                symbol(index % 25),
                Some(0.1),
            )
        }))
        .unwrap();
        let coarsening = coarsen(&observations).unwrap();
        assert_eq!(
            (coarsening.mean(), coarsening.independent_standard_error()),
            (0.1, 0.0)
        );
        assert_eq!(coarsening.groupings().len(), 8);
        for (grouping, clustered) in coarsening.groupings() {
            assert_eq!(
                clustered.map(|clustered| clustered.standard_error()),
                Ok(0.0),
                "{grouping:?}"
            );
        }
        assert_eq!(
            coarsening.effective_sample_size(),
            Err(EffectiveSampleSizeRefusal::ZeroError)
        );
    }

    /// 20 sessions of +1 and -1 on two names fresh to each session: the session sums vanish and the name and
    /// name-session variances cancel exactly, so the two-way variance is a measured zero, not a fallback.
    #[test]
    fn test_a_zero_two_way_variance_stands() {
        let day = chrono::Duration::days(1);
        let observations = Observations::new((0..40).map(|index| {
            (
                at(index as i64 % 2) + day * (index as i32 / 2),
                symbol(index),
                Some([1.0, -1.0][index % 2]),
            )
        }))
        .unwrap();
        let coarsening = coarsen(&observations).unwrap();
        let two_way = coarsening.groupings()[&Grouping::SessionAndName].unwrap();
        assert_eq!((two_way.standard_error(), two_way.blocks()), (0.0, 20));
        assert!(
            coarsening.groupings()[&Grouping::Name]
                .unwrap()
                .standard_error()
                > 0.0
        );
    }

    /// The quoting floor is 20 blocks: a grouping of 19 is refused and one of 20 is quoted.
    #[test]
    fn test_quoting_needs_twenty_blocks() {
        let day = chrono::Duration::days(1);
        let sessions = |count: usize| {
            Observations::new((0..count * 2).map(|index| {
                (
                    at(0) + day * (index as i32 / 2),
                    symbol(0),
                    Some(index as f64 % 3.0),
                )
            }))
            .unwrap()
        };
        assert_eq!(
            coarsen(&sessions(19)).unwrap().quoted(),
            Err(QuotedRefusal::TooFewBlocks { most_blocks: 19 })
        );
        // One name, so name-session ties session; either may be quoted.
        let twenty = coarsen(&sessions(20)).unwrap();
        let quoted = twenty.quoted().unwrap();
        let session = twenty.groupings()[&Grouping::Session].unwrap();
        assert_eq!(
            (quoted.blocks(), quoted.standard_error()),
            (20, session.standard_error())
        );
    }

    #[test]
    fn test_fewer_than_two_measured_readings_are_refused() {
        let one =
            Observations::new([(at(0), symbol(0), Some(1.0)), (at(1), symbol(0), None)]).unwrap();
        assert_eq!(
            coarsen(&one),
            Err(BlocksRefusal::TooFewMeasured { measured: 1 })
        );
        assert!(matches!(
            Observations::new([(at(0), symbol(0), Some(f64::NAN))]),
            Err(BlocksRefusal::OutOfRange { .. })
        ));
    }

    /// A shock shared by every reading on a session must widen the quoted error past the independent one, and the
    /// same readings with the shock removed must not: the instrument sees dependence that is there.
    #[test]
    fn test_a_shared_session_shock_widens_the_quoted_error() {
        let day = chrono::Duration::days(1);
        let build = |shocked: bool| {
            Observations::new((0..30).flat_map(|session| {
                let shock = if shocked {
                    [3.0, -3.0][session % 2]
                } else {
                    0.0
                };
                (0..10).map(move |index| {
                    let noise = [0.5, -0.5][(index + session) % 2];
                    (
                        at(index as i64 * 7) + day * session as i32,
                        symbol(index + 10 * (session % 3)),
                        Some(shock + noise),
                    )
                })
            }))
            .unwrap()
        };
        let shocked = coarsen(&build(true)).unwrap();
        let quoted = shocked.quoted().unwrap();
        assert_eq!(
            (shocked.measured(), quoted.grouping(), quoted.blocks()),
            (300, Grouping::Session, 30)
        );
        assert!(quoted.standard_error() > 2.0 * shocked.independent_standard_error());
        assert!(shocked.effective_sample_size().unwrap() < 75.0);
        let calm = coarsen(&build(false)).unwrap();
        assert!(calm.quoted().unwrap().standard_error() <= 1.1 * calm.independent_standard_error());
    }

    /// Sessions whose readings share a shock, 20 up and 10 down: flipping each reading reads the 300 readings as
    /// overwhelming evidence, while flipping each session's sum reads 30 pieces of it, so the block null must be the
    /// wider of the two.
    #[test]
    fn test_flipping_blocks_keeps_the_dependence_flipping_readings_breaks() {
        let day = chrono::Duration::days(1);
        let observations = Observations::new((0..30).flat_map(|session| {
            let shock = if session < 20 { 3.0 } else { -3.0 };
            (0..10).map(move |index| {
                (
                    at(index as i64 * 7) + day * session as i32,
                    symbol(index),
                    Some(shock + [0.5, -0.5][(index + session) % 2]),
                )
            })
        }))
        .unwrap();
        let permutations = NonZeroU32::new(999).unwrap();
        let readings =
            flipped_block_share(&observations, Grouping::Minute, 7, permutations).unwrap();
        let sessions =
            flipped_block_share(&observations, Grouping::Session, 7, permutations).unwrap();
        assert!(readings <= 0.005, "{readings}");
        assert!(sessions >= 0.03, "{sessions}");
    }

    #[test]
    fn test_a_block_null_refuses_two_way_and_a_single_block() {
        let observations =
            Observations::new([(at(0), symbol(0), Some(1.0)), (at(1), symbol(1), Some(2.0))])
                .unwrap();
        let permutations = NonZeroU32::new(9).unwrap();
        assert_eq!(
            flipped_block_share(&observations, Grouping::SessionAndName, 1, permutations),
            Err(BlocksRefusal::NotOneWay {
                grouping: Grouping::SessionAndName
            })
        );
        assert_eq!(
            flipped_block_share(&observations, Grouping::Session, 1, permutations),
            Err(BlocksRefusal::OneBlock {
                grouping: Grouping::Session
            })
        );
    }

    /// A coarsening reads back as written, and one misfiling, missing or contradicting a grouping is refused.
    #[test]
    fn test_a_coarsening_round_trips_and_refuses_what_coarsen_could_not_produce() {
        let day = chrono::Duration::days(1);
        let coarsening = coarsen(
            &Observations::new([
                (at(0), symbol(0), Some(1.0)),
                (at(1), symbol(1), Some(3.0)),
                (at(0) + day, symbol(0), Some(5.0)),
                (at(1) + day, symbol(1), Some(7.0)),
            ])
            .unwrap(),
        )
        .unwrap();
        let encoded = serde_json::to_value(&coarsening).unwrap();
        assert_eq!(
            encoded["groupings"]["session"],
            serde_json::json!({"clustered": {"grouping": "session", "standard_error": 2.0, "degrees_of_freedom": 1.0, "blocks": 2}})
        );
        assert_eq!(
            encoded["groupings"]["week"],
            serde_json::json!({"refused": {"cause": "one_block"}})
        );
        assert_eq!(
            serde_json::from_value::<Coarsening>(encoded.clone()).unwrap(),
            coarsening
        );
        let mut misfiled = encoded.clone();
        misfiled["groupings"]["session"]["clustered"]["grouping"] = serde_json::json!("name");
        assert!(serde_json::from_value::<Coarsening>(misfiled).is_err());
        let mut unsupported = encoded.clone();
        unsupported["groupings"]["session_and_name"]["clustered"]["blocks"] = serde_json::json!(3);
        unsupported["groupings"]["session_and_name"]["clustered"]["degrees_of_freedom"] =
            serde_json::json!(2.0);
        assert!(serde_json::from_value::<Coarsening>(unsupported).is_err());
        let mut orphaned = encoded.clone();
        orphaned["groupings"]["name"] = serde_json::json!({"refused": {"cause": "one_block"}});
        assert!(serde_json::from_value::<Coarsening>(orphaned).is_err());
        let mut missing = encoded;
        missing["groupings"].as_object_mut().unwrap().remove("hour");
        assert!(serde_json::from_value::<Coarsening>(missing).is_err());
    }

    fn readings() -> impl Strategy<Value = Vec<f64>> {
        prop::collection::vec(-100.0..100.0f64, 2..60)
    }

    proptest! {
        /// Every reading in its own session: flipping session blocks is flipping readings, so the block null agrees
        /// exactly with `flipped_share` over the same readings as a series.
        #[test]
        fn test_one_reading_per_block_matches_the_series_null(values in readings(), seed in 0u64..1000) {
            let observations = Observations::new(values.iter().enumerate().map(|(index, value)| {
                (at(0) + chrono::Duration::days(index as i64), symbol(0), Some(*value))
            }))
            .unwrap();
            let sessions = Series::new(values.iter().enumerate().map(|(index, value)| {
                (SessionDate::at(at(0) + chrono::Duration::days(index as i64)), Some(*value))
            }))
            .unwrap();
            let permutations = NonZeroU32::new(99).unwrap();
            prop_assert_eq!(
                flipped_block_share(&observations, Grouping::Session, seed, permutations).unwrap(),
                crate::common::laboratory::estimate::flipped_share(&sessions, seed, permutations).unwrap()
            );
        }

        /// Every reading on its own minute and its own name: both groupings are the independent error, which is
        /// what `Estimate` computes from the same readings as sessions.
        #[test]
        fn test_independent_readings_give_the_textbook_error(values in readings()) {
            let observations = Observations::new(
                values.iter().enumerate().map(|(index, value)| (at(index as i64), symbol(index), Some(*value))),
            )
            .unwrap();
            let coarsening = coarsen(&observations).unwrap();
            let sessions = Series::new(values.iter().enumerate().map(|(index, value)| {
                (SessionDate::at(at(0)).plus_calendar_days(index as i64), Some(*value))
            }))
            .unwrap();
            let estimate = Estimate::from_summary(summarize(&sessions)).unwrap();
            for grouping in [Grouping::Minute, Grouping::Name] {
                let clustered = coarsening.groupings()[&grouping].unwrap();
                prop_assert!((clustered.standard_error() - estimate.standard_error()).abs() <= 1e-9 * (1.0 + estimate.standard_error()));
                prop_assert_eq!(clustered.blocks(), values.len() as u64);
            }
            prop_assert!((coarsening.independent_standard_error() - estimate.standard_error()).abs() <= 1e-9 * (1.0 + estimate.standard_error()));
        }

        /// Every grouping is reported, and the quoted error is the largest among groupings with enough
        /// blocks, week excepted.
        #[test]
        fn test_the_quoted_error_is_the_largest_well_supported_one(
            values in prop::collection::vec(-100.0..100.0f64, 2..200),
            names in 1usize..40,
            spacing in 1i64..600,
        ) {
            let observations = Observations::new(values.iter().enumerate().map(|(index, value)| {
                (at(index as i64 * spacing), symbol(index % names), Some(*value))
            }))
            .unwrap();
            let coarsening = coarsen(&observations).unwrap();
            prop_assert_eq!(coarsening.groupings().keys().copied().collect::<Vec<_>>(), Grouping::iter().collect::<Vec<_>>());
            let eligible: Vec<Clustered> = coarsening
                .groupings()
                .values()
                .filter_map(|clustered| clustered.as_ref().ok().copied())
                .filter(|clustered| clustered.grouping() != Grouping::Week && clustered.blocks() >= 20)
                .collect();
            match coarsening.quoted() {
                Ok(quoted) => {
                    prop_assert!(eligible.iter().all(|clustered| clustered.standard_error() <= quoted.standard_error()));
                    prop_assert!(eligible.contains(&quoted));
                }
                Err(QuotedRefusal::TooFewBlocks { most_blocks }) => {
                    prop_assert!(eligible.is_empty());
                    prop_assert!(most_blocks < 20);
                }
            }
        }

        /// A name traded on one session only: its name and name-session blocks coincide, so the two-way variance is
        /// the session variance.
        #[test]
        fn test_two_way_is_the_session_error_when_names_never_repeat_across_sessions(
            values in prop::collection::vec(-100.0..100.0f64, 4..80),
        ) {
            let day = chrono::Duration::days(1);
            let observations = Observations::new(values.iter().enumerate().map(|(index, value)| {
                (at(index as i64 % 7) + day * (index as i32 % 3), symbol(index), Some(*value))
            }))
            .unwrap();
            let coarsening = coarsen(&observations).unwrap();
            match (coarsening.groupings()[&Grouping::Session], coarsening.groupings()[&Grouping::SessionAndName]) {
                (Ok(session), Ok(two_way)) => {
                    prop_assert!((session.standard_error() - two_way.standard_error()).abs() <= 1e-9 * (1.0 + session.standard_error()));
                }
                (session, two_way) => prop_assert_eq!(session.map(|_| ()), two_way.map(|_| ())),
            }
        }
    }
}
