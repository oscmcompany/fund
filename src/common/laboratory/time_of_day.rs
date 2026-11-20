//! Intraday volatility's shape through the session, measured from our own intraday bars, so a move is read against
//! its own time of day's volatility rather than the session's average.

use std::collections::BTreeMap;

use chrono::{DateTime, NaiveTime, Utc};

use super::volatility::{Measurement, mean_with_error};
use crate::common::market::Symbol;
use crate::common::market::record::{Bar, BarInterval};
use crate::common::time::{SessionDate, eastern_time};

/// Each bucket's volatility relative to the session's, keyed by the Eastern wall-clock start of the bar it holds; the
/// measured factors average to one.
#[derive(Debug, Clone, PartialEq)]
pub struct TimeOfDayCurve {
    interval: BarInterval,
    buckets: BTreeMap<NaiveTime, Result<Measurement, BucketRefusal>>,
    too_short_sessions: u64,
    still_sessions: u64,
    gap_returns: u64,
}

/// Why the curve was not measured, with what caused it.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum CurveRefusal {
    #[error("a {interval} bar cannot be placed within a session")]
    NotIntraday { interval: BarInterval },
    #[error("a {found} bar is among {expected} bars")]
    MixedIntervals {
        expected: BarInterval,
        found: BarInterval,
    },
    #[error("a bar for {found} is among bars for {expected}")]
    MixedSymbols { expected: Symbol, found: Symbol },
    #[error("a bar from {found} is among bars from {expected}")]
    MixedSessions {
        expected: SessionDate,
        found: SessionDate,
    },
    #[error("the bar at {next} does not follow the bar at {previous}")]
    Unordered {
        previous: DateTime<Utc>,
        next: DateTime<Utc>,
    },
    /// No bucket read on at least two sessions ever moved.
    #[error("no bucket was measured over {sessions} sessions")]
    NothingMeasured { sessions: u64 },
}

/// Why a bucket carries no factor, with what caused it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum BucketRefusal {
    #[error("the {bucket} bucket was read on {observations} sessions, fewer than two")]
    Unmeasured {
        bucket: NaiveTime,
        observations: u64,
    },
    /// Read, but never moved, so nothing can be divided by it.
    #[error("the {bucket} bucket never moved")]
    Still { bucket: NaiveTime },
}

impl TimeOfDayCurve {
    /// The curve over `sessions`, each one name's intraday bars over one session in time order.
    ///
    /// A bar's return runs from the previous bar's close, the first bar's from its own open; each session's squared
    /// returns are divided by their own mean, so a volatile name does not set the shape. A return across a missing bar
    /// spans more than one bucket, so it is counted and left out. A session of fewer than two
    /// bars, or one that never moved, is counted and left out.
    pub fn measure<'a>(
        interval: BarInterval,
        sessions: impl IntoIterator<Item = &'a [Bar]>,
    ) -> Result<Self, CurveRefusal> {
        match interval {
            BarInterval::OneMinute | BarInterval::FiveMinute => {}
            BarInterval::OneDay => return Err(CurveRefusal::NotIntraday { interval }),
        }
        let mut readings = BTreeMap::<NaiveTime, Vec<f64>>::new();
        let (mut read, mut too_short_sessions, mut still_sessions, mut gap_returns) =
            (0_u64, 0_u64, 0_u64, 0_u64);
        for bars in sessions {
            read += 1;
            check(interval, bars)?;
            if bars.len() < 2 {
                too_short_sessions += 1;
                continue;
            }
            let squares = squared_returns(interval, bars);
            let kept = squares.iter().flatten().collect::<Vec<_>>();
            let mean = kept.iter().copied().sum::<f64>() / kept.len() as f64;
            if mean <= 0.0 {
                still_sessions += 1;
                continue;
            }
            gap_returns += (squares.len() - kept.len()) as u64;
            let measured = bars
                .iter()
                .zip(squares)
                .filter_map(|(bar, square)| square.map(|square| (bar, square)));
            for (bar, square) in measured {
                readings
                    .entry(eastern_time(bar.timestamp()))
                    .or_default()
                    .push(square / mean);
            }
        }
        let variances = readings
            .into_iter()
            .map(|(bucket, values)| {
                let observations = values.len() as u64;
                let reading = match values.len() {
                    0 | 1 => Err(BucketRefusal::Unmeasured {
                        bucket,
                        observations,
                    }),
                    _ => Ok(mean_with_error(&values)),
                };
                (bucket, reading, observations)
            })
            .collect::<Vec<_>>();
        let roots = variances
            .iter()
            .filter_map(|(_, reading, _)| reading.as_ref().ok().map(|(mean, _)| mean.sqrt()))
            .collect::<Vec<_>>();
        let normaliser = match roots.is_empty() {
            true => 0.0,
            false => roots.iter().sum::<f64>() / roots.len() as f64,
        };
        if normaliser <= 0.0 {
            return Err(CurveRefusal::NothingMeasured { sessions: read });
        }
        let buckets = variances
            .into_iter()
            .map(|(bucket, reading, observations)| {
                let factor = reading.map(|(variance, variance_error)| {
                    let root = variance.sqrt();
                    // The delta method's error is undefined at zero, where every reading was zero.
                    let root_error = match root > 0.0 {
                        true => variance_error / (2.0 * root),
                        false => 0.0,
                    };
                    Measurement::new(root / normaliser, root_error / normaliser, observations)
                });
                (bucket, factor)
            })
            .collect();
        Ok(Self {
            interval,
            buckets,
            too_short_sessions,
            still_sessions,
            gap_returns,
        })
    }

    /// The factor for the bar `instant` falls in; its error leaves out the error of the normaliser.
    pub fn factor(&self, instant: DateTime<Utc>) -> Result<Measurement, BucketRefusal> {
        let bucket = eastern_time(self.interval.bucket(instant));
        self.buckets
            .get(&bucket)
            .copied()
            .unwrap_or(Err(BucketRefusal::Unmeasured {
                bucket,
                observations: 0,
            }))
    }

    /// A log return over the bar at `instant` divided by its bucket's factor, so returns from any hour compare.
    pub fn deseasonalize(
        &self,
        log_return: f64,
        instant: DateTime<Utc>,
    ) -> Result<f64, BucketRefusal> {
        let factor = self.factor(instant)?;
        match factor.value() > 0.0 {
            true => Ok(log_return / factor.value()),
            false => Err(BucketRefusal::Still {
                bucket: eastern_time(self.interval.bucket(instant)),
            }),
        }
    }

    /// Every bucket read, with its factor or why it has none.
    pub fn buckets(&self) -> &BTreeMap<NaiveTime, Result<Measurement, BucketRefusal>> {
        &self.buckets
    }

    pub fn too_short_sessions(&self) -> u64 {
        self.too_short_sessions
    }

    pub fn still_sessions(&self) -> u64 {
        self.still_sessions
    }

    /// Returns left out because the bar before them was missing; counted only in sessions that moved.
    pub fn gap_returns(&self) -> u64 {
        self.gap_returns
    }
}

fn check(interval: BarInterval, bars: &[Bar]) -> Result<(), CurveRefusal> {
    let Some(first) = bars.first() else {
        return Ok(());
    };
    let session = SessionDate::at(first.timestamp());
    for bar in bars {
        if bar.interval() != interval {
            return Err(CurveRefusal::MixedIntervals {
                expected: interval,
                found: bar.interval(),
            });
        }
        if bar.symbol() != first.symbol() {
            return Err(CurveRefusal::MixedSymbols {
                expected: first.symbol().clone(),
                found: bar.symbol().clone(),
            });
        }
        if SessionDate::at(bar.timestamp()) != session {
            return Err(CurveRefusal::MixedSessions {
                expected: session,
                found: SessionDate::at(bar.timestamp()),
            });
        }
    }
    match bars
        .windows(2)
        .find(|pair| pair[0].timestamp() >= pair[1].timestamp())
    {
        Some(pair) => Err(CurveRefusal::Unordered {
            previous: pair[0].timestamp(),
            next: pair[1].timestamp(),
        }),
        None => Ok(()),
    }
}

/// Each bar's squared log return from the previous close, the first bar's from its own open; `None` where the
/// previous bar is not the one immediately before it.
fn squared_returns(interval: BarInterval, bars: &[Bar]) -> Vec<Option<f64>> {
    let logarithm = |bar: &Bar| (bar.prices().close().ticks() as f64).ln();
    let first = &bars[0];
    std::iter::once(Some(
        logarithm(first) - (first.prices().open().ticks() as f64).ln(),
    ))
    .chain(bars.windows(2).map(|pair| {
        (interval.ends(pair[0].timestamp()) == pair[1].timestamp())
            .then(|| logarithm(&pair[1]) - logarithm(&pair[0]))
    }))
    .map(|log_return| log_return.map(|log_return| log_return.powi(2)))
    .collect()
}

#[cfg(test)]
mod tests {
    use chrono::{TimeDelta, TimeZone};
    use chrono_tz::America::New_York;
    use proptest::prelude::*;

    use super::*;
    use crate::common::laboratory::volatility::tests::{bar, close};

    fn eastern(month: u32, day: u32, hour: u32, minute: u32) -> DateTime<Utc> {
        New_York
            .with_ymd_and_hms(2026, month, day, hour, minute, 0)
            .unwrap()
            .with_timezone(&Utc)
    }

    fn time(hour: u32, minute: u32) -> NaiveTime {
        NaiveTime::from_hms_opt(hour, minute, 0).unwrap()
    }

    /// Five-minute bars from 09:30 Eastern, each opening at the previous close and wicking nowhere.
    fn session(raw: &str, start: DateTime<Utc>, open: i64, closes: &[i64]) -> Vec<Bar> {
        let mut previous = open;
        closes
            .iter()
            .enumerate()
            .map(|(index, close)| {
                let ticks = [previous, previous.max(*close), previous.min(*close), *close];
                previous = *close;
                bar(
                    raw,
                    BarInterval::FiveMinute,
                    start + TimeDelta::minutes(5 * index as i64),
                    ticks,
                )
            })
            .collect()
    }

    /// A winter session and a summer one, so a bucket keyed by a fixed offset would split 09:30 in two.
    fn hand() -> Vec<Vec<Bar>> {
        vec![
            session(
                "AAA",
                eastern(3, 2, 9, 30),
                100_000_000,
                &[101_000_000, 100_000_000, 100_500_000],
            ),
            session(
                "BBB",
                eastern(7, 1, 9, 30),
                50_000_000,
                &[50_500_000, 50_500_000, 50_000_000],
            ),
        ]
    }

    #[test]
    fn test_a_hand_curve_reads_its_literal_factors() {
        let sessions = hand();
        let curve =
            TimeOfDayCurve::measure(BarInterval::FiveMinute, sessions.iter().map(Vec::as_slice))
                .unwrap();
        assert_eq!(
            curve.buckets().keys().copied().collect::<Vec<_>>(),
            vec![time(9, 30), time(9, 35), time(9, 40)]
        );
        for (instant, value, standard_error) in [
            (
                eastern(3, 2, 9, 30),
                1.2044701070209274,
                0.03559162900319171,
            ),
            (eastern(7, 1, 9, 37), 0.826138648266124, 0.413069324133062),
            (eastern(3, 2, 9, 44), 0.9693912447129485, 0.307805008052576),
        ] {
            let factor = curve.factor(instant).unwrap();
            assert!(
                close(factor.value(), value),
                "{instant}: {}",
                factor.value()
            );
            assert!(close(factor.standard_error(), standard_error));
            assert_eq!(factor.observations(), 2);
        }
        assert!(close(
            curve.deseasonalize(0.01, eastern(3, 3, 9, 30)).unwrap(),
            0.01 / 1.2044701070209274
        ));
        assert_eq!(
            curve.factor(eastern(3, 2, 10, 0)),
            Err(BucketRefusal::Unmeasured {
                bucket: time(10, 0),
                observations: 0
            })
        );
    }

    /// A 09:50 bar after a missing 09:45 one spans two buckets, so it is skipped and the other buckets read as before.
    #[test]
    fn test_a_return_across_a_missing_bar_is_skipped() {
        let mut sessions = hand();
        for (bars, extension) in sessions
            .iter_mut()
            .zip([[120_000_000, 90_000_000], [70_000_000, 40_000_000]])
        {
            let last = bars.last().unwrap();
            let (raw, start, open) = (
                last.symbol().as_str().to_string(),
                last.timestamp() + TimeDelta::minutes(5),
                last.prices().close().ticks(),
            );
            let mut tail = session(&raw, start, open, &extension);
            tail.remove(0);
            bars.extend(tail);
        }
        assert_eq!(
            sessions[0].last().unwrap().timestamp(),
            eastern(3, 2, 9, 50)
        );
        let curve =
            TimeOfDayCurve::measure(BarInterval::FiveMinute, sessions.iter().map(Vec::as_slice))
                .unwrap();
        assert_eq!(curve.gap_returns(), 2);
        assert_eq!(
            curve.buckets().keys().copied().collect::<Vec<_>>(),
            vec![time(9, 30), time(9, 35), time(9, 40)]
        );
        for (instant, value) in [
            (eastern(3, 2, 9, 30), 1.2044701070209274),
            (eastern(3, 2, 9, 35), 0.826138648266124),
            (eastern(3, 2, 9, 40), 0.9693912447129485),
        ] {
            assert!(close(curve.factor(instant).unwrap().value(), value));
        }
    }

    #[test]
    fn test_short_and_still_sessions_are_counted_and_left_out() {
        let mut sessions = hand();
        sessions.push(session(
            "CCC",
            eastern(3, 4, 9, 30),
            10_000_000,
            &[10_000_000; 4],
        ));
        sessions.push(session(
            "CCC",
            eastern(3, 5, 9, 30),
            10_000_000,
            &[11_000_000],
        ));
        sessions.push(Vec::new());
        let curve =
            TimeOfDayCurve::measure(BarInterval::FiveMinute, sessions.iter().map(Vec::as_slice))
                .unwrap();
        assert_eq!((curve.too_short_sessions(), curve.still_sessions()), (2, 1));
        assert_eq!(
            curve.factor(eastern(3, 4, 9, 45)),
            Err(BucketRefusal::Unmeasured {
                bucket: time(9, 45),
                observations: 0
            })
        );
        assert_eq!(
            curve.factor(eastern(3, 4, 9, 30)).unwrap().observations(),
            2
        );
    }

    #[test]
    fn test_a_curve_refuses_what_it_cannot_place() {
        let measure =
            |interval, bars: Vec<Bar>| TimeOfDayCurve::measure(interval, [bars.as_slice()]);
        let morning = session("AAA", eastern(3, 2, 9, 30), 100_000_000, &[101_000_000; 2]);
        assert_eq!(
            measure(BarInterval::OneDay, morning.clone()),
            Err(CurveRefusal::NotIntraday {
                interval: BarInterval::OneDay
            })
        );
        assert_eq!(
            measure(BarInterval::OneMinute, morning.clone()),
            Err(CurveRefusal::MixedIntervals {
                expected: BarInterval::OneMinute,
                found: BarInterval::FiveMinute
            })
        );
        let reversed = morning.iter().rev().cloned().collect();
        assert_eq!(
            measure(BarInterval::FiveMinute, reversed),
            Err(CurveRefusal::Unordered {
                previous: eastern(3, 2, 9, 35),
                next: eastern(3, 2, 9, 30)
            })
        );
        let mut mixed = morning.clone();
        mixed.extend(session(
            "BBB",
            eastern(3, 2, 9, 40),
            1_000_000,
            &[1_000_000],
        ));
        assert_eq!(
            measure(BarInterval::FiveMinute, mixed),
            Err(CurveRefusal::MixedSymbols {
                expected: Symbol::new("AAA").unwrap(),
                found: Symbol::new("BBB").unwrap()
            })
        );
        let mut two_days = morning.clone();
        two_days.extend(session(
            "AAA",
            eastern(3, 3, 9, 30),
            1_000_000,
            &[1_000_000],
        ));
        assert_eq!(
            measure(BarInterval::FiveMinute, two_days),
            Err(CurveRefusal::MixedSessions {
                expected: SessionDate::at(eastern(3, 2, 12, 0)),
                found: SessionDate::at(eastern(3, 3, 12, 0))
            })
        );
        assert_eq!(
            measure(BarInterval::FiveMinute, morning[..1].to_vec()),
            Err(CurveRefusal::NothingMeasured { sessions: 1 })
        );
    }

    /// Up to twelve sessions of up to twenty bars, each close a step of up to two dollars from the last.
    fn any_sessions() -> impl Strategy<Value = Vec<Vec<i64>>> {
        prop::collection::vec(
            prop::collection::vec(-2_000_000_i64..2_000_000, 2..20),
            2..12,
        )
    }

    /// Sessions walking from 100 dollars by `steps`, every tick times `scale`.
    fn walk(steps: &[Vec<i64>], scale: i64) -> Vec<Vec<Bar>> {
        steps
            .iter()
            .enumerate()
            .map(|(day, steps)| {
                let closes = steps
                    .iter()
                    .scan(100_000_000_i64, |close, step| {
                        *close += step;
                        Some(*close * scale)
                    })
                    .collect::<Vec<_>>();
                let start = eastern(4, 1 + day as u32, 9, 30);
                session("AAA", start, 100_000_000 * scale, &closes)
            })
            .collect()
    }

    proptest! {
        /// The measured factors average to one by construction, so a factor reads as a multiple of a typical bucket.
        #[test]
        fn test_measured_factors_average_to_one(steps in any_sessions()) {
            let sessions = walk(&steps, 1);
            if let Ok(curve) =
                TimeOfDayCurve::measure(BarInterval::FiveMinute, sessions.iter().map(Vec::as_slice))
            {
                let factors = curve
                    .buckets()
                    .values()
                    .filter_map(|factor| factor.ok())
                    .map(Measurement::value)
                    .collect::<Vec<_>>();
                prop_assert!(!factors.is_empty());
                let mean = factors.iter().sum::<f64>() / factors.len() as f64;
                prop_assert!((mean - 1.0).abs() < 1e-12);
                prop_assert!(factors.iter().all(|factor| *factor >= 0.0));
            }
        }

        /// The curve is of log returns, so multiplying every price by a constant leaves it unchanged.
        #[test]
        fn test_the_curve_is_scale_free(steps in any_sessions(), scale in 2_i64..1_000) {
            let measure = |sessions: &Vec<Vec<Bar>>| {
                TimeOfDayCurve::measure(BarInterval::FiveMinute, sessions.iter().map(Vec::as_slice))
            };
            match (measure(&walk(&steps, 1)), measure(&walk(&steps, scale))) {
                (Ok(original), Ok(scaled)) => {
                    prop_assert_eq!(
                        original.buckets().keys().collect::<Vec<_>>(),
                        scaled.buckets().keys().collect::<Vec<_>>()
                    );
                    for (left, right) in original.buckets().values().zip(scaled.buckets().values()) {
                        match (left, right) {
                            (Ok(left), Ok(right)) => {
                                prop_assert!((left.value() - right.value()).abs() < 1e-9);
                                prop_assert!(
                                    (left.standard_error() - right.standard_error()).abs() < 1e-9
                                );
                            }
                            (left, right) => prop_assert_eq!(left, right),
                        }
                    }
                }
                (original, scaled) => prop_assert_eq!(original, scaled),
            }
        }
    }
}
