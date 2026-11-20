//! A name's per-session volatility from its daily bars: the Yang-Zhang range estimator, with its overnight term apart,
//! read beside close-to-close on the same days because a range estimator reads low where a name trades thinly.

use crate::common::market::record::{Bar, BarInterval};
use crate::common::market::{Price, Symbol};
use crate::common::time::SessionDate;
use crate::common::time::calendar::TradingCalendar;

/// The fewest daily returns a volatility is taken over: at two, the two centered squares are always equal, so the
/// close-to-close error would read zero however the name moved.
pub const MINIMUM_DAYS: usize = 3;

/// A value with its standard error and the observations it was taken over.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Measurement {
    value: f64,
    standard_error: f64,
    observations: u64,
}

impl Measurement {
    pub(super) fn new(value: f64, standard_error: f64, observations: u64) -> Self {
        Self {
            value,
            standard_error,
            observations,
        }
    }

    pub fn value(self) -> f64 {
        self.value
    }

    pub fn standard_error(self) -> f64 {
        self.standard_error
    }

    pub fn observations(self) -> u64 {
        self.observations
    }
}

/// The two estimators read on the same days, each a standard deviation of log return per session.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DailyVolatility {
    range: Measurement,
    close_to_close: Measurement,
}

/// Which of the two estimators a reading came from.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    strum::Display,
    strum::EnumString,
    strum::IntoStaticStr,
    strum::EnumIter,
)]
#[strum(serialize_all = "snake_case")]
pub enum Estimator {
    Range,
    CloseToClose,
}

/// Why a name's daily volatility was not measured, with what caused it.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum DailyVolatilityRefusal {
    #[error("a {interval} bar is not a daily bar")]
    NotDaily { interval: BarInterval },
    #[error("a bar for {found} is among bars for {expected}")]
    MixedSymbols { expected: Symbol, found: Symbol },
    /// Out of order, or with a trading day between them, which would fold two overnights into one.
    #[error("{next} does not follow {previous} as the next trading day")]
    NotConsecutive {
        previous: SessionDate,
        next: SessionDate,
    },
    /// The first bar's session is a holiday or outside the calendar, so the next bar's return cannot be placed.
    #[error("{session} is not a trading day on the calendar")]
    NotTradingDay { session: SessionDate },
    #[error("{days} daily returns are fewer than the {MINIMUM_DAYS} a volatility needs")]
    TooFewDays { days: usize },
    #[error("the {estimator} estimator read no movement over {days} days")]
    NoMovement { estimator: Estimator, days: usize },
}

/// One day's log returns against the previous close.
struct Day {
    overnight: f64,
    open_to_close: f64,
    rogers_satchell: f64,
    close_to_close: f64,
}

impl DailyVolatility {
    /// Both estimators over consecutive trading days of one name's daily bars, the first bar serving only as the
    /// previous close.
    pub fn measure(
        bars: &[Bar],
        calendar: &TradingCalendar,
    ) -> Result<Self, DailyVolatilityRefusal> {
        let days = days(bars, calendar)?;
        let count = days.len();
        if count < MINIMUM_DAYS {
            return Err(DailyVolatilityRefusal::TooFewDays { days: count });
        }
        let overnight = centered_squares(days.iter().map(|day| day.overnight));
        let open_to_close = centered_squares(days.iter().map(|day| day.open_to_close));
        let close_to_close = centered_squares(days.iter().map(|day| day.close_to_close));
        let days_float = count as f64;
        // Yang and Zhang's weight, which minimizes the estimator's variance under drift-free Brownian motion.
        let weight = 0.34 / (1.34 + (days_float + 1.0) / (days_float - 1.0));
        let range = days.iter().zip(overnight.iter().zip(&open_to_close)).map(
            |(day, (overnight, open_to_close))| {
                overnight + weight * open_to_close + (1.0 - weight) * day.rogers_satchell
            },
        );
        Ok(Self {
            range: standard_deviation(range, Estimator::Range)?,
            close_to_close: standard_deviation(close_to_close, Estimator::CloseToClose)?,
        })
    }

    pub fn range(self) -> Measurement {
        self.range
    }

    pub fn close_to_close(self) -> Measurement {
        self.close_to_close
    }

    /// Close-to-close less range in their combined standard error, positive where the range estimator reads low;
    /// `None` where both errors read zero, as when every day's contributions are equal.
    ///
    /// The errors are combined in quadrature though both estimators read the same days, which overstates the gap's
    /// error when the two move together.
    pub fn range_shortfall(self) -> Option<f64> {
        let error = self
            .range
            .standard_error
            .hypot(self.close_to_close.standard_error);
        let gap = self.close_to_close.value - self.range.value;
        match error > 0.0 {
            true => Some(gap / error),
            false => None,
        }
    }
}

fn days(bars: &[Bar], calendar: &TradingCalendar) -> Result<Vec<Day>, DailyVolatilityRefusal> {
    let Some(first) = bars.first() else {
        return Ok(Vec::new());
    };
    for bar in bars {
        match bar.interval() {
            BarInterval::OneDay => {}
            BarInterval::OneMinute | BarInterval::FiveMinute => {
                return Err(DailyVolatilityRefusal::NotDaily {
                    interval: bar.interval(),
                });
            }
        }
        if bar.symbol() != first.symbol() {
            return Err(DailyVolatilityRefusal::MixedSymbols {
                expected: first.symbol().clone(),
                found: bar.symbol().clone(),
            });
        }
    }
    // Each later session is checked as the next trading day after this one, so only this one needs checking here.
    let first_session = SessionDate::at(first.timestamp());
    if !calendar.is_trading_day(first_session) {
        return Err(DailyVolatilityRefusal::NotTradingDay {
            session: first_session,
        });
    }
    bars.windows(2)
        .map(|pair| {
            let (previous, bar) = (&pair[0], &pair[1]);
            let (previous_session, session) = (
                SessionDate::at(previous.timestamp()),
                SessionDate::at(bar.timestamp()),
            );
            if calendar.next_trading_day(previous_session) != Some(session) {
                return Err(DailyVolatilityRefusal::NotConsecutive {
                    previous: previous_session,
                    next: session,
                });
            }
            let prices = bar.prices();
            let logarithm = |price: Price| (price.ticks() as f64).ln();
            let (open, high, low, close) = (
                logarithm(prices.open()),
                logarithm(prices.high()),
                logarithm(prices.low()),
                logarithm(prices.close()),
            );
            let previous_close = logarithm(previous.prices().close());
            Ok(Day {
                overnight: open - previous_close,
                open_to_close: close - open,
                rogers_satchell: (high - close) * (high - open) + (low - close) * (low - open),
                close_to_close: close - previous_close,
            })
        })
        .collect()
}

/// Each value's squared deviation from their mean, scaled by `n / (n - 1)` so the squares average to the sample
/// variance.
fn centered_squares(values: impl Iterator<Item = f64> + Clone) -> Vec<f64> {
    let count = values.clone().count() as f64;
    let mean = values.clone().sum::<f64>() / count;
    values
        .map(|value| (value - mean).powi(2) * count / (count - 1.0))
        .collect()
}

/// The mean of per-observation contributions and its standard error, over at least two.
pub(super) fn mean_with_error(contributions: &[f64]) -> (f64, f64) {
    let count = contributions.len() as f64;
    let mean = contributions.iter().sum::<f64>() / count;
    let squared_deviations = contributions
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>();
    (mean, (squared_deviations / (count - 1.0) / count).sqrt())
}

/// The square root of the mean of per-day variance contributions, its error carried from the variance's by the delta
/// method; the error ignores that the contributions share an estimated mean.
fn standard_deviation(
    contributions: impl IntoIterator<Item = f64>,
    estimator: Estimator,
) -> Result<Measurement, DailyVolatilityRefusal> {
    let contributions = contributions.into_iter().collect::<Vec<_>>();
    let (variance, variance_error) = mean_with_error(&contributions);
    if variance <= 0.0 {
        return Err(DailyVolatilityRefusal::NoMovement {
            estimator,
            days: contributions.len(),
        });
    }
    let volatility = variance.sqrt();
    Ok(Measurement::new(
        volatility,
        variance_error / (2.0 * volatility),
        contributions.len() as u64,
    ))
}

#[cfg(test)]
pub(super) mod tests {
    use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
    use proptest::prelude::*;

    use super::*;
    use crate::common::market::Shares;
    use crate::common::market::record::BarPrices;
    use crate::common::time::SessionRange;
    use crate::common::time::calendar::TradingSession;

    pub(in crate::common::laboratory) fn bar(
        raw: &str,
        interval: BarInterval,
        timestamp: DateTime<Utc>,
        [open, high, low, close]: [i64; 4],
    ) -> Bar {
        let price = |ticks| Price::from_ticks(ticks).unwrap();
        Bar::new(
            Symbol::new(raw).unwrap(),
            interval,
            timestamp,
            BarPrices::new(price(open), price(high), price(low), price(close)).unwrap(),
            Shares::whole(100).unwrap(),
            None,
            None,
        )
        .unwrap()
    }

    fn session(day: i64) -> SessionDate {
        SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 3, 2).unwrap()).plus_calendar_days(day)
    }

    /// Every calendar day for 400 days from the first session trades, except those in `holidays`.
    fn calendar(holidays: &[i64]) -> TradingCalendar {
        calendar_from(0, holidays)
    }

    /// Every calendar day from session `first` through session 399 trades, except those in `holidays`.
    fn calendar_from(first: i64, holidays: &[i64]) -> TradingCalendar {
        let time = |hour| NaiveTime::from_hms_opt(hour, 0, 0).unwrap();
        let sessions = (first..400)
            .filter(|day| !holidays.contains(day))
            .map(|day| TradingSession::new(session(day), time(9), time(16)).unwrap())
            .collect();
        TradingCalendar::new(
            sessions,
            SessionRange::new(session(first), session(399)).unwrap(),
        )
        .unwrap()
    }

    fn daily(prices: &[[f64; 4]]) -> Vec<Bar> {
        prices
            .iter()
            .enumerate()
            .map(|(day, dollars)| {
                let ticks = dollars.map(|dollar| Price::from_dollars(dollar).unwrap().ticks());
                bar(
                    "AAA",
                    BarInterval::OneDay,
                    session(day as i64).regular_close(),
                    ticks,
                )
            })
            .collect()
    }

    const HAND: [[f64; 4]; 4] = [
        [100.0, 100.0, 100.0, 100.0],
        [101.0, 103.0, 100.0, 102.0],
        [101.0, 102.0, 99.0, 100.0],
        [100.5, 104.0, 100.0, 103.0],
    ];

    pub(in crate::common::laboratory) fn close(left: f64, right: f64) -> bool {
        (left - right).abs() <= 1e-12 * right.abs().max(1e-12)
    }

    #[test]
    fn test_a_hand_series_reads_its_literal_volatilities() {
        let measured = DailyVolatility::measure(&daily(&HAND), &calendar(&[])).unwrap();
        let (range, close_to_close) = (measured.range(), measured.close_to_close());
        assert_eq!(
            (range.observations(), close_to_close.observations()),
            (3, 3)
        );
        assert!(close(range.value(), 0.022688124399787526));
        assert!(close(range.standard_error(), 0.0010291048585266635));
        assert!(close(close_to_close.value(), 0.026141633036523792));
        assert!(close(close_to_close.standard_error(), 0.006535408259130948));
        assert!(close(
            measured.range_shortfall().unwrap(),
            (0.026141633036523792 - 0.022688124399787526)
                / 0.0010291048585266635_f64.hypot(0.006535408259130948)
        ));
    }

    #[test]
    fn test_daily_volatility_refuses_what_it_cannot_read() {
        let bars = daily(&HAND);
        assert_eq!(
            DailyVolatility::measure(&bars, &calendar(&[2])),
            Err(DailyVolatilityRefusal::NotConsecutive {
                previous: session(1),
                next: session(2)
            })
        );
        assert_eq!(
            DailyVolatility::measure(&[bars[1].clone(), bars[0].clone()], &calendar(&[])),
            Err(DailyVolatilityRefusal::NotConsecutive {
                previous: session(1),
                next: session(0)
            })
        );
        assert_eq!(
            DailyVolatility::measure(&bars[..2], &calendar(&[])),
            Err(DailyVolatilityRefusal::TooFewDays { days: 1 })
        );
        assert_eq!(
            DailyVolatility::measure(&bars[..3], &calendar(&[])),
            Err(DailyVolatilityRefusal::TooFewDays { days: 2 })
        );
        let still = daily(&[[50.0; 4]; 5]);
        assert_eq!(
            DailyVolatility::measure(&still, &calendar(&[])),
            Err(DailyVolatilityRefusal::NoMovement {
                estimator: Estimator::Range,
                days: 4
            })
        );
        let mut mixed = bars.clone();
        mixed[2] = bar(
            "BBB",
            BarInterval::OneDay,
            session(2).regular_close(),
            [1_000_000; 4],
        );
        assert_eq!(
            DailyVolatility::measure(&mixed, &calendar(&[])),
            Err(DailyVolatilityRefusal::MixedSymbols {
                expected: Symbol::new("AAA").unwrap(),
                found: Symbol::new("BBB").unwrap()
            })
        );
        let mut intraday = bars;
        intraday[3] = bar(
            "AAA",
            BarInterval::FiveMinute,
            session(3).regular_close(),
            [1_000_000; 4],
        );
        assert_eq!(
            DailyVolatility::measure(&intraday, &calendar(&[])),
            Err(DailyVolatilityRefusal::NotDaily {
                interval: BarInterval::FiveMinute
            })
        );
    }

    /// The first bar's session anchors every later one, so a holiday there would pass the next trading day's bar as
    /// one daily return over two overnights.
    #[test]
    fn test_a_first_bar_on_a_holiday_is_refused() {
        assert_eq!(
            DailyVolatility::measure(&daily(&HAND), &calendar(&[0])),
            Err(DailyVolatilityRefusal::NotTradingDay {
                session: session(0)
            })
        );
    }

    /// A first bar before the calendar's range is as unplaceable as a holiday, since the calendar's first session
    /// would read as its successor.
    #[test]
    fn test_a_first_bar_before_the_calendar_is_refused() {
        assert_eq!(
            DailyVolatility::measure(&daily(&HAND), &calendar_from(1, &[])),
            Err(DailyVolatilityRefusal::NotTradingDay {
                session: session(0)
            })
        );
    }

    /// Returns alternating up and down by the same log step, with no gap or wick, make every day's contribution equal,
    /// so neither estimator has an error to judge the gap by.
    #[test]
    fn test_a_shortfall_without_error_is_unmeasured() {
        let measured = DailyVolatility::measure(
            &daily(&[
                [100.0; 4],
                [100.0, 101.0, 100.0, 101.0],
                [101.0, 101.0, 100.0, 100.0],
                [100.0, 101.0, 100.0, 101.0],
                [101.0, 101.0, 100.0, 100.0],
            ]),
            &calendar(&[]),
        )
        .unwrap();
        assert_eq!(
            (
                measured.range().standard_error(),
                measured.close_to_close().standard_error()
            ),
            (0.0, 0.0)
        );
        assert!(measured.close_to_close().value() > measured.range().value());
        assert_eq!(measured.range_shortfall(), None);
    }

    #[test]
    fn test_estimator_names_round_trip() {
        use strum::IntoEnumIterator;
        assert_eq!(
            Estimator::iter().map(<&str>::from).collect::<Vec<_>>(),
            vec!["range", "close_to_close"]
        );
        for estimator in Estimator::iter() {
            assert_eq!(estimator.to_string().parse::<Estimator>(), Ok(estimator));
        }
    }

    type Step = (i64, i64, i64, i64);

    /// Daily bars walking from 100 dollars by `steps` of gap, body, wick above and wick below, every tick times `scale`.
    fn walk_bars(steps: &[Step], scale: i64) -> Vec<Bar> {
        let mut previous_close = 100_000_000_i64;
        steps
            .iter()
            .enumerate()
            .map(|(day, (gap, body, above, below))| {
                let open = previous_close + gap;
                let close = open + body;
                previous_close = close;
                let ticks = [
                    open,
                    open.max(close) + above,
                    open.min(close) - below,
                    close,
                ]
                .map(|ticks| ticks * scale);
                bar(
                    "AAA",
                    BarInterval::OneDay,
                    session(day as i64).regular_close(),
                    ticks,
                )
            })
            .collect()
    }

    fn steps() -> impl Strategy<Value = Vec<Step>> {
        prop::collection::vec(
            (
                -1_000_000_i64..1_000_000,
                -2_000_000_i64..2_000_000,
                0_i64..1_000_000,
                0_i64..1_000_000,
            ),
            3..40,
        )
    }

    proptest! {
        /// Volatility is of log returns, so multiplying every price by a constant leaves both readings unchanged.
        #[test]
        fn test_volatility_is_scale_free(steps in steps(), scale in 2_i64..1_000) {
            let calendar = calendar(&[]);
            let original = DailyVolatility::measure(&walk_bars(&steps, 1), &calendar);
            let scaled = DailyVolatility::measure(&walk_bars(&steps, scale), &calendar);
            match (original, scaled) {
                (Ok(original), Ok(scaled)) => {
                    for (left, right) in [
                        (original.range(), scaled.range()),
                        (original.close_to_close(), scaled.close_to_close()),
                    ] {
                        prop_assert!((left.value() - right.value()).abs() <= 1e-9 * left.value());
                        prop_assert!(
                            (left.standard_error() - right.standard_error()).abs()
                                <= 1e-9 * left.value()
                        );
                    }
                }
                (original, scaled) => prop_assert_eq!(original, scaled),
            }
        }

        /// Both readings are standard deviations with errors, so neither is negative or unbounded.
        #[test]
        fn test_volatility_is_a_finite_positive_deviation(steps in steps()) {
            if let Ok(measured) = DailyVolatility::measure(&walk_bars(&steps, 1), &calendar(&[])) {
                for measurement in [measured.range(), measured.close_to_close()] {
                    prop_assert!(measurement.value() > 0.0 && measurement.value().is_finite());
                    prop_assert!(measurement.standard_error() >= 0.0);
                    prop_assert!(measurement.standard_error().is_finite());
                    prop_assert_eq!(measurement.observations(), steps.len() as u64 - 1);
                }
            }
        }
    }
}
