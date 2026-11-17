//! Walk-forward folds over sessions: the fit sessions cut into contiguous periods, each tested by a fit on the sessions
//! before it less an embargo, and a confirm block later than every fit session.

use std::collections::BTreeSet;
use std::num::NonZeroUsize;

use chrono::Datelike;

use crate::common::time::SessionDate;

/// The calendar span one fold tests.
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
pub enum Period {
    /// An ISO week of the session's Eastern date.
    Week,
    Month,
}

impl Period {
    /// The period holding `session`, as a key that orders periods in time.
    fn of(self, session: SessionDate) -> (i32, u32) {
        let date = session.date();
        match self {
            Self::Week => {
                let week = date.iso_week();
                (week.year(), week.week())
            }
            Self::Month => (date.year(), date.month()),
        }
    }
}

/// The fit sessions held out of training just before what it is tested on, so a label reaching forward this many
/// sessions from the last training session cannot overlap the test.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Embargo(usize);

impl Embargo {
    /// No sessions held out, for a label that closes within its own session.
    pub const NONE: Self = Self(0);

    pub const fn sessions(count: usize) -> Self {
        Self(count)
    }

    pub const fn get(self) -> usize {
        self.0
    }
}

/// Why sessions cannot be cut into walk-forward folds, with the counts or sessions that refused them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum FoldsRefusal {
    #[error("a walk-forward split needs confirm sessions, and none were given")]
    NoConfirm,
    #[error("the confirm block starts {first_confirm}, not after the last fit session {last_fit}")]
    ConfirmNotLater {
        last_fit: SessionDate,
        first_confirm: SessionDate,
    },
    #[error("{periods} fit periods leave no fold after the {training_only} that only train")]
    TooFewPeriods {
        periods: usize,
        training_only: usize,
    },
    #[error("an embargo of {} sessions leaves the first fold nothing of its {warm_up} to train on", .embargo.get())]
    EmbargoEmptiesTraining { embargo: Embargo, warm_up: usize },
}

/// Fit sessions in ascending order, the index of each fold's first, the embargo before each, and the confirm block
/// after them all.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalkForward {
    fit: Vec<SessionDate>,
    starts: Vec<usize>,
    embargo: Embargo,
    confirm: Vec<SessionDate>,
}

/// One fold: the fit sessions before its test less the embargo, and the test's contiguous sessions of one period.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fold<'a> {
    train: &'a [SessionDate],
    test: &'a [SessionDate],
}

impl<'a> Fold<'a> {
    pub fn train(&self) -> &'a [SessionDate] {
        self.train
    }

    pub fn test(&self) -> &'a [SessionDate] {
        self.test
    }
}

/// Where a session falls in a walk-forward split.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Placement {
    /// A fit session in the first periods, which only ever train.
    TrainingOnly,
    /// A fit session tested by the fold at this index, which also trains every later fold outside its embargo.
    Fold(usize),
    Confirm,
    /// Neither fit nor confirm, so no part of the split reads it.
    Outside,
}

impl WalkForward {
    /// Cuts `fit` into one fold per `period` after the first `training_only` periods, each training on the fit
    /// sessions before it less the `embargo` fit sessions just before it; every confirm session must lie after every
    /// fit session, and the embargo is counted in fit sessions, so a gap of unlisted sessions adds to it.
    pub fn new(
        fit: BTreeSet<SessionDate>,
        confirm: BTreeSet<SessionDate>,
        period: Period,
        training_only: NonZeroUsize,
        embargo: Embargo,
    ) -> Result<Self, FoldsRefusal> {
        let first_confirm = confirm.first().copied().ok_or(FoldsRefusal::NoConfirm)?;
        match fit.last().copied() {
            Some(last_fit) if first_confirm <= last_fit => {
                return Err(FoldsRefusal::ConfirmNotLater {
                    last_fit,
                    first_confirm,
                });
            }
            Some(_) | None => {}
        }
        let fit: Vec<SessionDate> = fit.into_iter().collect();
        let period_starts: Vec<usize> = (0..fit.len())
            .filter(|index| *index == 0 || period.of(fit[*index - 1]) != period.of(fit[*index]))
            .collect();
        if period_starts.len() <= training_only.get() {
            return Err(FoldsRefusal::TooFewPeriods {
                periods: period_starts.len(),
                training_only: training_only.get(),
            });
        }
        let starts = period_starts[training_only.get()..].to_vec();
        // Folds start later and later, so the first trains on the least.
        if embargo.get() >= starts[0] {
            return Err(FoldsRefusal::EmbargoEmptiesTraining {
                embargo,
                warm_up: starts[0],
            });
        }
        Ok(Self {
            starts,
            fit,
            embargo,
            confirm: confirm.into_iter().collect(),
        })
    }

    pub fn fit(&self) -> &[SessionDate] {
        &self.fit
    }

    pub fn confirm(&self) -> &[SessionDate] {
        &self.confirm
    }

    /// The fit sessions a model read on the confirm block trains on: every one less the embargo before it.
    pub fn confirm_training(&self) -> &[SessionDate] {
        &self.fit[..self.fit.len() - self.embargo.get()]
    }

    pub fn folds(&self) -> impl Iterator<Item = Fold<'_>> {
        self.starts.iter().enumerate().map(|(index, start)| {
            let end = self
                .starts
                .get(index + 1)
                .copied()
                .unwrap_or(self.fit.len());
            Fold {
                train: &self.fit[..*start - self.embargo.get()],
                test: &self.fit[*start..end],
            }
        })
    }

    /// The part of the split that reads `session`, so rows are assigned by their session and never one by one.
    pub fn place(&self, session: SessionDate) -> Placement {
        match self.fit.binary_search(&session) {
            Ok(index) => match self.starts.partition_point(|start| *start <= index) {
                0 => Placement::TrainingOnly,
                folds => Placement::Fold(folds - 1),
            },
            Err(_) => match self.confirm.binary_search(&session) {
                Ok(_) => Placement::Confirm,
                Err(_) => Placement::Outside,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::{NaiveDate, TimeDelta};
    use proptest::prelude::*;
    use strum::IntoEnumIterator;

    use super::*;

    fn session(year: i32, month: u32, day: u32) -> SessionDate {
        SessionDate::from_date(NaiveDate::from_ymd_opt(year, month, day).unwrap())
    }

    fn sessions(dates: &[(i32, u32, u32)]) -> BTreeSet<SessionDate> {
        dates
            .iter()
            .map(|(year, month, day)| session(*year, *month, *day))
            .collect()
    }

    /// Five fit months with one warm-up month leave four folds, each testing one month on everything before it.
    #[test]
    fn test_monthly_folds_test_each_month_on_the_months_before() {
        let fit = sessions(&[
            (2024, 1, 2),
            (2024, 1, 30),
            (2024, 2, 1),
            (2024, 3, 4),
            (2024, 3, 5),
            (2024, 4, 1),
            (2024, 5, 31),
        ]);
        let split = WalkForward::new(
            fit,
            sessions(&[(2024, 6, 3)]),
            Period::Month,
            NonZeroUsize::MIN,
            Embargo::NONE,
        )
        .unwrap();
        let shapes: Vec<(usize, usize)> = split
            .folds()
            .map(|fold| (fold.train.len(), fold.test.len()))
            .collect();
        assert_eq!(shapes, [(2, 1), (3, 2), (5, 1), (6, 1)]);
        assert_eq!(
            [
                session(2024, 1, 30),
                session(2024, 3, 5),
                session(2024, 6, 3),
                session(2024, 6, 4)
            ]
            .map(|session| split.place(session)),
            [
                Placement::TrainingOnly,
                Placement::Fold(1),
                Placement::Confirm,
                Placement::Outside
            ]
        );
    }

    #[test]
    fn test_a_split_refuses_what_is_not_walk_forward() {
        let fit = sessions(&[(2024, 1, 2), (2024, 2, 1)]);
        let refused = |confirm: &[(i32, u32, u32)], training_only: usize| {
            WalkForward::new(
                fit.clone(),
                sessions(confirm),
                Period::Month,
                NonZeroUsize::new(training_only).unwrap(),
                Embargo::NONE,
            )
            .map_err(|refusal| refusal.to_string())
        };
        assert_eq!(
            refused(&[], 1),
            Err("a walk-forward split needs confirm sessions, and none were given".to_string())
        );
        assert_eq!(
            refused(&[(2024, 2, 1), (2024, 3, 1)], 1),
            Err(
                "the confirm block starts 2024-02-01, not after the last fit session 2024-02-01"
                    .to_string()
            )
        );
        assert_eq!(
            refused(&[(2024, 3, 1)], 2),
            Err("2 fit periods leave no fold after the 2 that only train".to_string())
        );
        assert!(refused(&[(2024, 3, 1)], 1).is_ok());
    }

    /// 2024-12-30 and 2025-01-03 share ISO week 1 of 2025, so they are tested together.
    #[test]
    fn test_weekly_folds_follow_the_iso_week_across_a_year_end() {
        let fit = sessions(&[(2024, 12, 23), (2024, 12, 30), (2025, 1, 3), (2025, 1, 6)]);
        let split = WalkForward::new(
            fit,
            sessions(&[(2025, 1, 7)]),
            Period::Week,
            NonZeroUsize::MIN,
            Embargo::NONE,
        )
        .unwrap();
        let tests: Vec<usize> = split.folds().map(|fold| fold.test.len()).collect();
        assert_eq!(tests, [2, 1]);
    }

    /// Two warm-up sessions in January and a two-session embargo: February trains on nothing and is refused, while
    /// with one held out each fold trains on all but the fit session just before it.
    #[test]
    fn test_an_embargo_holds_out_the_sessions_just_before_each_test() {
        let fit = sessions(&[
            (2024, 1, 2),
            (2024, 1, 30),
            (2024, 2, 1),
            (2024, 2, 2),
            (2024, 3, 4),
        ]);
        let confirm = sessions(&[(2024, 4, 1)]);
        let embargoed = |count: usize| {
            WalkForward::new(
                fit.clone(),
                confirm.clone(),
                Period::Month,
                NonZeroUsize::MIN,
                Embargo::sessions(count),
            )
        };
        assert_eq!(
            embargoed(2).map_err(|refusal| refusal.to_string()),
            Err(
                "an embargo of 2 sessions leaves the first fold nothing of its 2 to train on"
                    .to_string()
            )
        );
        let split = embargoed(1).unwrap();
        let trains: Vec<&[SessionDate]> = split.folds().map(|fold| fold.train).collect();
        assert_eq!(
            trains,
            [
                &[session(2024, 1, 2)][..],
                &[
                    session(2024, 1, 2),
                    session(2024, 1, 30),
                    session(2024, 2, 1)
                ][..]
            ]
        );
        assert_eq!(
            split.confirm_training(),
            [
                session(2024, 1, 2),
                session(2024, 1, 30),
                session(2024, 2, 1),
                session(2024, 2, 2)
            ]
        );
    }

    #[test]
    fn test_a_period_reads_back_from_its_name() {
        let names = ["week", "month"];
        for name in names {
            assert_eq!(name.parse::<Period>().unwrap().to_string(), name);
        }
        let iterated: Vec<&str> = Period::iter().map(|period| period.into()).collect();
        assert_eq!(iterated, names);
    }

    type Split = (
        BTreeSet<SessionDate>,
        BTreeSet<SessionDate>,
        Period,
        usize,
        usize,
    );

    fn split() -> impl Strategy<Value = Split> {
        (
            prop::collection::btree_set(0i64..400, 1..60),
            prop::collection::btree_set(0i64..40, 1..10),
            prop::sample::select(Period::iter().collect::<Vec<_>>()),
            1usize..4,
            0usize..6,
        )
            .prop_map(|(fit, confirm, period, training_only, embargo)| {
                let day = |offset: i64| {
                    SessionDate::from_date(
                        NaiveDate::from_ymd_opt(2024, 1, 1).unwrap() + TimeDelta::days(offset),
                    )
                };
                let last = fit.last().copied().unwrap_or(0);
                (
                    fit.into_iter().map(day).collect(),
                    confirm
                        .into_iter()
                        .map(|offset| day(last + 1 + offset))
                        .collect(),
                    period,
                    training_only,
                    embargo,
                )
            })
    }

    proptest! {
        /// The tests partition the fit sessions after the warm-up, each one contiguous period, each trained on every
        /// fit session before it but the `embargo` just before it, the confirm block trained on every fit session but
        /// the last `embargo` and starting after them all, and `place` agrees.
        #[test]
        fn test_folds_partition_the_sessions_and_train_before_their_embargo(
            (fit, confirm, period, training_only, embargo) in split(),
        ) {
            let split = match WalkForward::new(
                fit.clone(),
                confirm.clone(),
                period,
                NonZeroUsize::new(training_only).unwrap(),
                Embargo::sessions(embargo),
            ) {
                Ok(split) => split,
                Err(FoldsRefusal::TooFewPeriods { periods, .. }) => {
                    prop_assert!(periods <= training_only);
                    return Ok(());
                }
                Err(FoldsRefusal::EmbargoEmptiesTraining { warm_up, .. }) => {
                    let mut warm_up_periods = BTreeSet::new();
                    let counted = fit
                        .iter()
                        .take_while(|session| {
                            warm_up_periods.insert(period.of(**session));
                            warm_up_periods.len() <= training_only
                        })
                        .count();
                    prop_assert_eq!(warm_up, counted);
                    prop_assert!(embargo >= warm_up);
                    return Ok(());
                }
                Err(refusal) => return Err(TestCaseError::fail(refusal.to_string())),
            };
            let folds: Vec<Fold> = split.folds().collect();
            prop_assert!(!folds.is_empty());
            let tested: Vec<SessionDate> = folds.iter().flat_map(|fold| fold.test.iter().copied()).collect();
            let warm_up = fit.len() - tested.len();
            prop_assert_eq!(&tested, &fit.iter().copied().skip(warm_up).collect::<Vec<_>>());
            for (index, fold) in folds.iter().enumerate() {
                prop_assert!(!fold.test.is_empty());
                prop_assert!(fold.test.iter().all(|session| period.of(*session) == period.of(fold.test[0])));
                let before: Vec<SessionDate> = fit.iter().copied().filter(|session| *session < fold.test[0]).collect();
                prop_assert!(!fold.train.is_empty());
                prop_assert_eq!(fold.train, &before[..before.len() - embargo]);
                // No training session lies within the embargo: `embargo` fit sessions sit between the two.
                let last_train = *fold.train.last().unwrap();
                prop_assert_eq!(
                    fit.iter().filter(|session| **session > last_train && **session < fold.test[0]).count(),
                    embargo
                );
                prop_assert!(fold.test.iter().all(|session| split.place(*session) == Placement::Fold(index)));
            }
            let periods: BTreeSet<(i32, u32)> =
                fit.iter().take(warm_up).map(|session| period.of(*session)).collect();
            prop_assert_eq!(periods.len(), training_only);
            prop_assert!(fit.iter().take(warm_up).all(|session| split.place(*session) == Placement::TrainingOnly));
            prop_assert!(split.confirm().iter().all(|session| *session > *fit.last().unwrap()));
            let confirm_training = split.confirm_training();
            prop_assert_eq!(confirm_training, &fit.iter().copied().collect::<Vec<_>>()[..fit.len() - embargo]);
            prop_assert_eq!(
                fit.iter().filter(|session| **session > *confirm_training.last().unwrap()).count(),
                embargo
            );
            prop_assert!(confirm.iter().all(|session| split.place(*session) == Placement::Confirm));
        }
    }
}
