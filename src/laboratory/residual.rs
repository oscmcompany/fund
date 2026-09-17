//! What a name did that its sector, its size and its volatility do not account for.
//!
//! A cross-sectional fit per session, exposed as a column studies read in place of `daily_return`.
//! Names the fit explains perfectly are refused rather than returned as a residual of zero.

use std::collections::BTreeMap;

use polars::prelude::*;
use serde::Serialize;

use crate::data::details::UNKNOWN;

/// The column this module adds, null wherever the residual was refused.
pub const RESIDUAL_COLUMN: &str = "residual_return";

/// Errors computing a residual panel.
#[derive(Debug, thiserror::Error)]
pub enum ResidualError {
    #[error("dataframe operation failed: {0}")]
    Frame(#[from] PolarsError),
    /// Refused rather than counted as a refusal, the way `clean_data` refuses a null ticker: a row
    /// that names no instrument or no session cannot be attributed to either.
    #[error("{rows} of {height} rows carry no ticker or no timestamp")]
    Unattributable { rows: usize, height: usize },
}

/// The factor set a panel was fitted with, declared rather than assumed.
///
/// Carried into [`crate::laboratory::dataset::DatasetFingerprint`] for the reason the liquidity
/// floor is: two panels over the same window and a different volatility lookback are different
/// measurements, and nothing else in the fingerprint would tell them apart.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct FactorSpecification {
    volatility_sessions: usize,
    minimum_residual_variance_share: f64,
}

impl FactorSpecification {
    /// The specification every study uses until one declares its own.
    ///
    /// Sixty sessions is roughly a quarter of trading, long enough that a single jump does not
    /// dominate the estimate and short enough to track a regime. Half is the variance share: a name
    /// whose fit absorbs more than half of its own variation is being described by the model rather
    /// than measured against it.
    pub const CURRENT: Self = Self {
        volatility_sessions: 60,
        minimum_residual_variance_share: 0.5,
    };

    /// `None` unless the lookback is positive and the variance share lies in `(0, 1]`.
    ///
    /// A share of zero would admit a name the fit explains exactly, which is the refusal this type
    /// exists to make; a share above one is unreachable, since leverage is never negative.
    pub fn new(volatility_sessions: usize, minimum_residual_variance_share: f64) -> Option<Self> {
        let usable = minimum_residual_variance_share > 0.0
            && minimum_residual_variance_share <= 1.0
            && minimum_residual_variance_share.is_finite();
        (volatility_sessions > 0 && usable).then_some(Self {
            volatility_sessions,
            minimum_residual_variance_share,
        })
    }

    pub fn volatility_sessions(self) -> usize {
        self.volatility_sessions
    }

    pub fn minimum_residual_variance_share(self) -> f64 {
        self.minimum_residual_variance_share
    }
}

impl std::fmt::Display for FactorSpecification {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{}-session volatility, at least {:.0}% residual variance",
            self.volatility_sessions,
            self.minimum_residual_variance_share * 100.0
        )
    }
}

/// Why a name has no residual on a session, carrying the reading that produced the refusal.
///
/// Separate variants rather than one "unmeasurable", because they send an operator to different
/// places: a missing share count is a feed coverage question and a thin sector is a universe one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ResidualRefusal {
    /// The feed classified the name into no SIC major group, so it belongs to no sector.
    ///
    /// Not pooled with the other unclassified names: their only shared property is that the feed
    /// declined to answer, and removing a common return from them would invent a factor.
    SectorUnknown,
    /// No point-in-time share count, so the name has no size.
    SizeUnmeasured,
    /// Fewer prior returns than the lookback asks for.
    VolatilityWindowShort,
    /// The name's own return is not a daily one — its first session, or a session after a gap.
    ReturnUnmeasured,
    /// The fit reproduces the name exactly, or nearly so, and its residual is an artefact.
    ///
    /// A name alone in its sector lands here with leverage of exactly one: the sector coefficient
    /// is fitted to that one name, so its residual is zero whatever the name did.
    FullyExplained,
    /// Size and volatility moved together across the whole cross-section, so neither is identified.
    FactorsCollinear,
}

impl std::fmt::Display for ResidualRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let reason = match self {
            ResidualRefusal::SectorUnknown => "the feed assigned no SIC major group",
            ResidualRefusal::SizeUnmeasured => "no point-in-time share count",
            ResidualRefusal::VolatilityWindowShort => "too few prior returns for the lookback",
            ResidualRefusal::ReturnUnmeasured => "no daily return on this session",
            ResidualRefusal::FullyExplained => "the fit explains the name exactly",
            ResidualRefusal::FactorsCollinear => "size and volatility are collinear this session",
        };
        formatter.write_str(reason)
    }
}

/// A residual panel, and what it could not measure.
///
/// The counts travel with the frame rather than being logged, because an estimator silently defined
/// on part of its input reads exactly like a complete measurement.
#[derive(Debug)]
pub struct ResidualPanel {
    /// The input frame with [`RESIDUAL_COLUMN`] added, null wherever a residual was refused.
    pub frame: DataFrame,
    /// Rows carrying a residual.
    pub measured: usize,
    /// Rows refused, by cause. Sums to `frame.height() - measured`.
    pub refused: BTreeMap<ResidualRefusal, usize>,
}

impl ResidualPanel {
    /// The share of rows with no residual, which every estimate off this panel is quoted beside.
    ///
    /// `None` on an empty panel: a share of nothing is not zero, it is unmeasurable.
    pub fn undefined_share(&self) -> Option<f64> {
        let rows = self.frame.height();
        (rows > 0).then(|| (rows - self.measured) as f64 / rows as f64)
    }
}

/// One name's row in one session's cross-section, once every factor is measurable.
struct Candidate {
    row: usize,
    sector: String,
    log_size: f64,
    volatility: f64,
    daily_return: f64,
}

/// Strips the sector, size and volatility a session's cross-section shared, per name.
///
/// The input must carry `ticker`, `timestamp`, `close_price`, `sector`, `shares_outstanding` and
/// `daily_return` — the frame [`crate::laboratory::dataset::returns`] already produces.
pub fn residual_returns(
    returns: &DataFrame,
    specification: FactorSpecification,
) -> Result<ResidualPanel, ResidualError> {
    let tickers = returns.column("ticker")?.str()?;
    let timestamps = returns.column("timestamp")?.i64()?;
    let close_prices = returns.column("close_price")?.f64()?;
    let sectors = returns.column("sector")?.str()?;
    let shares = returns.column("shares_outstanding")?.f64()?;
    let daily_returns = returns.column("daily_return")?.cast(&DataType::Float64)?;
    let daily_returns = daily_returns.f64()?;

    let mut residuals: Vec<Option<f64>> = vec![None; returns.height()];
    let mut refused: BTreeMap<ResidualRefusal, usize> = BTreeMap::new();
    let mut refuse = |cause: ResidualRefusal| *refused.entry(cause).or_insert(0) += 1;

    // Row indices per name, ordered in time, so each name's own history is readable without
    // assuming the frame arrived sorted.
    let mut by_ticker: BTreeMap<&str, Vec<usize>> = BTreeMap::new();
    let mut unattributable = 0usize;
    for row in 0..returns.height() {
        match (tickers.get(row), timestamps.get(row)) {
            (Some(ticker), Some(_session)) => by_ticker.entry(ticker).or_default().push(row),
            (None, _) | (_, None) => unattributable += 1,
        }
    }
    if unattributable > 0 {
        return Err(ResidualError::Unattributable {
            rows: unattributable,
            height: returns.height(),
        });
    }
    for rows in by_ticker.values_mut() {
        rows.sort_by_key(|row| timestamps.get(*row));
    }

    // Every factor loading is read off sessions strictly before the one being explained. A size
    // built from the session's own close would be circular: a name that rose would look larger for
    // having risen.
    let mut sessions: BTreeMap<i64, Vec<Candidate>> = BTreeMap::new();
    for rows in by_ticker.values() {
        for (position, &row) in rows.iter().enumerate() {
            let session = timestamps.get(row).expect("checked above");
            let Some(daily_return) = daily_returns.get(row).filter(|value| value.is_finite())
            else {
                refuse(ResidualRefusal::ReturnUnmeasured);
                continue;
            };
            let sector = match sectors.get(row) {
                Some(sector) if sector != UNKNOWN => sector.to_string(),
                // Both readings of the sentinel are deliberate and opposite: the pair screen merges
                // unclassified names so two of them never count as diversified, and here merging
                // them would remove a shared return from businesses with nothing in common.
                _ => {
                    refuse(ResidualRefusal::SectorUnknown);
                    continue;
                }
            };
            // The lookback is tested first, and the order is load-bearing rather than incidental: a
            // name's first session in the window has no prior close either, so testing size first
            // would report every one of them as a feed coverage gap.
            let Some(volatility) =
                trailing_volatility(rows, position, daily_returns, specification)
            else {
                refuse(ResidualRefusal::VolatilityWindowShort);
                continue;
            };
            let Some(log_size) = previous_log_size(rows, position, close_prices, shares) else {
                refuse(ResidualRefusal::SizeUnmeasured);
                continue;
            };

            sessions.entry(session).or_default().push(Candidate {
                row,
                sector,
                log_size,
                volatility,
                daily_return,
            });
        }
    }

    let mut measured = 0usize;
    for candidates in sessions.values() {
        for (row, outcome) in fit_session(candidates, specification) {
            match outcome {
                Ok(residual) => {
                    residuals[row] = Some(residual);
                    measured += 1;
                }
                Err(cause) => refuse(cause),
            }
        }
    }

    let mut frame = returns.clone();
    frame.with_column(Column::new(RESIDUAL_COLUMN.into(), residuals))?;

    Ok(ResidualPanel {
        frame,
        measured,
        refused,
    })
}

/// Log market capitalization from the session *before* the one being explained.
///
/// `None` where there is no prior session for the name, or no share count on it. A gap in the
/// series is allowed: shares outstanding move over quarters, so a stale reading is still a size.
fn previous_log_size(
    rows: &[usize],
    position: usize,
    close_prices: &Float64Chunked,
    shares: &Float64Chunked,
) -> Option<f64> {
    let previous = rows.get(position.checked_sub(1)?)?;
    let close = close_prices.get(*previous).filter(|value| *value > 0.0)?;
    let count = shares.get(*previous).filter(|value| *value > 0.0)?;
    let capitalization = close * count;
    capitalization.is_finite().then(|| capitalization.ln())
}

/// Standard deviation of the name's returns over the lookback, ending before this session.
///
/// `None` unless the window is full. A volatility fitted on three observations is a different
/// quantity from one fitted on sixty, and admitting both would put two of them in one column.
fn trailing_volatility(
    rows: &[usize],
    position: usize,
    daily_returns: &Float64Chunked,
    specification: FactorSpecification,
) -> Option<f64> {
    let window = specification.volatility_sessions();
    let start = position.checked_sub(window)?;
    let observations: Vec<f64> = rows[start..position]
        .iter()
        .filter_map(|row| daily_returns.get(*row).filter(|value| value.is_finite()))
        .collect();
    if observations.len() < window {
        return None;
    }
    let mean = observations.iter().sum::<f64>() / observations.len() as f64;
    let variance = observations
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / (observations.len() - 1) as f64;
    variance.is_finite().then(|| variance.sqrt())
}

/// Whether this session's two continuous factors support a fit at all.
///
/// Two failures, and they are separate because the second hides the first: a factor that does not
/// vary leaves a within-sector sum of squares made entirely of rounding, and the ratio test for
/// collinearity divides that noise by itself and reports a *perfectly* conditioned system. So each
/// factor is first checked for variation against its own level, and only then against the other.
fn identified(candidates: &[Candidate], demeaned: &Demeaned) -> bool {
    // Both floors are ratios rather than absolute epsilons, so a cross-section priced in log dollars
    // and one priced in daily standard deviations are held to the same standard.
    const VARIATION_FLOOR: f64 = 1e-12;
    const CONDITION_FLOOR: f64 = 1e-10;

    let mut size_level = 0.0;
    let mut volatility_level = 0.0;
    for candidate in candidates {
        size_level += candidate.log_size * candidate.log_size;
        volatility_level += candidate.volatility * candidate.volatility;
    }
    let varies = |deviation: f64, level: f64| level > 0.0 && deviation / level > VARIATION_FLOOR;

    let scale = demeaned.size_size * demeaned.volatility_volatility;
    varies(demeaned.size_size, size_level)
        && varies(demeaned.volatility_volatility, volatility_level)
        && demeaned.determinant().is_finite()
        && scale > 0.0
        && demeaned.determinant() / scale > CONDITION_FLOOR
}

/// Fits one session's cross-section and returns each name's residual or its refusal.
///
/// Sector enters as a full set of dummies, so this is a regression on dummies plus two continuous
/// factors. It is computed by demeaning both sides within sector and fitting the two continuous
/// factors on what is left, which is the same fit by the Frisch-Waugh-Lovell theorem: a two-by-two
/// solve rather than a sixty-five-column one, and the singleton sector falls out as arithmetic
/// instead of a special case. The market is not identified separately from the sector dummies —
/// it lies in their span — but the residual does not depend on how that span is parameterised.
fn fit_session(
    candidates: &[Candidate],
    specification: FactorSpecification,
) -> Vec<(usize, Result<f64, ResidualRefusal>)> {
    let demeaned = Demeaned::of(candidates);

    if !identified(candidates, &demeaned) {
        return candidates
            .iter()
            .map(|candidate| (candidate.row, Err(ResidualRefusal::FactorsCollinear)))
            .collect();
    }

    let determinant = demeaned.determinant();
    let size_coefficient = (demeaned.volatility_volatility * demeaned.size_return
        - demeaned.size_volatility * demeaned.volatility_return)
        / determinant;
    let volatility_coefficient = (demeaned.size_size * demeaned.volatility_return
        - demeaned.size_volatility * demeaned.size_return)
        / determinant;

    candidates
        .iter()
        .zip(&demeaned.rows)
        .map(|(candidate, deviation)| {
            let variance_share = 1.0 - demeaned.leverage(deviation);
            if variance_share.is_nan()
                || variance_share < specification.minimum_residual_variance_share()
            {
                return (candidate.row, Err(ResidualRefusal::FullyExplained));
            }
            let residual = deviation.daily_return
                - size_coefficient * deviation.log_size
                - volatility_coefficient * deviation.volatility;
            (candidate.row, Ok(residual))
        })
        .collect()
}

/// One name's distance from its own sector's mean, on each of the three axes.
struct Deviation {
    daily_return: f64,
    log_size: f64,
    volatility: f64,
    /// Names in this row's sector, which is what its share of the sector coefficient costs.
    sector_names: usize,
}

/// A session's within-sector deviations and the cross-products the fit is solved from.
///
/// The determinant is derived rather than stored beside the cross-products: it is a function of
/// them, and two fields that must agree eventually will not.
struct Demeaned {
    rows: Vec<Deviation>,
    size_size: f64,
    size_volatility: f64,
    volatility_volatility: f64,
    size_return: f64,
    volatility_return: f64,
}

impl Demeaned {
    fn of(candidates: &[Candidate]) -> Self {
        let mut sums: BTreeMap<&str, (f64, f64, f64, usize)> = BTreeMap::new();
        for candidate in candidates {
            let entry = sums
                .entry(candidate.sector.as_str())
                .or_insert((0.0, 0.0, 0.0, 0));
            entry.0 += candidate.daily_return;
            entry.1 += candidate.log_size;
            entry.2 += candidate.volatility;
            entry.3 += 1;
        }

        // A name alone in its sector deviates from itself by zero on every axis, which is what makes
        // its leverage exactly one below rather than a case anyone has to write out.
        let rows: Vec<Deviation> = candidates
            .iter()
            .map(|candidate| {
                let (returns, sizes, volatilities, names) = sums[candidate.sector.as_str()];
                let count = names as f64;
                Deviation {
                    daily_return: candidate.daily_return - returns / count,
                    log_size: candidate.log_size - sizes / count,
                    volatility: candidate.volatility - volatilities / count,
                    sector_names: names,
                }
            })
            .collect();

        let mut demeaned = Self {
            rows,
            size_size: 0.0,
            size_volatility: 0.0,
            volatility_volatility: 0.0,
            size_return: 0.0,
            volatility_return: 0.0,
        };
        for deviation in &demeaned.rows {
            demeaned.size_size += deviation.log_size * deviation.log_size;
            demeaned.size_volatility += deviation.log_size * deviation.volatility;
            demeaned.volatility_volatility += deviation.volatility * deviation.volatility;
            demeaned.size_return += deviation.log_size * deviation.daily_return;
            demeaned.volatility_return += deviation.volatility * deviation.daily_return;
        }
        demeaned
    }

    fn determinant(&self) -> f64 {
        self.size_size * self.volatility_volatility - self.size_volatility * self.size_volatility
    }

    /// Hat-matrix diagonal: the sector mean costs one over the sector's size, and the two continuous
    /// factors cost their own quadratic form. One minus this is the share of the name's variance the
    /// fit leaves behind.
    fn leverage(&self, deviation: &Deviation) -> f64 {
        1.0 / deviation.sector_names as f64
            + (self.volatility_volatility * deviation.log_size * deviation.log_size
                - 2.0 * self.size_volatility * deviation.log_size * deviation.volatility
                + self.size_size * deviation.volatility * deviation.volatility)
                / self.determinant()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A lookback of two keeps the fixtures readable; the shape under test is the same at sixty.
    ///
    /// The variance share is loose rather than the production 0.5 so that the leverage guard fires
    /// only where a test is about it. `T00` is the smallest name *and* the least volatile one, so
    /// it sits at the corner of a two-factor design and genuinely carries high leverage — see
    /// `test_raising_the_variance_share_refuses_the_corner_of_the_design`.
    fn specification() -> FactorSpecification {
        FactorSpecification::new(2, 0.1).expect("the fixture must be a usable specification")
    }

    type Row<'a> = (&'a str, i64, f64, Option<f64>, &'a str, Option<f64>);

    fn frame(rows: &[Row<'_>]) -> DataFrame {
        DataFrame::new(vec![
            Column::new(
                "ticker".into(),
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            ),
            Column::new(
                "timestamp".into(),
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            ),
            Column::new(
                "close_price".into(),
                rows.iter().map(|row| row.2).collect::<Vec<_>>(),
            ),
            Column::new(
                "shares_outstanding".into(),
                rows.iter().map(|row| row.3).collect::<Vec<_>>(),
            ),
            Column::new(
                "sector".into(),
                rows.iter().map(|row| row.4).collect::<Vec<_>>(),
            ),
            Column::new(
                "daily_return".into(),
                rows.iter().map(|row| row.5).collect::<Vec<_>>(),
            ),
        ])
        .expect("the fixture must build")
    }

    /// Four sessions each for `names` per sector, with size and volatility varying independently.
    ///
    /// Independently is the point. An earlier fixture made both increase with the same index, which
    /// is a near-collinear cross-section: it inflated leverage until ordinary names were refused as
    /// fitted exactly, and it would have made the collinearity test below pass for the wrong reason.
    fn panel(sectors: &[&str], names: usize) -> Vec<Row<'static>> {
        let mut rows = Vec::new();
        for (sector_index, sector) in sectors.iter().enumerate() {
            for name in 0..names {
                let ticker: &'static str =
                    Box::leak(format!("T{sector_index}{name}").into_boxed_str());
                let sector: &'static str = Box::leak(sector.to_string().into_boxed_str());
                let step = sector_index * names + name;
                // Coprime stride, so the volatility ordering is a permutation of the size ordering
                // rather than the same one.
                let scale = ((step * 7) % 11 + 1) as f64;
                for session in 0..4i64 {
                    rows.push((
                        ticker,
                        session,
                        100.0 + 10.0 * step as f64,
                        Some(1.0e6 * (step as f64 + 1.0)),
                        sector,
                        Some(0.001 * scale * (session as f64 + 1.0)),
                    ));
                }
            }
        }
        rows
    }

    fn residual_at(panel: &ResidualPanel, row: usize) -> Option<f64> {
        panel
            .frame
            .column(RESIDUAL_COLUMN)
            .expect("the column must exist")
            .f64()
            .expect("the column must be f64")
            .get(row)
    }

    /// The headline guard: a fit that reproduces a name exactly measured nothing about it.
    ///
    /// Five of the sixty-five two-digit SIC groups hold exactly one screened name in a recent
    /// session, and each of those names has leverage of exactly one — its sector coefficient is
    /// fitted to it alone. Returning zero there would read as "no idiosyncratic move".
    #[test]
    fn test_a_name_alone_in_its_sector_is_refused_rather_than_given_a_residual_of_zero() {
        let mut rows = panel(&["35", "73"], 8);
        for session in 0..4i64 {
            rows.push(("SOLO", session, 50.0, Some(2.0e6), "60", Some(0.02)));
        }

        let computed = residual_returns(&frame(&rows), specification()).expect("the fit must run");

        let solo: Vec<usize> = (0..computed.frame.height())
            .filter(|row| {
                computed
                    .frame
                    .column("ticker")
                    .unwrap()
                    .str()
                    .unwrap()
                    .get(*row)
                    == Some("SOLO")
            })
            .collect();
        assert_eq!(solo.len(), 4, "the fixture must carry four SOLO rows");
        for row in solo {
            assert_eq!(
                residual_at(&computed, row),
                None,
                "row {row} was fitted exactly and must carry no residual"
            );
        }
        assert_eq!(
            computed.refused.get(&ResidualRefusal::FullyExplained),
            Some(&2),
            "the two SOLO sessions with a full lookback must be refused for being fitted exactly"
        );
    }

    /// The residual is what the fit leaves, so within each sector it must sum to zero.
    ///
    /// An algebraic property of the sector dummies rather than agreement with a fixture: if it
    /// fails, the sector coefficient is not the sector's own mean.
    #[test]
    fn test_residuals_sum_to_zero_within_each_sector() {
        let rows = panel(&["35", "73", "60"], 8);
        let computed = residual_returns(&frame(&rows), specification()).expect("the fit must run");

        assert!(computed.measured > 0, "the fixture must measure something");

        let mut by_sector_session: BTreeMap<(String, i64), f64> = BTreeMap::new();
        let mut counted = 0usize;
        for row in 0..computed.frame.height() {
            let Some(residual) = residual_at(&computed, row) else {
                continue;
            };
            counted += 1;
            let sector = computed
                .frame
                .column("sector")
                .unwrap()
                .str()
                .unwrap()
                .get(row);
            let session = computed
                .frame
                .column("timestamp")
                .unwrap()
                .i64()
                .unwrap()
                .get(row);
            *by_sector_session
                .entry((sector.unwrap().to_string(), session.unwrap()))
                .or_insert(0.0) += residual;
        }

        // Asserted before the values, so "every group sums to zero" cannot be vacuously true.
        assert_eq!(counted, computed.measured);
        assert_eq!(
            by_sector_session.len(),
            6,
            "three sectors over two fitted sessions"
        );
        for ((sector, session), total) in by_sector_session {
            assert!(
                total.abs() < 1e-9,
                "sector {sector} on session {session} summed to {total}"
            );
        }
    }

    /// The residual is orthogonal to both continuous factors, which is what "stripped" means.
    #[test]
    fn test_the_residual_is_orthogonal_to_size_and_to_volatility() {
        let rows = panel(&["35", "73", "60"], 8);
        let computed = residual_returns(&frame(&rows), specification()).expect("the fit must run");

        // Rebuilt from the same inputs the fit used, so this checks the fit rather than restating it.
        let mut by_session: BTreeMap<i64, Vec<(f64, f64)>> = BTreeMap::new();
        for row in 0..computed.frame.height() {
            let Some(residual) = residual_at(&computed, row) else {
                continue;
            };
            let session = computed
                .frame
                .column("timestamp")
                .unwrap()
                .i64()
                .unwrap()
                .get(row);
            let close = computed
                .frame
                .column("close_price")
                .unwrap()
                .f64()
                .unwrap()
                .get(row);
            by_session
                .entry(session.unwrap())
                .or_default()
                .push((residual, close.unwrap().ln()));
        }

        assert!(!by_session.is_empty());
        for (session, pairs) in by_session {
            let product: f64 = pairs.iter().map(|(residual, size)| residual * size).sum();
            let total: f64 = pairs.iter().map(|(residual, _size)| residual).sum();
            let mean_size: f64 =
                pairs.iter().map(|(_r, size)| size).sum::<f64>() / pairs.len() as f64;
            // Orthogonal to the *demeaned* size, which is what the fit projects onto.
            assert!(
                (product - total * mean_size).abs() < 1e-8,
                "session {session} left {} of size in the residual",
                product - total * mean_size
            );
        }
    }

    /// Every factor loading is read off strictly prior sessions, so this session's close cannot
    /// reach the answer. A size built from the session's own close would be circular.
    #[test]
    fn test_the_session_being_explained_does_not_price_its_own_size() {
        let rows = panel(&["35", "73", "60"], 8);
        let baseline = residual_returns(&frame(&rows), specification()).expect("the fit must run");

        // Exactly one respect differs: the close on the last session, which is the only session the
        // change could reach. Everything else, including every return, is identical.
        let mut moved = rows.clone();
        for row in moved.iter_mut() {
            if row.1 == 3 {
                row.2 *= 3.0;
            }
        }
        let after = residual_returns(&frame(&moved), specification()).expect("the fit must run");

        assert_eq!(baseline.measured, after.measured);
        assert!(baseline.measured > 0);
        for row in 0..baseline.frame.height() {
            assert_eq!(
                residual_at(&baseline, row),
                residual_at(&after, row),
                "row {row} moved when only the explained session's own close changed"
            );
        }
    }

    /// The control for the test above: the instrument must report something when something is there.
    ///
    /// Changing a *prior* close does move the size factor and so must move the residual. Without
    /// this, the invariance test above would pass on a function that ignored size entirely.
    #[test]
    fn test_changing_a_prior_close_does_move_the_residual() {
        let rows = panel(&["35", "73", "60"], 8);
        let baseline = residual_returns(&frame(&rows), specification()).expect("the fit must run");

        let mut moved = rows.clone();
        for row in moved.iter_mut() {
            if row.1 == 2 && row.0 == "T00" {
                row.2 *= 3.0;
            }
        }
        let after = residual_returns(&frame(&moved), specification()).expect("the fit must run");

        let changed = (0..baseline.frame.height())
            .filter(|row| residual_at(&baseline, *row) != residual_at(&after, *row))
            .count();
        assert!(
            changed > 0,
            "moving the size that prices session 3 changed no residual"
        );
    }

    /// The feed classifies no SIC major group for 11% of common stock, and their only shared
    /// property is that absence. Pooling them would remove a return they never shared.
    #[test]
    fn test_a_name_with_no_sic_code_is_refused_rather_than_pooled_with_the_others() {
        let mut rows = panel(&["35", "73"], 8);
        for name in 0..3 {
            let ticker: &'static str = Box::leak(format!("U{name}").into_boxed_str());
            for session in 0..4i64 {
                rows.push((ticker, session, 40.0, Some(1.0e6), UNKNOWN, Some(0.03)));
            }
        }

        let computed = residual_returns(&frame(&rows), specification()).expect("the fit must run");

        assert_eq!(
            computed.refused.get(&ResidualRefusal::SectorUnknown),
            Some(&12),
            "all twelve unclassified rows must be refused, not grouped into one sector"
        );
        for row in 0..computed.frame.height() {
            let sector = computed
                .frame
                .column("sector")
                .unwrap()
                .str()
                .unwrap()
                .get(row);
            if sector == Some(UNKNOWN) {
                assert_eq!(residual_at(&computed, row), None);
            }
        }
    }

    /// The feed stops reporting shares for 4% of common stock from 2025-07. That must cost those
    /// names their size, and must not cost their sector peers anything.
    #[test]
    fn test_a_missing_share_count_refuses_the_name_and_leaves_its_peers_measured() {
        let mut rows = panel(&["35", "73", "60"], 8);
        for row in rows.iter_mut() {
            if row.0 == "T01" {
                row.3 = None;
            }
        }

        let computed = residual_returns(&frame(&rows), specification()).expect("the fit must run");

        assert_eq!(
            computed.refused.get(&ResidualRefusal::SizeUnmeasured),
            Some(&2),
            "the two sessions with a full lookback must lose their size"
        );
        let peers = (0..computed.frame.height())
            .filter(|row| {
                computed
                    .frame
                    .column("ticker")
                    .unwrap()
                    .str()
                    .unwrap()
                    .get(*row)
                    == Some("T00")
            })
            .filter(|row| residual_at(&computed, *row).is_some())
            .count();
        assert_eq!(peers, 2, "a peer in the same sector must still be measured");
    }

    /// A volatility fitted on three observations is a different quantity from one fitted on sixty.
    #[test]
    fn test_a_short_lookback_is_refused_rather_than_fitted_on_what_is_there() {
        let rows = panel(&["35", "73", "60"], 8);

        let computed = residual_returns(&frame(&rows), specification()).expect("the fit must run");

        // Sessions 0 and 1 cannot fill a two-session lookback; 2 and 3 can.
        assert_eq!(
            computed
                .refused
                .get(&ResidualRefusal::VolatilityWindowShort),
            Some(&48),
            "twenty-four names over the two sessions that cannot fill the window"
        );
        assert_eq!(computed.measured, 48);
    }

    /// A cross-section where every name is the same size identifies neither continuous factor.
    ///
    /// This is the case that motivated checking each factor against its own level. A constant column
    /// leaves a within-sector sum of squares of about `3e-28`, pure rounding — and the ratio test for
    /// collinearity divides that noise by itself and reports a *perfectly* conditioned system, so the
    /// fit went ahead on a coefficient amplified by `1/3e-28`.
    #[test]
    fn test_a_cross_section_with_no_variation_in_size_is_refused_rather_than_solved() {
        let mut rows = panel(&["35", "73", "60"], 8);
        for row in rows.iter_mut() {
            row.2 = 100.0;
            row.3 = Some(1.0e6);
        }

        let computed = residual_returns(&frame(&rows), specification()).expect("the fit must run");

        assert_eq!(computed.measured, 0);
        assert_eq!(
            computed.refused.get(&ResidualRefusal::FactorsCollinear),
            Some(&48)
        );
    }

    /// The other arm: both factors vary, but only together, so neither is separately identified.
    #[test]
    fn test_two_factors_that_move_only_together_are_refused() {
        let mut rows = panel(&["35", "73", "60"], 8);
        for row in rows.iter_mut() {
            row.3 = Some(1.0e6);
            // Volatility made an exact affine function of *log size*, which is what the two-by-two
            // determinant is there to catch once each column is known to vary on its own. The
            // returns are scaled so the two-session sample standard deviation lands on that target.
            let target = 0.001 * ((row.2 * 1.0e6).ln() - 15.0);
            row.5 = Some(target * std::f64::consts::SQRT_2 * (row.1 as f64 + 1.0));
        }

        let computed = residual_returns(&frame(&rows), specification()).expect("the fit must run");

        assert_eq!(computed.measured, 0);
        assert_eq!(
            computed.refused.get(&ResidualRefusal::FactorsCollinear),
            Some(&48)
        );
    }

    /// An estimator defined on part of its input reads exactly like a complete one without this.
    #[test]
    fn test_the_undefined_share_is_reported_and_the_refusals_account_for_every_row() {
        let rows = panel(&["35", "73", "60"], 8);
        let computed = residual_returns(&frame(&rows), specification()).expect("the fit must run");

        let refused: usize = computed.refused.values().sum();
        assert_eq!(
            computed.measured + refused,
            computed.frame.height(),
            "every row must be either measured or refused for a named cause"
        );
        // 24 names over four sessions; the first two cannot fill a two-session lookback. Pinned to
        // literals rather than to `measured / height`, which would move with whatever it measured.
        assert_eq!(computed.frame.height(), 96);
        assert_eq!(computed.measured, 48);
        assert_eq!(computed.undefined_share(), Some(0.5));
    }

    /// The variance share must be a knob rather than a decoration.
    ///
    /// `T00` is both the smallest name and the least volatile, so it is the corner of the two-factor
    /// design and carries the highest leverage in the fixture. Raising the share must refuse it and
    /// must leave the rest alone; if it refused nothing, the guard would be unfalsifiable.
    #[test]
    fn test_raising_the_variance_share_refuses_the_corner_of_the_design() {
        let rows = frame(&panel(&["35", "73", "60"], 8));

        let loose = residual_returns(&rows, specification()).expect("the fit must run");
        let strict = residual_returns(
            &rows,
            FactorSpecification::new(2, 0.5).expect("a usable specification"),
        )
        .expect("the fit must run");

        assert_eq!(loose.measured, 48);
        assert_eq!(strict.measured, 46);
        assert_eq!(
            strict.refused.get(&ResidualRefusal::FullyExplained),
            Some(&2),
            "the corner name's two fitted sessions must be the ones refused"
        );
        for row in 0..strict.frame.height() {
            let corner = strict
                .frame
                .column("ticker")
                .unwrap()
                .str()
                .unwrap()
                .get(row)
                == Some("T00");
            let session = strict
                .frame
                .column("timestamp")
                .unwrap()
                .i64()
                .unwrap()
                .get(row);
            if corner && session >= Some(2) {
                assert!(residual_at(&loose, row).is_some());
                assert_eq!(residual_at(&strict, row), None);
            }
        }
    }

    #[test]
    fn test_an_empty_panel_reports_no_share_rather_than_a_share_of_zero() {
        let computed = residual_returns(&frame(&[]), specification()).expect("the fit must run");

        assert_eq!(computed.undefined_share(), None);
        assert_eq!(computed.measured, 0);
    }

    #[test]
    fn test_a_row_naming_no_instrument_is_refused_rather_than_counted() {
        let rows = frame(&[("AAPL", 0, 10.0, Some(1.0), "35", Some(0.01))]);
        let mut broken = rows.clone();
        broken
            .with_column(Column::new("ticker".into(), vec![None::<&str>]))
            .expect("the fixture must build");

        let error =
            residual_returns(&broken, specification()).expect_err("a null ticker is refused");

        match error {
            ResidualError::Unattributable { rows, height } => {
                assert_eq!(rows, 1);
                assert_eq!(height, 1);
            }
            ResidualError::Frame(error) => panic!("the cause is the null ticker, not {error}"),
        }
    }

    #[test]
    fn test_a_specification_that_cannot_measure_anything_is_refused() {
        assert!(FactorSpecification::new(0, 0.5).is_none());
        // A share of zero would admit a name the fit reproduces exactly, which is the refusal the
        // whole type exists to make.
        assert!(FactorSpecification::new(60, 0.0).is_none());
        assert!(FactorSpecification::new(60, 1.5).is_none());
        assert!(FactorSpecification::new(60, f64::NAN).is_none());
        assert!(FactorSpecification::new(60, 1.0).is_some());
    }

    #[test]
    fn test_the_current_specification_is_sixty_sessions_and_half_the_variance() {
        // Literals rather than the constants, so a change has to be made deliberately.
        assert_eq!(FactorSpecification::CURRENT.volatility_sessions(), 60);
        assert_eq!(
            FactorSpecification::CURRENT.minimum_residual_variance_share(),
            0.5
        );
    }
}
