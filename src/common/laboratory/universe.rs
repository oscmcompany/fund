//! The names a session's study covers: the largest common stocks by dollar volume over the calendar month before the
//! session, among those every tick provider holds, as known before the session opened.

use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::num::NonZeroU16;

use chrono::{Datelike, Months};
use strum::IntoEnumIterator;

use crate::common::laboratory::event_table::TickProvider;
use crate::common::market::industry::Industry;
use crate::common::market::record::BarInterval;
use crate::common::market::security_details::{
    CentralIndexKey, IndustryCode, SecurityDetails, SecurityType,
};
use crate::common::market::trade_bars::TradeBar;
use crate::common::market::{DollarVolume, Symbol};
use crate::common::time::SessionDate;

/// How many names a universe keeps at most.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UniverseSize(NonZeroU16);

impl UniverseSize {
    pub fn new(names: NonZeroU16) -> Self {
        Self(names)
    }
}

/// One name of a universe, ranked first for the largest prior-month dollar volume.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Member {
    symbol: Symbol,
    rank: NonZeroU16,
    dollar_volume: DollarVolume,
    industry_code: Option<IndustryCode>,
    central_index_key: Option<CentralIndexKey>,
}

impl Member {
    pub fn symbol(&self) -> &Symbol {
        &self.symbol
    }

    pub fn rank(&self) -> NonZeroU16 {
        self.rank
    }

    pub fn dollar_volume(&self) -> DollarVolume {
        self.dollar_volume
    }

    pub fn industry_code(&self) -> Option<IndustryCode> {
        self.industry_code
    }

    pub fn central_index_key(&self) -> Option<CentralIndexKey> {
        self.central_index_key
    }

    /// The name's Fama-French industry, `Other` when its details carry no code.
    pub fn industry(&self) -> Industry {
        self.industry_code.map_or(Industry::Other, Industry::of)
    }
}

/// The names traded over the prior month and why each one left was not ranked.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Screened {
    pub traded: u32,
    /// Traded, but absent from the details or present without a security type.
    pub undescribed: u32,
    pub not_common_stock: u32,
    /// A common stock some tick provider does not hold.
    pub unheld: u32,
}

/// A session's ranked names, of which there is at least one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Universe {
    session: SessionDate,
    members: Vec<Member>,
    screened: Screened,
}

impl Universe {
    pub fn session(&self) -> SessionDate {
        self.session
    }

    /// Ranked first to last.
    pub fn members(&self) -> &[Member] {
        &self.members
    }

    pub fn screened(&self) -> Screened {
        self.screened
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum UniverseRefusal {
    #[error("{symbol}'s bar is {interval}, not daily")]
    NotDaily {
        symbol: Symbol,
        interval: BarInterval,
    },
    /// A bar of the session itself or later, which the session could not have known.
    #[error("{symbol}'s bar of {bar_session} is not before {session}")]
    Lookahead {
        symbol: Symbol,
        bar_session: SessionDate,
        session: SessionDate,
    },
    /// Details of the session itself or later, fetched no earlier than the session they would describe.
    #[error("details as of {as_of} are not before {session}")]
    DetailsNotBeforeSession {
        as_of: SessionDate,
        session: SessionDate,
    },
    #[error("no name survived the screen: {screened:?}")]
    Empty { screened: Screened },
}

/// The `size` common stocks of `details` with the most dollar volume in `daily_bars` over the calendar month before
/// `session`, among those `held` says every tick provider holds; ties rank alphabetically.
pub fn universe<'a>(
    session: SessionDate,
    daily_bars: impl IntoIterator<Item = &'a TradeBar>,
    details_as_of: SessionDate,
    details: &[SecurityDetails],
    held: impl Fn(TickProvider, &Symbol) -> bool,
    size: UniverseSize,
) -> Result<Universe, UniverseRefusal> {
    if details_as_of >= session {
        return Err(UniverseRefusal::DetailsNotBeforeSession {
            as_of: details_as_of,
            session,
        });
    }
    let month = session.date().with_day(1).unwrap_or(session.date());
    let prior_month = month - Months::new(1);
    let mut volumes: BTreeMap<&Symbol, DollarVolume> = BTreeMap::new();
    for bar in daily_bars {
        if bar.interval() != BarInterval::OneDay {
            return Err(UniverseRefusal::NotDaily {
                symbol: bar.symbol().clone(),
                interval: bar.interval(),
            });
        }
        let bar_session = SessionDate::at(bar.timestamp());
        if bar_session >= session {
            return Err(UniverseRefusal::Lookahead {
                symbol: bar.symbol().clone(),
                bar_session,
                session,
            });
        }
        if (prior_month..month).contains(&bar_session.date()) {
            let volume = volumes.entry(bar.symbol()).or_default();
            *volume = volume.plus(bar.sums().totals().dollar_volume());
        }
    }
    let described: BTreeMap<&Symbol, &SecurityDetails> = details
        .iter()
        .map(|detail| (detail.symbol(), detail))
        .collect();
    let mut screened = Screened::default();
    let mut ranked = Vec::new();
    for (symbol, dollar_volume) in volumes {
        screened.traded += 1;
        let Some(detail) = described.get(symbol) else {
            screened.undescribed += 1;
            continue;
        };
        match detail.security_type() {
            None => screened.undescribed += 1,
            Some(SecurityType::CommonStock) => {
                match TickProvider::iter().all(|provider| held(provider, symbol)) {
                    true => ranked.push((dollar_volume, *detail)),
                    false => screened.unheld += 1,
                }
            }
            Some(
                SecurityType::ExchangeTradedFund
                | SecurityType::Warrant
                | SecurityType::DepositaryReceipt
                | SecurityType::Fund
                | SecurityType::Unit
                | SecurityType::StructuredProduct
                | SecurityType::PreferredStock
                | SecurityType::ExchangeTradedSecurity
                | SecurityType::ExchangeTradedNote
                | SecurityType::ExchangeTradedVehicle
                | SecurityType::Right
                | SecurityType::Index,
            ) => screened.not_common_stock += 1,
        }
    }
    ranked.sort_by_key(|(dollar_volume, detail)| (Reverse(*dollar_volume), detail.symbol()));
    let members: Vec<Member> = ranked
        .into_iter()
        .zip((1..=size.0.get()).filter_map(NonZeroU16::new))
        .map(|((dollar_volume, detail), rank)| Member {
            symbol: detail.symbol().clone(),
            rank,
            dollar_volume,
            industry_code: detail.industry_code(),
            central_index_key: detail.central_index_key(),
        })
        .collect();
    match members.is_empty() {
        true => Err(UniverseRefusal::Empty { screened }),
        false => Ok(Universe {
            session,
            members,
            screened,
        }),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use chrono::NaiveDate;
    use proptest::prelude::*;

    use super::*;
    use crate::common::market::aggregate::TradeTotals;
    use crate::common::market::trade_bars::TradeSums;
    use crate::common::market::{Price, Shares, TradeCount};

    fn session(month: u32, day: u32) -> SessionDate {
        SessionDate::from_date(NaiveDate::from_ymd_opt(2025, month, day).unwrap())
    }

    fn symbol(raw: &str) -> Symbol {
        Symbol::new(raw).unwrap()
    }

    /// `raw`'s daily bar of `day`, trading `shares` at $10.
    fn bar(raw: &str, day: SessionDate, shares: u32) -> TradeBar {
        let shares = Shares::whole(u64::from(shares)).unwrap();
        let dollar_volume = DollarVolume::of(Price::from_dollars(10.0).unwrap(), shares);
        TradeBar::new(
            symbol(raw),
            BarInterval::OneDay,
            day.regular_close(),
            TradeSums::new(
                TradeTotals::new(TradeCount::new(1), shares, dollar_volume),
                None,
                None,
            ),
        )
        .unwrap()
    }

    fn details(raw: &str, kind: Option<SecurityType>) -> SecurityDetails {
        SecurityDetails::new(
            symbol(raw),
            kind,
            Some(IndustryCode::from_code(3571).unwrap()),
            None,
            None,
            None,
            None,
            None,
        )
    }

    fn size(names: u16) -> UniverseSize {
        UniverseSize::new(NonZeroU16::new(names).unwrap())
    }

    fn symbols(universe: &Universe) -> Vec<&str> {
        universe
            .members()
            .iter()
            .map(|member| member.symbol().as_str())
            .collect()
    }

    /// Only May's bars rank a June session, so June's own earlier sessions and April's are ignored; a name some
    /// provider does not hold, a fund and an undescribed name are each counted by cause.
    #[test]
    fn test_a_universe_ranks_the_prior_month_among_held_common_stocks() {
        let bars = [
            bar("AAA", session(5, 1), 100),
            bar("AAA", session(5, 30), 100),
            bar("BBB", session(5, 15), 300),
            bar("CCC", session(4, 30), 10_000),
            bar("CCC", session(6, 2), 10_000),
            bar("CCC", session(5, 2), 50),
            bar("SPY", session(5, 2), 99_000),
            bar("ZZZ", session(5, 2), 99_000),
            bar("UNH", session(5, 2), 99_000),
            bar("NEW", session(5, 2), 99_000),
        ];
        let described = [
            details("AAA", Some(SecurityType::CommonStock)),
            details("BBB", Some(SecurityType::CommonStock)),
            details("CCC", Some(SecurityType::CommonStock)),
            details("SPY", Some(SecurityType::ExchangeTradedFund)),
            details("UNH", Some(SecurityType::CommonStock)),
            details("ZZZ", None),
        ];
        let held = |provider: TickProvider, symbol: &Symbol| match provider {
            TickProvider::Massive => true,
            TickProvider::Alpaca => symbol.as_str() != "UNH",
        };
        let universe = universe(
            session(6, 16),
            &bars,
            session(6, 13),
            &described,
            held,
            size(2),
        )
        .unwrap();
        assert_eq!(symbols(&universe), ["BBB", "AAA"]);
        assert_eq!(
            universe
                .members()
                .iter()
                .map(|member| (member.rank().get(), member.dollar_volume().dollars()))
                .collect::<Vec<_>>(),
            [(1, 3000.0), (2, 2000.0)]
        );
        assert_eq!(
            universe.members()[0].industry(),
            Industry::BusinessEquipment
        );
        assert_eq!(
            universe.screened(),
            Screened {
                traded: 7,
                undescribed: 2,
                not_common_stock: 1,
                unheld: 1,
            }
        );
    }

    /// A bar of the session or after it, details postdating it, a bar not daily and a screen leaving nobody are each
    /// refused with what produced the refusal.
    #[test]
    fn test_a_universe_refuses_what_the_session_could_not_know() {
        let common = [details("AAA", Some(SecurityType::CommonStock))];
        let build = |bars: &[TradeBar], as_of: SessionDate| {
            universe(session(6, 16), bars, as_of, &common, |_, _| true, size(5))
        };
        assert_eq!(
            build(&[bar("AAA", session(6, 16), 1)], session(6, 13)),
            Err(UniverseRefusal::Lookahead {
                symbol: symbol("AAA"),
                bar_session: session(6, 16),
                session: session(6, 16),
            })
        );
        assert_eq!(
            build(&[], session(6, 16)),
            Err(UniverseRefusal::DetailsNotBeforeSession {
                as_of: session(6, 16),
                session: session(6, 16),
            })
        );
        let minute = TradeBar::new(
            symbol("AAA"),
            BarInterval::OneMinute,
            session(5, 2).regular_close(),
            *bar("AAA", session(5, 2), 1).sums(),
        )
        .unwrap();
        assert_eq!(
            build(&[minute], session(6, 13)),
            Err(UniverseRefusal::NotDaily {
                symbol: symbol("AAA"),
                interval: BarInterval::OneMinute,
            })
        );
        assert_eq!(
            build(&[bar("BBB", session(5, 2), 1)], session(6, 13)),
            Err(UniverseRefusal::Empty {
                screened: Screened {
                    traded: 1,
                    undescribed: 1,
                    not_common_stock: 0,
                    unheld: 0,
                },
            })
        );
    }

    const NAMES: [&str; 8] = ["AAA", "BBB", "CCC", "DDD", "EEE", "FFF", "GGG", "HHH"];

    /// Bars for eight names over May, with each name a common stock or not and held or not.
    fn market() -> impl Strategy<Value = (Vec<TradeBar>, Vec<SecurityDetails>, BTreeSet<String>)> {
        (
            prop::collection::vec((0..NAMES.len(), 1..31u32, 1..1000u32), 0..40),
            prop::collection::vec((any::<bool>(), any::<bool>()), NAMES.len()),
        )
            .prop_map(|(trades, kinds)| {
                let bars = trades
                    .into_iter()
                    .map(|(name, day, shares)| bar(NAMES[name], session(5, day), shares))
                    .collect();
                let described = NAMES
                    .iter()
                    .zip(&kinds)
                    .map(|(name, (common, _))| {
                        let kind = match common {
                            true => SecurityType::CommonStock,
                            false => SecurityType::PreferredStock,
                        };
                        details(name, Some(kind))
                    })
                    .collect();
                let held = NAMES
                    .iter()
                    .zip(&kinds)
                    .filter(|(_, (_, held))| *held)
                    .map(|(name, _)| name.to_string())
                    .collect();
                (bars, described, held)
            })
    }

    proptest! {
        /// The order bars arrive in changes nothing, a smaller universe is the larger one's head, ranks count from one
        /// with dollar volume never rising, every member is a held common stock, and every name traded is ranked or
        /// counted by its cause.
        #[test]
        fn test_a_universe_is_an_ordered_prefix_of_the_held_common_stocks(
            (bars, described, held) in market(),
            names in 1..8u16,
        ) {
            let held_by = |_: TickProvider, symbol: &Symbol| held.contains(symbol.as_str());
            let build = |bars: &[TradeBar], names: u16| {
                universe(session(6, 16), bars, session(6, 1), &described, held_by, size(names))
            };
            let mut reversed = bars.clone();
            reversed.reverse();
            prop_assert_eq!(build(&bars, names), build(&reversed, names));
            let every = match build(&bars, 8) {
                Ok(every) => every,
                Err(UniverseRefusal::Empty { screened }) => {
                    prop_assert_eq!(screened.traded, screened.not_common_stock + screened.unheld);
                    return Ok(());
                }
                Err(refusal) => return Err(TestCaseError::fail(format!("{refusal}"))),
            };
            let smaller = build(&bars, names).unwrap();
            prop_assert!(every.members().starts_with(smaller.members()));
            prop_assert_eq!(smaller.members().len(), every.members().len().min(usize::from(names)));
            for (index, member) in every.members().iter().enumerate() {
                prop_assert_eq!(usize::from(member.rank().get()), index + 1);
                prop_assert!(held.contains(member.symbol().as_str()));
                prop_assert!(described.iter().any(|detail| detail.symbol() == member.symbol()
                    && detail.security_type() == Some(SecurityType::CommonStock)));
            }
            prop_assert!(every.members().windows(2).all(|pair| pair[0].dollar_volume() >= pair[1].dollar_volume()));
            let screened = every.screened();
            prop_assert_eq!(
                screened.traded,
                screened.undescribed + screened.not_common_stock + screened.unheld
                    + u32::try_from(every.members().len()).unwrap()
            );
        }
    }
}
