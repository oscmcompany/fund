//! The fees a fill pays per share under each fee regime; every rate is a parameter, since published schedules change
//! and a negotiated one is private.

use serde::{Deserialize, Serialize};

use super::{PerShare, PerShareRefusal};
use crate::common::book::Side;
use crate::common::market::{Dollars, PRICE_SCALE, Price};

/// A million dollars in millionths, the notional a sale rate is quoted against.
const RATE_NOTIONAL: u128 = 1_000_000 * PRICE_SCALE.unsigned_abs() as u128;

/// Whether a fill took liquidity resting on the book or was the resting liquidity another order took.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    strum::Display,
    strum::EnumString,
    strum::IntoStaticStr,
    strum::EnumIter,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum Liquidity {
    Removing,
    Adding,
}

/// The fee a refused rate stood for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display)]
#[strum(serialize_all = "snake_case")]
pub enum FeeComponent {
    SalePerShare,
    Commission,
    AccessFee,
    Rebate,
    AccessFeeCap,
}

/// Why a fee rate was refused, with the rate offered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum FeeRefusal {
    /// A charge or credit is a magnitude; its sign comes from the regime.
    #[error("the {component} of {rate} is negative")]
    Negative {
        component: FeeComponent,
        rate: PerShare,
    },
    #[error("a sale rate of {rate} per million dollars charges more than the notional")]
    RateAboveNotional { rate: Dollars },
}

fn magnitude(component: FeeComponent, rate: PerShare) -> Result<PerShare, FeeRefusal> {
    match rate >= PerShare::ZERO {
        true => Ok(rate),
        false => Err(FeeRefusal::Negative { component, rate }),
    }
}

/// What regulators levy on a sale under every regime: a rate on its notional (the SEC's Section 31 fee) and a charge
/// per share (FINRA's trading activity fee).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegulatoryFees {
    sale_rate_per_million: Dollars,
    sale_per_share: PerShare,
}

impl RegulatoryFees {
    pub fn new(
        sale_rate_per_million: Dollars,
        sale_per_share: PerShare,
    ) -> Result<Self, FeeRefusal> {
        if u128::from(sale_rate_per_million.millionths()) > RATE_NOTIONAL {
            return Err(FeeRefusal::RateAboveNotional {
                rate: sale_rate_per_million,
            });
        }
        Ok(Self {
            sale_rate_per_million,
            sale_per_share: magnitude(FeeComponent::SalePerShare, sale_per_share)?,
        })
    }

    /// The levy on one share sold at `price`, rounded up so rounding never flatters; nothing on a buy.
    pub fn per_share(self, side: Side, price: Price) -> Result<PerShare, PerShareRefusal> {
        match side {
            Side::Buy => Ok(PerShare::ZERO),
            Side::Sell => {
                let notional = u128::from(price.ticks_unsigned())
                    * u128::from(self.sale_rate_per_million.millionths());
                // At most the price, which a rate no larger than the notional keeps inside `PerShare`'s bound.
                let levy = notional.div_ceil(RATE_NOTIONAL) as i64;
                PerShare(levy).plus(self.sale_per_share)
            }
        }
    }
}

/// A broker's per-share commission with the venue's access fee charged on a removing fill and its rebate credited on
/// an adding one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PassThroughSchedule {
    commission: PerShare,
    access_fee: PerShare,
    rebate: PerShare,
}

impl PassThroughSchedule {
    pub fn new(
        commission: PerShare,
        access_fee: PerShare,
        rebate: PerShare,
    ) -> Result<Self, FeeRefusal> {
        Ok(Self {
            commission: magnitude(FeeComponent::Commission, commission)?,
            access_fee: magnitude(FeeComponent::AccessFee, access_fee)?,
            rebate: magnitude(FeeComponent::Rebate, rebate)?,
        })
    }

    fn per_share(
        self,
        liquidity: Liquidity,
        access_fee: PerShare,
    ) -> Result<PerShare, PerShareRefusal> {
        match liquidity {
            Liquidity::Removing => self.commission.plus(access_fee),
            Liquidity::Adding => self.commission.minus(self.rebate),
        }
    }
}

/// The most a venue may charge to remove a share, never negative.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AccessFeeCap(PerShare);

impl AccessFeeCap {
    pub fn new(cap: PerShare) -> Result<Self, FeeRefusal> {
        magnitude(FeeComponent::AccessFeeCap, cap).map(Self)
    }
}

/// The fee arms a cost is read under, each carrying its own rates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum FeeRegime {
    /// Commission-free: the broker keeps the venue's fees and rebates, so only the regulatory levy reaches us.
    CommissionFree { regulatory: RegulatoryFees },
    /// A commission with the venue's fee and rebate passed through, as the broker's Cost Plus plan does.
    PassThrough {
        regulatory: RegulatoryFees,
        schedule: PassThroughSchedule,
    },
    /// `PassThrough` with the access fee held at `cap`, as Rule 610's lowered cap does. Below a dollar the published cap is
    /// a share of the price, which this arm does not apply.
    AccessFeeCapped {
        regulatory: RegulatoryFees,
        schedule: PassThroughSchedule,
        cap: AccessFeeCap,
    },
}

impl FeeRegime {
    /// The fees on one share filled at `price`, signed so a credit is negative, refused when they pass the bound.
    pub fn per_share(
        self,
        liquidity: Liquidity,
        side: Side,
        price: Price,
    ) -> Result<PerShare, PerShareRefusal> {
        match self {
            Self::CommissionFree { regulatory } => regulatory.per_share(side, price),
            Self::PassThrough {
                regulatory,
                schedule,
            } => schedule
                .per_share(liquidity, schedule.access_fee)?
                .plus(regulatory.per_share(side, price)?),
            Self::AccessFeeCapped {
                regulatory,
                schedule,
                cap,
            } => schedule
                .per_share(liquidity, schedule.access_fee.min(cap.0))?
                .plus(regulatory.per_share(side, price)?),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::str::FromStr;

    use proptest::prelude::*;
    use strum::IntoEnumIterator;

    fn per_share(millionths: i64) -> PerShare {
        PerShare::from_millionths(millionths).unwrap()
    }

    fn price(dollars: i64) -> Price {
        Price::from_ticks(dollars * PRICE_SCALE).unwrap()
    }

    /// Synthetic rates: ten dollars a million on the notional and a hundred millionths a share on a sale.
    fn regulatory() -> RegulatoryFees {
        RegulatoryFees::new(Dollars::from_millionths(10_000_000), per_share(100)).unwrap()
    }

    fn schedule() -> PassThroughSchedule {
        PassThroughSchedule::new(per_share(2_000), per_share(3_000), per_share(2_500)).unwrap()
    }

    #[test]
    fn test_the_regulatory_levy_falls_on_the_sale_alone_and_rounds_up() {
        assert_eq!(
            regulatory().per_share(Side::Buy, price(100)),
            Ok(per_share(0))
        );
        // $100 at $10 a million is $0.001 a share, plus the per-share charge.
        assert_eq!(
            regulatory().per_share(Side::Sell, price(100)),
            Ok(per_share(1_100))
        );
        let tiny = RegulatoryFees::new(Dollars::from_millionths(1), per_share(0)).unwrap();
        assert_eq!(
            tiny.per_share(Side::Sell, Price::from_ticks(1).unwrap()),
            Ok(per_share(1))
        );
    }

    #[test]
    fn test_each_regime_charges_its_own_arm() {
        let base = FeeRegime::CommissionFree {
            regulatory: regulatory(),
        };
        let pass_through = FeeRegime::PassThrough {
            regulatory: regulatory(),
            schedule: schedule(),
        };
        let capped = FeeRegime::AccessFeeCapped {
            regulatory: regulatory(),
            schedule: schedule(),
            cap: AccessFeeCap::new(per_share(1_000)).unwrap(),
        };
        let read = |regime: FeeRegime, liquidity, side| {
            regime.per_share(liquidity, side, price(100)).unwrap()
        };
        assert_eq!(read(base, Liquidity::Removing, Side::Buy), per_share(0));
        assert_eq!(read(base, Liquidity::Adding, Side::Sell), per_share(1_100));
        assert_eq!(
            read(pass_through, Liquidity::Removing, Side::Buy),
            per_share(5_000)
        );
        assert_eq!(
            read(pass_through, Liquidity::Adding, Side::Buy),
            per_share(-500)
        );
        assert_eq!(
            read(pass_through, Liquidity::Removing, Side::Sell),
            per_share(6_100)
        );
        assert_eq!(
            read(capped, Liquidity::Removing, Side::Buy),
            per_share(3_000)
        );
        assert_eq!(read(capped, Liquidity::Adding, Side::Buy), per_share(-500));
        assert_eq!(capped.to_string(), "access_fee_capped");
    }

    #[test]
    fn test_negative_rates_and_a_rate_past_the_notional_are_refused() {
        assert_eq!(
            PassThroughSchedule::new(per_share(0), per_share(-1), per_share(0)),
            Err(FeeRefusal::Negative {
                component: FeeComponent::AccessFee,
                rate: per_share(-1)
            })
        );
        assert_eq!(
            AccessFeeCap::new(per_share(-3)),
            Err(FeeRefusal::Negative {
                component: FeeComponent::AccessFeeCap,
                rate: per_share(-3)
            })
        );
        let whole = Dollars::from_millionths(1_000_000_000_000);
        assert!(RegulatoryFees::new(whole, per_share(0)).is_ok());
        let past = Dollars::from_millionths(1_000_000_000_001);
        assert_eq!(
            RegulatoryFees::new(past, per_share(0)),
            Err(FeeRefusal::RateAboveNotional { rate: past })
        );
    }

    #[test]
    fn test_fees_past_the_bound_are_refused_rather_than_returned() {
        let bound = 10_000_000_000_000;
        let schedule =
            PassThroughSchedule::new(per_share(bound), per_share(1), per_share(0)).unwrap();
        let regime = FeeRegime::PassThrough {
            regulatory: RegulatoryFees::new(Dollars::from_millionths(0), per_share(0)).unwrap(),
            schedule,
        };
        assert_eq!(
            regime.per_share(Liquidity::Removing, Side::Buy, price(100)),
            Err(PerShareRefusal {
                millionths: bound + 1
            })
        );
        assert_eq!(
            regime.per_share(Liquidity::Adding, Side::Buy, price(100)),
            Ok(per_share(bound))
        );
        let levied = RegulatoryFees::new(Dollars::from_millionths(0), per_share(bound)).unwrap();
        assert_eq!(
            FeeRegime::PassThrough {
                regulatory: levied,
                schedule: PassThroughSchedule::new(per_share(0), per_share(1), per_share(0))
                    .unwrap(),
            }
            .per_share(Liquidity::Removing, Side::Sell, price(100)),
            Err(PerShareRefusal {
                millionths: bound + 1
            })
        );
    }

    #[test]
    fn test_liquidity_names_agree_between_strum_and_serde() {
        for liquidity in Liquidity::iter() {
            let serialized = serde_json::to_string(&liquidity).unwrap();
            assert_eq!(serialized, format!("\"{liquidity}\""));
            assert_eq!(Liquidity::from_str(&liquidity.to_string()), Ok(liquidity));
        }
        assert_eq!(Liquidity::Removing.to_string(), "removing");
    }

    proptest! {
        /// The cap only lowers a removing fill's fee, never touches an adding one, and binds only above itself.
        #[test]
        fn test_the_cap_never_charges_more_than_passing_through(
            access in 0i64..10_000,
            cap in 0i64..10_000,
            ticks in 1i64..1_000_000_000,
            sell in any::<bool>(),
        ) {
            let schedule = PassThroughSchedule::new(per_share(2_000), per_share(access), per_share(1_000)).unwrap();
            let side = match sell { true => Side::Sell, false => Side::Buy };
            let price = Price::from_ticks(ticks).unwrap();
            let open = FeeRegime::PassThrough { regulatory: regulatory(), schedule };
            let capped = FeeRegime::AccessFeeCapped { regulatory: regulatory(), schedule, cap: AccessFeeCap::new(per_share(cap)).unwrap() };
            let removing = |regime: FeeRegime| regime.per_share(Liquidity::Removing, side, price).unwrap();
            let adding = |regime: FeeRegime| regime.per_share(Liquidity::Adding, side, price).unwrap();
            prop_assert_eq!(removing(open).millionths() - removing(capped).millionths(), (access - cap).max(0));
            prop_assert_eq!(adding(open), adding(capped));
        }
    }
}
