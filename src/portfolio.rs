//! The strategy: which pairs to hold, how large, and when to let them go.
//!
//! [`execute`] is the only module that sends an order; [`evaluate`] is the five-minute pass that
//! drives the rest. [`account`] is the post-close half, writing back what Alpaca says actually
//! happened as the reference the next session's drawdown gate reads.

pub mod account;
pub mod evaluate;
pub mod execute;
pub mod pairs;
pub mod risk;
pub mod screen;
pub mod size;
