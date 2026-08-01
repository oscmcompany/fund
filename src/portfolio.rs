//! The strategy: which pairs to hold, how large, and when to let them go.
//!
//! [`pairs`] is the record — the long/short mapping and the signal that justified it. [`screen`]
//! decides what is worth holding, [`size`] how much, and [`risk`] whether the book can take it.
//! [`execute`] is the only module that sends an order, and [`evaluate`] is the five-minute pass
//! that drives the rest.
//!
//! [`account`] is the post-close half: what Alpaca says actually happened, written back as the
//! reference the next session's drawdown gate reads.

pub mod account;
pub mod evaluate;
pub mod execute;
pub mod pairs;
pub mod risk;
pub mod screen;
pub mod size;
