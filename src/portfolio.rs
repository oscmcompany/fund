//! The strategy: which pairs to hold, how large, and when to let them go.
//!
//! [`execute`] is the only module that sends an order and [`evaluate`] is the pass driving it;
//! [`account`] is the post-close half that writes back what Alpaca says actually happened.

pub mod account;
pub mod evaluate;
pub mod execute;
pub mod pairs;
pub mod risk;
pub mod screen;
pub mod size;
