//! The strategy: which pairs to hold, how large, and when to let them go.
//!
//! [`evaluate`] drives the pass, [`execute`] sends every order, [`account`] writes back the fills.

pub mod account;
pub mod evaluate;
pub mod execute;
pub mod pairs;
pub mod risk;
pub mod screen;
pub mod size;
