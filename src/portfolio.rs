//! Portfolio service: selects statistical arbitrage pairs from ensemble
//! predictions, sizes positions with volatility parity, and executes rebalance
//! cycles against the Alpaca trading API. Driven by the Postgres event bus.

pub mod alpaca;
pub mod beta;
pub mod consolidation;
pub mod consumer;
pub mod database;
pub mod execution;
pub mod math;
pub mod rebalance;
pub mod regime;
pub mod sizing;
pub mod state;
pub mod statistical_arbitrage;
