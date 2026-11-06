//! M0 evaluation harness: the pieces every benchmark needs, independent of any engine.
//!
//! - [`dataset`]: deterministic synthetic vectors with one integer attribute, so a filter's
//!   selectivity can be chosen exactly.
//! - [`truth`]: exact (brute-force) k nearest neighbours, with or without a filter. This is the
//!   ground truth recall is measured against.
//! - [`recall`]: recall@k per query and its summary per selectivity band, including the
//!   weakest band that the M0 criterion (3) is judged on (PLAN 2장).
//! - [`cost`]: monthly cost from measured object-store requests (the counters of
//!   `CountingObjectStore`), stored bytes and compute, for the M0 criterion (2).
//!
//! Everything is deterministic for a given seed, so a number in a report can be reproduced.

pub mod cost;
pub mod dataset;
pub mod recall;
pub mod rng;
pub mod truth;
