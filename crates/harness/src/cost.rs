//! Monthly cost from measured object-store requests, stored bytes and compute.
//!
//! The request counts come from `CountingObjectStore` (failed attempts included, because they
//! are billed), measured per operation on a small run and scaled by the workload. Prices are
//! inputs: the defaults are AWS S3 Standard list prices (see the constant for where they were
//! checked). Transfer inside a region is free on S3 and is not modelled; neither is anything
//! turbopuffer charges (the baseline is an input to [`within_budget`]).
//!
//! Storage is billed per GB-month where AWS's GB is 2^30 bytes (its price list puts the
//! 50 TB tier edge at 51,200 GB), so the model divides stored bytes by 2^30.

use seabed_adapter_objectstore_mem::RequestStats;

/// Prices in USD.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Prices {
    /// PUT, COPY, POST and LIST requests, per 1,000.
    pub put_list_per_1000: f64,
    /// GET (and HEAD) requests, per 1,000.
    pub get_per_1000: f64,
    /// DELETE requests, per 1,000 (free on S3).
    pub delete_per_1000: f64,
    /// Storage, per GB-month, GB = 2^30 bytes.
    pub storage_per_gb_month: f64,
    /// Compute, per node-hour.
    pub compute_per_hour: f64,
}

impl Prices {
    /// AWS S3 Standard, us-east-1, first 50 TB. Matched against the AWS Price List API
    /// (`AmazonS3/current/us-east-1`, published 2026-11-04) in the PR #29 review. Compute is 0
    /// until a node type is chosen.
    pub const S3_STANDARD_US_EAST_1: Prices = Prices {
        put_list_per_1000: 0.005,
        get_per_1000: 0.0004,
        delete_per_1000: 0.0,
        storage_per_gb_month: 0.023,
        compute_per_hour: 0.0,
    };
}

/// What a month looks like. Request counts are per operation; the model multiplies them out.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct Workload {
    pub queries_per_month: f64,
    /// Requests one query makes (a measured `RequestStats` delta).
    pub per_query: RequestStats,
    pub writes_per_month: f64,
    /// Requests one write makes, including its share of flushes and compaction.
    pub per_write: RequestStats,
    /// Requests made once a month regardless of traffic (rebuilds, GC sweeps).
    pub fixed: RequestStats,
    /// Average bytes stored over the month.
    pub stored_bytes: f64,
    /// Node-hours used in the month.
    pub compute_hours: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct MonthlyCost {
    pub requests: f64,
    pub storage: f64,
    pub compute: f64,
}

impl MonthlyCost {
    pub fn total(&self) -> f64 {
        self.requests + self.storage + self.compute
    }
}

const GIB: f64 = (1u64 << 30) as f64;

/// The price of one batch of requests.
pub fn request_cost(prices: &Prices, r: &RequestStats) -> f64 {
    (r.puts + r.lists) as f64 * prices.put_list_per_1000 / 1000.0
        + r.gets as f64 * prices.get_per_1000 / 1000.0
        + r.deletes as f64 * prices.delete_per_1000 / 1000.0
}

pub fn monthly_cost(prices: &Prices, w: &Workload) -> MonthlyCost {
    MonthlyCost {
        requests: w.queries_per_month * request_cost(prices, &w.per_query)
            + w.writes_per_month * request_cost(prices, &w.per_write)
            + request_cost(prices, &w.fixed),
        storage: w.stored_bytes / GIB * prices.storage_per_gb_month,
        compute: w.compute_hours * prices.compute_per_hour,
    }
}

/// The M0 criterion (2): `ours` is at most `baseline * (1 + margin)` (margin 0.30 in PLAN
/// 2장). A cheaper result always passes.
pub fn within_budget(ours: f64, baseline: f64, margin: f64) -> bool {
    assert!(baseline >= 0.0 && margin >= 0.0);
    ours <= baseline * (1.0 + margin)
}

/// The requests made between two snapshots of the same counter.
pub fn delta(before: RequestStats, after: RequestStats) -> RequestStats {
    RequestStats {
        gets: after.gets - before.gets,
        puts: after.puts - before.puts,
        lists: after.lists - before.lists,
        deletes: after.deletes - before.deletes,
        bytes_read: after.bytes_read - before.bytes_read,
        bytes_written: after.bytes_written - before.bytes_written,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stats(gets: u64, puts: u64, lists: u64, deletes: u64) -> RequestStats {
        RequestStats {
            gets,
            puts,
            lists,
            deletes,
            ..RequestStats::default()
        }
    }

    const P: Prices = Prices {
        put_list_per_1000: 5.0,
        get_per_1000: 0.4,
        delete_per_1000: 1.0,
        storage_per_gb_month: 23.0,
        compute_per_hour: 2.0,
    };

    #[test]
    fn each_request_kind_has_its_price() {
        assert_eq!(request_cost(&P, &stats(1000, 0, 0, 0)), 0.4);
        assert_eq!(request_cost(&P, &stats(0, 1000, 0, 0)), 5.0);
        assert_eq!(
            request_cost(&P, &stats(0, 0, 1000, 0)),
            5.0,
            "LIST is priced as PUT"
        );
        assert_eq!(request_cost(&P, &stats(0, 0, 0, 1000)), 1.0);
    }

    #[test]
    fn a_month_multiplies_per_operation_counts_out() {
        let w = Workload {
            queries_per_month: 2000.0,
            per_query: stats(3, 0, 1, 0), // 3 * 0.0004 + 1 * 0.005 = 0.0062
            writes_per_month: 100.0,
            per_write: stats(1, 2, 0, 0), // 0.0004 + 0.01 = 0.0104
            fixed: stats(0, 0, 0, 1000),  // 1.0
            stored_bytes: 2.0 * GIB,
            compute_hours: 10.0,
        };
        let c = monthly_cost(&P, &w);
        assert!((c.requests - (2000.0 * 0.0062 + 100.0 * 0.0104 + 1.0)).abs() < 1e-9);
        assert_eq!(c.storage, 46.0);
        assert_eq!(c.compute, 20.0);
        assert!((c.total() - (c.requests + 66.0)).abs() < 1e-9);
    }

    #[test]
    fn storage_is_billed_per_gib() {
        let w = Workload {
            stored_bytes: GIB,
            ..Workload::default()
        };
        assert_eq!(monthly_cost(&P, &w).storage, P.storage_per_gb_month);
        // 10^9 bytes is less than one billed GB.
        let w = Workload {
            stored_bytes: 1e9,
            ..Workload::default()
        };
        assert!(monthly_cost(&P, &w).storage < P.storage_per_gb_month * 0.94);
    }

    #[test]
    fn budget_allows_the_margin_and_no_more() {
        assert!(within_budget(130.0, 100.0, 0.30));
        assert!(!within_budget(130.01, 100.0, 0.30));
        assert!(within_budget(10.0, 100.0, 0.0));
    }

    #[test]
    fn delta_subtracts_every_counter() {
        let a = RequestStats {
            gets: 1,
            puts: 2,
            lists: 3,
            deletes: 4,
            bytes_read: 5,
            bytes_written: 6,
        };
        let b = RequestStats {
            gets: 11,
            puts: 22,
            lists: 33,
            deletes: 44,
            bytes_read: 55,
            bytes_written: 66,
        };
        assert_eq!(
            delta(a, b),
            RequestStats {
                gets: 10,
                puts: 20,
                lists: 30,
                deletes: 40,
                bytes_read: 50,
                bytes_written: 60,
            }
        );
    }
}
