//! The harness end to end: recall bands on a synthetic dataset, and a monthly cost built from
//! the engine's measured request counts.

use std::sync::Arc;

use seabed_adapter_objectstore_mem::{CountingObjectStore, MemObjectStore};
use seabed_domain::{NamespaceId, NamespaceKeys, TenantId};
use seabed_engine::namespace::{Namespace, Op};
use seabed_harness::cost::{delta, monthly_cost, Prices, Workload};
use seabed_harness::dataset::{Dataset, Filter, SyntheticSpec};
use seabed_harness::recall::{RecallReport, DEFAULT_BAND_EDGES};
use seabed_harness::truth::{exact_knn, Metric};

const K: usize = 10;

fn dataset() -> Dataset {
    Dataset::synthetic(&SyntheticSpec {
        n: 20_000,
        dims: 16,
        // Odd, so a document's cluster (id % clusters) does not follow its id's parity.
        clusters: 49,
        spread: 0.3,
        attr_cardinality: 100_000,
        seed: 1,
    })
}

/// One filter per default band: about 0.005%, 0.05%, 0.5%, 5% and 100% of the documents.
const FILTERS: [Filter; 5] = [
    Filter::AttrBelow(5),
    Filter::AttrBelow(50),
    Filter::AttrBelow(500),
    Filter::AttrBelow(5_000),
    Filter::None,
];

/// Runs `search` on every query and filter and reports recall per band.
fn measure(data: &Dataset, search: impl Fn(&[f32], Filter) -> Vec<usize>) -> RecallReport {
    let mut report = RecallReport::new(&DEFAULT_BAND_EDGES);
    for q in data.queries(20, 2) {
        for f in FILTERS {
            let truth = exact_knn(data, &q, K, Metric::L2, f);
            let got = search(&q, f);
            report.add_query(data.selectivity(f), &truth, &got, K);
        }
    }
    report
}

#[test]
fn an_exact_search_passes_every_band() {
    let data = dataset();
    let report = measure(&data, |q, f| exact_knn(&data, q, K, Metric::L2, f));
    let bands = report.bands();
    assert_eq!(
        bands.len(),
        5,
        "every filter lands in its own band: {bands:?}"
    );
    assert!(bands.iter().all(|b| b.min == 1.0));
    assert!(report.meets(0.90));
}

/// A search that only sees half the documents (even ids, spread over every cluster) loses
/// about half the neighbours of every query in every band with enough matches, and the
/// harness says so.
#[test]
fn a_search_that_misses_half_the_data_fails_the_criterion() {
    let data = dataset();
    let mut half = data.clone();
    for (i, a) in half.attrs.iter_mut().enumerate() {
        if i % 2 == 1 {
            *a = u32::MAX; // never passes a filter
        }
    }
    let report = measure(&data, |q, f| {
        let f = match f {
            Filter::None => Filter::AttrBelow(u32::MAX),
            f => f,
        };
        exact_knn(&half, q, K, Metric::L2, f)
    });
    assert!(!report.meets(0.90));
    // Bands above 0.1% keep at least 20 documents, so each query has a full top 10 to lose
    // half of: the mean is near 0.5 and no query loses everything.
    let wide: Vec<_> = report
        .bands()
        .into_iter()
        .filter(|b| b.lower >= 0.001)
        .collect();
    assert_eq!(wide.len(), 3, "{wide:?}");
    for b in wide {
        assert!((0.3..0.7).contains(&b.mean), "{b:?}");
        assert!(b.p5 > 0.0, "{b:?}");
    }
}

/// A strong read's measured requests, priced and scaled to a month. The counts match the
/// engine's own pinned cost (`read_all_request_cost_is_pinned`); this test checks the harness
/// carries them into money correctly.
#[test]
fn monthly_cost_of_strong_reads_from_measured_requests() {
    let store = Arc::new(CountingObjectStore::new(MemObjectStore::new()));
    let keys = NamespaceKeys::new(
        TenantId::new("acme").unwrap(),
        NamespaceId::new("ads").unwrap(),
    );
    let ns = Namespace::new(store.clone(), keys, 2).unwrap();
    for seg in 0..2u64 {
        ns.write(seg, &[Op::upsert(&format!("s{seg}"), vec![vec![1.0, 2.0]])])
            .unwrap();
        ns.flush().unwrap().unwrap();
    }
    ns.write(10, &[Op::upsert("t", vec![])]).unwrap();

    let before = store.stats();
    ns.read_all().unwrap();
    let per_query = delta(before, store.stats());
    // Manifest 3 GETs + 2 segments + 1 tail entry; 1 WAL LIST.
    assert_eq!((per_query.gets, per_query.lists, per_query.puts), (6, 1, 0));

    let w = Workload {
        queries_per_month: 1e6,
        per_query,
        ..Workload::default()
    };
    let c = monthly_cost(&Prices::S3_STANDARD_US_EAST_1, &w);
    // 1e6 * (6 * 0.0004 + 1 * 0.005) / 1000 = 7.4 USD.
    assert!((c.requests - 7.4).abs() < 1e-9, "{c:?}");
    assert_eq!(c.storage + c.compute, 0.0);
}
