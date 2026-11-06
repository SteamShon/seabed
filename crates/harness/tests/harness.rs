//! The harness end to end: recall bands on a synthetic dataset, the same run from vector files,
//! correlated filters, and a monthly cost built from the engine's measured request counts.

use std::sync::Arc;

use seabed_adapter_objectstore_mem::{CountingObjectStore, MemObjectStore};
use seabed_domain::{NamespaceId, NamespaceKeys, TenantId};
use seabed_engine::namespace::{Namespace, Op};
use seabed_harness::attrs;
use seabed_harness::cost::{delta, monthly_cost, Prices, Workload};
use seabed_harness::dataset::{Dataset, Filter, Synthetic, SyntheticSpec};
use seabed_harness::recall::{RecallReport, DEFAULT_BAND_EDGES};
use seabed_harness::truth::{exact_knn, Metric};
use seabed_harness::vecs::{read_fvecs, read_truth_ivecs, write_fvecs, write_ivecs};

const K: usize = 10;

fn synthetic() -> Synthetic {
    SyntheticSpec {
        n: 20_000,
        dims: 16,
        // Odd, so a document's cluster (id % clusters) does not follow its id's parity.
        clusters: 49,
        spread: 0.3,
        attr_cardinality: 100_000,
        seed: 1,
    }
    .generate()
}

/// One filter per default band: about 0.005%, 0.05%, 0.5%, 5% and 100% of the documents.
const FILTERS: [Filter; 5] = [
    // With this seed exactly one document has an attribute below 6.
    Filter::AttrBelow(6),
    Filter::AttrBelow(50),
    Filter::AttrBelow(500),
    Filter::AttrBelow(5_000),
    Filter::None,
];

/// Runs `search` on every query and filter and reports recall per band.
fn measure(
    data: &Dataset,
    queries: &[Vec<f32>],
    search: impl Fn(&[f32], Filter) -> Vec<usize>,
) -> RecallReport {
    let mut report = RecallReport::new(&DEFAULT_BAND_EDGES);
    for q in queries {
        for f in FILTERS {
            let truth = exact_knn(data, q, K, Metric::L2, f);
            let got = search(q, f);
            report.add_query(data.selectivity(f), &truth, &got, K);
        }
    }
    report
}

#[test]
fn an_exact_search_passes_every_band() {
    let s = synthetic();
    let data = &s.data;
    let report = measure(data, &s.queries(20, 2), |q, f| {
        exact_knn(data, q, K, Metric::L2, f)
    });
    let bands = report.bands();
    assert_eq!(
        bands.len(),
        5,
        "every filter lands in its own band: {bands:?}"
    );
    assert!(bands.iter().all(|b| b.min == 1.0));
    assert!(report.meets(0.90));
    assert!(report.unjudged_bands().is_empty());
}

/// A search that only sees half the documents (even ids, spread over every cluster) loses
/// about half the neighbours of every query in every band with enough matches, and the
/// harness says so.
#[test]
fn a_search_that_misses_half_the_data_fails_the_criterion() {
    let s = synthetic();
    let data = &s.data;
    let mut half = data.clone();
    for (i, a) in half.attrs.iter_mut().enumerate() {
        if i % 2 == 1 {
            *a = u32::MAX; // never passes a filter
        }
    }
    let report = measure(data, &s.queries(20, 2), |q, f| {
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

/// The file workflow of the classic datasets: base vectors in `.fvecs`, ground truth in
/// `.ivecs`. Written and read back here (in memory), the run gives the same report as the
/// in-memory one.
#[test]
fn a_run_from_vector_files_matches_the_in_memory_run() {
    let s = synthetic();
    let queries = s.queries(10, 2);
    let filter = Filter::AttrBelow(5_000);

    let mut base = Vec::new();
    write_fvecs(&mut base, &s.data.vectors).unwrap();
    let mut query_file = Vec::new();
    write_fvecs(&mut query_file, &queries).unwrap();
    let truth: Vec<Vec<i32>> = queries
        .iter()
        .map(|q| {
            exact_knn(&s.data, q, K, Metric::L2, filter)
                .into_iter()
                .map(|id| i32::try_from(id).unwrap())
                .collect()
        })
        .collect();
    let mut truth_file = Vec::new();
    write_ivecs(&mut truth_file, &truth).unwrap();

    let vectors = read_fvecs(&base[..], None).unwrap();
    let loaded = Dataset::new(s.data.dims, vectors, s.data.attrs.clone()).unwrap();
    assert_eq!(loaded, s.data);
    let loaded_queries = read_fvecs(&query_file[..], None).unwrap();
    let loaded_truth = read_truth_ivecs(&truth_file[..], None).unwrap();

    // A search that returns the second half of the true top 10 only.
    let mut from_files = RecallReport::new(&DEFAULT_BAND_EDGES);
    let mut in_memory = RecallReport::new(&DEFAULT_BAND_EDGES);
    for (q, t) in loaded_queries.iter().zip(&loaded_truth) {
        let got = &t[K / 2..];
        from_files.add_query(loaded.selectivity(filter), t, got, K);
        let t2 = exact_knn(&s.data, q, K, Metric::L2, filter);
        in_memory.add_query(s.data.selectivity(filter), &t2, &t2[K / 2..], K);
    }
    assert_eq!(from_files.bands(), in_memory.bands());
    assert_eq!(from_files.weakest().unwrap().mean, 0.5);
}

/// Correlated attributes keep the filter's selectivity but put the passing documents in one
/// region, so a typical query's filtered neighbours are much farther away: the case a
/// cluster-pruning index must not miss.
#[test]
fn correlated_filters_push_filtered_neighbours_away() {
    let s = synthetic();
    let queries = s.queries(30, 2);
    let filter = Filter::AttrBelow(1_000); // 1% of 100_000
    let mean_nn_distance = |correlation: f64| -> (f64, f64) {
        let mut data = s.data.clone();
        data.attrs = attrs::correlated(&data.vectors, 100_000, correlation, 7);
        let total: f64 = queries
            .iter()
            .map(|q| {
                let nn = exact_knn(&data, q, 1, Metric::L2, filter)[0];
                Metric::L2.distance(q, &data.vectors[nn]).sqrt()
            })
            .sum();
        (data.selectivity(filter), total / queries.len() as f64)
    };
    let (sel_independent, independent) = mean_nn_distance(0.0);
    let (sel_correlated, correlated) = mean_nn_distance(1.0);
    assert!((sel_independent - 0.01).abs() < 0.003, "{sel_independent}");
    assert_eq!(sel_correlated, 0.01);
    assert!(
        correlated > independent * 1.5,
        "correlated {correlated} vs independent {independent}"
    );
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
