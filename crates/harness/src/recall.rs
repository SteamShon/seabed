//! recall@k and its summary per selectivity band.
//!
//! The recall half of the M0 criterion (3) is judged on the weakest band: a system that is
//! good on average but poor for very selective filters fails (PLAN 2장, 3장). The latency half
//! (p99 ≤ 1 s) is measured separately.
//!
//! A query whose filter keeps no document has nothing to find. It is counted per band but
//! left out of the recall statistics, so empty filters cannot lift the band the criterion is
//! judged on.

/// recall@k of one query: the fraction of the true top `k` found in the returned top `k`.
///
/// - Only the first `k` of each list count; a returned id counts once even if repeated.
/// - When fewer than `k` documents pass the filter, the denominator is how many do.
/// - When none pass, there is nothing to find and recall is 1. `RecallReport` keeps such
///   queries out of its statistics (`add_query`, `add_empty`). Returning documents that do
///   not pass the filter is a correctness bug for the engine's own tests, not a recall loss.
pub fn recall_at_k(truth: &[usize], got: &[usize], k: usize) -> f64 {
    let truth = &truth[..k.min(truth.len())];
    if truth.is_empty() {
        return 1.0;
    }
    let got = &got[..k.min(got.len())];
    let found = truth.iter().filter(|t| got.contains(t)).count();
    found as f64 / truth.len() as f64
}

/// Upper edges of the default selectivity bands: 0.01%, 0.1%, 1%, 10%, 100%. The first band
/// is the "0.01% 이하" band the M0 criterion names.
pub const DEFAULT_BAND_EDGES: [f64; 5] = [0.0001, 0.001, 0.01, 0.1, 1.0];

/// Summary of the queries whose filter selectivity fell in `(lower, upper]` (the first band
/// also takes selectivity 0).
#[derive(Debug, Clone, PartialEq)]
pub struct BandSummary {
    pub lower: f64,
    pub upper: f64,
    /// Queries with something to find; the statistics below are over these.
    pub queries: usize,
    /// Queries whose filter kept no document. Not part of the statistics.
    pub empty: usize,
    pub mean: f64,
    pub min: f64,
    /// 5th percentile (nearest rank): the recall that 95% of queries in the band reach.
    pub p5: f64,
}

/// Collects (selectivity, recall) per query and summarises them per band.
#[derive(Debug, Clone)]
pub struct RecallReport {
    edges: Vec<f64>,
    samples: Vec<Vec<f64>>,
    empty: Vec<usize>,
}

impl RecallReport {
    /// `edges` are the bands' upper edges, strictly increasing and ending at 1.0.
    pub fn new(edges: &[f64]) -> Self {
        assert!(!edges.is_empty(), "no bands");
        assert!(
            edges.windows(2).all(|w| w[0] < w[1]),
            "band edges must increase"
        );
        assert_eq!(edges.last(), Some(&1.0), "the last band must end at 1.0");
        Self {
            edges: edges.to_vec(),
            samples: vec![Vec::new(); edges.len()],
            empty: vec![0; edges.len()],
        }
    }

    /// Adds one query's recall. Only for queries with something to find: a query whose filter
    /// kept no document goes to `add_empty` (or use `add_query`, which decides).
    pub fn add(&mut self, selectivity: f64, recall: f64) {
        assert!((0.0..=1.0).contains(&recall), "recall {recall}");
        let band = self.band(selectivity);
        self.samples[band].push(recall);
    }

    /// Counts a query whose filter kept no document.
    pub fn add_empty(&mut self, selectivity: f64) {
        let band = self.band(selectivity);
        self.empty[band] += 1;
    }

    /// Scores one query against its ground truth and adds it where it belongs.
    pub fn add_query(&mut self, selectivity: f64, truth: &[usize], got: &[usize], k: usize) {
        if truth.is_empty() || k == 0 {
            self.add_empty(selectivity);
        } else {
            self.add(selectivity, recall_at_k(truth, got, k));
        }
    }

    /// Queries counted by `add_empty`, over all bands.
    pub fn empty_queries(&self) -> usize {
        self.empty.iter().sum()
    }

    fn band(&self, selectivity: f64) -> usize {
        assert!(
            (0.0..=1.0).contains(&selectivity),
            "selectivity {selectivity}"
        );
        self.edges
            .iter()
            .position(|upper| selectivity <= *upper)
            .expect("the last edge is 1.0")
    }

    /// One summary per band with at least one query that had something to find, lowest
    /// selectivity first. Bands that only saw empty queries are left out (`empty_queries`
    /// still counts them).
    pub fn bands(&self) -> Vec<BandSummary> {
        self.samples
            .iter()
            .enumerate()
            .filter(|(_, s)| !s.is_empty())
            .map(|(i, s)| {
                let mut sorted = s.clone();
                sorted.sort_by(f64::total_cmp);
                let n = sorted.len();
                // Nearest rank: the ceil(0.05 n)-th smallest, 1-based.
                let rank = (n as f64 * 0.05).ceil().max(1.0) as usize;
                BandSummary {
                    lower: if i == 0 { 0.0 } else { self.edges[i - 1] },
                    upper: self.edges[i],
                    queries: n,
                    empty: self.empty[i],
                    mean: sorted.iter().sum::<f64>() / n as f64,
                    min: sorted[0],
                    p5: sorted[rank - 1],
                }
            })
            .collect()
    }

    /// The band with the lowest mean recall; on a tie the more selective band. `None` when
    /// no query was added.
    pub fn weakest(&self) -> Option<BandSummary> {
        self.bands()
            .into_iter()
            .reduce(|a, b| if b.mean < a.mean { b } else { a })
    }

    /// The recall half of the M0 criterion (3): mean recall in the weakest band is at least
    /// `threshold` (0.90 in PLAN 2장). The p99 latency half is not judged here. False when
    /// there is nothing to judge.
    pub fn meets(&self, threshold: f64) -> bool {
        self.weakest().is_some_and(|w| w.mean >= threshold)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recall_counts_the_true_top_k_found() {
        assert_eq!(recall_at_k(&[1, 2, 3], &[3, 2, 1], 3), 1.0);
        assert_eq!(recall_at_k(&[1, 2, 3, 4], &[1, 9, 2, 8], 4), 0.5);
        // Only the first k of each list count.
        assert_eq!(recall_at_k(&[1, 2, 3, 4], &[3, 4, 1, 2], 2), 0.0);
        // A repeated id counts once.
        assert_eq!(recall_at_k(&[1, 2], &[1, 1], 2), 0.5);
        // Fewer matches than k: the denominator is the number of matches.
        assert_eq!(recall_at_k(&[7], &[7, 1, 2], 10), 1.0);
        assert_eq!(recall_at_k(&[7, 8], &[7], 10), 0.5);
        // Nothing passes the filter: nothing to find.
        assert_eq!(recall_at_k(&[], &[1], 10), 1.0);
        assert_eq!(recall_at_k(&[], &[], 10), 1.0);
        // Nothing returned.
        assert_eq!(recall_at_k(&[1], &[], 10), 0.0);
    }

    #[test]
    fn queries_land_in_the_band_their_selectivity_falls_in() {
        let mut r = RecallReport::new(&DEFAULT_BAND_EDGES);
        r.add(0.0, 0.5); // zero selectivity goes to the first band
        r.add(0.0001, 0.7); // an edge belongs to the band it closes
        r.add(0.00011, 0.9);
        r.add(1.0, 1.0);
        let bands = r.bands();
        assert_eq!(bands.len(), 3);
        assert_eq!(
            (bands[0].lower, bands[0].upper, bands[0].queries),
            (0.0, 0.0001, 2)
        );
        assert_eq!(
            (bands[1].lower, bands[1].upper, bands[1].queries),
            (0.0001, 0.001, 1)
        );
        assert_eq!(
            (bands[2].lower, bands[2].upper, bands[2].queries),
            (0.1, 1.0, 1)
        );
        assert!((bands[0].mean - 0.6).abs() < 1e-12);
        assert_eq!(bands[0].min, 0.5);
    }

    #[test]
    fn weakest_band_decides_the_criterion() {
        let mut r = RecallReport::new(&DEFAULT_BAND_EDGES);
        assert_eq!(r.weakest(), None);
        assert!(!r.meets(0.9), "nothing measured is not a pass");
        for _ in 0..10 {
            r.add(0.5, 1.0);
            r.add(0.005, 0.95);
        }
        assert!(r.meets(0.9));
        // A very selective band at 0.85 fails the whole run although the overall mean is high.
        r.add(0.00005, 0.85);
        let w = r.weakest().unwrap();
        assert_eq!(w.upper, 0.0001);
        assert!(!r.meets(0.9));
        // Exactly at the threshold passes ("at least").
        let mut at = RecallReport::new(&[1.0]);
        at.add(0.5, 0.5);
        assert!(at.meets(0.5));
        assert!(!at.meets(0.5 + 1e-9));
        // Ties go to the more selective band.
        let mut t = RecallReport::new(&DEFAULT_BAND_EDGES);
        t.add(0.5, 0.9);
        t.add(0.005, 0.9);
        assert_eq!(t.weakest().unwrap().upper, 0.01);
    }

    #[test]
    fn empty_truth_queries_do_not_lift_the_weakest_band() {
        let mut r = RecallReport::new(&DEFAULT_BAND_EDGES);
        for _ in 0..10 {
            r.add(0.5, 1.0);
            r.add_query(0.00005, &[1, 2], &[1, 9], 2); // 0.5
            r.add_query(0.0, &[], &[3], 2); // nothing to find
            r.add_empty(0.00001);
        }
        let w = r.weakest().unwrap();
        assert_eq!((w.upper, w.queries, w.empty), (0.0001, 10, 20));
        assert_eq!(w.mean, 0.5);
        assert!(!r.meets(0.9));
        assert_eq!(r.empty_queries(), 20);
        // k = 0 has nothing to find either.
        r.add_query(0.5, &[1], &[1], 0);
        assert_eq!(r.empty_queries(), 21);
        // A band that only saw empty queries is not reported and cannot pass or fail.
        let mut only_empty = RecallReport::new(&DEFAULT_BAND_EDGES);
        only_empty.add_empty(0.0);
        assert!(only_empty.bands().is_empty());
        assert!(!only_empty.meets(0.9));
        assert_eq!(only_empty.empty_queries(), 1);
    }

    /// The criterion uses the band mean, not its minimum or low percentile.
    #[test]
    fn criterion_is_judged_on_the_band_mean() {
        let mut r = RecallReport::new(&DEFAULT_BAND_EDGES);
        // Band (1%, 10%]: mean 0.922, min and p5 0.5.
        r.add(0.05, 0.5);
        for _ in 0..9 {
            r.add(0.05, 0.968_888_888_888_888_9);
        }
        // Band (10%, 100%]: higher mean 0.95, but a lower min and p5 (0.4).
        r.add(0.5, 0.4);
        for _ in 0..19 {
            r.add(0.5, (19.0 - 0.4) / 19.0);
        }
        let w = r.weakest().unwrap();
        assert_eq!(
            w.upper, 0.1,
            "the weakest band is the one with the lowest mean"
        );
        assert!((w.mean - 0.922).abs() < 1e-9, "{}", w.mean);
        assert_eq!((w.min, w.p5), (0.5, 0.5));
        assert!(r.meets(0.9));
    }

    #[test]
    fn p5_is_the_nearest_rank_fifth_percentile() {
        let mut r = RecallReport::new(&[1.0]);
        // 40 queries: two at 0.1, the rest at 1.0. ceil(0.05 * 40) = 2nd smallest = 0.1.
        for i in 0..40 {
            r.add(0.5, if i < 2 { 0.1 } else { 1.0 });
        }
        assert_eq!(r.bands()[0].p5, 0.1);
        // One more good query: ceil(0.05 * 41) = 3rd smallest = 1.0.
        r.add(0.5, 1.0);
        assert_eq!(r.bands()[0].p5, 1.0);
    }
}
