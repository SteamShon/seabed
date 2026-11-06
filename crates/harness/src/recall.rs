//! recall@k and its summary per selectivity band.
//!
//! The M0 criterion (3) is judged on the weakest band: a system that is good on average but
//! poor for very selective filters fails (PLAN 2장, 3장).

/// recall@k of one query: the fraction of the true top `k` found in the returned top `k`.
///
/// - Only the first `k` of each list count; a returned id counts once even if repeated.
/// - When fewer than `k` documents pass the filter, the denominator is how many do.
/// - When none pass, there is nothing to find and recall is 1. Returning documents that do
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
    pub queries: usize,
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
        }
    }

    pub fn add(&mut self, selectivity: f64, recall: f64) {
        assert!(
            (0.0..=1.0).contains(&selectivity),
            "selectivity {selectivity}"
        );
        assert!((0.0..=1.0).contains(&recall), "recall {recall}");
        let band = self
            .edges
            .iter()
            .position(|upper| selectivity <= *upper)
            .expect("the last edge is 1.0");
        self.samples[band].push(recall);
    }

    /// One summary per band that received at least one query, lowest selectivity first.
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

    /// The M0 criterion (3): mean recall in the weakest band is at least `threshold` (0.90 in
    /// PLAN 2장). False when there is nothing to judge.
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
        // Ties go to the more selective band.
        let mut t = RecallReport::new(&DEFAULT_BAND_EDGES);
        t.add(0.5, 0.9);
        t.add(0.005, 0.9);
        assert_eq!(t.weakest().unwrap().upper, 0.01);
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
