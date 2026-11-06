//! Exact k nearest neighbours by brute force: the ground truth for recall.

use crate::dataset::{Dataset, Filter};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Metric {
    /// Squared Euclidean distance; smaller is nearer.
    L2,
    /// Dot product; larger is nearer.
    InnerProduct,
    /// Cosine similarity; larger is nearer. A zero vector has similarity 0 with everything.
    Cosine,
}

impl Metric {
    /// A distance where smaller is nearer, for every metric. Computed in f64 so ground truth
    /// does not depend on summation order more than necessary.
    pub fn distance(self, a: &[f32], b: &[f32]) -> f64 {
        debug_assert_eq!(a.len(), b.len());
        match self {
            Metric::L2 => a
                .iter()
                .zip(b)
                .map(|(x, y)| {
                    let d = f64::from(*x) - f64::from(*y);
                    d * d
                })
                .sum(),
            Metric::InnerProduct => -dot(a, b),
            Metric::Cosine => {
                let (na, nb) = (dot(a, a).sqrt(), dot(b, b).sqrt());
                if na == 0.0 || nb == 0.0 {
                    0.0
                } else {
                    -dot(a, b) / (na * nb)
                }
            }
        }
    }
}

fn dot(a: &[f32], b: &[f32]) -> f64 {
    a.iter()
        .zip(b)
        .map(|(x, y)| f64::from(*x) * f64::from(*y))
        .sum()
}

/// The ids of the `k` documents nearest to `query` among those the filter keeps, nearest
/// first. Ties go to the smaller id, so the answer is unique. Fewer than `k` ids come back
/// when fewer documents pass the filter.
pub fn exact_knn(
    data: &Dataset,
    query: &[f32],
    k: usize,
    metric: Metric,
    filter: Filter,
) -> Vec<usize> {
    let mut scored: Vec<(f64, usize)> = data
        .vectors
        .iter()
        .zip(&data.attrs)
        .enumerate()
        .filter(|(_, (_, attr))| filter.accepts(**attr))
        .map(|(id, (v, _))| (metric.distance(query, v), id))
        .collect();
    let k = k.min(scored.len());
    if k == 0 {
        return Vec::new();
    }
    let by = |a: &(f64, usize), b: &(f64, usize)| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1));
    scored.select_nth_unstable_by(k - 1, by);
    scored.truncate(k);
    scored.sort_unstable_by(by);
    scored.into_iter().map(|(_, id)| id).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn tiny(vectors: Vec<Vec<f32>>, attrs: Vec<u32>) -> Dataset {
        Dataset::new(vectors[0].len(), vectors, attrs).unwrap()
    }

    #[test]
    fn metrics_order_neighbours_as_defined() {
        let d = tiny(
            vec![
                vec![1.0, 0.0],
                vec![0.0, 2.0],
                vec![3.0, 0.0],
                vec![-1.0, 0.0],
            ],
            vec![0, 1, 2, 3],
        );
        let q = [1.0, 0.0];
        // L2 from (1,0): id0 = 0, id1 = 1 + 4 = 5, id2 = 4, id3 = 4. So 0, then 2 and 3 tie at 4
        // (smaller id first), then 1.
        assert_eq!(exact_knn(&d, &q, 4, Metric::L2, Filter::None), [0, 2, 3, 1]);
        // Inner product with (1,0): 1, 0, 3, -1 -> 2, 0, 1, 3.
        assert_eq!(
            exact_knn(&d, &q, 4, Metric::InnerProduct, Filter::None),
            [2, 0, 1, 3]
        );
        // Cosine with (1,0): 1, 0, 1, -1 -> 0 and 2 tie (smaller id first), 1, 3.
        assert_eq!(
            exact_knn(&d, &q, 4, Metric::Cosine, Filter::None),
            [0, 2, 1, 3]
        );
    }

    #[test]
    fn filter_k_and_empty_cases() {
        let d = tiny(vec![vec![0.0], vec![1.0], vec![2.0]], vec![5, 1, 7]);
        assert_eq!(
            exact_knn(&d, &[0.0], 2, Metric::L2, Filter::AttrBelow(6)),
            [0, 1]
        );
        assert_eq!(
            exact_knn(&d, &[0.0], 5, Metric::L2, Filter::AttrBelow(2)),
            [1]
        );
        assert!(exact_knn(&d, &[0.0], 5, Metric::L2, Filter::AttrBelow(1)).is_empty());
        assert!(exact_knn(&d, &[0.0], 0, Metric::L2, Filter::None).is_empty());
        // A zero vector is not nearer or farther than anything under cosine.
        assert_eq!(Metric::Cosine.distance(&[0.0], &[1.0]), 0.0);
    }

    proptest! {
        /// Against a plain full sort: same ids, for any data, k and filter.
        #[test]
        fn matches_a_full_sort(
            pts in prop::collection::vec((-5i8..5, -5i8..5, 0u32..4), 1..40),
            q in (-5i8..5, -5i8..5),
            k in 0usize..45,
            t in 0u32..5,
        ) {
            let d = tiny(
                pts.iter().map(|(x, y, _)| vec![f32::from(*x), f32::from(*y)]).collect(),
                pts.iter().map(|(_, _, a)| *a).collect(),
            );
            let q = [f32::from(q.0), f32::from(q.1)];
            for metric in [Metric::L2, Metric::InnerProduct, Metric::Cosine] {
                let mut all: Vec<(f64, usize)> = (0..d.len())
                    .filter(|i| d.attrs[*i] < t)
                    .map(|i| (metric.distance(&q, &d.vectors[i]), i))
                    .collect();
                all.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
                let want: Vec<usize> = all.into_iter().take(k).map(|(_, i)| i).collect();
                prop_assert_eq!(exact_knn(&d, &q, k, metric, Filter::AttrBelow(t)), want);
            }
        }
    }
}
