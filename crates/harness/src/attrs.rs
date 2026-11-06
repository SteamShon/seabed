//! Attribute generators: one `u32` per document, for filtering with `Filter::AttrBelow`.
//!
//! Both generators keep the attribute's marginal distribution uniform in `0..cardinality`, so
//! `AttrBelow(t)` keeps about `t / cardinality` of the documents either way. What changes is
//! *which* documents: independent of the vectors, or concentrated in one region of the vector
//! space. The second is the hard case of PLAN 10장 1번 (filters correlated with the
//! embedding, such as one advertiser's ads forming their own cluster): a query far from that
//! region finds its filtered neighbours far away, where a cluster-pruning index does not look.

use crate::rng::Rng;

/// Uniform in `0..cardinality`, independent of everything else.
pub fn uniform(n: usize, cardinality: u32, seed: u64) -> Vec<u32> {
    assert!(cardinality > 0, "cardinality 0");
    let mut rng = Rng::new(seed);
    (0..n)
        .map(|_| rng.below(u64::from(cardinality)) as u32)
        .collect()
}

/// An attribute that follows the vectors' position along one random direction.
///
/// Every document is ranked by its projection on a seeded random direction, and its
/// correlated attribute is its rank's quantile scaled to `0..cardinality`: low attributes
/// sit at one end of the space. With probability `correlation` a document takes that value,
/// otherwise a uniform one. So `correlation` 0 is [`uniform`] in distribution, and 1 puts
/// every document passing `AttrBelow(t)` in the slab of the lowest `t / cardinality`
/// projections.
pub fn correlated(vectors: &[Vec<f32>], cardinality: u32, correlation: f64, seed: u64) -> Vec<u32> {
    assert!(cardinality > 0, "cardinality 0");
    assert!(
        (0.0..=1.0).contains(&correlation),
        "correlation {correlation}"
    );
    let n = vectors.len();
    if n == 0 {
        return Vec::new();
    }
    let dims = vectors[0].len();
    let mut rng = Rng::new(seed);
    let direction: Vec<f64> = (0..dims).map(|_| rng.normal()).collect();
    let projection = |v: &[f32]| -> f64 {
        v.iter()
            .zip(&direction)
            .map(|(x, d)| f64::from(*x) * d)
            .sum()
    };
    // Rank by projection; ties by id, so the result does not depend on sort stability.
    let mut order: Vec<(f64, usize)> = vectors
        .iter()
        .enumerate()
        .map(|(i, v)| (projection(v), i))
        .collect();
    order.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
    let mut by_rank = vec![0u32; n];
    for (rank, (_, id)) in order.into_iter().enumerate() {
        // rank * cardinality / n, in u128 so it cannot overflow; always below cardinality.
        by_rank[id] = (rank as u128 * u128::from(cardinality) / n as u128) as u32;
    }
    by_rank
        .into_iter()
        .map(|a| {
            if rng.next_f64() < correlation {
                a
            } else {
                rng.below(u64::from(cardinality)) as u32
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataset::{Filter, SyntheticSpec};

    fn vectors() -> Vec<Vec<f32>> {
        SyntheticSpec {
            n: 5_000,
            dims: 8,
            clusters: 20,
            spread: 0.3,
            attr_cardinality: 1,
            seed: 3,
        }
        .generate()
        .data
        .vectors
    }

    fn kept(attrs: &[u32], t: u32) -> f64 {
        attrs
            .iter()
            .filter(|a| Filter::AttrBelow(t).accepts(**a))
            .count() as f64
            / attrs.len() as f64
    }

    /// Mean distance between documents that pass the filter: small when they are bunched in
    /// one region, close to the all-pairs mean when they are spread out.
    fn spread_of_passing(vectors: &[Vec<f32>], attrs: &[u32], t: u32) -> f64 {
        let ids: Vec<usize> = (0..vectors.len()).filter(|i| attrs[*i] < t).collect();
        let mut sum = 0.0;
        let mut pairs = 0u64;
        for (a, i) in ids.iter().enumerate() {
            for j in &ids[a + 1..] {
                let d: f32 = vectors[*i]
                    .iter()
                    .zip(&vectors[*j])
                    .map(|(x, y)| (x - y) * (x - y))
                    .sum();
                sum += f64::from(d).sqrt();
                pairs += 1;
            }
        }
        sum / pairs as f64
    }

    #[test]
    fn uniform_is_seeded_and_in_range() {
        let a = uniform(1000, 7, 1);
        assert_eq!(a, uniform(1000, 7, 1));
        assert_ne!(a, uniform(1000, 7, 2));
        assert!(a.iter().all(|x| *x < 7));
        assert!((0..7).all(|v| a.contains(&v)));
    }

    #[test]
    fn selectivity_does_not_depend_on_correlation() {
        let v = vectors();
        for c in [0.0, 0.5, 1.0] {
            let a = correlated(&v, 1000, c, 9);
            assert!(a.iter().all(|x| *x < 1000));
            for t in [10, 100, 500] {
                let s = kept(&a, t);
                let want = f64::from(t) / 1000.0;
                assert!((s - want).abs() < 0.02 + want * 0.3, "c {c} t {t}: {s}");
            }
        }
        // Fully correlated: exact quantiles, so the fraction is exact.
        let a = correlated(&v, 100, 1.0, 9);
        assert_eq!(kept(&a, 10), 0.1);
    }

    #[test]
    fn correlation_bunches_the_passing_documents_together() {
        let v = vectors();
        let independent = spread_of_passing(&v, &correlated(&v, 100, 0.0, 9), 5);
        let half = spread_of_passing(&v, &correlated(&v, 100, 0.5, 9), 5);
        let full = spread_of_passing(&v, &correlated(&v, 100, 1.0, 9), 5);
        assert!(
            full < half && half < independent,
            "{full} {half} {independent}"
        );
        assert!(full < independent * 0.8, "{full} vs {independent}");
    }

    #[test]
    fn correlated_is_deterministic_and_handles_edges() {
        let v = vectors();
        assert_eq!(correlated(&v, 50, 0.7, 4), correlated(&v, 50, 0.7, 4));
        assert_ne!(correlated(&v, 50, 1.0, 4), correlated(&v, 50, 1.0, 5));
        assert!(correlated(&[], 10, 1.0, 1).is_empty());
        // One document: rank 0, attribute 0.
        assert_eq!(correlated(&[vec![1.0]], 10, 1.0, 1), [0]);
        // Cardinality above n still stays in range.
        let a = correlated(&v[..10], u32::MAX, 1.0, 1);
        assert!(a.iter().all(|x| *x < u32::MAX));
    }
}
