//! Synthetic datasets: Gaussian clusters plus one integer attribute.
//!
//! Real datasets (and HCBGen-style filters) come later; this is enough to test the harness
//! itself and to give early engine work something reproducible to run on.
//!
//! The attribute is uniform in `0..attr_cardinality` and independent of the vectors, so
//! `Filter::AttrBelow(t)` keeps about `t / attr_cardinality` of the documents. Correlated
//! filters (PLAN 9장 1번) need a different generator.

use crate::rng::Rng;

#[derive(Debug, Clone, PartialEq)]
pub struct SyntheticSpec {
    pub n: usize,
    pub dims: usize,
    /// Number of cluster centres; documents are spread evenly over them.
    pub clusters: usize,
    /// Standard deviation of a document around its centre (centres are standard normal).
    pub spread: f32,
    pub attr_cardinality: u32,
    pub seed: u64,
}

/// Documents are identified by their index.
#[derive(Debug, Clone, PartialEq)]
pub struct Dataset {
    pub dims: usize,
    pub vectors: Vec<Vec<f32>>,
    pub attrs: Vec<u32>,
    pub attr_cardinality: u32,
    centres: Vec<Vec<f32>>,
    spread: f32,
}

/// Which documents a query may return.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Filter {
    None,
    /// `attr < t`.
    AttrBelow(u32),
}

impl Filter {
    pub fn accepts(self, attr: u32) -> bool {
        match self {
            Filter::None => true,
            Filter::AttrBelow(t) => attr < t,
        }
    }
}

impl Dataset {
    pub fn synthetic(spec: &SyntheticSpec) -> Self {
        assert!(spec.dims > 0 && spec.clusters > 0 && spec.attr_cardinality > 0);
        let mut rng = Rng::new(spec.seed);
        let centres: Vec<Vec<f32>> = (0..spec.clusters)
            .map(|_| (0..spec.dims).map(|_| rng.normal() as f32).collect())
            .collect();
        let mut vectors = Vec::with_capacity(spec.n);
        let mut attrs = Vec::with_capacity(spec.n);
        for i in 0..spec.n {
            let c = &centres[i % spec.clusters];
            vectors.push(around(&mut rng, c, spec.spread));
            attrs.push(rng.below(u64::from(spec.attr_cardinality)) as u32);
        }
        Self {
            dims: spec.dims,
            vectors,
            attrs,
            attr_cardinality: spec.attr_cardinality,
            centres,
            spread: spec.spread,
        }
    }

    pub fn len(&self) -> usize {
        self.vectors.len()
    }

    pub fn is_empty(&self) -> bool {
        self.vectors.is_empty()
    }

    /// Queries drawn from the same distribution as the documents (not copies of them), from
    /// their own seed so the query set can change without changing the data.
    pub fn queries(&self, n: usize, seed: u64) -> Vec<Vec<f32>> {
        let mut rng = Rng::new(seed);
        (0..n)
            .map(|_| {
                let c = &self.centres[rng.below(self.centres.len() as u64) as usize];
                around(&mut rng, c, self.spread)
            })
            .collect()
    }

    /// The fraction of documents the filter keeps, measured on this dataset.
    pub fn selectivity(&self, filter: Filter) -> f64 {
        if self.is_empty() {
            return 0.0;
        }
        let kept = self.attrs.iter().filter(|a| filter.accepts(**a)).count();
        kept as f64 / self.len() as f64
    }
}

fn around(rng: &mut Rng, centre: &[f32], spread: f32) -> Vec<f32> {
    centre
        .iter()
        .map(|x| x + spread * rng.normal() as f32)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> SyntheticSpec {
        SyntheticSpec {
            n: 10_000,
            dims: 8,
            clusters: 10,
            spread: 0.1,
            attr_cardinality: 1000,
            seed: 42,
        }
    }

    #[test]
    fn generation_is_deterministic_and_shaped() {
        let a = Dataset::synthetic(&spec());
        assert_eq!(a, Dataset::synthetic(&spec()));
        assert_ne!(a, Dataset::synthetic(&SyntheticSpec { seed: 43, ..spec() }));
        assert_eq!(a.len(), 10_000);
        assert!(a.vectors.iter().all(|v| v.len() == 8));
        assert!(a.attrs.iter().all(|x| *x < 1000));
        assert_eq!(a.queries(5, 1), a.queries(5, 1));
        assert_eq!(a.queries(5, 1).len(), 5);
    }

    #[test]
    fn attr_below_keeps_about_the_chosen_fraction() {
        let d = Dataset::synthetic(&spec());
        assert_eq!(d.selectivity(Filter::None), 1.0);
        assert_eq!(d.selectivity(Filter::AttrBelow(0)), 0.0);
        assert_eq!(d.selectivity(Filter::AttrBelow(1000)), 1.0);
        let s = d.selectivity(Filter::AttrBelow(100));
        assert!((s - 0.1).abs() < 0.01, "{s}");
    }

    #[test]
    fn documents_sit_near_their_centre() {
        let d = Dataset::synthetic(&spec());
        // Document i belongs to centre i % clusters; with spread 0.1 in 8 dims it is far
        // closer to its own centre than centres are to each other.
        let dist =
            |a: &[f32], b: &[f32]| -> f32 { a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum() };
        for i in 0..100 {
            let own = dist(&d.vectors[i], &d.centres[i % 10]);
            let other = dist(&d.vectors[i], &d.centres[(i + 1) % 10]);
            assert!(own < other, "doc {i}");
        }
    }
}
