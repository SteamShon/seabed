//! Datasets: vectors with one integer attribute each, and a synthetic generator.
//!
//! A `Dataset` is only data, whatever its source: the synthetic generator below, a file read
//! with [`crate::vecs`], or anything else through [`Dataset::new`]. Attributes for data that
//! has none come from [`crate::attrs`] (independent of the vectors, or correlated with them).

use crate::attrs;
use crate::rng::Rng;

/// Documents are identified by their index.
#[derive(Debug, Clone, PartialEq)]
pub struct Dataset {
    pub dims: usize,
    pub vectors: Vec<Vec<f32>>,
    pub attrs: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DatasetError {
    ZeroDims,
    /// `vectors` and `attrs` differ in length.
    AttrCount {
        vectors: usize,
        attrs: usize,
    },
    /// Vector `index` does not have `dims` components.
    WrongDims {
        index: usize,
        got: usize,
    },
    /// Vector `index` has a NaN or infinite component; distances would be meaningless.
    NonFinite {
        index: usize,
    },
}

impl std::fmt::Display for DatasetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DatasetError::ZeroDims => write!(f, "dims must be positive"),
            DatasetError::AttrCount { vectors, attrs } => {
                write!(f, "{vectors} vectors but {attrs} attributes")
            }
            DatasetError::WrongDims { index, got } => {
                write!(f, "vector {index} has {got} components")
            }
            DatasetError::NonFinite { index } => write!(f, "vector {index} is not finite"),
        }
    }
}

impl std::error::Error for DatasetError {}

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
    /// Checks that every vector has `dims` finite components and that there is one attribute
    /// per vector.
    pub fn new(dims: usize, vectors: Vec<Vec<f32>>, attrs: Vec<u32>) -> Result<Self, DatasetError> {
        if dims == 0 {
            return Err(DatasetError::ZeroDims);
        }
        if vectors.len() != attrs.len() {
            return Err(DatasetError::AttrCount {
                vectors: vectors.len(),
                attrs: attrs.len(),
            });
        }
        for (index, v) in vectors.iter().enumerate() {
            if v.len() != dims {
                return Err(DatasetError::WrongDims {
                    index,
                    got: v.len(),
                });
            }
            if !v.iter().all(|x| x.is_finite()) {
                return Err(DatasetError::NonFinite { index });
            }
        }
        Ok(Self {
            dims,
            vectors,
            attrs,
        })
    }

    pub fn len(&self) -> usize {
        self.vectors.len()
    }

    pub fn is_empty(&self) -> bool {
        self.vectors.is_empty()
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

/// Gaussian clusters with a uniform attribute in `0..attr_cardinality`, independent of the
/// vectors, so `Filter::AttrBelow(t)` keeps about `t / attr_cardinality` of the documents.
/// For an attribute correlated with the vectors, replace `data.attrs` with
/// [`attrs::correlated`].
#[derive(Debug, Clone, PartialEq)]
pub struct SyntheticSpec {
    pub n: usize,
    pub dims: usize,
    /// Number of cluster centres; document `i` belongs to centre `i % clusters`.
    pub clusters: usize,
    /// Standard deviation of a document around its centre (centres are standard normal).
    pub spread: f32,
    pub attr_cardinality: u32,
    pub seed: u64,
}

/// A generated dataset and what generated it, for drawing matching queries.
#[derive(Debug, Clone, PartialEq)]
pub struct Synthetic {
    pub data: Dataset,
    centres: Vec<Vec<f32>>,
    spread: f32,
}

impl SyntheticSpec {
    pub fn generate(&self) -> Synthetic {
        assert!(self.dims > 0 && self.clusters > 0 && self.attr_cardinality > 0);
        let mut rng = Rng::new(self.seed);
        let centres: Vec<Vec<f32>> = (0..self.clusters)
            .map(|_| (0..self.dims).map(|_| rng.normal() as f32).collect())
            .collect();
        let vectors: Vec<Vec<f32>> = (0..self.n)
            .map(|i| around(&mut rng, &centres[i % self.clusters], self.spread))
            .collect();
        // The attribute stream has its own generator, so changing how attributes are drawn
        // never changes the vectors.
        let attrs = attrs::uniform(self.n, self.attr_cardinality, self.seed ^ ATTR_SEED);
        Synthetic {
            data: Dataset {
                dims: self.dims,
                vectors,
                attrs,
            },
            centres,
            spread: self.spread,
        }
    }
}

/// Mixed into the spec's seed for the attribute stream.
const ATTR_SEED: u64 = 0xA77B_5EED_0000_0001;

impl Synthetic {
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

    fn dist(a: &[f32], b: &[f32]) -> f32 {
        a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum()
    }

    #[test]
    fn new_checks_shape_and_values() {
        let ok = Dataset::new(2, vec![vec![1.0, 2.0], vec![3.0, 4.0]], vec![0, 1]).unwrap();
        assert_eq!(ok.len(), 2);
        assert_eq!(Dataset::new(0, vec![], vec![]), Err(DatasetError::ZeroDims));
        assert_eq!(
            Dataset::new(2, vec![vec![1.0, 2.0]], vec![]),
            Err(DatasetError::AttrCount {
                vectors: 1,
                attrs: 0
            })
        );
        assert_eq!(
            Dataset::new(2, vec![vec![1.0, 2.0], vec![1.0]], vec![0, 0]),
            Err(DatasetError::WrongDims { index: 1, got: 1 })
        );
        assert_eq!(
            Dataset::new(1, vec![vec![0.0], vec![f32::NAN]], vec![0, 0]),
            Err(DatasetError::NonFinite { index: 1 })
        );
        assert_eq!(
            Dataset::new(1, vec![vec![f32::INFINITY]], vec![0]),
            Err(DatasetError::NonFinite { index: 0 })
        );
        assert!(Dataset::new(3, vec![], vec![]).unwrap().is_empty());
    }

    #[test]
    fn generation_is_deterministic_and_shaped() {
        let a = spec().generate();
        assert_eq!(a, spec().generate());
        assert_ne!(a, SyntheticSpec { seed: 43, ..spec() }.generate());
        let d = &a.data;
        assert_eq!(d.len(), 10_000);
        assert!(d.vectors.iter().all(|v| v.len() == 8));
        assert!(d.attrs.iter().all(|x| *x < 1000));
        // What `generate` builds is a valid dataset.
        assert_eq!(
            Dataset::new(d.dims, d.vectors.clone(), d.attrs.clone()).as_ref(),
            Ok(d)
        );
        assert_eq!(a.queries(5, 1), a.queries(5, 1));
        assert_eq!(a.queries(5, 1).len(), 5);
    }

    #[test]
    fn attr_below_keeps_about_the_chosen_fraction() {
        let d = spec().generate().data;
        assert_eq!(d.selectivity(Filter::None), 1.0);
        assert_eq!(d.selectivity(Filter::AttrBelow(0)), 0.0);
        assert_eq!(d.selectivity(Filter::AttrBelow(1000)), 1.0);
        let s = d.selectivity(Filter::AttrBelow(100));
        assert!((s - 0.1).abs() < 0.01, "{s}");
        assert_eq!(
            Dataset::new(1, vec![], vec![])
                .unwrap()
                .selectivity(Filter::None),
            0.0
        );
    }

    /// Queries come from every cluster, not one: each lands nearest its own centre, and 200
    /// of them reach all 10 centres.
    #[test]
    fn queries_spread_over_the_clusters() {
        let s = spec().generate();
        let mut hit = [false; 10];
        for q in s.queries(200, 9) {
            let nearest = (0..10)
                .min_by(|i, j| dist(&q, &s.centres[*i]).total_cmp(&dist(&q, &s.centres[*j])))
                .unwrap();
            hit[nearest] = true;
        }
        assert!(hit.iter().all(|h| *h), "{hit:?}");
    }

    #[test]
    fn documents_sit_near_their_centre() {
        let s = spec().generate();
        // Document i belongs to centre i % clusters; with spread 0.1 in 8 dims it is far
        // closer to its own centre than centres are to each other.
        for i in 0..100 {
            let own = dist(&s.data.vectors[i], &s.centres[i % 10]);
            let other = dist(&s.data.vectors[i], &s.centres[(i + 1) % 10]);
            assert!(own < other, "doc {i}");
        }
    }
}
