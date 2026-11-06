//! A small seeded generator. The harness keeps its own instead of depending on `rand`, so a
//! dataset generated from a seed stays the same across dependency upgrades.

/// SplitMix64: every seed (0 included) gives a full-period, well-mixed stream.
#[derive(Debug, Clone)]
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(seed)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `[0, 1)`, with 53 bits of precision.
    pub fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    /// Uniform in `0..n`. `n` must be positive.
    pub fn below(&mut self, n: u64) -> u64 {
        assert!(n > 0, "below(0)");
        // Lemire's multiply-shift; the bias is below 2^-64 * n, negligible here.
        ((u128::from(self.next_u64()) * u128::from(n)) >> 64) as u64
    }

    /// Standard normal (Box-Muller).
    pub fn normal(&mut self) -> f64 {
        let u1 = 1.0 - self.next_f64(); // (0, 1], so ln is finite
        let u2 = self.next_f64();
        (-2.0 * u1.ln()).sqrt() * (std::f64::consts::TAU * u2).cos()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_same_stream_and_different_seeds_differ() {
        let a: Vec<u64> = (0..4)
            .map({
                let mut r = Rng::new(7);
                move |_| r.next_u64()
            })
            .collect();
        let b: Vec<u64> = (0..4)
            .map({
                let mut r = Rng::new(7);
                move |_| r.next_u64()
            })
            .collect();
        let c = Rng::new(8).next_u64();
        assert_eq!(a, b);
        assert_ne!(a[0], c);
    }

    /// Pins the stream, so a change to the generator (which would change every dataset) is a
    /// visible test change. Reference values from the SplitMix64 paper's algorithm, seed 0.
    #[test]
    fn stream_is_pinned() {
        let mut r = Rng::new(0);
        assert_eq!(r.next_u64(), 0xE220_A839_7B1D_CDAF);
        assert_eq!(r.next_u64(), 0x6E78_9E6A_A1B9_65F4);
    }

    #[test]
    fn below_stays_in_range_and_normal_has_unit_spread() {
        let mut r = Rng::new(1);
        let mut seen = [false; 5];
        for _ in 0..1000 {
            seen[r.below(5) as usize] = true;
        }
        assert!(seen.iter().all(|s| *s));

        let n = 20_000;
        let xs: Vec<f64> = (0..n).map(|_| r.normal()).collect();
        let mean = xs.iter().sum::<f64>() / n as f64;
        let var = xs.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n as f64;
        assert!(mean.abs() < 0.05, "mean {mean}");
        assert!((var - 1.0).abs() < 0.05, "var {var}");
    }
}
