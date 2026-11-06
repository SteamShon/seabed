use std::io::Write;
use std::ops::Range;
use std::sync::Mutex;

use seabed_domain::{ETag, Key};
use seabed_ports::{ObjectMeta, ObjectStore, ObjectStoreCaps, PutCondition, StoreError};

use crate::lock;

/// Probabilities are in parts per million. `put`, `get`, `get_range` and `list` are affected;
/// `delete` is passed through (latency injection is not implemented yet).
#[derive(Debug, Clone, Copy)]
pub struct FaultConfig {
    pub seed: u64,
    /// The put never reaches the store and the caller sees `Unavailable`.
    pub put_unavailable_ppm: u32,
    /// The put is applied, but the caller sees `Unavailable` (lost response, ambiguous outcome).
    pub put_lose_response_ppm: u32,
    /// A conditional put is rejected with `Conflict` without being applied (HTTP 409).
    pub put_conflict_ppm: u32,
    pub get_unavailable_ppm: u32,
    pub list_unavailable_ppm: u32,
}

impl FaultConfig {
    pub fn none() -> Self {
        Self {
            seed: 1,
            put_unavailable_ppm: 0,
            put_lose_response_ppm: 0,
            put_conflict_ppm: 0,
            get_unavailable_ppm: 0,
            list_unavailable_ppm: 0,
        }
    }
}

/// Environment variable that widens fault-injection tests to many seeds (the nightly CI run
/// sets it to 1000).
pub const FAULT_SEEDS_ENV: &str = "SEABED_FAULT_SEEDS";

/// Environment variable that runs fault-injection tests with exactly one seed, to reproduce a
/// failure reported by `SeedOnPanic`.
pub const FAULT_SEED_ENV: &str = "SEABED_FAULT_SEED";

/// Seeds a fault-injection test runs with: `default` normally; the `N` seeds starting at
/// `default.start` when `SEABED_FAULT_SEEDS=N` is set; only seed `S` when `SEABED_FAULT_SEED=S`
/// is set (it wins). Pair each seed with a `SeedOnPanic` so a failure names the seed that
/// reproduces it.
///
/// Panics on a value that is not a number, on `SEABED_FAULT_SEEDS=0`, and on a range past
/// `u64::MAX`, so a bad setting can never pass by running no seed at all.
pub fn fault_seeds(default: Range<u64>) -> Range<u64> {
    let seed = std::env::var(FAULT_SEED_ENV).ok();
    let seeds = std::env::var(FAULT_SEEDS_ENV).ok();
    seeds_from(seed.as_deref(), seeds.as_deref(), default).unwrap_or_else(|e| panic!("{e}"))
}

/// The decision behind `fault_seeds`, given the two variables' values.
fn seeds_from(
    seed: Option<&str>,
    seeds: Option<&str>,
    default: Range<u64>,
) -> Result<Range<u64>, String> {
    if let Some(v) = seed {
        let s = parse_count(FAULT_SEED_ENV, v)?;
        let end = s
            .checked_add(1)
            .ok_or_else(|| format!("{FAULT_SEED_ENV}={s} is too large"))?;
        return Ok(s..end);
    }
    let Some(v) = seeds else {
        return Ok(default);
    };
    let n = parse_count(FAULT_SEEDS_ENV, v)?;
    if n == 0 {
        return Err(format!("{FAULT_SEEDS_ENV}=0 would run no seed"));
    }
    let end = default
        .start
        .checked_add(n)
        .ok_or_else(|| format!("{FAULT_SEEDS_ENV}={n} runs past the largest seed"))?;
    Ok(default.start..end)
}

fn parse_count(name: &str, v: &str) -> Result<u64, String> {
    v.trim()
        .parse()
        .map_err(|_| format!("{name} must be a non-negative number, got {v:?}"))
}

/// Reports the seed, and the test it belongs to, if the test panics while this guard is alive.
/// `new` writes through `eprint!`, so the test harness captures it with the failing test's
/// output; `with_output` takes another writer (for tests of the guard itself).
pub struct SeedOnPanic<W: Write = CapturedStderr> {
    seed: u64,
    out: W,
}

impl SeedOnPanic {
    pub fn new(seed: u64) -> Self {
        Self::with_output(seed, CapturedStderr)
    }
}

/// Writes through `eprint!` (which the test harness captures), unlike `std::io::stderr()`.
pub struct CapturedStderr;

impl Write for CapturedStderr {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        eprint!("{}", String::from_utf8_lossy(buf));
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// The line `SeedOnPanic` writes. The test harness names each test's thread after the test.
fn seed_report(seed: u64) -> String {
    let thread = std::thread::current();
    let test = thread.name().unwrap_or("<unnamed>");
    format!("fault-injection test {test} failed with seed {seed}")
}

impl<W: Write> SeedOnPanic<W> {
    pub fn with_output(seed: u64, out: W) -> Self {
        Self { seed, out }
    }
}

impl<W: Write> Drop for SeedOnPanic<W> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            let _ = writeln!(self.out, "{}", seed_report(self.seed));
        }
    }
}

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(if seed == 0 {
            0x9E37_79B9_7F4A_7C15
        } else {
            seed
        })
    }

    // xorshift64
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn hit(&mut self, ppm: u32) -> bool {
        ppm > 0 && (self.next_u64() % 1_000_000) < u64::from(ppm)
    }
}

/// Decorator that injects deterministic (seeded) failures on top of any ObjectStore.
pub struct FaultInjecting<S> {
    inner: S,
    cfg: FaultConfig,
    rng: Mutex<Rng>,
}

impl<S: ObjectStore> FaultInjecting<S> {
    pub fn new(inner: S, cfg: FaultConfig) -> Self {
        Self {
            inner,
            cfg,
            rng: Mutex::new(Rng::new(cfg.seed)),
        }
    }

    fn roll(&self, ppm: u32) -> bool {
        lock(&self.rng).hit(ppm)
    }
}

impl<S: ObjectStore> ObjectStore for FaultInjecting<S> {
    fn caps(&self) -> ObjectStoreCaps {
        self.inner.caps()
    }

    fn get(&self, key: &Key) -> Result<(Vec<u8>, ETag), StoreError> {
        if self.roll(self.cfg.get_unavailable_ppm) {
            return Err(StoreError::Unavailable("injected: get failed".into()));
        }
        self.inner.get(key)
    }

    fn get_range(&self, key: &Key, range: Range<u64>) -> Result<Vec<u8>, StoreError> {
        if self.roll(self.cfg.get_unavailable_ppm) {
            return Err(StoreError::Unavailable("injected: get_range failed".into()));
        }
        self.inner.get_range(key, range)
    }

    fn put(&self, key: &Key, data: Vec<u8>, cond: PutCondition) -> Result<ETag, StoreError> {
        let (unavailable, lose_response, conflict) = {
            let mut rng = lock(&self.rng);
            (
                rng.hit(self.cfg.put_unavailable_ppm),
                rng.hit(self.cfg.put_lose_response_ppm),
                rng.hit(self.cfg.put_conflict_ppm),
            )
        };
        if unavailable {
            return Err(StoreError::Unavailable(
                "injected: request not delivered".into(),
            ));
        }
        if conflict && cond != PutCondition::Unconditional {
            return Err(StoreError::Conflict);
        }
        let result = self.inner.put(key, data, cond);
        if lose_response && result.is_ok() {
            return Err(StoreError::Unavailable(
                "injected: response lost after the write was applied".into(),
            ));
        }
        result
    }

    fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>, StoreError> {
        if self.roll(self.cfg.list_unavailable_ppm) {
            return Err(StoreError::Unavailable("injected: list failed".into()));
        }
        self.inner.list(prefix)
    }

    fn delete(&self, key: &Key) -> Result<(), StoreError> {
        self.inner.delete(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn seeds_from_default_widen_single_and_precedence() {
        assert_eq!(seeds_from(None, None, 7..8), Ok(7..8));
        assert_eq!(seeds_from(None, Some("1000"), 7..8), Ok(7..1007));
        assert_eq!(seeds_from(None, Some(" 3 "), 1..11), Ok(1..4), "trimmed");
        assert_eq!(seeds_from(Some("500"), None, 7..8), Ok(500..501));
        assert_eq!(
            seeds_from(Some("500"), Some("1000"), 7..8),
            Ok(500..501),
            "one seed wins"
        );
    }

    #[test]
    fn seeds_from_rejects_zero_overflow_and_garbage() {
        let max = u64::MAX.to_string();
        for (seed, seeds) in [
            (None, Some("0")),
            (None, Some(max.as_str())),
            (Some(max.as_str()), None),
            (None, Some("")),
            (None, Some("-1")),
            (None, Some("abc")),
            (Some("x"), Some("1000")),
        ] {
            assert!(
                seeds_from(seed, seeds, 7..8).is_err(),
                "{seed:?} {seeds:?} must be refused"
            );
        }
    }

    #[derive(Clone, Default)]
    struct Shared(Arc<Mutex<Vec<u8>>>);

    impl Write for Shared {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn seed_on_panic_reports_the_seed_and_test_only_when_panicking() {
        let out = Shared::default();
        drop(SeedOnPanic::with_output(8, out.clone()));
        assert!(out.0.lock().unwrap().is_empty(), "silent without a panic");

        let o = out.clone();
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _seed = SeedOnPanic::with_output(7, o);
            panic!("injected failure");
        }));
        assert!(r.is_err());
        assert_eq!(
            String::from_utf8(out.0.lock().unwrap().clone()).unwrap(),
            format!(
                "fault-injection test {} failed with seed 7\n",
                std::thread::current().name().unwrap()
            )
        );
    }
}
