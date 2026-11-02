use std::ops::Range;
use std::sync::Mutex;

use seabed_domain::{ETag, Key};
use seabed_ports::{ObjectMeta, ObjectStore, ObjectStoreCaps, PutCondition, StoreError};

use crate::lock;

/// Probabilities are in parts per million. Only `put`, `get` and `get_range` are affected;
/// `list` and `delete` are passed through (latency injection is not implemented yet).
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
}

impl FaultConfig {
    pub fn none() -> Self {
        Self {
            seed: 1,
            put_unavailable_ppm: 0,
            put_lose_response_ppm: 0,
            put_conflict_ppm: 0,
            get_unavailable_ppm: 0,
        }
    }
}

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(if seed == 0 { 0x9E37_79B9_7F4A_7C15 } else { seed })
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
        self.inner.list(prefix)
    }

    fn delete(&self, key: &Key) -> Result<(), StoreError> {
        self.inner.delete(key)
    }
}
