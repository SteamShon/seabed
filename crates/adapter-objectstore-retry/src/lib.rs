//! ObjectStore decorator that retries transient read failures with exponential backoff.
//!
//! Only idempotent reads (`get`, `get_range`, `list`) are retried, and only on
//! `StoreError::Unavailable`. Writes and deletes are passed through unchanged: a failed write may
//! or may not have been applied, and only the caller (WAL, manifest) knows how to resolve that.
//!
//! Retry timing comes from the `backon` crate (exponential backoff with optional jitter).

use std::ops::Range;
use std::time::Duration;

use backon::{BlockingRetryable, ExponentialBuilder};
use seabed_domain::{ETag, Key};
use seabed_ports::{ObjectMeta, ObjectStore, ObjectStoreCaps, PutCondition, StoreError};

/// How reads are retried. `max_attempts` counts the first try, so `1` means "never retry".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    pub max_attempts: usize,
    /// Delay before the first retry. Later retries double it, up to `max_delay`.
    pub min_delay: Duration,
    pub max_delay: Duration,
    /// Randomise each delay so concurrent readers do not retry in lockstep.
    pub jitter: bool,
}

impl Default for RetryPolicy {
    /// A budget sized for S3-like stores: 8 attempts, 20 ms doubling to 1 s, with jitter.
    fn default() -> Self {
        Self {
            max_attempts: 8,
            min_delay: Duration::from_millis(20),
            max_delay: Duration::from_secs(1),
            jitter: true,
        }
    }
}

impl RetryPolicy {
    /// Retries without sleeping. For tests against in-memory stores.
    pub fn immediate(max_attempts: usize) -> Self {
        Self {
            max_attempts,
            min_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
            jitter: false,
        }
    }

    fn backoff(&self) -> ExponentialBuilder {
        let b = ExponentialBuilder::default()
            .with_min_delay(self.min_delay)
            .with_max_delay(self.max_delay)
            .with_max_times(self.max_attempts.saturating_sub(1));
        if self.jitter {
            b.with_jitter()
        } else {
            b
        }
    }
}

/// Decorator that retries transient read failures of the wrapped store.
///
/// When the budget runs out, the last `Unavailable` error is returned unchanged.
pub struct Retrying<S> {
    inner: S,
    policy: RetryPolicy,
}

impl<S: ObjectStore> Retrying<S> {
    pub fn new(inner: S, policy: RetryPolicy) -> Self {
        Self { inner, policy }
    }

    pub fn policy(&self) -> RetryPolicy {
        self.policy
    }

    pub fn inner(&self) -> &S {
        &self.inner
    }

    fn read<T>(&self, op: impl FnMut() -> Result<T, StoreError>) -> Result<T, StoreError> {
        op.retry(self.policy.backoff())
            .sleep(std::thread::sleep)
            .when(|e| matches!(e, StoreError::Unavailable(_)))
            .call()
    }
}

impl<S: ObjectStore> ObjectStore for Retrying<S> {
    fn caps(&self) -> ObjectStoreCaps {
        self.inner.caps()
    }

    fn get(&self, key: &Key) -> Result<(Vec<u8>, ETag), StoreError> {
        self.read(|| self.inner.get(key))
    }

    fn get_range(&self, key: &Key, range: Range<u64>) -> Result<Vec<u8>, StoreError> {
        self.read(|| self.inner.get_range(key, range.clone()))
    }

    fn put(&self, key: &Key, data: Vec<u8>, cond: PutCondition) -> Result<ETag, StoreError> {
        self.inner.put(key, data, cond)
    }

    fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>, StoreError> {
        self.read(|| self.inner.list(prefix))
    }

    fn delete(&self, key: &Key) -> Result<(), StoreError> {
        self.inner.delete(key)
    }
}
