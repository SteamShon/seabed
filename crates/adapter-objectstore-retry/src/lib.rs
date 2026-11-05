//! ObjectStore decorator that retries transient read failures with exponential backoff.
//!
//! Only idempotent reads (`get`, `get_range`, `list`) are retried, and only on
//! `StoreError::Unavailable`. Writes and deletes are passed through unchanged: a failed write may
//! or may not have been applied, and only the caller (WAL, manifest) knows how to resolve that.
//!
//! Retry timing comes from the `backon` crate (exponential backoff with optional jitter).
//!
//! `Retrying` deliberately does **not** implement `ObjectStore`. It offers the same methods as
//! inherent methods instead. Engine components (`Wal`, `ManifestStore`) take a plain
//! `S: ObjectStore` and wrap it themselves, so handing them a store that already retries is a
//! compile error rather than a silent multiplication of retries (8 x 8 attempts).

use std::ops::Range;
use std::time::Duration;

use backon::{BackoffBuilder, BlockingRetryable, ExponentialBackoff, ExponentialBuilder};
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

    /// Starts a retry budget for a caller-driven loop, such as a write whose outcome the caller
    /// resolves itself. See [`RetryBudget`].
    pub fn budget(&self) -> RetryBudget {
        RetryBudget {
            delays: self.backoff().build(),
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

/// The retries left in one caller-driven loop, with the same timing as `Retrying` reads.
///
/// Call [`RetryBudget::retry_or`] (or [`RetryBudget::wait`]) after each transient failure: it
/// sleeps the next backoff delay and lets the caller retry, or, once `max_attempts` tries have
/// been used (the first try counts), tells it to give up.
///
/// With the default policy and jitter off, the delays are 20, 40, 80, 160, 320, 640 and 1000 ms:
/// about 2.26 s of sleep before giving up. Jitter adds up to one more delay to each, so up to
/// about 4.5 s.
///
/// ```
/// use seabed_adapter_objectstore_retry::RetryPolicy;
///
/// let mut budget = RetryPolicy::immediate(3).budget();
/// assert!(budget.wait()); // retry 1
/// assert!(budget.wait()); // retry 2
/// assert!(!budget.wait()); // 3 tries used: give up
/// ```
#[derive(Debug)]
pub struct RetryBudget {
    delays: ExponentialBackoff,
}

impl RetryBudget {
    /// Takes the next retry's delay without sleeping, or `None` if no retry is left.
    pub fn next_delay(&mut self) -> Option<std::time::Duration> {
        self.delays.next()
    }

    /// Sleeps before the next retry and returns `true`, or returns `false` if none is left.
    pub fn wait(&mut self) -> bool {
        match self.next_delay() {
            Some(d) => {
                if !d.is_zero() {
                    std::thread::sleep(d);
                }
                true
            }
            None => false,
        }
    }

    /// After a transient failure `e`: sleeps and returns `Ok(())` so the caller retries, or
    /// returns `Err(e)` once the budget is spent, so the caller gives up with that failure.
    pub fn retry_or<E>(&mut self, e: E) -> Result<(), E> {
        if self.wait() {
            Ok(())
        } else {
            Err(e)
        }
    }
}

/// Wrapper that retries transient read failures of the wrapped store.
///
/// When the budget runs out, the last `Unavailable` error is returned unchanged.
///
/// It cannot be passed where an `ObjectStore` is expected, so it cannot be nested or handed to
/// a component that already retries:
///
/// ```compile_fail
/// use seabed_adapter_objectstore_mem::MemObjectStore;
/// use seabed_adapter_objectstore_retry::{RetryPolicy, Retrying};
/// use seabed_ports::ObjectStore;
///
/// fn needs_store(_: impl ObjectStore) {}
/// needs_store(Retrying::new(MemObjectStore::new(), RetryPolicy::default()));
/// ```
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

// Same signatures as `ObjectStore`, but inherent: see the module docs for why.
impl<S: ObjectStore> Retrying<S> {
    pub fn caps(&self) -> ObjectStoreCaps {
        self.inner.caps()
    }

    pub fn get(&self, key: &Key) -> Result<(Vec<u8>, ETag), StoreError> {
        self.read(|| self.inner.get(key))
    }

    pub fn get_range(&self, key: &Key, range: Range<u64>) -> Result<Vec<u8>, StoreError> {
        self.read(|| self.inner.get_range(key, range.clone()))
    }

    /// Not retried: a failed put may have been applied, and only the caller can resolve that.
    pub fn put(&self, key: &Key, data: Vec<u8>, cond: PutCondition) -> Result<ETag, StoreError> {
        self.inner.put(key, data, cond)
    }

    pub fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>, StoreError> {
        self.read(|| self.inner.list(prefix))
    }

    /// Not retried, so the caller sees every outcome.
    pub fn delete(&self, key: &Key) -> Result<(), StoreError> {
        self.inner.delete(key)
    }
}
