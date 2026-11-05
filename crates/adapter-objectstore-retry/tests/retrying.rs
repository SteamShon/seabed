use std::ops::Range;
use std::sync::atomic::{AtomicUsize, Ordering};

use seabed_adapter_objectstore_mem::{
    CountingObjectStore, FaultConfig, FaultInjecting, MemObjectStore,
};
use seabed_adapter_objectstore_retry::{RetryPolicy, Retrying};
use seabed_domain::{ETag, Key};
use seabed_ports::{ObjectMeta, ObjectStore, ObjectStoreCaps, PutCondition, StoreError};

/// Fails the first `fail_first` calls of every operation with `err`, then delegates.
struct Scripted {
    inner: CountingObjectStore<MemObjectStore>,
    fail_first: usize,
    err: StoreError,
    calls: AtomicUsize,
}

impl Scripted {
    fn new(fail_first: usize, err: StoreError) -> Self {
        Self {
            inner: CountingObjectStore::new(MemObjectStore::new()),
            fail_first,
            err,
            calls: AtomicUsize::new(0),
        }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    fn gate(&self) -> Result<(), StoreError> {
        let n = self.calls.fetch_add(1, Ordering::SeqCst);
        if n < self.fail_first {
            Err(self.err.clone())
        } else {
            Ok(())
        }
    }
}

impl ObjectStore for Scripted {
    fn caps(&self) -> ObjectStoreCaps {
        self.inner.caps()
    }
    fn get(&self, key: &Key) -> Result<(Vec<u8>, ETag), StoreError> {
        self.gate()?;
        self.inner.get(key)
    }
    fn get_range(&self, key: &Key, range: Range<u64>) -> Result<Vec<u8>, StoreError> {
        self.gate()?;
        self.inner.get_range(key, range)
    }
    fn put(&self, key: &Key, data: Vec<u8>, cond: PutCondition) -> Result<ETag, StoreError> {
        self.gate()?;
        self.inner.put(key, data, cond)
    }
    fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>, StoreError> {
        self.gate()?;
        self.inner.list(prefix)
    }
    fn delete(&self, key: &Key) -> Result<(), StoreError> {
        self.gate()?;
        self.inner.delete(key)
    }
}

fn unavailable() -> StoreError {
    StoreError::Unavailable("scripted".into())
}

fn seeded(fail_first: usize, err: StoreError) -> Scripted {
    let s = Scripted::new(0, err.clone());
    s.inner
        .put(
            &Key::new("k"),
            b"hello".to_vec(),
            PutCondition::Unconditional,
        )
        .unwrap();
    Scripted { fail_first, ..s }
}

#[test]
fn healthy_store_passes_reads_and_writes_through() {
    let store = Retrying::new(MemObjectStore::new(), RetryPolicy::immediate(3));
    let k = Key::new("a/1");
    store
        .put(&k, b"abc".to_vec(), PutCondition::IfNoneMatch)
        .unwrap();
    assert_eq!(
        store
            .put(&k, b"x".to_vec(), PutCondition::IfNoneMatch)
            .unwrap_err(),
        StoreError::PreconditionFailed
    );
    assert_eq!(store.get(&k).unwrap().0, b"abc");
    assert_eq!(store.get_range(&k, 0..2).unwrap(), b"ab");
    assert_eq!(store.list("a/").unwrap().len(), 1);
    store.delete(&k).unwrap();
    assert_eq!(store.get(&k).unwrap_err(), StoreError::NotFound);
    assert!(store.caps().conditional_put);
}

#[test]
fn transient_read_failures_are_retried_until_success() {
    let store = Retrying::new(seeded(3, unavailable()), RetryPolicy::immediate(4));
    let (bytes, _) = store.get(&Key::new("k")).unwrap();
    assert_eq!(bytes, b"hello");
}

#[test]
fn list_and_range_reads_are_retried() {
    let store = Retrying::new(seeded(2, unavailable()), RetryPolicy::immediate(3));
    assert_eq!(store.list("").unwrap().len(), 1);
    assert_eq!(store.get_range(&Key::new("k"), 1..3).unwrap(), b"el");
}

#[test]
fn exhausted_budget_returns_the_last_unavailable() {
    let store = Retrying::new(seeded(10, unavailable()), RetryPolicy::immediate(4));
    assert!(matches!(
        store.get(&Key::new("k")),
        Err(StoreError::Unavailable(_))
    ));
    // max_attempts counts the first try.
    assert_eq!(store_calls(&store), 4);
}

#[test]
fn max_attempts_one_never_retries() {
    let store = Retrying::new(seeded(1, unavailable()), RetryPolicy::immediate(1));
    assert!(store.list("").is_err());
    assert_eq!(store_calls(&store), 1);
}

#[test]
fn non_transient_errors_are_not_retried() {
    let store = Retrying::new(Scripted::new(0, unavailable()), RetryPolicy::immediate(5));
    assert_eq!(
        store.get(&Key::new("missing")).unwrap_err(),
        StoreError::NotFound
    );
    assert_eq!(store_calls(&store), 1);
}

#[test]
fn writes_and_deletes_are_never_retried() {
    // A failed put may have been applied; resolving that belongs to the caller.
    let store = Retrying::new(Scripted::new(1, unavailable()), RetryPolicy::immediate(5));
    let put = store.put(&Key::new("k"), b"x".to_vec(), PutCondition::IfNoneMatch);
    assert!(matches!(put, Err(StoreError::Unavailable(_))));
    assert_eq!(store_calls(&store), 1);

    let store = Retrying::new(Scripted::new(1, unavailable()), RetryPolicy::immediate(5));
    assert!(store.delete(&Key::new("k")).is_err());
    assert_eq!(store_calls(&store), 1);
}

#[test]
fn injected_list_faults_are_absorbed() {
    let cfg = FaultConfig {
        list_unavailable_ppm: 500_000,
        ..FaultConfig::none()
    };
    let faulty = FaultInjecting::new(MemObjectStore::new(), cfg);
    let store = Retrying::new(faulty, RetryPolicy::immediate(64));
    for _ in 0..100 {
        store.list("").unwrap();
    }
}

#[test]
fn default_policy_backs_off() {
    let p = RetryPolicy::default();
    assert!(p.max_attempts > 1);
    assert!(p.min_delay > std::time::Duration::ZERO);
    assert!(p.jitter);
}

fn store_calls(store: &Retrying<Scripted>) -> usize {
    store.inner().calls()
}

#[test]
fn budget_counts_the_first_try() {
    let mut one = RetryPolicy::immediate(1).budget();
    assert!(!one.wait(), "max_attempts 1: no retry at all");

    let mut four = RetryPolicy::immediate(4).budget();
    let retries = std::iter::from_fn(|| four.wait().then_some(())).count();
    assert_eq!(retries, 3, "4 tries = the first + 3 retries");
}

#[test]
fn budget_sleeps_between_retries() {
    let policy = RetryPolicy {
        max_attempts: 3,
        min_delay: std::time::Duration::from_millis(10),
        max_delay: std::time::Duration::from_millis(10),
        jitter: false,
    };
    let mut budget = policy.budget();
    let start = std::time::Instant::now();
    assert!(budget.wait());
    assert!(budget.wait());
    assert!(!budget.wait());
    assert!(start.elapsed() >= std::time::Duration::from_millis(20));
}

/// Pins how long the default policy keeps a caller-driven loop going before it gives up.
#[test]
fn default_budget_delay_sequence() {
    let policy = RetryPolicy {
        jitter: false,
        ..RetryPolicy::default()
    };
    let mut budget = policy.budget();
    // backon computes the doubling in floating point, so compare rounded milliseconds.
    let delays: Vec<u64> = std::iter::from_fn(|| budget.next_delay())
        .map(|d| (d.as_secs_f64() * 1000.0).round() as u64)
        .collect();
    assert_eq!(delays, [20, 40, 80, 160, 320, 640, 1000]);
    assert_eq!(delays.iter().sum::<u64>(), 2260);
}

#[test]
fn retry_or_hands_back_the_failure_once_the_budget_is_spent() {
    let mut budget = RetryPolicy::immediate(2).budget();
    assert_eq!(budget.retry_or("first"), Ok(()));
    assert_eq!(budget.retry_or("second"), Err("second"));
}
