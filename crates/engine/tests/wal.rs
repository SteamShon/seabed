use std::collections::HashSet;
use std::sync::Arc;
use std::thread;

use seabed_adapter_objectstore_mem::{
    CountingObjectStore, FaultConfig, FaultInjecting, MemObjectStore,
};
use seabed_domain::{Lsn, NamespaceId, NamespaceKeys, TenantId};
use seabed_engine::wal::{Wal, WalError};
use seabed_engine::RetryPolicy;
use seabed_ports::StoreError;

fn keys() -> NamespaceKeys {
    NamespaceKeys::new(
        TenantId::new("acme").unwrap(),
        NamespaceId::new("ads").unwrap(),
    )
}

#[test]
fn sequential_appends_are_contiguous() {
    let wal = Wal::new(Arc::new(MemObjectStore::new()), keys()).unwrap();
    for i in 1..=5u64 {
        let lsn = wal.append(i, format!("payload-{i}").as_bytes()).unwrap();
        assert_eq!(lsn, Lsn::new(i));
    }
    let entries = wal.read_from(Lsn::new(1)).unwrap();
    assert_eq!(entries.len(), 5);
    assert_eq!(entries[2].payload, b"payload-3");
    assert_eq!(wal.read_from(Lsn::new(4)).unwrap().len(), 2);
    assert!(wal.read_from(Lsn::new(6)).unwrap().is_empty());
}

#[test]
fn concurrent_appends_get_unique_contiguous_lsns() {
    let wal = Wal::new(Arc::new(MemObjectStore::new()), keys()).unwrap();

    let all: Vec<(u64, Lsn)> = thread::scope(|sc| {
        let handles: Vec<_> = (0..8u64)
            .map(|t| {
                let wal = &wal;
                sc.spawn(move || {
                    (0..50u64)
                        .map(|i| {
                            let id = t * 1000 + i;
                            (id, wal.append(id, &id.to_le_bytes()).unwrap())
                        })
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect()
    });

    let lsns: HashSet<u64> = all.iter().map(|(_, l)| l.get()).collect();
    assert_eq!(lsns.len(), 400, "every append must get a distinct LSN");
    assert_eq!(*lsns.iter().min().unwrap(), 1);
    assert_eq!(*lsns.iter().max().unwrap(), 400);

    let entries = wal.read_from(Lsn::new(1)).unwrap();
    assert_eq!(entries.len(), 400);
    for e in entries {
        // The entry stored at an LSN is the one whose append returned that LSN.
        let owner = all.iter().find(|(_, l)| *l == e.lsn).unwrap().0;
        assert_eq!(e.request_id, owner);
    }
}

#[test]
fn lost_responses_do_not_duplicate_or_lose_entries() {
    let mem = Arc::new(MemObjectStore::new());
    let cfg = FaultConfig {
        seed: 42,
        put_unavailable_ppm: 100_000,
        put_lose_response_ppm: 200_000,
        put_conflict_ppm: 50_000,
        get_unavailable_ppm: 100_000,
        list_unavailable_ppm: 100_000,
    };
    let faulty = Arc::new(FaultInjecting::new(mem.clone(), cfg));
    let wal = Wal::new(faulty, keys())
        .unwrap()
        .with_retry_policy(RetryPolicy::immediate(32))
        .with_write_retry_policy(RetryPolicy::immediate(64));

    thread::scope(|sc| {
        for t in 0..4u64 {
            let wal = &wal;
            sc.spawn(move || {
                for i in 0..25u64 {
                    let id = t * 1000 + i;
                    wal.append(id, &id.to_le_bytes()).unwrap();
                }
            });
        }
    });

    // Verify through the clean store: each request_id appears exactly once, log is contiguous.
    let clean = Wal::new(mem, keys()).unwrap();
    let entries = clean.read_from(Lsn::new(1)).unwrap();
    let mut seen = HashSet::new();
    for e in &entries {
        assert!(
            seen.insert(e.request_id),
            "duplicate request_id {}",
            e.request_id
        );
    }
    assert_eq!(seen.len(), 100, "no append may be lost");
    assert_eq!(entries.len(), 100);
}

#[test]
fn request_counts_are_observable() {
    let store = Arc::new(CountingObjectStore::new(MemObjectStore::new()));
    let wal = Wal::new(store.clone(), keys()).unwrap();
    for i in 1..=10u64 {
        wal.append(i, b"x").unwrap();
    }
    let stats = store.stats();
    // One list to find the log end, then one successful put per append.
    assert_eq!(stats.puts, 10);
    assert_eq!(stats.lists, 1);
}

#[test]
fn reads_survive_injected_read_faults_with_built_in_retries() {
    let mem = Arc::new(MemObjectStore::new());
    let writer = Wal::new(mem.clone(), keys()).unwrap();
    for i in 1..=30u64 {
        writer.append(i, &i.to_le_bytes()).unwrap();
    }

    let cfg = FaultConfig {
        seed: 9,
        get_unavailable_ppm: 200_000,
        list_unavailable_ppm: 200_000,
        ..FaultConfig::none()
    };

    // With retries disabled, injected faults surface to the caller.
    let raw = Wal::new(Arc::new(FaultInjecting::new(mem.clone(), cfg)), keys())
        .unwrap()
        .with_retry_policy(RetryPolicy::immediate(1));
    assert!((0..20).any(|_| raw.read_from(Lsn::new(1)).is_err()));

    // Retries are built in; only the policy is swapped for one that does not sleep.
    let reader = Wal::new(Arc::new(FaultInjecting::new(mem, cfg)), keys())
        .unwrap()
        .with_retry_policy(RetryPolicy::immediate(32));
    for _ in 0..20 {
        let entries = reader.read_from(Lsn::new(1)).unwrap();
        assert_eq!(entries.len(), 30);
    }
}

#[test]
fn default_constructor_retries_reads_without_extra_wiring() {
    let mem = Arc::new(MemObjectStore::new());
    let writer = Wal::new(mem.clone(), keys()).unwrap();
    for i in 1..=10u64 {
        writer.append(i, &i.to_le_bytes()).unwrap();
    }
    let cfg = FaultConfig {
        seed: 3,
        get_unavailable_ppm: 100_000,
        list_unavailable_ppm: 100_000,
        ..FaultConfig::none()
    };
    // No retry policy, no decorator: the default policy (with backoff) applies.
    let reader = Wal::new(Arc::new(FaultInjecting::new(mem, cfg)), keys()).unwrap();
    assert_eq!(reader.read_from(Lsn::new(1)).unwrap().len(), 10);
}

fn store_with(cfg: FaultConfig) -> Arc<CountingObjectStore<FaultInjecting<Arc<MemObjectStore>>>> {
    Arc::new(CountingObjectStore::new(FaultInjecting::new(
        Arc::new(MemObjectStore::new()),
        cfg,
    )))
}

/// Every put fails without being applied. Each failure costs one try of the write budget, and
/// when it runs out `append` returns the last failure instead of spinning.
#[test]
fn append_gives_up_when_the_write_budget_runs_out() {
    let store = store_with(FaultConfig {
        put_unavailable_ppm: 1_000_000,
        ..FaultConfig::none()
    });
    let wal = Wal::new(store.clone(), keys())
        .unwrap()
        .with_write_retry_policy(RetryPolicy::immediate(3));
    assert!(matches!(
        wal.append(1, b"x"),
        Err(WalError::Store(StoreError::Unavailable(_)))
    ));
    assert_eq!(store.stats().puts, 3, "3 tries, then give up");
}

/// The read-back of an unclear put keeps failing too: it also costs the write budget, so the
/// resolution loop cannot spin either.
#[test]
fn unresolvable_appends_stop_at_the_write_budget() {
    let store = store_with(FaultConfig {
        put_unavailable_ppm: 1_000_000,
        get_unavailable_ppm: 1_000_000,
        ..FaultConfig::none()
    });
    let wal = Wal::new(store.clone(), keys())
        .unwrap()
        .with_retry_policy(RetryPolicy::immediate(1))
        .with_write_retry_policy(RetryPolicy::immediate(3));
    assert!(matches!(
        wal.append(1, b"x"),
        Err(WalError::Store(StoreError::Unavailable(_)))
    ));
    let s = store.stats();
    // Try 1: the put fails. Tries 2 and 3: the read-back fails. Then give up.
    assert_eq!((s.puts, s.gets), (1, 2));
}

/// Losing LSNs to other writers is not a store failure: a write budget of one try still lets
/// every append through when half of all conditional puts conflict.
#[test]
fn contention_does_not_consume_the_write_budget() {
    let store = store_with(FaultConfig {
        seed: 7,
        put_conflict_ppm: 500_000,
        ..FaultConfig::none()
    });
    let wal = Wal::new(store.clone(), keys())
        .unwrap()
        .with_write_retry_policy(RetryPolicy::immediate(1));
    for i in 1..=50u64 {
        assert_eq!(wal.append(i, b"x").unwrap(), Lsn::new(i));
    }
    assert!(
        store.stats().puts > 60,
        "conflicts happened and were retried"
    );
}

/// While `down`, every GET fails and every put is either applied with its response lost
/// (`lose_response`) or not delivered at all. LIST always works.
struct Outage {
    inner: MemObjectStore,
    down: std::sync::atomic::AtomicBool,
    lose_response: bool,
}

impl Outage {
    fn new(lose_response: bool) -> Arc<Self> {
        Arc::new(Self {
            inner: MemObjectStore::new(),
            down: true.into(),
            lose_response,
        })
    }
    fn recover(&self) {
        self.down.store(false, std::sync::atomic::Ordering::SeqCst);
    }
    fn is_down(&self) -> bool {
        self.down.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl seabed_ports::ObjectStore for Outage {
    fn caps(&self) -> seabed_ports::ObjectStoreCaps {
        self.inner.caps()
    }
    fn get(&self, key: &seabed_domain::Key) -> Result<(Vec<u8>, seabed_domain::ETag), StoreError> {
        if self.is_down() {
            return Err(StoreError::Unavailable("down".into()));
        }
        self.inner.get(key)
    }
    fn get_range(
        &self,
        key: &seabed_domain::Key,
        range: std::ops::Range<u64>,
    ) -> Result<Vec<u8>, StoreError> {
        self.inner.get_range(key, range)
    }
    fn put(
        &self,
        key: &seabed_domain::Key,
        data: Vec<u8>,
        cond: seabed_ports::PutCondition,
    ) -> Result<seabed_domain::ETag, StoreError> {
        if !self.is_down() {
            return self.inner.put(key, data, cond);
        }
        if self.lose_response {
            self.inner.put(key, data, cond)?;
        }
        Err(StoreError::Unavailable("down".into()))
    }
    fn list(&self, prefix: &str) -> Result<Vec<seabed_ports::ObjectMeta>, StoreError> {
        self.inner.list(prefix)
    }
    fn delete(&self, key: &seabed_domain::Key) -> Result<(), StoreError> {
        self.inner.delete(key)
    }
}

fn outage_wal(store: &Arc<Outage>) -> Wal<Outage> {
    Wal::new(store.clone(), keys())
        .unwrap()
        .with_retry_policy(RetryPolicy::immediate(1))
        .with_write_retry_policy(RetryPolicy::immediate(2))
}

/// The entry landed but neither the put's response nor the read-back got through, so `append`
/// gave up with the outcome unknown. Retrying the same request on the same `Wal` finds it and
/// returns its LSN instead of appending it again.
#[test]
fn retry_after_an_unknown_outcome_returns_the_lsn_that_landed() {
    let store = Outage::new(true);
    let wal = outage_wal(&store);
    assert!(matches!(
        wal.append(1, b"x"),
        Err(WalError::Store(StoreError::Unavailable(_)))
    ));
    store.recover();
    assert_eq!(wal.append(1, b"x").unwrap(), Lsn::new(1));
    assert_eq!(wal.append(2, b"y").unwrap(), Lsn::new(2));
    let entries = wal.read_from(Lsn::new(1)).unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].payload, b"x");
}

/// The same, when the entry never landed: the retry finds nothing there and appends it once.
#[test]
fn retry_after_an_unknown_outcome_that_did_not_land_appends_once() {
    let store = Outage::new(false);
    let wal = outage_wal(&store);
    assert!(wal.append(1, b"x").is_err());
    store.recover();
    assert_eq!(wal.append(1, b"x").unwrap(), Lsn::new(1));
    assert_eq!(wal.read_from(Lsn::new(1)).unwrap().len(), 1);
}

/// Reusing a request id for another payload is a different request: it is appended, and the
/// earlier one's outcome is no longer tracked.
#[test]
fn same_request_id_with_another_payload_is_a_new_append() {
    let store = Outage::new(true);
    let wal = outage_wal(&store);
    assert!(wal.append(1, b"x").is_err());
    store.recover();
    assert_eq!(wal.append(1, b"other").unwrap(), Lsn::new(2));
    assert_eq!(wal.read_from(Lsn::new(1)).unwrap().len(), 2);
}

/// Known limit, pinned so that durable deduplication changes this test on purpose: another
/// `Wal` (another process, or after a restart) does not know the earlier attempt and appends
/// the entry a second time.
#[test]
fn retry_from_another_wal_may_append_twice() {
    let store = Outage::new(true);
    assert!(outage_wal(&store).append(1, b"x").is_err());
    store.recover();
    assert_eq!(outage_wal(&store).append(1, b"x").unwrap(), Lsn::new(2));
}

/// At most `MAX_PENDING` unknown outcomes are remembered: one more forgets exactly one of them,
/// whose retry appends a second copy; every other retry finds its entry.
#[test]
fn pending_outcomes_are_bounded() {
    use seabed_engine::wal::MAX_PENDING;
    let store = Outage::new(true);
    let wal = outage_wal(&store);
    let n = MAX_PENDING as u64 + 1;
    for id in 0..n {
        assert!(wal.append(id, &id.to_le_bytes()).is_err());
    }
    store.recover();
    for id in 0..n {
        wal.append(id, &id.to_le_bytes()).unwrap();
    }
    assert_eq!(wal.read_from(Lsn::new(1)).unwrap().len() as u64, n + 1);
}
