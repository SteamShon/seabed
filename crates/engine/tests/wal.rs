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

/// Runs `f` on its own thread and fails the test if it does not finish in time, so a lost
/// wake-up or a deadlock shows up as a failure instead of a hung test run.
fn within<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
    let (tx, rx) = std::sync::mpsc::channel();
    thread::spawn(move || {
        let _ = tx.send(std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)));
    });
    match rx.recv_timeout(std::time::Duration::from_secs(20)) {
        Ok(Ok(v)) => v,
        Ok(Err(panic)) => std::panic::resume_unwind(panic),
        Err(_) => panic!("timed out: an append never returned"),
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
    within(|| {
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
    });
}

/// The same, when the entry never landed: the retry finds nothing there and appends it once.
#[test]
fn retry_after_an_unknown_outcome_that_did_not_land_appends_once() {
    within(|| {
        let store = Outage::new(false);
        let wal = outage_wal(&store);
        assert!(wal.append(1, b"x").is_err());
        store.recover();
        assert_eq!(wal.append(1, b"x").unwrap(), Lsn::new(1));
        assert_eq!(wal.read_from(Lsn::new(1)).unwrap().len(), 1);
    });
}

/// Reusing a request id for another payload is a different request: it is appended, and the
/// earlier one's outcome is no longer tracked.
#[test]
fn same_request_id_with_another_payload_is_a_new_append() {
    within(|| {
        let store = Outage::new(true);
        let wal = outage_wal(&store);
        assert!(wal.append(1, b"x").is_err());
        store.recover();
        assert_eq!(wal.append(1, b"other").unwrap(), Lsn::new(2));
        assert_eq!(wal.read_from(Lsn::new(1)).unwrap().len(), 2);
    });
}

/// A request remembered as landed answers retries without touching the store, but only for the
/// same payload: reusing the id for another payload appends that payload.
#[test]
fn a_landed_request_does_not_answer_for_another_payload() {
    within(|| {
        let store = Outage::new(true);
        let wal = outage_wal(&store);
        assert!(wal.append(1, b"x").is_err());
        store.recover();
        assert_eq!(wal.append(1, b"x").unwrap(), Lsn::new(1));
        assert_eq!(wal.append(1, b"other").unwrap(), Lsn::new(2));
        assert_eq!(wal.read_from(Lsn::new(2)).unwrap()[0].payload, b"other");
    });
}

/// Known limit, pinned so that durable deduplication changes this test on purpose: another
/// `Wal` (another process, or after a restart) does not know the earlier attempt and appends
/// the entry a second time.
#[test]
fn retry_from_another_wal_may_append_twice() {
    within(|| {
        let store = Outage::new(true);
        assert!(outage_wal(&store).append(1, b"x").is_err());
        store.recover();
        assert_eq!(outage_wal(&store).append(1, b"x").unwrap(), Lsn::new(2));
    });
}

/// At most `MAX_PENDING` requests are remembered, the oldest forgotten first: one more than
/// that forgets exactly the first, whose retry appends a second copy; every other retry finds
/// its entry.
#[test]
fn pending_outcomes_are_bounded_and_the_oldest_is_forgotten() {
    within(|| {
        use seabed_engine::wal::MAX_PENDING;
        let store = Outage::new(true);
        let wal = outage_wal(&store);
        let n = MAX_PENDING as u64 + 1;
        for id in 0..n {
            assert!(wal.append(id, &id.to_le_bytes()).is_err());
        }
        assert_eq!(wal.pending_len(), MAX_PENDING);
        store.recover();
        for id in 1..n {
            wal.append(id, &id.to_le_bytes()).unwrap();
        }
        assert_eq!(
            wal.read_from(Lsn::new(1)).unwrap().len() as u64,
            n,
            "no copies yet"
        );
        assert_eq!(wal.append(0, &0u64.to_le_bytes()).unwrap(), Lsn::new(n + 1));
    });
}

/// Another writer (another process) uses the same request id with another payload and takes the
/// LSN our failed attempt tried. Our retry must not take that entry for ours: it appends ours.
#[test]
fn retry_does_not_claim_another_writers_entry_with_the_same_request_id() {
    within(|| {
        let store = Outage::new(false);
        let ours = outage_wal(&store);
        assert!(ours.append(1, b"x").is_err());
        store.recover();
        assert_eq!(outage_wal(&store).append(1, b"y").unwrap(), Lsn::new(1));

        assert_eq!(ours.append(1, b"x").unwrap(), Lsn::new(2));
        let payloads: Vec<Vec<u8>> = ours
            .read_from(Lsn::new(1))
            .unwrap()
            .into_iter()
            .map(|e| e.payload)
            .collect();
        assert_eq!(payloads, [b"y".to_vec(), b"x".to_vec()]);
    });
}

/// Two threads retry the same request at once on the same `Wal`: it is still appended once, and
/// both get its LSN. Whether the first attempt landed or not: when it did not, both retries find
/// nothing at the candidate, and only running them one at a time (the second sees the first's
/// LSN) keeps them from appending twice.
#[test]
fn concurrent_retries_of_one_request_append_it_once() {
    within(concurrent_retries_of_one_request_append_it_once_body);
}

fn concurrent_retries_of_one_request_append_it_once_body() {
    for round in 0..100 {
        let store = Outage::new(round % 2 == 0);
        let wal = outage_wal(&store);
        assert!(wal.append(1, b"x").is_err());
        store.recover();
        let lsns: Vec<Lsn> = thread::scope(|sc| {
            let hs: Vec<_> = (0..2)
                .map(|_| sc.spawn(|| wal.append(1, b"x").unwrap()))
                .collect();
            hs.into_iter().map(|h| h.join().unwrap()).collect()
        });
        assert_eq!(lsns, [Lsn::new(1), Lsn::new(1)], "round {round}");
        assert_eq!(
            wal.read_from(Lsn::new(1)).unwrap().len(),
            1,
            "round {round}"
        );
    }
}

/// Appends whose outcome became known in the call leave nothing behind; a retried request is
/// remembered as landed, and a further retry returns the same LSN without appending.
#[test]
fn only_requests_that_ended_unknown_are_remembered() {
    within(only_requests_that_ended_unknown_are_remembered_body);
}

fn only_requests_that_ended_unknown_are_remembered_body() {
    // Every put lands but loses its response; read-backs work, so each append resolves its
    // outcome within the call.
    let flaky = Arc::new(FaultInjecting::new(
        Arc::new(MemObjectStore::new()),
        FaultConfig {
            put_lose_response_ppm: 1_000_000,
            ..FaultConfig::none()
        },
    ));
    let wal = Wal::new(flaky, keys())
        .unwrap()
        .with_write_retry_policy(RetryPolicy::immediate(3));
    assert_eq!(wal.append(1, b"x").unwrap(), Lsn::new(1));
    assert_eq!(wal.pending_len(), 0, "resolved within the call");

    let store = Outage::new(true);
    let wal = outage_wal(&store);
    assert!(wal.append(1, b"x").is_err());
    assert_eq!(wal.pending_len(), 1);
    store.recover();
    assert_eq!(wal.append(1, b"x").unwrap(), Lsn::new(1));
    assert_eq!(wal.pending_len(), 1, "remembered as landed");
    assert_eq!(wal.append(1, b"x").unwrap(), Lsn::new(1));
    // Answered from memory; that must not leave the id marked in progress either.
    assert_eq!(wal.append(1, b"x").unwrap(), Lsn::new(1));
    assert_eq!(wal.append(2, b"y").unwrap(), Lsn::new(2));
    assert_eq!(wal.pending_len(), 1, "a plain success is not remembered");
    assert_eq!(wal.read_from(Lsn::new(1)).unwrap().len(), 2);
}

/// The retry fails again (the store is still down); its candidates are kept, and the retry after
/// recovery still finds the entry.
#[test]
fn a_retry_that_fails_again_keeps_its_candidates() {
    within(|| {
        let store = Outage::new(true);
        let wal = outage_wal(&store);
        assert!(wal.append(1, b"x").is_err());
        assert!(wal.append(1, b"x").is_err());
        store.recover();
        assert_eq!(wal.append(1, b"x").unwrap(), Lsn::new(1));
        assert_eq!(wal.read_from(Lsn::new(1)).unwrap().len(), 1);
    });
}

/// A retry finds that every remembered candidate holds another writer's entry, then fails before
/// trying a new LSN of its own (here it runs out of LSN attempts). Nothing about the request is
/// unknown any more, so the `Wal` forgets it instead of reading those candidates again later.
#[test]
fn a_retry_whose_candidates_were_not_ours_is_forgotten() {
    within(|| {
        let store = Outage::new(false);
        let ours = outage_wal(&store).with_max_attempts(1);
        assert!(ours.append(1, b"x").is_err());
        assert_eq!(ours.pending_len(), 1, "candidate LSN 1 remembered");
        store.recover();
        assert_eq!(outage_wal(&store).append(9, b"other").unwrap(), Lsn::new(1));

        // Reads LSN 1 (not ours), loses LSN 1 to the other writer's entry, and has no attempt
        // left for LSN 2.
        assert_eq!(ours.append(1, b"x"), Err(WalError::TooManyAttempts));
        assert_eq!(ours.pending_len(), 0, "nothing left to resolve");
        assert_eq!(
            ours.read_from(Lsn::new(1)).unwrap().len(),
            1,
            "only the other entry"
        );
    });
}

/// The first attempt did not land and another writer took its candidate LSN, so the retry
/// appends at a new LSN. A further retry, sequential or concurrent, must return that LSN (the
/// candidate no longer points at it), not append a third copy.
#[test]
fn retries_after_the_candidate_was_taken_return_the_new_lsn() {
    within(retries_after_the_candidate_was_taken_return_the_new_lsn_body);
}

fn retries_after_the_candidate_was_taken_return_the_new_lsn_body() {
    let store = Outage::new(false);
    let wal = outage_wal(&store);
    assert!(wal.append(1, b"x").is_err());
    store.recover();
    assert_eq!(outage_wal(&store).append(9, b"other").unwrap(), Lsn::new(1));
    assert_eq!(wal.append(1, b"x").unwrap(), Lsn::new(2));
    assert_eq!(wal.append(1, b"x").unwrap(), Lsn::new(2));

    let store = Outage::new(false);
    let wal = outage_wal(&store);
    assert!(wal.append(1, b"x").is_err());
    store.recover();
    assert_eq!(outage_wal(&store).append(9, b"other").unwrap(), Lsn::new(1));
    let lsns: Vec<Lsn> = thread::scope(|sc| {
        let hs: Vec<_> = (0..2)
            .map(|_| sc.spawn(|| wal.append(1, b"x").unwrap()))
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    assert_eq!(lsns, [Lsn::new(2), Lsn::new(2)]);
    assert_eq!(wal.read_from(Lsn::new(1)).unwrap().len(), 2);
}

/// Like `Outage`, plus instrumented WAL reads once recovered: each read is held for `hold` and
/// the number of reads in flight at once is recorded, and a read of LSN 1 can be held until the
/// test releases it.
struct Probe {
    outage: Arc<Outage>,
    hold: std::time::Duration,
    in_flight: std::sync::atomic::AtomicUsize,
    max_in_flight: std::sync::atomic::AtomicUsize,
    gate: std::sync::Mutex<Option<std::sync::mpsc::Receiver<()>>>,
}

impl Probe {
    fn new(hold: std::time::Duration) -> Arc<Self> {
        Arc::new(Self {
            outage: Outage::new(true),
            hold,
            in_flight: Default::default(),
            max_in_flight: Default::default(),
            gate: Default::default(),
        })
    }
}

impl seabed_ports::ObjectStore for Probe {
    fn caps(&self) -> seabed_ports::ObjectStoreCaps {
        self.outage.caps()
    }
    fn get(&self, key: &seabed_domain::Key) -> Result<(Vec<u8>, seabed_domain::ETag), StoreError> {
        use std::sync::atomic::Ordering::SeqCst;
        if self.outage.is_down() {
            return self.outage.get(key);
        }
        if *key == keys().wal_key(Lsn::new(1)) {
            let gate = self.gate.lock().unwrap().take();
            if let Some(rx) = gate {
                let _ = rx.recv();
            }
        }
        let now = self.in_flight.fetch_add(1, SeqCst) + 1;
        self.max_in_flight.fetch_max(now, SeqCst);
        thread::sleep(self.hold);
        let r = self.outage.get(key);
        self.in_flight.fetch_sub(1, SeqCst);
        r
    }
    fn get_range(
        &self,
        key: &seabed_domain::Key,
        range: std::ops::Range<u64>,
    ) -> Result<Vec<u8>, StoreError> {
        self.outage.get_range(key, range)
    }
    fn put(
        &self,
        key: &seabed_domain::Key,
        data: Vec<u8>,
        cond: seabed_ports::PutCondition,
    ) -> Result<seabed_domain::ETag, StoreError> {
        self.outage.put(key, data, cond)
    }
    fn list(&self, prefix: &str) -> Result<Vec<seabed_ports::ObjectMeta>, StoreError> {
        self.outage.list(prefix)
    }
    fn delete(&self, key: &seabed_domain::Key) -> Result<(), StoreError> {
        self.outage.delete(key)
    }
}

fn probe_wal(store: &Arc<Probe>) -> Wal<Probe> {
    Wal::new(store.clone(), keys())
        .unwrap()
        .with_retry_policy(RetryPolicy::immediate(1))
        .with_write_retry_policy(RetryPolicy::immediate(2))
}

/// Deterministic check of the one-at-a-time rule: two threads retry the same request together,
/// and each read of the candidate is held long enough that unserialized retries would overlap.
/// Reads never overlap (the second retry is answered from memory).
#[test]
fn same_id_retries_never_reach_the_store_together() {
    within(|| {
        let store = Probe::new(std::time::Duration::from_millis(200));
        let wal = probe_wal(&store);
        assert!(wal.append(1, b"x").is_err());
        store.outage.recover();
        let barrier = std::sync::Barrier::new(2);
        let lsns: Vec<Lsn> = thread::scope(|sc| {
            let hs: Vec<_> = (0..2)
                .map(|_| {
                    sc.spawn(|| {
                        barrier.wait();
                        wal.append(1, b"x").unwrap()
                    })
                })
                .collect();
            hs.into_iter().map(|h| h.join().unwrap()).collect()
        });
        assert_eq!(lsns, [Lsn::new(1), Lsn::new(1)]);
        assert_eq!(
            store
                .max_in_flight
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
    });
}

/// The one-at-a-time rule is per request id: while one request's retry is stuck reading its
/// candidate, an append with another id goes through.
#[test]
fn appends_with_other_ids_are_not_blocked() {
    within(|| {
        let store = Probe::new(std::time::Duration::ZERO);
        let wal = Arc::new(probe_wal(&store));
        assert!(wal.append(1, b"x").is_err());
        store.outage.recover();
        let (release, gate) = std::sync::mpsc::channel();
        *store.gate.lock().unwrap() = Some(gate);

        let stuck = {
            let wal = wal.clone();
            thread::spawn(move || wal.append(1, b"x").unwrap())
        };
        // Wait until the retry of 1 holds the read of its candidate.
        while store.gate.lock().unwrap().is_some() {
            thread::yield_now();
        }
        assert_eq!(wal.append(2, b"y").unwrap(), Lsn::new(2));
        release.send(()).unwrap();
        assert_eq!(stuck.join().unwrap(), Lsn::new(1));
    });
}
