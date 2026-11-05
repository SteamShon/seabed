use std::collections::HashSet;
use std::sync::Arc;
use std::thread;

use std::ops::Range;

use seabed_adapter_objectstore_mem::{
    CountingObjectStore, FaultConfig, FaultInjecting, MemObjectStore,
};
use seabed_domain::{ETag, Key, NamespaceId, NamespaceKeys, TenantId, Version};
use seabed_engine::manifest::{ManifestError, ManifestStore};
use seabed_engine::RetryPolicy;
use seabed_ports::{ObjectMeta, ObjectStore, ObjectStoreCaps, PutCondition, StoreError};

fn keys() -> NamespaceKeys {
    NamespaceKeys::new(
        TenantId::new("acme").unwrap(),
        NamespaceId::new("ads").unwrap(),
    )
}

#[test]
fn first_commit_then_stale_commit_conflicts() {
    let ms = ManifestStore::new(Arc::new(MemObjectStore::new()), keys()).unwrap();
    assert!(ms.load_latest().unwrap().is_none());

    let v1 = ms.commit(None, 1, b"first").unwrap();
    assert_eq!(v1, Version::new(1));

    // Creating "the first manifest" again must fail.
    assert_eq!(
        ms.commit(None, 2, b"again").unwrap_err(),
        ManifestError::CommitConflict
    );

    let v2 = ms.commit(Some(v1), 3, b"second").unwrap();
    assert_eq!(v2, Version::new(2));

    // A writer that still believes v1 is the latest must lose.
    assert_eq!(
        ms.commit(Some(v1), 4, b"stale").unwrap_err(),
        ManifestError::CommitConflict
    );

    let latest = ms.load_latest().unwrap().unwrap();
    assert_eq!(latest.version, v2);
    assert_eq!(latest.payload, b"second");
}

fn run_contended_commits(ms: &ManifestStore<impl seabed_ports::ObjectStore>) {
    const THREADS: u64 = 8;
    const PER_THREAD: u64 = 20;

    thread::scope(|sc| {
        for t in 0..THREADS {
            sc.spawn(move || {
                let mut done = 0;
                while done < PER_THREAD {
                    let cur = ms.load_latest().unwrap();
                    let expected = cur.as_ref().map(|m| m.version);
                    let commit_id = t * 1000 + done;
                    match ms.commit(expected, commit_id, &commit_id.to_le_bytes()) {
                        Ok(_) => done += 1,
                        Err(ManifestError::CommitConflict) => {}
                        Err(e) => panic!("unexpected error: {e:?}"),
                    }
                }
            });
        }
    });

    let total = THREADS * PER_THREAD;
    assert_eq!(
        ms.load_latest().unwrap().unwrap().version,
        Version::new(total)
    );

    // History is linear: versions 1..=total all exist and each commit appears exactly once.
    let mut ids = HashSet::new();
    for v in 1..=total {
        let m = ms.load(Version::new(v)).unwrap().expect("missing version");
        assert!(ids.insert(m.commit_id), "duplicate commit {}", m.commit_id);
    }
    assert_eq!(ids.len() as u64, total);
}

#[test]
fn concurrent_commits_form_a_linear_history() {
    let ms = ManifestStore::new(Arc::new(MemObjectStore::new()), keys()).unwrap();
    run_contended_commits(&ms);
}

#[test]
fn concurrent_commits_survive_injected_faults() {
    let mem = Arc::new(MemObjectStore::new());
    let cfg = FaultConfig {
        seed: 7,
        put_unavailable_ppm: 100_000,
        put_lose_response_ppm: 200_000,
        put_conflict_ppm: 50_000,
        get_unavailable_ppm: 50_000,
        list_unavailable_ppm: 50_000,
    };
    let faulty = Arc::new(FaultInjecting::new(mem, cfg));
    // Retries are built in; only the policy is swapped for one that does not sleep.
    let ms = ManifestStore::new(faulty, keys())
        .unwrap()
        .with_retry_policy(RetryPolicy::immediate(32))
        .with_write_retry_policy(RetryPolicy::immediate(64));
    run_contended_commits(&ms);
}

/// Hides `If-Match` support so the store falls back to listing versions.
struct NoIfMatch<S>(S);

impl<S: ObjectStore> ObjectStore for NoIfMatch<S> {
    fn caps(&self) -> ObjectStoreCaps {
        ObjectStoreCaps {
            if_match: false,
            ..self.0.caps()
        }
    }
    fn get(&self, key: &Key) -> Result<(Vec<u8>, ETag), StoreError> {
        self.0.get(key)
    }
    fn get_range(&self, key: &Key, range: Range<u64>) -> Result<Vec<u8>, StoreError> {
        self.0.get_range(key, range)
    }
    fn put(&self, key: &Key, data: Vec<u8>, cond: PutCondition) -> Result<ETag, StoreError> {
        self.0.put(key, data, cond)
    }
    fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>, StoreError> {
        self.0.list(prefix)
    }
    fn delete(&self, key: &Key) -> Result<(), StoreError> {
        self.0.delete(key)
    }
}

fn commit_n(ms: &ManifestStore<impl ObjectStore>, n: u64) {
    let mut cur = None;
    for i in 1..=n {
        cur = Some(ms.commit(cur, i, b"p").unwrap());
    }
}

fn pointer_value(store: &impl ObjectStore) -> Option<u64> {
    match store.get(&keys().manifest_pointer_key()) {
        Ok((b, _)) => Some(u64::from_le_bytes(b.try_into().unwrap())),
        Err(StoreError::NotFound) => None,
        Err(e) => panic!("{e:?}"),
    }
}

#[test]
fn load_latest_does_not_list_and_has_constant_cost() {
    let store = Arc::new(CountingObjectStore::new(MemObjectStore::new()));
    let ms = ManifestStore::new(store.clone(), keys()).unwrap();
    commit_n(&ms, 50);

    let before = store.stats();
    let latest = ms.load_latest().unwrap().unwrap();
    let after = store.stats();

    assert_eq!(latest.version, Version::new(50));
    assert_eq!(
        after.lists - before.lists,
        0,
        "the pointer replaces the LIST"
    );
    // Pointer, the pointed version, and one probe that finds nothing newer.
    assert_eq!(after.gets - before.gets, 3);
}

#[test]
fn pointer_tracks_the_latest_commit() {
    let mem = Arc::new(MemObjectStore::new());
    let ms = ManifestStore::new(mem.clone(), keys()).unwrap();
    assert_eq!(pointer_value(&*mem), None);
    commit_n(&ms, 7);
    assert_eq!(pointer_value(&*mem), Some(7));
}

#[test]
fn lagging_pointer_still_finds_the_newest_version() {
    let mem = Arc::new(MemObjectStore::new());
    let ms = ManifestStore::new(mem.clone(), keys()).unwrap();
    commit_n(&ms, 3);

    // A writer created versions 4 and 5 and stopped before advancing the pointer.
    for v in 4..=5u64 {
        let mut entry = (100 + v).to_le_bytes().to_vec();
        entry.extend_from_slice(b"late");
        mem.put(
            &keys().manifest_key(Version::new(v)),
            entry,
            PutCondition::IfNoneMatch,
        )
        .unwrap();
    }
    assert_eq!(pointer_value(&*mem), Some(3));

    let latest = ms.load_latest().unwrap().unwrap();
    assert_eq!(latest.version, Version::new(5));
    assert_eq!(latest.payload, b"late");

    // The next commit builds on 5 and moves the pointer past the gap.
    let v6 = ms.commit(Some(latest.version), 999, b"next").unwrap();
    assert_eq!(v6, Version::new(6));
    assert_eq!(pointer_value(&*mem), Some(6));
}

#[test]
fn versions_written_before_the_pointer_existed_are_found_by_listing() {
    let mem = Arc::new(MemObjectStore::new());
    commit_n(
        &ManifestStore::new(Arc::new(NoIfMatch(mem.clone())), keys()).unwrap(),
        4,
    );
    assert_eq!(pointer_value(&*mem), None);

    // Same data, now through a store with If-Match: no pointer yet, so it lists.
    let ms = ManifestStore::new(mem.clone(), keys()).unwrap();
    assert_eq!(ms.load_latest().unwrap().unwrap().version, Version::new(4));
    // The next commit creates the pointer.
    ms.commit(Some(Version::new(4)), 50, b"p").unwrap();
    assert_eq!(pointer_value(&*mem), Some(5));
}

#[test]
fn stores_without_if_match_use_list_and_never_write_a_pointer() {
    let mem = Arc::new(MemObjectStore::new());
    let ms = ManifestStore::new(Arc::new(NoIfMatch(mem.clone())), keys()).unwrap();
    commit_n(&ms, 5);
    assert_eq!(ms.load_latest().unwrap().unwrap().version, Version::new(5));
    assert_eq!(pointer_value(&*mem), None);
}

#[test]
fn pointer_ahead_of_any_version_is_reported_as_corrupt() {
    let mem = Arc::new(MemObjectStore::new());
    let ms = ManifestStore::new(mem.clone(), keys()).unwrap();
    commit_n(&ms, 2);
    let (_, etag) = mem.get(&keys().manifest_pointer_key()).unwrap();
    mem.put(
        &keys().manifest_pointer_key(),
        9u64.to_le_bytes().to_vec(),
        PutCondition::IfMatch(etag),
    )
    .unwrap();
    assert_eq!(ms.load_latest().unwrap_err(), ManifestError::CorruptPointer);
}

#[test]
fn malformed_pointer_is_reported_as_corrupt() {
    let mem = Arc::new(MemObjectStore::new());
    let ms = ManifestStore::new(mem.clone(), keys()).unwrap();
    commit_n(&ms, 1);
    let (_, etag) = mem.get(&keys().manifest_pointer_key()).unwrap();
    mem.put(
        &keys().manifest_pointer_key(),
        b"bad".to_vec(),
        PutCondition::IfMatch(etag),
    )
    .unwrap();
    assert_eq!(ms.load_latest().unwrap_err(), ManifestError::CorruptPointer);
}

/// Scripts the one sequence that reaches `create_version`'s "maybe landed" branch:
/// 1. the first put is applied but its response is lost (`Unavailable`),
/// 2. the confirming GET fails too, so the outcome is still unknown,
/// 3. the retried put is rejected because our own first attempt already created the object,
/// 4. the next confirming GET succeeds and shows the object is ours.
///
/// The writer must report success, not a conflict with itself.
///
/// Faults and counters apply only to the version-1 manifest key, so pointer traffic or a
/// change in call order cannot shift the script onto another request.
struct LostThenRejected {
    inner: MemObjectStore,
    target: Key,
    puts: std::sync::atomic::AtomicUsize,
    gets: std::sync::atomic::AtomicUsize,
}

impl ObjectStore for LostThenRejected {
    fn caps(&self) -> ObjectStoreCaps {
        self.inner.caps()
    }
    fn get(&self, key: &Key) -> Result<(Vec<u8>, ETag), StoreError> {
        use std::sync::atomic::Ordering::SeqCst;
        if *key == self.target && self.gets.fetch_add(1, SeqCst) == 0 {
            return Err(StoreError::Unavailable(
                "scripted: confirm GET fails".into(),
            ));
        }
        self.inner.get(key)
    }
    fn get_range(&self, key: &Key, range: Range<u64>) -> Result<Vec<u8>, StoreError> {
        self.inner.get_range(key, range)
    }
    fn put(&self, key: &Key, data: Vec<u8>, cond: PutCondition) -> Result<ETag, StoreError> {
        use std::sync::atomic::Ordering::SeqCst;
        let result = self.inner.put(key, data, cond);
        if *key == self.target && self.puts.fetch_add(1, SeqCst) == 0 {
            assert!(result.is_ok());
            return Err(StoreError::Unavailable("scripted: response lost".into()));
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

#[test]
fn own_commit_is_recognised_after_lost_response_then_precondition_failed() {
    let store = Arc::new(LostThenRejected {
        inner: MemObjectStore::new(),
        target: keys().manifest_key(Version::new(1)),
        puts: Default::default(),
        gets: Default::default(),
    });
    // No read retries, so the scripted GET failure is seen by `commit` itself.
    let ms = ManifestStore::new(store.clone(), keys())
        .unwrap()
        .with_retry_policy(RetryPolicy::immediate(1));

    assert_eq!(ms.commit(None, 77, b"mine").unwrap(), Version::new(1));
    // Exactly the scripted path, counted before the reads below add their own GET:
    // lost put, failed confirm, rejected retry, good confirm.
    use std::sync::atomic::Ordering::SeqCst;
    assert_eq!(store.puts.load(SeqCst), 2, "manifest puts");
    assert_eq!(store.gets.load(SeqCst), 2, "manifest confirm GETs");

    let latest = ms.load_latest().unwrap().unwrap();
    assert_eq!(latest.version, Version::new(1), "no duplicate version");
    assert_eq!(latest.commit_id, 77);
    assert_eq!(latest.payload, b"mine");
}

/// Every version put fails without being applied. `commit` gives up when the write budget runs
/// out, returning the last failure; once the store recovers, the same commit goes through.
#[test]
fn commit_gives_up_when_the_write_budget_runs_out_and_a_later_retry_succeeds() {
    let mem = Arc::new(MemObjectStore::new());
    let down = Arc::new(CountingObjectStore::new(FaultInjecting::new(
        mem.clone(),
        FaultConfig {
            put_unavailable_ppm: 1_000_000,
            ..FaultConfig::none()
        },
    )));
    let ms = ManifestStore::new(down.clone(), keys())
        .unwrap()
        .with_write_retry_policy(RetryPolicy::immediate(3));
    assert!(matches!(
        ms.commit(None, 7, b"first"),
        Err(ManifestError::Store(StoreError::Unavailable(_)))
    ));
    assert_eq!(down.stats().puts, 3, "3 tries, then give up");

    let ms = ManifestStore::new(mem, keys()).unwrap();
    assert_eq!(ms.commit(None, 7, b"first").unwrap(), Version::new(1));
}

fn faulty_counting(
    mem: &Arc<MemObjectStore>,
    cfg: FaultConfig,
) -> Arc<CountingObjectStore<FaultInjecting<Arc<MemObjectStore>>>> {
    Arc::new(CountingObjectStore::new(FaultInjecting::new(
        mem.clone(),
        cfg,
    )))
}

/// The put lands but its response is lost, and the read-back fails too, so `commit` gives up
/// with the outcome unknown. Committing again with the same `expected` and `commit_id` once the
/// store recovers must report the version that already holds this commit, not a conflict.
#[test]
fn retry_after_an_unknown_outcome_recognises_the_commit_that_landed() {
    let mem = Arc::new(MemObjectStore::new());
    let down = faulty_counting(
        &mem,
        FaultConfig {
            put_lose_response_ppm: 1_000_000,
            get_unavailable_ppm: 1_000_000,
            ..FaultConfig::none()
        },
    );
    let ms = ManifestStore::new(down, keys())
        .unwrap()
        .with_retry_policy(RetryPolicy::immediate(1))
        .with_write_retry_policy(RetryPolicy::immediate(2));
    assert!(matches!(
        ms.commit(None, 7, b"first"),
        Err(ManifestError::Store(StoreError::Unavailable(_)))
    ));

    let ms = ManifestStore::new(mem, keys()).unwrap();
    assert_eq!(ms.commit(None, 7, b"first").unwrap(), Version::new(1));
    assert_eq!(ms.load_latest().unwrap().unwrap().commit_id, 7);
    // A different commit for the same version is still a conflict.
    assert_eq!(
        ms.commit(None, 8, b"other"),
        Err(ManifestError::CommitConflict)
    );
}

/// A round where the put and its read-back both fail costs one try, not two; a refused put whose
/// read-back fails costs one try too (it cannot spin on `max_attempts`).
#[test]
fn a_failed_put_and_a_failed_read_back_cost_one_try_per_round() {
    for (what, cfg) in [
        (
            "unclear put",
            FaultConfig {
                put_unavailable_ppm: 1_000_000,
                get_unavailable_ppm: 1_000_000,
                ..FaultConfig::none()
            },
        ),
        (
            "refused put",
            FaultConfig {
                put_conflict_ppm: 1_000_000,
                get_unavailable_ppm: 1_000_000,
                ..FaultConfig::none()
            },
        ),
    ] {
        let store = faulty_counting(&Arc::new(MemObjectStore::new()), cfg);
        let ms = ManifestStore::new(store.clone(), keys())
            .unwrap()
            .with_retry_policy(RetryPolicy::immediate(1))
            .with_write_retry_policy(RetryPolicy::immediate(3));
        assert!(
            matches!(
                ms.commit(None, 1, b"x"),
                Err(ManifestError::Store(StoreError::Unavailable(_)))
            ),
            "{what}"
        );
        let s = store.stats();
        assert_eq!((s.puts, s.gets), (3, 3), "{what}");
    }
}

/// Puts to the pointer key always fail; everything else works.
struct PointerDown {
    inner: MemObjectStore,
    pointer_puts: std::sync::atomic::AtomicUsize,
}

impl ObjectStore for PointerDown {
    fn caps(&self) -> ObjectStoreCaps {
        self.inner.caps()
    }
    fn get(&self, key: &Key) -> Result<(Vec<u8>, ETag), StoreError> {
        self.inner.get(key)
    }
    fn get_range(&self, key: &Key, range: Range<u64>) -> Result<Vec<u8>, StoreError> {
        self.inner.get_range(key, range)
    }
    fn put(&self, key: &Key, data: Vec<u8>, cond: PutCondition) -> Result<ETag, StoreError> {
        if *key == keys().manifest_pointer_key() {
            self.pointer_puts
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            return Err(StoreError::Unavailable("pointer down".into()));
        }
        self.inner.put(key, data, cond)
    }
    fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>, StoreError> {
        self.inner.list(prefix)
    }
    fn delete(&self, key: &Key) -> Result<(), StoreError> {
        self.inner.delete(key)
    }
}

/// The pointer is best effort: when it cannot be written, the commit still succeeds after at
/// most `POINTER_RETRY_ATTEMPTS` pointer puts, and readers still find the version.
#[test]
fn pointer_put_failures_stop_early_and_the_commit_still_succeeds() {
    use seabed_engine::manifest::POINTER_RETRY_ATTEMPTS;
    let store = Arc::new(PointerDown {
        inner: MemObjectStore::new(),
        pointer_puts: Default::default(),
    });
    let ms = ManifestStore::new(store.clone(), keys())
        .unwrap()
        .with_write_retry_policy(RetryPolicy::immediate(5));
    assert_eq!(ms.commit(None, 1, b"first").unwrap(), Version::new(1));
    assert_eq!(
        store.pointer_puts.load(std::sync::atomic::Ordering::SeqCst),
        POINTER_RETRY_ATTEMPTS
    );
    assert_eq!(ms.load_latest().unwrap().unwrap().version, Version::new(1));

    // A write policy smaller than the cap wins.
    store
        .pointer_puts
        .store(0, std::sync::atomic::Ordering::SeqCst);
    let ms = ms.with_write_retry_policy(RetryPolicy::immediate(1));
    ms.commit(Some(Version::new(1)), 2, b"second").unwrap();
    assert_eq!(
        store.pointer_puts.load(std::sync::atomic::Ordering::SeqCst),
        1
    );
}
