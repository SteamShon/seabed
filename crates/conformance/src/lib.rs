//! Conformance suites for ports. Run the same suite against every adapter,
//! including local S3 emulators, before trusting it.

use std::ops::Range;
use std::thread;

use seabed_domain::{ETag, Key};
use seabed_ports::{ObjectStore, PutCondition, StoreError};

fn k(s: &str) -> Key {
    Key::new(s)
}

/// Runs the ObjectStore suite. `make` must return a fresh, empty store each time.
/// Tests that need a capability are skipped when the adapter does not declare it.
pub fn run_object_store_suite<S, F>(make: F)
where
    S: ObjectStore,
    F: Fn() -> S,
{
    let caps = make().caps();

    put_get_roundtrip(&make());
    range_reads(&make());
    list_prefix_sorted(&make());
    delete_is_idempotent(&make());

    if caps.conditional_put {
        if_none_match_creates_once(&make());
        concurrent_create_race(&make());
    } else {
        eprintln!("skipping If-None-Match tests: caps.conditional_put = false");
    }

    if caps.if_match {
        if_match_semantics(&make());
        concurrent_cas_counter(&make());
    } else {
        eprintln!("skipping If-Match tests: caps.if_match = false");
    }
}

fn put_get_roundtrip<S: ObjectStore>(s: &S) {
    let key = k("t/a/n/b/obj");
    assert_eq!(s.get(&key).unwrap_err(), StoreError::NotFound);

    let e1 = s
        .put(&key, b"hello".to_vec(), PutCondition::Unconditional)
        .unwrap();
    let (data, e) = s.get(&key).unwrap();
    assert_eq!(data, b"hello");
    assert_eq!(e, e1);

    let e2 = s
        .put(&key, b"world".to_vec(), PutCondition::Unconditional)
        .unwrap();
    assert_ne!(e1, e2, "overwrite must change the etag");
    assert_eq!(s.get(&key).unwrap().0, b"world");
}

fn range_reads<S: ObjectStore>(s: &S) {
    let key = k("t/a/n/b/range");
    s.put(&key, b"0123456789".to_vec(), PutCondition::Unconditional)
        .unwrap();

    assert_eq!(s.get_range(&key, 2..5).unwrap(), b"234");
    assert_eq!(s.get_range(&key, 0..0).unwrap(), b"");
    assert_eq!(s.get_range(&key, 10..10).unwrap(), b"");
    assert_eq!(s.get_range(&key, 0..10).unwrap(), b"0123456789");
    assert_eq!(s.get_range(&key, 5..11).unwrap_err(), StoreError::InvalidRange);

    let reversed = Range { start: 6, end: 5 };
    assert_eq!(s.get_range(&key, reversed).unwrap_err(), StoreError::InvalidRange);

    assert_eq!(
        s.get_range(&k("t/a/n/b/missing"), 0..1).unwrap_err(),
        StoreError::NotFound
    );
}

fn list_prefix_sorted<S: ObjectStore>(s: &S) {
    for name in ["t/a/n/b/wal/3", "t/a/n/b/wal/1", "t/a/n/b/wal/2", "t/a/n/b/other/1", "t/a/n/c/wal/1"] {
        s.put(&k(name), name.as_bytes().to_vec(), PutCondition::Unconditional)
            .unwrap();
    }
    let keys: Vec<String> = s
        .list("t/a/n/b/wal/")
        .unwrap()
        .into_iter()
        .map(|m| m.key.as_str().to_string())
        .collect();
    assert_eq!(
        keys,
        vec!["t/a/n/b/wal/1", "t/a/n/b/wal/2", "t/a/n/b/wal/3"],
        "list must return only the prefix, sorted by key"
    );
    assert!(s.list("t/zzz/").unwrap().is_empty());
}

fn delete_is_idempotent<S: ObjectStore>(s: &S) {
    let key = k("t/a/n/b/del");
    s.delete(&key).unwrap(); // deleting a missing key succeeds
    s.put(&key, b"x".to_vec(), PutCondition::Unconditional).unwrap();
    s.delete(&key).unwrap();
    assert_eq!(s.get(&key).unwrap_err(), StoreError::NotFound);
    s.delete(&key).unwrap();
}

fn if_none_match_creates_once<S: ObjectStore>(s: &S) {
    let key = k("t/a/n/b/once");
    s.put(&key, b"first".to_vec(), PutCondition::IfNoneMatch).unwrap();
    assert_eq!(
        s.put(&key, b"second".to_vec(), PutCondition::IfNoneMatch)
            .unwrap_err(),
        StoreError::PreconditionFailed
    );
    assert_eq!(s.get(&key).unwrap().0, b"first");
}

fn if_match_semantics<S: ObjectStore>(s: &S) {
    let key = k("t/a/n/b/cas");

    // S3 semantics: If-Match on a missing key fails with NotFound.
    assert_eq!(
        s.put(&key, b"x".to_vec(), PutCondition::IfMatch(ETag::new("\"nope\"")))
            .unwrap_err(),
        StoreError::NotFound
    );

    let e1 = s.put(&key, b"v1".to_vec(), PutCondition::Unconditional).unwrap();
    let e2 = s
        .put(&key, b"v2".to_vec(), PutCondition::IfMatch(e1.clone()))
        .unwrap();
    assert_ne!(e1, e2);

    // A stale etag must be rejected and must not change the object.
    assert_eq!(
        s.put(&key, b"v3".to_vec(), PutCondition::IfMatch(e1))
            .unwrap_err(),
        StoreError::PreconditionFailed
    );
    assert_eq!(s.get(&key).unwrap().0, b"v2");
}

/// 16 threads race to create the same key: exactly one must win.
fn concurrent_create_race<S: ObjectStore>(s: &S) {
    let key = k("t/a/n/b/race");
    let results: Vec<(u8, Result<ETag, StoreError>)> = thread::scope(|sc| {
        let handles: Vec<_> = (0..16u8)
            .map(|id| {
                let key = &key;
                sc.spawn(move || (id, s.put(key, vec![id], PutCondition::IfNoneMatch)))
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });

    let winners: Vec<u8> = results
        .iter()
        .filter(|(_, r)| r.is_ok())
        .map(|(id, _)| *id)
        .collect();
    assert_eq!(winners.len(), 1, "exactly one creator must win, got {winners:?}");

    for (_, r) in &results {
        if let Err(e) = r {
            assert!(
                matches!(e, StoreError::PreconditionFailed | StoreError::Conflict),
                "unexpected error: {e:?}"
            );
        }
    }
    assert_eq!(s.get(&key).unwrap().0, vec![winners[0]]);
}

/// 8 threads increment a counter with read-modify-write using If-Match: no lost updates.
fn concurrent_cas_counter<S: ObjectStore>(s: &S) {
    const THREADS: usize = 8;
    const PER_THREAD: u64 = 50;

    let key = k("t/a/n/b/counter");
    s.put(&key, b"0".to_vec(), PutCondition::Unconditional).unwrap();

    thread::scope(|sc| {
        for _ in 0..THREADS {
            sc.spawn(|| {
                for _ in 0..PER_THREAD {
                    loop {
                        let (data, etag) = s.get(&key).unwrap();
                        let n: u64 = String::from_utf8(data).unwrap().parse().unwrap();
                        match s.put(
                            &key,
                            (n + 1).to_string().into_bytes(),
                            PutCondition::IfMatch(etag),
                        ) {
                            Ok(_) => break,
                            Err(StoreError::PreconditionFailed) | Err(StoreError::Conflict) => {
                                continue
                            }
                            Err(e) => panic!("unexpected error: {e:?}"),
                        }
                    }
                }
            });
        }
    });

    let (data, _) = s.get(&key).unwrap();
    let total: u64 = String::from_utf8(data).unwrap().parse().unwrap();
    assert_eq!(total, THREADS as u64 * PER_THREAD, "lost updates detected");
}
