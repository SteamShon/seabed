use std::collections::HashSet;
use std::sync::Arc;
use std::thread;

use seabed_adapter_objectstore_mem::{
    CountingObjectStore, FaultConfig, FaultInjecting, MemObjectStore,
};
use seabed_domain::{Lsn, NamespaceId, NamespaceKeys, TenantId};
use seabed_engine::wal::Wal;
use seabed_engine::RetryPolicy;

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
        .with_retry_policy(RetryPolicy::immediate(32));

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
