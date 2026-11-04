use std::collections::HashSet;
use std::sync::Arc;
use std::thread;

use seabed_adapter_objectstore_mem::{FaultConfig, FaultInjecting, MemObjectStore};
use seabed_domain::{NamespaceId, NamespaceKeys, TenantId, Version};
use seabed_engine::manifest::{ManifestError, ManifestStore};
use seabed_engine::RetryPolicy;

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
        .with_retry_policy(RetryPolicy::immediate(32));
    run_contended_commits(&ms);
}
