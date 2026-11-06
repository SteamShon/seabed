//! Runs against a real S3 or S3-compatible endpoint, and only when one is configured:
//!
//! - `SEABED_S3_ENDPOINT`: e.g. `http://127.0.0.1:5055` for a local moto server
//! - `SEABED_S3_BUCKET`: an existing bucket (default `seabed-test`)
//! - credentials and region come from the usual `AWS_*` variables; for an emulator any
//!   non-empty values work (defaults `test`, `test`, `us-east-1`)
//!
//! Without `SEABED_S3_ENDPOINT` every test here returns early and says so, unless
//! `SEABED_S3_REQUIRED=1`. CI runs them in the `s3` job against moto (`.github/workflows/ci.yml`).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use object_store::aws::AmazonS3Builder;
use seabed_adapter_objectstore_s3::S3ObjectStore;
use seabed_domain::Key;
use seabed_domain::{NamespaceId, NamespaceKeys, TenantId};
use seabed_engine::namespace::{Namespace, Op};
use seabed_ports::{DurabilityClass, ObjectStore, PutCondition};

/// The endpoint, or `None` to skip. With `SEABED_S3_REQUIRED=1` (set in CI) a missing endpoint
/// fails the test instead of skipping it, so a broken CI setup cannot pass silently.
fn endpoint() -> Option<String> {
    let e = std::env::var("SEABED_S3_ENDPOINT")
        .ok()
        .filter(|e| !e.trim().is_empty());
    if e.is_none() && std::env::var("SEABED_S3_REQUIRED").is_ok_and(|v| v == "1") {
        panic!("SEABED_S3_REQUIRED=1 but SEABED_S3_ENDPOINT is not set");
    }
    e
}

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

/// A store under a root no other store in this run uses, so each starts empty.
fn fresh_store(endpoint: &str) -> S3ObjectStore {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = format!(
        "seabed-tests/{nanos}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    );
    let builder = AmazonS3Builder::new()
        .with_endpoint(endpoint)
        .with_allow_http(true)
        .with_bucket_name(env_or("SEABED_S3_BUCKET", "seabed-test"))
        .with_region(env_or("AWS_REGION", "us-east-1"))
        .with_access_key_id(env_or("AWS_ACCESS_KEY_ID", "test"))
        .with_secret_access_key(env_or("AWS_SECRET_ACCESS_KEY", "test"));
    S3ObjectStore::new(builder, DurabilityClass::Ephemeral)
        .unwrap()
        .with_root(&root)
}

#[test]
fn s3_store_passes_the_object_store_conformance_suite() {
    let Some(endpoint) = endpoint() else {
        eprintln!("skipped: SEABED_S3_ENDPOINT is not set");
        return;
    };
    seabed_conformance::run_object_store_suite(|| fresh_store(&endpoint));
}

/// A listing longer than one page comes back whole and in order.
#[test]
fn list_follows_every_page() {
    let Some(endpoint) = endpoint() else {
        eprintln!("skipped: SEABED_S3_ENDPOINT is not set");
        return;
    };
    let store = fresh_store(&endpoint).with_list_page_size(2);
    let names = ["p/0", "p/1", "p/2", "p/3", "p/4"];
    for n in names {
        store
            .put(&Key::new(n), vec![1], PutCondition::Unconditional)
            .unwrap();
    }
    let listed: Vec<String> = store
        .list("p/")
        .unwrap()
        .into_iter()
        .map(|m| m.key.as_str().to_string())
        .collect();
    assert_eq!(listed, names);
}

/// The manifest pointer's value, read straight from the store.
fn pointer(store: &S3ObjectStore, keys: &NamespaceKeys) -> u64 {
    let (bytes, _) = store.get(&keys.manifest_pointer_key()).unwrap();
    u64::from_le_bytes(bytes.as_slice().try_into().unwrap())
}

/// The engine's whole write path on S3: writes, two flushes into Parquet segments with manifest
/// commits, and a strong read that merges the segments with a newer tail. The first flush
/// creates the manifest pointer (`If-None-Match`), the second moves it (`If-Match`).
#[test]
fn namespace_writes_flushes_and_reads_back_on_s3() {
    let Some(endpoint) = endpoint() else {
        eprintln!("skipped: SEABED_S3_ENDPOINT is not set");
        return;
    };
    let keys = NamespaceKeys::new(
        TenantId::new("acme").unwrap(),
        NamespaceId::new("ads").unwrap(),
    );
    let store = Arc::new(fresh_store(&endpoint));
    let ns = Namespace::new(store.clone(), keys.clone(), 2).unwrap();
    ns.write(
        1,
        &[
            Op::upsert("a", vec![vec![1.0, 2.0]]),
            Op::upsert("b", vec![]),
        ],
    )
    .unwrap();
    let first = ns.flush().unwrap().expect("something to flush");
    assert_eq!(first.rows, 2);
    assert_eq!(pointer(&store, &keys), first.version.get());

    ns.write(2, &[Op::upsert("d", vec![vec![3.0, 4.0]])])
        .unwrap();
    let second = ns.flush().unwrap().expect("something to flush");
    assert!(second.version > first.version);
    assert_eq!(pointer(&store, &keys), second.version.get());

    ns.write(3, &[Op::delete("b"), Op::upsert("c", vec![])])
        .unwrap();

    // A second Namespace over the same store sees the same state: nothing lives in memory.
    let other = Namespace::new(store, keys, 2).unwrap();
    let ids: Vec<String> = other
        .read_all()
        .unwrap()
        .into_iter()
        .map(|r| r.id)
        .collect();
    assert_eq!(ids, ["a", "c", "d"]);
}
