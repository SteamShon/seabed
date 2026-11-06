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
use seabed_domain::{NamespaceId, NamespaceKeys, TenantId};
use seabed_engine::namespace::{Namespace, Op};
use seabed_ports::DurabilityClass;

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

/// The engine's whole write path on S3: a write, a flush into a Parquet segment with a manifest
/// commit (pointer included), and a strong read that merges the segment with a newer tail.
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
    let flushed = ns.flush().unwrap().expect("something to flush");
    assert_eq!(flushed.rows, 2);
    ns.write(2, &[Op::delete("b"), Op::upsert("c", vec![])])
        .unwrap();

    // A second Namespace over the same store sees the same state: nothing lives in memory.
    let other = Namespace::new(store, keys, 2).unwrap();
    let ids: Vec<String> = other
        .read_all()
        .unwrap()
        .into_iter()
        .map(|r| r.id)
        .collect();
    assert_eq!(ids, ["a", "c"]);
}
