use std::collections::BTreeSet;
use std::sync::Arc;
use std::thread;

use seabed_adapter_objectstore_mem::{FaultConfig, FaultInjecting, MemObjectStore};
use seabed_domain::{NamespaceId, NamespaceKeys, TenantId};
use seabed_engine::manifest::{ManifestError, ManifestStore};
use seabed_engine::manifest_content::AttrKind;
use seabed_engine::namespace::{Namespace, NamespaceError, Op};
use seabed_engine::sot::{AttrValue, SotError};
use seabed_engine::RetryPolicy;
use seabed_ports::ObjectStore;

const DIMS: u32 = 2;

fn keys() -> NamespaceKeys {
    NamespaceKeys::new(
        TenantId::new("acme").unwrap(),
        NamespaceId::new("ads").unwrap(),
    )
}

fn ns(mem: &Arc<MemObjectStore>) -> Namespace<MemObjectStore> {
    Namespace::new(mem.clone(), keys(), DIMS).unwrap()
}

fn ids(rows: &[seabed_engine::sot::DocRow]) -> Vec<&str> {
    rows.iter().map(|r| r.id.as_str()).collect()
}

/// Every object under the namespace's `segments/` directory.
fn segment_objects(mem: &MemObjectStore) -> Vec<String> {
    let all: Vec<String> = mem
        .list("")
        .unwrap()
        .into_iter()
        .map(|m| m.key.as_str().to_string())
        .filter(|k| k.contains("/segments/"))
        .collect();
    all
}

fn price(n: i64) -> AttrValue {
    AttrValue::I64(n)
}

#[test]
fn writes_are_visible_before_any_flush() {
    let mem = Arc::new(MemObjectStore::new());
    let n = ns(&mem);
    n.write(
        1,
        &[Op::upsert("a", vec![vec![1.0, 2.0]]).with_attr("price", price(10))],
    )
    .unwrap();
    let all = n.read_all().unwrap();
    assert_eq!(ids(&all), ["a"]);
    assert_eq!(all[0].attrs.get("price"), Some(&price(10)));
}

#[test]
fn flush_moves_the_tail_into_a_segment_without_changing_what_reads_see() {
    let mem = Arc::new(MemObjectStore::new());
    let n = ns(&mem);
    n.write(
        1,
        &[
            Op::upsert("a", vec![vec![1.0, 2.0]]),
            Op::upsert("b", vec![]),
        ],
    )
    .unwrap();
    let last = n.write(2, &[Op::upsert("c", vec![])]).unwrap();
    let before = n.read_all().unwrap();

    let f = n.flush().unwrap().unwrap();
    assert_eq!(f.indexed_through, last);
    assert_eq!(f.rows, 3);
    assert_eq!(n.read_all().unwrap(), before, "flush must not change reads");

    let (_, content) = ManifestStore::new(mem.clone(), keys())
        .unwrap()
        .load_latest_content()
        .unwrap()
        .unwrap();
    assert_eq!(content.indexed_through, Some(last.get()));
    assert_eq!(content.segments.len(), 1);
    assert_eq!(content.segments[0].id, f.segment_id);

    // Nothing new: no empty segments.
    assert_eq!(n.flush().unwrap(), None);
}

#[test]
fn latest_lsn_wins_across_segments_and_tail_and_tombstones_hide_older_versions() {
    let mem = Arc::new(MemObjectStore::new());
    let n = ns(&mem);
    n.write(
        1,
        &[
            Op::upsert("a", vec![]).with_attr("price", price(1)),
            Op::upsert("b", vec![]).with_attr("price", price(1)),
            Op::upsert("c", vec![]).with_attr("price", price(1)),
        ],
    )
    .unwrap();
    n.flush().unwrap().unwrap();

    // Newer versions in the tail override the segment; a delete hides b.
    n.write(
        2,
        &[
            Op::upsert("a", vec![]).with_attr("price", price(2)),
            Op::delete("b"),
        ],
    )
    .unwrap();
    let check = |n: &Namespace<MemObjectStore>| {
        let all = n.read_all().unwrap();
        assert_eq!(ids(&all), ["a", "c"]);
        assert_eq!(all[0].attrs.get("price"), Some(&price(2)));
        assert_eq!(all[1].attrs.get("price"), Some(&price(1)));
        assert!(n.get("b").unwrap().is_none());
    };
    check(&n);

    // Same answer once the tail is a second segment (older segment still listed).
    n.flush().unwrap().unwrap();
    check(&n);
    // And after re-creating b, it is back.
    n.write(3, &[Op::upsert("b", vec![]).with_attr("price", price(3))])
        .unwrap();
    assert_eq!(
        n.get("b").unwrap().unwrap().attrs.get("price"),
        Some(&price(3))
    );
}

#[test]
fn later_entries_win_within_one_flush() {
    let mem = Arc::new(MemObjectStore::new());
    let n = ns(&mem);
    n.write(1, &[Op::upsert("a", vec![]).with_attr("price", price(1))])
        .unwrap();
    n.write(2, &[Op::upsert("a", vec![]).with_attr("price", price(2))])
        .unwrap();
    n.write(3, &[Op::delete("a")]).unwrap();
    n.write(4, &[Op::upsert("a", vec![]).with_attr("price", price(4))])
        .unwrap();
    let f = n.flush().unwrap().unwrap();
    assert_eq!(f.rows, 1, "one row per document in a segment");
    assert_eq!(
        n.get("a").unwrap().unwrap().attrs.get("price"),
        Some(&price(4))
    );
}

#[test]
fn write_refuses_bad_batches_without_appending_anything() {
    let mem = Arc::new(MemObjectStore::new());
    let n = ns(&mem);
    n.write(1, &[Op::upsert("a", vec![]).with_attr("price", price(1))])
        .unwrap();
    n.flush().unwrap();
    let objects = mem.list("").unwrap().len();

    let bad: Vec<(&str, Vec<Op>)> = vec![
        ("empty", vec![]),
        (
            "wrong dims",
            vec![Op::upsert("x", vec![vec![1.0, 2.0, 3.0]])],
        ),
        ("nan", vec![Op::upsert("x", vec![vec![f32::NAN, 1.0]])]),
        ("dup id", vec![Op::upsert("x", vec![]), Op::delete("x")]),
        ("bad id", vec![Op::upsert("", vec![])]),
        (
            "bad attr",
            vec![Op::upsert("x", vec![]).with_attr("Lsn", price(1))],
        ),
        (
            "type vs batch",
            vec![
                Op::upsert("x", vec![]).with_attr("size", price(1)),
                Op::upsert("y", vec![]).with_attr("size", AttrValue::Str("s".into())),
            ],
        ),
    ];
    for (why, ops) in bad {
        assert!(
            matches!(n.write(9, &ops), Err(NamespaceError::Invalid(_))),
            "{why} must be refused"
        );
    }
    // Type conflicting with the namespace schema: refused like every other validation failure.
    assert_eq!(
        n.write(
            9,
            &[Op::upsert("x", vec![]).with_attr("price", AttrValue::F64(1.5))]
        )
        .unwrap_err(),
        NamespaceError::Invalid(SotError::AttrTypeConflict {
            name: "price".into()
        })
    );
    assert_eq!(mem.list("").unwrap().len(), objects, "nothing appended");
}

#[test]
fn flush_extends_the_schema_and_refuses_a_conflict_without_writing_a_segment() {
    let mem = Arc::new(MemObjectStore::new());
    let n = ns(&mem);
    n.write(1, &[Op::upsert("a", vec![]).with_attr("price", price(1))])
        .unwrap();
    n.flush().unwrap().unwrap();
    n.write(
        2,
        &[Op::upsert("b", vec![]).with_attr("color", AttrValue::Str("red".into()))],
    )
    .unwrap();
    n.flush().unwrap().unwrap();
    let (_, content) = ManifestStore::new(mem.clone(), keys())
        .unwrap()
        .load_latest_content()
        .unwrap()
        .unwrap();
    assert_eq!(content.attr_schema.get("price"), Some(&AttrKind::I64));
    assert_eq!(content.attr_schema.get("color"), Some(&AttrKind::Str));
}

/// The schema poison found in review: two sequential writes giving a new attribute different
/// types used to both reach the WAL, after which every flush failed forever. Now the first write
/// commits the attribute's type before appending, the second is refused, and flush keeps working.
#[test]
fn sequential_writes_with_conflicting_new_attr_types_do_not_block_flush() {
    let mem = Arc::new(MemObjectStore::new());
    let n = ns(&mem);
    n.write(1, &[Op::upsert("c", vec![]).with_attr("size", price(1))])
        .unwrap();
    // The type is committed at write time, before any flush.
    let (_, content) = ManifestStore::new(mem.clone(), keys())
        .unwrap()
        .load_latest_content()
        .unwrap()
        .unwrap();
    assert_eq!(content.attr_schema.get("size"), Some(&AttrKind::I64));
    assert!(content.segments.is_empty());

    let wal_before = mem.list(&keys().wal_prefix()).unwrap().len();
    assert_eq!(
        n.write(
            2,
            &[Op::upsert("d", vec![]).with_attr("size", AttrValue::F64(1.0))]
        )
        .unwrap_err(),
        NamespaceError::Invalid(SotError::AttrTypeConflict {
            name: "size".into()
        })
    );
    assert_eq!(
        mem.list(&keys().wal_prefix()).unwrap().len(),
        wal_before,
        "nothing appended"
    );

    let f = n.flush().unwrap().unwrap();
    assert_eq!(f.rows, 1);
    assert_eq!(ids(&n.read_all().unwrap()), ["c"]);
}

/// The same with concurrent writers: whichever commits the type first wins; every write of the
/// other type is refused; flush never gets stuck.
#[test]
fn concurrent_writers_racing_a_new_attr_type_never_block_flush() {
    for round in 0..10 {
        let mem = Arc::new(MemObjectStore::new());
        let results: Vec<Vec<bool>> = thread::scope(|sc| {
            let hs: Vec<_> = [AttrValue::I64(1), AttrValue::F64(1.0)]
                .into_iter()
                .enumerate()
                .map(|(w, v)| {
                    let mem = &mem;
                    sc.spawn(move || {
                        let n = ns(mem);
                        (0..5u64)
                            .map(|i| {
                                let id = format!("w{w}-{i}");
                                n.write(i, &[Op::upsert(&id, vec![]).with_attr("size", v.clone())])
                                    .is_ok()
                            })
                            .collect()
                    })
                })
                .collect();
            hs.into_iter().map(|h| h.join().unwrap()).collect()
        });
        let accepted: Vec<usize> = results
            .iter()
            .map(|r| r.iter().filter(|ok| **ok).count())
            .collect();
        // Exactly one writer's type got in, and that writer's writes all succeeded.
        assert!(
            accepted == vec![5, 0] || accepted == vec![0, 5],
            "round {round}: {accepted:?}"
        );
        let n = ns(&mem);
        assert!(n.flush().unwrap().is_some(), "round {round}: flush works");
        assert_eq!(n.read_all().unwrap().len(), 5);
    }
}

#[test]
fn racing_flushes_of_the_same_range_commit_once_and_leave_no_divergent_files() {
    let mem = Arc::new(MemObjectStore::new());
    let n = ns(&mem);
    for i in 0..20u64 {
        n.write(i, &[Op::upsert(&format!("d{i:02}"), vec![])])
            .unwrap();
    }
    let results: Vec<_> = thread::scope(|sc| {
        let hs: Vec<_> = (0..4).map(|_| sc.spawn(|| ns(&mem).flush())).collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let committed = results.iter().filter(|r| matches!(r, Ok(Some(_)))).count();
    for r in &results {
        assert!(
            matches!(
                r,
                Ok(Some(_))
                    | Ok(None)
                    | Err(NamespaceError::Manifest(ManifestError::CommitConflict))
            ),
            "unexpected {r:?}"
        );
    }
    assert_eq!(committed, 1, "exactly one flush commits");

    let (_, content) = ManifestStore::new(mem.clone(), keys())
        .unwrap()
        .load_latest_content()
        .unwrap()
        .unwrap();
    assert_eq!(content.segments.len(), 1);
    // Same range, same id, same bytes: losers reused the winner's file instead of leaving
    // another one behind.
    let docs_files = segment_objects(&mem)
        .into_iter()
        .filter(|k| k.ends_with("docs.parquet"))
        .count();
    assert_eq!(docs_files, 1);
    assert_eq!(n.read_all().unwrap().len(), 20);
}

#[test]
fn writes_racing_with_flushes_are_never_lost() {
    let mem = Arc::new(MemObjectStore::new());
    let total = 60u64;
    thread::scope(|sc| {
        sc.spawn(|| {
            let n = ns(&mem);
            for i in 0..total {
                n.write(i, &[Op::upsert(&format!("d{i:03}"), vec![])])
                    .unwrap();
            }
        });
        sc.spawn(|| {
            let n = ns(&mem);
            for _ in 0..30 {
                match n.flush() {
                    Ok(_) | Err(NamespaceError::Manifest(ManifestError::CommitConflict)) => {}
                    Err(e) => panic!("{e:?}"),
                }
            }
        });
    });
    let n = ns(&mem);
    while n.flush().unwrap().is_some() {}
    let all = n.read_all().unwrap();
    let got: BTreeSet<_> = all.iter().map(|r| r.id.clone()).collect();
    assert_eq!(
        got.len(),
        total as usize,
        "every acknowledged write is readable"
    );
}

#[test]
fn reads_and_flushes_survive_injected_faults() {
    let mem = Arc::new(MemObjectStore::new());
    let cfg = FaultConfig {
        seed: 5,
        put_unavailable_ppm: 50_000,
        put_lose_response_ppm: 100_000,
        put_conflict_ppm: 0,
        get_unavailable_ppm: 100_000,
        list_unavailable_ppm: 100_000,
    };
    let n = Namespace::new(
        Arc::new(FaultInjecting::new(mem.clone(), cfg)),
        keys(),
        DIMS,
    )
    .unwrap()
    .with_retry_policy(RetryPolicy::immediate(64));
    for i in 0..10u64 {
        n.write(i, &[Op::upsert(&format!("d{i}"), vec![])]).unwrap();
        if i % 3 == 0 {
            // A flush can lose its race against nobody but itself after a lost response: retry.
            loop {
                match n.flush() {
                    Ok(_) => break,
                    Err(NamespaceError::Manifest(ManifestError::CommitConflict)) => continue,
                    Err(e) => panic!("{e:?}"),
                }
            }
        }
    }
    assert_eq!(n.read_all().unwrap().len(), 10);
    // A clean view agrees.
    assert_eq!(ns(&mem).read_all().unwrap().len(), 10);
}

/// Runs a hook once, right before the first LIST of the WAL prefix, so a test can commit a
/// flush in the middle of another `Namespace`'s read, deterministically.
struct FlushDuringRead {
    inner: Arc<MemObjectStore>,
    wal_prefix: String,
    hook: std::sync::Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

impl ObjectStore for FlushDuringRead {
    fn caps(&self) -> seabed_ports::ObjectStoreCaps {
        self.inner.caps()
    }
    fn get(
        &self,
        key: &seabed_domain::Key,
    ) -> Result<(Vec<u8>, seabed_domain::ETag), seabed_ports::StoreError> {
        self.inner.get(key)
    }
    fn get_range(
        &self,
        key: &seabed_domain::Key,
        range: std::ops::Range<u64>,
    ) -> Result<Vec<u8>, seabed_ports::StoreError> {
        self.inner.get_range(key, range)
    }
    fn put(
        &self,
        key: &seabed_domain::Key,
        data: Vec<u8>,
        cond: seabed_ports::PutCondition,
    ) -> Result<seabed_domain::ETag, seabed_ports::StoreError> {
        self.inner.put(key, data, cond)
    }
    fn list(
        &self,
        prefix: &str,
    ) -> Result<Vec<seabed_ports::ObjectMeta>, seabed_ports::StoreError> {
        if prefix == self.wal_prefix {
            if let Some(hook) = self.hook.lock().unwrap().take() {
                hook();
            }
        }
        self.inner.list(prefix)
    }
    fn delete(&self, key: &seabed_domain::Key) -> Result<(), seabed_ports::StoreError> {
        self.inner.delete(key)
    }
}

/// Deferred from PR #14 review (M11): a strong read must take its segments and its tail from the
/// same manifest version. Here a flush commits between the reader's manifest load and its WAL
/// listing; every write acknowledged before the read must still be returned exactly once.
#[test]
fn strong_read_sees_every_acked_write_when_a_flush_commits_mid_read() {
    let mem = Arc::new(MemObjectStore::new());
    let writer = ns(&mem);
    writer.write(1, &[Op::upsert("a", vec![])]).unwrap();
    writer.flush().unwrap().unwrap();
    writer
        .write(2, &[Op::upsert("b", vec![]), Op::delete("a")])
        .unwrap();
    writer.write(3, &[Op::upsert("c", vec![])]).unwrap();

    let hooked = Arc::new(FlushDuringRead {
        inner: mem.clone(),
        wal_prefix: keys().wal_prefix(),
        hook: std::sync::Mutex::new(None),
    });
    let reader = Namespace::new(hooked.clone(), keys(), DIMS).unwrap();
    let mem2 = mem.clone();
    *hooked.hook.lock().unwrap() = Some(Box::new(move || {
        // Another process flushes b, the delete of a, and c while the read is in progress.
        assert!(ns(&mem2).flush().unwrap().is_some());
    }));
    let all = reader.read_all().unwrap();
    assert!(
        hooked.hook.lock().unwrap().is_none(),
        "the flush ran mid-read"
    );
    assert_eq!(ids(&all), ["b", "c"]);
    // And a fresh read after the flush agrees.
    assert_eq!(ids(&ns(&mem).read_all().unwrap()), ["b", "c"]);
}

/// Pins what a strong read costs, so a change in request pattern is a visible test change.
#[test]
fn read_all_request_cost_is_pinned() {
    use seabed_adapter_objectstore_mem::CountingObjectStore;
    let store = Arc::new(CountingObjectStore::new(MemObjectStore::new()));
    let n = Namespace::new(store.clone(), keys(), DIMS).unwrap();
    // Two segments, then three tail entries.
    for seg in 0..2u64 {
        n.write(seg, &[Op::upsert(&format!("s{seg}"), vec![])])
            .unwrap();
        n.flush().unwrap().unwrap();
    }
    for t in 0..3u64 {
        n.write(10 + t, &[Op::upsert(&format!("t{t}"), vec![])])
            .unwrap();
    }
    let before = store.stats();
    assert_eq!(n.read_all().unwrap().len(), 5);
    let after = store.stats();
    let (gets, lists) = (after.gets - before.gets, after.lists - before.lists);
    // Latest manifest: pointer + version + probe for a newer one = 3 GETs.
    // Segments: one GET per SoT file = 2. Tail: one LIST of the WAL, one GET per entry = 3.
    assert_eq!((gets, lists), (3 + 2 + 3, 1));
    assert_eq!(after.puts, before.puts, "reads never write");
}

/// Refuses the next manifest version put once armed, as if another writer had committed first:
/// the flush wrote its SoT file but never committed (same effect as dying before the commit).
struct FailNextCommit {
    inner: Arc<MemObjectStore>,
    manifest_prefix: String,
    armed: std::sync::atomic::AtomicBool,
}

impl ObjectStore for FailNextCommit {
    fn caps(&self) -> seabed_ports::ObjectStoreCaps {
        self.inner.caps()
    }
    fn get(
        &self,
        key: &seabed_domain::Key,
    ) -> Result<(Vec<u8>, seabed_domain::ETag), seabed_ports::StoreError> {
        self.inner.get(key)
    }
    fn get_range(
        &self,
        key: &seabed_domain::Key,
        range: std::ops::Range<u64>,
    ) -> Result<Vec<u8>, seabed_ports::StoreError> {
        self.inner.get_range(key, range)
    }
    fn put(
        &self,
        key: &seabed_domain::Key,
        data: Vec<u8>,
        cond: seabed_ports::PutCondition,
    ) -> Result<seabed_domain::ETag, seabed_ports::StoreError> {
        if key.as_str().starts_with(&self.manifest_prefix)
            && self.armed.swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(seabed_ports::StoreError::PreconditionFailed);
        }
        self.inner.put(key, data, cond)
    }
    fn list(
        &self,
        prefix: &str,
    ) -> Result<Vec<seabed_ports::ObjectMeta>, seabed_ports::StoreError> {
        self.inner.list(prefix)
    }
    fn delete(&self, key: &seabed_domain::Key) -> Result<(), seabed_ports::StoreError> {
        self.inner.delete(key)
    }
}

/// A flush that wrote its SoT but never committed must not block the next flush of the same WAL
/// range, even after the schema grew in between (different bytes for the same range). The
/// segment id's fingerprint keeps the two files apart; the abandoned one is an orphan for GC.
#[test]
fn an_uncommitted_flush_does_not_block_a_later_flush_after_the_schema_grew() {
    let mem = Arc::new(MemObjectStore::new());
    let failing = Arc::new(FailNextCommit {
        inner: mem.clone(),
        manifest_prefix: keys().manifest_prefix(),
        armed: std::sync::atomic::AtomicBool::new(false),
    });
    let n = Namespace::new(failing.clone(), keys(), DIMS).unwrap();
    n.write(1, &[Op::upsert("a", vec![]).with_attr("price", price(1))])
        .unwrap();

    failing
        .armed
        .store(true, std::sync::atomic::Ordering::SeqCst);
    assert_eq!(
        n.flush().unwrap_err(),
        NamespaceError::Manifest(ManifestError::CommitConflict)
    );
    let orphan_files = segment_objects(&mem).len();
    assert_eq!(orphan_files, 1, "the abandoned flush left its SoT file");

    // The schema grows WITHOUT a new WAL entry, so the next flush covers exactly the same range
    // (1..=1) but must encode an extra column: as if a writer committed a new attribute and then
    // crashed before appending.
    let ms = ManifestStore::new(mem.clone(), keys()).unwrap();
    let (v, mut c) = ms.load_latest_content().unwrap().unwrap();
    c.extend_attr_schema(&[("color".to_string(), AttrKind::Str)].into_iter().collect())
        .unwrap();
    ms.commit_content(Some(v), 99, &c).unwrap();

    let f = n.flush().unwrap().unwrap();
    assert_eq!(
        f.indexed_through.get(),
        1,
        "same WAL range as the abandoned flush"
    );
    assert_eq!(ids(&n.read_all().unwrap()), ["a"]);
    assert_eq!(segment_objects(&mem).len(), 2, "one orphan, one live file");
}
