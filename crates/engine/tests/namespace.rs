use std::collections::BTreeSet;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::{Arc, Mutex};
use std::thread;

use seabed_adapter_objectstore_mem::{FaultConfig, FaultInjecting, MemObjectStore};
use seabed_domain::{ETag, Key, NamespaceId, NamespaceKeys, TenantId, Version};
use seabed_engine::manifest::{ManifestError, ManifestStore};
use seabed_engine::manifest_content::AttrKind;
use seabed_engine::namespace::{Namespace, NamespaceError, Op};
use seabed_engine::sot::{AttrValue, SotError};
use seabed_engine::wal::WalError;
use seabed_engine::RetryPolicy;
use seabed_ports::{ObjectMeta, ObjectStore, ObjectStoreCaps, PutCondition, StoreError};

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
    // Flushes that start from the same manifest commit identical content, so they share a
    // commit id: a loser that reads the winner's version back recognises it as its own commit
    // (harmless, the content is the same). Either way, exactly one version is created.
    let versions: BTreeSet<_> = results
        .iter()
        .filter_map(|r| r.as_ref().ok().and_then(|f| f.as_ref()).map(|f| f.version))
        .collect();
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
    assert_eq!(
        versions.len(),
        1,
        "every successful flush reports the same version"
    );

    let (latest, content) = ManifestStore::new(mem.clone(), keys())
        .unwrap()
        .load_latest_content()
        .unwrap()
        .unwrap();
    assert_eq!(latest.get(), 1, "one manifest version");
    assert_eq!(versions.into_iter().next(), Some(latest));
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
    .with_retry_policy(RetryPolicy::immediate(64))
    .with_write_retry_policy(RetryPolicy::immediate(64));
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

/// What a put hook decides for one put.
enum PutAction {
    /// Pass the put to the inner store.
    Pass,
    /// Fail without applying the put.
    Fail(StoreError),
    /// Apply the put, then report this error (a lost response).
    ApplyThenFail(StoreError),
}

type GetHook = Box<dyn FnMut(&Key) + Send>;
type PutHook = Box<dyn FnMut(&Key) -> PutAction + Send>;

/// A `MemObjectStore` view whose GETs and PUTs run test hooks first, so a test can make another
/// process act at an exact point of an operation, deterministically. Hooks act on the inner
/// store directly, never through this view.
struct Hooked {
    inner: Arc<MemObjectStore>,
    on_get: Mutex<Option<GetHook>>,
    on_put: Mutex<Option<PutHook>>,
}

impl Hooked {
    fn new(inner: &Arc<MemObjectStore>) -> Arc<Self> {
        Arc::new(Self {
            inner: inner.clone(),
            on_get: Mutex::new(None),
            on_put: Mutex::new(None),
        })
    }
    fn on_get(&self, hook: impl FnMut(&Key) + Send + 'static) {
        *self.on_get.lock().unwrap() = Some(Box::new(hook));
    }
    fn on_put(&self, hook: impl FnMut(&Key) -> PutAction + Send + 'static) {
        *self.on_put.lock().unwrap() = Some(Box::new(hook));
    }
}

impl ObjectStore for Hooked {
    fn caps(&self) -> ObjectStoreCaps {
        self.inner.caps()
    }
    fn get(&self, key: &Key) -> Result<(Vec<u8>, ETag), StoreError> {
        if let Some(hook) = self.on_get.lock().unwrap().as_mut() {
            hook(key);
        }
        self.inner.get(key)
    }
    fn get_range(&self, key: &Key, range: std::ops::Range<u64>) -> Result<Vec<u8>, StoreError> {
        self.inner.get_range(key, range)
    }
    fn put(&self, key: &Key, data: Vec<u8>, cond: PutCondition) -> Result<ETag, StoreError> {
        let action = match self.on_put.lock().unwrap().as_mut() {
            Some(hook) => hook(key),
            None => PutAction::Pass,
        };
        match action {
            PutAction::Pass => self.inner.put(key, data, cond),
            PutAction::Fail(e) => Err(e),
            PutAction::ApplyThenFail(e) => {
                self.inner.put(key, data, cond)?;
                Err(e)
            }
        }
    }
    fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>, StoreError> {
        self.inner.list(prefix)
    }
    fn delete(&self, key: &Key) -> Result<(), StoreError> {
        self.inner.delete(key)
    }
}

/// True for a manifest version object (not the pointer).
fn is_manifest_version(key: &Key) -> bool {
    keys().parse_manifest_version(key).is_some()
}

/// Number of WAL entries in the store.
fn wal_entries(mem: &MemObjectStore) -> usize {
    mem.list(&keys().wal_prefix()).unwrap().len()
}

fn unavailable() -> StoreError {
    StoreError::Unavailable("injected".into())
}

/// Deferred from PR #14 review (M11): a strong read must take its segments and its tail from the
/// same manifest version. Here a flush commits right after the reader loaded its manifest (at
/// the reader's first segment GET); every write acknowledged before the read must still be
/// returned exactly once, whatever the reader does after that point.
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

    let hooked = Hooked::new(&mem);
    let reader = Namespace::new(hooked.clone(), keys(), DIMS).unwrap();
    let ran = Arc::new(AtomicBool::new(false));
    let (mem2, ran2) = (mem.clone(), ran.clone());
    hooked.on_get(move |key| {
        if key.as_str().contains("/segments/") && !ran2.swap(true, SeqCst) {
            // Another process flushes b, the delete of a, and c while the read is in progress.
            assert!(ns(&mem2).flush().unwrap().is_some());
        }
    });
    let all = reader.read_all().unwrap();
    assert!(ran.load(SeqCst), "the flush ran mid-read");
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

/// A flush that wrote its SoT but never committed must not block the next flush of the same WAL
/// range, even after the schema grew in between (different bytes for the same range). The
/// segment id's fingerprint keeps the two files apart; the abandoned one is an orphan for GC.
#[test]
fn an_uncommitted_flush_does_not_block_a_later_flush_after_the_schema_grew() {
    let mem = Arc::new(MemObjectStore::new());
    let hooked = Hooked::new(&mem);
    let n = Namespace::new(hooked.clone(), keys(), DIMS).unwrap();
    n.write(1, &[Op::upsert("a", vec![]).with_attr("price", price(1))])
        .unwrap();

    // Right before the flush commits, another writer commits a new attribute (and crashes
    // before appending it, so the WAL range stays 1..=1). The flush loses the race: its SoT file
    // is written but never committed (same effect as dying before the commit). The next flush
    // covers exactly the same range but must encode an extra column.
    let armed = Arc::new(AtomicBool::new(true));
    let (armed2, mem2) = (armed.clone(), mem.clone());
    hooked.on_put(move |key| {
        if is_manifest_version(key) && armed2.swap(false, SeqCst) {
            let ms = ManifestStore::new(mem2.clone(), keys()).unwrap();
            let (v, mut c) = ms.load_latest_content().unwrap().unwrap();
            c.extend_attr_schema(&[("color".to_string(), AttrKind::Str)].into_iter().collect())
                .unwrap();
            ms.commit_content(Some(v), 99, &c).unwrap();
        }
        PutAction::Pass
    });
    assert_eq!(
        n.flush().unwrap_err(),
        NamespaceError::Manifest(ManifestError::CommitConflict)
    );
    let orphan_files = segment_objects(&mem).len();
    assert_eq!(orphan_files, 1, "the abandoned flush left its SoT file");

    let f = n.flush().unwrap().unwrap();
    assert_eq!(
        f.indexed_through.get(),
        1,
        "same WAL range as the abandoned flush"
    );
    assert_eq!(ids(&n.read_all().unwrap()), ["a"]);
    assert_eq!(segment_objects(&mem).len(), 2, "one orphan, one live file");
}

fn latest_manifest<S: ObjectStore>(
    store: &Arc<S>,
) -> (Version, seabed_engine::manifest_content::ManifestContent) {
    ManifestStore::new(store.clone(), keys())
        .unwrap()
        .load_latest_content()
        .unwrap()
        .unwrap()
}

/// A writer that loses the schema commit race to a writer adding the same type must reload and
/// succeed, appending exactly once and committing no manifest version of its own.
#[test]
fn same_type_new_attr_race_loser_succeeds_after_reload() {
    let mem = Arc::new(MemObjectStore::new());
    ns(&mem)
        .write(
            1,
            &[Op::upsert("seed", vec![]).with_attr("price", price(1))],
        )
        .unwrap();
    let (base, _) = latest_manifest(&mem);

    let hooked = Hooked::new(&mem);
    let loser = Namespace::new(hooked.clone(), keys(), DIMS).unwrap();
    let ran = Arc::new(AtomicBool::new(false));
    let (mem2, ran2) = (mem.clone(), ran.clone());
    hooked.on_put(move |key| {
        if is_manifest_version(key) && !ran2.swap(true, SeqCst) {
            // The winner commits the same new attribute type first.
            ns(&mem2)
                .write(
                    2,
                    &[Op::upsert("w", vec![]).with_attr("size", AttrValue::Str("m".into()))],
                )
                .unwrap();
        }
        PutAction::Pass
    });
    loser
        .write(
            3,
            &[Op::upsert("l", vec![]).with_attr("size", AttrValue::Str("s".into()))],
        )
        .unwrap();
    assert!(ran.load(SeqCst), "the race happened");

    let (v, content) = latest_manifest(&mem);
    assert_eq!(
        v.get(),
        base.get() + 1,
        "only the winner committed a schema version"
    );
    assert_eq!(content.attr_schema.get("size"), Some(&AttrKind::Str));
    assert_eq!(ids(&ns(&mem).read_all().unwrap()), ["l", "seed", "w"]);
    assert_eq!(
        wal_entries(&mem),
        3,
        "seed, winner, loser: the loser appended exactly once"
    );
    assert!(ns(&mem).flush().unwrap().is_some());
}

/// The schema commit's put reports `Unavailable`, and meanwhile another writer committed a
/// DIFFERENT type for the same attribute at that version. The writer must not mistake that
/// commit for its own (commit ids differ by content), so it is refused and appends nothing.
#[test]
fn ambiguous_schema_commit_does_not_adopt_a_conflicting_commit() {
    let mem = Arc::new(MemObjectStore::new());
    let hooked = Hooked::new(&mem);
    let a = Namespace::new(hooked.clone(), keys(), DIMS).unwrap();
    let ran = Arc::new(AtomicBool::new(false));
    let (mem2, ran2) = (mem.clone(), ran.clone());
    hooked.on_put(move |key| {
        if is_manifest_version(key) && !ran2.swap(true, SeqCst) {
            ns(&mem2)
                .write(1, &[Op::upsert("b", vec![]).with_attr("size", price(1))])
                .unwrap();
            return PutAction::Fail(unavailable());
        }
        PutAction::Pass
    });
    assert_eq!(
        a.write(
            2,
            &[Op::upsert("a", vec![]).with_attr("size", AttrValue::Str("s".into()))]
        ),
        Err(NamespaceError::Invalid(SotError::AttrTypeConflict {
            name: "size".into()
        }))
    );
    assert!(ran.load(SeqCst));
    assert_eq!(
        ids(&ns(&mem).read_all().unwrap()),
        ["b"],
        "nothing appended"
    );
    assert!(ns(&mem).flush().unwrap().is_some(), "flush is not poisoned");
}

/// Pins what a write costs: no manifest commit unless the batch adds attributes.
#[test]
fn write_request_cost_is_pinned() {
    use seabed_adapter_objectstore_mem::CountingObjectStore;
    let store = Arc::new(CountingObjectStore::new(MemObjectStore::new()));
    let n = Namespace::new(store.clone(), keys(), DIMS).unwrap();
    n.write(1, &[Op::upsert("a", vec![]).with_attr("price", price(1))])
        .unwrap();
    let (v1, _) = latest_manifest(&store);

    let before = store.stats();
    n.write(2, &[Op::upsert("b", vec![]).with_attr("price", price(2))])
        .unwrap();
    let after = store.stats();
    assert_eq!(
        after.puts - before.puts,
        1,
        "known attributes: one WAL put only"
    );
    assert_eq!(
        latest_manifest(&store).0,
        v1,
        "no manifest version for known attributes"
    );

    let before = store.stats();
    n.write(
        3,
        &[Op::upsert("c", vec![]).with_attr("color", AttrValue::Str("red".into()))],
    )
    .unwrap();
    let after = store.stats();
    // Manifest version + pointer + WAL entry.
    assert_eq!(after.puts - before.puts, 3);
    assert_eq!(
        latest_manifest(&store).0.get(),
        v1.get() + 1,
        "one version per schema extension"
    );
}

/// The schema commit lands but the WAL append fails. The write reports the failure, the new
/// attribute's type stays reserved, a retry with the same request id succeeds once, and flush
/// still works.
#[test]
fn failed_append_after_schema_commit_keeps_the_type_and_retries_cleanly() {
    let mem = Arc::new(MemObjectStore::new());
    let hooked = Hooked::new(&mem);
    let n = Namespace::new(hooked.clone(), keys(), DIMS)
        .unwrap()
        .with_write_retry_policy(RetryPolicy::immediate(4));
    let wal_down = Arc::new(AtomicBool::new(true));
    let wal_puts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (down, puts) = (wal_down.clone(), wal_puts.clone());
    let wal_prefix = keys().wal_prefix();
    hooked.on_put(move |key| {
        if key.as_str().starts_with(&wal_prefix) && down.load(SeqCst) {
            puts.fetch_add(1, SeqCst);
            PutAction::Fail(unavailable())
        } else {
            PutAction::Pass
        }
    });
    let batch = [Op::upsert("a", vec![]).with_attr("size", AttrValue::Str("s".into()))];
    // Each unclear put costs one try of the write retry budget; when it runs out the last
    // failure is returned (the outcome of that put is unknown to the writer).
    assert_eq!(
        n.write(7, &batch),
        Err(NamespaceError::Wal(WalError::Store(unavailable())))
    );
    assert_eq!(wal_puts.load(SeqCst), 4, "4 tries, then give up");
    assert_eq!(wal_entries(&mem), 0);

    let (_, content) = latest_manifest(&mem);
    assert_eq!(
        content.attr_schema.get("size"),
        Some(&AttrKind::Str),
        "type reserved"
    );
    assert!(n.read_all().unwrap().is_empty(), "nothing appended");
    assert_eq!(
        n.write(8, &[Op::upsert("x", vec![]).with_attr("size", price(1))]),
        Err(NamespaceError::Invalid(SotError::AttrTypeConflict {
            name: "size".into()
        }))
    );

    wal_down.store(false, SeqCst);
    n.write(7, &batch).unwrap();
    assert_eq!(ids(&n.read_all().unwrap()), ["a"]);
    assert_eq!(wal_entries(&mem), 1);
    assert!(n.flush().unwrap().is_some());
}

/// The schema commit lands but its response is lost. The writer must recognise its own commit
/// (same commit id), append exactly once, and commit no second version. Without the
/// recognition the result would look the same after a reload, so the test pins what only the
/// recognising path does: it advances the pointer to its version (a writer that took the commit
/// for someone else's would leave the pointer alone).
#[test]
fn schema_commit_with_lost_response_adopts_own_commit_and_appends_once() {
    let mem = Arc::new(MemObjectStore::new());
    let hooked = Hooked::new(&mem);
    let n = Namespace::new(hooked.clone(), keys(), DIMS).unwrap();
    let ran = Arc::new(AtomicBool::new(false));
    let ran2 = ran.clone();
    hooked.on_put(move |key| {
        if is_manifest_version(key) && !ran2.swap(true, SeqCst) {
            PutAction::ApplyThenFail(unavailable())
        } else {
            PutAction::Pass
        }
    });
    n.write(1, &[Op::upsert("a", vec![]).with_attr("size", price(1))])
        .unwrap();
    assert!(ran.load(SeqCst));
    let (pointer, _) = mem.get(&keys().manifest_pointer_key()).unwrap();
    assert_eq!(
        pointer,
        1u64.to_le_bytes(),
        "the writer adopted v1 as its own commit and advanced the pointer"
    );
    let (v, content) = latest_manifest(&mem);
    assert_eq!(v.get(), 1, "one schema version, no second commit");
    assert_eq!(content.attr_schema.get("size"), Some(&AttrKind::I64));
    assert_eq!(wal_entries(&mem), 1);
    assert_eq!(ids(&n.read_all().unwrap()), ["a"]);
    assert!(n.flush().unwrap().is_some());
}

/// A schema commit failing with a non-retryable store error is returned at once: no reload loop,
/// no WAL append.
#[test]
fn schema_commit_store_error_is_returned_not_retried() {
    let mem = Arc::new(MemObjectStore::new());
    let hooked = Hooked::new(&mem);
    let n = Namespace::new(hooked.clone(), keys(), DIMS).unwrap();
    let puts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let puts2 = puts.clone();
    hooked.on_put(move |key| {
        if is_manifest_version(key) {
            puts2.fetch_add(1, SeqCst);
            PutAction::Fail(StoreError::Unsupported("injected"))
        } else {
            PutAction::Pass
        }
    });
    assert_eq!(
        n.write(1, &[Op::upsert("a", vec![]).with_attr("size", price(1))]),
        Err(NamespaceError::Manifest(ManifestError::Store(
            StoreError::Unsupported("injected")
        )))
    );
    assert_eq!(puts.load(SeqCst), 1, "not retried");
    assert_eq!(wal_entries(&mem), 0);
}

/// `Namespace::with_write_retry_policy` reaches the manifest and the SoT store too, not only the
/// WAL: with those writes failing, `write` and `flush` give up after the configured tries.
#[test]
fn write_retry_policy_applies_to_manifest_commits_and_sot_writes() {
    let mem = Arc::new(MemObjectStore::new());
    let hooked = Hooked::new(&mem);
    let n = Namespace::new(hooked.clone(), keys(), DIMS)
        .unwrap()
        .with_write_retry_policy(RetryPolicy::immediate(4));
    let failing = Arc::new(Mutex::new(String::new()));
    let puts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (f, p) = (failing.clone(), puts.clone());
    hooked.on_put(move |key| {
        let what = f.lock().unwrap().clone();
        if !what.is_empty() && key.as_str().contains(what.as_str()) {
            p.fetch_add(1, SeqCst);
            PutAction::Fail(unavailable())
        } else {
            PutAction::Pass
        }
    });

    // A new attribute needs a manifest commit first.
    *failing.lock().unwrap() = keys().manifest_key(Version::new(1)).as_str().to_string();
    assert_eq!(
        n.write(1, &[Op::upsert("a", vec![]).with_attr("size", price(1))]),
        Err(NamespaceError::Manifest(
            ManifestError::Store(unavailable())
        ))
    );
    assert_eq!(puts.swap(0, SeqCst), 4);

    // Flushing needs a SoT write.
    *failing.lock().unwrap() = "/segments/".to_string();
    n.write(2, &[Op::upsert("b", vec![])]).unwrap();
    assert_eq!(
        n.flush(),
        Err(NamespaceError::Sot(SotError::Store(unavailable())))
    );
    assert_eq!(puts.load(SeqCst), 4);
}
