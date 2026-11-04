use std::collections::BTreeSet;
use std::sync::Arc;
use std::thread;

use seabed_adapter_objectstore_mem::{FaultConfig, FaultInjecting, MemObjectStore};
use seabed_domain::{NamespaceId, NamespaceKeys, TenantId};
use seabed_engine::manifest::{ManifestError, ManifestStore};
use seabed_engine::manifest_content::{AttrKind, ContentError};
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
    // Type conflicting with the flushed namespace schema.
    assert!(matches!(
        n.write(
            9,
            &[Op::upsert("x", vec![]).with_attr("price", AttrValue::F64(1.5))]
        ),
        Err(NamespaceError::Content(
            ContentError::AttrSchemaChanged { .. }
        ))
    ));
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

    // Two writers racing a brand-new attribute with different types: both pass `write` (the
    // schema does not know `size` yet); the flush that would mix them is refused, and no
    // segment object is written for it.
    n.write(3, &[Op::upsert("c", vec![]).with_attr("size", price(1))])
        .unwrap();
    n.write(
        4,
        &[Op::upsert("d", vec![]).with_attr("size", AttrValue::F64(1.0))],
    )
    .unwrap();
    let segments_before = segment_objects(&mem).len();
    assert!(matches!(
        n.flush(),
        Err(NamespaceError::Sot(SotError::AttrTypeConflict { .. }))
    ));
    assert_eq!(segment_objects(&mem).len(), segments_before);
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
