use std::collections::BTreeSet;
use std::sync::Arc;
use std::thread;

use proptest::prelude::*;

use seabed_adapter_objectstore_mem::{
    CountingObjectStore, FaultConfig, FaultInjecting, MemObjectStore,
};
use seabed_domain::{Lsn, NamespaceId, NamespaceKeys, TenantId};
use seabed_engine::manifest::{ManifestError, ManifestStore};
use seabed_engine::manifest_content::{
    AttrKind, AttrSchema, ContentError, Distance, IndexBinding, IndexRef, ManifestContent,
    SegmentRef, Sha256Hex,
};
use seabed_engine::segment::{SegmentBuilder, SegmentError, SegmentStore, DATA_FILE};
use seabed_engine::sot::{schema_of, AttrValue, DocRow, SotError, SotRef, SotStore};
use seabed_engine::RetryPolicy;
use seabed_ports::{ObjectStore, PutCondition};

fn keys() -> NamespaceKeys {
    NamespaceKeys::new(
        TenantId::new("acme").unwrap(),
        NamespaceId::new("ads").unwrap(),
    )
}

fn other_keys() -> NamespaceKeys {
    NamespaceKeys::new(
        TenantId::new("acme").unwrap(),
        NamespaceId::new("other").unwrap(),
    )
}

fn binding() -> IndexBinding {
    IndexBinding {
        embedding_model: "e5-large@2".into(),
        dimensions: 768,
        distance: Distance::Cosine,
        quantization: "sq8".into(),
        tokenizer: Some("lindera-ko-dic".into()),
        tokenizer_dict_version: Some("2.1.2".into()),
    }
}

fn seg_ref(id: &str) -> SegmentRef {
    SegmentRef {
        id: id.into(),
        docs: SotRef {
            size: 100,
            sha256: Sha256Hex([0xcd; 32]),
        },
        index: Some(IndexRef {
            size: 100,
            toc_sha256: Sha256Hex([0xab; 32]),
        }),
    }
}

fn sample_content() -> ManifestContent {
    let mut c = ManifestContent::empty(&keys());
    c.indexed_through = Some(42);
    c.bind(binding()).unwrap();
    c.add_segment(seg_ref("seg-0001")).unwrap();
    c
}

#[test]
fn roundtrip_and_deterministic_json() {
    let c = sample_content();
    let bytes = c.encode();
    assert_eq!(bytes, sample_content().encode(), "same content, same bytes");
    assert_eq!(ManifestContent::decode(&bytes, &keys()).unwrap(), c);
    assert_eq!(c.indexed_through(), Some(Lsn::new(42)));

    // Readable with plain tools: hex digest, snake_case enum.
    let text = String::from_utf8(bytes).unwrap();
    assert!(text.contains(&format!("\"toc_sha256\":\"{}\"", "ab".repeat(32))));
    assert!(text.contains("\"distance\":\"cosine\""));
}

#[test]
fn decode_rejects_another_namespace() {
    let bytes = sample_content().encode();
    assert_eq!(
        ManifestContent::decode(&bytes, &other_keys()).unwrap_err(),
        ContentError::WrongNamespace {
            manifest: "acme/ads".into(),
            expected: "acme/other".into()
        }
    );
}

#[test]
fn decode_rejects_bad_input() {
    let k = keys();
    let mut future = sample_content();
    future.format_version = 99;
    assert_eq!(
        ManifestContent::decode(&future.encode(), &k).unwrap_err(),
        ContentError::UnsupportedVersion(99)
    );

    let mut dup = sample_content();
    dup.segments.push(seg_ref("seg-0001"));
    assert_eq!(
        ManifestContent::decode(&dup.encode(), &k).unwrap_err(),
        ContentError::DuplicateSegment("seg-0001".into())
    );

    let mut bad_id = sample_content();
    bad_id.segments.push(seg_ref("../escape"));
    assert_eq!(
        ManifestContent::decode(&bad_id.encode(), &k).unwrap_err(),
        ContentError::InvalidSegmentId("../escape".into())
    );

    let short_hex = String::from_utf8(sample_content().encode())
        .unwrap()
        .replace(&"ab".repeat(32), "abcd");
    assert!(matches!(
        ManifestContent::decode(short_hex.as_bytes(), &k),
        Err(ContentError::Decode(_))
    ));
    assert!(matches!(
        ManifestContent::decode(b"not json", &k),
        Err(ContentError::Decode(_))
    ));
}

#[test]
fn add_segment_validates_ids() {
    let mut c = ManifestContent::empty(&keys());
    c.add_segment(seg_ref("a_1-B")).unwrap();
    // 128 characters is the longest accepted id; 129 is rejected below.
    c.add_segment(seg_ref(&"x".repeat(128))).unwrap();
    assert_eq!(
        c.add_segment(seg_ref("a_1-B")).unwrap_err(),
        ContentError::DuplicateSegment("a_1-B".into())
    );
    for bad in ["", "has space", "slash/inside", &"x".repeat(129)] {
        assert_eq!(
            c.add_segment(seg_ref(bad)).unwrap_err(),
            ContentError::InvalidSegmentId(bad.to_string())
        );
    }
}

#[test]
fn binding_is_set_once_then_enforced() {
    let mut c = ManifestContent::empty(&keys());
    let mut other = binding();
    other.dimensions = 1024;
    // Unbound namespace accepts any binding.
    assert!(c.check_binding(&other).is_ok());
    c.bind(binding()).unwrap();
    c.bind(binding()).unwrap();
    assert!(c.check_binding(&binding()).is_ok());
    assert_eq!(
        c.check_binding(&other).unwrap_err(),
        ContentError::BindingMismatch {
            field: "dimensions",
            manifest: "768".into(),
            requested: "1024".into()
        }
    );
    let mut new_dict = binding();
    new_dict.tokenizer_dict_version = Some("2.2.0".into());
    assert!(matches!(
        c.bind(new_dict),
        Err(ContentError::BindingMismatch {
            field: "tokenizer_dict_version",
            ..
        })
    ));
}

#[test]
fn segments_written_then_committed_can_be_read_back_from_the_latest_manifest() {
    let mem = Arc::new(MemObjectStore::new());
    let k = keys();
    let segs = SegmentStore::new(mem.clone()).unwrap();
    let ms = ManifestStore::new(mem.clone(), k.clone()).unwrap();

    // First flush: one segment covering WAL up to LSN 10.
    let mut b1 = SegmentBuilder::new();
    b1.add("vectors", vec![1u8; 64]).unwrap();
    let sots = SotStore::new(mem.clone());
    let r1 = SegmentRef {
        id: "seg-0001".into(),
        docs: sots
            .write(&k, "seg-0001", &[], &AttrSchema::new(), 768)
            .unwrap(),
        index: Some(segs.write_segment(&k, "seg-0001", &b1).unwrap()),
    };
    let mut c = ManifestContent::empty(&k);
    c.bind(binding()).unwrap();
    c.add_segment(r1).unwrap();
    c.indexed_through = Some(10);
    let v1 = ms.commit_content(None, 1, &c).unwrap();

    // Second flush builds on the latest content.
    let (latest_v, mut c) = ms.load_latest_content().unwrap().unwrap();
    assert_eq!(latest_v, v1);
    c.check_binding(&binding()).unwrap();
    let mut b2 = SegmentBuilder::new();
    b2.add("vectors", vec![2u8; 32]).unwrap();
    c.add_segment(SegmentRef {
        id: "seg-0002".into(),
        docs: sots
            .write(&k, "seg-0002", &[], &AttrSchema::new(), 768)
            .unwrap(),
        index: Some(segs.write_segment(&k, "seg-0002", &b2).unwrap()),
    })
    .unwrap();
    c.indexed_through = Some(25);
    ms.commit_content(Some(v1), 2, &c).unwrap();

    let (_, latest) = ms.load_latest_content().unwrap().unwrap();
    assert_eq!(latest.indexed_through(), Some(Lsn::new(25)));
    let ids: Vec<_> = latest.segments.iter().map(|s| s.id.as_str()).collect();
    assert_eq!(ids, ["seg-0001", "seg-0002"]);
    let expected = [vec![1u8; 64], vec![2u8; 32]];
    for (s, want) in latest.segments.iter().zip(expected) {
        let index = s.index.as_ref().unwrap();
        assert_eq!(
            segs.open_ref(&k, &s.id, index)
                .unwrap()
                .read("vectors")
                .unwrap(),
            want
        );
    }
}

#[test]
fn open_ref_detects_a_different_object_at_the_recorded_key() {
    let mem = Arc::new(MemObjectStore::new());
    let k = keys();
    let segs = SegmentStore::new(mem.clone()).unwrap();
    let mut b = SegmentBuilder::new();
    b.add("vectors", vec![1u8; 16]).unwrap();
    let recorded = segs.write_segment(&k, "seg-0001", &b).unwrap();

    // Someone replaced the object with another segment of the same size.
    mem.delete(&k.segment_key("seg-0001", DATA_FILE)).unwrap();
    let mut other = SegmentBuilder::new();
    other.add("vectors", vec![9u8; 16]).unwrap();
    segs.write_segment(&k, "seg-0001", &other).unwrap();

    assert_eq!(
        segs.open_ref(&k, "seg-0001", &recorded).err(),
        Some(SegmentError::RefMismatch {
            id: "seg-0001".into()
        })
    );
}

#[test]
fn commit_content_refuses_content_of_another_namespace() {
    let mem = Arc::new(MemObjectStore::new());
    let ms = ManifestStore::new(mem, keys()).unwrap();
    let foreign = ManifestContent::empty(&other_keys());
    assert!(matches!(
        ms.commit_content(None, 1, &foreign),
        Err(ManifestError::Content(ContentError::WrongNamespace { .. }))
    ));
    assert!(ms.load_latest().unwrap().is_none(), "nothing was written");
}

#[test]
fn opaque_payload_is_not_valid_content() {
    let ms = ManifestStore::new(Arc::new(MemObjectStore::new()), keys()).unwrap();
    ms.commit(None, 1, b"raw bytes").unwrap();
    assert!(matches!(
        ms.load_latest_content(),
        Err(ManifestError::Content(ContentError::Decode(_)))
    ));
}

// ---- Added after pre-merge review (PR #8) ----

#[test]
fn binding_mismatch_is_reported_for_every_field() {
    type Mutate = fn(&mut IndexBinding);
    let cases: [(&str, Mutate); 6] = [
        ("embedding_model", |b| b.embedding_model = "other@1".into()),
        ("dimensions", |b| b.dimensions = 1024),
        ("distance", |b| b.distance = Distance::L2),
        ("quantization", |b| b.quantization = "rabitq-1bit@1".into()),
        ("tokenizer", |b| b.tokenizer = None),
        ("tokenizer_dict_version", |b| {
            b.tokenizer_dict_version = Some("9.9.9".into())
        }),
    ];
    for (field, mutate) in cases {
        let mut requested = binding();
        mutate(&mut requested);
        match binding().check(&requested) {
            Err(ContentError::BindingMismatch { field: f, .. }) => assert_eq!(f, field),
            other => panic!("{field}: expected BindingMismatch, got {other:?}"),
        }
    }
}

#[test]
fn decode_rejects_another_tenant_with_the_same_namespace() {
    let bytes = sample_content().encode();
    let other_tenant = NamespaceKeys::new(
        TenantId::new("globex").unwrap(),
        NamespaceId::new("ads").unwrap(),
    );
    assert_eq!(
        ManifestContent::decode(&bytes, &other_tenant).unwrap_err(),
        ContentError::WrongNamespace {
            manifest: "acme/ads".into(),
            expected: "globex/ads".into()
        }
    );
}

#[test]
fn sha256_hex_accepts_only_canonical_lowercase() {
    let json = String::from_utf8(sample_content().encode()).unwrap();
    let canonical = "ab".repeat(32);
    for bad in ["AB".repeat(32), "+a".repeat(32), "zz".repeat(32)] {
        let tampered = json.replace(&canonical, &bad);
        assert!(
            matches!(
                ManifestContent::decode(tampered.as_bytes(), &keys()),
                Err(ContentError::Decode(_))
            ),
            "{bad} must be rejected"
        );
    }
}

#[test]
fn successor_rules_are_checked_without_a_store() {
    let unbound = ManifestContent::empty(&keys());
    // Nothing stored yet: any binding and any indexed_through may follow.
    unbound.check_successor(&sample_content()).unwrap();

    let prev = sample_content(); // bound, indexed_through = 42
    let mut same = prev.clone();
    same.indexed_through = Some(42);
    prev.check_successor(&same).unwrap();
    same.indexed_through = Some(43);
    prev.check_successor(&same).unwrap();

    let mut dropped = prev.clone();
    dropped.index_binding = None;
    assert!(matches!(
        prev.check_successor(&dropped),
        Err(ContentError::BindingMismatch {
            field: "index_binding",
            ..
        })
    ));

    for next in [Some(41), None] {
        let mut back = prev.clone();
        back.indexed_through = next;
        assert_eq!(
            prev.check_successor(&back).unwrap_err(),
            ContentError::IndexedThroughRegressed { previous: 42, next }
        );
    }
}

#[test]
fn commit_content_refuses_a_changed_binding_even_when_set_directly() {
    let ms = ManifestStore::new(Arc::new(MemObjectStore::new()), keys()).unwrap();
    let v1 = ms.commit_content(None, 1, &sample_content()).unwrap();

    // `bind` would refuse this; writing the public field bypasses it.
    let mut next = sample_content();
    next.index_binding.as_mut().unwrap().embedding_model = "other@1".into();
    assert!(matches!(
        ms.commit_content(Some(v1), 2, &next),
        Err(ManifestError::Content(ContentError::BindingMismatch {
            field: "embedding_model",
            ..
        }))
    ));
    assert_eq!(
        ms.load_latest().unwrap().unwrap().version,
        v1,
        "nothing written"
    );
}

#[test]
fn commit_content_refuses_indexed_through_going_backwards() {
    let ms = ManifestStore::new(Arc::new(MemObjectStore::new()), keys()).unwrap();
    let mut c = sample_content();
    c.indexed_through = Some(50);
    let v1 = ms.commit_content(None, 1, &c).unwrap();

    c.indexed_through = Some(10);
    assert_eq!(
        ms.commit_content(Some(v1), 2, &c).unwrap_err(),
        ManifestError::Content(ContentError::IndexedThroughRegressed {
            previous: 50,
            next: Some(10)
        })
    );
    c.indexed_through = Some(50);
    let v2 = ms.commit_content(Some(v1), 3, &c).unwrap();
    assert_eq!(v2.get(), 2);
}

#[test]
fn write_segment_rejects_an_invalid_id_without_writing_anything() {
    let mem = Arc::new(MemObjectStore::new());
    let segs = SegmentStore::new(mem.clone()).unwrap();
    let mut b = SegmentBuilder::new();
    b.add("vectors", vec![1u8; 8]).unwrap();
    for bad in ["../../n/other/segments/x", "", "a/b", "sp ace"] {
        assert_eq!(
            segs.write_segment(&keys(), bad, &b).unwrap_err(),
            SegmentError::InvalidSegmentId(bad.to_string())
        );
    }
    assert!(mem.list("").unwrap().is_empty(), "no orphan objects");
}

fn arb_binding() -> impl Strategy<Value = IndexBinding> {
    (
        "[a-z0-9@.-]{1,20}",
        1u32..4096,
        prop_oneof![
            Just(Distance::Cosine),
            Just(Distance::L2),
            Just(Distance::InnerProduct)
        ],
        "[a-z0-9@-]{1,12}",
        proptest::option::of("[a-z-]{1,12}"),
        proptest::option::of("[0-9.]{1,8}"),
    )
        .prop_map(|(m, d, dist, q, t, v)| IndexBinding {
            embedding_model: m,
            dimensions: d,
            distance: dist,
            quantization: q,
            tokenizer: t,
            tokenizer_dict_version: v,
        })
}

fn arb_content() -> impl Strategy<Value = ManifestContent> {
    (
        proptest::option::of(any::<u64>()),
        proptest::option::of(arb_binding()),
        proptest::collection::btree_set("[A-Za-z0-9_-]{1,16}", 0..6),
        proptest::collection::vec((any::<u64>(), any::<[u8; 32]>()), 6),
    )
        .prop_map(
            |(indexed, binding, ids, extra): (_, _, BTreeSet<String>, Vec<(u64, [u8; 32])>)| {
                let mut c = ManifestContent::empty(&keys());
                c.indexed_through = indexed;
                c.index_binding = binding;
                for (id, (size, sha)) in ids.into_iter().zip(extra) {
                    c.add_segment(SegmentRef {
                        id,
                        docs: SotRef {
                            size,
                            sha256: Sha256Hex(sha),
                        },
                        index: (size % 2 == 0).then_some(IndexRef {
                            size,
                            toc_sha256: Sha256Hex(sha),
                        }),
                    })
                    .unwrap();
                }
                c
            },
        )
}

proptest! {
    /// encode -> decode gives back the same content, and re-encoding gives the same bytes.
    #[test]
    fn content_roundtrips_byte_for_byte(c in arb_content()) {
        let bytes = c.encode();
        let back = ManifestContent::decode(&bytes, &keys()).unwrap();
        prop_assert_eq!(&back, &c);
        prop_assert_eq!(back.encode(), bytes);
    }
}

// ---- Added after the second pre-merge review (PR #8) ----

#[test]
fn decode_rejects_unknown_fields_at_every_level() {
    let json = String::from_utf8(sample_content().encode()).unwrap();
    let tampered = [
        json.replacen("{", "{\"future_field\":1,", 1),
        json.replace("\"dimensions\":768", "\"dimensions\":768,\"extra\":true"),
        json.replace("\"size\":100", "\"size\":100,\"extra\":true"),
    ];
    for t in tampered {
        assert!(
            matches!(
                ManifestContent::decode(t.as_bytes(), &keys()),
                Err(ContentError::Decode(_))
            ),
            "must reject: {t}"
        );
    }
}

#[test]
fn successor_check_costs_exactly_one_extra_get() {
    fn second_commit_gets(typed: bool) -> (u64, u64) {
        let store = Arc::new(CountingObjectStore::new(MemObjectStore::new()));
        let ms = ManifestStore::new(store.clone(), keys()).unwrap();
        let c = sample_content();
        let v1 = ms.commit_content(None, 1, &c).unwrap();
        let before = store.stats();
        if typed {
            ms.commit_content(Some(v1), 2, &c).unwrap();
        } else {
            ms.commit(Some(v1), 2, &c.encode()).unwrap();
        }
        let after = store.stats();
        (after.gets - before.gets, after.lists - before.lists)
    }
    let (raw_gets, raw_lists) = second_commit_gets(false);
    let (typed_gets, typed_lists) = second_commit_gets(true);
    assert_eq!(typed_gets, raw_gets + 1, "one GET of the expected version");
    assert_eq!(typed_lists, raw_lists, "no extra LIST");
}

#[test]
fn concurrent_commit_content_loser_gets_commit_conflict() {
    let ms = ManifestStore::new(Arc::new(MemObjectStore::new()), keys()).unwrap();
    let v1 = ms.commit_content(None, 1, &sample_content()).unwrap();
    // Two writers both read v1 and try to commit v2.
    let mut a = sample_content();
    a.indexed_through = Some(60);
    let mut b = sample_content();
    b.indexed_through = Some(70);
    ms.commit_content(Some(v1), 2, &a).unwrap();
    assert_eq!(
        ms.commit_content(Some(v1), 3, &b).unwrap_err(),
        ManifestError::CommitConflict
    );
    let (_, latest) = ms.load_latest_content().unwrap().unwrap();
    assert_eq!(
        latest.indexed_through,
        Some(60),
        "the winner's content stands"
    );
}

#[test]
fn commit_content_after_a_missing_expected_version_is_a_conflict() {
    let ms = ManifestStore::new(Arc::new(MemObjectStore::new()), keys()).unwrap();
    ms.commit_content(None, 1, &sample_content()).unwrap();
    assert_eq!(
        ms.commit_content(Some(seabed_domain::Version::new(5)), 2, &sample_content())
            .unwrap_err(),
        ManifestError::CommitConflict
    );
    assert_eq!(ms.load_latest().unwrap().unwrap().version.get(), 1);
}

// ---- Follow-ups deferred from the third pre-merge review of PR #8 ----

#[test]
fn load_latest_content_rejects_a_foreign_or_invalid_payload_written_raw() {
    // Raw `commit` skips content validation, so the read path is the only guard here.
    let ms = ManifestStore::new(Arc::new(MemObjectStore::new()), keys()).unwrap();
    let v1 = ms
        .commit(None, 1, &ManifestContent::empty(&other_keys()).encode())
        .unwrap();
    assert!(matches!(
        ms.load_latest_content(),
        Err(ManifestError::Content(ContentError::WrongNamespace { .. }))
    ));

    let mut escaping = ManifestContent::empty(&keys());
    escaping.segments.push(seg_ref("../x")); // bypasses add_segment on purpose
    ms.commit(Some(v1), 2, &escaping.encode()).unwrap();
    assert_eq!(
        ms.load_latest_content().unwrap_err(),
        ManifestError::Content(ContentError::InvalidSegmentId("../x".into()))
    );
}

#[test]
fn open_ref_with_a_different_size_replacement_is_corrupt_not_ref_mismatch() {
    let mem = Arc::new(MemObjectStore::new());
    let k = keys();
    let segs = SegmentStore::new(mem.clone()).unwrap();
    let mut b = SegmentBuilder::new();
    b.add("vectors", vec![1u8; 16]).unwrap();
    let recorded = segs.write_segment(&k, "seg-0001", &b).unwrap();

    let mut bigger = SegmentBuilder::new();
    bigger.add("vectors", vec![9u8; 64]).unwrap();
    mem.put(
        &k.segment_key("seg-0001", DATA_FILE),
        bigger.encode(),
        PutCondition::Unconditional,
    )
    .unwrap();

    let err = segs.open_ref(&k, "seg-0001", &recorded).err().unwrap();
    assert!(
        matches!(err, SegmentError::Corrupt(_)),
        "a size change is caught by the footer read, not the hash: {err:?}"
    );
}

/// Several writers race on `commit_content` while the store drops requests and responses.
/// Whatever interleaving wins, every committed version must be a legal successor of the one
/// before it, and every writer's accepted commit must appear exactly once.
#[test]
fn concurrent_commit_content_with_faults_keeps_successor_rules() {
    const WRITERS: u64 = 4;
    const PER_WRITER: u64 = 5;
    for seed in 1..=10u64 {
        let mem = Arc::new(MemObjectStore::new());
        let cfg = FaultConfig {
            seed,
            put_unavailable_ppm: 100_000,
            put_lose_response_ppm: 150_000,
            put_conflict_ppm: 50_000,
            get_unavailable_ppm: 100_000,
            list_unavailable_ppm: 100_000,
        };
        // One fault injector per writer, all over the same store: the race is real, and each
        // writer's fault sequence depends only on its own seed (`seed * WRITERS + w`).
        let store_for = |s: u64| {
            ManifestStore::new(
                Arc::new(FaultInjecting::new(
                    mem.clone(),
                    FaultConfig { seed: s, ..cfg },
                )),
                keys(),
            )
            .unwrap()
            .with_retry_policy(RetryPolicy::immediate(64))
        };
        let ms = store_for(seed * 1000);
        // Even a lone writer can see an injected 409 (`CommitConflict`); the contract is to
        // reload and retry.
        let mut id = 0;
        while ms.load_latest().unwrap().is_none() {
            id += 1;
            match ms.commit_content(None, id, &sample_content()) {
                Ok(_) | Err(ManifestError::CommitConflict) => {}
                Err(e) => panic!("seed {seed}: {e:?}"),
            }
        }

        thread::scope(|sc| {
            for w in 0..WRITERS {
                let ms = store_for(seed * WRITERS + w);
                sc.spawn(move || {
                    let mut done = 0;
                    let mut attempt = 0u64;
                    while done < PER_WRITER {
                        attempt += 1;
                        // Fail instead of hanging CI if commits stop making progress.
                        assert!(
                            attempt < 10_000,
                            "seed {seed} writer {w}: no progress after {attempt} attempts"
                        );
                        let (v, prev) = ms.load_latest_content().unwrap().unwrap();
                        let commit_id = (w + 1) * 1_000_000 + attempt;

                        // An illegal successor must be refused whatever else is going on.
                        let mut back = prev.clone();
                        back.indexed_through = Some(prev.indexed_through.unwrap() - 1);
                        match ms.commit_content(Some(v), commit_id, &back) {
                            Err(ManifestError::Content(
                                ContentError::IndexedThroughRegressed { .. },
                            )) => {}
                            other => panic!("regression must be refused, got {other:?}"),
                        }

                        let mut next = prev;
                        next.indexed_through = Some(next.indexed_through.unwrap() + 1);
                        next.add_segment(seg_ref(&format!("w{w}-{done}"))).unwrap();
                        match ms.commit_content(Some(v), commit_id + 500_000, &next) {
                            Ok(_) => done += 1,
                            Err(ManifestError::CommitConflict) => {}
                            Err(e) => panic!("seed {seed}: unexpected error {e:?}"),
                        }
                    }
                });
            }
        });

        // Read the history back through the clean store.
        let clean = ManifestStore::new(mem, keys()).unwrap();
        let (latest, _) = clean.load_latest_content().unwrap().unwrap();
        assert_eq!(latest.get(), 1 + WRITERS * PER_WRITER, "seed {seed}");
        let mut prev: Option<ManifestContent> = None;
        for v in 1..=latest.get() {
            let m = clean.load(seabed_domain::Version::new(v)).unwrap().unwrap();
            let c = ManifestContent::decode(&m.payload, &keys()).unwrap();
            if let Some(p) = &prev {
                p.check_successor(&c)
                    .unwrap_or_else(|e| panic!("seed {seed}: v{v} breaks rules: {e}"));
            }
            prev = Some(c);
        }
        let ids: BTreeSet<_> = prev.unwrap().segments.into_iter().map(|s| s.id).collect();
        for w in 0..WRITERS {
            for n in 0..PER_WRITER {
                assert!(
                    ids.contains(&format!("w{w}-{n}")),
                    "seed {seed}: w{w}-{n} lost"
                );
            }
        }
    }
}

// ---- Added after the pre-merge review of PR #10 ----

#[test]
fn sha256_hex_stored_form_is_pinned() {
    // The stored form must never change; a symmetric encode/decode change would still
    // roundtrip, so pin the exact text.
    let h = Sha256Hex(std::array::from_fn(|i| i as u8));
    let expected = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
    assert_eq!(h.to_hex(), expected);
    assert_eq!(
        serde_json::to_string(&h).unwrap(),
        format!("\"{expected}\"")
    );
}

#[test]
fn open_ref_with_a_smaller_replacement_is_not_ref_mismatch() {
    let mem = Arc::new(MemObjectStore::new());
    let k = keys();
    let segs = SegmentStore::new(mem.clone()).unwrap();
    let mut b = SegmentBuilder::new();
    b.add("vectors", vec![1u8; 64]).unwrap();
    let recorded = segs.write_segment(&k, "seg-0001", &b).unwrap();

    let mut smaller = SegmentBuilder::new();
    smaller.add("vectors", vec![9u8; 16]).unwrap();
    mem.put(
        &k.segment_key("seg-0001", DATA_FILE),
        smaller.encode(),
        PutCondition::Unconditional,
    )
    .unwrap();

    // Reading the footer at the recorded (larger) size runs past the end of the object.
    let err = segs.open_ref(&k, "seg-0001", &recorded).err().unwrap();
    assert!(
        matches!(
            err,
            SegmentError::Corrupt(_) | SegmentError::Store(seabed_ports::StoreError::InvalidRange)
        ),
        "a smaller object is caught before the hash check: {err:?}"
    );
}

#[test]
fn open_ref_accepts_a_replacement_that_keeps_the_original_bytes_as_a_prefix() {
    // Documented boundary: only the recorded `size` bytes are read, and they are unchanged.
    let mem = Arc::new(MemObjectStore::new());
    let k = keys();
    let segs = SegmentStore::new(mem.clone()).unwrap();
    let mut b = SegmentBuilder::new();
    b.add("vectors", vec![1u8; 16]).unwrap();
    let recorded = segs.write_segment(&k, "seg-0001", &b).unwrap();

    let mut extended = b.encode();
    extended.extend_from_slice(b"trailing garbage");
    mem.put(
        &k.segment_key("seg-0001", DATA_FILE),
        extended,
        PutCondition::Unconditional,
    )
    .unwrap();

    let r = segs.open_ref(&k, "seg-0001", &recorded).unwrap();
    assert_eq!(r.read("vectors").unwrap(), vec![1u8; 16]);
}

// ---- Namespace attribute schema ----

fn schema(pairs: &[(&str, AttrKind)]) -> AttrSchema {
    pairs.iter().map(|(n, k)| (n.to_string(), *k)).collect()
}

#[test]
fn attr_schema_only_grows_between_versions() {
    let mut prev = ManifestContent::empty(&keys());
    prev.extend_attr_schema(&schema(&[("price", AttrKind::I64)]))
        .unwrap();

    let mut grown = prev.clone();
    grown
        .extend_attr_schema(&schema(&[("color", AttrKind::Str)]))
        .unwrap();
    prev.check_successor(&grown).unwrap();

    let mut retyped = prev.clone();
    retyped.attr_schema.insert("price".into(), AttrKind::F64); // bypasses extend on purpose
    assert_eq!(
        prev.check_successor(&retyped).unwrap_err(),
        ContentError::AttrSchemaChanged {
            name: "price".into(),
            before: AttrKind::I64,
            after: Some(AttrKind::F64)
        }
    );

    let mut dropped = prev.clone();
    dropped.attr_schema.clear();
    assert_eq!(
        prev.check_successor(&dropped).unwrap_err(),
        ContentError::AttrSchemaChanged {
            name: "price".into(),
            before: AttrKind::I64,
            after: None
        }
    );
}

#[test]
fn extend_attr_schema_is_all_or_nothing() {
    let mut c = ManifestContent::empty(&keys());
    c.extend_attr_schema(&schema(&[("price", AttrKind::I64)]))
        .unwrap();
    let before = c.attr_schema.clone();
    // One good addition and one conflicting: neither is applied.
    assert!(matches!(
        c.extend_attr_schema(&schema(&[
            ("color", AttrKind::Str),
            ("price", AttrKind::F64)
        ])),
        Err(ContentError::AttrSchemaChanged { .. })
    ));
    assert_eq!(c.attr_schema, before);
    assert_eq!(
        c.extend_attr_schema(&schema(&[("Bad", AttrKind::Str)]))
            .unwrap_err(),
        ContentError::InvalidAttrName("Bad".into())
    );
    assert_eq!(c.attr_schema, before);
}

#[test]
fn decode_rejects_an_invalid_attribute_name_in_the_schema() {
    let mut c = ManifestContent::empty(&keys());
    c.attr_schema.insert("lsn".into(), AttrKind::I64); // reserved column name
    assert_eq!(
        ManifestContent::decode(&c.encode(), &keys()).unwrap_err(),
        ContentError::InvalidAttrName("lsn".into())
    );
}

#[test]
fn commit_content_refuses_an_attribute_type_change() {
    let ms = ManifestStore::new(Arc::new(MemObjectStore::new()), keys()).unwrap();
    let mut c = ManifestContent::empty(&keys());
    c.extend_attr_schema(&schema(&[("price", AttrKind::I64)]))
        .unwrap();
    let v1 = ms.commit_content(None, 1, &c).unwrap();
    c.attr_schema.insert("price".into(), AttrKind::Str);
    assert!(matches!(
        ms.commit_content(Some(v1), 2, &c),
        Err(ManifestError::Content(
            ContentError::AttrSchemaChanged { .. }
        ))
    ));
    assert_eq!(
        ms.load_latest().unwrap().unwrap().version,
        v1,
        "nothing written"
    );
}

/// What a flush does: extend the namespace schema with the rows' attributes, write the SoT
/// with that schema, then commit a manifest listing the segment.
fn flush(
    ms: &ManifestStore<MemObjectStore>,
    sots: &SotStore<MemObjectStore>,
    id: &str,
    rows: &[DocRow],
    through: u64,
) -> Result<(), ContentError> {
    let latest = ms.load_latest_content().unwrap();
    let (expected, mut c) = match latest {
        Some((v, c)) => (Some(v), c),
        None => (None, ManifestContent::empty(&keys())),
    };
    // Schema first: a conflicting type is refused before any object is written.
    c.extend_attr_schema(&schema_of(rows).unwrap())?;
    let docs = sots.write(&keys(), id, rows, &c.attr_schema, 3).unwrap();
    c.add_segment(SegmentRef {
        id: id.into(),
        docs,
        index: None,
    })?;
    c.indexed_through = Some(through);
    ms.commit_content(expected, through, &c).unwrap();
    Ok(())
}

#[test]
fn flushes_with_a_growing_schema_read_back_consistently_and_type_changes_are_refused() {
    let mem = Arc::new(MemObjectStore::new());
    let ms = ManifestStore::new(mem.clone(), keys()).unwrap();
    let sots = SotStore::new(mem.clone());

    let first = vec![DocRow::upsert("a", 1, vec![]).with_attr("price", AttrValue::I64(10))];
    flush(&ms, &sots, "seg-0001", &first, 1).unwrap();
    let second = vec![DocRow::upsert("b", 2, vec![])
        .with_attr("price", AttrValue::I64(20))
        .with_attr("color", AttrValue::Str("red".into()))];
    flush(&ms, &sots, "seg-0002", &second, 2).unwrap();

    // `price` as a float would silently corrupt readers of older files: refused, nothing written.
    let objects_before = mem.list("").unwrap().len();
    let retyped = vec![DocRow::upsert("c", 3, vec![]).with_attr("price", AttrValue::F64(1.5))];
    assert!(matches!(
        flush(&ms, &sots, "seg-0003", &retyped, 3),
        Err(ContentError::AttrSchemaChanged { .. })
    ));
    assert_eq!(
        mem.list("").unwrap().len(),
        objects_before,
        "no SoT file written"
    );

    // Read every segment the latest manifest lists, with the manifest's schema.
    let (_, latest) = ms.load_latest_content().unwrap().unwrap();
    assert_eq!(
        latest.attr_schema,
        schema(&[("color", AttrKind::Str), ("price", AttrKind::I64)])
    );
    let mut all = Vec::new();
    for s in &latest.segments {
        all.extend(
            sots.read(&keys(), &s.id, &s.docs, &latest.attr_schema, 3)
                .unwrap(),
        );
    }
    assert_eq!(all.len(), 2);
    assert_eq!(all[0].attrs.get("price"), Some(&AttrValue::I64(10)));
    assert_eq!(
        all[0].attrs.get("color"),
        None,
        "older file lacks the newer column"
    );
    assert_eq!(
        all[1].attrs.get("color"),
        Some(&AttrValue::Str("red".into()))
    );

    // A row whose attribute is not in the schema cannot be written by mistake.
    let unknown = [DocRow::upsert("d", 4, vec![]).with_attr("size", AttrValue::I64(1))];
    assert_eq!(
        sots.write(&keys(), "seg-0009", &unknown, &latest.attr_schema, 3)
            .unwrap_err(),
        SotError::UnknownAttr("size".into())
    );
}

// ---- Added after PR #13 round-1 review ----

#[test]
fn decode_reports_a_v1_manifest_as_unsupported_version() {
    // The exact v1 layout: segments carried size/toc_sha256 directly and there was no
    // attr_schema. It must read as "other version", not as a damaged manifest.
    let v1 = format!(
        r#"{{"format_version":1,"tenant":"acme","namespace":"ads","indexed_through":null,"index_binding":null,"segments":[{{"id":"seg-0001","size":100,"toc_sha256":"{}"}}]}}"#,
        "ab".repeat(32)
    );
    assert_eq!(
        ManifestContent::decode(v1.as_bytes(), &keys()).unwrap_err(),
        ContentError::UnsupportedVersion(1)
    );
}

#[test]
fn open_ref_rejects_an_invalid_segment_id() {
    let segs = SegmentStore::new(Arc::new(MemObjectStore::new())).unwrap();
    let index = IndexRef {
        size: 100,
        toc_sha256: Sha256Hex([0; 32]),
    };
    assert_eq!(
        segs.open_ref(&keys(), "../x", &index).err(),
        Some(SegmentError::InvalidSegmentId("../x".into()))
    );
}

/// Two writers start from the same manifest and widen the schema with conflicting types.
/// Exactly one commits; the loser sees the conflict on reload; its already-written SoT file
/// stays behind as an orphan the manifest never lists (GC's job, documented in PLAN 6.7).
#[test]
fn racing_flushes_with_conflicting_attr_types_commit_only_one() {
    let mem = Arc::new(MemObjectStore::new());
    let ms = ManifestStore::new(mem.clone(), keys()).unwrap();
    let sots = SotStore::new(mem.clone());
    let v1 = ms
        .commit_content(None, 1, &ManifestContent::empty(&keys()))
        .unwrap();

    let (_, base) = ms.load_latest_content().unwrap().unwrap();
    let writer = |id: &str, value: AttrValue, commit_id: u64| {
        let rows = vec![DocRow::upsert(id, commit_id, vec![]).with_attr("price", value)];
        let mut c = base.clone();
        c.extend_attr_schema(&schema_of(&rows).unwrap()).unwrap();
        let docs = sots.write(&keys(), id, &rows, &c.attr_schema, 3).unwrap();
        c.add_segment(SegmentRef {
            id: id.into(),
            docs,
            index: None,
        })
        .unwrap();
        ms.commit_content(Some(v1), commit_id, &c)
    };
    writer("seg-a", AttrValue::I64(1), 10).unwrap();
    assert_eq!(
        writer("seg-b", AttrValue::F64(1.5), 11).unwrap_err(),
        ManifestError::CommitConflict
    );

    // The loser reloads and now sees the schema conflict.
    let (_, latest) = ms.load_latest_content().unwrap().unwrap();
    let mut retry = latest.clone();
    assert!(matches!(
        retry.extend_attr_schema(&schema(&[("price", AttrKind::F64)])),
        Err(ContentError::AttrSchemaChanged { .. })
    ));

    // Only the winner is listed, and everything listed reads with the manifest schema.
    let ids: Vec<_> = latest.segments.iter().map(|s| s.id.as_str()).collect();
    assert_eq!(ids, ["seg-a"]);
    for s in &latest.segments {
        sots.read(&keys(), &s.id, &s.docs, &latest.attr_schema, 3)
            .unwrap();
    }
    // The loser's file exists but is unreferenced.
    let orphans: Vec<_> = mem.list(keys().segment_key("seg-b", "").as_str()).unwrap();
    assert_eq!(orphans.len(), 1, "one orphan SoT file, for GC");
}
