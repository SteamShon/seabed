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
    ContentError, Distance, IndexBinding, ManifestContent, SegmentRef, Sha256Hex,
};
use seabed_engine::segment::{SegmentBuilder, SegmentError, SegmentStore, DATA_FILE};
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
        size: 100,
        toc_sha256: Sha256Hex([0xab; 32]),
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
    let r1 = segs.write_segment(&k, "seg-0001", &b1).unwrap();
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
    c.add_segment(segs.write_segment(&k, "seg-0002", &b2).unwrap())
        .unwrap();
    c.indexed_through = Some(25);
    ms.commit_content(Some(v1), 2, &c).unwrap();

    let (_, latest) = ms.load_latest_content().unwrap().unwrap();
    assert_eq!(latest.indexed_through(), Some(Lsn::new(25)));
    let ids: Vec<_> = latest.segments.iter().map(|s| s.id.as_str()).collect();
    assert_eq!(ids, ["seg-0001", "seg-0002"]);
    let expected = [vec![1u8; 64], vec![2u8; 32]];
    for (s, want) in latest.segments.iter().zip(expected) {
        assert_eq!(segs.open_ref(&k, s).unwrap().read("vectors").unwrap(), want);
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
        segs.open_ref(&k, &recorded).err(),
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
                        size,
                        toc_sha256: Sha256Hex(sha),
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

    let err = segs.open_ref(&k, &recorded).err().unwrap();
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
    let err = segs.open_ref(&k, &recorded).err().unwrap();
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

    let r = segs.open_ref(&k, &recorded).unwrap();
    assert_eq!(r.read("vectors").unwrap(), vec![1u8; 16]);
}
