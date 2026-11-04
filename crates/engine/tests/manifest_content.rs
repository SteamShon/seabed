use std::sync::Arc;

use seabed_adapter_objectstore_mem::MemObjectStore;
use seabed_domain::{Lsn, NamespaceId, NamespaceKeys, TenantId};
use seabed_engine::manifest::{ManifestError, ManifestStore};
use seabed_engine::manifest_content::{
    ContentError, Distance, IndexBinding, ManifestContent, SegmentRef, Sha256Hex,
};
use seabed_engine::segment::{SegmentBuilder, SegmentError, SegmentStore, DATA_FILE};
use seabed_ports::ObjectStore;

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
        Some(SegmentError::Corrupt(
            "segment does not match the manifest record"
        ))
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
