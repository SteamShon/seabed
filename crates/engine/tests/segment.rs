use std::sync::Arc;

use seabed_adapter_objectstore_mem::{CountingObjectStore, MemObjectStore};
use seabed_domain::{Key, NamespaceId, NamespaceKeys, TenantId};
use seabed_engine::segment::{
    SegmentBuilder, SegmentError, SegmentStore, FOOTER_LEN, FORMAT_VERSION, TAIL_HINT,
};
use seabed_ports::{ObjectStore, PutCondition};

fn key() -> Key {
    NamespaceKeys::new(
        TenantId::new("acme").unwrap(),
        NamespaceId::new("ads").unwrap(),
    )
    .segment_key("seg-1", "data.sbsg")
}

fn sample() -> SegmentBuilder {
    let mut b = SegmentBuilder::new();
    b.add("vectors", vec![7u8; 1000]).unwrap();
    b.add("attrs", b"color=red".to_vec()).unwrap();
    b.add("empty", Vec::new()).unwrap();
    b
}

/// Writes raw bytes (bypassing the builder) so tests can corrupt them.
fn put_raw(mem: &MemObjectStore, bytes: Vec<u8>) {
    mem.put(&key(), bytes, PutCondition::Unconditional).unwrap();
}

#[test]
fn roundtrip_reads_every_section() {
    let mem = Arc::new(MemObjectStore::new());
    let segs = SegmentStore::new(mem).unwrap();
    let size = segs.write(&key(), &sample()).unwrap().size;

    let r = segs.open(&key(), size).unwrap();
    let names: Vec<_> = r.sections().iter().map(|s| s.name.as_str()).collect();
    assert_eq!(
        names,
        ["attrs", "empty", "vectors"],
        "sections are in name order"
    );
    assert_eq!(r.read("vectors").unwrap(), vec![7u8; 1000]);
    assert_eq!(r.read("attrs").unwrap(), b"color=red");
    assert_eq!(r.read("empty").unwrap(), b"");
    assert_eq!(
        r.read("nope").unwrap_err(),
        SegmentError::MissingSection("nope".into())
    );
}

#[test]
fn encoding_is_deterministic_regardless_of_insertion_order() {
    let mut a = SegmentBuilder::new();
    a.add("x", b"1".to_vec()).unwrap();
    a.add("y", b"22".to_vec()).unwrap();
    let mut b = SegmentBuilder::new();
    b.add("y", b"22".to_vec()).unwrap();
    b.add("x", b"1".to_vec()).unwrap();
    assert_eq!(a.encode(), b.encode());
    assert_eq!(a.encode(), a.encode());
}

#[test]
fn empty_segment_is_valid() {
    let segs = SegmentStore::new(Arc::new(MemObjectStore::new())).unwrap();
    let size = segs.write(&key(), &SegmentBuilder::new()).unwrap().size;
    assert_eq!(size, (4 + FOOTER_LEN) as u64);
    assert!(segs.open(&key(), size).unwrap().sections().is_empty());
}

#[test]
fn segments_are_immutable() {
    let segs = SegmentStore::new(Arc::new(MemObjectStore::new())).unwrap();
    segs.write(&key(), &sample()).unwrap();
    assert_eq!(
        segs.write(&key(), &sample()).unwrap_err(),
        SegmentError::AlreadyExists
    );
}

#[test]
fn builder_rejects_bad_and_duplicate_names() {
    let mut b = SegmentBuilder::new();
    assert!(matches!(
        b.add("", vec![]),
        Err(SegmentError::InvalidName(_))
    ));
    assert!(matches!(
        b.add(&"n".repeat(256), vec![]),
        Err(SegmentError::InvalidName(_))
    ));
    b.add("a", vec![]).unwrap();
    assert_eq!(
        b.add("a", vec![1]).unwrap_err(),
        SegmentError::DuplicateSection("a".into())
    );
}

#[test]
fn open_costs_one_get_when_the_toc_fits_in_the_tail() {
    let store = Arc::new(CountingObjectStore::new(MemObjectStore::new()));
    let segs = SegmentStore::new(store.clone()).unwrap();
    let size = segs.write(&key(), &sample()).unwrap().size;

    let before = store.stats();
    let r = segs.open(&key(), size).unwrap();
    let opened = store.stats();
    assert_eq!(opened.gets - before.gets, 1);

    // Reading a section fetches exactly that section's bytes.
    r.read("attrs").unwrap();
    let after = store.stats();
    assert_eq!(after.gets - opened.gets, 1);
    assert_eq!(
        after.bytes_read - opened.bytes_read,
        b"color=red".len() as u64
    );
}

#[test]
fn large_toc_needs_a_second_get() {
    let mut b = SegmentBuilder::new();
    for i in 0..200 {
        b.add(
            &format!("section-{i:04}-{}", "x".repeat(40)),
            vec![i as u8; 8],
        )
        .unwrap();
    }
    let store = Arc::new(CountingObjectStore::new(MemObjectStore::new()));
    let segs = SegmentStore::new(store.clone()).unwrap();
    let size = segs.write(&key(), &b).unwrap().size;
    assert!(size > TAIL_HINT);

    let before = store.stats();
    let r = segs.open(&key(), size).unwrap();
    assert_eq!(store.stats().gets - before.gets, 2);
    assert_eq!(r.sections().len(), 200);
    assert_eq!(
        r.read(&r.sections()[150].name.clone()).unwrap(),
        vec![150u8; 8]
    );
}

#[test]
fn corrupted_section_fails_its_checksum() {
    let mem = Arc::new(MemObjectStore::new());
    let mut bytes = sample().encode();
    let size = bytes.len() as u64;
    // "attrs" is the first section, at offset 0.
    bytes[0] ^= 0xFF;
    put_raw(&mem, bytes);

    let segs = SegmentStore::new(mem).unwrap();
    let r = segs.open(&key(), size).unwrap();
    assert_eq!(
        r.read("attrs").unwrap_err(),
        SegmentError::ChecksumMismatch {
            section: "attrs".into()
        }
    );
    // Other sections are still readable.
    assert_eq!(r.read("vectors").unwrap(), vec![7u8; 1000]);
}

#[test]
fn corrupted_toc_is_detected() {
    let mem = Arc::new(MemObjectStore::new());
    let mut bytes = sample().encode();
    let size = bytes.len();
    // The byte just before the footer belongs to the TOC.
    bytes[size - FOOTER_LEN - 1] ^= 0xFF;
    put_raw(&mem, bytes);
    let segs = SegmentStore::new(mem).unwrap();
    assert_eq!(
        segs.open(&key(), size as u64).err(),
        Some(SegmentError::Corrupt("toc checksum mismatch"))
    );
}

#[test]
fn bad_magic_and_unknown_version_are_rejected() {
    let mem = Arc::new(MemObjectStore::new());
    let segs = SegmentStore::new(mem.clone()).unwrap();
    let good = sample().encode();
    let size = good.len();

    let mut bad_magic = good.clone();
    bad_magic[size - 1] = b'X';
    put_raw(&mem, bad_magic);
    assert_eq!(
        segs.open(&key(), size as u64).err(),
        Some(SegmentError::Corrupt("bad magic"))
    );

    let mut future = good;
    future[size - 6..size - 4].copy_from_slice(&(FORMAT_VERSION + 1).to_le_bytes());
    put_raw(&mem, future);
    assert_eq!(
        segs.open(&key(), size as u64).err(),
        Some(SegmentError::UnsupportedVersion(FORMAT_VERSION + 1))
    );
}

#[test]
fn wrong_recorded_size_is_an_error() {
    let mem = Arc::new(MemObjectStore::new());
    let segs = SegmentStore::new(mem).unwrap();
    let size = segs.write(&key(), &sample()).unwrap().size;
    assert!(segs.open(&key(), size - 1).is_err());
    assert!(segs.open(&key(), size + 1).is_err());
    assert!(segs.open(&key(), 3).is_err());
}
