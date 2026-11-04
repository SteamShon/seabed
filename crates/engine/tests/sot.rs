use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use parquet::file::reader::{FileReader, SerializedFileReader};
use proptest::prelude::*;
use seabed_adapter_objectstore_mem::{
    CountingObjectStore, FaultConfig, FaultInjecting, MemObjectStore,
};
use seabed_domain::{NamespaceId, NamespaceKeys, TenantId};
use seabed_engine::manifest_content::Sha256Hex;
use seabed_engine::manifest_content::{AttrKind, AttrSchema};
use seabed_engine::sot::{
    self as sot, schema_of, valid_attr_name, AttrValue, DocRow, SotError, SotRef, SotStore,
    DOCS_FILE,
};
use seabed_engine::RetryPolicy;
use seabed_ports::{ObjectStore, PutCondition};

const DIMS: u32 = 3;

/// Namespace schema covering every attribute these tests use.
fn test_schema() -> AttrSchema {
    [
        ("active", AttrKind::Bool),
        ("color", AttrKind::Str),
        ("n", AttrKind::I64),
        ("price", AttrKind::I64),
        ("score", AttrKind::F64),
    ]
    .into_iter()
    .map(|(n, k)| (n.to_string(), k))
    .collect()
}

/// Encodes with exactly the schema the rows use (inference only for tests that don't care).
fn encode(rows: &[DocRow], dims: u32) -> Result<Vec<u8>, SotError> {
    sot::encode(rows, &schema_of(rows)?, dims)
}

/// Decodes against the full test schema.
fn decode(bytes: &[u8], dims: u32) -> Result<Vec<DocRow>, SotError> {
    sot::decode(bytes, &test_schema(), dims)
}

fn keys() -> NamespaceKeys {
    NamespaceKeys::new(
        TenantId::new("acme").unwrap(),
        NamespaceId::new("ads").unwrap(),
    )
}

fn sample() -> Vec<DocRow> {
    vec![
        DocRow::upsert("item-b", 7, vec![vec![0.1, 0.2, 0.3], vec![1.0, 2.0, 3.0]])
            .with_attr("color", AttrValue::Str("red".into()))
            .with_attr("price", AttrValue::I64(1200))
            .with_attr("score", AttrValue::F64(0.75))
            .with_attr("active", AttrValue::Bool(true)),
        DocRow::upsert("item-a", 5, vec![vec![9.0, 8.0, 7.0]])
            .with_attr("color", AttrValue::Str("blue".into())),
        DocRow::tombstone("item-c", 9),
    ]
}

fn sorted(mut rows: Vec<DocRow>) -> Vec<DocRow> {
    rows.sort_by(|a, b| a.id.cmp(&b.id));
    rows
}

#[test]
fn roundtrip_keeps_every_field_and_sorts_by_id() {
    let bytes = encode(&sample(), DIMS).unwrap();
    let back = decode(&bytes, DIMS).unwrap();
    assert_eq!(back, sorted(sample()));
    // Absent attributes stay absent (null), not defaulted.
    assert!(!back[0].attrs.contains_key("price"));
    assert!(back[2].deleted && back[2].vectors.is_empty());
}

#[test]
fn encoding_is_deterministic_regardless_of_input_order() {
    let a = encode(&sample(), DIMS).unwrap();
    let mut reversed = sample();
    reversed.reverse();
    assert_eq!(a, encode(&reversed, DIMS).unwrap());
}

#[test]
fn empty_file_roundtrips() {
    let bytes = encode(&[], DIMS).unwrap();
    assert!(decode(&bytes, DIMS).unwrap().is_empty());
}

/// The point of using Parquet: a generic Parquet reader with no seabed code sees plain
/// columns, which is what DuckDB or Spark would see.
#[test]
fn file_is_plain_parquet_with_named_columns() {
    let bytes = encode(&sample(), DIMS).unwrap();
    let reader = SerializedFileReader::new(Bytes::from(bytes)).unwrap();
    let meta = reader.metadata();
    assert_eq!(meta.file_metadata().num_rows(), 3);
    assert_eq!(meta.file_metadata().created_by(), Some("seabed"));
    let schema = meta.file_metadata().schema_descr();
    let top: Vec<_> = schema
        .root_schema()
        .get_fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect();
    assert_eq!(
        top,
        ["id", "lsn", "deleted", "vectors", "active", "color", "price", "score"]
    );
}

type IsExpected = fn(&SotError) -> bool;

#[test]
fn validation_rejects_bad_rows() {
    let cases: Vec<(Vec<DocRow>, IsExpected)> = vec![
        (
            vec![
                DocRow::upsert("a", 1, vec![]),
                DocRow::upsert("a", 2, vec![]),
            ],
            |e| matches!(e, SotError::DuplicateDocId(id) if id == "a"),
        ),
        (vec![DocRow::upsert("", 1, vec![])], |e| {
            matches!(e, SotError::InvalidDocId(_))
        }),
        (vec![DocRow::upsert(&"x".repeat(257), 1, vec![])], |e| {
            matches!(e, SotError::InvalidDocId(_))
        }),
        (vec![DocRow::upsert("a\nb", 1, vec![])], |e| {
            matches!(e, SotError::InvalidDocId(_))
        }),
        (vec![DocRow::upsert("a", 1, vec![vec![1.0, 2.0]])], |e| {
            matches!(
                e,
                SotError::DimensionMismatch {
                    expected: 3,
                    found: 2,
                    ..
                }
            )
        }),
        (
            vec![DocRow::upsert("a", 1, vec![vec![1.0, f32::NAN, 2.0]])],
            |e| matches!(e, SotError::NonFinite { .. }),
        ),
        (
            vec![DocRow::upsert("a", 1, vec![]).with_attr("s", AttrValue::F64(f64::INFINITY))],
            |e| matches!(e, SotError::NonFinite { .. }),
        ),
        (
            vec![DocRow::tombstone("a", 1).with_attr("color", AttrValue::Str("x".into()))],
            |e| matches!(e, SotError::TombstoneWithData { .. }),
        ),
        (
            vec![
                DocRow::upsert("a", 1, vec![]).with_attr("price", AttrValue::I64(1)),
                DocRow::upsert("b", 1, vec![]).with_attr("price", AttrValue::F64(1.5)),
            ],
            |e| matches!(e, SotError::AttrTypeConflict { name } if name == "price"),
        ),
        (
            vec![DocRow::upsert("a", 1, vec![]).with_attr("lsn", AttrValue::I64(1))],
            |e| matches!(e, SotError::InvalidAttrName(n) if n == "lsn"),
        ),
    ];
    for (i, (rows, want)) in cases.into_iter().enumerate() {
        let err = encode(&rows, DIMS).unwrap_err();
        assert!(want(&err), "case {i}: unexpected {err:?}");
    }
}

#[test]
fn attr_names_are_restricted() {
    for ok in ["color", "a", "price_2", &"a".repeat(64)] {
        assert!(valid_attr_name(ok), "{ok}");
    }
    for bad in [
        "",
        "Color",
        "2x",
        "a-b",
        "a.b",
        "id",
        "vectors",
        &"a".repeat(65),
    ] {
        assert!(!valid_attr_name(bad), "{bad}");
    }
}

#[test]
fn decode_rejects_a_file_with_the_wrong_dimensions_or_garbage() {
    let bytes = encode(&sample(), DIMS).unwrap();
    assert!(matches!(decode(&bytes, 4), Err(SotError::Format(_))));
    assert!(matches!(
        decode(b"not parquet", DIMS),
        Err(SotError::Format(_))
    ));
}

#[test]
fn store_write_then_read_checks_size_and_hash() {
    let mem = Arc::new(MemObjectStore::new());
    let store = SotStore::new(mem.clone());
    let k = keys();
    let r = store
        .write(&k, "seg-0001", &sample(), &test_schema(), DIMS)
        .unwrap();
    assert_eq!(
        store
            .read(&k, "seg-0001", &r, &test_schema(), DIMS)
            .unwrap(),
        sorted(sample())
    );

    // The object lives at segments/<id>/docs.parquet.
    assert!(mem.get(&k.segment_key("seg-0001", DOCS_FILE)).is_ok());

    // Never overwritten.
    assert_eq!(
        store
            .write(&k, "seg-0001", &sample(), &test_schema(), DIMS)
            .unwrap_err(),
        SotError::AlreadyExists
    );

    // A record that does not match the object is refused before decoding.
    let wrong_hash = SotRef {
        size: r.size,
        sha256: Sha256Hex([0; 32]),
    };
    assert_eq!(
        store
            .read(&k, "seg-0001", &wrong_hash, &test_schema(), DIMS)
            .unwrap_err(),
        SotError::RefMismatch
    );
    let wrong_size = SotRef {
        size: r.size + 1,
        ..r.clone()
    };
    assert_eq!(
        store
            .read(&k, "seg-0001", &wrong_size, &test_schema(), DIMS)
            .unwrap_err(),
        SotError::RefMismatch
    );

    // A replaced object (valid Parquet, other content) is detected the same way.
    mem.put(
        &k.segment_key("seg-0001", DOCS_FILE),
        encode(&[DocRow::upsert("z", 1, vec![])], DIMS).unwrap(),
        PutCondition::Unconditional,
    )
    .unwrap();
    assert_eq!(
        store
            .read(&k, "seg-0001", &r, &test_schema(), DIMS)
            .unwrap_err(),
        SotError::RefMismatch
    );
}

#[test]
fn store_rejects_invalid_segment_ids_without_writing() {
    let mem = Arc::new(MemObjectStore::new());
    let store = SotStore::new(mem.clone());
    assert_eq!(
        store
            .write(&keys(), "../x", &sample(), &test_schema(), DIMS)
            .unwrap_err(),
        SotError::InvalidSegmentId("../x".into())
    );
    assert!(mem.list("").unwrap().is_empty());
}

#[test]
fn reading_costs_one_get() {
    let mem = Arc::new(CountingObjectStore::new(MemObjectStore::new()));
    let store = SotStore::new(mem.clone());
    let r = store
        .write(&keys(), "seg-0001", &sample(), &test_schema(), DIMS)
        .unwrap();
    let before = mem.stats();
    store
        .read(&keys(), "seg-0001", &r, &test_schema(), DIMS)
        .unwrap();
    let after = mem.stats();
    assert_eq!(after.gets - before.gets, 1);
    assert_eq!(after.lists, before.lists);
}

fn arb_value() -> impl Strategy<Value = AttrValue> {
    prop_oneof![
        "[a-z가-힣 ]{0,12}".prop_map(AttrValue::Str),
        any::<i64>().prop_map(AttrValue::I64),
        (-1e12f64..1e12).prop_map(AttrValue::F64),
        any::<bool>().prop_map(AttrValue::Bool),
    ]
}

/// Rows with consistent attribute types: each attribute name gets one type per test case.
fn arb_rows() -> impl Strategy<Value = Vec<DocRow>> {
    let names = ["a", "b", "c"];
    (
        proptest::collection::btree_map("[a-z0-9:_-]{1,12}", any::<bool>(), 0..20),
        proptest::collection::vec(arb_value(), 3),
    )
        .prop_flat_map(move |(ids, protos)| {
            let n = ids.len();
            (
                Just(ids),
                Just(protos),
                proptest::collection::vec(
                    (
                        0..=i64::MAX as u64,
                        proptest::collection::vec(
                            proptest::collection::vec(-1e6f32..1e6, DIMS as usize),
                            0..3,
                        ),
                        proptest::collection::vec(any::<bool>(), 3),
                    ),
                    n,
                ),
            )
        })
        .prop_map(move |(ids, protos, details)| {
            ids.into_iter()
                .zip(details)
                .map(|((id, deleted), (lsn, vectors, present))| {
                    if deleted {
                        return DocRow::tombstone(&id, lsn);
                    }
                    let mut attrs = BTreeMap::new();
                    for ((name, proto), on) in names.iter().zip(&protos).zip(present) {
                        if on {
                            attrs.insert(name.to_string(), proto.clone());
                        }
                    }
                    let mut r = DocRow::upsert(&id, lsn, vectors);
                    r.attrs = attrs;
                    r
                })
                .collect()
        })
}

proptest! {
    #[test]
    fn rows_roundtrip_and_reencode_identically(rows in arb_rows()) {
        let schema = schema_of(&rows).unwrap();
        let bytes = sot::encode(&rows, &schema, DIMS).unwrap();
        let back = sot::decode(&bytes, &schema, DIMS).unwrap();
        prop_assert_eq!(&back, &sorted(rows));
        prop_assert_eq!(sot::encode(&back, &schema, DIMS).unwrap(), bytes);
    }
}

// ---- Files written by other tools: decode must apply the same rules as encode ----

use arrow_array::builder::{FixedSizeListBuilder, Float32Builder, ListBuilder};
use arrow_array::{ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::file::metadata::KeyValue;

/// A vectors cell: `None` = null row; inner `None` = null vector; element `None` = null float.
type VecCell = Option<Vec<Option<Vec<Option<f32>>>>>;

fn vectors_array(rows: &[VecCell]) -> ArrayRef {
    let mut vb = ListBuilder::new(FixedSizeListBuilder::new(Float32Builder::new(), 3));
    for row in rows {
        match row {
            None => vb.append(false),
            Some(vs) => {
                for v in vs {
                    match v {
                        None => {
                            for _ in 0..3 {
                                vb.values().values().append_null();
                            }
                            vb.values().append(false);
                        }
                        Some(els) => {
                            for e in els {
                                vb.values().values().append_option(*e);
                            }
                            vb.values().append(true);
                        }
                    }
                }
                vb.append(true);
            }
        }
    }
    Arc::new(vb.finish())
}

/// Builds a SoT-shaped Parquet file with plain arrow, the way another tool might.
struct Foreign {
    ids: Vec<Option<&'static str>>,
    lsns: Vec<Option<i64>>,
    deleted: Vec<Option<bool>>,
    vectors: Vec<VecCell>,
    extra: Vec<(Field, ArrayRef)>,
    metadata: Option<Vec<(&'static str, &'static str)>>,
    row_group_size: Option<usize>,
}

impl Foreign {
    fn one(vector: [f32; 3]) -> Self {
        Foreign {
            ids: vec![Some("x")],
            lsns: vec![Some(1)],
            deleted: vec![Some(false)],
            vectors: vec![Some(vec![Some(vector.iter().map(|f| Some(*f)).collect())])],
            extra: vec![],
            row_group_size: None,
            metadata: Some(vec![
                ("seabed.sot.format_version", "1"),
                ("seabed.sot.dims", "3"),
            ]),
        }
    }

    fn with_row_group_size(mut self, n: usize) -> Self {
        self.row_group_size = Some(n);
        self
    }

    fn bytes(self) -> Vec<u8> {
        let vectors = vectors_array(&self.vectors);
        let mut fields = vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("lsn", DataType::Int64, true),
            Field::new("deleted", DataType::Boolean, true),
            Field::new("vectors", vectors.data_type().clone(), true),
        ];
        let mut cols: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(self.ids)),
            Arc::new(Int64Array::from(self.lsns)),
            Arc::new(BooleanArray::from(self.deleted)),
            vectors,
        ];
        for (f, c) in self.extra {
            fields.push(f);
            cols.push(c);
        }
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema.clone(), cols).unwrap();
        let mut props = parquet::file::properties::WriterProperties::builder();
        if let Some(kv) = self.metadata {
            props = props.set_key_value_metadata(Some(
                kv.into_iter()
                    .map(|(k, v)| KeyValue::new(k.to_string(), v.to_string()))
                    .collect(),
            ));
        }
        if let Some(n) = self.row_group_size {
            props = props.set_max_row_group_row_count(Some(n));
        }
        let mut w =
            parquet::arrow::ArrowWriter::try_new(Vec::new(), schema, Some(props.build())).unwrap();
        w.write(&batch).unwrap();
        w.into_inner().unwrap()
    }
}

fn is_format(r: Result<Vec<DocRow>, SotError>) -> bool {
    matches!(r, Err(SotError::Format(_)))
}

#[test]
fn decode_applies_the_same_rules_to_files_written_by_other_tools() {
    // Sanity: a well-formed foreign file is accepted.
    assert_eq!(
        decode(&Foreign::one([1.0, 2.0, 3.0]).bytes(), DIMS)
            .unwrap()
            .len(),
        1
    );
    assert!(matches!(
        decode(&Foreign::one([1.0, f32::NAN, 3.0]).bytes(), DIMS),
        Err(SotError::NonFinite { .. })
    ));
    let mut tomb = Foreign::one([1.0, 2.0, 3.0]);
    tomb.deleted = vec![Some(true)];
    assert!(matches!(
        decode(&tomb.bytes(), DIMS),
        Err(SotError::TombstoneWithData { .. })
    ));
}

#[test]
fn decode_rejects_nulls_inside_vectors_at_every_level() {
    let cells: [(&str, VecCell); 3] = [
        (
            "element",
            Some(vec![Some(vec![Some(1.0), None, Some(3.0)])]),
        ),
        ("vector", Some(vec![None])),
        ("row", None),
    ];
    for (level, cell) in cells {
        let mut f = Foreign::one([0.0; 3]);
        f.vectors = vec![cell];
        assert!(
            is_format(decode(&f.bytes(), DIMS)),
            "null {level} must be rejected"
        );
    }
}

#[test]
fn decode_rejects_nulls_in_fixed_columns() {
    let mut id = Foreign::one([1.0, 2.0, 3.0]);
    id.ids = vec![None];
    let mut lsn = Foreign::one([1.0, 2.0, 3.0]);
    lsn.lsns = vec![None];
    let mut deleted = Foreign::one([1.0, 2.0, 3.0]);
    deleted.deleted = vec![None];
    for (col, f) in [("id", id), ("lsn", lsn), ("deleted", deleted)] {
        // Otherwise a null lsn would read as 0 and a null deleted as false.
        assert!(
            is_format(decode(&f.bytes(), DIMS)),
            "null {col} must be rejected"
        );
    }
}

#[test]
fn decode_rejects_unsorted_or_duplicate_ids() {
    for ids in [[Some("b"), Some("a")], [Some("a"), Some("a")]] {
        let mut f = Foreign::one([1.0, 2.0, 3.0]);
        f.ids = ids.to_vec();
        f.lsns = vec![Some(1), Some(2)];
        f.deleted = vec![Some(true), Some(true)];
        f.vectors = vec![Some(vec![]), Some(vec![])];
        assert!(is_format(decode(&f.bytes(), DIMS)), "{ids:?}");
    }
}

#[test]
fn decode_rejects_a_bad_attribute_column_name() {
    let mut f = Foreign::one([1.0, 2.0, 3.0]);
    f.extra = vec![(
        Field::new("Color", DataType::Utf8, true),
        Arc::new(StringArray::from(vec![Some("red")])),
    )];
    assert_eq!(
        decode(&f.bytes(), DIMS).unwrap_err(),
        SotError::InvalidAttrName("Color".into())
    );

    // All values null: the row-level validation never sees the name, so only the
    // column-level check stops the column from silently disappearing.
    let mut all_null = Foreign::one([1.0, 2.0, 3.0]);
    all_null.extra = vec![(
        Field::new("Color", DataType::Utf8, true),
        Arc::new(StringArray::from(vec![None::<&str>])),
    )];
    assert_eq!(
        decode(&all_null.bytes(), DIMS).unwrap_err(),
        SotError::InvalidAttrName("Color".into())
    );
}

#[test]
fn decode_requires_matching_format_version_and_dims_metadata() {
    let cases = [
        None,
        Some(vec![("seabed.sot.dims", "3")]),
        Some(vec![("seabed.sot.format_version", "1")]),
        Some(vec![
            ("seabed.sot.format_version", "2"),
            ("seabed.sot.dims", "3"),
        ]),
        Some(vec![
            ("seabed.sot.format_version", "1"),
            ("seabed.sot.dims", "4"),
        ]),
    ];
    for kv in cases {
        let mut f = Foreign::one([1.0, 2.0, 3.0]);
        f.metadata = kv.clone();
        assert!(is_format(decode(&f.bytes(), DIMS)), "{kv:?}");
    }
}

#[test]
fn decode_rejects_a_negative_lsn() {
    let mut f = Foreign::one([1.0, 2.0, 3.0]);
    f.lsns = vec![Some(-1)];
    assert!(is_format(decode(&f.bytes(), DIMS)));
}

#[test]
fn file_carries_format_version_dims_and_writer_metadata() {
    let bytes = encode(&sample(), DIMS).unwrap();
    let reader = SerializedFileReader::new(Bytes::from(bytes)).unwrap();
    let kv = reader
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .unwrap();
    let get = |k: &str| kv.iter().find(|e| e.key == k).and_then(|e| e.value.clone());
    assert_eq!(get("seabed.sot.format_version").as_deref(), Some("1"));
    assert_eq!(get("seabed.sot.dims").as_deref(), Some("3"));
    assert!(get("seabed.sot.parquet_writer")
        .unwrap()
        .starts_with("parquet-rs"));
}

#[test]
fn lsn_is_stored_signed_and_must_fit_i64() {
    let max = DocRow::upsert("a", i64::MAX as u64, vec![]);
    let back = decode(&encode(std::slice::from_ref(&max), DIMS).unwrap(), DIMS).unwrap();
    assert_eq!(back, vec![max]);
    assert_eq!(
        encode(&[DocRow::upsert("a", i64::MAX as u64 + 1, vec![])], DIMS).unwrap_err(),
        SotError::LsnOutOfRange { id: "a".into() }
    );
}

#[test]
fn id_of_exactly_max_len_bytes_is_accepted() {
    let id = format!("{}a", "가".repeat(85)); // 85 * 3 + 1 = 256 bytes
    assert_eq!(id.len(), 256);
    let rows = vec![DocRow::upsert(&id, 1, vec![])];
    assert_eq!(decode(&encode(&rows, DIMS).unwrap(), DIMS).unwrap(), rows);
}

#[test]
fn many_rows_spanning_several_record_batches_roundtrip() {
    let rows: Vec<DocRow> = (0..3000u64)
        .map(|i| {
            DocRow::upsert(&format!("id-{i:05}"), i, vec![vec![i as f32, 0.5, -1.0]])
                .with_attr("n", AttrValue::I64(i as i64))
        })
        .collect();
    let back = decode(&encode(&rows, DIMS).unwrap(), DIMS).unwrap();
    assert_eq!(back, rows);
}

#[test]
fn read_retries_transient_get_failures() {
    let mem = Arc::new(MemObjectStore::new());
    let r = SotStore::new(mem.clone())
        .write(&keys(), "seg-0001", &sample(), &test_schema(), DIMS)
        .unwrap();
    let cfg = FaultConfig {
        get_unavailable_ppm: 500_000,
        ..FaultConfig::none()
    };
    let flaky = |policy| {
        SotStore::new(Arc::new(FaultInjecting::new(mem.clone(), cfg))).with_retry_policy(policy)
    };
    // Without retries, half the reads fail.
    let no_retry = flaky(RetryPolicy::immediate(1));
    assert!((0..20).any(|_| no_retry
        .read(&keys(), "seg-0001", &r, &test_schema(), DIMS)
        .is_err()));
    // With retries, all succeed.
    let retrying = flaky(RetryPolicy::immediate(64));
    for _ in 0..20 {
        retrying
            .read(&keys(), "seg-0001", &r, &test_schema(), DIMS)
            .unwrap();
    }
}

// ---- Namespace attribute schema (manifest-driven) ----

fn kinds(pairs: &[(&str, AttrKind)]) -> AttrSchema {
    pairs.iter().map(|(n, k)| (n.to_string(), *k)).collect()
}

#[test]
fn encode_writes_every_schema_column_even_if_no_row_uses_it() {
    let schema = kinds(&[("color", AttrKind::Str), ("price", AttrKind::I64)]);
    let rows =
        vec![DocRow::upsert("a", 1, vec![]).with_attr("color", AttrValue::Str("red".into()))];
    let bytes = sot::encode(&rows, &schema, DIMS).unwrap();
    let reader = SerializedFileReader::new(Bytes::from(bytes.clone())).unwrap();
    let names: Vec<_> = reader
        .metadata()
        .file_metadata()
        .schema_descr()
        .root_schema()
        .get_fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect();
    assert_eq!(names, ["id", "lsn", "deleted", "vectors", "color", "price"]);
    assert_eq!(sot::decode(&bytes, &schema, DIMS).unwrap(), rows);
}

#[test]
fn encode_rejects_attributes_outside_or_against_the_schema() {
    let schema = kinds(&[("price", AttrKind::I64)]);
    let unknown = [DocRow::upsert("a", 1, vec![]).with_attr("color", AttrValue::Str("x".into()))];
    assert_eq!(
        sot::encode(&unknown, &schema, DIMS).unwrap_err(),
        SotError::UnknownAttr("color".into())
    );
    let wrong = [DocRow::upsert("a", 1, vec![]).with_attr("price", AttrValue::F64(1.5))];
    assert_eq!(
        sot::encode(&wrong, &schema, DIMS).unwrap_err(),
        SotError::AttrTypeConflict {
            name: "price".into()
        }
    );
}

#[test]
fn a_file_from_an_older_smaller_schema_reads_with_the_current_one() {
    let old = kinds(&[("price", AttrKind::I64)]);
    let rows = vec![DocRow::upsert("a", 1, vec![]).with_attr("price", AttrValue::I64(3))];
    let bytes = sot::encode(&rows, &old, DIMS).unwrap();
    // The schema grew after the file was written; the new column simply reads as absent.
    let grown = kinds(&[("color", AttrKind::Str), ("price", AttrKind::I64)]);
    assert_eq!(sot::decode(&bytes, &grown, DIMS).unwrap(), rows);
}

#[test]
fn decode_refuses_columns_the_schema_lacks_or_types_differently() {
    let rows = vec![DocRow::upsert("a", 1, vec![]).with_attr("price", AttrValue::I64(3))];
    let bytes = sot::encode(&rows, &kinds(&[("price", AttrKind::I64)]), DIMS).unwrap();
    assert_eq!(
        sot::decode(&bytes, &AttrSchema::new(), DIMS).unwrap_err(),
        SotError::SchemaMismatch {
            name: "price".into()
        }
    );
    assert_eq!(
        sot::decode(&bytes, &kinds(&[("price", AttrKind::F64)]), DIMS).unwrap_err(),
        SotError::SchemaMismatch {
            name: "price".into()
        }
    );
}

/// Deferred from PR #11 review: validation must hold across row groups, not only the first.
#[test]
fn decode_validates_across_row_groups() {
    fn two_row_groups(f: Foreign) -> Vec<u8> {
        let bytes = f.with_row_group_size(1).bytes();
        let reader = SerializedFileReader::new(Bytes::from(bytes.clone())).unwrap();
        assert_eq!(reader.metadata().num_row_groups(), 2);
        bytes
    }
    let base = || {
        let mut f = Foreign::one([1.0, 2.0, 3.0]);
        f.ids = vec![Some("a"), Some("b")];
        f.lsns = vec![Some(1), Some(2)];
        f.deleted = vec![Some(true), Some(true)];
        f.vectors = vec![Some(vec![]), Some(vec![])];
        f
    };
    // Sanity: valid across two row groups.
    assert_eq!(decode(&two_row_groups(base()), DIMS).unwrap().len(), 2);
    // Null lsn only in the second row group.
    let mut null_late = base();
    null_late.lsns = vec![Some(1), None];
    assert!(is_format(decode(&two_row_groups(null_late), DIMS)));
    // Order broken only across the boundary.
    let mut unsorted = base();
    unsorted.ids = vec![Some("b"), Some("a")];
    assert!(is_format(decode(&two_row_groups(unsorted), DIMS)));
}
