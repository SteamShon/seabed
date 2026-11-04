//! Source-of-truth documents stored as plain Parquet (PLAN 5.1, 6.7).
//!
//! Each segment's documents live in their own Parquet object, `segments/<id>/docs.parquet`,
//! next to (not inside) the `.sbsg` index container. That keeps the SoT readable by any engine
//! that reads Parquet (DuckDB, Spark, DataFusion) without knowing seabed's formats.
//!
//! Columns:
//!
//! | column | type | meaning |
//! | --- | --- | --- |
//! | `id` | utf8, not null | document id; rows are sorted by it and unique per file |
//! | `lsn` | int64, not null | LSN of the write that produced this row (latest wins); stored signed
//! |  |  | because Spark/Hive-family readers handle unsigned 64-bit poorly, so LSNs above `i64::MAX` are refused |
//! | `deleted` | bool, not null | tombstone: hides older versions of `id` in older segments |
//! | `vectors` | list<fixed_size_list<f32, dims>>, not null | the document's vectors (empty when deleted) |
//! | one per attribute | utf8 / int64 / float64 / bool, nullable | absent attribute = null |
//!
//! Attribute columns come after the fixed ones, ordered by name. With the same Parquet writer
//! version, the same rows always encode to the same bytes: rows are sorted, columns are ordered,
//! and `created_by` is fixed. Across writer versions bytes may differ; that is fine because the
//! manifest records the hash of the bytes actually written (`SotRef`), not of the rows.
//!
//! File key-value metadata carries `seabed.sot.format_version`, `seabed.sot.dims` and
//! `seabed.sot.parquet_writer` (the real writer version, since `created_by` is fixed). Decoding
//! requires the first two and checks them. Any change to what `encode` writes for the same rows
//! and schema must bump `SOT_FORMAT_VERSION`: flush derives segment ids from it (see
//! `namespace`), and an unchanged id with different bytes would make a re-flush fail with
//! `AlreadyExists`.
//!
//! Attribute columns follow the namespace-wide schema stored in the manifest (`AttrSchema`):
//! `encode` writes every schema column (nullable), so files written at the same schema version
//! have identical columns, and a column never changes type across files. `decode` accepts a file
//! written with an older, smaller schema (missing columns read as null) but refuses a column the
//! schema does not have or has with another type. Engines reading several files directly should
//! still take the file list from the manifest and union columns by name.
//!
//! An upsert may have zero vectors (a document with attributes only). `MAX_ID_LEN` counts bytes.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow_array::builder::{FixedSizeListBuilder, Float32Builder, ListBuilder};
use arrow_array::cast::AsArray;
use arrow_array::types::{Float32Type, Float64Type, Int64Type};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use seabed_adapter_objectstore_retry::{RetryPolicy, Retrying};
use seabed_domain::{Key, Lsn, NamespaceKeys};
use seabed_ports::{ObjectStore, PutCondition, StoreError};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::manifest_content::{valid_segment_id, AttrKind, AttrSchema, Sha256Hex};

/// File name of a segment's documents inside `segments/<id>/`.
pub const DOCS_FILE: &str = "docs.parquet";
pub const MAX_ID_LEN: usize = 256;
pub const MAX_ATTR_NAME_LEN: usize = 64;
pub const SOT_FORMAT_VERSION: u32 = 1;
/// Attempts for one create-only SoT write before giving up on an unclear outcome.
const MAX_PUT_ATTEMPTS: usize = 32;
const KEY_VERSION: &str = "seabed.sot.format_version";
const KEY_DIMS: &str = "seabed.sot.dims";
const KEY_WRITER: &str = "seabed.sot.parquet_writer";
const RESERVED: [&str; 4] = ["id", "lsn", "deleted", "vectors"];

#[derive(Debug, Clone, PartialEq)]
pub enum SotError {
    Store(StoreError),
    AlreadyExists,
    InvalidSegmentId(String),
    InvalidDocId(String),
    DuplicateDocId(String),
    InvalidAttrName(String),
    /// The same attribute has values of two different types in one file.
    AttrTypeConflict {
        name: String,
    },
    /// A row has an attribute the namespace schema does not list. Extend the schema first.
    UnknownAttr(String),
    /// A file's column does not match the namespace schema (absent from it, or another type).
    SchemaMismatch {
        name: String,
    },
    /// A vector's length differs from the namespace's dimensions.
    DimensionMismatch {
        id: String,
        expected: u32,
        found: usize,
    },
    /// NaN or infinity in a vector or a float attribute.
    NonFinite {
        id: String,
    },
    /// LSN does not fit the signed 64-bit `lsn` column.
    LsnOutOfRange {
        id: String,
    },
    /// A tombstone carries vectors or attributes.
    TombstoneWithData {
        id: String,
    },
    /// The object is not the one the manifest recorded (size or SHA-256 differs).
    RefMismatch,
    /// The bytes are not a valid seabed SoT Parquet file.
    Format(String),
}

impl From<StoreError> for SotError {
    fn from(e: StoreError) -> Self {
        SotError::Store(e)
    }
}

fn format_err(e: impl std::fmt::Display) -> SotError {
    SotError::Format(e.to_string())
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttrValue {
    Str(String),
    I64(i64),
    F64(f64),
    Bool(bool),
}

/// One row of the SoT: the latest state of a document as of `lsn`.
#[derive(Debug, Clone, PartialEq)]
pub struct DocRow {
    pub id: String,
    pub lsn: Lsn,
    pub deleted: bool,
    pub vectors: Vec<Vec<f32>>,
    pub attrs: BTreeMap<String, AttrValue>,
}

impl DocRow {
    pub fn upsert(id: &str, lsn: u64, vectors: Vec<Vec<f32>>) -> Self {
        Self {
            id: id.to_string(),
            lsn: Lsn::new(lsn),
            deleted: false,
            vectors,
            attrs: BTreeMap::new(),
        }
    }

    pub fn tombstone(id: &str, lsn: u64) -> Self {
        Self {
            id: id.to_string(),
            lsn: Lsn::new(lsn),
            deleted: true,
            vectors: Vec::new(),
            attrs: BTreeMap::new(),
        }
    }

    pub fn with_attr(mut self, name: &str, value: AttrValue) -> Self {
        self.attrs.insert(name.to_string(), value);
        self
    }
}

pub fn valid_doc_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= MAX_ID_LEN && !id.chars().any(char::is_control)
}

/// Lowercase ASCII letters, digits and `_`, starting with a letter, not a fixed column name.
pub fn valid_attr_name(name: &str) -> bool {
    let b = name.as_bytes();
    !b.is_empty()
        && b.len() <= MAX_ATTR_NAME_LEN
        && b[0].is_ascii_lowercase()
        && b.iter()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'_')
        && !RESERVED.contains(&name)
}

fn kind_of(v: &AttrValue) -> AttrKind {
    match v {
        AttrValue::Str(_) => AttrKind::Str,
        AttrValue::I64(_) => AttrKind::I64,
        AttrValue::F64(_) => AttrKind::F64,
        AttrValue::Bool(_) => AttrKind::Bool,
    }
}

fn data_type(k: AttrKind) -> DataType {
    match k {
        AttrKind::Str => DataType::Utf8,
        AttrKind::I64 => DataType::Int64,
        AttrKind::F64 => DataType::Float64,
        AttrKind::Bool => DataType::Boolean,
    }
}

/// The attribute types `rows` use, for extending the namespace schema before writing.
/// Fails if one attribute has values of two types.
pub fn schema_of(rows: &[DocRow]) -> Result<AttrSchema, SotError> {
    let mut schema = AttrSchema::new();
    for r in rows {
        for (name, value) in &r.attrs {
            if !valid_attr_name(name) {
                return Err(SotError::InvalidAttrName(name.clone()));
            }
            let k = kind_of(value);
            if *schema.entry(name.clone()).or_insert(k) != k {
                return Err(SotError::AttrTypeConflict { name: name.clone() });
            }
        }
    }
    Ok(schema)
}

/// Checks rows against the SoT rules and `schema` without encoding them, so writers can refuse
/// bad input before it reaches the WAL.
pub fn validate_rows(rows: &[DocRow], schema: &AttrSchema, dims: u32) -> Result<(), SotError> {
    validate(rows, schema, dims)
}

/// Checks every row against the rules and the namespace schema. Pure, so the rules are
/// testable directly.
fn validate(rows: &[DocRow], schema: &AttrSchema, dims: u32) -> Result<(), SotError> {
    if let Some(bad) = schema.keys().find(|n| !valid_attr_name(n)) {
        return Err(SotError::InvalidAttrName(bad.clone()));
    }
    let mut ids = BTreeSet::new();
    for r in rows {
        if !valid_doc_id(&r.id) {
            return Err(SotError::InvalidDocId(r.id.clone()));
        }
        if !ids.insert(r.id.as_str()) {
            return Err(SotError::DuplicateDocId(r.id.clone()));
        }
        if r.lsn.get() > i64::MAX as u64 {
            return Err(SotError::LsnOutOfRange { id: r.id.clone() });
        }
        if r.deleted && (!r.vectors.is_empty() || !r.attrs.is_empty()) {
            return Err(SotError::TombstoneWithData { id: r.id.clone() });
        }
        for v in &r.vectors {
            if v.len() != dims as usize {
                return Err(SotError::DimensionMismatch {
                    id: r.id.clone(),
                    expected: dims,
                    found: v.len(),
                });
            }
            if v.iter().any(|x| !x.is_finite()) {
                return Err(SotError::NonFinite { id: r.id.clone() });
            }
        }
        for (name, value) in &r.attrs {
            if !valid_attr_name(name) {
                return Err(SotError::InvalidAttrName(name.clone()));
            }
            if matches!(value, AttrValue::F64(x) if !x.is_finite()) {
                return Err(SotError::NonFinite { id: r.id.clone() });
            }
            match schema.get(name) {
                None => return Err(SotError::UnknownAttr(name.clone())),
                Some(k) if *k != kind_of(value) => {
                    return Err(SotError::AttrTypeConflict { name: name.clone() })
                }
                Some(_) => {}
            }
        }
    }
    Ok(())
}

/// Encodes rows as a SoT Parquet file with one column per `schema` attribute. Rows may be
/// given in any order.
pub fn encode(rows: &[DocRow], schema: &AttrSchema, dims: u32) -> Result<Vec<u8>, SotError> {
    validate(rows, schema, dims)?;
    let attr_schema = schema;
    let mut sorted: Vec<&DocRow> = rows.iter().collect();
    sorted.sort_by(|a, b| a.id.cmp(&b.id));

    let ids: ArrayRef = Arc::new(StringArray::from_iter_values(
        sorted.iter().map(|r| r.id.as_str()),
    ));
    // Checked against i64::MAX in `validate`.
    let lsns: ArrayRef = Arc::new(Int64Array::from_iter_values(
        sorted.iter().map(|r| r.lsn.get() as i64),
    ));
    let deleted: ArrayRef = Arc::new(BooleanArray::from(
        sorted.iter().map(|r| r.deleted).collect::<Vec<_>>(),
    ));
    let mut vb = ListBuilder::new(FixedSizeListBuilder::new(
        Float32Builder::new(),
        dims as i32,
    ));
    for r in &sorted {
        for v in &r.vectors {
            vb.values().values().append_slice(v);
            vb.values().append(true);
        }
        vb.append(true);
    }
    let vectors: ArrayRef = Arc::new(vb.finish());

    let mut fields = vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("lsn", DataType::Int64, false),
        Field::new("deleted", DataType::Boolean, false),
        Field::new("vectors", vectors.data_type().clone(), false),
    ];
    let mut columns = vec![ids, lsns, deleted, vectors];
    for (name, t) in attr_schema {
        let col: ArrayRef = match t {
            AttrKind::Str => Arc::new(StringArray::from(
                sorted
                    .iter()
                    .map(|r| match r.attrs.get(name) {
                        Some(AttrValue::Str(s)) => Some(s.as_str()),
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
            )),
            AttrKind::I64 => Arc::new(Int64Array::from(
                sorted
                    .iter()
                    .map(|r| match r.attrs.get(name) {
                        Some(AttrValue::I64(x)) => Some(*x),
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
            )),
            AttrKind::F64 => Arc::new(Float64Array::from(
                sorted
                    .iter()
                    .map(|r| match r.attrs.get(name) {
                        Some(AttrValue::F64(x)) => Some(*x),
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
            )),
            AttrKind::Bool => Arc::new(BooleanArray::from(
                sorted
                    .iter()
                    .map(|r| match r.attrs.get(name) {
                        Some(AttrValue::Bool(x)) => Some(*x),
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
            )),
        };
        fields.push(Field::new(name, data_type(*t), true));
        columns.push(col);
    }

    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), columns).map_err(format_err)?;
    let props = WriterProperties::builder()
        .set_created_by("seabed".to_string())
        .set_key_value_metadata(Some(vec![
            KeyValue::new(KEY_VERSION.to_string(), SOT_FORMAT_VERSION.to_string()),
            KeyValue::new(KEY_DIMS.to_string(), dims.to_string()),
            KeyValue::new(
                KEY_WRITER.to_string(),
                parquet::file::properties::DEFAULT_CREATED_BY.to_string(),
            ),
        ]))
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(Vec::new(), schema, Some(props)).map_err(format_err)?;
    writer.write(&batch).map_err(format_err)?;
    writer.into_inner().map_err(format_err)
}

/// Decodes and validates a SoT Parquet file. Returns rows sorted by id.
///
/// `schema` is the namespace's current schema. The file may lack columns added to the schema
/// after it was written; those read as absent attributes.
pub fn decode(bytes: &[u8], schema: &AttrSchema, dims: u32) -> Result<Vec<DocRow>, SotError> {
    decode_bytes(bytes::Bytes::copy_from_slice(bytes), schema, dims)
}

/// Like `decode`, but takes ownership so the store's buffer is not copied again.
fn decode_bytes(
    bytes: bytes::Bytes,
    schema: &AttrSchema,
    dims: u32,
) -> Result<Vec<DocRow>, SotError> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes).map_err(format_err)?;
    check_file_metadata(
        builder.metadata().file_metadata().key_value_metadata(),
        dims,
    )?;
    let reader = builder.build().map_err(format_err)?;
    let mut rows = Vec::new();
    for batch in reader {
        let batch = batch.map_err(format_err)?;
        decode_batch(&batch, schema, dims, &mut rows)?;
    }
    for pair in rows.windows(2) {
        if pair[0].id >= pair[1].id {
            return Err(SotError::Format("rows are not sorted by unique id".into()));
        }
    }
    validate(&rows, schema, dims)?;
    Ok(rows)
}

fn check_file_metadata(kv: Option<&Vec<KeyValue>>, dims: u32) -> Result<(), SotError> {
    let get = |key: &str| {
        kv.and_then(|kv| kv.iter().find(|e| e.key == key))
            .and_then(|e| e.value.clone())
            .ok_or_else(|| SotError::Format(format!("missing file metadata {key}")))
    };
    let version = get(KEY_VERSION)?;
    if version != SOT_FORMAT_VERSION.to_string() {
        return Err(SotError::Format(format!(
            "unsupported SoT format version {version}"
        )));
    }
    let file_dims = get(KEY_DIMS)?;
    if file_dims != dims.to_string() {
        return Err(SotError::Format(format!(
            "file has {file_dims} dimensions, expected {dims}"
        )));
    }
    Ok(())
}

fn column<'a>(
    batch: &'a RecordBatch,
    name: &str,
    want: &DataType,
) -> Result<&'a ArrayRef, SotError> {
    let col = batch
        .column_by_name(name)
        .ok_or_else(|| SotError::Format(format!("missing column {name}")))?;
    if col.data_type() != want {
        return Err(SotError::Format(format!(
            "column {name} has type {}, expected {want}",
            col.data_type()
        )));
    }
    if col.null_count() > 0 {
        return Err(SotError::Format(format!("column {name} has nulls")));
    }
    Ok(col)
}

fn decode_batch(
    batch: &RecordBatch,
    schema: &AttrSchema,
    dims: u32,
    out: &mut Vec<DocRow>,
) -> Result<(), SotError> {
    let ids = column(batch, "id", &DataType::Utf8)?.as_string::<i32>();
    let lsns = column(batch, "lsn", &DataType::Int64)?.as_primitive::<Int64Type>();
    let deleted = column(batch, "deleted", &DataType::Boolean)?.as_boolean();
    let vec_col = batch
        .column_by_name("vectors")
        .ok_or_else(|| SotError::Format("missing column vectors".into()))?;
    let lists = vec_col
        .as_list_opt::<i32>()
        .ok_or_else(|| SotError::Format("column vectors is not a list".into()))?;
    let inner = lists
        .values()
        .as_fixed_size_list_opt()
        .filter(|f| f.value_length() == dims as i32)
        .ok_or_else(|| SotError::Format(format!("vectors are not fixed-size lists of {dims}")))?;
    if inner.values().as_primitive_opt::<Float32Type>().is_none() {
        return Err(SotError::Format("vector elements are not f32".into()));
    }
    // Nulls at any level would otherwise read as whatever bytes sit in the null slots.
    if lists.null_count() > 0 || inner.null_count() > 0 || inner.values().null_count() > 0 {
        return Err(SotError::Format("null inside column vectors".into()));
    }

    let mut attrs: Vec<(String, AttrKind, &ArrayRef)> = Vec::new();
    for (field, col) in batch.schema().fields().iter().zip(batch.columns()) {
        let name = field.name();
        if RESERVED.contains(&name.as_str()) {
            continue;
        }
        if !valid_attr_name(name) {
            return Err(SotError::InvalidAttrName(name.clone()));
        }
        // Checked per column so an all-null column is caught too: row-level validation never
        // sees attributes whose values are all null.
        let kind = match schema.get(name) {
            Some(k) if data_type(*k) == *col.data_type() => *k,
            _ => return Err(SotError::SchemaMismatch { name: name.clone() }),
        };
        attrs.push((name.clone(), kind, col));
    }

    for i in 0..batch.num_rows() {
        // Go through the array accessors, which respect slice offsets, not raw buffers.
        let row_vectors = lists.value(i);
        let row_vectors = row_vectors.as_fixed_size_list();
        let mut vectors = Vec::with_capacity(row_vectors.len());
        for v in 0..row_vectors.len() {
            let one = row_vectors.value(v);
            vectors.push(one.as_primitive::<Float32Type>().values().to_vec());
        }
        let mut row_attrs = BTreeMap::new();
        for (name, t, col) in &attrs {
            if col.is_null(i) {
                continue;
            }
            let value = match t {
                AttrKind::Str => AttrValue::Str(col.as_string::<i32>().value(i).to_string()),
                AttrKind::I64 => AttrValue::I64(col.as_primitive::<Int64Type>().value(i)),
                AttrKind::F64 => AttrValue::F64(col.as_primitive::<Float64Type>().value(i)),
                AttrKind::Bool => AttrValue::Bool(col.as_boolean().value(i)),
            };
            row_attrs.insert(name.clone(), value);
        }
        out.push(DocRow {
            id: ids.value(i).to_string(),
            lsn: Lsn::new(
                u64::try_from(lsns.value(i))
                    .map_err(|_| SotError::Format("negative lsn".into()))?,
            ),
            deleted: deleted.value(i),
            vectors,
            attrs: row_attrs,
        });
    }
    Ok(())
}

/// What the manifest records about a segment's SoT file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SotRef {
    pub size: u64,
    pub sha256: Sha256Hex,
}

fn sha256(data: &[u8]) -> [u8; 32] {
    Sha256::digest(data).into()
}

/// Writes and reads SoT files. Reads are retried like the other stores. Writes are create-only:
/// `write` retries an unclear put itself and accepts the key only if it holds the same bytes.
pub struct SotStore<S: ObjectStore> {
    store: Retrying<Arc<S>>,
}

impl<S: ObjectStore> SotStore<S> {
    pub fn new(store: Arc<S>) -> Self {
        Self {
            store: Retrying::new(store, RetryPolicy::default()),
        }
    }

    pub fn with_retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.store = Retrying::new(self.store.inner().clone(), policy);
        self
    }

    fn key(keys: &NamespaceKeys, segment_id: &str) -> Result<Key, SotError> {
        if !valid_segment_id(segment_id) {
            return Err(SotError::InvalidSegmentId(segment_id.to_string()));
        }
        Ok(keys.segment_key(segment_id, DOCS_FILE))
    }

    /// Encodes `rows` and creates the segment's SoT file. Never overwrites: if the key already
    /// holds exactly these bytes the write succeeds (idempotent retry), otherwise
    /// `AlreadyExists`.
    pub fn write(
        &self,
        keys: &NamespaceKeys,
        segment_id: &str,
        rows: &[DocRow],
        schema: &AttrSchema,
        dims: u32,
    ) -> Result<SotRef, SotError> {
        let key = Self::key(keys, segment_id)?;
        let bytes = encode(rows, schema, dims)?;
        let r = SotRef {
            size: bytes.len() as u64,
            sha256: Sha256Hex(sha256(&bytes)),
        };
        // Encoding is deterministic, so a retried write (after a lost response) or a racing
        // write of the same WAL range produces the same bytes. Whenever the outcome is unclear
        // or the key is taken, look at what is there: the same bytes count as success, other
        // bytes are a real conflict, nothing yet means try again.
        for _ in 0..MAX_PUT_ATTEMPTS {
            let outcome = self
                .store
                .put(&key, bytes.clone(), PutCondition::IfNoneMatch);
            let retry_put = match outcome {
                Ok(_) => return Ok(r),
                Err(StoreError::PreconditionFailed)
                | Err(StoreError::Conflict)
                | Err(StoreError::Unavailable(_)) => match self.store.get(&key) {
                    Ok((existing, _)) if sha256(&existing) == r.sha256.0 => return Ok(r),
                    Ok(_) => return Err(SotError::AlreadyExists),
                    Err(StoreError::NotFound) | Err(StoreError::Unavailable(_)) => true,
                    Err(e) => return Err(e.into()),
                },
                Err(e) => return Err(e.into()),
            };
            debug_assert!(retry_put);
        }
        Err(SotError::Store(StoreError::Unavailable(
            "SoT write: too many attempts".into(),
        )))
    }

    /// Reads the whole file, checks it is the recorded one, and decodes it.
    pub fn read(
        &self,
        keys: &NamespaceKeys,
        segment_id: &str,
        recorded: &SotRef,
        schema: &AttrSchema,
        dims: u32,
    ) -> Result<Vec<DocRow>, SotError> {
        let key = Self::key(keys, segment_id)?;
        let (bytes, _) = self.store.get(&key)?;
        if bytes.len() as u64 != recorded.size || sha256(&bytes) != recorded.sha256.0 {
            return Err(SotError::RefMismatch);
        }
        decode_bytes(bytes::Bytes::from(bytes), schema, dims)
    }
}
