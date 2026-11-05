//! One namespace's write, flush and read paths (PLAN 5.6, 6.4; M1 "searchable unindexed tail").
//!
//! - **write**: a batch of upserts and deletes is validated and appended as one WAL entry. Every
//!   op in the batch gets that entry's LSN. If the batch introduces attributes the namespace
//!   schema does not have yet, a manifest version that adds them is committed **before** the
//!   append (schema first). A type conflict is therefore always refused at `write`, whether the
//!   conflicting writes are sequential or concurrent. Without that, two writes with different
//!   types for a new attribute would both reach the WAL, and since WAL entries are never removed,
//!   every later flush would fail. Once `write` returns, strong reads see the batch.
//! - **flush**: the WAL after the manifest's `indexed_through` is folded to the latest row per
//!   document and written as one segment's Parquet SoT. Then a manifest version that lists the
//!   segment and moves `indexed_through` is committed.
//! - **read_all** (strong): one manifest version is loaded, then its segments and the WAL tail
//!   after its `indexed_through` are read and merged by highest LSN per document id, with
//!   tombstones hiding older versions. Both parts come from the same manifest version. This is
//!   correct because WAL entries are never deleted (a future WAL GC must keep entries above every
//!   readable manifest's `indexed_through`).
//!
//! Flushes are safe to retry and to race. The segment id is
//! `w<first lsn>-<last lsn>-<fingerprint>`, where the fingerprint hashes everything besides the
//! WAL range that decides the file's bytes: the attribute schema, `dims`, the SoT format version
//! and the Parquet writer version. A change to `sot::encode`'s output must bump
//! `SOT_FORMAT_VERSION` so the fingerprint changes too. Identical inputs give the same id and byte-identical files, so `SotStore::write`
//! accepts the existing object (retries and same-range races leave one file). Different inputs
//! (the schema grew between two flushes of one range, or a rolling upgrade changed the writer)
//! give a different id, so they can never block each other. They may leave an orphan file for GC.
//! Manifest commit ids are derived from the committed content, so mistaking an identical
//! concurrent commit for one's own after a lost response is harmless.

use std::collections::BTreeMap;
use std::sync::Arc;

use seabed_domain::{Lsn, NamespaceKeys, Version};
use seabed_ports::ObjectStore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::manifest::{ManifestError, ManifestStore};
use crate::manifest_content::{AttrSchema, ContentError, ManifestContent, SegmentRef};
use crate::sot::{
    schema_of, validate_rows, AttrValue, DocRow, SotError, SotStore, SOT_FORMAT_VERSION,
};
use crate::wal::{Wal, WalError};
use crate::RetryPolicy;

#[derive(Debug, Clone, PartialEq)]
pub enum NamespaceError {
    Wal(WalError),
    Manifest(ManifestError),
    Content(ContentError),
    /// The batch breaks a SoT rule (dimensions, ids, attribute types, ...). Nothing was written.
    Invalid(SotError),
    Sot(SotError),
    /// A WAL entry could not be decoded as a write batch.
    CorruptRecord {
        lsn: Lsn,
        reason: String,
    },
}

impl From<WalError> for NamespaceError {
    fn from(e: WalError) -> Self {
        NamespaceError::Wal(e)
    }
}

impl From<ManifestError> for NamespaceError {
    fn from(e: ManifestError) -> Self {
        NamespaceError::Manifest(e)
    }
}

impl From<ContentError> for NamespaceError {
    fn from(e: ContentError) -> Self {
        NamespaceError::Content(e)
    }
}

/// One change to a document.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Op {
    Upsert {
        id: String,
        vectors: Vec<Vec<f32>>,
        #[serde(default)]
        attrs: BTreeMap<String, AttrValue>,
    },
    Delete {
        id: String,
    },
}

impl Op {
    pub fn upsert(id: &str, vectors: Vec<Vec<f32>>) -> Self {
        Op::Upsert {
            id: id.to_string(),
            vectors,
            attrs: BTreeMap::new(),
        }
    }

    pub fn delete(id: &str) -> Self {
        Op::Delete { id: id.to_string() }
    }

    /// Adds an attribute to an upsert. No effect on a delete.
    pub fn with_attr(mut self, name: &str, value: AttrValue) -> Self {
        if let Op::Upsert { attrs, .. } = &mut self {
            attrs.insert(name.to_string(), value);
        }
        self
    }

    fn to_row(&self, lsn: Lsn) -> DocRow {
        match self {
            Op::Upsert { id, vectors, attrs } => {
                let mut r = DocRow::upsert(id, lsn.get(), vectors.clone());
                r.attrs = attrs.clone();
                r
            }
            Op::Delete { id } => DocRow::tombstone(id, lsn.get()),
        }
    }
}

/// Payload of one WAL entry. Versioned so the record format can change later.
///
/// Floats are JSON numbers; `serde_json` is built with `float_roundtrip` so f32 and f64 values
/// come back bit for bit (without it about 10% of f64 values change in the last bit).
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WalRecord {
    v: u32,
    ops: Vec<Op>,
}

const WAL_RECORD_VERSION: u32 = 1;

fn encode_record(ops: &[Op]) -> Vec<u8> {
    serde_json::to_vec(&WalRecord {
        v: WAL_RECORD_VERSION,
        ops: ops.to_vec(),
    })
    .expect("validated ops always serialize")
}

fn decode_record(lsn: Lsn, payload: &[u8]) -> Result<Vec<Op>, NamespaceError> {
    let corrupt = |reason: String| NamespaceError::CorruptRecord { lsn, reason };
    // Version first: the full record denies unknown fields, so another version's layout would
    // otherwise fail as a generic decode error.
    #[derive(Deserialize)]
    struct VersionOnly {
        v: u32,
    }
    let probe: VersionOnly = serde_json::from_slice(payload).map_err(|e| corrupt(e.to_string()))?;
    if probe.v != WAL_RECORD_VERSION {
        return Err(corrupt(format!("unsupported record version {}", probe.v)));
    }
    let rec: WalRecord = serde_json::from_slice(payload).map_err(|e| corrupt(e.to_string()))?;
    Ok(rec.ops)
}

fn sha256(data: &[u8]) -> [u8; 32] {
    Sha256::digest(data).into()
}

/// Commit id derived from the content being committed. Two commits share an id only if they
/// commit identical content, so recognising the other's object as one's own is harmless.
fn content_commit_id(content: &ManifestContent) -> u64 {
    u64::from_le_bytes(sha256(&content.encode())[..8].try_into().unwrap())
}

/// Everything besides the WAL range that determines a flush's SoT bytes.
fn segment_fingerprint(schema: &AttrSchema, dims: u32) -> String {
    let mut input = serde_json::to_vec(schema).expect("schema serializes");
    input.extend_from_slice(&dims.to_le_bytes());
    input.extend_from_slice(&SOT_FORMAT_VERSION.to_le_bytes());
    input.extend_from_slice(parquet::file::properties::DEFAULT_CREATED_BY.as_bytes());
    sha256(&input)[..4]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// Attempts at committing a schema extension before giving up (each loss is a real concurrent
/// manifest commit).
const SCHEMA_COMMIT_ATTEMPTS: usize = 32;

/// Keeps, per document id, the row with the highest LSN. Tombstones are kept so they can hide
/// older versions from earlier sources.
fn merge_latest(into: &mut BTreeMap<String, DocRow>, rows: impl IntoIterator<Item = DocRow>) {
    for row in rows {
        match into.get(&row.id) {
            Some(existing) if existing.lsn >= row.lsn => {}
            _ => {
                into.insert(row.id.clone(), row);
            }
        }
    }
}

/// What `flush` did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Flushed {
    pub version: Version,
    pub segment_id: String,
    pub indexed_through: Lsn,
    pub rows: usize,
}

pub struct Namespace<S: ObjectStore> {
    keys: NamespaceKeys,
    dims: u32,
    wal: Wal<S>,
    manifests: ManifestStore<S>,
    sots: SotStore<S>,
}

impl<S: ObjectStore> Namespace<S> {
    /// `dims` is the vector dimension every document in the namespace uses.
    pub fn new(store: Arc<S>, keys: NamespaceKeys, dims: u32) -> Result<Self, NamespaceError> {
        Ok(Self {
            wal: Wal::new(store.clone(), keys.clone())?,
            manifests: ManifestStore::new(store.clone(), keys.clone())?,
            sots: SotStore::new(store),
            keys,
            dims,
        })
    }

    /// Replaces the read retry policy of every store this namespace uses.
    pub fn with_retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.wal = self.wal.with_retry_policy(policy);
        self.manifests = self.manifests.with_retry_policy(policy);
        self.sots = self.sots.with_retry_policy(policy);
        self
    }

    /// Replaces the write retry policy of every store this namespace uses: the budget and
    /// backoff for transient failures while appending to the WAL, committing a manifest and
    /// writing a SoT file. Losing a race to another writer is not limited by it.
    pub fn with_write_retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.wal = self.wal.with_write_retry_policy(policy);
        self.manifests = self.manifests.with_write_retry_policy(policy);
        self.sots = self.sots.with_write_retry_policy(policy);
        self
    }

    fn latest(&self) -> Result<(Option<Version>, ManifestContent), NamespaceError> {
        Ok(match self.manifests.load_latest_content()? {
            Some((v, c)) => (Some(v), c),
            None => (None, ManifestContent::empty(&self.keys)),
        })
    }

    /// WAL entries after `indexed_through`, as rows in LSN order.
    fn tail(
        &self,
        content: &ManifestContent,
    ) -> Result<(Vec<DocRow>, Option<Lsn>), NamespaceError> {
        let from = Lsn::new(content.indexed_through.map_or(1, |l| l + 1));
        let mut rows = Vec::new();
        let mut last = None;
        for entry in self.wal.read_from(from)? {
            for op in decode_record(entry.lsn, &entry.payload)? {
                rows.push(op.to_row(entry.lsn));
            }
            last = Some(entry.lsn);
        }
        Ok((rows, last))
    }

    /// Validates `ops` and appends them as one WAL entry. Returns the entry's LSN.
    ///
    /// Refused without appending (`Invalid`): an empty batch; the same id twice in one batch; any
    /// SoT rule break (dimensions, NaN, id or attribute name); an attribute whose type conflicts
    /// with the namespace schema or with another op in the batch. If the batch adds attributes,
    /// the widened schema is committed first (see the module docs); a concurrent writer that
    /// commits a different type first makes this write fail with `Invalid` after a reload.
    ///
    /// A write that fails after the schema commit (for example the WAL append fails) still leaves
    /// its new attributes in the schema: their types stay reserved.
    ///
    /// `Wal(Store(Unavailable))` means the write retry budget ran out with the append's outcome
    /// unknown: the batch may or may not be in the WAL. The WAL does not yet recognise a retried
    /// `request_id` across calls, so retrying may append the batch twice (harmless for a lone
    /// writer; another writer's update to the same ids in between would be overwritten).
    pub fn write(&self, request_id: u64, ops: &[Op]) -> Result<Lsn, NamespaceError> {
        if ops.is_empty() {
            return Err(NamespaceError::Invalid(SotError::Format(
                "empty batch".into(),
            )));
        }
        let rows: Vec<DocRow> = ops.iter().map(|op| op.to_row(Lsn::new(1))).collect();
        let used = schema_of(&rows).map_err(NamespaceError::Invalid)?;
        for _ in 0..SCHEMA_COMMIT_ATTEMPTS {
            let (expected, content) = self.latest()?;
            let mut widened = content.clone();
            widened.extend_attr_schema(&used).map_err(|e| match e {
                ContentError::AttrSchemaChanged { name, .. } => {
                    NamespaceError::Invalid(SotError::AttrTypeConflict { name })
                }
                ContentError::InvalidAttrName(n) => {
                    NamespaceError::Invalid(SotError::InvalidAttrName(n))
                }
                other => NamespaceError::Content(other),
            })?;
            validate_rows(&rows, &widened.attr_schema, self.dims)
                .map_err(NamespaceError::Invalid)?;
            if widened.attr_schema != content.attr_schema {
                // Schema first: once committed, no later write can add a conflicting type.
                match self
                    .manifests
                    .commit_content(expected, content_commit_id(&widened), &widened)
                {
                    Ok(_) => {}
                    Err(ManifestError::CommitConflict) => continue, // reload and re-check
                    Err(e) => return Err(e.into()),
                }
            }
            // The schema only grows, so the batch stays valid against every later schema.
            return Ok(self.wal.append(request_id, &encode_record(ops))?);
        }
        Err(NamespaceError::Manifest(ManifestError::TooManyAttempts))
    }

    /// Moves the WAL tail into a new segment. Returns `None` when there is nothing to flush.
    ///
    /// Fails with `Manifest(CommitConflict)` if another manifest commit landed first (another
    /// flush, or a write adding attributes); call again to flush whatever is still unindexed.
    pub fn flush(&self) -> Result<Option<Flushed>, NamespaceError> {
        let (expected, mut content) = self.latest()?;
        let (tail, last) = self.tail(&content)?;
        let Some(through) = last else {
            return Ok(None);
        };
        let first = content.indexed_through.map_or(1, |l| l + 1);
        let mut latest = BTreeMap::new();
        merge_latest(&mut latest, tail);
        let rows: Vec<DocRow> = latest.into_values().collect();

        // `write` committed every attribute before appending, so this normally adds nothing.
        // Kept as a guard: a conflict fails here, before any object is written.
        content.extend_attr_schema(&schema_of(&rows).map_err(NamespaceError::Sot)?)?;
        let segment_id = format!(
            "w{first:020}-{:020}-{}",
            through.get(),
            segment_fingerprint(&content.attr_schema, self.dims)
        );
        let docs = self
            .sots
            .write(
                &self.keys,
                &segment_id,
                &rows,
                &content.attr_schema,
                self.dims,
            )
            .map_err(NamespaceError::Sot)?;
        content.add_segment(SegmentRef {
            id: segment_id.clone(),
            docs,
            index: None,
        })?;
        content.indexed_through = Some(through.get());
        let version =
            self.manifests
                .commit_content(expected, content_commit_id(&content), &content)?;
        Ok(Some(Flushed {
            version,
            segment_id,
            indexed_through: through,
            rows: rows.len(),
        }))
    }

    /// Strong read of every live document: all segments of the latest manifest plus the WAL
    /// tail after it, latest LSN per id, tombstones removed. Sorted by id.
    pub fn read_all(&self) -> Result<Vec<DocRow>, NamespaceError> {
        let (_, content) = self.latest()?;
        let mut latest = BTreeMap::new();
        for s in &content.segments {
            let rows = self
                .sots
                .read(&self.keys, &s.id, &s.docs, &content.attr_schema, self.dims)
                .map_err(NamespaceError::Sot)?;
            merge_latest(&mut latest, rows);
        }
        let (tail, _) = self.tail(&content)?;
        merge_latest(&mut latest, tail);
        Ok(latest.into_values().filter(|r| !r.deleted).collect())
    }

    /// Strong read of one document.
    pub fn get(&self, id: &str) -> Result<Option<DocRow>, NamespaceError> {
        Ok(self.read_all()?.into_iter().find(|r| r.id == id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deferred from PR #14 review: WAL records must keep floats bit for bit.
    #[test]
    fn wal_record_roundtrips_f32_and_f64_bit_exact() {
        let mut state = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let mut f64s: Vec<f64> = vec![
            -0.0,
            0.0,
            f64::MIN_POSITIVE / 2.0,
            f64::MAX,
            0.1,
            971.9863718547629,
        ];
        let mut f32s: Vec<f32> = vec![-0.0, 0.0, f32::MIN_POSITIVE / 2.0, f32::MAX, 0.1];
        for _ in 0..20_000 {
            let x = f64::from_bits(next());
            if x.is_finite() {
                f64s.push(x);
            }
            f64s.push((next() >> 11) as f64 / (1u64 << 53) as f64 * 1000.0);
            let y = f32::from_bits(next() as u32);
            if y.is_finite() {
                f32s.push(y);
            }
        }
        let ops: Vec<Op> = f64s
            .iter()
            .enumerate()
            .map(|(i, x)| {
                let v = f32s[i % f32s.len()];
                Op::upsert(&format!("d{i}"), vec![vec![v, -v]]).with_attr("x", AttrValue::F64(*x))
            })
            .collect();
        let back = decode_record(Lsn::new(1), &encode_record(&ops)).unwrap();
        for (a, b) in ops.iter().zip(&back) {
            match (a, b) {
                (
                    Op::Upsert {
                        vectors: va,
                        attrs: aa,
                        ..
                    },
                    Op::Upsert {
                        vectors: vb,
                        attrs: ab,
                        ..
                    },
                ) => {
                    for (x, y) in va[0].iter().zip(&vb[0]) {
                        assert_eq!(x.to_bits(), y.to_bits(), "f32 {x} changed");
                    }
                    match (&aa["x"], &ab["x"]) {
                        (AttrValue::F64(x), AttrValue::F64(y)) => {
                            assert_eq!(x.to_bits(), y.to_bits(), "f64 {x} changed")
                        }
                        other => panic!("{other:?}"),
                    }
                }
                other => panic!("{other:?}"),
            }
        }
    }

    #[test]
    fn decode_record_reports_unknown_versions_and_garbage_as_corrupt() {
        let lsn = Lsn::new(7);
        let future = br#"{"v":2,"ops":[],"new_field":1}"#;
        match decode_record(lsn, future) {
            Err(NamespaceError::CorruptRecord { lsn: l, reason }) => {
                assert_eq!(l, lsn);
                assert!(reason.contains("unsupported record version 2"), "{reason}");
            }
            other => panic!("{other:?}"),
        }
        for garbage in [
            &b"not json"[..],
            br#"{"ops":[]}"#,
            br#"{"v":1,"ops":[{"rename":{}}]}"#,
        ] {
            assert!(matches!(
                decode_record(lsn, garbage),
                Err(NamespaceError::CorruptRecord { .. })
            ));
        }
    }

    /// The rule is "highest LSN wins", not "last source wins". Today sources happen to arrive
    /// oldest first, but compaction will rewrite segments, so the order must not matter.
    #[test]
    fn merge_keeps_the_highest_lsn_whatever_the_source_order() {
        let mut m = BTreeMap::new();
        merge_latest(&mut m, [DocRow::upsert("a", 5, vec![])]);
        merge_latest(&mut m, [DocRow::tombstone("a", 3)]); // older, arrives later
        assert_eq!(m["a"].lsn, Lsn::new(5));
        assert!(!m["a"].deleted);
        merge_latest(&mut m, [DocRow::tombstone("a", 7)]);
        assert!(m["a"].deleted);
    }
}
