//! One namespace's write, flush and read paths (PLAN 5.6, 6.4; M1 "searchable unindexed tail").
//!
//! - **write**: a batch of upserts and deletes is validated, encoded as one WAL entry and
//!   appended. Every op in the batch gets that entry's LSN. Once `write` returns, strong reads see
//!   the batch.
//! - **flush**: the WAL after the manifest's `indexed_through` is folded to the latest row per
//!   document and written as one segment's Parquet SoT. Then a manifest version that lists the
//!   segment and moves `indexed_through` is committed. The order is fixed: widen the attribute
//!   schema (fail before writing anything on a conflict), write the SoT, commit.
//! - **read_all** (strong): the latest manifest's segments plus the WAL tail after its
//!   `indexed_through`, merged by highest LSN per document id, with tombstones hiding older
//!   versions.
//!
//! Flushes are safe to retry and to race. The segment id is derived from the WAL range it covers
//! (`w<first lsn>-<last lsn>`), and SoT encoding is deterministic, so two flushes of the same range
//! write identical bytes to the same key and `SotStore::write` accepts the existing object. Only
//! one of them can commit (the manifest commit is conditional on the version it read). The loser
//! gets `CommitConflict` and leaves nothing behind that the winner did not also write.

use std::collections::BTreeMap;
use std::sync::Arc;

use seabed_domain::{Lsn, NamespaceKeys, Version};
use seabed_ports::ObjectStore;
use serde::{Deserialize, Serialize};

use crate::manifest::{ManifestError, ManifestStore};
use crate::manifest_content::{ContentError, ManifestContent, SegmentRef};
use crate::sot::{schema_of, validate_rows, AttrValue, DocRow, SotError, SotStore};
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
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WalRecord {
    v: u32,
    ops: Vec<Op>,
}

const WAL_RECORD_VERSION: u32 = 1;

fn decode_record(lsn: Lsn, payload: &[u8]) -> Result<Vec<Op>, NamespaceError> {
    let corrupt = |reason: String| NamespaceError::CorruptRecord { lsn, reason };
    let rec: WalRecord = serde_json::from_slice(payload).map_err(|e| corrupt(e.to_string()))?;
    if rec.v != WAL_RECORD_VERSION {
        return Err(corrupt(format!("unsupported record version {}", rec.v)));
    }
    Ok(rec.ops)
}

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
    /// Refused without writing: an empty batch; the same id twice in one batch; any SoT rule
    /// break (dimensions, NaN, id or attribute name); an attribute whose type conflicts with the
    /// namespace schema or with another op in the batch. A new attribute is accepted here and
    /// added to the schema at flush. Two concurrent writers can still introduce the same new
    /// attribute with different types; the later one is then refused at flush.
    pub fn write(&self, request_id: u64, ops: &[Op]) -> Result<Lsn, NamespaceError> {
        if ops.is_empty() {
            return Err(NamespaceError::Invalid(SotError::Format(
                "empty batch".into(),
            )));
        }
        let rows: Vec<DocRow> = ops.iter().map(|op| op.to_row(Lsn::new(1))).collect();
        let (_, content) = self.latest()?;
        let mut schema = content.attr_schema.clone();
        let additions = schema_of(&rows).map_err(NamespaceError::Invalid)?;
        let mut probe = content.clone();
        probe.extend_attr_schema(&additions)?;
        schema.extend(additions);
        validate_rows(&rows, &schema, self.dims).map_err(NamespaceError::Invalid)?;

        let payload = serde_json::to_vec(&WalRecord {
            v: WAL_RECORD_VERSION,
            ops: ops.to_vec(),
        })
        .expect("validated ops always serialize");
        Ok(self.wal.append(request_id, &payload)?)
    }

    /// Moves the WAL tail into a new segment. Returns `None` when there is nothing to flush.
    ///
    /// Fails with `Manifest(CommitConflict)` if another flush committed first; call again to
    /// flush whatever is still unindexed.
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

        // Schema first, so a type conflict fails before any object is written.
        content.extend_attr_schema(&schema_of(&rows).map_err(NamespaceError::Sot)?)?;
        let segment_id = format!("w{first:020}-{:020}", through.get());
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
        let version = self
            .manifests
            .commit_content(expected, through.get(), &content)?;
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
