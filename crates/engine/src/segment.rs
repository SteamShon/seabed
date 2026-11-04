//! Immutable segment files with a footer table of contents (PLAN 6.5).
//!
//! Layout, front to back:
//!
//! ```text
//! [section bytes, in name order] [TOC] [footer, FOOTER_LEN bytes]
//!
//! TOC entry   = name_len u16 | name (UTF-8) | offset u64 | len u64 | sha256 [32]
//! TOC         = entry_count u32 | entries...
//! footer      = toc_len u32 | toc_sha256 [32] | format_version u16 | magic "SBSG"
//! ```
//!
//! All integers are little endian. A reader fetches the tail with one range GET (the TOC is
//! usually inside it), then range-GETs only the sections it needs and verifies each SHA-256.
//! The same set of sections always encodes to the same bytes: sections are ordered by name and
//! nothing time- or host-dependent is written.
//!
//! The object size comes from the caller (the manifest records it) rather than from an extra
//! HEAD/LIST request.

use std::collections::BTreeMap;
use std::sync::Arc;

use seabed_adapter_objectstore_retry::{RetryPolicy, Retrying};
use seabed_domain::{Key, NamespaceKeys};
use seabed_ports::{ObjectStore, PutCondition, StoreError};
use sha2::{Digest, Sha256};

use crate::manifest_content::{valid_segment_id, SegmentRef, Sha256Hex};

pub const FORMAT_VERSION: u16 = 1;
pub const MAGIC: [u8; 4] = *b"SBSG";
/// toc_len (4) + toc_sha256 (32) + format_version (2) + magic (4).
pub const FOOTER_LEN: usize = 4 + 32 + 2 + 4;
/// How many tail bytes `open` reads in its first request. A TOC that fits costs one GET.
pub const TAIL_HINT: u64 = 4096;
pub const MAX_NAME_LEN: usize = 255;
/// File name of a segment inside `segments/<id>/`.
pub const DATA_FILE: &str = "data.sbsg";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SegmentError {
    Store(StoreError),
    /// A segment already exists at this key. Segments are immutable.
    AlreadyExists,
    InvalidName(String),
    /// The segment id would not be accepted by the manifest (or could escape the namespace's
    /// key prefix). Checked before anything is written.
    InvalidSegmentId(String),
    /// The object at a manifest-listed segment's key is not the one the manifest recorded
    /// (different TOC hash): it was replaced, not damaged. Distinct from `Corrupt`.
    RefMismatch {
        id: String,
    },
    DuplicateSection(String),
    MissingSection(String),
    UnsupportedVersion(u16),
    ChecksumMismatch {
        section: String,
    },
    Corrupt(&'static str),
}

impl From<StoreError> for SegmentError {
    fn from(e: StoreError) -> Self {
        SegmentError::Store(e)
    }
}

/// What `write` stored: the values the manifest records about a segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Written {
    pub size: u64,
    pub toc_sha256: [u8; 32],
}

fn sha256(data: &[u8]) -> [u8; 32] {
    Sha256::digest(data).into()
}

/// One entry of the table of contents.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SectionInfo {
    pub name: String,
    pub offset: u64,
    pub len: u64,
    pub sha256: [u8; 32],
}

/// Collects named sections and encodes them into segment bytes.
#[derive(Debug, Default)]
pub struct SegmentBuilder {
    sections: BTreeMap<String, Vec<u8>>,
}

impl SegmentBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, name: &str, data: Vec<u8>) -> Result<&mut Self, SegmentError> {
        if name.is_empty() || name.len() > MAX_NAME_LEN {
            return Err(SegmentError::InvalidName(name.to_string()));
        }
        if self.sections.contains_key(name) {
            return Err(SegmentError::DuplicateSection(name.to_string()));
        }
        self.sections.insert(name.to_string(), data);
        Ok(self)
    }

    /// Encodes the segment. Deterministic for a given set of sections.
    pub fn encode(&self) -> Vec<u8> {
        let body_len: usize = self.sections.values().map(Vec::len).sum();
        let mut out = Vec::with_capacity(body_len + 64 * self.sections.len() + FOOTER_LEN);
        let mut toc = Vec::new();
        toc.extend_from_slice(&(self.sections.len() as u32).to_le_bytes());
        for (name, data) in &self.sections {
            let offset = out.len() as u64;
            out.extend_from_slice(data);
            toc.extend_from_slice(&(name.len() as u16).to_le_bytes());
            toc.extend_from_slice(name.as_bytes());
            toc.extend_from_slice(&offset.to_le_bytes());
            toc.extend_from_slice(&(data.len() as u64).to_le_bytes());
            toc.extend_from_slice(&sha256(data));
        }
        out.extend_from_slice(&toc);
        out.extend_from_slice(&(toc.len() as u32).to_le_bytes());
        out.extend_from_slice(&sha256(&toc));
        out.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        out.extend_from_slice(&MAGIC);
        out
    }
}

/// Little-endian cursor over a byte slice that fails instead of panicking.
struct Cursor<'a>(&'a [u8]);

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], SegmentError> {
        if self.0.len() < n {
            return Err(SegmentError::Corrupt("toc truncated"));
        }
        let (head, rest) = self.0.split_at(n);
        self.0 = rest;
        Ok(head)
    }
    fn u16(&mut self) -> Result<u16, SegmentError> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32, SegmentError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64, SegmentError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
}

fn parse_toc(toc: &[u8], body_len: u64) -> Result<Vec<SectionInfo>, SegmentError> {
    let mut c = Cursor(toc);
    let count = c.u32()?;
    let mut out = Vec::new();
    for _ in 0..count {
        let name_len = c.u16()? as usize;
        let name = std::str::from_utf8(c.take(name_len)?)
            .map_err(|_| SegmentError::Corrupt("section name is not UTF-8"))?
            .to_string();
        let offset = c.u64()?;
        let len = c.u64()?;
        let sha256: [u8; 32] = c.take(32)?.try_into().unwrap();
        let end = offset
            .checked_add(len)
            .ok_or(SegmentError::Corrupt("section range overflows"))?;
        if end > body_len {
            return Err(SegmentError::Corrupt("section points past the body"));
        }
        out.push(SectionInfo {
            name,
            offset,
            len,
            sha256,
        });
    }
    if !c.0.is_empty() {
        return Err(SegmentError::Corrupt("trailing bytes after toc"));
    }
    Ok(out)
}

/// Writes and reads segments on an object store. Reads are retried like `Wal` and
/// `ManifestStore`; the create-only write is not.
pub struct SegmentStore<S: ObjectStore> {
    store: Retrying<Arc<S>>,
}

impl<S: ObjectStore> SegmentStore<S> {
    pub fn new(store: Arc<S>) -> Result<Self, SegmentError> {
        if !store.caps().range_get {
            return Err(SegmentError::Store(StoreError::Unsupported(
                "object store lacks range reads",
            )));
        }
        Ok(Self {
            store: Retrying::new(store, RetryPolicy::default()),
        })
    }

    pub fn with_retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.store = Retrying::new(self.store.inner().clone(), policy);
        self
    }

    /// Creates the segment at `key` and returns its size and TOC hash (to record in the
    /// manifest). Fails with `AlreadyExists` if anything is already there: segments are never
    /// overwritten.
    pub fn write(&self, key: &Key, segment: &SegmentBuilder) -> Result<Written, SegmentError> {
        let bytes = segment.encode();
        let size = bytes.len() as u64;
        let footer = &bytes[bytes.len() - FOOTER_LEN..];
        let toc_sha256: [u8; 32] = footer[4..36].try_into().unwrap();
        match self.store.put(key, bytes, PutCondition::IfNoneMatch) {
            Ok(_) => Ok(Written { size, toc_sha256 }),
            Err(StoreError::PreconditionFailed) => Err(SegmentError::AlreadyExists),
            Err(e) => Err(e.into()),
        }
    }

    /// Writes segment `id` of the namespace and returns the reference to add to the manifest.
    pub fn write_segment(
        &self,
        keys: &NamespaceKeys,
        id: &str,
        segment: &SegmentBuilder,
    ) -> Result<SegmentRef, SegmentError> {
        if !valid_segment_id(id) {
            return Err(SegmentError::InvalidSegmentId(id.to_string()));
        }
        let w = self.write(&keys.segment_key(id, DATA_FILE), segment)?;
        Ok(SegmentRef {
            id: id.to_string(),
            size: w.size,
            toc_sha256: Sha256Hex(w.toc_sha256),
        })
    }

    /// Opens a segment listed in the manifest and checks that the object is the one recorded.
    pub fn open_ref(
        &self,
        keys: &NamespaceKeys,
        segment: &SegmentRef,
    ) -> Result<SegmentReader<'_, S>, SegmentError> {
        let reader = self.open(&keys.segment_key(&segment.id, DATA_FILE), segment.size)?;
        if reader.toc_sha256 != segment.toc_sha256.0 {
            return Err(SegmentError::RefMismatch {
                id: segment.id.clone(),
            });
        }
        Ok(reader)
    }

    /// Reads the footer and TOC. `size` is the object size recorded when it was written.
    pub fn open(&self, key: &Key, size: u64) -> Result<SegmentReader<'_, S>, SegmentError> {
        if size < FOOTER_LEN as u64 {
            return Err(SegmentError::Corrupt("smaller than the footer"));
        }
        let tail_start = size.saturating_sub(TAIL_HINT);
        let tail = self.store.get_range(key, tail_start..size)?;
        if tail.len() as u64 != size - tail_start {
            return Err(SegmentError::Corrupt("object size does not match"));
        }

        let footer = &tail[tail.len() - FOOTER_LEN..];
        if footer[FOOTER_LEN - 4..] != MAGIC {
            return Err(SegmentError::Corrupt("bad magic"));
        }
        let version = u16::from_le_bytes(footer[36..38].try_into().unwrap());
        if version != FORMAT_VERSION {
            return Err(SegmentError::UnsupportedVersion(version));
        }
        let toc_len = u32::from_le_bytes(footer[..4].try_into().unwrap()) as u64;
        let toc_sha: [u8; 32] = footer[4..36].try_into().unwrap();

        let toc_end = size - FOOTER_LEN as u64;
        let toc_start = toc_end
            .checked_sub(toc_len)
            .ok_or(SegmentError::Corrupt("toc larger than the object"))?;
        let toc = if toc_start >= tail_start {
            // Common case: the TOC came with the tail read.
            let from = (toc_start - tail_start) as usize;
            tail[from..from + toc_len as usize].to_vec()
        } else {
            self.store.get_range(key, toc_start..toc_end)?
        };
        if sha256(&toc) != toc_sha {
            return Err(SegmentError::Corrupt("toc checksum mismatch"));
        }
        let sections = parse_toc(&toc, toc_start)?;
        Ok(SegmentReader {
            store: &self.store,
            key: key.clone(),
            sections,
            toc_sha256: toc_sha,
        })
    }
}

/// An opened segment. Reading a section costs one range GET of exactly that section.
pub struct SegmentReader<'a, S: ObjectStore> {
    store: &'a Retrying<Arc<S>>,
    key: Key,
    sections: Vec<SectionInfo>,
    toc_sha256: [u8; 32],
}

impl<S: ObjectStore> SegmentReader<'_, S> {
    pub fn toc_sha256(&self) -> [u8; 32] {
        self.toc_sha256
    }

    /// Sections in name order.
    pub fn sections(&self) -> &[SectionInfo] {
        &self.sections
    }

    pub fn section(&self, name: &str) -> Option<&SectionInfo> {
        self.sections.iter().find(|s| s.name == name)
    }

    /// Fetches one section and verifies its SHA-256.
    pub fn read(&self, name: &str) -> Result<Vec<u8>, SegmentError> {
        let info = self
            .section(name)
            .ok_or_else(|| SegmentError::MissingSection(name.to_string()))?;
        let data = if info.len == 0 {
            Vec::new()
        } else {
            self.store
                .get_range(&self.key, info.offset..info.offset + info.len)?
        };
        if data.len() as u64 != info.len || sha256(&data) != info.sha256 {
            return Err(SegmentError::ChecksumMismatch {
                section: name.to_string(),
            });
        }
        Ok(data)
    }
}
