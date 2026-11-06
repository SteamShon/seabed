//! Port traits and capability declarations.
//!
//! The skeleton is synchronous and uses `Vec<u8>` so it builds with the standard library only.
//! Open design questions 1 and 4 in the design doc cover the async runtime and `Bytes`/Arrow buffers.

use std::fmt;
use std::ops::Range;
use std::sync::Arc;

use seabed_domain::{ETag, Key};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PutCondition {
    Unconditional,
    /// Create-only (`If-None-Match: *`).
    IfNoneMatch,
    /// Compare-and-swap (`If-Match: <etag>`). On a missing key an adapter may report
    /// `NotFound` (what S3 itself answers) or `PreconditionFailed` (what the HTTP spec and
    /// R2 answer, and what the `object_store` S3 client turns S3's 404 into). Callers must
    /// treat both as "the version I expected is not there".
    IfMatch(ETag),
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum StoreError {
    NotFound,
    /// HTTP 412: the precondition did not hold.
    PreconditionFailed,
    /// HTTP 409: concurrent conditional writes raced. Re-read and retry.
    Conflict,
    /// Transient failure. A write may or may not have been applied (ambiguous).
    Unavailable(String),
    InvalidRange,
    Unsupported(&'static str),
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StoreError::NotFound => write!(f, "not found"),
            StoreError::PreconditionFailed => write!(f, "precondition failed"),
            StoreError::Conflict => write!(f, "conflict"),
            StoreError::Unavailable(m) => write!(f, "unavailable: {m}"),
            StoreError::InvalidRange => write!(f, "invalid range"),
            StoreError::Unsupported(m) => write!(f, "unsupported: {m}"),
        }
    }
}

impl std::error::Error for StoreError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMeta {
    pub key: Key,
    pub size: u64,
    pub etag: ETag,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LatencyProfile {
    InMemory,
    SingleDigitMs,
    TensOfMs,
    HundredsOfMs,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurabilityClass {
    Ephemeral,
    SingleZone,
    MultiZone,
}

/// What an adapter can do. The planner and the engine read this instead of guessing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectStoreCaps {
    /// Supports `If-None-Match: *` on put.
    pub conditional_put: bool,
    /// Supports `If-Match: <etag>` on put.
    pub if_match: bool,
    /// List results are strongly consistent.
    pub strong_list: bool,
    pub range_get: bool,
    pub multipart: bool,
    pub latency: LatencyProfile,
    pub durability: DurabilityClass,
}

pub trait ObjectStore: Send + Sync {
    fn caps(&self) -> ObjectStoreCaps;
    fn get(&self, key: &Key) -> Result<(Vec<u8>, ETag), StoreError>;
    fn get_range(&self, key: &Key, range: Range<u64>) -> Result<Vec<u8>, StoreError>;
    fn put(&self, key: &Key, data: Vec<u8>, cond: PutCondition) -> Result<ETag, StoreError>;
    /// Returns objects whose key starts with `prefix`, sorted by key.
    fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>, StoreError>;
    /// Idempotent: deleting a missing key succeeds.
    fn delete(&self, key: &Key) -> Result<(), StoreError>;
}

impl<T: ObjectStore + ?Sized> ObjectStore for Arc<T> {
    fn caps(&self) -> ObjectStoreCaps {
        (**self).caps()
    }

    fn get(&self, key: &Key) -> Result<(Vec<u8>, ETag), StoreError> {
        (**self).get(key)
    }

    fn get_range(&self, key: &Key, range: Range<u64>) -> Result<Vec<u8>, StoreError> {
        (**self).get_range(key, range)
    }

    fn put(&self, key: &Key, data: Vec<u8>, cond: PutCondition) -> Result<ETag, StoreError> {
        (**self).put(key, data, cond)
    }

    fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>, StoreError> {
        (**self).list(prefix)
    }

    fn delete(&self, key: &Key) -> Result<(), StoreError> {
        (**self).delete(key)
    }
}
