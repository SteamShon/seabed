//! `ObjectStore` adapter for S3 and S3-compatible stores, built on the `object_store` crate.
//!
//! The port is synchronous and the client is async: the adapter owns a small tokio runtime and
//! blocks on it for each call. Do not call it from inside an async runtime.
//!
//! Mapping to the port:
//! - `If-None-Match: *` is `PutMode::Create` and `If-Match` is `PutMode::Update` with the etag.
//!   A refused create (412, or 409 for a concurrent conditional write) is `PreconditionFailed`;
//!   a refused update is `PreconditionFailed` (412) or `Conflict` (409). The WAL append, the
//!   SoT write, the manifest commit and the pointer advance re-read the key after either and
//!   handle both alike. `SegmentStore::write` does not re-read: a refused segment create, 412
//!   or 409, becomes `SegmentError::AlreadyExists`.
//! - The client's own retries are off: transient failures surface as `Unavailable`, and the
//!   engine's read retries and write budgets decide what to retry. A request that may have
//!   reached the store (a timeout, a 5xx) is always reported as `Unavailable`, never as a
//!   refusal.
//! - S3 etags are content hashes (MD5 for single-part uploads): writing the same bytes again
//!   gives the same etag. The engine only compares etags it read back, and the manifest pointer
//!   only moves forward, so this cannot cause an ABA on `If-Match`.
//! - `get_range` costs a HEAD (to report a missing key and a range past the end) and a GET
//!   that carries the HEAD's etag in `If-Match`. If the object changed in between, the GET is
//!   refused and the read reports `Unavailable`, so the caller retries it.
//! - `list` sends the prefix as is (no trailing delimiter), follows every page and sorts the
//!   result by key.
//!
//! Every key is stored under an optional root prefix, so one bucket can hold several
//! independent stores (tests give each store a fresh root).

use std::ops::Range;
use std::sync::Arc;

use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::list::{PaginatedListOptions, PaginatedListStore};
use object_store::path::Path;
use object_store::{
    GetOptions, GetRange, ObjectStore as _, ObjectStoreExt as _, PutMode, PutOptions, PutPayload,
    RetryConfig, UpdateVersion,
};
use seabed_domain::{ETag, Key};
use seabed_ports::{
    DurabilityClass, LatencyProfile, ObjectMeta, ObjectStore, ObjectStoreCaps, PutCondition,
    StoreError,
};

/// An S3 bucket (optionally a root prefix inside it) as an `ObjectStore`.
pub struct S3ObjectStore {
    client: Arc<AmazonS3>,
    runtime: tokio::runtime::Runtime,
    /// Prepended to every key; empty or ending in `/`.
    root: String,
    durability: DurabilityClass,
    /// Keys per `list` page; `None` leaves it to the store (1000 on S3).
    list_page_size: Option<usize>,
}

impl S3ObjectStore {
    /// Builds the store from an `AmazonS3Builder` (bucket, region, credentials, endpoint).
    /// The client's retries are switched off; see the module docs. `durability` is what the
    /// adapter declares in its caps: `MultiZone` for AWS S3 standard storage, `Ephemeral` for
    /// a local emulator.
    pub fn new(builder: AmazonS3Builder, durability: DurabilityClass) -> Result<Self, StoreError> {
        let client = builder
            .with_retry(RetryConfig {
                max_retries: 0,
                ..RetryConfig::default()
            })
            .build()
            .map_err(|e| StoreError::Unavailable(format!("building the S3 client: {e}")))?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .map_err(|e| StoreError::Unavailable(format!("starting the S3 runtime: {e}")))?;
        Ok(Self {
            client: Arc::new(client),
            runtime,
            root: String::new(),
            durability,
            list_page_size: None,
        })
    }

    /// Stores every key under `root/` inside the bucket.
    pub fn with_root(mut self, root: &str) -> Self {
        let root = root.trim_matches('/');
        self.root = if root.is_empty() {
            String::new()
        } else {
            format!("{root}/")
        };
        self
    }

    /// Asks for at most `n` keys per `list` page. Only tests need this: it makes a short
    /// listing span several pages.
    pub fn with_list_page_size(mut self, n: usize) -> Self {
        self.list_page_size = Some(n);
        self
    }

    fn path(&self, key: &Key) -> Result<Path, StoreError> {
        let full = format!("{}{}", self.root, key.as_str());
        Path::parse(&full).map_err(|_| StoreError::Unsupported("key is not a valid S3 path"))
    }
}

/// Maps a client error. `refused` is what a refused conditional write means in this call.
fn store_error(e: object_store::Error, refused_conflict: StoreError) -> StoreError {
    use object_store::Error as E;
    match e {
        E::NotFound { .. } => StoreError::NotFound,
        E::Precondition { .. } => StoreError::PreconditionFailed,
        // object_store reports both a refused create (412) and a conditional-write race (409)
        // as `AlreadyExists`.
        E::AlreadyExists { .. } => refused_conflict,
        E::NotSupported { .. } | E::NotImplemented { .. } => {
            StoreError::Unsupported("operation not supported by this S3 store")
        }
        other => StoreError::Unavailable(other.to_string()),
    }
}

fn plain(e: object_store::Error) -> StoreError {
    store_error(e, StoreError::Conflict)
}

impl ObjectStore for S3ObjectStore {
    fn caps(&self) -> ObjectStoreCaps {
        ObjectStoreCaps {
            conditional_put: true,
            if_match: true,
            strong_list: true,
            range_get: true,
            multipart: false,
            latency: LatencyProfile::TensOfMs,
            durability: self.durability,
        }
    }

    fn get(&self, key: &Key) -> Result<(Vec<u8>, ETag), StoreError> {
        let path = self.path(key)?;
        self.runtime.block_on(async {
            let r = self
                .client
                .get_opts(&path, GetOptions::default())
                .await
                .map_err(plain)?;
            let etag = r.meta.e_tag.clone().unwrap_or_default();
            let bytes = r.bytes().await.map_err(plain)?;
            Ok((bytes.to_vec(), ETag::new(etag)))
        })
    }

    fn get_range(&self, key: &Key, range: Range<u64>) -> Result<Vec<u8>, StoreError> {
        if range.start > range.end {
            return Err(StoreError::InvalidRange);
        }
        let path = self.path(key)?;
        self.runtime.block_on(async {
            // HEAD first: an empty range still has to report a missing key, and a range past
            // the end must fail even though S3 would return the bytes that do exist.
            let meta = self.client.head(&path).await.map_err(plain)?;
            if range.end > meta.size {
                return Err(StoreError::InvalidRange);
            }
            if range.start == range.end {
                return Ok(Vec::new());
            }
            // The GET only succeeds on the object the HEAD saw.
            let options = GetOptions {
                range: Some(GetRange::Bounded(range.clone())),
                if_match: meta.e_tag.clone(),
                ..GetOptions::default()
            };
            let changed = || StoreError::Unavailable("object changed during a range read".into());
            let r = match self.client.get_opts(&path, options).await {
                Ok(r) => r,
                Err(object_store::Error::Precondition { .. }) => return Err(changed()),
                Err(e) => return Err(plain(e)),
            };
            let bytes = r.bytes().await.map_err(plain)?;
            if bytes.len() as u64 != range.end - range.start {
                // A store that ignored the If-Match and served a shorter object.
                return Err(changed());
            }
            Ok(bytes.to_vec())
        })
    }

    fn put(&self, key: &Key, data: Vec<u8>, cond: PutCondition) -> Result<ETag, StoreError> {
        let path = self.path(key)?;
        let (mode, refused) = match cond {
            PutCondition::Unconditional => (PutMode::Overwrite, StoreError::Conflict),
            PutCondition::IfNoneMatch => (PutMode::Create, StoreError::PreconditionFailed),
            PutCondition::IfMatch(etag) => (
                PutMode::Update(UpdateVersion {
                    e_tag: Some(etag.as_str().to_string()),
                    version: None,
                }),
                StoreError::Conflict,
            ),
        };
        let options = PutOptions {
            mode,
            ..PutOptions::default()
        };
        self.runtime.block_on(async {
            let r = self
                .client
                .put_opts(&path, PutPayload::from(data), options)
                .await
                .map_err(|e| store_error(e, refused))?;
            Ok(ETag::new(r.e_tag.unwrap_or_default()))
        })
    }

    fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>, StoreError> {
        let full_prefix = format!("{}{}", self.root, prefix);
        self.runtime.block_on(async {
            let mut out = Vec::new();
            let mut page_token = None;
            loop {
                let page = self
                    .client
                    .list_paginated(
                        Some(&full_prefix),
                        PaginatedListOptions {
                            page_token: page_token.take(),
                            max_keys: self.list_page_size,
                            ..PaginatedListOptions::default()
                        },
                    )
                    .await
                    .map_err(plain)?;
                for m in page.result.objects {
                    let Some(rest) = m.location.as_ref().strip_prefix(&self.root) else {
                        continue;
                    };
                    out.push(ObjectMeta {
                        key: Key::new(rest),
                        size: m.size,
                        etag: ETag::new(m.e_tag.unwrap_or_default()),
                    });
                }
                match page.page_token {
                    Some(t) => page_token = Some(t),
                    None => break,
                }
            }
            out.sort_by(|a, b| a.key.as_str().cmp(b.key.as_str()));
            Ok(out)
        })
    }

    fn delete(&self, key: &Key) -> Result<(), StoreError> {
        let path = self.path(key)?;
        self.runtime.block_on(async {
            match self.client.delete(&path).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
                Err(e) => Err(plain(e)),
            }
        })
    }
}
