//! Versioned manifest commits on top of an ObjectStore.
//!
//! Each version is an immutable object created with `If-None-Match: *`; the object for version N+1
//! can only be created by a writer that saw version N. History is therefore linear.
//! Entry layout: `commit_id` (u64, little endian) followed by an opaque payload
//! (later: segment list, ingest policy, quantizer and tokenizer ids).
//!
//! Finding the latest version: when the store supports `If-Match`, a pointer object
//! (`manifest_pointer_key`) holds the latest committed version and only ever moves forward.
//! The pointer is a hint, not the commit point: a writer can stop after creating version N+1
//! and before advancing the pointer. Readers therefore read the pointer and then probe forward
//! (N+1, N+2, ...) until a version is missing. A read costs about three GETs regardless of how
//! many versions exist, instead of a LIST over all of them. Without `If-Match`, or before the
//! pointer exists, readers fall back to LIST.

use std::sync::Arc;

use seabed_adapter_objectstore_retry::{RetryPolicy, Retrying};
use seabed_domain::{ETag, NamespaceKeys, Version};
use seabed_ports::{ObjectStore, PutCondition, StoreError};

use crate::manifest_content::{ContentError, ManifestContent};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManifestError {
    Store(StoreError),
    Unsupported(&'static str),
    /// Another writer committed first (or `expected` was stale). Reload and retry.
    CommitConflict,
    Corrupt(Version),
    /// The pointer object does not hold an 8-byte version.
    CorruptPointer,
    /// The payload is not valid `ManifestContent` for this namespace.
    Content(ContentError),
    TooManyAttempts,
}

impl From<StoreError> for ManifestError {
    fn from(e: StoreError) -> Self {
        ManifestError::Store(e)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Manifest {
    pub version: Version,
    pub commit_id: u64,
    pub payload: Vec<u8>,
}

/// The store is always wrapped in `Retrying`, so transient read failures (`get`, `list`) are
/// retried with backoff without the caller having to remember it. Writes are not retried by the
/// wrapper: `commit` resolves ambiguous put outcomes itself. Pass a `RetryPolicy` with
/// `with_retry_policy` to change the read budget. `Retrying` does not implement `ObjectStore`, so
/// passing an already-retrying store here does not compile.
pub struct ManifestStore<S: ObjectStore> {
    store: Retrying<Arc<S>>,
    keys: NamespaceKeys,
    max_attempts: usize,
}

impl<S: ObjectStore> ManifestStore<S> {
    pub fn new(store: Arc<S>, keys: NamespaceKeys) -> Result<Self, ManifestError> {
        if !store.caps().conditional_put {
            return Err(ManifestError::Unsupported(
                "object store lacks conditional put (If-None-Match)",
            ));
        }
        Ok(Self {
            store: Retrying::new(store, RetryPolicy::default()),
            keys,
            max_attempts: 1_000,
        })
    }

    /// Replaces the read retry policy. `RetryPolicy::immediate(1)` disables read retries.
    pub fn with_retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.store = Retrying::new(self.store.inner().clone(), policy);
        self
    }

    fn parse(version: Version, bytes: &[u8]) -> Result<Manifest, ManifestError> {
        if bytes.len() < 8 {
            return Err(ManifestError::Corrupt(version));
        }
        let mut id = [0u8; 8];
        id.copy_from_slice(&bytes[..8]);
        Ok(Manifest {
            version,
            commit_id: u64::from_le_bytes(id),
            payload: bytes[8..].to_vec(),
        })
    }

    pub fn load(&self, version: Version) -> Result<Option<Manifest>, ManifestError> {
        match self.store.get(&self.keys.manifest_key(version)) {
            Ok((bytes, _)) => Ok(Some(Self::parse(version, &bytes)?)),
            Err(StoreError::NotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Latest version decoded as `ManifestContent` and checked against this namespace.
    pub fn load_latest_content(&self) -> Result<Option<(Version, ManifestContent)>, ManifestError> {
        match self.load_latest()? {
            Some(m) => {
                let c = ManifestContent::decode(&m.payload, &self.keys)
                    .map_err(ManifestError::Content)?;
                Ok(Some((m.version, c)))
            }
            None => Ok(None),
        }
    }

    /// Commits typed content. It is validated the same way readers validate it, so a manifest
    /// that readers would reject (wrong namespace, duplicate segment) is never written.
    pub fn commit_content(
        &self,
        expected: Option<Version>,
        commit_id: u64,
        content: &ManifestContent,
    ) -> Result<Version, ManifestError> {
        let bytes = content.encode();
        ManifestContent::decode(&bytes, &self.keys).map_err(ManifestError::Content)?;
        self.commit(expected, commit_id, &bytes)
    }

    pub fn load_latest(&self) -> Result<Option<Manifest>, ManifestError> {
        if !self.store.caps().if_match {
            return self.load_latest_by_list();
        }
        let Some((hint, _)) = self.read_pointer()? else {
            // No pointer yet: empty namespace, or versions written before the pointer existed.
            return self.load_latest_by_list();
        };
        let Some(mut latest) = self.load(hint)? else {
            // Versions are never deleted yet, so this means the pointer is ahead of reality.
            return Err(ManifestError::CorruptPointer);
        };
        // The pointer may lag behind the newest version; walk forward to the end.
        while let Some(newer) = self.load(latest.version.next())? {
            latest = newer;
        }
        Ok(Some(latest))
    }

    fn read_pointer(&self) -> Result<Option<(Version, ETag)>, ManifestError> {
        match self.store.get(&self.keys.manifest_pointer_key()) {
            Ok((bytes, etag)) => {
                let raw: [u8; 8] = bytes
                    .as_slice()
                    .try_into()
                    .map_err(|_| ManifestError::CorruptPointer)?;
                Ok(Some((Version::new(u64::from_le_bytes(raw)), etag)))
            }
            Err(StoreError::NotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Moves the pointer forward to `to`. Never moves it backward: a slower writer that finishes
    /// after a faster one leaves the pointer where it is.
    fn advance_pointer(&self, to: Version) -> Result<(), ManifestError> {
        let key = self.keys.manifest_pointer_key();
        let value = to.get().to_le_bytes().to_vec();
        for _ in 0..self.max_attempts {
            let cond = match self.read_pointer()? {
                Some((cur, _)) if cur >= to => return Ok(()),
                Some((_, etag)) => PutCondition::IfMatch(etag),
                None => PutCondition::IfNoneMatch,
            };
            match self.store.put(&key, value.clone(), cond) {
                Ok(_) => return Ok(()),
                // Lost a race, or the outcome is unknown: re-read and decide again.
                Err(StoreError::PreconditionFailed)
                | Err(StoreError::Conflict)
                | Err(StoreError::Unavailable(_)) => {}
                Err(e) => return Err(e.into()),
            }
        }
        Err(ManifestError::TooManyAttempts)
    }

    fn load_latest_by_list(&self) -> Result<Option<Manifest>, ManifestError> {
        let metas = self.store.list(&self.keys.manifest_prefix())?;
        let latest = metas
            .iter()
            .filter_map(|m| self.keys.parse_manifest_version(&m.key))
            .max();
        match latest {
            Some(v) => self.load(v),
            None => Ok(None),
        }
    }

    /// Commits `payload` as the version after `expected` (`None` means "no manifest exists yet").
    /// `commit_id` must be unique per commit attempt; it lets the writer recognise its own commit
    /// after an ambiguous failure.
    ///
    /// The commit point is the creation of the version object. Advancing the pointer afterwards
    /// is best effort: if it fails, the commit still stands and readers find the version by
    /// probing forward.
    pub fn commit(
        &self,
        expected: Option<Version>,
        commit_id: u64,
        payload: &[u8],
    ) -> Result<Version, ManifestError> {
        let version = self.create_version(expected, commit_id, payload)?;
        if self.store.caps().if_match {
            // Ignoring the error is deliberate; see the doc comment above.
            let _ = self.advance_pointer(version);
        }
        Ok(version)
    }

    fn create_version(
        &self,
        expected: Option<Version>,
        commit_id: u64,
        payload: &[u8],
    ) -> Result<Version, ManifestError> {
        let next = match expected {
            Some(v) => v.next(),
            None => Version::new(1),
        };
        let id_bytes = commit_id.to_le_bytes();
        let mut entry = Vec::with_capacity(8 + payload.len());
        entry.extend_from_slice(&id_bytes);
        entry.extend_from_slice(payload);

        let key = self.keys.manifest_key(next);
        // True once an attempt had an unknown outcome: that attempt may have landed.
        let mut maybe_landed = false;
        for _ in 0..self.max_attempts {
            let outcome = self
                .store
                .put(&key, entry.clone(), PutCondition::IfNoneMatch);
            let ambiguous = matches!(outcome, Err(StoreError::Unavailable(_)));
            let rejected = matches!(
                outcome,
                Err(StoreError::PreconditionFailed) | Err(StoreError::Conflict)
            );
            match outcome {
                Ok(_) => return Ok(next),
                Err(e) if !ambiguous && !rejected => return Err(e.into()),
                Err(_) => {}
            }
            if rejected && !maybe_landed {
                return Err(ManifestError::CommitConflict);
            }
            if ambiguous {
                maybe_landed = true;
            }
            // Either this attempt or an earlier one has an unknown outcome:
            // find out whether the object at `key` is ours.
            match self.store.get(&key) {
                Ok((bytes, _)) => {
                    return if bytes.len() >= 8 && bytes[..8] == id_bytes {
                        Ok(next)
                    } else {
                        Err(ManifestError::CommitConflict)
                    };
                }
                Err(StoreError::NotFound) | Err(StoreError::Unavailable(_)) => {}
                Err(e) => return Err(e.into()),
            }
        }
        Err(ManifestError::TooManyAttempts)
    }
}
