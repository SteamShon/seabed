//! Versioned manifest commits on top of an ObjectStore.
//!
//! Each version is an immutable object created with `If-None-Match: *`; the object for version N+1
//! can only be created by a writer that saw version N. History is therefore linear.
//! Entry layout: `commit_id` (u64, little endian) followed by an opaque payload
//! (later: segment list, ingest policy, quantizer and tokenizer ids).
//!
//! Variant not implemented yet: a single CURRENT pointer updated with `If-Match` (needs `caps().if_match`).

use std::sync::Arc;

use seabed_adapter_objectstore_retry::{RetryPolicy, Retrying};
use seabed_domain::{NamespaceKeys, Version};
use seabed_ports::{ObjectStore, PutCondition, StoreError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManifestError {
    Store(StoreError),
    Unsupported(&'static str),
    /// Another writer committed first (or `expected` was stale). Reload and retry.
    CommitConflict,
    Corrupt(Version),
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

    pub fn load_latest(&self) -> Result<Option<Manifest>, ManifestError> {
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
    pub fn commit(
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
