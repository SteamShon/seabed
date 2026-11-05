//! Single-node WAL on top of an ObjectStore.
//!
//! Each entry is one object created with `If-None-Match: *`, so the LSN is claimed atomically.
//! Entry layout: `request_id` (u64, little endian) followed by the payload.
//!
//! The `request_id` makes retries after an ambiguous failure idempotent: if a put outcome is
//! unknown (`Unavailable`), the writer reads the candidate key back and checks whether the entry
//! is its own before trying another LSN.
//!
//! `append` has two separate limits. Losing an LSN to another writer is progress elsewhere, so it
//! is retried at once, up to `with_max_attempts` times. A transient store failure (`Unavailable`
//! from a put, or from a read-back that still fails after the read retries) is retried with
//! backoff under the write retry policy (`with_write_retry_policy`). When that budget runs out,
//! `append` returns the last `Store(Unavailable)` error: the entry may or may not have been
//! appended.
//!
//! Not implemented yet (see the design doc): group commit, writer fencing (epochs), and handling
//! of a request that is still in flight after the writer gave up on it.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use seabed_adapter_objectstore_retry::{RetryPolicy, Retrying};
use seabed_domain::{Lsn, NamespaceKeys};
use seabed_ports::{ObjectStore, PutCondition, StoreError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalError {
    Store(StoreError),
    Unsupported(&'static str),
    TooManyAttempts,
    Gap { expected: Lsn, found: Lsn },
    CorruptEntry(Lsn),
}

impl From<StoreError> for WalError {
    fn from(e: StoreError) -> Self {
        WalError::Store(e)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalEntry {
    pub lsn: Lsn,
    pub request_id: u64,
    pub payload: Vec<u8>,
}

/// The store is always wrapped in `Retrying`, so transient read failures (`get`, `list`) are
/// retried with backoff without the caller having to remember it. Writes are not retried by the
/// wrapper: `append` resolves ambiguous put outcomes itself. Pass a `RetryPolicy` with
/// `with_retry_policy` to change the read budget. `Retrying` does not implement `ObjectStore`, so
/// passing an already-retrying store here does not compile.
///
/// A plain store works:
///
/// ```
/// use std::sync::Arc;
/// use seabed_adapter_objectstore_mem::MemObjectStore;
/// use seabed_domain::{NamespaceId, NamespaceKeys, TenantId};
/// use seabed_engine::wal::Wal;
///
/// let keys = NamespaceKeys::new(TenantId::new("t").unwrap(), NamespaceId::new("n").unwrap());
/// let wal = Wal::new(Arc::new(MemObjectStore::new()), keys).unwrap();
/// ```
///
/// The same call with a store that already retries is rejected by the compiler:
///
/// ```compile_fail
/// use std::sync::Arc;
/// use seabed_adapter_objectstore_mem::MemObjectStore;
/// use seabed_adapter_objectstore_retry::Retrying;
/// use seabed_domain::{NamespaceId, NamespaceKeys, TenantId};
/// use seabed_engine::{wal::Wal, RetryPolicy};
///
/// let keys = NamespaceKeys::new(TenantId::new("t").unwrap(), NamespaceId::new("n").unwrap());
/// let store = Retrying::new(MemObjectStore::new(), RetryPolicy::default());
/// let wal = Wal::new(Arc::new(store), keys).unwrap();
/// ```
pub struct Wal<S: ObjectStore> {
    store: Retrying<Arc<S>>,
    keys: NamespaceKeys,
    /// Hint for the next free LSN. 0 means unknown.
    next: AtomicU64,
    max_attempts: usize,
    write_policy: RetryPolicy,
}

impl<S: ObjectStore> Wal<S> {
    pub fn new(store: Arc<S>, keys: NamespaceKeys) -> Result<Self, WalError> {
        if !store.caps().conditional_put {
            return Err(WalError::Unsupported(
                "object store lacks conditional put (If-None-Match)",
            ));
        }
        Ok(Self {
            store: Retrying::new(store, RetryPolicy::default()),
            keys,
            next: AtomicU64::new(0),
            max_attempts: 10_000,
            write_policy: RetryPolicy::default(),
        })
    }

    /// Limits how many times one `append` tries another LSN after losing one to a concurrent
    /// writer. Transient failures have their own budget (`with_write_retry_policy`).
    pub fn with_max_attempts(mut self, n: usize) -> Self {
        self.max_attempts = n;
        self
    }

    /// Replaces the budget and backoff for transient failures inside `append` (see the module
    /// docs). The default is `RetryPolicy::default()`.
    pub fn with_write_retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.write_policy = policy;
        self
    }

    /// Replaces the read retry policy. `RetryPolicy::immediate(1)` disables read retries.
    pub fn with_retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.store = Retrying::new(self.store.inner().clone(), policy);
        self
    }

    fn refresh_next(&self) -> Result<u64, WalError> {
        let metas = self.store.list(&self.keys.wal_prefix())?;
        let last = metas
            .iter()
            .filter_map(|m| self.keys.parse_wal_lsn(&m.key))
            .max();
        let next = last.map(|l| l.get() + 1).unwrap_or(1);
        self.next.store(next, Ordering::SeqCst);
        Ok(next)
    }

    /// Appends an entry and returns its LSN. Safe to call from many threads.
    pub fn append(&self, request_id: u64, payload: &[u8]) -> Result<Lsn, WalError> {
        let id_bytes = request_id.to_le_bytes();
        let mut entry = Vec::with_capacity(8 + payload.len());
        entry.extend_from_slice(&id_bytes);
        entry.extend_from_slice(payload);

        let mut candidate = match self.next.load(Ordering::SeqCst) {
            0 => self.refresh_next()?,
            n => n,
        };
        // Candidates where an earlier put had an unknown outcome.
        let mut unresolved: Vec<u64> = Vec::new();
        let mut budget = self.write_policy.budget();

        for _ in 0..self.max_attempts {
            // 1. Resolve earlier ambiguous attempts first, so a retry can never duplicate an
            //    entry that actually landed.
            let mut still_unresolved = Vec::new();
            let mut last_unavailable = None;
            for &c in &unresolved {
                match self.store.get(&self.keys.wal_key(Lsn::new(c))) {
                    Ok((bytes, _)) => {
                        if bytes.len() >= 8 && bytes[..8] == id_bytes {
                            self.next.fetch_max(c + 1, Ordering::SeqCst);
                            return Ok(Lsn::new(c));
                        }
                        // Someone else's entry: this candidate is resolved (not ours).
                    }
                    Err(StoreError::NotFound) => {
                        // Not written: resolved.
                    }
                    Err(e @ StoreError::Unavailable(_)) => {
                        still_unresolved.push(c);
                        last_unavailable = Some(e);
                    }
                    Err(e) => return Err(e.into()),
                }
            }
            unresolved = still_unresolved;
            if let Some(e) = last_unavailable {
                budget.retry_or(e)?;
                continue;
            }

            // 2. Try to claim the candidate LSN.
            let key = self.keys.wal_key(Lsn::new(candidate));
            match self
                .store
                .put(&key, entry.clone(), PutCondition::IfNoneMatch)
            {
                Ok(_) => {
                    self.next.fetch_max(candidate + 1, Ordering::SeqCst);
                    return Ok(Lsn::new(candidate));
                }
                Err(StoreError::PreconditionFailed) | Err(StoreError::Conflict) => {
                    // Somebody else took it (or raced with us). Re-read where the log ends.
                    candidate = self.refresh_next()?;
                }
                Err(e @ StoreError::Unavailable(_)) => {
                    unresolved.push(candidate);
                    budget.retry_or(e)?;
                }
                Err(e) => return Err(e.into()),
            }
        }
        Err(WalError::TooManyAttempts)
    }

    /// Reads entries with LSN >= `from`. Fails with `Gap` if the log is not contiguous.
    pub fn read_from(&self, from: Lsn) -> Result<Vec<WalEntry>, WalError> {
        let metas = self.store.list(&self.keys.wal_prefix())?;
        let mut lsns: Vec<Lsn> = metas
            .iter()
            .filter_map(|m| self.keys.parse_wal_lsn(&m.key))
            .filter(|l| *l >= from)
            .collect();
        lsns.sort();

        let mut expected = from;
        let mut out = Vec::with_capacity(lsns.len());
        for lsn in lsns {
            if lsn != expected {
                return Err(WalError::Gap {
                    expected,
                    found: lsn,
                });
            }
            let (bytes, _) = self.store.get(&self.keys.wal_key(lsn))?;
            if bytes.len() < 8 {
                return Err(WalError::CorruptEntry(lsn));
            }
            let mut id = [0u8; 8];
            id.copy_from_slice(&bytes[..8]);
            out.push(WalEntry {
                lsn,
                request_id: u64::from_le_bytes(id),
                payload: bytes[8..].to_vec(),
            });
            expected = lsn.next();
        }
        Ok(out)
    }
}
