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
//! Retrying such an append with the same `request_id` and payload on the same `Wal` does not
//! duplicate it: the `Wal` remembers the LSNs whose outcome was unknown (for up to
//! `MAX_PENDING` request ids) and reads them back before trying a new one. If the entry did land,
//! the retry returns its LSN. The memory is per `Wal` instance: a retry from another process or
//! another `Wal`, or after a restart, may still append the entry twice.
//!
//! Not implemented yet (see the design doc): group commit, writer fencing (epochs), durable
//! deduplication of retries across processes, and handling of a request that is still in flight
//! after the writer gave up on it (its put may land after the read-back found nothing).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

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
    /// Appends that gave up with the outcome unknown, by request id: the entry and the LSNs it
    /// may have landed at.
    pending: Mutex<HashMap<u64, Pending>>,
}

/// How many unknown-outcome appends a `Wal` remembers. Beyond that, an arbitrary one is
/// forgotten (its retry may then append twice).
pub const MAX_PENDING: usize = 1024;

struct Pending {
    entry: Vec<u8>,
    candidates: Vec<u64>,
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
            pending: Mutex::new(HashMap::new()),
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
    ///
    /// If an earlier `append` on this `Wal` with the same `request_id` and payload gave up with
    /// the outcome unknown, the LSNs it may have landed at are checked first (see the module
    /// docs).
    pub fn append(&self, request_id: u64, payload: &[u8]) -> Result<Lsn, WalError> {
        let mut entry = Vec::with_capacity(8 + payload.len());
        entry.extend_from_slice(&request_id.to_le_bytes());
        entry.extend_from_slice(payload);

        // Candidates where an earlier put had an unknown outcome, from an earlier call too.
        let mut unresolved = self.take_pending(request_id, &entry);
        let result = self.append_entry(&entry, &mut unresolved);
        if result.is_err() && !unresolved.is_empty() {
            self.remember_pending(request_id, entry, unresolved);
        }
        result
    }

    fn take_pending(&self, request_id: u64, entry: &[u8]) -> Vec<u64> {
        let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        match pending.remove(&request_id) {
            Some(p) if p.entry == entry => p.candidates,
            // Same id, other payload: a different request. The old outcome stays unknown.
            _ => Vec::new(),
        }
    }

    fn remember_pending(&self, request_id: u64, entry: Vec<u8>, candidates: Vec<u64>) {
        let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        if pending.len() >= MAX_PENDING && !pending.contains_key(&request_id) {
            if let Some(&evict) = pending.keys().next() {
                pending.remove(&evict);
            }
        }
        pending.insert(request_id, Pending { entry, candidates });
    }

    fn append_entry(&self, entry: &[u8], unresolved: &mut Vec<u64>) -> Result<Lsn, WalError> {
        let id_bytes = &entry[..8];
        let mut candidate = match self.next.load(Ordering::SeqCst) {
            0 => self.refresh_next()?,
            n => n,
        };
        let mut budget = self.write_policy.budget();

        for _ in 0..self.max_attempts {
            // 1. Resolve earlier ambiguous attempts first, so a retry can never duplicate an
            //    entry that actually landed.
            let mut still_unresolved = Vec::new();
            let mut last_unavailable = None;
            for &c in unresolved.iter() {
                match self.store.get(&self.keys.wal_key(Lsn::new(c))) {
                    Ok((bytes, _)) => {
                        if bytes.len() >= 8 && bytes[..8] == *id_bytes {
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
            *unresolved = still_unresolved;
            if let Some(e) = last_unavailable {
                budget.retry_or(e)?;
                continue;
            }

            // 2. Try to claim the candidate LSN.
            let key = self.keys.wal_key(Lsn::new(candidate));
            match self
                .store
                .put(&key, entry.to_vec(), PutCondition::IfNoneMatch)
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
