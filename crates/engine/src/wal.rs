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
//! A `request_id` names one logical request: the caller picks a fresh id for each new request
//! and reuses it only to retry that request with the same payload. The same id and payload are
//! treated as the same request.
//!
//! Retrying such an append with the same `request_id` and payload on the same `Wal` does not
//! duplicate it: the `Wal` remembers the LSNs whose outcome was unknown and reads them back
//! before trying a new one. A candidate counts as this append only if it holds exactly this
//! entry (request id and payload), so another writer's entry under the same request id is never
//! taken for ours. Once such a retry finds or makes its entry, the LSN is remembered too, so a
//! later retry of the same request returns it without touching the store. Appends with the same
//! request id run one at a time on a `Wal`, so concurrent retries of one request also append
//! it once.
//!
//! The memory is per `Wal` instance and holds at most `MAX_PENDING` requests (a digest of the
//! entry and a few LSNs each; the oldest is forgotten first). A retry from another process or
//! another `Wal`, after a restart, or of a forgotten request may still append the entry twice.
//!
//! Not implemented yet (see the design doc): group commit, writer fencing (epochs), durable
//! deduplication of retries across processes, and handling of a request that is still in flight
//! after the writer gave up on it (its put may land after the read-back found nothing). Fencing
//! must also revisit the retry memory: a remembered landed LSN is answered without reading the
//! store, which stays right only while an LSN keeps one meaning.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};

use seabed_adapter_objectstore_retry::{RetryPolicy, Retrying};
use seabed_domain::{Lsn, NamespaceKeys};
use seabed_ports::{ObjectStore, PutCondition, StoreError};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
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
    /// Retry memory and the request ids with an `append` in progress.
    retries: Mutex<Retries>,
    /// Signalled whenever an `append` finishes, for appends waiting on the same request id.
    retry_done: Condvar,
}

/// How many requests whose append once ended with the outcome unknown a `Wal` remembers. Beyond
/// that, the oldest is forgotten (its retry may then append twice).
pub const MAX_PENDING: usize = 1024;

#[derive(Default)]
struct Retries {
    pending: PendingMap,
    in_progress: HashSet<u64>,
}

/// What a `Wal` knows about a request whose append once ended with the outcome unknown.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Outcome {
    /// Still unknown: the LSNs the entry may have landed at.
    Unknown(Vec<u64>),
    /// A later retry found or made the entry here.
    Landed(Lsn),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Pending {
    /// SHA-256 of the entry (request id and payload): a retry with another payload is another
    /// request.
    digest: [u8; 32],
    outcome: Outcome,
}

/// Request id to `Pending`, holding at most `MAX_PENDING`, forgetting the least recently set.
#[derive(Default)]
struct PendingMap {
    /// Request id to (generation of its last `set`, what is known).
    map: HashMap<u64, (u64, Pending)>,
    /// Generation to request id, one per entry of `map`: the first is the least recently set.
    order: BTreeMap<u64, u64>,
    generation: u64,
}

impl PendingMap {
    fn get(&self, id: u64) -> Option<&Pending> {
        self.map.get(&id).map(|(_, p)| p)
    }

    fn set(&mut self, id: u64, pending: Pending) {
        self.generation += 1;
        if let Some((old, _)) = self.map.insert(id, (self.generation, pending)) {
            self.order.remove(&old);
        }
        self.order.insert(self.generation, id);
        while self.map.len() > MAX_PENDING {
            let Some((_, oldest)) = self.order.pop_first() else {
                break;
            };
            self.map.remove(&oldest);
        }
    }

    fn remove(&mut self, id: u64) {
        if let Some((generation, _)) = self.map.remove(&id) {
            self.order.remove(&generation);
        }
    }

    fn len(&self) -> usize {
        self.map.len()
    }
}

/// What to do with the retry memory of a request once its `append` returns.
#[derive(Debug, PartialEq, Eq)]
enum Record {
    Set(Outcome),
    Remove,
    Nothing,
}

/// Decides how an `append` updates the retry memory. `retrying`: the request was remembered when
/// the call started. `unresolved`: the LSNs whose outcome is still unknown when it returned.
fn record(retrying: bool, result: &Result<Lsn, WalError>, unresolved: &[u64]) -> Record {
    match result {
        // A retry found or made its entry: answer later retries with this LSN.
        Ok(lsn) if retrying => Record::Set(Outcome::Landed(*lsn)),
        // A first attempt whose outcome is known: nothing to remember.
        Ok(_) => Record::Nothing,
        Err(_) if !unresolved.is_empty() => Record::Set(Outcome::Unknown(unresolved.to_vec())),
        // Every earlier candidate turned out not to be ours: nothing is unknown any more.
        Err(_) if retrying => Record::Remove,
        Err(_) => Record::Nothing,
    }
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
            retries: Mutex::new(Retries::default()),
            retry_done: Condvar::new(),
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
    /// If an earlier `append` on this `Wal` with the same `request_id` and payload ended with the
    /// outcome unknown, the LSNs it may have landed at are checked first (see the module docs).
    pub fn append(&self, request_id: u64, payload: &[u8]) -> Result<Lsn, WalError> {
        let mut entry = Vec::with_capacity(8 + payload.len());
        entry.extend_from_slice(&request_id.to_le_bytes());
        entry.extend_from_slice(payload);
        let digest: [u8; 32] = Sha256::digest(&entry).into();

        // One append per request id at a time, so a concurrent retry sees this one's outcome.
        let mut retries = self.lock_retries();
        while retries.in_progress.contains(&request_id) {
            retries = self
                .retry_done
                .wait(retries)
                .unwrap_or_else(|e| e.into_inner());
        }
        let earlier = match retries.pending.get(request_id) {
            Some(p) if p.digest == digest => Some(p.outcome.clone()),
            Some(_) => {
                // Same id, other payload: a different request. The old outcome is dropped.
                retries.pending.remove(request_id);
                None
            }
            None => None,
        };
        if let Some(Outcome::Landed(lsn)) = earlier {
            return Ok(lsn);
        }
        retries.in_progress.insert(request_id);
        drop(retries);
        let _done = InProgress {
            wal: self,
            request_id,
        };

        let retrying = earlier.is_some();
        let mut unresolved = match earlier {
            Some(Outcome::Unknown(candidates)) => candidates,
            _ => Vec::new(),
        };
        let result = self.append_entry(&entry, &mut unresolved);

        let mut retries = self.lock_retries();
        match record(retrying, &result, &unresolved) {
            Record::Set(outcome) => retries.pending.set(request_id, Pending { digest, outcome }),
            Record::Remove => retries.pending.remove(request_id),
            Record::Nothing => {}
        }
        // Release the lock before `_done` clears the in-progress mark, which locks it again.
        drop(retries);
        result
    }

    /// How many requests this `Wal` currently remembers (see the module docs). For diagnostics
    /// and tests.
    pub fn pending_len(&self) -> usize {
        self.lock_retries().pending.len()
    }

    fn lock_retries(&self) -> MutexGuard<'_, Retries> {
        // Only map operations run under this lock, so a panic there cannot leave it half
        // updated: recovering from poisoning is safe.
        self.retries.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn append_entry(&self, entry: &[u8], unresolved: &mut Vec<u64>) -> Result<Lsn, WalError> {
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
                        // Only this exact entry is ours: another writer may use the same
                        // request id.
                        if bytes == entry {
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

/// Clears a request id's in-progress mark when its `append` returns (or panics), and wakes
/// appends waiting on it.
struct InProgress<'a, S: ObjectStore> {
    wal: &'a Wal<S>,
    request_id: u64,
}

impl<S: ObjectStore> Drop for InProgress<'_, S> {
    fn drop(&mut self) {
        self.wal.lock_retries().in_progress.remove(&self.request_id);
        self.wal.retry_done.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unknown(lsn: u64) -> Pending {
        Pending {
            digest: [0; 32],
            outcome: Outcome::Unknown(vec![lsn]),
        }
    }

    #[test]
    fn pending_map_forgets_the_least_recently_set_first() {
        let mut m = PendingMap::default();
        for id in 0..MAX_PENDING as u64 {
            m.set(id, unknown(id));
        }
        // Touch 0 again: now 1 is the oldest.
        m.set(0, unknown(0));
        m.set(9999, unknown(9999));
        assert_eq!(m.len(), MAX_PENDING);
        assert!(m.get(0).is_some(), "recently set");
        assert!(m.get(1).is_none(), "oldest forgotten");
        assert!(m.get(9999).is_some());
    }

    #[test]
    fn pending_map_order_tracks_the_map_exactly() {
        let mut m = PendingMap::default();
        for round in 0..10 * MAX_PENDING as u64 {
            m.set(round % 3, unknown(round));
            if round % 2 == 0 {
                m.remove(round % 3);
            }
            assert_eq!(m.order.len(), m.map.len());
        }
    }

    #[test]
    fn record_decides_what_to_remember() {
        let ok = Ok(Lsn::new(5));
        let err = Err(WalError::TooManyAttempts);
        assert_eq!(
            record(true, &ok, &[]),
            Record::Set(Outcome::Landed(Lsn::new(5)))
        );
        assert_eq!(
            record(false, &ok, &[3]),
            Record::Nothing,
            "known in the call"
        );
        assert_eq!(
            record(false, &err, &[3]),
            Record::Set(Outcome::Unknown(vec![3]))
        );
        assert_eq!(
            record(true, &err, &[3, 4]),
            Record::Set(Outcome::Unknown(vec![3, 4]))
        );
        assert_eq!(record(true, &err, &[]), Record::Remove, "no longer unknown");
        assert_eq!(record(false, &err, &[]), Record::Nothing);
    }
}
