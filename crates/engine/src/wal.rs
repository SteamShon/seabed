//! Single-node WAL on top of an ObjectStore.
//!
//! Each entry is one object created with `If-None-Match: *`, so the LSN is claimed atomically.
//! Entry layout: `request_id` (u64, little endian) followed by the payload.
//!
//! The `request_id` makes retries after an ambiguous failure idempotent: if a put outcome is
//! unknown (`Unavailable`), the writer reads the candidate key back and checks whether the entry
//! is its own before trying another LSN.
//!
//! Not implemented yet (see the design doc): group commit, writer fencing (epochs), and handling
//! of a request that is still in flight after the writer gave up on it.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

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

pub struct Wal<S: ObjectStore> {
    store: Arc<S>,
    keys: NamespaceKeys,
    /// Hint for the next free LSN. 0 means unknown.
    next: AtomicU64,
    max_attempts: usize,
}

impl<S: ObjectStore> Wal<S> {
    pub fn new(store: Arc<S>, keys: NamespaceKeys) -> Result<Self, WalError> {
        if !store.caps().conditional_put {
            return Err(WalError::Unsupported(
                "object store lacks conditional put (If-None-Match)",
            ));
        }
        Ok(Self {
            store,
            keys,
            next: AtomicU64::new(0),
            max_attempts: 10_000,
        })
    }

    pub fn with_max_attempts(mut self, n: usize) -> Self {
        self.max_attempts = n;
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

        for _ in 0..self.max_attempts {
            // 1. Resolve earlier ambiguous attempts first, so a retry can never duplicate an
            //    entry that actually landed.
            let mut still_unresolved = Vec::new();
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
                    Err(StoreError::Unavailable(_)) => still_unresolved.push(c),
                    Err(e) => return Err(e.into()),
                }
            }
            unresolved = still_unresolved;
            if !unresolved.is_empty() {
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
                Err(StoreError::Unavailable(_)) => unresolved.push(candidate),
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
