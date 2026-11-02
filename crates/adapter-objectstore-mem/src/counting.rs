use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};

use seabed_domain::{ETag, Key};
use seabed_ports::{ObjectMeta, ObjectStore, ObjectStoreCaps, PutCondition, StoreError};

/// Snapshot of request counters. These feed the cost model (S3 request counts and bytes),
/// and can be collected on any local emulator too.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RequestStats {
    pub gets: u64,
    pub puts: u64,
    pub lists: u64,
    pub deletes: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
}

/// Decorator that counts requests. Failed attempts are counted too, because they are billed.
pub struct CountingObjectStore<S> {
    inner: S,
    gets: AtomicU64,
    puts: AtomicU64,
    lists: AtomicU64,
    deletes: AtomicU64,
    bytes_read: AtomicU64,
    bytes_written: AtomicU64,
}

impl<S: ObjectStore> CountingObjectStore<S> {
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            gets: AtomicU64::new(0),
            puts: AtomicU64::new(0),
            lists: AtomicU64::new(0),
            deletes: AtomicU64::new(0),
            bytes_read: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
        }
    }

    pub fn inner(&self) -> &S {
        &self.inner
    }

    pub fn stats(&self) -> RequestStats {
        RequestStats {
            gets: self.gets.load(Ordering::Relaxed),
            puts: self.puts.load(Ordering::Relaxed),
            lists: self.lists.load(Ordering::Relaxed),
            deletes: self.deletes.load(Ordering::Relaxed),
            bytes_read: self.bytes_read.load(Ordering::Relaxed),
            bytes_written: self.bytes_written.load(Ordering::Relaxed),
        }
    }
}

impl<S: ObjectStore> ObjectStore for CountingObjectStore<S> {
    fn caps(&self) -> ObjectStoreCaps {
        self.inner.caps()
    }

    fn get(&self, key: &Key) -> Result<(Vec<u8>, ETag), StoreError> {
        self.gets.fetch_add(1, Ordering::Relaxed);
        let r = self.inner.get(key);
        if let Ok((data, _)) = &r {
            self.bytes_read
                .fetch_add(data.len() as u64, Ordering::Relaxed);
        }
        r
    }

    fn get_range(&self, key: &Key, range: Range<u64>) -> Result<Vec<u8>, StoreError> {
        self.gets.fetch_add(1, Ordering::Relaxed);
        let r = self.inner.get_range(key, range);
        if let Ok(data) = &r {
            self.bytes_read
                .fetch_add(data.len() as u64, Ordering::Relaxed);
        }
        r
    }

    fn put(&self, key: &Key, data: Vec<u8>, cond: PutCondition) -> Result<ETag, StoreError> {
        self.puts.fetch_add(1, Ordering::Relaxed);
        self.bytes_written
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        self.inner.put(key, data, cond)
    }

    fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>, StoreError> {
        self.lists.fetch_add(1, Ordering::Relaxed);
        self.inner.list(prefix)
    }

    fn delete(&self, key: &Key) -> Result<(), StoreError> {
        self.deletes.fetch_add(1, Ordering::Relaxed);
        self.inner.delete(key)
    }
}
