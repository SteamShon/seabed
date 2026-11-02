use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::Mutex;

use seabed_domain::{ETag, Key};
use seabed_ports::{
    DurabilityClass, LatencyProfile, ObjectMeta, ObjectStore, ObjectStoreCaps, PutCondition,
    StoreError,
};

use crate::lock;

struct Object {
    data: Vec<u8>,
    etag: ETag,
}

#[derive(Default)]
struct Inner {
    objects: BTreeMap<String, Object>,
    etag_counter: u64,
}

/// Linearizable in-memory store with S3-like conditional write semantics.
pub struct MemObjectStore {
    inner: Mutex<Inner>,
}

impl MemObjectStore {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
        }
    }
}

impl Default for MemObjectStore {
    fn default() -> Self {
        Self::new()
    }
}

impl ObjectStore for MemObjectStore {
    fn caps(&self) -> ObjectStoreCaps {
        ObjectStoreCaps {
            conditional_put: true,
            if_match: true,
            strong_list: true,
            range_get: true,
            multipart: false,
            latency: LatencyProfile::InMemory,
            durability: DurabilityClass::Ephemeral,
        }
    }

    fn get(&self, key: &Key) -> Result<(Vec<u8>, ETag), StoreError> {
        let g = lock(&self.inner);
        g.objects
            .get(key.as_str())
            .map(|o| (o.data.clone(), o.etag.clone()))
            .ok_or(StoreError::NotFound)
    }

    fn get_range(&self, key: &Key, range: Range<u64>) -> Result<Vec<u8>, StoreError> {
        let g = lock(&self.inner);
        let o = g.objects.get(key.as_str()).ok_or(StoreError::NotFound)?;
        let len = o.data.len() as u64;
        if range.start > range.end || range.end > len {
            return Err(StoreError::InvalidRange);
        }
        Ok(o.data[range.start as usize..range.end as usize].to_vec())
    }

    fn put(&self, key: &Key, data: Vec<u8>, cond: PutCondition) -> Result<ETag, StoreError> {
        let mut g = lock(&self.inner);
        match &cond {
            PutCondition::Unconditional => {}
            PutCondition::IfNoneMatch => {
                if g.objects.contains_key(key.as_str()) {
                    return Err(StoreError::PreconditionFailed);
                }
            }
            PutCondition::IfMatch(expected) => match g.objects.get(key.as_str()) {
                None => return Err(StoreError::NotFound),
                Some(o) => {
                    if &o.etag != expected {
                        return Err(StoreError::PreconditionFailed);
                    }
                }
            },
        }
        g.etag_counter += 1;
        let etag = ETag::new(format!("\"{:016x}\"", g.etag_counter));
        g.objects.insert(
            key.as_str().to_string(),
            Object {
                data,
                etag: etag.clone(),
            },
        );
        Ok(etag)
    }

    fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>, StoreError> {
        let g = lock(&self.inner);
        let mut out = Vec::new();
        for (k, o) in g.objects.range(prefix.to_string()..) {
            if !k.starts_with(prefix) {
                break;
            }
            out.push(ObjectMeta {
                key: Key::new(k.clone()),
                size: o.data.len() as u64,
                etag: o.etag.clone(),
            });
        }
        Ok(out)
    }

    fn delete(&self, key: &Key) -> Result<(), StoreError> {
        let mut g = lock(&self.inner);
        g.objects.remove(key.as_str());
        Ok(())
    }
}
