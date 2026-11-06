//! Domain model. No I/O and no external dependencies.
//!
//! Design rule 7: every key embeds the tenant and namespace identifiers from day one,
//! because retrofitting them later is very hard.

use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum DomainError {
    InvalidId(String),
}

impl fmt::Display for DomainError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DomainError::InvalidId(msg) => write!(f, "invalid id: {msg}"),
        }
    }
}

impl std::error::Error for DomainError {}

fn validate_id(kind: &str, s: &str) -> Result<(), DomainError> {
    let ok = !s.is_empty()
        && s.len() <= 128
        && s.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'_');
    if ok {
        Ok(())
    } else {
        Err(DomainError::InvalidId(format!("{kind}: {s:?}")))
    }
}

macro_rules! id_type {
    ($name:ident, $kind:expr) => {
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(String);

        impl $name {
            pub fn new(s: impl Into<String>) -> Result<Self, DomainError> {
                let s = s.into();
                validate_id($kind, &s)?;
                Ok(Self(s))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}

macro_rules! counter_type {
    ($name:ident) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(u64);

        impl $name {
            pub const fn new(v: u64) -> Self {
                Self(v)
            }

            pub const fn get(self) -> u64 {
                self.0
            }

            pub const fn next(self) -> Self {
                Self(self.0 + 1)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }
    };
}

id_type!(TenantId, "tenant");
id_type!(NamespaceId, "namespace");

// Monotonic log sequence number of a write. The first LSN is 1.
counter_type!(Lsn);
// Monotonic manifest version. The first version is 1.
counter_type!(Version);

/// Opaque object-store key.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Key(String);

impl Key {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Opaque entity tag returned by the object store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ETag(String);

impl ETag {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Key layout of one namespace. Zero-padded numbers keep lexicographic order equal to numeric order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceKeys {
    tenant: TenantId,
    namespace: NamespaceId,
}

impl NamespaceKeys {
    pub fn new(tenant: TenantId, namespace: NamespaceId) -> Self {
        Self { tenant, namespace }
    }

    pub fn tenant(&self) -> &TenantId {
        &self.tenant
    }

    pub fn namespace(&self) -> &NamespaceId {
        &self.namespace
    }

    fn base(&self) -> String {
        format!("t/{}/n/{}", self.tenant, self.namespace)
    }

    pub fn wal_prefix(&self) -> String {
        format!("{}/wal/", self.base())
    }

    pub fn wal_key(&self, lsn: Lsn) -> Key {
        Key::new(format!("{}{:020}", self.wal_prefix(), lsn.get()))
    }

    pub fn parse_wal_lsn(&self, key: &Key) -> Option<Lsn> {
        let prefix = self.wal_prefix();
        let rest = key.as_str().strip_prefix(prefix.as_str())?;
        rest.parse::<u64>().ok().map(Lsn::new)
    }

    pub fn manifest_prefix(&self) -> String {
        format!("{}/manifest/", self.base())
    }

    pub fn manifest_key(&self, version: Version) -> Key {
        Key::new(format!("{}{:020}", self.manifest_prefix(), version.get()))
    }

    pub fn parse_manifest_version(&self, key: &Key) -> Option<Version> {
        let prefix = self.manifest_prefix();
        let rest = key.as_str().strip_prefix(prefix.as_str())?;
        rest.parse::<u64>().ok().map(Version::new)
    }

    /// Pointer object holding the latest committed manifest version (a hint, see the engine).
    /// It lives outside `manifest_prefix` so listing versions never returns it.
    pub fn manifest_pointer_key(&self) -> Key {
        Key::new(format!("{}/manifest-pointer", self.base()))
    }

    pub fn segment_key(&self, segment_id: &str, file: &str) -> Key {
        Key::new(format!("{}/segments/{}/{}", self.base(), segment_id, file))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys() -> NamespaceKeys {
        NamespaceKeys::new(
            TenantId::new("acme").unwrap(),
            NamespaceId::new("ads").unwrap(),
        )
    }

    #[test]
    fn ids_reject_bad_input() {
        assert!(TenantId::new("").is_err());
        assert!(TenantId::new("Acme").is_err());
        assert!(TenantId::new("a/b").is_err());
        assert!(TenantId::new("acme-1_x").is_ok());
    }

    #[test]
    fn keys_embed_tenant_and_namespace() {
        let k = keys();
        assert!(k.wal_key(Lsn::new(7)).as_str().starts_with("t/acme/n/ads/"));
        assert!(k
            .manifest_key(Version::new(3))
            .as_str()
            .starts_with("t/acme/n/ads/"));
        assert!(k
            .segment_key("seg1", "data.parquet")
            .as_str()
            .starts_with("t/acme/n/ads/"));
    }

    #[test]
    fn padded_keys_sort_like_numbers() {
        let k = keys();
        assert!(k.wal_key(Lsn::new(9)) < k.wal_key(Lsn::new(10)));
        assert!(k.manifest_key(Version::new(99)) < k.manifest_key(Version::new(100)));
    }

    #[test]
    fn parse_roundtrip() {
        let k = keys();
        assert_eq!(
            k.parse_wal_lsn(&k.wal_key(Lsn::new(42))),
            Some(Lsn::new(42))
        );
        assert_eq!(
            k.parse_manifest_version(&k.manifest_key(Version::new(5))),
            Some(Version::new(5))
        );
        // A WAL key is not a manifest key.
        assert_eq!(k.parse_manifest_version(&k.wal_key(Lsn::new(1))), None);
        // Neither is the pointer, which is also outside the listed prefix.
        let ptr = k.manifest_pointer_key();
        assert_eq!(k.parse_manifest_version(&ptr), None);
        assert!(!ptr.as_str().starts_with(&k.manifest_prefix()));
    }
}
