//! Typed manifest payload: which segments make up the namespace, how far the WAL is indexed,
//! and which models the indexes were built with (PLAN 6.4, 6.5).
//!
//! Encoded as JSON. Manifests are small, JSON is easy to inspect with plain object-store tools,
//! and serde writes struct fields in declaration order, so the same content always encodes to
//! the same bytes.
//!
//! Unknown fields are rejected (`deny_unknown_fields`). Otherwise an older writer would decode a
//! newer manifest, drop the fields it does not know, and commit the result. **Adding a field
//! therefore requires bumping `CONTENT_FORMAT_VERSION`.**

use std::collections::{BTreeMap, HashSet};
use std::fmt;

use seabed_domain::{Lsn, NamespaceKeys};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::sot::{valid_attr_name, SotRef};

/// 2: segments reference their Parquet SoT (`docs`) with an optional index; `attr_schema` added.
pub const CONTENT_FORMAT_VERSION: u32 = 2;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContentError {
    Decode(String),
    UnsupportedVersion(u32),
    InvalidSegmentId(String),
    DuplicateSegment(String),
    /// The manifest names a different tenant or namespace than the keys it was read from.
    WrongNamespace {
        manifest: String,
        expected: String,
    },
    /// An attribute name in the schema is not a valid column name.
    InvalidAttrName(String),
    /// A new version would change or drop an attribute's type. The schema only grows.
    AttrSchemaChanged {
        name: String,
        before: AttrKind,
        after: Option<AttrKind>,
    },
    /// A new version would move `indexed_through` backwards (or drop it). LSNs only grow.
    IndexedThroughRegressed {
        previous: u64,
        next: Option<u64>,
    },
    /// The requested binding differs from the one the indexes were built with.
    BindingMismatch {
        field: &'static str,
        manifest: String,
        requested: String,
    },
}

impl fmt::Display for ContentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ContentError::Decode(m) => write!(f, "cannot decode manifest content: {m}"),
            ContentError::UnsupportedVersion(v) => write!(f, "unsupported content version {v}"),
            ContentError::InvalidSegmentId(id) => write!(f, "invalid segment id {id:?}"),
            ContentError::DuplicateSegment(id) => write!(f, "segment {id:?} listed twice"),
            ContentError::WrongNamespace { manifest, expected } => {
                write!(f, "manifest belongs to {manifest}, expected {expected}")
            }
            ContentError::InvalidAttrName(n) => write!(f, "invalid attribute name {n:?}"),
            ContentError::AttrSchemaChanged {
                name,
                before,
                after,
            } => write!(
                f,
                "attribute {name:?} would change from {before:?} to {after:?}; the schema only grows"
            ),
            ContentError::IndexedThroughRegressed { previous, next } => write!(
                f,
                "indexed_through would go from {previous} back to {next:?}"
            ),
            ContentError::BindingMismatch {
                field,
                manifest,
                requested,
            } => write!(
                f,
                "index binding mismatch on {field}: built with {manifest:?}, requested {requested:?}"
            ),
        }
    }
}

impl std::error::Error for ContentError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Distance {
    Cosine,
    L2,
    InnerProduct,
}

/// Type of one attribute column, fixed namespace-wide once added.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttrKind {
    Str,
    I64,
    F64,
    Bool,
}

/// Namespace-wide attribute schema: name to type. Kept in the manifest so every SoT file is
/// written with the same column types; it only grows (PLAN 6.7).
pub type AttrSchema = BTreeMap<String, AttrKind>;

/// What the indexes were built with. Querying or writing with anything else must fail at once,
/// because the result would be silently wrong (wrong embedding space, wrong tokens).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexBinding {
    pub embedding_model: String,
    pub dimensions: u32,
    pub distance: Distance,
    /// Quantizer id and version, e.g. "none", "sq8", "rabitq-1bit@1".
    pub quantization: String,
    /// Tokenizer id for BM25, e.g. "lindera-ko-dic". `None` when the namespace has no text index.
    pub tokenizer: Option<String>,
    pub tokenizer_dict_version: Option<String>,
}

impl IndexBinding {
    /// Fails on the first field that differs from `requested`.
    pub fn check(&self, requested: &IndexBinding) -> Result<(), ContentError> {
        fn cmp<T: PartialEq + fmt::Debug>(
            field: &'static str,
            manifest: &T,
            requested: &T,
        ) -> Result<(), ContentError> {
            if manifest == requested {
                Ok(())
            } else {
                Err(ContentError::BindingMismatch {
                    field,
                    manifest: format!("{manifest:?}"),
                    requested: format!("{requested:?}"),
                })
            }
        }
        cmp(
            "embedding_model",
            &self.embedding_model,
            &requested.embedding_model,
        )?;
        cmp("dimensions", &self.dimensions, &requested.dimensions)?;
        cmp("distance", &self.distance, &requested.distance)?;
        cmp("quantization", &self.quantization, &requested.quantization)?;
        cmp("tokenizer", &self.tokenizer, &requested.tokenizer)?;
        cmp(
            "tokenizer_dict_version",
            &self.tokenizer_dict_version,
            &requested.tokenizer_dict_version,
        )
    }
}

/// A SHA-256 digest, written as lowercase hex in JSON.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Sha256Hex(pub [u8; 32]);

impl Sha256Hex {
    /// Canonical lowercase hex. This is the stored form, so it must not change.
    pub fn to_hex(&self) -> String {
        const DIGITS: &[u8; 16] = b"0123456789abcdef";
        let mut s = String::with_capacity(64);
        for b in self.0 {
            s.push(DIGITS[(b >> 4) as usize] as char);
            s.push(DIGITS[(b & 0x0f) as usize] as char);
        }
        s
    }
}

impl fmt::Debug for Sha256Hex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl Serialize for Sha256Hex {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for Sha256Hex {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        let bytes = s.as_bytes();
        if bytes.len() != 64 {
            return Err(serde::de::Error::custom("sha256 must be 64 hex characters"));
        }
        // Only canonical lowercase hex, so decode then encode gives back the same bytes.
        fn nibble(c: u8) -> Option<u8> {
            match c {
                b'0'..=b'9' => Some(c - b'0'),
                b'a'..=b'f' => Some(c - b'a' + 10),
                _ => None,
            }
        }
        let mut out = [0u8; 32];
        for (i, pair) in bytes.chunks(2).enumerate() {
            match (nibble(pair[0]), nibble(pair[1])) {
                (Some(hi), Some(lo)) => out[i] = hi << 4 | lo,
                _ => {
                    return Err(serde::de::Error::custom(
                        "sha256 must be lowercase hex (0-9, a-f)",
                    ))
                }
            }
        }
        Ok(Sha256Hex(out))
    }
}

/// One immutable segment that belongs to this manifest version.
///
/// The Parquet SoT (`docs`) is the segment's source of truth and is always present. Indexes are
/// derived from it (PLAN 5.1) and may be missing, e.g. before M2 builds vector indexes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SegmentRef {
    /// Directory name under the namespace's `segments/`. ASCII letters, digits, `-` and `_`.
    pub id: String,
    /// `segments/<id>/docs.parquet`: size and SHA-256 of the bytes actually written.
    pub docs: SotRef,
    /// `segments/<id>/data.sbsg`, if an index container was built.
    pub index: Option<IndexRef>,
}

/// What the manifest records about a segment's `.sbsg` index container.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexRef {
    /// Object size in bytes, needed to read the footer without an extra request.
    pub size: u64,
    /// SHA-256 of the container's TOC. Opening checks it, so a different object at the same key
    /// is detected.
    pub toc_sha256: Sha256Hex,
}

/// Segment ids become part of object keys, so only characters that cannot escape the
/// namespace's `segments/` directory are allowed.
pub fn valid_segment_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestContent {
    pub format_version: u32,
    pub tenant: String,
    pub namespace: String,
    /// WAL entries up to and including this LSN are in the segments. Entries after it are the
    /// unindexed tail that strong reads must also search. `None`: nothing indexed yet.
    pub indexed_through: Option<u64>,
    pub index_binding: Option<IndexBinding>,
    /// Attribute columns every SoT file is written with. Only grows.
    pub attr_schema: AttrSchema,
    /// In the order they were added.
    pub segments: Vec<SegmentRef>,
}

impl ManifestContent {
    /// Empty content for a namespace: no segments, nothing indexed, no binding yet.
    pub fn empty(keys: &NamespaceKeys) -> Self {
        Self {
            format_version: CONTENT_FORMAT_VERSION,
            tenant: keys.tenant().to_string(),
            namespace: keys.namespace().to_string(),
            indexed_through: None,
            index_binding: None,
            attr_schema: AttrSchema::new(),
            segments: Vec::new(),
        }
    }

    pub fn indexed_through(&self) -> Option<Lsn> {
        self.indexed_through.map(Lsn::new)
    }

    /// Adds a segment. Fails on an invalid or duplicate id.
    pub fn add_segment(&mut self, segment: SegmentRef) -> Result<(), ContentError> {
        if !valid_segment_id(&segment.id) {
            return Err(ContentError::InvalidSegmentId(segment.id));
        }
        if self.segments.iter().any(|s| s.id == segment.id) {
            return Err(ContentError::DuplicateSegment(segment.id));
        }
        self.segments.push(segment);
        Ok(())
    }

    /// Adds attributes to the schema. An attribute already present must keep its type.
    /// Fails without changing anything if any addition conflicts or has an invalid name.
    pub fn extend_attr_schema(&mut self, additions: &AttrSchema) -> Result<(), ContentError> {
        for (name, kind) in additions {
            if !valid_attr_name(name) {
                return Err(ContentError::InvalidAttrName(name.clone()));
            }
            if let Some(before) = self.attr_schema.get(name) {
                if before != kind {
                    return Err(ContentError::AttrSchemaChanged {
                        name: name.clone(),
                        before: *before,
                        after: Some(*kind),
                    });
                }
            }
        }
        self.attr_schema
            .extend(additions.iter().map(|(n, k)| (n.clone(), *k)));
        Ok(())
    }

    /// Checks `requested` against the stored binding. A namespace without a binding accepts
    /// any; the first writer sets it with `bind`.
    pub fn check_binding(&self, requested: &IndexBinding) -> Result<(), ContentError> {
        match &self.index_binding {
            Some(b) => b.check(requested),
            None => Ok(()),
        }
    }

    /// Sets the binding if none is stored, otherwise checks it.
    pub fn bind(&mut self, binding: IndexBinding) -> Result<(), ContentError> {
        self.check_binding(&binding)?;
        self.index_binding = Some(binding);
        Ok(())
    }

    /// Checks that `next` may follow `self` as the next manifest version:
    /// - once a binding is stored, every later version keeps exactly that binding;
    /// - `indexed_through` never moves backwards and is never dropped once set;
    /// - every attribute in the schema keeps its type (the schema only grows).
    ///
    /// The segment list is not constrained here: compaction will replace segments.
    ///
    /// Pure, so the rules are testable without a store; `ManifestStore::commit_content`
    /// applies it against the expected version.
    pub fn check_successor(&self, next: &ManifestContent) -> Result<(), ContentError> {
        if let Some(prev) = &self.index_binding {
            match &next.index_binding {
                Some(b) => prev.check(b)?,
                None => {
                    return Err(ContentError::BindingMismatch {
                        field: "index_binding",
                        manifest: format!("{prev:?}"),
                        requested: "None".into(),
                    })
                }
            }
        }
        for (name, before) in &self.attr_schema {
            let after = next.attr_schema.get(name).copied();
            if after != Some(*before) {
                return Err(ContentError::AttrSchemaChanged {
                    name: name.clone(),
                    before: *before,
                    after,
                });
            }
        }
        if let Some(prev) = self.indexed_through {
            if next.indexed_through.is_none_or(|n| n < prev) {
                return Err(ContentError::IndexedThroughRegressed {
                    previous: prev,
                    next: next.indexed_through,
                });
            }
        }
        Ok(())
    }

    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("manifest content always serializes")
    }

    /// Decodes and validates content read from `keys`' manifest.
    pub fn decode(bytes: &[u8], keys: &NamespaceKeys) -> Result<Self, ContentError> {
        let c: ManifestContent =
            serde_json::from_slice(bytes).map_err(|e| ContentError::Decode(e.to_string()))?;
        if c.format_version != CONTENT_FORMAT_VERSION {
            return Err(ContentError::UnsupportedVersion(c.format_version));
        }
        let (tenant, namespace) = (keys.tenant().to_string(), keys.namespace().to_string());
        if c.tenant != tenant || c.namespace != namespace {
            return Err(ContentError::WrongNamespace {
                manifest: format!("{}/{}", c.tenant, c.namespace),
                expected: format!("{tenant}/{namespace}"),
            });
        }
        if let Some(bad) = c.attr_schema.keys().find(|n| !valid_attr_name(n)) {
            return Err(ContentError::InvalidAttrName(bad.clone()));
        }
        let mut seen = HashSet::new();
        for s in &c.segments {
            if !valid_segment_id(&s.id) {
                return Err(ContentError::InvalidSegmentId(s.id.clone()));
            }
            if !seen.insert(s.id.as_str()) {
                return Err(ContentError::DuplicateSegment(s.id.clone()));
            }
        }
        Ok(c)
    }
}
