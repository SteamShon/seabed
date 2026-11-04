//! Typed manifest payload: which segments make up the namespace, how far the WAL is indexed,
//! and which models the indexes were built with (PLAN 6.4, 6.5).
//!
//! Encoded as JSON. Manifests are small, JSON is easy to inspect with plain object-store tools,
//! and serde writes struct fields in declaration order, so the same content always encodes to
//! the same bytes.

use std::collections::HashSet;
use std::fmt;

use seabed_domain::{Lsn, NamespaceKeys};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

pub const CONTENT_FORMAT_VERSION: u32 = 1;

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

/// What the indexes were built with. Querying or writing with anything else must fail at once,
/// because the result would be silently wrong (wrong embedding space, wrong tokens).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

impl fmt::Debug for Sha256Hex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for b in self.0 {
            write!(f, "{b:02x}")?;
        }
        Ok(())
    }
}

impl Serialize for Sha256Hex {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&format!("{self:?}"))
    }
}

impl<'de> Deserialize<'de> for Sha256Hex {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        let bytes = s.as_bytes();
        if bytes.len() != 64 {
            return Err(serde::de::Error::custom("sha256 must be 64 hex characters"));
        }
        let mut out = [0u8; 32];
        for (i, pair) in bytes.chunks(2).enumerate() {
            let hex = std::str::from_utf8(pair).map_err(serde::de::Error::custom)?;
            out[i] = u8::from_str_radix(hex, 16).map_err(serde::de::Error::custom)?;
        }
        Ok(Sha256Hex(out))
    }
}

/// One immutable segment that belongs to this manifest version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentRef {
    /// Directory name under the namespace's `segments/`. ASCII letters, digits, `-` and `_`.
    pub id: String,
    /// Object size in bytes, needed to read the footer without an extra request.
    pub size: u64,
    /// SHA-256 of the segment's TOC. Opening checks it, so a different object at the same key
    /// is detected.
    pub toc_sha256: Sha256Hex,
}

fn valid_segment_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestContent {
    pub format_version: u32,
    pub tenant: String,
    pub namespace: String,
    /// WAL entries up to and including this LSN are in the segments. Entries after it are the
    /// unindexed tail that strong reads must also search. `None`: nothing indexed yet.
    pub indexed_through: Option<u64>,
    pub index_binding: Option<IndexBinding>,
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
