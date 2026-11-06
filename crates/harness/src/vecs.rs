//! The `.fvecs`, `.ivecs` and `.bvecs` formats of the classic ANN datasets (SIFT, GIST,
//! BIGANN; see the INRIA TEXMEX corpus): each vector is a little-endian `i32` dimension
//! followed by that many components, `f32`, `i32` or `u8`. Ground truth comes as `.ivecs`.
//!
//! Readers take any `Read` and an optional limit on the number of vectors, so a prefix of a
//! large file can be used without reading the rest.

use std::io::{self, Read, Write};

/// Larger dimensions are rejected as corrupt rather than allocated.
pub const MAX_DIMS: usize = 1 << 20;

#[derive(Debug)]
pub enum VecsError {
    Io(io::Error),
    /// Vector `index` declares a dimension of 0, a negative one, or more than `MAX_DIMS`.
    BadDims {
        index: usize,
        dims: i64,
    },
    /// Vector `index` has a different dimension from the first vector.
    MixedDims {
        index: usize,
        expected: usize,
        got: usize,
    },
    /// The input ends inside vector `index`.
    Truncated {
        index: usize,
    },
    /// Vector `index` of a ground-truth file holds a negative id.
    NegativeId {
        index: usize,
    },
}

impl std::fmt::Display for VecsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VecsError::Io(e) => write!(f, "{e}"),
            VecsError::BadDims { index, dims } => write!(f, "vector {index}: bad dimension {dims}"),
            VecsError::MixedDims {
                index,
                expected,
                got,
            } => write!(f, "vector {index}: dimension {got}, expected {expected}"),
            VecsError::Truncated { index } => write!(f, "input ends inside vector {index}"),
            VecsError::NegativeId { index } => write!(f, "vector {index}: negative id"),
        }
    }
}

impl std::error::Error for VecsError {}

impl From<io::Error> for VecsError {
    fn from(e: io::Error) -> Self {
        VecsError::Io(e)
    }
}

pub fn read_fvecs(r: impl Read, limit: Option<usize>) -> Result<Vec<Vec<f32>>, VecsError> {
    read(r, limit, 4, |b| {
        b.as_chunks::<4>()
            .0
            .iter()
            .map(|c| f32::from_le_bytes(*c))
            .collect()
    })
}

pub fn read_ivecs(r: impl Read, limit: Option<usize>) -> Result<Vec<Vec<i32>>, VecsError> {
    read(r, limit, 4, |b| {
        b.as_chunks::<4>()
            .0
            .iter()
            .map(|c| i32::from_le_bytes(*c))
            .collect()
    })
}

/// `u8` components, widened to `f32` so the rest of the harness sees one vector type.
pub fn read_bvecs(r: impl Read, limit: Option<usize>) -> Result<Vec<Vec<f32>>, VecsError> {
    read(r, limit, 1, |b| b.iter().map(|x| f32::from(*x)).collect())
}

/// Ground-truth neighbour lists, as document ids.
pub fn read_truth_ivecs(r: impl Read, limit: Option<usize>) -> Result<Vec<Vec<usize>>, VecsError> {
    read_ivecs(r, limit)?
        .into_iter()
        .enumerate()
        .map(|(index, ids)| {
            ids.into_iter()
                .map(|id| usize::try_from(id).map_err(|_| VecsError::NegativeId { index }))
                .collect()
        })
        .collect()
}

pub fn write_fvecs(mut w: impl Write, vectors: &[Vec<f32>]) -> io::Result<()> {
    for v in vectors {
        write_dims(&mut w, v.len())?;
        for x in v {
            w.write_all(&x.to_le_bytes())?;
        }
    }
    Ok(())
}

pub fn write_ivecs(mut w: impl Write, vectors: &[Vec<i32>]) -> io::Result<()> {
    for v in vectors {
        write_dims(&mut w, v.len())?;
        for x in v {
            w.write_all(&x.to_le_bytes())?;
        }
    }
    Ok(())
}

fn write_dims(w: &mut impl Write, dims: usize) -> io::Result<()> {
    let d = i32::try_from(dims)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "dimension above i32::MAX"))?;
    w.write_all(&d.to_le_bytes())
}

fn read<T>(
    mut r: impl Read,
    limit: Option<usize>,
    component_bytes: usize,
    decode: impl Fn(&[u8]) -> Vec<T>,
) -> Result<Vec<Vec<T>>, VecsError> {
    let mut out = Vec::new();
    let mut expected = None;
    let mut buf = Vec::new();
    while limit.is_none_or(|l| out.len() < l) {
        let index = out.len();
        let mut header = [0u8; 4];
        match fill(&mut r, &mut header)? {
            0 => break, // clean end between vectors
            4 => {}
            _ => return Err(VecsError::Truncated { index }),
        }
        let raw = i32::from_le_bytes(header);
        let dims = match usize::try_from(raw) {
            Ok(d) if (1..=MAX_DIMS).contains(&d) => d,
            _ => {
                return Err(VecsError::BadDims {
                    index,
                    dims: i64::from(raw),
                })
            }
        };
        match expected {
            None => expected = Some(dims),
            Some(e) if e != dims => {
                return Err(VecsError::MixedDims {
                    index,
                    expected: e,
                    got: dims,
                })
            }
            Some(_) => {}
        }
        buf.resize(dims * component_bytes, 0);
        if fill(&mut r, &mut buf)? != buf.len() {
            return Err(VecsError::Truncated { index });
        }
        out.push(decode(&buf));
    }
    Ok(out)
}

/// Reads until `buf` is full or the input ends; returns how many bytes were read.
fn fill(r: &mut impl Read, buf: &mut [u8]) -> io::Result<usize> {
    let mut n = 0;
    while n < buf.len() {
        match r.read(&mut buf[n..]) {
            Ok(0) => break,
            Ok(k) => n += k,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fvecs(vectors: &[Vec<f32>]) -> Vec<u8> {
        let mut b = Vec::new();
        write_fvecs(&mut b, vectors).unwrap();
        b
    }

    #[test]
    fn layout_is_dimension_then_little_endian_components() {
        let b = fvecs(&[vec![1.0, -2.5]]);
        let mut want = 2i32.to_le_bytes().to_vec();
        want.extend(1.0f32.to_le_bytes());
        want.extend((-2.5f32).to_le_bytes());
        assert_eq!(b, want);

        let mut bv = 3i32.to_le_bytes().to_vec();
        bv.extend([0u8, 7, 255]);
        assert_eq!(read_bvecs(&bv[..], None).unwrap(), [vec![0.0, 7.0, 255.0]]);
    }

    #[test]
    fn round_trips_and_limits() {
        let v = vec![
            vec![1.0, 2.0, 3.0],
            vec![4.0, 5.0, 6.0],
            vec![7.0, 8.0, 9.0],
        ];
        let b = fvecs(&v);
        assert_eq!(read_fvecs(&b[..], None).unwrap(), v);
        assert_eq!(read_fvecs(&b[..], Some(2)).unwrap(), v[..2]);
        assert_eq!(read_fvecs(&b[..], Some(0)).unwrap(), Vec::<Vec<f32>>::new());
        assert_eq!(read_fvecs(&b[..], Some(10)).unwrap(), v);
        assert!(read_fvecs(&[][..], None).unwrap().is_empty());

        let ids = vec![vec![3, 1, 2], vec![0, 5, 4]];
        let mut ib = Vec::new();
        write_ivecs(&mut ib, &ids).unwrap();
        assert_eq!(read_ivecs(&ib[..], None).unwrap(), ids);
        assert_eq!(
            read_truth_ivecs(&ib[..], None).unwrap(),
            [vec![3usize, 1, 2], vec![0, 5, 4]]
        );
    }

    #[test]
    fn corrupt_input_is_an_error_not_a_short_read() {
        let b = fvecs(&[vec![1.0, 2.0], vec![3.0, 4.0]]);
        // Cut inside the second vector's components, and inside its header.
        for cut in [b.len() - 1, 12 + 2] {
            assert!(
                matches!(
                    read_fvecs(&b[..cut], None),
                    Err(VecsError::Truncated { index: 1 })
                ),
                "cut {cut}"
            );
        }
        // Dimension 0, negative, and absurdly large.
        for d in [0i32, -1, i32::MAX] {
            let bad = d.to_le_bytes();
            assert!(
                matches!(
                    read_fvecs(&bad[..], None),
                    Err(VecsError::BadDims { index: 0, .. })
                ),
                "dims {d}"
            );
        }
        // A second vector of another dimension.
        let mut mixed = fvecs(&[vec![1.0, 2.0]]);
        mixed.extend(fvecs(&[vec![1.0]]));
        assert!(matches!(
            read_fvecs(&mixed[..], None),
            Err(VecsError::MixedDims {
                index: 1,
                expected: 2,
                got: 1
            })
        ));
        // A negative id in ground truth.
        let mut ib = Vec::new();
        write_ivecs(&mut ib, &[vec![1], vec![-1]]).unwrap();
        assert!(matches!(
            read_truth_ivecs(&ib[..], None),
            Err(VecsError::NegativeId { index: 1 })
        ));
    }

    #[test]
    fn max_dims_is_the_largest_accepted_dimension() {
        let mut at_max = (MAX_DIMS as i32).to_le_bytes().to_vec();
        at_max.resize(4 + MAX_DIMS, 7);
        assert_eq!(read_bvecs(&at_max[..], None).unwrap()[0].len(), MAX_DIMS);
        let above = (MAX_DIMS as i32 + 1).to_le_bytes();
        assert!(matches!(
            read_bvecs(&above[..], None),
            Err(VecsError::BadDims { index: 0, dims }) if dims == MAX_DIMS as i64 + 1
        ));
    }

    /// A reader that hands out one byte at a time still reads whole vectors.
    #[test]
    fn short_reads_are_filled() {
        struct OneByte<'a>(&'a [u8]);
        impl Read for OneByte<'_> {
            fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
                if self.0.is_empty() || buf.is_empty() {
                    return Ok(0);
                }
                buf[0] = self.0[0];
                self.0 = &self.0[1..];
                Ok(1)
            }
        }
        let v = vec![vec![1.5, 2.5], vec![3.5, 4.5]];
        assert_eq!(read_fvecs(OneByte(&fvecs(&v)), None).unwrap(), v);
    }
}
