//! In-memory ObjectStore plus decorators for request counting and fault injection.

mod counting;
mod fault;
mod mem;

pub use counting::{CountingObjectStore, RequestStats};
pub use fault::{FaultConfig, FaultInjecting};
pub use mem::MemObjectStore;

use std::sync::{Mutex, MutexGuard};

/// Lock that ignores poisoning (a panicking test thread must not hide the real failure).
pub(crate) fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}
