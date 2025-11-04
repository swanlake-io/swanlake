//! Session ID generation and management.
//!
//! Session IDs are used to track client connections and their associated state.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use uuid::Uuid;

/// Unique identifier for a client session
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SessionId(String);

impl SessionId {
    /// Create a new random session ID
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    /// Create from existing string (for deserialization)
    pub fn from_string(s: String) -> Self {
        Self(s)
    }
}

impl Default for SessionId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for SessionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for SessionId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl AsRef<str> for SessionId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// Transaction ID generator
#[derive(Debug)]
pub struct TransactionIdGenerator {
    next_id: AtomicU64,
}

impl TransactionIdGenerator {
    pub fn new() -> Self {
        Self {
            next_id: AtomicU64::new(1),
        }
    }

    pub fn next(&self) -> Vec<u8> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        id.to_be_bytes().to_vec()
    }
}

impl Default for TransactionIdGenerator {
    fn default() -> Self {
        Self::new()
    }
}

/// Prepared statement handle generator
#[derive(Debug)]
pub struct StatementHandleGenerator {
    next_id: AtomicU64,
}

impl StatementHandleGenerator {
    pub fn new() -> Self {
        Self {
            next_id: AtomicU64::new(1),
        }
    }

    pub fn next(&self) -> Vec<u8> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        id.to_be_bytes().to_vec()
    }
}

impl Default for StatementHandleGenerator {
    fn default() -> Self {
        Self::new()
    }
}
