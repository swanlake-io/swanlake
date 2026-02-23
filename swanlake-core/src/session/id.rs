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

macro_rules! define_id_type {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub struct $name(u64);

        impl $name {
            /// Number of bytes required to encode this ID
            pub const BYTE_LEN: usize = std::mem::size_of::<u64>();

            /// Create a new ID from a u64 value
            pub fn new(id: u64) -> Self {
                Self(id)
            }

            /// Get the underlying u64 ID
            pub fn id(&self) -> u64 {
                self.0
            }

            /// Convert to big-endian bytes
            pub fn to_bytes(self) -> Vec<u8> {
                self.0.to_be_bytes().to_vec()
            }

            /// Create from big-endian bytes
            pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
                Some(Self(u64::from_be_bytes(bytes.try_into().ok()?)))
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl From<$name> for Vec<u8> {
            fn from(id: $name) -> Vec<u8> {
                id.to_bytes()
            }
        }

        impl From<$name> for prost::bytes::Bytes {
            fn from(id: $name) -> prost::bytes::Bytes {
                id.to_bytes().into()
            }
        }
    };
}

define_id_type!(
    StatementHandle,
    "Prepared statement handle - wraps a u64 ID"
);
define_id_type!(TransactionId, "Transaction ID - wraps a u64 ID");

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

    pub fn next(&self) -> TransactionId {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        TransactionId::new(id)
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

    pub fn next(&self) -> StatementHandle {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        StatementHandle::new(id)
    }
}

impl Default for StatementHandleGenerator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_id_conversions_and_formatting_work() {
        let custom = SessionId::from_string("session-123".to_string());
        assert_eq!(custom.as_ref(), "session-123");
        assert_eq!(custom.to_string(), "session-123");

        let from_impl: SessionId = "other-session".to_string().into();
        assert_eq!(from_impl.as_ref(), "other-session");

        let generated = SessionId::new();
        assert!(!generated.as_ref().is_empty());
    }

    #[test]
    fn statement_and_transaction_ids_round_trip_bytes() {
        let statement = StatementHandle::new(42);
        let bytes = statement.to_bytes();
        assert_eq!(bytes.len(), StatementHandle::BYTE_LEN);
        let decoded_statement = StatementHandle::from_bytes(&bytes);
        assert_eq!(decoded_statement.map(|id| id.id()), Some(42));

        let transaction = TransactionId::new(99);
        let tx_bytes = transaction.to_bytes();
        assert_eq!(tx_bytes.len(), TransactionId::BYTE_LEN);
        let decoded_transaction = TransactionId::from_bytes(&tx_bytes);
        assert_eq!(decoded_transaction.map(|id| id.id()), Some(99));

        assert!(StatementHandle::from_bytes(&[1, 2, 3]).is_none());
        assert!(TransactionId::from_bytes(&[1, 2, 3]).is_none());
    }

    #[test]
    fn id_generators_increment_monotonically() {
        let statement_gen = StatementHandleGenerator::new();
        let first_statement = statement_gen.next();
        let second_statement = statement_gen.next();
        assert_eq!(first_statement.id(), 1);
        assert_eq!(second_statement.id(), 2);

        let tx_gen = TransactionIdGenerator::new();
        let first_tx = tx_gen.next();
        let second_tx = tx_gen.next();
        assert_eq!(first_tx.id(), 1);
        assert_eq!(second_tx.id(), 2);
    }
}
