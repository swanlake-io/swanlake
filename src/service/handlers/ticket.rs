//! Flight ticket payload helpers.
//!
//! # Overview
//!
//! Every Flight `Ticket` we emit is opaque to clients, but the server needs to
//! remember *which* query/statement it represents. We therefore encode a small
//! protobuf payload that carries only a version number, the handle bytes, and
//! a `kind` flag (prepared vs. ephemeral). The payload acts as an index into
//! the session's prepared-statement map, so the actual SQL, schema cache, and
//! parameters never leave the server.
//!
//! ```text
//! client                server session
//!   │ Ticket(bytes) ───────▶ │ decode TicketStatementPayload
//!   │                       │ lookup handle (prepared/ephemeral)
//!   │ ◀──── stream data ────│ execute via stored metadata
//! ```
//!
//! Having the version field allows us to evolve the ticket layout later without
//! breaking existing binaries; we can reject or down-level tickets if we ever
//! change how handles are encoded.

use std::convert::TryFrom;

use prost::Message;

use crate::session::id::StatementHandle;

/// Discriminator carried in the ticket payload so we know how the handle was
/// created (long-lived prepared vs. one-shot ephemeral query).
#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
pub enum StatementTicketKind {
    Prepared = 1,
    Ephemeral = 2,
}

/// Serialized form that rides in the Flight ticket.
#[derive(Clone, PartialEq, Message)]
pub struct TicketStatementPayload {
    /// Version gate for future layout changes.
    #[prost(uint32, tag = "1")]
    pub version: u32,
    /// Whether the handle references a standard prepared statement or an
    /// ephemeral entry created on the fly for a direct query.
    #[prost(enumeration = "StatementTicketKind", tag = "2")]
    pub kind: i32,
    /// Raw bytes of the `StatementHandle` (big-endian u64 today).
    #[prost(bytes = "vec", tag = "3")]
    pub statement_handle: Vec<u8>,
}

impl TicketStatementPayload {
    pub const CURRENT_VERSION: u32 = 1;

    pub fn new(handle: StatementHandle, kind: StatementTicketKind) -> Self {
        Self {
            version: Self::CURRENT_VERSION,
            kind: kind as i32,
            statement_handle: handle.to_bytes(),
        }
    }

    pub fn handle(&self) -> Option<StatementHandle> {
        StatementHandle::from_bytes(&self.statement_handle)
    }

    pub fn ticket_kind(&self) -> Option<StatementTicketKind> {
        StatementTicketKind::try_from(self.kind).ok()
    }
}
