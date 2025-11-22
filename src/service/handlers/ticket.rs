//! Flight ticket payload helpers.
//!
//! # Overview
//!
//! Every Flight `Ticket` we emit is opaque to clients, but the server needs to
//! remember *which* query/statement it represents. We therefore encode a small
//! protobuf payload that carries only a version number, the handle bytes, and
//! a `kind` flag (prepared vs. ephemeral) plus a `returns_rows` bit so we know
//! whether to stream results or execute a command. The payload acts as an index
//! into the session's prepared-statement map, so the actual SQL, schema cache,
//! and parameters never leave the server.
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
    /// Optional SQL text to fall back to if the handle cannot be resolved.
    #[prost(string, optional, tag = "4")]
    pub fallback_sql: Option<String>,
    /// Whether the statement returns rows (queries) or is a command (DDL/DML).
    /// Defaults to `true` for backward compatibility if absent.
    #[prost(bool, optional, tag = "5")]
    pub returns_rows: Option<bool>,
}

impl TicketStatementPayload {
    pub const CURRENT_VERSION: u32 = 2;

    pub fn new(kind: StatementTicketKind) -> Self {
        Self {
            version: Self::CURRENT_VERSION,
            kind: kind as i32,
            statement_handle: Vec::new(),
            fallback_sql: None,
            returns_rows: None,
        }
    }

    pub fn with_handle(mut self, handle: StatementHandle) -> Self {
        self.statement_handle = handle.to_bytes();
        self
    }

    pub fn with_fallback_sql<S: Into<String>>(mut self, sql: S) -> Self {
        self.fallback_sql = Some(sql.into());
        self
    }

    pub fn with_returns_rows(mut self, returns_rows: bool) -> Self {
        self.returns_rows = Some(returns_rows);
        self
    }

    pub fn handle(&self) -> Option<StatementHandle> {
        if self.statement_handle.is_empty() {
            return None;
        }
        StatementHandle::from_bytes(&self.statement_handle)
    }

    pub fn fallback_sql_str(&self) -> Option<&str> {
        self.fallback_sql.as_deref()
    }

    pub fn ticket_kind(&self) -> Option<StatementTicketKind> {
        StatementTicketKind::try_from(self.kind).ok()
    }

    /// Default to true so older tickets that did not set this field remain query tickets.
    pub fn returns_rows_flag(&self) -> bool {
        self.returns_rows.unwrap_or(true)
    }
}
