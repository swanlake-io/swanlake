use std::pin::Pin;
use std::sync::Arc;

use arrow_flight::error::FlightError;
use arrow_flight::FlightData;
use futures::{stream, Stream};
use tonic::{Request, Status};
use tracing::{error, Span};
use uuid::Uuid;

use crate::dq::QueueRuntime;
use crate::error::ServerError;
use crate::session::{registry::SessionRegistry, Session, SessionId};

mod convert;
mod execute;
mod handlers;

// Phase 2 Complete: All state (prepared statements, transactions) is session-scoped
// - Each gRPC connection gets a dedicated session (based on remote_addr)
// - Sessions persist across requests from the same connection
// - Prepared statements and transactions are isolated per session
// - Automatic cleanup via idle timeout (30min default)

#[derive(Clone)]
pub struct SwanFlightSqlService {
    registry: Arc<SessionRegistry>,
    /// Holds the QueueRuntime to keep background queue management tasks (rotation, flushing, cleanup) alive.
    #[allow(dead_code)]
    dq_runtime: Option<Arc<QueueRuntime>>,
}

impl SwanFlightSqlService {
    /// Creates a new SwanFlightSqlService, holding the QueueRuntime to ensure background
    /// queue management tasks (rotation, flushing, cleanup) remain active.
    pub fn new(registry: Arc<SessionRegistry>, dq_runtime: Option<Arc<QueueRuntime>>) -> Self {
        Self {
            registry,
            dq_runtime,
        }
    }

    /// Extract session ID from tonic Request for session tracking (Phase 2)
    ///
    /// This uses the remote peer address as the session ID.
    /// Sessions persist across requests from the same gRPC connection.
    pub(crate) fn extract_session_id<T>(request: &Request<T>) -> SessionId {
        if let Some(addr) = request.remote_addr() {
            SessionId::from_string(addr.to_string())
        } else {
            SessionId::from_string(Uuid::new_v4().to_string())
        }
    }

    /// Get or create a session based on connection (Phase 2: connection-based persistence)
    ///
    /// Extracts session ID from the gRPC connection and reuses sessions across requests.
    pub(crate) fn get_session<T>(&self, request: &Request<T>) -> Result<Arc<Session>, Status> {
        let session_id = Self::extract_session_id(request);
        self.registry
            .get_or_create_by_id(&session_id)
            .map_err(Self::status_from_error)
    }

    /// Prepare request: extract session_id, record to tracing span, and get/create session
    ///
    /// This centralizes the common pattern of session management in handlers.
    pub(crate) fn prepare_request<T>(&self, request: &Request<T>) -> Result<Arc<Session>, Status> {
        let session_id = Self::extract_session_id(request);
        Span::current().record("session_id", session_id.as_ref());
        self.registry
            .get_or_create_by_id(&session_id)
            .map_err(Self::status_from_error)
    }

    pub(crate) fn status_from_error(err: ServerError) -> Status {
        match err {
            ServerError::DuckDb(e) => {
                error!(error = %e, "duckdb engine error");
                Status::internal(format!("duckdb error: {e}"))
            }
            ServerError::Arrow(e) => {
                error!(error = %e, "arrow conversion error");
                Status::internal(format!("arrow error: {e}"))
            }
            ServerError::TransactionNotFound => {
                error!("unknown transaction");
                Status::invalid_argument("unknown transaction")
            }
            ServerError::PreparedStatementNotFound => {
                error!("unknown prepared statement");
                Status::invalid_argument("unknown prepared statement")
            }
            ServerError::MaxSessionsReached => {
                error!("maximum number of sessions reached");
                Status::resource_exhausted("maximum number of sessions reached")
            }
            ServerError::UnsupportedParameter(param) => {
                error!(param = %param, "unsupported parameter type");
                Status::invalid_argument(format!("unsupported parameter type: {param}"))
            }
            ServerError::Internal(msg) => {
                error!(msg = %msg, "internal error");
                Status::internal(format!("internal error: {msg}"))
            }
        }
    }

    pub(crate) fn status_from_join(err: tokio::task::JoinError) -> Status {
        if err.is_panic() {
            error!(%err, "blocking task panicked");
            Status::internal("blocking task panicked")
        } else {
            error!(%err, "blocking task cancelled");
            Status::internal(format!("blocking task cancelled: {err}"))
        }
    }

    pub(crate) fn status_from_flight_error(err: FlightError) -> Status {
        match err {
            FlightError::Tonic(status) => {
                error!(status = ?status, "tonic flight error");
                *status
            }
            other => {
                error!(error = %other, "flight decode error");
                Status::internal(format!("flight decode error: {other}"))
            }
        }
    }

    pub(crate) fn into_stream(
        batches: Vec<FlightData>,
    ) -> Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>> {
        Box::pin(stream::iter(batches.into_iter().map(Ok)))
    }

    /// Detect if SQL is a query (returns results) or statement (doesn't return results).
    pub(crate) fn is_query_statement(sql: &str) -> bool {
        let trimmed = sql.trim_start();

        let mut cleaned = trimmed;
        loop {
            if let Some(rest) = cleaned.strip_prefix("--") {
                if let Some(newline_pos) = rest.find('\n') {
                    cleaned = rest[newline_pos + 1..].trim_start();
                } else {
                    return false;
                }
            } else if let Some(rest) = cleaned.strip_prefix("/*") {
                if let Some(end_pos) = rest.find("*/") {
                    cleaned = rest[end_pos + 2..].trim_start();
                } else {
                    return false;
                }
            } else {
                break;
            }
        }

        let first_word = cleaned
            .split(|c: char| c.is_whitespace() || c == '(' || c == ';')
            .find(|w| !w.is_empty())
            .unwrap_or("")
            .to_uppercase();

        matches!(
            first_word.as_str(),
            "SELECT"
                | "WITH"
                | "SHOW"
                | "DESCRIBE"
                | "DESC"
                | "EXPLAIN"
                | "VALUES"
                | "TABLE"
                | "PRAGMA"
        )
    }
}
