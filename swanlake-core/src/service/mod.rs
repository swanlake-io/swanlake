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
    pub(crate) async fn get_session<T>(
        &self,
        request: &Request<T>,
    ) -> Result<Arc<Session>, Status> {
        let session_id = Self::extract_session_id(request);
        self.registry
            .get_or_create_by_id(&session_id)
            .await
            .map_err(Self::status_from_error)
    }

    /// Prepare request: extract session_id, record to tracing span, and get/create session
    ///
    /// This centralizes the common pattern of session management in handlers.
    pub(crate) async fn prepare_request<T>(
        &self,
        request: &Request<T>,
    ) -> Result<Arc<Session>, Status> {
        let session_id = Self::extract_session_id(request);
        Span::current().record("session_id", session_id.as_ref());
        self.registry
            .get_or_create_by_id(&session_id)
            .await
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
            ServerError::TransactionAborted => {
                error!("transaction aborted and rolled back");
                Status::failed_precondition(
                    "transaction aborted and rolled back; start a new transaction",
                )
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
            ServerError::DucklingQueueDisabled => {
                error!("duckling queue runtime is disabled");
                Status::failed_precondition(
                    "duckling queue runtime is disabled; set SWANLAKE_DUCKLING_QUEUE_ENABLED=true",
                )
            }
            ServerError::DucklingQueueWriteOnly => {
                error!("duckling queue tables are write-only");
                Status::invalid_argument(
                    "duckling queue tables are write-only; only INSERT INTO duckling_queue.* is allowed",
                )
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
}
