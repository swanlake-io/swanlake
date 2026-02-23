use std::pin::Pin;
use std::sync::Arc;

use arrow_flight::error::FlightError;
use arrow_flight::FlightData;
use futures::{stream, Stream};
use tonic::{Request, Status};
use tracing::{error, Span};
use uuid::Uuid;

use crate::config::SessionIdMode;
use crate::error::ServerError;
use crate::metrics::Metrics;
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
    metrics: Arc<Metrics>,
    session_id_mode: SessionIdMode,
}

impl SwanFlightSqlService {
    pub fn new(
        registry: Arc<SessionRegistry>,
        metrics: Arc<Metrics>,
        session_id_mode: SessionIdMode,
    ) -> Self {
        Self {
            registry,
            metrics,
            session_id_mode,
        }
    }

    /// Extract session ID from tonic Request for session tracking (Phase 2)
    ///
    /// This uses the remote peer address as the session ID.
    /// Sessions persist across requests from the same gRPC connection.
    pub(crate) fn extract_session_id<T>(&self, request: &Request<T>) -> SessionId {
        match self.session_id_mode {
            SessionIdMode::PeerAddr => {
                if let Some(addr) = request.remote_addr() {
                    SessionId::from_string(addr.to_string())
                } else {
                    SessionId::from_string(Uuid::new_v4().to_string())
                }
            }
            SessionIdMode::PeerIp => {
                if let Some(addr) = request.remote_addr() {
                    SessionId::from_string(addr.ip().to_string())
                } else {
                    SessionId::from_string(Uuid::new_v4().to_string())
                }
            }
        }
    }

    /// Get or create a session based on connection (Phase 2: connection-based persistence)
    /// Prepare request: extract session_id, record to tracing span, and get/create session.
    /// Extracts session ID from the gRPC connection and reuses sessions across requests.
    pub(crate) async fn prepare_request<T>(
        &self,
        request: &Request<T>,
    ) -> Result<Arc<Session>, Status> {
        let session_id = self.extract_session_id(request);
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use anyhow::{anyhow, Result};
    use arrow_flight::error::FlightError;
    use arrow_flight::FlightData;
    use futures::StreamExt;
    use tonic::transport::server::TcpConnectInfo;
    use tonic::{Code, Request, Status};
    use uuid::Uuid;

    use super::*;
    use crate::config::{ServerConfig, SessionIdMode};
    use crate::engine::EngineFactory;

    fn test_service(mode: SessionIdMode) -> Result<SwanFlightSqlService> {
        let config = ServerConfig {
            session_id_mode: mode.clone(),
            ..ServerConfig::default()
        };
        let factory = Arc::new(EngineFactory::new(&config).map_err(|e| anyhow!(e.to_string()))?);
        let registry =
            Arc::new(SessionRegistry::new(&config, factory).map_err(|e| anyhow!(e.to_string()))?);
        let metrics = Arc::new(Metrics::new(1_000, 64));
        Ok(SwanFlightSqlService::new(registry, metrics, mode))
    }

    #[test]
    fn extract_session_id_uses_peer_addr_and_peer_ip_modes() -> Result<()> {
        let addr = "127.10.20.30:4321"
            .parse()
            .map_err(|e| anyhow!("failed to parse socket addr: {e}"))?;
        let mut request = Request::new(());
        request.extensions_mut().insert(TcpConnectInfo {
            local_addr: None,
            remote_addr: Some(addr),
        });

        let peer_addr_service = test_service(SessionIdMode::PeerAddr)?;
        let peer_addr_id = peer_addr_service.extract_session_id(&request);
        assert_eq!(peer_addr_id.as_ref(), "127.10.20.30:4321");

        let peer_ip_service = test_service(SessionIdMode::PeerIp)?;
        let peer_ip_id = peer_ip_service.extract_session_id(&request);
        assert_eq!(peer_ip_id.as_ref(), "127.10.20.30");

        let fallback_request = Request::new(());
        let fallback_id = peer_ip_service.extract_session_id(&fallback_request);
        assert!(Uuid::parse_str(fallback_id.as_ref()).is_ok());
        Ok(())
    }

    #[test]
    fn status_from_error_maps_expected_codes() -> Result<()> {
        let conn = duckdb::Connection::open_in_memory()?;
        let duck_error = match conn.execute_batch("SELECT * FROM __missing_table__") {
            Ok(()) => return Err(anyhow!("expected missing-table query to fail")),
            Err(err) => err,
        };
        let duck_status = SwanFlightSqlService::status_from_error(ServerError::DuckDb(duck_error));
        assert_eq!(duck_status.code(), Code::Internal);

        let arrow_status = SwanFlightSqlService::status_from_error(ServerError::Arrow(
            arrow_schema::ArrowError::ParseError("bad".to_string()),
        ));
        assert_eq!(arrow_status.code(), Code::Internal);

        assert_eq!(
            SwanFlightSqlService::status_from_error(ServerError::TransactionAborted).code(),
            Code::FailedPrecondition
        );
        assert_eq!(
            SwanFlightSqlService::status_from_error(ServerError::TransactionNotFound).code(),
            Code::InvalidArgument
        );
        assert_eq!(
            SwanFlightSqlService::status_from_error(ServerError::PreparedStatementNotFound).code(),
            Code::InvalidArgument
        );
        assert_eq!(
            SwanFlightSqlService::status_from_error(ServerError::MaxSessionsReached).code(),
            Code::ResourceExhausted
        );
        assert_eq!(
            SwanFlightSqlService::status_from_error(ServerError::UnsupportedParameter(
                "x".to_string()
            ))
            .code(),
            Code::InvalidArgument
        );
        assert_eq!(
            SwanFlightSqlService::status_from_error(ServerError::Internal("x".to_string())).code(),
            Code::Internal
        );
        Ok(())
    }

    #[test]
    fn status_from_join_handles_panic_and_cancellation() -> Result<()> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;

        let panic_join = runtime.block_on(async {
            tokio::task::spawn_blocking(|| {
                let values = [1_u8];
                let idx = values.len();
                let _ = values[idx];
            })
            .await
        });
        let panic_error = match panic_join {
            Ok(()) => return Err(anyhow!("expected panicking task to return JoinError")),
            Err(err) => err,
        };
        let panic_status = SwanFlightSqlService::status_from_join(panic_error);
        assert_eq!(panic_status.code(), Code::Internal);
        assert!(panic_status.message().contains("panicked"));

        let cancelled_join = runtime.block_on(async {
            let handle = tokio::spawn(async {
                tokio::time::sleep(Duration::from_secs(30)).await;
            });
            handle.abort();
            handle.await
        });
        let cancelled_error = match cancelled_join {
            Ok(()) => return Err(anyhow!("expected cancelled task to return JoinError")),
            Err(err) => err,
        };
        let cancelled_status = SwanFlightSqlService::status_from_join(cancelled_error);
        assert_eq!(cancelled_status.code(), Code::Internal);
        assert!(cancelled_status.message().contains("cancelled"));
        Ok(())
    }

    #[test]
    fn status_from_flight_error_preserves_tonic_status() {
        let tonic_status = Status::permission_denied("denied");
        let mapped =
            SwanFlightSqlService::status_from_flight_error(FlightError::from(tonic_status.clone()));
        assert_eq!(mapped.code(), Code::PermissionDenied);
        assert_eq!(mapped.message(), tonic_status.message());

        let decode_status = SwanFlightSqlService::status_from_flight_error(
            FlightError::DecodeError("bad payload".to_string()),
        );
        assert_eq!(decode_status.code(), Code::Internal);
        assert!(decode_status.message().contains("decode"));
    }

    #[test]
    fn into_stream_yields_all_batches() {
        let mut stream = SwanFlightSqlService::into_stream(vec![
            FlightData::default(),
            FlightData::default(),
            FlightData::default(),
        ]);

        let emitted = futures::executor::block_on(async {
            let mut count = 0usize;
            while let Some(item) = stream.next().await {
                assert!(item.is_ok());
                count += 1;
            }
            count
        });

        assert_eq!(emitted, 3);
    }
}
