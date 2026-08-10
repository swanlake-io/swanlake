use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use axum::{extract::State, response::Html, routing::get, Json, Router};
use serde::Serialize;
use tokio::sync::oneshot;

use swanlake_core::config::ServerConfig;
use swanlake_core::metrics::{Metrics, MetricsSnapshot};
use swanlake_core::session::registry::{SessionRegistry, SessionRegistrySnapshot};

#[derive(Clone)]
struct StatusState {
    metrics: Arc<Metrics>,
    registry: Arc<SessionRegistry>,
}

#[derive(Serialize)]
struct StatusPayload {
    generated_at_ms: u64,
    metrics: MetricsSnapshot,
    sessions: SessionRegistrySnapshot,
}

/// Spawns the status HTTP server.
///
/// Returns `Ok(None)` when the status server is disabled. When enabled,
/// waits for the listener to bind (bind failure is fatal and returned as
/// an error), then returns `Ok(Some(receiver))`. The receiver fires if the
/// status server later fails at runtime, so the caller can react (e.g.
/// shut down) instead of silently losing health/metrics endpoints.
pub async fn spawn_status_server(
    config: &ServerConfig,
    metrics: Arc<Metrics>,
    registry: Arc<SessionRegistry>,
) -> Result<Option<oneshot::Receiver<anyhow::Error>>> {
    if !config.status_enabled {
        return Ok(None);
    }

    let addr: SocketAddr = format!("{}:{}", config.status_host, config.status_port)
        .parse()
        .with_context(|| "invalid status server bind address")?;

    let state = StatusState { metrics, registry };

    let prefix = normalize_prefix(&config.status_path_prefix);
    let root_path = format!("{prefix}/");
    let json_path = format!("{prefix}/status.json");
    let app = Router::new()
        .route(&root_path, get(status_page))
        .route(&json_path, get(status_json))
        .route("/healthz", get(healthz))
        .with_state(state);

    let (bind_tx, bind_rx) = oneshot::channel();
    let (failure_tx, failure_rx) = oneshot::channel();

    tokio::spawn(run_listener(
        addr,
        bind_tx,
        failure_tx,
        move |listener| async move { axum::serve(listener, app).await },
    ));

    resolve_bind(addr, bind_rx.await, failure_rx)
}

async fn status_page() -> Html<&'static str> {
    Html(STATUS_PAGE)
}

async fn status_json(State(state): State<StatusState>) -> Json<StatusPayload> {
    let payload = StatusPayload {
        generated_at_ms: now_millis(),
        metrics: state.metrics.snapshot(),
        sessions: state.registry.snapshot(),
    };
    Json(payload)
}

fn now_millis() -> u64 {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_millis() as u64
}

fn normalize_prefix(prefix: &str) -> String {
    let trimmed = prefix.trim_matches('/');
    if trimmed.is_empty() {
        String::new()
    } else {
        format!("/{trimmed}")
    }
}

/// Builds the fatal error describing why the status `serve` loop ended.
/// `axum::serve` normally runs forever, so both arms are abnormal.
fn serve_outcome(result: std::io::Result<()>) -> anyhow::Error {
    match result {
        Ok(()) => anyhow!("status server exited unexpectedly"),
        Err(err) => anyhow!("status server failed: {err}"),
    }
}

/// Builds the fatal error returned when the listener fails to bind.
fn bind_failure(err: std::io::Error) -> anyhow::Error {
    anyhow!("status server bind failed: {err}")
}

/// Result of awaiting the listener task's bind notification.
type BindOutcome = std::result::Result<Result<()>, oneshot::error::RecvError>;

/// Drives the status server's listener task: binds the socket, reports the bind
/// result to the caller, then waits for the (normally infinite) serve future to
/// return — any return is treated as a fatal runtime failure. The `serve`
/// closure is injected so tests can drive the post-serve path deterministically.
async fn run_listener<F, Fut>(
    addr: SocketAddr,
    bind_tx: oneshot::Sender<Result<()>>,
    failure_tx: oneshot::Sender<anyhow::Error>,
    serve: F,
) where
    F: FnOnce(tokio::net::TcpListener) -> Fut,
    Fut: std::future::Future<Output = std::io::Result<()>>,
{
    match tokio::net::TcpListener::bind(addr).await {
        Ok(listener) => {
            // Notify caller that bind succeeded before entering serve loop
            let _ = bind_tx.send(Ok(()));
            // `axum::serve` normally runs forever; any return is abnormal.
            let outcome = serve_outcome(serve(listener).await);
            tracing::error!(error = %outcome, "status server stopped serving");
            let _ = failure_tx.send(outcome);
        }
        Err(err) => {
            let _ = bind_tx.send(Err(bind_failure(err)));
        }
    }
}

/// Translates the listener task's bind notification into the spawn result.
fn resolve_bind(
    addr: SocketAddr,
    bind_result: BindOutcome,
    failure_rx: oneshot::Receiver<anyhow::Error>,
) -> Result<Option<oneshot::Receiver<anyhow::Error>>> {
    match bind_result {
        Ok(Ok(())) => {
            tracing::info!(%addr, "status server listening");
            Ok(Some(failure_rx))
        }
        Ok(Err(err)) => Err(err),
        Err(_) => Err(anyhow!("status server task panicked before binding")),
    }
}

const STATUS_PAGE: &str = include_str!("status.html");

async fn healthz() -> &'static str {
    "OK"
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use anyhow::{anyhow, Ok, Result};
    use axum::extract::State;

    use super::*;
    use swanlake_core::{engine::EngineFactory, session::SessionId};

    fn build_registry(max_sessions: usize, timeout_secs: u64) -> Result<Arc<SessionRegistry>> {
        let config = ServerConfig {
            max_sessions: Some(max_sessions),
            session_timeout_seconds: Some(timeout_secs),
            ..ServerConfig::default()
        };
        let factory = Arc::new(EngineFactory::new(&config).map_err(|e| anyhow!(e.to_string()))?);
        let registry =
            SessionRegistry::new(&config, factory).map_err(|e| anyhow!(e.to_string()))?;
        Ok(Arc::new(registry))
    }

    #[test]
    fn normalize_prefix_handles_empty_and_non_empty_inputs() {
        assert_eq!(normalize_prefix(""), "");
        assert_eq!(normalize_prefix("/"), "");
        assert_eq!(normalize_prefix("status"), "/status");
        assert_eq!(normalize_prefix("/status/"), "/status");
    }

    #[tokio::test]
    async fn status_page_returns_embedded_html() {
        let page = status_page().await;
        assert!(page.0.contains("<html"));
    }

    #[tokio::test]
    async fn status_json_returns_metrics_and_session_snapshots() -> Result<()> {
        let metrics = Arc::new(Metrics::new(128, 32));
        let registry = build_registry(7, 600)?;
        let state = StatusState { metrics, registry };

        let Json(payload) = status_json(State(state)).await;
        assert!(payload.generated_at_ms > 0);
        assert_eq!(payload.metrics.slow_query_threshold_ms, 128);
        assert_eq!(payload.metrics.history_size, 32);
        assert_eq!(payload.sessions.max_sessions, 7);
        Ok(())
    }

    #[tokio::test]
    async fn spawn_status_server_is_noop_when_disabled() -> Result<()> {
        let config = ServerConfig {
            status_enabled: false,
            status_host: "not-a-valid-host:".to_string(),
            ..ServerConfig::default()
        };
        let metrics = Arc::new(Metrics::new(32, 8));
        let registry = build_registry(2, 60)?;
        let handle = spawn_status_server(&config, metrics, registry).await?;
        assert!(
            handle.is_none(),
            "disabled status server should return None"
        );
        Ok(())
    }

    #[tokio::test]
    async fn spawn_status_server_validates_bind_address_when_enabled() -> Result<()> {
        let config = ServerConfig {
            status_enabled: true,
            status_host: "invalid host".to_string(),
            status_port: 9999,
            ..ServerConfig::default()
        };
        let metrics = Arc::new(Metrics::new(32, 8));
        let registry = build_registry(2, 60)?;
        let err = spawn_status_server(&config, metrics, registry)
            .await
            .err()
            .ok_or_else(|| anyhow!("expected invalid bind address error"))?;
        assert!(err
            .to_string()
            .contains("invalid status server bind address"));
        Ok(())
    }

    #[tokio::test]
    async fn status_with_custom_prefix() -> Result<()> {
        let config = ServerConfig {
            status_enabled: true,
            status_path_prefix: "/admin".to_string(),
            status_host: "0.0.0.0".to_string(),
            status_port: 0,
            ..ServerConfig::default()
        };
        // Test that routes are properly configured with prefix
        let prefix = normalize_prefix(&config.status_path_prefix);
        assert_eq!(prefix, "/admin");

        // Verify root and json paths
        let root_path = format!("{prefix}/");
        let json_path = format!("{prefix}/status.json");
        assert_eq!(root_path, "/admin/");
        assert_eq!(json_path, "/admin/status.json");

        Ok(())
    }
    #[tokio::test]
    async fn status_server_config_changes() -> Result<()> {
        // Test with disabled status server
        let config_disabled = ServerConfig {
            status_enabled: false,
            ..ServerConfig::default()
        };

        let metrics = Arc::new(Metrics::new(32, 8));
        let registry = build_registry(2, 60)?;

        //should return early without error when disabled
        let handle =
            spawn_status_server(&config_disabled, metrics.clone(), registry.clone()).await?;
        assert!(
            handle.is_none(),
            "disabled status server should return None"
        );

        // Test with enabled but invalid config
        let config_invalid = ServerConfig {
            status_enabled: true,
            status_host: "invalid-host".to_string(),
            status_port: 9999,
            ..ServerConfig::default()
        };

        // Should return error for invalid bind address
        let result = spawn_status_server(&config_invalid, metrics, registry).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("invalid status server bind address"));

        Ok(())
    }

    #[tokio::test]
    async fn status_json_includes_all_session_fields() -> Result<()> {
        let metrics = Arc::new(Metrics::new(128, 32));
        let registry = build_registry(7, 600)?;
        let state = StatusState { metrics, registry };

        //create session to test non-zero values
        let session_id = SessionId::from_string("test".to_string());
        let _session = state.registry.get_or_create_by_id(&session_id).await?;

        let Json(payload) = status_json(State(state)).await;

        // Validate session fields
        assert_eq!(payload.sessions.total_sessions, 1);
        assert_eq!(payload.sessions.max_sessions, 7);
        assert_eq!(payload.sessions.session_timeout_seconds, 600);
        assert!(payload.sessions.oldest_idle_ms < 1000);
        assert!(payload.sessions.average_idle_ms < 1000);

        Ok(())
    }

    #[test]
    fn serve_outcome_maps_both_arms() {
        let exited: std::io::Result<()> = std::result::Result::Ok(());
        let exited_msg = serve_outcome(exited).to_string();
        assert!(exited_msg.contains("exited unexpectedly"));

        let failed: std::io::Result<()> = std::result::Result::Err(std::io::Error::other("boom"));
        let failed_msg = serve_outcome(failed).to_string();
        assert!(failed_msg.contains("status server failed"));
    }

    #[test]
    fn bind_failure_describes_error() {
        let msg = bind_failure(std::io::Error::other("in use")).to_string();
        assert!(msg.contains("status server bind failed"));
    }

    #[tokio::test]
    async fn run_listener_reports_serve_exit() -> Result<()> {
        let addr: SocketAddr = "127.0.0.1:0".parse()?;
        let (bind_tx, bind_rx) = oneshot::channel();
        let (failure_tx, failure_rx) = oneshot::channel();

        // Inject a serve future that returns immediately, exercising the
        // post-serve failure path (log + notify caller).
        run_listener(addr, bind_tx, failure_tx, |_listener| async {
            std::result::Result::<(), std::io::Error>::Ok(())
        })
        .await;

        assert!(bind_rx.await.is_ok());
        let failure = failure_rx.await.map_err(|e| anyhow!(e))?;
        let msg = failure.to_string();
        assert!(msg.contains("exited unexpectedly"));

        Ok(())
    }

    #[tokio::test]
    async fn resolve_bind_reports_task_panic() -> Result<()> {
        let addr: SocketAddr = "127.0.0.1:0".parse()?;
        let (_failure_tx, failure_rx) = oneshot::channel::<anyhow::Error>();

        // Listener task dropped the bind sender without sending: simulates a
        // panic before binding, surfaced to the caller as a RecvError.
        let (bind_tx, bind_rx) = oneshot::channel::<Result<()>>();
        drop(bind_tx);
        let bind_result = bind_rx.await;

        let outcome = resolve_bind(addr, bind_result, failure_rx);
        let err = outcome
            .err()
            .ok_or_else(|| anyhow!("expected task-panic error"))?;
        let msg = err.to_string();
        assert!(msg.contains("panicked before binding"));

        Ok(())
    }

    #[tokio::test]
    async fn spawn_status_server_reports_bind_conflict() -> Result<()> {
        // Hold a port open so the status server's listener bind fails.
        let blocker = std::net::TcpListener::bind("127.0.0.1:0")?;
        let port = blocker.local_addr()?.port();

        let config = ServerConfig {
            status_enabled: true,
            status_host: "127.0.0.1".to_string(),
            status_port: port,
            ..ServerConfig::default()
        };
        let metrics = Arc::new(Metrics::new(32, 8));
        let registry = build_registry(2, 60)?;

        let err = spawn_status_server(&config, metrics, registry)
            .await
            .err()
            .ok_or_else(|| anyhow!("expected bind conflict error"))?;
        let msg = err.to_string();
        assert!(msg.contains("status server bind failed"));

        Ok(())
    }
}
