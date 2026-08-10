use std::sync::Arc;

use anyhow::{Context, Result};
use swanlake_core::config::ServerConfig;
use swanlake_core::engine::EngineFactory;
use swanlake_core::maintenance::CheckpointService;
use swanlake_core::metrics::Metrics;
use swanlake_core::service::SwanFlightSqlService;
use tonic::transport::Server;

use tracing::info;
use tracing_subscriber::{fmt::format::FmtSpan, EnvFilter};

mod status;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    let config = ServerConfig::load().context("failed to load configuration")?;
    init_tracing(&config);
    info!("service config:\n{:?}", config);
    let addr = config
        .bind_addr()
        .context("failed to resolve bind address")?;

    let factory =
        Arc::new(EngineFactory::new(&config).context("failed to initialize engine factory")?);

    // Spawn DuckLake checkpoint maintenance task
    CheckpointService::spawn_from_config(&config, factory.clone())
        .await
        .context("failed to start checkpoint service")?;

    // Create session registry (Phase 2: connection-based session persistence)
    let registry = Arc::new(
        swanlake_core::session::registry::SessionRegistry::new(&config, factory.clone())
            .context("failed to initialize session registry")?,
    );

    // Spawn periodic session cleanup task
    let registry_clone = registry.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(300)); // 5 minutes
        loop {
            interval.tick().await;
            let removed = registry_clone.cleanup_idle_sessions();
            if removed > 0 {
                info!(removed, "cleaned up idle sessions");
            }
        }
    });

    let metrics = Arc::new(Metrics::new(
        config.metrics_slow_query_threshold_ms.unwrap_or(5000),
        config.metrics_history_size.unwrap_or(200),
    ));

    let flight_service = SwanFlightSqlService::new(
        registry.clone(),
        metrics.clone(),
        config.session_id_mode.clone(),
    );

    // Receiver fires if the status server fails at runtime (after a successful
    // bind); bind failures are already fatal via `?`.
    let status_failure_rx = status::spawn_status_server(&config, metrics, registry.clone()).await?;

    // Set up gRPC health service
    let (health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter.set_serving::<arrow_flight::flight_service_server::FlightServiceServer<SwanFlightSqlService>>().await;

    info!(%addr, "starting SwanLake Flight SQL server");

    // Set up graceful shutdown
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    tokio::spawn(async move {
        let ctrl_c = async {
            if let Err(err) = tokio::signal::ctrl_c().await {
                tracing::error!(%err, "failed to install CTRL+C handler");
                std::future::pending::<()>().await;
            }
        };

        #[cfg(unix)]
        let terminate = async {
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(mut signal) => {
                    signal.recv().await;
                }
                Err(err) => {
                    tracing::error!(%err, "failed to install SIGTERM handler");
                    std::future::pending::<()>().await;
                }
            }
        };

        #[cfg(not(unix))]
        let terminate = std::future::pending::<()>();

        tokio::select! {
            () = ctrl_c => {
                info!("received SIGINT, initiating graceful shutdown");
            }
            () = terminate => {
                info!("received SIGTERM, initiating graceful shutdown");
            }
        }

        // Set health status to NOT_SERVING before shutdown
        health_reporter.set_not_serving::<arrow_flight::flight_service_server::FlightServiceServer<SwanFlightSqlService>>().await;

        let _ = shutdown_tx.send(());
    });

    // Resolves with an error if the status server dies at runtime; pending
    // forever when the status server is disabled.
    let status_failed = await_status_failure(status_failure_rx);

    let mut fatal_err: Option<anyhow::Error> = None;
    Server::builder()
        .add_service(health_service)
        .add_service(arrow_flight::flight_service_server::FlightServiceServer::new(flight_service))
        .serve_with_shutdown(addr, async {
            fatal_err = shutdown_signal(shutdown_rx, status_failed).await;
        })
        .await
        .context("Flight SQL server terminated unexpectedly")?;

    finalize(fatal_err)
}

/// Awaits a runtime failure from the status server.
///
/// Resolves with the reported error if the status server dies after binding,
/// or a synthetic error if its task panicked (sender dropped). When the status
/// server is disabled (`None`), this never resolves so the caller's `select!`
/// relies solely on the shutdown signal.
async fn await_status_failure(
    rx: Option<tokio::sync::oneshot::Receiver<anyhow::Error>>,
) -> anyhow::Error {
    match rx {
        Some(rx) => match rx.await {
            Ok(err) => err,
            // Sender dropped without a message: the status task panicked.
            Err(_) => anyhow::anyhow!("status server task terminated unexpectedly"),
        },
        None => std::future::pending().await,
    }
}

/// Drives the gRPC server's shutdown future. Returns `Some(err)` when the
/// status server failed at runtime (triggering an abnormal shutdown), or
/// `None` on a normal signal-driven shutdown.
async fn shutdown_signal(
    shutdown_rx: tokio::sync::oneshot::Receiver<()>,
    status_failed: impl std::future::Future<Output = anyhow::Error>,
) -> Option<anyhow::Error> {
    tokio::select! {
        _ = shutdown_rx => None,
        err = status_failed => {
            tracing::error!(%err, "status server failed at runtime, shutting down");
            Some(err)
        }
    }
}

/// Converts the optional fatal error captured during shutdown into the
/// process exit result.
fn finalize(fatal_err: Option<anyhow::Error>) -> Result<()> {
    if let Some(err) = fatal_err {
        return Err(err.context("shutting down: status/health endpoints became unavailable"));
    }

    info!("server shutdown complete");
    Ok(())
}

fn init_tracing(config: &ServerConfig) {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,swanlake::service=debug"));

    if config.log_format == "json" {
        tracing_subscriber::fmt()
            .json()
            .with_env_filter(filter)
            .with_target(false)
            .with_file(true)
            .with_line_number(true)
            .with_span_events(FmtSpan::ENTER | FmtSpan::CLOSE)
            .init();
    } else {
        tracing_subscriber::fmt()
            .compact()
            .with_env_filter(filter)
            .with_target(false)
            .with_file(true)
            .with_line_number(true)
            .with_span_events(FmtSpan::ENTER | FmtSpan::CLOSE)
            .init();
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::sync::oneshot;

    use super::*;

    #[tokio::test]
    async fn await_status_failure_returns_received_error() {
        let (tx, rx) = oneshot::channel();
        tx.send(anyhow::anyhow!("boom")).expect("send failed");
        let err = await_status_failure(Some(rx)).await;
        let msg = err.to_string();
        assert!(msg.contains("boom"));
    }

    #[tokio::test]
    async fn await_status_failure_reports_panic_when_sender_dropped() {
        let (tx, rx) = oneshot::channel::<anyhow::Error>();
        drop(tx);
        let err = await_status_failure(Some(rx)).await;
        let msg = err.to_string();
        assert!(msg.contains("terminated unexpectedly"));
    }

    #[tokio::test]
    async fn await_status_failure_pends_forever_when_none() {
        let result =
            tokio::time::timeout(Duration::from_millis(50), await_status_failure(None)).await;
        assert!(result.is_err(), "None variant should never resolve");
    }

    #[tokio::test]
    async fn shutdown_signal_returns_none_on_normal_shutdown() {
        let (tx, rx) = oneshot::channel::<()>();
        tx.send(()).expect("send failed");
        let never = std::future::pending::<anyhow::Error>();
        assert!(shutdown_signal(rx, never).await.is_none());
    }

    #[tokio::test]
    async fn shutdown_signal_returns_error_on_status_failure() {
        // Shutdown signal that never fires, so the status-failure arm wins.
        let (_tx, rx) = oneshot::channel::<()>();
        let failed = async { anyhow::anyhow!("status down") };
        let outcome = shutdown_signal(rx, failed).await;
        let msg = outcome.expect("expected error").to_string();
        assert!(msg.contains("status down"));
    }

    #[test]
    fn finalize_maps_fatal_error() {
        let err = finalize(Some(anyhow::anyhow!("dead"))).expect_err("expected error");
        let msg = err.to_string();
        assert!(msg.contains("endpoints became unavailable"));
    }

    #[test]
    fn finalize_ok_when_no_error() {
        assert!(finalize(None).is_ok());
    }
}
