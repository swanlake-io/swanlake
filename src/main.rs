use std::sync::Arc;

use crate::config::ServerConfig;
use crate::dq::QueueManager;
use crate::service::SwanFlightSqlService;
use anyhow::{Context, Result};
use tonic::transport::Server;

use tracing::info;
use tracing_subscriber::{fmt::format::FmtSpan, EnvFilter};

mod config;
mod dq;
mod engine;
mod error;
mod lock;
mod service;
mod session;
mod sql_parser;
mod types;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    let config = ServerConfig::load().context("failed to load configuration")?;
    init_tracing(&config);
    info!("service config:\n{:?}", config);
    let addr = config
        .bind_addr()
        .context("failed to resolve bind address")?;

    let dq_manager = Arc::new(
        QueueManager::new(&config)
            .await
            .context("failed to initialize duckling queue manager")?,
    );

    // Create session registry (Phase 2: connection-based session persistence)
    let registry = Arc::new(
        crate::session::registry::SessionRegistry::new(&config, Some(dq_manager.clone()))
            .context("failed to initialize session registry")?,
    );

    // Initialize the QueueRuntime to start background tasks for queue rotation, scanning, flushing, and cleanup.
    let dq_runtime = Arc::new(dq::QueueRuntime::new(
        dq_manager.clone(),
        registry.engine_factory(),
        registry.clone(),
    ));

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

    // Pass dq_runtime to the service to keep the QueueRuntime alive throughout the server's lifetime.
    let flight_service = SwanFlightSqlService::new(registry, Some(dq_runtime));

    // Set up gRPC health service
    let (health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter.set_serving::<arrow_flight::flight_service_server::FlightServiceServer<SwanFlightSqlService>>().await;

    info!(%addr, "starting SwanLake Flight SQL server");

    // Set up graceful shutdown
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    tokio::spawn(async move {
        let ctrl_c = async {
            tokio::signal::ctrl_c()
                .await
                .expect("failed to install CTRL+C handler");
        };

        #[cfg(unix)]
        let terminate = async {
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("failed to install SIGTERM handler")
                .recv()
                .await;
        };

        #[cfg(not(unix))]
        let terminate = std::future::pending::<()>();

        tokio::select! {
            _ = ctrl_c => {
                info!("received SIGINT, initiating graceful shutdown");
            }
            _ = terminate => {
                info!("received SIGTERM, initiating graceful shutdown");
            }
        }

        // Set health status to NOT_SERVING before shutdown
        health_reporter.set_not_serving::<arrow_flight::flight_service_server::FlightServiceServer<SwanFlightSqlService>>().await;

        let _ = shutdown_tx.send(());
    });

    Server::builder()
        .add_service(health_service)
        .add_service(arrow_flight::flight_service_server::FlightServiceServer::new(flight_service))
        .serve_with_shutdown(addr, async {
            shutdown_rx.await.ok();
        })
        .await
        .context("Flight SQL server terminated unexpectedly")?;

    info!("server shutdown complete");
    // Locks held by dq_manager are released here as it goes out of scope
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
