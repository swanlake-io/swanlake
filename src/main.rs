use std::sync::Arc;

use crate::config::ServerConfig;
use crate::service::SwanFlightSqlService;
use anyhow::{Context, Result};
use tonic::transport::Server;
use tracing::info;
use tracing_subscriber::{fmt::format::FmtSpan, EnvFilter};

mod config;
mod engine;
mod error;
mod service;
mod session;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    let config = ServerConfig::load().context("failed to load configuration")?;
    init_tracing(&config);
    info!("service config:\n{:?}", config);
    let addr = config
        .bind_addr()
        .context("failed to resolve bind address")?;

    // Create session registry (Phase 2: connection-based session persistence)
    let registry = Arc::new(
        crate::session::registry::SessionRegistry::new(&config)
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

    let flight_service = SwanFlightSqlService::new(registry);

    info!(%addr, "starting SwanLake Flight SQL server");

    Server::builder()
        .add_service(arrow_flight::flight_service_server::FlightServiceServer::new(flight_service))
        .serve(addr)
        .await
        .context("Flight SQL server terminated unexpectedly")
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
            .with_ansi(config.log_ansi)
            .init();
    } else {
        tracing_subscriber::fmt()
            .compact()
            .with_env_filter(filter)
            .with_target(false)
            .with_file(true)
            .with_line_number(true)
            .with_span_events(FmtSpan::ENTER | FmtSpan::CLOSE)
            .with_ansi(config.log_ansi)
            .init();
    }
}
