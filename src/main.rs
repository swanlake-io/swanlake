mod config;
mod duckdb;
mod error;
mod service;

use std::env;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use config::ServerConfig;
use duckdb::DuckDbEngine;
use service::SwanFlightSqlService;
use tonic::transport::Server;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    init_tracing();

    let config_path = parse_args()?;
    let config =
        ServerConfig::load(config_path.as_deref()).context("failed to load configuration")?;
    let addr = config
        .bind_addr()
        .context("failed to resolve bind address")?;

    let engine = Arc::new(DuckDbEngine::new(&config).context("failed to initialize DuckDB")?);
    let flight_service = SwanFlightSqlService::new(engine);

    info!(%addr, "starting SwanDB Flight SQL server");

    Server::builder()
        .add_service(arrow_flight::flight_service_server::FlightServiceServer::new(flight_service))
        .serve(addr)
        .await
        .context("Flight SQL server terminated unexpectedly")
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .compact()
        .init();
}

fn parse_args() -> Result<Option<PathBuf>> {
    let mut args = env::args().skip(1);
    let mut config_path = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" | "-c" => {
                let path = args.next().context("--config requires a path argument")?;
                config_path = Some(PathBuf::from(path));
            }
            "--help" | "-h" => {
                println!("Usage: swandb [--config <path>]");
                std::process::exit(0);
            }
            other => bail!("unknown argument: {other}"),
        }
    }

    Ok(config_path)
}
