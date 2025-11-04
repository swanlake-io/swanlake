use std::env;
use std::path::PathBuf;
use std::sync::Arc;

use crate::config::ServerConfig;
use crate::duckdb::DuckDbEngine;
use crate::service::SwanFlightSqlService;
use anyhow::{bail, Context, Result};
use tonic::transport::Server;
use tracing::info;
use tracing_subscriber::{fmt::format::FmtSpan, EnvFilter};

mod config;
mod duckdb;
mod error;
mod service;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    init_tracing();

    let config_path = parse_args()?;
    let config = ServerConfig::load(&config_path).context("failed to load configuration")?;
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
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,swandb::service=debug,swandb::duckdb=debug"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_file(true)
        .with_line_number(true)
        .with_span_events(FmtSpan::ENTER | FmtSpan::CLOSE)
        .compact()
        .init();
}

fn parse_args() -> Result<PathBuf> {
    let mut args = env::args().skip(1);
    let mut config_path = PathBuf::from("config.toml");

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" | "-c" => {
                let path = args.next().context("--config requires a path argument")?;
                config_path = PathBuf::from(path);
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
