use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use axum::{extract::State, response::Html, routing::get, Json, Router};
use serde::Serialize;

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

pub async fn spawn_status_server(
    config: &ServerConfig,
    metrics: Arc<Metrics>,
    registry: Arc<SessionRegistry>,
) -> Result<()> {
    if !config.status_enabled {
        return Ok(());
    }

    let addr: SocketAddr = format!("{}:{}", config.status_host, config.status_port)
        .parse()
        .with_context(|| "invalid status server bind address")?;

    let state = StatusState { metrics, registry };
    let app = Router::new()
        .route("/", get(status_page))
        .route("/status.json", get(status_json))
        .with_state(state);

    tokio::spawn(async move {
        match tokio::net::TcpListener::bind(addr).await {
            Ok(listener) => {
                if let Err(err) = axum::serve(listener, app).await {
                    tracing::error!(%err, "status server failed");
                }
            }
            Err(err) => {
                tracing::error!(%err, "status server bind failed");
            }
        }
    });

    tracing::info!(%addr, "status server listening");
    Ok(())
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

const STATUS_PAGE: &str = include_str!("status.html");
