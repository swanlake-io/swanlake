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

pub fn spawn_status_server(
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

    let prefix = normalize_prefix(&config.status_path_prefix);
    let root_path = format!("{prefix}/");
    let json_path = format!("{prefix}/status.json");

    let app = Router::new()
        .route(&root_path, get(status_page))
        .route(&json_path, get(status_json))
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

fn normalize_prefix(prefix: &str) -> String {
    let trimmed = prefix.trim_matches('/');
    if trimmed.is_empty() {
        String::new()
    } else {
        format!("/{trimmed}")
    }
}

const STATUS_PAGE: &str = include_str!("status.html");

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use anyhow::{anyhow, Result};
    use axum::extract::State;

    use super::*;
    use swanlake_core::engine::EngineFactory;

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

    #[test]
    fn spawn_status_server_is_noop_when_disabled() -> Result<()> {
        let config = ServerConfig {
            status_enabled: false,
            status_host: "not-a-valid-host:".to_string(),
            ..ServerConfig::default()
        };
        let metrics = Arc::new(Metrics::new(32, 8));
        let registry = build_registry(2, 60)?;
        spawn_status_server(&config, metrics, registry)?;
        Ok(())
    }

    #[test]
    fn spawn_status_server_validates_bind_address_when_enabled() -> Result<()> {
        let config = ServerConfig {
            status_enabled: true,
            status_host: "invalid host".to_string(),
            status_port: 9999,
            ..ServerConfig::default()
        };
        let metrics = Arc::new(Metrics::new(32, 8));
        let registry = build_registry(2, 60)?;
        let err = spawn_status_server(&config, metrics, registry)
            .err()
            .ok_or_else(|| anyhow!("expected invalid bind address error"))?;
        assert!(err
            .to_string()
            .contains("invalid status server bind address"));
        Ok(())
    }
}
