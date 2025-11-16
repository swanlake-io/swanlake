use std::sync::{Arc, Mutex};

use tracing::{info, warn};

use crate::config::ServerConfig;
use crate::engine::{DuckDbConnection, EngineFactory};
use crate::error::ServerError;

const DUCKDB_UI_PORT: u16 = 4213;

/// Keeps the DuckDB UI server alive for the lifetime of the process.
pub struct UiServerGuard {
    connection: DuckDbConnection,
}

impl UiServerGuard {
    fn start(connection: DuckDbConnection) -> Result<Self, ServerError> {
        connection.execute_statement(&format!(
            "SET ui_local_port = {};CALL start_ui_server();",
            DUCKDB_UI_PORT
        ))?;
        info!(port = DUCKDB_UI_PORT, "started DuckDB UI server");
        Ok(Self { connection })
    }
}

impl Drop for UiServerGuard {
    fn drop(&mut self) {
        if let Err(err) = self.connection.execute_statement("CALL stop_ui_server();") {
            warn!(%err, "failed to stop DuckDB UI server cleanly");
        } else {
            info!("stopped DuckDB UI server");
        }
    }
}

pub fn maybe_start_ui_server(
    config: &ServerConfig,
    factory: Arc<Mutex<EngineFactory>>,
) -> Result<Option<UiServerGuard>, ServerError> {
    if !config.duckdb_ui_server_enabled {
        return Ok(None);
    }

    let connection = factory
        .lock()
        .expect("engine factory mutex poisoned")
        .create_connection()?;
    UiServerGuard::start(connection).map(Some)
}
