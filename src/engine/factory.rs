//! Factory for creating initialized DuckDB connections.
//!
//! Each connection is created with the same configuration and initialization SQL
//! (extensions, ATTACH statements, etc.).

use std::sync::{Arc, Mutex};

use duckdb::{Config, Connection};
use tracing::{info, instrument};

use crate::config::ServerConfig;
use crate::engine::connection::DuckDbConnection;
use crate::error::ServerError;

/// Factory for creating initialized DuckDB connections
#[derive(Clone)]
pub struct EngineFactory {
    init_sql: String,
    enable_ui_server: bool,
    ui_server_started: Arc<Mutex<bool>>,
}

impl EngineFactory {
    /// Create a new factory from configuration
    #[instrument(skip(config))]
    pub fn new(config: &ServerConfig) -> Result<Self, ServerError> {
        let mut init_statements = Vec::new();

        // Build initialization SQL based on config
        if config.ducklake_enable {
            info!("DuckLake extension enabled");
            init_statements.push(
                "INSTALL ducklake; INSTALL httpfs; INSTALL aws; INSTALL postgres; \
                 LOAD ducklake; LOAD httpfs; LOAD aws; LOAD postgres;"
                    .to_string(),
            );

            // Add user-provided init SQL
            if let Some(sql) = config.ducklake_init_sql.as_ref() {
                let trimmed = sql.trim();
                if !trimmed.is_empty() {
                    info!("Adding ducklake init SQL");
                    init_statements.push(trimmed.to_string());
                }
            }
        } else {
            info!("DuckLake extension disabled via configuration");
        }

        let init_sql = init_statements.join("\n");

        Ok(Self {
            init_sql,
            enable_ui_server: config.enable_ui_server,
            ui_server_started: Arc::new(Mutex::new(false)),
        })
    }

    /// Create a new initialized DuckDB connection
    #[instrument(skip(self))]
    pub fn create_connection(&self) -> Result<DuckDbConnection, ServerError> {
        // Create in-memory connection with extensions enabled
        let conn = Connection::open_in_memory_with_flags(
            Config::default()
                .enable_autoload_extension(true)?
                .allow_unsigned_extensions()?,
        )?;

        // Execute initialization SQL (extensions, ATTACH statements, etc.)
        if !self.init_sql.is_empty() {
            info!("Initializing connection with extensions and config");
            conn.execute_batch(&self.init_sql)?;
        }

        // Start UI server once (first connection only)
        if self.enable_ui_server {
            let mut started = self
                .ui_server_started
                .lock()
                .expect("ui_server_started mutex poisoned");
            if !*started {
                info!("Starting UI server on first connection: http://localhost:4213");
                conn.execute_batch("CALL start_ui_server();")?;
                *started = true;
            }
        }

        info!("DuckDB connection created and initialized");
        Ok(DuckDbConnection::new(conn))
    }
}
