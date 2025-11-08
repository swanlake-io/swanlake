//! Factory for creating initialized DuckDB connections.
//!
//! Each connection is created with the same configuration and initialization SQL
//! (extensions, ATTACH statements, etc.).

use std::sync::Arc;

use duckdb::{Config, Connection};
use tracing::{info, instrument};

use crate::config::ServerConfig;
use crate::dq::DucklingQueueManager;
use crate::engine::connection::DuckDbConnection;
use crate::error::ServerError;

/// Factory for creating initialized DuckDB connections
#[derive(Clone)]
pub struct EngineFactory {
    base_init_sql: String,
    dq_manager: Option<Arc<DucklingQueueManager>>,
}

impl EngineFactory {
    /// Create a new factory from configuration
    #[instrument(skip(config, dq_manager))]
    pub fn new(
        config: &ServerConfig,
        dq_manager: Option<Arc<DucklingQueueManager>>,
    ) -> Result<Self, ServerError> {
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
            base_init_sql: init_sql,
            dq_manager,
        })
    }

    /// Create a new initialized DuckDB connection
    #[instrument(skip(self))]
    pub fn create_connection(&self) -> Result<DuckDbConnection, ServerError> {
        let conn = self.create_raw_connection()?;
        info!("DuckDB connection created and initialized");
        Ok(DuckDbConnection::new(conn))
    }

    /// Create a raw DuckDB connection with initialization SQL executed.
    pub fn create_raw_connection(&self) -> Result<Connection, ServerError> {
        let conn = Connection::open_in_memory_with_flags(
            Config::default()
                .enable_autoload_extension(true)?
                .allow_unsigned_extensions()?,
        )?;

        // Execute initialization SQL (extensions, ATTACH statements, etc.)
        let mut init_sql = self.base_init_sql.clone();

        if let Some(manager) = &self.dq_manager {
            let attach_sql = manager.attach_sql();
            if !attach_sql.is_empty() {
                if !init_sql.is_empty() {
                    init_sql.push('\n');
                }
                init_sql.push_str(&attach_sql);
            }
        }

        if !init_sql.is_empty() {
            info!(
                "Initializing connection with extensions and config, {}",
                init_sql
            );
            conn.execute_batch(&init_sql)?;
        }

        Ok(conn)
    }
}
