//! Factory for creating initialized DuckDB connections.
//!
//! Each connection is created with the same configuration and initialization SQL
//! (extensions, ATTACH statements, etc.).

use duckdb::{Config, Connection};
use tracing::{info, instrument};

use crate::config::ServerConfig;
use crate::engine::connection::DuckDbConnection;
use crate::error::ServerError;

/// Factory for creating initialized DuckDB connections
#[derive(Clone)]
pub struct EngineFactory {
    init_sql: String,
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

        Ok(Self { init_sql })
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

        info!("DuckDB connection created and initialized");
        Ok(DuckDbConnection::new(conn))
    }
}
