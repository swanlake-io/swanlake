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
        init_statements.push(
            "INSTALL ducklake; INSTALL httpfs; INSTALL aws; INSTALL postgres; \
            LOAD ducklake; LOAD httpfs; LOAD aws; LOAD postgres;"
                .to_string(),
        );
        if let Some(sql) = config.ducklake_init_sql.as_ref() {
            let trimmed = sql.trim();
            if !trimmed.is_empty() {
                info!("Adding ducklake init SQL");
                init_statements.push(trimmed.to_string());
            }
        }

        let init_sql = init_statements.join("\n");
        info!("base init sql {}", init_sql);

        Ok(Self { init_sql })
    }

    /// Create a new initialized DuckDB connection
    ///
    /// Each connection is created fresh with its own in-memory database.
    /// This ensures complete isolation between sessions.
    #[instrument(skip(self))]
    pub fn create_connection(&self) -> Result<DuckDbConnection, ServerError> {
        let config = Config::default()
            .enable_autoload_extension(true)?
            .allow_unsigned_extensions()?;
        let conn = Connection::open_in_memory_with_flags(config)?;
        conn.execute_batch(&self.init_sql)?;
        info!("created new DuckDB connection");
        Ok(DuckDbConnection::new(conn))
    }
}
