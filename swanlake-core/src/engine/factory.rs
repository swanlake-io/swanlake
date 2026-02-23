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
    init_lock: Arc<Mutex<()>>,
}

impl EngineFactory {
    /// Create a new factory from configuration
    #[instrument(skip(config))]
    pub fn new(config: &ServerConfig) -> Result<Self, ServerError> {
        Ok(Self::new_with_extension_bootstrap(config, true))
    }

    #[cfg(test)]
    pub(crate) fn new_for_tests(config: &ServerConfig) -> Self {
        Self::new_with_extension_bootstrap(config, false)
    }

    fn new_with_extension_bootstrap(config: &ServerConfig, bootstrap_extensions: bool) -> Self {
        let mut init_statements = Vec::new();
        if bootstrap_extensions {
            init_statements.push(
                "INSTALL ducklake; INSTALL httpfs; INSTALL aws; INSTALL postgres; \
                LOAD ducklake; LOAD httpfs; LOAD aws; LOAD postgres;"
                    .to_string(),
            );
        }

        if let Some(threads) = config.duckdb_threads {
            let threads = threads.max(1);
            info!(threads, "applying DuckDB threads override");
            init_statements.push(format!("SET threads = {threads};"));
        }

        if let Some(sql) = config.ducklake_init_sql.as_ref() {
            let trimmed = sql.trim();
            if !trimmed.is_empty() {
                info!("Adding ducklake init SQL");
                init_statements.push(trimmed.to_string());
            }
        }

        let init_sql = init_statements.join("\n");
        if init_sql.is_empty() {
            info!("no additional DuckDB init SQL configured");
        } else {
            info!("base init sql {}", init_sql);
        }

        Self {
            init_sql,
            init_lock: Arc::new(Mutex::new(())),
        }
    }

    /// Create a new initialized DuckDB connection
    ///
    /// Each connection is created fresh with its own in-memory database.
    /// This ensures complete isolation between sessions.
    #[instrument(skip(self))]
    pub fn create_connection(&self) -> Result<DuckDbConnection, ServerError> {
        // Serialize connection bootstrap so DuckLake metadata initialization does not race
        // across concurrently-created sessions.
        let _guard = self
            .init_lock
            .lock()
            .map_err(|_| ServerError::Internal("engine init lock poisoned".to_string()))?;

        let config = Config::default()
            .enable_autoload_extension(true)?
            .allow_unsigned_extensions()?;
        let conn = Connection::open_in_memory_with_flags(config)?;
        if !self.init_sql.is_empty() {
            conn.execute_batch(&self.init_sql)?;
        }
        info!("created new DuckDB connection");
        Ok(DuckDbConnection::new(conn))
    }
}

#[cfg(test)]
mod tests {
    use super::EngineFactory;
    use crate::config::ServerConfig;

    #[test]
    fn new_for_tests_skips_extension_bootstrap_sql() {
        let factory = EngineFactory::new_for_tests(&ServerConfig::default());
        assert!(!factory.init_sql.contains("INSTALL ducklake"));
    }
}
