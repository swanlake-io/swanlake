//! Factory for creating initialized DuckDB connections.
//!
//! Each connection is created with the same configuration and initialization SQL
//! (extensions, ATTACH statements, etc.).

use std::path::Path;
use std::sync::{Arc, Mutex};

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
    base_connection: Arc<Mutex<Connection>>,
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
                "INSTALL ducklake; INSTALL sqlite; INSTALL httpfs; INSTALL aws; INSTALL postgres; \
                 LOAD ducklake; LOAD sqlite; LOAD httpfs; LOAD aws; LOAD postgres;"
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

        let base_init_sql = init_statements.join("\n");

        // Create base connection and execute init SQL + duckling queue attach
        let base_connection = Self::create_base_connection(&base_init_sql, &dq_manager)?;

        Ok(Self {
            base_init_sql,
            base_connection: Arc::new(Mutex::new(base_connection)),
        })
    }

    /// Create a base connection with init SQL and duckling queue attachment
    fn create_base_connection(
        base_init_sql: &str,
        dq_manager: &Option<Arc<DucklingQueueManager>>,
    ) -> Result<Connection, ServerError> {
        Self::create_connection_with_sql(base_init_sql, dq_manager.as_ref().map(|m| m.attach_sql()))
    }

    /// Create a DuckDB connection with the given init SQL and optional attach SQL
    fn create_connection_with_sql(
        base_init_sql: &str,
        attach_sql: Option<String>,
    ) -> Result<Connection, ServerError> {
        let conn = Connection::open_in_memory_with_flags(
            Config::default()
                .enable_autoload_extension(true)?
                .allow_unsigned_extensions()?,
        )?;

        // Execute initialization SQL (extensions, ATTACH statements, etc.)
        let mut init_sql = base_init_sql.to_string();

        if let Some(attach) = attach_sql {
            if !attach.is_empty() {
                if !init_sql.is_empty() {
                    init_sql.push('\n');
                }
                init_sql.push_str(&attach);
            }
        }

        if !init_sql.is_empty() {
            info!("Initializing connection with: {}", init_sql);
            conn.execute_batch(&init_sql)?;
        }

        Ok(conn)
    }

    /// Create a new initialized DuckDB connection by cloning the base connection
    #[instrument(skip(self))]
    pub fn create_connection(&self) -> Result<DuckDbConnection, ServerError> {
        let base = self
            .base_connection
            .lock()
            .map_err(|_| ServerError::Internal("base connection lock poisoned".into()))?;
        let conn = base
            .try_clone()
            .map_err(|e| ServerError::Internal(format!("failed to clone connection: {}", e)))?;
        info!("DuckDB connection cloned from base connection");
        Ok(DuckDbConnection::new(conn))
    }

    /// Create a raw DuckDB connection with initialization SQL executed.
    pub fn create_raw_connection(&self) -> Result<Connection, ServerError> {
        let base = self
            .base_connection
            .lock()
            .map_err(|_| ServerError::Internal("base connection lock poisoned".into()))?;
        base.try_clone()
            .map_err(|e| ServerError::Internal(format!("failed to clone connection: {}", e)))
    }

    /// Recreate the base connection with a new duckling queue path.
    /// This is called when the duckling queue rotates to a new file.
    #[instrument(skip(self))]
    pub fn recreate_base_connection(&self, new_dq_path: &Path) -> Result<(), ServerError> {
        info!(
            new_dq_path = %new_dq_path.display(),
            "recreating base connection with new duckling queue path"
        );

        let attach_sql = format!(
            "ATTACH IF NOT EXISTS '{}' AS duckling_queue;",
            new_dq_path.display()
        );

        let conn = Self::create_connection_with_sql(&self.base_init_sql, Some(attach_sql))?;

        // Replace the base connection
        let mut base = self
            .base_connection
            .lock()
            .map_err(|_| ServerError::Internal("base connection lock poisoned".into()))?;
        *base = conn;

        info!("base connection recreated successfully");
        Ok(())
    }
}
