//! Factory for creating initialized DuckDB connections.
//!
//! Each connection is created with the same configuration and initialization SQL
//! (extensions, ATTACH statements, etc.).

use std::path::Path;
use std::sync::{Arc, Mutex};

use duckdb::{Config, Connection};
use tracing::{info, instrument};

use crate::config::ServerConfig;
use crate::engine::connection::DuckDbConnection;
use crate::error::ServerError;

/// Factory for creating initialized DuckDB connections
#[derive(Clone)]
pub struct EngineFactory {
    base_init_sql: String,
    duckling_queue_file_path: String,
    base_connection: Arc<Mutex<Connection>>,
}

impl EngineFactory {
    /// Create a new factory from configuration
    #[instrument(skip(config))]
    pub fn new(config: &ServerConfig, duckling_queue_path: &str) -> Result<Self, ServerError> {
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

        let base_init_sql = init_statements.join("\n");
        info!("base init sql {}", base_init_sql);

        let duckling_queue_file_path = duckling_queue_path.to_string();

        // Create base connection and execute init SQL
        let base_connection = Self::create_connection_with_sql(&Self::get_init_sql_static(
            &base_init_sql,
            &duckling_queue_file_path,
        ))?;

        Ok(Self {
            base_init_sql,
            duckling_queue_file_path,
            base_connection: Arc::new(Mutex::new(base_connection)),
        })
    }

    /// Create a DuckDB connection with the given init SQL
    fn create_connection_with_sql(init_sql: &str) -> Result<Connection, ServerError> {
        let config = Config::default()
            .enable_autoload_extension(true)?
            .allow_unsigned_extensions()?;
        let conn = Connection::open_in_memory_with_flags(config)?;
        conn.execute_batch(init_sql)?;
        Ok(conn)
    }

    /// Get the full init SQL including the attach statement
    fn get_init_sql_static(base_init_sql: &str, duckling_queue_file_path: &str) -> String {
        let attach_sql = format!("ATTACH '{}' AS duckling_queue;", duckling_queue_file_path);
        format!("{}\n{}", base_init_sql, attach_sql)
    }

    /// Get the full init SQL including the attach statement
    fn get_init_sql(&self) -> String {
        Self::get_init_sql_static(&self.base_init_sql, &self.duckling_queue_file_path)
    }

    /// Create a new initialized DuckDB connection by cloning the base connection
    #[instrument(skip(self))]
    pub fn clone_connection(&self) -> Result<DuckDbConnection, ServerError> {
        let base = self
            .base_connection
            .lock()
            .map_err(|_| ServerError::Internal("base connection lock poisoned".into()))?;
        let conn = base
            .try_clone()
            .map_err(|e| ServerError::Internal(format!("failed to clone connection: {}", e)))?;
        info!("DuckDB connection cloned from base connection");

        // conn.execute_batch(&self.get_init_sql())?;

        Ok(DuckDbConnection::new(conn))
    }

    /// Recreate the base connection with a new duckling queue path.
    /// This is called when the duckling queue rotates to a new file.
    #[instrument(skip(self))]
    pub fn recreate_base_connection(&mut self, new_dq_path: &Path) -> Result<(), ServerError> {
        info!(
            new_dq_path = %new_dq_path.display(),
            "recreating base connection with new duckling queue path"
        );

        self.duckling_queue_file_path = new_dq_path.display().to_string();

        let conn = Self::create_connection_with_sql(&self.get_init_sql())?;
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
