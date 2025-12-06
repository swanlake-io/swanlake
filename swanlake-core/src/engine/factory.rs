//! Pooled DuckDB connection factory.
//!
//! Each pooled connection is created with the same initialization SQL
//! (extensions, ATTACH statements, etc.) so sessions can check out ready-to-use
//! DuckDB instances without re-running setup.

use std::time::Duration;

use duckdb::{Config, Connection};
use r2d2::{CustomizeConnection, ManageConnection, Pool};
use tracing::{info, instrument};

use crate::config::ServerConfig;
use crate::engine::connection::DuckDbConnection;
use crate::error::ServerError;

/// Pool of initialized DuckDB connections.
#[derive(Clone)]
pub struct EnginePool {
    pool: Pool<DuckDbManager>,
}

/// r2d2 manager that builds DuckDB connections with the configured init SQL.
#[derive(Clone)]
pub struct DuckDbManager {
    init_sql: String,
}

impl EnginePool {
    /// Create a new pool from configuration
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

        let manager = DuckDbManager {
            init_sql: init_sql.clone(),
        };

        let max_size = config.max_sessions.unwrap_or(100) as u32;
        let min_idle = max_size.clamp(1, 4);

        let pool = Pool::builder()
            .max_size(max_size)
            .min_idle(Some(min_idle))
            .connection_customizer(Box::new(ResetOnAcquire))
            .build(manager)
            .map_err(|err| ServerError::Internal(format!("failed to build DuckDB pool: {err}")))?;

        Ok(Self { pool })
    }

    /// Create a new initialized DuckDB connection
    ///
    /// Each connection is created fresh with its own in-memory database.
    /// This ensures complete isolation between sessions.
    #[instrument(skip(self))]
    pub fn create_connection(&self) -> Result<DuckDbConnection, ServerError> {
        let pooled = self
            .pool
            .get_timeout(Duration::from_secs(5))
            .map_err(|err| {
                let msg = err.to_string();
                if msg.contains("timed out") {
                    ServerError::MaxSessionsReached
                } else {
                    ServerError::Internal(format!("failed to get pooled DuckDB connection: {msg}"))
                }
            })?;

        info!("checked out pooled DuckDB connection");
        Ok(DuckDbConnection::new(pooled))
    }
}

impl DuckDbManager {
    fn new_connection(&self) -> Result<Connection, ServerError> {
        Self::create_connection_with_sql(&self.init_sql)
    }

    fn create_connection_with_sql(init_sql: &str) -> Result<Connection, ServerError> {
        let config = Config::default()
            .enable_autoload_extension(true)?
            .allow_unsigned_extensions()?;
        let conn = Connection::open_in_memory_with_flags(config)?;
        conn.execute_batch(init_sql)?;
        Ok(conn)
    }
}

#[derive(Debug, Clone)]
struct ResetOnAcquire;

impl ManageConnection for DuckDbManager {
    type Connection = Connection;
    type Error = ServerError;

    fn connect(&self) -> Result<Self::Connection, Self::Error> {
        self.new_connection()
    }

    fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        conn.execute_batch("SELECT 1;")?;
        Ok(())
    }

    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        false
    }
}

impl CustomizeConnection<Connection, ServerError> for ResetOnAcquire {
    fn on_acquire(&self, conn: &mut Connection) -> Result<(), ServerError> {
        if let Err(err) = conn.execute_batch("ROLLBACK;") {
            let msg = err.to_string();
            if !msg.contains("no transaction is active") {
                return Err(ServerError::DuckDb(err));
            }
        }

        conn.execute_batch("USE memory; SET schema = main;")?;
        Ok(())
    }
}
