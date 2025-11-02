use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use duckdb::{Config, DuckdbConnectionManager};
use r2d2::Pool;
use tracing::{info, instrument};

use crate::config::ServerConfig;
use crate::error::ServerError;

#[derive(Clone)]
pub struct DuckDbEngine {
    inner: Arc<EngineInner>,
}

struct EngineInner {
    pool: Pool<DuckdbConnectionManager>,
}

pub struct QueryResult {
    pub schema: Schema,
    pub batches: Vec<RecordBatch>,
    pub total_rows: usize,
    pub total_bytes: usize,
}

impl DuckDbEngine {
    pub fn new(config: &ServerConfig) -> Result<Self, ServerError> {
        let duckdb_config = Config::default()
            .enable_autoload_extension(true)?
            .allow_unsigned_extensions()?;

        let manager = match config.duckdb_path.as_ref() {
            Some(path) => DuckdbConnectionManager::file_with_flags(path, duckdb_config)?,
            None => DuckdbConnectionManager::memory_with_flags(duckdb_config)?,
        };

        let pool_size = config.pool_size.max(1);
        let pool = Pool::builder().max_size(pool_size).build(manager)?;

        let engine = Self {
            inner: Arc::new(EngineInner { pool }),
        };

        engine.initialize_ducklake(config)?;
        Ok(engine)
    }

    fn initialize_ducklake(&self, config: &ServerConfig) -> Result<(), ServerError> {
        if !config.ducklake_enable {
            info!("DuckLake extension disabled via configuration");
            return Ok(());
        }

        let conn = self.inner.pool.get()?;
        info!("Ensuring ducklake extension is installed and loaded");
        conn.execute_batch("INSTALL ducklake; LOAD ducklake;")?;

        if let Some(sql) = config.ducklake_init_sql.as_ref() {
            let trimmed = sql.trim();
            if !trimmed.is_empty() {
                info!("Running ducklake init SQL");
                conn.execute_batch(trimmed)?;
            }
        }

        Ok(())
    }

    #[instrument(skip_all, fields(sql = %sql))]
    pub fn schema_for_query(&self, sql: &str) -> Result<Schema, ServerError> {
        let conn = self.inner.pool.get()?;
        let mut stmt = conn.prepare(sql)?;
        let arrow = stmt.query_arrow([])?;
        let schema = arrow.get_schema();

        Ok(schema.as_ref().clone())
    }

    #[instrument(skip_all, fields(sql = %sql))]
    pub fn execute_query(&self, sql: &str) -> Result<QueryResult, ServerError> {
        let conn = self.inner.pool.get()?;
        let mut stmt = conn.prepare(sql)?;
        let arrow = stmt.query_arrow([])?;
        let schema = arrow.get_schema();

        let mut total_rows = 0usize;
        let mut total_bytes = 0usize;
        let batches: Vec<RecordBatch> = arrow
            .map(|batch| {
                total_rows += batch.num_rows();
                total_bytes += batch.get_array_memory_size();
                batch
            })
            .collect();

        Ok(QueryResult {
            schema: schema.as_ref().clone(),
            batches,
            total_rows,
            total_bytes,
        })
    }
}
