//! DuckDB connection wrapper with query execution methods.
//!
//! Each connection is owned by a Session and maintains persistent state
//! (ATTACH, temp tables, etc.).

use std::sync::Mutex;

use arrow_array::RecordBatch;
use arrow_schema::{Field, Schema};
use duckdb::types::Value;
use duckdb::{params_from_iter, Connection};
use r2d2::PooledConnection;
use tracing::{debug, info, instrument};

use crate::engine::factory::DuckDbManager;
use crate::error::ServerError;

use crate::types::duckdb_type_to_arrow;

/// Result of a query execution
pub struct QueryResult {
    pub schema: Schema,
    pub batches: Vec<RecordBatch>,
    pub total_rows: usize,
    pub total_bytes: usize,
}

/// Wrapper around duckdb::Connection with execution methods
///
/// The Connection is wrapped in a Mutex because duckdb::Connection contains
/// RefCell internally and is not Sync. This allows the connection to be
/// shared safely across async tasks.
pub struct DuckDbConnection {
    pub conn: Mutex<PooledConnection<DuckDbManager>>,
}

impl DuckDbConnection {
    /// Create a wrapper around a pooled connection.
    pub fn new(conn: PooledConnection<DuckDbManager>) -> Self {
        Self {
            conn: Mutex::new(conn),
        }
    }

    fn with_conn<T>(
        &self,
        f: impl FnOnce(&mut Connection) -> Result<T, ServerError>,
    ) -> Result<T, ServerError> {
        let mut guard = self
            .conn
            .lock()
            .map_err(|_| ServerError::Internal("connection mutex poisoned".to_string()))?;
        f(&mut guard)
    }

    /// Get the schema for a query without executing the full query.
    ///
    /// **Implementation Note:**
    /// DuckDB-rs doesn't provide a `Statement::schema()` method to get schema
    /// without execution. We execute the query as-is and rely on DuckDB's lazy
    /// evaluation to avoid pulling all data unnecessarily.
    ///
    /// This approach:
    /// 1. Is simple and always correct
    /// 2. Works with all SQL syntax (SHOW, DESCRIBE, PRAGMA, etc.)
    /// 3. Relies on DuckDB's streaming to avoid memory issues
    pub fn schema_for_query(&self, sql: &str) -> Result<Schema, ServerError> {
        if sql.contains('\0') {
            return Err(ServerError::UnsupportedParameter(
                "SQL contains null bytes".to_string(),
            ));
        }
        let trimmed_sql = sql.trim_end_matches(';').trim();

        self.with_conn(|conn| {
            let mut stmt = conn.prepare(trimmed_sql)?;
            let param_count = stmt.parameter_count();
            let arrow = if param_count == 0 {
                stmt.query_arrow([])?
            } else {
                let nulls: Vec<Value> = (0..param_count).map(|_| Value::Null).collect();
                stmt.query_arrow(params_from_iter(nulls))?
            };
            let schema = arrow.get_schema();

            debug!(field_count = schema.fields().len(), "retrieved schema");
            Ok(schema.as_ref().clone())
        })
    }

    /// Execute a SELECT query and return results
    #[instrument(skip(self), fields(sql = %sql))]
    pub fn execute_query(&self, sql: &str) -> Result<QueryResult, ServerError> {
        if sql.contains('\0') {
            return Err(ServerError::UnsupportedParameter(
                "SQL contains null bytes".to_string(),
            ));
        }
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(sql)?;
            let param_count = stmt.parameter_count();
            let arrow = if param_count == 0 {
                stmt.query_arrow([])?
            } else {
                let nulls: Vec<Value> = (0..param_count).map(|_| Value::Null).collect();
                stmt.query_arrow(params_from_iter(nulls))?
            };
            let schema = arrow.get_schema();

            let mut total_rows = 0usize;
            let mut total_bytes = 0usize;
            let batches: Vec<RecordBatch> = arrow
                .inspect(|batch| {
                    total_rows += batch.num_rows();
                    total_bytes += batch.get_array_memory_size();
                })
                .collect();

            debug!(
                batch_count = batches.len(),
                total_rows, total_bytes, "executed query"
            );
            Ok(QueryResult {
                schema: schema.as_ref().clone(),
                batches,
                total_rows,
                total_bytes,
            })
        })
    }

    /// Execute a query with parameters
    #[instrument(skip(self, params), fields(sql = %sql, param_count = params.len()))]
    pub fn execute_query_with_params(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<QueryResult, ServerError> {
        if sql.contains('\0') {
            return Err(ServerError::UnsupportedParameter(
                "SQL contains null bytes".to_string(),
            ));
        }
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(sql)?;
            let arrow = stmt.query_arrow(params_from_iter(params.iter()))?;
            let schema = arrow.get_schema();

            let mut total_rows = 0usize;
            let mut total_bytes = 0usize;
            let batches: Vec<RecordBatch> = arrow
                .inspect(|batch| {
                    total_rows += batch.num_rows();
                    total_bytes += batch.get_array_memory_size();
                })
                .collect();

            debug!(
                batch_count = batches.len(),
                total_rows, total_bytes, "executed query with parameters"
            );
            Ok(QueryResult {
                schema: schema.as_ref().clone(),
                batches,
                total_rows,
                total_bytes,
            })
        })
    }

    /// Execute a statement (DDL/DML) without returning results
    #[instrument(skip(self), fields(sql = %sql))]
    pub fn execute_statement(&self, sql: &str) -> Result<i64, ServerError> {
        if sql.contains('\0') {
            return Err(ServerError::UnsupportedParameter(
                "SQL contains null bytes".to_string(),
            ));
        }
        self.with_conn(|conn| Ok(conn.execute_batch(sql)?))?;
        debug!("executed statement");
        Ok(0)
    }

    /// Execute a statement with parameters
    #[instrument(skip(self, params), fields(sql = %sql, param_count = params.len()))]
    pub fn execute_statement_with_params(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<usize, ServerError> {
        if sql.contains('\0') {
            return Err(ServerError::UnsupportedParameter(
                "SQL contains null bytes".to_string(),
            ));
        }
        let affected = self.with_conn(|conn| {
            let mut stmt = conn.prepare(sql)?;
            if params.is_empty() {
                Ok(stmt.execute([])?)
            } else {
                Ok(stmt.execute(params_from_iter(params.iter()))?)
            }
        })?;
        debug!(affected, "executed statement with parameters");
        Ok(affected)
    }

    /// Execute a batch of SQL statements
    #[instrument(skip(self), fields(sql = %sql))]
    pub fn execute_batch(&self, sql: &str) -> Result<(), ServerError> {
        if sql.contains('\0') {
            return Err(ServerError::UnsupportedParameter(
                "SQL contains null bytes".to_string(),
            ));
        }
        self.with_conn(|conn| Ok(conn.execute_batch(sql)?))?;
        debug!("executed batch");
        Ok(())
    }

    /// Insert data using DuckDB's appender API with RecordBatches.
    ///
    /// This method is optimized for bulk inserts as it avoids converting
    /// RecordBatch to individual parameter values, reducing memory copies.
    ///
    /// # Arguments
    ///
    /// * `catalog_name` - The name of the catalog
    /// * `table_name` - The name of the table to insert into
    /// * `batches` - The RecordBatches containing data to insert
    ///
    /// # Returns
    ///
    /// The number of rows inserted
    #[instrument(skip(self, batches), fields(catalog_name = %catalog_name, table_name = %table_name, rows = batches.iter().map(|b| b.num_rows()).sum::<usize>()))]
    pub fn insert_with_appender(
        &self,
        catalog_name: &str,
        table_name: &str,
        batches: Vec<RecordBatch>,
    ) -> Result<usize, ServerError> {
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        info!(
            "appender to {}.{} with total rows {} and column {}",
            catalog_name,
            table_name,
            total_rows,
            batches.first().map(|b| b.num_columns()).unwrap_or(0)
        );

        self.with_conn(|conn| {
            conn.execute(&format!("USE {};", catalog_name), [])?;
            let mut appender = conn.appender(table_name)?;
            for batch in batches {
                appender.append_record_batch(batch)?;
            }
            appender.flush()?;
            Ok::<_, ServerError>(())
        })?;

        debug!(
            rows = total_rows,
            table = %table_name,
            "inserted data using appender"
        );

        Ok(total_rows)
    }

    /// Get the schema of a table using DESC SELECT
    pub fn table_schema(&self, table_name: &str) -> Result<arrow_schema::Schema, ServerError> {
        let desc_query = format!("DESC SELECT * FROM {}", table_name);
        let rows = self.with_conn(|conn| {
            let mut stmt = conn.prepare(&desc_query).map_err(ServerError::DuckDb)?;
            let mapped = stmt.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?, // column_name
                    row.get::<_, String>(1)?, // column_type
                    row.get::<_, String>(2)?, // null (YES or NO)
                ))
            })?;

            let mut collected = Vec::new();
            for row in mapped {
                collected.push(row.map_err(ServerError::DuckDb)?);
            }
            Ok::<_, ServerError>(collected)
        })?;

        let mut fields = Vec::new();
        for row in rows {
            let (name, duckdb_type, null_str) = row;
            let data_type = duckdb_type_to_arrow(&duckdb_type)?;
            let nullable = null_str == "YES";
            fields.push(Field::new(&name, data_type, nullable));
        }

        Ok(Schema::new(fields))
    }

    /// Return the currently selected catalog (database) for this connection.
    pub fn current_catalog(&self) -> Result<String, ServerError> {
        let catalog: String = self.with_conn(|conn| {
            let mut stmt = conn
                .prepare("SELECT current_database()")
                .map_err(ServerError::DuckDb)?;
            stmt.query_row([], |row| row.get(0))
                .map_err(ServerError::DuckDb)
        })?;
        Ok(catalog)
    }
}
