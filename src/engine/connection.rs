//! DuckDB connection wrapper with query execution methods.
//!
//! Each connection is owned by a Session and maintains persistent state
//! (ATTACH, temp tables, etc.).

use std::sync::Mutex;

use arrow_array::RecordBatch;
use arrow_schema::{Field, Schema};
use duckdb::types::Value;
use duckdb::{params_from_iter, Connection};
use tracing::{debug, info, instrument};

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
    pub conn: Mutex<Connection>,
}

impl DuckDbConnection {
    /// Create a new connection wrapper
    pub fn new(conn: Connection) -> Self {
        Self {
            conn: Mutex::new(conn),
        }
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

        let conn = self.conn.lock().expect("connection mutex poisoned");
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
    }

    /// Execute a SELECT query and return results
    #[instrument(skip(self), fields(sql = %sql))]
    pub fn execute_query(&self, sql: &str) -> Result<QueryResult, ServerError> {
        if sql.contains('\0') {
            return Err(ServerError::UnsupportedParameter(
                "SQL contains null bytes".to_string(),
            ));
        }
        let conn = self.conn.lock().expect("connection mutex poisoned");
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
        let conn = self.conn.lock().expect("connection mutex poisoned");
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
    }

    /// Execute a statement (DDL/DML) without returning results
    #[instrument(skip(self), fields(sql = %sql))]
    pub fn execute_statement(&self, sql: &str) -> Result<i64, ServerError> {
        if sql.contains('\0') {
            return Err(ServerError::UnsupportedParameter(
                "SQL contains null bytes".to_string(),
            ));
        }
        let conn = self.conn.lock().expect("connection mutex poisoned");
        conn.execute_batch(sql)?;
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
        let conn = self.conn.lock().expect("connection mutex poisoned");
        let mut stmt = conn.prepare(sql)?;
        let affected = if params.is_empty() {
            stmt.execute([])?
        } else {
            stmt.execute(params_from_iter(params.iter()))?
        };
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
        let conn = self.conn.lock().expect("connection mutex poisoned");
        conn.execute_batch(sql)?;
        debug!("executed batch");
        Ok(())
    }

    /// Execute a closure with access to the inner duckdb::Connection
    pub fn with_inner<F, R>(&self, f: F) -> Result<R, ServerError>
    where
        F: FnOnce(&duckdb::Connection) -> R,
    {
        let conn = self
            .conn
            .lock()
            .map_err(|_| ServerError::Internal("connection mutex poisoned".to_string()))?;
        Ok(f(&conn))
    }

    /// Insert data using DuckDB's appender API with a RecordBatch.
    ///
    /// This method is optimized for bulk inserts as it avoids converting
    /// RecordBatch to individual parameter values, reducing memory copies.
    ///
    /// # Arguments
    ///
    /// * `table_name` - The name of the table to insert into
    /// * `batch` - The RecordBatch containing data to insert
    ///
    /// # Returns
    ///
    /// The number of rows inserted
    #[instrument(skip(self, batch), fields(table_name = %table_name, rows = batch.num_rows()))]
    pub fn insert_with_appender(
        &self,
        table_name: &str,
        batch: RecordBatch,
    ) -> Result<usize, ServerError> {
        let row_count = batch.num_rows();
        info!(
            "appender to {} with row {} and column {}",
            table_name,
            row_count,
            batch.num_columns()
        );

        let conn = self
            .conn
            .lock()
            .map_err(|_| ServerError::Internal("connection mutex poisoned".to_string()))?;
        let mut appender = conn.appender(table_name)?;
        appender.append_record_batch(batch)?;
        appender.flush()?;

        debug!(
            rows = row_count,
            table = %table_name,
            "inserted data using appender"
        );

        Ok(row_count)
    }

    /// Get the schema of a table using DESC SELECT
    pub fn table_schema(&self, table_name: &str) -> Result<arrow_schema::Schema, ServerError> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| ServerError::Internal("connection mutex poisoned".to_string()))?;

        // Use DESC to get table schema without preparing parameters
        let desc_query = format!("DESC SELECT * FROM {}", table_name);
        let mut stmt = conn.prepare(&desc_query).map_err(ServerError::DuckDb)?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?, // column_name
                    row.get::<_, String>(1)?, // column_type
                    row.get::<_, String>(2)?, // null (YES or NO)
                ))
            })
            .map_err(ServerError::DuckDb)?;

        let mut fields = Vec::new();
        for row in rows {
            let (name, duckdb_type, null_str) = row.map_err(ServerError::DuckDb)?;
            let data_type = duckdb_type_to_arrow(&duckdb_type)?;
            let nullable = null_str == "YES";
            fields.push(Field::new(&name, data_type, nullable));
        }

        Ok(Schema::new(fields))
    }
}
