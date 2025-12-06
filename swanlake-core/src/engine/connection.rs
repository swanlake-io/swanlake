//! DuckDB connection wrapper with query execution methods.
//!
//! Each connection is owned by a Session and maintains persistent state
//! (ATTACH, temp tables, etc.).

use std::sync::Mutex;

use arrow_array::RecordBatch;
use arrow_schema::{Field, Schema};
use duckdb::types::Value;
use duckdb::{params_from_iter, Connection};
use duckdb::{Arrow, Statement};
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
        let trimmed_sql = sql.trim_end_matches(';').trim();

        self.with_prepared(trimmed_sql, |stmt| {
            let arrow = Self::query_arrow(stmt, None)?;
            let schema = arrow.get_schema();
            debug!(field_count = schema.fields().len(), "retrieved schema");
            Ok(schema.as_ref().clone())
        })
    }

    /// Execute a SELECT query and return results
    #[instrument(skip(self), fields(sql = %sql))]
    pub fn execute_query(&self, sql: &str) -> Result<QueryResult, ServerError> {
        self.with_prepared(sql, |stmt| {
            let arrow = Self::query_arrow(stmt, None)?;
            let result = Self::collect_query_result(arrow);
            debug!(
                batch_count = result.batches.len(),
                total_rows = result.total_rows,
                total_bytes = result.total_bytes,
                "executed query"
            );
            Ok(result)
        })
    }

    /// Execute a query with parameters
    #[instrument(skip(self, params), fields(sql = %sql, param_count = params.len()))]
    pub fn execute_query_with_params(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<QueryResult, ServerError> {
        self.with_prepared(sql, |stmt| {
            let arrow = Self::query_arrow(stmt, Some(params))?;
            let result = Self::collect_query_result(arrow);
            debug!(
                batch_count = result.batches.len(),
                total_rows = result.total_rows,
                total_bytes = result.total_bytes,
                "executed query with parameters"
            );
            Ok(result)
        })
    }

    /// Execute a statement (DDL/DML) without returning results
    #[instrument(skip(self), fields(sql = %sql))]
    pub fn execute_statement(&self, sql: &str) -> Result<i64, ServerError> {
        Self::validate_sql(sql)?;
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
        self.with_prepared(sql, |stmt| {
            let affected = Self::execute_with_params(stmt, params)?;
            debug!(affected, "executed statement with parameters");
            Ok(affected)
        })
    }

    /// Execute a batch of SQL statements
    #[instrument(skip(self), fields(sql = %sql))]
    pub fn execute_batch(&self, sql: &str) -> Result<(), ServerError> {
        Self::validate_sql(sql)?;
        let conn = self.conn.lock().expect("connection mutex poisoned");
        conn.execute_batch(sql)?;
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

        let conn = self
            .conn
            .lock()
            .map_err(|_| ServerError::Internal("connection mutex poisoned".to_string()))?;
        conn.execute(&format!("USE {};", catalog_name), [])?;
        let mut appender = conn.appender(table_name)?;
        for batch in batches {
            appender.append_record_batch(batch)?;
        }
        appender.flush()?;

        debug!(
            rows = total_rows,
            table = %table_name,
            "inserted data using appender"
        );

        Ok(total_rows)
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

    /// Return the currently selected catalog (database) for this connection.
    pub fn current_catalog(&self) -> Result<String, ServerError> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| ServerError::Internal("connection mutex poisoned".to_string()))?;
        let mut stmt = conn
            .prepare("SELECT current_database()")
            .map_err(ServerError::DuckDb)?;
        let catalog: String = stmt
            .query_row([], |row| row.get(0))
            .map_err(ServerError::DuckDb)?;
        Ok(catalog)
    }

    /// Ensure SQL does not contain null bytes.
    fn validate_sql(sql: &str) -> Result<(), ServerError> {
        if sql.contains('\0') {
            return Err(ServerError::UnsupportedParameter(
                "SQL contains null bytes".to_string(),
            ));
        }
        Ok(())
    }

    /// Prepare a statement under the connection lock and run the provided closure.
    fn with_prepared<T, F>(&self, sql: &str, f: F) -> Result<T, ServerError>
    where
        F: FnOnce(&mut Statement) -> Result<T, ServerError>,
    {
        Self::validate_sql(sql)?;
        let conn = self.conn.lock().expect("connection mutex poisoned");
        let mut stmt = conn.prepare(sql)?;
        f(&mut stmt)
    }

    /// Execute a statement with parameters, handling the empty-params case.
    fn execute_with_params(stmt: &mut Statement, params: &[Value]) -> Result<usize, ServerError> {
        let affected = if params.is_empty() {
            stmt.execute([])?
        } else {
            stmt.execute(params_from_iter(params.iter()))?
        };
        Ok(affected)
    }

    /// Run a query, binding parameters if provided, or filling with NULLs otherwise.
    fn query_arrow<'a>(
        stmt: &'a mut Statement,
        params: Option<&[Value]>,
    ) -> Result<Arrow<'a>, ServerError> {
        match params {
            Some(values) => Ok(stmt.query_arrow(params_from_iter(values.iter()))?),
            None => {
                let param_count = stmt.parameter_count();
                if param_count == 0 {
                    Ok(stmt.query_arrow([])?)
                } else {
                    let nulls: Vec<Value> = (0..param_count).map(|_| Value::Null).collect();
                    Ok(stmt.query_arrow(params_from_iter(nulls))?)
                }
            }
        }
    }

    /// Collect Arrow batches into a QueryResult with row/byte totals.
    fn collect_query_result(arrow: Arrow<'_>) -> QueryResult {
        let schema = arrow.get_schema();
        let mut total_rows = 0usize;
        let mut total_bytes = 0usize;
        let batches: Vec<RecordBatch> = arrow
            .inspect(|batch| {
                total_rows += batch.num_rows();
                total_bytes += batch.get_array_memory_size();
            })
            .collect();

        QueryResult {
            schema: schema.as_ref().clone(),
            batches,
            total_rows,
            total_bytes,
        }
    }
}
