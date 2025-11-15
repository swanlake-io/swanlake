use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};

use crate::arrow::{value_as_bool, value_as_f64, value_as_i64, value_as_string};
use adbc_core::{
    error::Status as AdbcStatus,
    options::{AdbcVersion, OptionDatabase, OptionValue},
    Connection, Database, Driver, Statement,
};
use adbc_driver_flightsql::DRIVER_PATH;
use adbc_driver_manager::{ManagedConnection, ManagedDriver};
use anyhow::{anyhow, Context, Result};
use arrow_array::{RecordBatch, RecordBatchReader, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

struct CachedDriver {
    driver: Mutex<ManagedDriver>,
}

impl CachedDriver {
    fn new(driver: ManagedDriver) -> Self {
        Self {
            driver: Mutex::new(driver),
        }
    }

    fn new_connection(&self, endpoint: &str) -> Result<ManagedConnection> {
        let mut driver = self
            .driver
            .lock()
            .map_err(|e| anyhow!("Flight SQL driver mutex poisoned: {}", e))?;
        let database = driver
            .new_database_with_opts([(OptionDatabase::Uri, OptionValue::from(endpoint))])
            .with_context(|| "failed to create database handle")?;
        drop(driver);
        let connection = database
            .new_connection()
            .with_context(|| "failed to create Flight SQL connection")?;
        Ok(connection)
    }
}

static DRIVER_CACHE: OnceLock<Result<Arc<CachedDriver>>> = OnceLock::new();

fn get_cached_driver() -> Result<Arc<CachedDriver>> {
    match DRIVER_CACHE.get_or_init(|| {
        ManagedDriver::load_dynamic_from_filename(
            &PathBuf::from(DRIVER_PATH),
            None,
            AdbcVersion::default(),
        )
        .with_context(|| "failed to load Flight SQL driver")
        .map(|driver| Arc::new(CachedDriver::new(driver)))
    }) {
        Ok(driver) => Ok(driver.clone()),
        Err(e) => Err(anyhow!("failed to load Flight SQL driver: {}", e)),
    }
}

/// A Flight SQL client for connecting to SwanLake servers.
///
/// This client wraps an ADBC-managed connection to a Flight SQL server,
/// providing high-level methods for executing queries and updates.
///
/// # Example
///
/// ```rust,ignore
/// use flight_sql_client::FlightSQLClient;
///
/// let mut client = FlightSQLClient::connect("grpc://localhost:4214")?;
/// let result = client.execute("SELECT 1 as col")?;
/// println!("Rows: {}", result.total_rows);
/// # Ok::<(), anyhow::Error>(())
/// ```
pub struct FlightSQLClient {
    conn: ManagedConnection,
}

impl Clone for FlightSQLClient {
    fn clone(&self) -> Self {
        Self {
            conn: self.conn.clone(),
        }
    }
}

/// Result of executing a query.
///
/// Contains the Arrow record batches and metadata about the result set.
#[derive(Debug, Clone)]
pub struct QueryResult {
    /// The record batches returned by the query.
    pub batches: Vec<RecordBatch>,
    /// Total number of rows across all batches.
    pub total_rows: usize,
}

impl QueryResult {
    /// Create a new QueryResult from batches.
    ///
    /// # Example
    ///
    /// ```rust
    /// use flight_sql_client::QueryResult;
    /// use arrow_array::RecordBatch;
    ///
    /// let batches = vec![]; // Assume some batches
    /// let result = QueryResult::new(batches);
    /// ```
    pub fn new(batches: Vec<RecordBatch>) -> Self {
        let total_rows = batches.iter().map(|b| b.num_rows()).sum();
        Self {
            batches,
            total_rows,
        }
    }

    /// Get the schema from the first batch (if available).
    ///
    /// Returns `None` if there are no batches.
    pub fn schema(&self) -> Option<Arc<Schema>> {
        self.batches.first().map(|b| b.schema())
    }

    /// Check if the result is empty.
    ///
    /// Returns `true` if no rows were returned.
    pub fn is_empty(&self) -> bool {
        self.total_rows == 0
    }
}

/// Result of executing an update/DDL statement.
///
/// Contains the number of rows affected by the operation, if reported by the server.
#[derive(Debug, Clone)]
pub struct UpdateResult {
    /// Number of rows affected (if available).
    pub rows_affected: Option<i64>,
}

/// Result when executing an arbitrary SQL statement.
///
/// Distinguishes between queries that return data and commands that modify data.
pub enum StatementResult {
    /// A query that returns rows.
    Query {
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    },
    /// A command (e.g., INSERT, UPDATE) that may affect rows.
    Command { rows_affected: Option<i64> },
}

impl FlightSQLClient {
    /// Connect to a SwanLake Flight SQL server.
    ///
    /// Establishes a connection and tests it with a simple query.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use flight_sql_client::FlightSQLClient;
    ///
    /// let client = FlightSQLClient::connect("grpc://localhost:4214")?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    pub fn connect(endpoint: &str) -> Result<Self> {
        let driver = get_cached_driver()?;
        let mut conn = driver.new_connection(endpoint)?;
        // Test the connection by executing a simple query.
        let mut stmt = conn.new_statement()?;
        stmt.set_sql_query("SELECT 1")?;
        let _reader = stmt.execute()?;
        Ok(Self { conn })
    }

    /// Execute a query and return results.
    ///
    /// Use this for SELECT statements that return data.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use flight_sql_client::FlightSQLClient;
    ///
    /// let mut client = FlightSQLClient::connect("grpc://localhost:4214")?;
    /// let result = client.execute("SELECT id, name FROM users")?;
    /// println!("Rows: {}", result.total_rows);
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    pub fn execute(&mut self, sql: &str) -> Result<QueryResult> {
        match self.run_statement(sql)? {
            StatementResult::Query { batches, .. } => Ok(QueryResult::new(batches)),
            StatementResult::Command { .. } => Err(anyhow!("statement did not return rows")),
        }
    }

    /// Execute an update/DDL statement (INSERT, UPDATE, DELETE, CREATE, etc.).
    ///
    /// Use this for statements that modify data or schema.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use flight_sql_client::FlightSQLClient;
    ///
    /// let mut client = FlightSQLClient::connect("grpc://localhost:4214")?;
    /// let result = client.execute_update("CREATE TABLE test (id INTEGER)")?;
    /// println!("Affected: {:?}", result.rows_affected);
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    pub fn execute_update(&mut self, sql: &str) -> Result<UpdateResult> {
        match self.run_statement(sql)? {
            StatementResult::Command { rows_affected } => Ok(UpdateResult { rows_affected }),
            StatementResult::Query { .. } => Err(anyhow!("statement returned rows unexpectedly")),
        }
    }

    /// Execute a query with string parameters using prepared statements.
    ///
    /// Binds the parameters as a single column of strings.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use flight_sql_client::FlightSQLClient;
    ///
    /// let mut client = FlightSQLClient::connect("grpc://localhost:4214")?;
    /// let params = vec!["value1".to_string(), "value2".to_string()];
    /// let result = client.execute_with_params("SELECT * FROM table WHERE col = ?", params)?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    pub fn execute_with_params(&mut self, sql: &str, params: Vec<String>) -> Result<QueryResult> {
        let mut stmt = self.conn.new_statement()?;
        stmt.set_sql_query(sql)?;
        stmt.prepare()?;

        if !params.is_empty() {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "param",
                DataType::Utf8,
                false,
            )]));
            let batch = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(params))])?;
            stmt.bind(batch)?;
        }

        let reader = stmt.execute()?;
        let mut batches = Vec::new();
        for batch in reader {
            batches.push(batch?);
        }

        Ok(QueryResult::new(batches))
    }

    /// Execute an update with a record batch for batch inserts.
    ///
    /// Useful for bulk operations with Arrow data.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use flight_sql_client::FlightSQLClient;
    /// use arrow_array::{Int64Array, RecordBatch};
    /// use arrow_schema::{DataType, Field, Schema};
    /// use std::sync::Arc;
    ///
    /// let mut client = FlightSQLClient::connect("grpc://localhost:4214")?;
    /// let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    /// let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2]))])?;
    /// let result = client.execute_batch_update("INSERT INTO table VALUES (?)", batch)?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    pub fn execute_batch_update(&mut self, sql: &str, batch: RecordBatch) -> Result<UpdateResult> {
        let mut stmt = self.conn.new_statement()?;
        stmt.set_sql_query(sql)?;
        stmt.prepare()?;
        stmt.bind(batch)?;
        let rows_affected = stmt.execute_update()?;
        Ok(UpdateResult { rows_affected })
    }

    /// Get a reference to the underlying ADBC connection.
    ///
    /// Use this for advanced operations not covered by the high-level API.
    pub fn connection(&mut self) -> &mut ManagedConnection {
        &mut self.conn
    }

    /// Run a statement and return whether it produced rows or affected rows.
    ///
    /// Automatically detects if the statement is a query or command.
    pub fn run_statement(&mut self, sql: &str) -> Result<StatementResult> {
        let mut stmt = self.conn.new_statement()?;
        stmt.set_sql_query(sql)?;
        let is_query = match stmt.execute_schema() {
            Ok(schema) => !schema.fields().is_empty(),
            Err(err) => match err.status {
                AdbcStatus::NotImplemented | AdbcStatus::Unknown => infer_query_from_sql(sql),
                _ => return Err(anyhow!("failed to inspect query schema: {err}")),
            },
        };

        if is_query {
            let reader = stmt.execute()?;
            let schema = reader.schema();
            let mut batches = Vec::new();
            for batch in reader {
                batches.push(batch?);
            }
            Ok(StatementResult::Query { schema, batches })
        } else {
            let rows_affected = stmt
                .execute_update()?
                .and_then(|value| value.try_into().ok());
            Ok(StatementResult::Command { rows_affected })
        }
    }

    /// Execute a statement expected to be a command (non-query).
    ///
    /// Returns the number of affected rows, if available.
    pub fn exec(&mut self, sql: &str) -> Result<Option<i64>> {
        match self.run_statement(sql)? {
            StatementResult::Command { rows_affected } => Ok(rows_affected),
            StatementResult::Query { .. } => Err(anyhow!("statement unexpectedly returned rows")),
        }
    }

    /// Execute a query that returns a single i64 value.
    ///
    /// Useful for COUNT(*) or similar scalar queries.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use flight_sql_client::FlightSQLClient;
    ///
    /// let mut client = FlightSQLClient::connect("grpc://localhost:4214")?;
    /// let count = client.query_scalar_i64("SELECT COUNT(*) FROM users")?;
    /// println!("User count: {}", count);
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    pub fn query_scalar_i64(&mut self, sql: &str) -> Result<i64> {
        match self.run_statement(sql)? {
            StatementResult::Query { batches, .. } => {
                let batch = batches
                    .first()
                    .ok_or_else(|| anyhow!("query returned no rows"))?;
                if batch.num_rows() == 0 || batch.num_columns() == 0 {
                    return Err(anyhow!("query returned empty result"));
                }
                let column = batch.column(0);
                Ok(value_as_i64(column.as_ref(), 0)?)
            }
            StatementResult::Command { .. } => {
                Err(anyhow!("expected query to return a scalar result"))
            }
        }
    }

    /// Execute a query that returns a single f64 value.
    ///
    /// Useful for scalar float queries.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use flight_sql_client::FlightSQLClient;
    ///
    /// let mut client = FlightSQLClient::connect("grpc://localhost:4214")?;
    /// let value = client.query_scalar_f64("SELECT AVG(price) FROM products")?;
    /// println!("Average price: {}", value);
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    pub fn query_scalar_f64(&mut self, sql: &str) -> Result<f64> {
        match self.run_statement(sql)? {
            StatementResult::Query { batches, .. } => {
                let batch = batches
                    .first()
                    .ok_or_else(|| anyhow!("query returned no rows"))?;
                if batch.num_rows() == 0 || batch.num_columns() == 0 {
                    return Err(anyhow!("query returned empty result"));
                }
                let column = batch.column(0);
                Ok(value_as_f64(column.as_ref(), 0)?)
            }
            StatementResult::Command { .. } => {
                Err(anyhow!("expected query to return a scalar result"))
            }
        }
    }

    /// Execute a query that returns a single bool value.
    ///
    /// Useful for scalar boolean queries.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use flight_sql_client::FlightSQLClient;
    ///
    /// let mut client = FlightSQLClient::connect("grpc://localhost:4214")?;
    /// let exists = client.query_scalar_bool("SELECT EXISTS(SELECT 1 FROM users WHERE id = 1)")?;
    /// println!("User exists: {}", exists);
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    pub fn query_scalar_bool(&mut self, sql: &str) -> Result<bool> {
        match self.run_statement(sql)? {
            StatementResult::Query { batches, .. } => {
                let batch = batches
                    .first()
                    .ok_or_else(|| anyhow!("query returned no rows"))?;
                if batch.num_rows() == 0 || batch.num_columns() == 0 {
                    return Err(anyhow!("query returned empty result"));
                }
                let column = batch.column(0);
                Ok(value_as_bool(column.as_ref(), 0)?)
            }
            StatementResult::Command { .. } => {
                Err(anyhow!("expected query to return a scalar result"))
            }
        }
    }

    /// Execute a query that returns a single string value.
    ///
    /// Useful for scalar string queries.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use flight_sql_client::FlightSQLClient;
    ///
    /// let mut client = FlightSQLClient::connect("grpc://localhost:4214")?;
    /// let name = client.query_scalar_string("SELECT name FROM users WHERE id = 1")?;
    /// println!("User name: {}", name);
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    pub fn query_scalar_string(&mut self, sql: &str) -> Result<String> {
        match self.run_statement(sql)? {
            StatementResult::Query { batches, .. } => {
                let batch = batches
                    .first()
                    .ok_or_else(|| anyhow!("query returned no rows"))?;
                if batch.num_rows() == 0 || batch.num_columns() == 0 {
                    return Err(anyhow!("query returned empty result"));
                }
                let column = batch.column(0);
                Ok(value_as_string(column.as_ref(), 0)?)
            }
            StatementResult::Command { .. } => {
                Err(anyhow!("expected query to return a scalar result"))
            }
        }
    }
}

fn infer_query_from_sql(sql: &str) -> bool {
    let trimmed = sql.trim_start();
    if trimmed.is_empty() {
        return false;
    }
    let first_token = trimmed
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase();
    matches!(
        first_token.as_str(),
        "select" | "with" | "show" | "describe" | "explain" | "values"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_result_empty() {
        let result = QueryResult::new(vec![]);
        assert!(result.is_empty());
        assert_eq!(result.total_rows, 0);
    }
}
