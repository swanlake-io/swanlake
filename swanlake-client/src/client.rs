use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};

use adbc_core::{
    options::{AdbcVersion, OptionDatabase, OptionValue},
    Connection, Database, Driver, Statement,
};
use adbc_driver_flightsql::DRIVER_PATH;
use adbc_driver_manager::{ManagedConnection, ManagedDriver};
use anyhow::{anyhow, Context, Result};
use arrow_array::RecordBatch;
use arrow_schema::Schema;

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
            PathBuf::from(DRIVER_PATH),
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
    /// Rows affected reported by the server (if available).
    pub rows_affected: Option<i64>,
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
            rows_affected: None,
        }
    }

    pub fn with_rows_affected(batches: Vec<RecordBatch>, rows_affected: Option<i64>) -> Self {
        let total_rows = batches.iter().map(|b| b.num_rows()).sum();
        Self {
            batches,
            total_rows,
            rows_affected,
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

    /// Execute a query and return results. Use when you expect rows.
    pub fn query(&mut self, sql: &str) -> Result<QueryResult> {
        let mut stmt = self.conn.new_statement()?;
        stmt.set_sql_query(sql)?;

        let reader = stmt.execute()?;
        let mut batches = Vec::new();
        for batch in reader {
            batches.push(batch?);
        }
        Ok(QueryResult::new(batches))
    }

    /// Convenience wrapper that always calls `query`. The server accepts
    /// commands via ExecuteQuery, so callers that are unsure can use this
    /// method.
    pub fn execute(&mut self, sql: &str) -> Result<QueryResult> {
        self.query(sql)
    }

    /// Execute an update/DDL statement (INSERT, UPDATE, DELETE, CREATE, etc.).
    pub fn update(&mut self, sql: &str) -> Result<UpdateResult> {
        let mut stmt = self.conn.new_statement()?;
        stmt.set_sql_query(sql)?;
        let rows_affected = stmt.execute_update()?;
        Ok(UpdateResult { rows_affected })
    }

    /// Execute a query with parameters using a prepared statement.
    ///
    /// The parameters are provided as a `RecordBatch`, allowing callers to bind
    /// typed values directly.
    pub fn query_with_param(&mut self, sql: &str, params: RecordBatch) -> Result<QueryResult> {
        let mut stmt = self.conn.new_statement()?;
        stmt.set_sql_query(sql)?;
        stmt.prepare()?;

        stmt.bind(params)?;

        let reader = stmt.execute()?;
        let mut batches = Vec::new();
        for batch in reader {
            batches.push(batch?);
        }

        Ok(QueryResult::new(batches))
    }

    /// Execute an update with a record batch for batch inserts or parameterized DML.
    pub fn update_with_record_batch(
        &mut self,
        sql: &str,
        batch: RecordBatch,
    ) -> Result<UpdateResult> {
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
